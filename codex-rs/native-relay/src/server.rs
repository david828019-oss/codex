//! HTTP front end: shared-secret check, route dispatch, and streaming the upstream reply back.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::extract::FromRequestParts;
use axum::extract::Request;
use axum::extract::State;
use axum::extract::ws::WebSocketUpgrade;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::any;
use axum::routing::get;
use constant_time_eq::constant_time_eq_32;
use futures::StreamExt;
use rand::RngCore;
use sha2::Digest;
use sha2::Sha256;
use tokio::net::TcpListener;

use crate::RELAY_ERROR_HEADER;
use crate::RELAY_SECRET_HEADER;
use crate::RelayAuthMode;
use crate::RelayUpstream;
use crate::forward::Forwarder;
use crate::forward::RelayError;
use crate::forward::UpstreamBody;
use crate::forward::UpstreamReply;
use crate::forward::UpstreamTarget;
use crate::multipart::parse_file_upload;
use crate::routes::RelayRoute;
use crate::routes::resolve_route;
use crate::websocket;

/// Upper bound for a model catalog that is buffered to convert it into the OpenAI list shape.
const MAX_MODEL_CATALOG_BYTES: usize = 16 * 1024 * 1024;

struct RelayState {
    secret_sha256: [u8; 32],
    max_body_bytes: usize,
    forwarder: Forwarder,
}

/// A bound relay listener that serves requests until its shutdown future resolves.
pub struct RelayServer {
    listener: TcpListener,
    router: Router,
}

impl RelayServer {
    pub async fn bind(
        listen: SocketAddr,
        secret: String,
        auth_mode: RelayAuthMode,
        max_body_bytes: usize,
        upstream: RelayUpstream,
    ) -> anyhow::Result<Self> {
        let state = Arc::new(RelayState {
            secret_sha256: Sha256::digest(secret.as_bytes()).into(),
            max_body_bytes,
            forwarder: Forwarder::new(auth_mode, upstream),
        });
        let router = Router::new()
            .route("/healthz", get(health))
            .fallback(any(relay))
            .with_state(state);
        let listener = TcpListener::bind(listen).await?;
        Ok(Self { listener, router })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], 0)))
    }

    pub async fn serve_until(
        self,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> anyhow::Result<()> {
        axum::serve(self.listener, self.router)
            .with_graceful_shutdown(shutdown)
            .await?;
        Ok(())
    }
}

pub(crate) fn generate_secret() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn secret_matches(state: &RelayState, headers: &HeaderMap) -> bool {
    let Some(presented) = headers.get(RELAY_SECRET_HEADER) else {
        return false;
    };
    let presented: [u8; 32] = Sha256::digest(presented.as_bytes()).into();
    constant_time_eq_32(&presented, &state.secret_sha256)
}

async fn health(State(state): State<Arc<RelayState>>, headers: HeaderMap) -> Response {
    if !secret_matches(&state, &headers) {
        return error_response(RelayError::new(
            StatusCode::UNAUTHORIZED,
            "missing or invalid relay secret".to_string(),
        ));
    }
    let auth_mode = match state.forwarder.auth_mode() {
        RelayAuthMode::Codex => "codex",
        RelayAuthMode::Passthrough => "passthrough",
    };
    let body = serde_json::json!({
        "status": "ok",
        "auth_mode": auth_mode,
        "codex_logged_in": state.forwarder.is_logged_in().await,
        "version": env!("CARGO_PKG_VERSION"),
    });
    (
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

async fn relay(State(state): State<Arc<RelayState>>, request: Request) -> Response {
    let (mut parts, body) = request.into_parts();
    if !secret_matches(&state, &parts.headers) {
        return error_response(RelayError::new(
            StatusCode::UNAUTHORIZED,
            "missing or invalid relay secret".to_string(),
        ));
    }
    let path = parts.uri.path().to_string();
    let websocket_upgrade = parts
        .headers
        .get(header::UPGRADE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("websocket"));
    let route = match resolve_route(&parts.method, &path, websocket_upgrade) {
        Ok(route) => route,
        Err(err) => return error_response(err),
    };
    let started = Instant::now();

    if route == RelayRoute::ResponsesWebSocket {
        let upgrade = match WebSocketUpgrade::from_request_parts(&mut parts, &()).await {
            Ok(upgrade) => upgrade,
            Err(rejection) => {
                return error_response(RelayError::new(
                    rejection.status(),
                    format!("invalid websocket upgrade: {}", rejection.body_text()),
                ));
            }
        };
        return relay_websocket(&state, upgrade, &parts.headers, &path, started).await;
    }

    let body = match axum::body::to_bytes(body, state.max_body_bytes).await {
        Ok(body) => body,
        Err(_) => {
            return error_response(RelayError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("request body exceeds {} bytes", state.max_body_bytes),
            ));
        }
    };
    let query = parts.uri.query().map(str::to_string);
    let headers = parts.headers;
    let result = match route {
        RelayRoute::Passthrough { base, method, path } => {
            let target = UpstreamTarget::Path {
                base,
                method,
                path,
                query,
            };
            state.forwarder.forward(&target, &headers, body).await
        }
        RelayRoute::Models { openai_list } => {
            match state
                .forwarder
                .forward(&UpstreamTarget::ModelCatalog, &headers, Bytes::new())
                .await
            {
                Ok(reply) if openai_list && reply.status.is_success() => {
                    Ok(into_openai_model_list(reply).await)
                }
                other => other,
            }
        }
        RelayRoute::FileUpload => {
            let content_type = headers
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default();
            match parse_file_upload(content_type, &body) {
                Ok(form) => state.forwarder.upload_file(&headers, form).await,
                Err(message) => Err(RelayError::new(StatusCode::BAD_REQUEST, message)),
            }
        }
        RelayRoute::ResponsesWebSocket => Err(RelayError::new(
            StatusCode::BAD_REQUEST,
            "websocket upgrade required".to_string(),
        )),
    };
    match result {
        Ok(reply) => {
            log_forwarded(&path, reply.status, started);
            reply_response(reply)
        }
        Err(err) => {
            log_failed(&path, &err);
            error_response(err)
        }
    }
}

async fn relay_websocket(
    state: &Arc<RelayState>,
    upgrade: WebSocketUpgrade,
    headers: &HeaderMap,
    path: &str,
    started: Instant,
) -> Response {
    match websocket::connect_upstream(&state.forwarder, headers).await {
        Ok(upstream) => {
            log_forwarded(path, StatusCode::SWITCHING_PROTOCOLS, started);
            let connection = upstream.connection;
            let mut response = upgrade
                .max_message_size(state.max_body_bytes)
                .on_upgrade(move |socket| websocket::bridge(socket, connection));
            for (name, value) in &upstream.headers {
                if !response.headers().contains_key(name) {
                    response.headers_mut().append(name.clone(), value.clone());
                }
            }
            response
        }
        Err(websocket::ConnectError::Rejected(reply)) => {
            log_forwarded(path, reply.status, started);
            reply_response(reply)
        }
        Err(websocket::ConnectError::Relay(err)) => {
            log_failed(path, &err);
            error_response(err)
        }
    }
}

/// Converts Codex's model catalog (`{"models":[{"slug":...}]}`) into the OpenAI platform list
/// shape. Anything unexpected is returned unchanged.
async fn into_openai_model_list(reply: UpstreamReply) -> UpstreamReply {
    let UpstreamReply {
        status,
        mut headers,
        body,
    } = reply;
    let raw = match body {
        UpstreamBody::Full(bytes) => bytes,
        UpstreamBody::Stream(mut stream) => {
            let mut buffer = Vec::new();
            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(chunk) if buffer.len() + chunk.len() <= MAX_MODEL_CATALOG_BYTES => {
                        buffer.extend_from_slice(&chunk);
                    }
                    Ok(_) | Err(_) => {
                        return UpstreamReply {
                            status: StatusCode::BAD_GATEWAY,
                            headers: HeaderMap::new(),
                            body: UpstreamBody::Full(Bytes::from_static(
                                b"{\"error\":{\"type\":\"codex_relay_error\",\"message\":\"failed to read model catalog\"}}",
                            )),
                        };
                    }
                }
            }
            Bytes::from(buffer)
        }
    };
    let data = serde_json::from_slice::<serde_json::Value>(&raw)
        .ok()
        .and_then(|catalog| {
            catalog.get("models")?.as_array().map(|models| {
                models
                    .iter()
                    .filter_map(|model| model.get("slug")?.as_str())
                    .map(|slug| {
                        serde_json::json!({
                            "id": slug,
                            "object": "model",
                            "created": 0,
                            "owned_by": "openai",
                        })
                    })
                    .collect::<Vec<_>>()
            })
        });
    let body = match data {
        Some(data) => {
            headers.remove(header::ETAG);
            headers.insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("application/json"),
            );
            Bytes::from(serde_json::json!({ "object": "list", "data": data }).to_string())
        }
        None => raw,
    };
    UpstreamReply {
        status,
        headers,
        body: UpstreamBody::Full(body),
    }
}

fn reply_response(reply: UpstreamReply) -> Response {
    let body = match reply.body {
        UpstreamBody::Stream(stream) => Body::from_stream(
            stream.map(|chunk| chunk.map_err(|err| std::io::Error::other(err.to_string()))),
        ),
        UpstreamBody::Full(bytes) => Body::from(bytes),
    };
    let mut response = Response::new(body);
    *response.status_mut() = reply.status;
    *response.headers_mut() = reply.headers;
    response
}

fn log_forwarded(path: &str, status: StatusCode, started: Instant) {
    tracing::info!(
        path,
        status = status.as_u16(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "codex relay forwarded request"
    );
}

fn log_failed(path: &str, err: &RelayError) {
    tracing::warn!(
        path,
        status = err.status.as_u16(),
        "codex relay failed: {}",
        err.message
    );
}

fn error_response(err: RelayError) -> Response {
    let stage = if err.upstream_attempted {
        "upstream"
    } else {
        "relay"
    };
    let body = serde_json::json!({
        "error": {
            "type": "codex_relay_error",
            "message": err.message,
        }
    });
    (
        err.status,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::HeaderName::from_static(RELAY_ERROR_HEADER), stage),
        ],
        body.to_string(),
    )
        .into_response()
}
