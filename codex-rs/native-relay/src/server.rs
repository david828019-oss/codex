//! HTTP front end: shared-secret check, path routing, and streaming the upstream reply back.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::extract::DefaultBodyLimit;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::Method;
use axum::http::StatusCode;
use axum::http::Uri;
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
use crate::forward::upstream_path;

struct RelayState {
    secret_sha256: [u8; 32],
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
            forwarder: Forwarder::new(auth_mode, upstream),
        });
        let router = Router::new()
            .route("/healthz", get(health))
            .fallback(any(relay))
            .layer(DefaultBodyLimit::max(max_body_bytes))
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

async fn relay(
    State(state): State<Arc<RelayState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !secret_matches(&state, &headers) {
        return error_response(RelayError::new(
            StatusCode::UNAUTHORIZED,
            "missing or invalid relay secret".to_string(),
        ));
    }
    let Some(path) = upstream_path(uri.path()) else {
        return error_response(RelayError::new(
            StatusCode::NOT_FOUND,
            format!("unsupported relay path {}", uri.path()),
        ));
    };
    if method != Method::POST {
        return error_response(RelayError::new(
            StatusCode::METHOD_NOT_ALLOWED,
            "only POST is supported".to_string(),
        ));
    }

    let started = Instant::now();
    match state.forwarder.forward(path, &headers, body).await {
        Ok(reply) => {
            tracing::info!(
                path,
                status = reply.status.as_u16(),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "codex relay forwarded request"
            );
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
        Err(err) => {
            tracing::warn!(
                path,
                status = err.status.as_u16(),
                "codex relay failed: {}",
                err.message
            );
            error_response(err)
        }
    }
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
