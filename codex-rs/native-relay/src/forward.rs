//! Builds and sends one relayed request through Codex's native provider, auth, and HTTP client.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use codex_api::Provider;
use codex_api::ReqwestTransport;
use codex_api::SharedAuthProvider;
use codex_api::TransportError;
use codex_http_client::ByteStream;
use codex_http_client::ClientRouteClass;
use codex_http_client::HttpClientFactory;
use codex_http_client::HttpTransport;
use codex_http_client::RequestBody;
use codex_login::AuthManager;
use codex_login::default_client::ClientRedirectPolicy;
use codex_login::default_client::create_client_for_route;
use codex_model_provider::BearerAuthProvider;
use codex_model_provider::SharedModelProvider;
use codex_model_provider::create_model_provider;
use codex_model_provider_info::ModelProviderInfo;
use codex_protocol::auth::AuthMode;
use http::HeaderMap;
use http::Method;
use http::StatusCode;
use http::header;

use crate::RELAY_SECRET_HEADER;
use crate::RelayAuthMode;
use crate::RelayUpstream;

const CHATGPT_ACCOUNT_ID_HEADER: &str = "chatgpt-account-id";
const FEDRAMP_HEADER: &str = "x-openai-fedramp";

/// Caller headers that never reach upstream: hop-by-hop headers, credentials (Codex attaches its
/// own), and the client identity headers Codex sets natively.
const DROPPED_REQUEST_HEADERS: &[&str] = &[
    "accept-encoding",
    "authorization",
    CHATGPT_ACCOUNT_ID_HEADER,
    "connection",
    "content-length",
    "cookie",
    "forwarded",
    "host",
    "keep-alive",
    "openai-organization",
    "openai-project",
    "originator",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "user-agent",
    "version",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
    FEDRAMP_HEADER,
    "x-real-ip",
    RELAY_SECRET_HEADER,
];

const DROPPED_RESPONSE_HEADERS: &[&str] = &[
    "connection",
    "content-length",
    "keep-alive",
    "set-cookie",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// Maps an inbound relay path to the Responses endpoint path on the configured provider.
///
/// Accepts the OpenAI platform shape (`/v1/responses`), the ChatGPT Codex backend shape
/// (`/backend-api/codex/responses`), and the bare shape (`/responses`), each optionally followed
/// by `/compact`.
pub(crate) fn upstream_path(path: &str) -> Option<&'static str> {
    let path = ["/backend-api/codex", "/v1"]
        .iter()
        .find_map(|prefix| path.strip_prefix(prefix))
        .unwrap_or(path);
    match path.trim_end_matches('/') {
        "/responses" => Some("/responses"),
        "/responses/compact" => Some("/responses/compact"),
        _ => None,
    }
}

/// Copies caller headers that are safe to forward without overriding native provider headers.
pub(crate) fn merge_caller_headers(target: &mut HeaderMap, caller: &HeaderMap) {
    for (name, value) in caller {
        if DROPPED_REQUEST_HEADERS.contains(&name.as_str()) || target.contains_key(name) {
            continue;
        }
        target.append(name.clone(), value.clone());
    }
}

pub(crate) fn filter_response_headers(headers: &HeaderMap) -> HeaderMap {
    let mut filtered = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers {
        if !DROPPED_RESPONSE_HEADERS.contains(&name.as_str()) {
            filtered.append(name.clone(), value.clone());
        }
    }
    filtered
}

pub(crate) enum UpstreamBody {
    Stream(ByteStream),
    Full(Bytes),
}

pub(crate) struct UpstreamReply {
    pub(crate) status: StatusCode,
    pub(crate) headers: HeaderMap,
    pub(crate) body: UpstreamBody,
}

/// A failure that happened before an upstream HTTP response was available.
#[derive(Debug)]
pub(crate) struct RelayError {
    pub(crate) status: StatusCode,
    pub(crate) message: String,
    /// Whether the upstream request may have been sent, so callers know if a retry could
    /// duplicate work.
    pub(crate) upstream_attempted: bool,
}

impl RelayError {
    pub(crate) fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            upstream_attempted: false,
        }
    }
}

pub(crate) struct Forwarder {
    auth_mode: RelayAuthMode,
    provider_info: ModelProviderInfo,
    model_provider: SharedModelProvider,
    auth_manager: Arc<AuthManager>,
    http_client_factory: HttpClientFactory,
    transports: Mutex<HashMap<String, ReqwestTransport>>,
}

impl Forwarder {
    pub(crate) fn new(auth_mode: RelayAuthMode, upstream: RelayUpstream) -> Self {
        let RelayUpstream {
            provider_info,
            auth_manager,
            http_client_factory,
        } = upstream;
        let model_provider =
            create_model_provider(provider_info.clone(), Some(Arc::clone(&auth_manager)));
        Self {
            auth_mode,
            provider_info,
            model_provider,
            auth_manager,
            http_client_factory,
            transports: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn auth_mode(&self) -> RelayAuthMode {
        self.auth_mode
    }

    pub(crate) async fn is_logged_in(&self) -> bool {
        self.auth_manager.auth().await.is_some()
    }

    pub(crate) async fn forward(
        &self,
        path: &str,
        caller_headers: &HeaderMap,
        body: Bytes,
    ) -> Result<UpstreamReply, RelayError> {
        let mut result = self.send_once(path, caller_headers, body.clone()).await;
        // Mirror Codex's own turn loop: a 401 on native auth triggers one refresh and retry.
        if self.auth_mode == RelayAuthMode::Codex
            && matches!(
                &result,
                Err(SendError::Transport(TransportError::Http { status, .. }))
                    if *status == StatusCode::UNAUTHORIZED
            )
        {
            match self.auth_manager.refresh_token().await {
                Ok(()) => result = self.send_once(path, caller_headers, body).await,
                Err(err) => tracing::warn!("codex relay token refresh failed: {err}"),
            }
        }
        match result {
            Ok(stream) => Ok(UpstreamReply {
                status: stream.status,
                headers: filter_response_headers(&stream.headers),
                body: UpstreamBody::Stream(stream.bytes),
            }),
            Err(SendError::Relay(err)) => Err(err),
            Err(SendError::Transport(TransportError::Http {
                status,
                headers,
                body,
                ..
            })) => Ok(UpstreamReply {
                status,
                headers: headers
                    .as_ref()
                    .map(filter_response_headers)
                    .unwrap_or_default(),
                body: UpstreamBody::Full(Bytes::from(body.unwrap_or_default())),
            }),
            Err(SendError::Transport(err)) => Err(RelayError {
                status: StatusCode::BAD_GATEWAY,
                message: format!("upstream request failed: {err}"),
                upstream_attempted: true,
            }),
        }
    }

    async fn send_once(
        &self,
        path: &str,
        caller_headers: &HeaderMap,
        body: Bytes,
    ) -> Result<codex_http_client::StreamResponse, SendError> {
        let (provider, auth) = self.resolve(caller_headers).await?;
        let transport = self.transport_for(&provider)?;
        let mut request = provider.build_request(Method::POST, path);
        merge_caller_headers(&mut request.headers, caller_headers);
        if !request.headers.contains_key(header::CONTENT_TYPE) {
            request.headers.insert(
                header::CONTENT_TYPE,
                http::HeaderValue::from_static("application/json"),
            );
        }
        request.body = Some(RequestBody::Raw(body));
        let request = auth.apply_auth(request).await.map_err(|err| {
            SendError::Relay(RelayError::new(
                StatusCode::UNAUTHORIZED,
                format!("failed to attach Codex credentials: {err}"),
            ))
        })?;
        transport
            .stream(request)
            .await
            .map_err(SendError::Transport)
    }

    async fn resolve(
        &self,
        caller_headers: &HeaderMap,
    ) -> Result<(Provider, SharedAuthProvider), SendError> {
        let internal = |err: codex_protocol::error::CodexErr| {
            SendError::Relay(RelayError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to resolve Codex provider: {err}"),
            ))
        };
        match self.auth_mode {
            RelayAuthMode::Codex => {
                if self.provider_info.requires_openai_auth
                    && self.model_provider.auth().await.is_none()
                {
                    return Err(SendError::Relay(RelayError::new(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "Codex is not logged in; run `codex login` on the relay host",
                    )));
                }
                let provider = self.model_provider.api_provider().await.map_err(internal)?;
                let auth = self.model_provider.api_auth().await.map_err(internal)?;
                Ok((provider, auth))
            }
            RelayAuthMode::Passthrough => {
                let token = bearer_token(caller_headers).ok_or_else(|| {
                    SendError::Relay(RelayError::new(
                        StatusCode::UNAUTHORIZED,
                        "passthrough mode requires an Authorization: Bearer header",
                    ))
                })?;
                let provider = self
                    .provider_info
                    .to_api_provider(Some(AuthMode::Chatgpt))
                    .map_err(internal)?;
                let auth: SharedAuthProvider = Arc::new(BearerAuthProvider {
                    token: Some(token),
                    account_id: header_str(caller_headers, CHATGPT_ACCOUNT_ID_HEADER)
                        .map(str::to_string),
                    is_fedramp_account: header_str(caller_headers, FEDRAMP_HEADER)
                        .is_some_and(|value| value.eq_ignore_ascii_case("true")),
                });
                Ok((provider, auth))
            }
        }
    }

    fn transport_for(&self, provider: &Provider) -> Result<ReqwestTransport, SendError> {
        let mut transports = self
            .transports
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(transport) = transports.get(&provider.base_url) {
            return Ok(transport.clone());
        }
        let client = create_client_for_route(
            &self.http_client_factory,
            &provider.url_for_path("/responses"),
            ClientRouteClass::Api,
            ClientRedirectPolicy::Default,
        )
        .map_err(|err| {
            SendError::Relay(RelayError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to build Codex HTTP client: {err}"),
            ))
        })?;
        let transport = ReqwestTransport::from_http_client(client);
        transports.insert(provider.base_url.clone(), transport.clone());
        Ok(transport)
    }
}

enum SendError {
    Relay(RelayError),
    Transport(TransportError),
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = header_str(headers, header::AUTHORIZATION.as_str())?;
    let (scheme, token) = value.split_once(' ')?;
    let token = token.trim();
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty()).then(|| token.to_string())
}

#[cfg(test)]
#[path = "forward_tests.rs"]
mod tests;
