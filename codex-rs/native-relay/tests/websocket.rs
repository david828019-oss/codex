#![allow(clippy::expect_used)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

use codex_http_client::HttpClientFactory;
use codex_http_client::OutboundProxyPolicy;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_model_provider_info::ModelProviderInfo;
use codex_native_relay::RELAY_SECRET_HEADER;
use codex_native_relay::RelayAuthMode;
use codex_native_relay::RelayServer;
use codex_native_relay::RelayUpstream;
use futures::SinkExt;
use futures::StreamExt;
use http::HeaderMap;
use pretty_assertions::assert_eq;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::server::Request;
use tokio_tungstenite::tungstenite::handshake::server::Response;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tungstenite::extensions::ExtensionsConfig;
use tungstenite::extensions::compression::deflate::DeflateConfig;

const SECRET: &str = "relay-secret";

/// Starts a WebSocket upstream that records its handshake headers and echoes text frames.
async fn start_upstream(handshake_headers: Arc<Mutex<Option<HeaderMap>>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind upstream");
    let addr = listener.local_addr().expect("upstream addr");
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let handshake_headers = Arc::clone(&handshake_headers);
            tokio::spawn(async move {
                let callback = |request: &Request, mut response: Response| {
                    *handshake_headers.lock().expect("lock") = Some(request.headers().clone());
                    response.headers_mut().insert(
                        "x-codex-turn-state",
                        http::HeaderValue::from_static("turn-1"),
                    );
                    Ok(response)
                };
                // Codex negotiates permessage-deflate, like the real Responses endpoint.
                let mut extensions = ExtensionsConfig::default();
                extensions.permessage_deflate = Some(DeflateConfig::default());
                let mut config = WebSocketConfig::default();
                config.extensions = extensions;
                let mut socket =
                    tokio_tungstenite::accept_hdr_async_with_config(stream, callback, Some(config))
                        .await
                        .expect("accept upstream websocket");
                while let Some(Ok(message)) = socket.next().await {
                    if let Message::Text(text) = message {
                        let reply = format!("echo:{}", text.as_str());
                        socket.send(Message::text(reply)).await.expect("send echo");
                    }
                }
            });
        }
    });
    addr
}

async fn start_relay(upstream: SocketAddr) -> SocketAddr {
    let upstream = RelayUpstream {
        provider_info: ModelProviderInfo::create_openai_provider(Some(format!(
            "http://{upstream}/backend-api/codex"
        ))),
        chatgpt_base_url: format!("http://{upstream}/backend-api/"),
        auth_manager: AuthManager::from_auth_for_testing(
            CodexAuth::create_dummy_chatgpt_auth_for_testing(),
        ),
        http_client_factory: HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
    };
    let server = RelayServer::bind(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        SECRET.to_string(),
        RelayAuthMode::Codex,
        /*max_body_bytes*/ 1024 * 1024,
        upstream,
    )
    .await
    .expect("bind relay");
    let addr = server.local_addr();
    tokio::spawn(server.serve_until(std::future::pending()));
    addr
}

#[tokio::test]
async fn responses_websocket_is_bridged_with_native_headers() {
    let handshake_headers = Arc::new(Mutex::new(None));
    let upstream = start_upstream(Arc::clone(&handshake_headers)).await;
    let relay = start_relay(upstream).await;

    let mut request = format!("ws://{relay}/backend-api/codex/responses")
        .into_client_request()
        .expect("client request");
    for (name, value) in [
        (RELAY_SECRET_HEADER, SECRET),
        ("authorization", "Bearer caller-token"),
        ("user-agent", "sub2api"),
        ("session_id", "session-1"),
    ] {
        request.headers_mut().insert(
            http::HeaderName::from_static(name),
            http::HeaderValue::from_static(value),
        );
    }
    let (mut socket, response) = tokio_tungstenite::connect_async(request)
        .await
        .expect("connect relay websocket");
    socket
        .send(Message::text("{\"type\":\"response.create\"}"))
        .await
        .expect("send frame");
    let echoed = socket
        .next()
        .await
        .expect("frame")
        .expect("frame ok")
        .into_text()
        .expect("text frame")
        .to_string();

    let headers = handshake_headers
        .lock()
        .expect("lock")
        .clone()
        .expect("upstream handshake");
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    };
    assert_eq!(
        (
            echoed,
            response
                .headers()
                .get("x-codex-turn-state")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
            header("authorization"),
            header("chatgpt-account-id"),
            header("session_id"),
            header("openai-beta"),
            header("originator"),
            header(RELAY_SECRET_HEADER),
            header("user-agent").is_some_and(|agent| agent.starts_with("codex_cli_rs/")),
        ),
        (
            "echo:{\"type\":\"response.create\"}".to_string(),
            Some("turn-1".to_string()),
            Some("Bearer Access Token".to_string()),
            Some("account_id".to_string()),
            Some("session-1".to_string()),
            Some("responses_websockets=2026-02-06".to_string()),
            Some("codex_cli_rs".to_string()),
            None,
            true,
        )
    );
}

#[tokio::test]
async fn rejected_upstream_handshake_is_returned_as_http() {
    // A plain HTTP upstream answers the upgrade with 429, as ChatGPT does when rate limited.
    let upstream = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::path("/backend-api/codex/responses"))
        .respond_with(
            wiremock::ResponseTemplate::new(429)
                .insert_header("retry-after", "3")
                .set_body_string("{\"error\":{\"type\":\"usage_limit_reached\"}}"),
        )
        .mount(&upstream)
        .await;
    let upstream_addr = *upstream.address();
    let relay = start_relay(upstream_addr).await;

    let mut request = format!("ws://{relay}/v1/responses")
        .into_client_request()
        .expect("client request");
    request
        .headers_mut()
        .insert(RELAY_SECRET_HEADER, http::HeaderValue::from_static(SECRET));
    let error = tokio_tungstenite::connect_async(request)
        .await
        .expect_err("handshake should be rejected");
    let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
        panic!("expected an HTTP rejection, got {error}");
    };
    assert_eq!(
        (
            response.status().as_u16(),
            response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
            response.headers().contains_key("x-codex-relay-error"),
        ),
        (429, Some("3".to_string()), false)
    );
}
