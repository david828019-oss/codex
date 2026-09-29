#![allow(clippy::expect_used)]

use std::net::SocketAddr;

use codex_http_client::HttpClientFactory;
use codex_http_client::OutboundProxyPolicy;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_model_provider_info::ModelProviderInfo;
use codex_native_relay::RELAY_ERROR_HEADER;
use codex_native_relay::RELAY_SECRET_HEADER;
use codex_native_relay::RelayAuthMode;
use codex_native_relay::RelayServer;
use codex_native_relay::RelayUpstream;
use pretty_assertions::assert_eq;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::body_string;
use wiremock::matchers::header;
use wiremock::matchers::method;
use wiremock::matchers::path;

const SECRET: &str = "relay-secret";
const SSE_BODY: &str = "event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n";

async fn start_relay(upstream: &MockServer, auth_mode: RelayAuthMode) -> SocketAddr {
    let upstream = RelayUpstream {
        provider_info: ModelProviderInfo::create_openai_provider(Some(upstream.uri())),
        auth_manager: AuthManager::from_auth_for_testing(
            CodexAuth::create_dummy_chatgpt_auth_for_testing(),
        ),
        http_client_factory: HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault),
    };
    let server = RelayServer::bind(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        SECRET.to_string(),
        auth_mode,
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
async fn codex_mode_uses_native_credentials_and_streams_body() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/responses"))
        .and(header("authorization", "Bearer Access Token"))
        .and(header("chatgpt-account-id", "account_id"))
        .and(header("session_id", "session-1"))
        .and(body_string("{\"model\":\"gpt-5\"}"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(SSE_BODY),
        )
        .expect(1)
        .mount(&upstream)
        .await;
    let relay = start_relay(&upstream, RelayAuthMode::Codex).await;

    let response = reqwest::Client::new()
        .post(format!("http://{relay}/backend-api/codex/responses"))
        .header(RELAY_SECRET_HEADER, SECRET)
        .header("authorization", "Bearer caller-token-is-ignored")
        .header("user-agent", "sub2api")
        .header("session_id", "session-1")
        .header("content-type", "application/json")
        .body("{\"model\":\"gpt-5\"}")
        .send()
        .await
        .expect("relay request");

    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.expect("body"), SSE_BODY);
    let received = upstream.received_requests().await.expect("requests");
    let user_agent = received[0]
        .headers
        .get("user-agent")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        user_agent.starts_with("codex_cli_rs/"),
        "unexpected user agent {user_agent}"
    );
}

#[tokio::test]
async fn passthrough_mode_uses_caller_credentials() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/responses/compact"))
        .and(header("authorization", "Bearer caller-token"))
        .and(header("chatgpt-account-id", "caller-account"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(1)
        .mount(&upstream)
        .await;
    let relay = start_relay(&upstream, RelayAuthMode::Passthrough).await;

    let response = reqwest::Client::new()
        .post(format!("http://{relay}/v1/responses/compact"))
        .header(RELAY_SECRET_HEADER, SECRET)
        .header("authorization", "Bearer caller-token")
        .header("chatgpt-account-id", "caller-account")
        .body("{}")
        .send()
        .await
        .expect("relay request");

    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn upstream_errors_are_returned_verbatim() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/responses"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("retry-after", "7")
                .set_body_string("{\"error\":{\"type\":\"usage_limit_reached\"}}"),
        )
        .mount(&upstream)
        .await;
    let relay = start_relay(&upstream, RelayAuthMode::Codex).await;

    let response = reqwest::Client::new()
        .post(format!("http://{relay}/v1/responses"))
        .header(RELAY_SECRET_HEADER, SECRET)
        .body("{}")
        .send()
        .await
        .expect("relay request");

    assert_eq!(
        (
            response.status().as_u16(),
            response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
            response.headers().contains_key(RELAY_ERROR_HEADER),
            response.text().await.expect("body"),
        ),
        (
            429,
            Some("7".to_string()),
            false,
            "{\"error\":{\"type\":\"usage_limit_reached\"}}".to_string(),
        )
    );
}

#[tokio::test]
async fn rejects_missing_secret_and_unknown_paths_without_calling_upstream() {
    let upstream = MockServer::start().await;
    let relay = start_relay(&upstream, RelayAuthMode::Codex).await;
    let client = reqwest::Client::new();

    let missing_secret = client
        .post(format!("http://{relay}/v1/responses"))
        .body("{}")
        .send()
        .await
        .expect("relay request");
    let unknown_path = client
        .post(format!("http://{relay}/v1/chat/completions"))
        .header(RELAY_SECRET_HEADER, SECRET)
        .body("{}")
        .send()
        .await
        .expect("relay request");

    let summarize = |response: &reqwest::Response| {
        (
            response.status().as_u16(),
            response
                .headers()
                .get(RELAY_ERROR_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
        )
    };
    assert_eq!(
        [summarize(&missing_secret), summarize(&unknown_path)],
        [
            (401, Some("relay".to_string())),
            (404, Some("relay".to_string())),
        ]
    );
    assert_eq!(
        upstream.received_requests().await.expect("requests").len(),
        0
    );
}
