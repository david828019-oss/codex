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
use wiremock::matchers::query_param;

const SECRET: &str = "relay-secret";
const SSE_BODY: &str = "event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n";

async fn start_relay(upstream: &MockServer, auth_mode: RelayAuthMode) -> SocketAddr {
    start_relay_with_base(
        upstream.uri(),
        format!("{}/backend-api/", upstream.uri()),
        auth_mode,
    )
    .await
}

async fn start_relay_with_base(
    provider_base_url: String,
    chatgpt_base_url: String,
    auth_mode: RelayAuthMode,
) -> SocketAddr {
    let upstream = RelayUpstream {
        provider_info: ModelProviderInfo::create_openai_provider(Some(provider_base_url)),
        chatgpt_base_url,
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

#[tokio::test]
async fn models_use_native_catalog_request_and_openai_list_shape() {
    let upstream = MockServer::start().await;
    let catalog = r#"{"models":[{"slug":"gpt-5.5"},{"slug":"gpt-5.5-mini"}]}"#;
    Mock::given(method("GET"))
        .and(path("/models"))
        .and(query_param(
            "client_version",
            codex_models_manager::client_version_to_whole(),
        ))
        .and(header("authorization", "Bearer Access Token"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("etag", "\"v1\"")
                .set_body_string(catalog),
        )
        .expect(2)
        .mount(&upstream)
        .await;
    let relay = start_relay(&upstream, RelayAuthMode::Codex).await;
    let client = reqwest::Client::new();

    let native = client
        .get(format!(
            "http://{relay}/backend-api/codex/models?client_version=0.1.0"
        ))
        .header(RELAY_SECRET_HEADER, SECRET)
        .send()
        .await
        .expect("relay request");
    let native = (
        native.status().as_u16(),
        native
            .headers()
            .get("etag")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string),
        native.text().await.expect("body"),
    );
    let openai: serde_json::Value = client
        .get(format!("http://{relay}/v1/models"))
        .header(RELAY_SECRET_HEADER, SECRET)
        .send()
        .await
        .expect("relay request")
        .json()
        .await
        .expect("json body");

    assert_eq!(
        (native, openai),
        (
            (200, Some("\"v1\"".to_string()), catalog.to_string()),
            serde_json::json!({
                "object": "list",
                "data": [
                    {"id": "gpt-5.5", "object": "model", "created": 0, "owned_by": "openai"},
                    {"id": "gpt-5.5-mini", "object": "model", "created": 0, "owned_by": "openai"},
                ],
            }),
        )
    );
}

#[tokio::test]
async fn images_and_files_are_passed_through_with_native_credentials() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/images/edits"))
        .and(header("authorization", "Bearer Access Token"))
        .and(header("content-type", "multipart/form-data; boundary=img"))
        .and(body_string("--img--"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{\"data\":[]}"))
        .expect(1)
        .mount(&upstream)
        .await;
    Mock::given(method("GET"))
        .and(path("/backend-api/files/download/file_1"))
        .and(query_param("gizmo_id", "g"))
        .and(header("chatgpt-account-id", "account_id"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{\"status\":\"success\"}"))
        .expect(1)
        .mount(&upstream)
        .await;
    let relay = start_relay(&upstream, RelayAuthMode::Codex).await;
    let client = reqwest::Client::new();

    let image = client
        .post(format!("http://{relay}/backend-api/codex/images/edits"))
        .header(RELAY_SECRET_HEADER, SECRET)
        .header("content-type", "multipart/form-data; boundary=img")
        .body("--img--")
        .send()
        .await
        .expect("relay request");
    let file = client
        .get(format!(
            "http://{relay}/backend-api/files/download/file_1?gizmo_id=g"
        ))
        .header(RELAY_SECRET_HEADER, SECRET)
        .send()
        .await
        .expect("relay request");

    assert_eq!(
        (image.status().as_u16(), file.status().as_u16()),
        (200, 200)
    );
}

#[tokio::test]
async fn openai_file_upload_uses_native_upload_flow() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/files"))
        .and(header("authorization", "Bearer Access Token"))
        .and(body_string(
            r#"{"file_name":"notes.txt","file_size":5,"use_case":"codex"}"#,
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "file_id": "file_1",
            "upload_url": format!("{}/blob/file_1", upstream.uri()),
        })))
        .expect(1)
        .mount(&upstream)
        .await;
    Mock::given(method("PUT"))
        .and(path("/blob/file_1"))
        .and(body_string("hello"))
        .respond_with(ResponseTemplate::new(201))
        .expect(1)
        .mount(&upstream)
        .await;
    Mock::given(method("POST"))
        .and(path("/backend-api/files/file_1/uploaded"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "success",
            "download_url": "https://files.example/file_1",
            "file_name": "notes.txt",
            "mime_type": "text/plain",
            "file_size_bytes": 5,
        })))
        .expect(1)
        .mount(&upstream)
        .await;
    let relay = start_relay(&upstream, RelayAuthMode::Codex).await;

    let mut uploaded: serde_json::Value = reqwest::Client::new()
        .post(format!("http://{relay}/v1/files"))
        .header(RELAY_SECRET_HEADER, SECRET)
        .header("content-type", "multipart/form-data; boundary=b")
        .body(
            "--b\r\nContent-Disposition: form-data; name=\"purpose\"\r\n\r\nassistants\r\n\
             --b\r\nContent-Disposition: form-data; name=\"file\"; filename=\"notes.txt\"\r\n\
             Content-Type: text/plain\r\n\r\nhello\r\n--b--\r\n",
        )
        .send()
        .await
        .expect("relay request")
        .json()
        .await
        .expect("json body");
    uploaded
        .as_object_mut()
        .expect("file object")
        .remove("created_at");

    assert_eq!(
        uploaded,
        serde_json::json!({
            "id": "file_1",
            "object": "file",
            "bytes": 5,
            "filename": "notes.txt",
            "purpose": "assistants",
            "status": "processed",
            "mime_type": "text/plain",
            "uri": "sediment://file_1",
            "download_url": "https://files.example/file_1",
        })
    );
}
