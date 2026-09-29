use super::*;
use http::HeaderValue;
use pretty_assertions::assert_eq;

#[test]
fn upstream_path_accepts_platform_backend_and_bare_shapes() {
    let cases = [
        ("/v1/responses", Some("/responses")),
        ("/backend-api/codex/responses", Some("/responses")),
        ("/responses", Some("/responses")),
        ("/responses/", Some("/responses")),
        ("/v1/responses/compact", Some("/responses/compact")),
        (
            "/backend-api/codex/responses/compact",
            Some("/responses/compact"),
        ),
        ("/v1/chat/completions", None),
        ("/backend-api/codex/models", None),
        ("/v1/v1/responses", None),
    ];
    let actual: Vec<_> = cases
        .iter()
        .map(|(path, _)| (*path, upstream_path(path)))
        .collect();
    assert_eq!(actual, cases.to_vec());
}

#[test]
fn merge_caller_headers_keeps_native_identity_and_drops_credentials() {
    let mut native = HeaderMap::new();
    native.insert("version", HeaderValue::from_static("native-version"));

    let mut caller = HeaderMap::new();
    for (name, value) in [
        ("authorization", "Bearer caller"),
        ("chatgpt-account-id", "caller-account"),
        ("user-agent", "caller-agent"),
        ("originator", "caller-originator"),
        ("version", "caller-version"),
        ("x-codex-relay-secret", "secret"),
        ("host", "relay.local"),
        ("content-length", "12"),
        ("session_id", "session-1"),
        ("openai-beta", "responses=experimental"),
        ("accept", "text/event-stream"),
    ] {
        caller.insert(
            http::HeaderName::from_static(name),
            HeaderValue::from_static(value),
        );
    }

    merge_caller_headers(&mut native, &caller);

    let mut expected = HeaderMap::new();
    expected.insert("version", HeaderValue::from_static("native-version"));
    expected.insert("session_id", HeaderValue::from_static("session-1"));
    expected.insert(
        "openai-beta",
        HeaderValue::from_static("responses=experimental"),
    );
    expected.insert("accept", HeaderValue::from_static("text/event-stream"));
    assert_eq!(native, expected);
}

#[test]
fn bearer_token_requires_bearer_scheme() {
    let token = |value: &'static str| {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, HeaderValue::from_static(value));
        bearer_token(&headers)
    };
    assert_eq!(
        [
            token("Bearer abc"),
            token("bearer  abc "),
            token("Basic abc"),
            token("Bearer "),
        ],
        [Some("abc".to_string()), Some("abc".to_string()), None, None]
    );
}
