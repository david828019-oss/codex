use super::*;
use pretty_assertions::assert_eq;

fn passthrough(base: UpstreamBase, method: Method, path: &str) -> Result<RelayRoute, StatusCode> {
    Ok(RelayRoute::Passthrough {
        base,
        method,
        path: path.to_string(),
    })
}

#[test]
fn resolves_every_supported_shape() {
    use UpstreamBase::ChatGpt;
    use UpstreamBase::Provider;
    let cases = [
        (
            Method::POST,
            "/v1/responses",
            false,
            passthrough(Provider, Method::POST, "/responses"),
        ),
        (
            Method::POST,
            "/backend-api/codex/responses",
            false,
            passthrough(Provider, Method::POST, "/responses"),
        ),
        (
            Method::POST,
            "/responses/",
            false,
            passthrough(Provider, Method::POST, "/responses"),
        ),
        (
            Method::POST,
            "/v1/responses/compact",
            false,
            passthrough(Provider, Method::POST, "/responses/compact"),
        ),
        (
            Method::POST,
            "/backend-api/codex/images/generations",
            false,
            passthrough(Provider, Method::POST, "/images/generations"),
        ),
        (
            Method::POST,
            "/v1/images/edits",
            false,
            passthrough(Provider, Method::POST, "/images/edits"),
        ),
        (
            Method::GET,
            "/backend-api/codex/models",
            false,
            Ok(RelayRoute::Models { openai_list: false }),
        ),
        (
            Method::GET,
            "/v1/models",
            false,
            Ok(RelayRoute::Models { openai_list: true }),
        ),
        (
            Method::GET,
            "/backend-api/codex/responses",
            true,
            Ok(RelayRoute::ResponsesWebSocket),
        ),
        (
            Method::GET,
            "/v1/responses",
            true,
            Ok(RelayRoute::ResponsesWebSocket),
        ),
        (Method::POST, "/v1/files", false, Ok(RelayRoute::FileUpload)),
        (
            Method::POST,
            "/backend-api/files",
            false,
            passthrough(ChatGpt, Method::POST, "/files"),
        ),
        (
            Method::POST,
            "/backend-api/files/file_1/uploaded",
            false,
            passthrough(ChatGpt, Method::POST, "/files/file_1/uploaded"),
        ),
        (
            Method::GET,
            "/backend-api/files/download/file_1",
            false,
            passthrough(ChatGpt, Method::GET, "/files/download/file_1"),
        ),
        (
            Method::GET,
            "/v1/responses",
            false,
            Err(StatusCode::METHOD_NOT_ALLOWED),
        ),
        (
            Method::POST,
            "/backend-api/codex/models",
            false,
            Err(StatusCode::METHOD_NOT_ALLOWED),
        ),
        (
            Method::DELETE,
            "/backend-api/files/file_1",
            false,
            Err(StatusCode::METHOD_NOT_ALLOWED),
        ),
        (
            Method::POST,
            "/v1/chat/completions",
            false,
            Err(StatusCode::NOT_FOUND),
        ),
        (
            Method::POST,
            "/backend-api/filesystem",
            false,
            Err(StatusCode::NOT_FOUND),
        ),
        (
            Method::POST,
            "/backend-api/codex/files",
            false,
            Err(StatusCode::NOT_FOUND),
        ),
        (
            Method::POST,
            "/v1/v1/responses",
            false,
            Err(StatusCode::NOT_FOUND),
        ),
    ];
    let actual: Vec<_> = cases
        .iter()
        .map(|(method, path, upgrade, _)| {
            (
                *path,
                resolve_route(method, path, *upgrade).map_err(|err| err.status),
            )
        })
        .collect();
    let expected: Vec<_> = cases
        .into_iter()
        .map(|(_, path, _, expected)| (path, expected))
        .collect();
    assert_eq!(actual, expected);
}
