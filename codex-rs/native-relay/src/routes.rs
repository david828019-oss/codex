//! Maps inbound relay paths onto the native Codex endpoint that serves them.

use http::Method;
use http::StatusCode;

use crate::forward::RelayError;

/// Which configured base URL an upstream path is resolved against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpstreamBase {
    /// The active model provider's base URL (`/backend-api/codex` for ChatGPT login).
    Provider,
    /// `chatgpt_base_url` (`/backend-api`), which hosts the files API.
    ChatGpt,
}

/// A request the relay knows how to serve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RelayRoute {
    /// Raw passthrough of the body and query to `path` on `base`.
    Passthrough {
        base: UpstreamBase,
        method: Method,
        path: String,
    },
    /// Codex's native model catalog request. `openai_list` converts the catalog into the OpenAI
    /// platform `GET /v1/models` list shape.
    Models { openai_list: bool },
    /// OpenAI-platform style multipart `POST /v1/files`, uploaded with Codex's native file
    /// upload flow (create, blob upload, finalize).
    FileUpload,
    /// Responses API over WebSocket (`GET` with an upgrade).
    ResponsesWebSocket,
}

const CODEX_PREFIX: &str = "/backend-api/codex";
const FILES_PREFIX: &str = "/backend-api/files";
const OPENAI_PREFIX: &str = "/v1";

/// Resolves an inbound request to a relay route.
///
/// Accepted shapes, each with the ChatGPT Codex backend prefix (`/backend-api/codex`), the OpenAI
/// platform prefix (`/v1`), or no prefix:
///
/// - `POST /responses`, `POST /responses/compact`, and `GET /responses` with a WebSocket upgrade
/// - `GET /models`
/// - `POST /images/generations`, `POST /images/edits`
///
/// plus `GET`/`POST /backend-api/files/**` and multipart `POST /v1/files`.
pub(crate) fn resolve_route(
    method: &Method,
    path: &str,
    websocket_upgrade: bool,
) -> Result<RelayRoute, RelayError> {
    let path = path.trim_end_matches('/');
    if let Some(rest) = path.strip_prefix(FILES_PREFIX)
        && (rest.is_empty() || rest.starts_with('/'))
    {
        return match *method {
            Method::GET | Method::POST => Ok(RelayRoute::Passthrough {
                base: UpstreamBase::ChatGpt,
                method: method.clone(),
                path: format!("/files{rest}"),
            }),
            _ => Err(method_not_allowed("GET, POST")),
        };
    }

    let (openai, endpoint) = if let Some(rest) = path.strip_prefix(CODEX_PREFIX) {
        (false, rest)
    } else if let Some(rest) = path.strip_prefix(OPENAI_PREFIX) {
        (true, rest)
    } else {
        (false, path)
    };
    match endpoint {
        "/responses" if websocket_upgrade => match *method {
            Method::GET => Ok(RelayRoute::ResponsesWebSocket),
            _ => Err(method_not_allowed("GET")),
        },
        "/responses" | "/responses/compact" | "/images/generations" | "/images/edits" => {
            match *method {
                Method::POST => Ok(RelayRoute::Passthrough {
                    base: UpstreamBase::Provider,
                    method: Method::POST,
                    path: endpoint.to_string(),
                }),
                _ => Err(method_not_allowed("POST")),
            }
        }
        "/models" => match *method {
            Method::GET => Ok(RelayRoute::Models {
                openai_list: openai,
            }),
            _ => Err(method_not_allowed("GET")),
        },
        "/files" if openai => match *method {
            Method::POST => Ok(RelayRoute::FileUpload),
            _ => Err(method_not_allowed("POST")),
        },
        _ => Err(RelayError::new(
            StatusCode::NOT_FOUND,
            format!("unsupported relay path {path}"),
        )),
    }
}

fn method_not_allowed(allowed: &str) -> RelayError {
    RelayError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        format!("only {allowed} is supported on this path"),
    )
}

#[cfg(test)]
#[path = "routes_tests.rs"]
mod tests;
