//! Responses API over WebSocket: the relay dials upstream the way Codex's own
//! `ResponsesWebsocketClient` does and then shuttles frames in both directions unchanged.

use axum::extract::ws;
use axum::extract::ws::WebSocket;
use codex_websocket_client::WebSocketConnection;
use codex_websocket_client::WebSocketConnector;
use futures::SinkExt;
use futures::StreamExt;
use http::HeaderMap;
use http::HeaderValue;
use http::StatusCode;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::extensions::ExtensionsConfig;
use tungstenite::extensions::compression::deflate::DeflateConfig;

use crate::forward::Forwarder;
use crate::forward::RelayError;
use crate::forward::UpstreamBody;
use crate::forward::UpstreamReply;
use crate::forward::filter_response_headers;
use crate::forward::merge_caller_headers;

const OPENAI_BETA_HEADER: &str = "openai-beta";
/// The Responses WebSocket protocol version Codex negotiates (`core/src/client.rs`).
const RESPONSES_WEBSOCKETS_BETA: &str = "responses_websockets=2026-02-06";

/// Upstream handshake headers that describe the WebSocket connection itself and are therefore
/// not copied onto the relay's own 101 response.
const HANDSHAKE_ONLY_HEADERS: &[&str] = &[
    "sec-websocket-accept",
    "sec-websocket-extensions",
    "sec-websocket-protocol",
];

pub(crate) struct UpstreamWebSocket {
    pub(crate) connection: WebSocketConnection,
    /// Upstream upgrade response headers worth passing on (turn state, model, request id).
    pub(crate) headers: HeaderMap,
}

/// A failed upstream WebSocket handshake.
pub(crate) enum ConnectError {
    Relay(RelayError),
    /// Upstream answered the upgrade with a regular HTTP response, returned to the caller as is.
    Rejected(UpstreamReply),
}

/// Opens the upstream Responses WebSocket with native provider headers, Codex's default headers,
/// and Codex credentials, refreshing the token once if upstream rejects the handshake with 401.
pub(crate) async fn connect_upstream(
    forwarder: &Forwarder,
    caller_headers: &HeaderMap,
) -> Result<UpstreamWebSocket, ConnectError> {
    let mut result = connect_once(forwarder, caller_headers).await;
    if matches!(
        &result,
        Err(ConnectError::Rejected(reply)) if reply.status == StatusCode::UNAUTHORIZED
    ) && forwarder.refresh_after_unauthorized().await
    {
        result = connect_once(forwarder, caller_headers).await;
    }
    result
}

async fn connect_once(
    forwarder: &Forwarder,
    caller_headers: &HeaderMap,
) -> Result<UpstreamWebSocket, ConnectError> {
    let (provider, auth) = forwarder
        .resolve(caller_headers)
        .await
        .map_err(ConnectError::Relay)?;
    let url = provider
        .websocket_url_for_path("/responses")
        .map_err(|err| relay_error(format!("failed to build websocket URL: {err}")))?;

    // Same precedence as `ResponsesWebsocketClient::connect`: provider headers, then request
    // headers, then Codex's default client headers where still unset.
    let mut headers = provider.headers.clone();
    merge_caller_headers(&mut headers, caller_headers);
    for (name, value) in &codex_login::default_client::default_headers() {
        if let http::header::Entry::Vacant(entry) = headers.entry(name) {
            entry.insert(value.clone());
        }
    }
    if let http::header::Entry::Vacant(entry) = headers.entry(OPENAI_BETA_HEADER) {
        entry.insert(HeaderValue::from_static(RESPONSES_WEBSOCKETS_BETA));
    }
    auth.add_auth_headers(&mut headers);

    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|err| relay_error(format!("failed to build websocket request: {err}")))?;
    request.headers_mut().extend(headers);
    let connector = WebSocketConnector::new(forwarder.http_client_factory())
        .map_err(|err| relay_error(format!("failed to configure websocket TLS: {err}")))?;

    match connector.connect(request, websocket_config()).await {
        Ok((connection, response)) => {
            let mut headers = filter_response_headers(response.headers());
            for name in HANDSHAKE_ONLY_HEADERS {
                headers.remove(*name);
            }
            Ok(UpstreamWebSocket {
                connection,
                headers,
            })
        }
        Err(WsError::Http(response)) => {
            let status = response.status();
            let headers = filter_response_headers(response.headers());
            let body = response.into_body().unwrap_or_default();
            Err(ConnectError::Rejected(UpstreamReply {
                status,
                headers,
                body: UpstreamBody::Full(body.into()),
            }))
        }
        Err(err) => Err(ConnectError::Relay(RelayError {
            status: StatusCode::BAD_GATEWAY,
            message: format!("upstream websocket connection failed: {err}"),
            upstream_attempted: true,
        })),
    }
}

/// Relays frames between the caller and upstream until either side closes.
///
/// Text, binary, and close frames are forwarded unchanged. Ping and pong frames stay on the hop
/// they belong to, since both WebSocket stacks answer pings themselves.
pub(crate) async fn bridge(client: WebSocket, upstream: WebSocketConnection) {
    let (mut client_tx, mut client_rx) = client.split();
    let (mut upstream_tx, mut upstream_rx) = upstream.split();

    let client_to_upstream = async {
        while let Some(Ok(message)) = client_rx.next().await {
            let Some(message) = to_upstream(message) else {
                continue;
            };
            let is_close = matches!(message, Message::Close(_));
            if upstream_tx.send(message).await.is_err() || is_close {
                break;
            }
        }
        let _ = upstream_tx.close().await;
    };
    let upstream_to_client = async {
        loop {
            let message = match upstream_rx.next().await {
                Some(Ok(message)) => message,
                Some(Err(err)) => {
                    tracing::warn!("codex relay upstream websocket failed: {err}");
                    let _ = client_tx
                        .send(ws::Message::Close(Some(ws::CloseFrame {
                            code: u16::from(CloseCode::Error),
                            reason: "upstream websocket failed".into(),
                        })))
                        .await;
                    break;
                }
                None => break,
            };
            let Some(message) = to_client(message) else {
                continue;
            };
            let is_close = matches!(message, ws::Message::Close(_));
            if client_tx.send(message).await.is_err() || is_close {
                break;
            }
        }
        let _ = client_tx.close().await;
    };
    tokio::select! {
        () = client_to_upstream => {}
        () = upstream_to_client => {}
    }
}

fn to_upstream(message: ws::Message) -> Option<Message> {
    match message {
        ws::Message::Text(text) => Some(Message::text(text.as_str())),
        ws::Message::Binary(bytes) => Some(Message::Binary(bytes)),
        ws::Message::Close(frame) => Some(Message::Close(frame.map(|frame| CloseFrame {
            code: CloseCode::from(frame.code),
            reason: frame.reason.as_str().into(),
        }))),
        ws::Message::Ping(_) | ws::Message::Pong(_) => None,
    }
}

fn to_client(message: Message) -> Option<ws::Message> {
    match message {
        Message::Text(text) => Some(ws::Message::Text(text.as_str().into())),
        Message::Binary(bytes) => Some(ws::Message::Binary(bytes)),
        Message::Close(frame) => Some(ws::Message::Close(frame.map(|frame| ws::CloseFrame {
            code: u16::from(frame.code),
            reason: frame.reason.as_str().into(),
        }))),
        Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => None,
    }
}

/// Matches Codex's Responses WebSocket configuration, including permessage-deflate.
fn websocket_config() -> WebSocketConfig {
    let mut extensions = ExtensionsConfig::default();
    extensions.permessage_deflate = Some(DeflateConfig::default());
    let mut config = WebSocketConfig::default();
    config.extensions = extensions;
    config
}

fn relay_error(message: String) -> ConnectError {
    ConnectError::Relay(RelayError::new(StatusCode::INTERNAL_SERVER_ERROR, message))
}
