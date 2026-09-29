//! Local HTTP relay that sends Responses API requests through Codex's native model path.
//!
//! The relay lets another gateway (for example a sub2api transport plugin) hand a raw Responses
//! request to a running Codex installation. Codex then sends it upstream with its own provider
//! configuration, HTTP client, `originator` / `User-Agent` / `version` headers, and — in the
//! default `codex` auth mode — the ChatGPT login managed by [`AuthManager`], including token
//! refresh. The upstream SSE stream is returned to the caller byte for byte.

mod forward;
mod server;

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use clap::ValueEnum;
use codex_http_client::HttpClientFactory;
use codex_login::AuthManager;
use codex_model_provider_info::ModelProviderInfo;

pub use server::RelayServer;

/// Environment variable that holds the shared secret callers must present.
pub const RELAY_SECRET_ENV_VAR: &str = "CODEX_RELAY_SECRET";

/// Request header that carries the shared secret.
pub const RELAY_SECRET_HEADER: &str = "x-codex-relay-secret";

/// Response header set on errors produced by the relay itself rather than by upstream. Its value
/// is `relay` when no upstream request was attempted and `upstream` when one may have been sent.
pub const RELAY_ERROR_HEADER: &str = "x-codex-relay-error";

const DEFAULT_MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

/// Command-line options for `codex relay`.
#[derive(Debug, Clone, Parser)]
pub struct RelayCommand {
    /// Address to listen on. Non-loopback addresses also require `--allow-remote`.
    #[arg(long, default_value = "127.0.0.1:8788")]
    pub listen: SocketAddr,

    /// Allow binding to a non-loopback address.
    #[arg(long, default_value_t = false)]
    pub allow_remote: bool,

    /// Which credentials are attached to upstream requests.
    #[arg(long, value_enum, default_value_t = RelayAuthMode::Codex)]
    pub auth: RelayAuthMode,

    /// Maximum accepted request body size in bytes.
    #[arg(long, default_value_t = DEFAULT_MAX_BODY_BYTES)]
    pub max_body_bytes: usize,
}

/// Credential source for relayed requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RelayAuthMode {
    /// Use the account this Codex installation is logged in with (`codex login`).
    Codex,
    /// Use the caller's `Authorization: Bearer` token and `ChatGPT-Account-Id` header.
    Passthrough,
}

impl RelayAuthMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Passthrough => "passthrough",
        }
    }
}

/// The native Codex pieces the relay reuses for every upstream request.
pub struct RelayUpstream {
    pub provider_info: ModelProviderInfo,
    pub auth_manager: Arc<AuthManager>,
    pub http_client_factory: HttpClientFactory,
}

/// Runs the relay until Ctrl-C.
pub async fn run_main(command: RelayCommand, upstream: RelayUpstream) -> anyhow::Result<()> {
    if !command.listen.ip().is_loopback() && !command.allow_remote {
        anyhow::bail!(
            "refusing to listen on non-loopback address {}; pass --allow-remote to override",
            command.listen
        );
    }
    let secret = match std::env::var(RELAY_SECRET_ENV_VAR) {
        Ok(secret) if !secret.trim().is_empty() => secret.trim().to_string(),
        _ => {
            let secret = server::generate_secret();
            eprintln!(
                "{RELAY_SECRET_ENV_VAR} is not set; generated a secret for this run:\n  {secret}"
            );
            secret
        }
    };
    if command.auth == RelayAuthMode::Codex && upstream.auth_manager.auth().await.is_none() {
        eprintln!("warning: Codex is not logged in; run `codex login` or use --auth passthrough");
    }

    let server = RelayServer::bind(
        command.listen,
        secret,
        command.auth,
        command.max_body_bytes,
        upstream,
    )
    .await?;
    eprintln!(
        "codex relay listening on http://{} (auth mode: {})",
        server.local_addr(),
        command.auth.as_str()
    );
    server
        .serve_until(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
}
