//! `codex relay`: serve Codex backend requests through this installation's native model client.

use codex_core::config::Config;
use codex_login::AuthManager;
use codex_native_relay::RelayCommand;
use codex_native_relay::RelayUpstream;
use codex_utils_cli::CliConfigOverrides;
use tracing_subscriber::EnvFilter;

pub(crate) async fn run_relay(
    command: RelayCommand,
    root_config_overrides: CliConfigOverrides,
) -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("codex_native_relay=info")),
        )
        .try_init();

    let cli_overrides = root_config_overrides
        .parse_overrides()
        .map_err(anyhow::Error::msg)?;
    let config = Config::load_with_cli_overrides(cli_overrides).await?;
    config.auth_config().validate()?;
    let auth_manager =
        AuthManager::shared_from_config(&config, /*enable_codex_api_key_env*/ true).await?;
    let upstream = RelayUpstream {
        provider_info: config.model_provider.clone(),
        chatgpt_base_url: config.chatgpt_base_url.clone(),
        auth_manager,
        http_client_factory: config.http_client_factory(),
    };
    codex_native_relay::run_main(command, upstream).await
}
