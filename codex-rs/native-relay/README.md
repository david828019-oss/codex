# codex-native-relay

`codex relay` runs a local HTTP server that sends Responses API requests upstream through this
Codex installation's native model path, so another gateway can reuse Codex's own client instead of
reimplementing it.

```shell
export CODEX_RELAY_SECRET="$(openssl rand -hex 32)"
codex relay --listen 127.0.0.1:8788 --auth codex
```

## Behavior

- Accepts `POST` to `/v1/responses`, `/backend-api/codex/responses`, or `/responses`, each
  optionally followed by `/compact`. The body is forwarded unchanged to the configured provider's
  `/responses` (or `/responses/compact`) endpoint and the upstream response, including SSE, is
  streamed back byte for byte. Upstream HTTP errors are returned with their status, headers, and
  body.
- Every request, including `GET /healthz`, must carry `X-Codex-Relay-Secret` matching
  `CODEX_RELAY_SECRET`. When the variable is unset, a random secret is generated and printed once.
- Upstream requests use the provider from the loaded Codex config (`-c` overrides apply), Codex's
  default HTTP client and proxy handling, and its `originator`, `User-Agent`, and provider headers
  such as `version`. The caller's `User-Agent`, `originator`, `version`, cookies, and hop-by-hop
  headers are dropped; other headers such as `session_id` are forwarded.
- `--auth codex` (default) attaches the credentials of `codex login` and refreshes the ChatGPT
  token once on a 401. `--auth passthrough` instead uses the caller's `Authorization: Bearer` and
  `ChatGPT-Account-Id` headers while still sending through the native client.
- Errors produced by the relay itself carry `X-Codex-Relay-Error: relay` when no upstream request
  was attempted, or `X-Codex-Relay-Error: upstream` when the upstream request may have been sent.
- Listens on loopback only unless `--allow-remote` is passed.
