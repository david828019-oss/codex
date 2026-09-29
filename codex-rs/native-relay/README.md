# codex-native-relay

`codex relay` runs a local HTTP server that sends Codex backend requests upstream through this
Codex installation's native model path, so another gateway can reuse Codex's own client instead of
reimplementing it.

```shell
export CODEX_RELAY_SECRET="$(openssl rand -hex 32)"
codex relay --listen 127.0.0.1:8788 --auth codex
```

## Routes

Paths may use the ChatGPT Codex backend prefix (`/backend-api/codex`), the OpenAI platform prefix
(`/v1`), or no prefix.

| Request | Upstream | Native path used |
| --- | --- | --- |
| `POST /responses`, `POST /responses/compact` | provider `/responses[/compact]` | provider, auth, HTTP client |
| `GET /responses` with `Upgrade: websocket` | provider `wss://…/responses` | `WebSocketConnector`, Codex's default headers and `OpenAI-Beta: responses_websockets=…`, permessage-deflate; frames are relayed unchanged |
| `GET /models` | the catalog URL Codex's models manager requests, with `client_version` set to this Codex's version | `ModelsClient` URL building; `/v1/models` is converted to the OpenAI list shape |
| `POST /images/generations`, `POST /images/edits` | provider `/images/…` | the endpoints `ImagesClient` uses; body passed through unchanged |
| `GET`/`POST /backend-api/files/**` | `chatgpt_base_url` `/files/**` | provider headers and auth, query passed through |
| `POST /v1/files` (multipart `file`, optional `purpose`) | create, blob upload, finalize | `upload_openai_file`; answers with an OpenAI file object |

## Behavior

- Bodies are forwarded unchanged and upstream responses, including SSE, are streamed back byte
  for byte. Upstream HTTP errors, including rejected WebSocket handshakes, are returned with their
  status, headers, and body.
- Every request, including `GET /healthz`, must carry `X-Codex-Relay-Secret` matching
  `CODEX_RELAY_SECRET`. When the variable is unset, a random secret is generated and printed once.
- Upstream requests use the provider from the loaded Codex config (`-c` overrides apply), Codex's
  default HTTP client and proxy handling, and its `originator`, `User-Agent`, and provider headers
  such as `version`. The caller's `User-Agent`, `originator`, `version`, cookies, and hop-by-hop
  headers are dropped; other headers such as `session_id` are forwarded.
- `--auth codex` (default) attaches the credentials of `codex login` and refreshes the ChatGPT
  token once on a 401, including a 401 WebSocket handshake. `--auth passthrough` instead uses the
  caller's `Authorization: Bearer` and `ChatGPT-Account-Id` headers while still sending through the
  native client.
- Errors produced by the relay itself carry `X-Codex-Relay-Error: relay` when no upstream request
  was attempted, or `X-Codex-Relay-Error: upstream` when the upstream request may have been sent.
- Listens on loopback only unless `--allow-remote` is passed.
