# Anthropic Compatibility (v2.2.0)

Route: `POST /v1/messages` (alias `/messages`).
Gate: `FMP_ENABLE_ANTHROPIC` (default true).

## Supported

- Non-stream message (`content[]`, `stop_reason`).
- Stream SSE (`message_start`, `content_block_*`, `message_delta`,
  `message_stop`).
- `stream_mode: full` (buffer via retry pipeline, then emit SSE).
- `tools` (`name` + `input_schema`) + `tool_use` blocks (transport
  only, see `tool-transport.md`).
- `tool_choice`: string or object; `none` disables bridge.
- `max_tokens` enforced by truncation with `stop_reason: max_tokens`
  (worker ignores the field; default 1024 per Anthropic requirement).
- `system` string/array mapped to worker system message.
- `thinking` forwarded as worker flag; no fabricated thinking blocks
  (`/health.thinking.separate_channel: false`).
- `_meta` with `route: "anthropic"` on non-stream success.

## Errors

Native Anthropic envelope (`{ type: "error", error: {...} }`):
missing model → 400, unknown → 404, disabled/killed → 503,
malformed tools → 400 `invalid tools schema`, upstream → 502
sanitized, rate limit → 429, auth → 401.

## Differences from native

- No native thinking blocks, no citations, no vision; token counts
  estimated; failures fail closed without model switching.
