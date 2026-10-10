# OpenAI Compatibility (v2.2.0)

Route: `POST /v1/chat/completions` (alias `/chat/completions`).
Gate: `FMP_ENABLE_OPENAI` (default true).

## Supported

- Non-stream chat completion (`choices[0].message.content`).
- Stream SSE (`data:` chunks + `data: [DONE]`).
- `stream_mode: full` (buffer via retry pipeline, then emit SSE).
- `tools` + `tool_calls` (transport only, see `tool-transport.md`).
- `tool_choice`: `"auto" | "none" | "required"` or object.
- `max_tokens` / `max_completion_tokens` enforced by truncation with
  `finish_reason: length` (worker ignores the field).
- `model`: bare id or `prefix/id` (prefix stripped, authoritative
  alias after slash).
- `_meta: { alias, route: "openai", declared_identity,
  verified_identity: "unknown" }` on every success.

## Errors

| Case | HTTP | Code |
|---|---|---|
| Missing model | 400 | `model_required` |
| Unknown alias | 404 | `model_not_found` |
| Disabled/killed | 503 | `alias_disabled` |
| Bad tools schema | 400 | `invalid_tool_schema` |
| Upstream failure | 502 | `upstream_error` (sanitized) |
| Rate limit | 429 | `rate_limited` + `retry-after` |
| Auth | 401 | `invalid_api_key` |

## Differences from native

- Token counts estimated (`len/4`); `total_tokens` always 0.
- `created: 0` on catalog entries; no embeddings/audio/images.
- No silent fallback: failures fail closed, never switch models.
