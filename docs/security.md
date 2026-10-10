# FMP Security (v2.2.0) — private development use only

## What FMP never does

- Execute shell commands, tool calls, or host actions.
- Read/write host files (except optional redacted trace append).
- Touch Docker, systemd, cron, 9Router config, or secrets.
- Expose raw upstream cookies, tokens, headers, session material.
- Bind publicly by default (`127.0.0.1`; override refuses startup
  without `FMP_ALLOW_PUBLIC_BIND=true` + printed warning).
- Claim verified model identity (`verified_identity: unknown` always).
- Silently switch models/providers (explicit selection, fail closed).
- Log full prompts/responses by default (`FMP_LOG_PROMPTS=false`).
- Serve as fallback for sensitive tasks; open public ports; deploy
  to public internet (all NOT AUTHORIZED by this task).

## Controls

| Control | Where | Evidence |
|---|---|---|
| Startup config validation | `lib/config.js`, top of `server.js` | `fmp doctor`, tests |
| Localhost guard | `validateConfig` | `security.test.js` public-bind cases |
| API key gate (non-health) | `authGate`, constant-time compare | 401 tests |
| Global rate limit + `retry-after` | `lib/security.js RateLimiter` | 429 test |
| Body size cap | `readBody` vs `FMP_MAX_REQUEST_BYTES` | code |
| CORS deny-by-default | `corsOriginFor` (no ACAO unless listed) | code |
| Request/correlation ids | `requestIdOf` | error bodies |
| Secret redaction (logs/trace/errors) | `redactSecrets` | redaction + sanitize tests |
| Sanitized upstream errors | `sanitizeUpstreamError` | no-leak test |
| Alias allowlist + kill switch | `MODELS_LIB.isKnown`, `DISABLED_MODELS`, `KILL_ALL` | 404/503 tests |
| Graceful shutdown | SIGTERM/SIGINT drain, exit 0 | shutdown test |
| Health states | `healthy/degraded/unavailable` | kill-all test |

## Operator rules

1. `cp .env.example .env`, set `FMP_API_KEYS` before any shared use.
2. Never commit `.env` (gitignored); never set `FMP_LOG_PROMPTS=true`
   with real data; never enable `FMP_ALLOW_PUBLIC_BIND` without need.
3. Kill switch: `FMP_DISABLED_MODELS=<id>` or `FMP_KILL_ALL=true`
   + restart; verify `/health.status == unavailable` for global.
4. Rotate keys by replacing `FMP_API_KEYS` + restart (no hot reload).
