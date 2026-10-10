# Troubleshooting (v2.2.0)

| Symptom | Cause | Fix |
|---|---|---|
| Refuses startup: `refusing to bind` | Non-localhost `FMP_HOST` without override | Use `127.0.0.1`, or set `FMP_ALLOW_PUBLIC_BIND=true` (warning applies; private use only) |
| `FMP_TOOL_MODE` error | Anything but `transport_only` | Set `FMP_TOOL_MODE=transport_only` |
| 401 on models/inference | Keys configured, none sent | `Authorization: Bearer <key>` or `x-api-key`; CLI: `FMP_API_KEY=...` |
| 429 `rate_limited` | `FMP_RATE_LIMIT_RPM` exceeded | Wait `retry-after` seconds; raise limit if appropriate |
| 503 `alias_disabled` | `FMP_DISABLED_MODELS` / `FMP_KILL_ALL` | Remove alias from list / `FMP_KILL_ALL=false` + restart |
| 404 `route_disabled` on `/v1/responses` | Responses off by default | `FMP_ENABLE_RESPONSES=true` (experimental) |
| 400 `model_required` | No `model` field | Select alias explicitly (`sol`, `fmp-oai/sol`) |
| Truncated streams (`length`) | Upstream cut without DONE | Retry; or `stream_mode: full` for buffered completion |
| Secrets in output | Misconfigured logging | Never `FMP_LOG_PROMPTS=true` with real data; check `redactSecrets` |
| `fmp verify` fails on health | Server not running / wrong `FMP_BASE` | Start `node server.js`; set `FMP_BASE=http://127.0.0.1:<port>` |
| Windows `install.ps1` blocked | Execution policy | `powershell -ExecutionPolicy Bypass -File .\install.ps1` |
