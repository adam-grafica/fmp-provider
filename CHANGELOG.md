# FMP Changelog

## v2.3.1 — PRODUCTION AGENTIC TOOL HARDENING & 9ROUTER REFINEMENT (2026-10-15)

Production readiness hardening for agentic tool calls across OpenAI and Anthropic routes,
Windows and Linux parity, and clean 9Router catalog routing:

- **OpenAI Streaming Tool Resolution**: When client requests tool calls with `stream: true`,
  upstream generation is buffered into a complete turn and re-emitted as standard SSE chunks
  (`delta.tool_calls`, `finish_reason: "tool_calls"`) followed by `data: [DONE]`.
- **Agnostic Tool Bridging**: Replaced heuristic keyword triggers with a dynamic, agnostic
  system prompt adapter that exposes the client harness's actual tool schemas (`read_file`,
  `terminal`, `View`, `Bash`, etc.) without hardcoding names or generating apologies.
- **Markdown Code Block Protection**: Prevented normal markdown code blocks (```bash`) from
  being falsely hijacked into premature `Bash` tool calls.
- **Anthropic Nested System Flattening**: Added recursive `flattenSystem` handling complex
  nested text arrays in Anthropic requests.
- **9Router Catalog Unification**: Standardized clean separation between `fmp-oai/*` (OpenAI
  chat completions) and `fmp-ant/*` (Anthropic messages native), removing redundant legacy prefixes.
- **Automated Verification**: Test suite expanded to 42 tests, verifying streaming tool
  calls, nested system prompts, markdown preservation, and multi-tool schemas.

## v2.2.0 — PRIVATE DEVELOPMENT READY (2026-10-10, unreleased)

Security + contracts professionalization. No release published; branch
`traycer/FMP-PRODUCTION-READINESS-001` only.

- Configuration contract: `.env.example`, `config.example.yaml`,
  startup validation, localhost guard (`FMP_ALLOW_PUBLIC_BIND`
  override + warning), `bin/fmp.js doctor`.
- Model catalog honesty: `transport`, `declared_identity`,
  `verified_identity: unknown`, `capabilities`, `status`,
  `last_checked_at` on every alias; explicit model required, no
  silent fallback; sanitized `_meta` on inference responses.
- Tool transport contract (`lib/tools.js`): OpenAI/Anthropic schema
  validation, `InternalToolCall` normalization, id preservation,
  malformed rejection; transport-only framing in prompts and logs.
- Security: API key gate on non-health endpoints, global rate limit
  (`FMP_RATE_LIMIT_RPM`), CORS deny-by-default, request/correlation
  ids, secret redaction in logs/traces/errors, sanitized upstream
  errors (status + request id), alias kill switch + `FMP_KILL_ALL`,
  health `healthy/degraded/unavailable`, graceful shutdown (SIGTERM/
  SIGINT drain, exit 0).
- Responses API disabled by default (`FMP_ENABLE_RESPONSES=false`).
- Install: `Dockerfile`, `docker-compose.yml` (loopback-bound),
  `install.sh` (+ dry-run + smoke), `install.ps1` (no admin).
- CLI: `doctor | health | models | verify | config validate`
  (+ `--json`), non-zero on failure, secrets never printed.
- Tests: 31 fixture-only tests (no network/secrets).
- Docs: architecture, security, compat x2, tool-transport,
  windows/linux setup, troubleshooting.

## v2.1.1

Fix false DIVERGED intra-attempt (direct emission without dedupe);
honest TRUNCATED trace.

## v2.1.0

Live streaming (TTFB ~3ms), real max_tokens on 4 routes,
`stream_mode: full`, premium tiers, honest thinking flag.

## v1.3.0

Native tool-use OpenAI + Anthropic (3 centralized levels), full tool
round-trip, anti-negative retry.

## v1.2.0

POST /v1/responses (Codex) + EXEC→function_call agentic bridge.

## v1.0.0

OpenAI + Anthropic provider over freemodels.pro with anti-truncation
buffer + retry + JSON-fallback streaming.
