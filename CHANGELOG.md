# FMP Changelog

## v2.3.3 — LIVE STREAMING WITH TOOLS & ANTHROPIC THINKING FLAG (2026-10-17)

Professional-chat UX for tool turns: the client sees the answer (and the
model's reasoning) streaming live while tools are active, and protocol
blocks never leak as visible text.

- **Live + tool gate**: `stream=true` with tools no longer forces full
  buffering (`stream_mode:"full"` remains the explicit opt-in). Both live
  SSE paths (OpenAI and Anthropic) stream text as it arrives and a
  marker gate (`makeToolGate`, `firstToolMarkerIndex`, `toolMarkerHoldback`)
  stops forwarding the moment a tool marker starts forming
  (`EXEC:::`/`<tool_call>`/`<tool_use`/`<invoke>`/`[tool_call`/
  `[llamada a herramienta`). The holdback is smart: 0 chars in normal
  prose (instant emission, bridge adds no visible latency) and only the
  tail that could still grow into a marker is withheld (ceiling
  `GATE_HOLDBACK=24`). At turn end the held text is flushed as normal
  text if no call resolves — nothing is ever lost.
- **Multi-call at the live end**: end-of-stream bridges now use
  `resolveToolCalls` (parallel calls + shell + FC-RETRY), emitting N
  `tool_calls` (OpenAI) / N `tool_use` blocks with real ids reused from
  prior turns (Anthropic) instead of a single call.
- **`matchIndex`** added to the polyglot parser (single + multi) so the
  gate can flush exactly the text before the first marker; `resolveToolCalls`
  propagates it (FC-RETRY uses the ORIGINAL text coordinates).
- **Anthropic `thinking` flag**: native `{type:"enabled"|"disabled"}`
  objects are honored — `{type:"disabled"}` no longer reads as truthy
  (which silently kept reasoning on for clients asking it off).

Known trade-off: the bare-JSON fallback (a model emitting a complete
`{"name":...,"arguments":...}` object without any marker) streams that
JSON live before it is transported as a call; the marker protocols
(EXEC/XML tags) never leak.

## v2.3.2 — PARALLEL TOOL CALLS & FULL WORKER CATALOG (2026-10-17)

Fixed the two production bugs behind "algunas tools se caen" (tools silently dropped):

- **Parallel tool calls**: `parsePolyglotToolCalls` collects EVERY call in a response
  (multiple `<tool_call>`/`<tool_use>`/`<invoke>`/EXEC blocks, mixed formats included);
  OpenAI emits N `tool_calls`, Anthropic N `tool_use` blocks, Responses N `function_call`
  items — each with its own id. Previously only the first call was transported and the
  rest vanished silently.
- **Full worker catalog**: every declared tool NAME is now always visible to the worker
  (previously hard-capped at 25). Full JSON schemas fill a 20k-char budget; the rest ship
  compact (name + description) instead of disappearing — tools #26+ are callable again
  (OpenCode/Codex send 30–60+ schemas).
- **Stable tool ids across turns**: prior calls are read from `messages` (assistant
  `tool_calls` / `tool_use` blocks) instead of the nonexistent `body.tool_calls`; retried
  or continued turns reuse the original ids instead of minting new ones.
- **FC-RETRY gating**: the directed retry only fires when the text shows real tool intent,
  saving a full extra upstream call on every conversational turn with tools active.
- **tool_choice**: `required` and forced-tool selections are now explained to the worker
  (OpenAI `{type:"function"}` and Anthropic `{type:"tool",name}` / `{type:"any"}`);
  Anthropic tool results flagged `is_error` now reach the worker marked `(ERROR)`.
- Tool names may now contain `:` (e.g. `mcp:tool`); test suite expanded to 51 tests.

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
