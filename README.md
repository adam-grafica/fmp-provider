# FMP — Private Development Inference Adapter (v2.2.0)

FMP is a private experimental inference adapter.
Upstream model labels are declared by upstream and not independently
verified.
FMP does not execute tools, commands or host actions.
Clients/harnesses own tool execution and permissions.
Default deployment is localhost-only.
Do not send secrets or sensitive data to unverified upstreams.

FMP normalizes OpenAI Chat Completions + Anthropic Messages over a
configured upstream worker and streams normalized responses. No
dependencies (Node 18+ only).

## Status

```text
FMP v2.2.0 — PRIVATE DEVELOPMENT READY
Linux: LOCALHOST / 9Router READY
Windows: LOCAL INSTALLATION READY
Production: NOT AUTHORIZED — public exposure NOT AUTHORIZED,
sensitive data NOT AUTHORIZED.
```

## Quick start (localhost)

```bash
cp .env.example .env      # set FMP_API_KEYS for any shared use
node bin/fmp.js doctor    # validate config, exit non-zero on failure
node server.js            # 127.0.0.1:2089
node bin/fmp.js health && node bin/fmp.js models
```

## Endpoints

| Method | Route | Auth | Notes |
|---|---|---|---|
| GET | `/health`, `/` | no | status `healthy/degraded/unavailable` + versions + routes |
| GET | `/stats`, `/v1/stats` | key* | request/error/retry counters |
| GET | `/v1/models`, `/models` | key* | 8 aliases, `verified_identity: unknown` |
| GET | `/v1/usage`, `/usage` | key* | tier metrics (legacy tiers) |
| POST | `/v1/chat/completions` | key* | OpenAI, stream + `stream_mode: full` |
| POST | `/v1/messages` | key* | Anthropic, stream + `stream_mode: full` |
| POST | `/v1/responses` | key* | DISABLED by default (experimental) |

\* key = required only when `FMP_API_KEYS` is configured; health stays public.

`model` must be explicit (no ambient fallback): bare id (`sol`) or
9Router-prefixed (`fmp-oai/sol`, prefix stripped). Unknown alias → 404,
disabled/killed alias → 503, missing → 400. Every inference response
carries sanitized `_meta: { alias, route, declared_identity,
verified_identity: "unknown" }`.

## Configuration

See `.env.example` + `config.example.yaml` + `docs/`:

- `FMP_HOST` default `127.0.0.1`; non-localhost bind refuses startup
  unless `FMP_ALLOW_PUBLIC_BIND=true` (warning printed).
- `FMP_AUTH_MODE=api_key` default; `FMP_API_KEYS` empty = local-open
  mode (localhost only).
- `FMP_TOOL_MODE=transport_only` — the only accepted value.
- Kill switch: `FMP_DISABLED_MODELS=sol,terra` or `FMP_KILL_ALL=true`.
- `docs/architecture.md`, `docs/security.md`,
  `docs/openai-compatibility.md`, `docs/anthropic-compatibility.md`,
  `docs/tool-transport.md`, `docs/windows-setup.md`,
  `docs/linux-setup.md`, `docs/troubleshooting.md`.

## CLI

```bash
node bin/fmp.js doctor            # config validation + safe summary
node bin/fmp.js health            # GET /health, exit 1 on failure
node bin/fmp.js models            # alias catalog (needs FMP_API_KEY if gated)
node bin/fmp.js verify            # live contract checks (no secrets)
node bin/fmp.js config validate   # validate only
# append --json for machine-readable output
```

## Install

- Linux: `./install.sh` (systemd unit `fmp-provider.service` OPTIONAL,
  server use only — not needed for local dev).
- Windows: `.\install.ps1` (no admin, no services).
- Docker: `docker compose up --build` (loopback-bound).
- Uninstall/rollback in each script header. Never touches the running
  production service.

## Tests

```bash
npm test   # node --test, fixtures only, no network/secrets
```

31 tests: chat/stream both routes, retry, JSON fallback, auth gate,
rate limit, kill switch, tool transport both routes, malformed
schemas, redaction, graceful shutdown, config validation, catalog.

## Versioning

`CHANGELOG.md` tracks releases. Current: v2.2.0 private development
ready. No release published by this task; no production routing.
