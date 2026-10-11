# Per-harness output limits (HARDENING-001)

FMP applies `min(global ceil, harness cap)` to every inference response.
The worker ignores `max_tokens`, so FMP truncates and reports honestly
(`length` / `max_tokens` / `incomplete`) — caps keep any single harness
from being the bottleneck or the blowup.

## Matching

1. `x-harness` request header (exact, lowercased): `hermes`, `opencode`,
   `claude-code`, `antigravity`, `codex`.
2. Else user-agent substring. Else `unknown` → global ceil only.

## Defaults (`FMP_HARNESS_LIMITS`)

| Harness | Cap (tokens) | Rationale |
|---|---|---|
| hermes | 8192 | long agentic turns, tool rounds |
| opencode | 8192 | long tasks with 30+ tools |
| claude-code | 4096 | responses + thinking transport |
| antigravity | 4096 | multi-turn stability |
| codex | 8192 | GPT pool models, function rounds |
| unknown | global ceil (`FMP_MAX_TOKENS_CEIL`, 8192) | safe default |

## Configure

```bash
FMP_HARNESS_LIMITS=hermes:4096,opencode:16384,codex:8192
node bin/fmp.js doctor   # validates + shows safe summary
```

Caps are logged per request (`HARNESS=<name> cap=<n>`) — names only,
never payloads. Change is reversible: unset the var to restore the
global ceil for all harnesses.
