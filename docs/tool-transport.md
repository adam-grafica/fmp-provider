# FMP Tool Transport Contract (v2.2.0) — transport only, never execution

FMP moves validated tool schemas between client and upstream as data.
It never executes a tool, shell command, or host action. The client
harness owns execution, permissions, and approvals.

## InternalToolCall

```text
{ id: string|null, name: string, arguments: object, transport: "openai"|"anthropic" }
```

## Flows

- **OpenAI in**: `tools[]` validated (`validateOpenAITools`), `tool_calls[]`
  normalized (`openAICallsToInternal`, ids preserved). Malformed → 400
  `invalid_tool_schema`.
- **Anthropic in**: `tools[]` validated (`validateAnthropicTools`),
  `tool_use` blocks normalized (`anthropicBlocksToInternal`).
- **Across worker**: text protocol `EXEC:::<name> ARGS <json>` (worker
  speaks plain text). Parsed only against declared defs
  (`parseExecBlock`); unknown names and bad JSON rejected (no phantom
  calls). Shell-fence fallback converts ONLY to a declared
  `exec_command` proposal — client decides whether to run it.
- **Out**: `internalToOpenAI` / `internalToAnthropic`, prior ids reused
  (`findPriorCallId`), new ids minted only when absent.

## Logging

Only tool NAMES and result status. Never arguments, commands, or
payloads. `grep -rn "args.slice\|cmd.slice" server.js lib/` must show
no argument logging on the transport path.

## Disabled-by-default fallback

A structured-action fallback independent of native tool calling is
DESIGNED but not enabled: clients may declare an `exec_command`-style
tool and FMP will transport the proposal; execution stays client-side.
No provider-side executor exists or is authorized.
