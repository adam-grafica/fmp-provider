# Claude Code Auto-Mode Safety Classifier with FMP

## Context & Issue

When running **Claude Code** in `auto-mode` (`--dangerously-skip-permissions` or interactive auto-mode), Claude Code executes a **secondary background request** to evaluate the safety of tool actions (e.g. `Bash`, `Agent`, `Fork`) before executing them.

By default, Claude Code uses the primary model or Opus/Sonnet for this evaluation.
With `claude-opus-5.5` on FMP, the Time To First Token (TTFT) is ~1.2s. Claude Code's internal classifier watchdog has a strict timeout:
```text
Error: fmp-ant/claude-opus-5.5 is temporarily unavailable (timed out),
so auto mode cannot determine the safety of Bash right now.
```

## Solution

FMP provides built-in fast-routing for auto-mode safety classifier sub-requests.

### 1. Gateway-Level Fast Routing (Default in FMP v2.3.1+)

When FMP receives a request from the `claude-code` harness with system instructions matching `security monitor` or `safety classifier`, it automatically redirects the classification sub-call to the fast model specified in `FMP_CLASSIFIER_MODEL` (default: `claude-fable-5`, TTFT ~550ms):

```env
# .env or environment variable
FMP_CLASSIFIER_MODEL=claude-fable-5
FMP_CLASSIFIER_MATCH="security monitor,safety classifier"
```

The main coding conversation remains on your selected model (e.g. `claude-sonnet-5` or `claude-opus-5.5`), while the background classifier runs without timing out.

### 2. Client-Side Workaround (Haiku Slot in Claude Code)

Alternatively, you can configure Claude Code's client environment to use `claude-fable-5` specifically for the haiku slot:

```bash
# Linux
export ANTHROPIC_DEFAULT_HAIKU_MODEL="claude-fable-5"
export ANTHROPIC_MODEL="claude-sonnet-5"
```

```powershell
# Windows PowerShell
$env:ANTHROPIC_DEFAULT_HAIKU_MODEL = "claude-fable-5"
$env:ANTHROPIC_MODEL = "claude-sonnet-5"
```
