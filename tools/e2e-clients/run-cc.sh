#!/bin/bash
# usage: run-cc.sh TAG "prompt"   -> runs real Claude Code against FMP ANT route
# run from a scratch working directory; isolated config under $FMP_E2E_BASE
TAG=$1; shift
PROMPT=$1; shift
BASE="${FMP_E2E_BASE:-/tmp/fmp-e2e}"
OUT="${FMP_E2E_OUT:-$BASE/out}"
mkdir -p "$OUT" "$BASE/cc-home"
if [ ! -f "$BASE/cc-home/.claude.json" ]; then
  printf '{"hasCompletedOnboarding": true, "bypassPermissionsModeAccepted": true, "theme": "dark", "numStartups": 5}\n' > "$BASE/cc-home/.claude.json"
fi
CNT() { curl -s http://127.0.0.1:2089/health | node -e "let d='';process.stdin.on('data',c=>d+=c).on('end',()=>console.log(JSON.parse(d).stats.requests))"; }
BEFORE=$(CNT)
env CLAUDE_CONFIG_DIR="$BASE/cc-home" \
  ANTHROPIC_BASE_URL=http://127.0.0.1:2089 \
  ANTHROPIC_API_KEY=fmp-test \
  ANTHROPIC_MODEL=claude-sonnet-5 \
  ANTHROPIC_DEFAULT_SONNET_MODEL=claude-sonnet-5 \
  ANTHROPIC_DEFAULT_OPUS_MODEL=claude-opus-5.5 \
  ANTHROPIC_DEFAULT_HAIKU_MODEL=claude-fable-5 \
  CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1 \
  CLAUDE_CODE_DISABLE_UNKNOWN_MODEL_WINDOW_ENFORCEMENT=1 \
  timeout 240 claude -p "$PROMPT" --model claude-sonnet-5 --dangerously-skip-permissions --output-format stream-json --verbose \
  > "$OUT/cc-$TAG.jsonl" 2>"$OUT/cc-$TAG.err"
RC=$?
AFTER=$(CNT)
echo "cc-$TAG: exit=$RC fmp_requests=$BEFORE->$AFTER lines=$(wc -l < "$OUT/cc-$TAG.jsonl")"
