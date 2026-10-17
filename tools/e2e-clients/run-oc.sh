#!/bin/bash
# usage: run-oc.sh TAG "prompt"   -> runs real OpenCode against FMP OAI route
# run from a scratch working directory; isolated XDG under $FMP_E2E_BASE
TAG=$1; shift
PROMPT=$1; shift
BASE="${FMP_E2E_BASE:-/tmp/fmp-e2e}"
OUT="${FMP_E2E_OUT:-$BASE/out}"
mkdir -p "$OUT" "$BASE/oc-xdg/config/opencode" "$BASE/oc-xdg/data" "$BASE/oc-xdg/cache" "$BASE/oc-xdg/state"
if [ ! -f "$BASE/oc-xdg/config/opencode/opencode.jsonc" ]; then
  cp "$(dirname "$0")/opencode-fmp-provider.jsonc" "$BASE/oc-xdg/config/opencode/opencode.jsonc"
fi
CNT() { curl -s http://127.0.0.1:2089/health | node -e "let d='';process.stdin.on('data',c=>d+=c).on('end',()=>console.log(JSON.parse(d).stats.requests))"; }
BEFORE=$(CNT)
env XDG_CONFIG_HOME="$BASE/oc-xdg/config" \
  XDG_DATA_HOME="$BASE/oc-xdg/data" \
  XDG_CACHE_HOME="$BASE/oc-xdg/cache" \
  XDG_STATE_HOME="$BASE/oc-xdg/state" \
  timeout 240 opencode run --pure -m fmp/sol --format json "$PROMPT" \
  > "$OUT/oc-$TAG.jsonl" 2>"$OUT/oc-$TAG.err"
RC=$?
AFTER=$(CNT)
echo "oc-$TAG: exit=$RC fmp_requests=$BEFORE->$AFTER lines=$(wc -l < "$OUT/oc-$TAG.jsonl")"
