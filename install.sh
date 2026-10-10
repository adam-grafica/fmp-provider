#!/usr/bin/env bash
# FMP Linux local install (loopback-only, no sudo required except for
# the OPTIONAL systemd unit). Dry-run: DRY_RUN=1 ./install.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
DRY="${DRY_RUN:-0}"
run() { if [ "$DRY" = 1 ]; then echo "dry-run: $*"; else eval "$@"; fi }

echo "== FMP install (Linux, localhost-only) =="
command -v node >/dev/null || { echo "ERR: node 18+ required (https://nodejs.org)"; exit 1; }
node -e "const v=process.versions.node.split('.')[0]; if (Number(v)<18){console.error('node 18+ required');process.exit(1)}"

if [ ! -f "$ROOT/.env" ]; then
  run "cp \"$ROOT/.env.example\" \"$ROOT/.env\""
  echo "created .env from .env.example — edit FMP_API_KEYS before shared use"
else
  echo ".env exists, leaving untouched"
fi

run "node --check \"$ROOT/server.js\""
run "node \"$ROOT/bin/fmp.js\" doctor"

# Optional smoke test against a temp port (no fixture secrets, no live data assertions).
if [ "${SMOKE:-1}" = 1 ]; then
  echo "smoke: starting temp instance on 127.0.0.1:23117"
  if [ "$DRY" = 1 ]; then
    echo "dry-run: FMP_PORT=23117 node server.js & curl /health && kill"
  else
    (cd "$ROOT" && FMP_PORT=23117 node server.js >/tmp/fmp-install-smoke.log 2>&1 & echo $! > /tmp/fmp-install-smoke.pid)
    sleep 1.5
    node "$ROOT/bin/fmp.js" verify || { echo "smoke verify needs server; set FMP_BASE=http://127.0.0.1:23117"; }
    FMP_BASE=http://127.0.0.1:23117 node "$ROOT/bin/fmp.js" verify || true
    kill "$(cat /tmp/fmp-install-smoke.pid)" 2>/dev/null || true
  fi
fi

echo ""
echo "OK. Start:  cd $ROOT && node server.js"
echo "Optional systemd (Linux server use only): see fmp-provider.service (OPTIONAL)"
echo "Uninstall: rm -rf $ROOT  (+ systemctl --user disable --now fmp-provider if enabled)"
echo "Rollback: git checkout main -- .  (or restore from backup)"
