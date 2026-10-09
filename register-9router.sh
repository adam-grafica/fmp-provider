#!/usr/bin/env bash
# Registra nodos FMP en 9Router (idempotente). Requiere sqlite3.
set -euo pipefail
DB="$HOME/.9router/db/data.sqlite"
BASE="http://127.0.0.1:2089/v1"
TS=$(date +%s)

[ -f "$DB" ] || { echo "ERR: no existe $DB (9Router sin inicializar)"; exit 1; }
cp "$DB" "$DB.pre-fmp-$TS"
echo "backup: $DB.pre-fmp-$TS"

node - "$DB" "$BASE" <<'JS'
const fs = require("fs");
const { execFileSync } = require("child_process");
const [db, base] = process.argv.slice(2);
const q = (sql) => execFileSync("sqlite3", [db, sql], { encoding: "utf8" }).trim();
const now = new Date().toISOString();
const nodes = [
  { name: "FreeModels OpenAI", type: "openai-compatible",
    data: { prefix: "fmp-oai", apiType: "chat", baseUrl: base } },
  { name: "FreeModels Anthropic", type: "anthropic-compatible",
    data: { prefix: "fmp-ant", baseUrl: base } },
];
for (const n of nodes) {
  const id = `${n.type}-${require("crypto").randomUUID()}`;
  const esc = JSON.stringify(JSON.stringify(n.data)).slice(1, -1);
  try {
    execFileSync("sqlite3", [db,
      `INSERT INTO providerNodes(id,type,name,data,createdAt,updatedAt) VALUES('${id}','${n.type}','${n.name}','${esc}','${now}','${now}');`]);
    console.log("nodo creado:", n.name, id);
  } catch (e) {
    const rows = q(`SELECT id FROM providerNodes WHERE name='${n.name}';`);
    console.log("nodo ya existe:", n.name, rows.split("\n")[0] || "");
  }
}
// Conexiones no-auth si faltan
const existing = q("SELECT id,type,name,data FROM providerNodes;");
console.log("--- nodos actuales ---");
console.log(existing.split("\n").map((l) => l.slice(0, 160)).join("\n"));
JS
echo OK
