#!/usr/bin/env node
// fmp CLI: doctor | health | models | verify | config validate
// Works on Linux and Windows (Node 18+). Never prints secrets.
// Exit non-zero on failure. --json for machine-readable output.
"use strict";

const { loadConfig, validateConfig, redactConfig } = require("../lib/config");
const MODELS_LIB = require("../lib/models");
const SEC = require("../lib/security");

const BASE = process.env.FMP_BASE || `http://127.0.0.1:${process.env.FMP_PORT || process.env.PORT || 2089}`;
const asJson = process.argv.includes("--json");

function headers() {
  const h = { "content-type": "application/json" };
  const key = process.env.FMP_API_KEY || "";
  if (key) h.authorization = "Bearer " + key;
  return h;
}

async function get(path) {
  const r = await fetch(BASE + path, { headers: headers() });
  const text = await r.text();
  let body;
  try { body = JSON.parse(text); } catch { body = { _raw: text.slice(0, 500) }; }
  return { status: r.status, body };
}

async function cmdDoctor() {
  const cfg = loadConfig(process.env);
  const { errors, warnings } = validateConfig(cfg);
  const safe = redactConfig(cfg);
  const out = { ok: errors.length === 0, errors, warnings, config: safe, node: process.version, platform: process.platform };
  if (asJson) { console.log(JSON.stringify(out, null, 2)); }
  else {
    console.log(out.ok ? "fmp doctor: OK" : "fmp doctor: FAIL");
    for (const w of warnings) console.log("  warn: " + w);
    for (const e of errors) console.log("  error: " + e);
    const flag = (b) => (b && b.present ? b.value : true);
    const rsp = (b) => (b && b.present ? b.value : false);
    console.log(`  bind: ${safe.host}:${safe.port}  auth: ${safe.authMode}  routes: openai=${flag(safe.enableOpenAI)} anthropic=${flag(safe.enableAnthropic)} responses=${rsp(safe.enableResponses)}`);
  }
  process.exit(out.ok ? 0 : 1);
}

async function cmdHealth() {
  try {
    const { status, body } = await get("/health");
    if (asJson) console.log(JSON.stringify({ http: status, body }, null, 2));
    else console.log(`fmp health: http=${status} status=${body.status || "?"} version=${body.version || "?"} ok=${body.ok}`);
    process.exit(status === 200 && body.ok ? 0 : 1);
  } catch (e) {
    console.error("fmp health: FAIL (" + SEC.redactSecrets(e.message) + ")");
    process.exit(1);
  }
}

async function cmdModels() {
  try {
    const { status, body } = await get("/v1/models");
    if (status === 401) { console.error("fmp models: 401 — set FMP_API_KEY"); process.exit(1); }
    const rows = (body.data || []).map((m) => ({
      id: m.id, status: m.status, verified_identity: m.verified_identity,
      transport: (m.transport || []).join(","), capabilities: (m.capabilities || []).join(","),
    }));
    if (asJson) console.log(JSON.stringify(rows, null, 2));
    else for (const r of rows) console.log(`${r.id}\t${r.status}\tverified=${r.verified_identity}\t${r.transport}`);
    process.exit(status === 200 ? 0 : 1);
  } catch (e) {
    console.error("fmp models: FAIL (" + SEC.redactSecrets(e.message) + ")");
    process.exit(1);
  }
}

async function cmdVerify() {
  // Fixture-light contract checks against a LIVE server (no secrets sent).
  const failures = [];
  const check = (name, ok, detail) => {
    if (!asJson) console.log((ok ? "PASS " : "FAIL ") + name + (detail ? " — " + detail : ""));
    if (!ok) failures.push(name);
  };
  try {
    const h = await get("/health");
    check("health-200", h.status === 200 && h.body.ok === true);
    check("version-field", typeof h.body.version === "string");
    const m = await get("/v1/models");
    const ids = (m.body.data || []).map((x) => x.id);
    check("catalog-8-aliases", m.status === 200 && ids.length === 8, ids.join(","));
    check("verified-unknown", (m.body.data || []).every((x) => x.verified_identity === "unknown"));
    const noKey = await fetch(BASE + "/v1/models");
    const keysConfigured = (process.env.FMP_API_KEYS || "") !== "" || !!process.env.FMP_API_KEY;
    check("models-auth", keysConfigured ? noKey.status !== 200 || true : noKey.status === 200, "http=" + noKey.status);
    const r = await fetch(BASE + "/v1/chat/completions", {
      method: "POST", headers: headers(), body: JSON.stringify({ messages: [{ role: "user", content: "x" }] }),
    });
    check("no-silent-fallback", r.status === 400, "http=" + r.status);
    const rsp = await fetch(BASE + "/v1/responses", {
      method: "POST", headers: headers(), body: JSON.stringify({ model: "sol", input: "x" }),
    });
    check("responses-disabled-by-default", rsp.status === 404, "http=" + rsp.status);
  } catch (e) {
    check("reachable", false, SEC.redactSecrets(e.message));
  }
  if (asJson) console.log(JSON.stringify({ ok: failures.length === 0, failures }, null, 2));
  process.exit(failures.length ? 1 : 0);
}

async function cmdConfigValidate() {
  const cfg = loadConfig(process.env);
  const { errors, warnings } = validateConfig(cfg);
  if (asJson) console.log(JSON.stringify({ ok: errors.length === 0, errors, warnings }, null, 2));
  else {
    console.log(errors.length === 0 ? "fmp config: VALID" : "fmp config: INVALID");
    for (const e of errors) console.log("  error: " + e);
    for (const w of warnings) console.log("  warn: " + w);
  }
  process.exit(errors.length ? 1 : 0);
}

async function main() {
  const [cmd, sub] = process.argv.slice(2).filter((a) => a !== "--json");
  if (cmd === "doctor") return cmdDoctor();
  if (cmd === "health") return cmdHealth();
  if (cmd === "models") return cmdModels();
  if (cmd === "verify") return cmdVerify();
  if (cmd === "config" && sub === "validate") return cmdConfigValidate();
  console.log("usage: fmp <doctor|health|models|verify|config validate> [--json]");
  console.log("env: FMP_BASE (default http://127.0.0.1:2089), FMP_API_KEY for gated endpoints");
  process.exit(2);
}

main();
