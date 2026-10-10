// FMP security tests: auth gate, localhost guard, rate limit, redaction,
// kill switch, graceful shutdown, config validation. Fixtures only.
"use strict";
const { test, before, after } = require("node:test");
const assert = require("node:assert");
const { spawn } = require("node:child_process");
const path = require("node:path");
const H = require("./helpers");
const CFG_LIB = require("../lib/config");
const SEC_LIB = require("../lib/security");

let FX; let FMP; let FMP_AUTH;
const FXP = 23321; const P = 23221; const PA = 23222;

before(async () => {
  FX = await H.spawnFixture(FXP, "ok");
  FMP = await H.spawnFmp(P, { FMP_UPSTREAM: `http://127.0.0.1:${FXP}/` });
  FMP_AUTH = await H.spawnFmp(PA, {
    FMP_UPSTREAM: `http://127.0.0.1:${FXP}/`,
    FMP_API_KEYS: "sk-test-aaa,sk-test-bbb",
    FMP_RATE_LIMIT_RPM: "3",
  });
});

after(async () => {
  await H.kill(FMP); await H.kill(FMP_AUTH); await H.kill(FX);
});

test("missing API key rejected when keys configured", async () => {
  const { status, body } = await H.post(PA, "/v1/chat/completions",
    { model: "sol", messages: [{ role: "user", content: "hi" }] });
  assert.equal(status, 401);
  assert.equal(body.error.code, "invalid_api_key");
});

test("invalid API key rejected", async () => {
  const { status } = await H.post(PA, "/v1/chat/completions",
    { model: "sol", messages: [{ role: "user", content: "hi" }] }, "sk-wrong");
  assert.equal(status, 401);
});

test("valid API key passes", async () => {
  const { status } = await H.post(PA, "/v1/chat/completions",
    { model: "sol", messages: [{ role: "user", content: "hi" }] }, "sk-test-aaa");
  assert.equal(status, 200);
});

test("health stays public", async () => {
  const { status } = await H.get(PA, "/health");
  assert.equal(status, 200);
});

test("rate limiting returns 429 with retry-after", async (t) => {
  const fx = await H.spawnFixture(23324, "ok");
  const f = await H.spawnFmp(23224, { FMP_UPSTREAM: "http://127.0.0.1:23324/", FMP_RATE_LIMIT_RPM: "2" });
  t.after(async () => { await H.kill(f); await H.kill(fx); });
  await H.post(23224, "/v1/chat/completions", { model: "sol", messages: [{ role: "user", content: "a" }] });
  await H.post(23224, "/v1/chat/completions", { model: "sol", messages: [{ role: "user", content: "b" }] });
  const r = await fetch(`http://127.0.0.1:23224/v1/chat/completions`, {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ model: "sol", messages: [{ role: "user", content: "c" }] }),
  });
  assert.equal(r.status, 429);
  assert.ok(r.headers.get("retry-after"), "429 must carry retry-after");
});

test("localhost default in clean env", () => {
  const cfg = CFG_LIB.loadConfig({});
  assert.equal(cfg.host, "127.0.0.1");
  assert.equal(CFG_LIB.validateConfig(cfg).errors.length, 0);
});

test("public bind refused without override", () => {
  const cfg = CFG_LIB.loadConfig({ FMP_HOST: "0.0.0.0" });
  assert.ok(CFG_LIB.validateConfig(cfg).errors.length > 0);
});

test("public bind with override warns", () => {
  const cfg = CFG_LIB.loadConfig({ FMP_HOST: "0.0.0.0", FMP_ALLOW_PUBLIC_BIND: "true" });
  const v = CFG_LIB.validateConfig(cfg);
  assert.equal(v.errors.length, 0);
  assert.ok(v.warnings.some((w) => w.includes("WARNING")));
});

test("tool execute mode refused at validation", () => {
  const cfg = CFG_LIB.loadConfig({ FMP_TOOL_MODE: "execute" });
  assert.ok(CFG_LIB.validateConfig(cfg).errors.length > 0);
});

test("logs redact secret-like strings", () => {
  const out = SEC_LIB.redactSecrets("api_key=sk-live-9999 and Bearer tok123 and session=abc");
  assert.ok(!out.includes("sk-live-9999"), "raw key must not survive");
  assert.ok(!out.includes("tok123"), "bearer value must not survive");
  assert.ok(out.includes("[REDACTED]"));
});

test("upstream errors sanitized (no secret leak)", async (t) => {
  const fx = await H.spawnFixture(23325, "error-secret");
  const f = await H.spawnFmp(23225, { FMP_UPSTREAM: "http://127.0.0.1:23325/", FMP_MAX_RETRIES: "0", FMP_UPSTREAM_TIMEOUT_MS: "5000" });
  t.after(async () => { await H.kill(f); await H.kill(fx); });
  const { status, body } = await H.post(23225, "/v1/chat/completions",
    { model: "sol", messages: [{ role: "user", content: "hi" }] });
  assert.ok(status === 502 || status === 500, "upstream failure must fail closed, got " + status);
  const text = JSON.stringify(body);
  assert.ok(!text.includes("sk-live-secret"), "secret must not leak to client, got: " + text.slice(0, 300));
});

test("disabled alias rejected with 503", async (t) => {
  const fx = await H.spawnFixture(23326, "ok");
  const f = await H.spawnFmp(23226, { FMP_UPSTREAM: "http://127.0.0.1:23326/", FMP_DISABLED_MODELS: "sol" });
  t.after(async () => { await H.kill(f); await H.kill(fx); });
  const { status, body } = await H.post(23226, "/v1/chat/completions",
    { model: "sol", messages: [{ role: "user", content: "hi" }] });
  assert.equal(status, 503);
  assert.equal(body.error.code, "alias_disabled");
  const ok = await H.post(23226, "/v1/chat/completions",
    { model: "terra", messages: [{ role: "user", content: "hi" }] });
  assert.equal(ok.status, 200);
});

test("kill-all switch returns 503 on inference", async (t) => {
  const fx = await H.spawnFixture(23327, "ok");
  const f = await H.spawnFmp(23227, { FMP_UPSTREAM: "http://127.0.0.1:23327/", FMP_KILL_ALL: "true" });
  t.after(async () => { await H.kill(f); await H.kill(fx); });
  const { status } = await H.post(23227, "/v1/chat/completions",
    { model: "sol", messages: [{ role: "user", content: "hi" }] });
  assert.equal(status, 503);
  const h = await H.get(23227, "/health");
  assert.equal(h.body.status, "unavailable");
});

test("graceful shutdown exits 0 on SIGTERM", async (t) => {
  const fx = await H.spawnFixture(23328, "ok");
  t.after(async () => { await H.kill(fx); });
  const child = spawn("node", [path.join(H.ROOT, "server.js")], {
    env: Object.assign({}, process.env, { FMP_PORT: "23228", FMP_HOST: "127.0.0.1", FMP_UPSTREAM: "http://127.0.0.1:23328/" }),
    stdio: ["ignore", "pipe", "pipe"],
  });
  await H.waitFor("http://127.0.0.1:23228/health");
  const code = await H.kill(child);
  assert.equal(code, 0);
});

test("config validation catches bad port/auth/retries", () => {
  const bad = CFG_LIB.loadConfig({ FMP_PORT: "99999", FMP_AUTH_MODE: "magic", FMP_MAX_RETRIES: "99" });
  assert.ok(CFG_LIB.validateConfig(bad).errors.length >= 3);
});
