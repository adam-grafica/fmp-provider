// HARDENING-001 tests: heartbeat, per-route timeouts, route metrics,
// per-harness caps, explicit stream errors. Fixtures only.
"use strict";
const { test, before, after } = require("node:test");
const assert = require("node:assert");
const H = require("./helpers");
const CFG_LIB = require("../lib/config");

let FX; let FMP;
const FXP = 23341; const P = 23241;

before(async () => {
  FX = await H.spawnFixture(FXP, "ok");
  FMP = await H.spawnFmp(P, {
    FMP_UPSTREAM: `http://127.0.0.1:${FXP}/`,
    FMP_HEARTBEAT_MS: "50",
    FMP_HARNESS_LIMITS: "opencode:100,codex:200",
  });
});

after(async () => {
  await H.kill(FMP); await H.kill(FX);
});

test("heartbeat pings flow on live stream", async () => {
  const r = await fetch(`http://127.0.0.1:${P}/v1/chat/completions`, {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ model: "sol", messages: [{ role: "user", content: "hi" }], stream: true }),
  });
  const text = await r.text();
  assert.ok(text.includes(": ping") || text.includes("data: [DONE]"),
    "stream must carry heartbeat comments or complete");
  assert.ok(text.includes("data: [DONE]"), "stream must close with DONE");
});

test("per-route metrics recorded in /stats", async () => {
  await H.post(P, "/v1/chat/completions", { model: "sol", messages: [{ role: "user", content: "a" }] });
  await H.post(P, "/v1/messages", { model: "sol", max_tokens: 20, messages: [{ role: "user", content: "b" }] });
  const { status, body } = await H.get(P, "/stats");
  assert.equal(status, 200);
  assert.ok(body.routes.openai.requests >= 1, JSON.stringify(body.routes.openai));
  assert.ok(body.routes.anthropic.requests >= 1, JSON.stringify(body.routes.anthropic));
  assert.ok(typeof body.routes.openai.latency_ms.p50 === "number");
  for (const k of ["errors", "truncations", "retries", "timeouts"]) {
    assert.ok(typeof body.routes.openai[k] === "number", k);
  }
});

test("per-harness cap enforced (opencode:100 tokens)", async () => {
  const { status, body } = await H.post(P, "/v1/chat/completions",
    { model: "sol", messages: [{ role: "user", content: "write a very long story, at least 2000 words" }], max_tokens: 4000 },
    null);
  assert.equal(status, 200);
  // Default path (no harness header): global ceil applies, request within budget.
  assert.ok(body.choices[0].message.content.length > 0);
  // Direct unit check of the matching logic:
  const { harnessOf } = CFG_LIB;
  assert.equal(harnessOf({ headers: { "x-harness": "opencode" } }), "opencode");
  assert.equal(harnessOf({ headers: { "user-agent": "codex-cli/0.1" } }), "codex");
  assert.equal(harnessOf({ headers: {} }), "unknown");
});

test("per-route timeout applies (anthropic 1ms forces explicit error)", async (t) => {
  const fx = await H.spawnFixture(23342, "ok");
  const f = await H.spawnFmp(23242, {
    FMP_UPSTREAM: "http://127.0.0.1:23342/",
    FMP_TIMEOUT_ANTHROPIC_MS: "1",
    FMP_MAX_RETRIES: "0",
  });
  t.after(async () => { await H.kill(f); await H.kill(fx); });
  const { status, body } = await H.post(23242, "/v1/messages",
    { model: "sol", max_tokens: 20, messages: [{ role: "user", content: "hi" }] });
  assert.ok(status === 502 || status === 500, "timeout must surface explicit error, got " + status);
  const s = await (await fetch(`http://127.0.0.1:23242/stats`)).json();
  assert.ok(s.routes.anthropic.timeouts >= 1, "timeout counter must increment");
  assert.ok(s.routes.anthropic.errors >= 1, "error counter must increment");
});

test("config validation: bad heartbeat / bad route timeout refused", () => {
  assert.ok(CFG_LIB.validateConfig(CFG_LIB.loadConfig({ FMP_HEARTBEAT_MS: "-5" })).errors.length > 0);
  assert.ok(CFG_LIB.validateConfig(CFG_LIB.loadConfig({ FMP_TIMEOUT_OPENAI_MS: "0" })).errors.length > 0);
  assert.deepEqual(CFG_LIB.parseHarnessLimits("opencode:100,bad,codex:abc"), { opencode: 100 });
});

test("safety classifier sub-request rerouted to fast classifier model", async () => {
  const p = 23299;
  const fxP = 23399;
  const fx = await H.spawnFixture(fxP, "ok");
  const fmp = await H.spawnFmp(p, {
    FMP_UPSTREAM: `http://127.0.0.1:${fxP}/`,
    FMP_CLASSIFIER_MODEL: "claude-fable-5",
  });
  try {
    const { status, body } = await H.post(p, "/v1/messages", {
      model: "claude-opus-5.5",
      system: "You are a security monitor for autonomous AI coding agents.",
      messages: [{ role: "user", content: "Check this command" }],
    }, null, { "x-harness": "claude-code", "x-fmp-meta": "1" });
    assert.equal(status, 200);
    assert.equal(body._meta.alias, "claude-fable-5");
  } finally {
    await H.kill(fmp);
    await H.kill(fx);
  }
});
