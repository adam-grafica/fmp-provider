// FMP contract tests: OpenAI/Anthropic non-stream + stream, retry, JSON
// fallback, model catalog metadata. Fixtures only (no live secrets).
"use strict";
const { test, before, after } = require("node:test");
const assert = require("node:assert");
const H = require("./helpers");

let FX; let FMP;
const FXP = 23311; const FMP_PORT = 23211;

before(async () => {
  FX = await H.spawnFixture(FXP, "ok");
  FMP = await H.spawnFmp(FMP_PORT, { FMP_UPSTREAM: `http://127.0.0.1:${FXP}/` });
});

after(async () => {
  await H.kill(FMP); await H.kill(FX);
});

test("OpenAI chat non-stream response", async () => {
  const { status, body } = await H.post(FMP_PORT, "/v1/chat/completions",
    { model: "sol", messages: [{ role: "user", content: "hi" }] });
  assert.equal(status, 200);
  // Complete text via SSE-retry or JSON fallback — either path is valid.
  assert.ok(["fixture json reply", "Hello world"].includes(body.choices[0].message.content),
    "got: " + body.choices[0].message.content);
  assert.equal(body._meta.alias, "sol");
  assert.equal(body._meta.route, "openai");
  assert.equal(body._meta.verified_identity, "unknown");
});

test("OpenAI chat stream completes", async () => {
  const r = await fetch(`http://127.0.0.1:${FMP_PORT}/v1/chat/completions`, {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ model: "sol", messages: [{ role: "user", content: "hi" }], stream: true }),
  });
  const text = await r.text();
  assert.ok(text.includes("data: [DONE]"), "stream must end with DONE");
  assert.ok(text.includes("Hello"), "stream carries fixture text");
});

test("Anthropic messages non-stream response", async () => {
  const { status, body } = await H.post(FMP_PORT, "/v1/messages",
    { model: "sol", max_tokens: 100, messages: [{ role: "user", content: "hi" }] });
  assert.equal(status, 200);
  assert.equal(body.stop_reason, "end_turn");
  assert.equal(body._meta.route, "anthropic");
  assert.equal(body._meta.verified_identity, "unknown");
});

test("Anthropic stream completes", async () => {
  const r = await fetch(`http://127.0.0.1:${FMP_PORT}/v1/messages`, {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ model: "sol", max_tokens: 100, messages: [{ role: "user", content: "hi" }], stream: true }),
  });
  const text = await r.text();
  assert.ok(text.includes("message_stop"), "anthropic stream must close");
});

test("model catalog metadata honest", async () => {
  const { status, body } = await H.get(FMP_PORT, "/v1/models");
  assert.equal(status, 200);
  assert.equal(body.data.length, 8);
  for (const m of body.data) {
    assert.ok(m.transport.includes("openai") && m.transport.includes("anthropic"));
    assert.equal(m.verified_identity, "unknown");
    assert.ok(m.declared_identity.includes("upstream-declared"));
    assert.equal(m.status, "active");
    assert.ok(Array.isArray(m.capabilities));
  }
});

test("no silent fallback: missing model -> 400", async () => {
  const { status, body } = await H.post(FMP_PORT, "/v1/chat/completions",
    { messages: [{ role: "user", content: "hi" }] });
  assert.equal(status, 400);
  assert.equal(body.error.code, "model_required");
});

test("unknown model -> 404, no fallback", async () => {
  const { status, body } = await H.post(FMP_PORT, "/v1/chat/completions",
    { model: "does-not-exist", messages: [{ role: "user", content: "hi" }] });
  assert.equal(status, 404);
  assert.equal(body.error.code, "model_not_found");
});

test("truncated upstream retries then completes", async (t) => {
  const fx2 = await H.spawnFixture(23312, "truncate-once");
  const fmp2 = await H.spawnFmp(23212, { FMP_UPSTREAM: "http://127.0.0.1:23312/" });
  t.after(async () => { await H.kill(fmp2); await H.kill(fx2); });
  const { status, body } = await H.post(23212, "/v1/chat/completions",
    { model: "sol", messages: [{ role: "user", content: "hi" }] });
  assert.equal(status, 200);
  assert.ok(body.choices[0].message.content.includes("Hello"));
});

test("JSON fallback on always-truncate stream", async (t) => {
  const fx3 = await H.spawnFixture(23313, "always-truncate");
  const fmp3 = await H.spawnFmp(23213, { FMP_UPSTREAM: "http://127.0.0.1:23313/" });
  t.after(async () => { await H.kill(fmp3); await H.kill(fx3); });
  const { status, body } = await H.post(23213, "/v1/chat/completions",
    { model: "sol", messages: [{ role: "user", content: "hi" }] });
  assert.equal(status, 200);
  assert.ok(body.choices[0].message.content.length > 0);
});
