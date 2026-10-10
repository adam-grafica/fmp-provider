// FMP tool transport tests: schemas validated, ids preserved, never
// executed, malformed rejected. Fixtures only.
"use strict";
const { test, before, after } = require("node:test");
const assert = require("node:assert");
const H = require("./helpers");
const T = require("../lib/tools");

let FX; let FMP;
const FXP = 23331; const P = 23231;

before(async () => {
  FX = await H.spawnFixture(FXP, "exec");
  FMP = await H.spawnFmp(P, { FMP_UPSTREAM: `http://127.0.0.1:${FXP}/` });
});

after(async () => {
  await H.kill(FMP); await H.kill(FX);
});

test("OpenAI tool schema transported as tool_calls", async () => {
  const tool = { type: "function", function: { name: "calc", description: "d", parameters: { type: "object" } } };
  const { status, body } = await H.post(P, "/v1/chat/completions",
    { model: "sol", messages: [{ role: "user", content: "use calc" }], tools: [tool] });
  assert.equal(status, 200);
  assert.equal(body.choices[0].finish_reason, "tool_calls");
  const tc = body.choices[0].message.tool_calls[0];
  assert.equal(tc.function.name, "calc");
  assert.deepEqual(JSON.parse(tc.function.arguments), { a: 1 });
  assert.ok(tc.id, "tool id must be present/preserved");
});

test("Anthropic tool schema transported as tool_use", async () => {
  const { status, body } = await H.post(P, "/v1/messages",
    { model: "sol", max_tokens: 100, messages: [{ role: "user", content: "use calc" }], tools: [{ name: "calc", input_schema: { type: "object" } }] });
  assert.equal(status, 200);
  assert.equal(body.stop_reason, "tool_use");
  const tu = body.content.find((b) => b.type === "tool_use");
  assert.ok(tu, "tool_use block present");
  assert.equal(tu.name, "calc");
  assert.ok(tu.id, "tool id present");
});

test("malformed OpenAI tool schema rejected", async () => {
  const bads = [
    [{ type: "function", function: { name: "bad name!" } }],
    [{ type: "function", function: { name: "x", parameters: "not-object" } }],
    ["not-an-object"],
  ];
  for (const tools of bads) {
    const { status, body } = await H.post(P, "/v1/chat/completions",
      { model: "sol", messages: [{ role: "user", content: "hi" }], tools });
    assert.equal(status, 400, JSON.stringify(tools));
    assert.equal(body.error.code, "invalid_tool_schema");
  }
});

test("malformed Anthropic tool schema rejected", async () => {
  const { status, body } = await H.post(P, "/v1/messages",
    { model: "sol", max_tokens: 10, messages: [{ role: "user", content: "hi" }], tools: [{ description: "no-name" }] });
  assert.equal(status, 400);
  assert.ok(body.error.message.includes("invalid tools schema"));
});

test("unit: ids preserved across normalization", () => {
  const norm = T.openAICallsToInternal([{ id: "call_keep1", type: "function", function: { name: "a", arguments: "{}" } }]);
  assert.ok(norm.ok);
  assert.equal(norm.calls[0].id, "call_keep1");
  const back = T.internalToOpenAI(norm.calls);
  assert.equal(back[0].id, "call_keep1");
  const prior = T.findPriorCallId(norm.calls, "a");
  assert.equal(prior, "call_keep1");
});

test("unit: unknown EXEC tool rejected (no phantom calls)", () => {
  const hit = T.parseExecBlock("x\nEXEC:::ghost ARGS {}", [{ name: "real" }]);
  assert.equal(hit, null);
});

test("unit: invalid JSON args rejected", () => {
  const norm = T.openAICallsToInternal([{ id: "c1", type: "function", function: { name: "a", arguments: "{bad" } }]);
  assert.equal(norm.ok, false);
});
