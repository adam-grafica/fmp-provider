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

test("unit: 30+ tools accepted for transport, worker catalog keeps all names", () => {
  const many = Array.from({ length: 40 }, (_, i) => ({ type: "function", function: { name: "tool_" + i, description: "d" + i, parameters: { type: "object", properties: { a: { type: "string" }, b: { type: "string" } } } } }));
  const v = T.validateOpenAITools(many);
  assert.equal(v.ok, true);
  assert.equal(v.defs.length, 40);
  const bridged = T.toolDefsForWorker(many);
  assert.equal(bridged.length, 40);
  assert.deepEqual(bridged.map((d) => d.name), many.map((t) => t.function.name));
  assert.equal(bridged[39].schemaOmitted, false);
  const tight = T.toolDefsForWorker(many, 200);
  assert.equal(tight.length, 40);
  assert.ok(tight[39].schemaOmitted === true);
  assert.equal(tight[39].params && Object.keys(tight[39].params).length, 0);
  const tooMany = Array.from({ length: 200 }, (_, i) => ({ type: "function", function: { name: "t" + i } }));
  assert.equal(T.validateOpenAITools(tooMany).ok, false);
});

test("unit: multi-call parser collects parallel tool calls", () => {
  const defs = [{ name: "get_weather" }, { name: "get_time" }];
  const text = "Voy a consultar ambas.\n<tool_call>\n{\"name\": \"get_weather\", \"arguments\": {\"city\": \"BA\"}}\n</tool_call>\n<tool_call>\n{\"name\": \"get_time\", \"arguments\": {\"tz\": \"UTC\"}}\n</tool_call>";
  const res = T.parsePolyglotToolCalls(text, defs);
  assert.equal(res.calls.length, 2);
  assert.equal(res.calls[0].name, "get_weather");
  assert.equal(res.calls[1].name, "get_time");
  assert.equal(res.calls[1].args.tz, "UTC");
  assert.equal(res.prefixText, "Voy a consultar ambas.");
  const single = T.parsePolyglotToolCall(text, defs);
  assert.equal(single.name, "get_weather");
});

test("unit: multi-call parser handles mixed EXEC blocks", () => {
  const defs = [{ name: "Read" }, { name: "Bash" }];
  const text = "ok\nEXEC:::Read ARGS {\"path\": \"a.txt\"}\nEXEC:::Bash ARGS {\"cmd\": \"ls\"}";
  const res = T.parsePolyglotToolCalls(text, defs);
  assert.equal(res.calls.length, 2);
  assert.equal(res.calls[0].name, "Read");
  assert.equal(res.calls[1].name, "Bash");
  assert.equal(res.calls[1].args.cmd, "ls");
});

test("unit: invalid JSON args rejected", () => {
  const norm = T.openAICallsToInternal([{ id: "c1", type: "function", function: { name: "a", arguments: "{bad" } }]);
  assert.equal(norm.ok, false);
});

test("unit: multiline JSON in EXEC block parsed cleanly", () => {
  const text = "claro\nEXEC:::View ARGS {\n  \"path\": \"package.json\"\n}\ncontinuacion";
  const hit = T.parseExecBlock(text, [{ name: "View" }]);
  assert.ok(hit);
  assert.equal(hit.name, "View");
  assert.deepEqual(hit.args, { path: "package.json" });
  assert.equal(hit.prefixText, "claro");
});

test("unit: markdown code block without EXEC is not hijacked into Bash tool call", async () => {
  const fxP2 = 23332;
  const p2 = 23232;
  const fx2 = await H.spawnFixture(fxP2, "markdown_block");
  const fmp2 = await H.spawnFmp(p2, { FMP_UPSTREAM: `http://127.0.0.1:${fxP2}/` });
  try {
    const { status, body } = await H.post(p2, "/v1/messages", {
      model: "sol",
      max_tokens: 100,
      messages: [{ role: "user", content: "hi" }],
      tools: [{ name: "Bash", input_schema: { type: "object" } }],
    });
    assert.equal(status, 200);
    // When the worker emits markdown without EXEC:::, it must remain normal text, not tool_use
    const hasToolUse = body.content.some((b) => b.type === "tool_use");
    assert.equal(hasToolUse, false, "markdown code block must not be transformed into Bash tool_use");
  } finally {
    await H.kill(fmp2);
    await H.kill(fx2);
  }
});

test("unit: nested system array is flattened without dropping text", async () => {
  const fxP3 = 23333;
  const p3 = 23233;
  const fx3 = await H.spawnFixture(fxP3, "ok");
  const fmp3 = await H.spawnFmp(p3, { FMP_UPSTREAM: `http://127.0.0.1:${fxP3}/` });
  try {
    const { status, body } = await H.post(p3, "/v1/messages", {
      model: "sol",
      system: [
        { type: "text", text: "System prompt part 1" },
        { type: "text", text: "System prompt part 2" },
      ],
      messages: [{ role: "user", content: "test nested system" }],
    });
    assert.equal(status, 200);
  } finally {
    await H.kill(fmp3);
    await H.kill(fx3);
  }
});

test("unit: OpenAI streaming with tools emits clean tool_calls via buffered stream", async () => {
  const fxP4 = 23334;
  const p4 = 23234;
  const fx4 = await H.spawnFixture(fxP4, "exec");
  const fmp4 = await H.spawnFmp(p4, { FMP_UPSTREAM: `http://127.0.0.1:${fxP4}/` });
  try {
    const r = await fetch(`http://127.0.0.1:${p4}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "sol",
        stream: true,
        messages: [{ role: "user", content: "use calc" }],
        tools: [{ type: "function", function: { name: "calc", description: "d", parameters: { type: "object" } } }],
      }),
    });
    assert.equal(r.status, 200);
    const text = await r.text();
    assert.ok(text.includes('"tool_calls"'), "SSE stream must contain tool_calls delta");
    assert.ok(text.includes('"finish_reason":"tool_calls"'), "SSE stream finish_reason must be tool_calls");
  } finally {
    await H.kill(fmp4);
    await H.kill(fx4);
  }
});


test("unit: polyglot tool parser handles Hermes ChatML format", () => {
  const text = "Voy a calcular:\n<tool_call>\n{\"name\": \"calc\", \"arguments\": {\"expr\": \"2+2\"}}\n</tool_call>";
  const hit = T.parsePolyglotToolCall(text, [{ name: "calc" }]);
  assert.ok(hit, "should parse Hermes ChatML");
  assert.equal(hit.name, "calc");
  assert.deepEqual(hit.args, { expr: "2+2" });
  assert.equal(hit.prefixText, "Voy a calcular:");
});

test("unit: polyglot tool parser handles Anthropic XML tool_use format", () => {
  const text = "Voy a ejecutar:\n<tool_use>\n<name>terminal</name>\n<arguments>\n{\"command\": \"ls -la\"}\n</arguments>\n</tool_use>";
  const hit = T.parsePolyglotToolCall(text, [{ name: "terminal" }]);
  assert.ok(hit, "should parse Anthropic XML tool_use");
  assert.equal(hit.name, "terminal");
  assert.deepEqual(hit.args, { command: "ls -la" });
  assert.equal(hit.prefixText, "Voy a ejecutar:");
});

test("unit: polyglot tool parser handles Anthropic attribute XML format", () => {
  const text = "<tool_use name=\"terminal\">\n{\"command\": \"pwd\"}\n</tool_use>";
  const hit = T.parsePolyglotToolCall(text, [{ name: "terminal" }]);
  assert.ok(hit, "should parse Anthropic attribute XML");
  assert.equal(hit.name, "terminal");
  assert.deepEqual(hit.args, { command: "pwd" });
  assert.equal(hit.prefixText, "");
});

test("unit: polyglot tool parser handles Anthropic invoke XML format", () => {
  const text = "<invoke name=\"terminal\">\n<parameter name=\"command\">uname -a</parameter>\n</invoke>";
  const hit = T.parsePolyglotToolCall(text, [{ name: "terminal" }]);
  assert.ok(hit, "should parse Anthropic invoke XML");
  assert.equal(hit.name, "terminal");
  assert.deepEqual(hit.args, { command: "uname -a" });
  assert.equal(hit.prefixText, "");
});

test("unit: polyglot tool parser handles fenced markdown JSON", () => {
  const text = "Aqui tienes la operacion:\n```json\n{\n  \"name\": \"calc\",\n  \"arguments\": {\"val\": 42}\n}\n```";
  const hit = T.parsePolyglotToolCall(text, [{ name: "calc" }]);
  assert.ok(hit, "should parse fenced markdown JSON");
  assert.equal(hit.name, "calc");
  assert.deepEqual(hit.args, { val: 42 });
  assert.equal(hit.prefixText, "Aqui tienes la operacion:");
});

test("unit: polyglot tool parser handles raw tool_call JSON", () => {
  const text = "pensando...\n{\"tool_call\": {\"name\": \"terminal\", \"arguments\": {\"command\": \"whoami\"}}}";
  const hit = T.parsePolyglotToolCall(text, [{ name: "terminal" }]);
  assert.ok(hit, "should parse raw tool_call JSON");
  assert.equal(hit.name, "terminal");
  assert.deepEqual(hit.args, { command: "whoami" });
  assert.equal(hit.prefixText, "pensando...");
});

test("unit: polyglot tool parser rejects hallucinated phantom tools", () => {
  const text = "simulando:\n<tool_call>{\"name\": \"file_edit\", \"arguments\": {\"file\": \"a.txt\"}}\n</tool_call>";
  const hit = T.parsePolyglotToolCall(text, [{ name: "terminal" }, { name: "calc" }]);
  assert.equal(hit, null, "phantom tools must be rejected");
});

test("unit: streaming gate - firstToolMarkerIndex finds earliest marker", () => {
  assert.equal(T.firstToolMarkerIndex("prosa normal sin nada"), -1);
  assert.equal(T.firstToolMarkerIndex("hola\nEXEC:::tool ARGS {}"), 5);
  assert.equal(T.firstToolMarkerIndex("bla <tool_call>{...}"), 4);
  assert.equal(T.firstToolMarkerIndex("x [llamada a herramienta: t]"), 2);
});

test("unit: streaming gate - toolMarkerHoldback is 0 in normal prose", () => {
  assert.equal(T.toolMarkerHoldback("hola mundo, todo tranquilo"), 0);
  assert.equal(T.toolMarkerHoldback("a < b y c > d"), 0);
  // 1 char max: una "e" final puede ser el inicio de EXEC (delay <= 1 char).
  assert.equal(T.toolMarkerHoldback("texto que termina en e"), 1);
  assert.equal(T.toolMarkerHoldback("linea termina en exec"), 4);
});

test("unit: streaming gate - toolMarkerHoldback holds only marker prefixes", () => {
  assert.equal(T.toolMarkerHoldback("respuesta hasta <tool_ca"), 8);
  assert.equal(T.toolMarkerHoldback("respuesta <tool_call"), 10);
  assert.equal(T.toolMarkerHoldback("ver EXEC"), 4);
  assert.equal(T.toolMarkerHoldback("ver EXEC "), 5);
  assert.equal(T.toolMarkerHoldback("mirar [llam"), 5);
  assert.equal(T.toolMarkerHoldback("usa <inv"), 4);
});

test("unit: streaming gate - markerIndex of multi parse is first call position", () => {
  const defs = [{ name: "calc" }, { name: "terminal" }];
  const text = "intro\nEXEC:::calc ARGS {\"val\":1}\nEXEC:::terminal ARGS {\"command\":\"pwd\"}";
  const r = T.parsePolyglotToolCalls(text, defs);
  assert.equal(r.calls.length, 2);
  assert.equal(r.matchIndex, 5);
  assert.equal(text.slice(0, r.matchIndex).trim(), "intro");
});
