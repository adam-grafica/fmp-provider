"use strict";
// FMP tool TRANSPORT contract (v2.2.0). Transport only — never execution.
//
// FMP moves validated tool schemas between client and upstream as data:
//   OpenAI tools/tool_calls  <->  InternalToolCall  <->  Anthropic tools/tool_use
// The upstream worker used by FMP speaks plain text, so tool calls that
// must cross the worker boundary travel inside a text protocol block
// (EXEC:::<name> ARGS <json>) and are converted back to native schema
// on return. FMP never executes a tool, shell command, or host action;
// the client harness owns execution and permissions.
//
// InternalToolCall: { id: string|null, name: string, arguments: object, transport: "openai"|"anthropic" }
// Only tool NAMES (and result status) are ever logged — never arguments.

const crypto = require("node:crypto");

const NAME_RE = /^[A-Za-z0-9_.-]{1,128}$/;
// Transport accepts up to 128 tool schemas (OpenCode/Codex send 30+);
// the WORKER bridge catalog stays capped at 25 (prompt budget).
const MAX_TOOLS = 128;
const MAX_WORKER_TOOLS = 25;

function rid(n) {
  return crypto.randomBytes(Math.ceil((n || 12) / 2)).toString("hex").slice(0, n || 12);
}

function isObject(v) {
  return !!v && typeof v === "object" && !Array.isArray(v);
}

// ---- schema validation (client -> FMP) ----

function validateOpenAITools(tools) {
  if (tools === undefined) return { ok: true, defs: [] };
  if (!Array.isArray(tools)) return { ok: false, error: "tools must be an array" };
  if (tools.length > MAX_TOOLS) return { ok: false, error: `too many tools (max ${MAX_TOOLS})` };
  const defs = [];
  for (let i = 0; i < tools.length; i++) {
    const t = tools[i];
    if (!isObject(t)) return { ok: false, error: `tools[${i}] must be an object` };
    const fn = t.type === "function" ? t.function : t.function || t;
    if (!isObject(fn)) return { ok: false, error: `tools[${i}].function must be an object` };
    const name = fn.name;
    if (typeof name !== "string" || !NAME_RE.test(name)) {
      return { ok: false, error: `tools[${i}].function.name is invalid (allowed: ${NAME_RE})` };
    }
    if (fn.parameters !== undefined && !isObject(fn.parameters)) {
      return { ok: false, error: `tools[${i}].function.parameters must be an object (JSON schema)` };
    }
    defs.push({
      name,
      description: typeof fn.description === "string" ? fn.description.slice(0, 400) : "",
      parameters: fn.parameters || { type: "object" },
    });
  }
  return { ok: true, defs };
}

function validateAnthropicTools(tools) {
  if (tools === undefined) return { ok: true, defs: [] };
  if (!Array.isArray(tools)) return { ok: false, error: "tools must be an array" };
  if (tools.length > MAX_TOOLS) return { ok: false, error: `too many tools (max ${MAX_TOOLS})` };
  const defs = [];
  for (let i = 0; i < tools.length; i++) {
    const t = tools[i];
    if (!isObject(t)) return { ok: false, error: `tools[${i}] must be an object` };
    if (typeof t.name !== "string" || !NAME_RE.test(t.name)) {
      return { ok: false, error: `tools[${i}].name is invalid (allowed: ${NAME_RE})` };
    }
    if (t.input_schema !== undefined && !isObject(t.input_schema)) {
      return { ok: false, error: `tools[${i}].input_schema must be an object (JSON schema)` };
    }
    defs.push({
      name: t.name,
      description: typeof t.description === "string" ? t.description.slice(0, 400) : "",
      parameters: t.input_schema || { type: "object" },
    });
  }
  return { ok: true, defs };
}

// ---- normalization to InternalToolCall (ids preserved) ----

function openAICallsToInternal(toolCalls) {
  if (toolCalls === undefined) return { ok: true, calls: [] };
  if (!Array.isArray(toolCalls)) return { ok: false, error: "tool_calls must be an array" };
  const calls = [];
  for (let i = 0; i < toolCalls.length; i++) {
    const tc = toolCalls[i];
    if (!isObject(tc)) return { ok: false, error: `tool_calls[${i}] must be an object` };
    const fn = tc.function;
    if (!isObject(fn)) return { ok: false, error: `tool_calls[${i}].function must be an object` };
    if (typeof fn.name !== "string" || !NAME_RE.test(fn.name)) {
      return { ok: false, error: `tool_calls[${i}].function.name is invalid` };
    }
    let args = {};
    if (fn.arguments !== undefined) {
      if (typeof fn.arguments !== "string") return { ok: false, error: `tool_calls[${i}].function.arguments must be a JSON string` };
      try { args = JSON.parse(fn.arguments); }
      catch { return { ok: false, error: `tool_calls[${i}].function.arguments is not valid JSON` }; }
      if (!isObject(args) && !Array.isArray(args)) return { ok: false, error: `tool_calls[${i}].function.arguments must decode to an object/array` };
    }
    calls.push({
      id: typeof tc.id === "string" && tc.id ? tc.id : null,
      name: fn.name,
      arguments: args,
      transport: "openai",
    });
  }
  return { ok: true, calls };
}

function anthropicBlocksToInternal(content) {
  if (!Array.isArray(content)) return { ok: true, calls: [] };
  const calls = [];
  content.forEach((b) => {
    if (isObject(b) && b.type === "tool_use") {
      if (typeof b.name === "string" && NAME_RE.test(b.name) && (b.input === undefined || isObject(b.input))) {
        calls.push({
          id: typeof b.id === "string" && b.id ? b.id : null,
          name: b.name,
          arguments: b.input || {},
          transport: "anthropic",
        });
      }
    }
  });
  return { ok: true, calls };
}

// ---- emission back to native schema (ids preserved, generated only when absent) ----

function internalToOpenAI(calls) {
  return (calls || []).map((c) => ({
    id: c.id || ("call_" + rid(20)),
    type: "function",
    function: { name: c.name, arguments: JSON.stringify(c.arguments || {}) },
  }));
}

function internalToAnthropic(calls) {
  return (calls || []).map((c) => ({
    type: "tool_use",
    id: c.id || ("toolu_" + rid(20)),
    name: c.name,
    input: c.arguments || {},
  }));
}

// Find a prior client-supplied call id for the same tool name so retried
// or continued turns keep stable ids instead of minting new ones.
function findPriorCallId(priorCalls, name) {
  const hit = (priorCalls || []).find((c) => c && c.name === name && c.id);
  return hit ? hit.id : null;
}

// ---- worker text-protocol bridge (transport detail, not execution) ----

function parseExecBlock(text, defs) {
  if (!text || !Array.isArray(defs) || !defs.length) return null;
  const i = text.indexOf("EXEC:::");
  if (i < 0) return null;
  const after = text.slice(i + 7).trim();
  const m = after.match(/^([A-Za-z0-9_.-]+)\s+ARGS\s+([\s\S]*)$/);
  if (!m) return null;
  const name = m[1];
  let rawJson = m[2].trim();
  if (!defs.some((d) => d.name === name)) return null;
  let args = {};
  try {
    args = JSON.parse(rawJson);
  } catch {
    const start = rawJson.indexOf("{");
    if (start >= 0) {
      let depth = 0;
      let end = -1;
      for (let k = start; k < rawJson.length; k++) {
        if (rawJson[k] === "{") depth++;
        else if (rawJson[k] === "}") {
          depth--;
          if (depth === 0) { end = k; break; }
        }
      }
      if (end > start) {
        try { args = JSON.parse(rawJson.slice(start, end + 1)); } catch { return null; }
      } else return null;
    } else return null;
  }
  return { name, args, prefixText: text.slice(0, i).trim() };
}

function toolDefsForWorker(tools) {
  return (tools || [])
    .filter((t) => t && (t.type === "function" || t.name) && (t.name || (t.function && t.function.name)))
    .map((t) => {
      const fn = t.type === "function" ? t : { name: t.name, description: t.description, parameters: t.input_schema };
      const name = fn.name || (t.function && t.function.name) || "";
      const desc = fn.description || (t.function && t.function.description) || "";
      const params = fn.parameters || (t.function && t.function.parameters) || {};
      return { name, description: String(desc).slice(0, 400), params };
    })
    .filter((d) => d.name)
    .slice(0, MAX_WORKER_TOOLS);
}

module.exports = {
  NAME_RE, MAX_TOOLS, MAX_WORKER_TOOLS,
  validateOpenAITools, validateAnthropicTools,
  openAICallsToInternal, anthropicBlocksToInternal,
  internalToOpenAI, internalToAnthropic, findPriorCallId,
  parseExecBlock, toolDefsForWorker, rid,
};
