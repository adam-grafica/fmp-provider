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

const NAME_RE = /^[A-Za-z0-9_.:-]{1,128}$/;
// Transport accepts up to 128 tool schemas (OpenCode/Codex send 30+).
// The worker prompt catalog no longer hides declared tools: every NAME
// is always listed; full JSON schemas fill a character budget first and
// the rest stay compact (name + description). Fixed-count caps made
// tools #26+ invisible to the model ("se caían" en producción).
const MAX_TOOLS = 128;
const MAX_WORKER_SCHEMA_CHARS = 20000;

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

// ---- worker text-protocol bridge (transport detail, not execution) ----

// ---- balanced JSON scanner & extraction ----

function extractBalancedJson(str, startIdx) {
  let inString = false;
  let escape = false;
  let depth = 0;
  for (let i = startIdx; i < str.length; i++) {
    const ch = str[i];
    if (escape) {
      escape = false;
      continue;
    }
    if (ch === "\\") {
      escape = true;
      continue;
    }
    if (ch === "\"") {
      inString = !inString;
      continue;
    }
    if (!inString) {
      if (ch === "{") depth++;
      else if (ch === "}") {
        depth--;
        if (depth === 0) {
          const slice = str.slice(startIdx, i + 1);
          try {
            return { obj: JSON.parse(slice), endIndex: i };
          } catch {
            return null;
          }
        }
      }
    }
  }
  return null;
}

function findJsonObjects(str) {
  const results = [];
  let i = 0;
  while (i < str.length) {
    if (str[i] === "{") {
      const res = extractBalancedJson(str, i);
      if (res && res.obj && typeof res.obj === "object" && !Array.isArray(res.obj)) {
        results.push({ obj: res.obj, startIndex: i, endIndex: res.endIndex });
        i = res.endIndex + 1;
        continue;
      }
    }
    i++;
  }
  return results;
}

function cleanPrefix(prefix) {
  return (prefix || "").replace(/```(?:json|bash|sh|xml|zsh)?\s*$/i, "").trim();
}

function matchToolDef(name, defs) {
  if (!name || typeof name !== "string") return null;
  return defs.find((d) => d.name.toLowerCase() === name.trim().toLowerCase()) || null;
}

// ---- polyglot multi-format tool parser ----
// Adapts to the LLM native training:
// 1. Hermes / ChatML / Qwen / DeepSeek: <tool_call>{"name":"...","arguments":{...}}</tool_call>
// 2. Anthropic Claude standard: <tool_use><name>...</name><arguments>{...}</arguments></tool_use>
// 3. Anthropic Claude attribute: <tool_use name="...">...</tool_use>
// 4. Anthropic Claude invoke: <invoke name="..."><parameter name="...">val</parameter></invoke>
// 5. Legacy EXEC::: or [llamada a herramienta / tool_call]
// 6. Fenced markdown JSON or bare JSON objects with tool_call signature
function parsePolyglotToolCall(text, defs) {
  if (!text || !Array.isArray(defs) || !defs.length) return null;

  // 1. Tag-based matching: <tool_call>, <tool_use>, <invoke>
  // Case A: <tool_call>...JSON...</tool_call>
  const toolCallTagRegex = /<tool_call>([\s\S]*?)<\/tool_call>/gi;
  let tagMatch;
  while ((tagMatch = toolCallTagRegex.exec(text)) !== null) {
    const inner = tagMatch[1];
    const jsons = findJsonObjects(inner);
    for (const j of jsons) {
      const rawName = j.obj.name || j.obj.tool || (j.obj.function && j.obj.function.name);
      const def = matchToolDef(rawName, defs);
      if (def) {
        let args = j.obj.arguments ?? j.obj.parameters ?? j.obj.params ?? {};
        if (typeof args === "string") {
          try { args = JSON.parse(args); } catch {}
        }
        return {
          name: def.name,
          args: typeof args === "object" && args !== null ? args : {},
          prefixText: cleanPrefix(text.slice(0, tagMatch.index)),
          matchIndex: tagMatch.index,
          endIndex: tagMatch.index + tagMatch[0].length,
        };
      }
    }
  }

  // Case B: <tool_use ...>...</tool_use>
  const toolUseTagRegex = /<tool_use(?:\s+name=["']([^"']+)["'])?>([\s\S]*?)<\/tool_use>/gi;
  while ((tagMatch = toolUseTagRegex.exec(text)) !== null) {
    let rawName = tagMatch[1];
    const inner = tagMatch[2];
    if (!rawName) {
      const nameTag = inner.match(/<name>([\s\S]*?)<\/name>/i);
      if (nameTag) rawName = nameTag[1].trim();
    }
    const def = matchToolDef(rawName, defs);
    if (def) {
      let args = {};
      const argsTag = inner.match(/<(?:arguments|parameters)>([\s\S]*?)<\/(?:arguments|parameters)>/i);
      const searchTarget = argsTag ? argsTag[1] : inner;
      const jsons = findJsonObjects(searchTarget);
      if (jsons.length > 0) {
        args = jsons[0].obj;
      }
      return {
        name: def.name,
        args,
        prefixText: cleanPrefix(text.slice(0, tagMatch.index)),
        matchIndex: tagMatch.index,
        endIndex: tagMatch.index + tagMatch[0].length,
      };
    }
  }

  // Case C: <invoke name="...">...</invoke>
  const invokeTagRegex = /<invoke\s+name=["']([^"']+)["']>([\s\S]*?)<\/invoke>/gi;
  while ((tagMatch = invokeTagRegex.exec(text)) !== null) {
    const rawName = tagMatch[1];
    const inner = tagMatch[2];
    const def = matchToolDef(rawName, defs);
    if (def) {
      let args = {};
      const paramRegex = /<parameter\s+name=["']([^"']+)["']>([\s\S]*?)<\/parameter>/gi;
      let pMatch;
      let foundParams = false;
      while ((pMatch = paramRegex.exec(inner)) !== null) {
        foundParams = true;
        const pKey = pMatch[1];
        let pVal = pMatch[2].trim();
        try { pVal = JSON.parse(pVal); } catch {}
        args[pKey] = pVal;
      }
      if (!foundParams) {
        const jsons = findJsonObjects(inner);
        if (jsons.length > 0) args = jsons[0].obj;
      }
      return {
        name: def.name,
        args,
        prefixText: cleanPrefix(text.slice(0, tagMatch.index)),
        matchIndex: tagMatch.index,
        endIndex: tagMatch.index + tagMatch[0].length,
      };
    }
  }

  // 2. Legacy EXEC::: or [llamada a herramienta / tool_call]
  const execRegex = /(?:^|\n)\s*(?:EXEC\s*:+|\[(?:llamada a herramienta|tool_call):?\s*)([A-Za-z0-9_.:-]+)(?:\s+ARGS\s*|\]\s*)([\s\S]*?)(?=(?:\n\s*(?:EXEC\s*:+|\[(?:llamada a herramienta|tool_call):?\s*)[A-Za-z0-9_.:-]+)|$)/gi;
  let execMatch;
  while ((execMatch = execRegex.exec(text)) !== null) {
    const rawName = execMatch[1];
    const def = matchToolDef(rawName, defs);
    if (!def) continue;
    let rawJson = execMatch[2].trim();
    let args = {};
    const jsons = findJsonObjects(rawJson);
    if (jsons.length > 0) {
      args = jsons[0].obj;
    } else {
      try { args = JSON.parse(rawJson); } catch { continue; }
    }
    return {
      name: def.name,
      args,
      prefixText: cleanPrefix(text.slice(0, execMatch.index)),
      matchIndex: execMatch.index,
      endIndex: execMatch.index + execMatch[0].length,
    };
  }

  // 3. Fenced markdown JSON or bare JSON objects
  const allJsons = findJsonObjects(text);
  for (const j of allJsons) {
    const obj = j.obj;
    let rawName = null;
    let args = null;
    if (obj.tool_call && typeof obj.tool_call === "object") {
      rawName = obj.tool_call.name || obj.tool_call.tool;
      args = obj.tool_call.arguments || obj.tool_call.parameters;
    } else if (obj.function && typeof obj.function === "object") {
      rawName = obj.function.name;
      args = obj.function.arguments;
    } else if (obj.name && (obj.arguments !== undefined || obj.parameters !== undefined || obj.params !== undefined)) {
      rawName = obj.name;
      args = obj.arguments ?? obj.parameters ?? obj.params;
    } else if (obj.tool && (obj.arguments !== undefined || obj.parameters !== undefined || obj.params !== undefined)) {
      rawName = obj.tool;
      args = obj.arguments ?? obj.parameters ?? obj.params;
    }

    if (rawName) {
      const def = matchToolDef(rawName, defs);
      if (def) {
        if (typeof args === "string") {
          try { args = JSON.parse(args); } catch {}
        }
        return {
          name: def.name,
          args: typeof args === "object" && args !== null ? args : {},
          prefixText: cleanPrefix(text.slice(0, j.startIndex)),
          matchIndex: j.startIndex,
          endIndex: j.endIndex + 1,
        };
      }
    }
  }

  return null;
}

// Aliased for full backwards compatibility
const parseExecBlock = parsePolyglotToolCall;

// Multi-call aware wrapper (v2.3.0): iterates the battle-tested single
// parser over the remainder of the text, collecting EVERY call (parallel
// tool use). Each iteration consumes the matched span via endIndex, so
// no call is double-counted. prefixText is the leading text before the
// first call (same shape the legacy single parser returned).
function parsePolyglotToolCalls(text, defs) {
  if (!text || !Array.isArray(defs) || !defs.length) return { calls: [], prefixText: "", matchIndex: -1 };
  const calls = [];
  let prefixText = "";
  let matchIndex = -1;
  let rest = text;
  for (let guard = 0; guard < 32; guard++) {
    const hit = parsePolyglotToolCall(rest, defs);
    if (!hit) break;
    if (!calls.length) {
      prefixText = hit.prefixText || "";
      matchIndex = typeof hit.matchIndex === "number" ? hit.matchIndex : -1;
    }
    calls.push({ name: hit.name, args: hit.args && typeof hit.args === "object" ? hit.args : {} });
    const end = typeof hit.endIndex === "number" ? hit.endIndex : -1;
    if (end <= 0 || end >= rest.length) break;
    rest = rest.slice(end);
  }
  return { calls, prefixText, matchIndex };
}

// Streaming tool-marker gate (v2.3.3): the live SSE paths stop forwarding
// worker text the moment a tool-call marker starts forming, so protocol
// blocks (EXEC:::<name> ARGS {json}, <tool_call>..., <tool_use ...>, ...)
// never leak to the client as visible text. If no call is resolved at turn
// end, the held text is flushed as plain text - nothing is ever lost.
const TOOL_MARKER_RES = [
  /EXEC\s*:+/i,
  /<tool_call>/i,
  /<tool_use\b/i,
  /<invoke\b/i,
  /\[tool_call/i,
  /\[llamada a herramienta/i,
];

// Earliest index in `text` (from `fromIdx`) at which a full tool marker
// appears; -1 when there is none.
function firstToolMarkerIndex(text, fromIdx) {
  if (!text) return -1;
  const start = Math.max(0, fromIdx || 0);
  if (start >= text.length) return -1;
  const scan = text.slice(start);
  let best = -1;
  for (const re of TOOL_MARKER_RES) {
    const m = scan.match(re);
    if (m && typeof m.index === "number") {
      const idx = start + m.index;
      if (best < 0 || idx < best) best = idx;
    }
  }
  return best;
}

// Longest marker in the wild is "[llamada a herramienta"; GATE_HOLDBACK is
// the hard ceiling, but the live gate uses toolMarkerHoldback() so normal
// prose releases instantly (0-char holdback) and only a tail that could
// still grow into a marker is withheld - near-zero added latency.
const GATE_HOLDBACK = 24;

// Literales iniciales de cada marcador (sin el \s*: entre EXEC y colones).
const MARKER_STARTERS = ["<tool_call>", "<tool_use", "<invoke", "[tool_call", "[llamada a herramienta"];

// Cuántos caracteres finales de `text` podrían seguir creciendo hasta ser un
// marcador completo (p.ej. "...<tool_ca" -> 8). 0 en prosa normal: el texto
// se puede emitir ya mismo. Reemplaza al holdback fijo en el gate de
// streaming (v2.3.3-latency): el delay solo existe mientras la cola es
// ambiguamente un posible marcador, y se resuelve con el siguiente delta.
function toolMarkerHoldback(text) {
  if (!text) return 0;
  const upto = Math.min(text.length, GATE_HOLDBACK);
  let best = 0;
  for (let L = 1; L <= upto; L++) {
    const low = text.slice(text.length - L).toLowerCase();
    let hit = false;
    for (const lit of MARKER_STARTERS) {
      if (lit.length >= L && lit.toLowerCase().startsWith(low)) { hit = true; break; }
    }
    // EXEC + espacios opcionales: el siguiente delta puede traer ":::".
    if (!hit && /^(?:e|ex|exe|exec)\s*$/.test(low)) hit = true;
    if (hit) best = L;
  }
  return best;
}

// Cheap text signal that the model is attempting a tool call. Used to
// avoid doubling upstream calls (FC-RETRY) on every plain-text turn.
const TOOL_INTENT_RE = /<tool_call>|<\/?tool_use\b|<\/?invoke\b|EXEC\s*:+|\[tool_call|```(?:bash|sh|shell|zsh)\b|"name"\s*:/i;
function toolIntentHints(text) {
  return !!text && TOOL_INTENT_RE.test(text);
}

// Worker prompt catalog (v2.3.0): EVERY declared tool name is listed so
// the model can always call it; full JSON schemas are included while the
// character budget lasts, the rest stay compact (name + description,
// schemaOmitted=true) instead of vanishing. The old fixed-count cap hid
// tools past #25 from the model entirely ("se caían" en producción).
function toolDefsForWorker(tools, maxSchemaChars) {
  const all = (tools || [])
    .filter((t) => t && (t.type === "function" || t.name) && (t.name || (t.function && t.function.name)))
    .map((t) => {
      const fn = t.type === "function" ? t : { name: t.name, description: t.description, parameters: t.input_schema };
      const name = fn.name || (t.function && t.function.name) || "";
      const desc = fn.description || (t.function && t.function.description) || "";
      const params = fn.parameters || (t.function && t.function.parameters) || {};
      return { name, description: String(desc).slice(0, 400), params };
    })
    .filter((d) => d.name);
  const budget = Number.isFinite(maxSchemaChars) && maxSchemaChars > 0 ? maxSchemaChars : MAX_WORKER_SCHEMA_CHARS;
  let used = 0;
  return all.map((d) => {
    const fullLen = d.name.length + d.description.length + JSON.stringify(d.params).length;
    if (used + fullLen <= budget) {
      used += fullLen;
      return { ...d, schemaOmitted: false };
    }
    return { ...d, params: {}, schemaOmitted: true };
  });
}

module.exports = {
  NAME_RE, MAX_TOOLS, MAX_WORKER_SCHEMA_CHARS,
  validateOpenAITools, validateAnthropicTools,
  openAICallsToInternal, anthropicBlocksToInternal,
  internalToOpenAI, internalToAnthropic,
  parsePolyglotToolCall, parsePolyglotToolCalls, parseExecBlock,
  extractBalancedJson, findJsonObjects, toolDefsForWorker, toolIntentHints, rid,
  firstToolMarkerIndex, GATE_HOLDBACK, toolMarkerHoldback,
};
