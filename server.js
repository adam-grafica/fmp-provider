// FMP — FreeModels Provider v1.3.0
// Traductor OpenAI-compatible + Anthropic-compatible hacia freemodels.pro.
// Sin dependencias. Loopback-only por defecto (127.0.0.1:2089).
//
// Hallazgo que motiva el diseño (2026-10-09, evidencia en mano):
// el worker upstream CORTA el SSE de forma intermitente (~1/3 de los casos):
// cierra la conexión a mitad de frase SIN emitir `data: [DONE]`.
// Estrategia: pedir `stream:true` al worker SIEMPRE en modo stream,
// acumular fragmentos en buffer hasta ver [DONE]; si el socket muere
// sin DONE, reintentar con backoff (hasta MAX_RETRIES) y CONTINUAR
// desde donde quedó (resume por longitud de texto). Solo cuando el
// texto total está íntegro se emite al cliente. En modo no-stream se
// pide JSON directo al worker.
//
// Endpoints:
//   GET  /health | /
//   GET  /v1/models | /models
//   POST /v1/chat/completions (OpenAI, stream y no-stream)
//   POST /v1/messages         (Anthropic, stream y no-stream)
//   POST /v1/responses        (Responses API p/Codex, stream y no-stream)
"use strict";

const http = require("node:http");
const crypto = require("node:crypto");
const fs = require("node:fs");

const PORT = Number(process.env.PORT || 2089);
const HOST = process.env.HOST || "127.0.0.1";
const UPSTREAM = process.env.FMP_UPSTREAM || "https://freemodels-chat.freemodels.workers.dev";
const ORIGIN = "https://freemodels.pro";
const UPSTREAM_TIMEOUT_MS = Number(process.env.FMP_TIMEOUT_MS || 120_000);
const MAX_BODY_BYTES = 10 * 1024 * 1024;
const MAX_RETRIES = Number(process.env.FMP_MAX_RETRIES || 5);
const RETRY_BASE_MS = Number(process.env.FMP_RETRY_BASE_MS || 1500);
const TRACE_FILE = process.env.SHIM_TRACE || process.env.FMP_TRACE || "";

const MODELS = [
  { id: "claude-opus-5.5", name: "Claude Opus 5.5", owned_by: "anthropic" },
  { id: "claude-sonnet-5", name: "Claude Sonnet 5", owned_by: "anthropic" },
  { id: "claude-fable-5", name: "Claude Fable 5", owned_by: "anthropic" },
  { id: "claude-fable-5.1", name: "Claude Fable 5.1", owned_by: "anthropic" },
  { id: "sol", name: "Sol", owned_by: "fmp" },
  { id: "terra", name: "Terra", owned_by: "fmp" },
  { id: "glm-5.2", name: "GLM 5.2", owned_by: "zai" },
  { id: "kimi-k3", name: "Kimi K3", owned_by: "moonshot" },
];
const MODEL_IDS = new Set(MODELS.map((m) => m.id));

const startedAt = Date.now();
const stats = { requests: 0, retries: 0, truncatedUpstream: 0, errors: 0 };
const rid = () => crypto.randomBytes(12).toString("hex");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const estTokens = (s) => Math.max(1, Math.ceil((s || "").length / 4));

function trace(msg) {
  if (!TRACE_FILE) return;
  try { fs.appendFileSync(TRACE_FILE, `[${new Date().toISOString()}] ${msg}\n`); } catch {}
}

function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(body),
    "access-control-allow-origin": "*",
  });
  res.end(body);
}

function openaiError(res, status, message, code = null) {
  sendJson(res, status, { error: { message, type: "server_error", code } });
}

function anthropicError(res, status, message) {
  sendJson(res, status, { type: "error", error: { type: "api_error", message } });
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on("data", (c) => {
      size += c.length;
      if (size > MAX_BODY_BYTES) { reject(new Error("body too large")); req.destroy(); return; }
      chunks.push(c);
    });
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

// "fmp-oai/sol" -> "sol"; "sol" -> "sol"
function stripPrefix(model) {
  if (typeof model !== "string") return "";
  const i = model.lastIndexOf("/");
  return (i >= 0 ? model.slice(i + 1) : model).trim();
}

function textOfContent(content) {
  if (content == null) return "";
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content.map((p) => {
      if (typeof p === "string") return p;
      if (p && typeof p === "object") {
        if (typeof p.text === "string") return p.text;
        if (p.type === "image_url" || p.type === "image") return "";
        if (p.type === "tool_result") return typeof p.output === "string" ? p.output : JSON.stringify(p.output ?? "");
        if (p.type === "tool_use") return "";
        return "";
      }
      return "";
    }).join("");
  }
  return String(content);
}

function toWorkerMessagesOA(messages) {
  const out = [];
  for (const m of messages || []) {
    if (!m || typeof m !== "object") continue;
    if (m.role === "tool") {
      out.push({ role: "user", content: `[resultado de herramienta ${m.tool_call_id || ""}]\n${textOfContent(m.content)}`.trim() });
      continue;
    }
    const role = m.role === "system" || m.role === "user" || m.role === "assistant" ? m.role : "user";
    let text = textOfContent(m.content);
    if (role === "assistant" && Array.isArray(m.tool_calls) && m.tool_calls.length) {
      for (const tc of m.tool_calls) {
        const fn = (tc && tc.function) || {};
        text += `\n[llamada a herramienta ${fn.name || (tc && tc.id) || ""}] ${fn.arguments || ""}`.trimEnd();
      }
      text = text.trim();
    }
    out.push({ role, content: text });
  }
  return out;
}

function toWorkerMessagesAnt(system, messages) {
  const out = [];
  if (typeof system === "string" && system) out.push({ role: "system", content: system });
  else if (Array.isArray(system)) {
    const t = textOfContent(system);
    if (t) out.push({ role: "system", content: t });
  }
  for (const m of messages || []) {
    if (!m || typeof m !== "object") continue;
    // tool_result -> worker lo ve como dato del usuario.
    if (m.role === "tool" || m.role === "user" || (m.role !== "assistant" && m.type === "tool_result")) {
      out.push({ role: "user", content: textOfContent(m.content) });
      continue;
    }
    const role = m.role === "assistant" ? "assistant" : "user";
    let text = textOfContent(m.content);
    // tool_use nativo Anthropic -> texto para el worker.
    if (role === "assistant" && Array.isArray(m.content)) {
      const uses = m.content.filter((p) => p && p.type === "tool_use");
      for (const u of uses) text += `\n[llamada a herramienta ${u.name || ""}] ${JSON.stringify(u.input || {})}`;
      text = text.trim();
    }
    out.push({ role, content: text });
  }
  return out;
}

async function callUpstream({ messages, modelId, stream, thinking, deepSearch, signal }) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(new Error("upstream timeout")), UPSTREAM_TIMEOUT_MS);
  const onAbort = () => ctrl.abort();
  if (signal) {
    if (signal.aborted) ctrl.abort();
    else signal.addEventListener("abort", onAbort, { once: true });
  }
  try {
    const r = await fetch(UPSTREAM, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        origin: ORIGIN,
        referer: ORIGIN + "/chat",
        "user-agent": "fmp-provider/1.0",
        accept: stream ? "text/event-stream" : "application/json",
      },
      body: JSON.stringify({ messages, modelId, thinking: !!thinking, deepSearch: !!deepSearch, stream }),
      signal: ctrl.signal,
    });
    if (!r.ok) {
      let detail = "";
      try {
        const j = await r.json();
        detail = (j && j.error) || JSON.stringify(j).slice(0, 300);
      } catch {
        try { detail = (await r.text()).slice(0, 300); } catch {}
      }
      const e = new Error(`upstream ${r.status}${detail ? ": " + detail : ""}`);
      e.status = r.status;
      throw e;
    }
    return r;
  } finally {
    clearTimeout(timer);
    if (signal) signal.removeEventListener("abort", onAbort);
  }
}

// Lee UN intento SSE del worker. Devuelve { text, done }.
// done=true solo si se vio `data: [DONE]`. Sin DONE = truncado.
async function readWorkerSSEOnce(response, signal) {
  const reader = response.body.getReader();
  const dec = new TextDecoder("utf-8");
  let buf = "";
  let full = "";
  let done = false;
  for (;;) {
    if (signal && signal.aborted) { try { await reader.cancel(); } catch {} break; }
    const { done: rd, value } = await reader.read();
    if (rd) break;
    buf += dec.decode(value, { stream: true });
    const lines = buf.split("\n");
    buf = lines.pop() || "";
    for (const line of lines) {
      const t = line.trim();
      if (!t || !t.startsWith("data:")) continue;
      const payload = t.slice(5).trim();
      if (!payload) continue;
      if (payload === "[DONE]") { done = true; continue; }
      try {
        const j = JSON.parse(payload);
        const c = j.choices && j.choices[0];
        const piece = (c && c.delta && c.delta.content) || (c && c.message && c.message.content) || "";
        if (piece) full += piece;
      } catch {}
    }
  }
  // Cola final: puede traer DONE pegado sin \n (upstream lo hace a veces).
  const tail = (buf + dec.decode()).trim();
  if (tail) {
    for (const chunk of tail.split("\n")) {
      const t = chunk.trim();
      if (!t || !t.startsWith("data:")) continue;
      const payload = t.slice(5).trim();
      if (payload === "[DONE]") { done = true; continue; }
      try {
        const j = JSON.parse(payload);
        const c = j.choices && j.choices[0];
        const piece = (c && c.delta && c.delta.content) || "";
        if (piece) full += piece;
      } catch {}
    }
  }
  return { text: full, done };
}

async function readWorkerJson(response) {
  const j = await response.json();
  if (typeof j.content === "string") return j.content;
  const c = j.choices && j.choices[0];
  if (c && c.message) return textOfContent(c.message.content);
  return "";
}

// Intento SSE con resume: si el worker corta sin DONE, reintenta y pega.
// resumeHint: texto ya obtenido; en reintento se pide de nuevo y se
// descarta solapamiento buscando la cola conocida al inicio del nuevo texto.
async function fetchCompleteText({ messages, modelId, thinking, deepSearch, signal, logTag }) {
  let full = "";
  let attempts = 0;
  for (let a = 0; a <= MAX_RETRIES; a++) {
    attempts = a + 1;
    let upstream;
    try {
      upstream = await callUpstream({ messages, modelId, stream: true, thinking, deepSearch, signal });
    } catch (e) {
      if (a === MAX_RETRIES) throw e;
      stats.retries++;
      trace(`${logTag} upstream-error attempt=${attempts} err=${e.message} -> retry`);
      await sleep(RETRY_BASE_MS * (a + 1));
      continue;
    }
    let chunk;
    try {
      chunk = await readWorkerSSEOnce(upstream, signal);
    } catch (e) {
      if (a === MAX_RETRIES) throw e;
      stats.retries++;
      trace(`${logTag} read-error attempt=${attempts} err=${e.message} -> retry`);
      await sleep(RETRY_BASE_MS * (a + 1));
      continue;
    }
    if (signal && signal.aborted) return { text: full + dedupeAppend(full, chunk.text), attempts, complete: false, aborted: true };
    if (chunk.done) {
      full = dedupeAppend(full, chunk.text);
      trace(`${logTag} complete attempts=${attempts} chars=${full.length}`);
      return { text: full, attempts, complete: true, aborted: false };
    }
    // Truncado sin DONE.
    stats.truncatedUpstream++;
    full = dedupeAppend(full, chunk.text);
    trace(`${logTag} TRUNCATED attempt=${attempts} charsSoFar=${full.length}`);
    if (a === MAX_RETRIES) {
      trace(`${logTag} retries-exhausted chars=${full.length}`);
      return { text: full, attempts, complete: false, aborted: false };
    }
    stats.retries++;
    await sleep(RETRY_BASE_MS * (a + 1));
  }
  return { text: full, attempts, complete: false, aborted: false };
}

// Pega `add` a `base` evitando duplicar reemisión (el worker reemite
// desde el inicio en cada reintento). Reglas:
//  1. add empieza igual que base -> reemisión: quedarse con el más largo.
//  2. cola de base empalma con inicio de add -> continuación.
//  3. sin relación -> el más largo (evita Frankenstein de dos historias).
function dedupeAppend(base, add) {
  if (!base) return add || "";
  if (!add) return base;
  const headLen = Math.min(200, base.length);
  if (headLen >= 32 && add.startsWith(base.slice(0, headLen))) {
    return add.length > base.length ? add : base;
  }
  const maxOverlap = Math.min(base.length, add.length, 2000);
  for (let n = maxOverlap; n >= 32; n--) {
    if (add.startsWith(base.slice(base.length - n))) return base + add.slice(n);
  }
  return add.length > base.length ? add : base;
}

// ---- handlers ----

function handleModels(res) {
  sendJson(res, 200, {
    object: "list",
    data: MODELS.map((m) => ({ id: m.id, object: "model", created: 0, owned_by: m.owned_by })),
  });
}

function handleHealth(res) {
  sendJson(res, 200, {
    ok: true,
    service: "fmp-provider",
    version: "1.3.0",
    upstream: UPSTREAM,
    models: MODELS.map((m) => m.id),
    uptime_s: Math.floor((Date.now() - startedAt) / 1000),
    stats,
  });
}

async function handleChatCompletions(req, res, body) {
  stats.requests++;
  const model = stripPrefix(body.model);
  if (!MODEL_IDS.has(model)) {
    return sendJson(res, 404, {
      error: { message: `model '${body.model}' not found. Valid: ${[...MODEL_IDS].join(", ")}`, type: "invalid_request_error", code: "model_not_found" },
    });
  }
  const workerMessages = toWorkerMessagesOA(body.messages);
  if (!workerMessages.length) {
    return sendJson(res, 400, { error: { message: "messages required", type: "invalid_request_error", code: "bad_request" } });
  }
  const stream = body.stream === true;
  const thinking = !!body.thinking;
  const deepSearch = !!(body.deepSearch ?? body.deep_search);

  const id = "chatcmpl-" + rid();
  const created = Math.floor(Date.now() / 1000);
  const logTag = `OAI/${model}/${id.slice(-6)}`;

  // Puente tool-use: si el cliente declara tools, pedir EXEC::: al worker.
  const oaiTools = Array.isArray(body.tools) ? body.tools : [];
  const oaiDefs = toolDefsForWorker(oaiTools.map((t) => (t && t.function ? { type: "function", name: t.function.name, description: t.function.description, parameters: t.function.parameters } : t)));
  let wm = workerMessages;
  if (oaiDefs.length && body.tool_choice !== "none") {
    const sys = agentSystemPrompt(oaiTools);
    if (sys) wm = [{ role: "system", content: sys }, ...workerMessages];
  }

  // Obtiene texto íntegro (SSE + retry + fallback JSON). Reutilizado
  // por ramas stream y no-stream.
  async function getOAIText() {
    const r = await fetchCompleteText({ messages: wm, modelId: model, thinking, deepSearch, signal: null, logTag });
    if (r.complete) return { text: r.text, complete: true, finish: "stop" };
    try {
      const ju = await callUpstream({ messages: wm, modelId: model, stream: false, thinking, deepSearch, signal: null });
      const jtext = await readWorkerJson(ju);
      if (jtext.length >= r.text.length && jtext.length > 0) {
        trace(`${logTag} FALLBACK-OK chars=${jtext.length}`);
        return { text: jtext, complete: true, finish: "stop" };
      }
      return { text: r.text, complete: false, finish: "length" };
    } catch (e) {
      trace(`${logTag} FALLBACK-FAIL err=${e.message}`);
      return { text: r.text, complete: false, finish: "length" };
    }
  }

  if (!stream) {
    let got;
    try {
      got = await getOAIText();
    } catch (e) {
      stats.errors++;
      return openaiError(res, e.status && e.status < 500 ? e.status : 502, e.message);
    }
    const tc = oaiDefs.length ? await resolveToolCall(got.text, oaiDefs, wm, model, logTag) : null;
    if (tc) {
      trace(`${logTag} TOOL-CALL name=${tc.name}`);
      return sendJson(res, 200, {
        id, object: "chat.completion", created, model,
        choices: [{
          index: 0,
          message: {
            role: "assistant",
            content: tc.prefixText || null,
            tool_calls: [{ id: "call_" + rid().slice(0, 20), type: "function", function: { name: tc.name, arguments: tc.args } }],
          },
          finish_reason: "tool_calls",
        }],
        usage: { prompt_tokens: estTokens(JSON.stringify(wm)), completion_tokens: estTokens(got.text), total_tokens: 0 },
      });
    }
    return sendJson(res, 200, {
      id, object: "chat.completion", created, model,
      choices: [{ index: 0, message: { role: "assistant", content: got.text }, finish_reason: got.finish }],
      usage: { prompt_tokens: estTokens(JSON.stringify(wm)), completion_tokens: estTokens(got.text), total_tokens: 0 },
    });
  }

  // STREAM: buffer anti-truncamiento primero, emitir después.
  const t0 = Date.now();
  trace(`${logTag} START msgs=${wm.length} tools=${oaiDefs.length}`);
  let result;
  try {
    result = await fetchCompleteText({ messages: wm, modelId: model, thinking, deepSearch, signal: null, logTag });
  } catch (e) {
    stats.errors++;
    // Fallback: error SSE bien formado en vez de colgar.
    res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive", "access-control-allow-origin": "*" });
    const send = (o) => res.write("data: " + JSON.stringify(o) + "\n\n");
    send({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] });
    send({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: {}, finish_reason: "error" }] });
    res.write("data: [DONE]\n\n");
    return res.end();
  }
  // Fallback JSON: el worker devuelve no-stream íntegro y estable.
  // Si el SSE quedó incompleto, siempre pedirlo y quedarse con el mejor.
  let finishReason = result.complete ? "stop" : "length";
  if (!result.complete) {
    try {
      trace(`${logTag} FALLBACK-JSON charsSoFar=${result.text.length}`);
      const ju = await callUpstream({ messages: wm, modelId: model, stream: false, thinking, deepSearch, signal: null });
      const jtext = await readWorkerJson(ju);
      if (jtext.length >= result.text.length && jtext.length > 0) {
        result = { text: jtext, attempts: result.attempts + 1, complete: true, aborted: false };
        finishReason = "stop";
        trace(`${logTag} FALLBACK-OK chars=${jtext.length}`);
      } else {
        trace(`${logTag} FALLBACK-SHORTER json=${jtext.length} keep=${result.text.length}`);
      }
    } catch (e) {
      trace(`${logTag} FALLBACK-FAIL err=${e.message}`);
    }
  }
  // Puente tool-use en stream (centralizado: EXEC -> shell-block -> retry).
  const tcS = oaiDefs.length ? await resolveToolCall(result.text, oaiDefs, wm, model, logTag) : null;
  if (tcS) {
    finishReason = "tool_calls";
    trace(`${logTag} TOOL-CALL name=${tcS.name}`);
  }

  res.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    connection: "keep-alive",
    "access-control-allow-origin": "*",
  });
  const send = (obj) => res.write("data: " + JSON.stringify(obj) + "\n\n");
  let closed = false;
  req.on("close", () => { closed = true; });
  send({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] });
  if (tcS) {
    // Emisión tool_calls: prefijo como texto + llamada completa.
    const tcId = "call_" + rid().slice(0, 20);
    if (tcS.prefixText && !closed) {
      send({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: { content: tcS.prefixText }, finish_reason: null }] });
    }
    if (!closed) {
      send({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: tcId, type: "function", function: { name: tcS.name, arguments: tcS.args } }] }, finish_reason: null }] });
      send({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }] });
      res.write("data: [DONE]\n\n");
    }
    trace(`${logTag} SERVED-FC name=${tcS.name} elapsed=${Date.now() - t0}ms`);
    return res.end();
  }
  // Re-emitir en trozos pequeños para preservar UX de streaming.
  const CH = 24;
  for (let i = 0; i < result.text.length && !closed; i += CH) {
    send({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: { content: result.text.slice(i, i + CH) }, finish_reason: null }] });
  }
  if (!closed) {
    send({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: {}, finish_reason: finishReason }] });
    res.write("data: [DONE]\n\n");
  }
  trace(`${logTag} SERVED chars=${result.text.length} attempts=${result.attempts} complete=${result.complete} elapsed=${Date.now() - t0}ms`);
  res.end();
}

// Responses API (Codex) -> worker. Acepta `input` string, array de
// items {role, content} o messages clásico. Reutiliza pipeline
// anti-truncamiento + fallback JSON.
// Items Responses -> worker. Incluye function_call / function_call_output
// para rondas agénticas (Codex): el modelo ve qué llamó y qué devolvió.
function itemTextOfOutput(output) {
  if (typeof output === "string") return output;
  if (Array.isArray(output)) {
    return output.map((p) => {
      if (typeof p === "string") return p;
      if (p && typeof p === "object") {
        if (typeof p.text === "string") return p.text;
        if (typeof p.output_text === "string") return p.output_text;
        return JSON.stringify(p).slice(0, 4000);
      }
      return "";
    }).join("\n");
  }
  return output == null ? "" : String(output);
}

function toWorkerMessagesResp(input) {
  if (typeof input === "string" && input) return [{ role: "user", content: input }];
  if (Array.isArray(input)) {
    const out = [];
    for (const m of input) {
      if (!m || typeof m !== "object") continue;
      // function_call: el asistente pidió ejecutar una tool.
      if (m.type === "function_call") {
        out.push({ role: "assistant", content: `[llamada a herramienta ${m.name || "tool"}] ${m.arguments || m.call_id || ""}`.trim() });
        continue;
      }
      // function_call_output: resultado de la ejecución (Codex lo ejecuta local).
      if (m.type === "function_call_output") {
        out.push({ role: "user", content: `[resultado de herramienta]\n${itemTextOfOutput(m.output)}` });
        continue;
      }
      // Item Responses: {type:'message', role, content:[{type:'input_text',text}]}
      const role = m.role === "assistant" || m.role === "system" ? m.role : "user";
      const c = m.content;
      let text = "";
      if (typeof c === "string") text = c;
      else if (Array.isArray(c)) {
        text = c.map((p) => {
          if (typeof p === "string") return p;
          if (p && typeof p === "object") {
            if (typeof p.text === "string") return p.text;
            if (typeof p.input_text === "string") return p.input_text;
            return "";
          }
          return "";
        }).join("");
      }
      if (text) out.push({ role, content: text });
    }
    return out;
  }
  return [];
}

// Extrae llamada a herramienta del texto del worker.
// Protocolo: el worker emite bloque
//   EXEC:::<tool> ARGS <json>
// y el puente lo convierte a function_call nativo.
function parseToolCall(text, availTools) {
  if (!text || !Array.isArray(availTools) || !availTools.length) return null;
  const i = text.indexOf("EXEC:::");
  if (i < 0) return null;
  const rest = text.slice(i + 7).trim().split("\n")[0].trim();
  const m = rest.match(/^([A-Za-z0-9_.-]+)\s+ARGS\s+(\{[\s\S]*\})\s*$/);
  if (!m) return null;
  const [, name, argsJson] = m;
  const tool = availTools.find((t) => t.name === name);
  if (!tool) return null;
  let args = argsJson;
  try { JSON.parse(argsJson); } catch { return null; }
  const t = text.slice(0, i).trim();
  return { name, args, prefixText: t };
}

// Fallback agéntico: si el worker no emitió EXEC::: pero el texto trae
// un bloque shell (```bash ... ```), convertir el primero a exec_command.
// Codex pide aprobación al usuario antes de ejecutar, no hay ejecución ciega.
function extractShellBlock(text) {
  if (!text) return null;
  const m = text.match(/```(?:bash|sh|shell|zsh|cmd|powershell|console|terminal)?\s*\n([\s\S]*?)```/);
  if (!m) return null;
  const cmd = m[1].trim();
  if (!cmd || cmd.length > 2000 || cmd.includes("\n\n\n")) return null;
  // Evitar bloques que son claramente ilustrativos múltiples: solo el primero.
  return cmd;
}

// Resuelve llamada a herramienta con 3 niveles:
// 1. EXEC::: explícito. 2. bloque shell -> exec_command.
// 3. reintento dirigido que pide SOLO el bloque.
async function resolveToolCall(text, defs, wm, modelId, logTag) {
  let tc = parseToolCall(text, defs);
  if (tc) return tc;
  const shellCmd = extractShellBlock(text);
  const execTool = (defs || []).find((t) => t.name === "exec_command");
  if (shellCmd && execTool) {
    const i = text.indexOf("```");
    trace(`${logTag} SHELL-BLOCK-CALL cmd=${shellCmd.slice(0, 200)}`);
    return { name: "exec_command", args: JSON.stringify({ cmd: shellCmd }), prefixText: text.slice(0, i).trim() };
  }
  if (defs && defs.length && wm && modelId) {
    try {
      const names = defs.map((t) => t.name).slice(0, 8).join(", ");
      const strictMsgs = [
        ...wm,
        { role: "user", content: `Responde SOLO con el bloque exacto (sin otro texto, sin explicaciones, sin mencionar el entorno ni permisos): EXEC:::<una de: ${names}> ARGS <json>. Si no corresponde usar herramientas, responde SIN bloque.` },
      ];
      trace(`${logTag} FC-RETRY tools=${names}`);
      const r2 = await fetchCompleteText({ messages: strictMsgs, modelId, thinking: false, deepSearch: false, signal: null, logTag: logTag + "/fc2" });
      const tc2 = parseToolCall(r2.text, defs);
      if (tc2) {
        trace(`${logTag} FC-RETRY-OK name=${tc2.name}`);
        return tc2;
      }
      trace(`${logTag} FC-RETRY-MISS chars=${r2.text.length}`);
    } catch (e) {
      trace(`${logTag} FC-RETRY-FAIL err=${e.message}`);
    }
  }
  return null;
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
    .slice(0, 25);
}

function agentSystemPrompt(tools) {
  const defs = toolDefsForWorker(tools);
  if (!defs.length) return "";
  const lines = defs.map((d) => `- ${d.name}: ${d.description} params=${JSON.stringify(d.params).slice(0, 800)}`);
  return [
    "Eres un agente con herramientas. PUEDES y DEBES ejecutar comandos cuando el usuario lo pida.",
    "Para actuar, responde PRIMERO un texto breve y LUEGO en línea propia el bloque:",
    "EXEC:::<nombre_herramienta> ARGS <json con los parámetros>",
    "Usa SOLO estas herramientas:",
    ...lines,
    "Para comandos shell usa exec_command con {\"cmd\": \"...\"}. Si no necesitas herramientas, responde normal sin bloque EXEC.",
  ].join("\n");
}

async function resolveResponsesText(body, workerMessages, model, logTag) {
  const thinking = !!body.thinking;
  let result;
  try {
    result = await fetchCompleteText({ messages: workerMessages, modelId: model, thinking, deepSearch: false, signal: null, logTag });
  } catch (e) {
    stats.errors++;
    throw e;
  }
  let status = result.complete ? "completed" : "incomplete";
  if (!result.complete) {
    try {
      trace(`${logTag} FALLBACK-JSON charsSoFar=${result.text.length}`);
      const ju = await callUpstream({ messages: workerMessages, modelId: model, stream: false, thinking, deepSearch: false, signal: null });
      const jtext = await readWorkerJson(ju);
      if (jtext.length >= result.text.length && jtext.length > 0) {
        result = { text: jtext, attempts: result.attempts + 1, complete: true, aborted: false };
        status = "completed";
        trace(`${logTag} FALLBACK-OK chars=${jtext.length}`);
      } else {
        trace(`${logTag} FALLBACK-SHORTER json=${jtext.length} keep=${result.text.length}`);
      }
    } catch (e) {
      trace(`${logTag} FALLBACK-FAIL err=${e.message}`);
    }
  }
  return { result, status };
}

async function handleResponses(req, res, body) {
  stats.requests++;
  try {
    const keys = Object.keys(body || {});
    const nTools = Array.isArray(body.tools) ? body.tools.length : 0;
    const nInput = Array.isArray(body.input) ? body.input.length : (typeof body.input);
    trace(`RSP-REQ keys=${keys.join(",")} tools=${nTools} input=${nInput} stream=${body.stream} tool_choice=${JSON.stringify(body.tool_choice || null).slice(0, 80)}`);
    try {
      const tn = (body.tools || []).map((t) => t.name || t.type).slice(0, 25);
      trace(`RSP-TOOLS ${JSON.stringify(tn)}`);
      const it = (body.input || []).map((m) => m.type || "?").slice(0, 10);
      trace(`RSP-INPUT-TYPES ${JSON.stringify(it)}`);
    } catch {}
  } catch {}
  const model = stripPrefix(body.model);
  if (!MODEL_IDS.has(model)) {
    return sendJson(res, 404, {
      error: { message: `model '${body.model}' not found. Valid: ${[...MODEL_IDS].join(", ")}`, type: "invalid_request_error", code: "model_not_found" },
    });
  }
  let workerMessages = toWorkerMessagesResp(body.input);
  if (!workerMessages.length && Array.isArray(body.messages)) workerMessages = toWorkerMessagesOA(body.messages);
  if (typeof body.instructions === "string" && body.instructions) {
    workerMessages = [{ role: "system", content: body.instructions }, ...workerMessages];
  }
  if (!workerMessages.length) {
    return sendJson(res, 400, { error: { message: "input (string|array) or messages required", type: "invalid_request_error", code: "bad_request" } });
  }
  const stream = body.stream === true;
  const respId = "resp_" + rid();
  const created = Math.floor(Date.now() / 1000);
  const logTag = `RSP/${model}/${respId.slice(-6)}`;
  const inputTokens = estTokens(JSON.stringify(workerMessages));

  const t0 = Date.now();
  trace(`${logTag} START stream=${stream}`);
  // Modo agéntico: si el cliente ofrece tools tipo function, inyectar
  // catálogo como prompt sistema para que el worker emita EXEC::: y
  // convertirlo a function_call nativo.
  const fnTools = Array.isArray(body.tools) ? body.tools.filter((t) => t && t.type === "function" && t.name) : [];
  if (fnTools.length && body.tool_choice !== "none") {
    const sys = agentSystemPrompt(body.tools);
    if (sys) workerMessages = [{ role: "system", content: sys }, ...workerMessages];
  }
  let resolved;
  try {
    resolved = await resolveResponsesText(body, workerMessages, model, logTag);
  } catch (e) {
    return openaiError(res, 502, e.message);
  }
  const { result, status } = resolved;
  const outputTokens = estTokens(result.text);
  // Puente agéntico centralizado (EXEC -> shell-block -> reintento).
  const toolCall = fnTools.length ? await resolveToolCall(result.text, fnTools, workerMessages, model, logTag) : null;
  if (toolCall) trace(`${logTag} TOOL-CALL name=${toolCall.name} args=${toolCall.args.slice(0, 200)}`);
  let outputItems;
  let fcItem = null;
  if (toolCall) {
    const msgPart = toolCall.prefixText
      ? [{ type: "output_text", text: toolCall.prefixText, annotations: [] }]
      : [];
    const msgItem = {
      type: "message", id: "msg_" + rid(), status: "completed",
      role: "assistant", content: msgPart,
    };
    fcItem = {
      type: "function_call", id: "fc_" + rid(), call_id: "call_" + rid().slice(0, 20),
      status: "completed", name: toolCall.name, arguments: toolCall.args,
    };
    outputItems = msgPart.length ? [msgItem, fcItem] : [fcItem];
    trace(`${logTag} TOOL-CALL name=${toolCall.name} args=${toolCall.args.slice(0, 200)}`);
  } else {
    outputItems = [{
      type: "message", id: "msg_" + rid(), status: status === "completed" ? "completed" : "incomplete",
      role: "assistant", content: [{ type: "output_text", text: result.text, annotations: [] }],
    }];
  }
  const outputItem = outputItems[outputItems.length - 1];

  if (!stream) {
    trace(`${logTag} SERVED chars=${result.text.length} status=${status} toolCall=${toolCall ? toolCall.name : "-"} elapsed=${Date.now() - t0}ms`);
    return sendJson(res, 200, {
      id: respId, object: "response", created, model,
      status, output: outputItems, parallel_tool_calls: true, tool_choice: "auto", tools: [],
      usage: { input_tokens: inputTokens, output_tokens: outputTokens, total_tokens: inputTokens + outputTokens },
    });
  }

  res.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    connection: "keep-alive",
    "access-control-allow-origin": "*",
  });
  const send = (ev, d) => res.write("event: " + ev + "\ndata: " + JSON.stringify(d) + "\n\n");
  let closed = false;
  req.on("close", () => { closed = true; });
  if (fcItem) {
    // Secuencia Responses para function_call.
    if (!closed) {
      send("response.created", { type: "response.created", response: { id: respId, object: "response", created, model, status: "in_progress", output: [] } });
      let oi = 0;
      for (const it of outputItems) {
        if (it.type === "message") {
          send("response.output_item.added", { type: "response.output_item.added", output_index: oi, item: { type: "message", id: it.id, status: "in_progress", role: "assistant", content: [] } });
          if (it.content.length) {
            send("response.content_part.added", { type: "response.content_part.added", item_id: it.id, output_index: oi, content_index: 0, part: { type: "output_text", text: "", annotations: [] } });
            send("response.output_text.delta", { type: "response.output_text.delta", item_id: it.id, output_index: oi, content_index: 0, delta: it.content[0].text, logprobs: [] });
            send("response.output_text.done", { type: "response.output_text.done", item_id: it.id, output_index: oi, content_index: 0, text: it.content[0].text, logprobs: [] });
            send("response.content_part.done", { type: "response.content_part.done", item_id: it.id, output_index: oi, content_index: 0, part: it.content[0] });
          }
          send("response.output_item.done", { type: "response.output_item.done", output_index: oi, item: it });
          oi++;
        } else {
          send("response.output_item.added", { type: "response.output_item.added", output_index: oi, item: { type: "function_call", id: it.id, status: "in_progress", name: it.name, arguments: "", call_id: it.call_id } });
          const A = it.arguments;
          for (let k = 0; k < A.length && !closed; k += 48) {
            send("response.function_call_arguments.delta", { type: "response.function_call_arguments.delta", item_id: it.id, output_index: oi, delta: A.slice(k, k + 48) });
          }
          send("response.function_call_arguments.done", { type: "response.function_call_arguments.done", item_id: it.id, output_index: oi, arguments: A });
          send("response.output_item.done", { type: "response.output_item.done", output_index: oi, item: it });
          oi++;
        }
      }
      send("response.completed", {
        type: "response.completed",
        response: {
          id: respId, object: "response", created, model, status,
          output: outputItems, parallel_tool_calls: true, tool_choice: "auto", tools: [],
          usage: { input_tokens: inputTokens, output_tokens: outputTokens, total_tokens: inputTokens + outputTokens },
        },
      });
    }
    trace(`${logTag} SERVED-FC name=${fcItem.name} elapsed=${Date.now() - t0}ms`);
    return res.end();
  }
  if (!closed) {
    send("response.created", { type: "response.created", response: { id: respId, object: "response", created, model, status: "in_progress", output: [] } });
    send("response.output_item.added", { type: "response.output_item.added", output_index: 0, item: { type: "message", id: outputItem.id, status: "in_progress", role: "assistant", content: [] } });
    send("response.content_part.added", { type: "response.content_part.added", item_id: outputItem.id, output_index: 0, content_index: 0, part: { type: "output_text", text: "", annotations: [] } });
  }
  const CH = 24;
  for (let i = 0; i < result.text.length && !closed; i += CH) {
    send("response.output_text.delta", { type: "response.output_text.delta", item_id: outputItem.id, output_index: 0, content_index: 0, delta: result.text.slice(i, i + CH), logprobs: [] });
  }
  if (!closed) {
    send("response.output_text.done", { type: "response.output_text.done", item_id: outputItem.id, output_index: 0, content_index: 0, text: result.text, logprobs: [] });
    send("response.content_part.done", { type: "response.content_part.done", item_id: outputItem.id, output_index: 0, content_index: 0, part: { type: "output_text", text: result.text, annotations: [] } });
    send("response.output_item.done", { type: "response.output_item.done", output_index: 0, item: outputItem });
    send("response.completed", {
      type: "response.completed",
      response: {
        id: respId, object: "response", created, model, status,
        output: outputItems, parallel_tool_calls: true, tool_choice: "auto", tools: [],
        usage: { input_tokens: inputTokens, output_tokens: outputTokens, total_tokens: inputTokens + outputTokens },
      },
    });
  }
  trace(`${logTag} SERVED chars=${result.text.length} status=${status} elapsed=${Date.now() - t0}ms`);
  res.end();
}

async function handleMessages(req, res, body) {
  stats.requests++;
  const model = stripPrefix(body.model);
  if (!MODEL_IDS.has(model)) {
    return anthropicError(res, 404, `model '${body.model}' not found. Valid: ${[...MODEL_IDS].join(", ")}`);
  }
  const maxTokens = Number(body.max_tokens) || 1024;
  const workerMessages = toWorkerMessagesAnt(body.system, body.messages);
  if (!workerMessages.length) return anthropicError(res, 400, "messages required");
  const stream = body.stream === true;

  const msgId = "msg_" + rid();
  const inputTokens = estTokens(JSON.stringify(workerMessages));
  const logTag = `ANT/${model}/${msgId.slice(-6)}`;
  void maxTokens;

  // Puente tool-use Anthropic: tools [{name, description, input_schema}].
  const antTools = Array.isArray(body.tools) ? body.tools : [];
  const antDefs = toolDefsForWorker(antTools.map((t) => ({ type: "function", name: t.name, description: t.description, parameters: t.input_schema })));
  let wmAnt = workerMessages;
  const antChoice = body.tool_choice;
  const antDisabled = antChoice && (antChoice.type === "none" || antChoice === "none");
  if (antDefs.length && !antDisabled) {
    const sys = agentSystemPrompt(antTools);
    if (sys) wmAnt = [{ role: "system", content: sys }, ...workerMessages];
  }

  const t0 = Date.now();
  trace(`${logTag} START msgs=${wmAnt.length} stream=${stream} tools=${antDefs.length}`);
  let result;
  try {
    result = await fetchCompleteText({ messages: wmAnt, modelId: model, thinking: false, deepSearch: false, signal: null, logTag });
  } catch (e) {
    stats.errors++;
    return anthropicError(res, 502, e.message);
  }
  // Fallback JSON si el SSE quedó incompleto (mismo criterio que ruta OpenAI).
  let antStop = result.complete ? "end_turn" : "max_tokens";
  if (!result.complete) {
    try {
      trace(`${logTag} FALLBACK-JSON charsSoFar=${result.text.length}`);
      const ju = await callUpstream({ messages: wmAnt, modelId: model, stream: false, thinking: false, deepSearch: false, signal: null });
      const jtext = await readWorkerJson(ju);
      if (jtext.length >= result.text.length && jtext.length > 0) {
        result = { text: jtext, attempts: result.attempts + 1, complete: true, aborted: false };
        antStop = "end_turn";
        trace(`${logTag} FALLBACK-OK chars=${jtext.length}`);
      } else {
        trace(`${logTag} FALLBACK-SHORTER json=${jtext.length} keep=${result.text.length}`);
      }
    } catch (e) {
      trace(`${logTag} FALLBACK-FAIL err=${e.message}`);
    }
  }
  // Puente: EXEC/shell-block/retry centralizado.
  const tcAnt = antDefs.length && !antDisabled ? await resolveToolCall(result.text, antDefs, wmAnt, model, logTag) : null;
  let antContent;
  if (tcAnt) {
    let input = {};
    try { input = JSON.parse(tcAnt.args); } catch { input = {}; }
    antContent = [];
    if (tcAnt.prefixText) antContent.push({ type: "text", text: tcAnt.prefixText });
    antContent.push({ type: "tool_use", id: "toolu_" + rid().slice(0, 20), name: tcAnt.name, input });
    antStop = "tool_use";
    trace(`${logTag} TOOL-CALL name=${tcAnt.name}`);
  } else {
    antContent = [{ type: "text", text: result.text }];
  }

  if (!stream) {
    return sendJson(res, 200, {
      id: msgId, type: "message", role: "assistant", model,
      content: antContent,
      stop_reason: antStop,
      stop_sequence: null,
      usage: { input_tokens: inputTokens, output_tokens: estTokens(result.text) },
    });
  }

  res.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    connection: "keep-alive",
    "access-control-allow-origin": "*",
  });
  const send = (obj) => res.write("event: " + obj._ev + "\ndata: " + JSON.stringify(obj._d) + "\n\n");
  let closed = false;
  req.on("close", () => { closed = true; });
  const ev = (name, d) => ({ _ev: name, _d: d });
  if (!closed) {
    send(ev("message_start", { type: "message_start", message: { id: msgId, type: "message", role: "assistant", model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: inputTokens, output_tokens: 1 } } }));
    let si = 0;
    for (const b of antContent) {
      if (b.type === "text") {
        send(ev("content_block_start", { type: "content_block_start", index: si, content_block: { type: "text", text: "" } }));
      } else {
        send(ev("content_block_start", { type: "content_block_start", index: si, content_block: { type: "tool_use", id: b.id, name: b.name, input: {} } }));
      }
      si++;
    }
  }
  let bi = 0;
  for (const b of antContent) {
    if (closed) break;
    if (b.type === "text") {
      const CH = 24;
      for (let i = 0; i < b.text.length && !closed; i += CH) {
        send(ev("content_block_delta", { type: "content_block_delta", index: bi, delta: { type: "text_delta", text: b.text.slice(i, i + CH) } }));
      }
      if (!closed) send(ev("content_block_stop", { type: "content_block_stop", index: bi }));
    } else {
      if (!closed) {
        send(ev("content_block_delta", { type: "content_block_delta", index: bi, delta: { type: "input_json_delta", partial_json: JSON.stringify(b.input) } }));
        send(ev("content_block_stop", { type: "content_block_stop", index: bi }));
      }
    }
    bi++;
  }
  if (!closed) {
    send(ev("message_delta", { type: "message_delta", delta: { stop_reason: antStop, stop_sequence: null }, usage: { output_tokens: estTokens(result.text) } }));
    send(ev("message_stop", { type: "message_stop" }));
  }
  trace(`${logTag} SERVED chars=${result.text.length} attempts=${result.attempts} complete=${result.complete} elapsed=${Date.now() - t0}ms`);
  res.end();
}

const server = http.createServer(async (req, res) => {
  try {
    const u = new URL(req.url || "/", "http://x");
    const path = u.pathname.replace(/\/+$/, "") || "/";

    if (req.method === "OPTIONS") {
      res.writeHead(204, { "access-control-allow-origin": "*", "access-control-allow-methods": "GET,POST,OPTIONS", "access-control-allow-headers": "content-type,authorization,x-api-key,anthropic-version" });
      return res.end();
    }
    if (req.method === "GET" && (path === "/health" || path === "/")) return handleHealth(res);
    if (req.method === "GET" && (path === "/v1/models" || path === "/models")) return handleModels(res);

    if (req.method === "POST" && (path === "/v1/chat/completions" || path === "/chat/completions")) {
      let body;
      try { body = JSON.parse(await readBody(req)); }
      catch { return sendJson(res, 400, { error: { message: "invalid JSON body", type: "invalid_request_error", code: "bad_request" } }); }
      return handleChatCompletions(req, res, body);
    }
    if (req.method === "POST" && (path === "/v1/messages" || path === "/messages")) {
      let body;
      try { body = JSON.parse(await readBody(req)); }
      catch { return anthropicError(res, 400, "invalid JSON body"); }
      return handleMessages(req, res, body);
    }
    if (req.method === "POST" && (path === "/v1/responses" || path === "/responses")) {
      let body;
      try { body = JSON.parse(await readBody(req)); }
      catch { return sendJson(res, 400, { error: { message: "invalid JSON body", type: "invalid_request_error", code: "bad_request" } }); }
      return handleResponses(req, res, body);
    }
    return sendJson(res, 404, { error: "not found: " + path });
  } catch (e) {
    try { sendJson(res, 500, { error: { message: e.message || "internal error" } }); } catch {}
  }
});

server.listen(PORT, HOST, () => {
  console.log(`[fmp-provider] listening on http://${HOST}:${PORT}`);
});
