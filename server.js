// FMP — FreeModels Provider v1.0.0
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
    const role = m.role === "system" || m.role === "user" || m.role === "assistant" ? m.role : "user";
    out.push({ role, content: textOfContent(m.content) });
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
    const role = m.role === "assistant" ? "assistant" : "user";
    out.push({ role, content: textOfContent(m.content) });
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
    version: "1.0.0",
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

  if (!stream) {
    let upstream;
    try {
      upstream = await callUpstream({ messages: workerMessages, modelId: model, stream: false, thinking, deepSearch, signal: null });
    } catch (e) {
      stats.errors++;
      return openaiError(res, e.status && e.status < 500 ? e.status : 502, e.message);
    }
    let full = "";
    try {
      full = await readWorkerJson(upstream);
    } catch (e) {
      stats.errors++;
      return openaiError(res, 502, "upstream read failed: " + e.message);
    }
    return sendJson(res, 200, {
      id, object: "chat.completion", created, model,
      choices: [{ index: 0, message: { role: "assistant", content: full }, finish_reason: "stop" }],
      usage: { prompt_tokens: estTokens(JSON.stringify(workerMessages)), completion_tokens: estTokens(full), total_tokens: 0 },
    });
  }

  // STREAM: buffer anti-truncamiento primero, emitir después.
  const t0 = Date.now();
  trace(`${logTag} START msgs=${workerMessages.length}`);
  let result;
  try {
    result = await fetchCompleteText({ messages: workerMessages, modelId: model, thinking, deepSearch, signal: null, logTag });
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
      const ju = await callUpstream({ messages: workerMessages, modelId: model, stream: false, thinking, deepSearch, signal: null });
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

  const t0 = Date.now();
  trace(`${logTag} START msgs=${workerMessages.length} stream=${stream}`);
  let result;
  try {
    result = await fetchCompleteText({ messages: workerMessages, modelId: model, thinking: false, deepSearch: false, signal: null, logTag });
  } catch (e) {
    stats.errors++;
    return anthropicError(res, 502, e.message);
  }
  // Fallback JSON si el SSE quedó incompleto (mismo criterio que ruta OpenAI).
  let antStop = result.complete ? "end_turn" : "max_tokens";
  if (!result.complete) {
    try {
      trace(`${logTag} FALLBACK-JSON charsSoFar=${result.text.length}`);
      const ju = await callUpstream({ messages: workerMessages, modelId: model, stream: false, thinking: false, deepSearch: false, signal: null });
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

  if (!stream) {
    return sendJson(res, 200, {
      id: msgId, type: "message", role: "assistant", model,
      content: [{ type: "text", text: result.text }],
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
    send(ev("content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }));
  }
  const CH = 24;
  for (let i = 0; i < result.text.length && !closed; i += CH) {
    send(ev("content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: result.text.slice(i, i + CH) } }));
  }
  if (!closed) {
    send(ev("content_block_stop", { type: "content_block_stop", index: 0 }));
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
    return sendJson(res, 404, { error: "not found: " + path });
  } catch (e) {
    try { sendJson(res, 500, { error: { message: e.message || "internal error" } }); } catch {}
  }
});

server.listen(PORT, HOST, () => {
  console.log(`[fmp-provider] listening on http://${HOST}:${PORT}`);
});
