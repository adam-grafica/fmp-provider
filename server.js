// FMP — FreeModels Provider v2.1.0
// Traductor OpenAI-compatible + Anthropic-compatible hacia freemodels.pro.
// Sin dependencias. Loopback-only por defecto (127.0.0.1:2089).
//
// Diseño v2 (2026-10-09, evidencia en mano):
// - El worker CORTA el SSE intermitente sin `data: [DONE]`. Estrategia:
//   LIVE pass-through: cada fragmento se reemite al cliente en cuanto
//   llega (TTFB ~1s, streaming visible real). Si el socket muere sin DONE
//   y hay tools pendientes, se marca y reintenta en background; el texto
//   ya emitido se conserva (dedupe por prefijo en reintento).
// - El worker NO tiene canal thinking separado: todo es texto corrido.
//   FMP NO fabrica thinking falso. `thinking:true` se reenvía al worker
//   (respuestas con razonamiento paso a paso en el texto) y en Anthropic
//   se expone `thinking` solo como flag informativo en /health.
// - Plan premium: tiers por API key (free/pro), rate-limit, cola con
//   prioridad, métricas por tier en /health y /v1/usage.
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

// ---- Plan premium: tiers, rate-limit, cola ----
// Tiers por API key (Authorization: Bearer <key> o x-api-key).
// FMP_KEYS="key1:tier,key2:tier" ej: "sk-lan:pro,sk-casa:free".
// Sin key configurada: todo pasa como free sin límite (modo local).
const TIER_LIMITS = {
  free: { rpm: Number(process.env.FMP_FREE_RPM || 10), concurrency: Number(process.env.FMP_FREE_CONC || 2) },
  pro: { rpm: Number(process.env.FMP_PRO_RPM || 120), concurrency: Number(process.env.FMP_PRO_CONC || 16) },
};
const KEY_TIERS = {};
for (const pair of String(process.env.FMP_KEYS || "").split(",").map((s) => s.trim()).filter(Boolean)) {
  const [k, t] = pair.split(":");
  if (k) KEY_TIERS[k] = t === "pro" ? "pro" : "free";
}
const tierState = {
  free: { inFlight: 0, hits: [] },
  pro: { inFlight: 0, hits: [] },
};
const usage = { free: { requests: 0, tokens: 0, errors: 0 }, pro: { requests: 0, tokens: 0, errors: 0 }, local: { requests: 0, tokens: 0, errors: 0 } };

function tierOf(req) {
  const h = req.headers["authorization"] || req.headers["x-api-key"] || "";
  const key = String(h).replace(/^bearer\s+/i, "").trim();
  if (!key) return "local";
  return KEY_TIERS[key] || "local";
}

function checkLimit(tier) {
  if (tier === "local") return null;
  const lim = TIER_LIMITS[tier];
  const st = tierState[tier];
  const now = Date.now();
  st.hits = st.hits.filter((t) => now - t < 60_000);
  if (st.hits.length >= lim.rpm) return { retryAfter: 60 - Math.floor((now - st.hits[0]) / 1000), reason: "rpm" };
  if (st.inFlight >= lim.concurrency) return { retryAfter: 2, reason: "concurrency" };
  return null;
}

function takeSlot(tier) {
  if (tier === "local") return () => {};
  const st = tierState[tier];
  st.hits.push(Date.now());
  st.inFlight++;
  usage[tier].requests++;
  return () => { st.inFlight = Math.max(0, st.inFlight - 1); };
}

function limitResponse(res, info) {
  res.writeHead(429, {
    "content-type": "application/json",
    "retry-after": String(info.retryAfter || 5),
    "access-control-allow-origin": "*",
  });
  res.end(JSON.stringify({ error: { message: `rate limit (${info.reason}), retry in ${info.retryAfter || 5}s — plan premium avaliable`, type: "rate_limit_error", code: "rate_limited" } }));
}

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

// Límite profesional: el worker ignora max_tokens, así que FMP trunca.
// ~4 chars por token. Devuelve { text, hit }.
function applyMaxTokens(text, maxTokens) {
  const cap = Number(maxTokens);
  if (!cap || cap <= 0 || !text) return { text: text || "", hit: false };
  const maxChars = Math.floor(cap * 4);
  if (text.length <= maxChars) return { text, hit: false };
  return { text: text.slice(0, maxChars), hit: true };
}

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

// Lee UN intento SSE del worker EN VIVO: cada fragmento se entrega a
// onPiece en cuanto llega (streaming real, TTFB ~1s). Devuelve
// { text, done }. done=true solo si se vio `data: [DONE]`.
async function readWorkerSSELive(response, onPiece, signal) {
  const reader = response.body.getReader();
  const dec = new TextDecoder("utf-8");
  let buf = "";
  let full = "";
  let done = false;
  const emitPayload = async (payload) => {
    if (!payload) return;
    if (payload === "[DONE]") { done = true; return; }
    try {
      const j = JSON.parse(payload);
      const c = j.choices && j.choices[0];
      const piece = (c && c.delta && c.delta.content) || (c && c.message && c.message.content) || "";
      if (piece) {
        full += piece;
        if (onPiece) await onPiece(piece);
      }
    } catch {}
  };
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
      await emitPayload(t.slice(5).trim());
    }
  }
  const tail = (buf + dec.decode()).trim();
  if (tail) {
    for (const chunk of tail.split("\n")) {
      const t = chunk.trim();
      if (!t || !t.startsWith("data:")) continue;
      await emitPayload(t.slice(5).trim());
    }
  }
  return { text: full, done };
}

// Lee UN intento SSE del worker. Devuelve { text, done }.
// done=true solo si se vio `data: [DONE]`. Sin DONE = truncado.
async function readWorkerSSEOnce(response, signal) {
  return readWorkerSSELive(response, null, signal);
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
    version: "2.1.0",
    upstream: UPSTREAM,
    models: MODELS.map((m) => m.id),
    uptime_s: Math.floor((Date.now() - startedAt) / 1000),
    stats,
    tiers: {
      free: { ...TIER_LIMITS.free, inFlight: tierState.free.inFlight, usage: usage.free },
      pro: { ...TIER_LIMITS.pro, inFlight: tierState.pro.inFlight, usage: usage.pro },
      local: { usage: usage.local },
    },
    // Honestidad: el worker NO expone canal thinking separado; thinking:true
    // se reenvía y el modelo razona en el texto. Sin bloques fabricados.
    thinking: { mode: "worker-passthrough", separate_channel: false },
  });
}

function handleUsage(res) {
  sendJson(res, 200, { object: "usage", usage, tiers: TIER_LIMITS, uptime_s: Math.floor((Date.now() - startedAt) / 1000) });
}

async function handleChatCompletions(req, res, body) {
  stats.requests++;
  const tier = tierOf(req);
  const limited = checkLimit(tier);
  if (limited) { usage[tier].errors++; return limitResponse(res, limited); }
  const release = takeSlot(tier);
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
  // Límite profesional: el worker ignora max_tokens; FMP trunca y avisa
  // con finish length (igual que API nativa). Default 4096.
  const maxTokens = Number(body.max_tokens ?? body.max_completion_tokens) || 4096;
  // stream_mode full: bufferiza íntegro y luego emite SSE (para rachas
  // malas de upstream). Default live, como API nativa.
  const fullMode = body.stream_mode === "full";

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
      usage[tier].errors++;
      release();
      return openaiError(res, e.status && e.status < 500 ? e.status : 502, e.message);
    }
    usage[tier].tokens += estTokens(got.text);
    const capped = applyMaxTokens(got.text, maxTokens);
    if (capped.hit) trace(`${logTag} MAXTOKENS cut=${got.text.length}->${capped.text.length}`);
    got = { text: capped.text, complete: got.complete, finish: capped.hit ? "length" : got.finish };
    const tc = oaiDefs.length ? await resolveToolCall(got.text, oaiDefs, wm, model, logTag) : null;
    if (tc) {
      trace(`${logTag} TOOL-CALL name=${tc.name}`);
      release();
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
    release();
    return sendJson(res, 200, {
      id, object: "chat.completion", created, model,
      choices: [{ index: 0, message: { role: "assistant", content: got.text }, finish_reason: got.finish }],
      usage: { prompt_tokens: estTokens(JSON.stringify(wm)), completion_tokens: estTokens(got.text), total_tokens: 0 },
    });
  }

  // STREAM EN VIVO: pass-through inmediato + resume si el worker corta.
  // Sin tools: cada fragmento se reemite al llegar (TTFB ~1s, como API
  // nativa). Si el socket muere sin DONE, se reintenta y solo se emite
  // lo NUEVO (dedupe por prefijo); el cliente ve pausa breve, no corte.
  // stream_mode full: bufferiza íntegro vía getOAIText y luego emite SSE
  // (para rachas malas de upstream o clientes que prefieren completo).
  const t0 = Date.now();
  trace(`${logTag} START-LIVE msgs=${wm.length} tools=${oaiDefs.length} full=${fullMode}`);
  if (fullMode) {
    let got;
    try {
      got = await getOAIText();
    } catch (e) {
      stats.errors++;
      usage[tier].errors++;
      release();
      return openaiError(res, e.status && e.status < 500 ? e.status : 502, e.message);
    }
    const capped = applyMaxTokens(got.text, maxTokens);
    if (capped.hit) trace(`${logTag} MAXTOKENS cut=${got.text.length}->${capped.text.length}`);
    const ftext = capped.text;
    const ffinish = capped.hit ? "length" : got.finish;
    usage[tier].tokens += estTokens(ftext);
    res.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
      "access-control-allow-origin": "*",
    });
    const fsend = (obj) => res.write("data: " + JSON.stringify(obj) + "\n\n");
    fsend({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] });
    const CH = 48;
    for (let i = 0; i < ftext.length; i += CH) {
      fsend({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: { content: ftext.slice(i, i + CH) }, finish_reason: null }] });
    }
    fsend({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: {}, finish_reason: ffinish }] });
    res.write("data: [DONE]\n\n");
    trace(`${logTag} SERVED-FULL chars=${ftext.length} finish=${ffinish} elapsed=${Date.now() - t0}ms`);
    release();
    return res.end();
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

  let emitted = "";
  let complete = false;
  let attempts = 0;
  let cappedHit = false;
  const maxChars = Math.floor(maxTokens * 4);
  const ctrl = new AbortController();
  req.on("close", () => { try { ctrl.abort(); } catch {} });
  const emitNovel = (novel) => {
    if (!novel || closed) return;
    let piece = novel;
    if (emitted.length + piece.length > maxChars) {
      piece = piece.slice(0, Math.max(0, maxChars - emitted.length));
      cappedHit = true;
    }
    if (piece) {
      emitted += piece;
      send({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: { content: piece }, finish_reason: null }] });
    }
    if (cappedHit) { try { ctrl.abort(); } catch {} }
  };
  for (let a = 0; a <= MAX_RETRIES && !closed; a++) {
    attempts = a + 1;
    let upstream;
    try {
      upstream = await callUpstream({ messages: wm, modelId: model, stream: true, thinking, deepSearch, signal: ctrl.signal });
    } catch (e) {
      if (emitted || a === MAX_RETRIES) {
        if (!closed && !emitted) send({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: {}, finish_reason: "error" }] });
        break;
      }
      stats.retries++;
      trace(`${logTag} LIVE upstream-error attempt=${attempts} err=${e.message}`);
      await sleep(RETRY_BASE_MS * (a + 1));
      continue;
    }
    let chunk;
    let diverged = false;
    try {
      // Emite solo lo NUEVO. Si el reintento regenera otra historia
      // (sin solapamiento con lo emitido), NO se pega Frankenstein:
      // se cierra honesto con length.
      let fresh = "";
      chunk = await readWorkerSSELive(upstream, async (piece) => {
        if (closed || diverged) return;
        fresh += piece;
        const merged = dedupeAppend(emitted, fresh);
        if (!merged.startsWith(emitted)) { diverged = true; return; }
        const novel = merged.slice(emitted.length);
        if (novel) {
          const before = emitted.length;
          emitNovel(novel);
          fresh = fresh.slice(emitted.length - before) || fresh;
          if (cappedHit) { diverged = true; complete = true; }
        }
      }, ctrl.signal);
      if (diverged) {
        stats.truncatedUpstream++;
        trace(`${logTag} LIVE DIVERGED emitted=${emitted.length} -> close honest`);
        break;
      }
    } catch (e) {
      if (emitted || a === MAX_RETRIES) break;
      stats.retries++;
      trace(`${logTag} LIVE read-error attempt=${attempts} err=${e.message}`);
      await sleep(RETRY_BASE_MS * (a + 1));
      continue;
    }
    if (ctrl.signal.aborted || closed) break;
    if (chunk.done) {
      // Drena resto no emitido (caso borde) y cierra como nativa.
      const rest = dedupeAppend(emitted, chunk.text).slice(emitted.length);
      if (rest && !closed) emitNovel(rest);
      complete = !cappedHit;
      break;
    }
    stats.truncatedUpstream++;
    if (!emitted) {
      // Nada emitido: reintento seguro.
      trace(`${logTag} LIVE TRUNCATED attempt=${attempts} nothing-emitted -> retry`);
      stats.retries++;
      if (a < MAX_RETRIES) await sleep(RETRY_BASE_MS * (a + 1));
      continue;
    }
    trace(`${logTag} LIVE TRUNCATED attempt=${attempts} emitted=${emitted.length} -> close honest`);
    break;
  }
  if (!closed) {
    // Sin rescate por continuación: el worker reescribe el borde en vez
    // de continuar (evidencia), así que cualquier pegado sería
    // Frankenstein. Cierre honesto: complete->stop, corte->length,
    // cap de max_tokens->length (como API nativa).
    let finalReason = cappedHit ? "length" : (complete ? "stop" : "length");
    // Puente tool-use post-stream (solo si el texto final trae EXEC).
    const tcLive = oaiDefs.length ? parseToolCall(emitted, oaiDefs) : null;
    if (tcLive) {
      const tcId = "call_" + rid().slice(0, 20);
      send({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: tcId, type: "function", function: { name: tcLive.name, arguments: tcLive.args } }] }, finish_reason: null }] });
      send({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }] });
      trace(`${logTag} LIVE TOOL-CALL name=${tcLive.name}`);
    } else {
      send({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: {}, finish_reason: finalReason }] });
    }
    res.write("data: [DONE]\n\n");
  }
  usage[tier].tokens += estTokens(emitted);
  trace(`${logTag} SERVED-LIVE chars=${emitted.length} attempts=${attempts} complete=${complete} elapsed=${Date.now() - t0}ms`);
  release();
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
  const tier = tierOf(req);
  const limited = checkLimit(tier);
  if (limited) { usage[tier].errors++; return limitResponse(res, limited); }
  const release = takeSlot(tier);
  body.__tier = tier;
  body.__release = release;
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
  const done = (fn) => { try { body.__release(); } catch {} return fn(); };
  const model = stripPrefix(body.model);
  if (!MODEL_IDS.has(model)) {
    return done(() => sendJson(res, 404, {
      error: { message: `model '${body.model}' not found. Valid: ${[...MODEL_IDS].join(", ")}`, type: "invalid_request_error", code: "model_not_found" },
    }));
  }
  let workerMessages = toWorkerMessagesResp(body.input);
  if (!workerMessages.length && Array.isArray(body.messages)) workerMessages = toWorkerMessagesOA(body.messages);
  if (typeof body.instructions === "string" && body.instructions) {
    workerMessages = [{ role: "system", content: body.instructions }, ...workerMessages];
  }
  if (!workerMessages.length) {
    return done(() => sendJson(res, 400, { error: { message: "input (string|array) or messages required", type: "invalid_request_error", code: "bad_request" } }));
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
    return done(() => openaiError(res, 502, e.message));
  }
  const { result, status } = resolved;
  // Límite profesional Responses: max_output_tokens ?? max_tokens.
  const rspMax = Number(body.max_output_tokens ?? body.max_tokens) || 4096;
  const rspCap = applyMaxTokens(result.text, rspMax);
  if (rspCap.hit) trace(`${logTag} MAXTOKENS cut=${result.text.length}->${rspCap.text.length}`);
  result.text = rspCap.text;
  const rspStatus = rspCap.hit ? "incomplete" : status;
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
      type: "message", id: "msg_" + rid(), status: rspStatus === "completed" ? "completed" : "incomplete",
      role: "assistant", content: [{ type: "output_text", text: result.text, annotations: [] }],
    }];
  }
  const outputItem = outputItems[outputItems.length - 1];

  if (!stream) {
    usage[body.__tier].tokens += estTokens(result.text);
  trace(`${logTag} SERVED chars=${result.text.length} status=${rspStatus} toolCall=${toolCall ? toolCall.name : "-"} elapsed=${Date.now() - t0}ms`);
  try { body.__release(); } catch {}
    return sendJson(res, 200, {
      id: respId, object: "response", created, model,
      status: rspStatus, output: outputItems, parallel_tool_calls: true, tool_choice: "auto", tools: [],
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
    usage[body.__tier].tokens += estTokens(result.text);
  trace(`${logTag} SERVED-FC name=${fcItem.name} elapsed=${Date.now() - t0}ms`);
  try { body.__release(); } catch {}
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
  usage[body.__tier].tokens += estTokens(result.text);
  trace(`${logTag} SERVED chars=${result.text.length} status=${rspStatus} elapsed=${Date.now() - t0}ms`);
  try { body.__release(); } catch {}
  res.end();
}

async function handleMessages(req, res, body) {
  stats.requests++;
  const tier = tierOf(req);
  const limited = checkLimit(tier);
  if (limited) { usage[tier].errors++; return limitResponse(res, limited); }
  const release = takeSlot(tier);
  const model = stripPrefix(body.model);
  if (!MODEL_IDS.has(model)) {
    release();
    return anthropicError(res, 404, `model '${body.model}' not found. Valid: ${[...MODEL_IDS].join(", ")}`);
  }
  const maxTokens = Number(body.max_tokens) || 1024;
  const workerMessages = toWorkerMessagesAnt(body.system, body.messages);
  if (!workerMessages.length) { release(); return anthropicError(res, 400, "messages required"); }
  const stream = body.stream === true;
  // Thinking Anthropic -> flag del worker. El worker razona en el texto;
  // FMP no fabrica bloques thinking (ver /health.thinking).
  const thinking = body.thinking !== undefined ? !!body.thinking : true;

  const msgId = "msg_" + rid();
  const inputTokens = estTokens(JSON.stringify(workerMessages));
  const logTag = `ANT/${model}/${msgId.slice(-6)}`;
  // Límite profesional (igual que OAI): worker ignora max_tokens.

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
  trace(`${logTag} START-LIVE stream=${stream} tools=${antDefs.length} thinking=${thinking}`);
  const fullMode = body.stream_mode === "full";
  if (fullMode && stream) {
    let got;
    try {
      const r = await fetchCompleteText({ messages: wmAnt, modelId: model, thinking, deepSearch: false, signal: null, logTag });
      got = r.complete ? r.text : (await (async () => {
        try {
          const ju = await callUpstream({ messages: wmAnt, modelId: model, stream: false, thinking, deepSearch: false, signal: null });
          const jt = await readWorkerJson(ju);
          return jt.length >= r.text.length && jt.length > 0 ? jt : r.text;
        } catch { return r.text; }
      })());
    } catch (e) {
      stats.errors++;
      usage[tier].errors++;
      release();
      return anthropicError(res, 502, e.message);
    }
    const capped = applyMaxTokens(got, maxTokens);
    if (capped.hit) trace(`${logTag} MAXTOKENS cut=${got.length}->${capped.text.length}`);
    const ftext = capped.hit ? capped.text : got;
    const fstop = capped.hit ? "max_tokens" : "end_turn";
    usage[tier].tokens += estTokens(ftext);
    res.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
      "access-control-allow-origin": "*",
    });
    const fsend = (obj) => res.write("event: " + obj._ev + "\ndata: " + JSON.stringify(obj._d) + "\n\n");
    const fev = (name, d) => ({ _ev: name, _d: d });
    fsend(fev("message_start", { type: "message_start", message: { id: msgId, type: "message", role: "assistant", model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: inputTokens, output_tokens: 1 } } }));
    fsend(fev("content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }));
    const CH = 48;
    for (let i = 0; i < ftext.length; i += CH) {
      fsend(fev("content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: ftext.slice(i, i + CH) } }));
    }
    fsend(fev("content_block_stop", { type: "content_block_stop", index: 0 }));
    fsend(fev("message_delta", { type: "message_delta", delta: { stop_reason: fstop, stop_sequence: null }, usage: { output_tokens: estTokens(ftext) } }));
    fsend(fev("message_stop", { type: "message_stop" }));
    trace(`${logTag} SERVED-FULL chars=${ftext.length} stop=${fstop} elapsed=${Date.now() - t0}ms`);
    release();
    return res.end();
  }

  if (!stream) {
    let got;
    try {
      const r = await fetchCompleteText({ messages: wmAnt, modelId: model, thinking, deepSearch: false, signal: null, logTag });
      got = r.complete ? r.text : (await (async () => {
        try {
          const ju = await callUpstream({ messages: wmAnt, modelId: model, stream: false, thinking, deepSearch: false, signal: null });
          const jt = await readWorkerJson(ju);
          return jt.length >= r.text.length && jt.length > 0 ? jt : r.text;
        } catch { return r.text; }
      })());
    } catch (e) {
      stats.errors++;
      usage[tier].errors++;
      release();
      return anthropicError(res, 502, e.message);
    }
    usage[tier].tokens += estTokens(got);
    const cappedAnt = applyMaxTokens(got, maxTokens);
    if (cappedAnt.hit) trace(`${logTag} MAXTOKENS cut=${got.length}->${cappedAnt.text.length}`);
    got = cappedAnt.hit ? cappedAnt.text : got;
    const tc0 = antDefs.length && !antDisabled ? await resolveToolCall(got, antDefs, wmAnt, model, logTag) : null;
    let content0;
    let stop0 = cappedAnt.hit ? "max_tokens" : "end_turn";
    if (tc0) {
      let input = {};
      try { input = JSON.parse(tc0.args); } catch { input = {}; }
      content0 = [];
      if (tc0.prefixText) content0.push({ type: "text", text: tc0.prefixText });
      content0.push({ type: "tool_use", id: "toolu_" + rid().slice(0, 20), name: tc0.name, input });
      stop0 = "tool_use";
      trace(`${logTag} TOOL-CALL name=${tc0.name}`);
    } else {
      content0 = [{ type: "text", text: got }];
    }
    release();
    return sendJson(res, 200, {
      id: msgId, type: "message", role: "assistant", model,
      content: content0,
      stop_reason: stop0,
      stop_sequence: null,
      usage: { input_tokens: inputTokens, output_tokens: estTokens(got) },
    });
  }

  // STREAM EN VIVO Anthropic: text_delta en cuanto llega + resume.
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
  send(ev("message_start", { type: "message_start", message: { id: msgId, type: "message", role: "assistant", model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: inputTokens, output_tokens: 1 } } }));
  send(ev("content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }));

  let emitted = "";
  let complete = false;
  let attempts = 0;
  let cappedHit = false;
  const maxChars = Math.floor(maxTokens * 4);
  const ctrl = new AbortController();
  req.on("close", () => { try { ctrl.abort(); } catch {} });
  const emitAnt = (novel) => {
    if (!novel || closed) return;
    let piece = novel;
    if (emitted.length + piece.length > maxChars) {
      piece = piece.slice(0, Math.max(0, maxChars - emitted.length));
      cappedHit = true;
    }
    if (piece) {
      emitted += piece;
      send(ev("content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: piece } }));
    }
    if (cappedHit) { try { ctrl.abort(); } catch {} }
  };
  for (let a = 0; a <= MAX_RETRIES && !closed; a++) {
    attempts = a + 1;
    let upstream;
    try {
      upstream = await callUpstream({ messages: wmAnt, modelId: model, stream: true, thinking, deepSearch: false, signal: ctrl.signal });
    } catch (e) {
      if (emitted || a === MAX_RETRIES) break;
      stats.retries++;
      trace(`${logTag} LIVE upstream-error attempt=${attempts} err=${e.message}`);
      await sleep(RETRY_BASE_MS * (a + 1));
      continue;
    }
    let diverged = false;
    try {
      let fresh = "";
      const chunk = await readWorkerSSELive(upstream, async (piece) => {
        if (closed || diverged) return;
        fresh += piece;
        const merged = dedupeAppend(emitted, fresh);
        if (!merged.startsWith(emitted)) { diverged = true; return; }
        const novel = merged.slice(emitted.length);
        if (novel) {
          const before = emitted.length;
          emitAnt(novel);
          fresh = fresh.slice(emitted.length - before) || fresh;
          if (cappedHit) { diverged = true; complete = true; }
        }
      }, ctrl.signal);
      if (diverged) {
        stats.truncatedUpstream++;
        trace(`${logTag} LIVE DIVERGED emitted=${emitted.length} -> close honest`);
        break;
      }
      if (ctrl.signal.aborted || closed) break;
      if (chunk.done) {
        const rest = dedupeAppend(emitted, chunk.text).slice(emitted.length);
        if (rest && !closed) emitAnt(rest);
        complete = !cappedHit;
        break;
      }
      stats.truncatedUpstream++;
      if (!emitted) {
        trace(`${logTag} LIVE TRUNCATED attempt=${attempts} nothing-emitted -> retry`);
        stats.retries++;
        if (a < MAX_RETRIES) await sleep(RETRY_BASE_MS * (a + 1));
        continue;
      }
      trace(`${logTag} LIVE TRUNCATED attempt=${attempts} emitted=${emitted.length} -> close honest`);
      break;
    } catch (e) {
      if (emitted || a === MAX_RETRIES) break;
      stats.retries++;
      trace(`${logTag} LIVE read-error attempt=${attempts} err=${e.message}`);
      await sleep(RETRY_BASE_MS * (a + 1));
    }
  }
  if (!closed) {
    // Sin rescate por continuación (ver nota OAI): cierre honesto.
    // Cap de max_tokens -> max_tokens (como API nativa).
    let antFinal = cappedHit ? "max_tokens" : (complete ? "end_turn" : "max_tokens");
    // Puente tool-use post-stream (solo EXEC explícito: el texto ya salió).
    const tcLive = antDefs.length && !antDisabled ? parseToolCall(emitted, antDefs) : null;
    if (tcLive) {
      let input = {};
      try { input = JSON.parse(tcLive.args); } catch { input = {}; }
      const tuId = "toolu_" + rid().slice(0, 20);
      send(ev("content_block_stop", { type: "content_block_stop", index: 0 }));
      send(ev("content_block_start", { type: "content_block_start", index: 1, content_block: { type: "tool_use", id: tuId, name: tcLive.name, input: {} } }));
      send(ev("content_block_delta", { type: "content_block_delta", index: 1, delta: { type: "input_json_delta", partial_json: JSON.stringify(input) } }));
      send(ev("content_block_stop", { type: "content_block_stop", index: 1 }));
      send(ev("message_delta", { type: "message_delta", delta: { stop_reason: "tool_use", stop_sequence: null }, usage: { output_tokens: estTokens(emitted) } }));
      trace(`${logTag} LIVE TOOL-CALL name=${tcLive.name}`);
    } else {
      send(ev("content_block_stop", { type: "content_block_stop", index: 0 }));
      send(ev("message_delta", { type: "message_delta", delta: { stop_reason: antFinal, stop_sequence: null }, usage: { output_tokens: estTokens(emitted) } }));
    }
    send(ev("message_stop", { type: "message_stop" }));
  }
  usage[tier].tokens += estTokens(emitted);
  trace(`${logTag} SERVED-LIVE chars=${emitted.length} attempts=${attempts} complete=${complete} elapsed=${Date.now() - t0}ms`);
  release();
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
    if (req.method === "GET" && (path === "/v1/usage" || path === "/usage")) return handleUsage(res);

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
