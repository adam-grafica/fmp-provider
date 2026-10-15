// FMP — FreeModels Provider v2.2.0
// Traductor OpenAI-compatible + Anthropic-compatible hacia freemodels.pro.
// Sin dependencias. Loopback-only por defecto (127.0.0.1:2089).
//
// Postura de seguridad (v2.2.0, contrato en lib/):
// - FMP solo TRANSPORTA esquemas de tool-calls; nunca ejecuta tools,
//   comandos ni acciones del host. El cliente/harness ejecuta.
// - Identidades de modelo declaradas por upstream, NO verificadas.
// - Auth por API key en endpoints no-salud cuando hay claves.
// - Falla cerrado: sin fallback silencioso de modelo.
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

const { loadConfig, validateConfig, redactConfig } = require("./lib/config");
const MODELS_LIB = require("./lib/models");
const TOOLS_LIB = require("./lib/tools");
const SEC = require("./lib/security");

// ---- configuration contract: validate before anything listens ----
const CFG = loadConfig(process.env);
{
  const { errors, warnings } = validateConfig(CFG);
  if (errors.length) {
    for (const e of errors) console.error(`[fmp-provider] CONFIG ERROR: ${e}`);
    console.error("[fmp-provider] refusing startup: fix configuration and retry (see .env.example / fmp doctor).");
    process.exit(1);
  }
  for (const w of warnings) console.error(`[fmp-provider] ${w}`);
}

const PORT = CFG.port;
const HOST = CFG.host;
const UPSTREAM = CFG.upstream;
const ORIGIN = "https://freemodels.pro";
const UPSTREAM_TIMEOUT_MS = CFG.upstreamTimeoutMs;
const MAX_BODY_BYTES = CFG.maxRequestBytes;
const MAX_RETRIES = CFG.maxRetries;
const RETRY_BASE_MS = CFG.retryBaseMs;
const TRACE_FILE = CFG.traceFile;
// Request IDs de correlación (Phase 4). Se propagan en respuestas y
// errores sanitizados; nunca llevan material secreto.
const rid = () => crypto.randomBytes(12).toString("hex");
const requestIdOf = (req) =>
  String((req.headers && (req.headers["x-request-id"] || req.headers["x-correlation-id"])) || "").slice(0, 64) || rid();
// Techo profesional: aunque el cliente pida más, FMP nunca emite más de
// este tope (protege al agente de blowups de 64k/128k). Default 8192.
const MAX_TOKENS_CEIL = CFG.maxTokensCeil;
// Normaliza el max pedido: default por ruta, respeta cliente, aplica techo.
function effMaxTokens(requested, routeDefault) {
  const dflt = Number(routeDefault) || 4096;
  let v = Number(requested);
  if (!v || v <= 0) v = dflt;
  return Math.min(v, MAX_TOKENS_CEIL);
}

// ---- Plan premium: tiers, rate-limit, cola ----
// Tiers por API key (Authorization: Bearer <key> o x-api-key) — legado,
// mantenido por compatibilidad. Si FMP_API_KEYS está configurado, el
// RateLimiter global (FMP_RATE_LIMIT_RPM, Phase 4) también aplica.
// Tiers por API key (Authorization: Bearer <key> o x-api-key).
// FMP_KEYS="key1:tier,key2:tier" ej: "sk-lan:pro,sk-casa:free".
// Sin key configurada: todo pasa como free sin límite (modo local).
const TIER_LIMITS = {
  free: { rpm: CFG.freeRpm, concurrency: CFG.freeConc },
  pro: { rpm: CFG.proRpm, concurrency: CFG.proConc },
};
const KEY_TIERS = {};
for (const pair of String(CFG.legacyKeys || "").split(",").map((s) => s.trim()).filter(Boolean)) {
  const [k, t] = pair.split(":");
  if (k) KEY_TIERS[k] = t === "pro" ? "pro" : "free";
}
const tierState = {
  free: { inFlight: 0, hits: [] },
  pro: { inFlight: 0, hits: [] },
};
const usage = { free: { requests: 0, tokens: 0, errors: 0 }, pro: { requests: 0, tokens: 0, errors: 0 }, local: { requests: 0, tokens: 0, errors: 0 } };
// Rate limiting global Phase 4 (FMP_RATE_LIMIT_RPM; 0 = deshabilitado).
const GLOBAL_LIMITER = new SEC.RateLimiter(CFG.rateLimitRpm);

function tierOf(req) {
  const key = SEC.extractKey(req);
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

function limitResponse(res, info, requestId, req) {
  const headers = {
    "content-type": "application/json",
    "retry-after": String(info.retryAfter || 5),
    "x-request-id": requestId || "",
  };
  const allow = req ? corsOriginFor(req) : null;
  if (allow) headers["access-control-allow-origin"] = allow;
  res.writeHead(429, headers);
  res.end(JSON.stringify({ error: { message: `rate limit (${info.reason}), retry in ${info.retryAfter || 5}s — plan premium avaliable`, type: "rate_limit_error", code: "rate_limited" } }));
}

// Catálogo de alias honesto (ver lib/models.js): identidades declaradas
// por upstream, verified_identity siempre "unknown" sin evidencia.
const DISABLED_MODELS = new Set(CFG.disabledModels);
const KILL_ALL = !!(CFG.killAll.present && CFG.killAll.value);
const MODELS = MODELS_LIB.CATALOG.map((m) => ({ id: m.id, owned_by: m.owned_by }));
const MODEL_IDS = new Set(MODELS.map((m) => m.id));

const startedAt = Date.now();
const stats = { requests: 0, retries: 0, truncatedUpstream: 0, errors: 0 };
// Per-route metrics (HARDENING-001): latency histogram (ms, capped buckets),
// errors, truncations, retries, timeouts. Exposed via /stats.
const ROUTES = ["openai", "anthropic", "responses"];
const routeMetrics = {};
for (const r of ROUTES) {
  routeMetrics[r] = {
    requests: 0, errors: 0, truncations: 0, retries: 0, timeouts: 0,
    latencies: [], // last 500 completion latencies, ms
  };
}
function routeTimeoutMs(route) {
  if (route === "openai" && CFG.timeoutOpenaiMs) return CFG.timeoutOpenaiMs;
  if (route === "anthropic" && CFG.timeoutAnthropicMs) return CFG.timeoutAnthropicMs;
  if (route === "responses" && CFG.timeoutResponsesMs) return CFG.timeoutResponsesMs;
  return UPSTREAM_TIMEOUT_MS;
}
function recordLatency(route, ms) {
  const m = routeMetrics[route];
  if (!m) return;
  m.requests++;
  m.latencies.push(ms);
  if (m.latencies.length > 500) m.latencies.shift();
}
function routeLatencyStats(route) {
  const arr = (routeMetrics[route] && routeMetrics[route].latencies || []).slice().sort((a, b) => a - b);
  if (!arr.length) return { n: 0, p50: null, p95: null, p99: null };
  const q = (p) => arr[Math.min(arr.length - 1, Math.floor(p * arr.length))];
  return { n: arr.length, p50: q(0.5), p95: q(0.95), p99: q(0.99) };
}
function harnessCapFor(req) {
  // Per-harness output cap: min(global ceil, harness limit when known).
  const h = require("./lib/config").harnessOf(req);
  const lim = CFG.harnessLimits[h];
  if (Number.isInteger(lim) && lim > 0) return { harness: h, cap: Math.min(lim, MAX_TOKENS_CEIL) };
  return { harness: h, cap: MAX_TOKENS_CEIL };
}
// SSE heartbeat: `: ping` comments every HEARTBEAT_MS keep idle streams
// alive through proxies/middleboxes. Returns a stopper. Comments are
// ignored by OpenAI/Anthropic SSE parsers (lines starting with ':').
function startHeartbeat(res, req, logTag) {
  const ms = CFG.heartbeatMs;
  if (!ms || ms <= 0) return () => {};
  let closed = false;
  req.on("close", () => { closed = true; });
  const timer = setInterval(() => {
    if (closed) { clearInterval(timer); return; }
    try { res.write(": ping\n\n"); } catch { clearInterval(timer); }
  }, ms);
  if (timer.unref) timer.unref();
  return () => clearInterval(timer);
}
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
  // Sanitizado: nunca escribir material secreto en trace.
  try { fs.appendFileSync(TRACE_FILE, `[${new Date().toISOString()}] ${SEC.redactSecrets(String(msg))}\n`); } catch {}
}

function corsOriginFor(req) {
  // CORS deny-by-default (Phase 4): solo orígenes explícitos reciben
  // Access-Control-Allow-Origin; el resto no recibe cabecera CORS.
  const origins = CFG.corsOrigins;
  if (!origins || !origins.length) return null;
  const reqOrigin = String((req.headers && req.headers.origin) || "").trim();
  if (!reqOrigin) return null;
  if (origins.includes("*")) return "*";
  return origins.includes(reqOrigin) ? reqOrigin : null;
}

function sendJson(res, status, obj, req) {
  const body = JSON.stringify(obj);
  const headers = {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(body),
  };
  const allow = req ? corsOriginFor(req) : null;
  if (allow) headers["access-control-allow-origin"] = allow;
  res.writeHead(status, headers);
  res.end(body);
}

function sseHeaders(req) {
  const h = {
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    connection: "keep-alive",
  };
  const allow = req ? corsOriginFor(req) : null;
  if (allow) h["access-control-allow-origin"] = allow;
  return h;
}

function openaiError(res, status, message, code, req) {
  sendJson(res, status, { error: { message, type: "server_error", code: code || null } }, req);
}

function anthropicError(res, status, message, req) {
  sendJson(res, status, { type: "error", error: { type: "api_error", message } }, req);
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

// "fmp-oai/sol" -> "sol"; "sol" -> "sol" (ver lib/models.js).
const stripPrefix = MODELS_LIB.stripPrefix;

function textOfContent(content) {
  if (content == null) return "";
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content.map((p) => {
      if (typeof p === "string") return p;
      if (Array.isArray(p)) return textOfContent(p);
      if (p && typeof p === "object") {
        if (typeof p.text === "string") return p.text;
        if (p.type === "image_url" || p.type === "image") return "";
        if (p.type === "tool_result") {
          const raw = p.content !== undefined ? p.content : p.output;
          return textOfContent(raw);
        }
        if (p.type === "tool_use") return "";
        return "";
      }
      return "";
    }).filter(Boolean).join("\n");
  }
  return String(content);
}

function flattenSystem(sys) {
  if (!sys) return [];
  if (typeof sys === "string") return sys.trim() ? [sys.trim()] : [];
  if (Array.isArray(sys)) {
    const parts = [];
    for (const item of sys) {
      if (!item) continue;
      if (typeof item === "string" && item.trim()) parts.push(item.trim());
      else if (Array.isArray(item)) parts.push(...flattenSystem(item));
      else if (typeof item === "object") {
        if (typeof item.text === "string" && item.text.trim()) parts.push(item.text.trim());
        else if (item.content !== undefined) parts.push(...flattenSystem(item.content));
      }
    }
    return parts;
  }
  if (typeof sys === "object" && typeof sys.text === "string" && sys.text.trim()) {
    return [sys.text.trim()];
  }
  return [];
}

// Inyecta instrucciones del sistema/tools como prefijo del primer user message
// porque el upstream worker descarta en silencio los mensajes con role: "system".
function injectSystemToFirstUser(messages, systemParts) {
  const sysText = (systemParts || []).filter(Boolean).join("\n\n").trim();
  if (!sysText) return messages;
  const out = [...messages];
  const firstUserIdx = out.findIndex((m) => m.role === "user");
  const block = `[INSTRUCCIONES DEL SISTEMA / HERRAMIENTAS]\n${sysText}\n[/INSTRUCCIONES DEL SISTEMA]\n\n`;
  if (firstUserIdx >= 0) {
    out[firstUserIdx] = {
      ...out[firstUserIdx],
      content: block + out[firstUserIdx].content,
    };
  } else {
    out.unshift({ role: "user", content: block.trim() });
  }
  return out;
}

function toWorkerMessagesOA(messages) {
  const raw = [];
  const systemParts = [];
  for (const m of messages || []) {
    if (!m || typeof m !== "object") continue;
    if (m.role === "system") {
      const s = textOfContent(m.content).trim();
      if (s) systemParts.push(s);
      continue;
    }
    if (m.role === "tool") {
      raw.push({ role: "user", content: `[resultado de herramienta ${m.tool_call_id || ""}]\n${textOfContent(m.content)}`.trim() });
      continue;
    }
    const role = m.role === "assistant" ? "assistant" : "user";
    let text = textOfContent(m.content);
    if (role === "assistant" && Array.isArray(m.tool_calls) && m.tool_calls.length) {
      for (const tc of m.tool_calls) {
        const fn = (tc && tc.function) || {};
        const toolName = fn.name || (tc && tc.id) || "";
        const argsStr = fn.arguments || "{}";
        text += `\n<tool_call>\n{"name": "${toolName}", "arguments": ${argsStr}}\n</tool_call>`.trimEnd();
      }
      text = text.trim();
    }
    raw.push({ role, content: text });
  }
  return injectSystemToFirstUser(raw, systemParts);
}

function toWorkerMessagesAnt(system, messages) {
  const raw = [];
  const systemParts = flattenSystem(system);
  for (const m of messages || []) {
    if (!m || typeof m !== "object") continue;
    if (m.role === "system") {
      const s = textOfContent(m.content).trim();
      if (s) systemParts.push(s);
      continue;
    }
    // tool_result -> worker lo ve como dato del usuario con id identificado.
    if (m.role === "tool" || (m.role === "user" && Array.isArray(m.content) && m.content.some((p) => p && p.type === "tool_result"))) {
      let toolResults = [];
      if (Array.isArray(m.content)) {
        for (const p of m.content) {
          if (p && p.type === "tool_result") {
            const rawRes = p.content !== undefined ? p.content : p.output;
            const resStr = typeof rawRes === "string" ? rawRes : (Array.isArray(rawRes) ? textOfContent(rawRes) : JSON.stringify(rawRes ?? ""));
            toolResults.push(`[resultado de herramienta ${p.tool_use_id || ""}]:\n${resStr}`);
          } else if (p && p.text) {
            toolResults.push(p.text);
          }
        }
      } else {
        toolResults.push(textOfContent(m.content));
      }
      raw.push({ role: "user", content: toolResults.join("\n\n").trim() });
      continue;
    }
    const role = m.role === "assistant" ? "assistant" : "user";
    let text = textOfContent(m.content);
    // tool_use nativo Anthropic -> texto para el worker en formato Anthropic XML nativo.
    if (role === "assistant" && Array.isArray(m.content)) {
      const uses = m.content.filter((p) => p && p.type === "tool_use");
      for (const u of uses) {
        text += `\n<tool_use>\n<name>${u.name || ""}</name>\n<arguments>\n${JSON.stringify(u.input || {}, null, 2)}\n</arguments>\n</tool_use>`;
      }
      text = text.trim();
    }
    raw.push({ role, content: text });
  }
  return injectSystemToFirstUser(raw, systemParts);
}

async function callUpstream({ messages, modelId, stream, thinking, deepSearch, signal, timeoutMs, route }) {
  const budget = Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : UPSTREAM_TIMEOUT_MS;
  const ctrl = new AbortController();
  let timedOut = false;
  const timer = setTimeout(() => { timedOut = true; ctrl.abort(new Error("upstream timeout")); }, budget);
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
        "user-agent": "fmp-provider/" + MODELS_LIB.VERSION,
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
  } catch (e) {
    // Explicit timeout signal for metrics: distinguish timeouts from
    // other upstream failures (never silent, never swallowed).
    if (timedOut && route && routeMetrics[route]) routeMetrics[route].timeouts++;
    throw e;
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
async function fetchCompleteText({ messages, modelId, thinking, deepSearch, signal, logTag, route, timeoutMs }) {
  let full = "";
  let attempts = 0;
  const rm = route && routeMetrics[route] ? routeMetrics[route] : null;
  for (let a = 0; a <= MAX_RETRIES; a++) {
    attempts = a + 1;
    let upstream;
    try {
      upstream = await callUpstream({ messages, modelId, stream: true, thinking, deepSearch, signal, timeoutMs, route });
    } catch (e) {
      if (a === MAX_RETRIES) throw e;
      stats.retries++;
      if (rm) rm.retries++;
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
      if (rm) rm.retries++;
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
    if (rm) rm.truncations++;
    full = dedupeAppend(full, chunk.text);
    trace(`${logTag} TRUNCATED attempt=${attempts} charsSoFar=${full.length}`);
    if (a === MAX_RETRIES) {
      trace(`${logTag} retries-exhausted chars=${full.length}`);
      return { text: full, attempts, complete: false, aborted: false };
    }
    stats.retries++;
    if (rm) rm.retries++;
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

function healthState() {
  // healthy: requests flowing, error rate low. degraded: errors present
  // but serving. unavailable: kill switch engaged.
  if (KILL_ALL) return "unavailable";
  if (stats.errors > 0 && stats.requests > 0 && stats.errors / stats.requests > 0.5) return "degraded";
  return "healthy";
}

function handleModels(res, req) {
  sendJson(res, 200, {
    object: "list",
    data: MODELS_LIB.listModels(DISABLED_MODELS),
  }, req);
}

function handleHealth(res, req) {
  sendJson(res, 200, {
    ok: true,
    service: "fmp-provider",
    version: MODELS_LIB.VERSION,
    status: healthState(),
    upstream: UPSTREAM,
    models: MODELS.map((m) => m.id),
    uptime_s: Math.floor((Date.now() - startedAt) / 1000),
    stats,
    routes: {
      openai: CFG.enableOpenAI.present ? !!CFG.enableOpenAI.value : true,
      anthropic: CFG.enableAnthropic.present ? !!CFG.enableAnthropic.value : true,
      responses: CFG.enableResponses.present ? !!CFG.enableResponses.value : false,
    },
    tool_mode: "transport_only",
    kill_switch: KILL_ALL,
    tiers: {
      free: { ...TIER_LIMITS.free, inFlight: tierState.free.inFlight, usage: usage.free },
      pro: { ...TIER_LIMITS.pro, inFlight: tierState.pro.inFlight, usage: usage.pro },
      local: { usage: usage.local },
    },
    // Honestidad: el worker NO expone canal thinking separado; thinking:true
    // se reenvía y el modelo razona en el texto. Sin bloques fabricados.
    thinking: { mode: "worker-passthrough", separate_channel: false },
  }, req);
}

function handleStats(res, req) {
  const routes = {};
  for (const r of ROUTES) {
    const m = routeMetrics[r];
    routes[r] = {
      requests: m.requests, errors: m.errors, truncations: m.truncations,
      retries: m.retries, timeouts: m.timeouts,
      latency_ms: routeLatencyStats(r),
    };
  }
  sendJson(res, 200, {
    ok: true,
    service: "fmp-provider",
    version: MODELS_LIB.VERSION,
    status: healthState(),
    requests: stats.requests,
    retries: stats.retries,
    truncatedUpstream: stats.truncatedUpstream,
    errors: stats.errors,
    stats,
    routes,
    uptime_s: Math.floor((Date.now() - startedAt) / 1000),
  }, req);
}

function handleUsage(res, req) {
  sendJson(res, 200, { object: "usage", usage, tiers: TIER_LIMITS, uptime_s: Math.floor((Date.now() - startedAt) / 1000) }, req);
}

// Sanitized route metadata on every inference response (Phase 2):
// alias seleccionado + ruta, sin material secreto ni fallback oculto.
function routeMeta(alias, route) {
  return {
    alias,
    route,
    declared_identity: (MODELS_LIB.CATALOG.find((m) => m.id === alias) || {}).declared_identity || "unknown",
    verified_identity: "unknown",
  };
}

async function handleChatCompletions(req, res, body, requestId) {
  stats.requests++;
  const ROUTE = "openai";
  const T0 = Date.now();
  const TIMEOUT = routeTimeoutMs(ROUTE);
  const { harness, cap: HARNESS_CAP } = harnessCapFor(req);
  const openaiOn = CFG.enableOpenAI.present ? !!CFG.enableOpenAI.value : true;
  if (!openaiOn) {
    stats.errors++;
    return sendJson(res, 404, {
      error: { message: "openai route disabled (FMP_ENABLE_OPENAI=false)", type: "invalid_request_error", code: "route_disabled" },
    }, req);
  }
  const tier = tierOf(req);
  const gl = GLOBAL_LIMITER.check(tier + ":chat");
  if (!gl.allowed) { stats.errors++; usage[tier].errors++; return limitResponse(res, { retryAfter: gl.retryAfter, reason: "rpm" }, requestId); }
  const limited = checkLimit(tier);
  if (limited) { usage[tier].errors++; return limitResponse(res, limited, requestId); }
  const release = takeSlot(tier);
  // Explicit model selection required; never fall back silently (Phase 2).
  if (typeof body.model !== "string" || !stripPrefix(body.model)) {
    release();
    stats.errors++;
    return sendJson(res, 400, {
      error: { message: "model is required: request must explicitly select an alias (no ambient fallback)", type: "invalid_request_error", code: "model_required" },
    }, req);
  }
  const model = stripPrefix(body.model);
  if (!MODELS_LIB.isKnown(model)) {
    release();
    stats.errors++;
    return sendJson(res, 404, {
      error: { message: `model '${body.model}' not found. Valid: ${[...MODEL_IDS].join(", ")}`, type: "invalid_request_error", code: "model_not_found" },
    }, req);
  }
  if (KILL_ALL || DISABLED_MODELS.has(model)) {
    release();
    stats.errors++;
    usage[tier].errors++;
    return sendJson(res, 503, {
      error: { message: `model '${model}' unavailable (kill switch / disabled alias)`, type: "server_error", code: "alias_disabled" },
    }, req);
  }
  // Tool schemas validated BEFORE any upstream call (Phase 3).
  // Malformed schemas rejected explicitly; never executed, only transported.
  const toolCheck = TOOLS_LIB.validateOpenAITools(body.tools);
  if (!toolCheck.ok) {
    release();
    stats.errors++;
    return sendJson(res, 400, {
      error: { message: `invalid tools schema: ${toolCheck.error}`, type: "invalid_request_error", code: "invalid_tool_schema" },
    }, req);
  }
  if (body.tool_choice !== undefined && body.tool_choice !== null
      && typeof body.tool_choice !== "string" && typeof body.tool_choice !== "object") {
    release();
    stats.errors++;
    return sendJson(res, 400, {
      error: { message: "invalid tool_choice: must be 'auto', 'none', 'required' or an object", type: "invalid_request_error", code: "invalid_tool_schema" },
    }, req);
  }
  const oaiDefs = toolCheck.defs;
  const workerMessages = toWorkerMessagesOA(body.messages);
  if (!workerMessages.length) {
    release();
    stats.errors++;
    return sendJson(res, 400, { error: { message: "messages required", type: "invalid_request_error", code: "bad_request" } }, req);
  }
  const stream = body.stream === true;
  const thinking = !!body.thinking;
  const deepSearch = !!(body.deepSearch ?? body.deep_search);
  // Límite profesional: el worker ignora max_tokens; FMP trunca y avisa
  // con finish length (igual que API nativa). Default 4096.
  // Per-harness cap (HARDENING-001): min(global ceil, FMP_HARNESS_LIMITS[harness]).
  const maxTokens = Math.min(effMaxTokens(body.max_tokens ?? body.max_completion_tokens, 4096), HARNESS_CAP);
  const hasTools = oaiDefs.length && body.tool_choice !== "none";
  // stream_mode full: bufferiza íntegro y luego emite SSE (para rachas
  // malas de upstream o cuando hay tools activas para resolución limpia).
  const fullMode = body.stream_mode === "full" || (hasTools && stream);

  const id = "chatcmpl-" + rid();
  const created = Math.floor(Date.now() / 1000);
  const logTag = `OAI/${model}/${id.slice(-6)}`;
  if (harness !== "unknown") trace(`${logTag} HARNESS=${harness} cap=${HARNESS_CAP}`);

  // Puente tool-use (TRANSPORTE, no ejecución): si el cliente declara
  // tools, pedir EXEC::: al worker. FMP nunca ejecuta la herramienta;
  // el harness cliente ejecuta y devuelve el resultado.
  const oaiTools = Array.isArray(body.tools) ? body.tools : [];
  const oaiToolSys = (oaiDefs.length && body.tool_choice !== "none") ? agentSystemPrompt(oaiTools, model, "openai") : "";
  const oaiInputMsgs = oaiToolSys ? [{ role: "system", content: oaiToolSys }, ...(body.messages || [])] : (body.messages || []);
  const wm = toWorkerMessagesOA(oaiInputMsgs);

  // Obtiene texto íntegro (SSE + retry + fallback JSON). Reutilizado
  // por ramas stream y no-stream.
  async function getOAIText() {
    const r = await fetchCompleteText({ messages: wm, modelId: model, thinking, deepSearch, signal: null, logTag, route: ROUTE, timeoutMs: TIMEOUT });
    if (r.complete) return { text: r.text, complete: true, finish: "stop" };
    try {
      const ju = await callUpstream({ messages: wm, modelId: model, stream: false, thinking, deepSearch, signal: null, timeoutMs: TIMEOUT, route: ROUTE });
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

  const routeErr = () => { stats.errors++; usage[tier].errors++; routeMetrics[ROUTE].errors++; };
  if (!stream) {
    let got;
    try {
      got = await getOAIText();
    } catch (e) {
      routeErr();
      release();
      const s = SEC.sanitizeUpstreamError(e, requestId);
      return openaiError(res, s.status, s.message, "upstream_error", req);
    }
    recordLatency(ROUTE, Date.now() - T0);
    usage[tier].tokens += estTokens(got.text);
    const capped = applyMaxTokens(got.text, maxTokens);
    if (capped.hit) trace(`${logTag} MAXTOKENS cut=${got.text.length}->${capped.text.length}`);
    got = { text: capped.text, complete: got.complete, finish: capped.hit ? "length" : got.finish };
    const tc = oaiDefs.length ? await resolveToolCall(got.text, oaiDefs, wm, model, logTag) : null;
    if (tc) {
      // Solo nombre de tool en logs — nunca argumentos (Phase 3).
      trace(`${logTag} TOOL-TRANSPORT name=${tc.name} (transport only, not executed)`);
      release();
      const priorId = TOOLS_LIB.findPriorCallId(
        (TOOLS_LIB.openAICallsToInternal(body.tool_calls || []).calls || []), tc.name);
      return sendJson(res, 200, {
        id, object: "chat.completion", created, model,
        choices: [{
          index: 0,
          message: {
            role: "assistant",
            content: tc.prefixText || null,
            tool_calls: [{ id: priorId || ("call_" + rid().slice(0, 20)), type: "function", function: { name: tc.name, arguments: tc.args } }],
          },
          finish_reason: "tool_calls",
        }],
        usage: { prompt_tokens: estTokens(JSON.stringify(wm)), completion_tokens: estTokens(got.text), total_tokens: 0 },
      }, req);
    }
    release();
    // _meta opt-in (x-fmp-meta: 1): strict clients (Claude Code SDK) get
    // byte-clean native schema by default.
    const oaiBody = {
      id, object: "chat.completion", created, model,
      choices: [{ index: 0, message: { role: "assistant", content: got.text }, finish_reason: got.finish }],
      usage: { prompt_tokens: estTokens(JSON.stringify(wm)), completion_tokens: estTokens(got.text), total_tokens: 0 },
    };
    if (req.headers["x-fmp-meta"] === "1") oaiBody._meta = routeMeta(model, "openai");
    return sendJson(res, 200, oaiBody, req);
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
      routeErr();
      release();
      const s = SEC.sanitizeUpstreamError(e, requestId);
      return openaiError(res, s.status, s.message, "upstream_error", req);
    }
    recordLatency(ROUTE, Date.now() - T0);
    const capped = applyMaxTokens(got.text, maxTokens);
    if (capped.hit) trace(`${logTag} MAXTOKENS cut=${got.text.length}->${capped.text.length}`);
    const ftext = capped.text;
    const ffinish = capped.hit ? "length" : got.finish;
    usage[tier].tokens += estTokens(ftext);

    const tc0 = hasTools ? await resolveToolCall(ftext, oaiDefs, wm, model, logTag) : null;
    res.writeHead(200, sseHeaders(req));
    const fsend = (obj) => res.write("data: " + JSON.stringify(obj) + "\n\n");
    fsend({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] });

    if (tc0) {
      if (tc0.prefixText) {
        const CH = 48;
        for (let i = 0; i < tc0.prefixText.length; i += CH) {
          fsend({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: { content: tc0.prefixText.slice(i, i + CH) }, finish_reason: null }] });
        }
      }
      const tcId = "call_" + rid().slice(0, 20);
      fsend({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: tcId, type: "function", function: { name: tc0.name, arguments: tc0.args } }] }, finish_reason: null }] });
      fsend({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }] });
      trace(`${logTag} SERVED-TOOL-STREAM-OAI name=${tc0.name} elapsed=${Date.now() - t0}ms`);
    } else {
      const CH = 48;
      for (let i = 0; i < ftext.length; i += CH) {
        fsend({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: { content: ftext.slice(i, i + CH) }, finish_reason: null }] });
      }
      fsend({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: {}, finish_reason: ffinish }] });
      trace(`${logTag} SERVED-FULL chars=${ftext.length} finish=${ffinish} elapsed=${Date.now() - t0}ms`);
    }
    res.write("data: [DONE]\n\n");
    release();
    return res.end();
  }
  res.writeHead(200, sseHeaders(req));
  const stopHeartbeat = startHeartbeat(res, req, logTag);
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
      upstream = await callUpstream({ messages: wm, modelId: model, stream: true, thinking, deepSearch, signal: ctrl.signal, timeoutMs: TIMEOUT, route: ROUTE });
    } catch (e) {
      if (emitted || a === MAX_RETRIES) {
        // Explicit stream error (never silent): error chunk carries code,
        // then honest close. Client sees failure, not a hang.
        if (!closed && !emitted) {
          send({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: {}, finish_reason: "error" }] });
          const s = SEC.sanitizeUpstreamError(e, requestId);
          trace(`${logTag} LIVE FAILED status=${s.status} request_id=${requestId}`);
        }
        break;
      }
      stats.retries++;
      routeMetrics[ROUTE].retries++;
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
        // Intra-intento: el worker SIEMPRE continúa (nunca reemite).
        // Emitir directo sin dedupe: el merge solo aplica ENTRE intentos.
        // (dedupeAppend regla 3 devolvía "el más largo" y rompía el
        // prefijo cuando una pieza corta llegaba tras texto emitido.)
        emitNovel(fresh);
        fresh = "";
        if (cappedHit) { diverged = true; complete = true; }
      }, ctrl.signal);
      if (diverged) {
        if (cappedHit) {
          trace(`${logTag} LIVE CAPPED emitted=${emitted.length} -> close length`);
        } else {
          stats.truncatedUpstream++;
          routeMetrics[ROUTE].truncations++;
          trace(`${logTag} LIVE DIVERGED emitted=${emitted.length} -> close honest`);
        }
        break;
      }
    } catch (e) {
      if (emitted || a === MAX_RETRIES) break;
      stats.retries++;
      routeMetrics[ROUTE].retries++;
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
    routeMetrics[ROUTE].truncations++;
    if (!emitted) {
      // Nada emitido: reintento seguro.
      trace(`${logTag} LIVE TRUNCATED attempt=${attempts} nothing-emitted -> retry`);
      stats.retries++;
      routeMetrics[ROUTE].retries++;
      if (a < MAX_RETRIES) await sleep(RETRY_BASE_MS * (a + 1));
      continue;
    }
    // CONTINUATION (stability-006, igual que Anthropic): rondas extra
    // acotadas (MAX_RETRIES) con la cola emitida como contexto mantienen
    // el SSE abierto y cierran en stop, no en length. Sin solapamiento
    // no se pega nada (cierre honesto).
    if (a < MAX_RETRIES) {
      let continued = false;
      for (let cr = 0; cr < MAX_RETRIES && !closed && !cappedHit; cr++) {
        trace(`${logTag} LIVE CONTINUE attempt=${attempts}.${cr} emitted=${emitted.length}`);
        stats.retries++;
        routeMetrics[ROUTE].retries++;
        const tail = emitted.slice(-500);
        const contMsgs = wm.concat([
          { role: "assistant", content: tail },
          { role: "user", content: "Continúa exactamente donde quedaste, sin repetir ni resumir." },
        ]);
        let cont;
        try {
          const contUp = await callUpstream({ messages: contMsgs, modelId: model, stream: true, thinking, deepSearch, signal: ctrl.signal, timeoutMs: TIMEOUT, route: ROUTE });
          // Collect WITHOUT emitting: merge decision first, emit once.
          cont = await readWorkerSSELive(contUp, null, ctrl.signal);
        } catch (e) {
          trace(`${logTag} LIVE CONTINUE-FAIL err=${String((e && e.message) || e).slice(0, 120)} -> close honest`);
          break;
        }
        const merged = dedupeAppend(emitted, cont.text);
        const novel = merged.slice(emitted.length);
        if (merged.length <= emitted.length + 8 && !cont.done) {
          trace(`${logTag} LIVE CONTINUE-STALLED total=${emitted.length} -> close honest`);
          break;
        }
        if (novel && !closed && !cappedHit) {
          emitted += novel;
          for (let k = 0; k < novel.length; k += 48) {
            send({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: { content: novel.slice(k, k + 48) }, finish_reason: null }] });
          }
        }
        if (cont.done) {
          complete = !cappedHit;
          continued = true;
          trace(`${logTag} LIVE CONTINUE-OK total=${emitted.length}`);
          break;
        }
        stats.truncatedUpstream++;
        routeMetrics[ROUTE].truncations++;
        trace(`${logTag} LIVE CONTINUE-CUT total=${emitted.length} -> next round`);
        await sleep(RETRY_BASE_MS);
      }
      if (continued) break;
      trace(`${logTag} LIVE CONTINUE-EXHAUSTED total=${emitted.length} -> close honest`);
    } else {
      trace(`${logTag} LIVE TRUNCATED attempt=${attempts} emitted=${emitted.length} -> close honest`);
    }
    break;
  }
  stopHeartbeat();
  recordLatency(ROUTE, Date.now() - T0);
  if (!closed) {
    // Sin rescate por continuación: el worker reescribe el borde en vez
    // de continuar (evidencia), así que cualquier pegado sería
    // Frankenstein. Cierre honesto: complete->stop, corte->length,
    // cap de max_tokens->length (como API nativa).
    let finalReason = cappedHit ? "length" : (complete ? "stop" : "length");
    // Puente tool-use post-stream (solo si el texto final trae EXEC).
    let tcLive = oaiDefs.length ? parseToolCall(emitted, oaiDefs) : null;
    if (!tcLive && oaiDefs.length) {
      const shellCmd = extractShellBlock(emitted);
      const execTool = (oaiDefs || []).find((t) => t.name === "exec_command");
      if (shellCmd && execTool) {
        const toolName = execTool.name;
        const args = { cmd: shellCmd };
        tcLive = { name: toolName, args: JSON.stringify(args) };
      }
    }
    if (tcLive) {
      const tcId = "call_" + rid().slice(0, 20);
      send({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: tcId, type: "function", function: { name: tcLive.name, arguments: tcLive.args } }] }, finish_reason: null }] });
      send({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }] });
      trace(`${logTag} LIVE TOOL-TRANSPORT name=${tcLive.name} (transport only, not executed)`);
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
        out.push({ role: "assistant", content: `EXEC:::${m.name || "tool"} ARGS ${m.arguments || "{}"}`.trim() });
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

// Extrae llamada a herramienta del texto del worker (TRANSPORTE).
// Protocolo: el worker emite bloque
//   EXEC:::<tool> ARGS <json>
// y el puente lo convierte a function_call nativo. FMP nunca ejecuta;
// el harness cliente ejecuta con sus propios permisos y aprobaciones.
// Valida contra lib/tools.js (fuente única); args se devuelve como
// string JSON validado para los emisores de esquema nativo.
function parseToolCall(text, availTools) {
  const hit = (TOOLS_LIB.parsePolyglotToolCall || TOOLS_LIB.parseExecBlock)(text, availTools);
  if (!hit) return null;
  return { name: hit.name, args: JSON.stringify(hit.args), prefixText: hit.prefixText };
}

// Fallback agéntico: si el worker no emitió EXEC::: pero el texto trae
// un bloque shell (```bash ... ```), convertir el primero a exec_command.
// Codex pide aprobación al usuario antes de ejecutar, no hay ejecución ciega.
// Fallback agéntico estricto (solo para herramientas tipo exec_command en flujos legacy):
// Si el worker emite un bloque explícito de shell (```bash ... ```), convertirlo.
// NUNCA aplica a bloques sin lenguaje ni a la herramienta 'Bash' de Claude Code (que requiere EXEC:::).
function extractShellBlock(text) {
  if (!text) return null;
  const m = text.match(/```(?:bash|sh|shell|zsh)\s*\n([\s\S]*?)```/);
  if (!m) return null;
  const cmd = m[1].trim();
  if (!cmd || cmd.length > 2000 || cmd.includes("\n\n\n")) return null;
  // Evitar bloques que son claramente ilustrativos múltiples: solo el primero.
  return cmd;
}

// Resuelve llamada a herramienta con 3 niveles (TRANSPORTE, no ejecución):
// 1. EXEC::: explícito. 2. bloque shell -> exec_command (SOLO si el
// cliente declaró exec_command; el cliente decide si ejecuta).
// 3. reintento dirigido que pide SOLO el bloque.
async function resolveToolCall(text, defs, wm, modelId, logTag) {
  let tc = parseToolCall(text, defs);
  if (tc) return tc;
  const shellCmd = extractShellBlock(text);
  const execTool = (defs || []).find((t) => t.name === "exec_command");
  if (shellCmd && execTool) {
    const i = text.indexOf("```");
    const toolName = execTool.name;
    const args = { cmd: shellCmd };
    // Solo el NOMBRE de la tool en logs — nunca el comando (Phase 3).
    trace(`${logTag} SHELL-BLOCK-TRANSPORT name=${toolName} (transport only, not executed)`);
    return { name: toolName, args: JSON.stringify(args), prefixText: text.slice(0, i).trim() };
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

const toolDefsForWorker = TOOLS_LIB.toolDefsForWorker;

function agentSystemPrompt(tools, modelId, transport) {
  const defs = toolDefsForWorker(tools);
  if (!defs.length) return "";
  const isAnthropic = transport === "anthropic" || (typeof modelId === "string" && /claude/i.test(modelId));

  if (isAnthropic) {
    const lines = defs.map((d) => `- <tool_description>\n<name>${d.name}</name>\n<description>${d.description}</description>\n<parameters>${JSON.stringify(d.params)}</parameters>\n</tool_description>`);
    return [
      "INSTRUCCIONES PARA EL USO DE HERRAMIENTAS:",
      "Tienes acceso real y directo a herramientas del sistema para interactuar con el entorno, archivos y procesos.",
      "ACCESO TOTAL: Si el usuario te pide inspeccionar, leer, crear, editar o buscar archivos, o ejecutar comandos en el sistema, DEBES invocar la herramienta correspondiente. NUNCA digas que no tienes acceso, NUNCA pidas que te peguen el contenido manualmente, y NUNCA generes simulaciones ni tutoriales ficticios.",
      "HERRAMIENTAS VÁLIDAS: ÚNICAMENTE puedes invocar las herramientas que figuran en el catálogo de herramientas disponibles. No inventes nombres inexistentes.",
      "OPERACIONES EN EL SISTEMA: Si tu herramienta disponible es \x27terminal\x27 (o \x27Bash\x27/\x27bash\x27) y necesitas leer o modificar archivos o inspeccionar el entorno, ejecuta comandos estándar de Linux (cat, sed, grep, ls, python3, etc.) a través de \x27terminal\x27.",
      "FORMATO DE LLAMADA: Para invocar una herramienta, responde usando el formato de etiquetas XML estándar:",
      "<tool_use>",
      "<name>nombre_herramienta</name>",
      "<arguments>",
      "{\"parametro\": \"valor\"}",
      "</arguments>",
      "</tool_use>",
      "(También se acepta: EXEC:::nombre_herramienta ARGS { ... })",
      "NUNCA finjas haber completado una tarea sin haber ejecutado la herramienta real que la realiza.",
      "Herramientas disponibles:",
      ...lines,
      "Si la consulta del usuario no requiere herramientas (preguntas conceptuales, explicaciones teóricas), responde normalmente en texto o markdown sin invocar herramientas.",
    ].join("\n");
  }

  const lines = defs.map((d) => `- ${d.name}: ${d.description} params=${JSON.stringify(d.params).slice(0, 800)}`);
  return [
    "INSTRUCCIONES PARA EL USO DE HERRAMIENTAS:",
    "Tienes herramientas reales conectadas al entorno para interactuar con el sistema operativo, archivos y procesos.",
    "ACCESO AL SISTEMA Y ARCHIVOS: Tienes acceso total. Si el usuario te pide leer, inspeccionar, editar o buscar archivos, configs, scripts o ejecutar comandos del sistema, DEBES usar la herramienta correspondiente de tu catálogo. NUNCA digas que no tienes acceso, NUNCA pidas que te peguen el contenido, y NUNCA generes tutoriales o simulaciones ficticias cuando se te pide una acción real.",
    "PROHIBIDO INVENTAR HERRAMIENTAS: ÚNICAMENTE puedes invocar las herramientas que figuran en la lista de \x27Herramientas disponibles en este entorno\x27. NUNCA inventes nombres como file_edit, file_write o similares si no están en tu lista.",
    "OPERACIONES DE ARCHIVOS Y CONFIGURACIÓN: Si tu herramienta disponible es \x27terminal\x27 (o \x27Bash\x27/\x27bash\x27) y necesitas leer, inspeccionar, crear o modificar archivos en el sistema, ejecuta directamente comandos estándar de Linux (como cat, sed, grep, python3, tee, echo, ls) a través de \x27terminal\x27.",
    "FORMATO DE LLAMADA: Cuando decidas invocar una herramienta, emite la llamada en formato <tool_call> o bloque JSON:",
    "<tool_call>",
    "{\"name\": \"nombre_herramienta\", \"arguments\": {\"parametro\": \"valor\"}}",
    "</tool_call>",
    "(También se acepta: EXEC:::nombre_herramienta ARGS { ... })",
    "NUNCA finjas haber completado una tarea sin haber ejecutado la herramienta real que la realiza.",
    "Herramientas disponibles en este entorno:",
    ...lines,
    "Si la consulta no requiere herramientas (ej: explicaciones teóricas, conceptos o preguntas generales), responde normalmente en texto plano o markdown sin invocar herramientas.",
  ].join("\n");
}

async function resolveResponsesText(body, workerMessages, model, logTag, route, timeoutMs) {
  const thinking = !!body.thinking;
  let result;
  try {
    result = await fetchCompleteText({ messages: workerMessages, modelId: model, thinking, deepSearch: false, signal: null, logTag, route, timeoutMs });
  } catch (e) {
    stats.errors++;
    throw e;
  }
  let status = result.complete ? "completed" : "incomplete";
  if (!result.complete) {
    try {
      trace(`${logTag} FALLBACK-JSON charsSoFar=${result.text.length}`);
      const ju = await callUpstream({ messages: workerMessages, modelId: model, stream: false, thinking, deepSearch: false, signal: null, timeoutMs, route });
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

async function handleResponses(req, res, body, requestId) {
  stats.requests++;
  const ROUTE = "responses";
  const TIMEOUT = routeTimeoutMs(ROUTE);
  const { harness, cap: HARNESS_CAP } = harnessCapFor(req);
  const routeErr = () => { stats.errors++; usage[tier].errors++; routeMetrics[ROUTE].errors++; };
  const T0 = Date.now();
  // Responses API: experimental, disabled by default (Phase 2).
  const rspOn = CFG.enableResponses.present ? !!CFG.enableResponses.value : false;
  if (!rspOn) {
    stats.errors++;
    return sendJson(res, 404, {
      error: { message: "responses route disabled by default (set FMP_ENABLE_RESPONSES=true to enable, experimental)", type: "invalid_request_error", code: "route_disabled" },
    }, req);
  }
  const tier = tierOf(req);
  const gl = GLOBAL_LIMITER.check(tier + ":responses");
  if (!gl.allowed) { stats.errors++; usage[tier].errors++; return limitResponse(res, { retryAfter: gl.retryAfter, reason: "rpm" }, requestId, req); }
  const limited = checkLimit(tier);
  if (limited) { usage[tier].errors++; return limitResponse(res, limited, requestId, req); }
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
  if (typeof body.model !== "string" || !stripPrefix(body.model)) {
    return done(() => {
      stats.errors++;
      routeMetrics[ROUTE].errors++;
      return sendJson(res, 400, {
        error: { message: "model is required: request must explicitly select an alias (no ambient fallback)", type: "invalid_request_error", code: "model_required" },
      }, req);
    });
  }
  const model = stripPrefix(body.model);
  if (!MODELS_LIB.isKnown(model)) {
    return done(() => {
      stats.errors++;
      routeMetrics[ROUTE].errors++;
      return sendJson(res, 404, {
        error: { message: `model '${body.model}' not found. Valid: ${[...MODEL_IDS].join(", ")}`, type: "invalid_request_error", code: "model_not_found" },
      }, req);
    });
  }
  if (KILL_ALL || DISABLED_MODELS.has(model)) {
    return done(() => {
      stats.errors++;
      routeMetrics[ROUTE].errors++;
      usage[tier].errors++;
      return sendJson(res, 503, {
        error: { message: `model '${model}' unavailable (kill switch / disabled alias)`, type: "server_error", code: "alias_disabled" },
      }, req);
    });
  }
  // Validate Responses function tools explicitly (transport only).
  if (body.tools !== undefined) {
    const rspTools = Array.isArray(body.tools) ? body.tools : null;
    if (!rspTools) {
      return done(() => {
        stats.errors++;
        return sendJson(res, 400, {
          error: { message: "invalid tools schema: tools must be an array", type: "invalid_request_error", code: "invalid_tool_schema" },
        }, req);
      });
    }
    for (let i = 0; i < rspTools.length; i++) {
      const t = rspTools[i];
      if (!t || typeof t !== "object" || (t.type !== "function" && !t.name)) {
        return done(() => {
          stats.errors++;
          return sendJson(res, 400, {
            error: { message: `invalid tools schema: tools[${i}] must be a function tool object`, type: "invalid_request_error", code: "invalid_tool_schema" },
          }, req);
        });
      }
      const nm = t.name || (t.function && t.function.name);
      if (typeof nm !== "string" || !TOOLS_LIB.NAME_RE.test(nm)) {
        return done(() => {
          stats.errors++;
          return sendJson(res, 400, {
            error: { message: `invalid tools schema: tools[${i}].name is invalid`, type: "invalid_request_error", code: "invalid_tool_schema" },
          }, req);
        });
      }
    }
  }
  let workerMessages = toWorkerMessagesResp(body.input);
  if (!workerMessages.length && Array.isArray(body.messages)) workerMessages = toWorkerMessagesOA(body.messages);
  if (typeof body.instructions === "string" && body.instructions) {
    workerMessages = [{ role: "system", content: body.instructions }, ...workerMessages];
  }
  if (!workerMessages.length) {
    return done(() => sendJson(res, 400, { error: { message: "input (string|array) or messages required", type: "invalid_request_error", code: "bad_request" } }, req));
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
    const sys = agentSystemPrompt(body.tools, body.model, "openai");
    if (sys) workerMessages = [{ role: "system", content: sys }, ...workerMessages];
  }
  let resolved;
  try {
    if (harness !== "unknown") trace(`${logTag} HARNESS=${harness} cap=${HARNESS_CAP}`);
    resolved = await resolveResponsesText(body, workerMessages, model, logTag, ROUTE, TIMEOUT);
  } catch (e) {
    return done(() => {
      const s = SEC.sanitizeUpstreamError(e, requestId);
      return openaiError(res, s.status, s.message, "upstream_error", req);
    });
  }
  const { result, status } = resolved;
  // Límite profesional Responses: max_output_tokens ?? max_tokens.
  const rspMax = effMaxTokens(body.max_output_tokens ?? body.max_tokens, 4096);
  const rspCap = applyMaxTokens(result.text, rspMax);
  if (rspCap.hit) trace(`${logTag} MAXTOKENS cut=${result.text.length}->${rspCap.text.length}`);
  result.text = rspCap.text;
  const rspStatus = rspCap.hit ? "incomplete" : status;
  const outputTokens = estTokens(result.text);
  // Puente agéntico centralizado (EXEC -> shell-block -> reintento).
  const toolCall = fnTools.length ? await resolveToolCall(result.text, fnTools, workerMessages, model, logTag) : null;
  if (toolCall) trace(`${logTag} TOOL-TRANSPORT name=${toolCall.name} (transport only, not executed)`);
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
  } else {
    outputItems = [{
      type: "message", id: "msg_" + rid(), status: rspStatus === "completed" ? "completed" : "incomplete",
      role: "assistant", content: [{ type: "output_text", text: result.text, annotations: [] }],
    }];
  }
  const outputItem = outputItems[outputItems.length - 1];

  if (!stream) {
    recordLatency(ROUTE, Date.now() - T0);
    usage[body.__tier].tokens += estTokens(result.text);
  trace(`${logTag} SERVED chars=${result.text.length} status=${rspStatus} toolCall=${toolCall ? toolCall.name : "-"} elapsed=${Date.now() - t0}ms`);
  try { body.__release(); } catch {}
    return sendJson(res, 200, {
      id: respId, object: "response", created, model,
      status: rspStatus, output: outputItems, parallel_tool_calls: true, tool_choice: "auto", tools: [],
      usage: { input_tokens: inputTokens, output_tokens: outputTokens, total_tokens: inputTokens + outputTokens },
    });
  }

  res.writeHead(200, sseHeaders(req));
  const stopHeartbeat = startHeartbeat(res, req, logTag);
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
  stopHeartbeat();
  recordLatency(ROUTE, Date.now() - T0);
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
  stopHeartbeat();
  recordLatency(ROUTE, Date.now() - T0);
  trace(`${logTag} SERVED chars=${result.text.length} status=${rspStatus} elapsed=${Date.now() - t0}ms`);
  try { body.__release(); } catch {}
  res.end();
}

async function handleMessages(req, res, body, requestId) {
  stats.requests++;
  const ROUTE = "anthropic";
  const TIMEOUT = routeTimeoutMs(ROUTE);
  const { harness, cap: HARNESS_CAP } = harnessCapFor(req);
  const routeErr = () => { stats.errors++; usage[tier].errors++; routeMetrics[ROUTE].errors++; };
  const anthropicOn = CFG.enableAnthropic.present ? !!CFG.enableAnthropic.value : true;
  if (!anthropicOn) {
    stats.errors++;
    return anthropicError(res, 404, "anthropic route disabled (FMP_ENABLE_ANTHROPIC=false)", req);
  }
  const tier = tierOf(req);
  const gl = GLOBAL_LIMITER.check(tier + ":messages");
  if (!gl.allowed) { stats.errors++; usage[tier].errors++; return limitResponse(res, { retryAfter: gl.retryAfter, reason: "rpm" }, requestId, req); }
  const limited = checkLimit(tier);
  if (limited) { usage[tier].errors++; return limitResponse(res, limited, requestId, req); }
  const release = takeSlot(tier);
  if (typeof body.model !== "string" || !stripPrefix(body.model)) {
    release();
    stats.errors++;
    return anthropicError(res, 400, "model is required: request must explicitly select an alias (no ambient fallback)", req);
  }
  let model = stripPrefix(body.model);
  if (!MODELS_LIB.isKnown(model)) {
    release();
    stats.errors++;
    return anthropicError(res, 404, `model '${body.model}' not found. Valid: ${[...MODEL_IDS].join(", ")}`, req);
  }
  // Safety Classifier fast-routing (FMP_CLASSIFIER_MODEL):
  // When Claude Code auto-mode sends a security monitor / safety classifier sub-prompt,
  // reroute to a low-latency model (e.g. claude-fable-5) to avoid classifier timeouts.
  const sysTextForClassifier = textOfContent(body.system).toLowerCase();
  const isClassifierReq = (harness === "claude-code" || req.headers["x-harness"] === "claude-code") &&
    CFG.classifierMatch.some((needle) => sysTextForClassifier.includes(needle));
  if (isClassifierReq && CFG.classifierModel && MODELS_LIB.isKnown(CFG.classifierModel)) {
    trace(`ANT/classifier reroute ${model} -> ${CFG.classifierModel} (fast classifier slot)`);
    model = CFG.classifierModel;
  }
  if (KILL_ALL || DISABLED_MODELS.has(model)) {
    release();
    stats.errors++;
    usage[tier].errors++;
    return anthropicError(res, 503, `model '${model}' unavailable (kill switch / disabled alias)`, req);
  }
  // Tool schemas validated BEFORE any upstream call (Phase 3, transport only).
  const antCheck = TOOLS_LIB.validateAnthropicTools(body.tools);
  if (!antCheck.ok) {
    release();
    stats.errors++;
    return anthropicError(res, 400, `invalid tools schema: ${antCheck.error}`, req);
  }
  if (body.tool_choice !== undefined && body.tool_choice !== null
      && typeof body.tool_choice !== "string" && typeof body.tool_choice !== "object") {
    release();
    stats.errors++;
    return anthropicError(res, 400, "invalid tool_choice: must be a string or object", req);
  }
  const antDefs = antCheck.defs;
  const maxTokens = Math.min(effMaxTokens(body.max_tokens, 1024), HARNESS_CAP);
  const workerMessages = toWorkerMessagesAnt(body.system, body.messages);
  if (!workerMessages.length) { release(); stats.errors++; return anthropicError(res, 400, "messages required", req); }
  const stream = body.stream === true;
  // Thinking Anthropic -> flag del worker. El worker razona en el texto;
  // FMP no fabrica bloques thinking (ver /health.thinking).
  const thinking = body.thinking !== undefined ? !!body.thinking : true;

  const msgId = "msg_" + rid();
  const inputTokens = estTokens(JSON.stringify(workerMessages));
  const logTag = `ANT/${model}/${msgId.slice(-6)}`;
  if (harness !== "unknown") trace(`${logTag} HARNESS=${harness} cap=${HARNESS_CAP}`);
  const T0 = Date.now();
  // Límite profesional (igual que OAI): worker ignora max_tokens.

  // Puente tool-use Anthropic: tools [{name, description, input_schema}]
  // (TRANSPORTE, no ejecución — el harness cliente ejecuta).
  const antTools = Array.isArray(body.tools) ? body.tools : [];
  const antChoice = body.tool_choice;
  const antDisabled = antChoice && (antChoice.type === "none" || antChoice === "none");
  const antToolSys = (antDefs.length && !antDisabled) ? agentSystemPrompt(antTools, model, "anthropic") : "";
  const combinedAntSys = [antToolSys, body.system].filter(Boolean);
  const wmAnt = toWorkerMessagesAnt(combinedAntSys, body.messages);

  const t0 = Date.now();
  trace(`${logTag} START-LIVE stream=${stream} tools=${antDefs.length} thinking=${thinking}`);
    const hasTools = antDefs.length && !antDisabled;
  const fullMode = body.stream_mode === "full" || (hasTools && stream);
  if (fullMode && stream) {
    let got;
    try {
      const r = await fetchCompleteText({ messages: wmAnt, modelId: model, thinking, deepSearch: false, signal: null, logTag, route: ROUTE, timeoutMs: TIMEOUT });
      got = r.complete ? r.text : (await (async () => {
        try {
          const ju = await callUpstream({ messages: wmAnt, modelId: model, stream: false, thinking, deepSearch: false, signal: null, timeoutMs: TIMEOUT, route: ROUTE });
          const jt = await readWorkerJson(ju);
          return jt.length >= r.text.length && jt.length > 0 ? jt : r.text;
        } catch { return r.text; }
      })());
    } catch (e) {
      routeErr();
      release();
      const s = SEC.sanitizeUpstreamError(e, requestId);
      return anthropicError(res, s.status, s.message, req);
    }
    recordLatency(ROUTE, Date.now() - T0);
    const capped = applyMaxTokens(got, maxTokens);
    if (capped.hit) trace(`${logTag} MAXTOKENS cut=${got.length}->${capped.text.length}`);
    const ftext = capped.hit ? capped.text : got;
    usage[tier].tokens += estTokens(ftext);

    const tc0 = hasTools ? await resolveToolCall(ftext, antDefs, wmAnt, model, logTag) : null;
    res.writeHead(200, sseHeaders(req));
    const fsend = (obj) => res.write("event: " + obj._ev + "\ndata: " + JSON.stringify(obj._d) + "\n\n");
    const fev = (name, d) => ({ _ev: name, _d: d });
    fsend(fev("message_start", { type: "message_start", message: { id: msgId, type: "message", role: "assistant", model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: inputTokens, output_tokens: 1 } } }));

    if (tc0) {
      let blockIdx = 0;
      if (tc0.prefixText) {
        fsend(fev("content_block_start", { type: "content_block_start", index: blockIdx, content_block: { type: "text", text: "" } }));
        const CH = 48;
        for (let i = 0; i < tc0.prefixText.length; i += CH) {
          fsend(fev("content_block_delta", { type: "content_block_delta", index: blockIdx, delta: { type: "text_delta", text: tc0.prefixText.slice(i, i + CH) } }));
        }
        fsend(fev("content_block_stop", { type: "content_block_stop", index: blockIdx }));
        blockIdx++;
      }
      const tuId = "toolu_" + rid().slice(0, 20);
      fsend(fev("content_block_start", { type: "content_block_start", index: blockIdx, content_block: { type: "tool_use", id: tuId, name: tc0.name, input: {} } }));
      fsend(fev("content_block_delta", { type: "content_block_delta", index: blockIdx, delta: { type: "input_json_delta", partial_json: tc0.args } }));
      fsend(fev("content_block_stop", { type: "content_block_stop", index: blockIdx }));
      fsend(fev("message_delta", { type: "message_delta", delta: { stop_reason: "tool_use", stop_sequence: null }, usage: { output_tokens: estTokens(ftext) } }));
      fsend(fev("message_stop", { type: "message_stop" }));
      trace(`${logTag} SERVED-TOOL-STREAM name=${tc0.name} elapsed=${Date.now() - t0}ms`);
    } else {
      const fstop = capped.hit ? "max_tokens" : "end_turn";
      fsend(fev("content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }));
      const CH = 48;
      for (let i = 0; i < ftext.length; i += CH) {
        fsend(fev("content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: ftext.slice(i, i + CH) } }));
      }
      fsend(fev("content_block_stop", { type: "content_block_stop", index: 0 }));
      fsend(fev("message_delta", { type: "message_delta", delta: { stop_reason: fstop, stop_sequence: null }, usage: { output_tokens: estTokens(ftext) } }));
      fsend(fev("message_stop", { type: "message_stop" }));
      trace(`${logTag} SERVED-FULL chars=${ftext.length} stop=${fstop} elapsed=${Date.now() - t0}ms`);
    }
    release();
    return res.end();
  }

  if (!stream) {
    let got;
    try {
      const r = await fetchCompleteText({ messages: wmAnt, modelId: model, thinking, deepSearch: false, signal: null, logTag, route: ROUTE, timeoutMs: TIMEOUT });
      got = r.complete ? r.text : (await (async () => {
        try {
          const ju = await callUpstream({ messages: wmAnt, modelId: model, stream: false, thinking, deepSearch: false, signal: null, timeoutMs: TIMEOUT, route: ROUTE });
          const jt = await readWorkerJson(ju);
          return jt.length >= r.text.length && jt.length > 0 ? jt : r.text;
        } catch { return r.text; }
      })());
    } catch (e) {
      routeErr();
      release();
      const s = SEC.sanitizeUpstreamError(e, requestId);
      return anthropicError(res, s.status, s.message, req);
    }
    recordLatency(ROUTE, Date.now() - T0);
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
      // Solo nombre de tool en logs — nunca argumentos (Phase 3).
      trace(`${logTag} TOOL-TRANSPORT name=${tc0.name} (transport only, not executed)`);
    } else {
      content0 = [{ type: "text", text: got }];
    }
    release();
    if (req.headers["x-fmp-meta"] === "1") {
      return sendJson(res, 200, {
        id: msgId, type: "message", role: "assistant", model,
        content: content0,
        stop_reason: stop0,
        stop_sequence: null,
        usage: { input_tokens: inputTokens, output_tokens: estTokens(got) },
        _meta: routeMeta(model, "anthropic"),
      }, req);
    }
    return sendJson(res, 200, {
      id: msgId, type: "message", role: "assistant", model,
      content: content0,
      stop_reason: stop0,
      stop_sequence: null,
      usage: { input_tokens: inputTokens, output_tokens: estTokens(got) },
    }, req);
  }

  // STREAM EN VIVO Anthropic: text_delta en cuanto llega + resume.
  res.writeHead(200, sseHeaders(req));
  const stopHeartbeat = startHeartbeat(res, req, logTag);
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
      upstream = await callUpstream({ messages: wmAnt, modelId: model, stream: true, thinking, deepSearch: false, signal: ctrl.signal, timeoutMs: TIMEOUT, route: ROUTE });
    } catch (e) {
      if (emitted || a === MAX_RETRIES) {
        // Explicit stream error (never silent): honest close below carries
        // max_tokens/end_turn; trace records the failure with request id.
        if (!closed && !emitted) trace(`${logTag} LIVE FAILED request_id=${requestId}`);
        break;
      }
      stats.retries++;
      routeMetrics[ROUTE].retries++;
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
        // Intra-intento: emisión directa, igual que OAI (ver nota arriba).
        emitAnt(fresh);
        fresh = "";
        if (cappedHit) { diverged = true; complete = true; }
      }, ctrl.signal);
      if (diverged) {
        if (cappedHit) {
          trace(`${logTag} LIVE CAPPED emitted=${emitted.length} -> close length`);
        } else {
          stats.truncatedUpstream++;
          routeMetrics[ROUTE].truncations++;
          trace(`${logTag} LIVE DIVERGED emitted=${emitted.length} -> close honest`);
        }
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
    routeMetrics[ROUTE].truncations++;
    if (!emitted) {
      trace(`${logTag} LIVE TRUNCATED attempt=${attempts} nothing-emitted -> retry`);
      stats.retries++;
      routeMetrics[ROUTE].retries++;
      if (a < MAX_RETRIES) await sleep(RETRY_BASE_MS * (a + 1));
      continue;
    }
    // CONTINUATION (stability-006): upstream cut mid-stream but the client
    // is still attached. Re-request rounds with the emitted tail as context
    // and keep the SAME SSE block open — the SDK sees one continuous stream
    // ending in end_turn, not a max_tokens stop (which it misreads as a
    // 128k overflow error). Bounded: MAX_RETRIES total rounds. Honest
    // max_tokens close only when rounds exhaust. No Frankenstein:
    // continuation text is appended only on tail-overlap (dedupeAppend),
    // a divergent round closes honestly instead of merging.
    if (a < MAX_RETRIES) {
      let continued = false;
      for (let cr = 0; cr < MAX_RETRIES && !closed && !cappedHit; cr++) {
        trace(`${logTag} LIVE CONTINUE attempt=${attempts}.${cr} emitted=${emitted.length}`);
        stats.retries++;
        routeMetrics[ROUTE].retries++;
        const tail = emitted.slice(-500);
        const contMsgs = wmAnt.concat([
          { role: "assistant", content: tail },
          { role: "user", content: "Continúa exactamente donde quedaste, sin repetir ni resumir." },
        ]);
        let cont;
        try {
          const contUp = await callUpstream({ messages: contMsgs, modelId: model, stream: true, thinking, deepSearch: false, signal: ctrl.signal, timeoutMs: TIMEOUT, route: ROUTE });
          // Collect WITHOUT emitting: merge decision first, emit once.
          // (Emitting inside the callback then slicing merged text would
          // always yield empty rest — the merge must precede emission.)
          cont = await readWorkerSSELive(contUp, null, ctrl.signal);
        } catch (e) {
          trace(`${logTag} LIVE CONTINUE-FAIL err=${String((e && e.message) || e).slice(0, 120)} -> close honest`);
          break;
        }
        const merged = dedupeAppend(emitted, cont.text);
        const novel = merged.slice(emitted.length);
        if (merged.length <= emitted.length + 8 && !cont.done) {
          // No progress and no DONE: further rounds would loop.
          trace(`${logTag} LIVE CONTINUE-STALLED total=${emitted.length} -> close honest`);
          break;
        }
        if (novel && !closed) {
          if (cappedHit) break;
          emitted += novel;
          for (let k = 0; k < novel.length; k += 48) {
            send(ev("content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: novel.slice(k, k + 48) } }));
          }
        }
        if (cont.done) {
          complete = !cappedHit;
          continued = true;
          trace(`${logTag} LIVE CONTINUE-OK total=${emitted.length}`);
          break;
        }
        stats.truncatedUpstream++;
        routeMetrics[ROUTE].truncations++;
        trace(`${logTag} LIVE CONTINUE-CUT total=${emitted.length} -> next round`);
        await sleep(RETRY_BASE_MS);
      }
      if (continued) break;
      trace(`${logTag} LIVE CONTINUE-EXHAUSTED total=${emitted.length} -> close honest`);
    } else {
      trace(`${logTag} LIVE TRUNCATED attempt=${attempts} emitted=${emitted.length} -> close honest`);
    }
    break;
    } catch (e) {
      if (emitted || a === MAX_RETRIES) break;
      stats.retries++;
      routeMetrics[ROUTE].retries++;
      trace(`${logTag} LIVE read-error attempt=${attempts} err=${e.message}`);
      await sleep(RETRY_BASE_MS * (a + 1));
    }
  }
  stopHeartbeat();
  recordLatency(ROUTE, Date.now() - T0);
  if (!closed) {
    // Sin rescate por continuación (ver nota OAI): cierre honesto.
    // Cap de max_tokens -> max_tokens (como API nativa).
    let antFinal = cappedHit ? "max_tokens" : (complete ? "end_turn" : "max_tokens");
    // Puente tool-use post-stream: solo si el worker emitió EXEC explícito.
    let tcLive = antDefs.length && !antDisabled ? parseToolCall(emitted, antDefs) : null;
    if (tcLive) {
      let input = {};
      try { input = JSON.parse(tcLive.args); } catch { input = {}; }
      const tuId = "toolu_" + rid().slice(0, 20);
      send(ev("content_block_stop", { type: "content_block_stop", index: 0 }));
      send(ev("content_block_start", { type: "content_block_start", index: 1, content_block: { type: "tool_use", id: tuId, name: tcLive.name, input: {} } }));
      send(ev("content_block_delta", { type: "content_block_delta", index: 1, delta: { type: "input_json_delta", partial_json: JSON.stringify(input) } }));
      send(ev("content_block_stop", { type: "content_block_stop", index: 1 }));
      send(ev("message_delta", { type: "message_delta", delta: { stop_reason: "tool_use", stop_sequence: null }, usage: { output_tokens: estTokens(emitted) } }));
      trace(`${logTag} LIVE TOOL-TRANSPORT name=${tcLive.name} (transport only, not executed)`);
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

// API key gate (Phase 4): every non-health endpoint requires a valid
// key when FMP_API_KEYS is configured. Health stays public so
// orchestrators and `fmp health` keep working. Local-open mode
// (no keys configured) passes everything on localhost.
function authGate(req, res, requestId) {
  if (CFG.authMode === "none") return true;
  if (!CFG.apiKeys.length) return true;
  if (!SEC.isAuthorized(req, CFG.apiKeys)) {
    stats.errors++;
    sendJson(res, 401, {
      error: { message: "missing or invalid API key (Authorization: Bearer <key> or x-api-key)", type: "authentication_error", code: "invalid_api_key" },
    }, req);
    return false;
  }
  return true;
}

const server = http.createServer(async (req, res) => {
  const requestId = requestIdOf(req);
  try {
    const u = new URL(req.url || "/", "http://x");
    const path = u.pathname.replace(/\/+$/, "") || "/";

    if (req.method === "OPTIONS") {
      // CORS deny-by-default: reflect only explicitly allowed origins.
      const allow = corsOriginFor(req);
      const headers = {
        "access-control-allow-methods": "GET,POST,OPTIONS",
        "access-control-allow-headers": "content-type,authorization,x-api-key,anthropic-version",
      };
      if (allow) headers["access-control-allow-origin"] = allow;
      res.writeHead(204, headers);
      return res.end();
    }
    if (req.method === "GET" && (path === "/health" || path === "/")) return handleHealth(res, req);
    // Non-health endpoints: auth gate first.
    if (req.method === "GET" && (path === "/v1/models" || path === "/models")) {
      if (!authGate(req, res, requestId)) return;
      return handleModels(res, req);
    }
    if (req.method === "GET" && (path === "/stats" || path === "/v1/stats")) {
      if (!authGate(req, res, requestId)) return;
      return handleStats(res, req);
    }
    if (req.method === "GET" && (path === "/v1/usage" || path === "/usage")) {
      if (!authGate(req, res, requestId)) return;
      return handleUsage(res, req);
    }

    if (req.method === "POST" && (path === "/v1/chat/completions" || path === "/chat/completions")) {
      if (!authGate(req, res, requestId)) return;
      let body;
      try { body = JSON.parse(await readBody(req)); }
      catch { stats.errors++; return sendJson(res, 400, { error: { message: "invalid JSON body", type: "invalid_request_error", code: "bad_request" } }, req); }
      return handleChatCompletions(req, res, body, requestId);
    }
    if (req.method === "POST" && (path === "/v1/messages" || path === "/messages")) {
      if (!authGate(req, res, requestId)) return;
      let body;
      try { body = JSON.parse(await readBody(req)); }
      catch { stats.errors++; return anthropicError(res, 400, "invalid JSON body", req); }
      return handleMessages(req, res, body, requestId);
    }
    if (req.method === "POST" && (path === "/v1/responses" || path === "/responses")) {
      if (!authGate(req, res, requestId)) return;
      let body;
      try { body = JSON.parse(await readBody(req)); }
      catch { stats.errors++; return sendJson(res, 400, { error: { message: "invalid JSON body", type: "invalid_request_error", code: "bad_request" } }, req); }
      return handleResponses(req, res, body, requestId);
    }
    return sendJson(res, 404, { error: "not found: " + path }, req);
  } catch (e) {
    // Never leak internals: sanitized 500 with correlation id.
    stats.errors++;
    try { sendJson(res, 500, { error: { message: `internal error (request_id: ${requestId})`, type: "server_error", code: "internal_error" } }, req); } catch {}
  }
});

// Graceful shutdown (Phase 4): stop accepting, drain in-flight, then exit.
// In-flight records are request-scoped (no shared file writes), so drain
// cannot corrupt state; worst case a client sees a truncated stream.
let shuttingDown = false;
function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.error(`[fmp-provider] ${signal}: draining, no new connections...`);
  server.close(() => {
    console.error("[fmp-provider] drained, exiting.");
    process.exit(0);
  });
  // Hard cap: force exit if drain stalls (in-flight streams only).
  setTimeout(() => {
    console.error("[fmp-provider] drain timeout, forcing exit.");
    process.exit(0);
  }, 15_000).unref();
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

server.listen(PORT, HOST, () => {
  console.log(`[fmp-provider] v${MODELS_LIB.VERSION} listening on http://${HOST}:${PORT} (localhost-only${CFG.apiKeys.length ? ", api_key auth on" : ", local-open mode"})`);
});
