"use strict";
// FMP configuration contract (v2.2.0).
//
// Single source of truth for every FMP_* / legacy env var. Load with
// loadConfig(), refuse startup when validate() reports errors.
//
// Security posture: localhost-only default. A non-localhost bind is
// refused unless FMP_ALLOW_PUBLIC_BIND=true is set explicitly, in
// which case a warning is always emitted.

const LOCALHOSTS = new Set(["127.0.0.1", "::1", "::ffff:127.0.0.1", "localhost"]);

function parseBoolStrict(v) {
  if (v === undefined || v === null || String(v).trim() === "") return { present: false, value: undefined };
  const s = String(v).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(s)) return { present: true, value: true };
  if (["0", "false", "no", "off"].includes(s)) return { present: true, value: false };
  return { present: true, value: undefined, invalid: true };
}

function parseNum(v, dflt) {
  if (v === undefined || v === null || String(v).trim() === "") return dflt;
  const n = Number(v);
  return Number.isFinite(n) ? n : NaN;
}

// "hermes:4096,opencode:8192" -> { hermes: 4096, opencode: 8192 }.
// Unknown harness names are kept (matched later); invalid entries dropped.
function parseHarnessLimits(v) {
  const out = {};
  if (v === undefined || v === null || String(v).trim() === "") return out;
  for (const part of String(v).split(",")) {
    const [name, num] = part.split(":").map((s) => (s || "").trim().toLowerCase());
    const n = Number(num);
    if (name && Number.isInteger(n) && n > 0 && n <= 1_000_000) out[name] = n;
  }
  return out;
}

// Known harness identifiers. Matching is substring on the `x-harness`
// header first, then on user-agent (both lowercased).
const KNOWN_HARNESSES = ["hermes", "opencode", "claude-code", "antigravity", "codex"];

function harnessOf(req) {
  const headers = (req && req.headers) || {};
  const explicit = String(headers["x-harness"] || "").trim().toLowerCase();
  if (explicit) {
    if (KNOWN_HARNESSES.includes(explicit)) return explicit;
    return "unknown";
  }
  const ua = String(headers["user-agent"] || "").toLowerCase();
  // Claude Code SDK sends `claude-cli/<ver> (external, sdk-cli)`.
  if (ua.includes("claude-cli") || ua.includes("claude-code")) return "claude-code";
  for (const h of KNOWN_HARNESSES) {
    if (h === "claude-code") continue; // handled above (dash + no-dash)
    if (ua.includes(h.replace("-", "")) || ua.includes(h)) return h;
  }
  return "unknown";
}

function isLocalHost(host) {
  const h = String(host || "").trim().replace(/^\[(.*)\]$/, "$1").toLowerCase();
  return LOCALHOSTS.has(h);
}

function isPublicBind(host) {
  const h = String(host || "").trim().replace(/^\[(.*)\]$/, "$1").toLowerCase();
  return h === "0.0.0.0" || h === "::" || (!isLocalHost(h) && h !== "");
}

const VALID_AUTH_MODES = new Set(["api_key", "none"]);
const VALID_LOG_LEVELS = new Set(["debug", "info", "warn", "error"]);

function loadConfig(env) {
  env = env || process.env;
  const b = (k, dflt) => parseBoolStrict(env[k]);
  const cfg = {
    host: String(env.FMP_HOST || env.HOST || "127.0.0.1").trim(),
    port: parseNum(env.FMP_PORT !== undefined && env.FMP_PORT !== "" ? env.FMP_PORT : env.PORT, 2089),
    authMode: String(env.FMP_AUTH_MODE || "api_key").trim().toLowerCase(),
    apiKeys: String(env.FMP_API_KEYS || "").split(",").map((s) => s.trim()).filter(Boolean),
    logLevel: String(env.FMP_LOG_LEVEL || "info").trim().toLowerCase(),
    logPrompts: b("FMP_LOG_PROMPTS", false),
    rateLimitRpm: parseNum(env.FMP_RATE_LIMIT_RPM, 30),
    maxRequestBytes: parseNum(env.FMP_MAX_REQUEST_BYTES, 10 * 1024 * 1024),
    maxRetries: parseNum(env.FMP_MAX_RETRIES, 5),
    upstreamTimeoutMs: parseNum(
      env.FMP_UPSTREAM_TIMEOUT_MS !== undefined && env.FMP_UPSTREAM_TIMEOUT_MS !== ""
        ? env.FMP_UPSTREAM_TIMEOUT_MS : env.FMP_TIMEOUT_MS, 120_000),
    // Per-route upstream timeouts (ms); each falls back to the global above.
    timeoutOpenaiMs: parseNum(env.FMP_TIMEOUT_OPENAI_MS, null),
    timeoutAnthropicMs: parseNum(env.FMP_TIMEOUT_ANTHROPIC_MS, null),
    timeoutResponsesMs: parseNum(env.FMP_TIMEOUT_RESPONSES_MS, null),
    // SSE heartbeat interval (ms): `: ping` comments keep idle streams alive
    // through proxies. 0 disables. Default 15s.
    heartbeatMs: parseNum(env.FMP_HEARTBEAT_MS, 15_000),
    // Per-harness output token caps: "hermes:4096,opencode:8192,...".
    // Matched from `x-harness` header, else user-agent substring. Documented
    // in docs/harness-limits.md. Unknown harnesses use the global ceil.
    harnessLimits: parseHarnessLimits(env.FMP_HARNESS_LIMITS),
    retryBaseMs: parseNum(env.FMP_RETRY_BASE_MS, 1500),
    enableOpenAI: b("FMP_ENABLE_OPENAI", true),
    enableAnthropic: b("FMP_ENABLE_ANTHROPIC", true),
    enableResponses: b("FMP_ENABLE_RESPONSES", false),
    toolMode: String(env.FMP_TOOL_MODE || "transport_only").trim().toLowerCase(),
    allowPublicBind: b("FMP_ALLOW_PUBLIC_BIND", false),
    upstream: String(env.FMP_UPSTREAM || "https://freemodels-chat.freemodels.workers.dev").trim(),
    disabledModels: String(env.FMP_DISABLED_MODELS || "").split(",").map((s) => s.trim()).filter(Boolean),
    killAll: b("FMP_KILL_ALL", false),
    corsOrigins: String(env.FMP_CORS_ALLOW_ORIGINS || "").split(",").map((s) => s.trim()).filter(Boolean),
    traceFile: String(env.FMP_TRACE || env.SHIM_TRACE || "").trim(),
    maxTokensCeil: parseNum(env.FMP_MAX_TOKENS_CEIL, 8192),
    classifierModel: String(env.FMP_CLASSIFIER_MODEL || "").trim(),
    classifierMatch: String(env.FMP_CLASSIFIER_MATCH || "security monitor,safety classifier").split(",").map((s) => s.trim().toLowerCase()).filter(Boolean),
    // Legacy tier knobs (kept for backward compatibility, documented in config.example.yaml).
    legacyKeys: String(env.FMP_KEYS || ""),
    freeRpm: parseNum(env.FMP_FREE_RPM, 10),
    freeConc: parseNum(env.FMP_FREE_CONC, 2),
    proRpm: parseNum(env.FMP_PRO_RPM, 120),
    proConc: parseNum(env.FMP_PRO_CONC, 16),
  };
  return cfg;
}

function validateConfig(cfg) {
  const errors = [];
  const warnings = [];

  if (!Number.isInteger(cfg.port) || cfg.port < 1 || cfg.port > 65535) {
    errors.push(`FMP_PORT out of range (1-65535): ${cfg.port}`);
  }
  if (!VALID_AUTH_MODES.has(cfg.authMode)) {
    errors.push(`FMP_AUTH_MODE must be one of api_key|none, got '${cfg.authMode}'`);
  }
  if (!VALID_LOG_LEVELS.has(cfg.logLevel)) {
    errors.push(`FMP_LOG_LEVEL must be one of debug|info|warn|error, got '${cfg.logLevel}'`);
  }
  for (const [key, obj] of [
    ["FMP_LOG_PROMPTS", cfg.logPrompts],
    ["FMP_ENABLE_OPENAI", cfg.enableOpenAI],
    ["FMP_ENABLE_ANTHROPIC", cfg.enableAnthropic],
    ["FMP_ENABLE_RESPONSES", cfg.enableResponses],
    ["FMP_TOOL_MODE", null],
    ["FMP_ALLOW_PUBLIC_BIND", cfg.allowPublicBind],
    ["FMP_KILL_ALL", cfg.killAll],
  ]) {
    if (obj && obj.invalid) errors.push(`${key} is not a boolean (got '${process.env[key]}')`);
  }
  if (!Number.isFinite(cfg.rateLimitRpm) || cfg.rateLimitRpm < 0) {
    errors.push(`FMP_RATE_LIMIT_RPM must be a number >= 0 (0 disables), got '${cfg.rateLimitRpm}'`);
  }
  if (!Number.isFinite(cfg.maxRequestBytes) || cfg.maxRequestBytes <= 0) {
    errors.push(`FMP_MAX_REQUEST_BYTES must be a positive number of bytes, got '${cfg.maxRequestBytes}'`);
  }
  if (!Number.isInteger(cfg.maxRetries) || cfg.maxRetries < 0 || cfg.maxRetries > 20) {
    errors.push(`FMP_MAX_RETRIES must be an integer 0-20, got '${cfg.maxRetries}'`);
  }
  if (!Number.isFinite(cfg.upstreamTimeoutMs) || cfg.upstreamTimeoutMs <= 0) {
    errors.push(`FMP_UPSTREAM_TIMEOUT_MS must be positive milliseconds, got '${cfg.upstreamTimeoutMs}'`);
  }
  for (const [key, val] of [
    ["FMP_TIMEOUT_OPENAI_MS", cfg.timeoutOpenaiMs],
    ["FMP_TIMEOUT_ANTHROPIC_MS", cfg.timeoutAnthropicMs],
    ["FMP_TIMEOUT_RESPONSES_MS", cfg.timeoutResponsesMs],
  ]) {
    if (val !== null && val !== undefined && (!Number.isFinite(val) || val <= 0)) {
      errors.push(`${key} must be positive milliseconds when set, got '${val}'`);
    }
  }
  if (!Number.isFinite(cfg.heartbeatMs) || cfg.heartbeatMs < 0 || cfg.heartbeatMs > 300_000) {
    errors.push(`FMP_HEARTBEAT_MS must be 0-300000 ms, got '${cfg.heartbeatMs}'`);
  }
  // Transport-only is the only supported tool mode. Anything else refuses startup.
  if (cfg.toolMode !== "transport_only") {
    errors.push(`FMP_TOOL_MODE='${cfg.toolMode}' refused: FMP only transports tool schemas, it never executes tools`);
  }

  // Localhost guard: non-localhost bind requires explicit override.
  const allowPublic = cfg.allowPublicBind.present ? cfg.allowPublicBind.value : false;
  if (isPublicBind(cfg.host) || !isLocalHost(cfg.host)) {
    if (!allowPublic) {
      errors.push(
        `refusing to bind '${cfg.host}': FMP defaults to localhost-only. ` +
        `Set FMP_ALLOW_PUBLIC_BIND=true to override (documented warning applies).`);
    } else {
      warnings.push(
        `WARNING: binding '${cfg.host}' publicly (FMP_ALLOW_PUBLIC_BIND=true). ` +
        `Do not expose FMP to the public internet; private development use only.`);
    }
  }

  if (cfg.authMode === "api_key" && cfg.apiKeys.length === 0) {
    warnings.push("FMP_AUTH_MODE=api_key but FMP_API_KEYS is empty: running in local-open mode (localhost only). Set keys before any shared use.");
  }
  if (cfg.authMode === "none") {
    warnings.push("FMP_AUTH_MODE=none: authentication disabled. Acceptable only on localhost for local development.");
  }
  if (cfg.logPrompts.present && cfg.logPrompts.value === true) {
    warnings.push("FMP_LOG_PROMPTS=true: request/response bodies may be logged. Never enable with sensitive data.");
  }
  if (cfg.enableResponses.present && cfg.enableResponses.value === true) {
    warnings.push("FMP_ENABLE_RESPONSES=true: Responses API is experimental and disabled by default.");
  }
  if (cfg.killAll.present && cfg.killAll.value === true) {
    warnings.push("FMP_KILL_ALL=true: kill switch engaged, all inference endpoints return 503.");
  }
  if (cfg.disabledModels.length) {
    warnings.push(`disabled aliases: ${cfg.disabledModels.join(", ")}`);
  }

  return { errors, warnings };
}

// Safe-to-log copy: never print actual keys.
function redactConfig(cfg) {
  return Object.assign({}, cfg, {
    apiKeys: cfg.apiKeys.length ? [`*** (${cfg.apiKeys.length} configured, redacted)`] : [],
  });
}

module.exports = {
  LOCALHOSTS, loadConfig, validateConfig, redactConfig,
  isLocalHost, isPublicBind, parseBoolStrict, parseNum,
  parseHarnessLimits, harnessOf, KNOWN_HARNESSES,
};
