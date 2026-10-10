"use strict";
// FMP security helpers (v2.2.0): redaction, auth extraction, rate limiting.
//
// Rules enforced here:
// - credentials and secret-like strings never reach logs or client errors;
// - upstream error detail is sanitized to status + request id (fail closed);
// - API key gate applies to every non-health endpoint when keys configured.

const crypto = require("node:crypto");

const REDACTED = "[REDACTED]";

// Redaction for secret-like material. Applied to any string before
// logging or before embedding it in a client-facing error. Each rule
// replaces the WHOLE secret (never echoes a prefix of it).
function redactSecrets(s) {
  if (typeof s !== "string") return s;
  let out = s;
  // sk-... style tokens (OpenAI-like, 9Router-like, generic long tokens)
  out = out.replace(/\bsk-[A-Za-z0-9_\-./]{4,}\b/g, REDACTED);
  // Bearer / Basic credentials (keep scheme label, drop credential)
  out = out.replace(/\b(bearer|basic)(\s+)[^\s"']+/gi, (_, scheme, sp) => scheme + sp + REDACTED);
  // key=value pairs in URLs / headers / dumps (keep key label, drop value)
  out = out.replace(/((?:api[_-]?key|access[_-]?token|secret|passwd|password|cookie|set-cookie|session|sessionid|authorization|x-api-key)\s*[:=]\s*["']?)([^"'\s;,}]+)/gi,
    (_, label) => label + REDACTED);
  // query-string secrets (keep param name, drop value)
  out = out.replace(/([?&](?:key|token|secret|auth|session)[^=]*=)([^&\s"']+)/gi,
    (_, label) => label + REDACTED);
  return out;
}

function looksLikeSecret(s) {
  if (typeof s !== "string") return false;
  const t = s.trim();
  if (/^sk-[A-Za-z0-9_\-./]{8,}$/.test(t)) return true;
  if (/^(Bearer|Basic)\s+\S+/i.test(t)) return true;
  if (t.length >= 32 && /^[A-Za-z0-9_\-./=+:]+$/.test(t) && /[A-Za-z]/.test(t) && /[0-9]/.test(t)) return true;
  return false;
}

// Client-facing upstream failure: status + request id only. Raw upstream
// bodies/headers/cookies must never cross this boundary.
function sanitizeUpstreamError(err, requestId) {
  const status = err && Number.isInteger(err.status) ? err.status : 502;
  return {
    status: status >= 400 && status < 600 ? status : 502,
    message: `upstream request failed (request_id: ${requestId || "n/a"})`,
  };
}

function extractKey(req) {
  const h = (req.headers && (req.headers.authorization || req.headers["x-api-key"])) || "";
  return String(h).replace(/^bearer\s+/i, "").trim();
}

// Constant-time compare to avoid timing oracles on key checks.
function keyMatches(provided, expected) {
  const a = Buffer.from(String(provided));
  const b = Buffer.from(String(expected));
  if (a.length !== b.length) return false;
  try {
    return crypto.timingSafeEqual(a, b);
  } catch {
    return false;
  }
}

function isAuthorized(req, apiKeys) {
  if (!apiKeys || !apiKeys.length) return true; // local-open mode
  const k = extractKey(req);
  if (!k) return false;
  return apiKeys.some((expected) => keyMatches(k, expected));
}

// Fixed-window per-bucket rate limiter. rpm=0 disables.
class RateLimiter {
  constructor(rpm) {
    this.rpm = rpm;
    this.hits = new Map(); // bucket -> [timestamps]
  }
  check(bucket) {
    if (!this.rpm || this.rpm <= 0) return { allowed: true, retryAfter: 0 };
    const now = Date.now();
    let arr = this.hits.get(bucket) || [];
    arr = arr.filter((t) => now - t < 60_000);
    if (arr.length >= this.rpm) {
      const retryAfter = Math.max(1, 60 - Math.floor((now - arr[0]) / 1000));
      this.hits.set(bucket, arr);
      return { allowed: false, retryAfter };
    }
    arr.push(now);
    this.hits.set(bucket, arr);
    return { allowed: true, retryAfter: 0 };
  }
}

module.exports = {
  REDACTED, redactSecrets, looksLikeSecret,
  sanitizeUpstreamError, extractKey, keyMatches, isAuthorized, RateLimiter,
};
