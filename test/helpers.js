// Test helpers: spawn fixture upstream + FMP on localhost, fixture only.
"use strict";
const { spawn } = require("node:child_process");
const path = require("node:path");

const ROOT = path.join(__dirname, "..");

function waitFor(url, timeoutMs) {
  const t0 = Date.now();
  return new Promise((resolve, reject) => {
    const tick = async () => {
      try {
        const r = await fetch(url);
        if (r.ok) return resolve();
      } catch {}
      if (Date.now() - t0 > (timeoutMs || 8000)) return reject(new Error("timeout waiting " + url));
      setTimeout(tick, 100);
    };
    tick();
  });
}

async function spawnFixture(port, mode) {
  const child = spawn("node", [path.join(ROOT, "test", "fixture-upstream.js")], {
    env: Object.assign({}, process.env, { FIXTURE_PORT: String(port), FIXTURE_MODE: mode }),
    stdio: ["ignore", "pipe", "pipe"],
  });
  await waitFor(`http://127.0.0.1:${port}/`);
  return child;
}

async function spawnFmp(port, extraEnv) {
  const child = spawn("node", [path.join(ROOT, "server.js")], {
    env: Object.assign({}, process.env, {
      FMP_PORT: String(port),
      FMP_HOST: "127.0.0.1",
      FMP_LOG_PROMPTS: "false",
      FMP_MAX_RETRIES: "1",
      FMP_RETRY_BASE_MS: "10",
    }, extraEnv || {}),
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stderr = "";
  child.stderr.on("data", (c) => { stderr += c; });
  child.stderrText = () => stderr;
  await waitFor(`http://127.0.0.1:${port}/health`);
  return child;
}

function kill(child) {
  return new Promise((resolve) => {
    if (!child || child.exitCode !== null) return resolve(child ? child.exitCode : null);
    const t = setTimeout(() => { try { child.kill("SIGKILL"); } catch {} }, 5000);
    child.on("exit", (code) => { clearTimeout(t); resolve(code); });
    try { child.kill("SIGTERM"); } catch { resolve(null); }
  });
}

async function post(port, p, body, key, extraHeaders) {
  const headers = Object.assign({ "content-type": "application/json" }, extraHeaders || {});
  if (key) headers.authorization = "Bearer " + key;
  const r = await fetch(`http://127.0.0.1:${port}${p}`, { method: "POST", headers, body: JSON.stringify(body) });
  let j = null;
  try { j = await r.json(); } catch {}
  return { status: r.status, body: j, headers: r.headers };
}

async function get(port, p, key) {
  const headers = {};
  if (key) headers.authorization = "Bearer " + key;
  const r = await fetch(`http://127.0.0.1:${port}${p}`, { headers });
  let j = null;
  try { j = await r.json(); } catch {}
  return { status: r.status, body: j, headers: r.headers };
}

module.exports = { ROOT, waitFor, spawnFixture, spawnFmp, kill, post, get };
