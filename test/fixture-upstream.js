// Fixture upstream worker for FMP tests. No network, no secrets.
// Env: FIXTURE_PORT, FIXTURE_MODE = ok | exec | truncate-once | always-truncate | error-secret
"use strict";
const http = require("node:http");

const PORT = Number(process.env.FIXTURE_PORT || 23301);
const MODE = String(process.env.FIXTURE_MODE || "ok");
let streamCount = 0;

function sseChunk(res, text) {
  res.write("data: " + JSON.stringify({ choices: [{ delta: { content: text } }] }) + "\n\n");
}

const server = http.createServer((req, res) => {
  if (req.method === "GET") {
    res.writeHead(200, { "content-type": "application/json" });
    return res.end(JSON.stringify({ ok: true, fixture: true }));
  }
  let body = "";
  req.on("data", (c) => { body += c; });
  req.on("end", () => {
    let parsed = {};
    try { parsed = JSON.parse(body); } catch {}
    const stream = parsed.stream === true;

    if (MODE === "error-secret") {
      res.writeHead(500, { "content-type": "application/json" });
      return res.end(JSON.stringify({ error: "boom session=sk-live-secret-123 cookie=abc" }));
    }

    if (!stream) {
      res.writeHead(200, { "content-type": "application/json" });
      if (MODE === "exec") return res.end(JSON.stringify({ content: "claro\nEXEC:::calc ARGS {\"a\":1}" }));
      if (MODE === "always-truncate") return res.end(JSON.stringify({ content: "fixture json fallback reply" }));
      return res.end(JSON.stringify({ content: "fixture json reply" }));
    }

    // stream: SSE
    res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache", connection: "keep-alive" });
    streamCount++;
    if (MODE === "truncate-once" && streamCount === 1) {
      sseChunk(res, "Hello ");
      return res.end(); // truncated: no DONE
    }
    if (MODE === "always-truncate") {
      sseChunk(res, "Hello ");
      return res.end(); // truncated: no DONE
    }
    if (MODE === "exec") {
      sseChunk(res, "claro\nEXEC:::calc ARGS {\"a\":1}");
      res.write("data: [DONE]\n\n");
      return res.end();
    }
    sseChunk(res, "Hello ");
    sseChunk(res, "world");
    res.write("data: [DONE]\n\n");
    return res.end();
  });
});

server.listen(PORT, "127.0.0.1", () => console.log(`[fixture] ${MODE} on 127.0.0.1:${PORT}`));
