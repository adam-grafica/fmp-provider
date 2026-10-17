"use strict";
const fs = require("fs");
const f = process.argv[2];
const lines = fs.readFileSync(f, "utf8").split("\n").filter(Boolean);
let turn = 0;
for (const l of lines) {
  let e; try { e = JSON.parse(l); } catch { continue; }
  if (e.type === "assistant" && e.message) {
    turn++;
    for (const c of e.message.content || []) {
      if (c.type === "tool_use") {
        console.log(`T${turn} tool_use ${c.name} id=${c.id} input=${JSON.stringify(c.input).slice(0, 120)}`);
      } else if (c.type === "text") {
        console.log(`T${turn} text: ${c.text.replace(/\n/g, " ").slice(0, 200)}`);
      }
    }
  } else if (e.type === "user" && e.message) {
    for (const c of e.message.content || []) {
      if (c.type === "tool_result") {
        const t = typeof c.content === "string" ? c.content : JSON.stringify(c.content);
        console.log(`   result ${c.tool_use_id} is_error=${!!c.is_error} :: ${t.replace(/\n/g, " ").slice(0, 140)}`);
      }
    }
  } else if (e.type === "result") {
    console.log(`FINAL subtype=${e.subtype} is_error=${e.is_error} num_turns=${e.num_turns} duration_ms=${e.duration_ms} ttft_stream_ms=${e.ttft_stream_ms} result="${(e.result || "").replace(/\n/g, " ").slice(0, 220)}"`);
  }
}
