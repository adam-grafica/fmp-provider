"use strict";
const fs = require("fs");
const f = process.argv[2];
const lines = fs.readFileSync(f, "utf8").split("\n").filter(Boolean);
for (const l of lines) {
  let e; try { e = JSON.parse(l); } catch { continue; }
  if (e.type === "tool_use" && e.part) {
    const p = e.part;
    const st = p.state || {};
    const inp = JSON.stringify(st.input || {}).slice(0, 120);
    const out = String(st.output || "").replace(/\n/g, " ").slice(0, 120);
    console.log(`tool_use ${p.tool} callID=${p.callID} status=${st.status} exit=${st.metadata ? st.metadata.exit : "?"} input=${inp} output="${out}"`);
  } else if (e.type === "text" && e.part) {
    console.log(`text: ${String(e.part.text || "").replace(/\n/g, " ").slice(0, 220)}`);
  } else if (e.type === "step_finish" && e.part) {
    console.log(`-- step_finish reason=${e.part.reason}`);
  } else if (e.type === "error") {
    console.log(`ERROR: ${JSON.stringify(e).slice(0, 200)}`);
  }
}
