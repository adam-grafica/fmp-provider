"use strict";
// FMP model alias catalog (v2.2.0).
//
// Honesty rule: upstream labels are DECLARED by the upstream worker and
// are NOT independently verified. Every alias therefore carries
// verified_identity: "unknown" unless real verification evidence exists
// (none exists at the time of writing). Do not change a value without
// attaching evidence in docs/ and updating last_checked_at.

const VERSION = "2.3.0";

const CATALOG = [
  { id: "claude-opus-5.5",  owned_by: "anthropic", declared_identity: "claude-opus-5.5 (upstream-declared)" },
  { id: "claude-sonnet-5",  owned_by: "anthropic", declared_identity: "claude-sonnet-5 (upstream-declared)" },
  { id: "claude-fable-5",   owned_by: "anthropic", declared_identity: "claude-fable-5 (upstream-declared)" },
  { id: "claude-fable-5.1", owned_by: "anthropic", declared_identity: "claude-fable-5.1 (upstream-declared)" },
  { id: "sol",              owned_by: "fmp",       declared_identity: "sol (upstream-declared)" },
  { id: "terra",            owned_by: "fmp",       declared_identity: "terra (upstream-declared)" },
  { id: "glm-5.2",          owned_by: "zai",       declared_identity: "glm-5.2 (upstream-declared)" },
  { id: "kimi-k3",          owned_by: "moonshot",  declared_identity: "kimi-k3 (upstream-declared)" },
];

const TRANSPORTS = ["openai", "anthropic"];
const CAPABILITIES = ["chat", "stream", "tool-transport"];

function entry(m, disabledSet) {
  const disabled = !!(disabledSet && disabledSet.has(m.id));
  return {
    id: m.id,
    object: "model",
    created: 0,
    owned_by: m.owned_by,
    transport: TRANSPORTS.slice(),
    declared_identity: m.declared_identity,
    verified_identity: "unknown",
    capabilities: CAPABILITIES.slice(),
    status: disabled ? "disabled" : "active",
    last_checked_at: null,
  };
}

function listModels(disabledSet) {
  return CATALOG.map((m) => entry(m, disabledSet));
}

function isKnown(id) {
  return CATALOG.some((m) => m.id === id);
}

function isEnabled(id, disabledSet, killAll) {
  if (killAll) return false;
  if (!isKnown(id)) return false;
  return !(disabledSet && disabledSet.has(id));
}

// "fmp-oai/sol" -> "sol"; "sol" -> "sol". Prefixes are 9Router routing
// labels, not model identities; the alias after the slash is authoritative.
function stripPrefix(model) {
  if (typeof model !== "string") return "";
  const i = model.lastIndexOf("/");
  return (i >= 0 ? model.slice(i + 1) : model).trim();
}

module.exports = { VERSION, CATALOG, TRANSPORTS, CAPABILITIES, listModels, isKnown, isEnabled, stripPrefix };
