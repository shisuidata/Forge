import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { createStrictForgeOutput, requireStrictForgeApi, requireStrictForgePayload, StrictForgeOutputError } from "../src/strict-forge-output.js";

const canonical = JSON.parse(readFileSync(new URL("../../../forge/schema.json", import.meta.url), "utf8"));
const strict = createStrictForgeOutput(canonical);
const query = (fields: Record<string, unknown>) => ({
  ...Object.fromEntries(Object.keys(canonical.properties).map((key) => [key, null])),
  ...fields,
});
const predicate = (fields: Record<string, unknown>) => ({ col: "parent_id", op: "eq", val: null, lo: null, hi: null, col2: null, ...fields });

test("strict transport preserves recursive query slots and SQL NULL without leaking unused fields", () => {
  const wire = query({
    scan: "tree", select: ["id"], distinct: false, offset: 0,
    cte: [{ name: "tree", query: query({ scan: "orders", select: ["id"], filter: [predicate({})] }), recursive: true,
      recursive_term: query({ scan: "tree", select: ["id"], filter: [predicate({ col: "id", op: "lt", val: 0 })] }), recursive_union: "union_all" }],
    filter: [predicate({ col: "id", op: "in", val: { subquery: query({ scan: "active", select: ["id"] }) } })],
    union: [{ mode: "union_all", query: query({ scan: "archived", select: ["id"] }) }],
    window: [{ fn: "lag", col: "id", as: "previous", offset: null, default: null, partition: null, order: [{ col: "id", dir: "asc" }], frame: null }],
  });
  const before = structuredClone(wire);
  assert.deepEqual(strict.decode(wire), {
    scan: "tree", select: ["id"], distinct: false, offset: 0,
    cte: [{ name: "tree", query: { scan: "orders", select: ["id"], filter: [{ col: "parent_id", op: "eq", val: null }] }, recursive: true,
      recursive_term: { scan: "tree", select: ["id"], filter: [{ col: "id", op: "lt", val: 0 }] }, recursive_union: "union_all" }],
    filter: [{ col: "id", op: "in", val: { subquery: { scan: "active", select: ["id"] } } }],
    union: [{ mode: "union_all", query: { scan: "archived", select: ["id"] } }],
    window: [{ fn: "lag", col: "id", as: "previous", default: null, order: [{ col: "id", dir: "asc" }] }],
  });
  assert.deepEqual(wire, before, "Raw provider output must remain available for audit");
});
test("malformed required fields and unsupported aggregate variants fail instead of being coerced", () => {
  assert.throws(() => strict.decode({ scan: "orders", select: ["id"] }), StrictForgeOutputError);
  assert.throws(() => strict.decode(query({ scan: null, select: ["id"] })), StrictForgeOutputError);
  assert.throws(() => strict.decode(query({ scan: "orders", select: ["id"], limit: "5" })), StrictForgeOutputError);
  const aggregate = { ...Object.fromEntries(Object.keys(canonical.definitions.AggWithCol.properties).map((key) => [key, null])), fn: "sum", col: "id", as: "n" };
  assert.deepEqual(strict.decode(query({ scan: "orders", select: ["n"], agg: [aggregate] })), { scan: "orders", select: ["n"], agg: [{ fn: "sum", col: "id", as: "n" }] });
  assert.throws(() => strict.decode(query({ scan: "orders", select: ["n"], agg: [{ ...aggregate, fn: "unknown" }] })), StrictForgeOutputError);
});

test("native provider payload requires strict output and only the terminating Forge function", () => {
  for (const api of ["openai-codex-responses", "openai-completions"]) {
    const definition = { name: "emit_forge_query", parameters: {}, strict: null };
    const body = { tools: [api === "openai-completions" ? { type: "function", function: definition } : { type: "function", ...definition }, { type: "function", name: "unrelated_tool" }] };
    const result = requireStrictForgePayload(body, api, strict.schema) as Record<string, any>;
    const sent = JSON.parse(JSON.stringify(result));
    assert.equal(sent.parallel_tool_calls, false);
    const function_ = api === "openai-completions" ? sent.tools[0].function : sent.tools[0];
    assert.equal(function_.strict, true);
    assert.equal(function_.name, "emit_forge_query");
    assert.equal(sent.tools.length, 1);
    assert.deepEqual(sent.tool_choice, api === "openai-completions"
      ? { type: "function", function: { name: "emit_forge_query" } }
      : { type: "function", name: "emit_forge_query" });
    assert.deepEqual(function_.parameters, strict.schema);
  }
  assert.throws(() => requireStrictForgeApi({ api: "anthropic-messages" }), StrictForgeOutputError);
  assert.throws(() => requireStrictForgeApi({ api: "openai-responses", compat: { supportsStrictMode: false } }), StrictForgeOutputError);
  assert.throws(() => requireStrictForgePayload({ tools: [] }, "openai-codex-responses", strict.schema), StrictForgeOutputError);
});
