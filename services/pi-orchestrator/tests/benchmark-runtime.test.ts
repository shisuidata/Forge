import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { setImmediate as nextTurn } from "node:timers/promises";

import type { TestContext } from "node:test";
import type { OrchestratorApplication } from "../src/application.js";
import { loadConfig } from "../src/config.js";

// Replace only the model/session package; run the real persistence, workers and HTTP handoff.
const nativeSdkUrl = import.meta.resolve("@earendil-works/pi-coding-agent");
const { DefaultResourceLoader } = await import(nativeSdkUrl);
const pi = {
  DefaultResourceLoader,
  recursiveCandidate: false,
  failure: null as "throw" | "empty" | "second-payload" | "second-turn" | "invalid-raw" | "partial-usage" | "unknown-usage" | "provider-error" | "deadline-finalization" | null,
  sessionsCreated: 0,
  ModelRuntime: { create: async () => ({
    getModel: () => ({ id: "fixed", api: "openai-codex-responses" }),
    getAvailable: async () => [{ id: "fixed" }],
  }) },
  defineTool: (tool: unknown) => tool,
  SettingsManager: { inMemory: () => ({}) },
  SessionManager: { inMemory: () => ({}) },
  createAgentSession: async (options: { customTools?: Array<{ parameters: { properties: Record<string, unknown> }; prepareArguments?(args: unknown): unknown; execute(id: string, params: unknown): Promise<unknown> }> }) => {
    pi.sessionsCreated += 1;
    let finishPrompt: (() => void) | undefined;
    let finalizing: Promise<void> | undefined;
    let finalized = false;
    let onEvent: ((event: unknown) => void) | undefined;
    const session = {
      agent: {
        streamFunction: async (_model: unknown, _context: unknown, options: { maxRetries?: number; timeoutMs?: number }) => {
          assert.equal(options.maxRetries, 0);
          assert.equal(options.timeoutMs, 120000);
        },
        onPayload: undefined as ((body: unknown, model: { api: string }) => Promise<unknown>) | undefined,
      },
      state: { messages: [{ role: "assistant", content: "SELECT orders.id FROM orders" as string | unknown[], stopReason: "stop", errorMessage: undefined as string | undefined }] },
      subscribe: (listener: (event: unknown) => void) => { onEvent = listener; return () => {}; },
      prompt: async () => {
        const tool = options.customTools?.[0];
        await session.agent.streamFunction({}, {}, {});
        const payload = tool ? { tools: [{ type: "function", name: "emit_forge_query" }] } : {};
        await session.agent.onPayload?.(payload, { api: "openai-codex-responses" });
        if (pi.failure === "second-payload") await session.agent.onPayload?.(payload, { api: "openai-codex-responses" });
        if (pi.failure === "second-turn") await session.agent.streamFunction({}, {}, {});
        if (pi.failure === "provider-error") {
          session.state.messages = [{ role: "assistant", content: [], stopReason: "error", errorMessage: "Synthetic provider rejection: request-test" }];
          return;
        }
        if (pi.failure === "deadline-finalization") {
          session.state.messages = [];
          onEvent?.({ type: "message_update", message: { role: "assistant", usage: { input: 80, output: 3, cacheRead: 0, cacheWrite: 0 } } });
          await new Promise<void>(resolve => { finishPrompt = resolve; });
          return;
        }
        if (pi.failure === "unknown-usage") throw new Error("Provider failed without usage");
        if (pi.failure === "partial-usage") {
          onEvent?.({ type: "message_update", message: { role: "assistant", usage: { input: 100, output: 30, cacheRead: 0, cacheWrite: 0 } } });
          throw new Error("Failure before final message persistence");
        }
        if (!tool && pi.failure === "empty") { session.state.messages = []; return; }
        if (tool) {
          if (pi.failure === "invalid-raw") tool.prepareArguments?.({ scan: 123, unrecognized: "retained" });
          if (pi.failure === "throw") throw new Error("Generation interrupted after reported usage");
          const raw: Record<string, unknown> = { ...Object.fromEntries(Object.keys(tool.parameters.properties).map((key) => [key, null])), scan: "orders", select: ["orders.id"] };
          if (pi.recursiveCandidate) {
            raw.cte = [{ name: "filtered", query: { ...raw }, recursive: null, recursive_term: null, recursive_union: null }];
            raw.scan = "filtered";
            raw.select = [{ expr: "filtered.id * 1", as: "value" }];
          }
          const args = tool.prepareArguments ? tool.prepareArguments(raw) : raw;
          await tool.execute("fixed", validateToolArguments(tool, { name: "emit_forge_query", arguments: args }));
        }
      },
      abort: () => {
        if (pi.failure !== "deadline-finalization") return Promise.resolve();
        return finalizing ??= (async () => {
          await nextTurn();
          finalized = true;
          const message = { role: "assistant", content: [{ type: "text", text: "SELECT orders.id FROM orders" }], stopReason: "aborted", errorMessage: "Synthetic cancelled stream" };
          session.state.messages = [message];
          onEvent?.({ type: "message_end", message: { ...message, usage: { input: 80, output: 13, cacheRead: 0, cacheWrite: 0 } } });
          finishPrompt?.();
        })();
      },
      dispose: () => {},
      getSessionStats: () => ({ tokens: pi.failure === "deadline-finalization"
        ? { input: finalized ? 80 : 0, output: finalized ? 13 : 0, cacheRead: 0, cacheWrite: 0, total: finalized ? 93 : 0 }
        : pi.failure === "partial-usage" || pi.failure === "unknown-usage" || pi.failure === "provider-error"
        ? { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 }
        : { input: 100, output: 30, cacheRead: 0, cacheWrite: 0, total: 130 } }),
    };
    return { session };
  },
};
const globals = globalThis as typeof globalThis & { benchmarkTestPi?: typeof pi };
globals.benchmarkTestPi = pi;
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier === "@earendil-works/pi-ai") return nextResolve(specifier, { ...context, parentURL: nativeSdkUrl });
    if (specifier === "@earendil-works/pi-coding-agent") {
      return {
        url: "data:text/javascript," + encodeURIComponent(
          "export const {ModelRuntime, DefaultResourceLoader, defineTool, SettingsManager, SessionManager, createAgentSession} = globalThis.benchmarkTestPi;",
        ),
        shortCircuit: true,
      };
    }
    return nextResolve(specifier, context);
  },
});
// Static import would bind the real model/session module before its isolated test replacement.
const nativeAiModule = "@earendil-works/pi-ai";
const { validateToolArguments } = await import(nativeAiModule);
const { PiBenchmarkRuntime } = await import("../src/benchmark-runtime.js");

test("selection and paid-call authorization reject before freezing or creating tasks", async (t) => {
  const { runtime } = await fixture(t);
  const paths: string[] = [];
  t.mock.method(globalThis, "fetch", async (input: string) => {
    paths.push(input);
    assert.ok(input.endsWith("/suite"));
    return Response.json({ suite: { suite: "fixed" }, cases: [caseData("a"), caseData("b")], metric_revision: revision });
  });
  const base = { provider: "test", model: "fixed", confirmModelCalls: 4 };
  for (const options of [
    { provider: "test", model: "fixed" },
    { ...base, confirmModelCalls: 2 },
    { ...base, limit: 0 }, { ...base, limit: -1 }, { ...base, limit: 1.5 }, { ...base, limit: 3 },
    { ...base, caseIds: [] }, { ...base, caseIds: ["a", "a"] },
    { ...base, caseIds: [""] }, { ...base, caseIds: ["a", "unknown"] },
  ]) await assert.rejects(runtime.start(options));
  assert.deepEqual(runtime.history(), []);
  assert.ok(paths.every((path) => path.endsWith("/suite")));
});

for (const violation of ["second-payload", "second-turn", "invalid-raw", "partial-usage"] as const) {
  test(violation + " fails closed with bounded dispatches and retained raw evidence/usage", async (t) => {
    const { runtime } = await fixture(t);
    pi.failure = violation;
    const evaluated: string[] = [];
    t.mock.method(globalThis, "fetch", async (input: string, options?: RequestInit) => {
      if (input.endsWith("/suite")) return Response.json({ suite: { suite: "fixed" }, cases: [caseData("a")], metric_revision: revision });
      const body = JSON.parse(String(options?.body));
      if (input.endsWith("/protocol")) return Response.json(protocolData(body.case_ids));
      assert.equal(body.protocol_revision, protocolRevision);
      if (input.endsWith("/context")) return Response.json(contextData(body.case_id));
      evaluated.push(body.arm);
      return Response.json(evaluationData);
    });
    const started = await runtime.start({ provider: "test", model: "fixed", confirmModelCalls: 2 });
    const run = await settled(runtime, started.run_id);
    assert.equal(run.status, "failed");
    assert.equal(run.completed_calls, 2);
    for (const arm of ["forge", "direct"] as const) {
      assert.equal(run.cases[0]![arm].evidence!.dispatches, 1);
      assert.equal(run.cases[0]![arm].total_tokens, 130);
      assert.match(run.cases[0]![arm].evidence!.payload_hash!, /^sha256:/);
      assert.match(run.cases[0]![arm].evidence!.response_output_hash!, /^sha256:/);
    }
    if (violation === "invalid-raw") {
      assert.deepEqual((run.cases[0]!.forge.raw_output as { tool_arguments: unknown }).tool_arguments, { scan: 123, unrecognized: "retained" });
      assert.deepEqual(evaluated, ["direct"]);
    } else assert.deepEqual(evaluated, []);
    assert.equal(run.metrics.forge && (run.metrics.forge as { total_tokens: number }).total_tokens, 130);
  });
}

test("dispatched calls without usage stay unknown rather than zero-cost", async (t) => {
  const { runtime } = await fixture(t);
  pi.failure = "unknown-usage";
  t.mock.method(globalThis, "fetch", async (input: string, options?: RequestInit) => {
    if (input.endsWith("/suite")) return Response.json({ suite: { suite: "fixed" }, cases: [caseData("a")], metric_revision: revision });
    const body = JSON.parse(String(options?.body));
    if (input.endsWith("/protocol")) return Response.json(protocolData(body.case_ids));
    if (input.endsWith("/context")) return Response.json(contextData(body.case_id));
    assert.fail("Missing-usage failed output must not be evaluated");
  });
  const started = await runtime.start({ provider: "test", model: "fixed", confirmModelCalls: 2 });
  const run = await settled(runtime, started.run_id);
  assert.equal(run.status, "failed");
  assert.equal(run.completed_calls, 2);
  for (const arm of ["forge", "direct"] as const) {
    assert.equal(run.cases[0]![arm].usage_observed, false);
    const metrics = run.metrics[arm] as { total_tokens: number | null; observed_total_tokens: number; unobserved_usage_arms: number };
    assert.equal(metrics.total_tokens, null);
    assert.equal(metrics.observed_total_tokens, 0);
    assert.equal(metrics.unobserved_usage_arms, 1);
  }
});

test("provider termination remains diagnosable after reopening the failed run", async (t) => {
  const { runtime, config, application } = await fixture(t);
  pi.failure = "provider-error";
  t.mock.method(globalThis, "fetch", async (input: string, options?: RequestInit) => {
    if (input.endsWith("/suite")) return Response.json({ suite: { suite: "fixed" }, cases: [caseData("a")], metric_revision: revision });
    const body = JSON.parse(String(options?.body));
    if (input.endsWith("/protocol")) return Response.json(protocolData(body.case_ids));
    if (input.endsWith("/context")) return Response.json(contextData(body.case_id));
    assert.fail("Incomplete assistant responses must not reach evaluation");
  });
  const started = await runtime.start({ provider: "test", model: "fixed", confirmModelCalls: 2 });
  await settled(runtime, started.run_id);
  const reopened = new PiBenchmarkRuntime(config, application);
  const run = reopened.get(started.run_id)!;
  assert.equal(run.status, "failed");
  assert.equal(run.completed_calls, 2);
  assert.equal(run.controls.can_resume, false);
  for (const arm of ["forge", "direct"] as const) {
    const outcome = run.cases[0]![arm];
    assert.equal(outcome.output, null);
    assert.equal(outcome.official_ea, false);
    assert.equal(outcome.usage_observed, false);
    assert.equal((run.metrics[arm] as { total_tokens: number | null }).total_tokens, null);
    const raw = outcome.raw_output as { assistant: Array<{ content: unknown; stopReason: string; errorMessage: string }> };
    assert.equal(raw.assistant[0]!.stopReason, "error");
    assert.equal(raw.assistant[0]!.errorMessage, "Synthetic provider rejection: request-test");
    assert.deepEqual(raw.assistant[0]!.content, []);
    const failure = reopened.logs(started.run_id, { arm, stage: "generation" }).items.find((log) => log.level === "error")!;
    assert.match(failure.message, /Synthetic provider rejection: request-test/);
  }
});

test("deadline retains terminal evidence without accepting late output or spending pending budget", async (t) => {
  const { runtime, config, application } = await fixture(t);
  pi.failure = "deadline-finalization";
  t.mock.timers.enable({ apis: ["setTimeout"] });
  t.mock.method(globalThis, "fetch", async (input: string, options?: RequestInit) => {
    if (input.endsWith("/suite")) return Response.json({ suite: { suite: "fixed" }, cases: [caseData("a"), caseData("b")], metric_revision: revision });
    const body = JSON.parse(String(options?.body));
    if (input.endsWith("/protocol")) return Response.json(protocolData(body.case_ids));
    if (input.endsWith("/context")) return Response.json(contextData(body.case_id));
    assert.fail("Output arriving during cancellation must not reach evaluation");
  });
  const started = await runtime.start({ provider: "test", model: "fixed", confirmModelCalls: 4 });
  for (let attempt = 0; attempt < 200 && runtime.get(started.run_id)!.completed_calls !== 2; attempt++) await nextTurn();
  assert.equal(runtime.get(started.run_id)!.completed_calls, 2);
  await nextTurn();
  t.mock.timers.tick(120000);
  await settled(runtime, started.run_id);
  const reopened = new PiBenchmarkRuntime(config, application);
  const run = reopened.get(started.run_id)!;
  assert.equal(run.status, "failed");
  assert.equal(run.completed_calls, 2);
  assert.equal(run.controls.can_resume, false);
  assert.equal(run.cases[1]!.status, "pending");
  for (const arm of ["forge", "direct"] as const) {
    const outcome = run.cases[0]![arm];
    const raw = outcome.raw_output as { assistant: Array<{ content: unknown; stopReason: string; errorMessage: string }> };
    assert.deepEqual(raw.assistant.map(message => message.content), [[{ type: "text", text: "SELECT orders.id FROM orders" }]]);
    assert.equal(raw.assistant[0]!.stopReason, "aborted");
    assert.equal(raw.assistant[0]!.errorMessage, "Synthetic cancelled stream");
    assert.equal(outcome.evidence!.response_output_hash, "sha256:" + createHash("sha256").update(JSON.stringify(raw)).digest("hex"));
    assert.equal(outcome.total_tokens, 93);
    assert.equal(outcome.usage_observed, true);
    assert.equal((run.metrics[arm] as { total_tokens: number }).total_tokens, 93);
    assert.equal(outcome.output, null);
    assert.equal(outcome.scored, true);
    assert.equal(outcome.official_ea, false);
    assert.equal(outcome.contract_accuracy, false);
    assert.equal(outcome.failure?.retryable, false);
    assert.equal(run.cases[1]![arm].scored, false);
    assert.equal(run.cases[1]![arm].official_ea, null);
    const failure = reopened.logs(started.run_id, { arm, stage: "generation" }).items.find(log => log.level === "error")!;
    const payload = failure.payload as { raw_output: unknown; tokens: { total: number } };
    assert.deepEqual(payload.raw_output, raw);
    assert.equal(payload.tokens.total, 93);
  }
});

test("explicit case order determines the limited selection and frozen protocol", async (t) => {
  const { runtime } = await fixture(t);
  const suppliedManifest = protocolData(["b"]).manifest;
  t.mock.method(globalThis, "fetch", async (input: string, options?: RequestInit) => {
    if (input.endsWith("/suite")) return Response.json({ suite: { suite: "fixed" }, cases: [caseData("a"), caseData("b")], metric_revision: revision });
    const body = JSON.parse(String(options?.body));
    if (input.endsWith("/protocol")) {
      assert.deepEqual(body.case_ids, ["b"]);
      assert.deepEqual(body.protocol_manifest, suppliedManifest);
      return Response.json(protocolData(body.case_ids));
    }
    if (input.endsWith("/context")) return Response.json(contextData(body.case_id));
    return Response.json(evaluationData);
  });
  const started = await runtime.start({ provider: "test", model: "fixed", caseIds: ["b", "a"], limit: 1,
    confirmModelCalls: 2, protocolManifest: suppliedManifest });
  const run = await settled(runtime, started.run_id);
  assert.equal(run.status, "completed");
  assert.deepEqual(run.cases.map((item) => item.case_id), ["b"]);
  assert.equal(run.completed_calls, 2);
});

test("protocol rejection is pre-generation and does not create a run", async (t) => {
  const { runtime } = await fixture(t);
  t.mock.method(globalThis, "fetch", async (input: string) => input.endsWith("/suite")
    ? Response.json({ suite: { suite: "fixed" }, cases: [caseData("a")], metric_revision: revision })
    : new Response(null, { status: 409 }));
  await assert.rejects(runtime.start({ provider: "test", model: "fixed", confirmModelCalls: 2, protocolManifest: protocolData(["a"]).manifest }));
  assert.deepEqual(runtime.history(), []);
});

test("explicit Gold skip retains every selected case, spends only the runnable budget and ends incomplete", async (t) => {
  const { runtime } = await fixture(t);
  const ids = ["a", "b", "c"];
  const readiness = { policy: "skip_unscorable" as const, blocked_cases: [{ case_id: "a", db_id: "fixed", code: "unknown_column" }] };
  const protocol = protocolData(ids, readiness);
  delete protocol.contexts.a;
  const evaluated: string[] = [];
  let release!: () => void;
  const handoff = new Promise<void>((resolve) => { release = resolve; });
  t.mock.method(globalThis, "fetch", async (input: string, options?: RequestInit) => {
    if (input.endsWith("/suite")) return Response.json({ suite: { suite: "fixed" }, cases: [...ids, "replacement"].map(caseData), metric_revision: revision });
    const body = JSON.parse(String(options?.body));
    if (input.endsWith("/protocol")) {
      assert.equal(body.confirm_model_calls, 4);
      return Response.json(protocol);
    }
    assert.notEqual(body.case_id, "a", "blocked Gold must not reach context or evaluation handoff");
    assert.notEqual(body.case_id, "replacement");
    if (input.endsWith("/context")) { await handoff; return Response.json(contextData(body.case_id)); }
    evaluated.push(body.case_id + ":" + body.arm);
    return Response.json(evaluationData);
  });
  const started = await runtime.start({ provider: "test", model: "fixed", caseIds: ids, confirmModelCalls: 4, protocolManifest: protocol.manifest });
  assert.equal(started.status, "running", "blocked rows must not fail-fast");
  assert.equal(pi.sessionsCreated, 0);
  const blocked = started.cases.find((item) => item.case_id === "a")!;
  assert.equal(blocked.status, "failed");
  assert.equal(blocked.current_stage, "gold_preflight");
  assert.equal(blocked.failure?.stage, "gold");
  assert.equal(blocked.started_at, null);
  assert.notEqual(blocked.completed_at, null);
  for (const arm of ["forge", "direct"] as const) {
    const row = blocked[arm];
    assert.equal(row.scored, false);
    assert.equal(row.official_ea, null);
    assert.equal(row.contract_accuracy, null);
    assert.equal(row.generation_ms, null);
    assert.equal(row.usage_observed, true);
    assert.equal(row.total_tokens, 0);
    assert.equal(row.prompt_tokens + row.completion_tokens + row.cache_read_tokens + row.cache_write_tokens, 0);
    assert.equal(row.evidence?.dispatches, 0);
    assert.equal(row.execution_status, "skipped");
    assert.equal(row.output, null);
  }
  release();
  const run = await settled(runtime, started.run_id);
  assert.equal(run.status, "failed");
  assert.equal(run.total_cases, 3);
  assert.equal(run.completed_cases, 3);
  assert.equal(run.total_calls, 4);
  assert.equal(run.completed_calls, 4);
  assert.equal(pi.sessionsCreated, 4, "only the two runnable pairs create SDK sessions");
  assert.deepEqual(evaluated.sort(), ["b:direct", "b:forge", "c:direct", "c:forge"]);
  assert.deepEqual(run.cases.map((item) => [item.case_id, item.status]), [["a", "failed"], ["b", "passed"], ["c", "passed"]]);
  assert.equal(run.cases[0]!.winner, null);
  assert.equal(run.controls.can_resume, false);
  for (const arm of ["forge", "direct"] as const) {
    const metrics = run.metrics[arm] as Record<string, unknown>;
    assert.equal(metrics.total_cases, 3);
    assert.equal(metrics.scored_cases, 2);
    assert.equal(metrics.unscored_cases, 1);
    assert.equal(metrics.official_ea, null);
    assert.equal(metrics.contract_accuracy, null);
    assert.equal(metrics.official_ea_correct_cases, 2);
    assert.equal(metrics.official_ea_missing_cases, 1);
    assert.equal(metrics.total_tokens, 260);
    assert.equal(metrics.unobserved_usage_arms, 0);
    assert.equal(metrics.generation_observed_cases, 2);
  }
});

test("Gold skip authorization rejects unbound, drifting or invalid blocked reports before creating sessions", async (t) => {
  const { runtime } = await fixture(t);
  const ids = ["a", "b", "c"];
  const blocked = { case_id: "a", db_id: "fixed", code: "unknown_column" };
  const readiness = { policy: "skip_unscorable" as const, blocked_cases: [blocked] };
  let protocol = protocolData(ids, readiness);
  t.mock.method(globalThis, "fetch", async (input: string) => input.endsWith("/suite")
    ? Response.json({ suite: { suite: "fixed" }, cases: ids.map(caseData), metric_revision: revision })
    : Response.json(protocol));
  const start = (manifest: Record<string, unknown> | undefined, calls = 4) => runtime.start({ provider: "test", model: "fixed", confirmModelCalls: calls, ...(manifest ? { protocolManifest: manifest } : {}) });
  for (const report of [
    undefined,
    { policy: "require_all", blocked_cases: [blocked] },
    { policy: "skip_unscorable", blocked_cases: [blocked, blocked] },
    { policy: "skip_unscorable", blocked_cases: [{ ...blocked, case_id: "outside" }] },
    { policy: "skip_unscorable", blocked_cases: [{ ...blocked, db_id: "other" }] },
  ]) await assert.rejects(start({ gold_readiness: report }));
  await assert.rejects(start(undefined));
  await assert.rejects(start(protocol.manifest, 6));
  const manifest = structuredClone(protocol.manifest);
  protocol = { ...protocol, gold_readiness: { policy: "require_all", blocked_cases: [] } };
  await assert.rejects(start(manifest));
  protocol = protocolData(ids, { ...readiness, blocked_cases: [{ ...blocked, case_id: "b" }] });
  await assert.rejects(start(manifest));
  protocol = protocolData(ids, readiness);
  protocol.generation.max_model_calls = 6;
  await assert.rejects(start(manifest));
  assert.deepEqual(runtime.history(), []);
  assert.equal(pi.sessionsCreated, 0);
});

test("all-blocked explicit Gold skip authorizes zero calls without creating SDK sessions", async (t) => {
  const { runtime } = await fixture(t);
  const protocol = protocolData(["a"], { policy: "skip_unscorable", blocked_cases: [{ case_id: "a", db_id: "fixed", code: "execution_failed" }] });
  t.mock.method(globalThis, "fetch", async (input: string) => {
    if (input.endsWith("/suite")) return Response.json({ suite: { suite: "fixed" }, cases: [caseData("a")], metric_revision: revision });
    assert.ok(input.endsWith("/protocol"), "all-blocked run cannot hand off context or candidates");
    return Response.json(protocol);
  });
  const started = await runtime.start({ provider: "test", model: "fixed", confirmModelCalls: 0, protocolManifest: protocol.manifest });
  const run = await settled(runtime, started.run_id);
  assert.equal(run.status, "failed");
  assert.equal(run.total_cases, 1);
  assert.equal(run.completed_cases, 1);
  assert.equal(run.total_calls, 0);
  assert.equal(run.completed_calls, 0);
  assert.equal(pi.sessionsCreated, 0);
  assert.equal((run.metrics.forge as Record<string, unknown>).total_tokens, 0);
  assert.equal((run.metrics.forge as Record<string, unknown>).official_ea, null);
});

hooks.deregister();
delete globals.benchmarkTestPi;

const revision = "test-fixed-comparator";
const caseData = (caseId: string) => ({
  case_id: caseId, question_id: 1, db_id: "fixed", difficulty: "simple", question: "List order ids", evidence: "orders.id",
});
const protocolRevision = "sha256:frozen-protocol";
const contextData = (caseId: string, promptRevision = "forge-structured-benchmark-v1") => ({
  protocol_revision: protocolRevision,
  metric_revision: revision,
  case: caseData(caseId),
  forge_prompt_revision: promptRevision,
  schema_context: "orders(id)", forge_instructions: "fixed", direct_instructions: "fixed",
  context_snapshot: {
    question: "List order ids", evidence: "orders.id", question_concepts: ["orders"],
    tables: ["orders"], fields: ["orders.id"], relationships: [], retrieval_rounds: [],
    sufficiency_status: "sufficient", content_hash: "sha256:fixed",
    result_contract: {},
  },
});
const protocolData = (ids: string[], goldReadiness: { policy: "require_all" | "skip_unscorable"; blocked_cases: Array<{ case_id: string; db_id: string; code: string }> } = { policy: "require_all", blocked_cases: [] }, promptRevision = "forge-structured-benchmark-v1") => ({
  manifest: { protocol_revision: protocolRevision, gold_readiness: goldReadiness, forge_prompt_revision: promptRevision }, protocol_revision: protocolRevision,
  gold_readiness: goldReadiness,
  case_ids: ids, metric_revision: revision, forge_prompt_revision: promptRevision,
  forge_schema_revision: "sha256:" + createHash("sha256").update(readFileSync(new URL("../../../forge/schema.json", import.meta.url))).digest("hex"),
  contexts: Object.fromEntries(ids.map((id) => [id, contextData(id, promptRevision)])),
  generation: { max_model_calls: (ids.length - goldReadiness.blocked_cases.length) * 2, provider_retries: 0, max_agent_turns_per_arm: 1,
    timeout_seconds: 120, sampling: "provider_default", max_output_tokens: null },
});
const evaluationData = {
  protocol_revision: protocolRevision,
  scored: true,
  metric_revision: revision, compile_status: "passed", execution_status: "passed",
  official_ea: true, contract_accuracy: true, failure: null, error_code: null, sql: "SELECT orders.id FROM orders",
};

async function fixture(t: TestContext) {
  pi.recursiveCandidate = false;
  pi.failure = null;
  pi.sessionsCreated = 0;
  const directory = await mkdtemp(join(tmpdir(), "pi-benchmark-runtime-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  await writeFile(join(directory, "models.json"), JSON.stringify({ providers: {} }));
  const config = loadConfig({
    PI_ORCHESTRATOR_AGENT_DIR: directory,
    PI_ORCHESTRATOR_STATE_DB: join(directory, "state.sqlite"),
    PI_BENCHMARK_CONCURRENCY: "1",
    FORGE_BASE_URL: "http://benchmark.invalid",
  });
  // The benchmark only uses createTask; its persistence is exercised by the real runtime.
  const application = {
    createTask: () => ({ task: { task_run_id: "fixed-task" } }),
  } as unknown as OrchestratorApplication;
  return { runtime: new PiBenchmarkRuntime(config, application), config, application };
}

async function settled(runtime: InstanceType<typeof PiBenchmarkRuntime>, runId: string) {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    const run = runtime.get(runId)!;
    if (["completed", "failed"].includes(run.status)) {
      await nextTurn();
      return runtime.get(runId)!;
    }
    await nextTurn();
  }
  assert.fail("Benchmark did not settle");
}

for (const drift of ["context", "evaluation", "conflict"] as const) {
  test(`comparator ${drift} drift fails the whole run instead of publishing mixed scores`, async (t) => {
    const { runtime } = await fixture(t);
    const evaluated: string[] = [];
    t.mock.method(globalThis, "fetch", async (input: string, options?: RequestInit) => {
      if (input.endsWith("/suite")) {
        return Response.json({ suite: { suite: "fixed" }, cases: [caseData("a"), caseData("b")], metric_revision: revision });
      }
      const body = JSON.parse(String(options?.body));
      if (input.endsWith("/protocol")) return Response.json(protocolData(body.case_ids));
      if (input.endsWith("/context")) {
        return Response.json({ ...contextData(body.case_id),
          metric_revision: body.case_id === "b" && drift === "context" ? "upgraded-comparator" : revision,
        });
      }
      assert.equal(body.metric_revision, revision);
      evaluated.push(body.case_id);
      if (body.case_id === "b" && drift === "conflict") return new Response(null, { status: 409 });
      return Response.json({ ...evaluationData,
        metric_revision: body.case_id === "b" && drift === "evaluation" ? "upgraded-comparator" : revision,
      });
    });
    const started = await runtime.start({ provider: "test", model: "fixed", confirmModelCalls: 4 });
    const run = await settled(runtime, started.run_id);
    assert.equal(run.metric_revision, revision);
    assert.equal(run.status, "failed");
    assert.equal(run.cases.find((item) => item.case_id === "a")!.status, "passed");
    assert.equal(run.cases.find((item) => item.case_id === "b")!.status, "failed");
    assert.equal(run.controls.can_resume, false);
    if (drift === "context") {
      assert.deepEqual(evaluated, ["a", "a"]);
      const unstarted = run.cases.find((item) => item.case_id === "b")!;
      assert.equal(unstarted.forge.usage_observed, true);
      assert.equal(unstarted.forge.total_tokens, 0);
    }
    else {
      const failed = run.cases.find((item) => item.case_id === "b")!;
      assert.equal(failed.forge.total_tokens, 130);
      assert.equal(failed.direct.total_tokens, 130);
      assert.equal((run.metrics.forge as { total_tokens: number }).total_tokens, 260);
      assert.notEqual(failed.forge.output, null);
    }
    assert.equal(runtime.history()[0]!.status, "failed");
  });
}

test("new runs require a known suite revision before persisting any run", async (t) => {
  const { runtime } = await fixture(t);
  t.mock.method(globalThis, "fetch", async () => Response.json({ suite: { suite: "fixed" }, cases: [] }));
  await assert.rejects(runtime.start({ provider: "test", model: "fixed", confirmModelCalls: 2 }));
  assert.deepEqual(runtime.history(), []);
});

test("inconclusive arm stays unknown and cannot win against a definite mismatch", async (t) => {
  const { runtime } = await fixture(t);
  t.mock.method(globalThis, "fetch", async (input: string, options?: RequestInit) => {
    if (input.endsWith("/suite")) {
      return Response.json({ suite: { suite: "fixed" }, cases: [caseData("a")], metric_revision: revision });
    }
    const body = JSON.parse(String(options?.body));
      if (input.endsWith("/protocol")) return Response.json(protocolData(body.case_ids));
    if (input.endsWith("/context")) return Response.json(contextData(body.case_id));
    assert.equal(body.metric_revision, revision);
    return Response.json({ ...evaluationData, contract_accuracy: body.arm === "forge" ? null : false });
  });
  const started = await runtime.start({ provider: "test", model: "fixed", confirmModelCalls: 2 });
  const run = await settled(runtime, started.run_id);
  assert.equal(run.status, "completed");
  assert.equal(run.cases[0]!.forge.contract_accuracy, null);
  assert.equal(run.cases[0]!.winner, null);
  assert.equal((run.metrics.forge as Record<string, unknown>).contract_accuracy, null);
  assert.equal((run.metrics.forge as Record<string, unknown>).contract_accuracy_missing_cases, 1);
});

test("unversioned historical runs project unknown, cannot resume, and never backfill stored rows", async (t) => {
  const { runtime, config, application } = await fixture(t);
  const db = new DatabaseSync(config.stateDbPath);
  t.after(() => db.close());
  const raw = JSON.stringify({
    run_id: "historical", task_run_id: "historical-task", status: "paused", suite_id: "old",
    model: { provider: "old", model: "old", revision: "old", temperature: 0, max_output_tokens: 1 },
    total_cases: 0, total_calls: 0, sequence: 1, current_case: null,
    created_at: "2020-01-01", started_at: null, completed_at: null, error: null,
  });
  db.prepare("INSERT INTO benchmark_v2_runs VALUES(?,?,?,?)").run("historical", "paused", "2020-01-01", raw);
  assert.equal(runtime.get("historical")!.metric_revision, null);
  assert.equal(runtime.get("historical")!.controls.can_resume, false);
  assert.throws(() => runtime.resume("historical"));
  assert.equal(db.prepare("SELECT data_json FROM benchmark_v2_runs").get()!.data_json, raw);
  const activeRaw = raw.replace('"paused"', '"running"');
  db.prepare("UPDATE benchmark_v2_runs SET status='running', data_json=?").run(activeRaw);
  const reopened = new PiBenchmarkRuntime(config, application);
  assert.equal(reopened.history()[0]!.metric_revision, null);
  assert.equal(db.prepare("SELECT data_json FROM benchmark_v2_runs").get()!.data_json, activeRaw);
  const knownMetricRaw = JSON.stringify({ ...JSON.parse(activeRaw), metric_revision: revision });
  db.prepare("UPDATE benchmark_v2_runs SET data_json=?").run(knownMetricRaw);
  const versionedButUnfrozen = new PiBenchmarkRuntime(config, application);
  assert.equal(versionedButUnfrozen.get("historical")!.controls.can_pause, false);
  assert.throws(() => versionedButUnfrozen.stop("historical"));
  assert.equal(db.prepare("SELECT data_json FROM benchmark_v2_runs").get()!.data_json, knownMetricRaw);
});

for (const drift of ["prefer", "wire-schema", "pi-runtime", "sdk-lock", "prompt"]) {
  test(`paused ${drift} generation contract cannot mix with required strict output`, async (t) => {
    const { runtime, config } = await fixture(t);
    t.mock.method(globalThis, "fetch", async (input: string, options?: RequestInit) => {
      if (input.endsWith("/suite")) return Response.json({ suite: { suite: "fixed" }, cases: [caseData("a")], metric_revision: revision });
      const body = JSON.parse(String(options?.body));
      if (input.endsWith("/protocol")) return Response.json(protocolData(body.case_ids));
      if (input.endsWith("/context")) return Response.json(contextData(body.case_id));
      return Response.json(evaluationData);
    });
    const started = await runtime.start({ provider: "test", model: "fixed", confirmModelCalls: 2 });
    await settled(runtime, started.run_id);
    const db = new DatabaseSync(config.stateDbPath);
    t.after(() => db.close());
    const stored = JSON.parse(db.prepare("SELECT data_json FROM benchmark_v2_runs WHERE run_id=?").get(started.run_id)!.data_json as string);
    stored.status = "paused";
    stored.completed_at = null;
    if (drift === "prefer") stored.generation_contract.provider_json_schema_request = "prefer";
    else if (drift === "wire-schema") stored.generation_contract.forge_wire_schema_revision = "sha256:old-wire-schema";
    else if (drift === "pi-runtime") stored.generation_contract.pi_runtime_revision = "sha256:old-runtime";
    else if (drift === "prompt") stored.generation_contract.forge_prompt_revision = "unbound-prompt";
    else stored.generation_contract.pi_sdk_lock_revision = "sha256:old-sdk-lock";
    const raw = JSON.stringify(stored);
    db.prepare("UPDATE benchmark_v2_runs SET status=?, data_json=? WHERE run_id=?").run("paused", raw, started.run_id);
    assert.equal(runtime.get(started.run_id)!.controls.can_resume, false);
    assert.throws(() => runtime.resume(started.run_id));
    assert.equal(db.prepare("SELECT data_json FROM benchmark_v2_runs WHERE run_id=?").get(started.run_id)!.data_json, raw);
  });
}

for (const drift of ["manifest", "initial-context", "fresh-context"] as const) {
  test(`registered prompt ${drift} drift rejects before opening model sessions`, async (t) => {
    const { runtime } = await fixture(t);
    const promptRevision = "forge-structured-benchmark-denominator-v1";
    const protocol = protocolData(["a"], undefined, promptRevision);
    if (drift === "manifest") protocol.manifest.forge_prompt_revision = "unbound";
    if (drift === "initial-context") protocol.contexts.a!.forge_prompt_revision = "unbound";
    t.mock.method(globalThis, "fetch", async (input: string) => {
      if (input.endsWith("/suite")) return Response.json({ suite: { suite: "fixed" }, cases: [caseData("a")], metric_revision: revision });
      if (input.endsWith("/protocol")) return Response.json(protocol);
      assert.ok(input.endsWith("/context"));
      return Response.json(contextData("a", "unbound"));
    });
    if (drift === "fresh-context") {
      const started = await runtime.start({ provider: "test", model: "fixed", confirmModelCalls: 2 });
      const run = await settled(runtime, started.run_id);
      assert.equal(run.status, "failed");
      assert.equal(run.completed_calls, 0);
    } else {
      await assert.rejects(runtime.start({ provider: "test", model: "fixed", confirmModelCalls: 2 }));
      assert.deepEqual(runtime.history(), []);
    }
    assert.equal(pi.sessionsCreated, 0);
  });
}

test("recursive strict parameters pass the real Pi validator before canonical evaluation", async (t) => {
  const { runtime } = await fixture(t);
  pi.recursiveCandidate = true;
  let candidate: unknown;
  t.mock.method(globalThis, "fetch", async (input: string, options?: RequestInit) => {
    if (input.endsWith("/suite")) return Response.json({ suite: { suite: "fixed" }, cases: [caseData("a")], metric_revision: revision });
    const body = JSON.parse(String(options?.body));
      if (input.endsWith("/protocol")) return Response.json(protocolData(body.case_ids));
    if (input.endsWith("/context")) return Response.json(contextData(body.case_id));
    if (body.arm === "forge") candidate = body.output;
    return Response.json(evaluationData);
  });
  const started = await runtime.start({ provider: "test", model: "fixed", confirmModelCalls: 2 });
  const run = await settled(runtime, started.run_id);
  assert.equal(run.status, "completed");
  assert.deepEqual(candidate, { scan: "filtered", select: [{ expr: "filtered.id * 1", as: "value" }], cte: [{ name: "filtered", query: { scan: "orders", select: ["orders.id"] } }] });
});

for (const failure of ["throw", "empty"] as const) {
  test(failure + " generation fails the run, retains spent tokens and scores the missing candidate as a known negative", async (t) => {
    const { runtime } = await fixture(t);
    pi.failure = failure;
    t.mock.method(globalThis, "fetch", async (input: string, options?: RequestInit) => {
      if (input.endsWith("/suite")) return Response.json({ suite: { suite: "fixed" }, cases: [caseData("a")], metric_revision: revision });
      const body = JSON.parse(String(options?.body));
      if (input.endsWith("/protocol")) return Response.json(protocolData(body.case_ids));
      if (input.endsWith("/context")) return Response.json(contextData(body.case_id));
      return Response.json(evaluationData);
    });
    const started = await runtime.start({ provider: "test", model: "fixed", confirmModelCalls: 2 });
    const run = await settled(runtime, started.run_id);
    assert.equal(run.status, "failed");
    const arm = failure === "throw" ? "forge" : "direct";
    assert.equal(run.cases[0]![arm].output, null);
    assert.equal(run.cases[0]![arm].total_tokens, 130);
    assert.equal(run.cases[0]![arm].official_ea, false);
    assert.equal(run.cases[0]![arm].contract_accuracy, false);
    assert.equal(run.cases[0]![arm].error_code, failure === "empty" ? "generation_empty" : "agent_failed");
    assert.equal((run.metrics[arm] as any).total_tokens, 130);
    assert.equal((run.metrics[arm] as any).official_ea, 0);
    assert.equal(run.controls.can_resume, false);
    const partner = arm === "forge" ? "direct" : "forge";
    assert.equal(run.cases[0]![arm].scored, true);
    assert.equal(run.cases[0]![partner].scored, true);
    assert.equal(run.cases[0]![partner].official_ea, true);
    assert.equal((run.metrics[partner] as any).official_ea, 1);
    assert.equal((run.metrics[arm] as any).official_ea_missing_cases, 0);
    assert.equal((run.metrics[arm] as any).average_generation_ms, run.cases[0]![arm].generation_ms);
  });
}

test("Gold failure stays unscored while a delayed successful partner and pending cases retain evidence", async (t) => {
  const { runtime } = await fixture(t);
  let releasePartner!: () => void;
  const partner = new Promise<void>((resolve) => { releasePartner = resolve; });
  let goldEvaluated = false;
  t.mock.method(globalThis, "fetch", async (input: string, options?: RequestInit) => {
    if (input.endsWith("/suite")) return Response.json({ suite: { suite: "fixed" }, cases: [caseData("a"), caseData("b")], metric_revision: revision });
    const body = JSON.parse(String(options?.body));
    if (input.endsWith("/protocol")) return Response.json(protocolData(body.case_ids));
    if (input.endsWith("/context")) return Response.json(contextData(body.case_id));
    assert.equal(body.case_id, "a");
    if (body.arm === "direct") { await partner; return Response.json(evaluationData); }
    goldEvaluated = true;
    return Response.json({ ...evaluationData, scored: false, official_ea: null, contract_accuracy: null,
      failure: { stage: "gold", code: "gold_execution_failed", retryable: false }, error_code: "gold_execution_failed" });
  });
  const started = await runtime.start({ provider: "test", model: "fixed", confirmModelCalls: 4 });
  while (!goldEvaluated) await nextTurn();
  assert.equal(runtime.get(started.run_id)!.status, "running");
  releasePartner();
  const run = await settled(runtime, started.run_id);
  assert.equal(run.status, "failed");
  assert.equal(run.completed_calls, 2);
  assert.equal(run.completed_cases, 1);
  assert.equal(run.cases[0]!.status, "failed");
  assert.equal(run.cases[0]!.failure?.stage, "gold");
  assert.equal(run.cases[0]!.forge.scored, false);
  assert.equal(run.cases[0]!.forge.official_ea, null);
  assert.equal(run.cases[0]!.forge.contract_accuracy, null);
  assert.equal(run.cases[0]!.forge.execution_status, "passed");
  assert.equal(run.cases[0]!.direct.scored, true);
  assert.equal(run.cases[0]!.direct.official_ea, true);
  assert.equal(run.cases[0]!.winner, null);
  assert.equal(run.cases[1]!.status, "pending");
  for (const name of ["forge", "direct"] as const) {
    const metrics = run.metrics[name] as Record<string, number | null>;
    assert.equal(metrics.total_cases, 2);
    assert.equal(metrics.official_ea, null);
    assert.equal(metrics.contract_accuracy, null);
    assert.equal(metrics.official_ea_observed_cases, name === "forge" ? 0 : 1);
    assert.equal(metrics.official_ea_missing_cases, name === "forge" ? 2 : 1);
    assert.equal(metrics.contract_accuracy_missing_cases, name === "forge" ? 2 : 1);
    assert.equal(metrics.execution_success_cases, 1);
    assert.equal(metrics.execution_observed_cases, 1);
    assert.equal(metrics.execution_missing_cases, 1);
    assert.equal(metrics.execution_success, null);
    assert.equal(metrics.generation_observed_cases, 1);
    assert.equal(metrics.average_generation_ms, run.cases[0]![name].generation_ms);
    assert.equal(metrics.total_tokens, 130);
  }
  assert.equal(run.metrics.delta_ea, null);
});

test("candidate execution timeout is a known negative and remains in execution and latency metrics", async (t) => {
  const { runtime } = await fixture(t);
  t.mock.method(globalThis, "fetch", async (input: string, options?: RequestInit) => {
    if (input.endsWith("/suite")) return Response.json({ suite: { suite: "fixed" }, cases: [caseData("a"), caseData("b")], metric_revision: revision });
    const body = JSON.parse(String(options?.body));
    if (input.endsWith("/protocol")) return Response.json(protocolData(body.case_ids));
    if (input.endsWith("/context")) return Response.json(contextData(body.case_id));
    return Response.json(body.arm === "forge" && body.case_id === "a" ? {
      ...evaluationData, execution_status: "failed", official_ea: false, contract_accuracy: false,
      failure: { stage: "execution", code: "execution_timeout", retryable: true }, error_code: "execution_timeout",
    } : evaluationData);
  });
  const started = await runtime.start({ provider: "test", model: "fixed", confirmModelCalls: 4 });
  const run = await settled(runtime, started.run_id);
  assert.equal(run.status, "completed");
  assert.equal(run.cases[0]!.forge.scored, true);
  assert.equal(run.cases[0]!.forge.official_ea, false);
  const metrics = run.metrics.forge as Record<string, number | null>;
  assert.equal(metrics.official_ea, 0.5);
  assert.equal(metrics.contract_accuracy, 0.5);
  assert.equal(metrics.scored_cases, 2);
  assert.equal(metrics.official_ea_missing_cases, 0);
  assert.equal(metrics.execution_success, 0.5);
  assert.equal(metrics.average_generation_ms, (run.cases[0]!.forge.generation_ms! + run.cases[1]!.forge.generation_ms!) / 2);
});

test("historical false-only scores are unknown and absent case rows do not shrink authorization", async (t) => {
  const { runtime, config } = await fixture(t);
  const db = new DatabaseSync(config.stateDbPath);
  t.after(() => db.close());
  const run = { run_id: "legacy", task_run_id: "legacy-task", status: "completed", suite_id: "old",
    model: { provider: "old", model: "old", revision: "old", temperature: null, max_output_tokens: null },
    total_cases: 3, total_calls: 6, sequence: 1, current_case: null, created_at: "2020-01-01", started_at: null, completed_at: null };
  db.prepare("INSERT INTO benchmark_v2_runs VALUES(?,?,?,?)").run(run.run_id, run.status, run.created_at, JSON.stringify(run));
  const arm = { generation_ms: 10, prompt_tokens: 100, completion_tokens: 30, cache_read_tokens: 0,
    cache_write_tokens: 0, total_tokens: 130, compile_status: "passed", execution_status: "failed",
    official_ea: false, contract_accuracy: false, error_code: "execution_failed", sql: "SELECT 1", output: "SELECT 1" };
  const item = { ...caseData("a"), status: "passed", current_stage: "evaluated", context_snapshot: null,
    forge: arm, direct: { ...arm, execution_status: "passed", official_ea: true, contract_accuracy: true }, winner: "direct", started_at: null, completed_at: null };
  const raw = JSON.stringify(item);
  db.prepare("INSERT INTO benchmark_v2_cases VALUES(?,?,?,?)").run(run.run_id, item.case_id, item.status, raw);
  const projection = runtime.get(run.run_id)!;
  assert.equal(projection.cases[0]!.forge.scored, false);
  assert.equal(projection.cases[0]!.forge.official_ea, null);
  assert.equal(projection.cases[0]!.forge.contract_accuracy, null);
  assert.equal(projection.cases[0]!.direct.official_ea, true);
  assert.equal(projection.cases[0]!.winner, null);
  const forge = projection.metrics.forge as Record<string, number | null>;
  const direct = projection.metrics.direct as Record<string, number | null>;
  assert.equal(forge.total_cases, 3);
  assert.equal(forge.official_ea, null);
  assert.equal(forge.official_ea_observed_cases, 0);
  assert.equal(forge.official_ea_missing_cases, 3);
  assert.equal(direct.official_ea, null);
  assert.equal(direct.official_ea_observed_cases, 1);
  assert.equal(direct.official_ea_missing_cases, 2);
  assert.equal(db.prepare("SELECT data_json FROM benchmark_v2_cases").get()!.data_json, raw);
});
