import assert from "node:assert/strict";
import test from "node:test";
import { OrchestratorApplication } from "../src/application.js";
import { loadConfig } from "../src/config.js";
import { InMemoryOrchestratorState, type OrchestratorState } from "../src/orchestrator-state.js";
import { SqliteOrchestratorState } from "../src/sqlite-store.js";
import { buildExecutionPlan, reviseExecutionPlan } from "../src/planning.js";
import { routeChannelMessage } from "../src/channels/intent.js";
import type { StructuredSkillExecutionPort } from "../src/skill-executor.js";

const clarification = {
  status: "needs_input" as const, goal: "分析订单变化", known_facts: [], assumptions: [],
  open_questions: ["确认周期"], dimensions: [], time_range: { description: "待确认" },
  acceptance_criteria: ["确认周期"],
};
const unused = async (): Promise<never> => { throw new Error("Unexpected external call"); };
const taskInput = { org_id: "synthetic_org", team_id: "synthetic_team", user_id: "synthetic_user",
  channel: "web" as const, channel_conversation_id: "synthetic_conversation", intent: "data_task", message: "分析订单变化" };
function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
function application(state: OrchestratorState, clarify: StructuredSkillExecutionPort["clarify"], timeout = 25, skills: Partial<StructuredSkillExecutionPort> = {}) {
  return new OrchestratorApplication({ state,
    config: loadConfig({ FORGE_REQUEST_TIMEOUT_MS: "1", PI_STAGE_TIMEOUT_MS: String(timeout), PI_STAGE_LEASE_MS: "500" }),
    forgeClient: { createQueryRun: unused, approveQueryRun: unused },
    skillExecutor: { clarify, reviewMetric: unused, analyze: unused, writeReport: unused, ...skills },
  });
}
function runningPlan(state: OrchestratorState, taskRunId: string) {
  const initial = state.artifacts.create({ artifactType: "execution_plan", taskRunId, producer: "pi-planner",
    payload: buildExecutionPlan(routeChannelMessage("分析订单变化并生成报告"), "分析订单变化") });
  return state.artifacts.create({ artifactType: "execution_plan", taskRunId, producer: "pi-planner",
    payload: reviseExecutionPlan(initial, { analysis: "running" }) });
}

for (const [name, createState] of [
  ["memory", () => new InMemoryOrchestratorState()],
  ["sqlite", () => new SqliteOrchestratorState(":memory:")],
] as const) {
  test(`${name}: timeout retries get a fresh attempt; explicit success replays and failed key rejects`, async (context) => {
    const state = createState();
    if (state instanceof SqliteOrchestratorState) context.after(() => state.close());
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const late = deferred<typeof clarification>();
    const started = deferred<void>();
    let calls = 0;
    const app = application(state, async () => { started.resolve(); return ++calls === 1 ? late.promise : clarification; });
    const task = app.createTask(taskInput).task;
    // The executor ignores abort: the Application, not the cooperative fake, must bound its wait.
    const timedOut = assert.rejects(app.clarifyRequirement(task.task_run_id, { message: "first" }));
    await started.promise;
    context.mock.timers.tick(25);
    await timedOut;
    assert.equal(app.getTask(task.task_run_id)?.status, "created");
    const failed = app.getStageAttempts(task.task_run_id)[0]!;
    assert.equal(failed.status, "timed_out");
    await assert.rejects(app.clarifyRequirement(task.task_run_id, { message: "same", idempotencyKey: failed.idempotency_key }));
    assert.equal(calls, 1);
    const success = await app.clarifyRequirement(task.task_run_id, { message: "retry" });
    assert.equal(success.task.status, "needs_input");
    const succeeded = app.getStageAttempts(task.task_run_id)[1]!;
    const replay = await app.clarifyRequirement(task.task_run_id, { message: "retry", idempotencyKey: succeeded.idempotency_key });
    assert.equal(replay.artifact.artifact_id, success.artifact.artifact_id);
    assert.equal(calls, 2);
    late.resolve(clarification);
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(app.getStageAttempts(task.task_run_id).map((attempt) => attempt.status), ["timed_out", "succeeded"]);
    assert.equal(app.getArtifacts(task.task_run_id).length, 1);
  });

  test(`${name}: cancellation aborts the live signal and ignores late success and failure`, async (context) => {
    const state = createState();
    if (state instanceof SqliteOrchestratorState) context.after(() => state.close());
    for (const lateFailure of [false, true]) {
      const late = deferred<typeof clarification>();
      const started = deferred<AbortSignal>();
      const app = application(state, async (_task, _message, signal) => { started.resolve(signal!); return late.promise; }, 100);
      const task = app.createTask(taskInput).task;
      runningPlan(state, task.task_run_id);
      const pending = app.clarifyRequirement(task.task_run_id, { message: "run" });
      const rejected = assert.rejects(pending);
      const signal = await started.promise;
      await app.cancelTask(task.task_run_id, "cancel");
      assert.equal(signal.aborted, true);
      await rejected;
      const artifactsAtCancellation = app.getArtifacts(task.task_run_id);
      if (lateFailure) late.reject(new Error("Late provider failure")); else late.resolve(clarification);
      await new Promise((resolve) => setImmediate(resolve));
      assert.equal(app.getTask(task.task_run_id)?.status, "cancelled");
      assert.equal(app.getStageAttempts(task.task_run_id)[0]?.status, "interrupted");
      assert.deepEqual(app.getArtifacts(task.task_run_id), artifactsAtCancellation);
      const detail = app.getTaskDetailProjection({ orgId: task.org_id, teamId: task.team_id, userId: task.user_id,
        channel: task.channel, taskRunId: task.task_run_id });
      assert.equal(detail?.plan?.steps.some((step) => step.status === "running"), false);
      assert.equal(detail?.attempts[0]?.usage_status, "unknown");
    }
  });

  test(`${name}: invalid artifacts roll back all stores and nested transactions are savepoints`, (context) => {
    const state = createState();
    if (state instanceof SqliteOrchestratorState) context.after(() => state.close());
    const app = application(state, unused);
    const task = app.createTask(taskInput).task;
    state.transactions.run(() => {
      state.events.append(task.task_run_id, "skill.started", {});
      assert.throws(() => state.transactions.run(() => {
        state.tasks.transition({ taskRunId: task.task_run_id, expectedStatus: "created", status: "clarifying", currentStage: "requirement_clarification" });
        state.attempts.start({ taskRunId: task.task_run_id, stage: "requirement_clarification", runningStatus: "clarifying", retryStatus: "created", idempotencyKey: "rollback", leaseMs: 100 });
        state.events.append(task.task_run_id, "artifact.created", {});
        state.channelEvents.claim({ event_id: "rollback-event", channel: "web", event_type: "message",
          external_user_id: "synthetic_user", conversation_id: "synthetic_conversation", message_id: "synthetic_message", task_run_id: null, payload: { text: "synthetic" } });
        state.skillPolicies.configure({ orgId: task.org_id, teamId: task.team_id, enabledSkills: [], expectedVersion: 0, actor: task.user_id });
        state.artifacts.create({ artifactType: "clarification", taskRunId: task.task_run_id, producer: "skill", payload: {} });
      }));
      assert.equal(state.tasks.get(task.task_run_id)?.status, "created");
      assert.deepEqual(state.attempts.list(task.task_run_id), []);
      assert.deepEqual(state.artifacts.list(task.task_run_id), []);
      assert.equal(state.channelEvents.get("web", "rollback-event"), undefined);
      assert.equal(state.skillPolicies.get(task.org_id, task.team_id), undefined);
    });
    assert.deepEqual(state.events.list(task.task_run_id).map((event) => event.event_type), ["task.created", "skill.started"]);
  });

  test(`${name}: cancellation wins a queued timeout and caller interruption remains retryable`, async (context) => {
    const state = createState();
    if (state instanceof SqliteOrchestratorState) context.after(() => state.close());
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const late = deferred<typeof clarification>();
    const started = deferred<void>();
    let calls = 0;
    const app = application(state, async () => { calls++; started.resolve(); return late.promise; });
    const task = app.createTask(taskInput).task;
    const pending = assert.rejects(app.clarifyRequirement(task.task_run_id, { message: "race" }));
    await started.promise;
    context.mock.timers.tick(25);
    await app.cancelTask(task.task_run_id, "cancel-at-deadline");
    await pending;
    late.resolve(clarification);
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(app.getTask(task.task_run_id)?.status, "cancelled");
    assert.equal(app.getStageAttempts(task.task_run_id)[0]?.status, "interrupted");
    assert.deepEqual(app.getArtifacts(task.task_run_id), []);
    const undispatched = app.createTask(taskInput).task;
    await assert.rejects(app.clarifyRequirement(undispatched.task_run_id, { message: "aborted before dispatch" }, AbortSignal.abort()));
    assert.equal(calls, 1);
    assert.equal(app.getTask(undispatched.task_run_id)?.status, "created");
    assert.equal(app.getStageAttempts(undispatched.task_run_id)[0]?.usage_status, "not_started");
    const retried = await app.clarifyRequirement(undispatched.task_run_id, { message: "retry interruption" });
    assert.equal(retried.task.status, "needs_input");
  });

  test(`${name}: supplemental analysis failure closes plan and does not consume child evidence`, async (context) => {
    const state = createState();
    if (state instanceof SqliteOrchestratorState) context.after(() => state.close());
    const started = deferred<void>();
    const app = application(state, unused, 100, { analyze: async () => { started.resolve(); throw new Error("Synthetic analysis failure"); } });
    const parent = app.createTask(taskInput).task;
    const child = app.createTask({ ...taskInput, parent_task_run_id: parent.task_run_id, intent: "analysis_supplement_query" }).task;
    state.tasks.transition({ taskRunId: parent.task_run_id, expectedStatus: "created", status: "incomplete", currentStage: "analysis_incomplete" });
    state.tasks.transition({ taskRunId: child.task_run_id, expectedStatus: "created", status: "completed", currentStage: "query_complete" });
    for (const task of [parent, child]) state.artifacts.create({ artifactType: "query_result", taskRunId: task.task_run_id, producer: "forge", payload: {
      query_run_id: task === parent ? "qr_primary" : "qr_child", sql_hash: "sha256:" + "a".repeat(64), columns: ["n"], rows: [[1]], row_count: 1,
      truncated: false, dialect: "sqlite", registry_version: "synthetic-registry", execution_ms: 0, executed_at: "2026-09-07T00:00:00Z",
    } });
    state.artifacts.create({ artifactType: "analysis", taskRunId: parent.task_run_id, producer: "skill", payload: {
      status: "incomplete", summary: "Need a supplement", method_summary: { objective: "Compare", dimensions: ["n"], comparison_baseline: "primary", approach_steps: ["Compare evidence"] },
      findings: [], hypotheses: [], recommendations: [], limitations: ["Missing dimension"], suggested_queries: [{ question: "Get dimension", reason: "Complete evidence", priority: "high" }],
    } });
    state.events.append(parent.task_run_id, "analysis.supplement_created", { child_task_run_id: child.task_run_id });
    runningPlan(state, parent.task_run_id);
    await assert.rejects(app.resumeAnalysisWithSupplement(parent.task_run_id, { childTaskRunId: child.task_run_id, idempotencyKey: "supplement-failure" }));
    await started.promise;
    assert.equal(app.getTask(parent.task_run_id)?.status, "failed");
    assert.equal(app.getStageAttempts(parent.task_run_id)[0]?.status, "failed");
    const plan = state.artifacts.latest(parent.task_run_id, "execution_plan")!;
    assert.equal((plan.payload.steps as Array<{ status: string }>).some((step) => step.status === "running"), false);
    assert.equal(plan.payload.status, "failed");
    assert.equal(state.events.list(parent.task_run_id).some((event) => event.event_type === "analysis.supplement_consumed"), false);
  });

  test(`${name}: reconciliation closes stale plan progress and interruption allows a new command`, async (context) => {
    const state = createState();
    if (state instanceof SqliteOrchestratorState) context.after(() => state.close());
    const app = application(state, async () => clarification);
    const task = app.createTask(taskInput).task;
    state.tasks.transition({ taskRunId: task.task_run_id, expectedStatus: "created", status: "clarifying", currentStage: "requirement_clarification" });
    runningPlan(state, task.task_run_id);
    state.attempts.start({ taskRunId: task.task_run_id, stage: "requirement_clarification", runningStatus: "clarifying", retryStatus: "created", idempotencyKey: "old-owner", leaseMs: 1 });
    const recovered = state.reconcileExpiredAttempts(new Date(Date.now() + 1000));
    assert.equal(recovered[0]?.status, "interrupted");
    assert.equal(state.tasks.get(task.task_run_id)?.status, "created");
    assert.deepEqual(state.reconcileExpiredAttempts(new Date(Date.now() + 1000)), []);
    const plan = state.artifacts.latest(task.task_run_id, "execution_plan")!;
    assert.equal((plan.payload.steps as Array<{ status: string }>).some((step) => step.status === "running"), false);
    const retried = await app.clarifyRequirement(task.task_run_id, { message: "new owner retry" });
    assert.equal(retried.task.status, "needs_input");
    assert.deepEqual(app.getStageAttempts(task.task_run_id).map((attempt) => attempt.status), ["interrupted", "succeeded"]);
  });
}
