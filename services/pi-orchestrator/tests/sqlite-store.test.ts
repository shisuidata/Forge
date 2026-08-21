import assert from "node:assert/strict";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { OrchestratorApplication, type ForgeQueryRunPort } from "../src/application.js";
import { loadConfig } from "../src/config.js";
import { SqliteOrchestratorState } from "../src/sqlite-store.js";
import { TaskStateError } from "../src/task-store.js";

const taskInput = {
  org_id: "org_001",
  team_id: "team_001",
  user_id: "user_001",
  channel: "web" as const,
  intent: "analysis",
};

const clarificationPayload = {
  status: "needs_input",
  goal: "定位转化率下降原因",
  known_facts: [],
  assumptions: [],
  open_questions: ["确认统计周期"],
  dimensions: ["channel"],
  time_range: {
    description: "最近两周",
    start: null,
    end: null,
    timezone: "Asia/Shanghai",
    granularity: "day",
  },
  acceptance_criteria: ["结论引用查询证据"],
};

test("SQLite stores recover TaskRun lineage, ordered events, and Artifacts after reopen", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-pi-state-"));
  const databasePath = join(directory, "orchestrator.sqlite3");

  const first = new SqliteOrchestratorState(databasePath);
  const parent = first.tasks.create(taskInput);
  first.events.append(parent.task_run_id, "task.created", { status: "created" });
  const artifact = first.artifacts.create({
    artifactType: "clarification",
    taskRunId: parent.task_run_id,
    producer: "skill:data-requirement-clarifier",
    payload: clarificationPayload,
  });
  const incomplete = first.tasks.transition({
    taskRunId: parent.task_run_id,
    expectedStatus: "created",
    status: "incomplete",
    currentStage: "analysis_incomplete",
  });
  const child = first.tasks.create({
    ...taskInput,
    intent: "analysis_supplement_query",
    parent_task_run_id: parent.task_run_id,
  });
  first.events.append(parent.task_run_id, "artifact.created", {
    artifact_id: artifact.artifact_id,
  });
  first.close();

  const reopened = new SqliteOrchestratorState(databasePath);
  assert.deepEqual(reopened.tasks.get(parent.task_run_id), incomplete);
  assert.equal(reopened.tasks.get(child.task_run_id)?.parent_task_run_id, parent.task_run_id);
  assert.deepEqual(
    reopened.events.list(parent.task_run_id).map((event) => event.sequence),
    [1, 2],
  );
  assert.equal(reopened.events.list(parent.task_run_id, 1)[0]?.sequence, 2);
  assert.deepEqual(reopened.artifacts.latest(parent.task_run_id, "clarification"), artifact);
  reopened.close();
});

test("a new Application instance resumes a persisted SQL approval wait", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-pi-application-"));
  const databasePath = join(directory, "orchestrator.sqlite3");
  const forgeClient: ForgeQueryRunPort = {
    async createQueryRun(input) {
      return {
        query_run_id: "qr_restart_001",
        task_run_id: input.taskRunId,
        status: "needs_review",
        question: input.question,
        user_id: input.userId,
        datasource_id: "demo",
        forge_json: {},
        sql: "SELECT 1",
        sql_hash: `sha256:${"a".repeat(64)}`,
        dialect: "postgresql",
        registry_version: "v1",
        review_required: true,
        can_execute: false,
        expires_at: "2099-01-01T00:00:00Z",
        error: "",
      };
    },
    async approveQueryRun(input) {
      return {
        query_run_id: input.queryRunId,
        task_run_id: "tr_restart",
        status: "completed",
        sql_hash: input.sqlHash,
        dialect: "postgresql",
        registry_version: "v1",
        columns: ["n"],
        rows: [[1]],
        row_count: 1,
        truncated: false,
        execution_ms: 1,
        executed_at: "2026-08-21T17:00:00Z",
        error: "",
      };
    },
  };
  const config = loadConfig({ PI_ORCHESTRATOR_STATE_DB: databasePath });
  const firstState = new SqliteOrchestratorState(databasePath);
  const firstApplication = new OrchestratorApplication({
    config,
    tasks: firstState.tasks,
    events: firstState.events,
    artifacts: firstState.artifacts,
    attempts: firstState.attempts,
    transactions: firstState.transactions,
    forgeClient,
  });
  const created = firstApplication.createTask({
    ...taskInput,
    message: "查询订单",
  });
  await firstApplication.prepareQuery(created.task.task_run_id, {
    question: "查询订单",
    dialect: "postgresql",
  });
  firstState.close();

  const secondState = new SqliteOrchestratorState(databasePath);
  const secondApplication = new OrchestratorApplication({
    config,
    tasks: secondState.tasks,
    events: secondState.events,
    artifacts: secondState.artifacts,
    attempts: secondState.attempts,
    transactions: secondState.transactions,
    forgeClient,
  });
  assert.equal(
    secondApplication.getTask(created.task.task_run_id)?.status,
    "waiting_for_query_approval",
  );
  assert.ok(
    secondApplication
      .getEvents(created.task.task_run_id)
      .some((event) => event.event_type === "query.review_requested"),
  );
  const approved = await secondApplication.approveQuery(created.task.task_run_id, {
    queryRunId: "qr_restart_001",
    sqlHash: `sha256:${"a".repeat(64)}`,
    idempotencyKey: "approve-restart-001",
  });
  assert.equal(approved.task.status, "ready_for_analysis");
  const attempts = secondState.attempts.list(created.task.task_run_id);
  assert.deepEqual(
    attempts.map((attempt) => [attempt.stage, attempt.status]),
    [
      ["query_prepare", "succeeded"],
      ["query_execution", "succeeded"],
    ],
  );
  secondState.close();
});

test("SQLite Task transitions preserve cross-connection compare-and-set semantics", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-pi-cas-"));
  const databasePath = join(directory, "orchestrator.sqlite3");
  const first = new SqliteOrchestratorState(databasePath);
  const second = new SqliteOrchestratorState(databasePath);
  const task = first.tasks.create(taskInput);

  second.tasks.transition({
    taskRunId: task.task_run_id,
    expectedStatus: "created",
    status: "ready_for_query",
    currentStage: "query_prepare",
  });
  assert.throws(
    () =>
      first.tasks.transition({
        taskRunId: task.task_run_id,
        expectedStatus: "created",
        status: "clarifying",
        currentStage: "requirement_clarification",
      }),
    TaskStateError,
  );

  first.events.append(task.task_run_id, "task.created", {});
  second.events.append(task.task_run_id, "task.status_changed", {});
  assert.deepEqual(
    first.events.list(task.task_run_id).map((event) => event.sequence),
    [1, 2],
  );
  first.close();
  second.close();
});

test("expired StageAttempt is reconciled once without replaying the Stage", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-pi-attempt-"));
  const databasePath = join(directory, "orchestrator.sqlite3");
  const first = new SqliteOrchestratorState(databasePath);
  const task = first.tasks.create(taskInput);
  first.events.append(task.task_run_id, "task.created", {});
  const running = first.tasks.transition({
    taskRunId: task.task_run_id,
    expectedStatus: "created",
    status: "analyzing",
    currentStage: "business_root_cause_analysis",
  });
  const attempt = first.attempts.start({
    taskRunId: task.task_run_id,
    stage: "business_root_cause_analysis",
    idempotencyKey: "analysis-attempt-001",
    runningStatus: running.status,
    retryStatus: "ready_for_analysis",
    leaseMs: 1,
  });
  await new Promise((resolve) => setTimeout(resolve, 5));

  const second = new SqliteOrchestratorState(databasePath);
  const recovered = second.reconcileExpiredAttempts();
  assert.equal(recovered.length, 1);
  assert.equal(recovered[0]?.attempt_id, attempt.attempt_id);
  assert.equal(second.attempts.get(attempt.attempt_id)?.status, "interrupted");
  assert.equal(second.tasks.get(task.task_run_id)?.status, "ready_for_analysis");
  assert.equal(
    second.events.list(task.task_run_id).at(-1)?.event_type,
    "stage.attempt_interrupted",
  );
  assert.deepEqual(first.reconcileExpiredAttempts(), []);
  first.close();
  second.close();
});

test("reconciliation restores report and query execution retry statuses", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-pi-recovery-map-"));
  const state = new SqliteOrchestratorState(join(directory, "orchestrator.sqlite3"));
  const reportTask = state.tasks.create({...taskInput, intent: "report"});
  const queryTask = state.tasks.create({...taskInput, intent: "query"});
  state.tasks.transition({
    taskRunId: reportTask.task_run_id,
    expectedStatus: "created",
    status: "rendering",
    currentStage: "data_analysis_report",
  });
  state.tasks.transition({
    taskRunId: queryTask.task_run_id,
    expectedStatus: "created",
    status: "querying",
    currentStage: "query_execution",
  });
  state.attempts.start({
    taskRunId: reportTask.task_run_id,
    stage: "data_analysis_report",
    idempotencyKey: "report-crash-001",
    runningStatus: "rendering",
    retryStatus: "ready_for_report",
    leaseMs: 1,
  });
  state.attempts.start({
    taskRunId: queryTask.task_run_id,
    stage: "query_execution",
    idempotencyKey: "query-crash-001",
    runningStatus: "querying",
    retryStatus: "waiting_for_query_approval",
    leaseMs: 1,
  });
  await new Promise((resolve) => setTimeout(resolve, 5));
  assert.equal(state.reconcileExpiredAttempts().length, 2);
  assert.equal(state.tasks.get(reportTask.task_run_id)?.status, "ready_for_report");
  assert.equal(
    state.tasks.get(queryTask.task_run_id)?.status,
    "waiting_for_query_approval",
  );
  state.close();
});

test("active lease and one-running-attempt constraint fail closed", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-pi-active-attempt-"));
  const state = new SqliteOrchestratorState(join(directory, "orchestrator.sqlite3"));
  const task = state.tasks.create(taskInput);
  const attempt = state.attempts.start({
    taskRunId: task.task_run_id,
    stage: "analysis",
    idempotencyKey: "attempt-active-001",
    runningStatus: "analyzing",
    retryStatus: "ready_for_analysis",
    leaseMs: 60_000,
  });
  assert.deepEqual(state.reconcileExpiredAttempts(), []);
  assert.throws(
    () =>
      state.attempts.start({
        taskRunId: task.task_run_id,
        stage: "report",
        idempotencyKey: "attempt-active-002",
        runningStatus: "rendering",
        retryStatus: "ready_for_report",
        leaseMs: 60_000,
      }),
    /already has a running StageAttempt/,
  );
  assert.equal(state.attempts.finish(attempt.attempt_id, "succeeded").status, "succeeded");
  assert.equal(state.attempts.list(task.task_run_id).length, 1);
  state.close();
});

test("SQLite Artifact Store retains schema and producer validation", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-pi-artifact-"));
  const state = new SqliteOrchestratorState(join(directory, "orchestrator.sqlite3"));
  const task = state.tasks.create(taskInput);

  assert.throws(
    () =>
      state.artifacts.create({
        artifactType: "query_result",
        taskRunId: task.task_run_id,
        producer: "skill:not-forge",
        payload: {},
      }),
    /Invalid query_result Artifact|producer must be forge/,
  );
  assert.deepEqual(state.artifacts.list(task.task_run_id), []);
  state.close();
});
