import assert from "node:assert/strict";
import test from "node:test";

import { InMemoryArtifactStore } from "../src/artifacts.js";
import { ProductProjectionService } from "../src/product-projection-builder.js";
import { InMemoryStageAttemptStore } from "../src/stage-attempts.js";
import { InMemoryTaskStore, TaskStateError, type TaskRun } from "../src/task-store.js";
import { InMemoryTaskEventStore } from "../src/task-events.js";

function state() {
  const tasks = new InMemoryTaskStore();
  const events = new InMemoryTaskEventStore();
  const artifacts = new InMemoryArtifactStore();
  const attempts = new InMemoryStageAttemptStore();
  const service = new ProductProjectionService({
    tasks,
    events,
    artifacts,
    attempts,
    now: () => new Date("2026-08-25T10:00:00Z"),
  });
  return { tasks, events, artifacts, attempts, service };
}

function createTask(
  tasks: InMemoryTaskStore,
  overrides: Partial<{
    org: string;
    team: string;
    user: string;
    conversation: string;
    message: string;
  }> = {},
): TaskRun {
  return tasks.create({
    org_id: overrides.org ?? "org_demo",
    team_id: overrides.team ?? "team_data",
    user_id: overrides.user ?? "user_001",
    channel: "web",
    channel_conversation_id: overrides.conversation ?? "web_conv_001",
    intent: "business_root_cause_analysis",
    metadata: { original_message: overrides.message ?? "统计本月各渠道支付金额" },
  });
}

function completeConversationTask(
  stores: ReturnType<typeof state>,
  task: TaskRun,
  markdown = "已完成当前问题。",
): TaskRun {
  stores.events.append(task.task_run_id, "task.created", { status: "created" });
  stores.events.append(task.task_run_id, "channel.response_created", {
    title: "Forge 回答",
    markdown,
  });
  return stores.tasks.transition({
    taskRunId: task.task_run_id,
    expectedStatus: "created",
    status: "completed",
    currentStage: "channel_response",
  });
}

function waitingReviewTask(stores: ReturnType<typeof state>): TaskRun {
  const task = createTask(stores.tasks, { conversation: "web_conv_review" });
  stores.events.append(task.task_run_id, "task.created", { status: "created" });
  stores.artifacts.create({
    artifactType: "execution_plan",
    taskRunId: task.task_run_id,
    producer: "pi-planner",
    payload: {
      plan_revision: 1,
      supersedes_artifact_id: null,
      route_kind: "query",
      goal: "统计本月各渠道支付金额",
      required_deliverables: ["query_result", "analysis", "report"],
      status: "active",
      steps: [{
        step_id: "step_query",
        capability: "query",
        title: "准备并审批查询",
        depends_on: [],
        required: true,
        status: "waiting_approval",
        deliverable: "query_result",
      }],
    },
  });
  stores.events.append(task.task_run_id, "query.review_requested", {
    query_run_id: "qr_demo_001",
    sql: "SELECT channel, SUM(amount) FROM orders GROUP BY channel",
    sql_hash: `sha256:${"a".repeat(64)}`,
    assurance_report_hash: `sha256:${"b".repeat(64)}`,
    dialect: "postgresql",
    expires_at: "2026-08-25T10:30:00Z",
  });
  return stores.tasks.transition({
    taskRunId: task.task_run_id,
    expectedStatus: "created",
    status: "waiting_for_query_approval",
    currentStage: "query_review",
  });
}

test("Product Projection builds scoped Conversation list and detail from Task truth", () => {
  const stores = state();
  const first = completeConversationTask(stores, createTask(stores.tasks));
  const second = completeConversationTask(
    stores,
    createTask(stores.tasks, { message: "继续解释直营渠道的变化" }),
    "直营渠道贡献了主要变化。",
  );
  completeConversationTask(
    stores,
    createTask(stores.tasks, { org: "org_other", conversation: "web_conv_other" }),
  );

  const page = stores.service.listConversations({
    orgId: "org_demo",
    teamId: "team_data",
    userId: "user_001",
    channel: "web",
    limit: 20,
  });
  assert.equal(page.conversations.length, 1);
  assert.equal(page.conversations[0]!.task_count, 2);
  assert.equal(page.conversations[0]!.title, "统计本月各渠道支付金额");
  assert.equal(page.conversations[0]!.latest_task_run_id, second.task_run_id);
  assert.equal(page.next_cursor, null);

  const detail = stores.service.getConversation({
    orgId: "org_demo",
    teamId: "team_data",
    userId: "user_001",
    channel: "web",
    conversationId: "web_conv_001",
  });
  assert.ok(detail);
  assert.deepEqual(detail.entries.map((entry) => entry.task.task_run_id), [
    first.task_run_id,
    second.task_run_id,
  ]);
  assert.match(detail.entries[1]!.presentation.markdown, /直营渠道/);
  assert.equal(
    stores.service.getConversation({
      orgId: "org_other",
      teamId: "team_data",
      userId: "user_001",
      channel: "web",
      conversationId: "web_conv_001",
    }),
    undefined,
  );
});

test("Conversation cursor is opaque, stable, and scope preserving", () => {
  const stores = state();
  for (const conversation of ["web_conv_a", "web_conv_b", "web_conv_c"]) {
    completeConversationTask(stores, createTask(stores.tasks, { conversation, message: conversation }));
  }
  const first = stores.service.listConversations({
    orgId: "org_demo", teamId: "team_data", userId: "user_001", channel: "web", limit: 2,
  });
  assert.equal(first.conversations.length, 2);
  assert.ok(first.next_cursor);
  const second = stores.service.listConversations({
    orgId: "org_demo", teamId: "team_data", userId: "user_001", channel: "web", limit: 2,
    cursor: first.next_cursor,
  });
  assert.equal(second.conversations.length, 1);
  assert.equal(new Set([...first.conversations, ...second.conversations].map((item) => item.conversation_id)).size, 3);
  assert.throws(
    () => stores.service.listConversations({
      orgId: "org_demo", teamId: "team_data", userId: "user_001", channel: "web", limit: 2,
      cursor: "not-a-valid-cursor",
    }),
    /Invalid Conversation cursor/,
  );
});

test("Conversation detail cursor pages older TaskRuns without duplicating entries", () => {
  const stores = state();
  for (let index = 0; index < 101; index += 1) {
    completeConversationTask(stores, createTask(stores.tasks, {
      conversation: "web_conv_long",
      message: `追问 ${index + 1}`,
    }));
  }
  const latest = stores.service.getConversation({
    orgId: "org_demo",
    teamId: "team_data",
    userId: "user_001",
    channel: "web",
    conversationId: "web_conv_long",
  });
  assert.ok(latest);
  assert.equal(latest.entries.length, 100);
  assert.equal(latest.projection_meta.availability, "partial");
  assert.ok(latest.next_cursor);
  const older = stores.service.getConversation({
    orgId: "org_demo",
    teamId: "team_data",
    userId: "user_001",
    channel: "web",
    conversationId: "web_conv_long",
    cursor: latest.next_cursor,
  });
  assert.ok(older);
  assert.equal(older.entries.length, 1);
  assert.equal(older.next_cursor, null);
  assert.equal(
    new Set([...latest.entries, ...older.entries].map((entry) => entry.task.task_run_id)).size,
    101,
  );
});

test("Task Detail does not present empty legacy Attempt errors as failures", () => {
  const stores = state();
  const task = createTask(stores.tasks);
  const attempt = stores.attempts.start({
    taskRunId: task.task_run_id,
    stage: "query_prepare",
    idempotencyKey: "projection-empty-error",
    runningStatus: "querying",
    retryStatus: "ready_for_query",
    leaseMs: 60_000,
  });
  stores.attempts.finish(attempt.attempt_id, "succeeded", "");

  const detail = stores.service.getTaskDetail({
    orgId: "org_demo",
    teamId: "team_data",
    userId: "user_001",
    channel: "web",
    taskRunId: task.task_run_id,
  });
  assert.equal(detail?.attempts[0]?.safe_error, null);
  assert.equal(
    detail?.projection_meta.redactions.some((item) => item.field_path === "attempts.safe_error"),
    false,
  );
});

test("Task Detail projects exact query review and fails closed on broken review lineage", () => {
  const stores = state();
  const task = waitingReviewTask(stores);
  const detail = stores.service.getTaskDetail({
    orgId: "org_demo",
    teamId: "team_data",
    userId: "user_001",
    channel: "web",
    taskRunId: task.task_run_id,
  });
  assert.ok(detail);
  assert.equal(detail.review_request?.query_run_id, "qr_demo_001");
  assert.equal(detail.review_request?.read_only, true);
  assert.equal(detail.actions.find((action) => action.action_type === "approve_query")?.availability, "enabled");
  assert.equal(detail.presentation.source_artifact_ids.length, 1);
  assert.equal(
    stores.service.getTaskDetail({
      orgId: "org_demo",
      teamId: "team_data",
      userId: "user_other",
      channel: "web",
      taskRunId: task.task_run_id,
    }),
    undefined,
  );

  const broken = state();
  const brokenTask = createTask(broken.tasks, { conversation: "web_conv_broken" });
  broken.events.append(brokenTask.task_run_id, "task.created", {});
  broken.tasks.transition({
    taskRunId: brokenTask.task_run_id,
    expectedStatus: "created",
    status: "waiting_for_query_approval",
    currentStage: "query_review",
  });
  assert.throws(
    () => broken.service.getTaskDetail({
      orgId: "org_demo",
      teamId: "team_data",
      userId: "user_001",
      channel: "web",
      taskRunId: brokenTask.task_run_id,
    }),
    (error: unknown) => error instanceof TaskStateError && /query_review_required/.test(error.message),
  );
});
