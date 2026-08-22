import assert from "node:assert/strict";
import test from "node:test";

import { InMemoryTaskStore, TaskStateError } from "../src/task-store.js";


test("Pi task store owns TaskRun creation and stage transitions", () => {
  const store = new InMemoryTaskStore();
  const created = store.create({
    org_id: "org_demo",
    team_id: "team_growth",
    user_id: "user_123",
    channel: "web",
    intent: "business_root_cause_analysis",
  });

  assert.match(created.task_run_id, /^tr_[A-Za-z0-9_-]+$/);
  assert.equal(created.status, "created");

  const clarifying = store.transition({
    taskRunId: created.task_run_id,
    expectedStatus: "created",
    status: "clarifying",
    currentStage: "requirement_clarification",
  });
  assert.equal(clarifying.status, "clarifying");
  assert.equal(clarifying.current_stage, "requirement_clarification");
});


test("task transitions use optimistic status checks to prevent two orchestrators", () => {
  const store = new InMemoryTaskStore();
  const task = store.create({
    org_id: "org_demo",
    team_id: "team_growth",
    user_id: "user_123",
    channel: "feishu",
    intent: "data_requirement_clarification",
  });

  store.transition({
    taskRunId: task.task_run_id,
    expectedStatus: "created",
    status: "clarifying",
    currentStage: "requirement_clarification",
  });

  assert.throws(
    () =>
      store.transition({
        taskRunId: task.task_run_id,
        expectedStatus: "created",
        status: "ready_for_query",
        currentStage: "query_prepare",
      }),
    TaskStateError,
  );
});


test("terminal tasks cannot be restarted", () => {
  const store = new InMemoryTaskStore();
  const task = store.create({
    org_id: "org_demo",
    team_id: "team_growth",
    user_id: "user_123",
    channel: "api",
    intent: "query",
  });

  store.transition({
    taskRunId: task.task_run_id,
    expectedStatus: "created",
    status: "cancelled",
    currentStage: null,
  });

  assert.throws(
    () =>
      store.transition({
        taskRunId: task.task_run_id,
        expectedStatus: "cancelled",
        status: "created",
        currentStage: null,
      }),
    TaskStateError,
  );
});


test("task store returns defensive copies", () => {
  const store = new InMemoryTaskStore();
  const task = store.create({
    org_id: "org_demo",
    team_id: "team_growth",
    user_id: "user_123",
    channel: "dingtalk",
    intent: "query",
    metadata: { source: "test" },
  });

  task.metadata.source = "mutated";
  assert.equal(store.get(task.task_run_id)?.metadata.source, "test");
});


test("task list is bounded to the requested team scope and channel", () => {
  const store = new InMemoryTaskStore();
  const feishu = store.create({
    org_id: "org_demo", team_id: "team_growth", user_id: "user_1",
    channel: "feishu", intent: "query",
  });
  store.create({
    org_id: "org_demo", team_id: "team_growth", user_id: "user_2",
    channel: "web", intent: "query",
  });
  store.create({
    org_id: "org_other", team_id: "team_growth", user_id: "user_3",
    channel: "feishu", intent: "query",
  });

  const tasks = store.list({
    orgId: "org_demo", teamId: "team_growth", channel: "feishu", limit: 10,
  });

  assert.deepEqual(tasks.map((task) => task.task_run_id), [feishu.task_run_id]);
  tasks[0]!.metadata.mutated = true;
  assert.equal(store.get(feishu.task_run_id)?.metadata.mutated, undefined);
});
