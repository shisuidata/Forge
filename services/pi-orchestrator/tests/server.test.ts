import assert from "node:assert/strict";
import { mkdtemp } from "node:fs/promises";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { OrchestratorApplication } from "../src/application.js";
import { loadConfig } from "../src/config.js";
import { createOrchestratorServer } from "../src/server.js";

const SQL_HASH = `sha256:${"a".repeat(64)}`;


test("health endpoints expose the restricted runtime capabilities", async (context) => {
  const baseConfig = loadConfig({});
  const agentDir = await mkdtemp(join(tmpdir(), "forge-pi-agent-"));
  const server = createOrchestratorServer({
    ...baseConfig,
    agentDir,
    stateDbPath: join(agentDir, "state.sqlite3"),
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  context.after(() => server.close());

  const address = server.address() as AddressInfo;
  const baseUrl = `http://127.0.0.1:${address.port}`;

  const liveResponse = await fetch(`${baseUrl}/health/live`);
  assert.equal(liveResponse.status, 200);

  const readinessResponse = await fetch(`${baseUrl}/health/readiness`);
  assert.equal(readinessResponse.status, 200);
  const readiness = (await readinessResponse.json()) as {
    status: string;
    capabilities: {
      builtinToolsEnabled: boolean;
      modelExecutionConfigured: boolean;
      skills: string[];
    };
  };
  assert.equal(readiness.status, "degraded");
  assert.equal(readiness.capabilities.builtinToolsEnabled, false);
  assert.equal(readiness.capabilities.modelExecutionConfigured, false);
  assert.equal(readiness.capabilities.skills.length, 4);
});


test("default Server wiring restores persisted TaskRun after restart", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-pi-server-state-"));
  const config = loadConfig({
    PI_ORCHESTRATOR_STATE_DB: join(directory, "orchestrator.sqlite3"),
  });
  const first = createOrchestratorServer(config);
  await new Promise<void>((resolve) => first.listen(0, "127.0.0.1", resolve));
  const firstAddress = first.address() as AddressInfo;
  const createdResponse = await fetch(`http://127.0.0.1:${firstAddress.port}/v1/tasks`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      org_id: "org_restart",
      team_id: "team_restart",
      user_id: "user_restart",
      channel: "web",
      intent: "query_prepare",
      message: "查询订单",
    }),
  });
  assert.equal(createdResponse.status, 201);
  const created = (await createdResponse.json()) as { task: { task_run_id: string } };
  await new Promise<void>((resolve, reject) =>
    first.close((error) => (error === undefined ? resolve() : reject(error))),
  );

  const second = createOrchestratorServer(config);
  await new Promise<void>((resolve) => second.listen(0, "127.0.0.1", resolve));
  const secondAddress = second.address() as AddressInfo;
  const restoredResponse = await fetch(
    `http://127.0.0.1:${secondAddress.port}/v1/tasks/${created.task.task_run_id}`,
  );
  assert.equal(restoredResponse.status, 200);
  const restored = (await restoredResponse.json()) as { task: { status: string } };
  assert.equal(restored.task.status, "created");
  const attemptsResponse = await fetch(
    `http://127.0.0.1:${secondAddress.port}/v1/tasks/${created.task.task_run_id}/attempts`,
  );
  assert.equal(attemptsResponse.status, 200);
  assert.deepEqual(await attemptsResponse.json(), { attempts: [] });
  await new Promise<void>((resolve, reject) =>
    second.close((error) => (error === undefined ? resolve() : reject(error))),
  );
});

test("async Stage returns 202 and completes through Task polling", async (context) => {
  let releaseStage: (() => void) | undefined;
  const gate = new Promise<void>((resolve) => { releaseStage = resolve; });
  const config = loadConfig({});
  const application = new OrchestratorApplication({
    config,
    forgeClient: {
      async createQueryRun() { throw new Error("not used"); },
      async approveQueryRun() { throw new Error("not used"); },
    },
    skillExecutor: {
      async clarify() {
        await gate;
        return {
          status: "needs_input",
          goal: "确认分析范围",
          known_facts: [],
          assumptions: [],
          open_questions: ["统计周期是什么？"],
          dimensions: [],
          time_range: { description: "待确认" },
          acceptance_criteria: ["周期明确"],
        };
      },
      async reviewMetric() { throw new Error("not used"); },
      async analyze() { throw new Error("not used"); },
      async writeReport() { throw new Error("not used"); },
    },
  });
  const created = application.createTask({
    org_id: "org_async",
    team_id: "team_async",
    user_id: "user_async",
    channel: "web",
    intent: "data_task",
    message: "分析转化率",
  });
  const server = createOrchestratorServer(config, application);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  context.after(() => server.close());
  const address = server.address() as AddressInfo;
  const baseUrl = `http://127.0.0.1:${address.port}`;

  const acceptedResponse = await fetch(
    `${baseUrl}/v1/tasks/${created.task.task_run_id}/clarify`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        message: "分析转化率",
        idempotency_key: "clarify-async-001",
        async: true,
      }),
    },
  );
  assert.equal(acceptedResponse.status, 202);
  const accepted = (await acceptedResponse.json()) as { status: string; task: { status: string } };
  assert.equal(accepted.status, "accepted");
  assert.equal(accepted.task.status, "clarifying");

  releaseStage?.();
  await new Promise((resolve) => setTimeout(resolve, 10));
  const completedResponse = await fetch(`${baseUrl}/v1/tasks/${created.task.task_run_id}`);
  const completed = (await completedResponse.json()) as { task: { status: string } };
  assert.equal(completed.task.status, "needs_input");
});

test("Task API exposes validated Skill Artifacts instead of model text", async (context) => {
  const config = loadConfig({});
  const application = new OrchestratorApplication({
    config,
    forgeClient: {
      async createQueryRun() {
        throw new Error("not used");
      },
      async approveQueryRun() {
        throw new Error("not used");
      },
    },
    skillExecutor: {
      async analyze() {
        throw new Error("not used");
      },
      async writeReport() {
        throw new Error("not used");
      },
      async clarify() {
        return {
          status: "needs_input",
          goal: "确认转化分析需求",
          known_facts: ["用户观察到转化下降"],
          assumptions: [],
          open_questions: ["时间范围是什么？"],
          dimensions: ["渠道"],
          time_range: { description: "待确认" },
          acceptance_criteria: ["时间范围明确"],
        };
      },
      async reviewMetric() {
        throw new Error("not used");
      },
    },
  });
  const server = createOrchestratorServer(config, application);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  context.after(() => server.close());
  const address = server.address() as AddressInfo;
  const baseUrl = `http://127.0.0.1:${address.port}`;

  const createdResponse = await fetch(`${baseUrl}/v1/tasks`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      org_id: "org_demo",
      team_id: "team_growth",
      user_id: "trusted-user",
      channel: "web",
      intent: "data_task",
      message: "最近转化为什么下降",
    }),
  });
  const created = (await createdResponse.json()) as { task: { task_run_id: string } };
  const clarifiedResponse = await fetch(
    `${baseUrl}/v1/tasks/${created.task.task_run_id}/clarify`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ message: "最近转化为什么下降" }),
    },
  );
  assert.equal(clarifiedResponse.status, 200);
  const clarified = (await clarifiedResponse.json()) as {
    task: { status: string };
    artifact: { artifact_type: string; payload: { status: string } };
  };
  assert.equal(clarified.task.status, "needs_input");
  assert.equal(clarified.artifact.artifact_type, "clarification");

  const artifactsResponse = await fetch(
    `${baseUrl}/v1/tasks/${created.task.task_run_id}/artifacts`,
  );
  const artifacts = (await artifactsResponse.json()) as { artifacts: unknown[] };
  assert.equal(artifacts.artifacts.length, 1);
});


test("Task API returns ordered events and a non-executable review request", async (context) => {
  const config = loadConfig({});
  const application = new OrchestratorApplication({
    config,
    forgeClient: {
      async createQueryRun(input) {
        return {
          query_run_id: "qr_demo_001",
          task_run_id: input.taskRunId,
          status: "needs_review",
          question: input.question,
          user_id: input.userId,
          datasource_id: "demo",
          forge_json: { scan: "orders", select: ["orders.id"] },
          sql: "SELECT orders.id FROM orders",
          sql_hash: SQL_HASH,
          dialect: input.dialect ?? "sqlite",
          registry_version: "sha256:registry",
          assurance_report: { status: "passed" },
          assurance_report_hash: `sha256:${"b".repeat(64)}`,
          assurance_revision: "query-assurance-v1",
          policy_revision: "convention-policy-v1",
          model_revision: "sha256:model",
          assurance_registry_revision: "sha256:assurance-registry",
          review_required: true,
          can_execute: false,
          expires_at: "2026-08-21T18:00:00Z",
          error: "",
        };
      },
      async approveQueryRun(input) {
        return {
          query_run_id: input.queryRunId,
          task_run_id: "tr_demo",
          status: "completed",
          sql_hash: input.sqlHash,
          dialect: "sqlite",
          registry_version: "sha256:registry",
          assurance_report: { status: "passed" },
          assurance_report_hash: `sha256:${"b".repeat(64)}`,
          assurance_revision: "query-assurance-v1",
          policy_revision: "convention-policy-v1",
          model_revision: "sha256:model",
          assurance_registry_revision: "sha256:assurance-registry",
          columns: ["n"],
          rows: [[1]],
          row_count: 1,
          truncated: false,
          execution_ms: 1,
          executed_at: "2026-08-21T18:00:00Z",
          error: "",
        };
      },
    },
  });
  const server = createOrchestratorServer(config, application);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  context.after(() => server.close());
  const address = server.address() as AddressInfo;
  const baseUrl = `http://127.0.0.1:${address.port}`;

  const createResponse = await fetch(`${baseUrl}/v1/tasks`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      org_id: "org_demo",
      team_id: "team_growth",
      user_id: "trusted-user",
      channel: "web",
      intent: "query_prepare",
      message: "查询订单 ID",
    }),
  });
  assert.equal(createResponse.status, 201);
  const created = (await createResponse.json()) as { task: { task_run_id: string } };

  const prepareResponse = await fetch(
    `${baseUrl}/v1/tasks/${created.task.task_run_id}/prepare-query`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ question: "查询订单 ID", dialect: "postgresql" }),
    },
  );
  assert.equal(prepareResponse.status, 200);
  const prepared = (await prepareResponse.json()) as {
    task: { status: string };
    events: Array<{ sequence: number; event_type: string; payload: Record<string, unknown> }>;
  };
  assert.equal(prepared.task.status, "waiting_for_query_approval");
  assert.deepEqual(
    prepared.events.map((event) => event.sequence),
    [1, 2, 3, 4],
  );
  assert.equal(prepared.events.at(-1)?.event_type, "query.review_requested");
  assert.equal(prepared.events.at(-1)?.payload.can_execute, false);

  const approvalResponse = await fetch(
    `${baseUrl}/v1/tasks/${created.task.task_run_id}/approve-query`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        query_run_id: "qr_demo_001",
        sql_hash: SQL_HASH,
        idempotency_key: `${created.task.task_run_id}:approve:qr_demo_001`,
      }),
    },
  );
  assert.equal(approvalResponse.status, 200);
  const approved = (await approvalResponse.json()) as {
    task: { status: string };
    events: Array<{ sequence: number; event_type: string }>;
  };
  assert.equal(approved.task.status, "completed");
  assert.equal(approved.events.length, 9);
  assert.equal(approved.events.at(-1)?.event_type, "query.completed");
});
