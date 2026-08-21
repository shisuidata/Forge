import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { OrchestratorApplication } from "../src/application.js";
import { ChannelIdentityError, ChannelIdentityResolver } from "../src/channels/identity.js";
import { renderChannelPresentation } from "../src/channels/renderer.js";
import { loadConfig } from "../src/config.js";
import { createOrchestratorServer } from "../src/server.js";
import { SqliteOrchestratorState } from "../src/sqlite-store.js";
import type { TaskEvent } from "../src/task-events.js";
import type { TaskRun } from "../src/task-store.js";

function task(status: TaskRun["status"]): TaskRun {
  return {
    task_run_id: "tr_channel_001",
    org_id: "org_demo",
    team_id: "team_data",
    user_id: "user_demo",
    channel: "feishu",
    channel_conversation_id: "oc_demo",
    intent: "business_root_cause_analysis",
    status,
    current_stage: "query_review",
    correlation_id: "feishu:evt_001",
    parent_task_run_id: null,
    created_at: "2026-08-21T00:00:00.000Z",
    updated_at: "2026-08-21T00:00:00.000Z",
    metadata: {},
  };
}

function event(sequence: number, eventType: TaskEvent["event_type"], payload: Record<string, unknown>): TaskEvent {
  return {
    event_id: `te_${sequence}`,
    task_run_id: "tr_channel_001",
    sequence,
    event_type: eventType,
    created_at: "2026-08-21T00:00:00.000Z",
    payload,
  };
}

test("channel renderer emits a hash-bound review action without advancing state", () => {
  const presentation = renderChannelPresentation({
    task: task("waiting_for_query_approval"),
    events: [
      event(1, "query.review_requested", {
        query_run_id: "qr_channel_001",
        sql: "SELECT 1",
        sql_hash: `sha256:${"a".repeat(64)}`,
        assurance_report_hash: `sha256:${"b".repeat(64)}`,
      }),
    ],
    artifacts: [],
  });
  assert.equal(presentation.kind, "query_review");
  assert.match(presentation.markdown, /SELECT 1/);
  assert.deepEqual(presentation.actions[0]?.payload, {
    query_run_id: "qr_channel_001",
    sql_hash: `sha256:${"a".repeat(64)}`,
    assurance_report_hash: `sha256:${"b".repeat(64)}`,
  });
  assert.equal(presentation.actions[0]?.type, "approve_query");
});

test("identity resolver fails closed for unknown channel users", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-channel-identity-"));
  const path = join(directory, "identities.json");
  await writeFile(path, JSON.stringify({
    feishu: {
      ou_allowed: { org_id: "org_demo", team_id: "team_data", user_id: "user_demo" },
    },
  }));
  const resolver = new ChannelIdentityResolver(path);
  assert.deepEqual(resolver.resolve("feishu", "ou_allowed"), {
    org_id: "org_demo",
    team_id: "team_data",
    user_id: "user_demo",
  });
  assert.throws(() => resolver.resolve("feishu", "ou_unknown"), ChannelIdentityError);
});

test("duplicate Feishu delivery returns one TaskRun and one Forge preparation", async (context) => {
  const directory = await mkdtemp(join(tmpdir(), "forge-channel-event-"));
  const identityPath = join(directory, "identities.json");
  await writeFile(identityPath, JSON.stringify({
    feishu: {
      ou_allowed: { org_id: "org_demo", team_id: "team_data", user_id: "user_demo" },
      ou_other: { org_id: "org_demo", team_id: "team_data", user_id: "user_other" },
    },
  }));
  const config = loadConfig({
    PI_ORCHESTRATOR_STATE_DB: join(directory, "state.sqlite3"),
    PI_CHANNEL_IDENTITY_MAP: identityPath,
    PI_CHANNEL_SERVICE_KEYS: "channel-secret",
  });
  const state = new SqliteOrchestratorState(config.stateDbPath);
  let releaseForge: (() => void) | undefined;
  const forgeGate = new Promise<void>((resolve) => { releaseForge = resolve; });
  let prepareCalls = 0;
  let approveCalls = 0;
  const application = new OrchestratorApplication({
    config,
    tasks: state.tasks,
    events: state.events,
    artifacts: state.artifacts,
    attempts: state.attempts,
    channelEvents: state.channelEvents,
    transactions: state.transactions,
    forgeClient: {
      async createQueryRun(input) {
        prepareCalls += 1;
        await forgeGate;
        return {
          query_run_id: "qr_channel_001",
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
          assurance_report: { status: "passed" },
          assurance_report_hash: `sha256:${"b".repeat(64)}`,
          assurance_revision: "query-assurance-v1",
          policy_revision: "convention-policy-v1",
          model_revision: "sha256:model",
          assurance_registry_revision: "sha256:assurance-registry",
          review_required: true,
          can_execute: false,
          expires_at: "2099-01-01T00:00:00Z",
          error: "",
        };
      },
      async approveQueryRun(input) {
        approveCalls += 1;
        return {
          query_run_id: input.queryRunId,
          task_run_id: "tr_channel",
          status: "completed",
          sql_hash: input.sqlHash,
          dialect: "postgresql",
          registry_version: "v1",
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
          executed_at: "2026-08-21T17:00:00Z",
          error: "",
        };
      },
    },
  });
  const server = createOrchestratorServer(config, application);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  context.after(() => {
    server.close();
    state.close();
  });
  const address = server.address() as AddressInfo;
  const url = `http://127.0.0.1:${address.port}/v1/channel-events`;
  const payload = {
    event_id: "evt_feishu_001",
    channel: "feishu",
    event_type: "message",
    external_user_id: "ou_allowed",
    conversation_id: "oc_demo",
    message_id: "om_demo",
    task_run_id: null,
    payload: { text: "查询订单" },
  };
  const headers = {
    "content-type": "application/json",
    "x-channel-service-key": "channel-secret",
  };
  const first = await fetch(url, { method: "POST", headers, body: JSON.stringify(payload) });
  assert.equal(first.status, 202);
  const accepted = (await first.json()) as { task: { task_run_id: string } };
  const duplicate = await fetch(url, { method: "POST", headers, body: JSON.stringify(payload) });
  assert.equal(duplicate.status, 200);
  const replayed = (await duplicate.json()) as { task: { task_run_id: string }; duplicate: boolean };
  assert.equal(replayed.task.task_run_id, accepted.task.task_run_id);
  assert.equal(replayed.duplicate, true);
  assert.equal(prepareCalls, 1);

  releaseForge?.();
  await new Promise((resolve) => setTimeout(resolve, 10));
  const presentationResponse = await fetch(
    `http://127.0.0.1:${address.port}/v1/tasks/${accepted.task.task_run_id}/presentation`,
    { headers },
  );
  const rendered = (await presentationResponse.json()) as {
    presentation: { kind: string; actions: Array<{ type: string }> };
  };
  assert.equal(rendered.presentation.kind, "query_review");
  assert.equal(rendered.presentation.actions[0]?.type, "approve_query");

  const actionPayload = {
    event_id: "evt_feishu_action_001",
    channel: "feishu",
    event_type: "action",
    external_user_id: "ou_allowed",
    conversation_id: "oc_demo",
    message_id: "om_card_demo",
    task_run_id: accepted.task.task_run_id,
    payload: {
      action: "approve_query",
      query_run_id: "qr_channel_001",
      sql_hash: `sha256:${"a".repeat(64)}`,
      assurance_report_hash: `sha256:${"b".repeat(64)}`,
    },
  };
  const forgedAction = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify({
      ...actionPayload,
      event_id: "evt_feishu_action_forged",
      external_user_id: "ou_other",
    }),
  });
  assert.equal(forgedAction.status, 409);
  assert.equal(approveCalls, 0);

  const action = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(actionPayload),
  });
  assert.ok(action.status === 200 || action.status === 202);
  await new Promise((resolve) => setTimeout(resolve, 10));
  const actionReplay = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(actionPayload),
  });
  assert.equal(actionReplay.status, 200);
  assert.equal(approveCalls, 1);
  const resultPresentation = await fetch(
    `http://127.0.0.1:${address.port}/v1/tasks/${accepted.task.task_run_id}/presentation`,
    { headers },
  );
  const resultRendered = (await resultPresentation.json()) as {
    presentation: { kind: string };
  };
  assert.equal(resultRendered.presentation.kind, "query_result");
});

test("channel endpoint rejects missing service credential before identity lookup", async (context) => {
  const directory = await mkdtemp(join(tmpdir(), "forge-channel-auth-"));
  const config = loadConfig({
    PI_ORCHESTRATOR_STATE_DB: join(directory, "state.sqlite3"),
    PI_CHANNEL_SERVICE_KEYS: "channel-secret",
  });
  const server = createOrchestratorServer(config);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  context.after(() => server.close());
  const address = server.address() as AddressInfo;
  const response = await fetch(`http://127.0.0.1:${address.port}/v1/channel-events`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({}),
  });
  assert.equal(response.status, 403);
});
