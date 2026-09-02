import assert from "node:assert/strict";
import { chmod, mkdtemp, readFile, stat, writeFile } from "node:fs/promises";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { OrchestratorApplication } from "../src/application.js";
import type { Artifact } from "../src/artifacts.js";
import { ChannelIdentityError, ChannelIdentityResolver } from "../src/channels/identity.js";
import { routeChannelMessage } from "../src/channels/intent.js";
import { parseChannelEvent } from "../src/channels/contracts.js";
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
  assert.deepEqual(presentation.fields, []);
  assert.doesNotMatch(presentation.markdown, /QueryRun|sha256/);
});

test("channel renderer exposes clarification, cancellation, and supplement actions", () => {
  const needsInput = renderChannelPresentation({
    task: task("needs_input"),
    events: [],
    artifacts: [],
  });
  assert.deepEqual(needsInput.actions.map((item) => item.type), ["provide_input", "cancel_task"]);

  const analysisArtifact = {
    artifact_id: "art_analysis",
    task_run_id: "tr_channel_001",
    artifact_type: "analysis",
    schema_version: "1.0.0",
    producer: "business-root-cause-analysis",
    created_at: "2026-08-21T00:00:00.000Z",
    payload: {
      status: "incomplete",
      method_summary: { objective: "定位原因", dimensions: ["channel"], comparison_baseline: "渠道对比", approach_steps: ["检查现有数据"] },
      summary: "需要补查",
      findings: [],
      hypotheses: [],
      evidence_refs: [],
      limitations: [],
      suggested_queries: [{ question: "按渠道补查", reason: "缺少渠道", priority: "high" }],
    },
  } as unknown as Artifact;
  const incomplete = renderChannelPresentation({
    task: task("incomplete"),
    events: [],
    artifacts: [analysisArtifact],
  });
  assert.equal(incomplete.actions[0]?.type, "request_supplement");
  assert.deepEqual(incomplete.actions[0]?.payload, { suggested_query_index: 0 });
  assert.equal(incomplete.actions.at(-1)?.type, "cancel_task");
});

test("channel intent routes greetings away from Forge and preserves data requests", () => {
  assert.equal(routeChannelMessage("你好").kind, "conversation");
  assert.equal(routeChannelMessage("销售额的口径是什么").kind, "knowledge");
  assert.equal(routeChannelMessage("统计本月各渠道销售额").kind, "query");
  assert.equal(routeChannelMessage("你好，帮我查询订单数").kind, "query");
});

test("channel greeting completes without calling Forge or a model", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-channel-greeting-"));
  const config = loadConfig({ PI_ORCHESTRATOR_STATE_DB: join(directory, "state.sqlite3") });
  const state = new SqliteOrchestratorState(config.stateDbPath);
  let forgeCalls = 0;
  const application = new OrchestratorApplication({
    config,
    tasks: state.tasks,
    events: state.events,
    artifacts: state.artifacts,
    attempts: state.attempts,
    channelEvents: state.channelEvents,
    transactions: state.transactions,
    forgeClient: {
      async createQueryRun() { forgeCalls += 1; throw new Error("must not query"); },
      async approveQueryRun() { throw new Error("must not execute"); },
    },
  });
  const result = await application.ingestChannelMessage({
    event_id: "evt_greeting", channel: "feishu", event_type: "message",
    external_user_id: "ou_allowed", conversation_id: "oc_demo", message_id: "om_greeting",
    task_run_id: null, payload: { text: "你好", chat_type: "p2p" },
  }, { org_id: "org_demo", team_id: "team_demo", user_id: "user_demo" });

  assert.equal(result.task.status, "completed");
  assert.equal(result.task.intent, "channel_conversation");
  assert.equal(result.presentation.kind, "report");
  assert.match(result.presentation.markdown, /查询、统计和分析业务数据/);
  assert.equal(forgeCalls, 0);
  state.close();
});

test("channel knowledge answer is bound to Forge context evidence without a QueryRun", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-channel-knowledge-"));
  const config = loadConfig({ PI_ORCHESTRATOR_STATE_DB: join(directory, "state.sqlite3") });
  const state = new SqliteOrchestratorState(config.stateDbPath);
  let queryCalls = 0;
  let suppliedEvidence: unknown;
  const contextEvidence = [{
    evidence_ref: `ctx_${"a".repeat(24)}`,
    source_type: "metric" as const,
    title: "revenue · 销售额",
    content: "销售额使用 orders.total_amount",
    score: 9,
    verification_level: "verified" as const,
    scope: "organization" as const,
    source_revision: `sha256:${"c".repeat(64)}`,
    updated_at: null,
    expires_at: null,
  }];
  const application = new OrchestratorApplication({
    config,
    tasks: state.tasks,
    events: state.events,
    artifacts: state.artifacts,
    attempts: state.attempts,
    channelEvents: state.channelEvents,
    transactions: state.transactions,
    forgeClient: {
      async searchContext() {
        return { status: "ok", question: "销售额口径", evidence: contextEvidence,
          evidence_count: 1, context_revision: `sha256:${"b".repeat(64)}`, bounded: true };
      },
      async createQueryRun() { queryCalls += 1; throw new Error("must not query"); },
      async approveQueryRun() { throw new Error("must not execute"); },
    },
    skillExecutor: {
      async clarify() { throw new Error("not used"); },
      async reviewMetric() { throw new Error("not used"); },
      async analyze() { throw new Error("not used"); },
      async writeReport() { throw new Error("not used"); },
      async advise(_task, skillName, input) {
        suppliedEvidence = input.contextEvidence;
        return {
          status: "complete", skill_name: skillName, title: "销售额口径", summary: "销售额取订单支付金额。",
          findings: [{ statement: "默认使用 orders.total_amount。", evidence_refs: [contextEvidence[0]!.evidence_ref], confidence: "high" }],
          recommendations: [], assumptions: [], limitations: [], open_questions: [], deliverables: [],
        };
      },
    },
  });
  const result = await application.ingestChannelMessage({
    event_id: "evt_knowledge", channel: "feishu", event_type: "message",
    external_user_id: "ou_allowed", conversation_id: "oc_demo", message_id: "om_knowledge",
    task_run_id: null, payload: { text: "销售额的口径是什么", chat_type: "p2p" },
  }, { org_id: "org_demo", team_id: "team_demo", user_id: "user_demo" });

  assert.equal(result.task.status, "completed");
  assert.equal(result.task.intent, "knowledge_answer");
  assert.equal(result.presentation.kind, "report");
  assert.match(result.presentation.markdown, /orders\.total_amount/);
  assert.doesNotMatch(result.presentation.markdown, /ctx_/);
  assert.deepEqual(suppliedEvidence, contextEvidence);
  assert.equal(queryCalls, 0);
  state.close();
});

test("channel renderer preserves advisory semantics as readable safe Markdown", () => {
  const presentation = renderChannelPresentation({
    task: task("completed"),
    events: [],
    artifacts: [{
      artifact_id: "art_advisory", task_run_id: "tr_channel_001", artifact_type: "advisory",
      schema_version: "1.0.0", producer: "metric-definition-reviewer",
      created_at: "2026-08-21T00:00:00.000Z",
      payload: {
        status: "complete", skill_name: "metric-definition-reviewer", title: "销售额口径说明",
        summary: "默认使用已支付订单金额。",
        findings: [{
          statement: "默认业务口径：使用 dwd_order_detail.total_amount。",
          evidence_refs: [], confidence: "high",
        }],
        recommendations: [{ action: "先确认退款口径", rationale: "不同报表可能不一致", priority: "high" }],
        assumptions: ["当前问题指订单支付口径"], limitations: ["未覆盖退款后净额"],
        open_questions: ["是否扣除退款？"],
        deliverables: [{ name: "字段说明", content: "requires_clarification 为 true 时先澄清。" }],
      },
    } as unknown as Artifact],
  });

  assert.match(presentation.markdown, /^## 核心说明/m);
  assert.match(presentation.markdown, /## 关键要点/);
  assert.match(presentation.markdown, /\*\*默认业务口径\*\*：使用 `dwd_order_detail\.total_amount`/);
  assert.match(presentation.markdown, /## 建议行动/);
  assert.match(presentation.markdown, /> \*\*前提假设\*\*/);
  assert.match(presentation.markdown, /> \*\*限制\*\*/);
  assert.match(presentation.markdown, /> \*\*待确认\*\*/);
  assert.match(presentation.markdown, /## 字段说明/);
  assert.doesNotMatch(presentation.markdown, /ctx_|TaskRun|sha256/);
});

test("channel renderer hides reasoning, internal lineage, raw errors, and stage names", () => {
  const failed = renderChannelPresentation({
    task: task("failed"),
    events: [event(1, "skill.execution_failed", {
      error: `Traceback (most recent call last): /home/forge TaskRun tr_${"a".repeat(24)}`,
    })],
    artifacts: [],
  });
  assert.equal(failed.markdown, "本次处理未能完成，请稍后重试或重新发起。");
  assert.deepEqual(failed.fields, []);

  const quota = renderChannelPresentation({
    task: task("failed"),
    events: [event(1, "query.prepare_failed", {
      error: "模型服务额度已用完，请在额度恢复后重新发起。",
    })],
    artifacts: [],
  });
  assert.equal(quota.markdown, "模型服务额度已用完，请在额度恢复后重新发起。");

  const progressTask = { ...task("analyzing"), current_stage: "skill:business-root-cause-analysis" };
  const progress = renderChannelPresentation({ task: progressTask, events: [], artifacts: [] });
  assert.equal(progress.markdown, "正在基于查询结果进行分析。");
  assert.doesNotMatch(JSON.stringify({ markdown: progress.markdown, fields: progress.fields }), /current_stage|business-root|analyzing/);

  const report = renderChannelPresentation({
    task: task("completed"),
    events: [],
    artifacts: [{
      artifact_id: "art_report", task_run_id: "tr_channel_001", artifact_type: "rendered_output",
      schema_version: "1.0.0", producer: "data-analysis-report-writer",
      created_at: "2026-08-21T00:00:00.000Z",
      payload: {
        title: "业务报告",
        markdown: `<think>internal reasoning</think>\n业务结论正常。\nTaskRun tr_${"b".repeat(24)}`,
      },
    } as unknown as Artifact],
  });
  assert.equal(report.markdown, "业务结论正常。");
});

test("personal memory requires an explicit channel approval and never escalates scope", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-channel-memory-"));
  const config = loadConfig({ PI_ORCHESTRATOR_STATE_DB: join(directory, "state.sqlite3") });
  const state = new SqliteOrchestratorState(config.stateDbPath);
  const memoryWrites: Array<Record<string, unknown>> = [];
  const application = new OrchestratorApplication({
    config, tasks: state.tasks, events: state.events, artifacts: state.artifacts,
    attempts: state.attempts, channelEvents: state.channelEvents, transactions: state.transactions,
    forgeClient: {
      async createQueryRun() { throw new Error("not used"); },
      async approveQueryRun() { throw new Error("not used"); },
      async writeMemory(input) { memoryWrites.push(input); return { status: "confirmed" }; },
    },
  });
  const identity = { org_id: "org_demo", team_id: "team_demo", user_id: "user_demo" };
  const proposed = await application.ingestChannelMessage({
    event_id: "evt_memory", channel: "feishu", event_type: "message", external_user_id: "ou_demo",
    conversation_id: "oc_demo", message_id: "om_memory", task_run_id: null,
    payload: { text: "记住我默认看自然月", chat_type: "p2p" },
  }, identity);
  assert.equal(proposed.task.status, "waiting_for_action_approval");
  assert.deepEqual(proposed.presentation.actions.map((item) => item.type), ["confirm_memory", "cancel_task"]);
  assert.equal(memoryWrites.length, 0);

  const confirmed = await application.ingestChannelAction({
    event_id: "evt_memory_confirm", channel: "feishu", event_type: "action", external_user_id: "ou_demo",
    conversation_id: "oc_demo", message_id: "om_memory", task_run_id: proposed.task.task_run_id,
    payload: { action: "confirm_memory" },
  }, identity);
  assert.equal(confirmed.task.status, "completed");
  assert.equal(memoryWrites[0]?.scope, undefined);
  assert.equal(memoryWrites[0]?.user_id, "user_demo");
  assert.equal(memoryWrites[0]?.operation, "upsert");
  state.close();
});

test("Web uses the same ChannelEvent and explicit identity-map contract", async () => {
  const parsed = parseChannelEvent({
    event_id: "web_msg_001", channel: "web", event_type: "message",
    external_user_id: "web_admin", conversation_id: "web_conv_001",
    message_id: "web_msg_001", task_run_id: null, payload: { text: "查询销售额" },
  });
  assert.equal(parsed.channel, "web");

  const directory = await mkdtemp(join(tmpdir(), "forge-channel-web-identity-"));
  const path = join(directory, "identities.json");
  await writeFile(path, JSON.stringify({
    web: { web_admin: { org_id: "org_default", team_id: "team_default", user_id: "web_admin" } },
    feishu: {}, dingtalk: {},
  }));
  const resolver = new ChannelIdentityResolver(path);
  assert.deepEqual(resolver.resolve("web", "web_admin"), {
    org_id: "org_default", team_id: "team_default", user_id: "web_admin",
  });
  assert.throws(() => resolver.resolve("web", "unknown"), ChannelIdentityError);
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

test("one-time Feishu bootstrap atomically binds only the first user", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-channel-bootstrap-"));
  const path = join(directory, "identities.json");
  await writeFile(path, JSON.stringify({ feishu: {}, dingtalk: {} }));
  await chmod(path, 0o600);
  const resolver = new ChannelIdentityResolver(path);
  const identity = { org_id: "org_default", team_id: "team_default", user_id: "feishu_owner" };

  assert.deepEqual(resolver.bindFirstFeishu("ou_first", identity), identity);
  assert.deepEqual(resolver.resolve("feishu", "ou_first"), identity);
  assert.throws(() => resolver.bindFirstFeishu("ou_second", identity), ChannelIdentityError);
  assert.equal((await stat(path)).mode & 0o777, 0o600);
  assert.doesNotMatch(await readFile(path, "utf8"), /ou_second/);
  assert.deepEqual(new ChannelIdentityResolver(path).resolve("feishu", "ou_first"), identity);
});

test("channel cancel action is owned, persisted, and idempotent", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-channel-cancel-"));
  const config = loadConfig({
    PI_ORCHESTRATOR_STATE_DB: join(directory, "state.sqlite3"),
  });
  const state = new SqliteOrchestratorState(config.stateDbPath);
  const application = new OrchestratorApplication({
    config,
    tasks: state.tasks,
    events: state.events,
    artifacts: state.artifacts,
    attempts: state.attempts,
    channelEvents: state.channelEvents,
    transactions: state.transactions,
  });
  const created = application.createTask({
    org_id: "org_demo",
    team_id: "team_data",
    user_id: "user_demo",
    channel: "feishu",
    channel_conversation_id: "oc_demo",
    intent: "business_root_cause_analysis",
    message: "待取消任务",
  }).task;
  const input = {
    event_id: "evt_cancel_001",
    channel: "feishu" as const,
    event_type: "action" as const,
    external_user_id: "ou_allowed",
    conversation_id: "oc_demo",
    message_id: "om_cancel",
    task_run_id: created.task_run_id,
    payload: { action: "cancel_task" },
  };
  const identity = { org_id: "org_demo", team_id: "team_data", user_id: "user_demo" };

  const first = await application.ingestChannelAction(input, identity);
  const replay = await application.ingestChannelAction(input, identity);

  assert.equal(first.task.status, "cancelled");
  assert.equal(first.presentation.title, "任务已取消");
  assert.equal(replay.duplicate, true);
  assert.equal(replay.task.task_run_id, created.task_run_id);
  state.close();
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
  let releaseApproval: (() => void) | undefined;
  const approvalGate = new Promise<void>((resolve) => { releaseApproval = resolve; });
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
          input_kind: "forge_json",
          candidate_revision: "query-candidate-v1",
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
        await approvalGate;
        return {
          query_run_id: input.queryRunId,
          task_run_id: "tr_channel",
          status: "completed",
          sql_hash: input.sqlHash,
          input_kind: "forge_json",
          candidate_revision: "query-candidate-v1",
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
  assert.equal(action.status, 202);
  const inFlightReplay = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(actionPayload),
  });
  assert.equal(inFlightReplay.status, 200);
  assert.equal(approveCalls, 1);
  releaseApproval?.();
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

test("channel endpoint bootstraps only the first authenticated Feishu private message", async (context) => {
  const directory = await mkdtemp(join(tmpdir(), "forge-channel-bootstrap-server-"));
  const identityPath = join(directory, "identities.json");
  await writeFile(identityPath, JSON.stringify({ feishu: {}, dingtalk: {} }));
  await chmod(identityPath, 0o600);
  const config = loadConfig({
    PI_ORCHESTRATOR_STATE_DB: join(directory, "state.sqlite3"),
    PI_CHANNEL_IDENTITY_MAP: identityPath,
    PI_CHANNEL_SERVICE_KEYS: "channel-secret",
    PI_CHANNEL_AUTO_BIND_FIRST_FEISHU: "true",
  });
  const server = createOrchestratorServer(config);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  context.after(() => server.close());
  const address = server.address() as AddressInfo;
  const url = `http://127.0.0.1:${address.port}/v1/channel-events`;
  const headers = { "content-type": "application/json", "x-channel-service-key": "channel-secret" };
  const payload = {
    event_id: "evt_bootstrap_001", channel: "feishu", event_type: "message",
    external_user_id: "ou_first", conversation_id: "oc_first", message_id: "om_first",
    task_run_id: null, payload: { text: "查询订单", chat_type: "p2p" },
  };

  const first = await fetch(url, { method: "POST", headers, body: JSON.stringify(payload) });
  assert.equal(first.status, 202);
  assert.match(await readFile(identityPath, "utf8"), /ou_first/);

  const second = await fetch(url, {
    method: "POST", headers, body: JSON.stringify({
      ...payload, event_id: "evt_bootstrap_002", external_user_id: "ou_second",
      payload: { text: "查询订单", chat_type: "group" },
    }),
  });
  assert.equal(second.status, 403);
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
