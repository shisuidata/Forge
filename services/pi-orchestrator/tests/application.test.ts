import assert from "node:assert/strict";
import test from "node:test";

import {
  OrchestratorApplication,
  type ForgeQueryRunPort,
} from "../src/application.js";
import { loadConfig } from "../src/config.js";
import type { QueryRunReview } from "../src/forge/query-run-client.js";
import { InMemoryStageAttemptStore } from "../src/stage-attempts.js";

const SQL_HASH = `sha256:${"a".repeat(64)}`;

function response(
  overrides: Partial<QueryRunReview> = {},
): QueryRunReview {
  return {
    status: "needs_review",
    question: "查询订单 ID",
    user_id: "trusted-user",
    forge_json: { scan: "orders", select: ["orders.id"] },
    sql: "SELECT orders.id FROM orders",
    dialect: "postgresql",
    review_required: true,
    can_execute: false,
    query_run_id: "qr_demo_001",
    task_run_id: "tr_placeholder",
    datasource_id: "demo",
    sql_hash: SQL_HASH,
    registry_version: "sha256:registry",
    assurance_report: { status: "passed" },
    assurance_report_hash: `sha256:${"b".repeat(64)}`,
    assurance_revision: "query-assurance-v1",
    policy_revision: "convention-policy-v1",
    model_revision: "sha256:model",
    assurance_registry_revision: "sha256:assurance-registry",
    expires_at: "2026-08-21T18:00:00Z",
    error: "",
    ...overrides,
  };
}

const unusedAnalysisSkills = {
  async analyze(): Promise<never> {
    throw new Error("not used");
  },
  async writeReport(): Promise<never> {
    throw new Error("not used");
  },
};

function createApplication(result: QueryRunReview) {
  const calls: Array<Record<string, unknown>> = [];
  const attempts = new InMemoryStageAttemptStore();
  const forgeClient: ForgeQueryRunPort = {
    async createQueryRun(input) {
      calls.push(input);
      return {...result, task_run_id: input.taskRunId};
    },
    async approveQueryRun(input) {
      return {
        query_run_id: input.queryRunId,
        task_run_id: "tr_placeholder",
        status: "completed",
        sql_hash: input.sqlHash,
        dialect: "postgresql",
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
        executed_at: "2026-08-21T17:00:00Z",
        error: "",
      };
    },
  };
  return {
    application: new OrchestratorApplication({
      config: loadConfig({}),
      forgeClient,
      attempts,
    }),
    calls,
    attempts,
  };
}


test("Pi application owns TaskRun progression and emits a review event", async () => {
  const { application, calls, attempts } = createApplication(response());
  const created = application.createTask({
    org_id: "org_demo",
    team_id: "team_growth",
    user_id: "trusted-user",
    channel: "web",
    intent: "query_prepare",
    message: "查询订单 ID",
  });

  const prepared = await application.prepareQuery(created.task.task_run_id, {
    question: "查询订单 ID",
    dialect: "postgresql",
  });

  assert.equal(prepared.task.status, "waiting_for_query_approval");
  assert.deepEqual(calls, [{
    taskRunId: created.task.task_run_id,
    orgId: "org_demo",
    teamId: "team_growth",
    userId: "trusted-user",
    question: "查询订单 ID",
    idempotencyKey: `${created.task.task_run_id}:prepare`,
    dialect: "postgresql",
  }]);
  assert.deepEqual(
    prepared.events.map((event) => event.event_type),
    [
      "task.created",
      "task.status_changed",
      "stage.attempt_started",
      "task.status_changed",
      "query.review_requested",
      "stage.attempt_succeeded",
    ],
  );
  const review = prepared.events.find(
    (event) => event.event_type === "query.review_requested",
  );
  assert.equal(review?.payload.can_execute, false);
  assert.equal(review?.payload.sql, "SELECT orders.id FROM orders");
  assert.equal(review?.payload.query_run_id, "qr_demo_001");
  assert.deepEqual(
    attempts.list(created.task.task_run_id).map((attempt) => [attempt.stage, attempt.status]),
    [["query_prepare", "succeeded"]],
  );
});


test("Forge prepare timeout restores a retryable task state", async () => {
  const { application, attempts } = createApplication(response({
    status: "timed_out",
    sql: null,
    sql_hash: null,
    forge_json: null,
    assurance_report: null,
    assurance_report_hash: null,
    assurance_revision: null,
    policy_revision: null,
    model_revision: null,
    assurance_registry_revision: null,
    review_required: false,
    error: "查询准备超时，请稍后重试或缩小问题范围。",
  }));
  const created = application.createTask({
    org_id: "org_demo",
    team_id: "team_growth",
    user_id: "trusted-user",
    channel: "web",
    intent: "query_prepare",
    message: "复杂查询",
  });

  const prepared = await application.prepareQuery(created.task.task_run_id, {
    question: "复杂查询",
  });

  assert.equal(prepared.task.status, "ready_for_query");
  assert.equal(prepared.task.current_stage, "query_prepare_retry");
  assert.equal(prepared.events.at(-2)?.event_type, "query.prepare_timed_out");
  assert.equal(attempts.list(created.task.task_run_id)[0]?.status, "timed_out");
});

test("structured clarification Artifact controls TaskRun progression", async () => {
  const attempts = new InMemoryStageAttemptStore();
  const withSkills = new OrchestratorApplication({
    attempts,
    config: loadConfig({}),
    forgeClient: {
      async createQueryRun(input) {
        return { ...response(), task_run_id: input.taskRunId };
      },
      async approveQueryRun() {
        throw new Error("not used");
      },
    },
    skillExecutor: {
      ...unusedAnalysisSkills,
      async clarify() {
        return {
          status: "needs_input",
          goal: "分析转化下降",
          known_facts: ["转化下降"],
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
  const created = withSkills.createTask({
    org_id: "org_demo",
    team_id: "team_growth",
    user_id: "trusted-user",
    channel: "web",
    intent: "data_task",
    message: "最近转化为什么下降",
  });
  const result = await withSkills.clarifyRequirement(created.task.task_run_id, {
    message: "最近转化为什么下降",
  });
  assert.equal(result.task.status, "needs_input");
  assert.equal(result.artifact.artifact_type, "clarification");
  assert.equal(result.artifact.task_run_id, created.task.task_run_id);
  assert.deepEqual(
    result.events.map((event) => event.event_type),
    [
      "task.created",
      "task.status_changed",
      "stage.attempt_started",
      "artifact.created",
      "task.status_changed",
      "stage.attempt_succeeded",
    ],
  );
  assert.equal(withSkills.getArtifacts(created.task.task_run_id).length, 1);
  assert.equal(attempts.list(created.task.task_run_id)[0]?.status, "succeeded");
});


test("Stage timeout records a timed-out Attempt and restores retry status", async () => {
  const attempts = new InMemoryStageAttemptStore();
  const app = new OrchestratorApplication({
    config: loadConfig({
      FORGE_REQUEST_TIMEOUT_MS: "1",
      PI_STAGE_TIMEOUT_MS: "10",
      PI_STAGE_LEASE_MS: "100",
    }),
    attempts,
    forgeClient: {
      async createQueryRun() { throw new Error("not used"); },
      async approveQueryRun() { throw new Error("not used"); },
    },
    skillExecutor: {
      ...unusedAnalysisSkills,
      async clarify() {
        await new Promise((resolve) => setTimeout(resolve, 30));
        return {
          status: "needs_input",
          goal: "分析转化下降",
          known_facts: [],
          assumptions: [],
          open_questions: ["确认周期"],
          dimensions: [],
          time_range: { description: "待确认" },
          acceptance_criteria: ["确认周期"],
        };
      },
      async reviewMetric() { throw new Error("not used"); },
    },
  });
  const created = app.createTask({
    org_id: "org_demo",
    team_id: "team_growth",
    user_id: "trusted-user",
    channel: "api",
    intent: "data_task",
    message: "分析转化下降",
  });
  await assert.rejects(
    () =>
      app.clarifyRequirement(created.task.task_run_id, {
        message: "分析转化下降",
        idempotencyKey: "clarify-timeout-001",
      }),
    /failed/,
  );
  assert.equal(app.getTask(created.task.task_run_id)?.status, "created");
  assert.equal(attempts.list(created.task.task_run_id)[0]?.status, "timed_out");
  assert.ok(
    app
      .getEvents(created.task.task_run_id)
      .some((event) => event.event_type === "stage.attempt_timed_out"),
  );
});

test("application rejects a Skill payload that violates the Artifact contract", async () => {
  const withSkills = new OrchestratorApplication({
    config: loadConfig({}),
    forgeClient: {
      async createQueryRun() {
        throw new Error("not used");
      },
      async approveQueryRun() {
        throw new Error("not used");
      },
    },
    skillExecutor: {
      ...unusedAnalysisSkills,
      async clarify() {
        return {
          status: "confirmed",
          goal: "分析转化下降",
          known_facts: [],
          assumptions: [],
          open_questions: [],
          dimensions: [],
          time_range: { description: "错误时间", start: "yesterday" },
          acceptance_criteria: [],
        };
      },
      async reviewMetric() {
        throw new Error("not used");
      },
    },
  });
  const created = withSkills.createTask({
    org_id: "org_demo",
    team_id: "team_growth",
    user_id: "trusted-user",
    channel: "web",
    intent: "data_task",
    message: "分析转化下降",
  });
  await assert.rejects(
    () =>
      withSkills.clarifyRequirement(created.task.task_run_id, {
        message: "分析转化下降",
      }),
    /Requirement clarification failed/,
  );
  assert.equal(withSkills.getArtifacts(created.task.task_run_id).length, 0);
  assert.equal(withSkills.getTask(created.task.task_run_id)?.status, "failed");
});


test("confirmed metric Artifact is required before query readiness", async () => {
  const attempts = new InMemoryStageAttemptStore();
  const withSkills = new OrchestratorApplication({
    attempts,
    config: loadConfig({}),
    forgeClient: {
      async createQueryRun(input) {
        return { ...response(), task_run_id: input.taskRunId };
      },
      async approveQueryRun() {
        throw new Error("not used");
      },
    },
    skillExecutor: {
      ...unusedAnalysisSkills,
      async clarify() {
        throw new Error("not used");
      },
      async reviewMetric() {
        return {
          status: "confirmed",
          metric_name: "首购转化率",
          business_definition: "注册后 7 天内支付用户占新注册用户比例",
          numerator: "注册后 7 天内完成支付的新用户数",
          denominator: "新注册用户数",
          grain: "渠道/注册日",
          window: "注册后 7 天",
          filters: [],
          boundary_conditions: ["退款处理已确认"],
          open_questions: [],
        };
      },
    },
  });
  const created = withSkills.createTask({
    org_id: "org_demo",
    team_id: "team_growth",
    user_id: "trusted-user",
    channel: "web",
    intent: "metric_review",
    message: "审查首购转化率",
  });
  const result = await withSkills.reviewMetricDefinition(created.task.task_run_id, {
    message: "审查首购转化率",
  });
  assert.equal(result.task.status, "ready_for_query");
  assert.equal(result.artifact.artifact_type, "metric_definition");
  assert.deepEqual(
    attempts.list(created.task.task_run_id).map((attempt) => [attempt.stage, attempt.status]),
    [["metric_definition_review", "succeeded"]],
  );
});


test("QueryResult flows through evidence-bound analysis and report Artifacts", async () => {
  let analysisArtifactId = "";
  const memoryWrites: Array<Record<string, unknown>> = [];
  const app = new OrchestratorApplication({
    config: loadConfig({}),
    forgeClient: {
      async createQueryRun(input) {
        return { ...response(), task_run_id: input.taskRunId };
      },
      async approveQueryRun(input) {
        return {
          query_run_id: input.queryRunId,
          task_run_id: "tr_placeholder",
          status: "completed",
          sql_hash: input.sqlHash,
          dialect: "postgresql",
          registry_version: "sha256:registry",
          assurance_report: { status: "passed" },
          assurance_report_hash: `sha256:${"b".repeat(64)}`,
          assurance_revision: "query-assurance-v1",
          policy_revision: "convention-policy-v1",
          model_revision: "sha256:model",
          assurance_registry_revision: "sha256:assurance-registry",
          columns: ["channel", "conversion_rate"],
          rows: [["mobile", 0.069]],
          row_count: 1,
          truncated: false,
          execution_ms: 2,
          executed_at: "2026-08-21T17:00:00Z",
          error: "",
        };
      },
      async createReport(input) {
        return {
          report_id: String(input.report_id), task_run_id: String(input.task_run_id), revision: 1,
          bundle_hash: String(input.bundle_hash), title: String(input.title), status: "published",
          pdf_status: "ready", pptx_status: "ready",
          internal_url: "https://forge.test/reports/rp_demo", technical_url: "https://forge.test/reports/rp_demo/technical",
          pdf_url: "https://forge.test/reports/rp_demo/download/pdf", pptx_url: "https://forge.test/reports/rp_demo/download/pptx",
          created_at: "2026-08-21T17:00:00Z", updated_at: "2026-08-21T17:00:01Z",
        };
      },
      async getReport() { throw new Error("published synchronously"); },
      async writeMemory(input) { memoryWrites.push(input); return { status: "confirmed" }; },
    },
    skillExecutor: {
      async clarify() {
        throw new Error("not used");
      },
      async reviewMetric() {
        throw new Error("not used");
      },
      async analyze(_task, input) {
        assert.equal(input.queryResults[0]?.artifact_type, "query_result");
        return {
          status: "complete",
          method_summary: { objective: "定位转化异常", dimensions: ["channel"], comparison_baseline: "渠道对比", approach_steps: ["比较各渠道转化率"] },
          summary: "移动端是异常集中点。",
          findings: [{
            statement: "移动端转化率为 6.9%。",
            evidence_refs: ["qr_demo_001#row:1"],
            confidence: "high",
          }],
          hypotheses: [{
            statement: "支付体验可能影响转化。",
            evidence_refs: [],
            status: "unverified",
          }],
          recommendations: [{
            action: "补查支付页性能",
            rationale: "当前数据不能确认产品原因",
            priority: "high",
          }],
          limitations: ["缺少支付性能数据"],
          suggested_queries: [],
        };
      },
      async writeReport(_task, input) {
        analysisArtifactId = input.analysis.artifact_id;
        return {
          status: "complete",
          title: "转化率分析",
          audience: input.audience,
          executive_summary: "移动端是异常集中点。",
          key_findings: [{
            statement: "移动端转化率为 6.9%。",
            interpretation: "优先检查移动支付链路。",
            evidence_refs: ["qr_demo_001#row:1"],
            confidence: "high",
          }],
          recommendations: [{
            action: "补查支付页性能",
            rationale: "当前数据不能确认产品原因",
            priority: "high",
          }],
          limitations: ["缺少支付性能数据"],
          next_steps: ["发起支付性能补查"],
          source_artifact_ids: [input.analysis.artifact_id],
          markdown: "# 转化率分析\n\n移动端是异常集中点。",
        };
      },
    },
  });
  const created = app.createTask({
    org_id: "org_demo",
    team_id: "team_growth",
    user_id: "trusted-user",
    channel: "web",
    intent: "business_root_cause_analysis",
    message: "分析转化率下降原因",
  });
  await app.prepareQuery(created.task.task_run_id, {
    question: "按渠道查询转化率",
    dialect: "postgresql",
  });
  const approved = await app.approveQuery(created.task.task_run_id, {
    queryRunId: "qr_demo_001",
    sqlHash: SQL_HASH,
    idempotencyKey: "approve-analysis-001",
  });
  assert.equal(approved.task.status, "ready_for_analysis");
  assert.equal(approved.artifact.artifact_type, "query_result");

  const analyzed = await app.analyzeTask(created.task.task_run_id, {});
  assert.equal(analyzed.task.status, "ready_for_report");
  assert.equal(analyzed.artifact.artifact_type, "analysis");

  const reported = await app.renderReport(created.task.task_run_id, {
    audience: "业务负责人",
  });
  assert.equal(reported.task.status, "completed");
  assert.equal(reported.artifact.artifact_type, "rendered_output");
  assert.equal(
    (reported.artifact.payload.source_artifact_ids as string[])[0],
    analysisArtifactId,
  );
  assert.equal(memoryWrites.length, 1);
  assert.equal(memoryWrites[0]?.category, "session_summary");
  assert.equal(memoryWrites[0]?.user_id, "trusted-user");
  assert.deepEqual(
    app.getArtifacts(created.task.task_run_id).map((artifact) => artifact.artifact_type),
    ["query_result", "chart", "analysis", "rendered_output", "technical_report", "report_bundle", "publication"],
  );
});


test("incomplete analysis pauses with suggested queries and cannot render", async () => {
  const app = new OrchestratorApplication({
    config: loadConfig({}),
    forgeClient: {
      async createQueryRun(input) {
        return { ...response(), task_run_id: input.taskRunId };
      },
      async approveQueryRun(input) {
        return {
          query_run_id: input.queryRunId,
          task_run_id: "tr_placeholder",
          status: "completed",
          sql_hash: input.sqlHash,
          dialect: "postgresql",
          registry_version: "sha256:registry",
          assurance_report: { status: "passed" },
          assurance_report_hash: `sha256:${"b".repeat(64)}`,
          assurance_revision: "query-assurance-v1",
          policy_revision: "convention-policy-v1",
          model_revision: "sha256:model",
          assurance_registry_revision: "sha256:assurance-registry",
          columns: ["conversion_rate"],
          rows: [[0.069]],
          row_count: 1,
          truncated: false,
          execution_ms: 1,
          executed_at: "2026-08-21T17:00:00Z",
          error: "",
        };
      },
    },
    skillExecutor: {
      async clarify() { throw new Error("not used"); },
      async reviewMetric() { throw new Error("not used"); },
      async analyze() {
        return {
          status: "incomplete",
          method_summary: { objective: "定位转化异常", dimensions: ["channel"], comparison_baseline: "渠道对比", approach_steps: ["检查现有结果"] },
          summary: "现有结果只能确认转化率水平。",
          findings: [{
            statement: "当前转化率为 6.9%。",
            evidence_refs: ["qr_demo_001#row:1"],
            confidence: "high",
          }],
          hypotheses: [],
          recommendations: [],
          limitations: ["没有对比周期和拆分维度"],
          suggested_queries: [{
            question: "按渠道和周期查询转化率",
            reason: "定位下降贡献来源",
            priority: "high",
          }],
        };
      },
      async writeReport() { throw new Error("must not render"); },
    },
  });
  const created = app.createTask({
    org_id: "org_demo",
    team_id: "team_growth",
    user_id: "trusted-user",
    channel: "api",
    intent: "business_root_cause_analysis",
    message: "分析转化率下降",
  });
  await app.prepareQuery(created.task.task_run_id, { question: "查询转化率" });
  await app.approveQuery(created.task.task_run_id, {
    queryRunId: "qr_demo_001",
    sqlHash: SQL_HASH,
    idempotencyKey: "approve-incomplete-001",
  });
  const analyzed = await app.analyzeTask(created.task.task_run_id, {});
  assert.equal(analyzed.task.status, "incomplete");
  assert.equal(
    (analyzed.artifact.payload.suggested_queries as unknown[]).length,
    1,
  );
  await assert.rejects(
    () => app.renderReport(created.task.task_run_id, { audience: "业务负责人" }),
    /cannot render/,
  );
});


test("one approved supplemental child QueryRun can resume parent analysis", async () => {
  let analysisCalls = 0;
  const attempts = new InMemoryStageAttemptStore();
  const app = new OrchestratorApplication({
    attempts,
    config: loadConfig({}),
    forgeClient: {
      async createQueryRun(input) {
        const queryRunId = input.question.includes("渠道") ? "qr_supplement_001" : "qr_primary_001";
        return { ...response({ query_run_id: queryRunId }), task_run_id: input.taskRunId };
      },
      async approveQueryRun(input) {
        return {
          query_run_id: input.queryRunId,
          task_run_id: "tr_placeholder",
          status: "completed",
          sql_hash: input.sqlHash,
          dialect: "postgresql",
          registry_version: "registry-v1",
          assurance_report: { status: "passed" },
          assurance_report_hash: `sha256:${"b".repeat(64)}`,
          assurance_revision: "query-assurance-v1",
          policy_revision: "convention-policy-v1",
          model_revision: "sha256:model",
          assurance_registry_revision: "sha256:assurance-registry",
          columns: ["segment", "conversion_rate"],
          rows: [[input.queryRunId.includes("supplement") ? "channel_a" : "all", 0.069]],
          row_count: 1,
          truncated: false,
          execution_ms: 1,
          executed_at: "2026-08-21T17:00:00Z",
          error: "",
        };
      },
    },
    skillExecutor: {
      async clarify() { throw new Error("not used"); },
      async reviewMetric() { throw new Error("not used"); },
      async analyze(_task, input) {
        analysisCalls += 1;
        if (analysisCalls === 1) {
          assert.equal(input.queryResults.length, 1);
          return {
            status: "incomplete",
            method_summary: { objective: "定位收入异常", dimensions: ["channel"], comparison_baseline: "渠道对比", approach_steps: ["检查渠道拆分"] },
            summary: "需要渠道拆分。",
            findings: [{
              statement: "整体转化率为 6.9%。",
              evidence_refs: ["qr_primary_001#row:1"],
              confidence: "high",
            }],
            hypotheses: [],
            recommendations: [],
            limitations: ["缺少渠道维度"],
            suggested_queries: [{
              question: "按渠道查询转化率",
              reason: "定位下降贡献渠道",
              priority: "high",
            }],
          };
        }
        assert.equal(input.queryResults.length, 2);
        assert.equal(input.priorAnalysis?.payload.status, "incomplete");
        return {
          status: "complete",
          method_summary: { objective: "定位收入异常", dimensions: ["channel"], comparison_baseline: "渠道对比", approach_steps: ["比较主查询与补查"] },
          summary: "渠道 A 是下降集中点。",
          findings: [{
            statement: "渠道 A 当前转化率为 6.9%。",
            evidence_refs: ["qr_supplement_001#row:1"],
            confidence: "high",
          }],
          hypotheses: [],
          recommendations: [],
          limitations: [],
          suggested_queries: [],
        };
      },
      async writeReport() { throw new Error("not used"); },
    },
  });
  const created = app.createTask({
    org_id: "org_demo",
    team_id: "team_growth",
    user_id: "trusted-user",
    channel: "web",
    intent: "business_root_cause_analysis",
    message: "分析转化率下降",
  });
  await app.prepareQuery(created.task.task_run_id, { question: "查询整体转化率" });
  await app.approveQuery(created.task.task_run_id, {
    queryRunId: "qr_primary_001",
    sqlHash: SQL_HASH,
    idempotencyKey: "approve-primary",
  });
  await app.analyzeTask(created.task.task_run_id, {});

  const supplement = app.createSupplementTask(created.task.task_run_id, {
    suggestedQueryIndex: 0,
    idempotencyKey: "supplement-001",
  });
  assert.equal(supplement.childTask.parent_task_run_id, created.task.task_run_id);
  assert.equal(supplement.suggestion.question, "按渠道查询转化率");
  const replayed = app.createSupplementTask(created.task.task_run_id, {
    suggestedQueryIndex: 0,
    idempotencyKey: "supplement-001",
  });
  assert.equal(replayed.childTask.task_run_id, supplement.childTask.task_run_id);
  await assert.rejects(
    async () =>
      app.createSupplementTask(created.task.task_run_id, {
        suggestedQueryIndex: 0,
        idempotencyKey: "different-supplement",
      }),
    /already used/,
  );

  await app.prepareQuery(supplement.childTask.task_run_id, {
    question: "按渠道查询转化率",
  });
  await app.approveQuery(supplement.childTask.task_run_id, {
    queryRunId: "qr_supplement_001",
    sqlHash: SQL_HASH,
    idempotencyKey: "approve-supplement",
  });
  assert.equal(app.getTask(supplement.childTask.task_run_id)?.status, "completed");

  const resumed = await app.resumeAnalysisWithSupplement(created.task.task_run_id, {
    childTaskRunId: supplement.childTask.task_run_id,
    idempotencyKey: "resume-supplement-001",
  });
  assert.equal(resumed.task.status, "ready_for_report");
  assert.equal(resumed.artifact.payload.status, "complete");
  assert.equal(
    resumed.events.filter((event) => event.event_type === "analysis.supplement_consumed").length,
    1,
  );
  const replayedResume = await app.resumeAnalysisWithSupplement(
    created.task.task_run_id,
    {
      childTaskRunId: supplement.childTask.task_run_id,
      idempotencyKey: "resume-supplement-001",
    },
  );
  assert.equal(replayedResume.artifact.artifact_id, resumed.artifact.artifact_id);
  assert.ok(
    attempts
      .list(created.task.task_run_id)
      .some(
        (attempt) =>
          attempt.stage === "supplemental_analysis" && attempt.status === "succeeded",
      ),
  );
});


test("clarification pauses the Pi task instead of creating a review request", async () => {
  const { application } = createApplication(
    response({
      status: "needs_clarification",
      forge_json: null,
      sql: null,
      error: "请确认时间范围",
    }),
  );
  const created = application.createTask({
    org_id: "org_demo",
    team_id: "team_growth",
    user_id: "trusted-user",
    channel: "web",
    intent: "query_prepare",
    message: "看一下最近的数据",
  });

  const prepared = await application.prepareQuery(created.task.task_run_id, {
    question: "看一下最近的数据",
  });

  assert.equal(prepared.task.status, "needs_input");
  assert.ok(
    prepared.events.some(
      (event) => event.event_type === "query.clarification_requested",
    ),
  );
});
