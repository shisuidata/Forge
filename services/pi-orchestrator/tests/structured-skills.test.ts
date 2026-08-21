import assert from "node:assert/strict";
import test from "node:test";

import type { ToolDefinition } from "@earendil-works/pi-coding-agent";

import { PiStructuredSkillExecutor } from "../src/skill-executor.js";
import {
  ArtifactSubmissionError,
  createClarificationSubmissionTool,
} from "../src/structured-artifact-tools.js";
import { loadConfig } from "../src/config.js";
import { InMemoryTaskStore } from "../src/task-store.js";

const validMetric = {
  status: "needs_confirmation" as const,
  metric_name: "首购转化率",
  business_definition: "注册后 7 天内支付用户占新注册用户比例",
  numerator: "注册后 7 天内完成支付的新用户数",
  denominator: "新注册用户数",
  grain: "渠道/注册日",
  window: "注册后 7 天",
  filters: [],
  boundary_conditions: ["退款订单是否排除待确认"],
  open_questions: ["退款订单是否排除？"],
};

const validClarification = {
  status: "needs_input" as const,
  goal: "确认最近转化下降的分析范围",
  known_facts: ["用户要求分析转化下降"],
  assumptions: [],
  open_questions: ["最近具体指哪个时间范围？"],
  dimensions: ["渠道"],
  time_range: { description: "待确认" },
  acceptance_criteria: ["明确时间范围后可以生成查询"],
};

async function invoke(tool: ToolDefinition, payload: Record<string, unknown>) {
  return tool.execute("call_1", payload, undefined, undefined, {} as never);
}

function task() {
  return new InMemoryTaskStore().create({
    org_id: "org_demo",
    team_id: "team_demo",
    user_id: "user_demo",
    channel: "web",
    intent: "data_task",
  });
}

test("clarification Artifact Tool accepts one schema-valid terminal submission", async () => {
  const submission = createClarificationSubmissionTool();
  const result = await invoke(submission.tool, validClarification);
  assert.equal(result.terminate, true);
  assert.deepEqual(submission.getSubmitted(), validClarification);
  await assert.rejects(
    () => invoke(submission.tool, validClarification),
    ArtifactSubmissionError,
  );
});

test("clarification Artifact Tool rejects invalid dates and extra fields", async () => {
  const invalidDate = createClarificationSubmissionTool();
  await assert.rejects(
    () =>
      invoke(invalidDate.tool, {
        ...validClarification,
        time_range: { description: "指定范围", start: "yesterday" },
      }),
    /RFC 3339/,
  );

  const extraField = createClarificationSubmissionTool();
  await assert.rejects(
    () => invoke(extraField.tool, { ...validClarification, sql: "SELECT 1" }),
    /Invalid structured artifact payload/,
  );
});

test("Pi Skill executor captures the terminating structured tool result", async () => {
  const executor = new PiStructuredSkillExecutor({
    config: loadConfig({}),
    sessionFactory: async ({ skillName, tool }) => ({
      async prompt(prompt) {
        assert.equal(skillName, "data-requirement-clarifier");
        assert.match(prompt, /用户输入/);
        await invoke(tool, validClarification);
      },
      async abort() {},
      dispose() {},
    }),
  });

  assert.deepEqual(await executor.clarify(task(), "最近转化为什么下降"), validClarification);
});

test("Pi Skill executor runs metric review through its dedicated Artifact Tool", async () => {
  const executor = new PiStructuredSkillExecutor({
    config: loadConfig({}),
    sessionFactory: async ({ skillName, tool }) => ({
      async prompt() {
        assert.equal(skillName, "metric-definition-reviewer");
        assert.equal(tool.name, "submit_metric_definition_artifact");
        await invoke(tool, validMetric);
      },
      async abort() {},
      dispose() {},
    }),
  });

  assert.deepEqual(await executor.reviewMetric(task(), "审查首购转化率"), validMetric);
});


test("analysis and report Skills preserve QueryRun evidence lineage", async () => {
  let analysisArtifactId = "";
  const executor = new PiStructuredSkillExecutor({
    config: loadConfig({}),
    sessionFactory: async ({ skillName, tool }) => ({
      async prompt(prompt) {
        if (skillName === "business-root-cause-analysis") {
          assert.match(prompt, /qr_demo_001#row:1/);
          await invoke(tool, {
            status: "complete",
            summary: "移动端是异常集中点。",
            findings: [{
              statement: "移动端转化率为 6.9%。",
              evidence_refs: ["qr_demo_001#row:1"],
              confidence: "high",
            }],
            hypotheses: [],
            recommendations: [],
            limitations: [],
            suggested_queries: [],
          });
        } else {
          assert.equal(skillName, "data-analysis-report-writer");
          await invoke(tool, {
            status: "complete",
            title: "转化分析",
            audience: "业务负责人",
            executive_summary: "移动端是异常集中点。",
            key_findings: [{
              statement: "移动端转化率为 6.9%。",
              interpretation: "优先检查移动支付链路。",
              evidence_refs: ["qr_demo_001#row:1"],
              confidence: "high",
            }],
            recommendations: [],
            limitations: [],
            next_steps: ["补查支付性能"],
            source_artifact_ids: [analysisArtifactId],
            markdown: "SERVER_RENDERED",
          });
        }
      },
      async abort() {},
      dispose() {},
    }),
  });
  const queryResult = {
    artifact_id: "ar_query_001",
    artifact_type: "query_result" as const,
    schema_version: 1 as const,
    task_run_id: "tr_demo",
    producer: "forge",
    created_at: "2026-08-21T00:00:00Z",
    payload: {
      query_run_id: "qr_demo_001",
      sql_hash: `sha256:${"a".repeat(64)}`,
      columns: ["channel", "conversion_rate"],
      rows: [["mobile", 0.069]],
      row_count: 1,
      truncated: false,
      dialect: "postgresql" as const,
      registry_version: "registry-v1",
      execution_ms: 2,
      executed_at: "2026-08-21T00:00:00Z",
    },
  };
  const analysisPayload = await executor.analyze(
    task(),
    { question: "分析转化下降", queryResults: [queryResult] },
  );
  const analysis = {
    artifact_id: "ar_analysis_001",
    artifact_type: "analysis" as const,
    schema_version: 1 as const,
    task_run_id: "tr_demo",
    producer: "skill:business-root-cause-analysis",
    created_at: "2026-08-21T00:00:00Z",
    payload: analysisPayload,
  };
  analysisArtifactId = analysis.artifact_id;
  const report = await executor.writeReport(
    task(),
    { audience: "业务负责人", analysis },
  );
  assert.equal(report.source_artifact_ids[0], analysis.artifact_id);
  assert.match(report.markdown, /^# 转化分析/);
  assert.notEqual(report.markdown, "SERVER_RENDERED");
  assert.deepEqual(report.key_findings[0]?.evidence_refs, ["qr_demo_001#row:1"]);
});


test("analysis rejects evidence references from another QueryRun", async () => {
  const executor = new PiStructuredSkillExecutor({
    config: loadConfig({}),
    sessionFactory: async ({ tool }) => ({
      async prompt() {
        await invoke(tool, {
          status: "complete",
          summary: "错误引用",
          findings: [{
            statement: "错误引用",
            evidence_refs: ["qr_other#row:1"],
            confidence: "high",
          }],
          hypotheses: [],
          recommendations: [],
          limitations: [],
          suggested_queries: [],
        });
      },
      async abort() {},
      dispose() {},
    }),
  });
  const queryResult = {
    artifact_id: "ar_query_001",
    artifact_type: "query_result" as const,
    schema_version: 1 as const,
    task_run_id: "tr_demo",
    producer: "forge",
    created_at: "2026-08-21T00:00:00Z",
    payload: {
      query_run_id: "qr_demo_001",
      sql_hash: `sha256:${"a".repeat(64)}`,
      columns: ["n"],
      rows: [[1]],
      row_count: 1,
      truncated: false,
      dialect: "postgresql" as const,
      registry_version: "v1",
      execution_ms: 1,
      executed_at: "2026-08-21T00:00:00Z",
    },
  };
  await assert.rejects(
    () => executor.analyze(task(), { question: "分析", queryResults: [queryResult] }),
    /not present in the supplied QueryResult/,
  );
});


test("Pi Skill executor fails closed when the model omits the Artifact Tool", async () => {
  const executor = new PiStructuredSkillExecutor({
    config: loadConfig({}),
    sessionFactory: async () => ({
      async prompt() {},
      async abort() {},
      dispose() {},
    }),
  });

  await assert.rejects(
    () => executor.clarify(task(), "最近转化为什么下降"),
    /ended without submitting an Artifact/,
  );
});

test("Pi Skill executor never falls back to global model configuration", async () => {
  const executor = new PiStructuredSkillExecutor({ config: loadConfig({}) });
  await assert.rejects(
    () => executor.clarify(task(), "最近转化为什么下降"),
    /PI_MODEL_PROVIDER and PI_MODEL_ID/,
  );
});
