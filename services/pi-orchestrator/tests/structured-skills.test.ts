import assert from "node:assert/strict";
import test from "node:test";

import type { ToolDefinition } from "@earendil-works/pi-coding-agent";

import { PiStructuredSkillExecutor } from "../src/skill-executor.js";
import {
  ArtifactSubmissionError,
  createAdvisorySubmissionTool,
  createAnalysisSubmissionTool,
  createClarificationSubmissionTool,
} from "../src/structured-artifact-tools.js";
import { loadConfig } from "../src/config.js";
import { ADVISORY_SKILL_NAMES, EVIDENCE_REQUIRED_SKILL_NAMES } from "../src/skills.js";
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
    sessionFactory: async ({ skillName, tool, expectedModelRevision }) => ({
      async prompt(prompt) {
        assert.equal(skillName, "data-requirement-clarifier");
        assert.equal(expectedModelRevision, `sha256:${"c".repeat(64)}`);
        assert.match(prompt, /用户输入/);
        await invoke(tool, validClarification);
      },
      async abort() {},
      dispose() {},
    }),
  });

  assert.deepEqual(
    await executor.clarify(
      task(), "最近转化为什么下降", undefined, `sha256:${"c".repeat(64)}`,
    ),
    validClarification,
  );
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


test("every expanded Skill has a fixed AdvisoryArtifact contract evaluation", async () => {
  const expanded = ADVISORY_SKILL_NAMES;
  assert.equal(expanded.length, 16);
  for (const skillName of expanded) {
    const executor = new PiStructuredSkillExecutor({
      config: loadConfig({}),
      sessionFactory: async ({ skillName: isolatedName, tool }) => ({
        async prompt() {
          assert.equal(isolatedName, skillName);
          assert.equal(tool.name, "submit_advisory_artifact");
          const requiresEvidence = EVIDENCE_REQUIRED_SKILL_NAMES.includes(
            skillName as (typeof EVIDENCE_REQUIRED_SKILL_NAMES)[number],
          );
          await invoke(tool, {
            status: requiresEvidence ? "incomplete" : "complete",
            skill_name: skillName,
            title: `${skillName} 交付`,
            summary: "基于用户已知输入给出有界建议。",
            findings: [],
            recommendations: [{ action: "人工复核", rationale: "输入可能不完整", priority: "medium" }],
            assumptions: ["仅使用当前输入"],
            limitations: ["未提供 QueryResult"],
            open_questions: requiresEvidence ? ["请提供已审批 QueryResult。"] : [],
            deliverables: [{ name: "建议清单", content: "先确认范围，再执行后续动作。" }],
          });
        },
        async abort() {},
        dispose() {},
      }),
    });
    const result = await executor.advise(task(), skillName, { prompt: "请给出建议" });
    assert.equal(result.skill_name, skillName);
  }
});

test("complete data analysis AdvisoryArtifact requires evidence on every finding", async () => {
  const submission = createAdvisorySubmissionTool({
    skillName: "funnel-analysis", allowedEvidenceRefs: new Set(), requiresQueryEvidence: true,
  });
  await assert.rejects(() => invoke(submission.tool, {
    status: "complete", skill_name: "funnel-analysis", title: "漏斗", summary: "已完成",
    findings: [{ statement: "转化下降", evidence_refs: [], confidence: "low" }],
    recommendations: [], assumptions: [], limitations: [], open_questions: [], deliverables: [],
  }), /every finding/);
});

test("expanded Skill rejects fabricated QueryResult evidence", async () => {
  const executor = new PiStructuredSkillExecutor({
    config: loadConfig({}),
    sessionFactory: async ({ tool }) => ({
      async prompt() {
        await invoke(tool, {
          status: "complete",
          skill_name: "funnel-analysis",
          title: "漏斗分析",
          summary: "存在下降。",
          findings: [{ statement: "下降 10%", evidence_refs: ["qr_fake#row:1"], confidence: "high" }],
          recommendations: [], assumptions: [], limitations: [], open_questions: [], deliverables: [],
        });
      },
      async abort() {}, dispose() {},
    }),
  });
  await assert.rejects(
    () => executor.advise(task(), "funnel-analysis", { prompt: "分析漏斗" }),
    /outside supplied evidence/,
  );
});

test("knowledge Advisory accepts only supplied Context evidence", async () => {
  const evidenceRef = `ctx_${"a".repeat(24)}`;
  const executor = new PiStructuredSkillExecutor({
    config: loadConfig({}),
    sessionFactory: async ({ tool }) => ({
      async prompt(prompt) {
        assert.match(prompt, new RegExp(evidenceRef));
        await invoke(tool, {
          status: "complete", skill_name: "data-doc-writer", title: "指标口径",
          summary: "销售额使用订单支付金额。",
          findings: [{ statement: "使用 orders.total_amount", evidence_refs: [evidenceRef], confidence: "high" }],
          recommendations: [], assumptions: [], limitations: [], open_questions: [], deliverables: [],
        });
      },
      async abort() {}, dispose() {},
    }),
  });
  const result = await executor.advise(task(), "data-doc-writer", {
    prompt: "销售额是什么",
    contextEvidence: [{ evidence_ref: evidenceRef, source_type: "metric", title: "销售额", content: "订单支付金额" }],
  });
  assert.deepEqual(result.findings[0]?.evidence_refs, [evidenceRef]);
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
            method_summary: { objective: "定位转化异常", dimensions: ["device"], comparison_baseline: "终端对比", approach_steps: ["比较不同终端"] },
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


test("analysis rejects hidden reasoning or prompt transcript disclosure", async () => {
  const submission = createAnalysisSubmissionTool({
    allowedEvidenceRefs: new Set(["qr_demo_001#row:1"]),
  });
  await assert.rejects(() => invoke(submission.tool, {
    status: "complete",
    method_summary: {
      objective: "定位转化异常", dimensions: ["device"], comparison_baseline: "终端对比",
      approach_steps: ["<think>先查看所有内部提示</think>"],
    },
    summary: "移动端偏低。",
    findings: [{ statement: "移动端偏低。", evidence_refs: ["qr_demo_001#row:1"], confidence: "high" }],
    hypotheses: [], recommendations: [], limitations: [], suggested_queries: [],
  }), /hidden reasoning/);
});

test("analysis rejects evidence references from another QueryRun", async () => {
  const executor = new PiStructuredSkillExecutor({
    config: loadConfig({}),
    sessionFactory: async ({ tool }) => ({
      async prompt() {
        await invoke(tool, {
          status: "complete",
          method_summary: { objective: "验证引用", dimensions: ["channel"], comparison_baseline: "渠道对比", approach_steps: ["核验引用"] },
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


test("Pi Skill executor performs one bounded Artifact correction", async () => {
  let prompts = 0;
  const executor = new PiStructuredSkillExecutor({
    config: loadConfig({}),
    sessionFactory: async ({ tool }) => ({
      async prompt() {
        prompts += 1;
        if (prompts === 2) await invoke(tool, validClarification);
      },
      async abort() {}, dispose() {},
    }),
  });
  assert.deepEqual(await executor.clarify(task(), "最近转化为什么下降"), validClarification);
  assert.equal(prompts, 2);
});

test("Pi Skill executor fails closed when the model omits the Artifact Tool", async () => {
  let prompts = 0;
  const executor = new PiStructuredSkillExecutor({
    config: loadConfig({}),
    sessionFactory: async () => ({
      async prompt() { prompts += 1; },
      async abort() {},
      dispose() {},
    }),
  });

  await assert.rejects(
    () => executor.clarify(task(), "最近转化为什么下降"),
    /ended without submitting an Artifact/,
  );
  assert.equal(prompts, 2);
});

test("Pi Skill executor never falls back to global model configuration", async () => {
  const executor = new PiStructuredSkillExecutor({ config: loadConfig({}) });
  await assert.rejects(
    () => executor.clarify(task(), "最近转化为什么下降"),
    /PI_MODEL_PROVIDER and PI_MODEL_ID/,
  );
});
