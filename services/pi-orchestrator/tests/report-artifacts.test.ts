import assert from "node:assert/strict";
import test from "node:test";

import type { Artifact } from "../src/artifacts.js";
import { renderChannelPresentation } from "../src/channels/renderer.js";
import { buildChartPayload, validateTechnicalReportPayload } from "../src/report-artifacts.js";
import type { QueryResultPayload } from "../src/structured-artifact-tools.js";
import type { TaskRun } from "../src/task-store.js";

const queryResult = {
  artifact_id: "ar_query", artifact_type: "query_result", schema_version: 1,
  task_run_id: "tr_demo", producer: "forge", created_at: new Date().toISOString(),
  payload: {
    query_run_id: "qr_demo", sql_hash: `sha256:${"a".repeat(64)}`,
    columns: ["region", "sales"], rows: [["华东", 120], ["华南", 80]], row_count: 2,
    truncated: false, dialect: "postgresql", registry_version: "v1", execution_ms: 1,
    executed_at: "2026-08-21T00:00:00Z",
  },
} as Artifact<QueryResultPayload>;

test("chart builder deterministically binds chart encoding to QueryResult evidence", () => {
  const first = buildChartPayload(queryResult);
  const second = buildChartPayload(queryResult);
  assert.deepEqual(first, second);
  assert.equal(first?.chart_type, "bar");
  assert.equal(first?.dimension, "region");
  assert.deepEqual(first?.measures, ["sales"]);
  assert.deepEqual(first?.evidence_refs, ["qr_demo#row:1", "qr_demo#row:2"]);
});

test("technical report contract rejects hidden reasoning and secret transcript", () => {
  const error = validateTechnicalReportPayload({
    title: "技术报告", sql: "SELECT 1", query_run_id: "qr_demo", sql_hash: `sha256:${"a".repeat(64)}`,
    approval: { approved: true, approved_at: null },
    execution: { executed_at: "2026-08-21T00:00:00Z", execution_ms: 1, row_count: 1, truncated: false },
    lineage: { registry: "v1" },
    decision_log: [{ stage: "analysis", decision: "<think>private</think>", rationale: "none", evidence_refs: [] }],
    source_artifact_ids: ["ar_query"],
  });
  assert.match(error ?? "", /forbidden/);
});

test("completed channel report exposes business, technical, PDF and PPTX links only", () => {
  const task: TaskRun = {
    task_run_id: "tr_demo", org_id: "org", team_id: "team", user_id: "user", channel: "feishu",
    channel_conversation_id: "oc", intent: "workflow", status: "completed", current_stage: "report_complete",
    correlation_id: null, parent_task_run_id: null, created_at: "2026-08-21T00:00:00Z",
    updated_at: "2026-08-21T00:00:01Z", metadata: {},
  };
  const publication = {
    artifact_id: "ar_publication", artifact_type: "publication", schema_version: 1,
    task_run_id: "tr_demo", producer: "forge-report-service", created_at: task.updated_at,
    payload: {
      report_id: "rp_demo", revision: 1, bundle_hash: `sha256:${"b".repeat(64)}`, status: "published",
      internal_url: "https://forge.test/reports/rp_demo",
      technical_url: "https://forge.test/reports/rp_demo/technical",
      pdf: { status: "ready", url: "https://forge.test/reports/rp_demo/download/pdf" },
      pptx: { status: "ready", url: "https://forge.test/reports/rp_demo/download/pptx" },
      published_at: task.updated_at,
    },
  } as Artifact;
  const rendered = renderChannelPresentation({ task, events: [], artifacts: [publication] });
  assert.match(rendered.markdown, /完整分析报告/);
  assert.match(rendered.markdown, /技术报告/);
  assert.match(rendered.markdown, /PDF/);
  assert.match(rendered.markdown, /PPTX/);
  assert.doesNotMatch(rendered.markdown, /sha256|TaskRun|QueryRun/);
});
