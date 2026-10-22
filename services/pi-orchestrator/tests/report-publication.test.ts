import assert from "node:assert/strict";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { OrchestratorApplication, type ForgeQueryRunPort } from "../src/application.js";
import { loadConfig } from "../src/config.js";
import { ForgeClientError } from "../src/forge/client.js";
import type { ReportPublication } from "../src/forge/query-run-client.js";
import { InMemoryOrchestratorState, type OrchestratorState } from "../src/orchestrator-state.js";
import { SqliteOrchestratorState } from "../src/sqlite-store.js";
import type { StructuredSkillExecutionPort } from "../src/skill-executor.js";

const unused = async (): Promise<never> => { throw new Error("Unexpected external call"); };

function seedReadyForReport(state: OrchestratorState): string {
  const task = state.tasks.create({ org_id: "org_demo", team_id: "team_demo", user_id: "alice",
    channel: "web", intent: "data_task" });
  state.events.append(task.task_run_id, "query.review_requested", { query_run_id: "qr_demo", sql: "SELECT 7 AS n" });
  state.artifacts.create({ artifactType: "query_result", taskRunId: task.task_run_id, producer: "forge", payload: {
    query_run_id: "qr_demo", sql_hash: "sha256:" + "a".repeat(64), columns: ["n"], rows: [[7]], row_count: 1,
    truncated: false, dialect: "sqlite", registry_version: "synthetic-registry",
    assurance_report_hash: "sha256:" + "b".repeat(64), assurance_revision: "query-assurance-v13",
    policy_revision: "scoped-policy-v1", execution_ms: 1, executed_at: "2026-10-22T00:00:00.000Z",
  } });
  state.events.append(task.task_run_id, "query.completed", { query_run_id: "qr_demo" });
  state.artifacts.create({ artifactType: "analysis", taskRunId: task.task_run_id,
    producer: "skill:business-root-cause-analysis", payload: {
      status: "complete",
      method_summary: { objective: "解释订单", dimensions: ["n"], comparison_baseline: "结果内部对比", approach_steps: ["核验结果"] },
      summary: "订单为 7。",
      findings: [{ statement: "订单为 7。", evidence_refs: ["qr_demo#row:1"], confidence: "high" }],
      hypotheses: [], recommendations: [], limitations: [], suggested_queries: [],
    } });
  state.tasks.transition({ taskRunId: task.task_run_id, expectedStatus: "created", status: "ready_for_report", currentStage: "report_prepare" });
  return task.task_run_id;
}

function reportWriter(counter: { calls: number }): StructuredSkillExecutionPort["writeReport"] {
  return async (_task, input) => {
    counter.calls += 1;
    return { status: "complete", title: "订单报告", audience: input.audience, executive_summary: "订单为 7。",
      key_findings: [{ statement: "订单为 7。", interpretation: "订单量稳定。", evidence_refs: ["qr_demo#row:1"], confidence: "high" }],
      recommendations: [], limitations: [], next_steps: [], source_artifact_ids: [input.analysis.artifact_id],
      markdown: "# 订单报告\n订单为 7。" };
  };
}

/** A synthetic Forge Report Service that, like the real one, accepts one bundle hash per task. */
function remoteReports() {
  const reports = new Map<string, ReportPublication>();
  const sent: string[] = [];
  return {
    reports, sent,
    publish(input: Record<string, unknown>): ReportPublication {
      const reportId = String(input.report_id);
      const existing = reports.get(reportId);
      if (existing && existing.bundle_hash !== input.bundle_hash)
        throw new ForgeClientError("Forge returned HTTP 422", 422, "forge_request_failed");
      sent.push(String(input.bundle_hash));
      if (existing && existing.status !== "failed") return existing;
      const now = new Date().toISOString();
      const publication: ReportPublication = { report_id: reportId, task_run_id: String(input.task_run_id),
        revision: Number(input.revision), bundle_hash: String(input.bundle_hash), title: String(input.title),
        status: "published", pdf_status: "ready", pptx_status: "ready",
        internal_url: `https://forge.test/reports/${reportId}`, technical_url: `https://forge.test/reports/${reportId}/technical`,
        pdf_url: null, pptx_url: null, created_at: now, updated_at: now };
      reports.set(reportId, publication);
      return publication;
    },
    async get(reportId: string): Promise<ReportPublication> {
      const publication = reports.get(reportId);
      if (!publication) throw new ForgeClientError("Forge returned HTTP 404", 404, "forge_request_failed");
      return publication;
    },
  };
}

function application(state: OrchestratorState, forge: Partial<ForgeQueryRunPort>, writer: { calls: number }, stageTimeoutMs = 2_000) {
  return new OrchestratorApplication({ state,
    config: loadConfig({ FORGE_REQUEST_TIMEOUT_MS: "10", PI_STAGE_TIMEOUT_MS: String(stageTimeoutMs),
      PI_STAGE_LEASE_MS: String(stageTimeoutMs + 1_000) }),
    forgeClient: { createQueryRun: unused, approveQueryRun: unused, executeQueryRun: unused,
      getQueryRun: unused, getMandate: unused, ...forge },
    skillExecutor: { clarify: unused, reviewMetric: unused, analyze: unused, writeReport: reportWriter(writer) },
  });
}

function assertSinglePublishedBundle(state: OrchestratorState, taskRunId: string) {
  const bundles = state.artifacts.list(taskRunId).filter(item => item.artifact_type === "report_bundle");
  const publications = state.artifacts.list(taskRunId).filter(item => item.artifact_type === "publication");
  assert.equal(bundles.length, 1);
  assert.equal(publications.length, 1);
  assert.equal(publications[0]!.payload.bundle_hash, bundles[0]!.payload.bundle_hash);
  return String(bundles[0]!.payload.bundle_hash);
}

test("lost publish response is retryable and converges on the remote publication without regenerating", async () => {
  const state = new InMemoryOrchestratorState();
  const taskRunId = seedReadyForReport(state);
  const remote = remoteReports();
  const writer = { calls: 0 };
  let creates = 0;
  const app = application(state, {
    async createReport(input) {
      creates += 1;
      remote.publish(input);
      throw new ForgeClientError("Forge QueryRun transport failed", undefined, "upstream_unavailable");
    },
    getReport: remote.get,
  }, writer);
  await assert.rejects(app.renderReport(taskRunId, { audience: "业务负责人" }));
  assert.equal(app.getTask(taskRunId)?.status, "ready_for_report");
  const completed = await app.renderReport(taskRunId, { audience: "业务负责人" });
  assert.equal(completed.task.status, "completed");
  assert.equal(writer.calls, 1);
  assert.equal(creates, 1);
  assert.equal(assertSinglePublishedBundle(state, taskRunId), remote.sent[0]);
});

test("timed-out publish resends the persisted bundle only after Forge confirms it is absent", async () => {
  const state = new InMemoryOrchestratorState();
  const taskRunId = seedReadyForReport(state);
  const remote = remoteReports();
  const writer = { calls: 0 };
  let hang = true;
  const app = application(state, {
    async createReport(input, _key, signal) {
      if (!hang) return remote.publish(input);
      remote.sent.push(String(input.bundle_hash));
      return new Promise<never>((_, reject) => signal?.addEventListener("abort", () => reject(signal.reason), { once: true }));
    },
    getReport: remote.get,
  }, writer, 50);
  await assert.rejects(app.renderReport(taskRunId, { audience: "业务负责人" }));
  assert.equal(app.getTask(taskRunId)?.status, "ready_for_report");
  hang = false;
  const completed = await app.renderReport(taskRunId, { audience: "业务负责人" });
  assert.equal(completed.task.status, "completed");
  assert.equal(writer.calls, 1);
  assert.equal(remote.sent.length, 2);
  assert.equal(remote.sent[0], remote.sent[1]);
  assert.equal(assertSinglePublishedBundle(state, taskRunId), remote.sent[0]);
});

test("process exit after publish recovers the same bundle from Forge after restart", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-pi-report-"));
  const databasePath = join(directory, "orchestrator.sqlite3");
  const first = new SqliteOrchestratorState(databasePath);
  const taskRunId = seedReadyForReport(first);
  const remote = remoteReports();
  const writer = { calls: 0 };
  let creates = 0;
  let sent!: () => void;
  const published = new Promise<void>(resolve => { sent = resolve; });
  const crashed = application(first, {
    async createReport(input) {
      creates += 1;
      remote.publish(input);
      sent();
      return new Promise<never>(() => undefined);
    },
    getReport: remote.get,
  }, writer, 60_000);
  void crashed.renderReport(taskRunId, { audience: "业务负责人" }).catch(() => undefined);
  await published;
  first.close();

  const second = new SqliteOrchestratorState(databasePath);
  second.reconcileExpiredAttempts(new Date(Date.now() + 24 * 60 * 60 * 1_000));
  const restarted = application(second, { async createReport() { creates += 1; throw new Error("must not resend"); }, getReport: remote.get }, writer);
  assert.equal(restarted.getTask(taskRunId)?.status, "ready_for_report");
  const completed = await restarted.renderReport(taskRunId, { audience: "业务负责人" });
  assert.equal(completed.task.status, "completed");
  assert.equal(writer.calls, 1);
  assert.equal(creates, 1);
  assert.equal(assertSinglePublishedBundle(second, taskRunId), remote.sent[0]);
  second.close();
});
