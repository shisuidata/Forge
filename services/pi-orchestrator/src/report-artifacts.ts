import { createHash } from "node:crypto";

import type { Artifact } from "./artifacts.js";
import type { QueryResultPayload } from "./structured-artifact-tools.js";

export interface ChartPayload extends Record<string, unknown> {
  chart_id: string;
  chart_type: "bar" | "line" | "table";
  title: string;
  data_ref: string;
  dimension: string;
  measures: string[];
  evidence_refs: string[];
  alt_text: string;
}

export interface TechnicalReportPayload extends Record<string, unknown> {
  title: string;
  sql: string;
  query_run_id: string;
  sql_hash: string;
  approval: { approved: boolean; approved_at: string | null };
  execution: { executed_at: string; execution_ms: number; row_count: number; truncated: boolean };
  lineage: Record<string, string>;
  decision_log: Array<{
    stage: string;
    decision: string;
    rationale: string;
    evidence_refs: string[];
  }>;
  source_artifact_ids: string[];
}

export interface ReportBundlePayload extends Record<string, unknown> {
  report_id: string;
  revision: number;
  title: string;
  business_artifact_id: string;
  technical_artifact_id: string;
  chart_artifact_ids: string[];
  source_artifact_ids: string[];
  bundle_hash: string;
}

export interface PublicationPayload extends Record<string, unknown> {
  report_id: string;
  revision: number;
  bundle_hash: string;
  status: "publishing" | "published" | "failed";
  internal_url: string;
  technical_url: string;
  pdf: { status: "pending" | "ready" | "failed"; url: string | null };
  pptx: { status: "pending" | "ready" | "failed"; url: string | null };
  published_at: string | null;
}

const id = /^[a-z]{2,8}_[A-Za-z0-9_-]+$/;
const hash = /^sha256:[a-f0-9]{64}$/;

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string" && item.length > 0);
}

export function validateChartPayload(value: unknown): string | undefined {
  if (!record(value)) return "chart payload must be an object";
  if (typeof value.chart_id !== "string" || !/^chart_[A-Za-z0-9_-]+$/.test(value.chart_id)) return "chart_id is invalid";
  if (!new Set(["bar", "line", "table"]).has(String(value.chart_type))) return "chart_type is invalid";
  if (typeof value.title !== "string" || value.title.length === 0) return "chart title is required";
  if (typeof value.data_ref !== "string" || !/^ar_[A-Za-z0-9_-]+$/.test(value.data_ref)) return "chart data_ref is invalid";
  if (typeof value.dimension !== "string" || value.dimension.length === 0) return "chart dimension is required";
  if (!stringArray(value.measures) || value.measures.length === 0) return "chart measures are required";
  if (!stringArray(value.evidence_refs)) return "chart evidence_refs are required";
  if (typeof value.alt_text !== "string" || value.alt_text.length === 0) return "chart alt_text is required";
  return undefined;
}

export function validateTechnicalReportPayload(value: unknown): string | undefined {
  if (!record(value)) return "technical report payload must be an object";
  if (typeof value.title !== "string" || typeof value.sql !== "string") return "technical report title/sql are required";
  if (typeof value.query_run_id !== "string" || !/^qr_[A-Za-z0-9_-]+$/.test(value.query_run_id)) return "query_run_id is invalid";
  if (typeof value.sql_hash !== "string" || !hash.test(value.sql_hash)) return "sql_hash is invalid";
  if (!record(value.approval) || typeof value.approval.approved !== "boolean") return "approval is invalid";
  if (!record(value.execution) || !Number.isInteger(value.execution.row_count)) return "execution is invalid";
  if (!record(value.lineage) || Object.values(value.lineage).some((item) => typeof item !== "string")) return "lineage is invalid";
  if (!Array.isArray(value.decision_log) || value.decision_log.some((item) => !record(item) ||
      typeof item.stage !== "string" || typeof item.decision !== "string" ||
      typeof item.rationale !== "string" || !stringArray(item.evidence_refs))) return "decision_log is invalid";
  if (!stringArray(value.source_artifact_ids)) return "source_artifact_ids are invalid";
  const serialized = JSON.stringify(value);
  if (/(?:<\/?think>|system prompt|tool call|chain[- ]of[- ]thought|api[_ -]?key|password|secret)/i.test(serialized)) {
    return "technical report contains forbidden prompt, reasoning, or secret material";
  }
  return undefined;
}

export function validateReportBundlePayload(value: unknown): string | undefined {
  if (!record(value)) return "report bundle payload must be an object";
  if (typeof value.report_id !== "string" || !/^rp_[A-Za-z0-9_-]+$/.test(value.report_id)) return "report_id is invalid";
  if (!Number.isInteger(value.revision) || Number(value.revision) < 1) return "report revision is invalid";
  if (typeof value.title !== "string" || value.title.length === 0) return "report title is required";
  if (typeof value.business_artifact_id !== "string" || !id.test(value.business_artifact_id)) return "business artifact is invalid";
  if (typeof value.technical_artifact_id !== "string" || !id.test(value.technical_artifact_id)) return "technical artifact is invalid";
  if (!stringArray(value.chart_artifact_ids) || !stringArray(value.source_artifact_ids)) return "bundle artifact ids are invalid";
  if (typeof value.bundle_hash !== "string" || !hash.test(value.bundle_hash)) return "bundle_hash is invalid";
  return undefined;
}

function reportUrl(value: unknown): boolean {
  if (typeof value !== "string") return false;
  try {
    const url = new URL(value);
    return ["http:", "https:"].includes(url.protocol) && url.username.length === 0 &&
      url.password.length === 0 && url.pathname.startsWith("/reports/");
  } catch {
    return false;
  }
}

export function validatePublicationPayload(value: unknown): string | undefined {
  if (!record(value)) return "publication payload must be an object";
  if (typeof value.report_id !== "string" || !/^rp_[A-Za-z0-9_-]+$/.test(value.report_id)) return "publication report_id is invalid";
  if (!Number.isInteger(value.revision) || typeof value.bundle_hash !== "string" || !hash.test(value.bundle_hash)) return "publication revision/hash is invalid";
  if (!new Set(["publishing", "published", "failed"]).has(String(value.status))) return "publication status is invalid";
  if (!reportUrl(value.internal_url)) return "internal_url is invalid";
  if (!reportUrl(value.technical_url)) return "technical_url is invalid";
  for (const format of [value.pdf, value.pptx]) {
    if (!record(format) || !new Set(["pending", "ready", "failed"]).has(String(format.status)) ||
        (format.url !== null && !reportUrl(format.url))) return "publication export is invalid";
  }
  if (value.published_at !== null && typeof value.published_at !== "string") return "published_at is invalid";
  return undefined;
}

function numeric(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function dateLike(value: unknown): boolean {
  return typeof value === "string" && /^\d{4}-\d{2}(?:-\d{2})?/.test(value);
}

export function buildChartPayload(
  queryResult: Artifact<QueryResultPayload>,
  title = "查询结果概览",
): ChartPayload | undefined {
  if (queryResult.payload.rows.length === 0 || queryResult.payload.columns.length < 2) return undefined;
  const sample = queryResult.payload.rows.slice(0, 100);
  const dimensionIndex = queryResult.payload.columns.findIndex((_column, index) =>
    sample.some((row) => typeof row[index] === "string"),
  );
  const measureIndexes = queryResult.payload.columns
    .map((_column, index) => index)
    .filter((index) => sample.some((row) => numeric(row[index])))
    .slice(0, 3);
  if (dimensionIndex < 0 || measureIndexes.length === 0) return undefined;
  const queryRunId = queryResult.payload.query_run_id;
  return {
    chart_id: `chart_${createHash("sha256").update(`${queryResult.artifact_id}:${dimensionIndex}:${measureIndexes.join(",")}`).digest("hex").slice(0, 24)}`,
    chart_type: sample.every((row) => row[dimensionIndex] === null || dateLike(row[dimensionIndex])) ? "line" : "bar",
    title,
    data_ref: queryResult.artifact_id,
    dimension: queryResult.payload.columns[dimensionIndex] as string,
    measures: measureIndexes.map((index) => queryResult.payload.columns[index] as string),
    evidence_refs: sample.map((_row, index) => `${queryRunId}#row:${index + 1}`),
    alt_text: `${title}：按${String(queryResult.payload.columns[dimensionIndex])}展示${measureIndexes.map((index) => queryResult.payload.columns[index]).join("、")}`,
  };
}

export function bundleHash(value: Record<string, unknown>): string {
  return `sha256:${createHash("sha256").update(JSON.stringify(value)).digest("hex")}`;
}
