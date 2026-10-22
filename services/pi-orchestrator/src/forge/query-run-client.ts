import type { ForgeDialect } from "./client.js";
import { ForgeClientError } from "./client.js";
import { currentRequestId } from "../request-context.js";

export type QueryCandidateInput =
  | { kind: "direct_sql"; sql: string; producer_revision?: string }
  | { kind: "forge_json"; forge_json: Record<string, unknown>; producer_revision?: string };

export interface QueryRunReview {
  schema_version: 2;
  query_run_id: string;
  task_run_id: string;
  status: "ready" | "review_required" | "denied" | "executing" | "completed" | "failed" | "cancelled" | "expired";
  decision: { verdict: string; review_required: boolean; execution_authorized: boolean; execution_mode: "autonomous" | "human_review" | null };
  context: { principal_context: Record<string, unknown>; delegated_mandate: Record<string, unknown>; purpose: string; resource_scope: unknown[] };
  candidate: { input_kind: QueryCandidateInput["kind"]; candidate_revision: string; producer_revision: string; candidate_hash: string };
  review: { sql: string | null; sql_hash: string | null; assurance_report_hash: string | null; enforcement_context_hash: string; expires_at: string };
  assurance: Record<string, unknown> | null;
  lineage: { assurance_revision: string | null; policy_revision: string; registry_revision: string; assurance_report_hash: string | null; enforcement_context_hash: string; [key: string]: unknown };
  result: { columns: string[]; rows: unknown[][]; row_count: number; truncated: boolean; execution_ms: number; executed_at: string } | null;
  failure: { code: string; stage: string; retryable: boolean } | null;
  authorization: Record<string, unknown> | null;
}

export interface QueryRunResult {
  query_run_id: string;
  task_run_id: string;
  status: "completed";
  sql_hash: string;
  input_kind: QueryCandidateInput["kind"];
  candidate_revision: string;
  dialect: string;
  registry_version: string;
  assurance_report_hash: string;
  assurance_revision: string;
  policy_revision: string;
  model_revision: string | null;
  assurance_registry_revision: string | null;
  columns: string[];
  rows: unknown[][];
  row_count: number;
  truncated: boolean;
  execution_ms: number;
  executed_at: string;
  error: string;
}

export interface ContextEvidence {
  evidence_ref: string;
  source_type: "schema" | "metric" | "disambiguation" | "convention" | "business_context" | "semantic_memory";
  title: string;
  content: string;
  score: number;
  verification_level: "verified" | "contextual" | "inferred" | "conflicted" | "unknown";
  scope: "user" | "team" | "org" | "organization";
  source_revision: string;
  updated_at: string | null;
  expires_at: string | null;
}
export interface ContextSearchResult {
  status: "ok";
  question: string;
  evidence: ContextEvidence[];
  evidence_count: number;
  context_revision: string;
  bounded: true;
}
export interface ReportPublication {
  report_id: string;
  task_run_id: string;
  revision: number;
  bundle_hash: string;
  title: string;
  status: "publishing" | "published" | "failed";
  pdf_status: "pending" | "ready" | "failed";
  pptx_status: "pending" | "ready" | "failed";
  internal_url: string;
  technical_url: string;
  pdf_url: string | null;
  pptx_url: string | null;
  created_at: string;
  updated_at: string;
}

function record(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new ForgeClientError("Forge returned a non-object response");
  return value as Record<string, unknown>;
}
function run(value: unknown): QueryRunReview {
  const body = record(value);
  const decision = record(body.decision);
  const review = record(body.review);
  if (body.schema_version !== 2 || typeof body.query_run_id !== "string" || typeof body.task_run_id !== "string" ||
    !["ready", "review_required", "denied", "executing", "completed", "failed", "cancelled", "expired"].includes(String(body.status)) ||
    !["autonomous", "human_review", null].includes(decision.execution_mode as string | null) ||
    typeof decision.execution_authorized !== "boolean" || typeof review.enforcement_context_hash !== "string" ||
    !body.candidate || !body.lineage || (body.status === "completed" && !body.result) ||
    (body.status === "executing" && !decision.execution_authorized)) {
    throw new ForgeClientError("Forge returned invalid governed QueryRun evidence");
  }
  return body as unknown as QueryRunReview;
}

export class ForgeQueryRunClient {
  readonly #baseUrl: string;
  readonly #serviceKey: string | undefined;
  readonly #timeoutMs: number;
  constructor(options: { baseUrl: string; serviceKey?: string; timeoutMs: number }) {
    this.#baseUrl = options.baseUrl.replace(/\/$/, "");
    this.#serviceKey = options.serviceKey;
    this.#timeoutMs = options.timeoutMs;
  }

  async createQueryRun(input: { taskRunId: string; mandateId: string; purpose: string; question: string; dialect?: ForgeDialect; candidate?: QueryCandidateInput; idempotencyKey: string }, signal?: AbortSignal): Promise<QueryRunReview> {
    let candidate = input.candidate;
    if (!candidate) {
      const generated = record(await this.#request("POST", "/api/internal/query-candidates", {
        task_run_id: input.taskRunId, mandate_id: input.mandateId, question: input.question,
        dialect: input.dialect ?? "auto",
      }, undefined, signal));
      if (!generated.candidate) {
        const code = generated.status === "needs_clarification" ? "candidate_needs_clarification" :
          generated.status === "timed_out" ? "candidate_timed_out" : "candidate_unavailable";
        throw new ForgeClientError(typeof generated.error === "string" ? generated.error : code, undefined, code);
      }
      candidate = record(generated.candidate) as unknown as QueryCandidateInput;
    }
    return run(await this.#request("POST", "/api/v2/enforce/query-runs", {
      schema_version: 2, mandate_id: input.mandateId, task_run_id: input.taskRunId,
      purpose: input.purpose, question: input.question, dialect: input.dialect ?? "auto", candidate,
    }, input.idempotencyKey, signal));
  }
  async getQueryRun(queryRunId: string, signal?: AbortSignal): Promise<QueryRunReview> {
    return run(await this.#request("GET", `/api/v2/enforce/query-runs/${encodeURIComponent(queryRunId)}`, undefined, undefined, signal));
  }
  async executeQueryRun(input: { queryRunId: string; sqlHash: string; assuranceReportHash: string; enforcementContextHash: string; idempotencyKey: string }, signal?: AbortSignal): Promise<QueryRunReview> {
    return this.#claim("execute", input, undefined, signal);
  }
  async approveQueryRun(input: { queryRunId: string; sqlHash: string; assuranceReportHash: string; enforcementContextHash: string; idempotencyKey: string; controlToken: string }, signal?: AbortSignal): Promise<QueryRunReview> {
    return this.#claim("approve", input, input.controlToken, signal);
  }
  async #claim(action: "execute" | "approve", input: { queryRunId: string; sqlHash: string; assuranceReportHash: string; enforcementContextHash: string; idempotencyKey: string }, controlToken?: string, signal?: AbortSignal): Promise<QueryRunReview> {
    const claimed = run(await this.#request("POST", `/api/v2/enforce/query-runs/${encodeURIComponent(input.queryRunId)}/${action}`, {
      schema_version: 2, sql_hash: input.sqlHash, assurance_report_hash: input.assuranceReportHash,
      enforcement_context_hash: input.enforcementContextHash,
    }, input.idempotencyKey, signal, controlToken));
    if (claimed.status !== "executing" && claimed.status !== "completed") throw new ForgeClientError("Forge did not commit execution claim");
    return claimed;
  }
  async cancelQueryRun(input: { queryRunId: string }, signal?: AbortSignal): Promise<QueryRunReview> {
    return run(await this.#request("POST", `/api/v2/enforce/query-runs/${encodeURIComponent(input.queryRunId)}/cancel`, {}, undefined, signal));
  }
  async getMandate(mandateId: string, signal?: AbortSignal): Promise<Record<string, unknown>> {
    return record(await this.#request("GET", `/api/v2/governance/mandates/${encodeURIComponent(mandateId)}`, undefined, undefined, signal));
  }
  async searchContext(input: { taskRunId: string; question: string; limit?: number }, signal?: AbortSignal): Promise<ContextSearchResult> {
    const body = record(await this.#request("POST", "/api/internal/context/search", {
      task_run_id: input.taskRunId, question: input.question, limit: input.limit ?? 8,
    }, undefined, signal));
    if (body.status !== "ok" || body.bounded !== true || !Array.isArray(body.evidence)) throw new ForgeClientError("Invalid bounded context response");
    return body as unknown as ContextSearchResult;
  }
  async writeMemory(input: Record<string, unknown>, signal?: AbortSignal): Promise<Record<string, unknown>> {
    return record(await this.#request("POST", "/api/internal/memory/entries", input, undefined, signal));
  }
  async createReport(input: Record<string, unknown>, idempotencyKey: string, signal?: AbortSignal): Promise<ReportPublication> {
    return this.#publication(record(await this.#request("POST", "/api/internal/reports", input, idempotencyKey, signal)));
  }
  async getReport(reportId: string, taskRunId: string, signal?: AbortSignal): Promise<ReportPublication> {
    return this.#publication(record(await this.#request("GET", `/api/internal/reports/${encodeURIComponent(reportId)}?task_run_id=${encodeURIComponent(taskRunId)}`, undefined, undefined, signal)));
  }
  #publication(body: Record<string, unknown>): ReportPublication {
    const publication = record(body.report);
    if (typeof publication.report_id !== "string" || typeof publication.task_run_id !== "string") throw new ForgeClientError("Invalid report publication");
    return publication as unknown as ReportPublication;
  }
  async #request(method: string, path: string, payload?: Record<string, unknown>, idempotencyKey?: string, signal?: AbortSignal, controlToken?: string): Promise<unknown> {
    if (!this.#serviceKey) throw new ForgeClientError("FORGE_PI_SERVICE_KEY is required");
    const headers: Record<string, string> = { accept: "application/json", "content-type": "application/json", "x-api-key": this.#serviceKey };
    if (idempotencyKey) headers["idempotency-key"] = idempotencyKey;
    if (controlToken) headers["x-forge-control-token"] = controlToken;
    const requestId = currentRequestId();
    if (requestId) headers["x-request-id"] = requestId;
    let response: Response;
    try {
      response = await fetch(`${this.#baseUrl}${path}`, { method, headers,
        ...(method === "GET" ? {} : { body: JSON.stringify(payload ?? {}) }),
        signal: AbortSignal.any([AbortSignal.timeout(this.#timeoutMs), ...(signal ? [signal] : [])]) });
    } catch {
      throw new ForgeClientError("Forge QueryRun transport failed", undefined, "upstream_unavailable");
    }
    let body: unknown;
    try { body = await response.json(); } catch { throw new ForgeClientError("Forge returned invalid JSON", response.status); }
    if (!response.ok) {
      const error = record(body);
      const failure = error.failure && typeof error.failure === "object" ? record(error.failure) : error;
      throw new ForgeClientError(`Forge returned HTTP ${response.status}`, response.status,
        typeof failure.code === "string" ? failure.code : "forge_request_failed");
    }
    return body;
  }
}
