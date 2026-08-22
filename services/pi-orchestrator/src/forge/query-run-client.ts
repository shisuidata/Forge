import type { ForgeDialect } from "./client.js";
import { ForgeClientError } from "./client.js";

export interface QueryRunReview {
  query_run_id: string;
  task_run_id: string;
  status: "needs_review" | "needs_clarification" | "timed_out" | "failed" | "cancelled";
  question: string;
  user_id: string;
  datasource_id: string;
  forge_json: Record<string, unknown> | null;
  sql: string | null;
  sql_hash: string | null;
  dialect: string;
  registry_version: string;
  assurance_report: Record<string, unknown> | null;
  assurance_report_hash: string | null;
  assurance_revision: string | null;
  policy_revision: string | null;
  model_revision: string | null;
  assurance_registry_revision: string | null;
  review_required: boolean;
  can_execute: false;
  expires_at: string;
  error: string;
}

export interface ContextEvidence {
  evidence_ref: string;
  source_type: "schema" | "metric" | "disambiguation" | "convention" | "business_context" | "semantic_memory";
  title: string;
  content: string;
  score: number;
}

export interface ContextSearchResult {
  status: "ok";
  question: string;
  evidence: ContextEvidence[];
  evidence_count: number;
  context_revision: string;
  bounded: true;
}

export interface QueryRunResult {
  query_run_id: string;
  task_run_id: string;
  status: "completed";
  sql_hash: string;
  dialect: string;
  registry_version: string;
  assurance_report_hash: string;
  assurance_revision: string;
  policy_revision: string;
  model_revision: string;
  assurance_registry_revision: string;
  columns: string[];
  rows: unknown[][];
  row_count: number;
  truncated: boolean;
  execution_ms: number;
  executed_at: string;
  error: string;
}

function asRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new ForgeClientError("Forge QueryRun API returned a non-object response");
  }
  return value as Record<string, unknown>;
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

  async createQueryRun(
    input: {
      taskRunId: string;
      orgId: string;
      teamId: string;
      userId: string;
      question: string;
      dialect?: ForgeDialect;
      idempotencyKey: string;
    },
    signal?: AbortSignal,
  ): Promise<QueryRunReview> {
    const body = await this.#request(
      "POST",
      "/api/internal/query-runs",
      {
        task_run_id: input.taskRunId,
        org_id: input.orgId,
        team_id: input.teamId,
        user_id: input.userId,
        question: input.question,
        dialect: input.dialect,
      },
      input.idempotencyKey,
      signal,
    );
    return this.#validateReview(body);
  }

  async approveQueryRun(
    input: {
      queryRunId: string;
      approverUserId: string;
      sqlHash: string;
      assuranceReportHash: string;
      idempotencyKey: string;
    },
    signal?: AbortSignal,
  ): Promise<QueryRunResult> {
    const body = await this.#request(
      "POST",
      `/api/internal/query-runs/${encodeURIComponent(input.queryRunId)}/approve`,
      {
        approver_user_id: input.approverUserId,
        sql_hash: input.sqlHash,
        assurance_report_hash: input.assuranceReportHash,
      },
      input.idempotencyKey,
      signal,
    );
    return this.#validateResult(body);
  }

  async searchContext(
    input: { orgId: string; teamId: string; userId: string; question: string; limit?: number },
    signal?: AbortSignal,
  ): Promise<ContextSearchResult> {
    const body = await this.#request(
      "POST",
      "/api/internal/context/search",
      {
        org_id: input.orgId,
        team_id: input.teamId,
        user_id: input.userId,
        question: input.question,
        limit: input.limit ?? 8,
      },
      undefined,
      signal,
    );
    return this.#validateContext(body);
  }

  async cancelQueryRun(
    input: { queryRunId: string; userId: string },
    signal?: AbortSignal,
  ): Promise<QueryRunReview> {
    const body = await this.#request(
      "POST",
      `/api/internal/query-runs/${encodeURIComponent(input.queryRunId)}/cancel`,
      { user_id: input.userId },
      undefined,
      signal,
    );
    return this.#validateReview(body);
  }

  async #request(
    method: string,
    path: string,
    payload: Record<string, unknown>,
    idempotencyKey?: string,
    signal?: AbortSignal,
  ): Promise<unknown> {
    if (!this.#serviceKey) {
      throw new ForgeClientError("FORGE_PI_SERVICE_KEY is required");
    }
    const signals = [AbortSignal.timeout(this.#timeoutMs)];
    if (signal !== undefined) signals.push(signal);
    const headers: Record<string, string> = {
      accept: "application/json",
      "content-type": "application/json",
      "x-pi-service-key": this.#serviceKey,
    };
    if (idempotencyKey !== undefined) headers["idempotency-key"] = idempotencyKey;

    let response: Response;
    try {
      response = await fetch(`${this.#baseUrl}${path}`, {
        method,
        headers,
        body: JSON.stringify(payload),
        signal: AbortSignal.any(signals),
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown transport error";
      throw new ForgeClientError(`Forge QueryRun request failed: ${message}`);
    }

    let body: unknown;
    try {
      body = await response.json();
    } catch {
      throw new ForgeClientError("Forge QueryRun API returned invalid JSON", response.status);
    }
    if (!response.ok) {
      const record = asRecord(body);
      throw new ForgeClientError(
        `Forge QueryRun API returned HTTP ${response.status}: ${String(record.error ?? "error")}`,
        response.status,
      );
    }
    return body;
  }

  #validateContext(value: unknown): ContextSearchResult {
    const body = asRecord(value);
    if (
      body.status !== "ok" || body.bounded !== true || !Array.isArray(body.evidence) ||
      body.evidence.length > 12 || typeof body.context_revision !== "string"
    ) {
      throw new ForgeClientError("Forge Context API returned an invalid bounded response");
    }
    const allowedTypes = new Set([
      "schema", "metric", "disambiguation", "convention", "business_context", "semantic_memory",
    ]);
    for (const raw of body.evidence) {
      const item = asRecord(raw);
      if (
        typeof item.evidence_ref !== "string" || !/^ctx_[a-f0-9]{24}$/.test(item.evidence_ref) ||
        typeof item.source_type !== "string" || !allowedTypes.has(item.source_type) ||
        typeof item.title !== "string" || typeof item.content !== "string" ||
        item.content.length > 1_200 || typeof item.score !== "number"
      ) {
        throw new ForgeClientError("Forge Context API returned invalid evidence");
      }
    }
    if (body.evidence_count !== body.evidence.length || typeof body.question !== "string") {
      throw new ForgeClientError("Forge Context API evidence count mismatch");
    }
    return body as unknown as ContextSearchResult;
  }

  #validateReview(value: unknown): QueryRunReview {
    const body = asRecord(value);
    if (body.can_execute !== false) {
      throw new ForgeClientError("Forge review response violated the non-executable boundary");
    }
    for (const field of [
      "query_run_id",
      "task_run_id",
      "status",
      "question",
      "user_id",
      "datasource_id",
      "dialect",
      "registry_version",
      "expires_at",
      "error",
    ]) {
      if (typeof body[field] !== "string") {
        throw new ForgeClientError(`Invalid QueryRun review field: ${field}`);
      }
    }
    if (body.status === "needs_review") {
      if (
        typeof body.sql !== "string" ||
        typeof body.sql_hash !== "string" ||
        typeof body.assurance_report_hash !== "string" ||
        typeof body.assurance_revision !== "string" ||
        typeof body.policy_revision !== "string" ||
        typeof body.model_revision !== "string" ||
        typeof body.assurance_registry_revision !== "string" ||
        body.assurance_report === null ||
        body.forge_json === null
      ) {
        throw new ForgeClientError("Reviewable QueryRun is missing SQL evidence");
      }
    }
    return body as unknown as QueryRunReview;
  }

  #validateResult(value: unknown): QueryRunResult {
    const body = asRecord(value);
    if (body.status !== "completed") {
      throw new ForgeClientError(`QueryRun did not complete: ${String(body.status)}`);
    }
    if (!Array.isArray(body.columns) || !Array.isArray(body.rows)) {
      throw new ForgeClientError("Completed QueryRun is missing result rows");
    }
    if (
      typeof body.sql_hash !== "string" ||
      typeof body.assurance_report_hash !== "string" ||
      typeof body.truncated !== "boolean"
    ) {
      throw new ForgeClientError("Completed QueryRun has invalid execution evidence");
    }
    return body as unknown as QueryRunResult;
  }
}
