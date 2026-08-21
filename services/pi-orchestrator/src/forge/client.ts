export const FORGE_DIALECTS = [
  "auto",
  "sqlite",
  "postgresql",
  "mysql",
  "bigquery",
  "snowflake",
] as const;

export type ForgeDialect = (typeof FORGE_DIALECTS)[number];
export type PrepareQueryStatus = "needs_review" | "needs_clarification" | "error";

export interface PrepareQueryResponse {
  status: PrepareQueryStatus;
  question: string;
  user_id: string;
  forge_json: Record<string, unknown> | null;
  sql: string | null;
  dialect: string;
  review_required: true;
  can_execute: false;
  retry_count: number;
  text: string;
  error: string;
}

export class ForgeClientError extends Error {
  constructor(
    message: string,
    readonly statusCode?: number,
  ) {
    super(message);
    this.name = "ForgeClientError";
  }
}

function asRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new ForgeClientError("Forge returned a non-object prepare-query response");
  }
  return value as Record<string, unknown>;
}

function validatePrepareQueryResponse(value: unknown): PrepareQueryResponse {
  const body = asRecord(value);
  const status = body.status;
  if (status !== "needs_review" && status !== "needs_clarification" && status !== "error") {
    throw new ForgeClientError(`Forge returned an invalid prepare-query status: ${String(status)}`);
  }
  if (body.review_required !== true || body.can_execute !== false) {
    throw new ForgeClientError("Forge prepare-query violated the non-executable review boundary");
  }
  if ("rows" in body || "columns" in body || "result" in body) {
    throw new ForgeClientError("Forge prepare-query unexpectedly returned execution results");
  }

  const stringFields = ["question", "user_id", "dialect", "text", "error"] as const;
  for (const field of stringFields) {
    if (typeof body[field] !== "string") {
      throw new ForgeClientError(`Forge prepare-query field must be a string: ${field}`);
    }
  }
  if (!Number.isInteger(body.retry_count) || (body.retry_count as number) < 0) {
    throw new ForgeClientError("Forge prepare-query retry_count must be a non-negative integer");
  }
  if (body.forge_json !== null) asRecord(body.forge_json);
  if (body.sql !== null && typeof body.sql !== "string") {
    throw new ForgeClientError("Forge prepare-query sql must be a string or null");
  }
  if (status === "needs_review") {
    if (typeof body.sql !== "string" || body.sql.trim().length === 0 || body.forge_json === null) {
      throw new ForgeClientError("Forge needs_review response is missing SQL or Forge JSON");
    }
  }

  return body as unknown as PrepareQueryResponse;
}

export class ForgeClient {
  readonly #baseUrl: string;
  readonly #apiKey: string | undefined;
  readonly #timeoutMs: number;

  constructor(options: { baseUrl: string; apiKey?: string; timeoutMs: number }) {
    this.#baseUrl = options.baseUrl.replace(/\/$/, "");
    this.#apiKey = options.apiKey;
    this.#timeoutMs = options.timeoutMs;
  }

  async prepareQuery(
    input: { question: string; userId: string; dialect?: ForgeDialect },
    signal?: AbortSignal,
  ): Promise<PrepareQueryResponse> {
    const headers: Record<string, string> = {
      accept: "application/json",
      "content-type": "application/json",
    };
    if (this.#apiKey !== undefined) headers["x-api-key"] = this.#apiKey;

    const signals = [AbortSignal.timeout(this.#timeoutMs)];
    if (signal !== undefined) signals.push(signal);

    let response: Response;
    try {
      response = await fetch(`${this.#baseUrl}/api/prepare-query`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          question: input.question,
          user_id: input.userId,
          dialect: input.dialect,
        }),
        signal: AbortSignal.any(signals),
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown transport error";
      throw new ForgeClientError(`Forge prepare-query request failed: ${message}`);
    }

    if (!response.ok) {
      const body = (await response.text()).slice(0, 500);
      throw new ForgeClientError(
        `Forge prepare-query returned HTTP ${response.status}: ${body}`,
        response.status,
      );
    }

    let body: unknown;
    try {
      body = await response.json();
    } catch {
      throw new ForgeClientError("Forge prepare-query returned invalid JSON", response.status);
    }
    return validatePrepareQueryResponse(body);
  }
}
