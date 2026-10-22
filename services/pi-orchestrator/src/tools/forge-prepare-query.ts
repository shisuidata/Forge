import { defineTool } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

import { FORGE_DIALECTS, type ForgeDialect } from "../forge/client.js";
import { ForgeQueryRunClient } from "../forge/query-run-client.js";

export interface TrustedTaskContext {
  taskRunId: string;
  mandateId: string;
  purpose: string;
  correlationId?: string;
}

const dialectSchema = Type.Union(
  FORGE_DIALECTS.map((dialect) => Type.Literal(dialect)),
);

export function createForgePrepareQueryTool(options: {
  client: ForgeQueryRunClient;
  task: TrustedTaskContext;
}) {
  return defineTool({
    name: "forge_prepare_query",
    label: "Forge Prepare QueryRun",
    description:
      "Ask the Forge trusted execution layer to persist a QueryRun and create reviewable SQL. " +
      "This tool cannot approve or execute the QueryRun.",
    parameters: Type.Object(
      {
        question: Type.String({
          minLength: 1,
          maxLength: 4_000,
          description: "The confirmed data question to prepare for review.",
        }),
        dialect: Type.Optional(dialectSchema),
      },
      { additionalProperties: false },
    ),
    async execute(_toolCallId, params, signal) {
      const result = await options.client.createQueryRun(
        {
          taskRunId: options.task.taskRunId,
          mandateId: options.task.mandateId,
          purpose: options.task.purpose,
          question: params.question,
          idempotencyKey: `${options.task.taskRunId}:prepare`,
          ...(params.dialect === undefined ? {} : { dialect: params.dialect as ForgeDialect }),
        },
        signal,
      );
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(
              {
                task_run_id: options.task.taskRunId,
                query_run_id: result.query_run_id,
                status: result.status,
                sql: result.review.sql,
                sql_hash: result.review.sql_hash,
                assurance_report_hash: result.review.assurance_report_hash,
                enforcement_context_hash: result.review.enforcement_context_hash,
                registry_version: result.lineage.registry_revision,
                expires_at: result.review.expires_at,
                review_required: result.decision.review_required,
                execution_mode: result.decision.execution_mode,
                clarification_or_error: result.failure?.code ?? null,
              },
              null,
              2,
            ),
          },
        ],
        details: {
          taskRunId: options.task.taskRunId,
          queryRunId: result.query_run_id,
          response: result,
        },
      };
    },
  });
}
