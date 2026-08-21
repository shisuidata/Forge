import { defineTool } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

import { FORGE_DIALECTS, type ForgeDialect } from "../forge/client.js";
import { ForgeQueryRunClient } from "../forge/query-run-client.js";

export interface TrustedTaskContext {
  taskRunId: string;
  orgId: string;
  teamId: string;
  userId: string;
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
          orgId: options.task.orgId,
          teamId: options.task.teamId,
          userId: options.task.userId,
          question: params.question,
          idempotencyKey: `${options.task.taskRunId}:prepare`,
          ...(params.dialect === undefined
            ? {}
            : { dialect: params.dialect as ForgeDialect }),
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
                sql: result.sql,
                sql_hash: result.sql_hash,
                forge_json: result.forge_json,
                dialect: result.dialect,
                registry_version: result.registry_version,
                expires_at: result.expires_at,
                review_required: result.review_required,
                can_execute: result.can_execute,
                clarification_or_error: result.error,
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
