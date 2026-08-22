import { existsSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";

import type { OrchestratorConfig } from "./config.js";

export const STAGE_MODEL_SCOPES = {
  intent_router: "pi.intent_router",
  clarification: "pi.clarification",
  metric_definition: "pi.metric_definition",
  query_generation: "forge.query_planning",
  query_repair: "forge.query_repair",
  knowledge_answer: "pi.knowledge_answer",
  analysis: "pi.analysis",
  report: "pi.report",
  memory_extraction: "pi.memory_extraction",
} as const;
export type ModelStage = keyof typeof STAGE_MODEL_SCOPES;

export interface StageModelBinding {
  stage: ModelStage;
  scope: string;
  revisionId: string;
  bindingVersion: number;
  provider: string;
  modelId: string;
  gateClass: "sql_critical" | "capability";
}

const SQL_CRITICAL = new Set<ModelStage>(["metric_definition", "query_generation", "query_repair"]);

export function skillModelStage(skillName: string): ModelStage {
  if (skillName === "data-requirement-clarifier") return "clarification";
  if (skillName === "metric-definition-reviewer") return "metric_definition";
  if (skillName === "data-analysis-report-writer") return "report";
  if (skillName === "data-doc-writer") return "knowledge_answer";
  return "analysis";
}

export function attemptModelStage(stage: string): ModelStage {
  if (stage.includes("clarif")) return "clarification";
  if (stage.includes("metric")) return "metric_definition";
  if (stage.includes("report")) return "report";
  if (stage.includes("knowledge") || stage.includes("data-doc")) return "knowledge_answer";
  if (stage.includes("query")) return "query_generation";
  return "analysis";
}

export function resolveStageModelBinding(
  config: OrchestratorConfig,
  stage: ModelStage,
): StageModelBinding | undefined {
  const path = config.modelControlDbPath;
  if (path === undefined || !existsSync(path)) return undefined;
  const database = new DatabaseSync(path, { readOnly: true });
  try {
    const row = database.prepare(
      `SELECT b.scope,b.binding_version,b.revision_id,r.config_json,r.validation_report_json
       FROM active_model_bindings b
       JOIN model_profile_revisions r ON r.revision_id=b.revision_id
       WHERE b.scope=?`,
    ).get(STAGE_MODEL_SCOPES[stage]) as {
      scope: string; binding_version: number; revision_id: string;
      config_json: string; validation_report_json: string;
    } | undefined;
    if (row === undefined) {
      if (SQL_CRITICAL.has(stage)) {
        throw new Error(`SQL-critical stage ${stage} has no validated active model binding`);
      }
      return undefined;
    }
    const modelConfig = JSON.parse(row.config_json) as Record<string, unknown>;
    const report = JSON.parse(row.validation_report_json) as Record<string, unknown>;
    const capabilities = typeof modelConfig.capabilities === "object" && modelConfig.capabilities !== null
      ? modelConfig.capabilities as Record<string, unknown>
      : {};
    const gateClass = SQL_CRITICAL.has(stage) ? "sql_critical" : "capability";
    const gate = gateClass === "sql_critical" ? report.quality_gate : report.capability_gate;
    if (typeof gate !== "object" || gate === null || (gate as Record<string, unknown>).passed !== true) {
      throw new Error(`Active ${stage} model binding no longer satisfies its gate`);
    }
    const provider = typeof capabilities.pi_provider_id === "string"
      ? capabilities.pi_provider_id
      : String(modelConfig.provider ?? "");
    const modelId = String(modelConfig.model ?? "");
    if (provider.length === 0 || modelId.length === 0) throw new Error("Stage model binding is incomplete");
    return {
      stage, scope: row.scope, revisionId: row.revision_id, bindingVersion: row.binding_version,
      provider, modelId, gateClass,
    };
  } finally {
    database.close();
  }
}
