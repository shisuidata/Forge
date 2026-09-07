import type { ModelStage } from "./model-bindings.js";

// Identity, output boundary and evidence policy live here; business execution stays
// in explicit typed methods. Advisory additions only need an authorized row.
export const STAGE_OUTPUTS = {
  clarification: { artifactType: "clarification", toolName: "submit_clarification_artifact" },
  metric_definition: { artifactType: "metric_definition", toolName: "submit_metric_definition_artifact" },
  analysis: { artifactType: "analysis", toolName: "submit_analysis_artifact" },
  advisory: { artifactType: "advisory", toolName: "submit_advisory_artifact" },
  report: { artifactType: "rendered_output", toolName: "submit_rendered_output_artifact" },
} as const;

interface SkillDescriptor {
  output: keyof typeof STAGE_OUTPUTS;
  modelStage: ModelStage;
  evidence: "none" | "query_result" | "supplied_context" | "analysis";
}

export const SKILL_DESCRIPTORS = {
  "data-requirement-clarifier": { output: "clarification", modelStage: "clarification", evidence: "none" },
  "metric-definition-reviewer": { output: "metric_definition", modelStage: "metric_definition", evidence: "none" },
  "business-root-cause-analysis": { output: "analysis", modelStage: "analysis", evidence: "query_result" },
  "data-analysis-report-writer": { output: "report", modelStage: "report", evidence: "analysis" },
  "exploratory-data-analysis": { output: "advisory", modelStage: "analysis", evidence: "query_result" },
  "funnel-analysis": { output: "advisory", modelStage: "analysis", evidence: "query_result" },
  "retention-cohort-analysis": { output: "advisory", modelStage: "analysis", evidence: "query_result" },
  "ab-test-analysis": { output: "advisory", modelStage: "analysis", evidence: "query_result" },
  "sql-reviewer": { output: "advisory", modelStage: "analysis", evidence: "supplied_context" },
  "data-quality-rule-generator": { output: "advisory", modelStage: "analysis", evidence: "supplied_context" },
  "table-design-advisor": { output: "advisory", modelStage: "analysis", evidence: "supplied_context" },
  "data-lineage-impact-analyzer": { output: "advisory", modelStage: "analysis", evidence: "supplied_context" },
  "dashboard-reviewer": { output: "advisory", modelStage: "analysis", evidence: "supplied_context" },
  "data-presentation-architect": { output: "advisory", modelStage: "analysis", evidence: "supplied_context" },
  "daily-report-writer": { output: "advisory", modelStage: "analysis", evidence: "supplied_context" },
  "weekly-monthly-report-writer": { output: "advisory", modelStage: "analysis", evidence: "supplied_context" },
  "data-doc-writer": { output: "advisory", modelStage: "knowledge_answer", evidence: "supplied_context" },
  "data-incident-postmortem-writer": { output: "advisory", modelStage: "analysis", evidence: "supplied_context" },
  "data-tool-integration-planner": { output: "advisory", modelStage: "analysis", evidence: "supplied_context" },
  "market-research-analyst": { output: "advisory", modelStage: "analysis", evidence: "supplied_context" },
} as const satisfies Record<string, SkillDescriptor>;

export type AuthorizedSkillName = keyof typeof SKILL_DESCRIPTORS;
export type AdvisorySkillName = {
  [K in AuthorizedSkillName]: (typeof SKILL_DESCRIPTORS)[K]["output"] extends "advisory" ? K : never;
}[AuthorizedSkillName];

export function skillDescriptor(name: string): SkillDescriptor {
  if (!Object.hasOwn(SKILL_DESCRIPTORS, name)) throw new Error(`Unauthorized Skill: ${name}`);
  return SKILL_DESCRIPTORS[name as AuthorizedSkillName];
}

// Persisted attempt identities are explicit, not substring matches. Query
// execution retains the query-planning revision binding, not a model call.
export const ATTEMPT_DESCRIPTORS = {
  requirement_clarification: { skillName: "data-requirement-clarifier" },
  metric_definition_review: { skillName: "metric-definition-reviewer" },
  query_prepare: { modelStage: "query_generation" },
  query_execution: { modelStage: "query_generation" },
  supplemental_analysis: { skillName: "business-root-cause-analysis" },
  business_root_cause_analysis: { skillName: "business-root-cause-analysis" },
  data_analysis_report: { skillName: "data-analysis-report-writer" },
} as const satisfies Record<string, { skillName: AuthorizedSkillName } | { modelStage: ModelStage }>;
