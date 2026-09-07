import { Type, type TSchema } from "typebox";
import { Value } from "typebox/value";

import {
  clarificationPayloadSchema, metricDefinitionPayloadSchema, queryResultPayloadSchema,
  analysisPayloadSchema, advisoryPayloadSchema, renderedOutputPayloadSchema,
  validateClarificationPayload, validateMetricDefinitionPayload, validateQueryResultPayload,
  validateAnalysisPayload, validateAdvisoryPayload, validateRenderedOutputPayload,
} from "./structured-artifact-tools.js";

// TypeBox owns this contract family's structure. Exported JSON is consumed by
// Python; non-structural semantics are defended by one shared semantic corpus.
// Product projection has its own TypeBox owner. Governance/benchmark/reporting
// remain Python-owned schema families and are not regenerated from TS mirrors.
export const structuredArtifactContracts = {
  clarification: { schema: clarificationPayloadSchema, validate: validateClarificationPayload },
  metric_definition: { schema: metricDefinitionPayloadSchema, validate: validateMetricDefinitionPayload },
  query_result: { schema: queryResultPayloadSchema, validate: validateQueryResultPayload },
  analysis: { schema: analysisPayloadSchema, validate: validateAnalysisPayload },
  advisory: { schema: advisoryPayloadSchema, validate: validateAdvisoryPayload },
  rendered_output: { schema: renderedOutputPayloadSchema, validate: validateRenderedOutputPayload },
} as const;
export type StructuredArtifactType = keyof typeof structuredArtifactContracts;

function envelope(artifactType: StructuredArtifactType, payload: TSchema) {
  return Type.Object({
    artifact_id: Type.String({ pattern: "^ar_[A-Za-z0-9_-]+$" }),
    artifact_type: Type.Literal(artifactType),
    schema_version: Type.Literal(1),
    task_run_id: Type.String({ pattern: "^tr_[A-Za-z0-9_-]+$" }),
    producer: artifactType === "query_result" ? Type.Literal("forge") : Type.String({ minLength: 1 }),
    created_at: Type.String({ format: "date-time" }),
    payload,
  }, {
    $schema: "https://json-schema.org/draft/2020-12/schema",
    $id: `https://forge.local/contracts/${artifactType.replaceAll("_", "-")}-artifact.schema.json`,
    additionalProperties: false,
  });
}

export const structuredArtifactSchemas = Object.fromEntries<TSchema>(
  Object.entries(structuredArtifactContracts).map(([name, contract]) => [name, envelope(name as StructuredArtifactType, contract.schema)]),
) as Record<StructuredArtifactType, TSchema>;

export function validateStructuredArtifact(artifactType: StructuredArtifactType, value: unknown): string | undefined {
  if (!Object.hasOwn(structuredArtifactSchemas, artifactType)) return "unknown structured artifact contract";
  if (!Value.Check(structuredArtifactSchemas[artifactType], value)) return "artifact does not match its versioned schema";
  return structuredArtifactContracts[artifactType].validate((value as { payload: unknown }).payload);
}
