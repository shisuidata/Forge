import { defineTool } from "@earendil-works/pi-coding-agent";
import { Type, type Static, type TSchema } from "typebox";
import { IsDateTime } from "typebox/format";
import { Value } from "typebox/value";

export const clarificationPayloadSchema = Type.Object(
  {
    status: Type.Union([
      Type.Literal("needs_input"),
      Type.Literal("needs_confirmation"),
      Type.Literal("confirmed"),
    ]),
    goal: Type.String({ minLength: 1 }),
    known_facts: Type.Array(Type.String({ minLength: 1 }), { uniqueItems: true }),
    assumptions: Type.Array(Type.String({ minLength: 1 }), { uniqueItems: true }),
    open_questions: Type.Array(Type.String({ minLength: 1 }), { uniqueItems: true }),
    dimensions: Type.Array(Type.String({ minLength: 1 }), { uniqueItems: true }),
    time_range: Type.Object(
      {
        description: Type.String({ minLength: 1 }),
        start: Type.Optional(Type.Union([Type.String(), Type.Null()])),
        end: Type.Optional(Type.Union([Type.String(), Type.Null()])),
        timezone: Type.Optional(Type.Union([Type.String(), Type.Null()])),
        granularity: Type.Optional(Type.Union([Type.String(), Type.Null()])),
      },
      { additionalProperties: false },
    ),
    acceptance_criteria: Type.Array(Type.String({ minLength: 1 }), {
      uniqueItems: true,
    }),
  },
  { additionalProperties: false },
);

export const metricDefinitionPayloadSchema = Type.Object(
  {
    status: Type.Union([
      Type.Literal("draft"),
      Type.Literal("needs_confirmation"),
      Type.Literal("confirmed"),
      Type.Literal("rejected"),
    ]),
    metric_name: Type.String({ minLength: 1 }),
    business_definition: Type.Optional(Type.Union([Type.String(), Type.Null()])),
    numerator: Type.String({ minLength: 1 }),
    denominator: Type.String({ minLength: 1 }),
    grain: Type.String({ minLength: 1 }),
    window: Type.String({ minLength: 1 }),
    filters: Type.Array(
      Type.Object(
        {
          field: Type.String({ minLength: 1 }),
          operator: Type.String({ minLength: 1 }),
          value: Type.Union([
            Type.String(),
            Type.Number(),
            Type.Boolean(),
            Type.Null(),
            Type.Array(
              Type.Union([Type.String(), Type.Number(), Type.Boolean(), Type.Null()]),
            ),
          ]),
        },
        { additionalProperties: false },
      ),
    ),
    boundary_conditions: Type.Array(Type.String({ minLength: 1 }), {
      uniqueItems: true,
    }),
    open_questions: Type.Array(Type.String({ minLength: 1 }), { uniqueItems: true }),
  },
  { additionalProperties: false },
);

const evidenceRefSchema = Type.String({ pattern: "^qr_[A-Za-z0-9_-]+#.+$" });
const prioritySchema = Type.Union([
  Type.Literal("immediate"),
  Type.Literal("high"),
  Type.Literal("medium"),
  Type.Literal("low"),
]);

export const queryResultPayloadSchema = Type.Object(
  {
    query_run_id: Type.String({ pattern: "^qr_[A-Za-z0-9_-]+$" }),
    sql_hash: Type.String({ pattern: "^sha256:[a-f0-9]{64}$" }),
    columns: Type.Array(Type.String({ minLength: 1 }), { uniqueItems: true }),
    rows: Type.Array(Type.Array(Type.Unknown())),
    row_count: Type.Integer({ minimum: 0 }),
    truncated: Type.Boolean(),
    dialect: Type.Union([
      Type.Literal("sqlite"),
      Type.Literal("postgresql"),
      Type.Literal("mysql"),
      Type.Literal("bigquery"),
      Type.Literal("snowflake"),
    ]),
    registry_version: Type.String({ minLength: 1 }),
    assurance_report_hash: Type.Optional(Type.String({ pattern: "^sha256:[a-f0-9]{64}$" })),
    assurance_revision: Type.Optional(Type.String({ minLength: 1 })),
    policy_revision: Type.Optional(Type.String({ minLength: 1 })),
    model_revision: Type.Optional(Type.String({ minLength: 1 })),
    assurance_registry_revision: Type.Optional(Type.String({ minLength: 1 })),
    execution_ms: Type.Integer({ minimum: 0 }),
    executed_at: Type.String({ minLength: 1 }),
    result_contract: Type.Optional(Type.Union([Type.Record(Type.String(), Type.Unknown()), Type.Null()])),
  },
  { additionalProperties: false },
);

export const analysisPayloadSchema = Type.Object(
  {
    status: Type.Union([Type.Literal("complete"), Type.Literal("incomplete")]),
    summary: Type.String({ minLength: 1 }),
    findings: Type.Array(
      Type.Object(
        {
          statement: Type.String({ minLength: 1 }),
          evidence_refs: Type.Array(evidenceRefSchema, { minItems: 1, uniqueItems: true }),
          confidence: Type.Union([
            Type.Literal("high"),
            Type.Literal("medium"),
            Type.Literal("low"),
          ]),
        },
        { additionalProperties: false },
      ),
    ),
    hypotheses: Type.Array(
      Type.Object(
        {
          statement: Type.String({ minLength: 1 }),
          evidence_refs: Type.Array(evidenceRefSchema, { uniqueItems: true }),
          status: Type.Union([
            Type.Literal("supported"),
            Type.Literal("plausible"),
            Type.Literal("unverified"),
            Type.Literal("rejected"),
          ]),
        },
        { additionalProperties: false },
      ),
    ),
    recommendations: Type.Array(
      Type.Object(
        {
          action: Type.String({ minLength: 1 }),
          rationale: Type.String({ minLength: 1 }),
          priority: prioritySchema,
        },
        { additionalProperties: false },
      ),
    ),
    limitations: Type.Array(Type.String({ minLength: 1 }), { uniqueItems: true }),
    suggested_queries: Type.Array(
      Type.Object(
        {
          question: Type.String({ minLength: 1 }),
          reason: Type.String({ minLength: 1 }),
          priority: Type.Union([
            Type.Literal("high"),
            Type.Literal("medium"),
            Type.Literal("low"),
          ]),
        },
        { additionalProperties: false },
      ),
    ),
  },
  { additionalProperties: false },
);

export const renderedOutputPayloadSchema = Type.Object(
  {
    status: Type.Union([Type.Literal("complete"), Type.Literal("incomplete")]),
    title: Type.String({ minLength: 1 }),
    audience: Type.String({ minLength: 1 }),
    executive_summary: Type.String({ minLength: 1 }),
    key_findings: Type.Array(
      Type.Object(
        {
          statement: Type.String({ minLength: 1 }),
          interpretation: Type.String({ minLength: 1 }),
          evidence_refs: Type.Array(evidenceRefSchema, { minItems: 1, uniqueItems: true }),
          confidence: Type.Union([
            Type.Literal("high"),
            Type.Literal("medium"),
            Type.Literal("low"),
          ]),
        },
        { additionalProperties: false },
      ),
    ),
    recommendations: Type.Array(
      Type.Object(
        {
          action: Type.String({ minLength: 1 }),
          rationale: Type.String({ minLength: 1 }),
          priority: prioritySchema,
        },
        { additionalProperties: false },
      ),
    ),
    limitations: Type.Array(Type.String({ minLength: 1 }), { uniqueItems: true }),
    next_steps: Type.Array(Type.String({ minLength: 1 }), { uniqueItems: true }),
    source_artifact_ids: Type.Array(
      Type.String({ pattern: "^ar_[A-Za-z0-9_-]+$" }),
      { minItems: 1, uniqueItems: true },
    ),
    markdown: Type.String({ minLength: 1 }),
  },
  { additionalProperties: false },
);

export type ClarificationPayload = Static<typeof clarificationPayloadSchema>;
export type MetricDefinitionPayload = Static<typeof metricDefinitionPayloadSchema>;
export type QueryResultPayload = Static<typeof queryResultPayloadSchema>;
export type AnalysisPayload = Static<typeof analysisPayloadSchema>;
export type RenderedOutputPayload = Static<typeof renderedOutputPayloadSchema>;

export class ArtifactSubmissionError extends Error {}

export function validateClarificationPayload(value: unknown): string | undefined {
  if (!Value.Check(clarificationPayloadSchema, value)) {
    return "payload does not match ClarificationArtifact schema";
  }
  for (const field of ["start", "end"] as const) {
    const dateValue = value.time_range[field];
    if (typeof dateValue === "string" && !IsDateTime(dateValue)) {
      return `time_range.${field} must be an RFC 3339 date-time`;
    }
  }
  return undefined;
}

export function validateMetricDefinitionPayload(value: unknown): string | undefined {
  return Value.Check(metricDefinitionPayloadSchema, value)
    ? undefined
    : "payload does not match MetricDefinitionArtifact schema";
}

export function validateQueryResultPayload(value: unknown): string | undefined {
  if (!Value.Check(queryResultPayloadSchema, value)) {
    return "payload does not match QueryResultArtifact schema";
  }
  if (!IsDateTime(value.executed_at)) return "executed_at must be an RFC 3339 date-time";
  if (value.row_count !== value.rows.length) return "row_count must equal persisted rows length";
  if (value.rows.some((row) => row.length !== value.columns.length)) {
    return "every result row must match the columns length";
  }
  return undefined;
}

const unsupportedCausalCertainty =
  /(可排除|已经排除|直接导致|证明了|确定(?:的)?原因|直接来源|必然导致|\bcaused by\b|\bproves?\b|\brules? out\b|\bdefinitely\b)/i;

export function validateAnalysisPayload(value: unknown): string | undefined {
  if (!Value.Check(analysisPayloadSchema, value)) {
    return "payload does not match AnalysisArtifact schema";
  }
  if (value.status === "incomplete" && value.suggested_queries.length === 0) {
    return "incomplete analysis requires at least one suggested query";
  }
  if (
    [value.summary, ...value.findings.map((finding) => finding.statement)].some(
      (text) => unsupportedCausalCertainty.test(text),
    )
  ) {
    return "analysis findings use unsupported causal certainty; use association or hypothesis wording";
  }
  return undefined;
}

export function validateRenderedOutputPayload(value: unknown): string | undefined {
  if (!Value.Check(renderedOutputPayloadSchema, value)) {
    return "payload does not match RenderedOutputArtifact schema";
  }
  if (
    [
      value.executive_summary,
      ...value.key_findings.flatMap((finding) => [
        finding.statement,
        finding.interpretation,
      ]),
    ].some((text) => unsupportedCausalCertainty.test(text))
  ) {
    return "report uses unsupported causal certainty; preserve the AnalysisArtifact boundary";
  }
  return undefined;
}

function createSubmissionTool<T extends TSchema>(options: {
  name: string;
  label: string;
  description: string;
  schema: T;
  validate?: (params: Static<T>) => string | undefined;
}) {
  let submitted: Static<T> | undefined;
  const tool = defineTool({
    name: options.name,
    label: options.label,
    description: options.description,
    promptSnippet: `Submit the final ${options.label} as a terminating structured result`,
    promptGuidelines: [
      `Call ${options.name} exactly once as the final action.`,
      "Do not emit JSON in free text and do not call any other tool.",
      "Use only known facts from the user input; put uncertainty in assumptions or open questions.",
    ],
    parameters: options.schema,
    async execute(_toolCallId, params) {
      if (submitted !== undefined) {
        throw new ArtifactSubmissionError(`${options.name} may only be called once`);
      }
      if (!Value.Check(options.schema, params)) {
        const details = [...Value.Errors(options.schema, params)]
          .slice(0, 5)
          .map((error) => error.message)
          .join("; ");
        throw new ArtifactSubmissionError(`Invalid structured artifact payload: ${details}`);
      }
      const customError = options.validate?.(params);
      if (customError !== undefined) {
        throw new ArtifactSubmissionError(customError);
      }
      submitted = structuredClone(params);
      return {
        content: [{ type: "text" as const, text: `${options.label} accepted` }],
        details: { submitted: true },
        terminate: true,
      };
    },
  });
  return {
    tool,
    getSubmitted(): Static<T> | undefined {
      return submitted === undefined ? undefined : structuredClone(submitted);
    },
  };
}

export function createClarificationSubmissionTool() {
  return createSubmissionTool({
    name: "submit_clarification_artifact",
    label: "Clarification Artifact",
    description:
      "Submit the final structured requirement clarification. This is the only valid final output for this stage.",
    schema: clarificationPayloadSchema,
    validate: validateClarificationPayload,
  });
}

export function createMetricDefinitionSubmissionTool() {
  return createSubmissionTool({
    name: "submit_metric_definition_artifact",
    label: "Metric Definition Artifact",
    description:
      "Submit the final structured metric definition review. Unconfirmed business rules must remain in open_questions.",
    schema: metricDefinitionPayloadSchema,
    validate: validateMetricDefinitionPayload,
  });
}

export function createAnalysisSubmissionTool(options: {
  allowedEvidenceRefs?: ReadonlySet<string>;
} = {}) {
  return createSubmissionTool({
    name: "submit_analysis_artifact",
    label: "Analysis Artifact",
    description:
      "Submit evidence-bound analysis. Every finding must cite one or more supplied QueryRun evidence references; unverified causes remain hypotheses.",
    schema: analysisPayloadSchema,
    validate: (payload) => {
      const contractError = validateAnalysisPayload(payload);
      if (contractError !== undefined) return contractError;
      const references = [
        ...payload.findings.flatMap((finding) => finding.evidence_refs),
        ...payload.hypotheses.flatMap((hypothesis) => hypothesis.evidence_refs),
      ];
      return options.allowedEvidenceRefs !== undefined &&
        references.some((reference) => !options.allowedEvidenceRefs?.has(reference))
        ? "analysis cited an evidence reference not present in the supplied QueryResult"
        : undefined;
    },
  });
}

export function createRenderedOutputSubmissionTool(options: {
  analysisArtifactId?: string;
  allowedFindingStatements?: ReadonlySet<string>;
  allowedEvidenceRefs?: ReadonlySet<string>;
} = {}) {
  return createSubmissionTool({
    name: "submit_rendered_output_artifact",
    label: "Rendered Output Artifact",
    description:
      "Submit the report structure derived only from the supplied AnalysisArtifact. Every key finding must preserve QueryRun evidence references. Set markdown to the exact sentinel SERVER_RENDERED; the trusted service renders Markdown deterministically.",
    schema: renderedOutputPayloadSchema,
    validate: (payload) => {
      const contractError = validateRenderedOutputPayload(payload);
      if (contractError !== undefined) return contractError;
      if (payload.markdown !== "SERVER_RENDERED") {
        return "markdown must equal SERVER_RENDERED";
      }
      if (
        options.analysisArtifactId !== undefined &&
        !payload.source_artifact_ids.includes(options.analysisArtifactId)
      ) {
        return "report must reference the supplied AnalysisArtifact";
      }
      if (
        options.allowedFindingStatements !== undefined &&
        payload.key_findings.some(
          (finding) => !options.allowedFindingStatements?.has(finding.statement),
        )
      ) {
        return "report key finding statements must exactly copy AnalysisArtifact findings";
      }
      if (
        options.allowedEvidenceRefs !== undefined &&
        payload.key_findings
          .flatMap((finding) => finding.evidence_refs)
          .some((reference) => !options.allowedEvidenceRefs?.has(reference))
      ) {
        return "report introduced evidence absent from AnalysisArtifact";
      }
      return undefined;
    },
  });
}
