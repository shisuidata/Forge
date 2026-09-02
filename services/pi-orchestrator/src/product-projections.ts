import { Type, type Static, type TSchema } from "typebox";
import { Value } from "typebox/value";

const id = Type.String({ minLength: 1, maxLength: 256, pattern: "^[A-Za-z0-9_.:-]+$" });
const taskId = Type.String({ pattern: "^tr_[A-Za-z0-9_-]+$" });
const artifactId = Type.String({ pattern: "^ar_[A-Za-z0-9_-]+$" });
const reportId = Type.String({ pattern: "^rp_[A-Za-z0-9_-]+$" });
const queryRunId = Type.String({ pattern: "^qr_[A-Za-z0-9_-]+$" });
const hash = Type.String({ pattern: "^sha256:[a-f0-9]{64}$" });
const dateTime = Type.String({ format: "date-time" });
const reasonCode = Type.String({ pattern: "^[a-z][a-z0-9_.:-]{0,127}$" });
const relativeHref = Type.String({ minLength: 1, maxLength: 1000, pattern: "^/(?!/)[^\\s\\\\]*$" });
const boundedText = Type.String({ minLength: 1, maxLength: 20_000 });
const shortText = Type.String({ minLength: 1, maxLength: 200 });

export const productDisplayStates = [
  "needs_input",
  "waiting_decision",
  "running",
  "partial",
  "ready",
  "failed",
  "forbidden",
  "offline",
  "completed",
  "cancelled",
] as const;

export type ProductDisplayState = (typeof productDisplayStates)[number];

const displayStateSchema = Type.Union([
  Type.Literal("needs_input"),
  Type.Literal("waiting_decision"),
  Type.Literal("running"),
  Type.Literal("partial"),
  Type.Literal("ready"),
  Type.Literal("failed"),
  Type.Literal("forbidden"),
  Type.Literal("offline"),
  Type.Literal("completed"),
  Type.Literal("cancelled"),
]);

const scopeSchema = Type.Object(
  {
    org_id: id,
    team_id: id,
    user_id: id,
    channel: Type.Union([
      Type.Literal("web"),
      Type.Literal("feishu"),
      Type.Literal("dingtalk"),
      Type.Literal("api"),
    ]),
  },
  { additionalProperties: false },
);

const sourceRevisionSchema = Type.Object(
  {
    source: Type.Union([
      Type.Literal("pi_task_store"),
      Type.Literal("pi_event_store"),
      Type.Literal("pi_artifact_store"),
      Type.Literal("pi_attempt_store"),
      Type.Literal("forge_query_store"),
      Type.Literal("forge_report_store"),
      Type.Literal("forge_registry_store"),
    ]),
    revision: Type.String({ minLength: 1, maxLength: 256 }),
  },
  { additionalProperties: false },
);

const redactionSchema = Type.Object(
  {
    field_path: Type.String({
      minLength: 1,
      maxLength: 256,
      pattern: "^[A-Za-z0-9_]+(?:\\.[A-Za-z0-9_]+|\\[[0-9]+\\])*$",
    }),
    reason_code: reasonCode,
  },
  { additionalProperties: false },
);

const projectionMetaSchema = Type.Object(
  {
    availability: Type.Union([
      Type.Literal("ready"),
      Type.Literal("partial"),
      Type.Literal("offline"),
    ]),
    generated_at: dateTime,
    source_revisions: Type.Array(sourceRevisionSchema, {
      minItems: 1,
      maxItems: 16,
    }),
    unavailable_reasons: Type.Array(reasonCode, {
      maxItems: 16,
      uniqueItems: true,
    }),
    redactions: Type.Array(redactionSchema, { maxItems: 32 }),
  },
  { additionalProperties: false },
);

export const actionCapabilityV1Schema = Type.Object(
  {
    schema_version: Type.Literal(1),
    projection_type: Type.Literal("action_capability_v1"),
    task_run_id: taskId,
    action_type: Type.Union([
      Type.Literal("provide_input"),
      Type.Literal("approve_query"),
      Type.Literal("cancel_task"),
      Type.Literal("request_supplement"),
      Type.Literal("analyze"),
      Type.Literal("render_report"),
      Type.Literal("confirm_memory"),
      Type.Literal("open_report"),
    ]),
    label: Type.String({ minLength: 1, maxLength: 80 }),
    availability: Type.Union([Type.Literal("enabled"), Type.Literal("disabled")]),
    reason_code: Type.Union([reasonCode, Type.Null()]),
    requires_confirmation: Type.Boolean(),
    requires_idempotency_key: Type.Boolean(),
  },
  { additionalProperties: false },
);

const taskStatusSchema = Type.Union([
  Type.Literal("created"),
  Type.Literal("clarifying"),
  Type.Literal("ready_for_query"),
  Type.Literal("waiting_for_query_approval"),
  Type.Literal("waiting_for_action_approval"),
  Type.Literal("querying"),
  Type.Literal("ready_for_analysis"),
  Type.Literal("analyzing"),
  Type.Literal("ready_for_report"),
  Type.Literal("rendering"),
  Type.Literal("completed"),
  Type.Literal("needs_input"),
  Type.Literal("incomplete"),
  Type.Literal("cancelled"),
  Type.Literal("failed"),
  Type.Literal("expired"),
]);

const taskSummarySchema = Type.Object(
  {
    task_run_id: taskId,
    conversation_id: Type.Union([id, Type.Null()]),
    parent_task_run_id: Type.Union([taskId, Type.Null()]),
    intent: Type.String({ minLength: 1, maxLength: 256 }),
    title: shortText,
    status: taskStatusSchema,
    display_state: displayStateSchema,
    current_stage: Type.Union([Type.String({ minLength: 1, maxLength: 256 }), Type.Null()]),
    created_at: dateTime,
    updated_at: dateTime,
  },
  { additionalProperties: false },
);

export const taskSummaryV1Schema = Type.Object(
  {
    schema_version: Type.Literal(1),
    projection_type: Type.Literal("task_summary_v1"),
    scope: scopeSchema,
    ...taskSummarySchema.properties,
    href: relativeHref,
    projection_meta: projectionMetaSchema,
  },
  { additionalProperties: false },
);

const tableCellSchema = Type.Union([
  Type.String({ maxLength: 2_000 }),
  Type.Number(),
  Type.Boolean(),
  Type.Null(),
]);

const presentationSchema = Type.Object(
  {
    kind: Type.Union([
      Type.Literal("progress"),
      Type.Literal("needs_input"),
      Type.Literal("query_review"),
      Type.Literal("query_result"),
      Type.Literal("analysis"),
      Type.Literal("report"),
      Type.Literal("error"),
    ]),
    title: shortText,
    markdown: Type.String({ maxLength: 40_000 }),
    fields: Type.Array(
      Type.Object(
        {
          label: Type.String({ minLength: 1, maxLength: 80 }),
          value: Type.String({ maxLength: 2_000 }),
        },
        { additionalProperties: false },
      ),
      { maxItems: 24 },
    ),
    table: Type.Union([
      Type.Object(
        {
          columns: Type.Array(Type.String({ minLength: 1, maxLength: 128 }), {
            minItems: 1,
            maxItems: 100,
          }),
          rows: Type.Array(Type.Array(tableCellSchema, { maxItems: 100 }), { maxItems: 100 }),
          truncated: Type.Boolean(),
        },
        { additionalProperties: false },
      ),
      Type.Null(),
    ]),
    source_event_sequence: Type.Integer({ minimum: 0 }),
    source_artifact_ids: Type.Array(artifactId, { maxItems: 100, uniqueItems: true }),
  },
  { additionalProperties: false },
);

const conversationSummaryBodySchema = Type.Object(
  {
    scope: scopeSchema,
    conversation_id: id,
    title: shortText,
    display_state: displayStateSchema,
    task_count: Type.Integer({ minimum: 1 }),
    latest_task_run_id: taskId,
    latest_message_preview: Type.String({ minLength: 1, maxLength: 280 }),
    started_at: dateTime,
    updated_at: dateTime,
    href: relativeHref,
  },
  { additionalProperties: false },
);

export const conversationSummaryV1Schema = Type.Object(
  {
    schema_version: Type.Literal(1),
    projection_type: Type.Literal("conversation_summary_v1"),
    ...conversationSummaryBodySchema.properties,
    projection_meta: projectionMetaSchema,
  },
  { additionalProperties: false },
);

const conversationEntrySchema = Type.Object(
  {
    task: taskSummarySchema,
    user_message: Type.Object(
      {
        message_id: Type.Union([id, Type.Null()]),
        text: boundedText,
        created_at: dateTime,
      },
      { additionalProperties: false },
    ),
    presentation: presentationSchema,
    actions: Type.Array(actionCapabilityV1Schema, { maxItems: 16 }),
  },
  { additionalProperties: false },
);

export const conversationDetailV1Schema = Type.Object(
  {
    schema_version: Type.Literal(1),
    projection_type: Type.Literal("conversation_detail_v1"),
    scope: scopeSchema,
    summary: conversationSummaryV1Schema,
    entries: Type.Array(conversationEntrySchema, { minItems: 1, maxItems: 100 }),
    next_cursor: Type.Union([Type.String({ minLength: 1, maxLength: 512 }), Type.Null()]),
    projection_meta: projectionMetaSchema,
  },
  { additionalProperties: false },
);

const planSchema = Type.Object(
  {
    plan_revision: Type.Integer({ minimum: 1 }),
    status: Type.Union([
      Type.Literal("active"),
      Type.Literal("completed"),
      Type.Literal("superseded"),
      Type.Literal("failed"),
    ]),
    required_deliverables: Type.Array(Type.String({ minLength: 1, maxLength: 128 }), {
      maxItems: 32,
      uniqueItems: true,
    }),
    steps: Type.Array(
      Type.Object(
        {
          step_id: Type.String({ minLength: 1, maxLength: 128 }),
          title: shortText,
          capability: Type.String({ minLength: 1, maxLength: 128 }),
          status: Type.Union([
            Type.Literal("pending"),
            Type.Literal("ready"),
            Type.Literal("running"),
            Type.Literal("waiting_approval"),
            Type.Literal("completed"),
            Type.Literal("skipped"),
            Type.Literal("failed"),
          ]),
          required: Type.Boolean(),
          depends_on: Type.Array(Type.String({ minLength: 1, maxLength: 128 }), {
            maxItems: 32,
            uniqueItems: true,
          }),
        },
        { additionalProperties: false },
      ),
      { maxItems: 64 },
    ),
  },
  { additionalProperties: false },
);

const queryReviewSchema = Type.Object(
  {
    review_type: Type.Literal("query"),
    query_run_id: queryRunId,
    sql: Type.String({ minLength: 1, maxLength: 100_000 }),
    sql_hash: hash,
    assurance_report_hash: hash,
    dialect: Type.String({ minLength: 1, maxLength: 64 }),
    expires_at: dateTime,
    read_only: Type.Literal(true),
    risk_summary: Type.Array(Type.String({ minLength: 1, maxLength: 500 }), { maxItems: 16 }),
  },
  { additionalProperties: false },
);

const activitySchema = Type.Object(
  {
    sequence: Type.Integer({ minimum: 1 }),
    event_type: Type.String({ minLength: 1, maxLength: 128 }),
    title: shortText,
    detail: Type.Union([Type.String({ minLength: 1, maxLength: 1_000 }), Type.Null()]),
    state: displayStateSchema,
    created_at: dateTime,
  },
  { additionalProperties: false },
);

const attemptSummarySchema = Type.Object(
  {
    attempt_id: Type.String({ pattern: "^sa_[A-Za-z0-9_-]+$" }),
    stage: Type.String({ minLength: 1, maxLength: 256 }),
    status: Type.Union([
      Type.Literal("running"),
      Type.Literal("succeeded"),
      Type.Literal("failed"),
      Type.Literal("timed_out"),
      Type.Literal("interrupted"),
    ]),
    started_at: dateTime,
    finished_at: Type.Union([dateTime, Type.Null()]),
    elapsed_ms: Type.Integer({ minimum: 0 }),
    safe_error: Type.Union([Type.String({ minLength: 1, maxLength: 1_000 }), Type.Null()]),
  },
  { additionalProperties: false },
);

const artifactSummarySchema = Type.Object(
  {
    artifact_id: artifactId,
    artifact_type: Type.Union([
      Type.Literal("clarification"),
      Type.Literal("execution_plan"),
      Type.Literal("chart"),
      Type.Literal("technical_report"),
      Type.Literal("report_bundle"),
      Type.Literal("publication"),
      Type.Literal("metric_definition"),
      Type.Literal("query_result"),
      Type.Literal("analysis"),
      Type.Literal("advisory"),
      Type.Literal("rendered_output"),
    ]),
    producer: Type.String({ minLength: 1, maxLength: 256 }),
    title: shortText,
    state: Type.Union([
      Type.Literal("ready"),
      Type.Literal("partial"),
      Type.Literal("failed"),
      Type.Literal("superseded"),
    ]),
    created_at: dateTime,
    evidence_refs: Type.Array(Type.String({ minLength: 1, maxLength: 512 }), {
      maxItems: 1_000,
      uniqueItems: true,
    }),
    href: Type.Union([relativeHref, Type.Null()]),
  },
  { additionalProperties: false },
);

export const taskDetailProjectionV1Schema = Type.Object(
  {
    schema_version: Type.Literal(1),
    projection_type: Type.Literal("task_detail_projection_v1"),
    scope: scopeSchema,
    task: taskSummarySchema,
    plan: Type.Union([planSchema, Type.Null()]),
    review_request: Type.Union([queryReviewSchema, Type.Null()]),
    presentation: presentationSchema,
    actions: Type.Array(actionCapabilityV1Schema, { maxItems: 16 }),
    activity: Type.Array(activitySchema, { maxItems: 200 }),
    attempts: Type.Array(attemptSummarySchema, { maxItems: 100 }),
    artifacts: Type.Array(artifactSummarySchema, { maxItems: 100 }),
    relations: Type.Object(
      {
        parent_task_run_id: Type.Union([taskId, Type.Null()]),
        child_task_run_ids: Type.Array(taskId, { maxItems: 100, uniqueItems: true }),
      },
      { additionalProperties: false },
    ),
    projection_meta: projectionMetaSchema,
  },
  { additionalProperties: false },
);

const workspaceItemSchema = Type.Object(
  {
    item_type: Type.Union([
      Type.Literal("task"),
      Type.Literal("report"),
      Type.Literal("dependency"),
    ]),
    item_id: id,
    title: shortText,
    state: displayStateSchema,
    updated_at: dateTime,
    href: relativeHref,
    reason: Type.Union([Type.String({ minLength: 1, maxLength: 500 }), Type.Null()]),
  },
  { additionalProperties: false },
);

export const workspaceProjectionV1Schema = Type.Object(
  {
    schema_version: Type.Literal(1),
    projection_type: Type.Literal("workspace_projection_v1"),
    scope: scopeSchema,
    counts: Type.Object(
      {
        needs_input: Type.Integer({ minimum: 0 }),
        waiting_decision: Type.Integer({ minimum: 0 }),
        running: Type.Integer({ minimum: 0 }),
        failed: Type.Integer({ minimum: 0 }),
        recent_reports: Type.Integer({ minimum: 0 }),
      },
      { additionalProperties: false },
    ),
    needs_input: Type.Array(workspaceItemSchema, { maxItems: 20 }),
    waiting_decision: Type.Array(workspaceItemSchema, { maxItems: 20 }),
    running: Type.Array(workspaceItemSchema, { maxItems: 20 }),
    failed: Type.Array(workspaceItemSchema, { maxItems: 20 }),
    recent_reports: Type.Array(workspaceItemSchema, { maxItems: 20 }),
    dependencies: Type.Array(workspaceItemSchema, { maxItems: 20 }),
    projection_meta: projectionMetaSchema,
  },
  { additionalProperties: false },
);

export const reportSummaryV1Schema = Type.Object(
  {
    schema_version: Type.Literal(1),
    projection_type: Type.Literal("report_summary_v1"),
    scope: Type.Object(
      {
        org_id: id,
        team_id: id,
        user_id: id,
      },
      { additionalProperties: false },
    ),
    report_id: reportId,
    task_run_id: taskId,
    revision: Type.Integer({ minimum: 1 }),
    title: shortText,
    status: Type.Union([
      Type.Literal("publishing"),
      Type.Literal("published"),
      Type.Literal("failed"),
    ]),
    display_state: displayStateSchema,
    pdf_status: Type.Union([
      Type.Literal("pending"),
      Type.Literal("ready"),
      Type.Literal("failed"),
    ]),
    pptx_status: Type.Union([
      Type.Literal("pending"),
      Type.Literal("ready"),
      Type.Literal("failed"),
    ]),
    internal_url: Type.Union([relativeHref, Type.Null()]),
    technical_url: Type.Union([relativeHref, Type.Null()]),
    pdf_url: Type.Union([relativeHref, Type.Null()]),
    pptx_url: Type.Union([relativeHref, Type.Null()]),
    created_at: dateTime,
    updated_at: dateTime,
    projection_meta: projectionMetaSchema,
  },
  { additionalProperties: false },
);

export const productProjectionV1Schema = Type.Union(
  [
    actionCapabilityV1Schema,
    conversationSummaryV1Schema,
    conversationDetailV1Schema,
    taskSummaryV1Schema,
    taskDetailProjectionV1Schema,
    workspaceProjectionV1Schema,
    reportSummaryV1Schema,
  ],
  { $id: "https://forge.local/contracts/product-projection-v1.schema.json" },
);

export const productProjectionSchemas = {
  action_capability_v1: actionCapabilityV1Schema,
  conversation_summary_v1: conversationSummaryV1Schema,
  conversation_detail_v1: conversationDetailV1Schema,
  task_summary_v1: taskSummaryV1Schema,
  task_detail_projection_v1: taskDetailProjectionV1Schema,
  workspace_projection_v1: workspaceProjectionV1Schema,
  report_summary_v1: reportSummaryV1Schema,
} as const satisfies Record<string, TSchema>;

export type ProductProjectionContractName = keyof typeof productProjectionSchemas;
export type ActionCapabilityV1 = Static<typeof actionCapabilityV1Schema>;
export type ConversationSummaryV1 = Static<typeof conversationSummaryV1Schema>;
export type ConversationDetailV1 = Static<typeof conversationDetailV1Schema>;
export type TaskSummaryV1 = Static<typeof taskSummaryV1Schema>;
export type TaskDetailProjectionV1 = Static<typeof taskDetailProjectionV1Schema>;
export type WorkspaceProjectionV1 = Static<typeof workspaceProjectionV1Schema>;
export type ReportSummaryV1 = Static<typeof reportSummaryV1Schema>;

function projectionMeta(value: Record<string, unknown>): Record<string, unknown> | undefined {
  const meta = value.projection_meta;
  return typeof meta === "object" && meta !== null && !Array.isArray(meta)
    ? meta as Record<string, unknown>
    : undefined;
}

function validateMeta(meta: Record<string, unknown> | undefined, errors: string[]): void {
  if (meta === undefined) return;
  const availability = meta.availability;
  const reasons = Array.isArray(meta.unavailable_reasons)
    ? meta.unavailable_reasons as unknown[]
    : [];
  if (availability === "ready" && reasons.length > 0) {
    errors.push("meta.ready_has_unavailable_reason");
  }
  if ((availability === "partial" || availability === "offline") && reasons.length === 0) {
    errors.push("meta.unavailable_reason_required");
  }
  const revisions = Array.isArray(meta.source_revisions)
    ? meta.source_revisions as Array<Record<string, unknown>>
    : [];
  const keys = revisions.map((revision) => `${String(revision.source)}:${String(revision.revision)}`);
  if (new Set(keys).size !== keys.length) errors.push("meta.duplicate_source_revision");
}

function validateAction(value: ActionCapabilityV1, errors: string[]): void {
  if (value.availability === "enabled" && value.reason_code !== null) {
    errors.push("action.enabled_has_reason");
  }
  if (value.availability === "disabled" && value.reason_code === null) {
    errors.push("action.disabled_reason_required");
  }
  if (value.action_type === "approve_query" && !value.requires_confirmation) {
    errors.push("action.query_approval_requires_confirmation");
  }
}

export function taskDisplayStateForStatus(
  status: TaskDetailProjectionV1["task"]["status"],
): ProductDisplayState {
  if (status === "needs_input") return "needs_input";
  if (status === "waiting_for_query_approval" || status === "waiting_for_action_approval") {
    return "waiting_decision";
  }
  if (["created", "clarifying", "querying", "analyzing", "rendering"].includes(status)) {
    return "running";
  }
  if (status === "incomplete") return "partial";
  if (status === "failed" || status === "expired") return "failed";
  if (status === "cancelled") return "cancelled";
  if (status === "completed") return "completed";
  return "ready";
}

function validatePresentation(
  presentation: Record<string, unknown> | undefined,
  errors: string[],
): void {
  const table = presentation?.table;
  if (typeof table !== "object" || table === null || Array.isArray(table)) return;
  const columns = Array.isArray((table as Record<string, unknown>).columns)
    ? (table as Record<string, unknown>).columns as unknown[]
    : [];
  const rows = Array.isArray((table as Record<string, unknown>).rows)
    ? (table as Record<string, unknown>).rows as unknown[]
    : [];
  if (rows.some((row) => !Array.isArray(row) || row.length !== columns.length)) {
    errors.push("presentation.table_shape_mismatch");
  }
}

export function validateProductProjection(
  name: ProductProjectionContractName,
  value: unknown,
): string[] {
  const schema = productProjectionSchemas[name];
  if (!Value.Check(schema, value)) return ["contract.invalid"];
  const projection = value as Record<string, unknown>;
  const errors: string[] = [];
  validateMeta(projectionMeta(projection), errors);

  if (name === "action_capability_v1") {
    validateAction(value as ActionCapabilityV1, errors);
  }

  if (name === "conversation_detail_v1") {
    const detail = value as ConversationDetailV1;
    validateMeta(projectionMeta(detail.summary as unknown as Record<string, unknown>), errors);
    if (JSON.stringify(detail.scope) !== JSON.stringify(detail.summary.scope)) {
      errors.push("conversation.scope_mismatch");
    }
    for (const entry of detail.entries) {
      validatePresentation(entry.presentation as unknown as Record<string, unknown>, errors);
      for (const capability of entry.actions) {
        validateAction(capability, errors);
        if (capability.task_run_id !== entry.task.task_run_id) {
          errors.push("conversation.action_task_mismatch");
        }
      }
    }
  }

  if (name === "task_summary_v1") {
    const summary = value as TaskSummaryV1;
    if (summary.display_state !== taskDisplayStateForStatus(summary.status)) {
      errors.push("task.display_state_mismatch");
    }
  }

  if (name === "task_detail_projection_v1") {
    const detail = value as TaskDetailProjectionV1;
    validatePresentation(detail.presentation as unknown as Record<string, unknown>, errors);
    if (detail.task.display_state !== taskDisplayStateForStatus(detail.task.status)) {
      errors.push("task.display_state_mismatch");
    }
    for (const capability of detail.actions) {
      validateAction(capability, errors);
      if (capability.task_run_id !== detail.task.task_run_id) {
        errors.push("task.action_task_mismatch");
      }
    }
    if (detail.task.status === "waiting_for_query_approval") {
      if (detail.review_request === null) errors.push("task.query_review_required");
      if (!detail.actions.some((action) => action.action_type === "approve_query")) {
        errors.push("task.query_approval_action_required");
      }
    }
    const artifactIds = new Set(detail.artifacts.map((artifact) => artifact.artifact_id));
    if (detail.presentation.source_artifact_ids.some((artifactId) => !artifactIds.has(artifactId))) {
      errors.push("task.presentation_artifact_missing");
    }
    if (detail.relations.parent_task_run_id !== detail.task.parent_task_run_id) {
      errors.push("task.parent_relation_mismatch");
    }
  }

  if (name === "workspace_projection_v1") {
    const workspace = value as WorkspaceProjectionV1;
    for (const section of ["needs_input", "waiting_decision", "running", "failed", "recent_reports"] as const) {
      if (workspace.counts[section] < workspace[section].length) {
        errors.push(`workspace.${section}_count_underflow`);
      }
    }
  }

  if (name === "report_summary_v1") {
    const report = value as ReportSummaryV1;
    if (report.status === "published" && report.internal_url === null) {
      errors.push("report.published_url_required");
    }
    const allowedDisplayStates = report.status === "publishing"
      ? new Set(["running"])
      : report.status === "failed"
        ? new Set(["failed"])
        : new Set(["completed", "partial"]);
    if (!allowedDisplayStates.has(report.display_state)) {
      errors.push("report.display_state_mismatch");
    }
    if (report.pdf_status === "ready" && report.pdf_url === null) {
      errors.push("report.ready_pdf_url_required");
    }
    if (report.pptx_status === "ready" && report.pptx_url === null) {
      errors.push("report.ready_pptx_url_required");
    }
  }

  return [...new Set(errors)];
}
