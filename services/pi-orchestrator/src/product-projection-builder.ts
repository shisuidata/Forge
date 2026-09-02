import { createHash } from "node:crypto";

import type { Artifact, ArtifactStore, ArtifactType } from "./artifacts.js";
import type { ChannelAction, ChannelPresentation } from "./channels/contracts.js";
import { renderChannelPresentation } from "./channels/renderer.js";
import {
  taskDisplayStateForStatus,
  type ActionCapabilityV1,
  type ConversationDetailV1,
  type ConversationSummaryV1,
  type TaskDetailProjectionV1,
  validateProductProjection,
} from "./product-projections.js";
import type { StageAttemptStore } from "./stage-attempts.js";
import {
  TaskStateError,
  type ConversationListCursor,
  type ConversationTaskCursor,
  type ConversationTaskGroup,
  type TaskChannel,
  type TaskRun,
  type TaskStore,
} from "./task-store.js";
import type { TaskEvent, TaskEventStore } from "./task-events.js";

const CONVERSATION_TASK_LIMIT = 100;
const TASK_ACTIVITY_LIMIT = 200;
const TASK_ARTIFACT_LIMIT = 100;
const TASK_ATTEMPT_LIMIT = 100;
const RESPONSE_LIMIT_BYTES = 2_000_000;
const INTERNAL_TEXT_PATTERN =
  /(?:\b(?:api[_-]?key|password|secret|authorization)\s*[:=]\s*\S+|bearer\s+\S+|\/(?:home|Users|tmp)\/|Traceback \(most recent call last\))/i;

export type ProductConversationChannel = Exclude<TaskChannel, "api">;

export interface ProductProjectionScope {
  orgId: string;
  teamId: string;
  userId: string;
  channel: TaskChannel;
}

export interface ConversationListInput {
  orgId: string;
  teamId: string;
  userId: string;
  channel: ProductConversationChannel;
  limit: number;
  cursor?: string;
}

export interface ConversationListProjection {
  schema_version: 1;
  conversations: ConversationSummaryV1[];
  next_cursor: string | null;
}

type TaskPlan = NonNullable<TaskDetailProjectionV1["plan"]>;
type TaskPlanStepStatus = TaskPlan["steps"][number]["status"];

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function bounded(value: unknown, maximum: number, fallback = ""): string {
  if (typeof value !== "string") return fallback;
  const withoutThink = value.replace(/<think>[\s\S]*?(?:<\/think>|$)/gi, "");
  const withoutSecrets = withoutThink
    .split("\n")
    .filter((line) => !INTERNAL_TEXT_PATTERN.test(line))
    .join("\n")
    .trim();
  return withoutSecrets.slice(0, maximum) || fallback;
}

function containsSensitiveText(value: unknown): boolean {
  return typeof value === "string" && value.split("\n").some((line) => INTERNAL_TEXT_PATTERN.test(line));
}

function titleFromTask(task: TaskRun): string {
  const original = bounded(task.metadata.original_message, 20_000, task.intent);
  return original.split("\n")[0]!.trim().slice(0, 200) || "未命名数据任务";
}

function preview(value: string): string {
  const plain = value
    .replace(/```[\s\S]*?```/g, " [代码内容] ")
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
    .replace(/[#>*_`~|-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return (plain || "任务已更新").slice(0, 280);
}

function productScope(task: TaskRun): ConversationSummaryV1["scope"] {
  return {
    org_id: task.org_id,
    team_id: task.team_id,
    user_id: task.user_id,
    channel: task.channel,
  };
}

function sourceRevision(
  source: ConversationSummaryV1["projection_meta"]["source_revisions"][number]["source"],
  revision: string,
) {
  return { source, revision };
}

function revisionHash(parts: string[]): string {
  return `sha256:${createHash("sha256").update(parts.join("\n")).digest("hex")}`;
}

function boundedCell(value: unknown): string | number | boolean | null {
  if (value === null || typeof value === "number" || typeof value === "boolean") return value;
  if (typeof value === "string") return value.slice(0, 2_000);
  try {
    const serialized = JSON.stringify(value);
    return typeof serialized === "string"
      ? serialized.slice(0, 2_000)
      : "[结构化值不可显示]";
  } catch {
    return "[结构化值不可显示]";
  }
}

function boundedPresentation(
  presentation: ChannelPresentation,
  allowedArtifactIds?: Set<string>,
): ConversationDetailV1["entries"][number]["presentation"] {
  const columns = presentation.table?.columns.slice(0, 100) ?? [];
  const rows = presentation.table?.rows.slice(0, 100).map((row) =>
    row.slice(0, columns.length).map(boundedCell)) ?? [];
  const sourceArtifactIds = presentation.source_artifact_ids
    .filter((artifactId) => allowedArtifactIds === undefined || allowedArtifactIds.has(artifactId))
    .slice(0, 100);
  return {
    kind: presentation.kind,
    title: bounded(presentation.title, 200, "任务更新"),
    markdown: bounded(presentation.markdown, 40_000),
    fields: presentation.fields.slice(0, 24).map((field) => ({
      label: bounded(field.label, 80, "信息"),
      value: bounded(field.value, 2_000),
    })),
    table: presentation.table === null
      ? null
      : {
          columns,
          rows,
          truncated:
            presentation.table.truncated ||
            presentation.table.columns.length > columns.length ||
            presentation.table.rows.length > rows.length ||
            presentation.table.rows.some((row) => row.length !== columns.length),
        },
    source_event_sequence: presentation.source_event_sequence,
    source_artifact_ids: sourceArtifactIds,
  };
}

function actionCapability(action: ChannelAction): ActionCapabilityV1 {
  const confirmation = ["approve_query", "cancel_task", "confirm_memory"].includes(action.type);
  return {
    schema_version: 1,
    projection_type: "action_capability_v1",
    task_run_id: action.task_run_id,
    action_type: action.type,
    label: bounded(action.label, 80, "继续"),
    availability: "enabled",
    reason_code: null,
    requires_confirmation: confirmation,
    requires_idempotency_key: true,
  };
}

function taskSummary(task: TaskRun): TaskDetailProjectionV1["task"] {
  return {
    task_run_id: task.task_run_id,
    conversation_id: task.channel_conversation_id,
    parent_task_run_id: task.parent_task_run_id,
    intent: task.intent.slice(0, 256),
    title: titleFromTask(task),
    status: task.status,
    display_state: taskDisplayStateForStatus(task.status),
    current_stage: task.current_stage?.slice(0, 256) ?? null,
    created_at: task.created_at,
    updated_at: task.updated_at,
  };
}

function latestEvent(events: TaskEvent[], eventType: string): TaskEvent | undefined {
  return [...events].reverse().find((event) => event.event_type === eventType);
}

function planProjection(artifact: Artifact | undefined): TaskDetailProjectionV1["plan"] {
  if (artifact === undefined || !record(artifact.payload)) return null;
  const payload = artifact.payload;
  if (!Number.isInteger(payload.plan_revision) || !Array.isArray(payload.steps)) return null;
  const validStatuses = new Set(["active", "completed", "superseded", "failed"]);
  const status = validStatuses.has(String(payload.status)) ? String(payload.status) : "active";
  const stepStatuses = new Set([
    "pending", "ready", "running", "waiting_approval", "completed", "skipped", "failed",
  ]);
  const steps = payload.steps.slice(0, 64).filter(record).map((step, index) => ({
    step_id: bounded(step.step_id, 128, `step_${index + 1}`),
    title: bounded(step.title, 200, `步骤 ${index + 1}`),
    capability: bounded(step.capability, 128, "unknown"),
    status: (stepStatuses.has(String(step.status))
      ? String(step.status)
      : "pending") as TaskPlanStepStatus,
    required: step.required === true,
    depends_on: Array.isArray(step.depends_on)
      ? step.depends_on.filter((item): item is string => typeof item === "string").slice(0, 32)
      : [],
  }));
  return {
    plan_revision: Number(payload.plan_revision),
    status: status as TaskPlan["status"],
    required_deliverables: Array.isArray(payload.required_deliverables)
      ? payload.required_deliverables
          .filter((item): item is string => typeof item === "string" && item.length > 0)
          .slice(0, 32)
      : [],
    steps,
  };
}

function reviewProjection(events: TaskEvent[]): TaskDetailProjectionV1["review_request"] {
  const review = latestEvent(events, "query.review_requested");
  if (review === undefined) return null;
  const payload = review.payload;
  const sql = typeof payload.sql === "string" ? payload.sql : "";
  const queryRunId = typeof payload.query_run_id === "string" ? payload.query_run_id : "";
  const sqlHash = typeof payload.sql_hash === "string" ? payload.sql_hash : "";
  const assuranceHash = typeof payload.assurance_report_hash === "string"
    ? payload.assurance_report_hash
    : "";
  const expiresAt = typeof payload.expires_at === "string" ? payload.expires_at : "";
  if (
    !/^qr_[A-Za-z0-9_-]+$/.test(queryRunId) ||
    !/^sha256:[a-f0-9]{64}$/.test(sqlHash) ||
    !/^sha256:[a-f0-9]{64}$/.test(assuranceHash) ||
    sql.length === 0 ||
    expiresAt.length === 0
  ) return null;
  return {
    review_type: "query",
    query_run_id: queryRunId,
    sql: sql.slice(0, 100_000),
    sql_hash: sqlHash,
    assurance_report_hash: assuranceHash,
    dialect: bounded(payload.dialect, 64, "unknown"),
    expires_at: expiresAt,
    read_only: true,
    risk_summary: [
      "查询仅允许只读执行。",
      "执行前将重新校验 SQL、Assurance 与 Registry revision。",
    ],
  };
}

function activityTitle(eventType: string): string {
  const titles: Record<string, string> = {
    "task.created": "任务已创建",
    "task.status_changed": "任务状态已更新",
    "plan.created": "执行计划已创建",
    "plan.revised": "执行计划已修订",
    "query.review_requested": "等待查询审批",
    "query.clarification_requested": "需要补充信息",
    "query.approval_submitted": "查询审批已提交",
    "query.completed": "只读查询已完成",
    "query.execution_failed": "查询未完成",
    "query.prepare_failed": "查询准备未完成",
    "query.prepare_timed_out": "查询准备超时",
    "artifact.created": "阶段交付已生成",
    "analysis.completed": "分析已生成",
    "report.completed": "报告已生成",
    "stage.attempt_started": "阶段已开始",
    "stage.attempt_succeeded": "阶段已完成",
    "stage.attempt_failed": "阶段未完成",
    "stage.attempt_timed_out": "阶段超时",
    "stage.attempt_interrupted": "阶段已中断",
  };
  return titles[eventType] ?? "任务活动已更新";
}

function activityState(event: TaskEvent): TaskDetailProjectionV1["activity"][number]["state"] {
  if (event.event_type.includes("failed") || event.event_type.includes("timed_out")) return "failed";
  if (event.event_type === "query.review_requested") return "waiting_decision";
  if (event.event_type === "query.clarification_requested") return "needs_input";
  if (event.event_type.endsWith("started")) return "running";
  return "ready";
}

function evidenceRefs(value: unknown, output = new Set<string>(), depth = 0): Set<string> {
  if (output.size >= 1_000 || depth > 8) return output;
  if (typeof value === "string") {
    if (/^(?:qr_[A-Za-z0-9_-]+#row:[1-9][0-9]*|ctx_[A-Za-z0-9_.:-]+)$/.test(value)) {
      output.add(value.slice(0, 512));
    }
    return output;
  }
  if (Array.isArray(value)) {
    for (const item of value) evidenceRefs(item, output, depth + 1);
    return output;
  }
  if (record(value)) {
    for (const item of Object.values(value)) evidenceRefs(item, output, depth + 1);
  }
  return output;
}

function artifactTitle(type: ArtifactType): string {
  const titles: Record<ArtifactType, string> = {
    clarification: "需求澄清",
    execution_plan: "执行计划",
    chart: "数据图表",
    technical_report: "技术报告",
    report_bundle: "报告包",
    publication: "已发布报告",
    metric_definition: "指标定义",
    query_result: "查询结果",
    analysis: "分析结果",
    advisory: "专业建议",
    rendered_output: "业务报告",
  };
  return titles[type];
}

function artifactHref(taskRunId: string, artifact: Artifact): string | null {
  if (artifact.artifact_type === "publication" && typeof artifact.payload.internal_url === "string") {
    const url = artifact.payload.internal_url;
    if (/^\/(?!\/)[^\s\\]*$/.test(url)) return url.slice(0, 1_000);
  }
  return `/tasks/${taskRunId}#artifact-${artifact.artifact_id}`;
}

function assertProjection(name: Parameters<typeof validateProductProjection>[0], value: unknown): void {
  const errors = validateProductProjection(name, value);
  if (errors.length > 0) {
    throw new TaskStateError(`Product Projection is inconsistent: ${errors.join(",")}`);
  }
  if (Buffer.byteLength(JSON.stringify(value), "utf8") > RESPONSE_LIMIT_BYTES) {
    throw new TaskStateError("Product Projection exceeds the 2 MB response boundary");
  }
}

function decodeCursor(cursor: string | undefined): ConversationListCursor | undefined {
  if (cursor === undefined) return undefined;
  try {
    const parsed = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8")) as unknown;
    if (
      !record(parsed) ||
      typeof parsed.updated_at !== "string" ||
      typeof parsed.conversation_id !== "string" ||
      parsed.updated_at.length > 64 ||
      parsed.conversation_id.length > 256
    ) throw new Error("invalid cursor fields");
    return { updatedAt: parsed.updated_at, conversationId: parsed.conversation_id };
  } catch {
    throw new TaskStateError("Invalid Conversation cursor");
  }
}

function encodeCursor(group: ConversationTaskGroup): string {
  return Buffer.from(JSON.stringify({
    updated_at: group.updatedAt,
    conversation_id: group.conversationId,
  })).toString("base64url");
}

function decodeTaskCursor(cursor: string | undefined): ConversationTaskCursor | undefined {
  if (cursor === undefined) return undefined;
  try {
    const parsed = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8")) as unknown;
    if (
      !record(parsed) ||
      typeof parsed.created_at !== "string" ||
      typeof parsed.task_run_id !== "string" ||
      parsed.created_at.length > 64 ||
      !/^tr_[A-Za-z0-9_-]+$/.test(parsed.task_run_id)
    ) throw new Error("invalid task cursor fields");
    return { createdAt: parsed.created_at, taskRunId: parsed.task_run_id };
  } catch {
    throw new TaskStateError("Invalid Conversation Task cursor");
  }
}

function encodeTaskCursor(task: TaskRun): string {
  return Buffer.from(JSON.stringify({
    created_at: task.created_at,
    task_run_id: task.task_run_id,
  })).toString("base64url");
}

export class ProductProjectionService {
  constructor(private readonly ports: {
    tasks: TaskStore;
    events: TaskEventStore;
    artifacts: ArtifactStore;
    attempts?: StageAttemptStore;
    now?: () => Date;
  }) {}

  listConversations(input: ConversationListInput): ConversationListProjection {
    if (!Number.isInteger(input.limit) || input.limit < 1 || input.limit > 50) {
      throw new TaskStateError("Conversation limit must be an integer from 1 to 50");
    }
    const before = decodeCursor(input.cursor);
    const groups = this.ports.tasks.listConversations({
      orgId: input.orgId,
      teamId: input.teamId,
      userId: input.userId,
      channel: input.channel,
      limit: input.limit + 1,
      taskLimit: 1,
      includeTasks: false,
      ...(before === undefined ? {} : { before }),
    });
    const visible = groups.slice(0, input.limit);
    const conversations = visible.map((group) => this.#conversationSummary(group));
    for (const conversation of conversations) {
      assertProjection("conversation_summary_v1", conversation);
    }
    return {
      schema_version: 1,
      conversations,
      next_cursor: groups.length > input.limit && visible.at(-1) !== undefined
        ? encodeCursor(visible.at(-1)!)
        : null,
    };
  }

  getConversation(input: {
    orgId: string;
    teamId: string;
    userId: string;
    channel: ProductConversationChannel;
    conversationId: string;
    cursor?: string;
  }): ConversationDetailV1 | undefined {
    const beforeTask = decodeTaskCursor(input.cursor);
    const group = this.ports.tasks.getConversation({
      orgId: input.orgId,
      teamId: input.teamId,
      userId: input.userId,
      channel: input.channel,
      conversationId: input.conversationId,
      taskLimit: CONVERSATION_TASK_LIMIT,
      ...(beforeTask === undefined ? {} : { beforeTask }),
    });
    if (group === undefined) return undefined;
    if (group.tasks.length === 0) {
      throw new TaskStateError("Conversation cursor does not reference a visible Task page");
    }
    const summary = this.#conversationSummary(group);
    const reasons = group.tasksTruncated ? ["conversation_entries_truncated"] : [];
    const contexts = group.tasks.map((task) => {
      const events = this.ports.events.list(task.task_run_id);
      const artifacts = this.ports.artifacts.list(task.task_run_id);
      return {
        task,
        events,
        artifacts,
        presentation: renderChannelPresentation({ task, events, artifacts }),
      };
    });
    const conversationRedactions = contexts.some(({ task, presentation }) =>
      containsSensitiveText(task.metadata.original_message) ||
      containsSensitiveText(presentation.markdown));
    const detail: ConversationDetailV1 = {
      schema_version: 1,
      projection_type: "conversation_detail_v1",
      scope: summary.scope,
      summary,
      entries: contexts.map(({ task, presentation }) => ({
        task: taskSummary(task),
        user_message: {
          message_id: typeof task.metadata.channel_message_id === "string"
            ? task.metadata.channel_message_id.slice(0, 256)
            : null,
          text: bounded(task.metadata.original_message, 20_000, task.intent),
          created_at: task.created_at,
        },
        presentation: boundedPresentation(presentation),
        actions: presentation.actions.slice(0, 16).map(actionCapability),
      })),
      next_cursor: group.tasksTruncated ? encodeTaskCursor(group.tasks[0]!) : null,
      projection_meta: {
        availability: reasons.length > 0 ? "partial" : "ready",
        generated_at: this.#now(),
        source_revisions: [
          sourceRevision("pi_task_store", group.updatedAt),
          sourceRevision("pi_event_store", revisionHash(contexts.map(({ task, events }) =>
            `${task.task_run_id}:${events.at(-1)?.sequence ?? 0}`))),
          sourceRevision("pi_artifact_store", revisionHash(contexts.map(({ task, artifacts }) =>
            `${task.task_run_id}:${artifacts.length}`))),
        ],
        unavailable_reasons: reasons,
        redactions: conversationRedactions
          ? [{ field_path: "entries", reason_code: "sensitive_text_redacted" }]
          : [],
      },
    };
    assertProjection("conversation_detail_v1", detail);
    return detail;
  }

  getTaskDetail(input: ProductProjectionScope & { taskRunId: string }): TaskDetailProjectionV1 | undefined {
    const task = this.ports.tasks.get(input.taskRunId);
    if (
      task === undefined ||
      task.org_id !== input.orgId ||
      task.team_id !== input.teamId ||
      task.user_id !== input.userId ||
      task.channel !== input.channel
    ) return undefined;
    const events = this.ports.events.list(task.task_run_id);
    const allArtifacts = this.ports.artifacts.list(task.task_run_id);
    const artifacts = allArtifacts.slice(-TASK_ARTIFACT_LIMIT);
    const allowedArtifactIds = new Set(artifacts.map((artifact) => artifact.artifact_id));
    const allAttempts = this.ports.attempts?.list(task.task_run_id) ?? [];
    const attempts = allAttempts.slice(-TASK_ATTEMPT_LIMIT);
    const presentation = renderChannelPresentation({ task, events, artifacts: allArtifacts });
    const reasons: string[] = [];
    const redactions: TaskDetailProjectionV1["projection_meta"]["redactions"] = [];
    if (allArtifacts.length > artifacts.length) reasons.push("artifact_projection_truncated");
    if (allAttempts.length > attempts.length) reasons.push("attempt_projection_truncated");
    if (events.length > TASK_ACTIVITY_LIMIT) reasons.push("activity_projection_truncated");
    if (allAttempts.some((attempt) => Boolean(attempt.error))) {
      redactions.push({ field_path: "attempts.safe_error", reason_code: "technical_error_redacted" });
    }
    if (containsSensitiveText(task.metadata.original_message) || containsSensitiveText(presentation.markdown)) {
      redactions.push({ field_path: "task.title", reason_code: "sensitive_text_redacted" });
    }
    const planArtifact = this.ports.artifacts.latest(task.task_run_id, "execution_plan");
    const plan = planProjection(planArtifact);
    if (planArtifact !== undefined && plan === null) reasons.push("execution_plan_projection_invalid");
    const actions = presentation.actions.slice(0, 16).map(actionCapability);
    if (
      task.status === "waiting_for_query_approval" &&
      !actions.some((action) => action.action_type === "approve_query")
    ) {
      actions.push({
        schema_version: 1,
        projection_type: "action_capability_v1",
        task_run_id: task.task_run_id,
        action_type: "approve_query",
        label: "审核材料不可用",
        availability: "disabled",
        reason_code: "query_review_incomplete",
        requires_confirmation: true,
        requires_idempotency_key: true,
      });
    }
    const allChildren = this.ports.tasks.listChildren(task.task_run_id, 101);
    const scopedChildren = allChildren.filter((child) =>
      child.org_id === task.org_id && child.team_id === task.team_id && child.user_id === task.user_id);
    if (allChildren.length !== scopedChildren.length) reasons.push("cross_scope_child_omitted");
    const children = scopedChildren.slice(0, 100);
    if (scopedChildren.length > children.length) reasons.push("child_task_projection_truncated");
    const detail: TaskDetailProjectionV1 = {
      schema_version: 1,
      projection_type: "task_detail_projection_v1",
      scope: productScope(task),
      task: taskSummary(task),
      plan,
      review_request: reviewProjection(events),
      presentation: boundedPresentation(presentation, allowedArtifactIds),
      actions,
      activity: events.slice(-TASK_ACTIVITY_LIMIT).map((event) => ({
        sequence: event.sequence,
        event_type: event.event_type,
        title: activityTitle(event.event_type),
        detail: null,
        state: activityState(event),
        created_at: event.created_at,
      })),
      attempts: attempts.map((attempt) => {
        const elapsed = Date.parse(attempt.finished_at ?? attempt.updated_at) - Date.parse(attempt.started_at);
        if (!Number.isFinite(elapsed)) reasons.push("attempt_time_projection_invalid");
        return {
          attempt_id: attempt.attempt_id,
          stage: attempt.stage.slice(0, 256),
          status: attempt.status,
          started_at: attempt.started_at,
          finished_at: attempt.finished_at,
          elapsed_ms: Number.isFinite(elapsed) ? Math.max(0, elapsed) : 0,
          safe_error: attempt.error ? "该阶段未完成；技术详情已省略。" : null,
        };
      }),
      artifacts: artifacts.map((artifact) => ({
        artifact_id: artifact.artifact_id,
        artifact_type: artifact.artifact_type,
        producer: artifact.producer.slice(0, 256),
        title: artifactTitle(artifact.artifact_type),
        state: "ready",
        created_at: artifact.created_at,
        evidence_refs: [...evidenceRefs(artifact.payload)].slice(0, 1_000),
        href: artifactHref(task.task_run_id, artifact),
      })),
      relations: {
        parent_task_run_id: task.parent_task_run_id,
        child_task_run_ids: children.map((child) => child.task_run_id),
      },
      projection_meta: {
        availability: reasons.length > 0 ? "partial" : "ready",
        generated_at: this.#now(),
        source_revisions: [
          sourceRevision("pi_task_store", task.updated_at),
          sourceRevision("pi_event_store", `${task.task_run_id}:${events.at(-1)?.sequence ?? 0}`),
          sourceRevision("pi_artifact_store", `${task.task_run_id}:${allArtifacts.length}`),
          sourceRevision("pi_attempt_store", `${task.task_run_id}:${allAttempts.length}`),
        ],
        unavailable_reasons: [...new Set(reasons)],
        redactions,
      },
    };
    assertProjection("task_detail_projection_v1", detail);
    return detail;
  }

  #conversationSummary(group: ConversationTaskGroup): ConversationSummaryV1 {
    const first = this.ports.tasks.get(group.firstTaskRunId);
    const latest = this.ports.tasks.get(group.latestTaskRunId);
    if (first === undefined || latest === undefined) {
      throw new TaskStateError("Conversation references a missing TaskRun");
    }
    const events = this.ports.events.list(latest.task_run_id);
    const artifacts = this.ports.artifacts.list(latest.task_run_id);
    const presentation = renderChannelPresentation({ task: latest, events, artifacts });
    const summaryRedactions: ConversationSummaryV1["projection_meta"]["redactions"] = [];
    if (containsSensitiveText(first.metadata.original_message)) {
      summaryRedactions.push({ field_path: "title", reason_code: "sensitive_text_redacted" });
    }
    if (containsSensitiveText(presentation.markdown)) {
      summaryRedactions.push({
        field_path: "latest_message_preview",
        reason_code: "sensitive_text_redacted",
      });
    }
    const summary: ConversationSummaryV1 = {
      schema_version: 1,
      projection_type: "conversation_summary_v1",
      scope: productScope(latest),
      conversation_id: group.conversationId,
      title: titleFromTask(first),
      display_state: taskDisplayStateForStatus(latest.status),
      task_count: group.taskCount,
      latest_task_run_id: latest.task_run_id,
      latest_message_preview: preview(presentation.markdown || titleFromTask(latest)),
      started_at: group.startedAt,
      updated_at: group.updatedAt,
      href: `/chat?conversation=${encodeURIComponent(group.conversationId)}`,
      projection_meta: {
        availability: "ready",
        generated_at: this.#now(),
        source_revisions: [
          sourceRevision("pi_task_store", group.updatedAt),
          sourceRevision("pi_event_store", `${latest.task_run_id}:${events.at(-1)?.sequence ?? 0}`),
          sourceRevision("pi_artifact_store", `${latest.task_run_id}:${artifacts.length}`),
        ],
        unavailable_reasons: [],
        redactions: summaryRedactions,
      },
    };
    return summary;
  }

  #now(): string {
    return (this.ports.now?.() ?? new Date()).toISOString();
  }
}
