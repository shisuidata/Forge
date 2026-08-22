import type { Artifact } from "./artifacts.js";
import type { ChannelIntentRoute } from "./channels/intent.js";

export const PLAN_CAPABILITIES = [
  "clarification",
  "context",
  "query",
  "analysis",
  "chart",
  "report",
  "registry_draft",
  "memory_proposal",
  "approval",
] as const;
export type PlanCapability = (typeof PLAN_CAPABILITIES)[number];

export const PLAN_STEP_STATUSES = [
  "pending",
  "ready",
  "running",
  "waiting_approval",
  "completed",
  "failed",
  "skipped",
] as const;
export type PlanStepStatus = (typeof PLAN_STEP_STATUSES)[number];

export interface ExecutionPlanStep {
  step_id: string;
  capability: PlanCapability;
  title: string;
  depends_on: string[];
  required: boolean;
  status: PlanStepStatus;
  deliverable: string | null;
}

export interface ExecutionPlanPayload extends Record<string, unknown> {
  plan_revision: number;
  supersedes_artifact_id: string | null;
  route_kind: ChannelIntentRoute["kind"];
  goal: string;
  required_deliverables: string[];
  status: "active" | "completed" | "failed";
  steps: ExecutionPlanStep[];
}

const CAPABILITY_SET = new Set<string>(PLAN_CAPABILITIES);
const STEP_STATUS_SET = new Set<string>(PLAN_STEP_STATUSES);
const ROUTE_SET = new Set<string>([
  "query", "knowledge", "conversation", "action", "workflow", "clarification", "forbidden",
]);
const PLAN_KEYS = new Set([
  "plan_revision", "supersedes_artifact_id", "route_kind", "goal",
  "required_deliverables", "status", "steps",
]);
const STEP_KEYS = new Set([
  "step_id", "capability", "title", "depends_on", "required", "status", "deliverable",
]);

function exactKeys(value: Record<string, unknown>, allowed: Set<string>): boolean {
  return Object.keys(value).every((key) => allowed.has(key));
}

export function validateExecutionPlanPayload(value: unknown): string | undefined {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return "payload must be an object";
  const payload = value as Record<string, unknown>;
  if (!exactKeys(payload, PLAN_KEYS)) return "payload contains unsupported fields";
  if (!Number.isInteger(payload.plan_revision) || Number(payload.plan_revision) < 1) {
    return "plan_revision must be a positive integer";
  }
  if (payload.supersedes_artifact_id !== null &&
      (typeof payload.supersedes_artifact_id !== "string" || !/^ar_[A-Za-z0-9_-]+$/.test(payload.supersedes_artifact_id))) {
    return "supersedes_artifact_id must be null or an Artifact id";
  }
  if (typeof payload.route_kind !== "string" || !ROUTE_SET.has(payload.route_kind)) return "route_kind is invalid";
  if (typeof payload.goal !== "string" || payload.goal.trim().length === 0 || payload.goal.length > 2_000) {
    return "goal must be a non-empty bounded string";
  }
  if (!Array.isArray(payload.required_deliverables) ||
      payload.required_deliverables.some((item) => typeof item !== "string" || item.length === 0)) {
    return "required_deliverables must be strings";
  }
  if (!new Set(["active", "completed", "failed"]).has(String(payload.status))) return "plan status is invalid";
  if (!Array.isArray(payload.steps) || payload.steps.length === 0 || payload.steps.length > 12) {
    return "steps must contain 1-12 items";
  }
  const ids = new Set<string>();
  for (const raw of payload.steps) {
    if (typeof raw !== "object" || raw === null || Array.isArray(raw)) return "plan step must be an object";
    const step = raw as Record<string, unknown>;
    if (!exactKeys(step, STEP_KEYS)) return "plan step contains unsupported fields";
    if (typeof step.step_id !== "string" || !/^step_[a-z0-9_]{1,48}$/.test(step.step_id) || ids.has(step.step_id)) {
      return "plan step_id must be unique and valid";
    }
    ids.add(step.step_id);
    if (typeof step.capability !== "string" || !CAPABILITY_SET.has(step.capability)) return "plan capability is invalid";
    if (typeof step.title !== "string" || step.title.length === 0 || step.title.length > 200) return "plan title is invalid";
    if (!Array.isArray(step.depends_on) || step.depends_on.some((item) => typeof item !== "string")) {
      return "plan dependencies must be strings";
    }
    if (typeof step.required !== "boolean") return "plan required must be boolean";
    if (typeof step.status !== "string" || !STEP_STATUS_SET.has(step.status)) return "plan step status is invalid";
    if (step.deliverable !== null && (typeof step.deliverable !== "string" || step.deliverable.length === 0)) {
      return "plan deliverable must be null or a string";
    }
  }
  for (const raw of payload.steps) {
    const step = raw as unknown as ExecutionPlanStep;
    if (step.depends_on.some((dependency) => !ids.has(dependency) || dependency === step.step_id)) {
      return "plan dependency is missing or cyclic at one step";
    }
  }
  return undefined;
}

function step(
  stepId: string,
  capability: PlanCapability,
  title: string,
  dependsOn: string[],
  deliverable: string | null,
  required = true,
): ExecutionPlanStep {
  return {
    step_id: stepId,
    capability,
    title,
    depends_on: dependsOn,
    required,
    status: dependsOn.length === 0 ? "ready" : "pending",
    deliverable,
  };
}

export function buildExecutionPlan(route: ChannelIntentRoute, goal: string): ExecutionPlanPayload {
  let steps: ExecutionPlanStep[];
  if (route.kind === "conversation") {
    steps = [step("step_respond", "context", "直接响应", [], "channel_response")];
  } else if (route.kind === "knowledge") {
    steps = [
      step("step_context", "context", "检索可信上下文", [], "context_evidence"),
      step("step_answer", "report", "生成证据绑定回答", ["step_context"], "advisory"),
    ];
  } else if (route.kind === "clarification") {
    steps = [step("step_clarify", "clarification", "澄清目标与范围", [], "clarification")];
  } else if (route.kind === "action") {
    const capability: PlanCapability = route.action === "memory" ? "memory_proposal" : "registry_draft";
    steps = [
      step("step_draft", capability, "生成待审核变更", [], `${route.action ?? "action"}_draft`),
      step("step_approve", "approval", "等待用户确认", ["step_draft"], "approval"),
    ];
  } else if (route.kind === "forbidden") {
    steps = [step("step_explain", "context", "说明能力或权限边界", [], "channel_response")];
  } else {
    steps = [
      step("step_query", "query", "准备并审批查询", [], "query_result"),
      step("step_analyze", "analysis", "分析查询结果", ["step_query"], "analysis"),
    ];
    if (route.requested_deliverables.includes("chart")) {
      steps.push(step("step_chart", "chart", "生成确定性图表", ["step_query"], "chart"));
    }
    steps.push(step(
      "step_report",
      "report",
      "生成业务报告",
      ["step_analyze", ...(route.requested_deliverables.includes("chart") ? ["step_chart"] : [])],
      "rendered_output",
    ));
  }
  return {
    plan_revision: 1,
    supersedes_artifact_id: null,
    route_kind: route.kind,
    goal: goal.slice(0, 2_000),
    required_deliverables: steps
      .filter((item) => item.required && item.deliverable !== null)
      .map((item) => item.deliverable as string),
    status: "active",
    steps,
  };
}

export function reviseExecutionPlan(
  artifact: Artifact,
  updates: Partial<Record<PlanCapability, PlanStepStatus>>,
): ExecutionPlanPayload {
  const current = artifact.payload as ExecutionPlanPayload;
  const steps = current.steps.map((item) => {
    const status = updates[item.capability] ?? item.status;
    return { ...item, status };
  });
  const completedIds = new Set(
    steps.filter((item) => item.status === "completed" || item.status === "skipped").map((item) => item.step_id),
  );
  const unlocked = steps.map((item) =>
    item.status === "pending" && item.depends_on.every((dependency) => completedIds.has(dependency))
      ? { ...item, status: "ready" as const }
      : item,
  );
  const required = unlocked.filter((item) => item.required);
  return {
    ...current,
    plan_revision: current.plan_revision + 1,
    supersedes_artifact_id: artifact.artifact_id,
    status: required.every((item) => item.status === "completed" || item.status === "skipped")
      ? "completed"
      : required.some((item) => item.status === "failed") ? "failed" : "active",
    steps: unlocked,
  };
}
