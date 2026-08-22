import type { Artifact } from "../artifacts.js";
import type { TaskEvent } from "../task-events.js";
import type { TaskChannel, TaskRun } from "../task-store.js";

export const CHANNEL_EVENT_TYPES = ["message", "action"] as const;
export type ChannelEventType = (typeof CHANNEL_EVENT_TYPES)[number];

export const CHANNEL_ACTION_TYPES = [
  "provide_input",
  "approve_query",
  "cancel_task",
  "request_supplement",
  "analyze",
  "render_report",
  "confirm_memory",
] as const;
export type ChannelActionType = (typeof CHANNEL_ACTION_TYPES)[number];

export interface ChannelEventInput {
  event_id: string;
  channel: Exclude<TaskChannel, "web" | "api">;
  event_type: ChannelEventType;
  external_user_id: string;
  conversation_id: string;
  message_id: string;
  task_run_id: string | null;
  payload: Record<string, unknown>;
}

export interface ChannelIdentity {
  org_id: string;
  team_id: string;
  user_id: string;
}

export interface ChannelAction {
  type: ChannelActionType;
  label: string;
  task_run_id: string;
  payload: Record<string, unknown>;
  style: "primary" | "default" | "danger";
}

export type ChannelPresentationKind =
  | "progress"
  | "needs_input"
  | "query_review"
  | "query_result"
  | "analysis"
  | "report"
  | "error";

export interface ChannelPresentation {
  kind: ChannelPresentationKind;
  task_run_id: string;
  title: string;
  markdown: string;
  fields: Array<{ label: string; value: string }>;
  table: { columns: string[]; rows: unknown[][]; truncated: boolean } | null;
  actions: ChannelAction[];
  source_event_sequence: number;
  source_artifact_ids: string[];
}

export interface ChannelRenderInput {
  task: TaskRun;
  events: TaskEvent[];
  artifacts: Artifact[];
}

function requiredString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${field} must be a non-empty string`);
  }
  return value;
}

export function parseChannelEvent(value: unknown): ChannelEventInput {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("ChannelEvent must be an object");
  }
  const input = value as Record<string, unknown>;
  const channel = requiredString(input.channel, "channel");
  if (channel !== "feishu" && channel !== "dingtalk") {
    throw new Error(`Unsupported channel: ${channel}`);
  }
  const eventType = requiredString(input.event_type, "event_type");
  if (!CHANNEL_EVENT_TYPES.includes(eventType as ChannelEventType)) {
    throw new Error(`Unsupported event_type: ${eventType}`);
  }
  if (typeof input.payload !== "object" || input.payload === null || Array.isArray(input.payload)) {
    throw new Error("payload must be an object");
  }
  const taskRunId = input.task_run_id;
  if (taskRunId !== null && taskRunId !== undefined && typeof taskRunId !== "string") {
    throw new Error("task_run_id must be a string or null");
  }
  return {
    event_id: requiredString(input.event_id, "event_id"),
    channel,
    event_type: eventType as ChannelEventType,
    external_user_id: requiredString(input.external_user_id, "external_user_id"),
    conversation_id: requiredString(input.conversation_id, "conversation_id"),
    message_id: requiredString(input.message_id, "message_id"),
    task_run_id: taskRunId ?? null,
    payload: structuredClone(input.payload as Record<string, unknown>),
  };
}
