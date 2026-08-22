import { randomUUID } from "node:crypto";

export const TASK_STATUSES = [
  "created",
  "clarifying",
  "ready_for_query",
  "waiting_for_query_approval",
  "waiting_for_action_approval",
  "querying",
  "ready_for_analysis",
  "analyzing",
  "ready_for_report",
  "rendering",
  "completed",
  "needs_input",
  "incomplete",
  "cancelled",
  "failed",
  "expired",
] as const;

export type TaskStatus = (typeof TASK_STATUSES)[number];
export type TaskChannel = "web" | "feishu" | "dingtalk" | "api";

export interface TaskRun {
  task_run_id: string;
  org_id: string;
  team_id: string;
  user_id: string;
  channel: TaskChannel;
  channel_conversation_id: string | null;
  intent: string;
  status: TaskStatus;
  current_stage: string | null;
  correlation_id: string | null;
  parent_task_run_id: string | null;
  created_at: string;
  updated_at: string;
  metadata: Record<string, unknown>;
}

export interface CreateTaskInput {
  org_id: string;
  team_id: string;
  user_id: string;
  channel: TaskChannel;
  channel_conversation_id?: string | null;
  intent: string;
  correlation_id?: string | null;
  parent_task_run_id?: string | null;
  metadata?: Record<string, unknown>;
}

const TERMINAL_STATUSES = new Set<TaskStatus>([
  "completed",
  "cancelled",
  "failed",
  "expired",
]);

export class TaskStateError extends Error {}

export interface TaskStore {
  create(input: CreateTaskInput): TaskRun;
  get(taskRunId: string): TaskRun | undefined;
  transition(options: {
    taskRunId: string;
    expectedStatus: TaskStatus;
    status: TaskStatus;
    currentStage: string | null;
  }): TaskRun;
}

export class InMemoryTaskStore implements TaskStore {
  readonly #tasks = new Map<string, TaskRun>();

  create(input: CreateTaskInput): TaskRun {
    for (const [field, value] of Object.entries({
      org_id: input.org_id,
      team_id: input.team_id,
      user_id: input.user_id,
      intent: input.intent,
    })) {
      if (value.trim().length === 0) {
        throw new TaskStateError(`${field} must not be empty`);
      }
    }

    const now = new Date().toISOString();
    const task: TaskRun = {
      task_run_id: `tr_${randomUUID().replaceAll("-", "")}`,
      org_id: input.org_id,
      team_id: input.team_id,
      user_id: input.user_id,
      channel: input.channel,
      channel_conversation_id: input.channel_conversation_id ?? null,
      intent: input.intent,
      status: "created",
      current_stage: null,
      correlation_id: input.correlation_id ?? null,
      parent_task_run_id: input.parent_task_run_id ?? null,
      created_at: now,
      updated_at: now,
      metadata: structuredClone(input.metadata ?? {}),
    };
    this.#tasks.set(task.task_run_id, task);
    return structuredClone(task);
  }

  get(taskRunId: string): TaskRun | undefined {
    const task = this.#tasks.get(taskRunId);
    return task === undefined ? undefined : structuredClone(task);
  }

  transition(options: {
    taskRunId: string;
    expectedStatus: TaskStatus;
    status: TaskStatus;
    currentStage: string | null;
  }): TaskRun {
    const task = this.#tasks.get(options.taskRunId);
    if (task === undefined) {
      throw new TaskStateError(`TaskRun not found: ${options.taskRunId}`);
    }
    if (task.status !== options.expectedStatus) {
      throw new TaskStateError(
        `TaskRun status conflict: expected=${options.expectedStatus}, actual=${task.status}`,
      );
    }
    if (TERMINAL_STATUSES.has(task.status)) {
      throw new TaskStateError(`Terminal TaskRun cannot transition: ${task.status}`);
    }

    const updated: TaskRun = {
      ...task,
      status: options.status,
      current_stage: options.currentStage,
      updated_at: new Date().toISOString(),
    };
    this.#tasks.set(updated.task_run_id, updated);
    return structuredClone(updated);
  }
}
