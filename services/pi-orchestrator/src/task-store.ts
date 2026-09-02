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

function conversationGroup(
  conversationId: string,
  tasks: TaskRun[],
  taskLimit: number,
  beforeTask?: ConversationTaskCursor,
): ConversationTaskGroup {
  if (!Number.isInteger(taskLimit) || taskLimit < 1) {
    throw new TaskStateError("Conversation task limit must be a positive integer");
  }
  const chronological = [...tasks].sort((left, right) =>
    left.created_at.localeCompare(right.created_at) ||
    left.task_run_id.localeCompare(right.task_run_id));
  const latest = [...tasks].sort((left, right) =>
    right.updated_at.localeCompare(left.updated_at) ||
    right.task_run_id.localeCompare(left.task_run_id))[0];
  const eligible = beforeTask === undefined
    ? chronological
    : chronological.filter((task) =>
        task.created_at < beforeTask.createdAt ||
        (task.created_at === beforeTask.createdAt && task.task_run_id < beforeTask.taskRunId));
  if (latest === undefined || chronological[0] === undefined) {
    throw new TaskStateError("Conversation cannot be built without TaskRuns");
  }
  return {
    conversationId,
    taskCount: tasks.length,
    startedAt: chronological[0].created_at,
    updatedAt: latest.updated_at,
    firstTaskRunId: chronological[0].task_run_id,
    latestTaskRunId: latest.task_run_id,
    tasks: eligible.slice(-taskLimit).map((task) => structuredClone(task)),
    tasksTruncated: eligible.length > taskLimit,
  };
}

export interface TaskListOptions {
  orgId: string;
  teamId: string;
  userId?: string;
  channel?: TaskChannel;
  status?: TaskStatus;
  limit: number;
}

export interface ConversationListCursor {
  updatedAt: string;
  conversationId: string;
}

export interface ConversationTaskGroup {
  conversationId: string;
  taskCount: number;
  startedAt: string;
  updatedAt: string;
  firstTaskRunId: string;
  latestTaskRunId: string;
  tasks: TaskRun[];
  tasksTruncated: boolean;
}

export interface ConversationListOptions {
  orgId: string;
  teamId: string;
  userId: string;
  channel: Exclude<TaskChannel, "api">;
  limit: number;
  taskLimit: number;
  includeTasks?: boolean;
  before?: ConversationListCursor;
}

export interface ConversationTaskCursor {
  createdAt: string;
  taskRunId: string;
}

export interface ConversationGetOptions {
  orgId: string;
  teamId: string;
  userId: string;
  channel: Exclude<TaskChannel, "api">;
  conversationId: string;
  taskLimit: number;
  beforeTask?: ConversationTaskCursor;
}

export interface TaskStore {
  create(input: CreateTaskInput): TaskRun;
  get(taskRunId: string): TaskRun | undefined;
  list(options: TaskListOptions): TaskRun[];
  listChildren(taskRunId: string, limit: number): TaskRun[];
  listConversations(options: ConversationListOptions): ConversationTaskGroup[];
  getConversation(options: ConversationGetOptions): ConversationTaskGroup | undefined;
  transition(options: {
    taskRunId: string;
    expectedStatus: TaskStatus;
    status: TaskStatus;
    currentStage: string | null;
  }): TaskRun;
}

export class InMemoryTaskStore implements TaskStore {
  readonly #tasks = new Map<string, TaskRun>();
  #lastCreatedAtMs = 0;

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

    const timestamp = Math.max(Date.now(), this.#lastCreatedAtMs + 1);
    this.#lastCreatedAtMs = timestamp;
    const now = new Date(timestamp).toISOString();
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

  list(options: TaskListOptions): TaskRun[] {
    return [...this.#tasks.values()]
      .filter((task) =>
        task.org_id === options.orgId &&
        task.team_id === options.teamId &&
        (options.userId === undefined || task.user_id === options.userId) &&
        (options.channel === undefined || task.channel === options.channel) &&
        (options.status === undefined || task.status === options.status)
      )
      .sort((left, right) =>
        right.updated_at.localeCompare(left.updated_at) ||
        right.task_run_id.localeCompare(left.task_run_id)
      )
      .slice(0, options.limit)
      .map((task) => structuredClone(task));
  }

  listChildren(taskRunId: string, limit: number): TaskRun[] {
    return [...this.#tasks.values()]
      .filter((task) => task.parent_task_run_id === taskRunId)
      .sort((left, right) =>
        left.created_at.localeCompare(right.created_at) ||
        left.task_run_id.localeCompare(right.task_run_id))
      .slice(0, limit)
      .map((task) => structuredClone(task));
  }

  listConversations(options: ConversationListOptions): ConversationTaskGroup[] {
    const groups = new Map<string, TaskRun[]>();
    for (const task of this.#tasks.values()) {
      if (
        task.org_id !== options.orgId ||
        task.team_id !== options.teamId ||
        task.user_id !== options.userId ||
        task.channel !== options.channel ||
        task.channel_conversation_id === null
      ) continue;
      const tasks = groups.get(task.channel_conversation_id) ?? [];
      tasks.push(task);
      groups.set(task.channel_conversation_id, tasks);
    }
    return [...groups.entries()]
      .map(([conversationId, tasks]) => conversationGroup(conversationId, tasks, options.taskLimit))
      .filter((group) => options.before === undefined ||
        group.updatedAt < options.before.updatedAt ||
        (group.updatedAt === options.before.updatedAt &&
          group.conversationId < options.before.conversationId))
      .sort((left, right) =>
        right.updatedAt.localeCompare(left.updatedAt) ||
        right.conversationId.localeCompare(left.conversationId))
      .slice(0, options.limit)
      .map((group) => structuredClone(options.includeTasks === false
        ? { ...group, tasks: [], tasksTruncated: group.taskCount > 0 }
        : group));
  }

  getConversation(options: ConversationGetOptions): ConversationTaskGroup | undefined {
    const tasks = [...this.#tasks.values()].filter((task) =>
      task.org_id === options.orgId &&
      task.team_id === options.teamId &&
      task.user_id === options.userId &&
      task.channel === options.channel &&
      task.channel_conversation_id === options.conversationId);
    return tasks.length === 0
      ? undefined
      : structuredClone(conversationGroup(
          options.conversationId,
          tasks,
          options.taskLimit,
          options.beforeTask,
        ));
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

    const updatedAtMs = Math.max(Date.now(), Date.parse(task.updated_at) + 1);
    this.#lastCreatedAtMs = Math.max(this.#lastCreatedAtMs, updatedAtMs);
    const updated: TaskRun = {
      ...task,
      status: options.status,
      current_stage: options.currentStage,
      updated_at: new Date(updatedAtMs).toISOString(),
    };
    this.#tasks.set(updated.task_run_id, updated);
    return structuredClone(updated);
  }
}
