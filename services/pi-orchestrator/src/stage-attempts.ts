import { randomUUID } from "node:crypto";

import { TaskStateError, type TaskStatus } from "./task-store.js";

export const STAGE_ATTEMPT_STATUSES = [
  "running",
  "succeeded",
  "failed",
  "timed_out",
  "interrupted",
] as const;

export type StageAttemptStatus = (typeof STAGE_ATTEMPT_STATUSES)[number];

export interface StageAttempt {
  attempt_id: string;
  task_run_id: string;
  stage: string;
  status: StageAttemptStatus;
  attempt_number: number;
  idempotency_key: string;
  running_status: TaskStatus;
  retry_status: TaskStatus;
  lease_expires_at: string;
  started_at: string;
  updated_at: string;
  finished_at: string | null;
  error: string | null;
  model_revision: string | null;
  skill_policy_version: number;
}

export interface StartStageAttemptInput {
  taskRunId: string;
  stage: string;
  idempotencyKey: string;
  runningStatus: TaskStatus;
  retryStatus: TaskStatus;
  leaseMs: number;
  modelRevision?: string | null;
  skillPolicyVersion?: number;
}

export interface StageAttemptStore {
  start(input: StartStageAttemptInput): StageAttempt;
  get(attemptId: string): StageAttempt | undefined;
  findByIdempotencyKey(taskRunId: string, idempotencyKey: string): StageAttempt | undefined;
  list(taskRunId: string): StageAttempt[];
  finish(
    attemptId: string,
    status: Exclude<StageAttemptStatus, "running" | "interrupted">,
    error?: string,
  ): StageAttempt;
}

export class InMemoryStageAttemptStore implements StageAttemptStore {
  readonly #attempts = new Map<string, StageAttempt[]>();

  start(input: StartStageAttemptInput): StageAttempt {
    const attempts = this.#attempts.get(input.taskRunId) ?? [];
    const existing = attempts.find(
      (attempt) => attempt.idempotency_key === input.idempotencyKey,
    );
    if (existing !== undefined) return structuredClone(existing);
    if (attempts.some((attempt) => attempt.status === "running")) {
      throw new TaskStateError("TaskRun already has a running StageAttempt");
    }
    const now = new Date();
    const attempt: StageAttempt = {
      attempt_id: `sa_${randomUUID().replaceAll("-", "")}`,
      task_run_id: input.taskRunId,
      stage: input.stage,
      status: "running",
      attempt_number: attempts.length + 1,
      idempotency_key: input.idempotencyKey,
      running_status: input.runningStatus,
      retry_status: input.retryStatus,
      lease_expires_at: new Date(now.getTime() + input.leaseMs).toISOString(),
      started_at: now.toISOString(),
      updated_at: now.toISOString(),
      finished_at: null,
      error: null,
      model_revision: input.modelRevision ?? null,
      skill_policy_version: input.skillPolicyVersion ?? 0,
    };
    attempts.push(attempt);
    this.#attempts.set(input.taskRunId, attempts);
    return structuredClone(attempt);
  }

  get(attemptId: string): StageAttempt | undefined {
    for (const attempts of this.#attempts.values()) {
      const attempt = attempts.find((candidate) => candidate.attempt_id === attemptId);
      if (attempt !== undefined) return structuredClone(attempt);
    }
    return undefined;
  }

  findByIdempotencyKey(taskRunId: string, idempotencyKey: string): StageAttempt | undefined {
    const attempt = (this.#attempts.get(taskRunId) ?? []).find(
      (candidate) => candidate.idempotency_key === idempotencyKey,
    );
    return attempt === undefined ? undefined : structuredClone(attempt);
  }

  list(taskRunId: string): StageAttempt[] {
    return (this.#attempts.get(taskRunId) ?? []).map((attempt) => structuredClone(attempt));
  }

  finish(
    attemptId: string,
    status: Exclude<StageAttemptStatus, "running" | "interrupted">,
    error?: string,
  ): StageAttempt {
    for (const attempts of this.#attempts.values()) {
      const index = attempts.findIndex((attempt) => attempt.attempt_id === attemptId);
      if (index < 0) continue;
      const attempt = attempts[index];
      if (attempt === undefined) continue;
      if (attempt.status !== "running") {
        if (attempt.status === status) return structuredClone(attempt);
        throw new TaskStateError(`StageAttempt is already terminal: ${attempt.status}`);
      }
      const now = new Date().toISOString();
      const updated: StageAttempt = {
        ...attempt,
        status,
        updated_at: now,
        finished_at: now,
        error: error?.slice(0, 2_000) ?? null,
      };
      attempts[index] = updated;
      return structuredClone(updated);
    }
    throw new TaskStateError(`StageAttempt not found: ${attemptId}`);
  }
}
