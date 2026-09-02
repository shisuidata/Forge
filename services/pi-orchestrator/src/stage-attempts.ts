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

export const STAGE_PROGRESS_PHASES = [
  "waiting_for_model",
  "model_responding",
  "artifact_submitted",
] as const;
export type StageProgressPhase = (typeof STAGE_PROGRESS_PHASES)[number];

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
  deadline_at: string | null;
  started_at: string;
  updated_at: string;
  finished_at: string | null;
  error: string | null;
  progress_phase: StageProgressPhase;
  first_model_activity_at: string | null;
  tool_submitted_at: string | null;
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
  timeoutMs?: number;
  modelRevision?: string | null;
  skillPolicyVersion?: number;
}

export interface StageAttemptStore {
  start(input: StartStageAttemptInput): StageAttempt;
  get(attemptId: string): StageAttempt | undefined;
  findByIdempotencyKey(taskRunId: string, idempotencyKey: string): StageAttempt | undefined;
  list(taskRunId: string): StageAttempt[];
  markProgress(attemptId: string, phase: Exclude<StageProgressPhase, "waiting_for_model">): StageAttempt;
  finish(
    attemptId: string,
    status: Exclude<StageAttemptStatus, "running" | "interrupted">,
    error?: string,
  ): StageAttempt;
}

export class InMemoryStageAttemptStore implements StageAttemptStore {
  readonly #attempts = new Map<string, StageAttempt[]>();

  start(input: StartStageAttemptInput): StageAttempt {
    if (input.timeoutMs !== undefined && (
      !Number.isInteger(input.timeoutMs) || input.timeoutMs < 1 || input.timeoutMs >= input.leaseMs
    )) {
      throw new TaskStateError("Stage timeout must be positive and shorter than its lease");
    }
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
      deadline_at: input.timeoutMs === undefined
        ? null
        : new Date(now.getTime() + input.timeoutMs).toISOString(),
      started_at: now.toISOString(),
      updated_at: now.toISOString(),
      finished_at: null,
      error: null,
      progress_phase: "waiting_for_model",
      first_model_activity_at: null,
      tool_submitted_at: null,
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

  markProgress(
    attemptId: string,
    phase: Exclude<StageProgressPhase, "waiting_for_model">,
  ): StageAttempt {
    const order: Record<StageProgressPhase, number> = {
      waiting_for_model: 0,
      model_responding: 1,
      artifact_submitted: 2,
    };
    for (const attempts of this.#attempts.values()) {
      const index = attempts.findIndex((attempt) => attempt.attempt_id === attemptId);
      if (index < 0) continue;
      const attempt = attempts[index];
      if (attempt === undefined) continue;
      if (attempt.status !== "running" || order[phase] <= order[attempt.progress_phase]) {
        return structuredClone(attempt);
      }
      const now = new Date().toISOString();
      const updated: StageAttempt = {
        ...attempt,
        progress_phase: phase,
        updated_at: now,
        first_model_activity_at: attempt.first_model_activity_at ?? now,
        tool_submitted_at: phase === "artifact_submitted" ? now : attempt.tool_submitted_at,
      };
      attempts[index] = updated;
      return structuredClone(updated);
    }
    throw new TaskStateError(`StageAttempt not found: ${attemptId}`);
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
      const normalizedError = error?.trim();
      const updated: StageAttempt = {
        ...attempt,
        status,
        updated_at: now,
        finished_at: now,
        error: normalizedError ? normalizedError.slice(0, 2_000) : null,
      };
      attempts[index] = updated;
      return structuredClone(updated);
    }
    throw new TaskStateError(`StageAttempt not found: ${attemptId}`);
  }
}
