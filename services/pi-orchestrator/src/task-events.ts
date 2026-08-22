import { randomUUID } from "node:crypto";

export type TaskEventType =
  | "task.created"
  | "task.status_changed"
  | "plan.created"
  | "plan.revised"
  | "channel.response_created"
  | "query.review_requested"
  | "query.clarification_requested"
  | "query.prepare_failed"
  | "query.prepare_timed_out"
  | "query.approval_submitted"
  | "query.completed"
  | "query.execution_failed"
  | "artifact.created"
  | "skill.execution_failed"
  | "skill.started"
  | "skill.completed"
  | "skill.failed"
  | "analysis.completed"
  | "report.completed"
  | "analysis.supplement_created"
  | "analysis.supplement_consumed"
  | "stage.attempt_started"
  | "stage.attempt_succeeded"
  | "stage.attempt_failed"
  | "stage.attempt_timed_out"
  | "stage.attempt_interrupted";

export interface TaskEvent {
  event_id: string;
  task_run_id: string;
  sequence: number;
  event_type: TaskEventType;
  created_at: string;
  payload: Record<string, unknown>;
}

export interface TaskEventStore {
  append(
    taskRunId: string,
    eventType: TaskEventType,
    payload: Record<string, unknown>,
  ): TaskEvent;
  list(taskRunId: string, afterSequence?: number): TaskEvent[];
}

export class InMemoryTaskEventStore implements TaskEventStore {
  readonly #events = new Map<string, TaskEvent[]>();

  append(
    taskRunId: string,
    eventType: TaskEventType,
    payload: Record<string, unknown>,
  ): TaskEvent {
    const events = this.#events.get(taskRunId) ?? [];
    const event: TaskEvent = {
      event_id: `te_${randomUUID().replaceAll("-", "")}`,
      task_run_id: taskRunId,
      sequence: events.length + 1,
      event_type: eventType,
      created_at: new Date().toISOString(),
      payload: structuredClone(payload),
    };
    events.push(event);
    this.#events.set(taskRunId, events);
    return structuredClone(event);
  }

  list(taskRunId: string, afterSequence = 0): TaskEvent[] {
    return (this.#events.get(taskRunId) ?? [])
      .filter((event) => event.sequence > afterSequence)
      .map((event) => structuredClone(event));
  }
}
