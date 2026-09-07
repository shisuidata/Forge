import { InMemoryTaskStore, type TaskStore } from "./task-store.js";
import { InMemoryTaskEventStore, type TaskEventStore } from "./task-events.js";
import { InMemoryArtifactStore, type ArtifactStore } from "./artifacts.js";
import { InMemoryStageAttemptStore, type StageAttemptStore, type StageAttempt } from "./stage-attempts.js";
import { settleExecutionPlan } from "./planning.js";
import { InMemoryChannelEventStore, type ChannelEventStore } from "./channels/event-store.js";
import { InMemorySkillPolicyStore, type SkillPolicyStore } from "./skill-policy.js";
import { AUTHORIZED_SKILL_NAMES } from "./skills.js";
import { TaskStateError } from "./task-store.js";

export interface StateTransactionPort {
  run<T>(operation: () => T): T;
}

/** Stores and their transaction boundary are one adapter, never independently injected. */
export abstract class OrchestratorState {
  declare private readonly stateAdapter: void;
  abstract readonly tasks: TaskStore;
  abstract readonly events: TaskEventStore;
  abstract readonly artifacts: ArtifactStore;
  abstract readonly attempts: StageAttemptStore;
  abstract readonly transactions: StateTransactionPort;
  abstract readonly channelEvents: ChannelEventStore;
  abstract readonly skillPolicies: SkillPolicyStore;

  reconcileExpiredAttempts(now = new Date()): StageAttempt[] {
    return this.transactions.run(() => {
      const interrupted: StageAttempt[] = [];
      for (const attempt of this.attempts.listExpired(now)) {
        const finished = this.attempts.finish(attempt.attempt_id, "interrupted", "Stage lease expired before completion");
        const task = this.tasks.get(attempt.task_run_id);
        let recovered = false;
        if (task?.status === attempt.running_status) {
          this.tasks.transition({ taskRunId: task.task_run_id, expectedStatus: task.status,
            status: attempt.retry_status, currentStage: `${attempt.stage}_retry` });
          this.events.append(task.task_run_id, "task.status_changed", { from: task.status,
            to: attempt.retry_status, current_stage: `${attempt.stage}_retry`, recovery: true });
          recovered = true;
        }
        this.events.append(attempt.task_run_id, "stage.attempt_interrupted", { attempt_id: attempt.attempt_id,
          stage: attempt.stage, retry_status: attempt.retry_status, task_recovered: recovered,
          request_id: attempt.request_id ?? null });
        const current = this.tasks.get(attempt.task_run_id);
        if (current !== undefined) settleExecutionPlan(this, current);
        interrupted.push(finished);
      }
      return interrupted;
    });
  }
}

export class InMemoryOrchestratorState extends OrchestratorState {
  readonly tasks = new InMemoryTaskStore();
  readonly events = new InMemoryTaskEventStore((taskRunId) => this.#assertTask(taskRunId));
  readonly artifacts = new InMemoryArtifactStore((taskRunId) => this.#assertTask(taskRunId));
  readonly attempts = new InMemoryStageAttemptStore((taskRunId) => this.#assertTask(taskRunId));
  readonly channelEvents = new InMemoryChannelEventStore((taskRunId) => this.#assertTask(taskRunId));
  readonly skillPolicies = new InMemorySkillPolicyStore(AUTHORIZED_SKILL_NAMES);

  #assertTask(taskRunId: string): void {
    if (this.tasks.get(taskRunId) === undefined) throw new TaskStateError(`TaskRun not found: ${taskRunId}`);
  }

  readonly transactions: StateTransactionPort = {
    run: <T>(operation: () => T): T => {
      const restore = [this.tasks, this.events, this.artifacts, this.attempts, this.channelEvents, this.skillPolicies].map(
        (store) => store.checkpoint(),
      );
      try {
        const result = operation();
        if (result instanceof Promise) throw new TypeError("State transactions must be synchronous");
        return result;
      } catch (error) {
        for (const rollback of restore) rollback();
        throw error;
      }
    },
  };
}
