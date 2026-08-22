import type { ChannelEventStore } from "./channels/event-store.js";
import type { ChannelEventInput, ChannelIdentity, ChannelPresentation } from "./channels/contracts.js";
import { renderChannelPresentation } from "./channels/renderer.js";
import { routeChannelMessage } from "./channels/intent.js";
import {
  buildExecutionPlan,
  reviseExecutionPlan,
  type PlanCapability,
  type PlanStepStatus,
} from "./planning.js";
import { buildChartPayload, bundleHash, type TechnicalReportPayload } from "./report-artifacts.js";
import {
  InMemoryArtifactStore,
  type Artifact,
  type ArtifactStore,
} from "./artifacts.js";
import { computePiModelRevision, type OrchestratorConfig } from "./config.js";
import type { ForgeDialect } from "./forge/client.js";
import {
  ForgeQueryRunClient,
  type ContextSearchResult,
  type ReportPublication,
  type QueryRunReview,
  type QueryRunResult,
} from "./forge/query-run-client.js";
import {
  PiStructuredSkillExecutor,
  type StructuredSkillExecutionPort,
} from "./skill-executor.js";
import type { StageAttempt, StageAttemptStore } from "./stage-attempts.js";
import type {
  AdvisoryPayload,
  AnalysisPayload,
  QueryResultPayload,
} from "./structured-artifact-tools.js";
import {
  AUTHORIZED_SKILL_NAMES,
  type AdvisorySkillName,
  type AuthorizedSkillName,
} from "./skills.js";
import { InMemorySkillPolicyStore, type SkillPolicyStore, type TeamSkillPolicy } from "./skill-policy.js";
import {
  InMemoryTaskStore,
  TaskStateError,
  type CreateTaskInput,
  type TaskRun,
  type TaskStore,
} from "./task-store.js";
import {
  InMemoryTaskEventStore,
  type TaskEvent,
  type TaskEventStore,
} from "./task-events.js";

export interface CreateDataTaskInput extends CreateTaskInput {
  message: string;
}

export interface StateTransactionPort {
  run<T>(operation: () => T): T;
}

export interface ForgeQueryRunPort {
  searchContext?(
    input: { orgId: string; teamId: string; userId: string; question: string; limit?: number },
    signal?: AbortSignal,
  ): Promise<ContextSearchResult>;
  createQueryRun(
    input: {
      taskRunId: string;
      orgId: string;
      teamId: string;
      userId: string;
      question: string;
      dialect?: ForgeDialect;
      idempotencyKey: string;
    },
    signal?: AbortSignal,
  ): Promise<QueryRunReview>;
  approveQueryRun(
    input: {
      queryRunId: string;
      approverUserId: string;
      sqlHash: string;
      assuranceReportHash: string;
      idempotencyKey: string;
    },
    signal?: AbortSignal,
  ): Promise<QueryRunResult>;
  cancelQueryRun?(
    input: { queryRunId: string; userId: string },
    signal?: AbortSignal,
  ): Promise<QueryRunReview>;
  createReport?(
    input: Record<string, unknown>,
    idempotencyKey: string,
    signal?: AbortSignal,
  ): Promise<ReportPublication>;
  getReport?(reportId: string, signal?: AbortSignal): Promise<ReportPublication>;
}

export class OrchestratorApplication {
  readonly #tasks: TaskStore;
  readonly #events: TaskEventStore;
  readonly #forge: ForgeQueryRunPort;
  readonly #skills: StructuredSkillExecutionPort;
  readonly #artifacts: ArtifactStore;
  readonly #transactions: StateTransactionPort;
  readonly #attempts: StageAttemptStore | undefined;
  readonly #stageTimeoutMs: number;
  readonly #stageLeaseMs: number;
  readonly #channelEvents: ChannelEventStore | undefined;
  readonly #config: OrchestratorConfig;
  readonly #skillPolicies: SkillPolicyStore;

  constructor(options: {
    config: OrchestratorConfig;
    tasks?: TaskStore;
    events?: TaskEventStore;
    artifacts?: ArtifactStore;
    transactions?: StateTransactionPort;
    attempts?: StageAttemptStore;
    channelEvents?: ChannelEventStore;
    skillPolicies?: SkillPolicyStore;
    forgeClient?: ForgeQueryRunPort;
    skillExecutor?: StructuredSkillExecutionPort;
  }) {
    this.#tasks = options.tasks ?? new InMemoryTaskStore();
    this.#events = options.events ?? new InMemoryTaskEventStore();
    this.#artifacts = options.artifacts ?? new InMemoryArtifactStore();
    this.#transactions = options.transactions ?? { run: (operation) => operation() };
    this.#attempts = options.attempts;
    this.#stageTimeoutMs = options.config.stageTimeoutMs;
    this.#stageLeaseMs = options.config.stageLeaseMs;
    this.#channelEvents = options.channelEvents;
    this.#config = options.config;
    this.#skillPolicies = options.skillPolicies ?? new InMemorySkillPolicyStore(AUTHORIZED_SKILL_NAMES);
    this.#skills =
      options.skillExecutor ?? new PiStructuredSkillExecutor({ config: options.config });
    this.#forge =
      options.forgeClient ??
      new ForgeQueryRunClient({
        baseUrl: options.config.forgeBaseUrl,
        timeoutMs: options.config.forgeTimeoutMs,
        ...(options.config.forgePiServiceKey === undefined
          ? {}
          : { serviceKey: options.config.forgePiServiceKey }),
      });
  }

  createTask(input: CreateDataTaskInput): { task: TaskRun; events: TaskEvent[] } {
    if (input.message.trim().length === 0) {
      throw new TaskStateError("message must not be empty");
    }
    return this.#transactions.run(() => {
      const task = this.#tasks.create({
        org_id: input.org_id,
        team_id: input.team_id,
        user_id: input.user_id,
        channel: input.channel,
        intent: input.intent,
        ...(input.channel_conversation_id === undefined
          ? {}
          : { channel_conversation_id: input.channel_conversation_id }),
        ...(input.correlation_id === undefined
          ? {}
          : { correlation_id: input.correlation_id }),
        ...(input.parent_task_run_id === undefined
          ? {}
          : { parent_task_run_id: input.parent_task_run_id }),
        metadata: {
          ...(input.metadata ?? {}),
          original_message: input.message,
        },
      });
      this.#events.append(task.task_run_id, "task.created", {
        status: task.status,
        intent: task.intent,
        channel: task.channel,
      });
      return { task, events: this.#events.list(task.task_run_id) };
    });
  }

  getTask(taskRunId: string): TaskRun | undefined {
    return this.#tasks.get(taskRunId);
  }

  getEvents(taskRunId: string, afterSequence = 0): TaskEvent[] {
    return this.#events.list(taskRunId, afterSequence);
  }

  getArtifacts(taskRunId: string): Artifact[] {
    return this.#artifacts.list(taskRunId);
  }

  getTeamSkillPolicy(orgId: string, teamId: string): TeamSkillPolicy | undefined {
    return this.#skillPolicies.get(orgId, teamId);
  }

  configureTeamSkills(input: {
    orgId: string;
    teamId: string;
    enabledSkills: AuthorizedSkillName[];
    expectedVersion: number;
    actor: string;
  }): TeamSkillPolicy {
    return this.#skillPolicies.configure(input);
  }

  async runAdvisory(
    taskRunId: string,
    input: {
      skillName: AdvisorySkillName;
      prompt: string;
      idempotencyKey?: string;
      contextEvidence?: ContextSearchResult["evidence"];
    },
    signal?: AbortSignal,
  ): Promise<{ task: TaskRun; artifact: Artifact<AdvisoryPayload> }> {
    const task = this.#tasks.get(taskRunId);
    if (task === undefined) throw new TaskStateError(`TaskRun not found: ${taskRunId}`);
    if (input.prompt.trim().length === 0) throw new TaskStateError("prompt must not be empty");
    if (this.#skills.advise === undefined) {
      throw new TaskStateError("Advisory Skill runtime is unavailable");
    }
    if (!this.#skillPolicies.isEnabled(task.org_id, task.team_id, input.skillName)) {
      throw new TaskStateError(`Skill is disabled for this team: ${input.skillName}`);
    }
    const attemptRound = (this.#attempts?.list(taskRunId) ?? []).filter(
      (attempt) => attempt.stage === `skill:${input.skillName}`,
    ).length + 1;
    const idempotencyKey = input.idempotencyKey ?? `${taskRunId}:skill:${input.skillName}:${attemptRound}`;
    const existingAttempt = this.#attempts?.findByIdempotencyKey(taskRunId, idempotencyKey);
    if (existingAttempt?.status === "succeeded") {
      const completedEvent = this.#events.list(taskRunId).find(
        (event) =>
          event.event_type === "skill.completed" &&
          event.payload.idempotency_key === idempotencyKey,
      );
      const artifactId = completedEvent?.payload.artifact_id;
      const artifact = this.#artifacts.list(taskRunId).find(
        (candidate) => candidate.artifact_id === artifactId && candidate.artifact_type === "advisory",
      ) as Artifact<AdvisoryPayload> | undefined;
      const current = this.#tasks.get(taskRunId);
      if (artifact !== undefined && current !== undefined) return { task: current, artifact };
      throw new TaskStateError("Idempotent AdvisoryArtifact is missing");
    }
    if (existingAttempt !== undefined) {
      throw new TaskStateError(
        `Advisory attempt already exists with status ${existingAttempt.status}; retry with a new idempotency key`,
      );
    }
    if (
      task.status !== "created" &&
      task.status !== "ready_for_analysis" &&
      task.status !== "incomplete"
    ) {
      throw new TaskStateError(
        `Advisory requires created, ready_for_analysis, or incomplete, got ${task.status}`,
      );
    }
    const retryStatus = task.status;
    let attempt: StageAttempt | undefined;
    const running = this.#transactions.run(() => {
      const next = this.#transition(task, "analyzing", `skill:${input.skillName}`);
      attempt = this.#startAttempt(
        next,
        `skill:${input.skillName}`,
        retryStatus,
        idempotencyKey,
      );
      this.#events.append(taskRunId, "skill.started", { skill_name: input.skillName });
      return next;
    });
    const stageExecution = this.#stageSignal(signal);
    try {
      const queryResults = this.#artifacts.list(taskRunId).filter(
        (artifact): artifact is Artifact<QueryResultPayload> => artifact.artifact_type === "query_result",
      );
      const payload = await this.#skills.advise(running, input.skillName, {
        prompt: input.prompt,
        queryResults,
        ...(input.contextEvidence === undefined ? {} : { contextEvidence: input.contextEvidence }),
      }, stageExecution.signal, attempt?.model_revision);
      if (stageExecution.timedOut()) throw new Error("Advisory Skill Stage timed out");
      return this.#transactions.run(() => {
        const artifact = this.#artifacts.create({
        artifactType: "advisory",
        taskRunId,
        producer: `pi-skill:${input.skillName}`,
        payload,
      });
        const completed = this.#transition(
          running,
          payload.status === "complete" ? "completed" : "incomplete",
          "skill_complete",
        );
        this.#finishAttempt(attempt, "succeeded");
        this.#events.append(taskRunId, "skill.completed", {
          skill_name: input.skillName,
          artifact_id: artifact.artifact_id,
          attempt_id: attempt?.attempt_id ?? null,
          idempotency_key: idempotencyKey,
          status: payload.status,
        });
        return { task: completed, artifact };
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown Skill error";
      this.#transactions.run(() => {
        if (stageExecution.timedOut()) {
          this.#finishAttempt(attempt, "timed_out", message);
          this.#transition(running, retryStatus, `skill:${input.skillName}:retry`);
        } else {
          this.#finishAttempt(attempt, "failed", message);
          this.#transition(running, "failed", `skill:${input.skillName}`);
        }
        this.#events.append(taskRunId, "skill.failed", {
          skill_name: input.skillName,
          timed_out: stageExecution.timedOut(),
          error: message,
        });
      });
      throw new TaskStateError(`Advisory Skill failed for ${taskRunId}`, { cause: error });
    }
  }

  getStageAttempts(taskRunId: string): StageAttempt[] {
    return this.#attempts?.list(taskRunId) ?? [];
  }

  getTaskRunIdForChannelEvent(
    channel: ChannelEventInput["channel"],
    eventId: string,
  ): string | null {
    return this.#channelEvents?.get(channel, eventId)?.task_run_id ?? null;
  }

  getChannelPresentation(taskRunId: string): ChannelPresentation {
    const task = this.#tasks.get(taskRunId);
    if (task === undefined) throw new TaskStateError(`TaskRun not found: ${taskRunId}`);
    return renderChannelPresentation({
      task,
      events: this.#events.list(taskRunId),
      artifacts: this.#artifacts.list(taskRunId),
    });
  }

  async ingestChannelMessage(
    event: ChannelEventInput,
    identity: ChannelIdentity,
    signal?: AbortSignal,
  ): Promise<{ task: TaskRun; presentation: ChannelPresentation; duplicate: boolean }> {
    if (this.#channelEvents === undefined) {
      throw new TaskStateError("ChannelEvent Store is not configured");
    }
    if (event.event_type !== "message") {
      throw new TaskStateError(`Unsupported ChannelEvent type: ${event.event_type}`);
    }
    const text = event.payload.text;
    if (typeof text !== "string" || text.trim().length === 0) {
      throw new TaskStateError("Channel message text must not be empty");
    }

    const route = routeChannelMessage(text);
    const claimed = this.#transactions.run(() => {
      const claim = this.#channelEvents?.claim(event);
      if (claim === undefined) throw new TaskStateError("ChannelEvent Store is not configured");
      if (!claim.created) return { record: claim.record, task: undefined };
      const created = this.createTask({
        org_id: identity.org_id,
        team_id: identity.team_id,
        user_id: identity.user_id,
        channel: event.channel,
        channel_conversation_id: event.conversation_id,
        intent: route.kind === "query"
          ? "business_root_cause_analysis"
          : route.kind === "knowledge"
            ? "knowledge_answer"
            : "channel_conversation",
        correlation_id: `${event.channel}:${event.event_id}`,
        message: text.trim(),
        metadata: {
          channel_event_id: event.event_id,
          channel_message_id: event.message_id,
          external_user_id: event.external_user_id,
        },
      });
      const plan = this.#artifacts.create({
        artifactType: "execution_plan",
        taskRunId: created.task.task_run_id,
        producer: "pi-planner",
        payload: buildExecutionPlan(route, text.trim()),
      });
      this.#events.append(created.task.task_run_id, "plan.created", {
        artifact_id: plan.artifact_id,
        plan_revision: 1,
        route_kind: route.kind,
      });
      this.#channelEvents?.complete(event.channel, event.event_id, created.task.task_run_id);
      return { record: claim.record, task: created.task };
    });

    if (claimed.task === undefined) {
      const taskRunId = claimed.record.task_run_id;
      if (taskRunId === null) {
        throw new TaskStateError("ChannelEvent is already being processed");
      }
      const task = this.#tasks.get(taskRunId);
      if (task === undefined) throw new TaskStateError(`TaskRun not found: ${taskRunId}`);
      return {
        task,
        presentation: this.getChannelPresentation(task.task_run_id),
        duplicate: true,
      };
    }

    if (route.kind === "conversation" || route.kind === "forbidden") {
      this.#advanceExecutionPlan(claimed.task.task_run_id, { context: "completed" });
      const completed = this.#completeChannelResponse(
        claimed.task,
        route.title ?? "Forge",
        route.markdown ?? "我暂时无法执行这项操作。",
      );
      return {
        task: completed,
        presentation: this.getChannelPresentation(completed.task_run_id),
        duplicate: false,
      };
    }

    if (route.kind === "clarification") {
      const waiting = this.#transactions.run(() => {
        this.#events.append(claimed.task!.task_run_id, "query.clarification_requested", {
          prompt: route.clarification_question ?? "请补充希望了解的指标和时间范围。",
        });
        this.#advanceExecutionPlan(claimed.task!.task_run_id, { clarification: "running" });
        return this.#transition(claimed.task!, "needs_input", "requirement_clarification");
      });
      return {
        task: waiting,
        presentation: this.getChannelPresentation(waiting.task_run_id),
        duplicate: false,
      };
    }

    if (route.kind === "action") {
      this.#advanceExecutionPlan(claimed.task.task_run_id, {
        [route.action === "memory" ? "memory_proposal" : "registry_draft"]: "running",
      });
      const completed = this.#completeChannelResponse(
        claimed.task,
        "需要确认变更内容",
        route.action === "memory"
          ? "请明确要记住或删除的内容，以及它属于个人、团队还是组织范围。我会先生成待审核变更，不会直接写入长期记忆。"
          : "请提供指标或业务规则的完整定义。我会先生成 Registry Draft 和差异，确认后才会发布。",
      );
      return {
        task: completed,
        presentation: this.getChannelPresentation(completed.task_run_id),
        duplicate: false,
      };
    }

    if (route.kind === "knowledge") {
      const context = this.#forge.searchContext === undefined
        ? undefined
        : await this.#forge.searchContext({
            orgId: claimed.task.org_id,
            teamId: claimed.task.team_id,
            userId: claimed.task.user_id,
            question: text.trim(),
            limit: 8,
          }, signal);
      if (context === undefined || context.evidence.length === 0) {
        this.#advanceExecutionPlan(claimed.task.task_run_id, { context: "completed", report: "completed" });
        const completed = this.#completeChannelResponse(
          claimed.task,
          "我可以继续帮你",
          "我暂时没有从当前 Registry、语义规则或组织知识中找到足够依据。你可以补充具体指标、表名、业务场景或希望查询的数据范围，我会继续澄清，而不会直接编造答案。",
        );
        return {
          task: completed,
          presentation: this.getChannelPresentation(completed.task_run_id),
          duplicate: false,
        };
      }
      this.#advanceExecutionPlan(claimed.task.task_run_id, { context: "completed", report: "running" });
      const advised = await this.runAdvisory(claimed.task.task_run_id, {
        skillName: "data-doc-writer",
        prompt: [
          `用户问题：${text.trim()}`,
          "请仅根据 context_evidence 回答。回答应直接、自然，并在事实 finding 中引用对应 ctx evidence_ref。",
          `Context revision: ${context.context_revision}`,
        ].join("\n"),
        contextEvidence: context.evidence,
        idempotencyKey: `${event.channel}:${event.event_id}:knowledge`,
      }, signal);
      this.#advanceExecutionPlan(claimed.task.task_run_id, { report: "completed" });
      return {
        task: advised.task,
        presentation: this.getChannelPresentation(advised.task.task_run_id),
        duplicate: false,
      };
    }

    this.#advanceExecutionPlan(claimed.task.task_run_id, { query: "running" });
    await this.prepareQuery(
      claimed.task.task_run_id,
      {
        question: text.trim(),
        idempotencyKey: `${event.channel}:${event.event_id}:prepare`,
      },
      signal,
    );
    this.#advanceExecutionPlan(claimed.task.task_run_id, { query: "waiting_approval" });
    const task = this.#tasks.get(claimed.task.task_run_id);
    if (task === undefined) throw new TaskStateError(`TaskRun not found: ${claimed.task.task_run_id}`);
    return {
      task,
      presentation: this.getChannelPresentation(task.task_run_id),
      duplicate: false,
    };
  }

  async ingestChannelAction(
    event: ChannelEventInput,
    identity: ChannelIdentity,
    signal?: AbortSignal,
  ): Promise<{ task: TaskRun; presentation: ChannelPresentation; duplicate: boolean }> {
    if (this.#channelEvents === undefined) {
      throw new TaskStateError("ChannelEvent Store is not configured");
    }
    if (event.event_type !== "action" || event.task_run_id === null) {
      throw new TaskStateError("Channel action requires task_run_id");
    }
    const task = this.#tasks.get(event.task_run_id);
    if (task === undefined) throw new TaskStateError(`TaskRun not found: ${event.task_run_id}`);
    if (
      task.channel !== event.channel ||
      task.channel_conversation_id !== event.conversation_id ||
      task.org_id !== identity.org_id ||
      task.team_id !== identity.team_id ||
      task.user_id !== identity.user_id
    ) {
      throw new TaskStateError("Channel action identity does not own this TaskRun");
    }
    const actionType = event.payload.action;
    if (![
      "approve_query",
      "analyze",
      "render_report",
      "provide_input",
      "cancel_task",
      "request_supplement",
    ].includes(String(actionType))) {
      throw new TaskStateError(`Unsupported channel action: ${String(actionType)}`);
    }
    const queryRunId = actionType === "approve_query"
      ? this.#channelPayloadString(event, "query_run_id")
      : undefined;
    const sqlHash = actionType === "approve_query"
      ? this.#channelPayloadString(event, "sql_hash")
      : undefined;
    const assuranceReportHash = actionType === "approve_query"
      ? this.#channelPayloadString(event, "assurance_report_hash")
      : undefined;
    const inputText = actionType === "provide_input"
      ? this.#channelPayloadString(event, "text")
      : undefined;
    const suggestedQueryIndex = actionType === "request_supplement"
      ? event.payload.suggested_query_index
      : undefined;
    if (
      actionType === "request_supplement" &&
      (!Number.isInteger(suggestedQueryIndex) || Number(suggestedQueryIndex) < 0)
    ) {
      throw new TaskStateError("suggested_query_index must be a non-negative integer");
    }

    const claim = this.#transactions.run(() => {
      const claimed = this.#channelEvents?.claim(event);
      if (claimed === undefined) throw new TaskStateError("ChannelEvent Store is not configured");
      return claimed;
    });
    if (!claim.created) {
      const recordedTask = claim.record.task_run_id === null
        ? task
        : this.#tasks.get(claim.record.task_run_id) ?? task;
      return {
        task: recordedTask,
        presentation: this.getChannelPresentation(recordedTask.task_run_id),
        duplicate: true,
      };
    }

    try {
      let responseTask = task;
      if (
      actionType === "approve_query" &&
      queryRunId !== undefined &&
      sqlHash !== undefined &&
      assuranceReportHash !== undefined
    ) {
      await this.approveQuery(
        task.task_run_id,
        {
          queryRunId,
          sqlHash,
          assuranceReportHash,
          idempotencyKey: `${event.channel}:${event.event_id}:approve`,
        },
        signal,
      );
    } else if (actionType === "analyze") {
      await this.analyzeTask(
        task.task_run_id,
        { idempotencyKey: `${event.channel}:${event.event_id}:analysis` },
        signal,
      );
    } else if (actionType === "render_report") {
      await this.renderReport(
        task.task_run_id,
        {
          audience:
            typeof event.payload.audience === "string"
              ? event.payload.audience
              : "业务负责人和产品经理",
          idempotencyKey: `${event.channel}:${event.event_id}:report`,
        },
        signal,
      );
    } else if (actionType === "provide_input" && inputText !== undefined) {
      await this.clarifyRequirement(
        task.task_run_id,
        {
          message: inputText,
          idempotencyKey: `${event.channel}:${event.event_id}:clarify`,
        },
        signal,
      );
    } else if (actionType === "cancel_task") {
      responseTask = await this.cancelTask(
        task.task_run_id,
        `${event.channel}:${event.event_id}:cancel`,
        signal,
      );
    } else if (actionType === "request_supplement" && typeof suggestedQueryIndex === "number") {
      const supplement = this.createSupplementTask(task.task_run_id, {
        suggestedQueryIndex,
        idempotencyKey: `${event.channel}:${event.event_id}:supplement`,
      });
      await this.prepareQuery(
        supplement.childTask.task_run_id,
        {
          question: String(supplement.suggestion.question),
          idempotencyKey: `${event.channel}:${event.event_id}:supplement:prepare`,
        },
        signal,
      );
      responseTask = this.#tasks.get(supplement.childTask.task_run_id) ?? supplement.childTask;
    } else {
      throw new TaskStateError(`Unsupported channel action: ${String(actionType)}`);
    }
    const updated = responseTask.task_run_id === task.task_run_id
      ? this.#tasks.get(task.task_run_id)
      : responseTask;
    if (updated === undefined) throw new TaskStateError(`TaskRun not found: ${task.task_run_id}`);
    this.#channelEvents.complete(event.channel, event.event_id, updated.task_run_id);
      return {
        task: updated,
        presentation: this.getChannelPresentation(updated.task_run_id),
        duplicate: false,
      };
    } catch (error) {
      this.#channelEvents.fail(
        event.channel,
        event.event_id,
        error instanceof Error ? error.message : "Channel action failed",
      );
      throw error;
    }
  }

  async cancelTask(
    taskRunId: string,
    idempotencyKey: string,
    signal?: AbortSignal,
  ): Promise<TaskRun> {
    const task = this.#tasks.get(taskRunId);
    if (task === undefined) throw new TaskStateError(`TaskRun not found: ${taskRunId}`);
    if (task.status === "cancelled") return task;
    if (["completed", "failed", "expired"].includes(task.status)) {
      throw new TaskStateError(`Terminal TaskRun cannot be cancelled: ${task.status}`);
    }
    if (task.status === "waiting_for_query_approval") {
      const review = [...this.#events.list(taskRunId)]
        .reverse()
        .find((event) => event.event_type === "query.review_requested");
      const queryRunId = review?.payload.query_run_id;
      if (typeof queryRunId !== "string" || this.#forge.cancelQueryRun === undefined) {
        throw new TaskStateError("Reviewable QueryRun cannot be cancelled safely");
      }
      await this.#forge.cancelQueryRun(
        { queryRunId, userId: task.user_id },
        signal,
      );
    }
    return this.#transactions.run(() => {
      const current = this.#tasks.get(taskRunId);
      if (current === undefined) throw new TaskStateError(`TaskRun not found: ${taskRunId}`);
      if (current.status === "cancelled") return current;
      const cancelled = this.#tasks.transition({
        taskRunId,
        expectedStatus: current.status,
        status: "cancelled",
        currentStage: null,
      });
      this.#events.append(taskRunId, "task.status_changed", {
        from: current.status,
        to: "cancelled",
        reason: "user_cancelled",
        idempotency_key: idempotencyKey,
      });
      return cancelled;
    });
  }

  async clarifyRequirement(
    taskRunId: string,
    input: { message: string; idempotencyKey?: string },
    signal?: AbortSignal,
  ): Promise<{ task: TaskRun; artifact: Artifact; events: TaskEvent[] }> {
    const initialTask = this.#tasks.get(taskRunId);
    if (initialTask === undefined) throw new TaskStateError(`TaskRun not found: ${taskRunId}`);
    this.#assertSkillEnabled(initialTask, "data-requirement-clarifier");
    if (initialTask.status !== "created" && initialTask.status !== "needs_input") {
      throw new TaskStateError(`TaskRun cannot clarify from status: ${initialTask.status}`);
    }
    const retryStatus = initialTask.status;
    const round = this.#artifacts.list(taskRunId).filter(
      (artifact) => artifact.artifact_type === "clarification",
    ).length + 1;
    let attempt: StageAttempt | undefined;
    let task = this.#transactions.run(() => {
      const running = this.#transition(initialTask, "clarifying", "requirement_clarification");
      attempt = this.#startAttempt(
        running,
        "requirement_clarification",
        retryStatus,
        input.idempotencyKey ?? `${taskRunId}:clarification:${round}`,
      );
      return running;
    });
    const stageExecution = this.#stageSignal(signal);
    try {
      const payload = await this.#skills.clarify(
        task, input.message, stageExecution.signal, attempt?.model_revision,
      );
      if (stageExecution.timedOut()) throw new Error("Clarification Stage timed out");
      return this.#transactions.run(() => {
        const artifact = this.#artifacts.create({
          artifactType: "clarification",
          taskRunId,
          producer: "skill:data-requirement-clarifier",
          payload,
        });
        this.#events.append(taskRunId, "artifact.created", {
          artifact_id: artifact.artifact_id,
          artifact_type: artifact.artifact_type,
          schema_version: artifact.schema_version,
          payload: artifact.payload,
        });
        task = this.#transition(
          task,
          payload.status === "confirmed" ? "ready_for_query" : "needs_input",
          payload.status === "confirmed" ? "query_prepare" : "requirement_clarification",
        );
        this.#finishAttempt(attempt, "succeeded");
        return { task, artifact, events: this.#events.list(taskRunId) };
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown Skill error";
      this.#transactions.run(() => {
        if (stageExecution.timedOut()) {
          this.#finishAttempt(attempt, "timed_out", message);
          task = this.#transition(task, retryStatus, "clarification_retry");
        } else {
          this.#finishAttempt(attempt, "failed", message);
          task = this.#transition(task, "failed", "requirement_clarification");
        }
        this.#events.append(taskRunId, "skill.execution_failed", {
          skill: "data-requirement-clarifier",
          timed_out: stageExecution.timedOut(),
          error: message,
        });
      });
      throw new TaskStateError(`Requirement clarification failed for ${taskRunId}`, {
        cause: error,
      });
    }
  }

  async reviewMetricDefinition(
    taskRunId: string,
    input: { message: string; idempotencyKey?: string },
    signal?: AbortSignal,
  ): Promise<{ task: TaskRun; artifact: Artifact; events: TaskEvent[] }> {
    const initialTask = this.#tasks.get(taskRunId);
    if (initialTask === undefined) throw new TaskStateError(`TaskRun not found: ${taskRunId}`);
    this.#assertSkillEnabled(initialTask, "metric-definition-reviewer");
    if (
      initialTask.status !== "created" &&
      initialTask.status !== "needs_input" &&
      initialTask.status !== "ready_for_query"
    ) {
      throw new TaskStateError(`TaskRun cannot review a metric from status: ${initialTask.status}`);
    }
    const retryStatus = initialTask.status;
    const round = this.#artifacts.list(taskRunId).filter(
      (artifact) => artifact.artifact_type === "metric_definition",
    ).length + 1;
    let attempt: StageAttempt | undefined;
    let task = this.#transactions.run(() => {
      const running = this.#transition(initialTask, "clarifying", "metric_definition_review");
      attempt = this.#startAttempt(
        running,
        "metric_definition_review",
        retryStatus,
        input.idempotencyKey ?? `${taskRunId}:metric-review:${round}`,
      );
      return running;
    });
    const stageExecution = this.#stageSignal(signal);
    try {
      const payload = await this.#skills.reviewMetric(
        task, input.message, stageExecution.signal, attempt?.model_revision,
      );
      if (stageExecution.timedOut()) throw new Error("Metric review Stage timed out");
      return this.#transactions.run(() => {
        const artifact = this.#artifacts.create({
          artifactType: "metric_definition",
          taskRunId,
          producer: "skill:metric-definition-reviewer",
          payload,
        });
        this.#events.append(taskRunId, "artifact.created", {
          artifact_id: artifact.artifact_id,
          artifact_type: artifact.artifact_type,
          schema_version: artifact.schema_version,
          payload: artifact.payload,
        });
        task = this.#transition(
          task,
          payload.status === "confirmed" ? "ready_for_query" : "needs_input",
          payload.status === "confirmed" ? "query_prepare" : "metric_definition_review",
        );
        this.#finishAttempt(attempt, "succeeded");
        return { task, artifact, events: this.#events.list(taskRunId) };
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown Skill error";
      this.#transactions.run(() => {
        if (stageExecution.timedOut()) {
          this.#finishAttempt(attempt, "timed_out", message);
          task = this.#transition(task, retryStatus, "metric_review_retry");
        } else {
          this.#finishAttempt(attempt, "failed", message);
          task = this.#transition(task, "failed", "metric_definition_review");
        }
        this.#events.append(taskRunId, "skill.execution_failed", {
          skill: "metric-definition-reviewer",
          timed_out: stageExecution.timedOut(),
          error: message,
        });
      });
      throw new TaskStateError(`Metric definition review failed for ${taskRunId}`, {
        cause: error,
      });
    }
  }

  async prepareQuery(
    taskRunId: string,
    input: { question: string; dialect?: ForgeDialect; idempotencyKey?: string },
    signal?: AbortSignal,
  ): Promise<{ task: TaskRun; result: QueryRunReview; events: TaskEvent[] }> {
    let task = this.#tasks.get(taskRunId);
    if (task === undefined) throw new TaskStateError(`TaskRun not found: ${taskRunId}`);
    if (input.question.trim().length === 0) {
      throw new TaskStateError("question must not be empty");
    }

    if (task.status === "created" || task.status === "needs_input") {
      task = this.#transition(task, "ready_for_query", "query_prepare");
    } else if (task.status !== "ready_for_query") {
      throw new TaskStateError(`TaskRun cannot prepare a query from status: ${task.status}`);
    }

    let stageTask: TaskRun = task;
    const prepareNumber =
      (this.#attempts?.list(taskRunId).filter((candidate) => candidate.stage === "query_prepare")
        .length ?? 0) + 1;
    let attempt: StageAttempt | undefined;
    this.#transactions.run(() => {
      attempt = this.#startAttempt(
        stageTask,
        "query_prepare",
        "ready_for_query",
        input.idempotencyKey ?? `${taskRunId}:query-prepare:${prepareNumber}`,
      );
    });
    const stageExecution = this.#stageSignal(signal);
    let result: QueryRunReview;
    try {
      result = await this.#forge.createQueryRun(
        {
          taskRunId: stageTask.task_run_id,
          orgId: stageTask.org_id,
          teamId: stageTask.team_id,
          userId: stageTask.user_id,
          question: input.question,
          idempotencyKey: `${stageTask.task_run_id}:prepare`,
          ...(input.dialect === undefined ? {} : { dialect: input.dialect }),
        },
        stageExecution.signal,
      );
      if (stageExecution.timedOut()) throw new Error("Query preparation Stage timed out");
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown Forge error";
      this.#transactions.run(() => {
        if (stageExecution.timedOut()) {
          this.#finishAttempt(attempt, "timed_out", message);
          stageTask = this.#transition(stageTask, "ready_for_query", "query_prepare_retry");
        } else {
          this.#finishAttempt(attempt, "failed", message);
          stageTask = this.#transition(stageTask, "failed", "query_prepare");
        }
        this.#events.append(taskRunId, "query.prepare_failed", {
          timed_out: stageExecution.timedOut(),
          error: message,
        });
      });
      throw new TaskStateError(
        `Query preparation failed for ${stageTask.task_run_id}: ${message}`,
        { cause: error },
      );
    }

    let finalizedTask = stageTask;
    return this.#transactions.run(() => {
      if (result.status === "needs_review") {
        finalizedTask = this.#transition(finalizedTask, "waiting_for_query_approval", "query_review");
        this.#events.append(taskRunId, "query.review_requested", {
          query_run_id: result.query_run_id,
          sql: result.sql,
          sql_hash: result.sql_hash,
          forge_json: result.forge_json,
          dialect: result.dialect,
          registry_version: result.registry_version,
          assurance_report: result.assurance_report,
          assurance_report_hash: result.assurance_report_hash,
          assurance_revision: result.assurance_revision,
          policy_revision: result.policy_revision,
          model_revision: result.model_revision,
          assurance_registry_revision: result.assurance_registry_revision,
          expires_at: result.expires_at,
          review_required: true,
          can_execute: false,
        });
      } else if (result.status === "needs_clarification") {
        finalizedTask = this.#transition(finalizedTask, "needs_input", "requirement_clarification");
        this.#events.append(taskRunId, "query.clarification_requested", {
          prompt: result.error,
        });
      } else if (result.status === "timed_out") {
        finalizedTask = this.#transition(finalizedTask, "ready_for_query", "query_prepare_retry");
        this.#events.append(taskRunId, "query.prepare_timed_out", {
          error: result.error || "查询准备超时，请重试。",
          retryable: true,
        });
      } else {
        finalizedTask = this.#transition(finalizedTask, "failed", "query_prepare");
        this.#events.append(taskRunId, "query.prepare_failed", {
          error: result.error || "Forge could not prepare the query",
        });
      }

      this.#finishAttempt(
        attempt,
        result.status === "timed_out"
          ? "timed_out"
          : result.status === "needs_review" || result.status === "needs_clarification"
            ? "succeeded"
            : "failed",
        result.error,
      );
      return { task: finalizedTask, result, events: this.#events.list(taskRunId) };
    });
  }

  async approveQuery(
    taskRunId: string,
    input: {
      queryRunId: string;
      sqlHash: string;
      assuranceReportHash?: string;
      idempotencyKey: string;
    },
    signal?: AbortSignal,
  ): Promise<{
    task: TaskRun;
    result: QueryRunResult;
    artifact: Artifact;
    events: TaskEvent[];
  }> {
    let task = this.#tasks.get(taskRunId);
    if (task === undefined) throw new TaskStateError(`TaskRun not found: ${taskRunId}`);
    const review = [...this.#events.list(taskRunId)]
      .reverse()
      .find((event) => event.event_type === "query.review_requested");
    const assuranceReportHash = review?.payload.assurance_report_hash;
    if (
      review?.payload.query_run_id !== input.queryRunId ||
      review.payload.sql_hash !== input.sqlHash ||
      typeof assuranceReportHash !== "string" ||
      (input.assuranceReportHash !== undefined &&
        input.assuranceReportHash !== assuranceReportHash)
    ) {
      throw new TaskStateError("Approval does not match the TaskRun review request");
    }
    if (
      task.status !== "waiting_for_query_approval" &&
      task.status !== "querying" &&
      task.status !== "completed" &&
      task.status !== "ready_for_analysis"
    ) {
      throw new TaskStateError(`TaskRun cannot approve a query from status: ${task.status}`);
    }

    let attempt: StageAttempt | undefined;
    let executionTask: TaskRun = task;
    if (executionTask.status === "waiting_for_query_approval") {
      let approvalTask: TaskRun = executionTask;
      executionTask = this.#transactions.run(() => {
        approvalTask = this.#transition(approvalTask, "querying", "query_execution");
        this.#events.append(taskRunId, "query.approval_submitted", {
          query_run_id: input.queryRunId,
          sql_hash: input.sqlHash,
          assurance_report_hash: assuranceReportHash,
          approver_user_id: approvalTask.user_id,
        });
        attempt = this.#startAttempt(
          approvalTask,
          "query_execution",
          "waiting_for_query_approval",
          input.idempotencyKey,
        );
        return approvalTask;
      });
    } else if (executionTask.status === "querying") {
      attempt = this.#attempts?.findByIdempotencyKey(taskRunId, input.idempotencyKey);
      if (this.#attempts !== undefined && attempt?.status !== "running") {
        throw new TaskStateError("Query execution attempt is not resumable");
      }
    }

    const stageExecution = this.#stageSignal(signal);
    let result: QueryRunResult;
    try {
      result = await this.#forge.approveQueryRun(
        {
          queryRunId: input.queryRunId,
          approverUserId: executionTask.user_id,
          sqlHash: input.sqlHash,
          assuranceReportHash,
          idempotencyKey: input.idempotencyKey,
        },
        stageExecution.signal,
      );
      if (stageExecution.timedOut()) throw new Error("Query execution Stage timed out");
    } catch (error) {
      if (executionTask.status === "querying") {
        const message = error instanceof Error ? error.message : "unknown Forge error";
        this.#transactions.run(() => {
          if (stageExecution.timedOut()) {
            this.#finishAttempt(attempt, "timed_out", message);
            executionTask = this.#transition(
              executionTask,
              "waiting_for_query_approval",
              "query_retry",
            );
          } else {
            this.#finishAttempt(attempt, "failed", message);
            executionTask = this.#transition(executionTask, "failed", "query_execution");
          }
          this.#events.append(taskRunId, "query.execution_failed", {
            query_run_id: input.queryRunId,
            timed_out: stageExecution.timedOut(),
            error: message,
          });
        });
      }
      throw new TaskStateError(`Query execution failed for ${taskRunId}`, { cause: error });
    }

    let artifact = this.#artifacts.latest(taskRunId, "query_result");
    if (executionTask.status === "querying") {
      this.#transactions.run(() => {
        artifact = this.#artifacts.create({
          artifactType: "query_result",
          taskRunId,
          producer: "forge",
          payload: {
            query_run_id: result.query_run_id,
            sql_hash: result.sql_hash,
            columns: result.columns,
            rows: result.rows,
            row_count: result.row_count,
            truncated: result.truncated,
            dialect: result.dialect,
            registry_version: result.registry_version,
            assurance_report_hash: result.assurance_report_hash,
            assurance_revision: result.assurance_revision,
            policy_revision: result.policy_revision,
            model_revision: result.model_revision,
            assurance_registry_revision: result.assurance_registry_revision,
            execution_ms: result.execution_ms,
            executed_at: result.executed_at,
          },
        });
        this.#events.append(taskRunId, "artifact.created", {
          artifact_id: artifact.artifact_id,
          artifact_type: artifact.artifact_type,
          schema_version: artifact.schema_version,
        });
        const chartPayload = buildChartPayload(artifact as Artifact<QueryResultPayload>);
        if (chartPayload !== undefined) {
          const chart = this.#artifacts.create({
            artifactType: "chart",
            taskRunId,
            producer: "pi-chart-builder",
            payload: chartPayload,
          });
          this.#events.append(taskRunId, "artifact.created", {
            artifact_id: chart.artifact_id,
            artifact_type: chart.artifact_type,
            schema_version: chart.schema_version,
          });
          this.#advanceExecutionPlan(taskRunId, { chart: "completed" });
        } else {
          this.#advanceExecutionPlan(taskRunId, { chart: "skipped" });
        }
        const continueToAnalysis =
          executionTask.intent !== "query_prepare" &&
          executionTask.intent !== "analysis_supplement_query";
        executionTask = this.#transition(
          executionTask,
          continueToAnalysis ? "ready_for_analysis" : "completed",
          continueToAnalysis ? "analysis_prepare" : "query_complete",
        );
        this.#events.append(taskRunId, "query.completed", {
          ...result,
          artifact_id: artifact.artifact_id,
        });
        this.#advanceExecutionPlan(taskRunId, { query: "completed" });
        this.#finishAttempt(attempt, "succeeded");
      });
    }
    if (artifact === undefined) {
      throw new TaskStateError(`QueryResultArtifact not found for ${taskRunId}`);
    }
    return { task: executionTask, result, artifact, events: this.#events.list(taskRunId) };
  }

  createSupplementTask(
    taskRunId: string,
    input: { suggestedQueryIndex: number; idempotencyKey: string },
  ): {
    parentTask: TaskRun;
    childTask: TaskRun;
    suggestion: Record<string, unknown>;
    parentEvents: TaskEvent[];
    childEvents: TaskEvent[];
  } {
    const parent = this.#tasks.get(taskRunId);
    if (parent === undefined) throw new TaskStateError(`TaskRun not found: ${taskRunId}`);
    if (parent.status !== "incomplete") {
      throw new TaskStateError(`TaskRun cannot request a supplement from status: ${parent.status}`);
    }
    if (!Number.isInteger(input.suggestedQueryIndex) || input.suggestedQueryIndex < 0) {
      throw new TaskStateError("suggested_query_index must be a non-negative integer");
    }
    if (input.idempotencyKey.trim().length === 0) {
      throw new TaskStateError("idempotency_key must not be empty");
    }
    const analysis = this.#artifacts.latest(taskRunId, "analysis") as
      | Artifact<AnalysisPayload>
      | undefined;
    const suggestion = analysis?.payload.suggested_queries[input.suggestedQueryIndex];
    if (analysis === undefined || analysis.payload.status !== "incomplete" || suggestion === undefined) {
      throw new TaskStateError("Suggested query does not exist in the latest AnalysisArtifact");
    }
    const existing = this.#events
      .list(taskRunId)
      .find((event) => event.event_type === "analysis.supplement_created");
    if (existing !== undefined) {
      if (existing.payload.idempotency_key !== input.idempotencyKey) {
        throw new TaskStateError("TaskRun has already used its single supplemental query");
      }
      const existingChildId = existing.payload.child_task_run_id;
      const existingChild =
        typeof existingChildId === "string" ? this.#tasks.get(existingChildId) : undefined;
      if (existingChild === undefined) {
        throw new TaskStateError("Idempotent supplemental child TaskRun is missing");
      }
      return {
        parentTask: parent,
        childTask: existingChild,
        suggestion: structuredClone(suggestion) as unknown as Record<string, unknown>,
        parentEvents: this.#events.list(taskRunId),
        childEvents: this.#events.list(existingChild.task_run_id),
      };
    }
    return this.#transactions.run(() => {
      const child = this.#tasks.create({
        org_id: parent.org_id,
      team_id: parent.team_id,
      user_id: parent.user_id,
      channel: parent.channel,
      channel_conversation_id: parent.channel_conversation_id,
      intent: "analysis_supplement_query",
      correlation_id: parent.correlation_id,
      parent_task_run_id: parent.task_run_id,
        metadata: {
          original_message: suggestion.question,
          supplement_reason: suggestion.reason,
          supplement_priority: suggestion.priority,
          suggested_query_index: input.suggestedQueryIndex,
        },
      });
      this.#events.append(child.task_run_id, "task.created", {
        status: child.status,
        intent: child.intent,
        channel: child.channel,
        parent_task_run_id: parent.task_run_id,
      });
      this.#events.append(taskRunId, "analysis.supplement_created", {
        child_task_run_id: child.task_run_id,
        suggested_query_index: input.suggestedQueryIndex,
        question: suggestion.question,
        idempotency_key: input.idempotencyKey,
      });
      return {
        parentTask: parent,
        childTask: child,
        suggestion: structuredClone(suggestion) as unknown as Record<string, unknown>,
        parentEvents: this.#events.list(taskRunId),
        childEvents: this.#events.list(child.task_run_id),
      };
    });
  }

  async resumeAnalysisWithSupplement(
    taskRunId: string,
    input: { childTaskRunId: string; idempotencyKey: string },
    signal?: AbortSignal,
  ): Promise<{ task: TaskRun; artifact: Artifact; events: TaskEvent[] }> {
    let parent = this.#tasks.get(taskRunId);
    if (parent === undefined) throw new TaskStateError(`TaskRun not found: ${taskRunId}`);
    if (input.idempotencyKey.trim().length === 0) {
      throw new TaskStateError("idempotency_key must not be empty");
    }
    const consumedEvent = this.#events
      .list(taskRunId)
      .find((event) => event.event_type === "analysis.supplement_consumed");
    if (consumedEvent !== undefined) {
      if (
        consumedEvent.payload.idempotency_key !== input.idempotencyKey ||
        consumedEvent.payload.child_task_run_id !== input.childTaskRunId
      ) {
        throw new TaskStateError("Supplemental QueryResult has already been consumed");
      }
      const artifact = this.#artifacts.latest(taskRunId, "analysis");
      if (artifact === undefined) {
        throw new TaskStateError("Idempotent supplemental AnalysisArtifact is missing");
      }
      return { task: parent, artifact, events: this.#events.list(taskRunId) };
    }
    this.#assertSkillEnabled(parent, "business-root-cause-analysis");
    if (parent.status !== "incomplete") {
      throw new TaskStateError(`TaskRun cannot resume analysis from status: ${parent.status}`);
    }
    const supplementEvent = this.#events
      .list(taskRunId)
      .find(
        (event) =>
          event.event_type === "analysis.supplement_created" &&
          event.payload.child_task_run_id === input.childTaskRunId,
      );
    if (supplementEvent === undefined) {
      throw new TaskStateError("Supplement child does not belong to this TaskRun");
    }
    const child = this.#tasks.get(input.childTaskRunId);
    if (
      child === undefined ||
      child.parent_task_run_id !== taskRunId ||
      child.intent !== "analysis_supplement_query" ||
      child.status !== "completed"
    ) {
      throw new TaskStateError("Supplement child query is not completed");
    }
    const primaryResult = this.#artifacts.latest(taskRunId, "query_result") as
      | Artifact<QueryResultPayload>
      | undefined;
    const supplementResult = this.#artifacts.latest(
      input.childTaskRunId,
      "query_result",
    ) as Artifact<QueryResultPayload> | undefined;
    const priorAnalysis = this.#artifacts.latest(taskRunId, "analysis") as
      | Artifact<AnalysisPayload>
      | undefined;
    if (primaryResult === undefined || supplementResult === undefined || priorAnalysis === undefined) {
      throw new TaskStateError("Supplement analysis source Artifacts are incomplete");
    }
    const originalMessage = parent.metadata.original_message;
    const question =
      typeof originalMessage === "string" ? originalMessage : parent.intent;
    let attempt: StageAttempt | undefined;
    let stageTask: TaskRun = parent;
    stageTask = this.#transactions.run(() => {
      const running = this.#transition(stageTask, "analyzing", "supplemental_analysis");
      attempt = this.#startAttempt(
        running,
        "supplemental_analysis",
        "incomplete",
        input.idempotencyKey,
      );
      return running;
    });
    const stageExecution = this.#stageSignal(signal);
    try {
      const queryResults = [primaryResult, supplementResult];
      const payload = await this.#skills.analyze(
        stageTask,
        { question, queryResults, priorAnalysis },
        stageExecution.signal,
        attempt?.model_revision,
      );
      if (stageExecution.timedOut()) throw new Error("Supplemental Analysis Stage timed out");
      const allowedEvidenceRefs = new Set(
        queryResults.flatMap((result) =>
          result.payload.rows.map(
            (_row, index) => `${result.payload.query_run_id}#row:${index + 1}`,
          ),
        ),
      );
      const references = [
        ...payload.findings.flatMap((finding) => finding.evidence_refs),
        ...payload.hypotheses.flatMap((hypothesis) => hypothesis.evidence_refs),
      ];
      if (references.some((reference) => !allowedEvidenceRefs.has(reference))) {
        throw new Error("Supplement analysis evidence is absent from parent/child QueryResults");
      }
      return this.#transactions.run(() => {
        const artifact = this.#artifacts.create({
          artifactType: "analysis",
          taskRunId,
          producer: "skill:business-root-cause-analysis",
          payload,
        });
        this.#events.append(taskRunId, "analysis.supplement_consumed", {
          child_task_run_id: child.task_run_id,
          query_result_artifact_id: supplementResult.artifact_id,
          idempotency_key: input.idempotencyKey,
        });
        this.#events.append(taskRunId, "artifact.created", {
          artifact_id: artifact.artifact_id,
          artifact_type: artifact.artifact_type,
          schema_version: artifact.schema_version,
        });
        stageTask = this.#transition(
          stageTask,
          payload.status === "complete" ? "ready_for_report" : "incomplete",
          payload.status === "complete" ? "report_prepare" : "analysis_incomplete",
        );
        this.#events.append(taskRunId, "analysis.completed", {
          artifact_id: artifact.artifact_id,
          status: payload.status,
          suggested_queries: payload.suggested_queries,
          supplemental: true,
        });
        this.#finishAttempt(attempt, "succeeded");
        return { task: stageTask, artifact, events: this.#events.list(taskRunId) };
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown Skill error";
      this.#transactions.run(() => {
        if (stageExecution.timedOut()) {
          this.#finishAttempt(attempt, "timed_out", message);
          stageTask = this.#transition(stageTask, "incomplete", "supplemental_analysis_retry");
        } else {
          this.#finishAttempt(attempt, "failed", message);
          stageTask = this.#transition(stageTask, "failed", "supplemental_analysis");
        }
        this.#events.append(taskRunId, "skill.execution_failed", {
          skill: "business-root-cause-analysis",
          supplemental: true,
          timed_out: stageExecution.timedOut(),
          error: message,
        });
      });
      throw new TaskStateError(`Supplemental analysis failed for ${taskRunId}`, {
        cause: error,
      });
    }
  }

  async analyzeTask(
    taskRunId: string,
    input: { question?: string; idempotencyKey?: string },
    signal?: AbortSignal,
  ): Promise<{ task: TaskRun; artifact: Artifact; events: TaskEvent[] }> {
    let task = this.#tasks.get(taskRunId);
    if (task === undefined) throw new TaskStateError(`TaskRun not found: ${taskRunId}`);
    this.#assertSkillEnabled(task, "business-root-cause-analysis");
    if (task.status !== "ready_for_analysis") {
      throw new TaskStateError(`TaskRun cannot analyze from status: ${task.status}`);
    }
    const queryResult = this.#artifacts.latest(taskRunId, "query_result") as
      | Artifact<QueryResultPayload>
      | undefined;
    if (queryResult === undefined) {
      throw new TaskStateError(`QueryResultArtifact not found for ${taskRunId}`);
    }
    const originalMessage = task.metadata.original_message;
    const question =
      input.question?.trim() ||
      (typeof originalMessage === "string" ? originalMessage : task.intent);
    let attempt: StageAttempt | undefined;
    let stageTask: TaskRun = task;
    stageTask = this.#transactions.run(() => {
      const running = this.#transition(stageTask, "analyzing", "business_root_cause_analysis");
      this.#advanceExecutionPlan(taskRunId, { analysis: "running" });
      attempt = this.#startAttempt(
        running,
        "business_root_cause_analysis",
        "ready_for_analysis",
        input.idempotencyKey ?? `${taskRunId}:analysis:${queryResult.artifact_id}`,
      );
      return running;
    });
    const stageExecution = this.#stageSignal(signal);
    try {
      const payload = await this.#skills.analyze(
        stageTask,
        { question, queryResults: [queryResult] },
        stageExecution.signal,
        attempt?.model_revision,
      );
      if (stageExecution.timedOut()) throw new Error("Analysis Stage timed out");
      const allowedEvidenceRefs = new Set(
        queryResult.payload.rows.map(
          (_row, index) => `${queryResult.payload.query_run_id}#row:${index + 1}`,
        ),
      );
      const analysisRefs = [
        ...payload.findings.flatMap((finding) => finding.evidence_refs),
        ...payload.hypotheses.flatMap((hypothesis) => hypothesis.evidence_refs),
      ];
      if (analysisRefs.some((reference) => !allowedEvidenceRefs.has(reference))) {
        throw new Error("Analysis evidence is not present in QueryResultArtifact");
      }
      let finalizedTask = stageTask;
      return this.#transactions.run(() => {
        const artifact = this.#artifacts.create({
          artifactType: "analysis",
          taskRunId,
          producer: "skill:business-root-cause-analysis",
          payload,
        });
        this.#events.append(taskRunId, "artifact.created", {
          artifact_id: artifact.artifact_id,
          artifact_type: artifact.artifact_type,
          schema_version: artifact.schema_version,
        });
        finalizedTask = this.#transition(
          finalizedTask,
          payload.status === "complete" ? "ready_for_report" : "incomplete",
          payload.status === "complete" ? "report_prepare" : "analysis_incomplete",
        );
        this.#events.append(taskRunId, "analysis.completed", {
          artifact_id: artifact.artifact_id,
          status: payload.status,
          suggested_queries: payload.suggested_queries,
        });
        this.#advanceExecutionPlan(taskRunId, {
          analysis: payload.status === "complete" ? "completed" : "running",
        });
        this.#finishAttempt(attempt, "succeeded");
        return { task: finalizedTask, artifact, events: this.#events.list(taskRunId) };
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown Skill error";
      this.#transactions.run(() => {
        if (stageExecution.timedOut()) {
          this.#finishAttempt(attempt, "timed_out", message);
          stageTask = this.#transition(stageTask, "ready_for_analysis", "analysis_retry");
        } else {
          this.#finishAttempt(attempt, "failed", message);
          stageTask = this.#transition(stageTask, "failed", "business_root_cause_analysis");
        }
        this.#events.append(taskRunId, "skill.execution_failed", {
          skill: "business-root-cause-analysis",
          timed_out: stageExecution.timedOut(),
          error: message,
        });
      });
      throw new TaskStateError(`Business analysis failed for ${taskRunId}`, {
        cause: error,
      });
    }
  }

  async renderReport(
    taskRunId: string,
    input: { audience: string; idempotencyKey?: string },
    signal?: AbortSignal,
  ): Promise<{ task: TaskRun; artifact: Artifact; events: TaskEvent[] }> {
    let task = this.#tasks.get(taskRunId);
    if (task === undefined) throw new TaskStateError(`TaskRun not found: ${taskRunId}`);
    this.#assertSkillEnabled(task, "data-analysis-report-writer");
    if (task.status !== "ready_for_report") {
      throw new TaskStateError(`TaskRun cannot render a report from status: ${task.status}`);
    }
    if (input.audience.trim().length === 0) {
      throw new TaskStateError("audience must not be empty");
    }
    const analysis = this.#artifacts.latest(taskRunId, "analysis") as
      | Artifact<AnalysisPayload>
      | undefined;
    if (analysis === undefined) {
      throw new TaskStateError(`AnalysisArtifact not found for ${taskRunId}`);
    }
    let attempt: StageAttempt | undefined;
    let stageTask: TaskRun = task;
    stageTask = this.#transactions.run(() => {
      const running = this.#transition(stageTask, "rendering", "data_analysis_report");
      this.#advanceExecutionPlan(taskRunId, { report: "running" });
      attempt = this.#startAttempt(
        running,
        "data_analysis_report",
        "ready_for_report",
        input.idempotencyKey ?? `${taskRunId}:report:${analysis.artifact_id}`,
      );
      return running;
    });
    const stageExecution = this.#stageSignal(signal);
    try {
      const payload = await this.#skills.writeReport(
        stageTask,
        { audience: input.audience, analysis },
        stageExecution.signal,
        attempt?.model_revision,
      );
      if (stageExecution.timedOut()) throw new Error("Report Stage timed out");
      const analysisStatements = new Set(
        analysis.payload.findings.map((finding) => finding.statement),
      );
      const analysisEvidenceRefs = new Set([
        ...analysis.payload.findings.flatMap((finding) => finding.evidence_refs),
        ...analysis.payload.hypotheses.flatMap(
          (hypothesis) => hypothesis.evidence_refs,
        ),
      ]);
      if (!payload.source_artifact_ids.includes(analysis.artifact_id)) {
        throw new Error("Report source does not include AnalysisArtifact");
      }
      if (
        payload.key_findings.some(
          (finding) => !analysisStatements.has(finding.statement),
        ) ||
        payload.key_findings
          .flatMap((finding) => finding.evidence_refs)
          .some((reference) => !analysisEvidenceRefs.has(reference))
      ) {
        throw new Error("Report introduced findings outside AnalysisArtifact");
      }
      const assembled = this.#transactions.run(() => {
        const artifact = this.#artifacts.create({
          artifactType: "rendered_output",
          taskRunId,
          producer: "skill:data-analysis-report-writer",
          payload,
        });
        this.#events.append(taskRunId, "artifact.created", {
          artifact_id: artifact.artifact_id,
          artifact_type: artifact.artifact_type,
          schema_version: artifact.schema_version,
        });
        const queryResult = this.#artifacts.latest(taskRunId, "query_result") as
          | Artifact<QueryResultPayload>
          | undefined;
        const reportEvents = this.#events.list(taskRunId);
        const review = [...reportEvents].reverse().find((event) => event.event_type === "query.review_requested");
        const queryCompleted = [...reportEvents].reverse().find((event) => event.event_type === "query.completed");
        if (queryResult === undefined || review === undefined) {
          throw new Error("Report publication requires QueryResult and approved SQL lineage");
        }
        const technicalPayload: TechnicalReportPayload = {
          title: `${payload.title} · 技术报告`,
          sql: typeof review.payload.sql === "string" ? review.payload.sql : "",
          query_run_id: queryResult.payload.query_run_id,
          sql_hash: queryResult.payload.sql_hash,
          approval: { approved: true, approved_at: queryCompleted?.created_at ?? null },
          execution: {
            executed_at: queryResult.payload.executed_at,
            execution_ms: queryResult.payload.execution_ms,
            row_count: queryResult.payload.row_count,
            truncated: queryResult.payload.truncated,
          },
          lineage: Object.fromEntries(Object.entries({
            registry_version: queryResult.payload.registry_version,
            assurance_report_hash: queryResult.payload.assurance_report_hash,
            assurance_revision: queryResult.payload.assurance_revision,
            policy_revision: queryResult.payload.policy_revision,
            model_revision: queryResult.payload.model_revision,
            assurance_registry_revision: queryResult.payload.assurance_registry_revision,
          }).filter((entry): entry is [string, string] => typeof entry[1] === "string")),
          decision_log: analysis.payload.method_summary.approach_steps.map((step) => ({
            stage: "analysis",
            decision: step,
            rationale: analysis.payload.method_summary.comparison_baseline,
            evidence_refs: analysis.payload.findings.flatMap((finding) => finding.evidence_refs),
          })),
          source_artifact_ids: [queryResult.artifact_id, analysis.artifact_id, artifact.artifact_id],
        };
        const technical = this.#artifacts.create({
          artifactType: "technical_report",
          taskRunId,
          producer: "pi-report-builder",
          payload: technicalPayload,
        });
        const charts = this.#artifacts.list(taskRunId).filter((candidate) => candidate.artifact_type === "chart");
        const reportId = `rp_${bundleHash({ task_run_id: taskRunId }).slice("sha256:".length, "sha256:".length + 32)}`;
        const sourceArtifactIds = [queryResult.artifact_id, analysis.artifact_id, artifact.artifact_id, technical.artifact_id, ...charts.map((chart) => chart.artifact_id)];
        const reportBundlePayload = {
          report_id: reportId,
          revision: 1,
          title: payload.title,
          business_artifact_id: artifact.artifact_id,
          technical_artifact_id: technical.artifact_id,
          chart_artifact_ids: charts.map((chart) => chart.artifact_id),
          source_artifact_ids: sourceArtifactIds,
          bundle_hash: bundleHash({
            report_id: reportId,
            business_report: payload,
            analysis: analysis.payload,
            query_result_hash: queryResult.payload.sql_hash,
            charts: charts.map((chart) => chart.payload),
            technical_report: technicalPayload,
          }),
        };
        const bundle = this.#artifacts.create({
          artifactType: "report_bundle",
          taskRunId,
          producer: "pi-report-builder",
          payload: reportBundlePayload,
        });
        for (const created of [technical, bundle]) {
          this.#events.append(taskRunId, "artifact.created", {
            artifact_id: created.artifact_id,
            artifact_type: created.artifact_type,
            schema_version: created.schema_version,
          });
        }
        return { artifact, queryResult, technical, technicalPayload, charts, bundle, reportBundlePayload };
      });
      if (this.#forge.createReport === undefined || this.#forge.getReport === undefined) {
        throw new Error("Forge Report Service is not configured");
      }
      let publication = await this.#forge.createReport({
        report_id: assembled.reportBundlePayload.report_id,
        task_run_id: taskRunId,
        org_id: stageTask.org_id,
        team_id: stageTask.team_id,
        user_id: stageTask.user_id,
        revision: assembled.reportBundlePayload.revision,
        bundle_hash: assembled.reportBundlePayload.bundle_hash,
        title: payload.title,
        business_report: payload,
        analysis: analysis.payload,
        query_result: assembled.queryResult.payload,
        charts: assembled.charts.map((chart) => chart.payload),
        technical_report: assembled.technicalPayload,
      }, input.idempotencyKey ?? `${taskRunId}:publish:${assembled.bundle.artifact_id}`, stageExecution.signal);
      while (publication.status === "publishing") {
        if (stageExecution.timedOut()) throw new Error("Report publication Stage timed out");
        await new Promise((resolve) => setTimeout(resolve, 500));
        publication = await this.#forge.getReport(publication.report_id, stageExecution.signal);
      }
      if (publication.status !== "published") throw new Error("Report publication failed");
      return this.#transactions.run(() => {
        const publicationArtifact = this.#artifacts.create({
          artifactType: "publication",
          taskRunId,
          producer: "forge-report-service",
          payload: {
            report_id: publication.report_id,
            revision: publication.revision,
            bundle_hash: publication.bundle_hash,
            status: "published",
            internal_url: publication.internal_url,
            technical_url: publication.technical_url,
            pdf: { status: publication.pdf_status, url: publication.pdf_url },
            pptx: { status: publication.pptx_status, url: publication.pptx_url },
            published_at: publication.updated_at,
          },
        });
        this.#events.append(taskRunId, "artifact.created", {
          artifact_id: publicationArtifact.artifact_id,
          artifact_type: publicationArtifact.artifact_type,
          schema_version: publicationArtifact.schema_version,
        });
        const finalizedTask = this.#transition(stageTask, "completed", "report_complete");
        this.#events.append(taskRunId, "report.completed", {
          artifact_id: assembled.artifact.artifact_id,
          publication_artifact_id: publicationArtifact.artifact_id,
          report_id: publication.report_id,
          status: payload.status,
        });
        this.#advanceExecutionPlan(taskRunId, { report: "completed" });
        this.#finishAttempt(attempt, "succeeded");
        return { task: finalizedTask, artifact: assembled.artifact, events: this.#events.list(taskRunId) };
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown Skill error";
      this.#transactions.run(() => {
        if (stageExecution.timedOut()) {
          this.#finishAttempt(attempt, "timed_out", message);
          stageTask = this.#transition(stageTask, "ready_for_report", "report_retry");
        } else {
          this.#finishAttempt(attempt, "failed", message);
          stageTask = this.#transition(stageTask, "failed", "data_analysis_report");
        }
        this.#events.append(taskRunId, "skill.execution_failed", {
          skill: "data-analysis-report-writer",
          timed_out: stageExecution.timedOut(),
          error: message,
        });
      });
      throw new TaskStateError(`Report rendering failed for ${taskRunId}`, {
        cause: error,
      });
    }
  }

  #channelPayloadString(event: ChannelEventInput, field: string): string {
    const value = event.payload[field];
    if (typeof value !== "string" || value.trim().length === 0) {
      throw new TaskStateError(`Channel action ${field} must not be empty`);
    }
    return value;
  }

  #assertSkillEnabled(task: TaskRun, skillName: AuthorizedSkillName): void {
    if (!this.#skillPolicies.isEnabled(task.org_id, task.team_id, skillName)) {
      throw new TaskStateError(`Skill is disabled for this team: ${skillName}`);
    }
  }

  #startAttempt(
    task: TaskRun,
    stage: string,
    retryStatus: TaskRun["status"],
    idempotencyKey: string,
  ): StageAttempt | undefined {
    if (this.#attempts === undefined) return undefined;
    const modelRevision = computePiModelRevision({
      agentDir: this.#config.agentDir,
      provider: this.#config.piModelProvider,
      modelId: this.#config.piModelId,
    });
    if (modelRevision?.startsWith("unresolved:") === true) {
      throw new TaskStateError("Pi model catalog is unavailable; Stage revision cannot be fixed");
    }
    const attempt = this.#attempts.start({
      taskRunId: task.task_run_id,
      stage,
      idempotencyKey,
      runningStatus: task.status,
      retryStatus,
      leaseMs: this.#stageLeaseMs,
      modelRevision,
      skillPolicyVersion: this.#skillPolicies.get(task.org_id, task.team_id)?.version ?? 0,
    });
    this.#events.append(task.task_run_id, "stage.attempt_started", {
      attempt_id: attempt.attempt_id,
      stage,
      attempt_number: attempt.attempt_number,
      lease_expires_at: attempt.lease_expires_at,
    });
    return attempt;
  }

  #finishAttempt(
    attempt: StageAttempt | undefined,
    status: "succeeded" | "failed" | "timed_out",
    error?: string,
  ): void {
    if (attempt === undefined || this.#attempts === undefined) return;
    const finished = this.#attempts.finish(attempt.attempt_id, status, error);
    const eventType =
      status === "succeeded"
        ? "stage.attempt_succeeded"
        : status === "timed_out"
          ? "stage.attempt_timed_out"
          : "stage.attempt_failed";
    this.#events.append(attempt.task_run_id, eventType, {
      attempt_id: attempt.attempt_id,
      stage: attempt.stage,
      status: finished.status,
      ...(error === undefined ? {} : { error: error.slice(0, 2_000) }),
    });
  }

  #stageSignal(parent?: AbortSignal): { signal: AbortSignal; timedOut: () => boolean } {
    const timeoutSignal = AbortSignal.timeout(this.#stageTimeoutMs);
    return {
      signal: parent === undefined ? timeoutSignal : AbortSignal.any([parent, timeoutSignal]),
      timedOut: () => timeoutSignal.aborted,
    };
  }

  #advanceExecutionPlan(
    taskRunId: string,
    updates: Partial<Record<PlanCapability, PlanStepStatus>>,
  ): Artifact | undefined {
    const current = this.#artifacts.latest(taskRunId, "execution_plan");
    if (current === undefined) return undefined;
    const payload = reviseExecutionPlan(current, updates);
    const revised = this.#artifacts.create({
      artifactType: "execution_plan",
      taskRunId,
      producer: "pi-planner",
      payload,
    });
    this.#events.append(taskRunId, "plan.revised", {
      artifact_id: revised.artifact_id,
      supersedes_artifact_id: current.artifact_id,
      plan_revision: payload.plan_revision,
      status: payload.status,
    });
    return revised;
  }

  #completeChannelResponse(task: TaskRun, title: string, markdown: string): TaskRun {
    return this.#transactions.run(() => {
      this.#events.append(task.task_run_id, "channel.response_created", {
        title: title.slice(0, 200),
        markdown: markdown.slice(0, 8_000),
      });
      return this.#transition(task, "completed", "channel_response");
    });
  }

  #transition(task: TaskRun, status: TaskRun["status"], stage: string): TaskRun {
    return this.#transactions.run(() => {
      const updated = this.#tasks.transition({
        taskRunId: task.task_run_id,
        expectedStatus: task.status,
        status,
        currentStage: stage,
      });
      this.#events.append(task.task_run_id, "task.status_changed", {
        from: task.status,
        to: updated.status,
        current_stage: stage,
      });
      return updated;
    });
  }
}
