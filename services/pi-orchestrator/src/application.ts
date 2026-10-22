import { createHash } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { OrchestratorState, type StateTransactionPort } from "./orchestrator-state.js";
import { SqliteOrchestratorState } from "./sqlite-store.js";
import { currentRequestId } from "./request-context.js";

import { assertSameChannelEvent, channelEventScope, type ChannelEventStore } from "./channels/event-store.js";
import type { ChannelEventInput, ChannelPresentation } from "./channels/contracts.js";
import { renderChannelPresentation } from "./channels/renderer.js";
import { routeChannelMessage } from "./channels/intent.js";
import {
  buildExecutionPlan,
  reviseExecutionPlan,
  settleExecutionPlan,
  type PlanCapability,
  type PlanStepStatus,
} from "./planning.js";
import { buildChartPayload, bundleHash, type ReportBundlePayload, type TechnicalReportPayload } from "./report-artifacts.js";
import { attemptModelStage, resolveStageModelBinding } from "./model-bindings.js";
import {
  type Artifact,
  type ArtifactStore,
} from "./artifacts.js";
import { computePiModelRevision, type OrchestratorConfig } from "./config.js";
import { ForgeClientError, type ForgeDialect } from "./forge/client.js";
import {
  ProductProjectionService,
  type ConversationListProjection,
  type ProductConversationChannel,
} from "./product-projection-builder.js";
import type { ConversationDetailV1, TaskDetailProjectionV1 } from "./product-projections.js";
import {
  ForgeQueryRunClient,
  type ContextSearchResult,
  type ReportPublication,
  type QueryCandidateInput,
  type QueryRunReview,
  type QueryRunResult,
} from "./forge/query-run-client.js";
import {
  PiStructuredSkillExecutor,
  type StructuredSkillExecutionPort,
} from "./skill-executor.js";
import type { StageAttempt, StageAttemptStore, StageProgressPhase } from "./stage-attempts.js";
import type {
  AdvisoryPayload,
  AnalysisPayload,
  QueryResultPayload,
  RenderedOutputPayload,
} from "./structured-artifact-tools.js";
import {
  AUTHORIZED_SKILL_NAMES,
  type AdvisorySkillName,
  type AuthorizedSkillName,
} from "./skills.js";
import type { SkillPolicyStore, TeamSkillPolicy } from "./skill-policy.js";
import {
  TaskStateError,
  type CreateTaskInput,
  type TaskChannel,
  type TaskRun,
  type TaskStatus,
  type TaskStore,
} from "./task-store.js";
import {
  type TaskEvent,
  type TaskEventStore,
} from "./task-events.js";

export interface CreateDataTaskInput extends CreateTaskInput {
  message: string;
}

export interface TrustedRequestPrincipal {
  principal_id: string;
  principal_type: "human" | "agent" | "service";
  organization_id: string;
  workspace_id: string;
  role: "admin" | "reviewer" | "member";
  identity_revision: number;
  credential_id: string | null;
  credential_revision: number | null;
  authentication_method: string;
  authenticated_at: string;
  channel_binding_id: string | null;
  channel_binding_revision: number | null;
}

export interface ForgeQueryRunPort {
  searchContext?(input: { taskRunId: string; question: string; limit?: number }, signal?: AbortSignal): Promise<ContextSearchResult>;
  createQueryRun(input: { taskRunId: string; mandateId: string; purpose: string; question: string; dialect?: ForgeDialect; candidate?: QueryCandidateInput; idempotencyKey: string }, signal?: AbortSignal): Promise<QueryRunReview>;
  getQueryRun(queryRunId: string, signal?: AbortSignal): Promise<QueryRunReview>;
  executeQueryRun(input: { queryRunId: string; sqlHash: string; assuranceReportHash: string; enforcementContextHash: string; idempotencyKey: string }, signal?: AbortSignal): Promise<QueryRunReview>;
  approveQueryRun(input: { queryRunId: string; sqlHash: string; assuranceReportHash: string; enforcementContextHash: string; idempotencyKey: string; controlToken: string }, signal?: AbortSignal): Promise<QueryRunReview>;
  getMandate(mandateId: string, signal?: AbortSignal): Promise<Record<string, unknown>>;
  cancelQueryRun?(input: { queryRunId: string }, signal?: AbortSignal): Promise<QueryRunReview>;
  createReport?(input: Record<string, unknown>, idempotencyKey: string, signal?: AbortSignal): Promise<ReportPublication>;
  getReport?(reportId: string, taskRunId: string, signal?: AbortSignal): Promise<ReportPublication>;
  writeMemory?(input: Record<string, unknown>, signal?: AbortSignal): Promise<Record<string, unknown>>;
}

export class OrchestratorApplication {
  readonly #tasks: TaskStore;
  readonly #events: TaskEventStore;
  readonly #forge: ForgeQueryRunPort;
  readonly #skills: StructuredSkillExecutionPort;
  readonly #artifacts: ArtifactStore;
  readonly #transactions: StateTransactionPort;
  readonly #attempts: StageAttemptStore;
  readonly state: OrchestratorState;
  readonly #activeStages = new Map<string, { controller: AbortController; dispose: () => void }>();
  readonly #activeQueryObservers = new Set<string>();
  readonly #recoveringQueryRuns = new Set<string>();
  readonly #stageTimeoutMs: number;
  readonly #stageLeaseMs: number;
  readonly #channelEvents: ChannelEventStore;
  readonly #config: OrchestratorConfig;
  readonly #skillPolicies: SkillPolicyStore;
  readonly #productProjections: ProductProjectionService;

  constructor(options: {
    config: OrchestratorConfig;
    state?: OrchestratorState;
    forgeClient?: ForgeQueryRunPort;
    skillExecutor?: StructuredSkillExecutionPort;
  }) {
    if (options.state !== undefined && !(options.state instanceof OrchestratorState)) {
      throw new TypeError("Application state must be one coherent OrchestratorState adapter");
    }
    this.state = options.state ?? new SqliteOrchestratorState(":memory:");
    this.#tasks = this.state.tasks;
    this.#events = this.state.events;
    this.#artifacts = this.state.artifacts;
    this.#transactions = this.state.transactions;
    this.#attempts = this.state.attempts;
    this.#stageTimeoutMs = options.config.stageTimeoutMs;
    this.#stageLeaseMs = options.config.stageLeaseMs;
    this.#channelEvents = this.state.channelEvents;
    this.#config = options.config;
    this.#skillPolicies = this.state.skillPolicies;
    this.#productProjections = new ProductProjectionService({
      tasks: this.#tasks,
      events: this.#events,
      artifacts: this.#artifacts,
      attempts: this.#attempts,
    });
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

  createTask(input: CreateDataTaskInput, principal: TrustedRequestPrincipal): { task: TaskRun; events: TaskEvent[] } {
    if (principal.organization_id !== input.org_id || principal.workspace_id !== input.team_id ||
      principal.principal_id !== input.user_id || principal.principal_type !== "human")
      throw new TaskStateError("Task requester does not match authenticated principal");
    if (input.parent_task_run_id && (input.metadata?.mandate_id || input.metadata?.execution_actor_id))
      throw new TaskStateError("Child TaskRun cannot inherit parent mandate");
    if (input.metadata?.mandate_id || input.metadata?.execution_actor_id)
      throw new TaskStateError("Task creation cannot grant execution authority");
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

  async attachMandate(taskRunId: string, input: { mandateId: string; expectedMandateId: string | null }, principal: TrustedRequestPrincipal): Promise<TaskRun> {
    const task = this.#tasks.get(taskRunId);
    if (!task) throw new TaskStateError("TaskRun not found");
    if (principal.principal_type !== "human" || principal.role !== "admin" ||
      principal.organization_id !== task.org_id || principal.workspace_id !== task.team_id)
      throw new TaskStateError("Only task-scope admin can attach mandate");
    if (!this.#config.executionPrincipalId) throw new TaskStateError("Pi execution principal is not registered");
    const mandate = await this.#forge.getMandate(input.mandateId);
    const delegated = mandate.delegated_mandate as Record<string, unknown> | undefined;
    const delegate = delegated?.delegate_principal as Record<string, unknown> | undefined;
    const accountable = delegated?.accountable_principal as Record<string, unknown> | undefined;
    if (mandate.status !== "active" || delegated?.task_run_id !== taskRunId ||
      delegated.organization_id !== task.org_id || delegated.workspace_id !== task.team_id ||
      delegate?.principal_id !== this.#config.executionPrincipalId || accountable?.principal_id !== task.user_id)
      throw new TaskStateError("Mandate does not authorize this TaskRun and execution actor");
    const previous = typeof task.metadata.mandate_id === "string" ? task.metadata.mandate_id : null;
    if (previous && previous !== input.mandateId) {
      const old = await this.#forge.getMandate(previous);
      if (old.status === "active") throw new TaskStateError("Old mandate must be revoked before replacement");
      if (task.status === "querying") throw new TaskStateError("Cannot replace mandate while query is executing");
    }
    const attached = this.#transactions.run(() => {
      const current = this.#tasks.get(taskRunId);
      if (!current) throw new TaskStateError("TaskRun not found");
      const old = typeof current.metadata.mandate_id === "string" ? current.metadata.mandate_id : null;
      const next = this.#tasks.attachMandate(taskRunId, input.expectedMandateId, input.mandateId,
        this.#config.executionPrincipalId!);
      if (old !== input.mandateId) this.#events.append(taskRunId, "task.mandate_attached", {
        mandate_id: input.mandateId, execution_actor_id: this.#config.executionPrincipalId,
      });
      return next;
    });
    if (attached.status === "needs_input" && attached.current_stage === "query_authorization") {
      const pending = [...this.#events.list(taskRunId)].reverse()
        .find(event => event.event_type === "query.authorization_required");
      if (pending && typeof pending.payload.question === "string") {
        await this.prepareQuery(taskRunId, {
          question: pending.payload.question, dialect: pending.payload.dialect as ForgeDialect,
          ...(pending.payload.candidate ? { candidate: pending.payload.candidate as QueryCandidateInput } : {}),
          ...(typeof pending.payload.command_key === "string" ? { idempotencyKey: pending.payload.command_key } : {}),
        });
      }
    }
    return this.#tasks.get(taskRunId)!;
  }

  transitionBenchmarkTask(input: {
    taskRunId: string;
    runId: string;
    status: "queued" | "running" | "pausing" | "paused" | "stopping" | "completed" | "stopped" | "failed" | "interrupted";
    currentStage: string;
  }): TaskRun {
    return this.#transactions.run(() => {
      const task = this.#tasks.get(input.taskRunId);
      if (task === undefined || task.channel !== "api" || task.metadata.benchmark !== true ||
          task.metadata.benchmark_run_id !== input.runId) {
        throw new TaskStateError("Benchmark lifecycle does not match its TaskRun");
      }
      const status = input.status === "queued" ? "created"
        : input.status === "running" || input.status === "paused" || input.status === "pausing" || input.status === "stopping" ? "analyzing"
        : input.status === "stopped" ? "cancelled"
        : input.status === "interrupted" ? "failed" : input.status;
      if (task.status === status && task.current_stage === input.currentStage) return task;
      return this.#transition(task, status, input.currentStage);
    });
  }

  getTask(taskRunId: string): TaskRun | undefined {
    return this.#tasks.get(taskRunId);
  }

  listTasks(options: {
    orgId: string;
    teamId: string;
    userId?: string;
    channel?: TaskChannel;
    status?: TaskStatus;
    limit: number;
  }): TaskRun[] {
    return this.#tasks.list(options);
  }

  listConversationProjections(input: {
    orgId: string;
    teamId: string;
    userId: string;
    channel: ProductConversationChannel;
    limit: number;
    cursor?: string;
  }): ConversationListProjection {
    return this.#productProjections.listConversations(input);
  }

  getConversationProjection(input: {
    orgId: string;
    teamId: string;
    userId: string;
    channel: ProductConversationChannel;
    conversationId: string;
    cursor?: string;
  }): ConversationDetailV1 | undefined {
    return this.#productProjections.getConversation(input);
  }

  getTaskDetailProjection(input: {
    orgId: string;
    teamId: string;
    userId: string;
    channel: TaskChannel;
    taskRunId: string;
  }): TaskDetailProjectionV1 | undefined {
    return this.#productProjections.getTaskDetail(input);
  }

  getQueryReviewProjection(taskRunId: string, principal: TrustedRequestPrincipal): {
    task_run_id: string; query_run_id: string; sql: string; sql_hash: string;
    assurance_report_hash: string; enforcement_context_hash: string;
    assurance_report: unknown; mandate_id: string | null;
  } | undefined {
    const task = this.#tasks.get(taskRunId);
    if (!task || task.status !== "waiting_for_query_approval" || principal.principal_type !== "human" ||
      principal.organization_id !== task.org_id || principal.workspace_id !== task.team_id) return undefined;
    const review = [...this.#events.list(taskRunId)].reverse()
      .find(event => event.event_type === "query.review_requested" && event.payload.review_required === true);
    if (!review || review.payload.reviewer_principal_id !== principal.principal_id ||
      typeof review.payload.query_run_id !== "string" || typeof review.payload.sql !== "string" ||
      typeof review.payload.sql_hash !== "string" ||
      typeof review.payload.assurance_report_hash !== "string" ||
      typeof review.payload.enforcement_context_hash !== "string") return undefined;
    return {
      task_run_id: taskRunId, query_run_id: review.payload.query_run_id,
      sql: review.payload.sql, sql_hash: review.payload.sql_hash,
      assurance_report_hash: review.payload.assurance_report_hash,
      enforcement_context_hash: review.payload.enforcement_context_hash,
      assurance_report: review.payload.assurance_report,
      mandate_id: typeof task.metadata.mandate_id === "string" ? task.metadata.mandate_id : null,
    };
  }

  #channelReviewResponse(task: TaskRun, principal: TrustedRequestPrincipal, duplicate: boolean): {
    task: TaskRun; presentation: ChannelPresentation; duplicate: boolean;
  } {
    const review = this.getQueryReviewProjection(task.task_run_id, principal);
    if (!review) throw new TaskStateError("Reviewer cannot access this query");
    return { task: { ...task, metadata: { mandate_id: review.mandate_id } }, duplicate,
      presentation: { kind: "query_review", task_run_id: task.task_run_id,
        title: "查询审核", markdown: "查询审核已提交，执行状态请稍后回读。",
        fields: [{ label: "SQL", value: review.sql },
          { label: "SQL hash", value: review.sql_hash },
          { label: "Assurance hash", value: review.assurance_report_hash }],
        table: null, actions: [], source_event_sequence: 0, source_artifact_ids: [] } };
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
    const advise = this.#skills.advise;
    if (advise === undefined) {
      throw new TaskStateError("Advisory Skill runtime is unavailable");
    }
    if (!this.#skillPolicies.isEnabled(task.org_id, task.team_id, input.skillName)) {
      throw new TaskStateError(`Skill is disabled for this team: ${input.skillName}`);
    }
    const attemptRound = this.#attempts.list(taskRunId).filter(
      (attempt) => attempt.stage === `skill:${input.skillName}`,
    ).length + 1;
    const idempotencyKey = input.idempotencyKey ?? `${taskRunId}:skill:${input.skillName}:${attemptRound}`;
    const existingAttempt = this.#attempts.findByIdempotencyKey(taskRunId, idempotencyKey);
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
    let attempt!: StageAttempt;
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
    const stageExecution = this.#stageSignal(attempt, signal);
    try {
      const queryResults = this.#artifacts.list(taskRunId).filter(
        (artifact): artifact is Artifact<QueryResultPayload> => artifact.artifact_type === "query_result",
      );
      const payload = await stageExecution.wait(() => advise.call(this.#skills, running, input.skillName, {
        prompt: input.prompt,
        queryResults,
        ...(input.contextEvidence === undefined ? {} : { contextEvidence: input.contextEvidence }),
      }, stageExecution.signal, attempt.model_revision));
      if (stageExecution.timedOut()) throw new Error("Advisory Skill Stage timed out");
      return this.#commitStage(attempt, () => {
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
        this.#finishAttempt(attempt, "succeeded", undefined, { artifact_id: artifact.artifact_id });
        this.#events.append(taskRunId, "skill.completed", {
          skill_name: input.skillName,
          artifact_id: artifact.artifact_id,
          attempt_id: attempt.attempt_id,
          idempotency_key: idempotencyKey,
          status: payload.status,
        });
        return { task: completed, artifact };
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown Skill error";
      this.#transactions.run(() => {
        if (!this.#ownsAttempt(attempt)) { this.#releaseAttempt(attempt); return; }
        if (stageExecution.timedOut() || stageExecution.interrupted()) {
          this.#finishAttempt(attempt, stageExecution.timedOut() ? "timed_out" : "interrupted", message);
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
    return this.#attempts.list(taskRunId);
  }

  getTaskRunIdForChannelEvent(
    channel: ChannelEventInput["channel"],
    eventId: string,
    principal: TrustedRequestPrincipal,
  ): string | null {
    return this.#channelEvents.get(channel, channelEventScope(principal), eventId)?.task_run_id ?? null;
  }

  /** A replayed event may only return a TaskRun in the caller's own tenant scope. */
  #assertChannelReplayAccess(task: TaskRun, principal: TrustedRequestPrincipal, requireOwner: boolean): void {
    if (task.org_id !== principal.organization_id || task.team_id !== principal.workspace_id ||
      (requireOwner && task.user_id !== principal.principal_id)) {
      throw new TaskStateError("Channel event cannot access this TaskRun");
    }
  }

  getChannelPresentation(taskRunId: string, principal?: TrustedRequestPrincipal): ChannelPresentation {
    const task = this.#tasks.get(taskRunId);
    if (task === undefined) throw new TaskStateError(`TaskRun not found: ${taskRunId}`);
    const events = this.#events.list(taskRunId);
    const presentation = renderChannelPresentation({ task, events, artifacts: this.#artifacts.list(taskRunId) });
    if (presentation.kind !== "query_review") return presentation;
    const review = [...events].reverse().find(event => event.event_type === "query.review_requested");
    return principal?.principal_id === review?.payload.reviewer_principal_id ? presentation : {
      ...presentation, actions: presentation.actions.filter(action => action.type !== "approve_query"),
    };
  }

  async ingestChannelEvent(event: ChannelEventInput, principal: TrustedRequestPrincipal,
    controlToken?: string, signal?: AbortSignal): Promise<{ task: TaskRun; presentation: ChannelPresentation; duplicate: boolean }> {
    if (event.event_type === "message") return this.ingestChannelMessage(event, principal, signal);
    return this.ingestChannelAction(event, principal, controlToken, signal);
  }

  async ingestChannelMessage(
    event: ChannelEventInput,
    principal: TrustedRequestPrincipal,
    signal?: AbortSignal,
  ): Promise<{ task: TaskRun; presentation: ChannelPresentation; duplicate: boolean }> {
    if (principal.principal_type !== "human" ||
      (event.channel === "web" ? (!principal.credential_id || event.external_user_id !== principal.principal_id) :
        principal.channel_binding_id === null))
      throw new TaskStateError("Authenticated channel human required");
    if (event.event_type !== "message") {
      throw new TaskStateError(`Unsupported ChannelEvent type: ${event.event_type}`);
    }
    const text = event.payload.text;
    if (typeof text !== "string" || text.trim().length === 0) {
      throw new TaskStateError("Channel message text must not be empty");
    }

    const route = routeChannelMessage(text);
    const scope = channelEventScope(principal);
    const claimed = this.#transactions.run(() => {
      const claim = this.#channelEvents.claim(event, scope);
      if (!claim.created) return { record: claim.record, task: undefined };
      const created = this.createTask({
        org_id: principal.organization_id,
        team_id: principal.workspace_id,
        user_id: principal.principal_id,
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
      }, principal);
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
      this.#channelEvents.complete(event.channel, scope, event.event_id, created.task.task_run_id);
      return { record: claim.record, task: created.task };
    });

    if (claimed.task === undefined) {
      assertSameChannelEvent(claimed.record, event);
      const taskRunId = claimed.record.task_run_id;
      if (taskRunId === null) {
        throw new TaskStateError("ChannelEvent is already being processed");
      }
      const task = this.#tasks.get(taskRunId);
      if (task === undefined) throw new TaskStateError(`TaskRun not found: ${taskRunId}`);
      this.#assertChannelReplayAccess(task, principal, true);
      return {
        task,
        presentation: this.getChannelPresentation(task.task_run_id, principal),
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
        presentation: this.getChannelPresentation(completed.task_run_id, principal),
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
        presentation: this.getChannelPresentation(waiting.task_run_id, principal),
        duplicate: false,
      };
    }

    if (route.kind === "action") {
      if (route.action !== "memory") {
        this.#advanceExecutionPlan(claimed.task.task_run_id, { registry_draft: "running" });
        const completed = this.#completeChannelResponse(
          claimed.task,
          "需要确认 Registry 变更",
          "请提供指标或业务规则的完整定义。我会先生成 Registry Draft 和差异，确认后才会发布。",
        );
        return { task: completed, presentation: this.getChannelPresentation(completed.task_run_id, principal), duplicate: false };
      }
      const forgetting = /(?:忘记|删除.*记忆|清除.*记忆)/i.test(text);
      const memoryValue = text
        .replace(/^(?:请)?(?:记住|记下来|以后都用|忘记|删除.*?记忆|清除.*?记忆)\s*/i, "")
        .trim();
      if (memoryValue.length === 0) {
        const waiting = this.#transactions.run(() => {
          this.#events.append(claimed.task!.task_run_id, "query.clarification_requested", {
            prompt: "请说明希望记住或删除的具体内容。",
          });
          return this.#transition(claimed.task!, "needs_input", "memory_clarification");
        });
        return { task: waiting, presentation: this.getChannelPresentation(waiting.task_run_id, principal), duplicate: false };
      }
      const category = /(?:默认|偏好|习惯)/.test(memoryValue) ? "user_profile" : "confirmed_fact";
      const memoryKey = `channel_${createHash("sha256").update(memoryValue).digest("hex").slice(0, 24)}`;
      const expiresAt = new Date(Date.now() + 180 * 24 * 60 * 60 * 1_000).toISOString();
      const waiting = this.#transactions.run(() => {
        this.#events.append(claimed.task!.task_run_id, "memory.proposed", {
          operation: forgetting ? "delete" : "upsert",
          scope: "user",
          category,
          key: memoryKey,
          value: memoryValue.slice(0, 2_000),
          expires_at: expiresAt,
        });
        this.#advanceExecutionPlan(claimed.task!.task_run_id, {
          memory_proposal: "completed", approval: "waiting_approval",
        });
        return this.#transition(claimed.task!, "waiting_for_action_approval", "memory_approval");
      });
      return { task: waiting, presentation: this.getChannelPresentation(waiting.task_run_id, principal), duplicate: false };
    }

    if (route.kind === "knowledge") {
      const context = this.#forge.searchContext === undefined
        ? undefined
        : await this.#forge.searchContext({
            taskRunId: claimed.task.task_run_id,
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
          presentation: this.getChannelPresentation(completed.task_run_id, principal),
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
      await this.#persistSessionSummary(
        advised.task,
        String(advised.artifact.payload.summary ?? "知识回答已完成").slice(0, 1_000),
        advised.artifact.artifact_id,
        signal,
      );
      return {
        task: advised.task,
        presentation: this.getChannelPresentation(advised.task.task_run_id, principal),
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
      presentation: this.getChannelPresentation(task.task_run_id, principal),
      duplicate: false,
    };
  }

  async ingestChannelAction(
    event: ChannelEventInput,
    principal: TrustedRequestPrincipal,
    controlToken?: string,
    signal?: AbortSignal,
  ): Promise<{ task: TaskRun; presentation: ChannelPresentation; duplicate: boolean }> {
    if (principal.principal_type !== "human" ||
      (event.channel === "web" ? (!principal.credential_id || event.external_user_id !== principal.principal_id) :
        principal.channel_binding_id === null))
      throw new TaskStateError("Authenticated channel human required");
    if (event.event_type !== "action" || event.task_run_id === null) {
      throw new TaskStateError("Channel action requires task_run_id");
    }
    const task = this.#tasks.get(event.task_run_id);
    if (task === undefined) throw new TaskStateError(`TaskRun not found: ${event.task_run_id}`);
    if (
      task.channel !== event.channel ||
      (event.payload.action !== "approve_query" && task.channel_conversation_id !== event.conversation_id) ||
      task.org_id !== principal.organization_id ||
      task.team_id !== principal.workspace_id ||
      (event.payload.action !== "approve_query" && task.user_id !== principal.principal_id)
    ) {
      throw new TaskStateError("Channel action cannot access this TaskRun");
    }
    const actionType = event.payload.action;
    if (![
      "approve_query",
      "analyze",
      "render_report",
      "provide_input",
      "cancel_task",
      "request_supplement",
      "confirm_memory",
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
    const enforcementContextHash = actionType === "approve_query"
      ? this.#channelPayloadString(event, "enforcement_context_hash")
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

    const scope = channelEventScope(principal);
    const claim = this.#transactions.run(() => {
      const claimed = this.#channelEvents.claim(event, scope);
      return claimed;
    });
    if (!claim.created) {
      assertSameChannelEvent(claim.record, event);
      const recordedTask = claim.record.task_run_id === null
        ? task
        : this.#tasks.get(claim.record.task_run_id) ?? task;
      this.#assertChannelReplayAccess(recordedTask, principal, actionType !== "approve_query");
      if (actionType === "approve_query" && recordedTask.user_id !== principal.principal_id)
        return this.#channelReviewResponse(recordedTask, principal, true);
      return {
        task: recordedTask,
        presentation: this.getChannelPresentation(recordedTask.task_run_id, principal),
        duplicate: true,
      };
    }

    try {
      let responseTask = task;
      if (actionType === "approve_query" && queryRunId && sqlHash && assuranceReportHash &&
        enforcementContextHash && controlToken) {
        await this.approveQuery(task.task_run_id, {
          queryRunId, sqlHash, assuranceReportHash, enforcementContextHash,
          idempotencyKey: event.channel + ":" + event.event_id + ":approve", controlToken,
        }, principal, signal);
      } else if (actionType === "approve_query") {
        throw new TaskStateError("Channel approval requires exact hashes and fresh control token");
      } else if (actionType === "analyze") {
      const analyzed = await this.analyzeTask(
        task.task_run_id,
        { idempotencyKey: `${event.channel}:${event.event_id}:analysis` },
        signal,
      );
      await this.#persistSessionSummary(
        analyzed.task,
        String(analyzed.artifact.payload.summary ?? "分析已完成"),
        analyzed.artifact.artifact_id,
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
    } else if (actionType === "confirm_memory") {
      if (task.status !== "waiting_for_action_approval" || this.#forge.writeMemory === undefined) {
        throw new TaskStateError("Memory proposal is not ready for confirmation");
      }
      const proposal = [...this.#events.list(task.task_run_id)].reverse()
        .find((candidate) => candidate.event_type === "memory.proposed");
      if (proposal === undefined) throw new TaskStateError("Memory proposal not found");
      await this.#forge.writeMemory({
        task_run_id: task.task_run_id,
        operation: proposal.payload.operation,
        category: proposal.payload.category,
        key: proposal.payload.key,
        value: proposal.payload.value,
        source_session: task.task_run_id,
        source_revision: proposal.event_id,
        confidence: 1,
        expires_at: proposal.payload.expires_at,
      }, signal);
      this.#events.append(task.task_run_id, "memory.confirmed", {
        operation: proposal.payload.operation,
        scope: "user",
        category: proposal.payload.category,
      });
      this.#advanceExecutionPlan(task.task_run_id, { approval: "completed" });
      responseTask = this.#completeChannelResponse(
        task,
        "个人记忆已更新",
        proposal.payload.operation === "delete"
          ? "已删除匹配的个人记忆（如果存在）。"
          : "已保存为有期限的个人记忆。它不会自动成为团队或组织事实。",
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
    this.#channelEvents.complete(event.channel, scope, event.event_id, updated.task_run_id);
    if (actionType === "approve_query" && updated.user_id !== principal.principal_id)
      return this.#channelReviewResponse(updated, principal, false);
    return {
      task: updated,
      presentation: this.getChannelPresentation(updated.task_run_id, principal),
      duplicate: false,
    };
    } catch (error) {
      this.#channelEvents.fail(
        event.channel,
        scope,
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
        { queryRunId },
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
      for (const attempt of this.#attempts.list(taskRunId)) {
        if (attempt.status !== "running") continue;
        this.#activeStages.get(attempt.attempt_id)?.controller.abort(new Error("Task cancelled"));
        this.#finishAttempt(attempt, "interrupted", "Task cancelled by user; remote effects may already have occurred");
      }
      settleExecutionPlan(this.state, cancelled);
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
    const commandKey = this.#commandKey(taskRunId, "requirement_clarification", input.idempotencyKey);
    const replay = this.#replayStage<Awaited<ReturnType<OrchestratorApplication["clarifyRequirement"]>>>(taskRunId, commandKey);
    if (replay !== undefined) return replay;
    this.#assertSkillEnabled(initialTask, "data-requirement-clarifier");
    if (initialTask.status !== "created" && initialTask.status !== "needs_input") {
      throw new TaskStateError(`TaskRun cannot clarify from status: ${initialTask.status}`);
    }
    const retryStatus = initialTask.status;

    let attempt!: StageAttempt;
    let task = this.#transactions.run(() => {
      const running = this.#transition(initialTask, "clarifying", "requirement_clarification");
      attempt = this.#startAttempt(
        running,
        "requirement_clarification",
        retryStatus,
        commandKey,
      );
      return running;
    });
    const stageExecution = this.#stageSignal(attempt, signal);
    try {
      const payload = await stageExecution.wait(() => this.#skills.clarify(task, input.message, stageExecution.signal, attempt.model_revision,));
      if (stageExecution.timedOut()) throw new Error("Clarification Stage timed out");
      return this.#commitStage(attempt, () => {
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
        this.#finishAttempt(attempt, "succeeded", undefined, { artifact_id: artifact.artifact_id });
        return { task, artifact, events: this.#events.list(taskRunId) };
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown Skill error";
      this.#transactions.run(() => {
        if (!this.#ownsAttempt(attempt)) { this.#releaseAttempt(attempt); return; }
        if (stageExecution.timedOut() || stageExecution.interrupted()) {
          this.#finishAttempt(attempt, stageExecution.timedOut() ? "timed_out" : "interrupted", message);
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
    const commandKey = this.#commandKey(taskRunId, "metric_definition_review", input.idempotencyKey);
    const replay = this.#replayStage<Awaited<ReturnType<OrchestratorApplication["reviewMetricDefinition"]>>>(taskRunId, commandKey);
    if (replay !== undefined) return replay;
    this.#assertSkillEnabled(initialTask, "metric-definition-reviewer");
    if (
      initialTask.status !== "created" &&
      initialTask.status !== "needs_input" &&
      initialTask.status !== "ready_for_query"
    ) {
      throw new TaskStateError(`TaskRun cannot review a metric from status: ${initialTask.status}`);
    }
    const retryStatus = initialTask.status;

    let attempt!: StageAttempt;
    let task = this.#transactions.run(() => {
      const running = this.#transition(initialTask, "clarifying", "metric_definition_review");
      attempt = this.#startAttempt(
        running,
        "metric_definition_review",
        retryStatus,
        commandKey,
      );
      return running;
    });
    const stageExecution = this.#stageSignal(attempt, signal);
    try {
      const payload = await stageExecution.wait(() => this.#skills.reviewMetric(task, input.message, stageExecution.signal, attempt.model_revision,));
      if (stageExecution.timedOut()) throw new Error("Metric review Stage timed out");
      return this.#commitStage(attempt, () => {
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
        this.#finishAttempt(attempt, "succeeded", undefined, { artifact_id: artifact.artifact_id });
        return { task, artifact, events: this.#events.list(taskRunId) };
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown Skill error";
      this.#transactions.run(() => {
        if (!this.#ownsAttempt(attempt)) { this.#releaseAttempt(attempt); return; }
        if (stageExecution.timedOut() || stageExecution.interrupted()) {
          this.#finishAttempt(attempt, stageExecution.timedOut() ? "timed_out" : "interrupted", message);
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
    input: { question: string; dialect?: ForgeDialect; candidate?: QueryCandidateInput; idempotencyKey?: string },
    signal?: AbortSignal,
  ): Promise<{ task: TaskRun; result: QueryRunReview | null; events: TaskEvent[] }> {
    const loaded = this.#tasks.get(taskRunId);
    if (!loaded) throw new TaskStateError("TaskRun not found");
    let task: TaskRun = loaded;
    if (!input.question.trim()) throw new TaskStateError("question must not be empty");
    const commandKey = this.#commandKey(taskRunId, "query_prepare", input.idempotencyKey);
    const replay = this.#replayStage<Awaited<ReturnType<OrchestratorApplication["prepareQuery"]>>>(taskRunId, commandKey);
    if (replay) return replay;
    const mandateId = task.metadata.mandate_id;
    if (typeof mandateId !== "string" || !mandateId) {
      return this.#transactions.run(() => {
        if (task.status !== "created" && task.status !== "ready_for_query" && task.status !== "needs_input")
          throw new TaskStateError("Task cannot request authorization from current status");
        this.#events.append(taskRunId, "query.authorization_required", {
          question: input.question, dialect: input.dialect ?? "auto", candidate: input.candidate ?? null,
          command_key: input.idempotencyKey ?? null,
        });
        task = task.status === "needs_input" && task.current_stage === "query_authorization"
          ? task : this.#transition(task, "needs_input", "query_authorization");
        return { task, result: null, events: this.#events.list(taskRunId) };
      });
    }
    if (task.metadata.execution_actor_id !== this.#config.executionPrincipalId)
      throw new TaskStateError("Task execution actor does not match registered Pi principal");
    if (task.status === "created" || task.status === "needs_input") task = this.#transition(task, "ready_for_query", "query_prepare");
    if (task.status !== "ready_for_query") throw new TaskStateError("Task cannot prepare query from current status");
    let attempt!: StageAttempt;
    this.#transactions.run(() => { attempt = this.#startAttempt(task, "query_prepare", "ready_for_query", commandKey); });
    const stage = this.#stageSignal(attempt, signal);
    let result: QueryRunReview;
    let reviewerPrincipalId: string;
    try {
      const mandate = await stage.wait(() => this.#forge.getMandate(mandateId, stage.signal));
      const delegated = mandate.delegated_mandate as Record<string, unknown>;
      if (mandate.status !== "active" || delegated.task_run_id !== taskRunId ||
        delegated.organization_id !== task.org_id || delegated.workspace_id !== task.team_id)
        throw new TaskStateError("Mandate no longer authorizes task scope");
      reviewerPrincipalId = String((mandate.policy as Record<string, unknown>).reviewer_principal_id);
      result = await stage.wait(() => this.#forge.createQueryRun({
        taskRunId, mandateId, purpose: String(delegated.purpose), question: input.question,
        idempotencyKey: commandKey, ...(input.dialect ? { dialect: input.dialect } : {}),
        ...(input.candidate ? { candidate: input.candidate } : {}),
      }, stage.signal));
    } catch (error) {
      const message = error instanceof Error ? error.message : "Forge preparation failed";
      const clarification = error instanceof ForgeClientError && error.code === "candidate_needs_clarification";
      // Like other Stages, deadline and caller interruption stay retryable; only Forge failures are terminal.
      const retryable = stage.timedOut() || stage.interrupted();
      this.#transactions.run(() => {
        if (!this.#ownsAttempt(attempt)) { this.#releaseAttempt(attempt); return; }
        this.#finishAttempt(attempt, stage.timedOut() ? "timed_out" : stage.interrupted() ? "interrupted"
          : clarification ? "succeeded" : "failed", message);
        task = this.#transition(task, clarification ? "needs_input" : retryable ? "ready_for_query" : "failed",
          clarification ? "requirement_clarification" : "query_prepare_retry");
        this.#events.append(taskRunId, clarification ? "query.clarification_requested" : "query.prepare_failed",
          clarification ? { prompt: message } : { error: message, timed_out: stage.timedOut(), interrupted: stage.interrupted() });
      });
      if (clarification) return { task, result: null, events: this.#events.list(taskRunId) };
      throw new TaskStateError("Query preparation failed", { cause: error });
    }
    const committed = this.#commitStage(attempt, () => {
      if (result.status === "review_required" || result.status === "ready") {
        const review = result.review;
        if (!review.sql_hash || !review.assurance_report_hash || !review.enforcement_context_hash)
          throw new TaskStateError("Forge did not provide claim hashes");
        this.#events.append(taskRunId, "query.review_requested", {
          query_run_id: result.query_run_id, sql: review.sql, sql_hash: review.sql_hash,
          dialect: input.dialect ?? "sqlite", registry_version: result.lineage.registry_revision,
          input_kind: result.candidate.input_kind, candidate_revision: result.candidate.candidate_revision,
          assurance_report_hash: review.assurance_report_hash,
          enforcement_context_hash: review.enforcement_context_hash,
          assurance_report: result.assurance, reviewer_principal_id:
            reviewerPrincipalId,
          expires_at: review.expires_at, review_required: result.status === "review_required",
          can_execute: false,
        });
        task = this.#transition(task, result.status === "ready" ? "ready_for_query" : "waiting_for_query_approval",
          result.status === "ready" ? "query_execution" : "query_review");
      } else {
        task = this.#transition(task, "failed", "query_prepare");
        this.#events.append(taskRunId, "query.prepare_failed", { error: result.failure?.code ?? "query_denied" });
      }
      this.#finishAttempt(attempt, result.status === "ready" || result.status === "review_required" ? "succeeded" : "failed",
        result.failure?.code, { result });
      return { task, result, events: this.#events.list(taskRunId) };
    });
    if (result.status === "ready") await this.startQueryExecution(taskRunId, {
      queryRunId: result.query_run_id, sqlHash: result.review.sql_hash!,
      assuranceReportHash: result.review.assurance_report_hash!,
      enforcementContextHash: result.review.enforcement_context_hash!,
      idempotencyKey: commandKey + ":execute",
    }, "execute", signal);
    return { ...committed, task: this.#tasks.get(taskRunId)!, events: this.#events.list(taskRunId) };
  }

  async approveQuery(taskRunId: string, input: {
    queryRunId: string; sqlHash: string; assuranceReportHash: string;
    enforcementContextHash: string; idempotencyKey: string; controlToken: string;
  }, principal: TrustedRequestPrincipal, signal?: AbortSignal): Promise<{
    task: TaskRun; result: QueryRunReview; events: TaskEvent[];
  }> {
    const task = this.#tasks.get(taskRunId);
    if (!task) throw new TaskStateError("TaskRun not found");
    if (principal.principal_type !== "human" || !["reviewer", "admin"].includes(principal.role) ||
      principal.organization_id !== task.org_id || principal.workspace_id !== task.team_id)
      throw new TaskStateError("Reviewer not authorized for task");
    if (task.status !== "waiting_for_query_approval") throw new TaskStateError("Task is not awaiting review");
    return this.startQueryExecution(taskRunId, input, "approve", signal, principal);
  }

  async startQueryExecution(taskRunId: string, input: {
    queryRunId: string; sqlHash: string; assuranceReportHash: string;
    enforcementContextHash: string; idempotencyKey: string; controlToken?: string;
  }, action: "execute" | "approve", signal?: AbortSignal, principal?: TrustedRequestPrincipal): Promise<{
    task: TaskRun; result: QueryRunReview; events: TaskEvent[];
  }> {
    const task = this.#tasks.get(taskRunId);
    if (!task) throw new TaskStateError("TaskRun not found");
    const review = [...this.#events.list(taskRunId)].reverse().find(event => event.event_type === "query.review_requested");
    if (review?.payload.query_run_id !== input.queryRunId || review.payload.sql_hash !== input.sqlHash ||
      review.payload.assurance_report_hash !== input.assuranceReportHash ||
      review.payload.enforcement_context_hash !== input.enforcementContextHash)
      throw new TaskStateError("Claim hashes do not match TaskRun review");
    if (action === "approve" && (task.status !== "waiting_for_query_approval" || !input.controlToken ||
      principal?.principal_id !== review.payload.reviewer_principal_id))
      throw new TaskStateError("Specified reviewer and control token required");
    if (action === "execute" && (task.status !== "ready_for_query" || review.payload.review_required !== false))
      throw new TaskStateError("Query is not autonomous-ready");
    const claimInput = { queryRunId: input.queryRunId, sqlHash: input.sqlHash,
      assuranceReportHash: input.assuranceReportHash, enforcementContextHash: input.enforcementContextHash,
      idempotencyKey: input.idempotencyKey };
    // The remote claim commits before any local approval or execution event and before the HTTP 202.
    let claimed: QueryRunReview;
    try {
      claimed = action === "approve"
        ? await this.#forge.approveQueryRun({ ...claimInput, controlToken: input.controlToken! }, signal)
        : await this.#forge.executeQueryRun(claimInput, signal);
    } catch (error) {
      // Forge closed the review window: nothing ran, so the Task ends instead of waiting forever.
      if (error instanceof ForgeClientError && error.code === "review_expired")
        this.#settleTerminalQueryRun(task, "expired", input.queryRunId);
      throw error;
    }
    if (claimed.status !== "executing" && claimed.status !== "completed")
      throw new TaskStateError("Forge did not commit execution authorization");
    const running = this.#transactions.run(() => {
      const current = this.#tasks.get(taskRunId);
      if (!current) throw new TaskStateError("TaskRun not found");
      if (current.status === "querying") return current;
      if (current.status !== task.status) throw new TaskStateError("Task changed during execution claim");
      const next = this.#transition(current, "querying", "query_execution");
      this.#events.append(taskRunId, "query.execution_authorized", {
        query_run_id: input.queryRunId, execution_mode: action === "approve" ? "human_review" : "autonomous",
        authorization_hash: claimed.lineage.authorization_hash,
        reviewer_principal_id: action === "approve" ? principal!.principal_id : null,
      });
      return next;
    });
    // Observation carries no human ticket and never submits a second claim.
    this.#observeQueryRun(taskRunId, input.queryRunId);
    return { task: running, result: claimed, events: this.#events.list(taskRunId) };
  }

  async finishQueryExecution(taskRunId: string, queryRunId: string): Promise<{
    task: TaskRun; result: QueryRunResult; artifact: Artifact; events: TaskEvent[];
  }> {
    const deadline = Date.now() + this.#config.stageLeaseMs;
    let run: QueryRunReview;
    do {
      run = await this.#forge.getQueryRun(queryRunId);
      if (run.status === "completed" || run.status === "failed" || run.status === "cancelled") break;
      if (run.status !== "executing") throw new TaskStateError("Unclaimed QueryRun cannot be observed as executing");
      await delay(500);
    } while (Date.now() < deadline);
    if (run.status !== "completed" || !run.result) {
      if (run.status === "failed" || run.status === "cancelled") {
        this.#transactions.run(() => {
          const current = this.#tasks.get(taskRunId);
          if (current?.status !== "querying") return;
          this.#transition(current, "failed", "query_execution");
          this.#events.append(taskRunId, "query.execution_failed", { query_run_id: queryRunId, error: run.failure?.code ?? run.status });
        });
      }
      throw new TaskStateError("QueryRun result not yet available; observe same run later");
    }
    const result: QueryRunResult = {
      query_run_id: run.query_run_id, task_run_id: run.task_run_id, status: "completed",
      sql_hash: run.review.sql_hash!, input_kind: run.candidate.input_kind,
      candidate_revision: run.candidate.candidate_revision, dialect: "sqlite",
      registry_version: run.lineage.registry_revision,
      assurance_report_hash: run.review.assurance_report_hash!,
      assurance_revision: run.lineage.assurance_revision!, policy_revision: run.lineage.policy_revision,
      model_revision: null, assurance_registry_revision: null,
      columns: run.result.columns, rows: run.result.rows, row_count: run.result.row_count,
      truncated: run.result.truncated, execution_ms: run.result.execution_ms,
      executed_at: run.result.executed_at, error: "",
    };
    return this.#transactions.run(() => {
      let current = this.#tasks.get(taskRunId);
      if (!current) throw new TaskStateError("TaskRun not found");
      const existing = this.#artifacts.latest(taskRunId, "query_result");
      if (current.status !== "querying") {
        if (existing?.payload.query_run_id === queryRunId)
          return { task: current, result, artifact: existing, events: this.#events.list(taskRunId) };
        throw new TaskStateError("Task changed while observing QueryRun");
      }
      const artifact = this.#artifacts.create({ artifactType: "query_result", taskRunId, producer: "forge", payload: {
        query_run_id: result.query_run_id, sql_hash: result.sql_hash, columns: result.columns,
        rows: result.rows, row_count: result.row_count, truncated: result.truncated,
        dialect: result.dialect, registry_version: result.registry_version,
        assurance_report_hash: result.assurance_report_hash, assurance_revision: result.assurance_revision,
        policy_revision: result.policy_revision,
        execution_ms: result.execution_ms, executed_at: result.executed_at,
      } });
      this.#events.append(taskRunId, "artifact.created", { artifact_id: artifact.artifact_id,
        artifact_type: artifact.artifact_type, schema_version: artifact.schema_version });
      const chartPayload = buildChartPayload(artifact as Artifact<QueryResultPayload>);
      if (chartPayload) {
        const chart = this.#artifacts.create({ artifactType: "chart", taskRunId, producer: "pi-chart-builder", payload: chartPayload });
        this.#events.append(taskRunId, "artifact.created", { artifact_id: chart.artifact_id,
          artifact_type: chart.artifact_type, schema_version: chart.schema_version });
        this.#advanceExecutionPlan(taskRunId, { chart: "completed" });
      } else this.#advanceExecutionPlan(taskRunId, { chart: "skipped" });
      const continueToAnalysis = current.intent !== "query_prepare" && current.intent !== "analysis_supplement_query";
      current = this.#transition(current, continueToAnalysis ? "ready_for_analysis" : "completed",
        continueToAnalysis ? "analysis_prepare" : "query_complete");
      this.#events.append(taskRunId, "query.completed", { ...result, artifact_id: artifact.artifact_id });
      this.#advanceExecutionPlan(taskRunId, { query: "completed" });
      return { task: current, result, artifact, events: this.#events.list(taskRunId) };
    });
  }

  #observeQueryRun(taskRunId: string, queryRunId: string): void {
    if (this.#activeQueryObservers.has(taskRunId)) return;
    this.#activeQueryObservers.add(taskRunId);
    void this.finishQueryExecution(taskRunId, queryRunId)
      .catch(() => undefined)
      .finally(() => this.#activeQueryObservers.delete(taskRunId));
  }

  observePendingQueryExecutions(): void {
    for (const task of this.#tasks.listRecoverableQueries()) {
      const event = [...this.#events.list(task.task_run_id)].reverse()
        .find(item => item.event_type === "query.review_requested");
      if (typeof event?.payload.query_run_id !== "string" || this.#recoveringQueryRuns.has(task.task_run_id)) continue;
      if (task.status === "querying") {
        this.#observeQueryRun(task.task_run_id, event.payload.query_run_id);
        continue;
      }
      // Crash after Forge claim but before local commit: recover by GET, never by a new approval/execute POST.
      this.#recoveringQueryRuns.add(task.task_run_id);
      void this.#forge.getQueryRun(event.payload.query_run_id).then(run => {
        if (run.status === "failed" || run.status === "cancelled" || run.status === "expired") {
          this.#settleTerminalQueryRun(task, run.status, run.query_run_id, run.failure?.code);
          return;
        }
        if ((run.status !== "executing" && run.status !== "completed") || !run.decision.execution_authorized) return;
        this.#transactions.run(() => {
          const current = this.#tasks.get(task.task_run_id);
          if (!current || current.status !== task.status) return;
          this.#transition(current, "querying", "query_execution");
          this.#events.append(task.task_run_id, "query.execution_authorized", {
            query_run_id: run.query_run_id, authorization_hash: run.lineage.authorization_hash,
            execution_mode: run.decision.execution_mode,
            reviewer_principal_id: run.authorization?.reviewer_principal_id ?? null,
          });
        });
        if (this.#tasks.get(task.task_run_id)?.status === "querying")
          this.#observeQueryRun(task.task_run_id, run.query_run_id);
      }).catch(() => undefined).finally(() => this.#recoveringQueryRuns.delete(task.task_run_id));
    }
  }

  // Forge already holds the terminal outcome: record it locally without retrying any remote side effect.
  #settleTerminalQueryRun(task: TaskRun, status: "failed" | "cancelled" | "expired", queryRunId: string,
    failureCode?: string): void {
    this.#transactions.run(() => {
      const current = this.#tasks.get(task.task_run_id);
      if (!current || current.status !== task.status) return;
      if (status === "failed") {
        this.#transition(current, "failed", "query_execution");
        this.#events.append(task.task_run_id, "query.execution_failed", {
          query_run_id: queryRunId, error: failureCode ?? status,
        });
        return;
      }
      const settled = this.#tasks.transition({
        taskRunId: task.task_run_id, expectedStatus: current.status, status, currentStage: null,
      });
      this.#events.append(task.task_run_id, "task.status_changed", {
        from: current.status, to: status, current_stage: null,
        reason: status === "expired" ? "query_review_expired" : "query_run_cancelled", query_run_id: queryRunId,
      });
      settleExecutionPlan(this.state, settled);
    });
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
    const commandKey = this.#commandKey(taskRunId, "supplemental_analysis", input.idempotencyKey);
    const replay = this.#replayStage<Awaited<ReturnType<OrchestratorApplication["resumeAnalysisWithSupplement"]>>>(taskRunId, commandKey);
    if (replay !== undefined) return replay;
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
    let attempt!: StageAttempt;
    let stageTask: TaskRun = parent;
    stageTask = this.#transactions.run(() => {
      const running = this.#transition(stageTask, "analyzing", "supplemental_analysis");
      attempt = this.#startAttempt(
        running,
        "supplemental_analysis",
        "incomplete",
        commandKey,
      );
      return running;
    });
    const stageExecution = this.#stageSignal(attempt, signal);
    try {
      const queryResults = [primaryResult, supplementResult];
      const payload = await stageExecution.wait(() => this.#skills.analyze(stageTask,
      { question, queryResults, priorAnalysis },
      stageExecution.signal,
      attempt.model_revision,
      (progress) => this.#markAttemptProgress(attempt, progress.phase),));
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
      return this.#commitStage(attempt, () => {
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
        this.#finishAttempt(attempt, "succeeded", undefined, { artifact_id: artifact.artifact_id });
        return { task: stageTask, artifact, events: this.#events.list(taskRunId) };
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown Skill error";
      this.#transactions.run(() => {
        if (!this.#ownsAttempt(attempt)) { this.#releaseAttempt(attempt); return; }
        if (stageExecution.timedOut() || stageExecution.interrupted()) {
          this.#finishAttempt(attempt, stageExecution.timedOut() ? "timed_out" : "interrupted", message);
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
    const commandKey = this.#commandKey(taskRunId, "business_root_cause_analysis", input.idempotencyKey);
    const replay = this.#replayStage<Awaited<ReturnType<OrchestratorApplication["analyzeTask"]>>>(taskRunId, commandKey);
    if (replay !== undefined) return replay;
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
    let attempt!: StageAttempt;
    let stageTask: TaskRun = task;
    stageTask = this.#transactions.run(() => {
      const running = this.#transition(stageTask, "analyzing", "business_root_cause_analysis");
      this.#advanceExecutionPlan(taskRunId, { analysis: "running" });
      attempt = this.#startAttempt(
        running,
        "business_root_cause_analysis",
        "ready_for_analysis",
        commandKey,
      );
      return running;
    });
    const stageExecution = this.#stageSignal(attempt, signal);
    try {
      const payload = await stageExecution.wait(() => this.#skills.analyze(stageTask,
      { question, queryResults: [queryResult] },
      stageExecution.signal,
      attempt.model_revision,
      (progress) => this.#markAttemptProgress(attempt, progress.phase),));
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
      return this.#commitStage(attempt, () => {
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
          analysis: payload.status === "complete" ? "completed" : "ready",
        });
        this.#finishAttempt(attempt, "succeeded", undefined, { artifact_id: artifact.artifact_id });
        return { task: finalizedTask, artifact, events: this.#events.list(taskRunId) };
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown Skill error";
      this.#transactions.run(() => {
        if (!this.#ownsAttempt(attempt)) { this.#releaseAttempt(attempt); return; }
        if (stageExecution.timedOut() || stageExecution.interrupted()) {
          this.#finishAttempt(attempt, stageExecution.timedOut() ? "timed_out" : "interrupted", message);
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
    const commandKey = this.#commandKey(taskRunId, "data_analysis_report", input.idempotencyKey);
    const replay = this.#replayStage<Awaited<ReturnType<OrchestratorApplication["renderReport"]>>>(taskRunId, commandKey);
    if (replay !== undefined) return replay;
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
    // A bundle persisted by an interrupted attempt is the only report for this task: never regenerate it.
    const pendingBundle = this.#unpublishedReportBundle(taskRunId);
    let attempt!: StageAttempt;
    let stageTask: TaskRun = task;
    stageTask = this.#transactions.run(() => {
      const running = this.#transition(stageTask, "rendering", "data_analysis_report");
      this.#advanceExecutionPlan(taskRunId, { report: "running" });
      attempt = this.#startAttempt(
        running,
        "data_analysis_report",
        "ready_for_report",
        commandKey,
      );
      return running;
    });
    const stageExecution = this.#stageSignal(attempt, signal);
    let bundle = pendingBundle;
    try {
      if (bundle === undefined) {
        const payload = await stageExecution.wait(() => this.#skills.writeReport(stageTask,
        { audience: input.audience, analysis },
        stageExecution.signal,
        attempt.model_revision,));
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
        bundle = this.#commitStage(attempt, () => {
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
          const methodSummary = this.#reportMethodSummary(stageTask, analysis, queryResult);
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
            decision_log: methodSummary.approach_steps.map((step) => ({
              stage: "analysis",
              decision: step,
              rationale: methodSummary.comparison_baseline,
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
          const created = this.#artifacts.create({
            artifactType: "report_bundle",
            taskRunId,
            producer: "pi-report-builder",
            payload: reportBundlePayload,
          });
          for (const item of [technical, created]) {
            this.#events.append(taskRunId, "artifact.created", {
              artifact_id: item.artifact_id,
              artifact_type: item.artifact_type,
              schema_version: item.schema_version,
            });
          }
          return created as Artifact<ReportBundlePayload>;
        });
      }
      const request = this.#reportPublishRequest(stageTask, bundle);
      const publication = await this.#publishReport(taskRunId, request, `${taskRunId}:publish:${bundle.artifact_id}`,
        pendingBundle !== undefined, stageExecution);
      if (publication.status !== "published") throw new Error("Report publication failed");
      const business = this.#bundleArtifact<RenderedOutputPayload>(taskRunId, bundle.payload.business_artifact_id, "rendered_output");
      const completedReport = this.#commitStage(attempt, () => {
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
          artifact_id: business.artifact_id,
          publication_artifact_id: publicationArtifact.artifact_id,
          report_id: publication.report_id,
          status: business.payload.status,
        });
        this.#advanceExecutionPlan(taskRunId, { report: "completed" });
        this.#finishAttempt(attempt, "succeeded", undefined, { artifact_id: business.artifact_id });
        return { task: finalizedTask, artifact: business, events: this.#events.list(taskRunId) };
      });
      await this.#persistSessionSummary(
        completedReport.task,
        String(business.payload.executive_summary ?? business.payload.title),
        business.artifact_id,
        signal,
      );
      return completedReport;
    } catch (error) {
      const message = error instanceof Error ? error.message : "unknown Skill error";
      // Transport loss or a Forge 5xx leaves the publication outcome unknown; the next attempt asks Forge first.
      const publicationUncertain = bundle !== undefined && error instanceof ForgeClientError &&
        (error.statusCode === undefined || error.statusCode >= 500);
      this.#transactions.run(() => {
        if (!this.#ownsAttempt(attempt)) { this.#releaseAttempt(attempt); return; }
        if (stageExecution.timedOut() || stageExecution.interrupted() || publicationUncertain) {
          this.#finishAttempt(attempt, stageExecution.timedOut() ? "timed_out" : "interrupted", message);
          stageTask = this.#transition(stageTask, "ready_for_report", "report_retry");
        } else {
          this.#finishAttempt(attempt, "failed", message);
          stageTask = this.#transition(stageTask, "failed", "data_analysis_report");
        }
        this.#events.append(taskRunId, "skill.execution_failed", {
          skill: "data-analysis-report-writer",
          timed_out: stageExecution.timedOut(),
          publication_uncertain: publicationUncertain,
          error: message,
        });
      });
      throw new TaskStateError(`Report rendering failed for ${taskRunId}`, {
        cause: error,
      });
    }
  }

  #unpublishedReportBundle(taskRunId: string): Artifact<ReportBundlePayload> | undefined {
    const bundle = this.#artifacts.latest(taskRunId, "report_bundle") as Artifact<ReportBundlePayload> | undefined;
    if (bundle === undefined) return undefined;
    const published = this.#artifacts.list(taskRunId).some((item) =>
      item.artifact_type === "publication" && item.payload.bundle_hash === bundle.payload.bundle_hash);
    return published ? undefined : bundle;
  }

  #bundleArtifact<T extends Record<string, unknown>>(taskRunId: string, artifactId: string, type: Artifact["artifact_type"]): Artifact<T> {
    const artifact = this.#artifacts.list(taskRunId).find((item) => item.artifact_id === artifactId);
    if (artifact?.artifact_type !== type) throw new TaskStateError(`Report bundle ${type} Artifact is missing`);
    return artifact as Artifact<T>;
  }

  #reportMethodSummary(task: TaskRun, analysis: Artifact<AnalysisPayload>, queryResult: Artifact<QueryResultPayload> | undefined) {
    return analysis.payload.method_summary ?? {
      objective: typeof task.metadata.original_message === "string"
        ? task.metadata.original_message
        : "解释当前查询结果",
      dimensions: queryResult?.payload.columns.slice(0, 3) ?? [],
      comparison_baseline: "当前已审批查询结果内部对比",
      approach_steps: ["核验结果字段与范围", "比较主要维度", "提炼证据绑定结论"],
    };
  }

  /** Rebuilds the publish request from the persisted bundle, so first publish and recovery send identical content. */
  #reportPublishRequest(task: TaskRun, bundle: Artifact<ReportBundlePayload>): Record<string, unknown> {
    const [queryResultId, analysisId] = bundle.payload.source_artifact_ids;
    const queryResult = this.#bundleArtifact<QueryResultPayload>(task.task_run_id, String(queryResultId), "query_result");
    const analysis = this.#bundleArtifact<AnalysisPayload>(task.task_run_id, String(analysisId), "analysis");
    const business = this.#bundleArtifact<RenderedOutputPayload>(task.task_run_id, bundle.payload.business_artifact_id, "rendered_output");
    const technical = this.#bundleArtifact<TechnicalReportPayload>(task.task_run_id, bundle.payload.technical_artifact_id, "technical_report");
    return {
      report_id: bundle.payload.report_id,
      task_run_id: task.task_run_id,
      revision: bundle.payload.revision,
      bundle_hash: bundle.payload.bundle_hash,
      title: business.payload.title,
      business_report: business.payload,
      analysis: { ...analysis.payload, method_summary: this.#reportMethodSummary(task, analysis, queryResult) },
      query_result: queryResult.payload,
      charts: bundle.payload.chart_artifact_ids.map((id) => this.#bundleArtifact(task.task_run_id, id, "chart").payload),
      technical_report: technical.payload,
    };
  }

  /** Recovery asks Forge first and resends the same bundle only when Forge has no successful publication. */
  async #publishReport(
    taskRunId: string,
    request: Record<string, unknown>,
    idempotencyKey: string,
    recovering: boolean,
    stageExecution: { signal: AbortSignal; timedOut(): boolean; wait<T>(operation: () => Promise<T>): Promise<T> },
  ): Promise<ReportPublication> {
    const createReport = this.#forge.createReport;
    const getReport = this.#forge.getReport;
    if (createReport === undefined || getReport === undefined) {
      throw new Error("Forge Report Service is not configured");
    }
    const reportId = String(request.report_id);
    let publication: ReportPublication | undefined;
    if (recovering) {
      try {
        publication = await stageExecution.wait(() => getReport.call(this.#forge, reportId, taskRunId, stageExecution.signal));
      } catch (error) {
        if (!(error instanceof ForgeClientError && error.statusCode === 404)) throw error;
      }
      if (publication !== undefined && publication.bundle_hash !== request.bundle_hash)
        throw new Error("Forge holds a different report bundle for this task");
    }
    if (publication === undefined || publication.status === "failed") {
      publication = await stageExecution.wait(() => createReport.call(this.#forge, request, idempotencyKey, stageExecution.signal));
    }
    while (publication.status === "publishing") {
      if (stageExecution.timedOut()) throw new Error("Report publication Stage timed out");
      await stageExecution.wait(() => delay(500, undefined, { signal: stageExecution.signal }));
      publication = await stageExecution.wait(() => getReport.call(this.#forge, reportId, taskRunId, stageExecution.signal));
    }
    return publication;
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

  #commandKey(taskRunId: string, stage: string, explicit?: string): string {
    const key = explicit ?? `${taskRunId}:${stage}:${this.#attempts.list(taskRunId).filter((item) => item.stage === stage).length + 1}`;
    if (key.trim().length === 0) throw new TaskStateError("idempotency_key must not be empty");
    const existing = this.#attempts.findByIdempotencyKey(taskRunId, key);
    if (existing !== undefined && (existing.stage !== stage || existing.status !== "succeeded")) {
      throw new TaskStateError("Stage command already exists; retry with a new idempotency key");
    }
    return key;
  }

  #replayStage<T>(taskRunId: string, key: string): T | undefined {
    const attempt = this.#attempts.findByIdempotencyKey(taskRunId, key);
    if (attempt === undefined) return undefined;
    const completed = this.#events.list(taskRunId).find((event) =>
      event.event_type === "stage.attempt_succeeded" && event.payload.attempt_id === attempt.attempt_id);
    const response = completed?.payload.response as { artifact_id?: string; result?: unknown } | undefined;
    if (response === undefined) throw new TaskStateError("Idempotent Stage response is missing");
    const artifact = response.artifact_id === undefined ? undefined : this.#artifacts.list(taskRunId)
      .find((item) => item.artifact_id === response.artifact_id);
    if (response.artifact_id !== undefined && artifact === undefined) throw new TaskStateError("Idempotent Stage Artifact is missing");
    return { task: this.#tasks.get(taskRunId), events: this.#events.list(taskRunId),
      ...(artifact === undefined ? {} : { artifact }),
      ...(response.result === undefined ? {} : { result: response.result }),
    } as T;
  }

  #startAttempt(
    task: TaskRun,
    stage: string,
    retryStatus: TaskRun["status"],
    idempotencyKey: string,
  ): StageAttempt {
    if (this.#attempts.findByIdempotencyKey(task.task_run_id, idempotencyKey) !== undefined) {
      throw new TaskStateError("StageAttempt already exists; command must be checked before execution");
    }
    const stageBinding = resolveStageModelBinding(this.#config, attemptModelStage(stage));
    const modelRevision = stageBinding?.revisionId ?? computePiModelRevision({
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
      timeoutMs: this.#stageTimeoutMs,
      modelRevision,
      skillPolicyVersion: this.#skillPolicies.get(task.org_id, task.team_id)?.version ?? 0,
      requestId: currentRequestId() ?? task.correlation_id,
    });
    this.#events.append(task.task_run_id, "stage.attempt_started", {
      attempt_id: attempt.attempt_id,
      stage,
      attempt_number: attempt.attempt_number,
      request_id: attempt.request_id ?? null,
      deadline_at: attempt.deadline_at,
      lease_expires_at: attempt.lease_expires_at,
      progress_phase: attempt.progress_phase,
    });
    return attempt;
  }

  #markAttemptProgress(
    attempt: StageAttempt,
    phase: Exclude<StageProgressPhase, "waiting_for_model">,
  ): void {
    this.#attempts.markProgress(attempt.attempt_id, phase);
  }

  #releaseAttempt(attempt: StageAttempt): void {
    this.#activeStages.get(attempt.attempt_id)?.dispose();
    this.#activeStages.delete(attempt.attempt_id);
  }

  #finishAttempt(
    attempt: StageAttempt,
    status: "succeeded" | "failed" | "timed_out" | "interrupted",
    error?: string,
    response?: { artifact_id?: string; result?: QueryRunReview | QueryRunResult },
  ): void {
    const finished = this.#attempts.finish(attempt.attempt_id, status, error);
    this.#releaseAttempt(attempt);
    const eventType =
      status === "succeeded"
        ? "stage.attempt_succeeded"
        : status === "timed_out"
          ? "stage.attempt_timed_out"
          : status === "interrupted" ? "stage.attempt_interrupted" : "stage.attempt_failed";
    this.#events.append(attempt.task_run_id, eventType, {
      attempt_id: attempt.attempt_id,
      stage: attempt.stage,
      status: finished.status,
      request_id: attempt.request_id ?? null,
      ...(response === undefined ? {} : { response }),
      ...(error === undefined ? {} : { error: error.slice(0, 2_000) }),
    });
  }

  #commitStage<T>(attempt: StageAttempt, operation: () => T): T {
    try {
      return this.#transactions.run(() => {
        this.#assertAttemptOwner(attempt);
        return operation();
      });
    } catch (error) {
      if (this.#ownsAttempt(attempt)) {
        this.#transactions.run(() => {
          this.#finishAttempt(attempt, "failed", "Stage result could not be committed");
          const current = this.#tasks.get(attempt.task_run_id)!;
          this.#transition(current, "failed", attempt.stage);
        });
      }
      throw error;
    }
  }

  #ownsAttempt(attempt: StageAttempt): boolean {
    return this.#attempts.get(attempt.attempt_id)?.status === "running" &&
      this.#tasks.get(attempt.task_run_id)?.status === attempt.running_status;
  }

  #assertAttemptOwner(attempt: StageAttempt): void {
    if (!this.#ownsAttempt(attempt)) throw new TaskStateError("Stage result no longer owns its TaskRun");
  }

  #stageSignal(attempt: StageAttempt, parent?: AbortSignal) {
    const controller = new AbortController();
    let expired = false;
    const timer = setTimeout(() => {
      expired = true;
      controller.abort(new Error("Stage deadline exceeded"));
    }, this.#stageTimeoutMs);
    timer.unref();
    const abortParent = () => controller.abort(parent?.reason);
    parent?.addEventListener("abort", abortParent, { once: true });
    if (parent?.aborted) abortParent();
    this.#activeStages.set(attempt.attempt_id, {
      controller,
      dispose: () => { clearTimeout(timer); parent?.removeEventListener("abort", abortParent); },
    });
    const signal = controller.signal;
    let dispatched = false;
    return {
      signal,
      timedOut: () => expired,
      interrupted: () => signal.aborted && !expired,
      wait: <T>(operation: () => Promise<T>): Promise<T> => {
        this.#assertAttemptOwner(attempt);
        if (signal.aborted) return Promise.reject(signal.reason);
        return new Promise<T>((resolve, reject) => {
          const abort = () => reject(signal.reason);
          signal.addEventListener("abort", abort, { once: true });
          Promise.resolve().then(() => {
            signal.throwIfAborted();
            this.#assertAttemptOwner(attempt);
            if (!dispatched) { this.#attempts.markDispatched(attempt.attempt_id); dispatched = true; }
            return operation();
          }).then((result) => {
            signal.throwIfAborted();
            this.#assertAttemptOwner(attempt);
            resolve(result);
          }, reject).catch(reject).finally(() => signal.removeEventListener("abort", abort));
        });
      },
    };
  }

  async #persistSessionSummary(
    task: TaskRun,
    summary: string,
    sourceRevision: string,
    signal?: AbortSignal,
  ): Promise<void> {
    if (this.#forge.writeMemory === undefined || summary.trim().length === 0) return;
    const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1_000).toISOString();
    try {
      await this.#forge.writeMemory({
        task_run_id: task.task_run_id,
        operation: "upsert",
        category: "session_summary",
        key: `task_${task.task_run_id.slice(3)}`,
        value: {
          question: typeof task.metadata.original_message === "string"
            ? task.metadata.original_message.slice(0, 1_000)
            : task.intent,
          summary: summary.slice(0, 1_000),
          intent: task.intent,
        },
        source_session: task.task_run_id,
        source_revision: sourceRevision,
        confidence: 1,
        expires_at: expiresAt,
      }, signal);
      this.#events.append(task.task_run_id, "memory.persisted", {
        category: "session_summary",
        scope: "user",
        expires_at: expiresAt,
      });
    } catch {
      this.#events.append(task.task_run_id, "memory.persist_failed", {
        category: "session_summary",
        retryable: true,
      });
    }
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
      settleExecutionPlan(this.state, updated);
      return updated;
    });
  }
}
