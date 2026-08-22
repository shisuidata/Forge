import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { DatabaseSync, type StatementResultingChanges } from "node:sqlite";

import {
  type ChannelEventRecord,
  type ChannelEventStore,
} from "./channels/event-store.js";
import type { ChannelEventInput } from "./channels/contracts.js";
import {
  type Artifact,
  type ArtifactStore,
  type ArtifactType,
  type CreateArtifactInput,
  validateArtifactInput,
} from "./artifacts.js";
import { AUTHORIZED_SKILL_NAMES, type AuthorizedSkillName } from "./skills.js";
import {
  SkillPolicyConflictError,
  type SkillPolicyStore,
  type TeamSkillPolicy,
} from "./skill-policy.js";
import {
  type StageAttempt,
  type StageAttemptStatus,
  type StageAttemptStore,
  type StartStageAttemptInput,
} from "./stage-attempts.js";
import {
  TASK_STATUSES,
  TaskStateError,
  type CreateTaskInput,
  type TaskRun,
  type TaskStatus,
  type TaskStore,
} from "./task-store.js";
import {
  type TaskEvent,
  type TaskEventStore,
  type TaskEventType,
} from "./task-events.js";

const TERMINAL_STATUSES = new Set<TaskStatus>([
  "completed",
  "cancelled",
  "failed",
  "expired",
]);
const TASK_STATUS_SET = new Set<string>(TASK_STATUSES);

function parseJson<T>(raw: unknown, label: string): T {
  if (typeof raw !== "string") throw new Error(`Corrupt ${label}: JSON column is not text`);
  try {
    return JSON.parse(raw) as T;
  } catch (error) {
    throw new Error(`Corrupt ${label}: invalid JSON`, { cause: error });
  }
}

function changed(result: StatementResultingChanges): number {
  return Number(result.changes);
}

function normalizeStageAttempt(attempt: StageAttempt): StageAttempt {
  return {
    ...attempt,
    model_revision: attempt.model_revision ?? null,
    skill_policy_version: attempt.skill_policy_version ?? 0,
  };
}

class SqliteTaskStore implements TaskStore {
  constructor(private readonly database: DatabaseSync) {}

  create(input: CreateTaskInput): TaskRun {
    for (const [field, value] of Object.entries({
      org_id: input.org_id,
      team_id: input.team_id,
      user_id: input.user_id,
      intent: input.intent,
    })) {
      if (value.trim().length === 0) throw new TaskStateError(`${field} must not be empty`);
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
    this.database
      .prepare(
        `INSERT INTO task_runs
          (task_run_id, status, parent_task_run_id, created_at, updated_at, data_json)
         VALUES (?, ?, ?, ?, ?, ?)`,
      )
      .run(
        task.task_run_id,
        task.status,
        task.parent_task_run_id,
        task.created_at,
        task.updated_at,
        JSON.stringify(task),
      );
    return structuredClone(task);
  }

  get(taskRunId: string): TaskRun | undefined {
    const row = this.database
      .prepare("SELECT data_json FROM task_runs WHERE task_run_id = ?")
      .get(taskRunId) as { data_json: string } | undefined;
    if (row === undefined) return undefined;
    const task = parseJson<TaskRun>(row.data_json, `TaskRun ${taskRunId}`);
    if (task.task_run_id !== taskRunId || !TASK_STATUS_SET.has(task.status)) {
      throw new Error(`Corrupt TaskRun ${taskRunId}: identity or status mismatch`);
    }
    return structuredClone(task);
  }

  transition(options: {
    taskRunId: string;
    expectedStatus: TaskStatus;
    status: TaskStatus;
    currentStage: string | null;
  }): TaskRun {
    const task = this.get(options.taskRunId);
    if (task === undefined) throw new TaskStateError(`TaskRun not found: ${options.taskRunId}`);
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
    const result = this.database
      .prepare(
        `UPDATE task_runs
         SET status = ?, updated_at = ?, data_json = ?
         WHERE task_run_id = ? AND status = ?`,
      )
      .run(
        updated.status,
        updated.updated_at,
        JSON.stringify(updated),
        options.taskRunId,
        options.expectedStatus,
      );
    if (changed(result) !== 1) {
      const actual = this.get(options.taskRunId)?.status ?? "missing";
      throw new TaskStateError(
        `TaskRun status conflict: expected=${options.expectedStatus}, actual=${actual}`,
      );
    }
    return structuredClone(updated);
  }
}

class SqliteTaskEventStore implements TaskEventStore {
  constructor(private readonly database: DatabaseSync) {}

  append(
    taskRunId: string,
    eventType: TaskEventType,
    payload: Record<string, unknown>,
  ): TaskEvent {
    const ownsTransaction = !this.database.isTransaction;
    if (ownsTransaction) this.database.exec("BEGIN IMMEDIATE");
    try {
      const row = this.database
        .prepare(
          "SELECT COALESCE(MAX(sequence), 0) AS sequence FROM task_events WHERE task_run_id = ?",
        )
        .get(taskRunId) as { sequence: number | bigint };
      const event: TaskEvent = {
        event_id: `te_${randomUUID().replaceAll("-", "")}`,
        task_run_id: taskRunId,
        sequence: Number(row.sequence) + 1,
        event_type: eventType,
        created_at: new Date().toISOString(),
        payload: structuredClone(payload),
      };
      this.database
        .prepare(
          `INSERT INTO task_events
            (event_id, task_run_id, sequence, event_type, created_at, data_json)
           VALUES (?, ?, ?, ?, ?, ?)`,
        )
        .run(
          event.event_id,
          event.task_run_id,
          event.sequence,
          event.event_type,
          event.created_at,
          JSON.stringify(event),
        );
      if (ownsTransaction) this.database.exec("COMMIT");
      return structuredClone(event);
    } catch (error) {
      if (ownsTransaction && this.database.isTransaction) this.database.exec("ROLLBACK");
      throw error;
    }
  }

  list(taskRunId: string, afterSequence = 0): TaskEvent[] {
    const rows = this.database
      .prepare(
        `SELECT data_json FROM task_events
         WHERE task_run_id = ? AND sequence > ? ORDER BY sequence ASC`,
      )
      .all(taskRunId, afterSequence) as Array<{ data_json: string }>;
    return rows.map((row) => structuredClone(parseJson<TaskEvent>(row.data_json, "TaskEvent")));
  }
}

class SqliteChannelEventStore implements ChannelEventStore {
  constructor(private readonly database: DatabaseSync) {}

  claim(input: ChannelEventInput): { record: ChannelEventRecord; created: boolean } {
    const existing = this.get(input.channel, input.event_id);
    if (existing !== undefined) return { record: existing, created: false };
    const now = new Date().toISOString();
    const record: ChannelEventRecord = {
      channel: input.channel,
      event_id: input.event_id,
      event_type: input.event_type,
      status: "processing",
      task_run_id: null,
      received_at: now,
      updated_at: now,
      error: null,
      input: structuredClone(input),
    };
    try {
      this.database
        .prepare(
          `INSERT INTO channel_events
            (channel, event_id, event_type, status, task_run_id, received_at, updated_at, data_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          record.channel,
          record.event_id,
          record.event_type,
          record.status,
          record.task_run_id,
          record.received_at,
          record.updated_at,
          JSON.stringify(record),
        );
      return { record: structuredClone(record), created: true };
    } catch (error) {
      const raced = this.get(input.channel, input.event_id);
      if (raced !== undefined) return { record: raced, created: false };
      throw error;
    }
  }

  complete(channel: ChannelEventInput["channel"], eventId: string, taskRunId: string): ChannelEventRecord {
    return this.#finish(channel, eventId, "completed", taskRunId, null);
  }

  fail(channel: ChannelEventInput["channel"], eventId: string, error: string): ChannelEventRecord {
    return this.#finish(channel, eventId, "failed", null, error.slice(0, 2_000));
  }

  get(channel: ChannelEventInput["channel"], eventId: string): ChannelEventRecord | undefined {
    const row = this.database
      .prepare("SELECT data_json FROM channel_events WHERE channel = ? AND event_id = ?")
      .get(channel, eventId) as { data_json: string } | undefined;
    return row === undefined
      ? undefined
      : structuredClone(parseJson<ChannelEventRecord>(row.data_json, "ChannelEvent"));
  }

  #finish(
    channel: ChannelEventInput["channel"],
    eventId: string,
    status: "completed" | "failed",
    taskRunId: string | null,
    error: string | null,
  ): ChannelEventRecord {
    const current = this.get(channel, eventId);
    if (current === undefined) throw new TaskStateError(`ChannelEvent not found: ${channel}/${eventId}`);
    if (current.status === status) return current;
    if (current.status !== "processing") {
      throw new TaskStateError(`ChannelEvent is already terminal: ${current.status}`);
    }
    const updated: ChannelEventRecord = {
      ...current,
      status,
      task_run_id: taskRunId,
      updated_at: new Date().toISOString(),
      error,
    };
    const result = this.database
      .prepare(
        `UPDATE channel_events
         SET status = ?, task_run_id = ?, updated_at = ?, data_json = ?
         WHERE channel = ? AND event_id = ? AND status = 'processing'`,
      )
      .run(
        status,
        taskRunId,
        updated.updated_at,
        JSON.stringify(updated),
        channel,
        eventId,
      );
    if (changed(result) !== 1) throw new TaskStateError("ChannelEvent status conflict");
    return structuredClone(updated);
  }
}

class SqliteStageAttemptStore implements StageAttemptStore {
  constructor(private readonly database: DatabaseSync) {}

  start(input: StartStageAttemptInput): StageAttempt {
    if (input.stage.trim().length === 0 || input.idempotencyKey.trim().length === 0) {
      throw new TaskStateError("Stage and idempotency key must not be empty");
    }
    if (!Number.isInteger(input.leaseMs) || input.leaseMs < 1) {
      throw new TaskStateError("Stage attempt lease must be a positive integer");
    }
    const ownsTransaction = !this.database.isTransaction;
    if (ownsTransaction) this.database.exec("BEGIN IMMEDIATE");
    try {
      const existing = this.findByIdempotencyKey(input.taskRunId, input.idempotencyKey);
      if (existing !== undefined) {
        if (ownsTransaction) this.database.exec("COMMIT");
        return existing;
      }
      const active = this.database
        .prepare(
          "SELECT attempt_id FROM stage_attempts WHERE task_run_id = ? AND status = 'running'",
        )
        .get(input.taskRunId);
      if (active !== undefined) {
        throw new TaskStateError("TaskRun already has a running StageAttempt");
      }
      const numberRow = this.database
        .prepare(
          "SELECT COALESCE(MAX(attempt_number), 0) AS attempt_number FROM stage_attempts WHERE task_run_id = ?",
        )
        .get(input.taskRunId) as { attempt_number: number | bigint };
      const now = new Date();
      const attempt: StageAttempt = {
        attempt_id: `sa_${randomUUID().replaceAll("-", "")}`,
        task_run_id: input.taskRunId,
        stage: input.stage,
        status: "running",
        attempt_number: Number(numberRow.attempt_number) + 1,
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
      this.database
        .prepare(
          `INSERT INTO stage_attempts
            (attempt_id, task_run_id, stage, status, attempt_number, idempotency_key,
             running_status, retry_status, lease_expires_at, started_at, updated_at,
             finished_at, error, data_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          attempt.attempt_id,
          attempt.task_run_id,
          attempt.stage,
          attempt.status,
          attempt.attempt_number,
          attempt.idempotency_key,
          attempt.running_status,
          attempt.retry_status,
          attempt.lease_expires_at,
          attempt.started_at,
          attempt.updated_at,
          attempt.finished_at,
          attempt.error,
          JSON.stringify(attempt),
        );
      if (ownsTransaction) this.database.exec("COMMIT");
      return structuredClone(attempt);
    } catch (error) {
      if (ownsTransaction && this.database.isTransaction) this.database.exec("ROLLBACK");
      throw error;
    }
  }

  get(attemptId: string): StageAttempt | undefined {
    const row = this.database
      .prepare("SELECT data_json FROM stage_attempts WHERE attempt_id = ?")
      .get(attemptId) as { data_json: string } | undefined;
    return row === undefined
      ? undefined
      : structuredClone(normalizeStageAttempt(parseJson<StageAttempt>(row.data_json, `StageAttempt ${attemptId}`)));
  }

  findByIdempotencyKey(taskRunId: string, idempotencyKey: string): StageAttempt | undefined {
    const row = this.database
      .prepare(
        "SELECT data_json FROM stage_attempts WHERE task_run_id = ? AND idempotency_key = ?",
      )
      .get(taskRunId, idempotencyKey) as { data_json: string } | undefined;
    return row === undefined
      ? undefined
      : structuredClone(normalizeStageAttempt(parseJson<StageAttempt>(row.data_json, "StageAttempt")));
  }

  list(taskRunId: string): StageAttempt[] {
    const rows = this.database
      .prepare(
        "SELECT data_json FROM stage_attempts WHERE task_run_id = ? ORDER BY attempt_number ASC",
      )
      .all(taskRunId) as Array<{ data_json: string }>;
    return rows.map((row) =>
      structuredClone(normalizeStageAttempt(parseJson<StageAttempt>(row.data_json, "StageAttempt"))),
    );
  }

  finish(
    attemptId: string,
    status: Exclude<StageAttemptStatus, "running" | "interrupted">,
    error?: string,
  ): StageAttempt {
    const attempt = this.get(attemptId);
    if (attempt === undefined) throw new TaskStateError(`StageAttempt not found: ${attemptId}`);
    if (attempt.status !== "running") {
      if (attempt.status === status) return attempt;
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
    const result = this.database
      .prepare(
        `UPDATE stage_attempts
         SET status = ?, updated_at = ?, finished_at = ?, error = ?, data_json = ?
         WHERE attempt_id = ? AND status = 'running'`,
      )
      .run(status, now, now, updated.error, JSON.stringify(updated), attemptId);
    if (changed(result) !== 1) {
      throw new TaskStateError(`StageAttempt status conflict: ${attemptId}`);
    }
    return structuredClone(updated);
  }
}

class SqliteArtifactStore implements ArtifactStore {
  constructor(private readonly database: DatabaseSync) {}

  create<TPayload extends Record<string, unknown>>(
    input: CreateArtifactInput<TPayload>,
  ): Artifact<TPayload> {
    validateArtifactInput(input);
    const artifact: Artifact<TPayload> = {
      artifact_id: `ar_${randomUUID().replaceAll("-", "")}`,
      artifact_type: input.artifactType,
      schema_version: 1,
      task_run_id: input.taskRunId,
      producer: input.producer,
      created_at: new Date().toISOString(),
      payload: structuredClone(input.payload),
    };
    this.database
      .prepare(
        `INSERT INTO artifacts
          (artifact_id, task_run_id, artifact_type, created_at, data_json)
         VALUES (?, ?, ?, ?, ?)`,
      )
      .run(
        artifact.artifact_id,
        artifact.task_run_id,
        artifact.artifact_type,
        artifact.created_at,
        JSON.stringify(artifact),
      );
    return structuredClone(artifact);
  }

  list(taskRunId: string): Artifact[] {
    const rows = this.database
      .prepare("SELECT data_json FROM artifacts WHERE task_run_id = ? ORDER BY row_id ASC")
      .all(taskRunId) as Array<{ data_json: string }>;
    return rows.map((row) => structuredClone(parseJson<Artifact>(row.data_json, "Artifact")));
  }

  latest(taskRunId: string, artifactType: ArtifactType): Artifact | undefined {
    const row = this.database
      .prepare(
        `SELECT data_json FROM artifacts
         WHERE task_run_id = ? AND artifact_type = ? ORDER BY row_id DESC LIMIT 1`,
      )
      .get(taskRunId, artifactType) as { data_json: string } | undefined;
    return row === undefined
      ? undefined
      : structuredClone(parseJson<Artifact>(row.data_json, "Artifact"));
  }
}

class SqliteSkillPolicyStore implements SkillPolicyStore {
  constructor(
    private readonly database: DatabaseSync,
    private readonly defaults: readonly AuthorizedSkillName[],
  ) {}

  get(orgId: string, teamId: string): TeamSkillPolicy | undefined {
    const row = this.database.prepare(
      "SELECT data_json FROM team_skill_policies WHERE org_id = ? AND team_id = ?",
    ).get(orgId, teamId) as { data_json: string } | undefined;
    return row === undefined ? undefined : parseJson<TeamSkillPolicy>(row.data_json, "TeamSkillPolicy");
  }

  isEnabled(orgId: string, teamId: string, skillName: AuthorizedSkillName): boolean {
    return (this.get(orgId, teamId)?.enabled_skills ?? this.defaults).includes(skillName);
  }

  configure(input: {
    orgId: string;
    teamId: string;
    enabledSkills: AuthorizedSkillName[];
    expectedVersion: number;
    actor: string;
  }): TeamSkillPolicy {
    const current = this.get(input.orgId, input.teamId);
    const version = current?.version ?? 0;
    if (version !== input.expectedVersion) {
      throw new SkillPolicyConflictError(
        `Skill policy version mismatch: expected ${input.expectedVersion}, current ${version}`,
      );
    }
    const policy: TeamSkillPolicy = {
      org_id: input.orgId,
      team_id: input.teamId,
      enabled_skills: [...new Set(input.enabledSkills)].sort(),
      version: version + 1,
      updated_at: new Date().toISOString(),
      updated_by: input.actor,
    };
    const ownsTransaction = !this.database.isTransaction;
    if (ownsTransaction) this.database.exec("BEGIN IMMEDIATE");
    try {
      const result = this.database.prepare(
        `INSERT INTO team_skill_policies (org_id, team_id, version, data_json)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(org_id, team_id) DO UPDATE SET version = excluded.version, data_json = excluded.data_json
         WHERE team_skill_policies.version = ?`,
      ).run(input.orgId, input.teamId, policy.version, JSON.stringify(policy), version);
      if (changed(result) !== 1) throw new SkillPolicyConflictError("Concurrent Skill policy update");
      this.database.prepare(
        `INSERT INTO team_skill_policy_audit
         (audit_id, org_id, team_id, version, actor, created_at, data_json)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
      ).run(
        `spa_${randomUUID().replaceAll("-", "")}`,
        input.orgId,
        input.teamId,
        policy.version,
        input.actor,
        policy.updated_at,
        JSON.stringify(policy),
      );
      if (ownsTransaction) this.database.exec("COMMIT");
      return structuredClone(policy);
    } catch (error) {
      if (ownsTransaction && this.database.isTransaction) this.database.exec("ROLLBACK");
      if (error instanceof SkillPolicyConflictError) throw error;
      throw new SkillPolicyConflictError("Concurrent Skill policy update", { cause: error });
    }
  }
}

export class SqliteOrchestratorState {
  readonly tasks: TaskStore;
  readonly events: TaskEventStore;
  readonly artifacts: ArtifactStore;
  readonly attempts: StageAttemptStore;
  readonly channelEvents: ChannelEventStore;
  readonly skillPolicies: SkillPolicyStore;
  readonly transactions: { run<T>(operation: () => T): T };
  readonly #database: DatabaseSync;

  constructor(databasePath: string) {
    if (databasePath !== ":memory:") mkdirSync(dirname(databasePath), { recursive: true });
    this.#database = new DatabaseSync(databasePath);
    this.#database.exec("PRAGMA foreign_keys = ON");
    this.#database.exec("PRAGMA busy_timeout = 5000");
    this.#database.exec("PRAGMA journal_mode = WAL");
    const version = this.#database.prepare("PRAGMA user_version").get() as {
      user_version: number | bigint;
    };
    if (Number(version.user_version) > 4) {
      this.#database.close();
      throw new Error(`Unsupported Pi state schema version: ${String(version.user_version)}`);
    }
    this.#database.exec(`
      CREATE TABLE IF NOT EXISTS task_runs (
        task_run_id TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        parent_task_run_id TEXT REFERENCES task_runs(task_run_id),
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        data_json TEXT NOT NULL
      ) STRICT;
      CREATE INDEX IF NOT EXISTS idx_task_runs_parent ON task_runs(parent_task_run_id);
      CREATE TABLE IF NOT EXISTS task_events (
        event_id TEXT PRIMARY KEY,
        task_run_id TEXT NOT NULL REFERENCES task_runs(task_run_id) ON DELETE CASCADE,
        sequence INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        created_at TEXT NOT NULL,
        data_json TEXT NOT NULL,
        UNIQUE(task_run_id, sequence)
      ) STRICT;
      CREATE TABLE IF NOT EXISTS artifacts (
        row_id INTEGER PRIMARY KEY AUTOINCREMENT,
        artifact_id TEXT NOT NULL UNIQUE,
        task_run_id TEXT NOT NULL REFERENCES task_runs(task_run_id) ON DELETE CASCADE,
        artifact_type TEXT NOT NULL,
        created_at TEXT NOT NULL,
        data_json TEXT NOT NULL
      ) STRICT;
      CREATE INDEX IF NOT EXISTS idx_artifacts_task_type
        ON artifacts(task_run_id, artifact_type, row_id);
      CREATE TABLE IF NOT EXISTS stage_attempts (
        attempt_id TEXT PRIMARY KEY,
        task_run_id TEXT NOT NULL REFERENCES task_runs(task_run_id) ON DELETE CASCADE,
        stage TEXT NOT NULL,
        status TEXT NOT NULL,
        attempt_number INTEGER NOT NULL,
        idempotency_key TEXT NOT NULL,
        running_status TEXT NOT NULL,
        retry_status TEXT NOT NULL,
        lease_expires_at TEXT NOT NULL,
        started_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        finished_at TEXT,
        error TEXT,
        data_json TEXT NOT NULL,
        UNIQUE(task_run_id, attempt_number),
        UNIQUE(task_run_id, idempotency_key)
      ) STRICT;
      CREATE UNIQUE INDEX IF NOT EXISTS idx_stage_attempts_one_running
        ON stage_attempts(task_run_id) WHERE status = 'running';
      CREATE INDEX IF NOT EXISTS idx_stage_attempts_expired
        ON stage_attempts(status, lease_expires_at);
      CREATE TABLE IF NOT EXISTS channel_events (
        channel TEXT NOT NULL,
        event_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        status TEXT NOT NULL,
        task_run_id TEXT REFERENCES task_runs(task_run_id),
        received_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        data_json TEXT NOT NULL,
        PRIMARY KEY(channel, event_id)
      ) STRICT;
      CREATE INDEX IF NOT EXISTS idx_channel_events_task ON channel_events(task_run_id);
      CREATE TABLE IF NOT EXISTS team_skill_policies (
        org_id TEXT NOT NULL,
        team_id TEXT NOT NULL,
        version INTEGER NOT NULL,
        data_json TEXT NOT NULL,
        PRIMARY KEY(org_id, team_id)
      ) STRICT;
      CREATE TABLE IF NOT EXISTS team_skill_policy_audit (
        audit_id TEXT PRIMARY KEY,
        org_id TEXT NOT NULL,
        team_id TEXT NOT NULL,
        version INTEGER NOT NULL,
        actor TEXT NOT NULL,
        created_at TEXT NOT NULL,
        data_json TEXT NOT NULL
      ) STRICT;
      CREATE INDEX IF NOT EXISTS idx_skill_policy_audit_scope
        ON team_skill_policy_audit(org_id, team_id, version);
      PRAGMA user_version = 4;
    `);
    this.tasks = new SqliteTaskStore(this.#database);
    this.events = new SqliteTaskEventStore(this.#database);
    this.artifacts = new SqliteArtifactStore(this.#database);
    this.attempts = new SqliteStageAttemptStore(this.#database);
    this.channelEvents = new SqliteChannelEventStore(this.#database);
    this.skillPolicies = new SqliteSkillPolicyStore(this.#database, AUTHORIZED_SKILL_NAMES);
    this.transactions = {
      run: <T>(operation: () => T): T => {
        if (this.#database.isTransaction) return operation();
        this.#database.exec("BEGIN IMMEDIATE");
        try {
          const result = operation();
          this.#database.exec("COMMIT");
          return result;
        } catch (error) {
          if (this.#database.isTransaction) this.#database.exec("ROLLBACK");
          throw error;
        }
      },
    };
  }

  reconcileExpiredAttempts(now = new Date()): StageAttempt[] {
    return this.transactions.run(() => {
      const rows = this.#database
        .prepare(
          `SELECT data_json FROM stage_attempts
           WHERE status = 'running' AND lease_expires_at <= ?
           ORDER BY started_at ASC`,
        )
        .all(now.toISOString()) as Array<{ data_json: string }>;
      const interrupted: StageAttempt[] = [];
      for (const row of rows) {
        const attempt = normalizeStageAttempt(parseJson<StageAttempt>(row.data_json, "StageAttempt"));
        const finishedAt = now.toISOString();
        const updatedAttempt: StageAttempt = {
          ...attempt,
          status: "interrupted",
          updated_at: finishedAt,
          finished_at: finishedAt,
          error: "Stage lease expired before completion",
        };
        const attemptResult = this.#database
          .prepare(
            `UPDATE stage_attempts
             SET status = 'interrupted', updated_at = ?, finished_at = ?, error = ?, data_json = ?
             WHERE attempt_id = ? AND status = 'running' AND lease_expires_at <= ?`,
          )
          .run(
            finishedAt,
            finishedAt,
            updatedAttempt.error,
            JSON.stringify(updatedAttempt),
            attempt.attempt_id,
            now.toISOString(),
          );
        if (changed(attemptResult) !== 1) continue;

        const task = this.tasks.get(attempt.task_run_id);
        let recovered = false;
        if (task?.status === attempt.running_status) {
          this.tasks.transition({
            taskRunId: task.task_run_id,
            expectedStatus: attempt.running_status,
            status: attempt.retry_status,
            currentStage: `${attempt.stage}_retry`,
          });
          this.events.append(task.task_run_id, "task.status_changed", {
            from: attempt.running_status,
            to: attempt.retry_status,
            current_stage: `${attempt.stage}_retry`,
            recovery: true,
          });
          recovered = true;
        }
        this.events.append(attempt.task_run_id, "stage.attempt_interrupted", {
          attempt_id: attempt.attempt_id,
          stage: attempt.stage,
          retry_status: attempt.retry_status,
          task_recovered: recovered,
        });
        interrupted.push(structuredClone(updatedAttempt));
      }
      return interrupted;
    });
  }

  close(): void {
    this.#database.close();
  }
}
