import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import type { DatabaseSync } from "node:sqlite";
import { hostname } from "node:os";
import { dirname, extname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  createAgentSession,
  DefaultResourceLoader,
  defineTool,
  ModelRuntime,
  SessionManager,
  SettingsManager,
} from "@earendil-works/pi-coding-agent";
import type { TSchema } from "typebox";

import type { OrchestratorApplication } from "./application.js";
import { computePiModelRevision, type OrchestratorConfig } from "./config.js";
import type {
  ArmMetricsV2,
  BenchmarkArm,
  BenchmarkCaseProjectionV2,
  BenchmarkLogV2,
  BenchmarkGenerationContractV2,
  BenchmarkGoldReadinessV2,
  BenchmarkRunProjectionV2,
  BenchmarkRunStatus,
  ContextSnapshotV2,
} from "./benchmark-contracts.js";
import { createStrictForgeOutput, requireStrictForgeApi, requireStrictForgePayload, StrictForgeOutputError } from "./strict-forge-output.js";
import { SqliteOrchestratorState } from "./sqlite-store.js";
import { currentRequestId } from "./request-context.js";

const moduleDir = dirname(fileURLToPath(import.meta.url));
const forgeSchemaJson = readFileSync(resolve(moduleDir, "../../../forge/schema.json"), "utf8");
const canonicalForgeSchema = JSON.parse(forgeSchemaJson);
const strictForgeOutput = createStrictForgeOutput(canonicalForgeSchema);
const forgeToolSchema = canonicalForgeSchema as TSchema;
const forgeToolSchemaChars = JSON.stringify(strictForgeOutput.schema).length;

const FORGE_SCHEMA_REVISION = `sha256:${createHash("sha256").update(forgeSchemaJson).digest("hex")}`;
const PI_RUNTIME_REVISION = "sha256:" + createHash("sha256")
  .update("benchmark-runtime\0").update(readFileSync(fileURLToPath(import.meta.url)))
  .update("strict-forge-output\0").update(readFileSync(new URL("./strict-forge-output" + extname(fileURLToPath(import.meta.url)), import.meta.url)))
  .digest("hex");
const PI_SDK_LOCK_REVISION = "sha256:" + createHash("sha256")
  .update(readFileSync(resolve(moduleDir, "../package-lock.json"))).digest("hex");
const structuredGenerationContract: Omit<BenchmarkGenerationContractV2, "forge_prompt_revision"> = {
  forge_output_mode: "pi_tool_schema",
  forge_schema_revision: FORGE_SCHEMA_REVISION,
  provider_json_schema_request: "required",
  forge_wire_schema_revision: strictForgeOutput.revision,
  sampling: "provider_default",
  transport: "sse",
  max_output_tokens: null,
  timeout_seconds: 120,
  provider_retries: 0,
  max_agent_turns_per_arm: 1,
  isolation_revision: "pi-benchmark-isolation-v1",
  pi_runtime_revision: PI_RUNTIME_REVISION,
  pi_sdk_lock_revision: PI_SDK_LOCK_REVISION,
  direct_output_mode: "text_sql",
};
const legacyGenerationContract: BenchmarkGenerationContractV2 = {
  forge_output_mode: "text_json",
  forge_prompt_revision: "forge-benchmark-text-legacy",
  forge_schema_revision: null,
  provider_json_schema_request: "disabled",
  forge_wire_schema_revision: null,
  direct_output_mode: "text_sql",
};

interface SuiteCase {
  case_id: string;
  question_id: number;
  db_id: string;
  difficulty: string;
  question: string;
  evidence: string;
}
interface ContextResponse {
  protocol_revision: string;
  metric_revision: string;
  case: SuiteCase;
  context_snapshot: ContextSnapshotV2;
  schema_context: string;
  forge_prompt_revision: string;
  forge_instructions: string;
  direct_instructions: string;
}
interface ArmEvaluation extends Record<string, unknown> {
  protocol_revision: string;
  metric_revision: string;
  compile_status: ArmMetricsV2["compile_status"];
  execution_status: ArmMetricsV2["execution_status"];
  scored: boolean;
  official_ea: boolean | null;
  contract_accuracy: boolean | null;
  failure: NonNullable<ArmMetricsV2["failure"]> | null;
  error_code: ArmMetricsV2["error_code"];
  sql: string | null;
}
interface PersistedRun {
  protocol_manifest?: Record<string, unknown>;
  gold_readiness?: BenchmarkGoldReadinessV2;
  protocol_revision?: string;
  contexts?: Record<string, ContextResponse>;
  dispatched_calls?: number;
  run_id: string;
  task_run_id: string;
  status: BenchmarkRunStatus;
  suite_id: string;
  metric_revision?: string;
  model: BenchmarkRunProjectionV2["model"];
  generation_contract?: BenchmarkGenerationContractV2;
  total_cases: number;
  total_calls: number;
  sequence: number;
  current_case: { case_id: string; question: string } | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
}

interface RuntimeOwner { owner_id: string; host: string; pid: number }
const TERMINAL_RUN_STATUSES = new Set<BenchmarkRunStatus>(["completed", "failed", "stopped", "interrupted"]);

const emptyArm = (): ArmMetricsV2 => ({
  generation_ms: null,
  prompt_tokens: 0,
  completion_tokens: 0,
  cache_read_tokens: 0,
  cache_write_tokens: 0,
  total_tokens: 0,
  usage_observed: true,
  compile_status: "pending",
  execution_status: "pending",
  scored: false,
  official_ea: null,
  contract_accuracy: null,
  failure: null,
  error_code: null,
  sql: null,
  output: null,
});

export function benchmarkResourceLoader(cwd: string, agentDir: string, settingsManager: SettingsManager): DefaultResourceLoader {
  // Never reload: this explicit loader has no filesystem resources.
  const loader = new DefaultResourceLoader({ cwd, agentDir, settingsManager,
    noExtensions: true, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true });
  loader.getSystemPrompt = () => "Follow only the controlled benchmark instructions. Produce exactly one final candidate.";
  return loader;
}

function outputHash(value: unknown): string {
  return "sha256:" + createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

function now(): string { return new Date().toISOString(); }
function parse<T>(raw: unknown): T { return JSON.parse(String(raw)) as T; }
function assistantText(messages: readonly any[]): string {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message?.role !== "assistant") continue;
    if (typeof message.content === "string") return message.content.trim();
    if (Array.isArray(message.content)) {
      return message.content
        .filter((item: any) => item?.type === "text")
        .map((item: any) => String(item.text))
        .join("")
        .trim();
    }
  }
  return "";
}

export class BenchmarkInputError extends Error {}
class MetricRevisionMismatch extends Error {}
class EmptyGenerationError extends Error {}

function requireGoldReadiness(value: unknown, cases: SuiteCase[]): BenchmarkGoldReadinessV2 {
  const readiness = value as BenchmarkGoldReadinessV2 | undefined;
  if (!readiness || !["require_all", "skip_unscorable"].includes(readiness.policy)
    || !Array.isArray(readiness.blocked_cases)) throw new BenchmarkInputError("Frozen Gold readiness policy/report is required");
  const selected = new Map(cases.map((item) => [item.case_id, item.db_id]));
  const seen = new Set<string>();
  for (const blocked of readiness.blocked_cases) {
    if (!blocked || !selected.has(blocked.case_id) || selected.get(blocked.case_id) !== blocked.db_id
      || seen.has(blocked.case_id) || typeof blocked.code !== "string" || !blocked.code.trim()) {
      throw new BenchmarkInputError("Gold blocked cases must be unique selected cases with matching databases and failure codes");
    }
    seen.add(blocked.case_id);
  }
  if (readiness.policy === "require_all" && seen.size) throw new BenchmarkInputError("require_all forbids unscorable Gold cases");
  return readiness;
}

function sameGoldReadiness(left: BenchmarkGoldReadinessV2, right: BenchmarkGoldReadinessV2): boolean {
  return left.policy === right.policy && left.blocked_cases.length === right.blocked_cases.length
    && left.blocked_cases.every((item) => right.blocked_cases.some((other) =>
      item.case_id === other.case_id && item.db_id === other.db_id && item.code === other.code));
}

function requireMetricRevision(expected: string | undefined, received: unknown): asserts received is string {
  if (typeof expected !== "string" || !expected || received !== expected) {
    throw new MetricRevisionMismatch(`Result comparator revision mismatch: expected ${expected ?? "unknown"}, received ${received ?? "unknown"}`);
  }
}
function hasCurrentGenerationContract(run: PersistedRun): boolean {
  const contract = run.generation_contract;
  return contract?.provider_json_schema_request === "required"
    && contract.forge_wire_schema_revision === strictForgeOutput.revision
    && contract.forge_schema_revision === FORGE_SCHEMA_REVISION
    && typeof run.protocol_manifest?.forge_prompt_revision === "string"
    && Boolean(run.protocol_manifest.forge_prompt_revision)
    && contract.forge_prompt_revision === run.protocol_manifest.forge_prompt_revision
    && contract.sampling === "provider_default" && contract.transport === "sse" && contract.max_output_tokens === null
    && contract.timeout_seconds === 120 && contract.provider_retries === 0
    && contract.max_agent_turns_per_arm === 1 && contract.isolation_revision === "pi-benchmark-isolation-v1"
    && contract.pi_runtime_revision === PI_RUNTIME_REVISION && contract.pi_sdk_lock_revision === PI_SDK_LOCK_REVISION;
}

function requireGenerationContract(run: PersistedRun): void {
  if (!hasCurrentGenerationContract(run)) throw new StrictForgeOutputError("Strict Forge generation contract mismatch; start a new run");
}


export class PiBenchmarkRuntime {
  readonly #db: DatabaseSync;
  readonly #controllers = new Map<string, Set<AbortController>>();
  readonly #forgeHeaders: Record<string, string>;
  #modelRuntime: ModelRuntime | undefined;
  readonly #ownerId = randomUUID();
  readonly #executions = new Map<string, Promise<void>>();
  readonly #forgeRequests = new Set<AbortController>();
  #starting: Promise<BenchmarkRunProjectionV2> | undefined;
  #closing = false;
  #closePromise: Promise<void> | undefined;

  constructor(
    private readonly config: OrchestratorConfig,
    private readonly application: OrchestratorApplication,
  ) {
    if (!(application.state instanceof SqliteOrchestratorState)) {
      throw new Error("Benchmark requires the application SQLite state transaction boundary");
    }
    this.#db = application.state.database;
    this.#db.exec(`
      CREATE TABLE IF NOT EXISTS benchmark_v2_runs (
        run_id TEXT PRIMARY KEY, status TEXT NOT NULL, created_at TEXT NOT NULL, data_json TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS benchmark_v2_cases (
        run_id TEXT NOT NULL, case_id TEXT NOT NULL, status TEXT NOT NULL, data_json TEXT NOT NULL,
        PRIMARY KEY(run_id, case_id)
      );
      CREATE TABLE IF NOT EXISTS benchmark_v2_logs (
        log_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, case_id TEXT, arm TEXT,
        stage TEXT NOT NULL, level TEXT NOT NULL, message TEXT NOT NULL, payload_json TEXT NOT NULL, created_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS benchmark_runtime_owner (
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1), owner_id TEXT NOT NULL, host TEXT NOT NULL, pid INTEGER NOT NULL
      ) STRICT;
    `);
    this.#forgeHeaders = { "content-type": "application/json" };
    if (config.forgePiServiceKey) this.#forgeHeaders["x-pi-service-key"] = config.forgePiServiceKey;
    application.state.transactions.run(() => {
      const owner = this.#db.prepare("SELECT owner_id,host,pid FROM benchmark_runtime_owner WHERE singleton=1").get() as RuntimeOwner | undefined;
      if (owner) {
        // This SQLite runtime is single-host. Unknown/living owners fail closed; no lease can replay paid work.
        if (owner.host !== hostname()) return;
        try { process.kill(owner.pid, 0); return; }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") return; }
      }
      this.#db.prepare("INSERT OR REPLACE INTO benchmark_runtime_owner VALUES(1,?,?,?)").run(this.#ownerId, hostname(), process.pid);
      const active = this.#db.prepare(
        "SELECT data_json FROM benchmark_v2_runs WHERE status IN ('queued','running','pausing','paused','stopping')",
      ).all() as Array<{ data_json: string }>;
      for (const row of active) {
        const run = parse<PersistedRun>(row.data_json);
        if (!run.protocol_revision) continue;
        run.status = "interrupted";
        run.completed_at = now();
        run.current_case = null;
        run.sequence += 1;
        this.#saveRun(run);
        this.#log(run.run_id, null, "shared", "runtime", "warning", "Previous owner exited: interrupted without replaying model calls.", {});
      }
      this.#db.exec("CREATE UNIQUE INDEX IF NOT EXISTS benchmark_one_active_run ON benchmark_v2_runs((1)) WHERE status IN ('queued','running','pausing','paused','stopping') AND json_extract(data_json, '$.protocol_revision') IS NOT NULL");
    });
  }

  async modelOptions(): Promise<Array<{ provider: string; model: string; ready: boolean }>> {
    const runtime = await this.#runtime();
    const providers = runtime.getProviders();
    const options: Array<{ provider: string; model: string; ready: boolean }> = [];
    for (const provider of providers) {
      const available = new Set((await runtime.getAvailable(provider.id)).map((model) => model.id));
      for (const model of runtime.getModels(provider.id)) {
        options.push({ provider: provider.id, model: model.id, ready: available.has(model.id) });
      }
    }
    return options;
  }

  async start(options: {
    provider: string;
    model: string;
    limit?: number;
    caseIds?: string[];
    confirmModelCalls?: number;
    protocolManifest?: Record<string, unknown>;
  }): Promise<BenchmarkRunProjectionV2> {
    this.#assertOwner();
    if (this.#starting) throw new BenchmarkInputError("A benchmark admission is already in progress");
    this.#starting = this.#start(options);
    try { return await this.#starting; }
    finally { this.#starting = undefined; }
  }

  async #start(options: Parameters<PiBenchmarkRuntime["start"]>[0]): Promise<BenchmarkRunProjectionV2> {
    if (options.limit !== undefined && (!Number.isSafeInteger(options.limit) || options.limit <= 0)) throw new BenchmarkInputError("limit must be a positive integer");
    if (options.caseIds !== undefined && (!Array.isArray(options.caseIds) || !options.caseIds.length
      || options.caseIds.some((id) => typeof id !== "string" || !id.trim())
      || new Set(options.caseIds).size !== options.caseIds.length)) throw new BenchmarkInputError("case_ids must contain unique nonempty IDs");
    if (!Number.isSafeInteger(options.confirmModelCalls) || Number(options.confirmModelCalls) < 0) throw new BenchmarkInputError("Explicit nonnegative confirm_model_calls is required");
    const active = this.#db.prepare(
      "SELECT run_id FROM benchmark_v2_runs WHERE status IN ('queued','running','pausing','paused','stopping') AND json_extract(data_json, '$.protocol_revision') IS NOT NULL LIMIT 1",
    ).get();
    if (active) throw new Error("A Pi Benchmark run is already active");
    const runtime = await this.#runtime();
    const selected = runtime.getModel(options.provider, options.model);
    if (!selected) throw new Error(`Model is not registered: ${options.provider}/${options.model}`);
    requireStrictForgeApi(selected);
    const ready = (await runtime.getAvailable(options.provider)).some((model) => model.id === options.model);
    if (!ready) throw new Error(`Model is unavailable: ${options.provider}/${options.model}`);

    const suite = await this.#forgeGet<{ suite: { suite: string }; cases: SuiteCase[]; metric_revision: string }>(
      "/api/internal/benchmark-v2/suite",
    );
    requireMetricRevision(suite.metric_revision, suite.metric_revision);
    let cases = suite.cases;
    if (options.caseIds) {
      const byId = new Map(cases.map((item) => [item.case_id, item]));
      cases = options.caseIds.map((id) => {
        const item = byId.get(id);
        if (!item) throw new BenchmarkInputError("Unknown case ID: " + id);
        return item;
      });
    }
    if (options.limit !== undefined) {
      if (options.limit > cases.length) throw new BenchmarkInputError("limit exceeds selected suite size");
      cases = cases.slice(0, options.limit);
    }
    const requestedReadiness = options.protocolManifest
      ? requireGoldReadiness(options.protocolManifest.gold_readiness, cases)
      : { policy: "require_all" as const, blocked_cases: [] };
    const modelCalls = 2 * (cases.length - requestedReadiness.blocked_cases.length);
    if (!cases.length || options.confirmModelCalls !== modelCalls) throw new BenchmarkInputError("confirm_model_calls must equal exactly 2(N - frozen Gold blocked cases)");
    const protocol = await this.#forgePost<{
      manifest: Record<string, unknown>; protocol_revision: string; case_ids: string[];
      metric_revision: string; forge_prompt_revision: string; forge_schema_revision: string;
      contexts: Record<string, ContextResponse>;
      gold_readiness: BenchmarkGoldReadinessV2;
      generation: { max_model_calls: number; provider_retries: number; max_agent_turns_per_arm: number; timeout_seconds: number; sampling: string; max_output_tokens: number | null };
    }>("/api/internal/benchmark-v2/protocol", {
      provider: options.provider, model: options.model, case_ids: cases.map((item) => item.case_id),
      confirm_model_calls: options.confirmModelCalls,
      ...(options.protocolManifest ? { protocol_manifest: options.protocolManifest } : {}),
    });
    requireMetricRevision(suite.metric_revision, protocol.metric_revision);
    const goldReadiness = requireGoldReadiness(protocol.gold_readiness, cases);
    const manifestReadiness = requireGoldReadiness(protocol.manifest?.gold_readiness, cases);
    if (!sameGoldReadiness(goldReadiness, manifestReadiness) || !sameGoldReadiness(goldReadiness, requestedReadiness)) {
      throw new BenchmarkInputError("Frozen Gold readiness differs from the authorized manifest/report");
    }
    const blockedIds = new Set(goldReadiness.blocked_cases.map((item) => item.case_id));
    if (!protocol.protocol_revision || !protocol.manifest ||
      JSON.stringify(protocol.case_ids) !== JSON.stringify(cases.map((item) => item.case_id)) ||
      typeof protocol.forge_prompt_revision !== "string" || !protocol.forge_prompt_revision ||
      protocol.forge_prompt_revision !== protocol.manifest.forge_prompt_revision || protocol.forge_schema_revision !== FORGE_SCHEMA_REVISION ||
      protocol.generation.max_model_calls !== options.confirmModelCalls || protocol.generation.provider_retries !== 0 ||
      protocol.generation.max_agent_turns_per_arm !== 1 || protocol.generation.timeout_seconds !== 120 ||
      protocol.generation.sampling !== "provider_default" || protocol.generation.max_output_tokens !== null) {
      throw new Error("Frozen benchmark protocol does not match the generation contract");
    }
    for (const item of cases) {
      if (blockedIds.has(item.case_id)) continue;
      const context = protocol.contexts[item.case_id];
      if (!context || context.case.case_id !== item.case_id || context.metric_revision !== protocol.metric_revision ||
        context.forge_prompt_revision !== protocol.forge_prompt_revision) throw new Error("Frozen benchmark context mismatch");
    }
    const generationContract: BenchmarkGenerationContractV2 = {
      ...structuredGenerationContract, forge_prompt_revision: protocol.forge_prompt_revision,
    };
    this.#assertOwner();
    const run = this.application.state.transactions.run(() => {
      const runId = `pbr_${randomUUID().replaceAll("-", "")}`;
      const created = this.application.createTask({
        org_id: "org_benchmark",
        team_id: "team_benchmark",
        user_id: "benchmark_operator",
        channel: "api",
        intent: "pi-native rag dual-subagent benchmark",
        message: `Benchmark ${cases.length} BIRD cases`,
        metadata: {
          benchmark: true,
          benchmark_run_id: runId,
          suite_id: String(suite.suite.suite),
          provider: options.provider,
          model: options.model,
          forge_output_mode: generationContract.forge_output_mode,
          forge_prompt_revision: generationContract.forge_prompt_revision,
          forge_schema_revision: generationContract.forge_schema_revision,
        },
      });
      const revision = computePiModelRevision({
        agentDir: this.config.agentDir,
        provider: options.provider,
        modelId: options.model,
      }) ?? `unresolved:${options.provider}/${options.model}`;
      const run: PersistedRun = {
        run_id: runId,
        task_run_id: created.task.task_run_id,
        status: "queued",
        suite_id: String(suite.suite.suite),
        metric_revision: suite.metric_revision,
        model: {
          provider: options.provider,
          model: options.model,
          revision,
          temperature: null,
          max_output_tokens: null,
        },
        generation_contract: generationContract,
        protocol_manifest: protocol.manifest,
        gold_readiness: goldReadiness,
        protocol_revision: protocol.protocol_revision,
        contexts: protocol.contexts,
        dispatched_calls: 0,
        total_cases: cases.length,
        total_calls: modelCalls,
        sequence: 1,
        current_case: null,
        created_at: now(),
        started_at: null,
        completed_at: null,
        error: null,
      };
      this.#db.prepare("INSERT INTO benchmark_v2_runs VALUES(?,?,?,?)").run(
        run.run_id,
        run.status,
        run.created_at,
        JSON.stringify(run),
      );
      const insert = this.#db.prepare("INSERT INTO benchmark_v2_cases VALUES(?,?,?,?)");
      for (const item of cases) {
        const projection: BenchmarkCaseProjectionV2 = {
          ...item,
          status: "pending",
          current_stage: "queued",
          context_snapshot: null,
          failure: null,
          forge: emptyArm(),
          direct: emptyArm(),
          winner: null,
          started_at: null,
          completed_at: null,
        };
        if (blockedIds.has(item.case_id)) {
          projection.status = "failed";
          projection.current_stage = "gold_preflight";
          projection.completed_at = run.created_at;
          projection.failure = { stage: "gold", code: "gold_execution_failed", retryable: false };
          for (const arm of ["forge", "direct"] as const) {
            projection[arm] = { ...emptyArm(), compile_status: "not_applicable", execution_status: "skipped",
              failure: projection.failure, error_code: "gold_execution_failed",
              evidence: { dispatches: 0, payload_hash: null, response_output_hash: null } };
          }
        }
        insert.run(run.run_id, item.case_id, projection.status, JSON.stringify(projection));
      }
      this.#log(
        run.run_id,
        null,
        "shared",
        "run",
        "info",
        `已创建 Pi Benchmark：${cases.length} cases / ${modelCalls} Sub-Agent calls，模型 ${options.provider}/${options.model}。`,
        { generation_contract: generationContract },
      );
      return run;
    });
    this.#launch(run.run_id);
    return this.get(run.run_id)!;
  }

  get(runId: string): BenchmarkRunProjectionV2 | undefined {
    const row = this.#db.prepare("SELECT data_json FROM benchmark_v2_runs WHERE run_id=?").get(runId) as
      | { data_json: string }
      | undefined;
    if (!row) return undefined;
    const run = parse<PersistedRun>(row.data_json);
    const cases = (
      this.#db.prepare("SELECT data_json FROM benchmark_v2_cases WHERE run_id=? ORDER BY case_id").all(runId) as Array<{ data_json: string }>
    ).map((item) => parse<BenchmarkCaseProjectionV2>(item.data_json));
    for (const item of cases) {
      for (const name of ["forge", "direct"] as const) {
        const arm = item[name];
        // Legacy false scores also represented Gold failures. Only positive evidence is safe.
        arm.scored ??= arm.official_ea === true || arm.contract_accuracy === true;
        if (!arm.scored) {
          arm.official_ea = null;
          arm.contract_accuracy = null;
        }
      }
      if (item.forge.contract_accuracy === null || item.direct.contract_accuracy === null) item.winner = null;
    }
    const completed = cases.filter((item) => item.status === "passed" || item.status === "failed").length;
    const calls = cases.reduce(
      (sum, item) => sum + (item.forge.generation_ms == null ? 0 : 1) + (item.direct.generation_ms == null ? 0 : 1),
      0,
    );
    const writable = this.#writable;
    return {
      schema_version: 2,
      projection_type: "pi_benchmark_run_v2",
      ...run,
      metric_revision: run.metric_revision ?? null,
      generation_contract: { ...legacyGenerationContract, ...run.generation_contract },
      completed_cases: completed,
      completed_calls: run.dispatched_calls ?? calls,
      controls: {
        can_pause: writable && Boolean(run.protocol_revision) && run.status === "running",
        can_resume: writable && run.status === "paused" && Boolean(run.protocol_revision) && hasCurrentGenerationContract(run),
        can_stop: writable && Boolean(run.protocol_revision) && ["queued", "running", "pausing", "paused"].includes(run.status),
      },
      dag: this.#dag(run, cases),
      metrics: this.#metrics(cases, run.total_cases),
      cases,
    };
  }

  latest(): BenchmarkRunProjectionV2 | undefined {
    const row = this.#db.prepare(
      "SELECT run_id FROM benchmark_v2_runs ORDER BY created_at DESC LIMIT 1",
    ).get() as { run_id: string } | undefined;
    return row ? this.get(row.run_id) : undefined;
  }

  history(limit = 20): BenchmarkRunProjectionV2[] {
    return (
      this.#db.prepare("SELECT run_id FROM benchmark_v2_runs ORDER BY created_at DESC LIMIT ?").all(
        Math.max(1, Math.min(100, limit)),
      ) as Array<{ run_id: string }>
    ).map((item) => this.get(item.run_id)!).filter(Boolean);
  }

  logs(
    runId: string,
    options: { arm?: string; stage?: string; caseId?: string; search?: string; limit?: number; offset?: number } = {},
  ): { total: number; items: BenchmarkLogV2[] } {
    const clauses = ["run_id=?"];
    const values: any[] = [runId];
    for (const [column, value] of [
      ["arm", options.arm],
      ["stage", options.stage],
      ["case_id", options.caseId],
    ] as const) {
      if (value) {
        clauses.push(`${column}=?`);
        values.push(value);
      }
    }
    if (options.search) {
      clauses.push("message LIKE ?");
      values.push(`%${options.search.slice(0, 100)}%`);
    }
    const where = clauses.join(" AND ");
    const limit = Math.max(1, Math.min(500, options.limit ?? 100));
    const offset = Math.max(0, options.offset ?? 0);
    const total = Number(
      (this.#db.prepare(`SELECT COUNT(*) n FROM benchmark_v2_logs WHERE ${where}`).get(...values) as { n: number }).n,
    );
    const rows = this.#db.prepare(
      `SELECT * FROM benchmark_v2_logs WHERE ${where} ORDER BY log_id DESC LIMIT ? OFFSET ?`,
    ).all(...values, limit, offset) as any[];
    return { total, items: rows.map((row) => ({ ...row, payload: parse(row.payload_json) })) };
  }

  pause(runId: string): BenchmarkRunProjectionV2 { return this.#control(runId, "pausing"); }
  resume(runId: string): BenchmarkRunProjectionV2 { return this.#control(runId, "running"); }
  stop(runId: string): BenchmarkRunProjectionV2 {
    const projection = this.#control(runId, "stopping");
    for (const controller of this.#controllers.get(runId) ?? []) controller.abort();
    return projection;
  }

  async #execute(runId: string): Promise<void> {
    const run = this.#run(runId);
    requireMetricRevision(run.metric_revision, run.metric_revision);
    requireGenerationContract(run);
    run.status = "running";
    run.started_at = run.started_at ?? now();
    run.sequence += 1;
    this.#saveRun(run);
    const pending = () => this.#db.prepare(
      "SELECT case_id FROM benchmark_v2_cases WHERE run_id=? AND status='pending' ORDER BY case_id",
    ).all(runId) as Array<{ case_id: string }>;
    const workers = Array.from({ length: this.config.benchmarkConcurrency }, async () => {
      while (true) {
        let current = this.#run(runId);
        if (current.status === "stopping" || TERMINAL_RUN_STATUSES.has(current.status)) return;
        if (current.status === "pausing") {
          current.status = "paused";
          current.sequence += 1;
          this.#saveRun(current);
        }
        if (current.status === "paused") {
          await new Promise((resolve) => setTimeout(resolve, 250));
          continue;
        }
        const next = pending()[0];
        if (!next) return;
        const claimed = this.#db.prepare(
          "UPDATE benchmark_v2_cases SET status='running' WHERE run_id=? AND case_id=? AND status='pending'",
        ).run(runId, next.case_id);
        if (Number(claimed.changes) !== 1) continue;
        await this.#processCase(runId, next.case_id);
      }
    });
    await Promise.all(workers);
    const final = this.#run(runId);
    if (final.status === "stopping") final.status = "stopped";
    else if (final.status === "running") {
      if (final.gold_readiness?.blocked_cases.length) {
        final.status = "failed";
        final.error = "Diagnostic run incomplete: frozen unscorable Gold cases were skipped without replacement";
      } else final.status = "completed";
    }
    if (["completed", "stopped", "failed"].includes(final.status)) final.completed_at = now();
    final.current_case = null;
    final.sequence += 1;
    this.#saveRun(final);
    this.#log(runId, null, "shared", "run", final.status === "failed" ? "error" : "success", `Benchmark ${final.status}。`, {});
  }

  async #processCase(runId: string, caseId: string): Promise<void> {
    let item = this.#case(runId, caseId);
    item.status = "running";
    item.current_stage = "rag";
    item.started_at = now();
    this.#saveCase(runId, item);
    let run = this.#run(runId);
    run.current_case = { case_id: caseId, question: item.question };
    run.sequence += 1;
    this.#saveRun(run);
    this.#log(runId, caseId, "shared", "rag", "info", "开始 RAG 分析与有界召回。", {});
    try {
      const checkedContext = await this.#forgePost<ContextResponse>(
        "/api/internal/benchmark-v2/context",
        { case_id: caseId, protocol_revision: run.protocol_revision },
        runId,
      );
      const context = run.contexts?.[caseId];
      if (!context || checkedContext.protocol_revision !== run.protocol_revision ||
        checkedContext.forge_prompt_revision !== context.forge_prompt_revision ||
        JSON.stringify(checkedContext.context_snapshot) !== JSON.stringify(context.context_snapshot) ||
        checkedContext.forge_instructions !== context.forge_instructions || checkedContext.direct_instructions !== context.direct_instructions) {
        throw new MetricRevisionMismatch("Frozen protocol context drift");
      }
      requireMetricRevision(run.metric_revision, checkedContext.metric_revision);
      requireMetricRevision(run.metric_revision, context.metric_revision);
      if (["failed", "stopping", "stopped", "interrupted"].includes(this.#run(runId).status)) {
        item.status = "cancelled";
        item.completed_at = now();
        this.#saveCase(runId, item);
        return;
      }
      const expectedPromptRevision = (run.generation_contract ?? legacyGenerationContract).forge_prompt_revision;
      if (context.forge_prompt_revision !== expectedPromptRevision) {
        throw new Error(
          `Forge prompt revision mismatch: expected ${expectedPromptRevision}, received ${context.forge_prompt_revision}`,
        );
      }
      item.context_snapshot = context.context_snapshot;
      item.current_stage = "parallel_generation";
      this.#saveCase(runId, item);
      this.#log(
        runId,
        caseId,
        "shared",
        "rag",
        "success",
        `ContextSnapshot ${context.context_snapshot.content_hash.slice(0, 20)} · ${context.context_snapshot.tables.length} tables · ${context.context_snapshot.fields.length} fields。`,
        { rounds: context.context_snapshot.retrieval_rounds },
      );
      for (const round of context.context_snapshot.retrieval_rounds) {
        this.#log(
          runId,
          caseId,
          "shared",
          "rag.round",
          round.sufficient ? "success" : "info",
          "第 " + round.round_index + " 轮召回：top_k=" + round.top_k
            + "，" + round.selected_tables.length + " 张表，"
            + round.selected_fields.length + " 个字段，覆盖率 "
            + (round.concept_coverage * 100).toFixed(1) + "% ，"
            + (round.sufficient ? "判定充分。" : "继续扩展。"),
          {
            top_k: round.top_k,
            tables: round.selected_tables,
            fields: round.selected_fields,
            relationships: round.relationship_paths,
            coverage: round.concept_coverage,
            sufficient: round.sufficient,
          },
        );
      }
      if (context.context_snapshot.sufficiency_status !== "sufficient") {
        throw new Error("retrieval_insufficient");
      }
      const [forge, direct] = await Promise.all([
        this.#runArm(runId, item, context, "forge"),
        this.#runArm(runId, item, context, "direct"),
      ]);
      item.forge = forge;
      item.direct = direct;
      const generationFailure = [forge, direct].find((arm) => arm.failure?.stage === "generation");
      if (generationFailure) {
        item.failure = generationFailure.failure ?? null;
        const error = new Error("Benchmark generation did not produce both candidates");
        this.#failRun(runId, error);
        throw error;
      }
      const unscored = !forge.scored ? forge : !direct.scored ? direct : null;
      if (unscored) {
        item.failure = unscored.failure ?? null;
        const error = new Error("Benchmark evaluation is unscored");
        this.#failRun(runId, error);
        throw error;
      }
      item.current_stage = "evaluated";
      item.failure = null;
      item.winner = forge.contract_accuracy === null || direct.contract_accuracy === null
        ? null
        : forge.contract_accuracy === direct.contract_accuracy
          ? "tie"
          : forge.contract_accuracy ? "forge" : "direct";
      item.status = "passed";
      item.completed_at = now();
    } catch (error) {
      const message = error instanceof Error ? error.message : "case failed";
      const cancelled = ["stopping", "stopped", "interrupted"].includes(this.#run(runId).status);
      if (!cancelled) this.#failRun(runId, error);
      item.status = cancelled ? "cancelled" : "failed";
      item.current_stage = item.status;
      item.failure ??= message === "retrieval_insufficient"
        ? { stage: "context", code: "retrieval_insufficient", retryable: true }
        : { stage: "context", code: "context_failed", retryable: true };
      item.completed_at = now();
      this.#log(
        runId,
        caseId,
        "shared",
        "case",
        "error",
        message,
        { failure: item.failure },
      );
    }
    this.#saveCase(runId, item);
    run = this.#run(runId);
    run.sequence += 1;
    this.#saveRun(run);
  }

  async #runArm(
    runId: string,
    item: BenchmarkCaseProjectionV2,
    context: ContextResponse,
    arm: BenchmarkArm,
  ): Promise<ArmMetricsV2> {
    const controller = new AbortController();
    const controllers = this.#controllers.get(runId) ?? new Set<AbortController>();
    controllers.add(controller);
    this.#controllers.set(runId, controllers);
    const started = performance.now();
    let activeSession: Awaited<ReturnType<typeof createAgentSession>>["session"] | undefined;
    let sessionAbort: Promise<void> | undefined;
    const abortSession = () => sessionAbort ??= activeSession?.abort();
    let unsubscribe: (() => void) | undefined;
    let timeout: ReturnType<typeof setTimeout> | undefined;
    let output: unknown = null;
    let rawArguments: unknown = null;
    let generationMs: number | null = null;
    let observedUsage: { input: number; output: number; cacheRead: number; cacheWrite: number; total: number } | undefined;
    const knownTokens = () => {
      const recorded = activeSession?.getSessionStats().tokens;
      // One dispatched response per arm: partial usage is a snapshot, never an additive event counter.
      return recorded?.total ? recorded : observedUsage ?? recorded;
    };
    const evidence = { dispatches: 0, payload_hash: null as string | null, response_output_hash: null as string | null };
    const usageObserved = () => evidence.dispatches === 0 || Number(knownTokens()?.total) > 0;
    const rawOutput = () => ({
      assistant: activeSession?.state.messages.filter((message) => message.role === "assistant")
        .map(({ content, stopReason, errorMessage }) => ({ content, stopReason, errorMessage: errorMessage ?? null })) ?? [],
      tool_arguments: rawArguments,
    });
    const run = this.#run(runId);
    this.#log(
      runId,
      item.case_id,
      arm,
      "generation",
      "info",
      `启动 ${arm} Pi Sub-Agent。`,
      { context_snapshot_id: context.context_snapshot.content_hash },
    );
    try {
      const runtime = await this.#runtime();
      const model = runtime.getModel(run.model.provider, run.model.model);
      if (!model) throw new Error(`Model unavailable: ${run.model.provider}/${run.model.model}`);
      this.#log(
        runId, item.case_id, arm, "generation.model", "info",
        "模型已就绪：" + run.model.provider + " / " + run.model.model
          + "，revision " + run.model.revision.slice(0, 20) + "。",
        { provider: run.model.provider, model: run.model.model, revision: run.model.revision },
      );
      let forgeOutput: Record<string, unknown> | null = null;
      let strictRequests = 0;
      const forgeTool = arm === "forge" ? defineTool({
        name: "emit_forge_query",
        label: "Emit Forge Query",
        description: "Submit the final Forge query. All schema properties are required: use null for unused optional fields. Preserve SQL NULL values in val/default. Call exactly once as the final action.",
        parameters: forgeToolSchema,
        // Validate raw wire arguments before Pi attempts recursive JSON coercion.
        prepareArguments: (args) => {
          rawArguments = structuredClone(args);
          this.#log(runId, item.case_id, arm, "generation.raw_arguments", "info", "Raw native tool arguments captured before validation.", { raw_tool_arguments: rawArguments });
          return strictForgeOutput.decode(args as Record<string, unknown>);
        },
        async execute(_toolCallId, params) {
          if (strictRequests !== 1) throw new StrictForgeOutputError("Forge output without a strict provider request");
          if (forgeOutput !== null) throw new StrictForgeOutputError("Multiple Forge candidates are forbidden");
          forgeOutput = params as Record<string, unknown>;
          return {
            content: [{ type: "text", text: "Forge query captured." }],
            details: {},
            terminate: true,
          };
        },
      }) : null;
      const settingsManager = SettingsManager.inMemory({
        enableSkillCommands: false,
        compaction: { enabled: false },
        retry: { enabled: false, maxRetries: 0, provider: { maxRetries: 0, timeoutMs: 120000 } },
      });
      const { session } = await createAgentSession({
        cwd: this.config.skillsRoot,
        resourceLoader: benchmarkResourceLoader(this.config.skillsRoot, this.config.agentDir, settingsManager),
        agentDir: this.config.agentDir,
        modelRuntime: runtime,
        model,
        settingsManager,
        sessionManager: SessionManager.inMemory(this.config.skillsRoot),
        ...(forgeTool === null
          ? { noTools: "all" as const, tools: [] }
          : { noTools: "builtin" as const, tools: ["emit_forge_query"], customTools: [forgeTool] }),
      });
      activeSession = session;
      const stream = session.agent.streamFunction;
      session.agent.streamFunction = (selected, providerContext, options) => {
        const current = this.#run(runId);
        if (controller.signal.aborted || ["failed", "stopping", "stopped"].includes(current.status)) throw new Error("Benchmark dispatch cancelled");
        if (evidence.dispatches !== 0 || (current.dispatched_calls ?? 0) >= current.total_calls) {
          throw new StrictForgeOutputError("A second agent turn or excess budget dispatch is forbidden");
        }
        evidence.dispatches += 1;
        item[arm] = { ...item[arm], usage_observed: false, evidence };
        this.#saveCase(runId, item);
        current.dispatched_calls = (current.dispatched_calls ?? 0) + 1;
        current.sequence += 1;
        this.#saveRun(current);
        this.#log(runId, item.case_id, arm, "generation.dispatch", "info", "Pi provider dispatch admitted; HTTP count/status is unobserved.", { dispatches: evidence.dispatches });
        return stream(selected, providerContext, { ...options, maxRetries: 0, timeoutMs: 120000, transport: "sse" });
      };
      session.agent.onPayload = (payload, selected) => {
        if (++strictRequests !== 1) throw new StrictForgeOutputError("A second provider payload is forbidden");
        if (controller.signal.aborted) throw new Error("Benchmark dispatch cancelled");
        const effective = arm === "forge"
          ? requireStrictForgePayload(payload, selected.api, strictForgeOutput.schema) : payload;
        evidence.payload_hash = outputHash(effective);
        this.#log(runId, item.case_id, arm, "generation.payload", "info", "Effective provider payload observed; not HTTP delivery evidence.", {
          payload_hash: evidence.payload_hash, wire_schema_revision: arm === "forge" ? strictForgeOutput.revision : null,
        });
        return effective;
      };
      this.#log(
        runId, item.case_id, arm, "generation.session", "info",
        arm === "forge"
          ? "Pi AgentSession 已创建；内置工具关闭，仅启用 schema-bound terminating tool。"
          : "Pi AgentSession 已创建；全部工具关闭，等待文本 SQL。",
        {
          context_snapshot_id: context.context_snapshot.content_hash,
          output_mode: arm === "forge" ? "pi_tool_schema" : "text_sql",
          tool_schema_chars: arm === "forge" ? forgeToolSchemaChars : 0,
        },
      );
      let streamEvents = 0;
      let firstActivityLogged = false;
      let turns = 0;
      unsubscribe = session.subscribe((event: any) => {
        if (event.type === "turn_start" && ++turns > 1) throw new StrictForgeOutputError("A second agent turn is forbidden");
        const usage = event.message?.role === "assistant" ? event.message.usage : undefined;
        if (usage && [usage.input, usage.output, usage.cacheRead, usage.cacheWrite].some((value) => Number.isFinite(value) && value > 0)) {
          observedUsage = { input: usage.input ?? 0, output: usage.output ?? 0,
            cacheRead: usage.cacheRead ?? 0, cacheWrite: usage.cacheWrite ?? 0,
            total: (usage.input ?? 0) + (usage.output ?? 0) + (usage.cacheRead ?? 0) + (usage.cacheWrite ?? 0) };
        }
        if (event.type === "message_update") {
          streamEvents += 1;
          if (!firstActivityLogged) {
            firstActivityLogged = true;
            this.#log(runId, item.case_id, arm, "generation.stream", "info", "模型开始流式返回。", {});
          } else if (streamEvents % 250 === 0) {
            this.#log(
              runId, item.case_id, arm, "generation.stream", "info",
              "已接收 " + streamEvents + " 个响应片段。",
              { stream_events: streamEvents },
            );
          }
        } else if (event.type === "auto_retry_start") {
          controller.abort();
          throw new StrictForgeOutputError("Provider/session retry is forbidden");
        }
      });
      controller.signal.addEventListener("abort", () => { void abortSession(); }, { once: true });
      const branchInstructions = arm === "forge" ? context.forge_instructions : context.direct_instructions;
      const outputInstruction = arm === "forge"
        ? "Call emit_forge_query exactly once with the final Forge query. Do not emit text, SQL, Markdown, or explanation."
        : "Return exactly one read-only SQLite SELECT query. No Markdown or explanation.";
      const prompt = [
        "You are the " + arm + " branch of a controlled SQL benchmark.",
        outputInstruction,
        "The Gold SQL and Gold result are intentionally hidden.",
        branchInstructions,
        "Question: " + item.question,
        "ContextSnapshot: " + JSON.stringify(
          context.context_snapshot,
          (key, value) => key === "question" || key === "evidence" ? undefined : value,
        ),
      ].join("\n\n");
      this.#log(
        runId, item.case_id, arm, "generation.prompt", "info",
        "Prompt 已提交：" + prompt.length + " 字符，"
          + context.context_snapshot.tables.length + " 张表，"
          + context.context_snapshot.fields.length + " 个字段。",
        {
          prompt_chars: prompt.length,
          tool_schema_chars: arm === "forge" ? forgeToolSchemaChars : 0,
          tables: context.context_snapshot.tables.length,
          fields: context.context_snapshot.fields.length,
        },
      );
      if (controller.signal.aborted) throw new Error("Benchmark cancelled before prompt");
      await Promise.race([
        session.prompt(prompt, { expandPromptTemplates: false, source: "rpc" }),
        new Promise<never>((_, reject) => {
          timeout = setTimeout(() => { controller.abort(); reject(new Error("Benchmark generation timeout")); }, 120000);
        }),
      ]);
      clearTimeout(timeout);
      generationMs = Math.round((performance.now() - started) * 10) / 10;
      evidence.response_output_hash = outputHash(rawOutput());

      const messages = session.state.messages;
      for (let index = messages.length - 1; index >= 0; index -= 1) {
        const message = messages[index];
        if (message?.role !== "assistant") continue;
        if ("stopReason" in message && ["error", "aborted", "length", "pending"].includes(String(message.stopReason))) {
          throw new Error("Incomplete model response (" + message.stopReason + ")" + (message.errorMessage ? ": " + message.errorMessage : ""));
        }
        break;
      }
      const raw = assistantText(messages as any[]);
      const tokens = knownTokens() ?? session.getSessionStats().tokens;
      if (arm === "direct" && !raw.trim()) throw new EmptyGenerationError("Direct model returned no SQL candidate");
      if (arm === "forge" && forgeOutput === null) {
        throw new EmptyGenerationError("Forge strict output tool was not called with valid arguments");
      }
      output = arm === "forge" ? forgeOutput : raw;
      const outputChars = typeof output === "string" ? output.length : JSON.stringify(output).length;
      this.#log(
        runId, item.case_id, arm, "generation.completed", "success",
        "模型响应结束：" + outputChars + " 字符，输入 " + (usageObserved() ? tokens.input : "未知")
          + "，输出 " + (usageObserved() ? tokens.output : "未知") + "，缓存读取 " + (usageObserved() ? tokens.cacheRead : "未知") + "。",
        {
          output_chars: outputChars,
          output,
          raw_output: rawOutput(),
          response_output_hash: evidence.response_output_hash,
          output_mode: arm === "forge" ? "pi_tool_schema" : "text_sql",
          stream_events: streamEvents,
          tokens,
          usage_observed: usageObserved(),
        },
      );

      this.#log(
        runId,
        item.case_id,
        arm,
        "output.handoff",
        "info",
        arm === "forge" ? "提交已校验的结构化 Forge 候选。" : "提交原始 SQL 候选。",
        {},
      );
      this.#log(runId, item.case_id, arm, "evaluation.request", "info", "提交 Forge 执行层进行编译、只读执行和双评价。", {});
      const evaluation = await this.#forgePost<ArmEvaluation>(
        "/api/internal/benchmark-v2/evaluate",
        { case_id: item.case_id, arm, output, context_snapshot: context.context_snapshot, metric_revision: run.metric_revision, protocol_revision: run.protocol_revision },
        runId,
      );
      requireMetricRevision(run.metric_revision, evaluation.metric_revision);
      if (evaluation.protocol_revision !== run.protocol_revision) throw new MetricRevisionMismatch("Evaluation protocol revision mismatch");
      if (typeof evaluation.scored !== "boolean") throw new MetricRevisionMismatch("Evaluation scored status is missing");
      const metrics: ArmMetricsV2 = {
        generation_ms: generationMs,
        elapsed_ms: Math.round((performance.now() - started) * 10) / 10,
        raw_output: rawOutput(),
        evidence,
        prompt_tokens: tokens.input,
        completion_tokens: tokens.output,
        cache_read_tokens: tokens.cacheRead,
        cache_write_tokens: tokens.cacheWrite,
        total_tokens: tokens.total,
        usage_observed: usageObserved(),
        compile_status: evaluation.compile_status,
        execution_status: evaluation.execution_status,
        scored: evaluation.scored,
        official_ea: evaluation.scored ? evaluation.official_ea : null,
        contract_accuracy: evaluation.scored ? evaluation.contract_accuracy : null,
        failure: evaluation.failure,
        error_code: evaluation.error_code,
        sql: evaluation.sql,
        output,
      };
      this.#log(
        runId,
        item.case_id,
        arm,
        "evaluation",
        evaluation.contract_accuracy ? "success" : "warning",
        `${arm}: EA=${evaluation.official_ea} Contract=${evaluation.contract_accuracy} tokens=${metrics.total_tokens}。`,
        {
          generation_ms: metrics.generation_ms,
          compile_status: metrics.compile_status,
          execution_status: metrics.execution_status,
          scored: metrics.scored,
        },
      );
      return metrics;
    } catch (error) {
      if (error instanceof MetricRevisionMismatch) this.#failRun(runId, error);
      // Cancellation may finalize the assistant message and usage before the session becomes idle.
      await abortSession();
      const generationFailed = output === null && !(error instanceof MetricRevisionMismatch);
      const failureCode = generationFailed ? error instanceof EmptyGenerationError ? "generation_empty" : "agent_failed" : "context_failed";
      evidence.response_output_hash = outputHash(rawOutput());
      const tokens = knownTokens();
      this.#log(
        runId,
        item.case_id,
        arm,
        "generation",
        "error",
        error instanceof Error ? error.message : "arm failed",
        { output, raw_output: rawOutput(), evidence, tokens, usage_observed: usageObserved() },
      );
      return {
        ...emptyArm(),
        output,
        raw_output: rawOutput(),
        evidence,
        generation_ms: generationMs ?? (evidence.dispatches ? Math.round((performance.now() - started) * 10) / 10 : null),
        elapsed_ms: Math.round((performance.now() - started) * 10) / 10,
        prompt_tokens: tokens?.input ?? 0,
        completion_tokens: tokens?.output ?? 0,
        cache_read_tokens: tokens?.cacheRead ?? 0,
        cache_write_tokens: tokens?.cacheWrite ?? 0,
        total_tokens: tokens?.total ?? 0,
        usage_observed: usageObserved(),
        compile_status: arm === "forge" ? "pending" : "not_applicable",
        execution_status: output === null ? "skipped" : "pending",
        scored: generationFailed,
        official_ea: generationFailed ? false : null,
        contract_accuracy: generationFailed ? false : null,
        failure: { stage: generationFailed ? "generation" : "context", code: failureCode, retryable: false },
        error_code: failureCode,
      };
    } finally {
      clearTimeout(timeout);
      try {
        await abortSession();
      } finally {
        unsubscribe?.();
        activeSession?.dispose();
        controllers.delete(controller);
      }
    }
  }

  async #runtime(): Promise<ModelRuntime> {
    if (!this.#modelRuntime) {
      const runtime = await ModelRuntime.create({
        authPath: join(this.config.agentDir, "auth.json"),
        modelsPath: join(this.config.agentDir, "models.json"),
        refreshOnCreate: false,
        allowModelNetwork: false,
      });
      // Benchmark provider registration is configuration-only; never execute project extensions.
      this.#modelRuntime = runtime;
    }
    return this.#modelRuntime;
  }

  async #forgeGet<T>(path: string): Promise<T> {
    return this.#forgeRequest<T>(path, { headers: this.#forgeHeaders });
  }
  async #forgePost<T>(path: string, body: unknown, runId?: string): Promise<T> {
    return this.#forgeRequest<T>(path, { method: "POST", headers: this.#forgeHeaders, body: JSON.stringify(body) }, runId);
  }
  async #forgeRequest<T>(path: string, init: RequestInit, runId?: string): Promise<T> {
    const controller = new AbortController();
    if (this.#closing) throw new BenchmarkInputError("Benchmark runtime is closing");
    this.#forgeRequests.add(controller);
    const controllers = runId ? this.#controllers.get(runId) ?? new Set<AbortController>() : undefined;
    if (runId && controllers) {
      controllers.add(controller);
      this.#controllers.set(runId, controllers);
      const run = this.#run(runId);
      if (run.status === "stopping" || TERMINAL_RUN_STATUSES.has(run.status)) controller.abort();
    }
    const timeout = setTimeout(() => controller.abort(new Error("Forge benchmark request timed out")), this.config.forgeTimeoutMs);
    const headers = new Headers(init.headers);
    const requestId = currentRequestId();
    if (requestId) headers.set("x-request-id", requestId);
    try {
      const response = await fetch(this.config.forgeBaseUrl + path, { ...init, headers, signal: controller.signal });
      if (response.status === 409) throw new MetricRevisionMismatch("Benchmark evaluation revision or context mismatch (409)");
      if (!response.ok) throw new Error(`Forge ${path} returned ${response.status}`);
      return await response.json() as T;
    } finally {
      clearTimeout(timeout);
      controllers?.delete(controller);
      this.#forgeRequests.delete(controller);
    }
  }
  #run(id: string): PersistedRun {
    const row = this.#db.prepare("SELECT data_json FROM benchmark_v2_runs WHERE run_id=?").get(id) as
      | { data_json: string }
      | undefined;
    if (!row) throw new Error("Benchmark run not found");
    return parse(row.data_json);
  }
  #case(runId: string, caseId: string): BenchmarkCaseProjectionV2 {
    const row = this.#db.prepare(
      "SELECT data_json FROM benchmark_v2_cases WHERE run_id=? AND case_id=?",
    ).get(runId, caseId) as { data_json: string } | undefined;
    if (!row) throw new Error("Benchmark case not found");
    return parse(row.data_json);
  }
  #saveRun(run: PersistedRun): void {
    this.application.state.transactions.run(() => {
      this.#db.prepare("UPDATE benchmark_v2_runs SET status=?,data_json=? WHERE run_id=?").run(run.status, JSON.stringify(run), run.run_id);
      // Historical benchmark records may predate the linked Task contract; never fabricate a parent.
      if (this.application.getTask(run.task_run_id)?.metadata.benchmark_run_id === run.run_id) {
        this.application.transitionBenchmarkTask({ taskRunId: run.task_run_id, runId: run.run_id, status: run.status, currentStage: `benchmark_${run.status}` });
      }
    });
  }
  #saveCase(runId: string, item: BenchmarkCaseProjectionV2): void {
    this.#db.prepare(
      "UPDATE benchmark_v2_cases SET status=?,data_json=? WHERE run_id=? AND case_id=?",
    ).run(item.status, JSON.stringify(item), runId, item.case_id);
  }
  #log(
    runId: string,
    caseId: string | null,
    arm: BenchmarkArm | "shared",
    stage: string,
    level: string,
    message: string,
    payload: Record<string, unknown>,
  ): void {
    this.#db.prepare(
      "INSERT INTO benchmark_v2_logs(run_id,case_id,arm,stage,level,message,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?)",
    ).run(runId, caseId, arm, stage, level, message, JSON.stringify(payload), now());
  }
  #control(runId: string, status: BenchmarkRunStatus): BenchmarkRunProjectionV2 {
    this.#assertOwner();
    const run = this.#run(runId);
    if (!run.protocol_revision || !run.protocol_manifest) throw new Error("Historical runs without a frozen protocol are read-only");
    const allowed: Record<string, string[]> = {
      pausing: ["running"],
      running: ["paused"],
      stopping: ["queued", "running", "pausing", "paused"],
    };
    if (!(allowed[status] ?? []).includes(run.status)) {
      throw new Error(`Cannot transition ${run.status} to ${status}`);
    }
    if (status === "running") {
      requireMetricRevision(run.metric_revision, run.metric_revision);
      requireGenerationContract(run);
    }
    run.status = status;
    run.sequence += 1;
    this.#saveRun(run);
    this.#log(runId, null, "shared", "control", "info", `Run ${status}。`, {});
    return this.get(runId)!;
  }
  #failRun(runId: string, error: unknown): void {
    const run = this.#run(runId);
    if (TERMINAL_RUN_STATUSES.has(run.status) || run.status === "stopping") return;
    run.status = "failed";
    run.error = error instanceof Error ? error.message : "runtime failed";
    run.current_case = null;
    for (const controller of this.#controllers.get(runId) ?? []) controller.abort();
    run.completed_at = now();
    run.sequence += 1;
    this.#saveRun(run);
    this.#log(runId, null, "shared", "run", "error", run.error, {});
  }

  get #writable(): boolean {
    const owner = this.#db.prepare("SELECT owner_id FROM benchmark_runtime_owner WHERE singleton=1").get() as { owner_id: string } | undefined;
    return !this.#closing && owner?.owner_id === this.#ownerId;
  }

  #assertOwner(): void {
    if (!this.#writable) throw new BenchmarkInputError("Benchmark runtime is read-only: another owner holds this SQLite database");
  }

  #launch(runId: string): void {
    const execution = this.#execute(runId).catch((error: unknown) => this.#failRun(runId, error));
    this.#executions.set(runId, execution);
    void execution.then(() => this.#executions.delete(runId), () => this.#executions.delete(runId));
  }

  close(): Promise<void> {
    return this.#closePromise ??= this.#shutdown();
  }

  async #shutdown(): Promise<void> {
    this.#closing = true;
    for (const controller of this.#forgeRequests) controller.abort();
    for (const [runId] of this.#executions) {
      const run = this.#run(runId);
      if (!TERMINAL_RUN_STATUSES.has(run.status)) {
        run.status = "interrupted";
        run.completed_at = now();
        run.current_case = null;
        run.sequence += 1;
        this.#saveRun(run);
      }
      for (const controller of this.#controllers.get(runId) ?? []) controller.abort();
    }
    await Promise.allSettled([...this.#executions.values(), ...(this.#starting ? [this.#starting] : [])]);
    this.#db.prepare("DELETE FROM benchmark_runtime_owner WHERE singleton=1 AND owner_id=?").run(this.#ownerId);
  }
  #metrics(cases: BenchmarkCaseProjectionV2[], totalCases: number): Record<string, unknown> {
    const arm = (name: BenchmarkArm) => {
      let scored = 0, eaObserved = 0, eaCorrect = 0, contractObserved = 0, contractCorrect = 0;
      let executionObserved = 0, executionPassed = 0, generationObserved = 0, generationMs = 0;
      let observedTotal = 0, unobservedArms = totalCases - cases.length;
      for (const item of cases) {
        const value = item[name];
        if (value.scored) {
          scored += 1;
          if (typeof value.official_ea === "boolean") eaObserved += 1;
          if (value.official_ea === true) eaCorrect += 1;
          if (typeof value.contract_accuracy === "boolean") contractObserved += 1;
          if (value.contract_accuracy === true) contractCorrect += 1;
        }
        if (["passed", "failed", "skipped"].includes(value.execution_status)) executionObserved += 1;
        if (value.execution_status === "passed") executionPassed += 1;
        if (typeof value.generation_ms === "number") {
          generationObserved += 1;
          generationMs += value.generation_ms;
        }
        observedTotal += value.total_tokens;
        if (value.usage_observed === false || (value.usage_observed === undefined
          && value.generation_ms !== null && value.total_tokens === 0)) unobservedArms += 1;
      }
      return {
        total_cases: totalCases,
        scored_cases: scored,
        unscored_cases: totalCases - scored,
        official_ea: totalCases > 0 && eaObserved === totalCases ? eaCorrect / totalCases : null,
        official_ea_correct_cases: eaCorrect,
        official_ea_observed_cases: eaObserved,
        official_ea_missing_cases: totalCases - eaObserved,
        contract_accuracy: totalCases > 0 && contractObserved === totalCases ? contractCorrect / totalCases : null,
        contract_accuracy_correct_cases: contractCorrect,
        contract_accuracy_observed_cases: contractObserved,
        contract_accuracy_missing_cases: totalCases - contractObserved,
        execution_success: totalCases > 0 && executionObserved === totalCases ? executionPassed / totalCases : null,
        execution_success_cases: executionPassed,
        execution_observed_cases: executionObserved,
        execution_missing_cases: totalCases - executionObserved,
        total_tokens: unobservedArms ? null : observedTotal,
        observed_total_tokens: observedTotal,
        usage_observed: unobservedArms === 0,
        unobserved_usage_arms: unobservedArms,
        average_generation_ms: generationObserved ? generationMs / generationObserved : null,
        generation_observed_cases: generationObserved,
        generation_missing_cases: totalCases - generationObserved,
      };
    };
    const forge = arm("forge");
    const direct = arm("direct");
    return {
      forge,
      direct,
      delta_ea: typeof forge.official_ea === "number" && typeof direct.official_ea === "number"
        ? forge.official_ea - direct.official_ea
        : null,
    };
  }
  #dag(run: PersistedRun, cases: BenchmarkCaseProjectionV2[]) {
    const active = cases.find((item) => item.status === "running");
    const stage = active?.current_stage ?? (run.status === "completed" ? "completed" : "pending");
    const status = (id: string) => stage === id
      ? "running" as const
      : run.status === "completed" ? "passed" as const : "pending" as const;
    return [
      {
        id: "rag",
        label: "RAG / ContextSnapshot",
        lane: "shared" as const,
        status: status("rag"),
        detail: active?.context_snapshot?.content_hash ?? "",
      },
      { id: "forge", label: "Forge JSON Sub-Agent", lane: "forge" as const, status: status("parallel_generation"), detail: "" },
      { id: "direct", label: "Direct SQL Sub-Agent", lane: "direct" as const, status: status("parallel_generation"), detail: "" },
      { id: "evaluation", label: "Official EA + ResultContract", lane: "evaluation" as const, status: status("evaluated"), detail: "" },
    ];
  }
}
