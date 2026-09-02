import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { dirname, join } from "node:path";
import {
  createAgentSession,
  ModelRuntime,
  SessionManager,
  SettingsManager,
} from "@earendil-works/pi-coding-agent";

import type { OrchestratorApplication } from "./application.js";
import { computePiModelRevision, type OrchestratorConfig } from "./config.js";
import type {
  ArmMetricsV2,
  BenchmarkArm,
  BenchmarkCaseProjectionV2,
  BenchmarkLogV2,
  BenchmarkRunProjectionV2,
  BenchmarkRunStatus,
  ContextSnapshotV2,
} from "./benchmark-contracts.js";

interface SuiteCase {
  case_id: string;
  question_id: number;
  db_id: string;
  difficulty: string;
  question: string;
  evidence: string;
}
interface ContextResponse { case: SuiteCase; context_snapshot: ContextSnapshotV2; schema_context: string; forge_instructions: string; direct_instructions: string; }
interface ArmEvaluation extends Record<string, unknown> {
  compile_status: ArmMetricsV2["compile_status"];
  execution_status: ArmMetricsV2["execution_status"];
  official_ea: boolean;
  contract_accuracy: boolean;
  failure: NonNullable<ArmMetricsV2["failure"]> | null;
  error_code: ArmMetricsV2["error_code"];
  sql: string | null;
}
interface PersistedRun {
  run_id: string;
  task_run_id: string;
  status: BenchmarkRunStatus;
  suite_id: string;
  model: BenchmarkRunProjectionV2["model"];
  total_cases: number;
  total_calls: number;
  sequence: number;
  current_case: { case_id: string; question: string } | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
}

const emptyArm = (): ArmMetricsV2 => ({
  generation_ms: null,
  prompt_tokens: 0,
  completion_tokens: 0,
  cache_read_tokens: 0,
  cache_write_tokens: 0,
  total_tokens: 0,
  compile_status: "pending",
  execution_status: "pending",
  official_ea: null,
  contract_accuracy: null,
  failure: null,
  error_code: null,
  sql: null,
  output: null,
});

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


export class PiBenchmarkRuntime {
  readonly #db: DatabaseSync;
  readonly #controllers = new Map<string, Set<AbortController>>();
  readonly #forgeHeaders: Record<string, string>;
  #modelRuntime: ModelRuntime | undefined;

  constructor(
    private readonly config: OrchestratorConfig,
    private readonly application: OrchestratorApplication,
  ) {
    mkdirSync(dirname(config.stateDbPath), { recursive: true });
    this.#db = new DatabaseSync(config.stateDbPath);
    this.#db.exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;");
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
    `);
    this.#forgeHeaders = { "content-type": "application/json" };
    if (config.forgePiServiceKey) this.#forgeHeaders["x-pi-service-key"] = config.forgePiServiceKey;
    const active = this.#db.prepare(
      "SELECT data_json FROM benchmark_v2_runs WHERE status IN ('queued','running','pausing','stopping')",
    ).all() as Array<{ data_json: string }>;
    for (const row of active) {
      const run = parse<PersistedRun>(row.data_json);
      run.status = "interrupted";
      run.completed_at = now();
      run.sequence += 1;
      this.#saveRun(run);
      this.#log(run.run_id, null, "shared", "runtime", "warning", "Pi 服务重启：运行已中断，未自动重放模型调用。", {});
    }
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
  }): Promise<BenchmarkRunProjectionV2> {
    const active = this.#db.prepare(
      "SELECT run_id FROM benchmark_v2_runs WHERE status IN ('queued','running','pausing','paused','stopping') LIMIT 1",
    ).get();
    if (active) throw new Error("A Pi Benchmark run is already active");
    const runtime = await this.#runtime();
    const selected = runtime.getModel(options.provider, options.model);
    if (!selected) throw new Error(`Model is not registered: ${options.provider}/${options.model}`);
    const ready = (await runtime.getAvailable(options.provider)).some((model) => model.id === options.model);
    if (!ready) throw new Error(`Model is unavailable: ${options.provider}/${options.model}`);

    const suite = await this.#forgeGet<{ suite: Record<string, any>; cases: SuiteCase[] }>(
      "/api/internal/benchmark-v2/suite",
    );
    let cases = suite.cases;
    if (options.caseIds?.length) {
      const ids = new Set(options.caseIds);
      cases = cases.filter((item) => ids.has(item.case_id));
    }
    if (options.limit) cases = cases.slice(0, options.limit);
    const created = this.application.createTask({
      org_id: "org_benchmark",
      team_id: "team_benchmark",
      user_id: "benchmark_operator",
      channel: "api",
      intent: "pi-native rag dual-subagent benchmark",
      message: `Benchmark ${cases.length} BIRD cases`,
      metadata: {
        benchmark: true,
        suite_id: String(suite.suite.suite),
        provider: options.provider,
        model: options.model,
      },
    });
    const revision = computePiModelRevision({
      agentDir: this.config.agentDir,
      provider: options.provider,
      modelId: options.model,
    }) ?? `unresolved:${options.provider}/${options.model}`;
    const run: PersistedRun = {
      run_id: `pbr_${randomUUID().replaceAll("-", "")}`,
      task_run_id: created.task.task_run_id,
      status: "queued",
      suite_id: String(suite.suite.suite),
      model: {
        provider: options.provider,
        model: options.model,
        revision,
        temperature: 0,
        max_output_tokens: 8192,
      },
      total_cases: cases.length,
      total_calls: cases.length * 2,
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
      insert.run(run.run_id, item.case_id, projection.status, JSON.stringify(projection));
    }
    this.#log(
      run.run_id,
      null,
      "shared",
      "run",
      "info",
      `已创建 Pi Benchmark：${cases.length} cases / ${cases.length * 2} Sub-Agent calls，模型 ${options.provider}/${options.model}。`,
      {},
    );
    void this.#execute(run.run_id).catch((error: unknown) => this.#failRun(run.run_id, error));
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
    const completed = cases.filter((item) => item.status === "passed" || item.status === "failed").length;
    const calls = cases.reduce(
      (sum, item) => sum + (item.forge.generation_ms == null ? 0 : 1) + (item.direct.generation_ms == null ? 0 : 1),
      0,
    );
    return {
      schema_version: 2,
      projection_type: "pi_benchmark_run_v2",
      ...run,
      completed_cases: completed,
      completed_calls: calls,
      controls: {
        can_pause: run.status === "running",
        can_resume: run.status === "paused",
        can_stop: ["queued", "running", "pausing", "paused"].includes(run.status),
      },
      dag: this.#dag(run, cases),
      metrics: this.#metrics(cases),
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
        if (current.status === "stopping") return;
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
    else if (final.status !== "paused") final.status = "completed";
    if (["completed", "stopped"].includes(final.status)) final.completed_at = now();
    final.current_case = null;
    final.sequence += 1;
    this.#saveRun(final);
    this.#log(runId, null, "shared", "run", "success", `Benchmark ${final.status}。`, {});
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
      const context = await this.#forgePost<ContextResponse>(
        "/api/internal/benchmark-v2/context",
        { case_id: caseId },
      );
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
      item.current_stage = "evaluated";
      item.failure = null;
      item.winner = forge.contract_accuracy === direct.contract_accuracy
        ? "tie"
        : forge.contract_accuracy ? "forge" : "direct";
      item.status = "passed";
      item.completed_at = now();
    } catch (error) {
      const message = error instanceof Error ? error.message : "case failed";
      item.status = "failed";
      item.current_stage = "failed";
      item.failure = message === "retrieval_insufficient"
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
      const { session } = await createAgentSession({
        cwd: this.config.skillsRoot,
        agentDir: this.config.agentDir,
        modelRuntime: runtime,
        model,
        settingsManager: SettingsManager.inMemory({
          enableSkillCommands: false,
          compaction: { enabled: false },
        }),
        sessionManager: SessionManager.inMemory(this.config.skillsRoot),
        noTools: "all",
        tools: [],
      });
      this.#log(
        runId, item.case_id, arm, "generation.session", "info",
        "Pi AgentSession 已创建；内置工具关闭，等待模型响应。",
        { context_snapshot_id: context.context_snapshot.content_hash },
      );
      let streamEvents = 0;
      let firstActivityLogged = false;
      const unsubscribe = session.subscribe((event: any) => {
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
          this.#log(
            runId, item.case_id, arm, "generation.retry", "warning",
            "Provider 自动重试第 " + event.attempt + " 次：" + event.errorMessage,
            { attempt: event.attempt, max_attempts: event.maxAttempts },
          );
        }
      });
      controller.signal.addEventListener("abort", () => { void session.abort(); }, { once: true });
      const branchInstructions = arm === "forge" ? context.forge_instructions : context.direct_instructions;
      const outputInstruction = arm === "forge"
        ? "Return exactly one valid Forge JSON object following the supplied Forge JSON rules. No SQL wrapper, Markdown, or explanation."
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
        { prompt_chars: prompt.length, tables: context.context_snapshot.tables.length, fields: context.context_snapshot.fields.length },
      );
      await session.prompt(prompt, { expandPromptTemplates: false, source: "rpc" });
      unsubscribe();
      const raw = assistantText(session.state.messages as any[]);
      const stats = session.getSessionStats();
      this.#log(
        runId, item.case_id, arm, "generation.completed", "success",
        "模型响应结束：" + raw.length + " 字符，输入 " + stats.tokens.input
          + "，输出 " + stats.tokens.output + "，缓存读取 " + stats.tokens.cacheRead + "。",
        { output_chars: raw.length, stream_events: streamEvents, tokens: stats.tokens },
      );
      session.dispose();
      this.#log(runId, item.case_id, arm, "output.handoff", "info", "提交原始候选，由 Forge 统一解析与保障。", {});
      const output = raw;
      this.#log(runId, item.case_id, arm, "evaluation.request", "info", "提交 Forge 执行层进行编译、只读执行和双评价。", {});
      const evaluation = await this.#forgePost<ArmEvaluation>(
        "/api/internal/benchmark-v2/evaluate",
        { case_id: item.case_id, arm, output, context_snapshot: context.context_snapshot },
      );
      const metrics: ArmMetricsV2 = {
        generation_ms: Math.round((performance.now() - started) * 10) / 10,
        prompt_tokens: stats.tokens.input,
        completion_tokens: stats.tokens.output,
        cache_read_tokens: stats.tokens.cacheRead,
        cache_write_tokens: stats.tokens.cacheWrite,
        total_tokens: stats.tokens.total,
        compile_status: evaluation.compile_status,
        execution_status: evaluation.execution_status,
        official_ea: evaluation.official_ea,
        contract_accuracy: evaluation.contract_accuracy,
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
        },
      );
      return metrics;
    } catch (error) {
      this.#log(
        runId,
        item.case_id,
        arm,
        "generation",
        "error",
        error instanceof Error ? error.message : "arm failed",
        {},
      );
      return {
        ...emptyArm(),
        generation_ms: Math.round((performance.now() - started) * 10) / 10,
        compile_status: arm === "forge" ? "failed" : "not_applicable",
        execution_status: "failed",
        failure: { stage: "generation", code: "agent_failed", retryable: true },
        error_code: "agent_failed",
      };
    } finally {
      controllers.delete(controller);
    }
  }

  async #runtime(): Promise<ModelRuntime> {
    if (!this.#modelRuntime) {
      this.#modelRuntime = await ModelRuntime.create({
        authPath: join(this.config.agentDir, "auth.json"),
        modelsPath: join(this.config.agentDir, "models.json"),
        refreshOnCreate: false,
        allowModelNetwork: false,
      });
    }
    return this.#modelRuntime;
  }

  async #forgeGet<T>(path: string): Promise<T> {
    const response = await fetch(this.config.forgeBaseUrl + path, { headers: this.#forgeHeaders });
    if (!response.ok) throw new Error(`Forge ${path} returned ${response.status}`);
    return await response.json() as T;
  }
  async #forgePost<T>(path: string, body: unknown): Promise<T> {
    const response = await fetch(this.config.forgeBaseUrl + path, {
      method: "POST",
      headers: this.#forgeHeaders,
      body: JSON.stringify(body),
    });
    if (!response.ok) throw new Error(`Forge ${path} returned ${response.status}`);
    return await response.json() as T;
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
    this.#db.prepare("UPDATE benchmark_v2_runs SET status=?,data_json=? WHERE run_id=?").run(
      run.status,
      JSON.stringify(run),
      run.run_id,
    );
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
    const run = this.#run(runId);
    const allowed: Record<string, string[]> = {
      pausing: ["running"],
      running: ["paused"],
      stopping: ["queued", "running", "pausing", "paused"],
    };
    if (!(allowed[status] ?? []).includes(run.status)) {
      throw new Error(`Cannot transition ${run.status} to ${status}`);
    }
    run.status = status;
    run.sequence += 1;
    this.#saveRun(run);
    this.#log(runId, null, "shared", "control", "info", `Run ${status}。`, {});
    return this.get(runId)!;
  }
  #failRun(runId: string, error: unknown): void {
    const run = this.#run(runId);
    if (["completed", "stopped"].includes(run.status)) return;
    run.status = "failed";
    run.error = error instanceof Error ? error.message : "runtime failed";
    run.completed_at = now();
    run.sequence += 1;
    this.#saveRun(run);
    this.#log(runId, null, "shared", "run", "error", run.error, {});
  }
  #metrics(cases: BenchmarkCaseProjectionV2[]): Record<string, unknown> {
    const completed = cases.filter((item) => item.status === "passed");
    const arm = (name: BenchmarkArm) => {
      const values = completed.map((item) => item[name]);
      return {
        official_ea: values.length
          ? values.filter((item) => item.official_ea === true).length / values.length
          : null,
        contract_accuracy: values.length
          ? values.filter((item) => item.contract_accuracy === true).length / values.length
          : null,
        execution_success: values.length
          ? values.filter((item) => item.execution_status === "passed").length / values.length
          : null,
        total_tokens: values.reduce((sum, item) => sum + item.total_tokens, 0),
        average_generation_ms: values.length
          ? values.reduce((sum, item) => sum + (item.generation_ms ?? 0), 0) / values.length
          : null,
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
