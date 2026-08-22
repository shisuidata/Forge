import { join } from "node:path";

import {
  createAgentSession,
  ModelRuntime,
  SessionManager,
  SettingsManager,
  type ToolDefinition,
} from "@earendil-works/pi-coding-agent";

import type { Artifact } from "./artifacts.js";
import { computePiModelRevision, type OrchestratorConfig } from "./config.js";
import {
  EVIDENCE_REQUIRED_SKILL_NAMES,
  loadStageSkillResources,
  type AdvisorySkillName,
  type MvpSkillName,
} from "./skills.js";
import {
  createAdvisorySubmissionTool,
  createAnalysisSubmissionTool,
  createClarificationSubmissionTool,
  createMetricDefinitionSubmissionTool,
  createRenderedOutputSubmissionTool,
  type AdvisoryPayload,
  type AnalysisPayload,
  type ClarificationPayload,
  type MetricDefinitionPayload,
  type QueryResultPayload,
  type RenderedOutputPayload,
} from "./structured-artifact-tools.js";
import type { TaskRun } from "./task-store.js";

export class SkillExecutionError extends Error {}

function isAborted(signal: AbortSignal | undefined): boolean {
  return signal?.aborted === true;
}

function renderReportMarkdown(payload: RenderedOutputPayload): string {
  const lines = [
    `# ${payload.title}`,
    "",
    "## 执行摘要",
    "",
    payload.executive_summary,
    "",
    "## 核心发现",
  ];
  payload.key_findings.forEach((finding, index) => {
    lines.push(
      "",
      `### ${index + 1}. ${finding.statement}`,
      "",
      finding.interpretation,
      "",
      `- 证据：${finding.evidence_refs.join(", ")}`,
      `- 置信度：${finding.confidence}`,
    );
  });
  lines.push("", "## 建议动作");
  payload.recommendations.forEach((recommendation) => {
    lines.push(
      "",
      `- **${recommendation.priority}** ${recommendation.action}：${recommendation.rationale}`,
    );
  });
  lines.push("", "## 限制条件");
  payload.limitations.forEach((limitation) => lines.push(`- ${limitation}`));
  lines.push("", "## 下一步");
  payload.next_steps.forEach((step) => lines.push(`- ${step}`));
  return lines.join("\n");
}

interface StageSession {
  prompt(text: string): Promise<void>;
  abort(): Promise<void>;
  dispose(): void;
}

export type StageSessionFactory = (options: {
  skillName: MvpSkillName;
  tool: ToolDefinition;
  expectedModelRevision?: string | null;
}) => Promise<StageSession>;

export interface AnalysisSkillInput {
  question: string;
  queryResults: Artifact<QueryResultPayload>[];
  priorAnalysis?: Artifact<AnalysisPayload>;
}

export interface ReportSkillInput {
  audience: string;
  analysis: Artifact<AnalysisPayload>;
}

export interface AdvisorySkillInput {
  prompt: string;
  queryResults?: Artifact<QueryResultPayload>[];
  contextEvidence?: Array<{
    evidence_ref: string;
    source_type: string;
    title: string;
    content: string;
  }>;
}

export interface StructuredSkillExecutionPort {
  clarify(task: TaskRun, message: string, signal?: AbortSignal, expectedModelRevision?: string | null): Promise<ClarificationPayload>;
  reviewMetric(
    task: TaskRun,
    message: string,
    signal?: AbortSignal,
    expectedModelRevision?: string | null,
  ): Promise<MetricDefinitionPayload>;
  analyze(
    task: TaskRun,
    input: AnalysisSkillInput,
    signal?: AbortSignal,
    expectedModelRevision?: string | null,
  ): Promise<AnalysisPayload>;
  advise?(
    task: TaskRun,
    skillName: AdvisorySkillName,
    input: AdvisorySkillInput,
    signal?: AbortSignal,
    expectedModelRevision?: string | null,
  ): Promise<AdvisoryPayload>;
  writeReport(
    task: TaskRun,
    input: ReportSkillInput,
    signal?: AbortSignal,
    expectedModelRevision?: string | null,
  ): Promise<RenderedOutputPayload>;
}

export class PiStructuredSkillExecutor implements StructuredSkillExecutionPort {
  readonly #config: OrchestratorConfig;
  readonly #sessionFactory: StageSessionFactory;
  #runtimePromise: Promise<ModelRuntime> | undefined;
  #runtimeCatalogSignature: string | undefined;

  constructor(options: {
    config: OrchestratorConfig;
    sessionFactory?: StageSessionFactory;
  }) {
    this.#config = options.config;
    this.#sessionFactory = options.sessionFactory ?? ((input) => this.#createSession(input));
  }

  async clarify(
    task: TaskRun,
    message: string,
    signal?: AbortSignal,
    expectedModelRevision?: string | null,
  ): Promise<ClarificationPayload> {
    const submission = createClarificationSubmissionTool();
    await this.#runStage({
      task,
      message,
      skillName: "data-requirement-clarifier",
      tool: submission.tool,
      isSubmitted: () => submission.getSubmitted() !== undefined,
      ...(signal === undefined ? {} : { signal }),
      ...(expectedModelRevision === undefined ? {} : { expectedModelRevision }),
    });
    const payload = submission.getSubmitted();
    if (payload === undefined) {
      throw new SkillExecutionError(
        "data-requirement-clarifier ended without submitting an Artifact",
      );
    }
    return payload;
  }

  async reviewMetric(
    task: TaskRun,
    message: string,
    signal?: AbortSignal,
    expectedModelRevision?: string | null,
  ): Promise<MetricDefinitionPayload> {
    const submission = createMetricDefinitionSubmissionTool();
    await this.#runStage({
      task,
      message,
      skillName: "metric-definition-reviewer",
      tool: submission.tool,
      isSubmitted: () => submission.getSubmitted() !== undefined,
      ...(signal === undefined ? {} : { signal }),
      ...(expectedModelRevision === undefined ? {} : { expectedModelRevision }),
    });
    const payload = submission.getSubmitted();
    if (payload === undefined) {
      throw new SkillExecutionError(
        "metric-definition-reviewer ended without submitting an Artifact",
      );
    }
    return payload;
  }

  async analyze(
    task: TaskRun,
    input: AnalysisSkillInput,
    signal?: AbortSignal,
    expectedModelRevision?: string | null,
  ): Promise<AnalysisPayload> {
    if (input.queryResults.length === 0) {
      throw new SkillExecutionError("Analysis requires at least one QueryResultArtifact");
    }
    const queryResults = input.queryResults.map((artifact) => ({
      query_run_id: artifact.payload.query_run_id,
      columns: artifact.payload.columns,
      rows: artifact.payload.rows.map((values, index) => ({
        evidence_ref: `${artifact.payload.query_run_id}#row:${index + 1}`,
        values,
      })),
      row_count: artifact.payload.row_count,
      truncated: artifact.payload.truncated,
      registry_version: artifact.payload.registry_version,
    }));
    const allowedReferences = new Set(
      queryResults.flatMap((result) => result.rows.map((row) => row.evidence_ref)),
    );
    const submission = createAnalysisSubmissionTool({
      allowedEvidenceRefs: allowedReferences,
    });
    await this.#runStage({
      task,
      message: JSON.stringify({
        question: input.question,
        query_results: queryResults,
        prior_analysis: input.priorAnalysis ?? null,
        evidence_rule:
          "findings.evidence_refs 只能使用 rows 中给出的 evidence_ref；相关性不得表述为确定因果；不得使用‘可排除、已经排除、直接导致、证明了、确定原因、直接来源、必然导致’等过度确定措辞。缺少根因证据时保留 hypotheses/limitations/suggested_queries。",
      }),
      skillName: "business-root-cause-analysis",
      tool: submission.tool,
      isSubmitted: () => submission.getSubmitted() !== undefined,
      ...(signal === undefined ? {} : { signal }),
      ...(expectedModelRevision === undefined ? {} : { expectedModelRevision }),
    });
    const payload = submission.getSubmitted();
    if (payload === undefined) {
      throw new SkillExecutionError(
        "business-root-cause-analysis ended without submitting an Artifact",
      );
    }
    const references = [
      ...payload.findings.flatMap((finding) => finding.evidence_refs),
      ...payload.hypotheses.flatMap((hypothesis) => hypothesis.evidence_refs),
    ];
    if (references.some((reference) => !allowedReferences.has(reference))) {
      throw new SkillExecutionError("Analysis cited evidence outside the supplied QueryRun");
    }
    return payload;
  }

  async advise(
    task: TaskRun,
    skillName: AdvisorySkillName,
    input: AdvisorySkillInput,
    signal?: AbortSignal,
    expectedModelRevision?: string | null,
  ): Promise<AdvisoryPayload> {
    const queryResults = (input.queryResults ?? []).map((artifact) => ({
      query_run_id: artifact.payload.query_run_id,
      columns: artifact.payload.columns,
      rows: artifact.payload.rows.map((values, index) => ({
        evidence_ref: `${artifact.payload.query_run_id}#row:${index + 1}`,
        values,
      })),
      row_count: artifact.payload.row_count,
      truncated: artifact.payload.truncated,
    }));
    const contextEvidence = input.contextEvidence ?? [];
    const allowedEvidenceRefs = new Set([
      ...queryResults.flatMap((result) => result.rows.map((row) => row.evidence_ref)),
      ...contextEvidence.map((item) => item.evidence_ref),
    ]);
    const submission = createAdvisorySubmissionTool({
      skillName,
      allowedEvidenceRefs,
      requiresQueryEvidence: EVIDENCE_REQUIRED_SKILL_NAMES.includes(
        skillName as (typeof EVIDENCE_REQUIRED_SKILL_NAMES)[number],
      ),
    });
    await this.#runStage({
      task,
      skillName,
      tool: submission.tool,
      isSubmitted: () => submission.getSubmitted() !== undefined,
      message: JSON.stringify({
        request: input.prompt,
        query_results: queryResults,
        context_evidence: contextEvidence,
        evidence_rule: "任何事实 finding 必须引用给定 evidence_ref；不得引用输入以外的来源。没有证据时只可写 assumption、limitation 或 open_question。",
      }),
      ...(signal === undefined ? {} : { signal }),
      ...(expectedModelRevision === undefined ? {} : { expectedModelRevision }),
    });
    const payload = submission.getSubmitted();
    if (payload === undefined) {
      throw new SkillExecutionError(`${skillName} ended without submitting an Artifact`);
    }
    return payload;
  }

  async writeReport(
    task: TaskRun,
    input: ReportSkillInput,
    signal?: AbortSignal,
    expectedModelRevision?: string | null,
  ): Promise<RenderedOutputPayload> {
    const analysisReferences = new Set([
      ...input.analysis.payload.findings.flatMap((finding) => finding.evidence_refs),
      ...input.analysis.payload.hypotheses.flatMap(
        (hypothesis) => hypothesis.evidence_refs,
      ),
    ]);
    const submission = createRenderedOutputSubmissionTool({
      analysisArtifactId: input.analysis.artifact_id,
      allowedFindingStatements: new Set(
        input.analysis.payload.findings.map((finding) => finding.statement),
      ),
      allowedEvidenceRefs: analysisReferences,
    });
    await this.#runStage({
      task,
      message: JSON.stringify({
        audience: input.audience,
        analysis_artifact: input.analysis,
        evidence_refs: [...analysisReferences],
        evidence_rule:
          "不得增加 AnalysisArtifact 中不存在的事实；key_findings.statement 必须逐字复制 AnalysisArtifact.findings.statement，不得把 hypothesis 提升为 finding；key_findings 必须保留 QueryRun evidence_refs；不得使用‘可排除、已经排除、直接导致、证明了、确定原因、直接来源、必然导致’等过度确定措辞；markdown 必须精确填写 SERVER_RENDERED，由服务端确定性渲染。",
      }),
      skillName: "data-analysis-report-writer",
      tool: submission.tool,
      isSubmitted: () => submission.getSubmitted() !== undefined,
      ...(signal === undefined ? {} : { signal }),
      ...(expectedModelRevision === undefined ? {} : { expectedModelRevision }),
    });
    const payload = submission.getSubmitted();
    if (payload === undefined) {
      throw new SkillExecutionError(
        "data-analysis-report-writer ended without submitting an Artifact",
      );
    }
    if (!payload.source_artifact_ids.includes(input.analysis.artifact_id)) {
      throw new SkillExecutionError("Report does not reference the supplied AnalysisArtifact");
    }
    const reportReferences = payload.key_findings.flatMap(
      (finding) => finding.evidence_refs,
    );
    if (reportReferences.some((reference) => !analysisReferences.has(reference))) {
      throw new SkillExecutionError("Report introduced evidence absent from AnalysisArtifact");
    }
    return {
      ...payload,
      markdown: renderReportMarkdown(payload),
    };
  }

  async #runStage(options: {
    task: TaskRun;
    message: string;
    skillName: MvpSkillName;
    tool: ToolDefinition;
    isSubmitted: () => boolean;
    signal?: AbortSignal;
    expectedModelRevision?: string | null;
  }): Promise<void> {
    if (options.message.trim().length === 0) {
      throw new SkillExecutionError("Skill input must not be empty");
    }
    if (options.message.length > 120_000) {
      throw new SkillExecutionError(
        "Skill input exceeds the bounded analysis context; prepare a narrower QueryRun",
      );
    }
    if (isAborted(options.signal)) throw new SkillExecutionError("Skill execution aborted");
    const session = await this.#sessionFactory({
      skillName: options.skillName,
      tool: options.tool,
      ...(options.expectedModelRevision === undefined
        ? {}
        : { expectedModelRevision: options.expectedModelRevision }),
    });
    const abort = () => void session.abort();
    if (isAborted(options.signal)) {
      await session.abort();
      session.dispose();
      throw new SkillExecutionError("Skill execution aborted");
    }
    options.signal?.addEventListener("abort", abort, { once: true });
    try {
      await session.prompt(
        [
          `TaskRun: ${options.task.task_run_id}`,
          `Organization: ${options.task.org_id}`,
          `Team: ${options.task.team_id}`,
          `必须调用唯一的 ${options.tool.name} 工具提交最终 Artifact；禁止只输出自由文本。`,
          "用户输入：",
          options.message,
        ].join("\n"),
      );
      if (!options.isSubmitted() && !isAborted(options.signal)) {
        await session.prompt(
          `上一次没有提交 Artifact。现在必须调用 ${options.tool.name}；不要解释，不要输出自由文本。`,
        );
      }
    } finally {
      options.signal?.removeEventListener("abort", abort);
      session.dispose();
    }
  }

  async #createSession(options: {
    skillName: MvpSkillName;
    tool: ToolDefinition;
    expectedModelRevision?: string | null;
  }): Promise<StageSession> {
    const provider = this.#config.piModelProvider;
    const modelId = this.#config.piModelId;
    if (provider === undefined || modelId === undefined) {
      throw new SkillExecutionError(
        "Pi model execution is not configured; set PI_MODEL_PROVIDER and PI_MODEL_ID",
      );
    }
    const runtime = await this.#getRuntime(options.expectedModelRevision);
    const model = runtime.getModel(provider, modelId);
    if (model === undefined) {
      throw new SkillExecutionError(`Configured Pi model not found: ${provider}/${modelId}`);
    }
    const resources = await loadStageSkillResources({
      cwd: this.#config.skillsRoot,
      agentDir: this.#config.agentDir,
      skillsRoot: this.#config.skillsRoot,
      skillName: options.skillName,
    });
    const { session } = await createAgentSession({
      cwd: this.#config.skillsRoot,
      agentDir: this.#config.agentDir,
      modelRuntime: runtime,
      model,
      resourceLoader: resources.loader,
      settingsManager: SettingsManager.inMemory({
        enableSkillCommands: false,
        compaction: { enabled: false },
      }),
      sessionManager: SessionManager.inMemory(this.#config.skillsRoot),
      noTools: "builtin",
      tools: [options.tool.name],
      customTools: [options.tool],
    });
    return session;
  }

  async #getRuntime(expectedRevision?: string | null): Promise<ModelRuntime> {
    const currentRevision = computePiModelRevision({
      agentDir: this.#config.agentDir,
      provider: this.#config.piModelProvider,
      modelId: this.#config.piModelId,
    });
    if (expectedRevision !== undefined && currentRevision !== expectedRevision) {
      throw new SkillExecutionError("Pi model revision changed before Stage session creation");
    }
    const signature = currentRevision ?? "unconfigured";
    if (this.#runtimePromise === undefined || signature !== this.#runtimeCatalogSignature) {
      this.#runtimeCatalogSignature = signature;
      this.#runtimePromise = ModelRuntime.create({
        authPath: join(this.#config.agentDir, "auth.json"),
        modelsPath: join(this.#config.agentDir, "models.json"),
        refreshOnCreate: false,
        allowModelNetwork: false,
      });
    }
    const runtime = await this.#runtimePromise;
    const revisionAfterLoad = computePiModelRevision({
      agentDir: this.#config.agentDir,
      provider: this.#config.piModelProvider,
      modelId: this.#config.piModelId,
    });
    if (revisionAfterLoad !== currentRevision) {
      this.#runtimePromise = undefined;
      this.#runtimeCatalogSignature = undefined;
      throw new SkillExecutionError("Pi model revision changed while loading Stage runtime");
    }
    return runtime;
  }
}
