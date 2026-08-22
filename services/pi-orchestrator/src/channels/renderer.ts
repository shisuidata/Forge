import type { Artifact, ArtifactType } from "../artifacts.js";
import type { TaskEvent, TaskEventType } from "../task-events.js";
import type { ChannelAction, ChannelPresentation, ChannelRenderInput } from "./contracts.js";

function latestEvent(events: TaskEvent[], type: TaskEventType): TaskEvent | undefined {
  return [...events].reverse().find((event) => event.event_type === type);
}

function latestArtifact(artifacts: Artifact[], type: ArtifactType): Artifact | undefined {
  return [...artifacts].reverse().find((artifact) => artifact.artifact_type === type);
}

function strings(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
}

const INTERNAL_LINE_PATTERN =
  /(?:\b(?:TaskRun|QueryRun|StageAttempt|current_stage|assurance_report_hash|sql_hash|model_revision|registry_revision|runtime-context|lease_expires|idempotency_key)\b|\b(?:tr|qr|sa|ar|ctx)_[A-Za-z0-9]{8,}\b|sha256:[a-f0-9]{16,}|https?:\/\/(?:127\.0\.0\.1|localhost)(?::\d+)?|\/(?:home|Users|tmp)\/|Traceback \(most recent call last\)|\b(?:System Prompt|Tool Call)\b)/i;
const REASONING_LINE_PATTERN =
  /^\s*(?:thought|reasoning|chain[- ]of[- ]thought|analysis|思考(?:过程)?|内部分析|推理过程|工具调用)\s*[:：]/i;

function safeBusinessText(value: unknown, fallback: string): string {
  if (typeof value !== "string") return fallback;
  const withoutThinkBlocks = value.replace(/<think>[\s\S]*?(?:<\/think>|$)/gi, "");
  const visible = withoutThinkBlocks
    .split("\n")
    .filter((line) => !REASONING_LINE_PATTERN.test(line) && !INTERNAL_LINE_PATTERN.test(line))
    .join("\n")
    .trim()
    .slice(0, 8_000);
  return visible.length > 0 ? visible : fallback;
}

function safeOptionalBusinessText(value: unknown): string | undefined {
  const visible = safeBusinessText(value, "");
  return visible.length > 0 ? visible : undefined;
}

function action(
  taskRunId: string,
  type: ChannelAction["type"],
  label: string,
  payload: Record<string, unknown> = {},
  style: ChannelAction["style"] = "default",
): ChannelAction {
  return { type, label, task_run_id: taskRunId, payload, style };
}

function base(input: ChannelRenderInput): Pick<
  ChannelPresentation,
  "task_run_id" | "source_event_sequence" | "source_artifact_ids"
> {
  return {
    task_run_id: input.task.task_run_id,
    source_event_sequence: input.events.at(-1)?.sequence ?? 0,
    source_artifact_ids: input.artifacts.map((artifact) => artifact.artifact_id),
  };
}

export function renderChannelPresentation(input: ChannelRenderInput): ChannelPresentation {
  const common = base(input);
  const failure =
    latestEvent(input.events, "skill.execution_failed") ??
    latestEvent(input.events, "query.execution_failed") ??
    latestEvent(input.events, "query.prepare_failed");
  const prepareTimeout = latestEvent(input.events, "query.prepare_timed_out");
  if (input.task.status === "ready_for_query" && prepareTimeout !== undefined) {
    return {
      ...common,
      kind: "error",
      title: "查询准备超时，可重试",
      markdown: "查询准备时间超过预期，请稍后重新提交。",
      fields: [],
      table: null,
      actions: [],
    };
  }

  if (["failed", "cancelled", "expired"].includes(input.task.status)) {
    return {
      ...common,
      kind: "error",
      title: input.task.status === "cancelled" ? "任务已取消" : "任务未完成",
      markdown: input.task.status === "cancelled"
        ? "任务已按你的要求取消。"
        : "本次处理未能完成，请稍后重试或重新发起。",
      fields: [],
      table: null,
      actions: [],
    };
  }

  const channelResponse = latestEvent(input.events, "channel.response_created");
  if (input.task.status === "completed" && channelResponse !== undefined) {
    return {
      ...common,
      kind: "report",
      title: safeBusinessText(channelResponse.payload.title, "Forge"),
      markdown: safeBusinessText(channelResponse.payload.markdown, "已完成。"),
      fields: [],
      table: null,
      actions: [],
    };
  }

  if (input.task.status === "waiting_for_query_approval") {
    const review = latestEvent(input.events, "query.review_requested");
    const sql = typeof review?.payload.sql === "string" ? review.payload.sql : "";
    const queryRunId = typeof review?.payload.query_run_id === "string"
      ? review.payload.query_run_id
      : "";
    const sqlHash = typeof review?.payload.sql_hash === "string" ? review.payload.sql_hash : "";
    const assuranceReportHash = typeof review?.payload.assurance_report_hash === "string"
      ? review.payload.assurance_report_hash
      : "";
    return {
      ...common,
      kind: "query_review",
      title: "Forge SQL 审核",
      markdown: sql.length > 0
        ? `请审核以下 SQL。确认后将以只读方式执行。\n\n\`\`\`sql\n${sql}\n\`\`\``
        : "待审核 SQL 不可用。",
      fields: [],
      table: null,
      actions:
        queryRunId.length > 0 && sqlHash.length > 0 && assuranceReportHash.length > 0
          ? [
              action(
                input.task.task_run_id,
                "approve_query",
                "确认执行",
                {
                  query_run_id: queryRunId,
                  sql_hash: sqlHash,
                  assurance_report_hash: assuranceReportHash,
                },
                "primary",
              ),
              action(input.task.task_run_id, "cancel_task", "取消任务", {}, "danger"),
            ]
          : [],
    };
  }

  if (input.task.status === "needs_input") {
    const clarification = latestArtifact(input.artifacts, "clarification");
    const questions = strings(clarification?.payload.open_questions);
    const queryPrompt = latestEvent(input.events, "query.clarification_requested")?.payload.prompt;
    const prompt = questions.length > 0
      ? questions.map((question, index) => `${index + 1}. ${question}`).join("\n")
      : typeof queryPrompt === "string"
        ? queryPrompt
        : "请补充任务所需信息。";
    return {
      ...common,
      kind: "needs_input",
      title: "需要补充信息",
      markdown: safeBusinessText(prompt, "请补充任务所需信息。"),
      fields: [],
      table: null,
      actions: [
        action(
          input.task.task_run_id,
          "provide_input",
          "提交补充信息",
          { requires_text: true },
          "primary",
        ),
        action(input.task.task_run_id, "cancel_task", "取消任务", {}, "danger"),
      ],
    };
  }

  const advisory = latestArtifact(input.artifacts, "advisory");
  if (input.task.status === "completed" && advisory !== undefined) {
    const findings = Array.isArray(advisory.payload.findings)
      ? advisory.payload.findings
          .map((finding) => {
            if (typeof finding !== "object" || finding === null || typeof finding.statement !== "string") {
              return undefined;
            }
            const statement = safeOptionalBusinessText(finding.statement);
            return statement === undefined ? undefined : `- ${statement}`;
          })
          .filter((item): item is string => item !== undefined)
      : [];
    const summary = safeBusinessText(advisory.payload.summary, "知识回答已完成。");
    return {
      ...common,
      kind: "report",
      title: safeBusinessText(advisory.payload.title, "Forge 回答"),
      markdown: [summary, findings.length > 0 ? `\n${findings.join("\n")}` : ""].join(""),
      fields: [],
      table: null,
      actions: [],
    };
  }

  const rendered = latestArtifact(input.artifacts, "rendered_output");
  if (input.task.status === "completed" && rendered !== undefined) {
    return {
      ...common,
      kind: "report",
      title: safeBusinessText(rendered.payload.title, "分析报告"),
      markdown: safeBusinessText(rendered.payload.markdown, "报告已完成。"),
      fields: [],
      table: null,
      actions: [],
    };
  }

  const analysis = latestArtifact(input.artifacts, "analysis");
  if ((input.task.status === "incomplete" || input.task.status === "ready_for_report") && analysis !== undefined) {
    const findings = Array.isArray(analysis.payload.findings) ? analysis.payload.findings : [];
    const findingText = findings
      .map((finding) =>
        typeof finding === "object" && finding !== null
          ? safeOptionalBusinessText(finding.statement)
          : undefined,
      )
      .filter((item): item is string => item !== undefined)
      .map((item) => `- ${item}`)
      .join("\n");
    const suggestedQueries = Array.isArray(analysis.payload.suggested_queries)
      ? analysis.payload.suggested_queries
      : [];
    const actions: ChannelAction[] = [];
    if (input.task.status === "ready_for_report") {
      actions.push(action(input.task.task_run_id, "render_report", "生成业务报告", {}, "primary"));
    } else if (suggestedQueries.length > 0) {
      suggestedQueries.slice(0, 3).forEach((suggestion, index) => {
        const label = typeof suggestion === "object" && suggestion !== null &&
            typeof suggestion.question === "string"
          ? `补查：${suggestion.question.slice(0, 24)}`
          : `执行补查 ${index + 1}`;
        actions.push(action(
          input.task.task_run_id,
          "request_supplement",
          label,
          { suggested_query_index: index },
          index === 0 ? "primary" : "default",
        ));
      });
      actions.push(action(input.task.task_run_id, "cancel_task", "取消任务", {}, "danger"));
    }
    return {
      ...common,
      kind: "analysis",
      title: input.task.status === "incomplete" ? "分析需要补查" : "分析完成",
      markdown:
        findingText ||
        safeBusinessText(analysis.payload.summary, "分析已完成。"),
      fields: [],
      table: null,
      actions,
    };
  }

  const queryResult = latestArtifact(input.artifacts, "query_result");
  if (queryResult !== undefined && ["ready_for_analysis", "completed"].includes(input.task.status)) {
    const columns = strings(queryResult.payload.columns);
    const rows = Array.isArray(queryResult.payload.rows) ? queryResult.payload.rows : [];
    return {
      ...common,
      kind: "query_result",
      title: "查询结果",
      markdown: `共 ${String(queryResult.payload.row_count ?? rows.length)} 行。`,
      fields: [],
      table: {
        columns,
        rows: rows.slice(0, 20).filter((row): row is unknown[] => Array.isArray(row)),
        truncated: queryResult.payload.truncated === true || rows.length > 20,
      },
      actions:
        input.task.status === "ready_for_analysis"
          ? [action(input.task.task_run_id, "analyze", "开始分析", {}, "primary")]
          : [],
    };
  }

  return {
    ...common,
    kind: "progress",
    title: "Forge 正在处理",
    markdown: input.task.status === "querying"
      ? "正在安全执行已审批的只读查询。"
      : input.task.status === "analyzing"
        ? "正在基于查询结果进行分析。"
        : input.task.status === "rendering"
          ? "正在整理分析结论并生成报告。"
          : "正在处理你的请求，完成后会自动更新。",
    fields: [],
    table: null,
    actions: [],
  };
}
