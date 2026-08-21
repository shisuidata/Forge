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
  if (["failed", "cancelled", "expired"].includes(input.task.status)) {
    return {
      ...common,
      kind: "error",
      title: input.task.status === "cancelled" ? "任务已取消" : "任务未完成",
      markdown:
        typeof failure?.payload.error === "string"
          ? failure.payload.error
          : `TaskRun 状态：${input.task.status}`,
      fields: [{ label: "TaskRun", value: input.task.task_run_id }],
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
    return {
      ...common,
      kind: "query_review",
      title: "Forge SQL 审核",
      markdown: sql.length > 0 ? `\`\`\`sql\n${sql}\n\`\`\`` : "待审核 SQL 不可用。",
      fields: [
        { label: "QueryRun", value: queryRunId },
        { label: "SQL Hash", value: sqlHash },
      ],
      table: null,
      actions:
        queryRunId.length > 0 && sqlHash.length > 0
          ? [
              action(
                input.task.task_run_id,
                "approve_query",
                "确认执行",
                { query_run_id: queryRunId, sql_hash: sqlHash },
                "primary",
              ),
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
      markdown: prompt,
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
      title: typeof rendered.payload.title === "string" ? rendered.payload.title : "分析报告",
      markdown:
        typeof rendered.payload.markdown === "string"
          ? rendered.payload.markdown
          : "报告已完成。",
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
        typeof finding === "object" && finding !== null && typeof finding.statement === "string"
          ? `- ${finding.statement}`
          : undefined,
      )
      .filter((item): item is string => item !== undefined)
      .join("\n");
    const suggestedQueries = Array.isArray(analysis.payload.suggested_queries)
      ? analysis.payload.suggested_queries
      : [];
    const actions: ChannelAction[] = [];
    if (input.task.status === "ready_for_report") {
      actions.push(action(input.task.task_run_id, "render_report", "生成业务报告", {}, "primary"));
    } else if (suggestedQueries.length > 0) {
      // 补查需要创建并切换到 child TaskRun，待渠道 lineage 契约完成后再开放按钮。
    }
    return {
      ...common,
      kind: "analysis",
      title: input.task.status === "incomplete" ? "分析需要补查" : "分析完成",
      markdown:
        findingText ||
        (typeof analysis.payload.summary === "string" ? analysis.payload.summary : "分析已完成。"),
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
    markdown: `当前阶段：${input.task.current_stage ?? input.task.status}`,
    fields: [{ label: "状态", value: input.task.status }],
    table: null,
    actions: [],
  };
}
