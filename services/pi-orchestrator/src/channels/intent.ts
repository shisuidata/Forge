export type ChannelRouteKind =
  | "query"
  | "knowledge"
  | "conversation"
  | "action"
  | "workflow"
  | "clarification"
  | "forbidden";

export interface ChannelIntentRoute {
  kind: ChannelRouteKind;
  confidence: "high" | "medium" | "low";
  requested_deliverables: string[];
  requires_fresh_data: boolean;
  clarification_question: string | null;
  action?: "memory" | "registry";
  title?: string;
  markdown?: string;
}

const GREETING_PATTERN = /^(?:你好|您好|嗨|哈喽|hello|hi|hey|早|早上好|下午好|晚上好|早安|晚安|在吗|你是谁|帮助|help|菜单|有什么功能)(?:呀|啊|哦| there)?[!！,.，。?？\s]*$/i;
const FORBIDDEN_PATTERN = /(?:执行|运行|帮我)?\s*(?:drop\s+table|truncate\s+table|delete\s+from|update\s+\S+\s+set|insert\s+into)|(?:删除|修改|写入|覆盖)\s*(?:生产|线上)?\s*(?:数据库|数据表)|(?:显示|导出|告诉我).{0,8}(?:密码|密钥|secret|api\s*key)/i;
const MEMORY_ACTION_PATTERN = /(?:请)?(?:记住|记下来|以后都用|保存.*偏好|忘记|删除.*记忆|清除.*记忆)/i;
const REGISTRY_ACTION_PATTERN = /(?:保存|新增|修改|定义|发布).{0,12}(?:指标|口径|字段约定|业务规则|registry)/i;
const WORKFLOW_PATTERN = /(?:分析|归因|原因|诊断|洞察|建议).{0,24}(?:报告|汇报|图表|可视化|方案)|(?:生成|制作).{0,12}(?:分析报告|数据报告|图表)|(?:为什么|为何).{0,24}(?:下降|上升|异常|变化)/i;
const QUERY_PATTERN = /(?:查询|统计|计算|汇总|列出|找出|筛选|排名|排行|趋势|分布|同比|环比|多少|数量|总额|平均|最大|最小|top\s*\d*|销售额|订单量|用户数|转化率|留存率|复购率|gmv)/i;
const KNOWLEDGE_PATTERN = /(?:口径|定义|含义|什么意思|怎么算|计算方式|公式|字段|列名|表结构|schema|哪张表|什么表|数据源|业务规则|约定|registry|知识库|语义|关系|主键|外键|枚举值)/i;
const VAGUE_PATTERN = /^(?:帮我)?(?:看看|查一下|分析一下|这个|那个|数据|情况|怎么样|有问题吗)[!！,.，。?？\s]*$/i;
const CHART_PATTERN = /(?:图表|可视化|趋势图|柱状图|折线图|饼图)/i;
const REPORT_PATTERN = /(?:报告|汇报|总结|结论|建议|方案)/i;

function route(
  kind: ChannelRouteKind,
  options: Partial<Omit<ChannelIntentRoute, "kind">> = {},
): ChannelIntentRoute {
  return {
    kind,
    confidence: options.confidence ?? "high",
    requested_deliverables: options.requested_deliverables ?? [],
    requires_fresh_data: options.requires_fresh_data ?? false,
    clarification_question: options.clarification_question ?? null,
    ...(options.action === undefined ? {} : { action: options.action }),
    ...(options.title === undefined ? {} : { title: options.title }),
    ...(options.markdown === undefined ? {} : { markdown: options.markdown }),
  };
}

export function routeChannelMessage(text: string): ChannelIntentRoute {
  const normalized = text.trim();
  if (GREETING_PATTERN.test(normalized)) {
    return route("conversation", {
      requested_deliverables: ["channel_response"],
      title: "你好，我是 Forge",
      markdown: "我可以帮助你查询、统计和分析业务数据，也可以解释指标口径、表字段和业务规则。需要执行 SQL 时，我会先展示 SQL，并在你确认后才执行。",
    });
  }
  if (FORBIDDEN_PATTERN.test(normalized)) {
    return route("forbidden", {
      requested_deliverables: ["channel_response"],
      title: "这项操作无法执行",
      markdown: "Forge 只执行经过审批的只读查询，不会修改生产数据或披露凭证。你可以改为描述希望查看的数据或业务问题。",
    });
  }
  if (MEMORY_ACTION_PATTERN.test(normalized)) {
    return route("action", {
      action: "memory",
      requested_deliverables: ["memory_draft", "approval"],
    });
  }
  if (REGISTRY_ACTION_PATTERN.test(normalized)) {
    return route("action", {
      action: "registry",
      requested_deliverables: ["registry_draft", "approval"],
    });
  }
  if (WORKFLOW_PATTERN.test(normalized)) {
    return route("workflow", {
      requested_deliverables: [
        "query_result",
        "analysis",
        ...(CHART_PATTERN.test(normalized) ? ["chart"] : []),
        ...(REPORT_PATTERN.test(normalized) ? ["report"] : []),
      ],
      requires_fresh_data: true,
    });
  }
  if (KNOWLEDGE_PATTERN.test(normalized)) {
    return route("knowledge", {
      requested_deliverables: ["context_evidence", "answer"],
    });
  }
  if (QUERY_PATTERN.test(normalized)) {
    return route("query", {
      requested_deliverables: ["query_result", "analysis", "report"],
      requires_fresh_data: true,
    });
  }
  if (VAGUE_PATTERN.test(normalized) || normalized.length < 4) {
    return route("clarification", {
      confidence: "low",
      requested_deliverables: ["clarification"],
      clarification_question: "你希望了解哪个业务指标、时间范围，以及需要查询数据还是解释现有口径？",
    });
  }
  return route("knowledge", {
    confidence: "medium",
    requested_deliverables: ["context_evidence", "answer"],
  });
}
