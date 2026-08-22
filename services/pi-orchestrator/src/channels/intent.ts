export type ChannelMessageRoute =
  | { kind: "query" }
  | { kind: "knowledge" }
  | { kind: "conversation"; title: string; markdown: string };

const QUERY_PATTERN = /(?:统计|查询|查一下|查出|列出|筛选|计算|多少|几笔|排名|排行|趋势|同比|环比|占比|分布|明细|汇总|聚合|分析.{0,8}(?:数据|变化|原因)|gmv|销售额|营收|订单量|订单数|用户数|转化率|复购率|留存率|select\s|count\s*\(|sum\s*\(|average|show\s+me|how\s+many|top\s*\d*)/i;
const KNOWLEDGE_PATTERN = /(?:定义|口径|是什么意思|怎么算|公式|字段|表结构|哪张表|数据字典|业务规则|语义规则|约定|规范|知识库|数据源|指标说明|schema|metric|definition|column|table)/i;
const GREETING_PATTERN = /^(?:你好|您好|嗨|哈喽|hello|hi|hey|早上好|下午好|晚上好|早安|晚安|在吗|你是谁|帮助|help|菜单|有什么功能)[!！,.，。?？\s]*$/i;

export function routeChannelMessage(message: string): ChannelMessageRoute {
  const normalized = message.trim();
  if (KNOWLEDGE_PATTERN.test(normalized)) return { kind: "knowledge" };
  if (QUERY_PATTERN.test(normalized)) return { kind: "query" };
  if (GREETING_PATTERN.test(normalized)) {
    return {
      kind: "conversation",
      title: "你好，我是 Forge",
      markdown:
        "我可以帮你：\n\n" +
        "- 查询、统计和分析业务数据（生成 SQL 后由你审批执行）\n" +
        "- 解释指标口径、表结构、字段约定和语义规则\n" +
        "- 基于组织知识库回答数据业务问题\n\n" +
        "直接告诉我你想了解什么即可。",
    };
  }
  return { kind: "knowledge" };
}
