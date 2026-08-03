# 外部 Agent 集成边界

Forge 可以作为企业数据查询工具暴露给外部 Agent，但第一版只开放 **prepare-query** 能力：生成可审核 SQL，不直接执行数据库。这个接口让 Forge 成为其他 Agent 的可信问数组件，而不是让外部 Agent 直接接触数据库。

## 设计原则

- 外部 Agent 只能提交自然语言问题和可选上下文。
- Forge 返回 Forge JSON、编译后的 SQL、方言、Registry 版本和需要人工确认的状态。
- 数据库执行默认关闭；执行必须来自 Forge 内部审核流，并有审计记录。
- 外部 Agent 不接收数据库账号、API Key、Cookie 或完整客户敏感结果集。

## prepare_query HTTP 契约

Endpoint：

```text
POST /api/prepare-query
```

使用现有 API 认证机制。认证开启后需要 `X-API-Key`、`?api_key=` 或有效 Web session。

最小输入：

```json
{
  "question": "本月各渠道支付 GMV 是多少？",
  "user_id": "external-agent",
  "dialect": "postgresql"
}
```

最小输出：

```json
{
  "status": "needs_review",
  "question": "本月各渠道支付 GMV 是多少？",
  "user_id": "external-agent",
  "forge_json": {},
  "sql": "SELECT ...",
  "dialect": "postgresql",
  "review_required": true,
  "can_execute": false,
  "retry_count": 0,
  "text": "",
  "error": ""
}
```

状态说明：

| status | 含义 |
|---|---|
| `needs_review` | 已生成 Forge JSON 和 SQL，必须由宿主系统或 Forge App 审核 |
| `needs_clarification` | 缺少必要口径、时间范围或权限上下文 |
| `error` | LLM、lint、compile、Registry 或 dialect 校验失败 |

`review_required` 在 v1 恒为 `true`，`can_execute` 在 v1 恒为 `false`。`dialect` 只允许 `auto / sqlite / postgresql / mysql / bigquery / snowflake`，省略时沿用 Forge 配置。

`prepare_query` 不会创建可由 `/api/approve` 消费的 pending SQL。即使返回了 SQL，外部 Agent 也只能拿它进入自己的审核流，不能借 Forge 的 approve 接口直接执行。若未来需要执行，必须增加人工批准记录、用户身份、数据源权限、审计关联 ID 和部署级开关。

## 适用入口

- MCP / Claude Desktop：作为只生成 SQL 的工具。
- OpenAI Agents / ChatGPT Apps：作为企业内网 Action，默认只返回待审核 SQL。
- Slack / 企业微信 / 钉钉：先走消息入口适配层，再接同一个 prepare-query 内核。

这些入口未产品化前，不对外宣称“已支持”。当前交付状态以 `docs/compatibility-matrix.md` 为准。
