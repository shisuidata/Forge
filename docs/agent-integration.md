# 外部 Agent 集成边界

Forge 可以作为企业数据查询工具暴露给外部 Agent，但第一版只开放 **prepare-query** 能力：生成可审核 SQL，不直接执行数据库。

## 设计原则

- 外部 Agent 只能提交自然语言问题和可选上下文。
- Forge 返回 Forge JSON、编译后的 SQL、方言、Registry 版本和需要人工确认的状态。
- 数据库执行默认关闭；执行必须来自 Forge 内部审核流，并有审计记录。
- 外部 Agent 不接收数据库账号、API Key、Cookie 或完整客户敏感结果集。

## prepare_query 契约

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
  "forge_json": {},
  "sql": "SELECT ...",
  "dialect": "postgresql",
  "can_execute": false,
  "review_required": true
}
```

`can_execute` 在外部 Agent 场景默认恒为 `false`。如果未来需要执行，必须增加人工批准记录、用户身份、数据源权限、审计关联 ID 和部署级开关。

## 适用入口

- MCP / Claude Desktop：作为只生成 SQL 的工具。
- OpenAI Agents / ChatGPT Apps：作为企业内网 Action，默认只返回待审核 SQL。
- Slack / 企业微信 / 钉钉：先走消息入口适配层，再接同一个 prepare-query 内核。

这些入口未产品化前，不对外宣称“已支持”。当前交付状态以 `docs/compatibility-matrix.md` 为准。
