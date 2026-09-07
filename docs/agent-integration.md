# 外部 Agent 集成边界

Forge 是既有 Agent 的可信数据执行层。已有 Direct SQL 或 Forge JSON 候选时，使用公共、版本化的 **Evaluate → Enforce → Explain** API；不要求先转成自然语言、Forge JSON 或接入 Pi。下方 `prepare-query` 是只生成待审核 SQL 的兼容入口，不代表当前公共 API 的全部能力。

## 当前推荐接入路径

| 步骤 | 公共接口 | 执行与权限边界 |
|---|---|---|
| Evaluate | `POST /api/v1/evaluate` | 校验候选、Policy/Assurance及可选的预期／实际结果对；不执行SQL，`execution_authorized`恒为false。 |
| 创建受控运行 | `POST /api/v1/enforce/query-runs` | 校验Principal、Purpose、Resource Scope与可选Mandate，创建待审核QueryRun；不是执行许可。 |
| 回读审核材料 | `GET /api/v1/enforce/query-runs/{query_run_id}` | 使用创建凭证回读实际SQL、状态及绑定hash，不创建第二份运行状态。 |
| 人工批准 | `POST /api/v1/enforce/query-runs/{query_run_id}/approve` | 提交审核过的SQL、Assurance和Enforcement Context hashes及人工责任主体；服务端重新校验后才执行。 |
| Explain | `GET /api/v1/explain/query-runs/{query_run_id}` | 使用创建凭证读取同一QueryRun的Evidence、lineage、完整性与限制；只读，不新增执行权。 |

候选使用[query-candidate-v1](../agent/contracts/query-candidate-v1.schema.json)，Direct SQL与Forge JSON互斥。Enforce的完整输入、批准和返回形状分别见[请求契约](../agent/contracts/enforce-query-request-v1.schema.json)、[批准契约](../agent/contracts/enforce-query-approval-v1.schema.json)、[运行投影](../agent/contracts/enforce-query-response-v1.schema.json)；Explain见[证据响应](../agent/contracts/explain-query-response-v1.schema.json)。运行服务的`/docs`列出实际HTTP输入及响应。

- 对外部署应启用认证，调用端使用`X-API-Key`。创建／回读凭证与配置在`ENFORCE_REVIEWER_API_KEYS`中的reviewer凭证分开；普通调用凭证不能自批。Reviewer凭证由可信人工审核端持有，不交给模型生成步骤。
- Principal的认证上下文由服务端绑定到当前凭证。客户端填写Principal字段不等于完成企业身份认证；QueryRun回读和Explain继续校验创建凭证绑定。
- 创建和批准均携带稳定的`Idempotency-Key`。遇到结果未知先回读运行，不换新key盲目重放审批／执行；`retryable`也不等于副作用重放授权。
- 只读身份、执行开关、Registry/Policy/Assurance、SQL与审核有效期仍由Forge校验；任一适用条件不满足则失败关闭。Evaluate通过不能跳过Enforce，Evidence verified也不证明业务语义必然正确。
- 外部Agent保留自身任务编排；Forge只持有受控QueryRun。不要使用内部Pi服务凭证，也不要在外部适配器中复制Forge的审批／执行状态机。

## 无模型的本地入口验证

在全新克隆中，按[README](../README.md)安装后运行：

```bash
forge quickstart --workdir .forge/independent-run
```

Quickstart使用隔离合成SQLite，先证明写候选被拒绝，再完成只读候选的Evaluate、SQL人工审核、受控执行和Explain；不需要模型Key、Pi或已有数据库。其本地演示关闭认证，不能把该配置用于共享部署。自动化检查可显式使用`--yes --json`，但不把自动批准当作生产人工审核。

这验证的是现有公共API，不是某个外部Agent框架的适配器或外部采用证明。[#8](https://github.com/shisuidata/Forge/issues/8)的独立适配器仍未完成；未参与实现者的试跑／失败回执按[#9](https://github.com/shisuidata/Forge/issues/9)收集。维护者自己的烟测不关闭R0.6。

## 兼容入口：prepare-query

以下约束只适用于`/api/prepare-query`：它接收自然语言，生成可审核SQL而不创建公共Enforce运行或直接执行数据库。需要从生成结果继续执行时，应作为新候选显式进入上面的公共Enforce审核链，不能直接送旧`/api/approve`。

### prepare-query 设计原则

- 外部 Agent 只能提交自然语言问题和可选上下文。
- Forge 返回 Forge JSON、编译后的 SQL、方言、Registry 版本和需要人工确认的状态。
- 数据库执行默认关闭；执行必须来自 Forge 内部审核流，并有审计记录。
- 外部 Agent 不接收数据库账号、API Key、Cookie 或完整客户敏感结果集。

### prepare-query HTTP 契约

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

`prepare_query` 不会创建可由 `/api/approve` 消费的 pending SQL。即使返回了 SQL，也不能借旧 approve 接口直接执行；公共 Enforce 是独立的显式请求，仍要求完整 Principal、Resource Scope、审核 hash 和部署级权限条件。

审计日志中，成功的 prepare-query 记录使用 `needs_external_review` 状态，而不是内部审核流的 `pending`。只有 Forge Web/飞书内部生成、可由 `/api/approve` 消费的 SQL 才能进入 `pending`。

### prepare-query 适用入口

- MCP / Claude Desktop：作为只生成 SQL 的工具。
- OpenAI Agents / ChatGPT Apps：作为企业内网 Action，默认只返回待审核 SQL。
- Slack / 企业微信 / 钉钉：先走消息入口适配层，再接同一个 prepare-query 内核。

这些入口未产品化前，不对外宣称“已支持”。当前交付状态以 `docs/compatibility-matrix.md` 为准。

## 内部 Pi Control Plane 契约

`/api/prepare-query` 的外部安全语义保持不变。Forge 另为同一私有化部署内的 Pi Orchestrator 提供内部 QueryRun API：

```text
POST /api/internal/query-runs
GET  /api/internal/query-runs/{query_run_id}
POST /api/internal/query-runs/{query_run_id}/approve
POST /api/internal/query-runs/{query_run_id}/cancel
GET  /api/internal/query-runs/{query_run_id}/result
```

这不是通用外部 Agent 执行接口。它使用独立的 `X-Pi-Service-Key`，对应 `PI_SERVICE_API_KEYS`，与普通 `AUTH_API_KEYS` 分离。

批准执行必须同时满足：

- QueryRun 仍处于 `needs_review`。
- 批准人与 QueryRun 用户一致。
- `sql_hash` 与审核 SQL 一致。
- Registry 内容版本没有漂移。
- 审核没有过期。
- `EXECUTION_ENABLED=true`。
- `DATABASE_READONLY_CONFIRMED=true`。
- 创建和批准操作带幂等键。

批准前 Forge 原子地将状态切换为 `executing`，避免重复渠道事件执行两次查询。完整结果只在 `completed` 后通过 QueryRun result 契约返回。
