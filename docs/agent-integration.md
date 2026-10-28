# 外部 Agent 集成边界

Forge 是既有 Agent 的可信数据执行层。已有 Direct SQL 或 Forge JSON 候选时，使用 **Evaluate → Enforce → Explain**：Evaluate 是 `/api/v1/evaluate`（不执行），Enforce 与 Explain 是受治理的 v2 API。不要求先转成自然语言、Forge JSON 或接入 Pi。v1 Enforce 写入口（`POST /api/v1/enforce/query-runs` 及 `/approve`）已关闭，返回 `410 governed_api_v2_required`。

## 角色与凭证

v2 中每个调用方都是已登记的主体（principal），持有自己在数据库中签发、可撤销的凭证，请求头统一使用 `X-API-Key`。

| 角色 | 主体类型 | 能做什么 |
|---|---|---|
| 管理员 | human，`role=admin` | 签发、查看、撤销 Mandate；读取本工作区全部 QueryRun 与 Explain |
| Agent | agent | 在有效 Mandate 内 prepare；仅对自主表执行；读取自己创建的 QueryRun |
| 审核人 | human，`role=reviewer` 或 `admin`，且被 Mandate 指定 | 批准需人审的 QueryRun；读取其 Explain |

Mandate 把一个 Agent 绑定到一个任务（`task_run_id` 须以 `tr_` 开头）、一个数据源、可查询表集合 `allowed_tables`、其中可免审执行的 `autonomous_tables`、责任人、指定审核人和到期时间（必须在未来 24 小时之内）。管理员撤销后，新的 prepare 返回 `mandate_inactive`。

主体通过 `forge governance bootstrap --input <manifest>` 登记，凭证通过 `forge governance credential create --principal <id> --kind api_key --out <file>` 签发（文件权限 0600，终端不打印密钥）。旧的 `AUTH_API_KEYS`、`ENFORCE_REVIEWER_API_KEYS` 共享密钥不授予 v2 访问权。

## 当前推荐接入路径

| 步骤 | 接口 | 调用方 | 执行与权限边界 |
|---|---|---|---|
| Evaluate | `POST /api/v1/evaluate` | 任意已认证主体 | 校验候选、Policy/Assurance 及可选结果对；不执行 SQL，`execution_authorized` 恒为 false。 |
| 签发授权 | `POST /api/v2/governance/mandates`（CLI：`forge governance mandate issue`） | 管理员 | 契约见 [mandate-issue-v1](../agent/contracts/mandate-issue-v1.schema.json)；撤销需带当前 `state_revision`。 |
| 创建受控运行 | `POST /api/v2/enforce/query-runs` | Agent | 契约见 [enforce-query-request-v2](../agent/contracts/enforce-query-request-v2.schema.json)；创建不执行。仅涉及自主表返回 `ready`，否则 `review_required`；越界、写 SQL、Mandate 失效或 Registry/Policy 不符均失败关闭。 |
| 回读 | `GET /api/v2/enforce/query-runs/{id}` | 创建者、责任人、审核人、管理员 | 回读实际 SQL、状态和三项审核 hash，不创建第二份运行状态。 |
| 自主执行 | `POST /api/v2/enforce/query-runs/{id}/execute` | 创建该运行的 Agent | 仅 `ready` 可用；提交 [enforce-query-action-v2](../agent/contracts/enforce-query-action-v2.schema.json) 的三项 hash。 |
| 人工批准 | `POST /api/v2/enforce/query-runs/{id}/approve` | 指定审核人 | 仅 `review_required` 可用；同一 action 契约。Agent 调用返回 `reviewer_not_authorized`。 |
| 取消 | `POST /api/v2/enforce/query-runs/{id}/cancel` | 创建者、责任人、指定审核人、管理员 | 只能取消仍为 `ready` / `review_required` 的运行。 |
| Explain | `GET /api/v2/explain/query-runs/{id}` | 同回读（Agent 须 Mandate 仍有效） | 契约见 [explain-query-response-v2](../agent/contracts/explain-query-response-v2.schema.json)；只读，不新增执行权；无权读取一律返回 404。 |

运行投影见 [enforce-query-response-v2](../agent/contracts/enforce-query-response-v2.schema.json)。运行服务的 `/docs` 列出实际 HTTP 输入及响应。

## 本地沙箱演练

```bash
forge quickstart --agent --yes --serve --workdir .forge/sandbox
```

沙箱用隔离合成 SQLite 登记 `quickstart-admin`、`quickstart-reviewer`、`quickstart-agent` 三个主体，并把它们的凭证留在工作目录（0600）。服务保持运行后，可用这三份凭证依次完成签发 Mandate → Agent prepare → 自主执行 / 审核人批准 → Explain → 撤销。逐条命令与请求样例见 [README 的 “Connect your own agent (governed API v2)”](../README.md#connect-your-own-agent-governed-api-v2)；这些命令已在本地沙箱实测。

不使用沙箱时，执行还需要 `AUTH_ENABLED=true`、`EXECUTION_ENABLED=true`，以及已确认只读的数据源（`DATABASE_READONLY_CONFIRMED=true`）。数据库只读角色是最终边界；Forge 的只读校验对副作用函数采用拒绝清单，不能替代数据库权限。

## MCP 接入（`forge mcp`）

`forge mcp` 让 Claude Code、Cursor 等 MCP 客户端以 Agent 主体使用上面的 v2 链路。它在用户本机以 stdio 运行，是 v2 HTTP API 的无状态客户端：不保存 QueryRun 状态、不做授权判断，维护者不托管任何服务。依赖为可选 extra（在 Forge 检出目录 `pip install -e ".[mcp]"`；PyPI 上同名的 `forge` 是无关项目）。

| 工具 | 对应接口 | 说明 |
|---|---|---|
| `forge_scope` | `GET /api/v2/governance/mandates/{id}` | 只读：任务、目的、数据源、可查询表、自主表、审核人、到期时间 |
| `forge_prepare_query` | `POST /api/v2/enforce/query-runs` | 以 Mandate 的 task/purpose 创建 QueryRun，不执行；`denied` 时返回失败的 gate 与诊断，供模型修正 |
| `forge_execute_query` | `POST .../{id}/execute` | 仅 `ready`；每个 QueryRun 固定幂等键，重试只回放原 claim |
| `forge_get_query_run` | `GET .../{id}` | 只读：状态、SQL、有界结果 |
| `forge_explain_query` | `GET /api/v2/explain/query-runs/{id}` | 只读：完整性、实际 SQL、授权来源、Evidence 类型与限制 |
| `forge_cancel_query` | `POST .../{id}/cancel` | 取消尚未 claim 的运行 |

没有审批工具。`review_required` 的结果只告诉模型请用户在自己的终端运行 `forge review <query_run_id>`：它展示实际 SQL、请求者、目的与范围，交互确认后提交一次批准；没有交互式终端时拒绝运行，因此 Agent 的非交互 shell 不能代为批准。创建响应丢失时按幂等键找回同一运行，不重复创建。

- 正式接入：环境变量 `FORGE_BASE_URL`、`FORGE_API_KEY`（Agent 凭证）、`FORGE_MANDATE_ID`。客户端配置文件会保存这些值，应按密钥管理；凭证不出现在工具参数、工具结果或日志中。
- 演示：`forge mcp --demo` 在 `~/.forge/mcp-demo`（每次启动重建，仅删除带演示标记的目录）拉起合成 Forge 服务并签发 12 小时 Mandate：`numbers` 自主、`secrets` 需审核；人审用 `forge review --demo <id>`。合成审核人密钥与 Agent 在同一台机器上，具备文件访问能力的 Agent 仍可能读取，仅因数据为合成才可接受。MCP 进程退出（关闭 stdin 或 SIGTERM）时演示服务随之停止。
- 结果会进入模型上下文；敏感表不应设为自主表或不纳入 Mandate。

## 调用约束

- 创建与 execute/approve 都必须携带稳定的 `Idempotency-Key`。同一 key 重放 action 返回原 claim，不会执行第二次。
- 创建响应丢失时，用 `GET /api/v2/enforce/query-runs?create_idempotency_key=<key>` 找回（仅创建者可见），不要换新 key 盲目重建。结果未知时先回读，`retryable` 也不等于副作用重放授权。
- execute/approve 提交的是 prepare 响应中的 `sql_hash`、`assurance_report_hash`、`enforcement_context_hash`；授权人承诺的正是展示过的 SQL 与上下文，任一漂移失败关闭。
- 审核等待有有效期（`QUERY_RUN_REVIEW_TTL_SECONDS`），过期的运行变为 `expired`，需重新 prepare。
- 执行结果会回到调用方，进入 Agent 的上下文。敏感表应不设为自主表，或不纳入 Mandate。
- 审核人凭证由可信的人工审核端持有，不交给模型生成步骤。外部 Agent 保留自身任务编排，Forge 只持有受控 QueryRun；不要使用 Pi 服务凭证，也不要在适配器中复制 Forge 的审批/执行状态机。
- Evaluate 通过不能跳过 Enforce；Explain 的 `integrity=verified` 证明证据未被篡改，不证明业务语义必然正确。

外部开发者的试跑与失败回执按 [#9](https://github.com/shisuidata/Forge/issues/9) 收集；[#8](https://github.com/shisuidata/Forge/issues/8) 的框架中立接入现以 `forge mcp` 提供，尚无外部使用者验证。维护者自己的烟测不关闭 R0.6。

## 兼容入口：prepare-query

`POST /api/prepare-query` 接收自然语言，用服务端配置的模型生成可审核 SQL；它不创建 QueryRun，也不执行数据库。调用方身份取自 `X-API-Key` 对应的主体，表范围取自该主体所在团队。

最小输入：

```json
{
  "question": "本月各渠道支付 GMV 是多少？",
  "dialect": "postgresql"
}
```

返回 `status`（`needs_review` / `needs_clarification` / `error`）、`forge_json`、`sql`、`dialect`、`review_required: true` 与 `can_execute: false`。`dialect` 只允许 `auto / sqlite / postgresql / mysql / bigquery / snowflake`，省略时沿用 Forge 配置。成功记录在审计中标为 `needs_external_review`。

需要执行时，把生成结果作为新候选显式提交到上面的 v2 Enforce 链。旧的 `/api/approve`、`/api/chat`、`/api/cancel` 已关闭（410），不存在通过 prepare-query 直接执行的路径。该入口依赖模型配置，不属于无模型的公开 Golden Path。

## 内部 Pi Control Plane

Pi Orchestrator 不使用单独的内部执行 API，而以 `pi_service` 凭证（`forge governance credential create --kind pi_service`）调用同一组 `/api/v2/enforce/query-runs` 接口，并以 `create_idempotency_key` 找回中断的 prepare。渠道（飞书/钉钉）中的真人审批经 Forge 签发的控制令牌（`X-Forge-Control-Token`）提交；令牌绑定请求方法、路径、三项 hash 与幂等键并会过期，仍由 Forge 校验审核人身份与渠道绑定。

旧的 `/api/internal/query-runs` 系列接口已关闭（410）；`/api/internal/query-candidates` 只为 Pi 生成候选，不创建 QueryRun、不执行。这些内部接口只服务同一私有化部署内的 Pi，不是外部 Agent 的接入面。
