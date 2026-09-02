# Product Projection v1 Contracts

> Status: SP1 Pi Conversation/Task Projection Ready · Schema version: 1 · Workspace/Report BFF Coverage: 0%
>
> 本文定义 Product Spine 的只读产品边界。TypeBox 真相定义位于 `services/pi-orchestrator/src/product-projections.ts`，生成的跨语言 JSON Schema 位于 `agent/contracts/product-projection-v1.schema.json`，共享正反 fixtures 位于 `agent/contracts/product-projection-fixtures.v1.json`。

## 1. 目的

Product Projection 把 Pi/Forge 的内部 Task、Event、Artifact、Attempt、QueryRun、Report 和 Registry 状态转换为前端可稳定消费、可去敏、可降级的读模型。

它只做 Projection：

- 不创建 Conversation、Task、Approval 或 Report；
- 不推进状态；
- 不产生权限；
- 不替代 Pi/Forge/ReportStore 真相源；
- 不证明结果在开放世界中必然正确。

SP1 已实现 Pi Conversation/Task Builder 与内部只读 API；Workspace/Report BFF 仍未实现，因此本文不能被解读为完整 Product Spine 或 Web Product 已可用。

## 2. 七个公开 Contract

| Contract | 责任 | 主要来源 |
|---|---|---|
| `ActionCapabilityV1` | 某个 Task 当前允许或禁止的 typed action 及原因 | Task status、Event、Artifact、Policy availability |
| `ConversationSummaryV1` | Conversation 列表中的稳定摘要 | TaskRun 按 `channel_conversation_id` 分组 |
| `ConversationDetailV1` | 同一 Conversation 下有序 Task/message/presentation | TaskRun、TaskEvent、Artifact、ChannelPresentation |
| `TaskSummaryV1` | scope-aware 任务列表条目；不暴露任意 metadata | Pi TaskRun |
| `TaskDetailProjectionV1` | Plan、Review、Activity、Attempt、Artifact、Evidence 和关系 | Pi Task/Event/Attempt/Artifact + Forge Query review |
| `WorkspaceProjectionV1` | 待补充、待决定、进行中、失败、最近报告和依赖状态 | Pi Task Projection + Report/Registry availability |
| `ReportSummaryV1` | scope-aware 报告库条目与导出状态 | Forge ReportStore |

所有顶层对象固定：

```text
schema_version = 1
projection_type = <明确类型>
additionalProperties = false
```

未知字段、未知状态、超出 bounds 或不合法 ID 必须失败关闭。

## 3. Scope 与真相源

Conversation v1 不增加可写 `ConversationStore`。它按以下键从 TaskRun 重建：

```text
org_id
+ team_id
+ user_id
+ channel
+ channel_conversation_id
```

`ConversationDetailV1.scope` 必须与 `summary.scope` 完全一致。Task、Workspace 和 Report 也携带 scope；SP1/SP2 必须先做服务端 scope 过滤，再构建 Projection，不能依靠前端隐藏。

`projection_meta.source_revisions` 明确本次读模型使用的来源及 revision，允许来源包括：

- `pi_task_store`；
- `pi_event_store`；
- `pi_artifact_store`；
- `pi_attempt_store`；
- `forge_query_store`；
- `forge_report_store`；
- `forge_registry_store`。

同一个 `source + revision` 不得重复。

## 4. Availability 与降级

顶层 `projection_meta.availability` 只有：

| 状态 | 语义 |
|---|---|
| `ready` | 所有声明来源可用，`unavailable_reasons` 必须为空 |
| `partial` | 部分来源或字段不可用，必须给出 reason code |
| `offline` | 主依赖不可用，必须给出 reason code |

`partial/offline` 不允许以空原因返回；`ready` 不允许同时声明 unavailable reason。

Web 的 `loading` 是客户端读取状态，不属于已返回的 Projection 数据。

## 5. 产品状态映射

Product Projection 使用稳定的 display state：

```text
needs_input
waiting_decision
running
partial
ready
failed
forbidden
offline
completed
cancelled
```

TaskRun 到 display state 的 v1 映射固定为：

| TaskRun status | display state |
|---|---|
| `needs_input` | `needs_input` |
| `waiting_for_query_approval` / `waiting_for_action_approval` | `waiting_decision` |
| `created` / `clarifying` / `querying` / `analyzing` / `rendering` | `running` |
| `ready_for_query` / `ready_for_analysis` / `ready_for_report` | `ready` |
| `incomplete` | `partial` |
| `failed` / `expired` | `failed` |
| `cancelled` | `cancelled` |
| `completed` | `completed` |

Task 的 Runtime status 与 display state 不一致时 Contract semantic gate 拒绝。

Report 映射：

- `publishing → running`；
- `failed → failed`；
- `published → completed | partial`。

## 6. Action Capability

Action 只允许固定枚举：

```text
provide_input
approve_query
cancel_task
request_supplement
analyze
render_report
confirm_memory
open_report
```

规则：

- `enabled` 必须没有 `reason_code`；
- `disabled` 必须给出稳定 `reason_code`；
- `approve_query` 必须 `requires_confirmation=true`；
- Action 的 `task_run_id` 必须与所在 Conversation entry/Task Detail 一致；
- Contract 不携带自由 endpoint、脚本或模型生成 payload；SP2 继续使用 typed action route。

## 7. Query Review 与 Evidence

处于 `waiting_for_query_approval` 的 Task Detail 必须同时包含：

- `review_request.review_type=query`；
- `query_run_id`；
- 完整待审 SQL；
- `sql_hash`；
- `assurance_report_hash`；
- dialect、expiry；
- `read_only=true`；
- 至少一个 `approve_query` Action Capability。

批准表示批准执行精确 SQL/Assurance 对象，不表示批准分析结论正确。

Presentation 引用的 `source_artifact_ids` 必须存在于同一个 Task Detail 的 Artifact summaries 中。缺失引用失败关闭，不静默删掉 lineage。

## 8. Bounds 与去敏

主要 v1 bounds：

- Conversation detail 最多 100 entries；
- Task activity 最多 200 条；
- Task attempts/artifacts 各最多 100 条；
- Presentation table 最多 100 列、100 行；
- SQL 最长 100,000 字符；
- Presentation Markdown 最长 40,000 字符；
- source revisions 最多 16 个；
- redactions 最多 32 条；
- 外部 URL 不进入产品 Contract，`href` 只允许非 `//` 开头且不含反斜线的站内绝对路径。

这些是单字段/数组上限。SP1 Builder 和 SP2 BFF 仍必须增加序列化响应体总大小上限，不能依赖 JSON Schema 防止组合后超大响应。

`projection_meta.redactions` 只记录：

```text
field_path
reason_code
```

不回显被删除内容。Schema 使用 `additionalProperties=false`，`api_key` 等非 Contract 字段会被拒绝；Builder 仍需执行业务文本和 error 的去敏策略。

## 9. Contract 与 semantic gate

TypeBox/JSON Schema 负责结构、枚举、pattern 和 bounds；TypeScript `validateProductProjection(...)` 与 Python `validate_product_projection(...)` 使用同一 fixture corpus 和稳定 reason code 负责跨字段语义：

- availability/reason 一致性；
- source revision 去重；
- Action availability/reason；
- scope、task、parent 和 Artifact lineage；
- Task/Report status 映射；
- Query review 完整性；
- table shape；
- Workspace count 不小于返回列表；
- published/export-ready URL 完整性。

稳定 reason code 用于测试和后续 bounded API error，不直接作为业务页面文案。

## 10. 生成与验证

TypeBox Schema 是编辑真相源。修改后必须执行：

```bash
npm --prefix services/pi-orchestrator run export:product-contract
npm --prefix services/pi-orchestrator run typecheck
npm --prefix services/pi-orchestrator test
.venv/bin/pytest -q tests/test_product_projection_contracts.py tests/test_artifact_contracts.py
```

TypeScript 测试会检查生成 JSON Schema 与 TypeBox 完全同步；Python 使用同一生成 Schema，并通过 `agent/contracts/product_projection_semantics.py` 对全部正向 fixtures 和全部结构/语义负向 mutation 做 reason-code parity。

## 11. SP1 结果与 SP2 入口

SP1 已完成：

- `TaskStore` scope/user/channel/conversation/cursor 查询；
- Conversation Summary/Detail Builder；
- Task Detail Builder；
- authenticated read-only Pi API；
- 2 MB 响应边界、去敏、损坏引用、pagination、10K Task 性能与 restart recovery 测试。

证据见 [`product-spine-sp1-evidence-2026-08-25.md`](product-spine-sp1-evidence-2026-08-25.md)。

SP2 只能增加 scope-aware `ReportStore.list`、authenticated `/api/product/*` BFF、Workspace partial/offline 聚合和对应 Python 测试。不得在 SP2 新增第二状态源、开始 Product Shell 或开放 Agent Runtime。
