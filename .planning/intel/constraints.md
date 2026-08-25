# Constraints

## Product Projection 只读边界
- source: docs/product-projection-contracts.md
- type: protocol
- content:
  DATA_X7n2Qa9L_START
  Product Projection 把 Pi/Forge 的内部 Task、Event、Artifact、Attempt、QueryRun、Report 和 Registry 状态转换为前端可稳定消费、可去敏、可降级的读模型。
  它只做 Projection：
  - 不创建 Conversation、Task、Approval 或 Report；
  - 不推进状态；
  - 不产生权限；
  - 不替代 Pi/Forge/ReportStore 真相源；
  - 不证明结果在开放世界中必然正确。
  DATA_X7n2Qa9L_END

## 七个公开 Contract
- source: docs/product-projection-contracts.md
- type: api-contract
- content:
  DATA_Y4k8Vs1P_START
  - ActionCapabilityV1：某个 Task 当前允许或禁止的 typed action 及原因；主要来源为 Task status、Event、Artifact、Policy availability。
  - ConversationSummaryV1：Conversation 列表中的稳定摘要；主要来源为 TaskRun 按 channel_conversation_id 分组。
  - ConversationDetailV1：同一 Conversation 下有序 Task/message/presentation；主要来源为 TaskRun、TaskEvent、Artifact、ChannelPresentation。
  - TaskSummaryV1：scope-aware 任务列表条目；不暴露任意 metadata；主要来源为 Pi TaskRun。
  - TaskDetailProjectionV1：Plan、Review、Activity、Attempt、Artifact、Evidence 和关系；主要来源为 Pi Task/Event/Attempt/Artifact + Forge Query review。
  - WorkspaceProjectionV1：待补充、待决定、进行中、失败、最近报告和依赖状态；主要来源为 Pi Task Projection + Report/Registry availability。
  - ReportSummaryV1：scope-aware 报告库条目与导出状态；主要来源为 Forge ReportStore。
  DATA_Y4k8Vs1P_END

## 顶层对象固定字段
- source: docs/product-projection-contracts.md
- type: schema
- content:
  DATA_Z9m3Ch6R_START
  schema_version = 1
  projection_type = <明确类型>
  additionalProperties = false
  DATA_Z9m3Ch6R_END

## 非法 Product Projection 失败关闭
- source: docs/product-projection-contracts.md
- type: protocol
- content: DATA_a2F7qL8N_START未知字段、未知状态、超出 bounds 或不合法 ID 必须失败关闭。DATA_a2F7qL8N_END

## Conversation v1 重建键
- source: docs/product-projection-contracts.md
- type: schema
- content:
  DATA_b5T1xW9K_START
  Conversation v1 不增加可写 ConversationStore。它按以下键从 TaskRun 重建：
  org_id
  + team_id
  + user_id
  + channel
  + channel_conversation_id
  DATA_b5T1xW9K_END

## Scope 一致性与服务端过滤
- source: docs/product-projection-contracts.md
- type: protocol
- content: DATA_c8P4nH2V_STARTConversationDetailV1.scope 必须与 summary.scope 完全一致。Task、Workspace 和 Report 也携带 scope；SP1/SP2 必须先做服务端 scope 过滤，再构建 Projection，不能依靠前端隐藏。DATA_c8P4nH2V_END

## Projection 来源与 revision
- source: docs/product-projection-contracts.md
- type: schema
- content:
  DATA_d3J9rM6Q_START
  projection_meta.source_revisions 明确本次读模型使用的来源及 revision，允许来源包括：
  - pi_task_store；
  - pi_event_store；
  - pi_artifact_store；
  - pi_attempt_store；
  - forge_query_store；
  - forge_report_store；
  - forge_registry_store。
  同一个 source + revision 不得重复。
  DATA_d3J9rM6Q_END

## Availability 与降级
- source: docs/product-projection-contracts.md
- type: protocol
- content:
  DATA_e6V2kB7T_START
  顶层 projection_meta.availability 只有：
  - ready：所有声明来源可用，unavailable_reasons 必须为空；
  - partial：部分来源或字段不可用，必须给出 reason code；
  - offline：主依赖不可用，必须给出 reason code。
  partial/offline 不允许以空原因返回；ready 不允许同时声明 unavailable reason。
  Web 的 loading 是客户端读取状态，不属于已返回的 Projection 数据。
  DATA_e6V2kB7T_END

## Task display state 映射
- source: docs/product-projection-contracts.md
- type: protocol
- content:
  DATA_f1Q8yN4C_START
  Product Projection 使用稳定的 display state：needs_input、waiting_decision、running、partial、ready、failed、forbidden、offline、completed、cancelled。
  TaskRun 到 display state 的 v1 映射固定为：
  - needs_input → needs_input；
  - waiting_for_query_approval / waiting_for_action_approval → waiting_decision；
  - created / clarifying / querying / analyzing / rendering → running；
  - ready_for_query / ready_for_analysis / ready_for_report → ready；
  - incomplete → partial；
  - failed / expired → failed；
  - cancelled → cancelled；
  - completed → completed。
  Task 的 Runtime status 与 display state 不一致时 Contract semantic gate 拒绝。
  DATA_f1Q8yN4C_END

## Report display state 映射
- source: docs/product-projection-contracts.md
- type: protocol
- content:
  DATA_g7L3sX9M_START
  - publishing → running；
  - failed → failed；
  - published → completed | partial。
  DATA_g7L3sX9M_END

## Action Capability
- source: docs/product-projection-contracts.md
- type: api-contract
- content:
  DATA_h4R8pD2W_START
  Action 只允许固定枚举：provide_input、approve_query、cancel_task、request_supplement、analyze、render_report、confirm_memory、open_report。
  规则：
  - enabled 必须没有 reason_code；
  - disabled 必须给出稳定 reason_code；
  - approve_query 必须 requires_confirmation=true；
  - Action 的 task_run_id 必须与所在 Conversation entry/Task Detail 一致；
  - Contract 不携带自由 endpoint、脚本或模型生成 payload；SP2 继续使用 typed action route。
  DATA_h4R8pD2W_END

## Query Review
- source: docs/product-projection-contracts.md
- type: api-contract
- content:
  DATA_i9C1mK5V_START
  处于 waiting_for_query_approval 的 Task Detail 必须同时包含：
  - review_request.review_type=query；
  - query_run_id；
  - 完整待审 SQL；
  - sql_hash；
  - assurance_report_hash；
  - dialect、expiry；
  - read_only=true；
  - 至少一个 approve_query Action Capability。
  批准表示批准执行精确 SQL/Assurance 对象，不表示批准分析结论正确。
  DATA_i9C1mK5V_END

## Evidence lineage
- source: docs/product-projection-contracts.md
- type: protocol
- content: DATA_j2N7tQ8F_STARTPresentation 引用的 source_artifact_ids 必须存在于同一个 Task Detail 的 Artifact summaries 中。缺失引用失败关闭，不静默删掉 lineage。DATA_j2N7tQ8F_END

## Product Projection v1 bounds
- source: docs/product-projection-contracts.md
- type: nfr
- content:
  DATA_k5W9bH3R_START
  - Conversation detail 最多 100 entries；
  - Task activity 最多 200 条；
  - Task attempts/artifacts 各最多 100 条；
  - Presentation table 最多 100 列、100 行；
  - SQL 最长 100,000 字符；
  - Presentation Markdown 最长 40,000 字符；
  - source revisions 最多 16 个；
  - redactions 最多 32 条；
  - 外部 URL 不进入产品 Contract，href 只允许非 // 开头且不含反斜线的站内绝对路径。
  DATA_k5W9bH3R_END

## 序列化响应体总大小上限
- source: docs/product-projection-contracts.md
- type: nfr
- content: DATA_l8X4vM1P_START这些是单字段/数组上限。SP1 Builder 和 SP2 BFF 仍必须增加序列化响应体总大小上限，不能依赖 JSON Schema 防止组合后超大响应。DATA_l8X4vM1P_END

## Redaction 与非 Contract 字段
- source: docs/product-projection-contracts.md
- type: schema
- content:
  DATA_m3Q6zT9K_START
  projection_meta.redactions 只记录：
  field_path
  reason_code
  不回显被删除内容。Schema 使用 additionalProperties=false，api_key 等非 Contract 字段会被拒绝；Builder 仍需执行业务文本和 error 的去敏策略。
  DATA_m3Q6zT9K_END

## 跨字段 semantic gate
- source: docs/product-projection-contracts.md
- type: protocol
- content:
  DATA_n7B2rL5C_START
  TypeBox/JSON Schema 负责结构、枚举、pattern 和 bounds；TypeScript validateProductProjection(...) 与 Python validate_product_projection(...) 使用同一 fixture corpus 和稳定 reason code 负责跨字段语义：
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
  DATA_n7B2rL5C_END

## Contract 编辑真相源与 parity
- source: docs/product-projection-contracts.md
- type: protocol
- content: DATA_p1H8wV4N_STARTTypeBox Schema 是编辑真相源。TypeScript 测试会检查生成 JSON Schema 与 TypeBox 完全同步；Python 使用同一生成 Schema，并通过 agent/contracts/product_projection_semantics.py 对全部正向 fixtures 和全部结构/语义负向 mutation 做 reason-code parity。DATA_p1H8wV4N_END

## SP2 实施边界
- source: docs/product-projection-contracts.md
- type: protocol
- content: DATA_q6M3cJ9S_STARTSP2 只能增加 scope-aware ReportStore.list、authenticated /api/product/* BFF、Workspace partial/offline 聚合和对应 Python 测试。不得在 SP2 新增第二状态源、开始 Product Shell 或开放 Agent Runtime。DATA_q6M3cJ9S_END
