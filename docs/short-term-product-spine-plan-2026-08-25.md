# Forge 短期 Product Spine 实施计划（2026-08-25）

> 状态：历史实施计划，SP0–SP5 已完成并保留为 Product Projection/BFF/Web Shell 基础；`REQ-2026-08-25-023` 已将当前短期目标重排为 S0–S4 真实用户闭环，本文件不再承载主动 TODO
>
> Requirement：[`REQ-2026-08-25-017`](requirements-pool.md#req-2026-08-25-017短期-product-spine-底层优先实施计划)
>
> 原则：**底层真实框架先行，前端真实投影随后，最后形成可持续人工测试抓手。**
>
> 本计划只覆盖近期单用户私有化产品闭环，不批准企业多用户、Agent Runtime 执行、Economics、通用 Decision Runtime、自动调度或第二场景。

> **后续边界（2026-08-25）**：本文解决“如何获得真实、可恢复、可人工测试的产品骨架”，不证明目标用户愿意持续使用。当前唯一主动路线见 [`forge-enterprise-evolution-plan.md`](forge-enterprise-evolution-plan.md)：先以 Design Partner 建立真实问题基线，再验证 Direct Trusted Answer、Semantic Learning Loop 和三环境证据；不继续在本文追加页面、Runtime 或企业治理工作包。

## 1. 短期目标

在最短路径上建立一条可以持续测试的真实产品主链：

```text
真实 Conversation Event
→ Pi Task / Plan / Stage
→ Forge Query Review / Read-only Execution
→ Artifact / Evidence
→ Analysis / Report
→ Product Projection API
→ Web Product Shell
→ 用户持续测试、反馈和补功能
```

短期结束时，用户不需要阅读内部 API 或手工拼接 URL，能够在一个统一 Web 产品中：

1. 开始和恢复一个 Conversation；
2. 查看同一 Conversation 下的真实 Task；
3. 看清 Plan、当前阶段、等待项、失败和下一步；
4. 审核真实 SQL 并确认只读执行；
5. 查看 QueryResult、Evidence、Analysis 和 Report；
6. 从工作台找到待处理任务、失败任务和最近报告；
7. 刷新页面或服务重启后继续，而不丢失主链；
8. 对未实现能力得到明确说明，而不是无效按钮或 fixture 假成功。

这条主链是后续增加 Agent Runtime、治理、可复用报告和多人协同的产品抓手，不是最终功能全集。

## 2. 当前基线与直接缺口

### 2.1 已有真实能力

- Pi 已持久化 TaskRun、TaskEvent、StageAttempt、Artifact、ChannelEvent 和 Skill Policy；
- 每条 Web/飞书/钉钉消息都会创建 TaskRun，并带 `channel_conversation_id`；
- Pi 已支持 conversation / knowledge / query / workflow / action / clarification 路由；
- Forge 已支持 Registry Context、QueryRun、SQL hash 审批、只读执行和查询结果；
- Pi 已支持 QueryResult、Analysis、ReportBundle、Publication 等 Artifact；
- Web 已有 `/chat`、`/tasks` 和任务 action proxy；
- ReportStore 已持久化不可变报告及 PDF/PPTX 状态；
- Web 已有任务 scope/channel/admin 门禁和最小披露逻辑。

### 2.2 造成“没有产品抓手”的缺口

1. ChannelEvent 只支持按 event ID 获取，没有 Conversation list/detail 产品 Projection；
2. 当前 `/chat` 左侧显示“最近任务”，不能稳定恢复完整 Conversation；
3. Task Detail 需要前端分别请求 task/events/artifacts/attempts/presentation，缺少统一、版本化、去敏的产品读模型；
4. Task list 没有围绕“待我处理、失败恢复、最近完成”组织；
5. ReportStore 只有 `get`，没有 scope-aware list，因此没有真实报告库；
6. 当前 `/tasks` 同时承担创建、监控、审批、结果、分析和报告，难以深链接和持续测试；
7. `base.html` 导航过多且依赖 Tailwind/Marked CDN，无法成为稳定 Product Shell；
8. Runtime Governance Coverage 仍为 0%，所以短期只能声明单用户私有化 Human Control Plane。

## 3. 短期架构：只增加 Projection，不增加第二真相源

```text
Pi Truth Sources
TaskStore / TaskEventStore / ArtifactStore / StageAttemptStore / ChannelEventStore
          │
          ▼
Pi Product Projection v1
ConversationSummary / ConversationDetail / TaskDetail / ActionCapability
          │
          ▼
Forge Web Product BFF
Workspace / Conversations / Tasks / Reports / Data Summary
          │
          ▼
Jinja Product Shell + local CSS/JS
```

### 3.1 写入责任保持不变

| 操作 | 唯一写入方 |
|---|---|
| Conversation message/action | Web ChannelEvent → Pi |
| Task/Plan/Stage/Event | Pi |
| Query review/approval/execution | Forge QueryRun |
| Analysis/Report Artifact | Pi + Forge ReportStore |
| Registry | Forge Registry Studio |
| Product Web | 只做输入、Projection 和 typed action proxy |

短期不新增 `ConversationStore`、`DecisionStore`、`WorkspaceStore` 或前端状态数据库。

### 3.2 Conversation 的短期定义

Conversation v1 是从已有 TaskRun 投影出来的稳定主题视图：

```text
channel + channel_conversation_id + org_id + team_id + user_id
→ ordered TaskRuns
→ original user message
→ latest ChannelPresentation / status / Artifact refs
```

每个消息当前都会产生 TaskRun，因此可先由 Task Store 重建 Conversation。若实现时证明存在无法恢复的消息或响应，必须暂停并提出最小 Event Index；不能直接复制一份可写聊天记录。

## 4. 工作包与严格顺序

## SP0：Contract 与真相源封口

**目标**：先固定前端将消费什么，不先写页面。

### 实施内容

- 定义版本化只读 Contract：
  - `ConversationSummaryV1`；
  - `ConversationDetailV1`；
  - `TaskDetailProjectionV1`；
  - `ActionCapabilityV1`；
  - `WorkspaceProjectionV1`；
  - `ReportSummaryV1`。
- 每个 Contract 明确：scope、字段上限、排序、cursor、状态、redaction、source revision 和 unavailable reason；
- 建立状态到产品语言的映射：`needs_input / waiting_decision / running / partial / ready / failed / forbidden / offline`；
- 明确 Task/Artifact/Report 的链接关系和 Evidence 最小展示字段；
- 固定短期 Route 与 API 名称，不冻结长期 IA。

### 不做

- 不增加新数据库表；
- 不设计完整 Principal/Mandate Runtime；
- 不创建通用 Action/Decision DSL；
- 不修改页面。

### 验收

- Contract fixtures 覆盖 ready、needs_input、waiting_query_approval、running、partial、failed、completed；
- 负向 fixtures 覆盖跨 scope、超大 Artifact、未知状态、Secret-like 字段和损坏引用；
- Contract 评审确认前不进入 SP1。

**预计**：1–2 个工作日。

---

## SP1：Pi Product Projection 底层

**目标**：从现有 Pi 真相源生成 Conversation 和 Task 的真实、可恢复读模型。

### 实施内容

- 扩展 `TaskStore` 只读查询：
  - scope + user + channel；
  - `channel_conversation_id`；
  - cursor/limit；
  - 必要的 parent/child Task 关系。
- 增加纯 Projection Builder，不写状态：
  - Conversation list；
  - Conversation detail；
  - Task detail 聚合 task/events/attempts/artifacts/presentation；
  - Action capability 根据真实 Task status、Artifact 和可用 action 计算。
- 增加 Pi read-only API：
  - `GET /v1/conversations`；
  - `GET /v1/conversations/{conversation_id}`；
  - `GET /v1/tasks/{task_run_id}/detail`。
- 所有列表必须有 scope、limit、稳定排序和最大响应体；
- Projection 对 QueryResult rows、SQL、error 和技术 metadata 采用业务/技术双层最小披露；
- 仅在 10K Task 基准证明查询不可接受时再增加 schema v5 索引/显式列迁移，不因“框架感”提前复制字段。

### 不做

- 不增加 Conversation 写 API；
- 不改变 Task 状态机；
- 不合并 Pi 与 Forge 数据库；
- 不增加 Agent Client。

### 验收

- SQLite 重启后 Conversation/Task Projection 一致；
- 同一 Conversation 的 Task 顺序、parent/child 和 Presentation 可恢复；
- 跨 org/team/user/channel 查询失败关闭；
- 大 Artifact、损坏引用和未知状态返回 bounded partial/failed，不导致整页 500；
- TypeScript test、typecheck 和 SQLite migration/recovery tests 通过。

**预计**：3–5 个工作日。

---

## SP2：Forge Web Product BFF 与报告索引

**目标**：给前端一个稳定、scope-aware 的产品 API，不让页面直接拼装多个底层 Store。

### 实施内容

- `ReportStore.list(...)`：按 org/team/user/status/cursor/limit 返回 `ReportSummaryV1`；
- 增加 authenticated product API：
  - `GET /api/product/workspace`；
  - `GET /api/product/conversations`；
  - `GET /api/product/conversations/{conversation_id}`；
  - `GET /api/product/tasks`；
  - `GET /api/product/tasks/{task_run_id}`；
  - `GET /api/product/reports`；
  - `GET /api/product/data-summary`。
- Web BFF 只聚合 Pi API、ReportStore 和 Registry 摘要，不缓存可写业务状态；
- Workspace v1 只包含：待补充、待 SQL 审批、失败恢复、进行中任务、最近报告、依赖不可用；
- action 继续走已有 typed endpoints，不建立 `/actions/execute` 通用入口；
- 每个 action 由 `ActionCapabilityV1` 决定是否显示、可否执行和禁用原因，前端不根据 status 自行猜测；
- 加入 timeout、partial dependency 和 offline 语义，Pi 不可用时 Report/Data 仍可部分展示。

### 不做

- 不把 Report 复制进 Pi Store；
- 不把 Task 复制进 Python 数据库；
- 不开放 Agent Runtime；
- 不新增未实现 Governance 页面数据。

### 验收

- Web auth、scope、channel 和 admin 门禁测试通过；
- Pi 离线、ReportStore 离线、Registry 损坏均返回明确 partial/offline；
- Report list 不能跨 scope，失败时不泄漏 report 是否存在；
- BFF 响应可由 Contract fixture 重放，页面不依赖内部 Store 字段。

**预计**：2–4 个工作日。

### Backend Gate

只有以下条件同时成立才进入前端：

```text
Contract 固定
+ Conversation 可恢复
+ Task Detail 可聚合
+ Report 可列表
+ Workspace 可部分降级
+ Scope 负向测试通过
+ 无第二真相源
```

---

## SP3：前端基础 Shell

**目标**：建立真实产品页面的共同结构，不一开始丰富所有细节。

### 实施内容

- 新建 `product_base.html`，与旧 Admin `base.html` 解耦；
- 使用本地 `web/static/product.css` 和小型原生 JS modules，不引入 React/Vue，不使用 CDN；
- 建立稳定设计 token、按钮、状态、表格、drawer/dialog、empty/partial/offline 组件；
- 短期一级入口仅保留：
  - 工作台；
  - 对话；
  - 任务；
  - 报告；
  - 数据资产；
  - 底部管理入口。
- 不显示 Agents & Apps、通用治理中心、Economics、Memory、Pipeline 或 Architecture；
- 页面骨架支持 loading、empty、needs_input、waiting_decision、running、partial、failed、forbidden、offline；
- 先保证桌面端 1440×900、1600×1000 和键盘操作。

### 不做

- 不重写全部 20 个 Admin 页面；
- 不为“现代化”引入前端框架或第二构建系统；
- 不在页面中硬编码 fixture 作为正常数据；
- 不提供不可工作的按钮。

### 验收

- 0 CDN、0 console error、0 横向溢出；
- back/forward、deep link、focus、dialog/drawer 可用；
- 每个页面都能区分真实、partial、offline 和未开放；
- 旧 Admin 仍可从底部入口访问，不被一次性重写。

**预计**：2–3 个工作日。

---

## SP4：真实页面与 Human Golden Journey

**目标**：把 SP1/SP2 的真实能力投影成用户可持续测试的产品。

### 页面切片

#### `/workspace`

- 待我补充；
- 待 SQL 审批；
- 失败恢复；
- 进行中的任务；
- 最近报告；
- 依赖 partial/offline。

#### `/chat`

- Conversation list 与稳定 URL/选中状态；
- 同一 Conversation 下按时间展示多个真实 Task；
- 普通交流、知识回答、查询审批、结果、分析和报告 Presentation；
- 固定输入区；
- 任务详情和 Evidence 深链接；
- “新对话”只是开始 Conversation，不是独立 brief form。

#### `/tasks` 与 `/tasks/{task_run_id}`

- 列表按待处理、进行中、失败、完成过滤；
- Detail 展示目标、Plan、当前阶段、Action capability、Artifact、Evidence、Activity、parent/child；
- SQL Review 显示精确 SQL/hash/风险/expiry 的业务解释；
- 审批、补充、分析和生成报告均调用真实 typed action。

#### `/reports`

- scoped Report Library；
- status、时间、Task、revision、PDF/PPTX availability；
- 复用现有不可变详情和分享/下载；
- 不实现 ReusableReportDefinition。

#### `/data`

- 只聚合已有 Registry/Schema/Metric/Semantic/Staging/Knowledge 入口与当前状态；
- 不假装已有 Conflict/Owner/Quality Runtime。

### 必须通过的真实 Journey

1. **GJ-H1**：Conversation → Task → SQL Review → 一次只读执行 → QueryResult → Analysis → Report → Report Library；
2. **GJ-H2**：Knowledge question → Registry/Context Evidence → answer，无 QueryRun；
3. **GJ-H3**：模糊问题 → needs_input → 补充 → 同一 Task 继续；
4. **GJ-H4**：Pi/Model/Report 部分失败 → 显示已发生/未发生副作用 → 合法恢复；
5. **GJ-H5**：刷新/浏览器返回/服务重启 → Conversation 与 Task 可恢复。

### 验收

- 所有关键 action 真实可用或明确 disabled；
- 重复消息/重复审批不重放 SQL；
- 查询完成不提前把需要报告的 Task 标为完成；
- Evidence 可从分析 finding 回到 QueryResult row/source；
- 一个用户不需要手工输入 Task ID 或 Report ID；
- 真实页面内容遵守观察 → 有限判断 → 限制 → 待补 Evidence。

**预计**：4–6 个工作日。

---

## SP5：集成门禁与持续测试环境

**目标**：把产品主链变成长期可回归的抓手，而不是一次性演示。

### 实施内容

- 自动测试：
  - TypeScript Contract/Projection/SQLite tests；
  - Python BFF/auth/scope/report list tests；
  - Playwright desktop journeys；
  - idempotency/restart/offline/partial tests；
  - Web content/no-CDN/no-dead-action tests。
- 真实 Golden Journey：使用真实模型、隔离的只读测试数据源、真实 Forge QueryRun 和真实 Report exporter；
- Atlas 发布独立 candidate revision，使用独立 Pi state、Report DB 和只读测试 datasource，不覆盖现有生产状态；
- 用户按页面和 Journey 给出 `PASS / CHANGE / REMOVE`；
- 每轮反馈进入需求池，按 P0/P1/P2 归类后再迭代，不直接在页面上堆补丁。

### 最终门禁

- 1440×900、1600×1000：0 overflow、0 console/page error；
- 0 外部 CDN；
- 0 生产副作用请求；
- SQL 审批/执行一致，重复动作不重放；
- refresh/restart 恢复通过；
- cross-scope 负向测试通过；
- Report HTML/PDF/PPTX ready 或明确 partial；
- Human Golden Journey 至少连续通过 3 次；
- 用户获得稳定 Atlas URL 进行持续测试。

**预计**：2–4 个工作日。

### SP5 实施结果（2026-08-25）

- Candidate 固定为 `product-spine-5dcd4715941a`，使用独立 Pi/QueryRun/Report/Registry 状态和 candidate 内 mode `0400` 只读测试数据副本；认证已开启，生产 Forge/Pi 未被替换。
- 固定渠道指标问题在最终 candidate 配置下连续 3 次完成 Conversation → SQL Review → 单次只读执行 → Analysis → Report → Report Library；每次均为 1 个 QueryRun、1 个 `query.completed`、4 个 succeeded StageAttempt，PDF/PPTX ready。
- 重复消息返回原任务，过期重复批准返回 409 且不重放 SQL；等待审批与完成态均通过 Pi/API restart recovery；Pi offline 时 Workspace 明确降级为 partial，Report/Data 继续可读。
- 真实链路发现并修复 insecure-HTTP ID、瞬时 ready 轮询、同源 HTTP 报告链接、空 Attempt error、长 SQL Grid overflow 和完成态历史审核误标权限六个缺陷；复杂查询 Assurance 拒绝与数据质量 `incomplete` 作为反证保留，未通过放宽门禁掩盖。
- 最终验证：Python `583 passed / 24 skipped`、Pi `115 passed`、typecheck、npm audit、JS syntax、`git diff --check`、双桌面 12 routes/0 external request/0 error/0 overflow 全部通过。
- 证据：[`product-spine-sp5-evidence-2026-08-25.md`](product-spine-sp5-evidence-2026-08-25.md)。自动门禁 PASS；用户 Atlas 门禁仍为 PENDING，不能把稳定 URL 冒充用户接受。
- 用户 Atlas 首轮返回 `CHANGE`：Workspace/Task 等页面性能严重不可接受。诊断确认 Pi 原始读取仅 `44.4–55.9 ms`，主瓶颈是 Product BFF 对每条 Task/Report 重复执行 JSON Schema `check_schema` 与 validator construction。
- `agent.contracts.validate_contract` 改为按 Contract name 缓存已检查 validator；不减少数据、Contract、Evidence、Assurance 或 scope gate。批量 100 Task + 50 Report 回归从 `12.83s` 失败降为 `0.18s` 测试完成。
- 性能候选 `product-spine-d0aa8c9e3a0e` 已在同一隔离状态、只读数据和稳定 URL 上发布。稳定 Product API 为 `54.5–258.8ms`；Workspace/Chat/Tasks/Task Detail/Reports/Data 严格等待 DOM 内容替换后的完成时间为 `83.7–586.9ms`，0 console/page error、0 横向溢出；定向回归 `41 passed`。
- 自动性能门禁 PASS；用户 Atlas 复验仍为 PENDING，不能据此替换生产或启动后续工作包。
- 用户 Atlas 第二项 `CHANGE`：Conversation QueryResult 已显示“共 107 行”，但没有 Table。诊断确认 Pi Presentation 与 Product Projection 均已携带 Table，只有 Web `renderConversation()` 未消费。
- Conversation entry 改为复用 Task Detail 的 `renderPresentation()`；真实只读 Task `tr_c0c65389a9a344e9b711cfa68909f6eb` 返回 107 行后，页面显示 2 列、20 行有界预览、总行数和截断提示，0 横向溢出。
- Table 最终候选 `product-spine-6a23e71276e5` 已发布；六个 Product 页面统一引用 `product-pages.js?v=2`，正常缓存路径已确认加载 v2 并显示 20 行 Table。浏览器行为回归从失败转为通过，页面/缓存契约 `11 passed`、JS syntax PASS。用户 Atlas 复验仍为 PENDING。

## 5. 总体时间与依赖

| 顺序 | 工作包 | 预计 | 依赖 |
|---:|---|---:|---|
| 1 | SP0 Contract/Truth Source | 1–2 天 | 当前代码与北极星 |
| 2 | SP1 Pi Product Projection | 3–5 天 | SP0 |
| 3 | SP2 Product BFF/Report Index | 2–4 天 | SP1 |
| 4 | SP3 Product Shell Foundation | 2–3 天 | Backend Gate |
| 5 | SP4 Real Product Pages/Journeys | 4–6 天 | SP2、SP3 |
| 6 | SP5 Golden Gate/Atlas | 2–4 天 | SP4 |

总量约 **14–24 个工作日**。这是风险范围，不是交付承诺；每个工作包单独验收，前一包未通过不并行堆叠下一层。

## 6. 短期明确不做

- M1A 完整 Runtime Governance；
- Agent Client、Agent execute API 或 Agents & Apps 生产页面；
- 多 Workspace、OIDC/SCIM、完整 RBAC；
- 通用 DecisionRequest/DecisionRecord Runtime；
- Economics、Budget、Usage Ledger；
- ReusableReportDefinition、自动调度和免审批重跑；
- 第二非 SQL 场景；
- 全部 Admin 页面重写；
- 通用 Memory Store；
- ECharts H5 生产切换。

这些能力不会被取消，但必须建立在可持续测试的 Product Spine 之后重新排序。

## 7. 停止条件

出现以下任一情况必须暂停而不是继续堆 UI：

1. Conversation 不能从现有 Task/Event 真相源无损恢复；
2. Product BFF 需要复制可写 Task/Approval/Report 状态；
3. Task Detail 必须暴露未去敏 Artifact 才能工作；
4. Scope 校验只能依靠前端隐藏；
5. 页面需要 fixture 才能显得完整；
6. 后端 Contract 频繁随页面实现变化，说明 SP0 未封口；
7. 真实 Golden Journey 出现 Silent Error、重复 SQL、审批对象漂移或不可解释副作用。

## 8. 短期结束后的决策点

Product Spine 通过并经过一轮真实人工测试后，再根据实际阻力选择下一项：

- 如果最大阻力是身份与 Agent 调用：进入 M1A → Agent Runtime MVP；
- 如果最大阻力是语义冲突和数据资产：进入 G1 Data Trust Control Plane；
- 如果最大阻力是结果质量和错误不可见：进入 Q1 Quality/Assurance；
- 如果最大阻力是重复报告制作：进入 H6 Reusable Deliverables；
- 如果只是页面细节问题：进入按证据排序的 Product UX P1，不扩后端边界。

不能在短期计划开始前预设下一项一定是什么。

## 9. 本轮批准请求

用户只需要确认以下四点即可启动实施：

1. 是否批准 `SP0 → SP1 → SP2 → Backend Gate → SP3 → SP4 → SP5` 的底层优先顺序；
2. 是否接受短期仅做单用户 Human Control Plane，不开放 Agent Runtime；
3. 是否接受前端只先保留“工作台 / 对话 / 任务 / 报告 / 数据资产 / 管理”；
4. 是否接受 14–24 个工作日为风险范围，并按工作包逐项验收，而不是等待全部完成后一次验收。
