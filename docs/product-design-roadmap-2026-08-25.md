# Forge 产品设计与阶段路线重建提案（2026-08-25）

> 状态：三个产品面与完整 Product Shell 已完成方向和前端表达；`REQ-2026-08-25-023` 进一步确认短期不建设未来企业对象，而先面向已有数据库/数仓的小型数据团队，以真实 Design Partner 验证“直接可信答案 → 语义纠正 → 安全复用”。M1A/R1/G1/Q1/H6 仍未批准
>
> Requirement：[`REQ-2026-08-25-016`](requirements-pool.md#req-2026-08-25-016基于产品北极星重建产品设计与阶段计划)
>
> 本文依据 [`product-north-star.md`](product-north-star.md) 重建 Forge 的产品结构与未来顺序。它不批准代码实施，不替代 [`forge-enterprise-evolution-plan.md`](forge-enterprise-evolution-plan.md)。旧 [`web-product-shell-plan-2026-08-24.md`](web-product-shell-plan-2026-08-24.md) 和 `821065f` 原型保留为第一版历史与反例。

## 1. 重建结论

Forge 不应再被设计为以下任何单一产品：

- Text-to-SQL 页面；
- 分析工作台；
- Chat 应用；
- 任务管理器；
- 数据治理后台；
- 通用 Agent 平台。

新的产品结构是：

> **一个 Forge，三个相互约束的产品面。**

```text
Human Work Surface
· Conversation · Task · Decision · Deliverable
                    │
                    ▼
Shared Trust & Data Foundation
· Registry · Policy · Evidence · Assurance · Audit
                    │
                    ▼
Agent-facing Trusted Data Runtime
· Context · Data Task · Approval wait · Artifact/Evidence API
```

三者共享同一组 Task、Artifact、Evidence、Decision 和数据执行真相源，不建立“人用一套、Agent 用一套”的产品或状态机。

### 1.1 人的工作面

让 Requester、Analyst、Steward、Approver 和 Auditor 能够：

- 连续表达问题与追问；
- 看清当前任务、依据、阻断和下一步；
- 对精确 Action 作出决定；
- 查看可复算结果和不可变交付物；
- 维护语义、规则和冲突；
- 观察人和 Agent 的执行记录。

### 1.2 共享信任与数据基础

负责把企业原始系统中的数据能力变成可安全使用的事实与 Evidence：

- 数据源、Schema、指标、关系和时间粒度；
- Principal、Mandate、Policy 和资源范围；
- Query Assurance、Approval、只读执行和 Audit；
- Claim、Evidence、Conflict、Knowledge 与版本；
- 失败、反馈、评测和规则提案。

### 1.3 Agent-facing Runtime

让企业 Copilot、经营 Agent、分析 Agent 或工作流服务能够：

- 解析有界语义和 Context；
- 发起受治理的数据任务；
- 等待澄清或人工 Decision；
- 获取结构化 QueryResult、Analysis、Report 和 Evidence；
- 提交反馈或知识候选；
- 在权限、预算或证据不足时得到明确失败，而不是无来源答案。

它不是数据库代理、裸 SQL API 或 Agent Marketplace。

## 2. 服务对象与核心工作

| 参与者 | 要完成的工作 | 不应承担的工作 |
|---|---|---|
| Business/Human Requester | 提出目标、补充背景、消费结论与交付物 | 维护表关系、判断底层 SQL 安全 |
| Analyst | 探索数据、解释结果、组织 Evidence、形成交付 | 绕过 Registry/Policy 直接定义组织事实 |
| Data Steward/Owner | 维护语义、关系、质量、有效期和冲突 | 审核每一次低风险普通对话 |
| Approver/Decision Owner | 对明确 Action、风险和例外作出决定 | 替 Agent 修复任意自由输出 |
| Auditor/Challenger | 查看来源、权限、Decision、Action 和 Outcome | 推进或修改业务 Task 状态 |
| Enterprise Agent | 代表 Principal 发起和消费有界数据任务 | 自行扩大身份、权限、预算或事实范围 |
| Agent Operator/Admin | 配置 Agent Client、Mandate、模型、渠道和运行边界 | 通过一个超级 Token 获得所有数据权限 |

采用关系保持：

```text
Data-Team Led
Business Accessible
Agent Native
Human Accountable
```

## 3. 产品对象模型

```text
Workspace / Data Domain
├── Principal / Membership
├── Agent Client / Delegated Mandate
├── Conversation
│   ├── Message Event
│   ├── Context Reference
│   └── Task Reference(s)
├── Task / Data Task Contract
│   ├── Intent / Scope / Deliverable Contract
│   ├── Execution Plan / Stage
│   ├── Participant / Actor
│   ├── Decision Request / Record
│   ├── Action / QueryRun
│   ├── Artifact
│   │   └── Evidence Link(s)
│   └── Outcome
├── Data Asset
│   ├── Datasource / Schema
│   ├── Metric / Semantic Rule / Relationship
│   ├── Claim / Conflict / Knowledge Revision
│   └── Policy / Quality
└── Deliverable
    ├── Report / Export
    └── Reusable Definition（后续）
```

### 3.1 对象与真相源

| 产品对象 | 责任 | 当前/目标真相源 | 当前差距 |
|---|---|---|---|
| Conversation | 人的连续交互和主题上下文 | Pi Session/ChannelEvent；目标增加稳定只读索引 | 已有 `conversation_id`，无正式 Conversation list/detail 与跨 Task Context Contract |
| Task | 可恢复的执行承诺 | Pi Task Store | 已实现单用户 TaskRun；Principal/Mandate/Participant 未进入 Runtime |
| Plan/Stage | 交付物、依赖和完成条件 | Pi ExecutionPlan/StageAttempt | 已实现，但用户产品投影仍弱 |
| Decision | 谁对哪个 Action 作出什么决定 | 当前 Query Approval 在 Forge；目标 Pi Decision Contract + Domain Action | 通用 DecisionRequest/Record 尚无 Runtime |
| Query/Action | 实际数据操作和副作用 | Forge QueryRun/Executor/Audit | 查询链较强；其他 Action 仍有限 |
| Artifact | 阶段结果与不可变交付 | Pi Artifact/Report Store/Forge domain stores | 类型较完整，缺少统一 Library 与跨对象检索 |
| Evidence | 支持 Claim/Decision 的来源 | QueryResult、Registry、Source refs | 查询 Evidence 强；通用 Claim/Conflict projection 未完成 |
| Data Asset | 正式结构、语义、关系和规则 | Forge Registry/Policy | Registry Studio 已有；Owner、Conflict、Runtime Policy 不完整 |
| Agent Client | 代表 Principal 调用 Forge 的受托客户端 | 目标 Pi Governance Store | 未实现；外部只开放 prepare-query |
| Outcome | 结果是否被接受、纠正和复用 | 目标 Pi/Domain Outcome Record | 未实现统一账本 |

### 3.2 三条不可混淆的关系

1. **Conversation ≠ Task**：一个对话可以包含多个相关任务；Conversation 提供人的连续性，Task 提供机器执行边界。
2. **Artifact ≠ Fact**：Artifact 是结果载体；其中内容仍需区分 Claim、Evidence、Knowledge、Decision 和限制。
3. **Approval ≠ Correctness**：批准只能证明有权主体同意精确 Action，不能证明业务结论必然正确。

## 4. 产品信息架构提案

### 4.1 导航分组

```text
工作
├── 工作台
├── 对话
├── 任务
└── 交付

信任
├── 数据资产
└── 治理与审计

接入
└── Agents & Apps

系统
└── 管理
```

这不是要求所有角色看到全部入口。正式产品按 Principal/Role/Policy 显示授权范围；单用户私有部署可看到完整管理入口。

### 4.2 页面职责

#### 工作台

回答“现在需要我处理什么”：

- 待补充、待 Decision、失败恢复；
- 进行中的人/Agent 任务；
- 最近完成的交付；
- 数据或系统阻断；
- 与当前角色相关的异常和过期项。

它不是 KPI Dashboard，也不是系统健康页。

#### 对话

回答“我如何持续提出、澄清和推进问题”：

- 最近 Conversation；
- 连续消息与结构化 Presentation；
- Knowledge、Query、Workflow 和 Rule Proposal；
- 当前关联 Task、Plan、Evidence 和交付；
- 固定输入区、后续追问和任务分支；
- 不把所有 Artifact 塞成聊天气泡。

#### 任务

回答“这个可信数据任务当前处于什么状态”：

- 目标、范围、所需交付物；
- Plan、依赖、Actor、Decision wait；
- Query/Action、Artifact、Evidence；
- 已发生/未发生副作用；
- 失败、恢复和历史 revision。

Task Detail 是结构化审查和恢复面，不替代 Conversation。

#### 交付

回答“组织已获得并可再次找到什么”：

- Report、Export 和重要 Artifact；
- 数据范围、来源、限制和 revision；
- HTML/PDF/PPTX 等确定性投影；
- 后续 Reusable Definition/Run History；
- 从交付回到 Task、Query、Evidence 和 Decision。

第一版生产可继续使用“报告”名称；只有出现报告之外的稳定交付类型后再升级为“交付”。

#### 数据资产

回答“Agent 依据什么理解和访问数据”：

- Datasource、Schema、Table、Field；
- Metric、Semantic Rule、Relationship、Grain；
- Data quality/freshness；
- Draft、Diff、Review、Publish、Rollback；
- Owner、有效期和冲突。

#### 治理与审计

回答“谁依据什么做了什么”：

- Decision inbox/history；
- Policy、Mandate 和例外；
- Query/Registry/Model/Report Action audit；
- Assurance、Evidence coverage 和失败样本；
- 当前 Runtime Governance Coverage。

在 M1/M3 前，生产只投影已有 Query Approval/Audit，不伪造通用 Decision Center。

#### Agents & Apps

回答“其他 Agent 如何安全使用 Forge”：

- 可见的 Agent Client/Application；
- Principal、Owner、Purpose、Mandate、Workspace 和 expiry；
- 允许的能力和资源范围；
- 调用示例、Task 请求和结构化返回；
- 最近调用、失败原因和人工接管；
- credential 只创建/轮换，不回显 Secret。

在 M1A 与 Agent Runtime MVP 前，生产不开放该入口；短期 Product Spine 也不显示 fixture Agent Client，避免把长期方向误认为已实现能力。

#### 管理

只保存部署级能力：

- Team/Workspace；
- Model/Skill；
- Channel/Database；
- System/readiness/diagnostics。

Pipeline、Session、Memory 和 Architecture 不进入普通日常导航。

## 5. Route 提案与兼容边界

| 产品面 | 建议生产 Route | 现有 Route/兼容 | 备注 |
|---|---|---|---|
| 工作台 | `/workspace` | `/admin/dashboard` 保留为系统诊断 | 登录默认页是否进入工作台或对话由角色门禁决定 |
| 对话 | `/chat`、后续 `/chat/{conversation_id}` | 已有 `/chat` | 不先新增第二个 Conversation Store |
| 任务 | `/tasks`、`/tasks/{task_run_id}` | 已有列表，缺 Detail route | Pi Task 为真相源 |
| 交付/报告 | `/reports`、`/reports/{report_id}` | 已有 detail，缺 scoped library | Report Store 为真相源 |
| 数据资产 | `/data/...` | 兼容 Registry/Schema/Metrics/Semantic/Staging | 聚合入口，不搬迁领域状态 |
| 治理与审计 | `/governance/...` | 兼容 audit/settings/query review | 按 Runtime 能力逐步开放 |
| Agents & Apps | `/runtime/...` | 外部仅 `/api/prepare-query` | M1A 前不开放执行能力 |
| 管理 | `/admin/...` | 兼容现有管理 Route | 诊断能力下沉二级入口 |

## 6. 核心产品 Journeys

### Journey H1：人通过 Conversation 完成连续数据任务

```text
提出问题
→ 判断 conversation / knowledge / query / workflow
→ 必要澄清语义、范围和交付物
→ 创建/关联 Task 与 Plan
→ SQL/Action Decision
→ 可信执行与 QueryResult
→ 解释结果、继续追问或有界补查
→ 生成交付
→ 回到 Conversation 或 Task
```

验收重点：连续性不依赖浏览器临时状态；追问不会静默丢失原 Task/Evidence；查询成功不提前结束任务。

### Journey H2：人只问知识，不执行查询

```text
询问指标、表、字段或组织规则
→ Forge 获取有权限的 Registry/Context Evidence
→ 返回已确认内容、作用域、版本和缺口
→ 若存在冲突，展示差异或进入 Steward Proposal
```

验收重点：不创建不必要 QueryRun；无证据时不编造确定答案。

### Journey A1：企业 Agent 请求可信数据事实

```text
Agent Client + Principal + Mandate
→ 提交 Data Task/Purpose/Deliverable
→ Policy 与 Registry Context
→ needs_input / waiting_decision / execution
→ 结构化 Artifact + Evidence + quality/limitations
→ Agent 消费或由 Human 接管
```

验收重点：Agent 不能用请求体扩大身份和 scope；没有 Mandate/Policy 时失败关闭；人可以在 Web 看见请求、Decision 和结果。

### Journey S1：Steward 处理语义缺口或冲突

```text
任务暴露未知/冲突口径
→ 创建 Claim/Rule Proposal
→ 查看来源、影响范围和相关任务
→ Steward 修订并作出发布 Decision
→ 发布 Registry revision
→ 原 Task 明确重规划或继续
```

验收重点：不静默覆盖旧定义；新 revision 不自动继承旧审批。

### Journey F1：证据不足时诚实失败

```text
缺少数据/权限/语义/Evidence
→ partial / needs_input / forbidden / blocked
→ 展示已知、未知、影响和最小下一步
→ 不生成“看起来完整”的结论或图表
```

验收重点：Safe Abstention 可见；失败不会被误报为完成。

## 7. 状态与语言 Contract

| 产品状态 | 含义 | 用户必须看到 |
|---|---|---|
| `loading` | 正在读取真相源 | 读取对象、可否安全重试 |
| `empty` | 当前没有对象 | 原因和唯一合理下一步 |
| `needs_input` | 缺少目标、语义或范围 | 缺少什么、为什么需要、谁补充 |
| `waiting_decision` | 精确 Action 等待有权主体 | Action、数据范围、风险、expiry、Decision Owner |
| `running` | 已授权步骤执行中 | 当前阶段、真实 elapsed/deadline、不伪造百分比 |
| `partial` | 有结果但交付或证据不完整 | 已知、缺失、影响和是否可采用 |
| `ready` | 对象可消费或可操作 | 来源、范围、主操作、限制 |
| `failed` | 阶段失败 | 已发生/未发生副作用、恢复路径 |
| `forbidden` | 身份或 Policy 拒绝 | 拒绝范围和合法申请路径，不泄漏资源存在性 |
| `offline` | 依赖不可用 | 受影响能力和安全重试边界 |
| `superseded` | 已被新 revision 替代 | 替代对象、时间和 lineage |

产品语言规则：

- 不使用“100% 正确”“绝对可信”或无作用域的“已验证”；
- “已确认”必须能说明确认者、范围、版本和时间；
- 分析正文按观察 → 有限判断 → 限制 → 待补 Evidence；
- Approval 表达为“批准执行该 Action”，不表达为“批准结论正确”；
- 内部 TaskRun、Artifact type、hash 和 Stage code 只在技术/审计视图按需出现。

## 8. 当前能力与产品缺口

| 能力 | 当前证据 | 产品判断 |
|---|---|---|
| 连续 Chat | `/chat`、ChannelEvent、Presentation、`conversation_id` | 有入口，无完整 Conversation product/read model 与跨 Task Context continuity |
| Task/Plan | TaskRun、ExecutionPlan、StageAttempt、Event | 执行底座较强，人的 Task Detail/Decision projection 不完整 |
| Query Trust | Registry、Assurance、hash Approval、QueryRun、只读执行 | 当前最成熟的 Trusted Data Runtime 切片 |
| Evidence/Artifact | QueryResult、Analysis、Report、Chart、Technical Report | 类型较强，Library、统一 Evidence navigation 和 Outcome 弱 |
| Reports | HTML/PDF/PPTX、分享、revision | 有交付能力；Reusable Definition 尚无 Runtime |
| Data Assets | Registry Studio、Schema/Metric/Semantic Draft/Revision | 有基础；Owner、Conflict、Policy、Quality/Freshness 不完整 |
| Agent API | 外部 `/api/prepare-query` 只准备不执行 | 正确安全边界，但远未达到 Agent Data Runtime |
| Runtime Governance | Contract Coverage 100% | Runtime Coverage 0%，不能开放企业 Agent 执行或多用户生产 |
| Decision | Query Approval | 通用 DecisionRequest/Record、职责分离和义务未实现 |
| Economics/Outcome | latency/attempt metadata | 无 Usage Ledger、Budget、Outcome acceptance 和 Cost per Trusted Outcome |

## 9. 重新排序后的阶段路线提案

> **2026-08-25 第二次短期顺序修订**：SP0–SP5 和完整 Product Shell 已完成产品基础，但真实产品价值尚未由目标用户重复使用证明。用户通过 `REQ-2026-08-25-023` 将近期顺序改为 `S0 Design Partner/Problem Baseline → S1 Direct Trusted Answer → S2 Semantic Learning Loop → S3 Three-Environment Validation → S4 Product Gate`。个人经营数据只作 Thin Founder Sandbox，Enterprise Reference 只作确定性门禁，Design Partner 承担主要产品证据；M1A/R1/G1/Q1/H6 均延后到 S4 后重评。

### Phase N2：产品设计闭合（已形成方向提案）

交付：本文、Product Map、术语、对象、Route、Journey、状态和阶段门禁。

退出条件：用户对三个产品面、核心对象、一级分组、Journeys 和阶段顺序给出 `PASS / CHANGE / REMOVE`。

### 原 Phase W3A.2：北极星驱动的隔离交互原型（不再作为近期主路径）

用户已否决继续由 fixture prototype 主导近期顺序。H1、H2、F1 和视觉门禁转入 SP3–SP5，并在底层 Projection/BFF 通过后连接真实能力；A1 Agent Runtime 与 S1 完整 Conflict/Proposal 分别延期到 M1A/R1 和 G1，不在短期页面中用 fixture 冒充。

### Phase W3B：生产 Human Control Plane（已拆入 SP1–SP5）

只连接当前已有真相源，保持单用户/私有部署边界：

- 统一 Product Shell；
- 恢复并加强 `/chat`；
- Conversation 只读索引/深链接方案；
- 可寻址 Task Detail；
- scoped Report Library；
- Data Assets 聚合入口；
- 当前 Query Approval/Audit projection。

不开放 Agents & Apps，不声称通用 Decision 或多用户治理已完成。

退出条件：真实 Human Golden Journeys H1/H2/F1 通过；刷新、恢复、跨页面和 Evidence 链不依赖第二状态源。

### Phase S0–S4：真实用户驱动的短期产品闭环（当前方向）

短期产品定义：

> **面向已有数据库/数仓的小型数据团队的可信业务问数助手；不要求先完成完整数据治理，在真实提问中逐步沉淀和复用业务语义。**

- **S0**：获得一个真实 Design Partner，固定一个 Domain、一个可查询数据源、一名语义 Owner、真实问题 corpus、隐私边界和现状人工流程基线。
- **S1**：让目标用户直接提问，只处理会改变结果的最小歧义，获得业务答案、数据表和限制；口径、SQL、数据范围和 Evidence 按需展开。
- **S2**：把用户纠正形成 task-local binding 或有来源的 Registry Proposal；Owner 发布后进入 Runtime lineage，后续任务安全复用，drift 时失败关闭。
- **S3**：同一 Runtime 分别通过 Design Partner 的真实重复使用、Enterprise Reference 的 Ground Truth/负向门禁和可选 Thin Founder Sandbox 的辅助交互验证。
- **S4**：以用户主动再次提问、语义真实复用、至少一次安全停止、完整 Evidence 回溯和可解释治理成本作为短期退出门禁。

Forge 当前从已有数据库/数仓开始，不建设外部内容、支付、广告和 SaaS 平台的通用 Connector/ETL。完整 Product Shell 保留为未来地图和已有入口，不再驱动近期功能扩张。详细实施边界以 [`forge-enterprise-evolution-plan.md`](forge-enterprise-evolution-plan.md) 为准。

### Phase M1A：Runtime Trust Foundation

实现 Service Identity、PrincipalContext、task-scoped DelegatedMandate、Datasource/Registry Binding、Policy Decision 和生产 Default Deny。

退出条件：Runtime Governance Coverage 对第一批 Agent Data Actions 从 0 提升为可证明覆盖；跨 Task/audience/scope/expiry/重放负向测试失败关闭。

### Phase R1：Agent Data Runtime MVP

只开放一个真实 Agent consumer 的最小闭环：

- 注册一个 Agent Client/Owner/Purpose；
- 受控提交 Data Task；
- 获取 Registry/Context Evidence；
- 进入 human clarification/Decision；
- 获取结构化 QueryResult/Analysis/Report reference；
- 查询状态、失败和 lineage；
- 不直接获得数据库凭证或通用 execute 权限。

现有 `/api/prepare-query` 继续兼容，不被放宽为执行 API。

退出条件：A1 真实 Agent Golden Journey 通过；Agent 和 Human Web 看到同一 Task/Decision/Artifact；无身份字段越权和第二任务状态。

### Phase G1：Data Trust Control Plane

深化数据团队核心工作：

- Metric/Relationship/Rule Owner；
- Claim/Conflict/Proposal；
- Quality/Freshness；
- Rule impact；
- Draft/Review/Publish/Rollback；
- Agent 使用与受影响 Task。

退出条件：S1 真实冲突 Journey 通过；发布 revision 后旧 Task/Approval 不被静默复用。

### Phase Q1：平台级 Quality 与 Assurance

- Quality Contract；
- Coverage/Clarification/Safe Abstention/Silent Error/Evidence Coverage/Human Override；
- Decision Evidence Package；
- Outcome acceptance；
- 失败样本到 Proposal。

退出条件：产品不再以单一 Accuracy 表示质量；能比较“回答更多”和“静默错误更少”的真实取舍。

### Phase H6：Reusable Deliverables

在身份、Binding 和 Assurance 稳定后实现：

- ReusableReportDefinition；
- SemanticQuerySpec；
- Criteria revision；
- manual rerun；
- QueryReuseDecision；
- immutable Run history。

不做自动调度、不继承旧审批。

### Phase M1B/M2/M3：按证据选择

- 出现第二个团队/Workspace：优先 M1B Membership/Policy/多 Binding；
- 成本成为主要阻力：优先 M2 Usage/Budget/Cost per Trusted Outcome；
- 合法多人审批和职责分离成为主要阻力：优先 M3 Participant/Decision；
- 不同时全面开工。

### Phase M5：第二场景与产品边界验证

选择一个非 SQL 但仍可验证的场景，例如运营信息分析，验证同一 Principal/Claim/Evidence/Policy/Decision/Action Contract。

退出条件决定：

1. 继续聚焦 Trusted Data Runtime；
2. 抽独立 Context/Memory Service；
3. 进入更广 Agent Trust Infrastructure。

没有第二消费者或不可替代价值时，否决通用化。

## 10. 第一版 W3A 的保留、修改与删除

### 保留

- 克制的桌面视觉和 design token；
- Workspace、身份上下文和全局 Shell；
- Task list/detail、SQL Review、Report detail、Data Assets、Admin 的基础组件；
- loading/empty/needs_input/waiting/failed/offline 状态样本；
- 无 CDN、无生产请求、键盘/深链接/viewport 自动门禁。

### 修改

- “新建任务”改回 Conversation 中的动作，而不是独立产品中心；
- 工作台从任务统计改为角色相关的待办、Decision、异常和交付；
- Task Detail 从阶段 tabs 堆叠改为 Data Task Contract、Plan、Decision、Evidence 和交付的结构化视图；
- “报告”评估是否只作为第一版名称，后续演进为交付；
- 管理入口拆分 Data Trust、Agent Access 和部署设置。

### 删除

- 用“分析工作台”概括整个产品；
- 一次性 brief form 代表核心任务入口；
- 只演示 Human Query 而不演示 Knowledge、Agent request、Conflict 和 Safe Abstention；
- 用前端 fixture 假装 Agent Client、Mandate、Decision Store 或 Runtime Governance 已实现。

## 11. 产品成功指标

### 当前近期指标

- Trusted Task Completion；
- Time to first useful Evidence；
- Clarification resolution；
- Query Approval/Execution consistency；
- Evidence Coverage；
- Safe Abstention 与 Silent Error；
- Human override/recovery；
- Report/Artifact acceptance；
- Registry reuse 与 conflict resolution。

### Agent Runtime 指标

- Agent-requested Task completion；
- Policy/Mandate deny 与合法恢复；
- Human takeover rate；
- Structured Artifact consumption success；
- duplicate/replay suppression；
- Agent task Cost per Trusted Outcome。

不把消息数、SQL 数、Agent 数、页面数或 Token 用量本身当作成功。

## 12. 关键风险与反证

| 风险 | 控制/证伪 |
|---|---|
| 产品面过多导致复杂 | SP4/SP5 先验证 Human Journey；不能回答当前角色任务的入口不进入短期 Shell |
| Chat 再次成为全部产品 | Conversation 与 Task/Decision/Evidence 分离；Task Detail 独立可寻址 |
| Control Plane 变成后台菜单集合 | 工作台以待办、Decision、异常和交付组织，不以系统模块组织 |
| Agent Runtime 只是换名 API | 必须有 Principal/Mandate/Policy、human wait、Artifact/Evidence 和真实 consumer |
| Forge 成为第二事实主库 | Source system 保留真相；Forge保存 Binding、Evidence、Claim、Decision 和 lineage |
| 100% Governed 被误读为正确 | UI 和指标分离 invariant、statistical quality 与 epistemic limits |
| 前端先行掩盖 Runtime Coverage=0 | Agents & Apps 在 M1A 前不进入生产；页面明确能力边界 |
| 路线过宽 | 每阶段只允许一个真实 Journey 与退出门禁；未通过不并行扩张 |

## 13. 本轮需要用户确认的决策

1. 是否同意“Human Work Surface + Shared Trust/Data Foundation + Agent-facing Runtime”三个产品面；
2. 是否同意产品核心对象是 Data Task Contract，而 Conversation 是人的主要连续交互面；
3. 是否同意导航按“工作 / 信任 / 接入 / 系统”分组；
4. 是否保留“报告”作为近期名称，等稳定出现多类交付后再升级为“交付”；
5. 是否同意 W3B 只先完成 Human Control Plane，Agent Runtime 必须等待 M1A；
6. 是否同意用真实 Agent Golden Journey 作为 R1 Agent Runtime 的产品门禁；
7. 是否同意 G1/Q1/H6 在 Agent Runtime MVP 后按上述顺序推进；
8. 对旧 W3A 各页面给出 `保留 / 修改 / 删除`。

`REQ-2026-08-25-020` 已确认完整未来 Product Shell，并以 capability-aware planned/blocked 边界完成前端实现；各 Runtime 阶段仍需独立需求评估、用户批准和真实 Golden Journey，不能由页面存在自动获得实施授权。
