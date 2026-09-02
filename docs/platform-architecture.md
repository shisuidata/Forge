# Forge AI 数据任务平台架构

> 状态：已确认目标架构，企业演进横向平面已纳入规划 · Last updated: 2026-08-25。本文描述 Pi、Forge、拾穗 DATA Skills 与渠道之间的长期边界，不表示所有模块均已实现。产品存在理由和方向边界见 [`product-north-star.md`](product-north-star.md)，主动实施计划见 [`forge-enterprise-evolution-plan.md`](forge-enterprise-evolution-plan.md)。
>
> 中文架构全景图：[`architecture-diagrams/forge-platform-architecture.html`](architecture-diagrams/forge-platform-architecture.html)，包含产品、技术、元数据、流程、闭环、部署、模型、安全、状态和接口等 11 个视角。

## 1. 产品定位

Forge 的目标产品形态是面向数据团队建设、供人和企业 Agent 共同使用的**可信数据运行时与数据任务控制/执行平台**。它不只回答“这条 SQL 怎么写”，还要为人和受委托 Agent 提供有来源、语义、权限、版本和 Evidence 的数据能力，并把一个模糊业务问题推进为可审核、可执行、可解释、可交付的数据任务。

整体能力由四层组成：

```text
Web / 飞书 / 钉钉
        │
        ▼
渠道适配层
· 身份映射 · 消息收发 · 卡片审批 · 结果渲染
        │
        ▼
Pi Agent Platform
· 会话 · 任务识别 · Skill 选择 · 编排 · 中断与恢复
        │
        ├──────────────────┐
        ▼                  ▼
拾穗 DATA Skills          Forge Trusted Execution
· 需求澄清                · Registry / Schema RAG
· 指标审查                · Forge JSON / Lint / Compiler
· 专项分析                · SQL 审核 / 只读执行
· 报告与表达              · ACL / Audit / Feedback
```

对外产品仍可统一使用 **Forge** 品牌。Pi 和拾穗 DATA Skills 是内部能力层，不要求最终用户理解其存在。

### 1.1 长期问题框架与当前边界

2026-08-24 确认采用第一性原理重新审视长期产品边界。长期研究问题不是预设“Data Agent”“统一记忆”或“Agent Control Plane”必然成立，而是：

> 一个概率性的机器，如何在不拥有最终责任能力的前提下，安全参与组织的认知、决策和行动？

由此识别四个相互约束的企业 AI 问题：

- **Governance**：Agent 依据什么、允许做什么；数据、知识、身份、Mandate、决策权和作用域如何治理。
- **Coordination**：多人、多 Agent 如何通过 Task、Artifact、Evidence、Decision 和 Action 协作，而不是依赖群聊形成隐式状态。
- **Economics**：如何控制模型、上下文、工具、重试和人工审核的总成本，优化每个可信结果的成本，而不是只优化 Token 单价。
- **Assurance**：如何证明结果正确、权限合法、行动与审批一致，并把结果、失败和修正转化为可追溯证据。

当前确认的是上述问题框架和产品公理，不是新的大范围实施承诺：

- Forge 当前对外定位继续是**可信 AI 数据任务平台**；结构化数据任务是高价值、高风险、可复算的第一验证场景。
- “跨 Agent 长期上下文连续性”是高可信需求；“所有信息进入单一统一记忆系统”仍是待验证假设。集中式 Memory Store、联邦式 Context Plane、Registry/Event Store 组合之间尚未定案。
- 长期架构研究可讨论 Agent Native、Data-Team Led、Business Accessible、Human Accountable，但不得据此让 Forge 吞并通用 Agent Runtime、全部业务真相源或无边界工具执行。
- 任何新基础设施必须先由第二个真实消费者、明确责任边界、可证伪指标和相对现有方案的不可替代价值证明，再进入实施计划。

稳定原则见 [`product-axioms.md`](product-axioms.md)，完整论证、反证和待验证假设见 [`ai-native-enterprise-thesis.md`](ai-native-enterprise-thesis.md)。

## 2. 核心原则

1. **Pi 负责任务，不负责数据真相。** Pi 管理会话、路由和工作流，但不持有数据库凭证，也不直接执行 SQL。
2. **Forge 负责可信执行。** 所有数据库访问、语义解析、SQL 编译、权限、审批和审计都经过 Forge。
3. **Skills 负责专业方法。** Skill 定义一类数据任务应收集什么、验证什么、如何分析以及怎样交付，不绕过 Forge 查询数据。
4. **渠道保持轻薄。** Web、飞书和钉钉只处理身份、交互和渲染，不复制业务流程。
5. **高风险操作必须审核。** SQL 执行、指标入库、组织知识提升等动作必须绑定明确的批准人和审计记录。
6. **阶段之间传递 Artifact。** Agent 与 Skill 不通过隐式自由文本耦合，关键阶段输出采用可版本化的结构化数据。
7. **一个任务贯穿一个追踪 ID。** 渠道、Pi、Skill 和 Forge 日志通过 `task_run_id` 与 `correlation_id` 串联。
8. **调度权与执行权分离。** Pi 决定任务何时进入哪个阶段；Forge 独立判断一项数据操作是否允许以及如何安全执行。
9. **Forge 不再自我编排完整任务。** Forge 可以在一次受控能力调用内部完成确定性子步骤，但不能自行选择下一个 Skill、发起补查、切换渠道或推进整个业务流程。

## 3. 控制面与执行面

目标架构采用明确的双层控制：

```text
Pi Control Plane
· 创建 TaskRun
· 选择 Workflow / Skill
· 调度 Stage 与绑定模型
· 执行 API Key / OAuth 模型推理
· 等待用户输入和审批
· 决定继续、暂停、回退或结束
             │ 受控 Tool / API 调用
             ▼
Forge Execution Plane
· 校验身份、权限和输入契约
· 签发有界 QueryPlanningEnvelope
· 验证候选 Forge JSON 并生成 QueryPlan
· Compile / Lint / Assurance
· 验证审批
· 只读执行
· 返回事实 Artifact 和审计记录
```

这意味着“基于 Pi 调度执行”不等于“把数据库执行搬进 Pi”：

- Pi 拥有**流程控制权**，没有数据库执行权。
- Forge 拥有**数据操作执行权和否决权**，没有完整任务调度权。
- 用户保留高风险操作的最终批准权。

任何需要多个阶段、等待用户、选择 Skill、补查、重试业务步骤或跨渠道恢复的工作，都由 Pi 管理。Forge 内部只保留一次能力调用所必需的有限重试，例如 Provider 瞬时错误重试或 Forge JSON 编译纠错；这类重试必须有上限、可审计，且不能越过审批边界。

### 3.1 企业演进的横向控制契约

现有四层职责不变，但后续企业化需要四个横向平面共同约束 Task 和 Action：

```text
业务人员 / Data Team / Enterprise Agent
                    │
          Identity & Delegation Boundary
                    │
                    ▼
              Pi Coordination
         Task · Plan · Participant · Decision
                    │
       ┌────────────┼─────────────┐
       ▼            ▼             ▼
 Governance       Economics      Context
 Principal        Budget         Source / Claim
 Mandate          Usage          Evidence / Conflict
 Policy           Outcome        ContextBundle
       └────────────┼─────────────┘
                    ▼
          Forge Trusted Execution
```

这些名称首先表示版本化 Contract 和职责，不预设立即拆成独立微服务：

- **Governance**：可信身份、Principal、task-scoped DelegatedMandate（Agent Mandate 是其 Agent 特化）、Membership、资源 Policy、Datasource/Registry Binding。
- **Coordination**：Task Participant、DecisionRequest/Record、ActionRef、依赖、责任和恢复，仍由 Pi 持有流程真相。
- **Economics**：Task/Stage/Agent/Team 的 Budget、Usage、CostCatalog 和 Outcome；成本策略不能绕过权限与 Assurance。
- **Context**：Source、Claim、Evidence、Conflict、MemoryProposal 和按 Purpose 编译的 ContextBundle；不把全部业务数据复制为第二真相源。
- **Assurance** 是跨平面约束：身份、Context、Decision、Budget、Registry、Model 和 Action lineage 必须可组合回放。

外部 IdP 或部署身份系统负责认证；Pi 对 Task 执行授权，Forge 对数据资源和实际 Action 独立执行授权。是否需要共享 PDP、独立 Context/Memory Service 或 Cost Service，必须由真实负载、第二消费者和安全边界证明后决定。

质量目标采用分层策略：安全/权限/审批一致等系统不变量和支持范围内的确定性投影追求 100% 回归；自然语言理解、SQL 语义和分析属于统计质量，必须同时测量 coverage、clarification、safe abstention 和 silent error；开放式推断必须暴露来源、假设、冲突和限制。Forge 承诺 `100% Governed` 的过程边界，不承诺开放世界端到端 `100% Correct`。

## 4. 四层职责

### 4.1 渠道层

渠道包括 Forge Web、飞书 Bot 和钉钉 Bot，负责：

- 将渠道身份映射为 `org_id / team_id / user_id`。
- 接收文本、文件、按钮和表单输入。
- 展示澄清问题、SQL、表格、图表和报告。
- Web 对话右侧可只读投影当前 Web Task 的最新 ExecutionPlan DAG、TaskEvent 与 StageAttempt 实时流；数据仍来自 Pi 真相源，页面不得自行推进或持久化状态。
- 收集确认、取消、修改、纠错和追问。
- 传递稳定的 `task_run_id`，支持跨渠道恢复同一个任务。

渠道层不负责：

- 选择业务指标口径。
- 拼接 prompt 或实现分析方法。
- 保存数据库凭证。
- 直接调用数据库。

### 4.2 Pi Agent Platform

Pi 是任务底座和 Agent Runtime，负责：

- 维护当前会话和短期上下文。
- 识别任务类型并选择 Skill 或 Workflow。
- 根据用户目标和交付物生成版本化执行计划，将复杂任务拆成有依赖关系的 PlanStep，并持续判断是否已完成全部交付物。
- 发起澄清、计划确认、暂停、恢复、回退、重规划和受控分支。
- 调用受控的 Forge Tools。
- 将上一步 Artifact 交给下一阶段。
- 记录 Stage 状态、耗时、失败和模型用量。
- 按 Stage 读取版本化 ActiveModelBinding，并在 StageAttempt 中固定 model revision。

生产运行时只暴露业务所需的自定义工具。默认的 `bash`、`write`、`edit` 和任意文件读取能力不进入客户运行环境。

Pi 不负责：

- 直接连接数据库。
- 绕过 Forge 签发的 QueryPlanningEnvelope、Forge JSON Contract、Compiler 或 Assurance 生成并执行任意 SQL。Pi 可在 OAuth 模型 Stage 中生成**不可信候选 Forge JSON**，但候选只有经 Forge 独立验证后才能成为 QueryPlan。
- 将未经确认的对话内容提升为组织事实。
- 成为 Registry、审计日志或长期业务记忆的真相源。

### 4.3 Forge Trusted Execution

Forge 是唯一可信执行层，负责：

- 数据源连接与只读账号管理。
- Schema 同步、Registry、业务上下文和 ACL。
- 拥有自然语言到 Forge JSON 的规划契约、Registry Context 裁剪和最终接受权。API-key 模型可以在 Forge 内生成候选；Pi OAuth 模型可以消费 Forge 签发的有界 `QueryPlanningEnvelope` 并通过终止型 Tool 提交不可信候选，但不得读取数据库，也不得自行把候选解释为可执行计划。
- 统一 Query Assurance Pipeline：Contract、Registry/ACL、Scope/Type、Convention Policy、Compiler 和 SQL Safety Gate；无论候选由 Forge 内部模型还是 Pi OAuth 模型生成，都进入同一 Pipeline。
- 每次准备输出版本化 `QueryAssuranceReport`；所有入口复用同一服务，不允许各自散落调用 Schema/Lint/Compiler。
- 生成待审核 SQL，并确保 Assurance Report、审核 SQL hash 与实际执行内容一致。
- 查询超时、结果行数上限、敏感数据和表权限控制。
- QueryRun、审批、执行、Audit、Feedback 与回放。
- Forge 查询规划模型的 ModelProfile 验证、热切换、revision 固定与审计。
- 结构层 Canonical Schema、版本、差异、草案审核与回滚。
- 从同一 Canonical Schema 确定性投影表格、DDL、ER 图和 JSON 视图。

Forge 返回事实型 Artifact，不承担所有业务分析和写作方法。

结构层的多视图不是多真相源：数据库 introspection、DDL import、表格编辑和 ER 关系编辑都只能形成 `RegistryDraft`，经 Schema 校验、确定性 diff 和人工审核后发布为 `RegistryRevision`。DDL 编辑默认只改变 Registry 草案，不直接向数据库执行 migration。ER 中根据命名推断的关系必须保持 `inferred/unconfirmed`，不能冒充真实外键。

### 4.4 拾穗 DATA Skills

拾穗 DATA Skills 是专业方法和交付能力层，负责：

- 需求澄清与验收标准。
- 指标定义和口径审查。
- 归因、漏斗、留存、EDA、A/B 等分析方法。
- 数据质量、血缘、表设计、SQL Review 等工程方法。
- 分析报告、PPT、日报周报和数据文档等表达交付。

Skill 输出必须区分：

- 已知事实。
- 合理推断。
- 未验证假设。
- 待确认问题。
- 建议动作。

需要数据库证据的 Skill 必须通过 Pi 调用 Forge，不允许自己访问数据源。生产 Runtime 对固定 Skills 包中实际存在的 20 个 Skill 使用显式 allowlist，每个 Stage 只注入一个 Skill；四个核心流程使用专用 Artifact，其余 Skill 使用有界 `AdvisoryArtifact`。组织管理员通过独立凭证和版本化 Team Policy 控制可用 Skill，普通任务不能修改 Policy。

## 5. 任务与 Artifact 模型

### 5.1 TaskRun

`TaskRun` 是跨层任务的顶级标识，由 Pi 平台创建和编排：

```json
{
  "task_run_id": "tr_01...",
  "org_id": "org_...",
  "team_id": "team_...",
  "user_id": "user_...",
  "channel": "web",
  "intent": "business_root_cause_analysis",
  "status": "waiting_for_query_approval",
  "current_stage": "query_review",
  "created_at": "...",
  "updated_at": "..."
}
```

建议状态：

```text
created
→ clarifying
→ planning
→ waiting_for_plan_approval（按策略可跳过）
→ ready_for_query
→ waiting_for_query_approval
→ querying
→ analyzing
→ visualizing（按计划可跳过）
→ rendering
→ completed
```

异常和控制状态：

```text
needs_input / incomplete / cancelled / failed / expired
```

### 5.2 核心 Artifact

| Artifact | 生产者 | 主要消费者 | 用途 |
|---|---|---|---|
| `IntentArtifact` | Pi | Skill Router | 任务类型、目标、初始约束 |
| `ClarificationArtifact` | 需求澄清 Skill | Pi、用户 | 已知信息、缺口、验收标准 |
| `ExecutionPlanArtifact` | Pi Plan Runtime | Pi、用户、渠道 | 目标交付物、PlanStep、依赖、审批点、完成条件与计划版本 |
| `MetricDefinitionArtifact` | 指标审查 Skill | Forge、用户 | 指标公式、粒度、窗口、边界 |
| `QueryPlanArtifact` | Forge | Pi、渠道 | Forge JSON、SQL、方言、Registry 版本 |
| `ReviewRequestArtifact` | Forge | 渠道、用户 | 审批对象、SQL hash、过期时间 |
| `QueryResultArtifact` | Forge | 分析 Skill | 列、行、口径、执行元数据 |
| `AnalysisArtifact` | 分析 Skill | 报告/图表 Skill | 证据、洞察、假设、建议、缺口 |
| `AdvisoryArtifact` | 扩展专业 Skill | Pi、用户、渠道 | 有界发现、证据引用、建议、假设、限制、待确认问题和交付物 |
| `ChartArtifact` | 可视化能力 | 渠道、报告 Skill | 图表规格、数据引用、标注 |
| `RenderedOutputArtifact` | 输出 Skill | 渠道 | Markdown、Web、飞书或钉钉表示 |

所有 Artifact 至少包含：

```json
{
  "artifact_id": "ar_...",
  "artifact_type": "analysis",
  "schema_version": 1,
  "task_run_id": "tr_...",
  "producer": "business-root-cause-analysis",
  "created_at": "...",
  "payload": {}
}
```

Artifact 采用 Schema-on-Read，并通过 `schema_version` 保证向后兼容。原始 Artifact 不原地覆盖；修改和重跑产生新版本，保留来源关系。

### 5.2 可复用报告与语义查询资产

跨天/月更新的报告不把旧 SQL 或 Prompt 当复用真相源。目标对象分为：

```text
ReusableReportDefinition（版本化方法）
  ├── SemanticQuerySpec（稳定 metric/dimension/grain/filter/time/relationship ID）
  ├── RegistryBindingSet（semantic ID → 当前物理 Schema）
  ├── JudgementCriteria（版本化目标/阈值/基线）
  ├── Chart Story / Skill / Delivery policy
  └── Parameters
             ↓ Pi 创建新 TaskRun
Forge QueryReuseDecision
  ├── reuse_compiled_sql
  ├── rebind_and_recompile
  ├── replan_from_semantics
  └── blocked_needs_input
             ↓ Assurance + 必要审批
ReportRun（不可变证据快照）
```

职责边界：

- Pi 编排 Definition Draft 确认、手动 rerun、等待输入和 Run 对比；不判断 SQL 是否仍兼容。
- Forge 持有 SemanticQuerySpec 的查询契约、Registry Binding compatibility、Forge JSON/Compiler/Assurance 和 QueryReuseDecision；旧 SQL 只是 `CompiledQuerySnapshot`，不是未来执行授权。
- Web 提供“保存为可复用报告 / 用最新数据更新 / 调整判断标准”、Definition Library 和 Run History，只投影 Pi/Forge 真相源。
- 每次 Run 固定 definition/semantic/binding/criteria/skill/model/registry/policy/sql lineage。`CompiledQuerySnapshot` 保存参数化 SQL、parameter schema、非敏感 bound parameter snapshot、dialect 与 hash；Secret parameter 只保留受控 SecretRef，不写入 Artifact 明文。数据更新产生新 Run；标准更新先产生新 Definition/Criteria revision；历史 Report revision 永不原地重写。
- 只有物理 binding 和所有当前 Gate 可证明兼容时才允许复用 compiled SQL，且仍重新校验当前 Authorization/Safety；参数化 SQL 负责安全替换 literal 与复现，但不能处理 Schema drift。Schema drift 时优先按 stable semantic ID rebind/recompile，语义冲突时失败关闭或重新规划并人工 review。
- 自动调度和免逐次审批需要独立 Budget/Owner/Policy/DelegatedMandate，不由“保存模板”隐式获得。

## 6. 查询审批与信任边界

查询执行必须满足：

```text
用户问题
→ Forge 生成 QueryPlan
→ Forge 生成 ReviewRequest
→ 渠道展示 SQL
→ 用户确认
→ Forge 验证批准信息
→ Forge 使用只读连接执行
→ 返回 QueryResult
```

审批记录至少绑定：

```text
query_run_id
approver_user_id
org_id / team_id
datasource_id
sql_hash
registry_version
approved_at
expires_at
```

以下任一变化都使原审批失效：

- SQL 内容变化。
- 数据源变化。
- 用户或团队上下文变化。
- Registry 版本变化且影响查询计划。
- 审批过期。

Pi 只能提交计划和转交批准结果，不能自行伪造批准，也不能持有 Forge 数据库凭证。

## 7. 身份、权限与租户

渠道身份先进入统一身份映射：

```text
web session / feishu open_id / dingtalk user_id
                    ↓
          org_id + team_id + user_id
                    ↓
       Pi Task Context + Forge ACL Context
```

规则：

- 渠道提供的 `user_id` 不能直接作为可信权限依据。
- Forge 必须校验服务身份和最终用户身份。
- Pi 的服务凭证只允许调用受限业务 API，不代表数据库超级用户。
- 表、字段、数据源和 Registry 的可见性由 Forge 根据租户上下文裁决。

## 8. 状态与记忆归属

| 数据 | 真相源 | 说明 |
|---|---|---|
| ModelProfile Revision、OAuth Auth Slot 与 ActiveBinding | Platform Model Control Plane + Pi Auth Store | API Key 只保存 secret_ref；OAuth refresh/access token 只保存在 Pi 专用 mode-600 `auth.json`，Profile 仅引用 provider/auth slot，不复制 token。切换使用 CAS，新任务生效，在途任务固定旧 revision |
| 当前对话和推理上下文 | Pi Session | 可压缩、可过期，不作为组织事实 |
| TaskRun 与 Stage 状态 | Pi Task Store | 支持暂停、恢复和渠道切换 |
| QueryRun、SQL、审批、执行 | Forge | 可信查询审计真相源 |
| 结构层 Canonical Schema、DDL/ER 投影、Revision/Draft | Forge Registry | Canonical Schema 是唯一真相源；DDL/ER/表格/JSON 只做确定性投影和受控草案编辑 |
| ER 布局 | Forge Registry UI Metadata | 只保存坐标、分组和视图偏好，不改变表、字段或关系事实 |
| 指标、歧义、字段约定 | Forge Registry | 需要版本、审核和回滚 |
| 组织业务上下文 | Forge Registry / SMP | 确认后才能提升为正式知识 |
| Skill 定义与测试 | 拾穗 DATA 仓库 | 独立版本管理和发布门禁 |
| 最终报告和图表 | Artifact Store | 关联 TaskRun 与数据来源 |

Pi Session 不替代 Forge EMS/SMP/Registry。跨会话长期知识必须经过候选、确认和入库流程。

## 9. 标准业务流程

以“最近两周新用户首购转化为什么下降”为例：

1. 渠道创建用户消息并完成身份映射。
2. Pi 创建 `TaskRun`，选择需求澄清 Skill。
3. Skill 生成 `ClarificationArtifact`；缺少口径时向用户提问。
4. Pi 根据已确认目标生成 `ExecutionPlanArtifact`。例如“包含可视化图表的报告”必须显式包含取数、分析、图表和报告步骤及其依赖，不能在 QueryResult 后误判任务完成。
5. 指标审查 Skill 生成 `MetricDefinitionArtifact`。
6. Pi 调度已就绪的查询 PlanStep。API-key Planner 可直接调用 Forge `prepare_query`；OAuth Planner 先向 Forge 获取短期、有界、绑定身份与 Registry revision 的 `QueryPlanningEnvelope`，在隔离 Pi Session 中通过唯一 Tool 生成候选 Forge JSON，再提交 Forge。
7. Forge 对两类候选统一执行 Contract、Registry/ACL、Compiler、Lint 与 Assurance；只有通过后才返回 QueryPlan 和 ReviewRequest。
8. 渠道展示 SQL，用户确认或修改需求。
9. Forge 验证 `sql_hash`、身份和权限后执行。
10. Forge 返回带口径和执行元数据的 QueryResult；Pi 将查询 PlanStep 标记完成并自动解锁依赖它的分析步骤。
11. 归因分析 Skill 生成证据、假设树和补查建议；可视化步骤基于已确认 QueryResult/Analysis 生成 `ChartArtifact`。
12. 若需补查，创建关联 QueryRun，继续经过审批；不得后台无限循环，必要时生成新 Plan revision。
13. 报告 Skill 消费 Analysis 与 Chart Artifact，生成渠道无关的 RenderedOutput。
14. Pi 只有在计划声明的所有必需交付物均存在且通过契约校验后，才能把 TaskRun 标记为 `completed`。
15. Web、飞书或钉钉适配器展示计划进度、当前审批点和最终交付物。

## 10. 渠道无关输出

Skill 先生成语义结果，不直接拼装具体渠道组件：

```json
{
  "title": "新用户首购转化下降分析",
  "summary": "...",
  "sections": [],
  "tables": [],
  "charts": [],
  "actions": [
    {"type": "approve_query", "query_run_id": "qr_..."}
  ]
}
```

随后由 Renderer 转换为：

- Web 页面或流式 Chat 组件。
- 飞书互动卡片和长文。
- 钉钉互动卡片和 Markdown。

渠道能力不一致时允许降级，但不能改变事实、审批对象和查询状态。

### 10.1 飞书即配即用与一次性身份引导

个人部署的飞书设置页采用“保存即启动”：服务端验证 App 凭证后写入 mode-600 配置，受管 WebSocket Runtime 热重载，无需重启 Forge API。Forge Adapter 与 Pi 的 Channel Service Key 由部署层预连，设置页不得显示或返回密钥。

未知渠道身份仍默认失败关闭。个人部署可由管理员显式开启一次性首用户绑定：仅当飞书 Identity Map 中不存在任何用户、事件已通过 Adapter Service Key 鉴权且事件是 `p2p` 私聊 message 时，Pi 才能将首个 `open_id` 原子绑定到预设 org/team/user；绑定完成后 bootstrap 永久关闭，后续未知用户继续拒绝。该机制不能扩展为 wildcard 身份，也不能绕过 SQL 审批或 Forge Assurance。

渠道 ingress 不把每条消息都解释为查询任务，也不把“不需要 SQL”解释为拒绝。Pi 将消息路由为 `query / knowledge / conversation / forbidden`：明确取数进入 QueryRun；指标口径、Schema、语义规则和组织知识问题通过 Forge 的受控只读 Context API 获取有界 evidence，由 Pi 生成证据绑定回答；问候和正常对话直接响应；仅明确超出产品能力、身份未授权或权限策略命中时拒绝。渠道不得自行读取 Registry、Knowledge 或生成 SQL，任何查询仍由 Forge 独立 Assurance 和审批。

### 10.1 分析简报与报告发布边界

飞书分析卡与完整报告是两个交付层：飞书只展示用户可理解的方法摘要、结论、限制和下一步；模型 hidden chain-of-thought、Prompt、Tool transcript、内部 ID/hash/path/Secret 永不进入渠道。可复现性由结构化 DecisionLog、SQL、审批记录、执行摘要和版本 lineage 提供，不以保存私有思维链实现。

Pi 拥有 ReportJob 的编排状态、Attempt、lease、幂等和 Artifact 依赖；Forge Web/受限 Report Service 负责报告 ACL、Canonical HTML 投影、分享交换和 PDF/PPTX 下载，不访问数据库、不生成 SQL。业务报告、技术报告、Chart 和 Publication 都固定到同一不可变 Report Bundle revision。大文件进入受限 Artifact Store，SQLite 只保存状态与索引。业务分享与 technical scope 分离，外部链接默认只能看到业务报告，分享 token 只保存 hash并支持过期、撤销和下载审计。飞书 Adapter 只消费 presentation 与最终 URL，不承载报告生成逻辑。

### 10.2 分阶段模型控制

模型不是全局可变单例。Model Control Plane 以 Task Stage 为 scope 管理不可变 Profile Revision、Active Binding、CAS、Audit 与回滚；Pi 在 StageAttempt 开始时固定对应 revision，在途任务不跟随热切换。Profile 明确区分 `api_key` 与 `pi_oauth` execution backend：前者由 Secret Ref 解析，后者只引用 Pi Auth Store 的 provider/auth slot，token 不进入 Model Control DB、Artifact 或日志。所有 9 个 Stage 都可绑定 OAuth Provider；非查询 Stage 直接使用终止型 Artifact Tool，SQL Critical Stage 只能使用 Forge 签发的 PlanningEnvelope 和候选提交 Tool，最终 QueryPlan、SQL 与执行权仍属于 Forge。

`metric_definition/query_generation/query_repair` 是 SQL Critical scope。完整 Accuracy/Assurance/Retry/P95/Timeout 质量门禁由管理员持久化开关控制；无论开关状态，Provider、凭证、协议、Tool Calling 与 Structured Output compatibility gate 始终必需。开关打开时固定 Runtime/Registry/Assurance/Policy lineage 并 fail-closed；关闭时必须审计为 compatibility-only，不能伪称已做完整质量保证。OAuth token 失效、订阅额度耗尽、Provider 限流、binding 漂移或 Forge Assurance 失败均不得降级绕过审批。分析、报告等非核心 Stage 只通过能力与 Artifact 安全门禁，且永远不获得 Forge SQL 执行权。

## 11. 可观测性

每次任务应能按 `task_run_id` 查看：

- 原始用户问题和渠道。
- 选择了哪些 Skill 及其版本。
- 每个 Stage 的输入和输出 Artifact。
- Forge QueryRun、SQL hash、审批人和执行状态。
- 模型、Token、耗时和失败原因。
- 用户反馈和后续修正规则。

日志不得记录数据库密码、模型 API Key、渠道密钥或未经授权的完整敏感结果集。

Web 后台是跨渠道只读观测面：它从同一个 Pi Store 按已认证管理员的 `org_id + team_id` scope 列出 Web、飞书、钉钉和 API TaskRun，并增量读取 TaskEvent 与 StageAttempt。浏览器不能自行指定或扩大 scope；查看任务不会推进状态、批准 SQL 或重放 Attempt。渠道展示继续保持最小披露，后台日志也不保存或展示 hidden chain-of-thought 和 Secret。

Web `/chat` 可复用同一观察能力提供当前任务右侧视图，但权限更窄：只允许当前 Web 用户所属 scope 中 `channel=web` 的 Task。Forge Web 服务端聚合最新有效 `ExecutionPlanArtifact`、增量 TaskEvent 和有界 StageAttempt 字段，浏览器根据 `depends_on` 绘制业务 DAG 并按 sequence 单调追加任务流。该视图是只读 projection，不新增 Web 状态库；切换 Task 只切换观察焦点，不改变 Pi TaskRun。

## 12. 当前 Forge 职责迁移映射

目标不是在现有 Forge Pipeline 外再套一层 Pi，而是消除重复调度。当前模块应按下表演进：

| 当前模块/能力 | 目标归属 | 处理方式 |
|---|---|---|
| `agent/pipeline.py` Pipeline 路由与 Stage 推进 | Pi | 在 Pi Task Runtime 稳定后迁移并停止作为主编排器 |
| `agent/agent.py` 通用对话循环、pending state | Pi + Forge QueryRun | 对话和任务状态归 Pi；查询准备、审批状态改为 Forge QueryRun |
| `agent/llm.py` Registry 注入与 Forge JSON 生成 | Forge Contract + 可插拔 Model Backend | Forge 保留 PlanningEnvelope、Context、Contract 与接受权；API-key backend 可进程内生成，Pi OAuth backend 只提交不可信候选，统一进入 Forge Assurance |
| `agent/prompts.py` 通用分析/表达 prompt | 拾穗 DATA Skills | 逐步替换为版本化 Skill 和 Artifact Schema |
| `agent/memory` EMS/WMB 会话状态 | Pi | 当前会话、断点和工作记忆迁移到 Pi Task/Session |
| `agent/memory` 已确认业务知识 | Forge Registry/SMP | 保留正式知识，但写入必须经过候选和确认 |
| `agent/knowledge.py` 文档/RSS/URL 收集流程 | Pi | Pi 调度收集与审核；Forge 接收确认后的知识候选 |
| `agent/feishu.py` Bot 对话流程 | 渠道适配层 + Pi | Bot 只做消息与卡片，任务推进由 Pi 完成 |
| `agent/tenant.py` 用户到团队映射 | 身份层 | 渠道完成身份解析；Forge仍独立执行 ACL 校验 |
| `forge/retriever.py`、Registry | Forge | 保留；结构层升级为版本化 Canonical Schema，表格/DDL/ER/JSON 均从它投影 |
| `forge/compiler.py`、`forge/lint.py` | Forge | 保留 |
| `forge/executor.py` | Forge | 保留，且仍是唯一数据库执行入口 |
| `agent/audit.py` 查询审计 | Forge | 保留并增加 `task_run_id` 关联 |
| 图表和报告 Stage 调度 | Pi | Pi 选择 Skill 和顺序；确定性渲染器可以作为受控工具 |
| Web `/api/chat` 任务入口 | Pi | 渐进切换到 Pi Task API |
| Forge `/api/prepare-query` | Forge | 保持查询能力 API，不承担任务编排 |

迁移完成后的硬约束：

- Forge 不根据关键词自行选择 `query / analyze / visualize / report` Pipeline。
- Forge 不在分析结果不足时自行发起下一次业务查询。
- Forge 不直接向飞书、钉钉推进多轮任务。
- Pi 不复制 Registry 检索、Forge JSON 业务契约、Compiler、Lint 或 Executor；OAuth Session 只能消费 Forge 签发的有界 Context，并通过 Forge 定义的 Tool 生成不可信候选。
- 同一职责在目标架构中只能有一个主实现；旧实现只允许作为有明确下线时间的兼容路径。

## 13. 当前实现基础

当前已有基础：

- Forge `agent/pipeline.py`：Pipeline、Stage、Artifact 和断点状态雏形。
- Forge `/api/prepare-query`：外部 Agent 生成待审核 SQL的安全边界。
- Forge Web `/api/chat`、`/api/approve`：内部查询与审批路径。
- Forge Registry、Compiler、Executor、Audit、Feedback 和 Memory。
- 拾穗 DATA 的正式 Skills、示例、测试用例和发布门禁。
- Pi 的 Skills、Extensions、SDK 和 RPC 能力。

目标架构需要新增：

- 独立、受限的 Pi Orchestrator Runtime。
- 稳定的 TaskRun 与 Artifact Contract。
- Pi 到 Forge 的 QueryRun 级审批协议。
- 拾穗 DATA Skills 的 Pi Package 发布方式。
- Web、飞书、钉钉共享的渠道适配接口。

迁移期间不删除 Forge 当前查询主链；新链路先以受控垂直切片并行验证，达到验收标准后再逐步替换硬编码的分析和报告 Stage。
