# Forge 产品北极星

> 状态：已确认的长期产品指导 · Last updated: 2026-09-03
>
> 本文沉淀 2026-08-21 至 2026-08-25 的关键产品讨论，用于回答 Forge 为什么存在、服务谁、提供什么，以及如何处理正确性、事实、分歧、权力和责任。它不是营销文案、功能清单或实施计划。
>
> 稳定公理见 [`product-axioms.md`](product-axioms.md)，完整论证和反证见 [`ai-native-enterprise-thesis.md`](ai-native-enterprise-thesis.md)，职责边界见 [`platform-architecture.md`](platform-architecture.md)，当前唯一实施计划见 [`forge-enterprise-evolution-plan.md`](forge-enterprise-evolution-plan.md)。若实现与本文冲突，先暂停并重新评估，不通过兼容层或页面文案掩盖冲突。

## 1. 北极星命题

> **Forge 的目标不是让 AI 给出更多答案，也不是承诺开放世界 100% 正确，而是让人和被授权的企业 Agent，在明确的语义、来源、权限、版本和责任边界内，取得可复算的数据事实与证据，并把模糊意图推进为可审核、可执行、可追溯的数据任务。**

对应的产品定义是：

> **Forge 是面向企业 Data Agent 的开源可信数据运行时，并以真实消费者证据逐步演进为企业可信数据平台。**

近期入口是开发者可以独立采用的 `Evaluate → Enforce → Explain` Trust Runtime；Human Control Plane、Chat、飞书、钉钉、API 或 MCP 都只是消费者和渠道，不是产品边界。结构化数据访问是第一验证场景，也不是预设终局。

## 2. Forge 为什么存在

### 2.1 模型能力不能产生组织事实

模型可以生成、归纳、检索和推断，但不能凭自身决定：

- 一个业务指标采用什么定义；
- 哪个来源在当前范围内有效；
- 谁可以访问什么数据；
- 哪个冲突观点应被组织采用；
- 一项行动是否值得承担风险；
- 谁对结果和后果负责。

模型越流畅，未经验证的错误越容易获得事实外观。Forge 要解决的不是“让模型永远正确”，而是阻止概率输出绕过组织语义、权限、证据和责任直接成为行动依据。

### 2.2 企业 Agent 需要可信数据能力，而不只是 SQL

其他 Agent 真正需要的不是一段 SQL，也不是一个没有来源的答案，而是：

```text
问题采用了什么语义
数据来自哪个正式来源和快照
当前 Principal 是否有权使用
执行了什么查询或确定性变换
结果是否截断、过期或存在冲突
哪些是事实、推断、限制和未知
谁批准了实际 Action
如何复算、回放和纠正
```

因此 Forge 的长期价值不以“模型写 SQL 更聪明”为中心，而以**可信数据能力能否被人和其他 Agent 安全复用**为中心。

### 2.3 组织资产比模型更持久

模型和 Agent Runtime 会变化。可持续资产应沉淀在：

- Registry 与稳定语义 ID；
- Datasource/Registry/Policy Binding；
- Task、Artifact、Evidence、Decision 和 Action lineage；
- Assurance、评测集、失败样本和修正规则；
- 经确认的组织知识和作用域；
- 可复现的报告与查询快照。

Prompt、单一 Provider 和隐藏会话状态不能成为组织长期价值的唯一载体。

## 3. 产品定位分层

必须同时保留现实切入、中期产品和长期假设，不能相互冒充。

### 3.1 现实切入：Open-source Trust Runtime for Data Agents

近期第一用户是建设或维护内部 Data Agent 的 Data/AI Engineer、数据架构师和数据平台团队。Forge 不要求他们迁移到另一套问数 UI，而是接入既有 Agent 的数据访问路径：

- **Evaluate**：对模型、Prompt、RAG、语义和 Agent 版本执行可复现评测、Exact Result Comparison、失败分层和回归门禁；
- **Enforce**：在运行时绑定 Principal、Purpose、Task、Policy 和 Resource Scope，执行只读、安全、审批和 drift 检查；
- **Explain**：返回结果及其语义、数据源/快照、实际 SQL、版本、限制、Policy/Assurance、Approval、Evidence 和 lineage。

Direct SQL、Forge JSON 与后续 Semantic Query 是可替换输入。Forge JSON 是 Planner Adapter，不是产品身份；Forge 的稳定价值位于生成后的验证、可信执行、Evidence 和 Audit。自然语言问数本身会持续商品化，不能单独构成长久边界。

### 3.2 中期产品：Trusted Data Runtime

Forge 将可信数据能力安全开放给：

- 业务人员；
- 数据分析师和数据团队；
- 企业 Copilot；
- 经营、分析、报告和工作流 Agent；
- 其他持有合法 Mandate 的服务。

中期必须形成两类产品面：

```text
面向人的 Control Plane
· 对话 · 任务 · 审批 · 交付
· Registry · Policy · Audit · Model Control

面向 Agent 的 Data Runtime
· resolve semantic context
· prepare governed data task
· request query / approval
· retrieve evidence
· consume trusted result
· submit feedback / knowledge proposal
```

Agent-facing Runtime 不是裸 SQL API，也不是一个超级 Token。每次调用必须绑定 `Principal + DelegatedMandate + Task + Purpose + Policy + Resource Scope`。

### 3.3 长期假设：Enterprise AI Trust Infrastructure

Forge 可能进一步验证组织 Context、Trust 与 Action Contract 能否跨越 SQL 场景。但以下方向尚未被证明：

- Forge 成为完整通用企业 AI Infra；
- 所有信息进入一个统一 Memory Store；
- Forge 替代 CRM、ERP、数仓、Git、文档或身份系统；
- 多 Agent 默认优于最小 Actor 或确定性流程；
- Agent 获得无边界跨系统执行权。

没有第二个非 SQL 场景、第二个真实消费者和可测的不可替代价值，不进入通用化实施。

## 4. 不追求开放世界 100% 正确

### 4.1 为什么理论上不可普遍实现

端到端结果经过：

```text
现实世界
→ 数据采集
→ 数据建模
→ 业务定义
→ 用户表达
→ Agent 理解
→ 查询与变换
→ 分析
→ 决策
```

各层错误性质不同：

- 数据只能记录可观察到的现实，天然有损；
- 迟到、退款、补录和回填会改变历史结果；
- 业务语义是组织约定，不是唯一自然真理；
- 自然语言存在不可消除的歧义；
- 分析相关性不能自动成为因果；
- 开放问题不存在有限测试集可以覆盖的完整分布。

固定问题、Schema、Registry、模型和代码上的 100% 只是一份有边界的回归证据，不是对未知客户和未知问题的无限承诺。

### 4.2 四类质量必须分开

| 类型 | 目标 | 例子 |
|---|---|---|
| Governance/System Invariant | 严格保证，负向失败关闭 | 权限、租户隔离、审批与执行一致、幂等、不盲目重放 |
| Deterministic Correctness | 支持范围内 100% 回归 | Compiler、Schema 校验、hash、数值/报告确定性投影 |
| Statistical Quality | 测量与持续改善，不作绝对保证 | 意图理解、表选择、SQL 语义、分析质量 |
| Epistemic Uncertainty | 明确来源、冲突和限制 | 外部信息、趋势判断、根因假设、因果解释 |

因此：

```text
100% Governed
≠
100% Correct
```

`Contract Coverage=100%` 也不等于 `Runtime Governance Coverage=100%`。Contract 完整只能证明语言和边界已定义，只有生产 PEP 真正执行并通过负向测试，才能声明运行时覆盖。

### 4.3 正确的质量指标

不能只报告 Accuracy。至少同时观察：

- Coverage：多少请求产生了有效结果；
- Conditional Accuracy：已回答请求中的正确率；
- Clarification Rate：多少请求在关键歧义处暂停澄清；
- Safe Abstention：多少高风险或证据不足请求被正确停止；
- Silent Error：多少错误未被发现却以确定语气输出；
- Evidence Coverage：关键结论是否有可回放证据；
- Human Override：有权主体修正了什么；
- Outcome Acceptance：结果是否被实际接受和使用。

Forge 优先降低 **Silent Error**，而不是靠拒绝全部问题制造虚假的高准确率。

### 4.4 Forge 可以诚实承诺什么

1. **能确定性保证的，严格保证。**
2. **不能保证的，持续测量并保留失败证据。**
3. **证据不足的，澄清、暂停、降级或拒绝。**
4. **不确定的，明确标记来源、假设、冲突和限制。**
5. **风险越高，语义、Evidence、审批和对账门槛越高。**

## 5. 共识不是所有参与者想法一致

### 5.1 人人共识不可作为前提

组织本来就存在：

- 信息不对称；
- 部门目标和口径差异；
- 风险偏好差异；
- 多个同时成立但作用域不同的 Claim；
- 稳定责任组织与动态任务组织的差异。

系统不应静默选一个观点、覆盖其他版本，或用单一摘要制造虚假共识。

### 5.2 人机共识不是让 Agent 获得 Authority

Agent 不拥有法律、财务、组织和价值责任。它可以提出、检索、验证、建议和执行，但不能自行决定组织事实、权限和最终行动。

正确目标是一次任务内的**合法、可验证、可执行一致性**：

```text
目标明确
+ 语义明确
+ Evidence 有界
+ 权限合法
+ 假设与冲突可见
+ Decision authority 明确
+ Action 精确绑定
+ 预算与停止条件明确
+ 责任可追溯
```

### 5.3 标准协同闭环

```text
提案
→ 质疑
→ 授权
→ 执行
→ 学习提案
```

- **提案**：Agent 暴露目标理解、假设、风险、证据缺口和所需权限；
- **质疑**：人和其他 Agent 处理冲突、差异、例外和证据不足；
- **授权**：合法 Decision Owner 对精确 Action 作出有界决定；
- **执行**：Agent 在 Mandate、Policy、预算和停止条件内行动；
- **学习提案**：任务结果形成知识或规则候选，由有权主体确认是否提升作用域。

AI 可以提出组织知识，但不能自行宣布组织真相。

## 6. Forge 提供什么样的“数据事实”

### 6.1 信息必须分型

| 对象 | 含义 |
|---|---|
| Source | 数据库、文档、消息、网页或业务系统等原始来源 |
| Event | 实际发生过的消息、调用、操作和状态变化 |
| Claim | 人或 Agent 对世界提出的主张，未必为真 |
| Evidence | 支持或反驳 Claim 的可追溯依据 |
| Knowledge | 在明确作用域内，经合法流程确认和版本化的信息 |
| Decision | 有决策权的 Principal 对明确对象作出的选择 |
| Action | 人或机器实际执行的操作 |

一句对话只能证明这句话发生过；模型输出首先是 Claim；查询结果是特定数据源、快照、语义和执行条件下的 Evidence。它们不能因被系统保存就自动成为普遍事实。

### 6.2 面向 Agent 的可信数据结果

其他 Agent 消费的数据能力应至少携带：

```text
value / rows / artifact reference
semantic definition and grain
datasource and snapshot/freshness
registry / policy / model / execution revision
principal and authorized scope
query or deterministic transform lineage
truncation / quality status
claim, confidence and limitations
conflicts and missing evidence
approval / decision when required
```

Forge 提供的是**有边界的数据事实与证据**，不是脱离来源、时间和作用域的“唯一现实”。

### 6.3 不建立第二套业务真相源

| 信息 | 主要真相源 |
|---|---|
| 订单、库存、财务、行为 | 数据库或数仓 |
| 客户和交易状态 | CRM/ERP 等业务系统 |
| 文档和代码 | 文档系统、Git、对象存储 |
| 当前会话与任务状态 | Pi Session / Task Store |
| 正式数据结构和业务语义 | Forge Registry |
| 查询、审批和执行 | Forge QueryRun / Audit |
| 报告与分析交付 | 不可变 Artifact / Report Store |
| 身份认证 | 企业 IdP 或部署身份系统 |

Forge 统一的是可信访问、语义绑定、Policy、Assurance、Evidence 和 Action 协议，而不是复制并取代全部原始系统。

## 7. 服务对象与责任模型

Forge 不是围绕单一 Persona 设计，而是服务一个由人和 Agent 组成的协作关系。

| 参与者 | 主要责任 |
|---|---|
| Human/Agent Requester | 提出目标、背景和所需交付物 |
| Data Steward / Owner | 定义语义、关系、质量和作用域 |
| Human Approver / Decision Owner | 对有风险的精确 Action 授权 |
| Agent/Service Executor | 在有界 Mandate 内执行任务 |
| Auditor / Challenger | 检查 Evidence、Policy、Decision 和 Outcome |
| Human/Team/Organization Principal | 承担最终责任 |

采用原则保持：

```text
GTM：Data-Team Led
体验：Business Accessible
架构：Agent Native
治理：Human Accountable
```

Agent 的有效权限不是一个裸 `agent_id` 或 API Key，而是：

```text
Principal
+ DelegatedMandate
+ Task
+ Purpose
+ Policy
+ Resource Scope
+ Expiry
```

## 8. 产品核心对象不是 Chat 或 SQL

### 8.1 Conversation 是连续交互面

Conversation 负责：

- 让人连续表达、澄清和追问；
- 在同一主题中查看进展和交付；
- 承接 `conversation / knowledge / query / action / workflow`；
- 连接 Web、飞书、钉钉和其他渠道。

Conversation 不负责：

- 保存最终 Task 状态；
- 建立权限；
- 将对话自动提升为事实；
- 替代审批、Evidence 和 Artifact lineage。

### 8.2 Data Task Contract 是执行核心

任何人或 Agent 的请求进入执行层后，都必须能够回答：

```text
谁提出、代表谁
要解决什么，交付什么
采用什么语义、范围和时间
需要哪些数据与 Context
允许访问和执行什么
有哪些假设、冲突和风险
谁拥有决定权
由谁或哪个 Agent 执行
产生了哪些 Evidence 和 Artifact
是否满足完成条件
结果是否形成新的知识候选
```

一个 Conversation 可以包含多个相关 Task；一个 Task 可以经历多个 QueryRun、Decision 和 Artifact revision。Conversation 提供人的连续性，Task 提供机器可恢复的执行承诺。

### 8.3 查询成功不等于任务完成

任务完成由用户要求的交付物决定：

```text
查询
→ 结果
→ 分析
→ 必要补查
→ 图表
→ 报告
→ Decision / Action（若在范围内）
```

只有 ExecutionPlan 声明的必需交付物存在并通过 Contract，Task 才能完成。

## 9. 架构责任边界

```text
Web / 飞书 / 钉钉 / API
          │
          ▼
Pi Agent Platform
· Conversation · Task · Plan · Stage · Decision wait
          │
   ┌──────┴───────────┐
   ▼                  ▼
拾穗 DATA Skills     Forge Trusted Data Runtime
· 专业方法           · Registry / Context binding
· 分析与交付         · Query planning / Assurance
· 结构化 Artifact    · Approval / Read-only execution
                     · Evidence / Audit / Feedback
```

硬边界：

- Pi 是唯一主 Orchestrator，不持有数据库执行权；
- Forge 是可信数据执行层并保留独立否决权，不编排完整业务任务；
- Skills 定义专业方法，不绕过 Forge 访问数据；
- 渠道只做身份映射、交互和投影，不复制状态机；
- Human/Team/Organization 持有最终 Authority；
- 高风险副作用不自动重放。

## 10. 四个长期问题必须共同闭环

| 平面 | 必须回答的问题 |
|---|---|
| Governance | 依据什么、代表谁、允许做什么 |
| Coordination | 人和 Agent 如何形成可执行任务、Decision 和 Handoff |
| Economics | 为可信结果可以花多少，何时停止 |
| Assurance | 结果是否正确、合法、可追溯和值得采用 |

任何一个平面单独完成都不足以产生可信 Action：

- 有治理但无 Assurance，流程合规仍可能得出错误结论；
- 有协同但无权力边界，Agent 网络会放大错误；
- 有质量但无成本，无法规模化；
- 有成本优化但绕过 Evidence 和权限，会降低可信度。

长期优化目标是：

> **在质量、权限和风险约束下，降低每个可信 Outcome 的总成本。**

## 11. 产品体验如何投影北极星

本文不提前冻结具体一级导航，但所有 Web/渠道设计必须覆盖以下产品面。

### 11.1 连续交互面

- Chat/Conversation 是一等入口；
- 支持知识、查询、澄清、任务、分析、规则提案和报告；
- 后续追问必须继承有界 Context，不能伪装成无上下文新任务；
- 不把页面线程当作权限或任务真相源。

### 11.2 任务与决策面

- 显示目标、Plan、状态、阻断、风险和下一步；
- SQL、Registry 发布、报告分享等审批必须绑定精确对象；
- Task Detail 承担结构化查看、恢复、审计和深链接，不替代 Conversation；
- 未实现能力明确 disabled，不出现看起来成功的假按钮。

### 11.3 交付与证据面

- 查询结果、分析、图表和报告都有稳定 Artifact；
- 业务正文区分观察、有限判断、限制和待补证据；
- 用户能从结论回到来源、范围、时间和复算路径；
- 报告 revision 不原地修改。

### 11.4 数据与治理面

- 数据团队可以维护结构、指标、语义、关系、Policy 和质量；
- Draft、Diff、Review、Publish、Rollback 和 Owner 可见；
- 冲突和过期不被静默隐藏；
- Governance 必须进入运行时，不只是后台文档。

### 11.5 Agent Runtime 面

- 其他 Agent 通过受控 API/Tool 请求能力；
- 返回机器可消费的结构化 Artifact 和 Evidence；
- 不要求 Agent 解析 Web 页面或自由文本报告；
- Agent 调用也必须经过同一身份、语义、Assurance 和审批边界。

## 12. 正向飞轮

Forge 的学习不应是“模型自动记住一切”，而是：

```text
真实任务
→ 发现歧义、错误或证据缺口
→ 形成 Registry / Policy / Test / Knowledge Proposal
→ Steward 审核
→ 发布新 revision
→ 后续任务在明确作用域内复用
→ 用 Outcome 和失败样本继续验证
```

飞轮资产属于组织，不属于单个模型。错误反馈不能未经确认直接污染全局知识。

## 13. 当前非目标

- 不承诺所有数据问题 100% 正确；
- 不把有限 benchmark 100% 宣称为真实世界 100%；
- 不把 Chat、SQL 编辑器、Dashboard 或单个 Agent 当成完整产品；
- 不建设第二套 Task/Artifact/Approval 真相源；
- 不让 Agent 从 Prompt 或用户文字自行推断权限；
- 不让模型自由决定组织语义、事实和最终行动；
- 不把全部对话自动写成长期知识；
- 不复制所有业务数据建设统一事实主库；
- 不默认增加大量 Agent 或无限循环补查；
- 不在第二场景和第二消费者出现前扩张为通用 AI Infra；
- 不以更现代、更多功能或更漂亮页面替代真实可信闭环。

## 14. 新需求和产品设计审查

每项重要方向必须回答：

1. 它让哪个人或 Agent 更容易获得可信数据事实或完成可信数据任务？
2. 它解决的是交互、协调、执行、治理还是保障问题？
3. 哪个对象是真相源，是否制造第二份可写状态？
4. 哪些内容是 Source、Claim、Evidence、Knowledge、Decision 和 Action？
5. 谁拥有事实、权限、预算和最终责任？
6. 统计能力失败时是否会产生 Silent Error？
7. 能否澄清、停止、回滚、重放审计和诚实失败？
8. 它是否让其他 Agent 获得结构化、可授权、可复算的能力，而不是更多无来源文本？
9. 是否减少每个可信 Outcome 的总成本？
10. 如果声称为通用基础设施，第二个真实消费者和不可替代证据是什么？

任何需求如果只能增加输出数量、页面数量、Agent 数量或营销叙事，却不能改善上述问题，应延期或拒绝。

## 15. 需要持续验证的假设

- 企业 Agent 是否会成为 Forge 的主要运行时调用者；
- 数据团队是否愿意以 Forge 把可信数据能力开放给业务人员和 Agent；
- 结构化数据场景的 Contract 能否复用于运营信息或市场情报；
- Agent-facing Data Runtime 相比数据库、BI、语义层和 Agent Runtime 组合是否具有不可替代价值；
- 可执行一致性是否真实降低澄清、审批、错误和交接成本；
- `Cost per Trusted Outcome` 能否被可靠测量并指导模型与流程选择；
- 联邦 Context、Registry/Event Store 组合是否已经足够，独立 Memory Service 是否必要。

假设被否证时应收缩产品边界，不为维护宏大叙事继续堆叠基础设施。

## 16. 文档关系与决策来源

### 16.1 文档职责

| 文档 | 职责 |
|---|---|
| 本文 | 产品北极星、定位、正确性与共识边界 |
| [`product-axioms.md`](product-axioms.md) | 不可轻易违反的稳定公理 |
| [`ai-native-enterprise-thesis.md`](ai-native-enterprise-thesis.md) | 完整论证、反证、未来情景和待验证假设 |
| [`product-direction-architecture-review-2026-08-24.md`](product-direction-architecture-review-2026-08-24.md) | 当前代码与产品方向的证据化复审 |
| [`product-design-roadmap-2026-08-25.md`](product-design-roadmap-2026-08-25.md) | 基于北极星重建的产品面、对象、信息架构与长期阶段方向 |
| [`short-term-product-spine-plan-2026-08-25.md`](short-term-product-spine-plan-2026-08-25.md) | 先底层 Product Projection/BFF、后真实 Web Shell 的近期实施计划 |
| [`platform-architecture.md`](platform-architecture.md) | Pi、Forge、Skills、渠道和真相源职责 |
| [`forge-enterprise-evolution-plan.md`](forge-enterprise-evolution-plan.md) | 唯一主动实施计划和阶段门禁 |
| [`requirements-pool.md`](requirements-pool.md) | 所有需求、评估、决策和状态历史 |

### 16.2 关键讨论时间线

- **2026-08-21**：确认 Pi / Forge / 拾穗 DATA Skills / 渠道四层职责；查询完成不等于任务完成；需求需要多轮澄清和 Plan。
- **2026-08-22**：确认入口需处理 conversation、knowledge、query，并向 action/workflow 和 deliverable-driven Plan 演进；Chat 与 Task 监控分离；报告成为不可变交付物。
- **2026-08-23**：讨论 Data Agent 是否成立、人—Agent/人—人共识、企业 Agent 作为长期调用者、数据范围扩展、统一记忆反证以及 100% 准确率边界。
- **2026-08-24**：沉淀产品公理、四平面、企业架构复审、Governance Contract、Golden Journey 和 Product Shell 人工门禁。
- **2026-08-25**：重新核对近期 Pi Session，确认“可信数据运行时 + 可执行一致性 + 非 100% 正确承诺”为产品北极星。

相关 Pi Session：`01a0235d-b7fb-735f-a472-88f7e3641da7`、`01a02507-63e6-7873-822e-4e64698d116e`、`01a02eb4-d672-7839-bdea-22849c6c7d56`。

## 17. 最终判断

Forge 的长期价值不在于替企业生成一个永远正确的答案，而在于建立一条可信边界：

```text
模糊意图
→ 显式语义与假设
→ 有界 Context 与权限
→ 可审核计划与精确 Decision
→ Forge 可信数据执行
→ 可复算 Evidence 与 Artifact
→ 人和 Agent 在明确责任内继续行动
```

当系统不能证明一项结果时，它必须诚实失败；当组织存在分歧时，它必须保留作用域、冲突和决策权；当其他 Agent 需要数据时，它必须提供可追溯的事实与证据，而不是一段看似确定的文本。

这就是 Forge 后续产品、架构、Web、API、Skills、治理和商业决策共同遵循的方向。
