# AI Native 企业：治理、协同、成本、保障与长期上下文论证

> 状态：战略论证资产，不是已承诺路线图 · Last updated: 2026-08-24
>
> 本文沉淀 2026-08-24 围绕 Data Agent、AI Native 组织、人—Agent 共识、统一记忆和企业 AI Infra 的完整讨论。它同时保留支持理由、反证、风险与待验证假设，避免把探索性判断误写成既定事实。稳定约束见 [`product-axioms.md`](product-axioms.md)，已确认目标架构见 [`platform-architecture.md`](platform-architecture.md)。

## 1. 结论分层

### 已确认的问题框架

1. 企业采用 Agent 后，治理、协同、成本和结果保障会相互耦合。
2. 模型不能凭自身获得组织 Authority；Agent 不能承担最终组织责任。
3. 人和机器不需要认知完全一致，但行动前必须形成有范围、可验证、可授权的一致性。
4. Task、Artifact、Evidence、Decision 和 Action 比聊天记录更适合作为 AI Native 协同真相源。
5. 跨模型、跨 Agent、跨应用的长期上下文连续性是高可信需求。
6. 数据任务是验证上述问题的良好入口，但不证明 Forge 必然成为通用 AI Infra。

### 高可信、仍需验证的产品假设

1. 企业会需要面向人和 Agent 的 Context Governance Plane。
2. Agent 将成为新的受委托安全主体，需要 Identity、Mandate、Delegation 和 Lifecycle。
3. 企业会从“每一步人工审批”转向风险分级、Policy Approval 和异常人工接管。
4. 模型单价可能下降，但多 Agent、长上下文、评测、重试和人工审核将推高 AI 总支出。
5. 企业最终会优化每个可信 Outcome 的总成本，而不只是 Token 单价。

### 尚未证明的假设

1. 所有对话、事实和信息应进入单一统一 Memory Store。
2. Forge 应直接承担完整的企业 Agent Control Plane。
3. 多 Agent 默认优于单 Agent。
4. 企业会脱离现有责任、预算和权力结构。
5. 一个通用 Agent 会取代专业 Agent 和确定性系统。

## 2. Data Agent 是入口还是终局

如果 Data Agent 只表示：

```text
自然语言 → 模型写 SQL → 返回结果
```

它的长期价值有限。模型增强和现有数据库/BI 产品内置能力会持续压缩这一功能的独立空间。

如果 Data Agent 表示：

> 把人的业务意图转化为经过组织语义、权限、证据和责任约束的数据行动。

命题更可能成立。它处理的不只是 SQL，而是：

- 指标和业务概念如何定义。
- 数据来自哪里、是否新鲜、能否复算。
- 谁可以查看、审批和执行。
- 哪些是假设、推断和限制。
- 结论如何绑定证据。
- 错误如何沉淀为规则、评测和组织知识。

因此 Data Agent 更适合作为高价值切入点，而不是预设终局类别。

## 3. 数据不只包括结构化业务数据

企业 Agent 使用的信息至少包括六类：

| 类型 | 示例 | 可信属性 |
|---|---|---|
| 结构化事实 | 订单、库存、财务、行为 | 可查询、可复算、强权限 |
| 组织规则 | 指标、客户分级、审批政策 | 由组织确认，不是自然真理 |
| 运营信息 | 项目延期、客户风险、负责人变化 | 强时效、强作用域 |
| 外部证据 | 新闻、政策、研报、竞品 | 必须保留来源、时间和引用 |
| 分析推断 | 根因假设、趋势判断 | 必须标记 confidence、evidence 和 limitations |
| 决策承诺 | 预算调整、任务负责人、批准行动 | 代表组织行动和责任，不等于事实 |

扩展“数据”不意味着把所有内容放进同一数据库。更合理的对象是带类型、来源、时间、作用域和权限的**组织证据**。

## 4. AI Native 协同不是人与 Agent 一起群聊

### 4.1 传统隐式协同为什么失效

人类组织长期依靠隐性修复运行：惯例、关系、上下文、试探和未写入系统的权力结构。AI 会把模糊指令快速补全为明确行动，从而放大歧义：

```text
模糊意图
→ Agent 静默补全假设
→ 生成流畅结果
→ 多系统传播
→ 错误获得事实外观
```

AI 越能行动，组织越需要把隐性规则转化为可验证契约。

### 4.2 不追求完全共识

组织本就不依靠所有人认知一致，而依靠：

- 哪些事实被采用。
- 谁拥有决定权。
- 谁可以提出异议。
- 谁承诺下一步。
- 谁承担最终后果。

人—Agent 的目标应是特定任务上的**可执行一致性**：目标、语义、证据、权限、预算、审批、停止条件和责任明确。

### 4.3 新协同对象

AI Native 协同应围绕以下对象：

```text
Intent
→ Assumption / Claim
→ Evidence
→ Policy
→ Plan
→ Decision / Approval
→ Action
→ Outcome
→ Learning Proposal
```

对话只是交互层；结构化对象和工作图才是可恢复、可审计的协同状态。

### 4.4 标准闭环

```text
提案：Agent 暴露目标理解、假设、风险和所需权限
→ 质疑：人和 Agent 只处理冲突、差异和证据缺口
→ 授权：合法 Decision Owner 对精确 Action 授权
→ 执行：Agent 在 Mandate、预算和停止条件内行动
→ 学习：系统生成知识候选，由有权主体确认是否提升作用域
```

Agent 可以提出组织知识，但不能自行宣布组织真相。

## 5. 产品不面向单一 Persona，而面向协作关系

### 5.1 四种角色不能混为一个用户

| 责任 | 可能参与者 |
|---|---|
| 提出需求 | 业务人员、业务系统、业务 Agent |
| 定义规则 | Data Steward、数据 Owner、业务 Owner |
| 执行任务 | Agent、确定性工具、数据系统 |
| 承担责任 | Human、Team、Organization Principal |

### 5.2 当前采用与长期架构

较稳健的框架是：

```text
GTM：Data-Team Led
体验：Business Accessible
架构：Agent Native
治理：Human Accountable
```

- 数据团队是当前最自然的购买和治理入口。
- 业务人员通常只想获得可信结论和行动，不一定进入独立产品 UI。
- 长期运行时调用者可能主要是企业 Agent。
- Agent 无论多自主，都必须代表明确 Principal，并受 Mandate 约束。

### 5.3 可能出现的新职责

`Agent Steward / AI Data Steward` 可能成为新职责，负责 Agent Mandate、数据权限、质量评测、模型/Skill/Policy、冲突处理和知识提升。产品不应要求客户先创建全新岗位；该职责可以先由数据平台、AI 平台、治理或企业架构团队承担。

## 6. 长期上下文与统一记忆命题

### 6.1 支持理由

当前记忆分散在不同模型、Agent、渠道和应用中，导致：

- 同一信息有多个不一致版本。
- 用户难以查看、纠正和删除。
- 更换模型或 Agent 后上下文丢失。
- Agent 之间无法安全复用决策与经验。
- 企业无法统一控制跨团队信息流动。

因此以下判断较强：

> 记忆应属于 Person/Team/Organization Principal，而不应属于某个模型或单个 Agent。

### 6.2 不能直接推出单一统一存储

“Agent 需要跨任务上下文”并不等于“所有信息必须集中保存”。单一 Memory Store 会产生：

- 超级敏感数据中心和单点泄露。
- 多作用域信息的错误关联。
- 员工监控与隐私风险。
- Agent 错误写入污染所有消费者。
- 与 CRM、ERP、数仓、Git 和文档系统争夺真相源。
- 服务故障导致所有 Agent 同时失忆。

企业也不存在完全统一现实：销售、财务、法务、管理层和一线经验可能对同一问题保持不同 Claim。系统应保存冲突和作用域，而不是强行覆盖。

### 6.3 写入比检索更困难

自动从对话提取记忆会遇到：

- 临时表达是否是长期偏好。
- 玩笑、反讽和探索性讨论如何识别。
- 观点变化后如何 supersede。
- 两个 Agent 提取冲突结论怎么办。
- 错误记忆被反复召回后如何阻断复利污染。

因此对话首先是 Event；提取结果首先是 Claim/Proposal，不是 Fact。

### 6.4 更稳健的候选架构

```text
CRM / ERP / 数仓 / Git / 文档 / Web / 对话事件
                       │
                       ▼
              Context Governance Plane
· Identity · Purpose · Policy · Provenance · Scope
· Claim · Conflict · Decision · Context Assembly
                       │
                       ▼
               有界 Context Bundle
                       │
                       ▼
                    Agent
```

集中管理的“小而重要”核心可包括：

- 身份和明确授权的稳定偏好。
- 决策与承诺。
- Agent Mandate。
- 跨系统 Evidence lineage。
- 经确认的组织语义。

其余事实优先保留原始真相源并按需访问。

### 6.5 Context 不等于 Memory

- Memory 是跨任务保留的信息资产。
- Context 是某次任务实际提供给 Agent 的最小充分信息。

Context Assembly 必须绑定 Principal、Agent Mandate、Purpose、Task、权限、新鲜度、冲突和 Token 预算。Agent 不应直接遍历整个记忆库。

## 7. 企业 AI 的四个相互约束问题

### 7.1 Governance：Agent 依据什么、允许做什么

治理范围从数据治理扩展为：

- Data Governance：结构、指标、质量、血缘、权限。
- Knowledge Governance：来源、时效、冲突、作用域和正式性。
- Agent Governance：身份、Mandate、Tool、Delegation、预算和生命周期。
- Decision Governance：Owner、Approval、例外、责任和重新评估。

治理应进入任务运行时，而不是只存在于后台文档。

### 7.2 Coordination：多人、多 Agent 如何完成任务

多 Agent 不天然优于单 Agent。新增 Agent 会带来重复上下文、状态冲突、错误传播、协调延迟和 Token 成本。只有职责分离、权限隔离、专业上下文或独立保障确有必要时才增加 Actor。

Work Graph 节点至少需要：

```text
owner / actor / input / output / status / authority
dependencies / budget / deadline / evidence
```

人负责目标、价值、权力和最终责任；Agent 负责结构化、检索、验证、执行和维护状态。

### 7.3 Economics：AI 工作方式是否可持续

单 Token 价格可能下降，但企业总 AI 支出很可能上升：

```text
模型更便宜
→ 使用场景增加
→ 上下文变长
→ Agent 数量和调用轮次增加
→ 评测、重试和审核增加
→ 总成本上升
```

总成本应包括：

```text
Model Inference
+ Embedding / Rerank
+ Tool / Database Compute
+ Storage / Network
+ Human Review
+ Latency Cost
+ Expected Error Loss
```

企业应优化 `Cost per Trusted Outcome`，而不是 `Cost per Million Tokens`。便宜模型如果造成更多重试、人工检查和错误，最终可能更贵。

每个 Task 需要 Budget Contract：模型成本、Token、调用次数、重试、时长、工具成本、人工审核和质量底线。能确定性完成的工作不应长期调用模型；已有 Artifact 应优先复用。

### 7.4 Assurance：结果是否正确、合法、可信和值得

没有结果保障，企业无法判断：

- 治理是否改善了质量。
- 多 Agent 是否优于单 Agent。
- 更贵模型是否值得。
- 任务完成是否产生业务价值。

Assurance 需要 Evidence、Evaluation、Approval、Policy Compliance、Audit、Outcome Feedback 和 Regression，并贯穿治理、协同和成本决策。

## 8. 高概率未来情景

### 8.1 Agent Sprawl 与 Shadow AI（很高）

企业会出现员工自建、部门采购、SaaS 内置和官方平台 Agent。需要 Agent Registry、Owner、Mandate、评测、上线、监控、权限收缩、停用和注销。

### 8.2 Agent 成为新安全主体（很高）

传统 RBAC 难以表达任务目的、动态委托、预算和子 Agent。身份将演进为 Principal、Mandate、Task、Policy 和 Delegation Chain。

### 8.3 人类注意力成为瓶颈（很高）

逐项审批会导致 Approval Fatigue。协同将向低风险自动执行、中风险抽样、高风险审批和异常人工接管演进。

### 8.4 AI 合成信息污染组织知识（很高）

AI 报告、摘要和会议纪要可能循环引用。未来重点不是简单标注“AI 生成”，而是能否回到独立原始 Evidence，并识别自引用。

### 8.5 模型商品化、组织上下文保持稀缺（高）

企业会混用不同模型。可持续资产是 Registry、Policy、Artifact、Decision、评测集、失败样本和 Evidence，而不是单一 Prompt 或 Provider。

### 8.6 Agent 错误升级为系统性错误（高）

错误事实可能经过分析、决策和自动执行形成反馈闭环。需要 Sandbox、Dry-run、Impact Preview、Reversible Action、Circuit Breaker、Kill Switch 和补偿机制。

### 8.7 SaaS 逐步退到 Agent 交互层之后（中高）

员工可能更多通过 Agent 调用 CRM、数仓和项目系统。现有系统仍是记录、交易和责任真相源，关键接口会转向 Tool API、Policy、Artifact 和 Event。

### 8.8 Agent 互操作不止 Tool 调用（中高）

即使 Tool Protocol 兼容，也可能丢失身份、Mandate、Evidence、审批和错误语义。未来可能需要更高层的 Task、Identity、Artifact、Approval 和 Cost Contract。

### 8.9 Agent 之间争夺资源（中高）

模型额度、数据库计算、API 限额、人工审批和业务系统写锁会成为共享资源，需要优先级、配额、预算预留、并发限制和公平策略。

### 8.10 组织激励滞后于技术能力（很高）

数据不共享、Owner 不愿担责、员工担心替代、团队推卸 AI 错误责任都可能阻断落地。产品不能假设客户先完成组织重构。

### 8.11 合规转向过程证据（高，行业相关）

企业需要证明模型、数据、上下文、Policy、Approval、Action 和 Outcome。Audit 会从日志演进为 `AI Decision Evidence Package`。

### 8.12 跨企业 Agent 协作（中等、更长期）

采购、销售、银行、物流和客服 Agent 可能跨组织协作，但身份、合同、最小披露和争议解决尚不成熟，不应作为当前主路线假设。

## 9. 不应高概率押注的叙事

- 一个通用 Agent 取代所有专业 Agent。
- 企业完全脱离现有责任、预算和权力结构。
- 人完全退出高风险审批与最终决策。
- 所有企业信息进入一个统一物理 Memory Store。
- 模型足够强后治理不再重要。
- 多 Agent 数量越多，效果越好。

## 10. Forge 的可能位置与边界

### 当前可验证定位

> 面向数据团队的可信 AI 数据任务平台。

### 长期技术研究位置

> 人—Agent 协作网络中的组织上下文、可信数据执行与行动保障层。

### 已有基础

| 问题 | Forge / Pi 当前基础 |
|---|---|
| Governance | Registry、ACL、Model/Skill Policy、Query Assurance |
| Coordination | TaskRun、ExecutionPlan、Artifact、StageAttempt、ChannelEvent |
| Economics | Stage Model Binding、Timeout、Retry；完整成本账本尚缺 |
| Assurance | Evidence、SQL hash Approval、Audit、Quality Gate、Report lineage |

### 不能据此越过的边界

- Pi 继续是唯一主 Orchestrator。
- Forge 继续是可信数据执行层，不获得无边界跨系统执行权。
- 正式业务数据仍留在各自真相源。
- 未确认 Memory/Context 方案前，不把 EMS/SMP/Registry/Task State 合并成单一存储。
- 通用基础设施在没有第二个真实消费者前只定义 Contract，不急于拆服务。

## 11. 待验证问题与证伪标准

### 11.1 企业是否需要统一 Context Governance Plane

需要证明：相比数据仓库、企业搜索、RAG、Agent Runtime 和现有 IAM 的组合，它显著降低跨 Agent 上下文错误、权限风险或集成成本。

### 11.2 独立 Memory Service 是否必要

需要至少两个真实 Agent/应用消费者，并比较：

- 集中 Memory Store。
- 联邦 Context Broker。
- Registry + Event Store + Source Adapter 组合。

关注召回精度、过期和冲突率、权限泄露、删除传播、延迟、运维成本和用户纠正成本。

### 11.3 多 Agent 是否创造净收益

相同任务对比单 Agent、最小职责分离和多 Agent，测量可信完成率、Token、工具调用、延迟、人工审核和错误传播。

### 11.4 成本能否与可信 Outcome 关联

不仅记录 Provider 账单，还要证明 Task/Stage/Agent/Team 成本归因能改善模型路由、预算控制或业务决策。

### 11.5 数据场景机制能否跨域复用

至少选择第二个非 SQL 场景，例如运营信息分析或市场情报，验证同一 Principal、Claim、Evidence、Policy、Decision、Action Contract 是否仍成立；不能只因概念相似就宣布通用化。

## 12. 后续讨论与实施纪律

1. 先使用 [`product-axioms.md`](product-axioms.md) 审查新方向。
2. 区分已确认原则、高可信假设和探索性叙事。
3. 新方向影响 Pi/Forge/Skills/渠道职责时，先更新架构与实施计划。
4. 不用宏大概念替代真实用户、责任主体、付费者和失败场景。
5. 不因“AI Native”假设客户先重构组织。
6. 不以功能数量证明基础设施必要性；优先寻找可证伪的第二场景和第二消费者。
