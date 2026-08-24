# Forge 产品方向与架构复审（2026-08-24）

> 状态：评审完成，产品方向与四平面框架已确认 · Review basis: [`product-axioms.md`](product-axioms.md)
>
> 本文以 Governance、Coordination、Economics、Assurance 四个问题平面和 32 条产品公理重新审核当前产品方向、目标架构与实际代码。评审结论已确认并纳入 [`platform-architecture.md`](platform-architecture.md)；当前唯一主动实施计划是 [`forge-enterprise-evolution-plan.md`](forge-enterprise-evolution-plan.md)。本文保留评审依据，不再作为待确认计划。

## 1. 执行摘要

### 1.1 核心判断

Forge 当前不是一个普通 Text-to-SQL 原型，也还不是完整企业 AI Infra。它已经形成一个较强的**可信数据执行纵向切片**：

```text
模糊数据意图
→ 结构化 Artifact
→ Registry / Policy / Assurance
→ SQL hash 审批
→ 只读执行
→ Evidence-bound 分析与报告
→ Audit / Feedback / Model Gate
```

这条纵向链路符合第一性原则，并构成真实差异化。

当前主要问题不是查询功能不足，而是四个横向平面发展不均衡：

| 平面 | 当前成熟度 | 判断 |
|---|---:|---|
| Governance | 约 45% | Registry、关系和模型治理较强；统一身份、Mandate、授权和多数据源治理不足 |
| Coordination | 约 65% | Task/Plan/Artifact/Attempt 强；多人角色、委托审批和责任模型不足 |
| Economics | 约 20% | 有 timeout/retry/model binding；没有 Token/工具/人工成本账本、预算和结果成本 |
| Assurance | SQL 场景约 85%，平台整体约 55% | 查询 Assurance 很强；Knowledge、Memory、协同决策和成本 Outcome 保障不足 |

### 1.2 推荐产品方向

不建议继续以“更准确的 SQL Agent”作为核心叙事，也不建议当前直接改成“通用企业 AI Infra”。推荐中期定位：

> **Forge 是面向数据团队、供人和企业 Agent 共同使用的可信数据任务控制与执行平台。**

更具体的价值表达：

> **把人或 Agent 的模糊数据意图，编译为语义明确、权限合法、成本受控、证据可追溯的可信数据任务。**

定位分层：

```text
现实切入：Trusted AI Data Task Platform
默认体验：Data Agent / 飞书 / Web / API
核心能力：Governed Context + Trusted Data Execution + Evidence
长期假设：Enterprise AI Trust / Context / Action Infrastructure
```

长期假设必须由第二个非 SQL 场景和第二个真实消费者证明，不能提前扩张为通用平台。

### 1.3 当前最重要的架构结论

下一阶段的核心不是增加更多 Skill、渠道或信息源，而是补齐：

1. `Principal / Membership / Role / Agent Mandate / Policy Decision`。
2. `Task Participant / Decision Owner / Delegated Approval`。
3. `Budget / Usage / Cost / Outcome`。
4. `Claim / Evidence / Decision / Memory Proposal / Context Bundle` 分型。
5. 多数据源、默认拒绝和资源级授权。

## 2. 产品方向复审

### 2.1 “Data Agent”仍然成立，但只能作为入口

成立的部分：

- 数据任务高价值、高风险且可复算。
- 企业愿意为口径、权限、执行和审计问题付费。
- Forge 已有真实技术证据，而不是概念设计。
- 数据团队天然拥有治理责任和初始购买动机。

不成立或不足的部分：

- 裸自然语言问数会被模型、数据库和 BI 产品持续商品化。
- 业务人员不愿维护 Registry 和 SQL 安全。
- 长期调用者可能是企业 Agent，而不是直接操作 UI 的人。
- 企业价值不能只用 SQL 准确率解释。

结论：Data Agent 是默认交互形态和市场切口，不是产品最终边界。

### 2.2 “企业 AI Infra”目前只能是方向假设

Forge 已触及 Infra 问题：

- 统一 Task 和 Artifact。
- 模型治理与质量门禁。
- 可信 Tool / Query 执行。
- Evidence 与 Audit。
- Channel Adapter。

但仍缺少证明：

- 第二个非 SQL 受控行动场景。
- 第二个真实 Agent/应用消费者。
- 独立 Context/Memory 服务相对现有系统组合的不可替代价值。
- 企业是否愿意单独为横向 Control Plane 付费。

结论：技术设计保持 Agent Native，但商业定位不应提前泛化。

### 2.3 参与者与采用路径

推荐保持：

```text
GTM：Data-Team Led
体验：Business Accessible
架构：Agent Native
治理：Human Accountable
```

| 参与者 | 当前/长期作用 |
|---|---|
| 数据平台/数据治理负责人 | 购买、部署、数据源和治理责任 |
| Data Steward / Analytics Engineer | Registry、关系、口径和质量维护 |
| Analyst / 业务人员 | 提出目标、确认语义、消费结果 |
| Query/Decision Approver | 承担明确范围的批准责任 |
| 企业 Agent | 代表 Principal 发起或执行有界任务 |
| Auditor / Security | 查看 Evidence、Policy Decision、Action 和成本 |

Forge 不应要求客户先设立新岗位；`Agent Steward` 可先作为现有数据/AI 平台团队的一项职责。

## 3. 当前架构值得保留的部分

### 3.1 Pi / Forge 权力分离

`docs/platform-architecture.md` 定义：

- Pi 拥有任务调度权。
- Forge 拥有数据执行权和否决权。
- Skills 不拥有数据库凭证。
- Channel 不复制状态机。

这符合 A7、A10、A12、A30，应继续作为硬边界。

### 3.2 TaskRun、ExecutionPlan 和 Artifact

证据：

- `services/pi-orchestrator/src/task-store.ts`
- `services/pi-orchestrator/src/planning.ts`
- `services/pi-orchestrator/src/artifacts.ts`
- `services/pi-orchestrator/src/stage-attempts.ts`

已实现：

- 单一 TaskRun 真相源。
- 版本化 ExecutionPlan。
- 有依赖关系的 PlanStep。
- 结构化 Artifact 和 producer 校验。
- Attempt、lease、timeout、幂等和中断恢复。
- 高风险副作用不自动重放。

这符合 A10、A12、A20、A26、A27、A30、A32，是平台化最有价值的基础之一。

### 3.3 Query Assurance 和精确审批

证据：

- `forge/assurance.py`
- `forge/query_runs.py`
- `web/routes/query_runs.py`

已实现：

- Registry/ACL/字段/关系/粒度/口径/意图/Compiler/只读 Gate。
- 不可变 Assurance Report。
- SQL、Assurance、Registry、Policy 和模型 lineage。
- SQL hash 与 Assurance hash 审批。
- Registry/Policy 漂移后拒绝执行。
- execution owner、lease、幂等和失败关闭。

这高度符合 A1-A3、A10-A12、A26、A28、A29、A32，是当前最成熟的能力。

### 3.4 Registry Studio 和 Model Control

Registry 的 Draft/Revision/Diff/CAS/Publish/Rollback 与 Model 的 Profile/Revision/Quality Gate/CAS/rollback，把概率模型和组织语义放入可审计控制面，符合 A2、A3、A6、A29。

### 3.5 Evidence-bound 分析与确定性报告

Analysis finding 必须引用 QueryResult evidence，报告不能从 hypothesis 私自提升结论，HTML/PDF/PPTX 从同一 Bundle 确定性投影。这符合 A3、A4、A26、A27。

## 4. 与产品公理的主要冲突

## 4.1 Governance：身份上下文存在，但 Principal 与 Authority 尚未建立

### 证据

- `TaskRun` 只有 `org_id/team_id/user_id`，没有 Principal、actor type、Mandate 或 delegated_by。
- Pi 普通 Task API 主要依赖 loopback/网络边界，创建请求可直接提供 org/team/user；Channel 和 Skill Policy 才有独立服务 Key。
- Forge 内部 API 使用统一 `X-Pi-Service-Key`，该服务身份可以提交任意 org/team/user，上下文未绑定可验证 delegation。
- `web/auth.py` 是共享管理员密码和固定 `admin` Session，不是独立企业用户身份。
- `agent/tenant.py` 一个 user 只能属于一个 team；`role` 只是字符串，没有统一授权执行。

### 影响

`org_id/team_id/user_id` 目前更多是上下文标签，不是端到端安全主体。这与 A7-A9 冲突。

### 建议

先定义共享 Contract，不急于拆独立服务：

```text
PrincipalContext
· principal_id / principal_type
· actor_id / actor_type
· org_id / workspace_id
· memberships / roles
· authentication_context

AgentMandate
· agent_id / delegated_by
· purpose / task_run_id
· capabilities / resource_scope
· budget / approval_policy
· expires_at / can_delegate

PolicyDecision
· subject / action / resource
· decision / reason / policy_revision
· obligations / evaluated_at
```

外部 IdP 负责认证；Pi 是 Task PEP；Forge 是数据/Action PEP；是否需要共享 PDP 在第二阶段决定。

## 4.2 Governance：ACL 不是企业默认拒绝

### 证据

`agent/tenant.py` 明确规定：团队没有 ACL 行时返回 `None`，含义是“不限制，看所有表”；空列表同样清除限制。

Forge 查询时 ACL 主要通过 `get_allowed_tables_for_user(user_id)` 从另一份本地 user→team 映射获得，而 Pi 已经传入 org/team。

### 影响

- 未配置策略等于全表可见，不符合 A9、A28、A32。
- Pi 与 Forge 存在两个身份上下文来源。
- 不能表达 datasource、schema、column、row、mask、export 和 sensitivity。

### 建议

生产模式改为 deny-by-default，并建立：

```text
Datasource → Schema → Table → Column → Result/Export
```

资源层级授权。行级策略优先使用数据库 RLS；Forge 保持独立 enforcement。

## 4.3 Governance：当前仍是单数据源和全局 Registry 形态

### 证据

- `cfg.DATASOURCE_ID` 是全局配置。
- QueryRun 记录 datasource_id，但创建时固定为全局值。
- Registry、Metrics、Conventions 和 Business Context 都由全局路径加载。

### 影响

无法真正表达多个 Workspace、业务域和数据源的独立 Owner、Policy、Registry Revision 与成本归因。

### 建议

不要直接做任意多租户 SaaS；先引入：

```text
Organization
→ Workspace / Data Domain
→ Datasource Binding
→ Registry Binding
→ Policy Binding
```

并保证 QueryRun 在准备时固定完整 binding snapshot。

## 4.4 Coordination：Task 是单用户任务，不是多人—多 Agent 协作对象

### 证据

- `TaskRun` 只有一个 `user_id`。
- `ExecutionPlanStep` 没有 actor、owner、authority、budget、evidence requirement 或 deadline。
- Query approval 中 `approver_user_id` 必须等于 QueryRun 发起 `user_id`。
- 没有 assignee、collaborator、watcher、Decision Owner、delegation 或 separation of duties。

### 影响

当前可以完成多 Stage 自动协同，但不能表达真正的组织协同和职责分离。与 A19-A22 部分冲突。

### 建议

新增而不是塞入 `metadata`：

```text
TaskParticipant
· principal_id
· role: owner/requester/steward/approver/auditor/viewer
· joined_at / delegated_by

DecisionRequest
· decision_type / action_ref
· required_roles / quorum
· expires_at / policy_revision

DecisionRecord
· decided_by / decision
· evidence_refs / rationale
· obligations / supersedes
```

第一步只实现单 Owner + 可指定 Approver，不立即建设复杂 BPM。

## 4.5 Coordination：ExecutionPlan 是能力 DAG，但还不是完整 Work Graph

当前 Plan 对依赖和交付物表达很好，但 Step 缺少 actor、权限、预算和 Evidence Contract。`metadata` 是开放对象，未来若把责任和预算临时塞入 metadata，会形成新的隐式状态。

建议通过 schema version 2 显式演进，不用无类型 metadata 承担平台核心语义。

## 4.6 Economics：几乎没有真实成本控制面

### 证据

- StageAttempt 记录 model revision、状态和时间，但不记录 input/output/cache/reasoning token、Provider cost、tool cost 或 human review。
- WMB 只有粗略 token 预算裁剪。
- timeout 和 retry budget 控制可靠性，不是经济预算。
- models catalog 可以声明 cost，但运行时没有持久化 Usage Ledger。

### 影响

无法回答：

- 哪个 Task/Agent/Team 花了多少。
- 强模型是否减少了重试和人工审核。
- 多 Agent 是否创造净收益。
- 每个可信 Outcome 的总成本是多少。

与 A23-A25 明显不符。

### 建议

优先建设可观测账本，不先做智能路由：

```text
UsageRecord
· task_run_id / stage_attempt_id
· org/team/principal/agent/model
· input/output/cache/reasoning tokens
· provider_cost / tool_cost / duration
· retry_index / outcome_status

BudgetContract
· max_cost / max_tokens / max_calls
· max_retries / max_duration
· quality_floor / approval_policy

OutcomeRecord
· deliverables / accepted_by
· quality_result / human_review_minutes
· business_value_proxy
```

先能测量，后做 Model Routing 和预算优化。

## 4.7 Assurance：SQL 很强，但平台级 Claim 与 Decision 保障不足

### 证据

`forge/context.py` 已有 source_type、verification_level、scope、revision、expiry 和 bounded evidence，是良好雏形；但：

- Registry 内容被统一标为 `verified/organization`，没有 Owner/confirmed_by/valid_from。
- Memory 除 session summary 外多被标为 verified。
- 冲突主要按相同 title + 不同 content 判断。
- 没有 Claim、EvidenceLink、DecisionRecord 和独立 conflict group。

### 影响

当前可以安全回答 Registry/Memory 中的有界上下文，但还不能证明“为什么这是一条正式组织知识”。与 A1、A4-A6、A15 部分冲突。

### 建议

先定义分型 Contract：

```text
SourceRef
ClaimRecord
EvidenceLink
ConflictSet
DecisionRecord
MemoryProposal
ContextBundle
```

不立即建设统一 Memory Store。Registry 继续保存正式数据语义；其他系统先通过 Context Adapter 和引用接入。

## 4.8 Memory：已有正确边界，但也存在概念混用

### 正确部分

- Pi Task state 与 Forge QueryRun 已分离。
- 个人“记住/忘记”需要 Channel action 确认。
- 内部 memory API 只允许 user scope，不能提升 team/org。
- SMP 有 scope、revision、status、expiry、soft delete。

### 不足

- 内部 API 批准后直接返回 `status=confirmed`，没有可复用 Proposal/Decision 记录。
- `org_id/team_id` 被接收但实际写入只按 user_id。
- EMS、SMP、Registry、Context API 的长期关系仍是实现历史，不是统一 Contract。
- source_session/source_revision 仍不足以表达完整 provenance 和 confirmer。

### 结论

不应继续扩展 Forge 内部 Memory 为万能知识库。应保持：

```text
Pi Session：当前推理与任务上下文
Forge Registry：正式数据语义真相源
Memory Proposal：个人/团队长期信息候选
Context API：有界、只读、证据化访问
```

独立服务化等待第二个消费者。

## 4.9 Assurance：报告和 Audit 的资源权限仍不完整

- ReportStore 保存 org/team/user，但任何有效 Web 管理 Cookie 都能访问任意 report_id，未基于报告记录执行 owner/team/technical scope 权限。
- legacy `agent/audit.py` 缺少 org/team/task/query/datasource/actor/policy decision，多个 Audit Store 尚未统一事件模型。

这不影响当前单管理员受控部署，但不符合企业 Principal、最小权限和过程证据目标。

## 5. 产品公理符合度矩阵

| 公理组 | 强符合 | 部分符合 | 主要缺口 |
|---|---|---|---|
| A1-A6 认识与证据 | SQL/Report lineage | Context/Memory | Claim 分型、Owner、冲突、有效期治理 |
| A7-A12 身份与权力 | Pi/Forge 分权、hash 审批、lease | Channel identity | Principal、Mandate、统一 AuthZ、委托审批 |
| A13-A18 Context/Memory | scope/expiry/soft delete、有界 Context | SMP/Context API | Proposal/Promotion、purpose binding、联邦来源 Contract |
| A19-A22 协同 | Task/Plan/Artifact | Channel/单用户审批 | 多参与者、Decision Owner、职责分离 |
| A23-A28 经济 | timeout、retry、确定性核心、Artifact 复用 | model binding | Usage、Budget、Cost attribution、Outcome cost |
| A29-A32 架构 | 高度符合 | 企业身份尚弱 | 第二消费者验证、完整 fail-closed AuthZ |

## 6. 推荐目标架构调整

以下是评审建议，尚未自动写入目标架构。

```text
业务人员 / Data Team / Enterprise Agents / API
                    │
                    ▼
          Identity & Delegation Boundary
· Authentication · PrincipalContext · AgentMandate
                    │
                    ▼
             Pi Coordination Plane
· Task · Plan · Participant · Decision · Artifact · Attempt
                    │
       ┌────────────┼──────────────┐
       ▼            ▼              ▼
Governance Contracts Economics   Context Contracts
· PolicyDecision     · Budget     · Source / Claim
· Registry Binding  · Usage      · Evidence / Conflict
· Skill/Model Policy · Outcome    · ContextBundle
       │            │              │
       └────────────┼──────────────┘
                    ▼
          Forge Trusted Data Execution
· Datasource Binding · Registry · Assurance · Approval
· Read-only Execution · Result · Audit · Feedback
                    │
                    ▼
     Warehouse / DB / Registry / Approved Sources
```

### 6.1 这不是立即新增三个微服务

`Governance Contracts`、`Economics`、`Context Contracts` 先作为版本化 Contract 和现有服务内的单一职责模块验证。只有出现独立扩容、安全边界或第二个真实消费者后才拆服务。

### 6.2 责任归属建议

| 能力 | 主真相源/执行者 |
|---|---|
| Authentication / Directory | 企业 IdP 或部署身份系统 |
| Principal / Membership / Agent Registry | Platform Governance Store |
| Task / Plan / Participant / Decision wait | Pi |
| Data resource authorization | Forge PEP，读取可信 Policy Decision/Binding |
| Registry / Query / Approval / SQL execution | Forge |
| Stage Usage / Budget / Outcome | Pi 为 Task ledger；Forge补充 Query/tool usage |
| 正式数据语义 | Forge Registry |
| 当前 Session | Pi Runtime |
| 跨 Agent Memory/Context | 先定义 Contract，物理归属待第二场景验证 |

## 7. 分阶段建议

## Phase A：产品与 Contract 对齐

目标：在不拆服务、不改主链的前提下消除概念空缺。

1. 确认新产品定位和非目标。
2. 定义 `PrincipalContext / AgentMandate / PolicyDecision`。
3. 定义 `TaskParticipant / DecisionRequest / DecisionRecord`。
4. 定义 `BudgetContract / UsageRecord / OutcomeRecord`。
5. 定义 `SourceRef / Claim / EvidenceLink / MemoryProposal / ContextBundle`。
6. 明确哪些 Contract 进入第一版实现，哪些只作研究。

退出条件：每个 Contract 有 Owner、真相源、失败边界和至少一个真实数据任务用例。

## Phase B：企业身份与数据授权基础

1. 独立用户身份与 OIDC 接入边界。
2. Organization/Workspace/Membership，多团队归属。
3. Agent/Service Principal 和 Task-scoped Mandate。
4. Pi Task API 服务认证和 delegation 校验。
5. Datasource/Registry Binding。
6. 生产 deny-by-default，表/字段/导出资源授权。
7. Report、Audit、Model、Registry、Skill Policy 统一资源权限。

退出条件：修改 org/team/user 请求字段不能扩大权限；普通 Analyst 不能发布 Registry 或切换模型；技术报告有独立权限。

## Phase C：成本可观测与预算

1. 采集真实 Provider usage。
2. Stage/Task/Team 成本归因。
3. Budget、Quota 和停止条件。
4. Tool/DB compute 与人工审核记录。
5. Cost per Trusted Outcome 初始指标。

退出条件：能比较两个模型或两种协同流程的可信结果总成本，不只比较 Token 单价。

## Phase D：多人—多 Agent 协同

1. Task owner/requester/steward/approver/auditor。
2. 可指定审批人和职责分离。
3. Decision Request/Record 与 Evidence。
4. 异常路由、过期、代理和只读协作者。
5. 用最小 Actor 实验验证多 Agent 净收益。

退出条件：复杂流程不依赖共享账号和群聊隐式状态；所有 Action 可追溯到 Principal 与 Decision。

## Phase E：Context/Memory 第二场景实验

选择一个非 SQL 场景，例如运营信息分析或市场情报：

1. 复用 Source/Claim/Evidence/Decision/Context Contract。
2. 比较集中 Memory、联邦 Context 和组合方案。
3. 引入第二个真实消费者。
4. 测量召回、冲突、过期、权限泄露、纠正和删除成本。

退出条件：只有证明独立服务有不可替代价值，才进入 Memory/Context 服务化。

## 8. 现在应停止或延后的事情

在 Phase A/B/C 前，不建议优先：

- 继续增加无明确 Outcome 评测的新 Skills。
- 继续新增渠道而不补 Principal 与统一授权。
- 把更多外部信息源直接写入 SMP/Registry。
- 拆出通用 Memory Service。
- 引入更多 Agent 角色作为默认流程。
- 给 Forge 增加无边界业务系统写能力。
- 对外宣称已是完整企业 AI Infra 或完整多租户平台。

## 9. 商业与产品指标建议

下一阶段不应只看 SQL Accuracy。建议同时建立：

| 类别 | 指标 |
|---|---|
| Governance | 语义澄清率、Policy deny/exception、权限错误、知识过期/冲突 |
| Coordination | Trusted Task Completion、人工澄清轮次、审批等待、handoff、重复执行 |
| Economics | 每任务 Token/成本、重试成本、人工审核分钟、Cost per Trusted Outcome |
| Assurance | Evidence coverage、Assurance pass、错误回放、审批/执行一致率、Outcome acceptance |
| Adoption | 数据团队治理使用、业务/Agent 发起比例、Registry 复用、第二消费者 |

## 10. 最终评审结论

### 保留

- 可信数据任务作为现实切入点。
- Pi 唯一 Orchestrator。
- Forge 唯一可信数据执行层。
- Registry、Assurance、Artifact、Evidence 和 hash-bound Approval。
- Data-Team Led / Business Accessible / Agent Native / Human Accountable。

### 调整

- 从“可信问数 Agent”提升为“可信数据任务控制与执行平台”。
- 把 org/team/user 标签升级为 Principal、Membership、Mandate 和 Policy。
- 把单用户 Task 升级为有责任角色的协作对象。
- 把 timeout/retry 升级为完整 Economics Plane。
- 把 Memory 检索升级为 Claim/Evidence/Decision/Context 分型，但暂不抽通用服务。

### 暂不确认

- Forge 直接成为通用企业 AI Infra。
- 单一统一 Memory Store。
- 默认多 Agent 工作流。
- 无人类责任主体的自主组织。

### 优先级判断

当前最优先的不是继续拓宽能力，而是让现有纵向优势具备企业横向闭环：

```text
Principal
+ Governed Context
+ Task / Decision
+ Budget / Usage
+ Trusted Execution
+ Evidence / Outcome
```

只有这组闭环在数据任务中被真实客户验证，Forge 才有资格向更广泛的企业 Agent Trust Infrastructure 扩展。
