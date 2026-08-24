# Forge 企业演进阶段性实施计划 v1.1

> 状态：产品方向与计划已确认；M0 Governance 内核与 Contract Review 已通过，M0.4 保留未开始且不阻塞；W1/H1/H2 已完成并部署；H3 产品验收 FAIL 后，H4 已关闭三个 P0 并通过复验；H5 R0 Contract/双 fixture/跨媒介视觉候选实施中；运行时 M1 尚未批准 · Last updated: 2026-08-24
>
> 本文是 2026-08-24 起的**唯一主动计划真相源**。历史实施与验收证据保留在 [`pi-forge-integration-plan.md`](pi-forge-integration-plan.md)；目标职责边界见 [`platform-architecture.md`](platform-architecture.md)；产品约束见 [`product-axioms.md`](product-axioms.md)；本轮评审依据见 [`product-direction-architecture-review-2026-08-24.md`](product-direction-architecture-review-2026-08-24.md)。
>
> 每个工作包继续遵循“先更新计划 → 再实现 → 验证 → 回写状态、风险与下一步”。M0 Contract 评审通过前，不修改运行时授权、API、数据库 Schema 或 OAuth Runtime。

## 0. 摘要与硬约束

采用“近期详、远期粗”的门禁式路线：先收口计划与 Contract，再分三步完成企业治理基础；随后建设成本账本、多人决策、平台 Assurance，最后以第二场景决定 Context/Memory 和更广 AI Infra 的边界。

全程保持以下硬约束：

- Pi 是唯一主 Orchestrator。
- Forge 是唯一可信数据执行层并保留独立否决权。
- 渠道和 Skills 不获得数据库执行权。
- 高风险副作用不自动重放。
- 不新建第二套任务状态或业务真相源。
- 新需求先进入 [`requirements-pool.md`](requirements-pool.md)，完成澄清、评估和用户确认后才能进入本文。
- 每个已批准工作包遵循“先更新计划 → 再实现 → 验证 → 回写需求状态/计划状态/风险”。

中期产品定位：

> **Forge 是面向数据团队、供人和企业 Agent 共同使用的可信数据任务控制与执行平台。**

采用原则：

```text
GTM：Data-Team Led
体验：Business Accessible
架构：Agent Native
治理：Human Accountable
```

质量策略是 `100% Governed` 的受支持过程边界，而不是开放世界端到端 `100% Correct`。Action Catalog 必须同时报告：① `Contract Coverage`，表示受支持 Action 的治理契约是否完整；② `Runtime Governance Coverage`，表示这些 Contract 是否已由生产 PEP 执行并通过负向门禁。两者不得混称；目录外高风险 Action 失败关闭，不纳入能力声明。

## 1. 当前基线与状态

### 1.1 已可复用

- Pi TaskRun、ExecutionPlan、Artifact、StageAttempt、lease、timeout、幂等和恢复。
- Forge Registry、Relationship/Grain Gate、Query Assurance、Compiler、Executor 和 QueryRun。
- SQL/Assurance hash 审批，Registry/Policy/Model lineage。
- Model Profile/Revision/Binding/Quality Gate/CAS/rollback。
- 20 个受控 Skills、Structured Artifact 和 Evidence-bound 分析/报告。
- Web/飞书/钉钉统一 ChannelEvent/Presentation。
- Registry Studio Draft/Revision/Diff/Publish/Rollback。
- 报告 Bundle、分享、HTML/PDF/PPTX 确定性投影。
- M0 实施前基线：Python `540 passed / 24 skipped`；Pi `86 passed`；TypeScript typecheck 通过；npm audit 0 vulnerabilities。
- M0.1–M0.3 实施后基线：Python `544 passed / 24 skipped`；Pi `88 passed`；TypeScript typecheck 通过；npm audit 0 vulnerabilities。

### 1.2 当前主要缺口

- 共享管理员 Web 身份，缺少独立企业用户和 OIDC 边界。
- 一个 user 只能属于一个 team，角色没有统一执行。
- Pi 普通 Task API 缺少完整服务身份与 delegation。
- 生产 ACL 空配置等于允许全部表。
- 单全局 Datasource/Registry Binding。
- Task 只有单一 user，没有 Participant、Decision Owner 和职责分离。
- StageAttempt 不记录真实 Token、Provider/Tool cost 和 Outcome。
- Memory/Context 缺少 Claim、Evidence、Decision、Proposal 和 Purpose Contract。
- Report、legacy Audit、Model、Registry 等资源权限尚未统一。
- SQLite 单机、对象本地存储、生产 Compose 未包含完整 Pi 拓扑，尚不适合规模化 HA。

### 1.3 当前执行状态

| 工作包 | 状态 | 边界 |
|---|---|---|
| M0.1 计划文档收口 | 已完成 | 状态与基线已统一；历史计划已标记为快照；Spider data symlink 已恢复 |
| M0.2 Governance Contract 内核 | 评审修订完成 | `DelegatedMandate v1` 同时覆盖 Pi Service/Agent，强制 Task+Audience，v1 固定禁止再委托；PrincipalContext fixture 的 delegation 均有匹配 Mandate |
| M0.3 Governance Coverage Catalog | 评审修订完成 | v1.1.0 分离 Contract Coverage=100% 与 Runtime Governance Coverage=0%；Human 直接 Action 不强制 Mandate，Service/Agent 代理时 conditional required |
| M0.4 其余 Contract 草案 | 保留未开始 | 不阻塞 M1A；按 Coordination/Economics/Context/OAuth 的首次真实消费者 Just-in-Time 细化，避免当前过早冻结抽象 |
| M0.5 Contract Review Closure | 已完成 | `REQ-2026-08-24-003`：Web/飞书/Agent review trace、40 个负向 mutation、Threat Model、迁移/回滚设计完成；正式 verdict 为 Approved for M1A proposal，Runtime Coverage 仍为 0% |
| W1 Web 对话实时任务视图 | 已完成 | `REQ-2026-08-24-001`：`/chat` 已提供 Pi 真相源的业务 DAG、有界实时任务流和移动抽屉；跨渠道/跨 scope 失败关闭，不新增状态机。Python 546 passed，Pi 88 passed，Playwright 桌面/移动端通过。 |
| H1 Analysis 延迟与进度修复 | 已完成 | `REQ-2026-08-24-005`：Artifact-first Adapter、Provider failure 分类、StageAttempt deadline/phase 时间元数据和 Web elapsed/slow 提示；107 行真实 smoke 从临界 229/240s 降至 119s，不改变 SQL、审批或 Task 真相源 |
| H2 长文本语义化阅读体验 | 已完成并部署 | `REQ-2026-08-24-006`：NAS `9fca1ea` health/readiness/认证门禁通过；隔离 ReportStore HTML/PDF/PPTX exporter 全部 ready，无 SQL/Task 重放 |
| H3 Golden Journey 双验收 | 已完成，产品 FAIL | `REQ-2026-08-24-007`：物理链路 PASS；桌面旅程/可信交付 FAIL。发现 PDF 路径泄漏、same-page Publication 空白、Chart grain 误导 3 个 P0 |
| H4 Golden Journey P0 Closure | 已完成并验证 | `REQ-2026-08-24-008`：真实 NAS PDF 路径清除、same-page Report/Publication 可见、重复 label Chart fail-closed；同一 Golden Journey 复验 262.399s，物理与三个 P0 门禁 PASS |
| H5 Evidence-bound Chart Storytelling | R0 实施中 | `REQ-2026-08-24-009` 已确认：先完成 ChartArtifact v2 Contract、横截面/时间趋势双 fixture、HTML/PDF/PPTX 视觉候选；用户视觉确认前不接生产 Runtime |
| H6 Reusable Report Definitions | 待确认 | `REQ-2026-08-24-010`：Definition/SemanticQuery/Binding/Criteria/Run 版本化，Forge 确定性选择 SQL reuse/rebind/recompile/replan/blocked；先 Contract/双时间 fixture/桌面原型，不原地改旧报告、不自动重放 SQL |
| M1A–M1C | 未批准 | M0 Contract 评审通过后分别批准 |
| M2–M7 | 规划中 | 保留门禁级或粗粒度规划，不提前拆服务 |

## 2. M0：计划与 Contract Ready（近期，详细）

### M0.1 计划文档收口

仅修改文档和仓库基线，不改变运行时行为：

1. 统一状态：产品方向与四平面框架标记为“已确认”；本文标记为当前唯一主动计划；是否进入运行时 M1 与计划确认分开记录。
2. 将 `docs/pi-forge-integration-plan.md` 明确标记为历史实施快照；章节中的“进行中/当前”增加历史时间语义，避免被误当作当前 TODO。
3. 更新实际验证基线为 Python `540 passed / 24 skipped`、Pi `86 passed`、TypeScript typecheck 通过、npm audit 0 vulnerabilities。
4. 恢复 `tests/datasets/spider/data` 为仓库记录的 symlink `../../spider2/data`，不修改 benchmark 数据内容，使全局 `git diff --check` 可复现。
5. 将新战略文档和对应测试纳入版本管理范围；不提交 `.env`、运行状态、个人 `.pi/.codex` 配置或凭证。

### M0.2 Governance Contract 内核

以现有 `agent/contracts/` JSON Schema 为跨 Python/TypeScript 的权威边界，新增版本化治理 Contract；TypeBox/TypeScript 运行时定义必须通过 parity 测试与 JSON Schema 一致。

第一批完整定义：

- `PrincipalContext v1`
  - 明确区分 `actor_principal` 与 `accountable_principal`。
  - Actor 可为 Human/Service/Agent；最终责任主体只能为 Human/Team/Organization。
  - 包含 Organization、Workspace、authentication context、delegation chain、签发/过期时间。
- `DelegatedMandate v1`
  - 同时覆盖 Service/Agent delegate，绑定 delegator、delegate、accountable principal、purpose、task、audience、capabilities、resource scope、budget ref、approval policy、expiry。
  - active mandate 必须绑定具体 `task_run_id`；v1 的 `can_delegate` 固定为 `false`，不声明尚不可验证的递归委托能力。
  - “Agent Mandate”是 `delegate_principal.principal_type=agent` 的领域称谓，不另建一份重复 Contract。
- `PolicyDecision v1`
  - 固定 subject、action、resource、`allow/deny/conditional`、reason、obligations、policy revision 和有效期。
- `ResourceRef v1`
  - 支持 Organization、Workspace、Datasource、Registry、Schema/Table/Column、QueryResult、Report/Export、Model、Skill、Audit。
- `DatasourceBinding v1` 与 `RegistryBinding v1`
  - 绑定 Workspace、资源 revision、Policy revision 和生命周期。

安全语义不得藏在开放 `metadata` 中；Contract 不携带 token、API Key、数据库凭证或完整敏感结果。

Contract Owner 与真相源：

| Contract | Owner | 正式真相源 | 最小披露 |
|---|---|---|---|
| PrincipalContext | Pi Governance | 认证映射与 Principal/Membership Store | ID、类型、scope、auth method/hash、有效期；不含 token |
| DelegatedMandate | Pi Governance | Mandate Store | Delegate、Audience、Purpose、Task、能力、资源、责任主体、有效期；不含凭证 |
| PolicyDecision | 做出资源裁决的 PEP | 对应 Policy/Audit Store | 决策、原因码、义务、policy revision；不展开敏感策略全文 |
| ResourceRef | 资源 Owner | 对应领域 Store | 稳定资源类型、ID、Organization/Workspace scope |
| DatasourceBinding | Forge | Forge Datasource/Policy Store | datasource ref、revision、policy revision、生命周期 |
| RegistryBinding | Forge | Forge Registry/Policy Store | registry ref、datasource binding、revision、policy revision、生命周期 |

### M0.3 Governance Coverage Catalog

建立版本化 Action Catalog，作为“100% Governed”的可测分母。第一版至少覆盖：

- `query.prepare / approve / execute / cancel`
- `registry.publish / rollback`
- `model.activate / rollback`
- `skill_policy.update`
- `report.read / share / export`
- `memory_proposal.confirm / forget`

每个 Action 标记 Owner、执行者、风险级别、需要的 Principal/Mandate/Policy/Decision、真相源和失败策略。`support_status` 只表示产品是否支持该 Action；`contract_status` 表示治理契约是否完整；`runtime_enforcement_status` 表示 v1 Contract 尚未接入、部分接入或已完整执行。Human 直接 Action 依据 Membership/Role/Policy/Decision，不强制持有 DelegatedMandate；Service/Agent 代表 Principal 行动时 mandate 才是必需。未进入支持目录的高风险 Action 必须 fail closed。

### M0.4 其余 Contract 草案

以下 Contract 在 M0 只形成 Schema、fixture 和威胁边界，不进入生产运行时，也不阻塞 M1A：

- Coordination：`TaskParticipant`、`DecisionRequest`、`DecisionRecord`、`ExecutionPlan v2`。
- Economics：`BudgetContract`、`UsageRecord`、`CostCatalogRevision`、`OutcomeRecord`。
- Context：`SourceRef`、`ClaimRecord`、`EvidenceLink`、`ConflictSet`、`MemoryProposal`、`ContextBundle`。
- OAuth：`ModelBackend`、`AuthSlotRef`、`ModelCompatibilityResult`、`ModelFallbackPolicy`、`QueryPlanningEnvelope`、`ForgeQueryCandidateSubmission`。

### M0.5 Contract Review Closure

> Requirement：[`REQ-2026-08-24-003`](requirements-pool.md#req-2026-08-24-003完成-m05-contract-review-closure) · 决策：`accepted_with_changes`

本工作包只形成 review fixture、语义验证、Threat Model、迁移/回滚设计和正式 verdict，不修改生产授权、API、数据库 Schema、QueryRun 或 OAuth Runtime。

#### M0.5A 完整 review trace

使用现有 Web、飞书和 Agent 请求各制作一条完整 fixture，覆盖：

- Human requester → Pi service actor → Forge trusted executor。
- Agent actor → accountable human/org principal → task-scoped mandate。
- PrincipalContext、DelegatedMandate、PolicyDecision、Datasource/Registry Binding、SQL Action、human approval snapshot 和 QueryRun lineage。
- 空的 Economics/Context 扩展位显式为 `null`，不靠任意 metadata，也不把 test-only approval snapshot 冒充未来 DecisionRecord Contract。

#### M0.5B 跨 Contract 语义门禁

JSON Schema/TypeBox 继续负责形状；共享 review fixture 和 Python/TypeScript 语义验证负责：Organization/Workspace、时间有效性、delegation 连续性、Task/Audience/Capability/Resource、Policy subject/action/effect、Binding revision、human approval 与 SQL/Assurance hash lineage。每条核心不变量必须有命名负向 mutation 和稳定 reason code。

#### M0.5C Threat Model

覆盖请求身份篡改、Service Key 重放、跨 Task/Audience delegation、Capability/Resource 扩权、默认允许 ACL、跨租户枚举、过期/撤销 Mandate、Context 跨 Purpose 泄露、预算绕过、Binding/SQL 漂移复用旧审批和 legacy 身份伪造。

#### M0.5D 迁移、兼容与回滚

设计 `legacy_single_user` 显式兼容、TaskRun v2 引用/hash、无法安全映射任务的 `needs_input/expired`、单切换点、feature flag、无授权双写和 rollback；只设计不迁移数据。

#### M0.5E 正式评审

输出独立 review 文档与 `Approved / Approved with blockers / Rejected` verdict。即使 Approved，也只表示可以提出 M1A 实施工作包，不表示 Runtime Governance Coverage 大于 0 或 M1A 自动获批。

#### M0.5 实施结果（2026-08-24）

- 新增 `governance-review-fixtures.v1.json`，以 Web Human、Feishu Human 和 Agent 三条 review-only trace 组合 Principal、Mandate、Policy、Binding、Action、human approval snapshot、request binding 与 Query lineage；Economics/Context 扩展显式为 `null`。
- Python `governance_semantics.py` 与 TypeScript `validateGovernanceReviewTrace` 对同一共享 corpus 验证 Organization/Workspace、时间、delegation、Task/Audience/Purpose/Capability/Resource、Policy/Binding、approval 和 SQL/Assurance lineage。
- 40 个命名 mutation 覆盖跨租户、过期/撤销、扩权、默认拒绝前置条件、漂移审批、请求重用和隐式 Context；两端使用同一稳定 reason code 断言。
- [`governance-contract-review-2026-08-24.md`](governance-contract-review-2026-08-24.md) 完成 Threat Model、legacy migration、TaskRun v2、切换与 rollback 设计；Verdict 为 **Approved for M1A proposal**。
- 当前验证：Python `550 passed / 24 skipped`；Pi `91 passed`；TypeScript typecheck 通过；npm audit 0 vulnerabilities；JSON 与 `git diff --check` 通过。
- 未修改 Task API、数据库 Schema、现有授权逻辑、QueryRun 行为或 OAuth Runtime；Action Catalog 的 Runtime Governance Coverage 保持 0%。

### M0 验收门禁

- 所有 Governance 内核 Contract 有 JSON Schema、有效/无效 fixture 和 Python/TypeScript parity 测试。
- Actor 与最终责任主体不可混淆；Agent 不能成为最终 Decision authority。
- Action Catalog 能计算支持范围内的 Governance Coverage。
- 一条现有 Query Task 能完整映射 Principal、Mandate、Policy、Binding、Decision 和 Action。
- 当前主链测试、typecheck、audit、链接检查和全局 `git diff --check` 全部通过。
- 无运行时授权、API 和数据库行为变化。

M0 通过后单独进行 Contract 评审，才进入 M1A。

### M0.1–M0.3 实施结果（2026-08-24）

- 计划状态已收口：产品方向与四平面框架确认，本文成为唯一主动计划；`pi-forge-integration-plan.md` 明确为历史快照。
- `tests/datasets/spider/data` 已恢复为仓库记录的 `../../spider2/data` symlink，未修改 benchmark 数据内容。
- 初版 `agent/contracts/` 新增 `PrincipalContext v1`、`AgentMandate v1`、`PolicyDecision v1`、`ResourceRef v1`、`DatasourceBinding v1`、`RegistryBinding v1` 及共享有效/无效 fixture；Contract 评审发现 AgentMandate 与 Coverage 语义问题，当前正在修订，不能据初版进入 M1A。
- `services/pi-orchestrator/src/governance-contracts.ts` 提供对应 TypeBox 类型；Python JSON Schema 与 TypeScript 使用同一 fixture corpus 做行为 parity。
- Governance Action Catalog 初版收录 14 个 supported Action，固定 Owner、Executor、风险、Required Context、Truth Source 与失败策略；评审发现 `governed=true` 和 100% 测试仍会把字段完整误读为运行时 enforcement，必须升级目录语义后再批准。
- 新增 Contract 与 Catalog 文档，明确 Owner、真相源和最小披露；评审修订将使“Contract Coverage 不等于运行时 enforcement”成为机器可读字段，而不只是一句文档说明。
- 未修改 Task API、TaskRun/QueryRun 数据库 Schema、现有授权逻辑、数据库行为或 OAuth Runtime。

验证：

- Python：`544 passed, 24 skipped`。
- Pi Orchestrator：`88 passed`。
- TypeScript typecheck：通过。
- `npm audit --omit=dev`：0 vulnerabilities。
- JSON 解析、文档链接和全局 `git diff --check`：通过。
- TypeScript/Python LSP 未配置；分别使用 `tsc --noEmit`、Python 全量测试和契约测试替代。

评审修订结果与下一步：

- M0.2/M0.3 阻断项已修复：`DelegatedMandate v1` 同时表达 Service/Agent delegate，active/historical Mandate 均固定具体 Task 与 Audience，v1 `can_delegate=false`；Human 直接 Action 与受托 Service/Agent 的 Mandate 条件已区分。
- Action Catalog v1.1.0 使用 `support_status + contract_status + runtime_enforcement_status`，机器可读地报告 Contract Coverage 100% 和 v1 Runtime Governance Coverage 0%，不再把字段完整冒充运行时已治理。
- Web/Agent PrincipalContext 的共享 fixture 引用真实匹配的 Mandate；Python/TypeScript 增加跨 Contract 引用一致性和 task/recursive delegation 负向测试。
- 当前验证：Python `548 passed / 24 skipped`；Pi `89 passed`、TypeScript typecheck 通过；npm audit 0 vulnerabilities；JSON/文档/diff check 通过。
- M0.5 已在后续工作包完成完整 Query review trace、Threat Model 和迁移设计；M0.4 其余草案保留未开始且不阻塞，M1A 仍未批准。
- v1 Schema 负责稳定形状和最小类型不变量；跨对象 Organization/Workspace 一致性、时间先后、撤销状态和 delegation chain 连续性已由 M0.5 review validator 与共享 mutation corpus固定，仍需 M1 PEP 在生产路径失败关闭执行。
- 新 Contract 和 Catalog 当前没有生产调用方；未经本轮 Contract 评审不得接入 M1A。

## W1：Web 对话实时任务视图（独立只读切片）

> Requirement：[`REQ-2026-08-24-001`](requirements-pool.md#req-2026-08-24-001web-对话右侧任务-dag-与实时任务流) · 决策：`accepted_with_changes`

### W1.1 用户体验

- 桌面端 `/chat` 右侧常驻当前任务面板；窄屏降级为可展开抽屉，不挤压主对话。
- 上半部分显示最新 `ExecutionPlanArtifact` 的业务 DAG：节点标题、依赖、状态和 plan revision。
- 下半部分显示可折叠实时任务流：Task 状态、StageAttempt 和关键 TaskEvent；按 sequence 单调追加，不因轮询闪烁。
- 创建消息、执行卡片 Action、选择最近任务时同步观察焦点；补查 child 可成为当前执行焦点，但不能改写 parent Task 真相。

### W1.2 数据与安全边界

- 唯一数据源是 Pi 的 Task、最新 ExecutionPlan Artifact、TaskEvent 和 StageAttempt；Web 不计算或持久化新的任务状态。
- Forge Web 提供 Web-chat-scoped 聚合读取接口，服务端再次验证 Organization/Team、`channel=web` 和当前 Web 用户。
- 响应只返回 DAG/状态展示所需字段；排除 Secret、Prompt、hidden CoT、Tool transcript、完整异常、内部 hash/path 和不必要 payload。
- 轮询使用 event sequence 增量读取和有界退避；切换任务后旧轮询失效。

### W1.3 验收门禁

- DAG 来自最新有效 ExecutionPlan revision，依赖边和节点状态一致。
- StageAttempt running/terminal 状态实时更新；TaskEvent 按 sequence 去重、单调追加。
- Web Task 可见；跨渠道、跨 scope 和非法 task ID 失败关闭。
- 新对话、最近任务恢复、等待审批、执行中、完成和失败有展示测试。
- 页面不推进 Task、不批准 SQL、不重放 Attempt；折叠面板不影响执行。
- 支持 `prefers-reduced-motion`、键盘操作和窄屏抽屉。

### W1.4 实施结果（2026-08-24）

- Forge Web 新增 Web-chat-scoped `/flow` 聚合读取，只返回有界 Task、最新 ExecutionPlan、增量 Event 和去敏 Attempt；服务端复核 Organization/Team、`channel=web` 和 `web_admin`。
- `/chat` 桌面端右侧绘制最多 12 个步骤的依赖 DAG，节点区分 waiting/running/completed/failed/skipped；实时流按 sequence 只追加，Attempt 状态原地更新。
- 窄屏使用带 backdrop、Escape/关闭按钮和 ARIA 状态的抽屉；`prefers-reduced-motion` 关闭动画。
- 新消息、Presentation、Action 返回的 child Task 和最近任务恢复都会切换观察焦点；旧轮询通过 epoch 失效，不影响 Pi 执行。
- 自动验证：Python `546 passed / 24 skipped`；Pi `88 passed`、TypeScript typecheck 通过；Web 定向测试 77 passed；桌面和 390px 移动端 Playwright 通过且 0 console/page error；网站构建和 `git diff --check` 通过。
- 遗留边界：当前使用有界 polling；未来 PlanStep 超过 12 或出现大规模动态 Work Graph 时必须重新进入需求池评估布局与推送方案。

## H1：Analysis Stage 延迟与真实进度修复（P0 独立切片）

> Requirement：[`REQ-2026-08-24-005`](requirements-pool.md#req-2026-08-24-005修复-analysis-stage-临界超时与假死体验) · 决策：`accepted_with_changes`

### H1.1 Problem 与边界

同一 107 行 QueryResult 在当前全局 fallback model revision 上出现 229s 成功、240s 超时和未提交 Artifact，已接近 240s Stage deadline；Web 在一次模型调用期间没有真实 elapsed/deadline，因此用户无法区分“仍在生成”和“已经失联”。本切片只修 Analysis 模型固定、输出边界和观测体验，不修改 SQL、审批、QueryRun、Governance PEP 或 Task 状态机。

### H1.2 实施

1. Production Analysis Adapter 将 Skill 的 Markdown 输出示例解释为方法参考，明确要求模型直接调用唯一 `submit_analysis_artifact`，并限制 findings/hypotheses/suggested queries 的数量；不得先生成自由文本长文。
2. Pi SDK session 的 Provider 错误即使 `prompt()` resolve 也必须映射为有界类别；quota/rate-limit/auth/provider/context/abort 不得误报为 Artifact omission，也不得触发无意义 correction。
3. StageAttempt 以向后兼容可空字段记录 `deadline_at`、`progress_phase`、`first_model_activity_at`、`tool_submitted_at`；Pi SDK session event 只提升稀疏生命周期时间，不保存 streaming text/thinking。
4. Web flow allowlist 只投影上述业务安全字段；浏览器本地计算 elapsed/remaining，60s 后显示“耗时较长但仍在安全窗口”，不伪造百分比或新增 heartbeat Event。
5. 通用 Tool capability gate 不再作为 `pi.analysis` 的充分条件。只有真实 `submit_analysis_artifact` smoke 通过后才能激活独立 Binding；失败候选立即 rollback，不修改 SQL Critical scopes、Secret 或全局 catalog。
6. timeout 继续回到 `analysis_retry`；不得自动重跑 SQL、自动切未验证模型或把长耗时误标为成功。

### H1.3 验收与回滚

实施发现：`deepseek-official` 候选在 Pi 无可用 credential；`openai/deepseek-v4-flash` 与 bounded `ark-code-latest` 虽通过 generic Tool smoke，但真实 Analysis Artifact smoke 均未提交 Artifact。NAS 已完整恢复到 `e4e3cb0`、无 Analysis Binding、原 catalog 和健康服务。该证据否证 generic gate 的充分性，后续不得强行激活候选。

- 旧 StageAttempt 缺少新字段仍可读取；SQLite user_version 不变化。
- 稀疏 progress 更新不写 Prompt、Tool payload、模型正文、Secret 或 hidden CoT。
- Provider 错误类别可验证且不披露原始响应；Artifact correction 只在模型正常结束但未调用 Tool 时执行一次。
- Web 正确展示 running、slow、deadline、terminal；键盘/移动端/reduced-motion 无回归。
- `analysis_artifact_gate` 未通过前保持现有兼容模型路径并固定 revision；不得激活 generic-gate-only Binding。两个候选失败与自动回滚必须保留评审证据。
- Python/Pi/typecheck/audit/Playwright/NAS health 通过；用隔离、无 SQL 的真实 Analysis smoke 验证 Artifact、耗时和阶段元数据。
- 代码回滚不删除已有 attempt JSON 字段；失败时保持 `ready_for_analysis`，不放宽授权。

### H1.4 实施结果（2026-08-24）

- Analysis prompt adapter 明确将 Skill Markdown 转为唯一终止型 `submit_analysis_artifact`，并对核心数组设置有界数量；不修改专业分析方法或 Evidence 约束。
- Pi SDK `prompt()` resolve 但 session 含 Provider error 时，Adapter 现在输出安全类别并停止 correction；原始响应、Prompt、Secret 和 hidden CoT 不落 StageAttempt/Event。
- StageAttempt JSON 增加兼容可空 deadline/progress 时间字段，SQLite Schema/user_version 不变；Web 本地每秒显示 elapsed/remaining/slow，不伪造百分比或 heartbeat Event。
- 两个 generic-gate-only 候选的真实 Analysis smoke 失败后均完整回滚，未保留 Binding/catalog 变更；这成为后续 `analysis_artifact_gate` 的反例测试要求。
- NAS 原兼容模型无 SQL 隔离 smoke：2 行 `33.292s`、107 行/3 列 `119.232s`，均生成合法 Artifact 和 progress phase；修复前同规模为 `229.106s` 成功与 `240.051s` timeout。
- 验证：Python `550 passed / 24 skipped`；Pi `93 passed`、typecheck、npm audit 通过；Web 定向测试和 Playwright 通过；NAS `45fcc87` Forge/Pi health/readiness 正常。
- 遗留风险：119s 仍是长响应；在真实 `analysis_artifact_gate`、场景 P95 和 rollback 通过前，不激活独立 Analysis Binding，不宣称延迟问题已被任意输入完全消除。

## H2：对话与报告的长文本语义化阅读体验

> Requirement：[`REQ-2026-08-24-006`](requirements-pool.md#req-2026-08-24-006对话与报告的长文本可读性和语义化强调) · 决策：`accepted_with_changes`

### H2.1 共同设计与信任边界

- Artifact 继续保存事实、建议、限制、优先级、置信度和证据关系；Renderer 只负责表示，不创造或重新分类业务结论。
- 模型不能输出任意 HTML/CSS、脚本、颜色或组件类型。渠道只接受安全 Markdown 子集；业务强调色和 callout 由固定语义标签与 design token 决定。
- 普通下划线不作为强调，避免与链接混淆；强调使用字重、斜体、inline code、层级标题、左边框、背景和明确标签。
- Web、PDF 和 PPTX 必须保持同一信息优先级，但允许按媒介能力使用不同布局；技术报告只改善排版，不加入业务化结论组件。

### H2.2 R1 Chat readability（当前实施切片）

1. 完善 Channel Renderer 对 `AdvisoryArtifact` 和 `AnalysisArtifact` 的确定性投影：summary、findings、recommendations、assumptions、limitations、open questions 和 deliverables 不再被静默丢弃；使用固定标题和 blockquote 标签表达语义。
2. 扩展 Web Chat 现有无依赖 Renderer，安全支持 H2/H3、strong、emphasis、inline code、http(s)/站内链接、ordered/unordered/nested list、fenced code 和连续 blockquote；原始 HTML/script 一律作为纯文本。
3. 由 Web 将固定的“核心说明/注意/限制/待确认”等标签映射为 `info/success/warning/limitation` callout；模型不能自行指定颜色。
4. 统一正文最大阅读宽度、行高、段距、列表缩进、代码换行和移动端密度；保留链接下划线、键盘可达性、外链 `noopener noreferrer` 与 reduced-motion。

R1 门禁：截图对应的指标口径长文、分析长文、SQL code block、原始 HTML 注入、站内/外链接、390px 移动端与桌面端均有自动或 Playwright 验证；0 console/page error。R1 完成后先由用户确认视觉方向，再进入 R2，不在未经确认时同时重做所有报告媒介。

R1 实施结果：

- Channel Renderer 完整投影 Advisory/Analysis 已有结构化语义；字段内容被收敛为 inline 表示，只有 Renderer 固定标签能形成 block-level callout。
- Web Chat 的无依赖安全 Markdown DOM renderer 与 editorial design token 已完成；HTTP(S)/站内链接白名单、外链安全属性、原始 HTML 纯文本和嵌套列表均通过 Playwright。
- 390px 全局导航改为带 backdrop、Escape 和 ARIA 状态的抽屉；Chat 正文无横向溢出，桌面保留当前三栏+任务流布局。
- 验证：Python `550 passed / 24 skipped`；Pi `94 passed`、typecheck、npm audit 通过；Web 定向 `16 passed`；桌面/移动 Playwright 0 console/page error、恶意 script 未执行。
- 视觉候选保存在 `/tmp/forge-chat-readability-desktop.png` 与 `/tmp/forge-chat-readability-mobile.png`；用户于 2026-08-24 确认 R1 视觉方向并要求继续 R2。

### H2.3 R2 Report readability（实施中）

1. 业务 Web/PDF：同一确定性 HTML 使用 editorial hierarchy；Executive Summary、关键发现、建议、限制/风险和证据说明分别映射为固定组件，confidence/priority 使用文字+颜色双编码。
2. PPTX：按内容长度拆页；摘要、发现、建议和限制使用不同版式及文字标签，避免整页同级 bullet，不裁掉 Artifact 内容。
3. 技术报告：只改善 heading、code、table、line-height、打印和长字段换行，不使用业务化 callout。
4. 保持 immutable Report Bundle、分享 ACL、PDF/PPTX 下载审计与 HTML/PDF 同源；不修改 SQL、查询、报告事实或 Artifact Contract。

R2 门禁：HTML 与 PDF 视觉层级一致；PPTX 无文字溢出且信息不丢失；高对比度、打印、窄屏和长中英文内容通过；现有报告 idempotency、share scope、下载审计与 forbidden-content 门禁不回归。

R2 实施结果：

- 业务 HTML/PDF 使用同一确定性 editorial design：Executive Summary、方法、confidence/evidence 发现卡、图表/明细、priority 行动卡、下一步和 limitation 风险区；所有业务内容继续从 Artifact escape 后投影。
- Print CSS 使用 A4 色彩、section/card 分页和重复表头；Playwright Chromium 实际生成 424KB PDF。技术报告只改善基础 typography、code/table 和长字段换行。
- PPTX 改为固定 16:9 语义版式并按 3 cards/page 和有界字符片段分页；长内容测试确认 300 字发现、220 字报告标题、180 字图表标题未丢失，单 shape 文本不超过 160 字；Quick Look 封面验证通过。
- 自动验证：Python `551 passed / 24 skipped`；Pi `94 passed`、typecheck、npm audit 通过；报告专项 `7 passed`；桌面/390px/print/technical Playwright 0 console/page error、0 横向溢出。
- 本地未安装系统级 Chrome/Chromium 命令，因此 ReportStore subprocess PDF exporter 留到部署 smoke；同一 Chromium print engine 的 PDF 已由 Playwright验证。NAS 未在本工作包自动部署。

### H2.4 回滚与退出条件

- R1 可独立回滚到旧安全文本 Renderer，不修改 Task/Artifact Store；R2 只影响新生成的不可变 Report revision，不原地改写已发布文件。
- 如果固定语义字段无法表达所需层级、只能依靠关键词正则猜测，则暂停并重新评估版本化 Presentation Block Contract；本轮不提前新增通用 RichText DSL。
- H2 不改变 Pi/Forge/Skill 职责、Runtime Governance Coverage、模型 Binding、数据库访问或审批边界。
- 用户已确认将 H2 部署到 NAS：沿用 Git bundle fast-forward、running Attempt 空闲检查、SQLite online backup、API/Pi restart 和目标机隔离 exporter smoke；不得读取/修改 Secret、Identity Map、Registry 或数据库连接，不重放 SQL。
- 部署结果：NAS 从 `caa8b69` fast-forward 到 `9fca1ea`；10 个 SQLite online backup 位于 `~/services/forge-m4.1/backups/readability-20260824T094102Z/`；Forge/Pi active、health/readiness ok、匿名 Chat/flow 门禁正确、running Attempt=0。
- 目标机 `/usr/bin/google-chrome` 隔离报告 smoke 返回 HTML published、PDF ready（468,786 bytes）、PPTX ready（42,333 bytes）；临时输入和产物自动删除。未 push、未改依赖/Secret/Identity/Registry/数据库配置，回滚点保留 `caa8b69`。

## H3：完整问数 Golden Journey 的物理与视觉双验收

> Requirement：[`REQ-2026-08-24-007`](requirements-pool.md#req-2026-08-24-007完整问数旅程的物理链路与逐阶段视觉验收) · 决策：`accepted_with_changes`

### H3.1 隔离拓扑与授权

- 在 NAS loopback 临时目录启动当前代码的独立 Web、Pi、Forge 状态与 Report Store；复用版本化只读测试数据库和当前模型 credential reference，但不读取/回显 Secret。
- 生产 Forge/Pi/Web、认证配置、Task/Query/Audit/Report Store 保持不变；临时 Web 可关闭认证，但只监听 loopback 并经 SSH tunnel 供本地 Playwright 访问。
- test principal 只在本旅程批准一次测试数据 SQL；不得访问生产数据库、修改 Registry、写生产 Store 或执行写 SQL。测试结束停止临时服务并保留去敏证据包。
- 若任一状态介质、端口、service key、datasource 或 model context 不能证明隔离，立即停止，不靠事后清理正式审计记录补救。

### H3.2 Golden Journey

固定问题：`统计不同品类的销售额，分析主要差异，并生成完整报告。`

Playwright 驱动并在每一步建立 checkpoint：

1. 提交问题并观察初始计划/实时流。
2. 到达 SQL Review，检查 SQL、风险文案、审批 action 和 DAG。
3. 以 test principal 批准一次；验证 hash 绑定、只读执行和重复 action 幂等。
4. 检查 QueryResult 表格、行数/截断说明和“开始分析”动作。
5. 发起 Analysis，记录 progress、elapsed/deadline、Artifact 和结果可读性。
6. 发起完整报告，检查 publication links、Web business/technical report、PDF/PPTX。
7. 以桌面端捕获所有关键状态；当前产品暂不考虑移动端，已采集的移动 projection 仅作非门禁诊断证据，不进入 finding 和修复范围。

### H3.3 物理验收

- 同一 `task_run_id` 的 ExecutionPlan、PlanStep、TaskEvent、StageAttempt、Artifact 顺序与最终状态一致。
- QueryRun、SQL/Assurance hash、批准主体、执行次数、QueryResult、Evidence、Analysis、Report Bundle 和 Publication lineage 可组合回放。
- SQL 只读且仅执行一次；重复批准/轮询不重复执行；Web/Playwright 不直接推进 Pi Store。
- Stage latency、无事件窗口、timeout/retry、Provider failure 和 exporter status 有界记录；不保存 Prompt、模型正文、hidden CoT、Secret 或无关完整结果集。

### H3.4 视觉与交互验收

- 每个 checkpoint 使用 Playwright 做 DOM/ARIA/action/focus/overflow/console/page-error 断言，并保存桌面截图。
- 视觉模型逐图评估桌面端：信息层级、当前状态、下一步、风险/审批显著性、等待可信度、表格/SQL/长文阅读和错误恢复。
- 后端成功但用户不清楚发生了什么、下一步不可发现或关键限制被淹没，均判产品失败。移动端当前不参与 Pass/Fail。

### H3.5 产物、门禁与退出

- 输出一份版本化 acceptance report：逐阶段 `Pass/Fail/Blocked`、物理时序、视觉评审、P0/P1/P2 findings、截图 contact sheet、重现步骤和剩余风险。
- 主旅程不允许通过测试脚本跳过产品 action、篡改状态、注入 Artifact 或直接调用后续 Stage 冒充用户流程。
- 真实模型失败按失败记录；可用 deterministic control 定位基础设施，但不能替代最终结果。
- 本工作包只跑一次有界主旅程。needs-input、取消/拒绝、timeout/retry edge journeys 根据本次发现重新进入需求池，不在 H3 内无限扩张。

### H3.6 实施结果（2026-08-24）

- NAS loopback 临时环境复用当前真实模型 credential reference，使用独立 State/Query/Audit/Report Store、mode-0400 测试数据副本和 test principal；生产认证、数据库和 Store 未修改。
- 同一 TaskRun 最终 `completed / report_complete`：Query prepare 4.144s、execution 0.220s、Analysis 183.265s、Report 49.051s、全任务 349.028s；4 个 Attempt 全 succeeded，1 次审批/1 次执行，exact duplicate ChannelEvent HTTP 200 且未重放 SQL。
- 9 个 ExecutionPlan revision、QueryResult/Chart/Analysis/RenderedOutput/TechnicalReport/ReportBundle/Publication lineage 连续；HTML/PDF/PPTX ready；测试 datasource 无 WAL/SHM。
- Playwright 保存桌面逐阶段 screenshot，DOM/ARIA/focus/overflow/console/page-error 自动断言通过；视觉模型逐图评审。用户后续明确当前不考虑移动端，移动证据不参与 Verdict。
- 正式 verdict：**Physical chain PASS / Trusted product outcome FAIL**。P0 为：①真实 PDF footer 泄漏内部 `file:///home/...` 路径；②长 Analysis 底部 action 后 same-page Report/Publication 主区空白，刷新后才可见；③ Chart builder 未验证 grain/重复 label，报告可生成误导性品类图。
- P1 包括 decision-readiness、SQL review 修改需求路径、结果单位/异常、主进度可读、长卡片 action、报告风险前置和 PPTX 封面截断。完整证据见 [`golden-journey-acceptance-2026-08-24.md`](golden-journey-acceptance-2026-08-24.md)。
- 隔离服务已停止，临时 service/channel keys 删除；生产 Forge/Pi health/readiness 正常。P0 修复已登记 `REQ-2026-08-24-008`。

### H4：Golden Journey P0 Closure（已完成）

> Requirement：[`REQ-2026-08-24-008`](requirements-pool.md#req-2026-08-24-008关闭-golden-journey-的-p0-可信交付缺陷) · 用户已确认

实施顺序与职责：

1. **P0-A PDF leak（Forge deterministic exporter）**：关闭 Chrome 默认页眉页脚；新增实际 PDF 内容负向回归，拒绝 `file://`、`/home/` 和浏览器默认标题/日期，不改写已发布 Report revision。
2. **P0-B same-page completion（Web projection）**：把桌面 Chat/Flow 约束在 viewport-bounded layout，主 feed 与 Task Flow 独立滚动；长 Analysis 底部 action 被 focus/click 后，短 progress/publication replacement 必须回到可见锚点。Web 只修 projection，不推进或复制 Task 状态。
3. **P0-C Chart grain（Pi deterministic Chart builder + Report projection）**：Chart 生成前验证可见 dimension label 的 grain；重复 label 只能使用稳定 key、确定性聚合或拒绝。若无法证明安全聚合，本轮优先 fail-closed 抑制 Chart，不让模型或 Renderer猜业务口径。Chart evidence refs 必须与实际投影一致。
4. 分别运行最小单元/合约/Playwright 回归，再运行 Python/Pi/TypeScript/report exporter 相关套件。
5. 部署前建立 NAS 回滚点；部署后使用独立 Store、只读 datasource 和 test principal 重跑同一桌面 Golden Journey。只修改新 Report revision；不读取 Secret、不改生产认证或数据库。

H4 门禁：

- 实际目标 exporter PDF 不含默认 header/footer、`file://`、`/home/` 或内部 report path。
- 桌面同页 `long Analysis → focus/click Report → progress → publication` 无刷新可见，且 0 console/page error；新页面恢复仍保持正确。
- 重复可见 dimension label fixture 不会静默生成前 N 行品类图；唯一 label fixture 保持 Chart，evidence refs 精确绑定所渲染数据。
- 同一 Golden Journey 的物理不变量继续全 PASS，三个 P0 均通过视觉和自动断言；否则 H4 继续失败。
- P1、移动端和 M1A 不在本工作包范围。
- 用户在 H4 实施期间新增“专业报告多图、现代样式、交互与标注”方向，已评估为独立 `REQ-2026-08-24-009 / H5`。H4 只保留重复 label fail-closed，不在 P0 修复中仓促加入自由图表 DSL 或视觉大重写。

### H4 实施结果（2026-08-24）

- `b5e4884`：PDF exporter 关闭 Chrome 默认 header/footer；Chat/Flow 固定 viewport ownership 和独立 scroll；Chart builder 要求 unique visible grain，固定 10-point evidence projection，Report Renderer 对 legacy unsafe Chart 再次 fail-closed。
- 自动验证：Python `553 passed / 24 skipped`；Pi `96 passed`；TypeScript、npm audit、targeted report/Web tests 和桌面 Playwright 通过。桌面 80 条 Flow event 下 body 高度保持 1000px，Report/Publication 同页可见，0 console/page error。
- NAS Chrome 146 实际 PDF 内容扫描不含 `file://`、`/home/`、`forge-m4.1`、`index.html` 或默认日期 header；不是只检查 command/file size。
- 同一真实 Golden Journey 在独立 Store 和 mode-0400 datasource 重跑完成：Task 262.399s，Query 107 rows/31ms，4/4 Attempt succeeded，1 approval/1 execution，重复 Web message HTTP 200 且 QueryRun count=1；PDF/PPTX ready，0 ChartArtifact，same-page completion 可见。
- NAS 已部署并保留回滚点 `~/services/forge-m4.1/backups/h4-p0-20260824T110913Z/`；隔离服务、override 和 tunnel 已清理，生产 health/readiness `ok`。完整证据见 [`golden-journey-p0-closure-2026-08-24.md`](golden-journey-p0-closure-2026-08-24.md)。
- 剩余风险显式转入 P1/H5：当前 0 Chart 是正确的安全降级，不是理想专业报告体验。

### H5：Evidence-bound Chart Storytelling（R0 实施中）

> Requirement：[`REQ-2026-08-24-009`](requirements-pool.md#req-2026-08-24-009专业报告的多图叙事现代图表与证据绑定交互) · 用户已确认第一门

R0 交付范围：

1. 定义独立 `ChartArtifact v2` JSON Schema 与 TypeScript Contract，不修改生产 v1 Artifact consumer。固定 `purpose/grain/unit/encoding/series/transform/annotations/evidence_refs/quality_status`，拒绝 HTML/CSS/script、任意颜色和无 Evidence annotation。
2. 建立两个版本化真实 fixture：
   - 横截面品类比较：支持排名、贡献/结构与 Top-N/Other，可复算且 label 唯一；
   - 时间趋势/多系列：支持趋势、目标线/拐点/异常标注和系列对比，时间 grain 连续可验证。
3. 生成一个自包含 HTML 视觉候选，同一报告内只放回答不同决策问题的 2–4 张图；默认正文已完整，候选交互只做无副作用的 tooltip/focus、series toggle、table fallback 和 Evidence 定位演示。
4. 从同一 fixture/Chart v2 候选确定性生成 PDF/PPTX 视觉候选；静态媒介保留关键 annotation、单位、来源和数据质量状态，不依赖 hover。
5. 输出生产影响与迁移清单：`business-root-cause-analysis`、`data-analysis-report-writer`、Pi Structured Artifact Tool、`skill-executor` Prompt/约束、Skills package revision、Model compatibility、Renderer/Exporter 必须如何同版本切换；R0 只设计和验证 Contract，不修改已固定的生产 Skill/Prompt。
6. 使用 Schema tests、确定性复算、浏览器 DOM/ARIA/console/print、PDF/PPTX 内容与视觉审查建立 R0 evidence pack；由用户确认视觉、图表价值和交互方向。

R0 非目标与门禁：

- 不替换 `buildChartPayload` v1，不修改当前生产报告 revision，不部署候选到 NAS 生产主链。
- 不让模型输出 HTML/CSS/script/颜色；Renderer 只消费结构化语义。
- 不为凑数量重复同一数据；每张图必须声明非重复 `purpose`，并能从 fixture evidence 确定性复算。
- 重复 label、截断结果、未知 unit/grain、Annotation 无 evidence、Top-N/Other 对不上原始总量时，Contract/fixture test 失败关闭。
- 用户未通过 R0 视觉门禁前，不进入 R1 生产 Renderer、完整交互或 Skills/Prompt 修改。
- R1 生产切换时，Analysis/Report Skills、Structured Tool Schema、`skill-executor`、Skills package revision、Chart Contract 和 Renderer 必须作为一个兼容矩阵门禁同步发布；任何一项仍是旧版本则失败关闭。Skill/Prompt 只输出结构化语义与 Evidence，不控制视觉 token、HTML/CSS/script 或颜色。
- 用户在 R0 评审期间提出“报告跨时间复用、更新数据与判断标准”的长期入口，已评估为独立 `REQ-2026-08-24-010 / H6`。H5 Chart Contract 可成为 Definition 的一个依赖，但 H5 不顺带建设 Definition Store、Scheduler 或 rerun 状态机。
- H6 的复用真相源必须是 `SemanticQuerySpec + stable semantic IDs + RegistryBindingSet`，不是旧 SQL 或旧 Prompt。每个 Run 仍保存 CompiledQuerySnapshot 供复现；Forge compatibility planner 确定性选择 `reuse_compiled_sql / rebind_and_recompile / replan_from_semantics / blocked_needs_input`，模型不得自由决定绕过 Assurance/审批。

## 3. M1A：服务身份、Delegation 与默认拒绝（近期，详细）

### 3.1 行为与接口变化

1. Pi 除 health 外的 API 全部要求服务身份；Web、Channel、Admin、Automation 使用不同 credential 和 scope。
2. 浏览器或渠道请求中的 `org_id/team_id/user_id` 不再是授权依据；严格模式从已认证身份和服务端 Identity/Membership 映射生成 `PrincipalContext`。
3. Pi → Forge 请求携带 task-scoped、短期、audience-bound 的 Delegation Envelope；Forge验证服务身份、Task、Mandate、expiry 和 resource scope。
4. TaskRun v2 保存 `principal_context_ref/hash`、`mandate_ref/revision`，不保存 token 或完整认证材料。
5. 企业/生产 profile 改为 deny-by-default；无明确 Policy/Binding 时拒绝准备或执行查询。
6. 错误语义固定：未认证返回 401；已认证但越权按接口风险返回 403 或统一 404，跨租户资源枚举统一 404。

### 3.2 真相源

- Pi Governance 模块：Principal、Membership、Service Identity、DelegatedMandate 和 Task delegation。
- Pi Task Store：Task 状态与 Principal/Mandate snapshot 引用。
- Forge：数据资源 Policy enforcement、QueryRun 和执行审计。
- 不建立通用共享 PDP；统一 Contract，不统一可写数据库。

### 3.3 兼容与迁移

- 现有单用户私有部署保留显式 `legacy_single_user` profile；企业 profile 不允许启用。
- 旧 `tenant_users/team_table_acl` 只作为一次性迁移源，不双写。
- 在途 Task 固定旧 auth context；无法安全映射的 Task 进入 `needs_input/expired`，不静默升级权限。
- 外部 `/api/prepare-query` 的“只准备、不执行”语义保持不变。

### 3.4 M1A 验收门禁

- 修改请求体身份字段不能扩大权限。
- 缺失、过期、跨 Task、跨 audience、跨 Workspace 的 delegation 全部失败关闭。
- 未配置 ACL/Binding 的生产查询被拒绝。
- Web、飞书、钉钉对同一 Principal 得到相同授权结果。
- 重启、超时和重复请求不重放 SQL 或 Decision。
- 完成迁移、回滚、跨租户和枚举负向测试。

## 4. M1B：Membership、资源 Policy 与多 Binding（近期，详细）

### 4.1 交付内容

1. Organization → Workspace/Data Domain → Datasource/Registry 的资源层级。
2. 一人多 Workspace/Team Membership、角色 assignment、有效期与撤销。
3. 最小角色模板：OrgAdmin、DataAdmin、RegistrySteward、ModelAdmin、Analyst、QueryApprover、Auditor、Viewer；角色仅生成默认 Policy，不写死为唯一授权模型。
4. QueryRun 准备时固定 Datasource、Registry、Policy、Principal 和 Mandate snapshot。
5. 表/字段/结果/导出授权；行级策略优先复用数据库 RLS，Forge 不复制客户行级业务逻辑。
6. Report、Audit、Registry Publish、Model Activate 和 Skill Policy 接入对应资源权限。
7. 发布、激活、导出等高风险操作进入 Governance Action Catalog，并绑定精确 Action/Decision。

### 4.2 M1B 验收门禁

- Analyst 不能发布 Registry、激活模型或查看未授权 technical report。
- QueryApprover 只能批准其 Workspace、Datasource 和 Action scope 内的请求。
- QueryRun 的任何 Binding/Policy/Registry 漂移都使原审批失效。
- 跨 Organization/Workspace/Report/Artifact 枚举统一失败。
- 旧 ACL 主路径有明确下线条件，运行时不存在双写授权真相源。

## 5. M1C：OAuth Provider 全 Stage（近期，详细）

### 5.1 交付内容

1. `ModelProfile.execution_backend = api_key | pi_oauth`；OAuth revision 只保存 AuthSlot 引用和非密 capability。
2. refresh/access token 只进入 Pi 专用 mode-600 `auth.json`；Forge Python、Model DB、Task/Event/Artifact 和日志均不能读取或复制 token。
3. 九个 Stage 分别进行 Pi-native compatibility gate 和 CAS Binding；SQL Critical Stage 继续受管理员当前质量门禁策略约束。
4. Forge 签发短期 `QueryPlanningEnvelope`，绑定 Principal、Mandate、Task、ACL 裁剪 Context、Registry/Assurance/Policy revision、context hash 和 expiry。
5. Pi OAuth Session 只能提交 untrusted Forge JSON candidate；Forge 重验 Envelope 后走与 API-key 相同的 Contract、Registry/ACL、Compiler、Assurance、QueryRun 和审批链。
6. Fallback 只允许发生在候选生成前的 auth/quota/rate-limit/provider-unavailable，且必须使用管理员预配置、同 Stage、已通过相应门禁的 revision。

### 5.2 M1C 验收门禁

- 九个 Stage 各有 compatibility test。
- Envelope 篡改、过期、跨租户、Task/Principal 不匹配和 lineage 漂移全部失败关闭。
- Pi OAuth Runtime 无数据库连接；候选不能直接成为可审批 SQL。
- OAuth token、Prompt 和 Tool transcript 不进入日志或 Artifact。
- OAuth 撤销后新 Stage 明确失败或按预配置 fallback 切换；API-key 路径无回归。
- 真实 OAuth 登录和订阅额度消耗只在用户显式授权后进行。

## 6. M2：Economics Ledger 与 Budget（中期，门禁级规划）

先采集事实，再做优化：

1. StageAttempt 和 Forge 查询链记录 provider-reported usage、retry、timeout、tool/DB duration。
2. 建立幂等 Usage Ledger 和版本化 CostCatalog；区分 reported、estimated、billed、subscription、unknown。
3. 按 Organization/Workspace/Principal/Agent/Task/Stage/Model/Tool/Datasource 归因。
4. 增加 Task/Stage Budget 的 reserve/settle；预算耗尽后不启动新模型或工具调用。
5. 记录 Outcome acceptance、人工审核时间和 Cost per Trusted Outcome。

门禁：重放不重复计费；未知成本不伪造；低价模型和预算策略不能绕过权限、Evidence 或 SQL Critical Gate。

## 7. M3：多人—多 Agent Decision 协同（中期，门禁级规划）

1. Task 支持 Owner、Requester、Steward、Approver、Auditor、Viewer、Agent Executor。
2. ExecutionPlan v2 显式包含 actor requirement、authority、evidence、budget、decision 和 deadline。
3. Query approval 可由合法 Approver 完成，不再强制等于 requester。
4. DecisionRequest 固定 ActionRef、Evidence、Policy、风险和 expiry；DecisionRecord 支持 supersedes 和 obligations。
5. 第一版只做单指定 Approver和最小职责分离，不做 BPMN、复杂会签或组织聊天系统。

门禁：Agent 没有最终 Decision authority；Action 变化或 Decision 过期后不能执行；渠道重投不重复执行；新增 Agent 必须通过质量、成本和延迟对照证明净收益。

## 8. M4：平台级 Assurance 与质量闭环（中期，门禁级规划）

1. 定义任务级 QualityContract，区分 G/D/S/E 四类质量。
2. Assurance lineage 组合 Principal、Mandate、Policy、Budget、Context、Decision、Registry、Model 和 Action hash，不把所有状态合并到单一函数或数据库。
3. 建立统一 Audit Event envelope；各领域 Store 仍是业务真相源。
4. 同时报告 accuracy、coverage、clarification、safe abstention、silent error、evidence coverage 和 human override。
5. Failure/Feedback 只形成规则或知识 Proposal，经 Steward 审核后进入 Registry/Policy/Test revision。

门禁：G 类负向测试全部失败关闭；D 类支持范围 100% 回归；S/E 类不使用单一准确率冒充开放世界正确性；能生成不含 Secret/hidden CoT 的 Decision Evidence Package。

## 9. M5：Context/Memory 第二场景实验（远期，粗规划）

前置条件：M1 授权可执行，且存在 Forge 之外的第二消费者。

只选择一个非 SQL 场景，默认优先“运营信息分析”；对比集中 Memory Store、联邦 Context Broker、Registry + Event Store + Adapter 三种方案。测量召回、过期、冲突、权限泄露、纠正/删除、Token、延迟和总成本。

只有两个真实消费者复用同一 Contract，并证明独立服务显著优于现有组合时，才抽 Context/Memory Service；否则保持联邦模式。

## 10. M6：企业运维、HA 与合规交付（远期，粗规划）

在客户负载证明后补齐完整 Forge/Pi 部署拓扑、PostgreSQL 状态后端、对象存储、Worker/Queue、OpenTelemetry、Prometheus、备份恢复、RPO/RTO、KMS/Secret Store、SSO/SCIM、Retention、SBOM/SAST/Secret Scan 和 CI/CD 恢复测试。

门禁：故障转移不重复 SQL/Action；企业 profile 禁止测试 Registry、共享管理员和默认允许 ACL；升级有可验证回滚路径。

## 11. M7：产品边界决策（远期，粗规划）

根据第二场景、第二消费者、客户付费意愿、Contract 复用率和 Cost per Trusted Outcome，三选一：

1. 继续聚焦可信数据任务平台。
2. 抽出 Context/Memory 服务。
3. 演进为更广 Agent Trust Control Plane。

若仍只有 Forge 一个消费者、需要复制全部业务数据、缺少责任主体/买单者，或平台扩张降低可信数据主链质量，则否决通用化。

## 12. 公共接口与类型演进摘要

- 新增 Governance Contract v1 和 Action Catalog。
- TaskRun 从 v1 演进到 v2，增加 Principal/Mandate 引用与 hash；旧数据通过显式迁移读取，不原地伪造身份。
- 普通 Task API 的身份由认证上下文派生，不再信任请求体身份标签。
- Pi → Forge 内部请求增加 Service Identity、PrincipalContext、Mandate 和 Policy/Binding snapshot。
- QueryRun 增加 Datasource/Registry/Policy/Principal/Mandate 固定 lineage。
- ExecutionPlan v2 在 M3 才进入运行时，M0 只定义草案。
- ModelProfile 增加 OAuth backend/AuthSlot；API-key 和 OAuth candidate 最终共享同一 Forge Assurance 服务。
- 外部 `/api/prepare-query` 保持只准备、不执行的兼容语义。

## 13. 全阶段验证矩阵

每个运行时里程碑至少执行：

- Python 全量测试。
- Pi Orchestrator tests 与 TypeScript typecheck。
- `npm audit --omit=dev`。
- JSON Schema/TypeBox parity 和跨语言 Contract tests。
- 跨租户、过期、篡改、重放、枚举和默认拒绝负向测试。
- SQLite migration/reopen/restart、Attempt lease 和幂等恢复测试。
- Web/飞书共享 TaskRun E2E；M1C 增加 OAuth candidate → Forge Assurance → hash 审批 → 只读执行 E2E。
- Secret、hidden CoT、Prompt、token 和内部路径泄漏扫描。
- 文档链接与全局 `git diff --check`。

## 14. 工作包治理、风险与当前不做

每个实施工作包开始前必须写清：

```text
Problem
Axiom
Owner / Truth Source
Contract
Threat / Failure Mode
Migration / Compatibility
Acceptance Gate
Rollback
Observed Cost / Outcome
```

完成后回写实际代码与文档、自动化和人工验证、当前状态、遗留风险与下一步。若实现与本文或 `platform-architecture.md` 冲突，先停止并更新计划，不通过兼容层偷偷改变职责。

当前不做：

- 新增大量 Skills 或渠道。
- 通用多 Agent 市场/编排器。
- 保存全部信息的单一 Memory Store。
- 无边界 CRM/ERP 写操作。
- 全自动组织知识提升。
- 复杂 BPMN 和项目管理套件。
- 在证据不足时对外改称通用企业 AI Infra。

主要风险与控制：

| 风险 | 控制 |
|---|---|
| 一次设计过多 Contract | M0.2 只完整实现 Governance 内核；其余只做草案且不阻塞 M1A |
| 身份重构破坏现有渠道 | Channel Identity 迁移、feature flag、在途 Task 固定旧上下文 |
| 新旧 ACL 双写 | 明确单一 Policy 真相源和旧路径退出条件 |
| 成本数据不准确 | 区分 provider-reported、estimated、billed；版本化 Cost Catalog |
| 多人审批降低效率 | 风险分级，低风险 Policy 自动批准并保留审计 |
| Economics 优化损害质量 | Assurance/Quality floor 是硬约束，成本策略不能绕过 |
| Memory 抽象过早 | 第二场景、第二消费者和对比实验作为前置门禁 |
| 平台愿景拖慢现有产品 | 每阶段必须通过真实数据任务垂直切片，不做无消费者基础设施 |

## 15. 实施顺序与首个工作包

本计划确认后的首个工作包只执行 **M0.1–M0.3**：先更新计划状态和基线，再定义 Governance 内核与 Action Catalog；不修改 Task API、数据库 Schema、授权行为或 OAuth Runtime。完成 Contract 评审后，再分别批准 M1A、M1B、M1C，避免一次性身份重构和大范围回归。

明确假设：

- 采用“近期详、远期粗”，不为远期阶段提前设计全部存储和服务边界。
- 当前优先保护私有单机部署兼容，但企业 profile 必须严格默认拒绝。
- 近期不拆 Governance、Economics、Context 独立微服务。
- 不引入新的通用 Agent、渠道或无边界业务系统写操作。
- 所有真实 OAuth 登录、生产凭证、客户数据源和权限变更均需要用户单独明确授权。
