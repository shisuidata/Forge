# Forge 企业演进阶段性实施计划 v1.2

> 状态：`REQ-2026-09-03-025` 为当前产品主线；`REQ-2026-08-26-024` SQL Accuracy Benchmark 已验证并作为反证基线；`REQ-2026-08-25-023` 已吸收为历史短期切口。当前阶段为 R0 Open-source Trust Runtime Product Cut / Adoption Baseline，R0.1–R0.5 已完成，当前切片为 R0.6 External Adoption Evidence；Runtime Governance Coverage 为 3/14（21.4%） · Last updated: 2026-09-05
>
> 本文是 2026-08-24 起的**唯一主动计划真相源**。历史实施与验收证据保留在 [`pi-forge-integration-plan.md`](pi-forge-integration-plan.md)；目标职责边界见 [`platform-architecture.md`](platform-architecture.md)；产品约束见 [`product-axioms.md`](product-axioms.md)；当前产品决策见 [`requirements-pool.md`](requirements-pool.md#req-2026-09-03-025以开源-trust-runtime-收敛-forge-产品方向)。
>
> 每个工作包继续遵循“先更新计划 → 再实现 → 验证 → 回写状态、风险与下一步”。真实客户数据、生产凭证、权限变化和高风险副作用必须单独授权。

## 0. 摘要与硬约束

长期产品角色保持为企业可信数据平台，近期入口改为面向 Data/AI Engineer 的开源 Trust Runtime。当前不再以完整问数应用、Product Shell 或 Forge JSON 为产品边界，而以 `Evaluate → Enforce → Explain` 建立开发者采用：

```text
R0 Product Cut / Adoption Baseline
→ R1 Evaluate Golden Path
→ R2 Enforce Runtime Gate
→ R3 Explain Evidence Contract
→ R4 Open-source Adoption Gate
→ 基于真实消费者证据重评更广企业能力
```

全程保持以下硬约束：

- Pi 是默认部署中的唯一主 Orchestrator 和 Task 真相源。
- Forge 是唯一可信数据执行层并保留独立校验、拒绝和失败关闭能力。
- Direct SQL 必须成为一等输入；Forge JSON 是可替换 Planner Adapter，不再以其准确率代表 Forge 产品价值。
- 上游 Agent、渠道、Skills 和 MCP Client 不直接获得数据库执行权。
- 高风险副作用不自动重放；不新建第二套任务状态或业务真相源。
- 公共 Benchmark 保留 Official 指标、版本、上下文、失败和方法边界；自有题集与 stars/forks 不得替代外部运行证据。
- 新需求先进入 [`requirements-pool.md`](requirements-pool.md)，完成澄清、评估和用户确认后才能进入本文。

当前产品定位：

> **Forge 是面向企业 Data Agent 的开源可信数据运行时：让既有 Agent 的数据访问可验证、可约束、可追溯。**

采用原则：

```text
GTM：Open-source Developer First
用户：Data/AI Engineer 与数据平台团队
架构：Agent Native
治理：Human Accountable
证据：Real Run Before Platform Expansion
```

质量策略是 `100% Governed` 的受支持过程边界，而不是开放世界端到端 `100% Correct`。Contract Coverage、Runtime Governance Coverage、Statistical Quality 与外部 Adoption Evidence 必须分开报告。

### R0：Open-source Trust Runtime Product Cut / Adoption Baseline（当前唯一主动工作）

**目标**：把现有工程资产切割成外部开发者可独立理解和运行的单一产品路径，不新增平台面来掩盖采用缺口。

**当前范围**：

1. 定义统一输入边界，使既有 Agent 的 Direct SQL/结果与 Forge JSON 都能进入同一 Evaluate、Assurance、Executor、Evidence 和 Audit 链。
2. 固定唯一 Golden Path：“现有 Agent/样例输出 → Evaluate → 失败定位 → Policy Gate → Evidence”。
3. 将 Benchmark 的 Exact Result Comparison、失败分层、lineage 和版本绑定产品化为可复现发布门禁。
4. 收敛 README、Quickstart、CLI/API 和 Dashboard；隐藏或降级与 Golden Path 无关的 Product Shell、报告和未来平台入口。
5. 建立开源采用证据：独立 Quickstart、外部 failure case、Adapter/Rule/Dataset 贡献和下游集成。

**R0 退出门禁**：外部开发者无需理解 Pi、Forge JSON 或内部产品对象，即可从公开入口独立完成 Golden Path；Direct SQL 不需要先转换为 Forge JSON；运行产出可复算结果、失败分类、Policy verdict 和 Evidence；当前仍无真实外部采用时不得宣称门禁通过。

**R0 明确不做**：新增通用 Product Shell 页面、报告 Renderer、SaaS Connector、非 SQL Action、Economics/Outcome Ledger、完整企业身份权限或新的独立 Runtime 服务。

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
| W2 Web 主体内容规则 | 已实施，待用户视觉确认 | 已审计 19 个模板并清理 Chat/Tasks/Registry/全局/登录页宣传与口号；H5 candidate 同步去除候选/Renderer 自述。静态回归和桌面候选通过，用户确认前不标记 verified。 |
| N1 产品北极星沉淀 | 已完成 | `REQ-2026-08-25-015`：`docs/product-north-star.md` 已固定 100% 正确性边界、可执行一致性、Agent-facing Trusted Data Runtime、事实/Evidence/真相源和产品非目标；已接 README/Architecture/AGENTS，未改变 Runtime |
| N2 产品设计与路线重建 | 方向已形成；短期顺序由 REQ-017 修订 | `REQ-2026-08-25-016`：三产品面与对象模型保留；不再由 fixture W3A 主导近期实施 |
| SP0–SP5 短期 Product Spine | 两个 Atlas P0 已验证，等待用户继续复验 | `REQ-2026-08-25-019`：Table 已存在于 Pi Presentation/Product Projection，Web Conversation Renderer 未消费。统一复用 `renderPresentation` 后，真实 107 行任务显示 2 列、20 行有界预览、总行数与截断提示；最终 candidate `product-spine-6a23e71276e5` 以 `product-pages.js?v=2` 强制缓存刷新。性能 P0 保持通过 |
| F0–F2 完整未来 Product Shell | 闪烁 P0 已修复，待用户 Atlas 确认 | `REQ-2026-08-25-022`：Sidebar 语义 fingerprint 排除易变 projection metadata；相同轮询 no-op，真实变化保留 scroll 后更新，失败保留最后有效状态；candidate `product-spine-beb59d1a56f7` |
| R0 Open-source Trust Runtime | R0.1–R0.5 已完成；R0.6 已公开招募，等待外部回执 | `REQ-2026-09-03-025`：公开 revision 提供无需 Pi、Forge JSON、模型 Key 或已有数据库的 Direct SQL Evaluate → Enforce → Explain 路径；远端 fresh-clone bootstrap + Quickstart 内部基线为 36 秒。Quickstart 同时证明 `assurance/readonly_violation` 失败关闭并生成隐私有界回执，[Issue #9](https://github.com/shisuidata/Forge/issues/9) 收集 setup time、摩擦与独立解释。内部 smoke、维护者提交与 stars/forks 均不关闭 R0.6 |
| S0–S4 真实用户短期产品闭环 | 已被 R0 吸收为历史验证路线 | `REQ-2026-08-25-023`：Design Partner、Enterprise Reference 与 Thin Founder Sandbox 的证据分工继续有效，但不再是唯一主动计划 |
| W3 Web 产品骨架与交互框架 | 已吸收进 SP3–SP5 | `REQ-2026-08-24-014`：`821065f` 隔离 Product Shell 已证明页面骨架，但错误移除了连续 Chat；“分析工作台”修订也被判定过窄。先以 N1 北极星重建产品地图，确认后再修订原型；W3B 不进入 |
| H1 Analysis 延迟与进度修复 | 已完成 | `REQ-2026-08-24-005`：Artifact-first Adapter、Provider failure 分类、StageAttempt deadline/phase 时间元数据和 Web elapsed/slow 提示；107 行真实 smoke 从临界 229/240s 降至 119s，不改变 SQL、审批或 Task 真相源 |
| H2 长文本语义化阅读体验 | 已完成并部署 | `REQ-2026-08-24-006`：NAS `9fca1ea` health/readiness/认证门禁通过；隔离 ReportStore HTML/PDF/PPTX exporter 全部 ready，无 SQL/Task 重放 |
| H3 Golden Journey 双验收 | 已完成，产品 FAIL | `REQ-2026-08-24-007`：物理链路 PASS；桌面旅程/可信交付 FAIL。发现 PDF 路径泄漏、same-page Publication 空白、Chart grain 误导 3 个 P0 |
| H4 Golden Journey P0 Closure | 已完成并验证 | `REQ-2026-08-24-008`：真实 NAS PDF 路径清除、same-page Report/Publication 可见、重复 label Chart fail-closed；同一 Golden Journey 复验 262.399s，物理与三个 P0 门禁 PASS |
| H5 Evidence-bound Chart Storytelling | Editorial revision 暂定保留，生产 R1 未批准 | 已改为连续报告并建立受控强调规范；用户要求先保留当前形态，后续再做视觉与语言精修。Atlas 只发布隔离静态预览，不等于接入生产 Renderer |
| D1 Atlas 隔离报告预览 | 已完成 | `REQ-2026-08-24-013`：`929e8d4` 固定构建物独立发布到 `preview.internal.invalid:18005`；生产源码仍为干净 `d2b0fd9`，Forge/Pi 未重启；阶段差距重评估完成 |
| H6 Reusable Report Definitions | 延后 | `REQ-2026-08-25-023`：不属于“随时问真实业务数据 → 可信答案 → 语义复用”的首个短期闭环；S4 通过后按真实重复交付需求重评 |
| M1A–M1C | 延后，未批准 | M0 Contract 已允许提出 M1A，但当前先验证 Human/Data-Team 产品闭环；S4 通过后再决定 Runtime Trust Foundation 与单一 Agent Consumer |
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

## N1：产品北极星沉淀（当前先行文档工作包）

> Requirement：[`REQ-2026-08-25-015`](requirements-pool.md#req-2026-08-25-015沉淀-forge-产品北极星指导文档) · 决策：`accepted`

本工作包只沉淀并接入战略指导，不修改 Runtime、API、数据库、Web 原型或生产部署：

1. 新增 `docs/product-north-star.md`，明确产品存在理由、定位层级、100% 正确性边界、可执行一致性、Agent 数据事实能力、信息分型、参与者、核心对象、四平面、产品投影和非目标。
2. 明确“其他 Agent 的数据底座”是受控 Data Runtime/Trust Boundary，不是复制所有业务事实的单一数据库；原系统继续持有业务真相，Forge 提供语义、权限、Evidence、Assurance 与可信 Action。
3. 明确 Conversation 是人的连续交互面，Task/Artifact/Evidence/Decision/Action 是协同真相源；该原则用于下一轮 W3 Product Map，但不在 N1 内提前冻结一级导航。
4. 将文档加入 README，并在 `AGENTS.md` 中设为产品、体验、架构与商业方向判断的必读依据；旧“查询 Agent”描述降为当前结构化查询验证切片，不再代表完整产品边界。
5. 运行文档链接、静态内容和 `git diff --check`；回写实际结果后，W3A 才可继续提出修订产品地图。

### N1 实施结果（2026-08-25）

- 新增 [`product-north-star.md`](product-north-star.md)：明确北极星命题、Data Agent/Trusted Data Runtime/长期假设三层定位、四类质量、Silent Error、合法可执行一致性、Source/Event/Claim/Evidence/Knowledge/Decision/Action 分型、Agent Runtime、Conversation/Task、四平面和非目标。
- README 与 `platform-architecture.md` 对齐为“面向数据团队建设、供人和企业 Agent 使用的可信数据运行时与数据任务控制/执行平台”；结构化查询仍是第一验证切片，不声明开放世界 100% 正确。
- `AGENTS.md` 将北极星加入产品/体验/架构相关任务必读清单，并移除“生成错误物理上不可能”“Registry 消灭业务逻辑错误”等绝对化旧表述。
- 文档链接与 Web 内容静态测试 `4 passed`；北极星 8 项必要主题断言及 `git diff --check` 通过。未修改 Runtime、API、数据库、Web 原型或部署。
- N1 只提供 W3A 复核依据，不自动冻结新导航。下一步仍需提出并由用户确认 Conversation、Task、Decision、Delivery、Data Governance 和 Agent Runtime 的 Product Map。

## N2：基于北极星重建产品设计与阶段计划（方向提案已完成，短期顺序由 REQ-017 修订）

> Requirement：[`REQ-2026-08-25-016`](requirements-pool.md#req-2026-08-25-016基于产品北极星重建产品设计与阶段计划) · 当前只批准规划，不批准实现

本工作包输出一份独立、可门禁的产品设计与路线提案：

1. 以 Human Control Plane、Agent-facing Trusted Data Runtime 和共享 Trust/Data Foundation 三个产品面重新定义 Forge，不以单一 Chat、分析页面或治理后台概括产品。
2. 明确 Conversation、Task、Plan、Decision、Artifact、Evidence、Data Asset、Agent Client 和 Outcome 的关系、Owner、真相源和当前实现差距。
3. 提出按角色分组的信息架构、深链接 Route、核心 Human/Agent/Steward Journeys，以及 loading/needs_input/waiting_decision/partial/failed/forbidden 等状态投影。
4. 重新排序 W3A/W3B、M1A、Agent Runtime MVP、Data Trust Control Plane、平台 Assurance、Reusable Deliverables、Economics/Coordination 和第二场景；每阶段必须有证伪门禁与不做项。
5. 标明旧 `web-product-shell-plan-2026-08-24.md` 和 `821065f` 原型中可保留、需删除和必须重建的部分；不修改现有原型和生产 Web。
6. 用户对提案给出 `PASS / CHANGE / REMOVE` 后，才回写批准后的实施顺序并启动下一版 W3A。

### N2 提案结果（2026-08-25）

- 新增 [`product-design-roadmap-2026-08-25.md`](product-design-roadmap-2026-08-25.md)，将 Forge 重建为 Human Work Surface、Shared Trust & Data Foundation、Agent-facing Trusted Data Runtime 三个共享真相链的产品面。
- 定义 Workspace/Principal/Agent Client/Conversation/Task/Plan/Decision/Action/Artifact/Evidence/Data Asset/Deliverable/Outcome 的产品关系、Owner、当前真相源和实现缺口。
- 提出按“工作 / 信任 / 接入 / 系统”分组的 IA、Route 兼容、H1/H2/A1/S1/F1 五条 Journey、统一状态语言、旧 W3A 的保留/修改/删除清单和质量/Agent 指标。
- 初版曾建议 W3A.2 隔离原型 → W3B Human Control Plane → M1A/R1/G1/Q1/H6；用户随后通过 `REQ-2026-08-25-017` 明确改为底层 Product Spine 先行，初版短期顺序不再生效。
- 旧 [`web-product-shell-plan-2026-08-24.md`](web-product-shell-plan-2026-08-24.md) 已标记为历史第一版；N2 本身未修改 `tools/web-product-shell-prototype/`、生产 Web、Runtime、API、数据库或部署。
- 文档链接/内容测试 `4 passed`，提案 8 项必要结构断言与 `git diff --check` 通过；长期 Agent/Governance 阶段仍需后续真实测试证据，不由 N2 自动批准。

## SP0–SP5：短期 Product Spine（自动门禁通过，待用户 Atlas 确认）

> Requirement：[`REQ-2026-08-25-017`](requirements-pool.md#req-2026-08-25-017短期-product-spine-底层优先实施计划) · 详细计划：[`short-term-product-spine-plan-2026-08-25.md`](short-term-product-spine-plan-2026-08-25.md)

用户确认近期不再以隔离 fixture 原型作为主路径，而是先完成足以支撑真实产品 Journey 的底层框架，再接 Web Product Shell：

```text
SP0 Projection Contract / truth source closure
  → SP1 Pi Conversation + Task Product Projection
  → SP2 Web Product BFF + scoped Report Index
  → Backend Gate
  → SP3 local-asset Product Shell
  → SP4 real Workspace / Conversation / Task / Report / Data pages
  → SP5 real Human Golden Journeys + isolated Atlas candidate
```

短期范围保持单用户私有化 Human Control Plane。Conversation 从现有 TaskRun/ChannelEvent 关系只读投影，不增加可写 Conversation Store；Task/Approval/Report 继续由 Pi/Forge/ReportStore 持有；Agent Runtime、完整 Decision Runtime、Economics、多 Workspace、Reusable Report 和 H5 Renderer 不并行实施。

### SP0 验证结果（2026-08-25）

- TypeBox + generated JSON Schema + Python semantic gate 在 SP0 固定六类 Product Projection v1；SP2 为真实 Task list BFF 补充同版本 `TaskSummaryV1`，不允许前端消费 raw TaskRun。共享 fixture 持续覆盖关键产品状态和结构/语义负向边界。
- Pi `108 passed`；Python 全量 `567 passed / 24 skipped`；SP0 Python 定向最终 `19 passed`；typecheck、npm audit、JSON parse、schema sync、`git diff --check` 通过。
- 未改 DB/API/Task state/UI/deployment。详细契约见 [`product-projection-contracts.md`](product-projection-contracts.md)。

### SP1 验证结果（2026-08-25）

- 从真实 Pi Store 构建 Conversation/Task Product Projection，增加 authenticated/no-store read API、opaque pagination、scope/lineage/redaction/2 MB boundary 与 restart recovery。
- 10K Task Conversation list 从首版约 19,040 ms 优化到平均约 26.8 ms；使用单次 window CTE 消除相关子查询/N+1，没有提前增加 schema v5。
- Pi `114 passed`、Python `569 passed / 24 skipped`、typecheck/npm audit/`git diff --check` 通过；正式证据见 [`product-spine-sp1-evidence-2026-08-25.md`](product-spine-sp1-evidence-2026-08-25.md)。
- 未新增 Conversation Store、DB Schema、Task 状态或 Web 页面。SP2 当前开始。

### SP2 验证结果（2026-08-25）

- ReportStore scope-aware list/index 与 authenticated Product BFF 已完成；Workspace 可在 Pi/Report/Registry 失效时 partial/offline，不复制 Task/Report 状态。
- 增加同版本 `TaskSummaryV1`，BFF 执行二次 Contract/scope gate、去敏、bounded 和 no-store；Registry revision 使用内容 hash。
- Python `575 passed / 24 skipped`、SP2 定向 `34 passed`、Pi `114 passed`、typecheck/npm audit/`git diff --check` 通过；证据见 [`product-spine-sp2-evidence-2026-08-25.md`](product-spine-sp2-evidence-2026-08-25.md)。
- Backend Gate PASS，SP3 当前开始；仍无 Product 页面或部署变更。

### SP3 验证结果（2026-08-25）

- 新增 local-only Product Shell template/CSS/JS 与 `/static` mount；短期导航、共享状态、focus/reduced-motion/mobile navigation 基础完成。
- 0 CDN、0 inline style/script、0 fixture、0 data fetch/localStorage；现有 Admin 未被一次性重写。
- Python `581 passed / 24 skipped`、SP3/Docs `10 passed`、Pi `114 passed`、typecheck/npm audit/`git diff --check` 通过；证据见 [`product-spine-sp3-evidence-2026-08-25.md`](product-spine-sp3-evidence-2026-08-25.md)。
- SP4 当前开始，首次将 Shell 接到真实 Product BFF。

### SP4 验证结果（2026-08-25）

- Workspace/Conversation/Task list+detail/Report Library/Data 页面已接 Product BFF；Chat/Task action 复用原 typed ChannelEvent endpoint。
- 任务详情首轮视觉审查的主审批可发现性、pending 状态和层级 P0 已修正；复审无 P0。
- Python `583 passed / 24 skipped`、Product/Web/Docs `33 passed`、Pi `114 passed`、typecheck/npm audit/JS syntax/Playwright 双桌面 viewport/0 error/0 overflow/`git diff --check` 通过；证据见 [`product-spine-sp4-evidence-2026-08-25.md`](product-spine-sp4-evidence-2026-08-25.md)。
### SP5 验证结果（2026-08-25）

- Candidate `product-spine-5dcd4715941a` 已使用独立 Pi/QueryRun/Report/Registry/Artifact 状态和 candidate 内 mode `0400` 只读数据副本，认证开启；生产 Forge/Pi 未替换。
- 固定渠道指标问题在最终配置下连续 3 次完成真实 Conversation → SQL Review → 单次只读执行 → Analysis → Report → Report Library；每次 1 个 QueryRun/`query.completed`、4 个 succeeded StageAttempt，PDF/PPTX ready。
- 重复消息返回原任务，过期重复批准 409 且不重放 SQL；等待审批/完成态 restart recovery、Pi offline partial、双桌面 12 routes/0 external request/0 error/0 overflow 通过。
- Live Gate 修复 insecure-HTTP ID、瞬时 ready 轮询、同源 HTTP Report URL、空 Attempt error、长 SQL Grid overflow 和完成态历史审核误标权限；复杂查询 Assurance 拒绝与 Analysis `incomplete` 作为反证保留。
- Python `583 passed / 24 skipped`、Pi `115 passed`、typecheck/npm audit/JS syntax/`git diff --check` 通过；证据见 [`product-spine-sp5-evidence-2026-08-25.md`](product-spine-sp5-evidence-2026-08-25.md)。

每个工作包单独门禁。SP0–SP5 自动门禁现已通过，但用户尚未给出 Atlas `PASS / CHANGE / REMOVE`，因此 Product Spine 不能标记为最终接受，也不能据此自动启动 M1A、G1、Q1 或 H6。

## F0–F2：完整未来 Product Shell（已实施，待用户确认）

> Requirement：[`REQ-2026-08-25-020`](requirements-pool.md#req-2026-08-25-020按未来产品方向补全前端产品面)

用户选择“完整未来产品壳”。本工作包只建设前端产品地图、页面职责、稳定 Route、状态与现有能力入口，不据此批准或实现 Agent Runtime、通用 Decision、Policy/Mandate PEP、Economics、Reusable Definition、Outcome Ledger 或多用户治理。

### F0 Product Map / Shell

- 一级导航固定为：
  - 工作：工作台、对话、任务、交付；
  - 信任：数据资产、治理与审计；
  - 接入：Agents & Apps；
  - 系统：管理。
- 增加稳定聚合 Route：`/deliverables`、`/governance`、`/runtime`、`/manage`；现有 `/reports`、`/data`、`/admin/*` 保持兼容。
- 所有未来能力显示机器可读/可样式化的 `available / partial / planned / blocked` 状态、依赖阶段和 disabled 原因。
- 复用现有 Product Shell、design token、键盘/焦点、reduced motion、移动导航、local-only asset 和 no-store 规则。

### F1 Existing Truth Wiring

- 交付页接真实 scoped Report Library、Export readiness、Task/Report lineage；Reusable Definition 保持 planned。
- 治理页接现有 Query Approval/Audit、Registry Revision、Model/Skill/Channel/System 管理入口；通用 Decision/Policy/Mandate 保持 planned。
- 数据资产页组织现有 Datasource/Schema/Metric/Semantic/Relationship/Knowledge/Registry Studio 入口；Quality/Freshness/Conflict/Proposal 保持 planned。
- 管理页组织现有 Workspace/Team、Model、Skill、Channel、Database 和 readiness 入口，不复制 Admin 状态。

### F2 Future Surface

- Agents & Apps 页面展示未来 Agent Client、Owner/Purpose、Mandate、Capabilities、Task/Artifact consumption 和 Human takeover 的真实信息结构；M1A/R1 前所有执行与 credential Action 为 blocked。
- Evidence & Assurance、Decision Inbox、Policy/Mandate、Quality/Conflict、Reusable Deliverable、Outcome/Feedback 页面只展示对象职责、依赖和 planned/blocked 状态，不生成记录。
- 每个 disabled Action 必须说明“缺少什么 Runtime/Contract”，不能只显示灰按钮。

### 验收门禁

- 所有一级和子级 Route 可深链接、刷新、后退；1440×900、1600×1000 和 390px 无横向溢出、0 console/page error。
- 当前可用能力最多两次导航到达；未来能力可发现但不会被误认成已上线。
- 0 fixture 业务记录、0 死按钮、0 前端业务状态库、0 新 Task/Approval/Report 真相源。
- 现有 Conversation → Task → SQL Review → QueryResult Table → Analysis → Report 主链不回归。
- Product Shell 与旧 Admin 保持兼容；只重新组织入口，不在本工作包把所有 Admin 页面换皮。

### F0–F2 实施结果（2026-08-25）

- Product Shell 已按“工作 / 信任 / 接入 / 系统”组织 8 个一级入口；Report 近期产品名升级为“交付”，`/reports` 保持兼容。
- 新增交付、治理、Agent Runtime、管理、搜索、待办及 11 个子产品 Route；页面使用同一 capability-aware 模板和 `available / partial / planned / blocked` 状态。
- Existing Truth Wiring：
  - 交付 → scoped Report Library、PDF/PPTX readiness；
  - Decision/Evidence → Workspace/Task/Query Approval/Audit；
  - Data Trust → Schema/Metric/Semantic/Registry Studio/Staging/Knowledge；
  - 管理 → Team/Model/Skill/Channel/Database/Readiness。
- Future Surface 不生成业务记录：Agent Client/execute/credential、通用 Decision、Policy/Mandate PEP、Quality/Freshness、ConflictSet、Reusable Definition、Outcome/Feedback 均明确依赖阶段和不可用原因。
- 增加默认 Workspace context、Search、Inbox、Evidence Drawer/Diff Viewer 产品边界，以及统一 Product Shell 404/403/offline 页面；未知产品 Route 返回 404 且不泄漏对象存在性。
- Candidate `product-spine-beb59d1a56f7` 使用原独立 Pi/QueryRun/Report/Registry/Artifact 状态和 mode `0400` 只读测试数据；生产 Forge/Pi 未替换。
- 自动验证：资源/页面契约 `11 passed`、Product Conversation 浏览器行为 `2 passed`、JS syntax PASS。
- Chat Sidebar 使用语义 fingerprint 稳定刷新：相同 Task/Projection 轮询不替换 DOM；真实 Plan/Action/Artifact/Activity/Review 变化才更新并恢复 scroll；已有有效内容时刷新失败不覆盖。
- 真实浏览器：侧栏、移动 Drawer、QueryResult Table、1440×900/390px、0 console/page error、0 横向溢出保持通过；Product Pages JS 已提升为 `v6`。
- 本工作包只完成前端产品面和现有入口；Runtime Governance Coverage 仍为 0%，任何 blocked capability 都没有执行路径。

## S0–S4：真实用户驱动的短期产品闭环（历史验证路线）

> Requirement：[`REQ-2026-08-25-023`](requirements-pool.md) · 决策：`accepted_with_changes`

短期产品定义：

> **面向已有数据库/数仓的小型数据团队的可信业务问数助手；不要求先完成完整数据治理，在真实提问中逐步沉淀和复用业务语义。**

长期“可信数据与知识底座”方向不变，但短期不再把未来企业对象、完整 Product Shell 或内部 Semantic Gap 机制当作用户产品。核心 Job 是：

```text
连接一个现有可查询数据源
→ 提出真实业务问题
→ 只澄清会改变结果的最小语义
→ 只读可信执行
→ 直接获得业务答案、表格和限制
→ 按需查看口径、SQL、数据范围与 Evidence
→ 纠正并安全复用已确认语义
```


### S0-B SQL Accuracy Benchmark 观测（已批准实施）

- Requirement：`REQ-2026-08-26-024`。使用现有 Ark Coding Plan `method_ai` 与 large 40 题 Enterprise Reference 数据集重跑 SQL Accuracy Benchmark。
- Benchmark Runtime 持久化 run/case/call 状态；Web 页面只读订阅同一状态，实时显示 partial/final EA、Run Accuracy、编译成功率、分类成绩、延迟和失败。
- 固定默认参数：40 题、每题 3 次、最多 2 次编译修复；运行绑定方法、数据集、模型 revision、Registry/code lineage。
- 页面必须区分部分成绩与最终成绩，明确该结果不能代表开放世界或真实客户 SQL 100% 准确。
- 不读取真实客户数据，不修改模型绑定，不暴露 Secret，不创建第二测试真相源；进程中断后标记 interrupted，不自动重放模型调用。
- **退出门禁**：Web 可启动 run；状态与成绩无需刷新实时同步；重连恢复持久 snapshot；定向契约测试和真实浏览器运行均通过。

**实施结果（2026-08-26）**：工作包已完成。Accuracy Lab 通过持久 SQLite Benchmark Store + SSE 只读投影实时同步；真实 Ark Coding Plan run `abr_b410ab2b05ef40d88050b1b9be1eb097` 完成 120/120 calls、40/40 cases，Case EA 100.0%、Run Accuracy 98.3%、Compile Success 100.0%、P95 58,945 ms。Case 23 与 38 均为 2/3，页面保留 mixed 与有界错误说明。61 个定向回归通过，桌面/移动浏览器与服务重启恢复通过。结果仅代表固定 Enterprise Reference + 当前 dirty code/model/Registry lineage。

### S0-B2 Hard Benchmark 双臂对照与结果可解释页面（已批准实施）

- 在 S0-B 基础上新增 BIRD-SQL 官方 hard 诊断集；题目、Gold SQL、Evidence、Schema 和 SQLite 数据均保持官方来源。
- Forge 与 Direct SQL 共享 Ark Coding Plan、问题、结构层、Oracle Evidence 和数据库；路径专属系统提示与 Forge 编译修复预算属于被测方法差异，必须显式披露，不能声称上下文完全相同。
- 主评分严格使用 BIRD Execution Accuracy：两个 SQL 在同一 SQLite 数据库执行后比较精确结果 tuple 集合；不比较 SQL 文本，不允许数值容差或文本归一化。Execution Success 与延迟单独报告。
- 重复运行时，Mean EA 是每次生成的官方 EA 均值；First-run EA、Pass@K、Consistent@K 分开命名，Pass@K 不得再标为 Case EA。
- 持久化 method/case/stage 日志、生成 SQL 和安全结果摘要；Web 实时展示双臂进度、成绩、差异与逐题详情。
- **退出门禁**：诊断集来源与抽样边界可审查；全部 Gold SQL 可执行；双臂真实 run 完成；实时日志、逐题 SQL/结果查看、重连恢复和桌面/移动浏览器验证通过。完整公共成绩必须覆盖 Mini-Dev 500 题与 11 个数据库；12 题子集不得用于 leaderboard 或泛化声明。

**EA 审计修订（2026-08-26）**：已确认当前 12 题字段与官方记录完全一致，但样本只占 challenging 的 12/102、数据库 2/11，且原选择规则不能解释同两库另 6 道可执行非空题为何未入选，因此降级为诊断子集。NAS run hbr_9a78d73cc64642709b03d4dc8aef978a 按官方 exact-set EA 重算：Forge 5.56% (2/36)，Direct SQL 27.78% (10/36)，Direct 领先 22.22pp；旧近似比较器造成 11 个假阳性，旧的 30.56% / 33.33% 结论作废。下一公共验证门禁是完整 Mini-Dev 500 题一次生成；102 道 challenging 只作难题切片，3-run 指标只作稳定性分析。

**修订部署证据**：NAS commit e076573，API/Pi active；目标回归 8 passed，前端脚本语法通过；最新 run hbr_c99bb3d506f54a25b528d191c3955944 独立重执行与存储 verdict 一致（Forge 2/36，Direct 11/36）。三轮完整运行聚合 Forge 7/108 (6.48%)、Direct 28/108 (25.93%)，仍只作为 12 题诊断样本证据。

**完整数据看板部署（2026-08-26）**：NAS commit 4056986。完整 Mini-Dev 500 题、11 个数据库与 1000-call 运行契约已接入；启动 API 要求 confirm_model_calls=1000，未确认返回 409。页面改为克制的浅色实时看板，包含历史/累计 EA、延迟分布、结果构成、逐题筛选、详情弹窗和日志筛选分页。NAS 回归 9 passed，桌面 1440px 与移动 390px 无页面级横向溢出；部署前后保持 6 runs、246 observations、0 active，未触发模型测试。

**启动阻塞修复（2026-08-26）**：完整套件首次启动卡在 run 创建前。根因是 create_hard_run 同步执行全部 500 条 Gold SQL，阻塞 FastAPI 事件循环；execute_result 无 progress timeout 且 Connection context 不负责 close。修订后 POST 先落 queued run 并立即返回，Gold 预检在后台 worker 中执行，4-way 并行、单 SQL 30 秒超时、失败取消剩余任务、每 10 题持久日志；预检失败关闭且模型调用为 0。NAS 11 passed，health 3.5ms，未确认启动门禁 31ms；原请求未落库，恢复后仍为 6 runs / 246 observations / 0 active。

### S0-B3 Pi-native RAG 双 Sub-Agent Benchmark（已批准实施）

- Pi 创建根 TaskRun 并持有新 Benchmark Run、Case、日志和控制状态；Python 旧 Runtime 仅保留 GET 历史投影，POST 启动返回 410。
- 每个 Case 先由 Forge 内部受认证 API 构造字段级有界 RAG ContextSnapshot 和 ResultContract；同一 hash 并行交给两个独立 Pi AgentSession，分别生成 Forge JSON 与 Direct SQL。
- Provider 与 Model 在运行前从 Pi ModelRuntime ready catalog 选择；运行开始后绑定不可变 provider/model/revision。默认推荐 deepseek-v4-flash，但不硬编码且不修改生产 ActiveBinding。
- Forge 负责 JSON 编译、只读 SQL 校验、执行、Official EA 与 ResultContract Accuracy；Pi 持久化生成时延、prompt/completion/cache tokens、compile/execute/error 和双分支日志。
- 页面投影当前问题、模型、进度、DAG、RAG rounds、并排双日志、实时 Case 表、准确率/Token/速度/失败/维度图表，支持 pause/resume/stop。
- 生产 Canary：openai/deepseek-v4-flash 固定 revision 已完成 2-case、1-case、pause/resume 3-case 和 stop 3-case 验证；Pause 时 2 Case 完成、1 Case 保持 pending，Resume 后恰好 6/6 calls 且无重复；Stop 后 4 calls 封存、1 Case pending。
- 完整验收 Run pbr_1f735d433a284366bfe6526146511792 已完成 500/500 cases、1000/1000 Sub-Agent calls；模型 openai/deepseek-v4-flash revision sha256:f75be09a。固定 500 分母：Forge Official EA 45.40%、Contract Accuracy 39.80%、Execution Success 73.00%、3,506,756 tokens、平均生成 29.11s；Direct 分别 56.40%、50.80%、91.20%、2,386,708 tokens、16.46s；Forge Delta -11.00pp。生产 head c7eb9da，API/Pi active，源码 clean。
- GPT-5.6 理论复验 Run `pbr_76da9a18d96c4e13b2b810ba111bd599` 已完成 500/500 cases、1000/1000 新调用；Pi AgentSession 使用 `openai-codex/gpt-5.6-sol` OAuth，固定 revision `sha256:64aabcc80506d63ad711bdc89b9f3a29fe8a275d62dab5b97c04d5af222724a0`、temperature 0、max output 8192。Forge Official EA 53.20%、Contract 48.00%、Execution 94.40%、5,443,601 tokens、平均生成 9.27s；Direct 分别 62.20%、56.80%、99.80%、3,558,117 tokens、6.65s；Forge Delta -9.00pp。配对结果 Forge only 22、Direct only 67，双侧 exact p=1.90e-6；本轮不支持 Forge JSON 生成准确率高于 Direct SQL 的假设，不改变 R0.6 外部采用门禁。
- Structured GPT-5.6 完整复验 Run `pbr_6778bf9d34ae42fba0b070a5f9c154ba` 已完成 500/500 cases、1000/1000 新调用；500/500 ContextSnapshot 与历史 Run 一致，Forge 500/500 取得 schema-bound tool 对象。运行时封存为 Forge 287/500 EA、257/500 Contract、439/500 Execution，Direct 为 314/500、291/500、500/500；随后对相同 Forge 对象修复确定性 Compiler 缺陷并离线重评为 313/500 EA、280/500 Contract、496/500 Execution、500/500 Compile，零 EA 回退。最终配对 Forge only 22、Direct only 23，exact p=1.0，支持统计持平而非 Forge 优势；Forge 仍多 59.57% tokens、51.69% 平均生成时间。该理论证据不替代 R0.6 外部采用门禁。

### S0 Design Partner 与问题基线（历史验证路线，证据分工保留）

- 选择一个已经拥有数据库或数仓、存在持续临时问数需求、语义治理尚不完整的小型数据团队。
- 第一阶段固定一个数据 Domain、一套现有可查询数据源、一名可确认业务口径的 Owner/Steward 和一组真实历史问题；后续问题必须来自真实工作，不由 Forge 团队为演示反向设计。
- 明确私有化、数据不出域、最小权限、允许记录的有界 Evidence、禁止读取/回显的 Secret/PII，以及退出和删除边界。
- 建立当前人工流程基线：问题如何进入、由谁写 SQL、澄清几次、如何复核、结果如何交付、哪些错误曾静默发生。没有该基线，不能证明 Forge 提效。
- **S0 退出门禁**：Design Partner、Domain、Datasource、Owner、真实问题 corpus、隐私/授权边界和现状基线均明确；否则停止，不以 Demo 或个人低频数据替代。

### S1 Direct Trusted Answer（计划候选，未批准实现）

- 围绕 S0 的真实问题审计现有 Conversation → Task → Query → Result 链，只修复阻碍“直接可信答案”的最小缺口。
- 默认产品体验是答案、数据表、关键限制和继续追问；Task/DAG、SQL、Assurance、Registry revision 与 Audit 按需展开，不默认要求用户理解内部架构。
- 不要求先维护完整 Registry；只有会实质改变结果的缺口进入澄清。低风险只读路径是否使用有界预授权，必须由 S0 真实摩擦和安全评估另立需求决定，不能在本计划中预设绕过精确审批。
- 默认不生成完整报告；报告仅在用户交付物明确要求时进入 Plan。
- **S1 退出门禁**：目标用户能在一个现有数据源上完成真实问题，获得可理解且可追溯的答案；歧义和 Evidence 不足时诚实停止；页面不制造第二 Task/Evidence 状态。

### S2 Semantic Learning Loop（计划候选，未批准实现）

- Conversation 中的业务纠正先形成 task-local binding，用户明确选择后才进入 Domain-level Proposal。
- AI 可以提取来源、比较定义、生成 Diff 和影响分析；不能自行发布组织知识、覆盖冲突定义或扩大作用域。
- Owner Review/Publish 生成 Registry revision，并进入 Forge IR、Assurance、Approval、QueryRun 和 Evidence lineage。
- 第二次相关任务应减少重复澄清；Purpose、Domain、Datasource、Grain 或 revision 不匹配时重新绑定。Schema/语义 drift 后旧审批失败关闭。
- **S2 退出门禁**：至少一项真实语义纠正被后续真实任务正确复用，并有一项不安全复用被系统阻断。

### S3 三环境验证（计划候选，未批准实现）

| 环境 | 唯一责任 | 不得冒充 |
|---|---|---|
| Design Partner | 真实问题、真实责任、重复使用和产品价值 | 不能要求客户承担破坏性压测或泄露数据 |
| Enterprise Reference Workspace | Ground Truth、复杂 Join、规模、脏数据、漂移、冲突、权限和负向回归 | 不能证明目标用户愿意使用 |
| Thin Founder Sandbox | 在无需定制采集时补充交互、纠正和 Evidence 体感 | 不能证明企业代表性，也不驱动 Connector/ETL |

Forge 当前从可查询数据库/数仓开始，不承担外部内容、支付、广告或 SaaS 平台的通用采集、身份统一和 Attribution。多个真实 Partner 因相同接入阻断无法采用时，再单独评估 Connector Contract 或现有生态集成。

### S4 短期产品退出门禁

- 目标用户在没有演示脚本或 Forge 团队推动的情况下，再次提出新的真实问题。
- 至少一次语义确认减少后续任务的澄清或维护成本。
- 至少一次歧义、证据不足、权限问题或 drift 被正确停止，没有以完整语气输出 Silent Error。
- 用户可以从答案回到采用口径、数据范围、SQL、Datasource/Registry revision 和 Evidence。
- 额外治理与审批成本能够由复用、错误减少或交付效率解释；否则收缩机制而不是继续增加治理页面。

原 S0–S4 路线在 S4 通过前暂停新增 Product Shell、通用 Decision Center、Economics/Outcome Ledger、Reusable Report、更多渠道、非 SQL Action、通用 SaaS Connector、M1A Runtime PEP 和 Agent Runtime；现由 R0 门禁承接近期优先级。已有功能的安全事故、数据损坏和已确认行为回归仍可按需求池规则修复。

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

## W2：Web 页面主体内容规则（跨切片门禁）

> Requirement：[`REQ-2026-08-24-011`](requirements-pool.md#req-2026-08-24-011web-页面只呈现主体内容禁止宣传口号与营销文案) · 决策：`accepted`

实施顺序：

1. H5 ECharts focused candidate 先删除所有实验、宣传、营销和 Renderer 自我说明，只保留当前报告的标题、摘要、数据状态、决策内容、Evidence 与操作。
2. 审计 `web/templates/` 的最终用户页面，区分业务主体、必要帮助与营销文案；输出命中清单，不靠宽泛关键词直接批量替换。
3. 对确认命中项做小范围替换，保持路由、Task、Artifact、审批、身份和导航行为不变。
4. 增加已拒绝短语的静态回归与桌面首屏视觉检查；开发文档、架构论证和明确开发工具不属于终端用户页面。
5. 完成后回写命中项、未修改项及理由；在审计和视觉门禁完成前，状态不得标记为全站 `verified`。

实施结果：

- 已审计 `web/templates/` 19 个模板及 Web 暴露的 Architecture Atlas，命中与保留理由见 [`web-product-content-audit-2026-08-24.md`](web-product-content-audit-2026-08-24.md)。
- 已清理 Chat slogan/营销空状态、Tasks integration/架构宣传、Registry 控制面 eyebrow、全局与登录页产品描述，以及 Architecture Atlas 中的产品主张；管理员技术状态、架构事实与审批/DDL 风险说明保留。
- H5 candidate 删除“可信数据报告”、英文氛围标签和 Renderer/版本/候选说明；业务 Evidence 与报告限制保留。
- `tests/test_web_product_content.py` 与相关 Web 定向测试共 76 passed；桌面 H5 candidate 0 browser errors。等待用户视觉确认后再标记 verified。

## W3：Web 产品骨架与可人工测试交互框架（当前唯一主动方向）

> Requirement：[`REQ-2026-08-24-014`](requirements-pool.md#req-2026-08-24-014web-产品骨架与可人工测试交互框架优先) · 决策：`accepted`

### W3.1 Problem 与原则

当前 20 个 Jinja 模板具备零散能力，但导航平铺最终用户、管理和开发入口，Chat、Tasks、Admin、Report 使用多套视觉语法；Dashboard 偏系统健康，Task 没有可寻址详情，Report 没有 Library。用户无法通过稳定产品框架持续人工测试和指导方向。

采用 `Interaction-first, Contract-backed`：Web 可以先搭完整页面骨架和状态，但 Task、Artifact、QueryRun、Approval、Report 与 Registry 继续使用原真相源；演示数据只进入隔离原型并明确标记；生产中的按钮必须真实可用或 disabled 并说明原因。

### W3.2 第一版目标信息架构（已被用户门禁退回）

以下结构只保留为 `821065f` 原型的历史设计依据，不再作为下一版已确认 IA。N1 北极星完成后，W3A 需要重新提出 Conversation、Task、Decision、Delivery、Data Governance 和 Agent Runtime 的产品投影并再次获得用户确认。

- **工作台**：待处理、进行中任务、最近报告、阻断状态；
- **新建任务**：对话提出问题、补充目标和选择交付物；
- **任务**：Task inbox 与可寻址 Detail，组织计划、补充输入、SQL 审批、结果、分析、报告和活动；
- **报告**：Library、Detail、下载/分享；H6 前不伪造 Reusable Definition；
- **数据资产**：Schema、Metrics、Semantic、Staging、Registry Draft/Revision；
- **管理**：Team、Audit、Model、Channel、Database、System；Pipeline/Session/Memory/Architecture 降为诊断入口。

现有 URL 尽量保持兼容；通过聚合页和分组导航迁移，不立即删除旧路由。

### W3.3 分门顺序

1. **W3A 产品地图与高保真骨架**：按 [`web-product-shell-plan-2026-08-24.md`](web-product-shell-plan-2026-08-24.md) 的页面/对象/路由矩阵、关键旅程、状态与动作清单，构建无 CDN 的桌面隔离原型，覆盖全部主页面和 waiting/failed/empty 等关键状态；发布 Atlas 独立预览，由用户逐页门禁。
2. **W3B 生产 Shell 与核心旅程**：通过门禁后才改生产 `base.html` 与本地静态资源；新增 Task Detail 和 Report Library projection；打通新建任务→计划→补充/审批→结果→分析→报告→列表的真实桌面路径；使用单一 feature flag 和回滚点。
3. **W3C 数据资产与管理收口**：重组二级导航和入口，保留现有领域真相源；不顺带建设 M1B、M2、M3 或通用 Memory。

### W3.4 验收门禁

- 1440×900 与 1600×1000 桌面优先；移动端不参与当前 Pass/Fail；
- 用户无需理解 Pi、Forge JSON、Artifact 或 stage code，即可找到任务状态、风险、下一步和报告；
- 从任意主页面最多两次导航到达新建任务、等待审批、失败任务和最近报告；
- 所有页面覆盖适用的 loading、empty、ready、partial、needs_input、waiting_approval、failed、forbidden/offline；
- 无死按钮；真实与演示数据不会混淆；后退、刷新、深链接和焦点行为可预测；
- Playwright 检查导航、键盘、dialog/drawer、0 console error 和无横向溢出；用户人工判断信息架构、下一步可发现性和产品一致性，自动测试不能替代视觉/交互门禁。

### W3.5 后端顺延边界

M1A 不取消，但顺延为 W3 核心 Product Shell 稳定后的首个后端工作包。W3 不允许对真实跨用户/跨团队能力作已完成声明，也不能因前端优先而扩大默认允许 ACL。涉及企业多用户生产开放前，M1A/M1B 仍是硬阻断。

### W3A 实施结果（2026-08-24）

- `tools/web-product-shell-prototype/` 建立无 CDN、fixture-only 的桌面 Product Shell，覆盖六个一级区域、可寻址 Task Detail、SQL/Analysis/Report/Activity tabs、Report Library/Detail、数据资产 tabs 和管理分组。
- 所有页面显示“交互原型/演示数据”；源码无生产网络请求。审批 dialog 绑定任务、数据源、范围、限制、完整 SQL 和检查结果；演示确认不会写生产 Store。
- prototype tests `5 passed`、build/audit 通过；Python 全量 `564 passed / 24 skipped`（Web 定向 `19 passed`）；Pi typecheck/`103 passed`。Playwright 在 1440×900、1600×1000 本地与 Atlas 走通关键 route、dialog、fixture state、search、back/forward/reload，0 console error、0 横向溢出。
- 固定 commit `821065f` 发布到 Atlas `/srv/forge/previews/web-shell-821065f/`，`forge-web-shell-preview.service` 仅绑定 `preview.internal.invalid:18006`。生产 Jinja、Forge/Pi、Store 和 `d2b0fd9` checkout 未修改。
- 用户门禁已给出 `CHANGE`：连续 Chat 不应被一次性创建表单替代；同时“分析工作台”不足以概括产品。当前先复核 2026-08-21 至 2026-08-24 的产品对话，重新确认 Conversation、Task、Artifact、Decision、Governance 与 Agent-facing Runtime 的产品投影。新的 IA 未确认前不修改生产 route/API，不进入 W3B。原型自动证据见 [`web-product-shell-w3a-evidence-2026-08-24.md`](web-product-shell-w3a-evidence-2026-08-24.md)。

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

### H5：Evidence-bound Chart Storytelling（R0 自动化通过、用户视觉门禁失败，修订中）

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

R0 实际结果：

- 完成 Python Schema、TypeScript validator 和 QueryResult semantic gate；截断、重复可见 grain、未知 unit、非连续月份、越界 Evidence、stack total mismatch 等失败关闭。
- 完成品类横截面与月度多系列两个 fixture：4 张图分别回答排名、集中度、拐点和增长来源，非重复视图。
- 完成自包含 HTML、5 页 PDF、5 页 16:9 PPTX 候选；HTML DOM/ARIA/交互/console/print PASS，PDF 无本地路径/header/footer 泄漏，PDF/PPTX 静态保留单位、Annotation、quality 和 Evidence。
- 完成 Analysis/Report Skills、Structured Tool、`skill-executor`、Skills package、Contract、Renderer、Exporter 的 R1 同版本兼容矩阵。
- 正式证据：[`chart-storytelling-r0-evidence-2026-08-24.md`](chart-storytelling-r0-evidence-2026-08-24.md)。R1 未自动批准，NAS 与生产 Skills/Prompt 未修改。
- 用户视觉门禁反馈为 FAIL：首屏深绿色候选宣传 Hero 不承载报告决策内容，却占据接近整屏；交互位于首屏以下且缺少可发现反馈，元信息标签外观又误导为按钮。修订要求是删除宣传壳，首屏直接显示数据范围/质量/执行摘要/第一决策图，并把 tooltip、series 控制、table fallback 和 Evidence feedback 做成无需猜测的可见操作。
- 用户明确要求“产品不要重复造轮子”。生产 Renderer 不继续扩展手写 SVG/JavaScript；图表 tooltip、legend、zoom/selection、annotation geometry、layout 与 SVG/canvas rendering 必须复用成熟 chart engine。Forge 自有代码只负责 ChartArtifact v2 的受控适配、Evidence/quality binding、设计 token 和跨媒介 Gate。正式实现前用同一双 fixture 比较 ECharts、Vega/Vega-Lite、AntV G2；Highcharts/AG Charts 只有在商业授权成本被明确接受后才进入候选。生产最终只选一个默认 engine，不建设多引擎插件平台。
- 用户已批准继续开源 engine bake-off。实现必须位于隔离开发工具包，不修改生产 Pi `package.json`、Skills/Prompt 或 Renderer；每个 engine 必须消费相同 normalized fixture、使用本地固定依赖而非 CDN，并生成可比较的桌面 HTML/截图/交互与构建体积证据。
- Bake-off 已完成，正式证据见 [`chart-engine-bakeoff-2026-08-24.md`](chart-engine-bakeoff-2026-08-24.md)。初步选择 ECharts：按需 SVG bundle 约 193 kB gzip、warm median 58.5 ms；Vega-Lite 约 276 kB/85.3 ms 且需 CSP interpreter；G2 约 398 kB/368.5 ms。数字仅为同机相对证据。
- 用户已确认继续 ECharts focused candidate。该门仍只修改隔离工具包：报告首屏直接进入摘要和第一决策图；渠道视图必须从存量堆叠图改为可复算的 4→6 月增量贡献拆解，标出总增量 174K、直营 87K/50% 和 Evidence；排名视图表达前两名差距，避免赢家错觉；ECharts Option 只能由 allowlisted semantic adapter 生成。完成 HTML tooltip/legend/Evidence/table、strict CSP、print/PDF/PPTX 静态一致性和桌面视觉审查后回写证据。生产 package、Skills/Prompt/Tool/Renderer 和 NAS 仍不得修改。
- Focused candidate 已完成，正式证据见 [`chart-storytelling-echarts-focused-evidence-2026-08-24.md`](chart-storytelling-echarts-focused-evidence-2026-08-24.md)。4 SVG/0 Canvas、tooltip、series toggle、Evidence、table、no-JS 核心结论、5 页 PDF/PPTX、strict CSP 和零浏览器错误通过；首图在 1600×1000 的 y=585.8px 开始可见。候选同时遵守 W2，只呈现报告主体内容。
- R1 新增阻断：ChartArtifact v2 当前无法完整声明 period-delta/output-grain。正式进入生产前必须扩展确定性 transform 和 semantic gate，并与 Skills/Tool/Compatibility/Renderer 同版本发布；不允许 focused adapter 的固定计算静默变成 Renderer 猜测。
- 用户再次判定视觉门禁 FAIL：虽然宣传文案已删除，但大标题双栏、深色摘要块、导航卡片、大圆角章节和彩色侧栏仍是 Landing Page composition，不是专业报告。新增 `REQ-2026-08-24-012` 并进入 Editorial Report revision：文档画布、紧凑报告头、连续章节、figure caption、观察/判断/限制结构；保留现代 ECharts 交互。
- Inline 强调固定为版本化语义 token：strong=证据化结论/数字，emphasis=术语/假设，superseded=有 revision lineage 的旧标准，underline=仅链接/Evidence，code=标识符，mark=少量待审定义；Callout 仅 `info/decision/warning/limitation`，不得由模型提供 HTML/CSS/class/color。
- 用户补充“内容专业不等于术语密度”：正文必须优先用准确普通中文，按观察→有限判断→限制→待补证据组织；内部 `Evidence/Revision/Ready/baseline/comparison` 不占据业务正文，不能用语气、粗体或 Callout 制造确定性。
- 用户决定当前 Editorial revision 先暂定保留、后续迭代。该决定不是完整视觉 PASS，也不批准生产 R1。当前按 `REQ-2026-08-24-013` 只将固定构建物部署为 Atlas 独立预览，然后重评估总体目标差距。
- Atlas 隔离预览已完成：`/srv/forge/previews/editorial-929e8d4/` 为不可写固定构建物，`forge-report-preview.service` 仅绑定 LAN `preview.internal.invalid:18005`；远端 browser gate 通过，生产 Forge/Pi active、源码仍为干净 `d2b0fd9`。生产 readiness 保留已知的内网 HTTP `secure_cookie` fail，本次未改认证或 HTTPS。
- 阶段重评估见 [`forge-goal-gap-assessment-2026-08-24.md`](forge-goal-gap-assessment-2026-08-24.md)：近期可信数据任务产品约完成 65%–70%，长期企业目标约完成 30%–35%。当前首要差距不是图表视觉，而是 Runtime Governance Coverage=0；下一建议工作包仍是单独批准 M1A。

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

## 15. 当前实施顺序

M0.1–M0.5、N1/N2、SP0–SP5 与 Benchmark v2 已形成可复用工程资产；Atlas 人工复验保留为历史验收债务，但不再阻塞 R0。当前只推进开源 Trust Runtime 产品切割：

```text
R0.1 Unified Input Contract：Direct SQL / Forge JSON（已完成）
  → R0.2 Evaluate：Exact Result / Regression / Failure Taxonomy（已完成）
  → R0.3 Enforce：Policy / Assurance / Read-only / Approval（已完成）
  → R0.4 Explain：Evidence / Lineage / Limitations（已完成）
  → R0.5 Public Golden Path：README / Quickstart / CLI-API / Dashboard（已完成）
  → R0.6 External Adoption Evidence（当前）
```

R0.1 关闭证据：共享 JSON Schema `query-candidate-v1` 定义互斥的 `direct_sql` / `forge_json` 输入；Python 与 Pi 均传递并校验该契约。Direct SQL 通过 `sqlglot` 解析与限定，执行只读、Registry/ACL 和字段校验；两类输入持久化 `input_kind` 与 candidate revision，并进入同一 QueryRun 审批、执行和 SQL/Assurance hash 绑定链。相同 SQL 的两条路径共享 assurance/policy/registry/candidate revision 和 SQL hash，来源身份保持可追溯。Python 全套 `612 passed / 28 skipped`，Pi `118 passed`，TypeScript typecheck 通过；R0 外部采用门禁仍未通过。

R0.2 关闭证据：版本化 `POST /api/v1/evaluate` 与 `forge evaluate` 复用 `query-candidate-v1`、Registry Assurance 和 `benchmark-failure-v1`，统一返回 Policy verdict、有界失败、Exact Result、lineage 与响应内 Evidence refs；`evaluation-suite-v1` 和 `evaluation-run-manifest-v1` 持久化 dataset/producer/prompt/retrieval/retry/timeout/evaluator/metric/Registry 等修订、完整 suite、原始 outcomes 与可复算 aggregate。相同 evaluation basis 可跨 producer 版本执行 Regression release gate；dataset、case selection、evaluation basis、Policy、evaluator、Registry 或 dialect 漂移时标记 `not_comparable` 并失败关闭。入口始终不执行 SQL，`execution_authorized` 为 false。公开 fixture 的持久运行、suite revision 回放、baseline gate 和 manifest 导出实际 CLI/API smoke 均通过；R0.2 聚焦测试 `42 passed`，Python 全套 `649 passed / 28 skipped`。R0.2 完成，当前进入 R0.3 Enforce；外部采用门禁仍未通过。

R0.3 关闭证据：新增 `enforce-query-request-v1`、`enforce-query-approval-v1`、`enforce-query-response-v1`，以及版本化 `POST /api/v1/enforce/query-runs`、`GET /api/v1/enforce/query-runs/{query_run_id}`、`POST /api/v1/enforce/query-runs/{query_run_id}/approve` 和 `forge enforce`。现有 QueryRun 继续作为唯一执行真相源，持久化 Principal、Purpose、Task、可选 Agent/Service DelegatedMandate、Resource Scope、Policy/Assurance/Registry、candidate/SQL 及其 hashes；创建阶段只生成 `review_required`，只有独立 reviewer credential 提交匹配 SQL、Assurance 与 Enforcement Context hashes 后才执行。Policy、Registry、授权上下文、范围、候选、审批或只读凭证漂移均失败关闭；Direct SQL 与 Forge JSON 不互相转换。Governance Action Catalog v1.2.0 仅将 `query.prepare`、`query.approve`、`query.execute` 标为 enforced，Runtime Coverage 为 3/14。聚焦回归 `67 passed`，Python 全套 `663 passed / 28 skipped`，Pi `118 passed`，TypeScript typecheck 通过；公开 API/CLI 创建、审批执行、结果截断与回读实际 smoke 通过。该证据关闭 R0.3，不代表 R0.4 Explain、公共 Golden Path 或外部采用门禁已通过。

R0.4 关闭证据：新增版本化 `explain-query-response-v1`、`GET /api/v1/explain/query-runs/{query_run_id}` 与 `forge explain`。Explain 不创建新存储或状态机，而从同一 QueryRun 稳定投影实际 SQL/结果、Registry 表列语义与数据源、Principal/Purpose/Policy/Approval、Assurance、版本、Evidence、lineage 和显式 limitations；创建、审批和完成阶段分别持久化来源上下文、approval 与 result hash。持久证据篡改返回有界错误并失败关闭；历史未锚定 QueryRun 只返回 `integrity=partial` 和对应限制，当前 Registry 漂移不会改写历史解释。读取继续绑定创建凭证，响应不披露 session hash、内部存储或原始数据库错误。Explain/Enforce 聚焦测试 `24 passed`，Python 全套 `673 passed / 28 skipped`，Pi `118 passed`，TypeScript typecheck 通过；真实本地服务经 CLI 完成 Enforce 创建 → 审批执行 → Explain，验证 2 行截断结果、7 类 Evidence 与 3 项显式限制。Explain 是只读证据投影，不扩大 Governance Action Catalog 的 Runtime Enforcement，覆盖仍为 3/14（21.4%）。R0.4 关闭，当前进入 R0.5。
R0.5 关闭证据：默认英文 README 与中文本地化入口均以 `forge quickstart` 作为首个可运行路径。Quickstart 创建隔离合成 SQLite/Registry、启动真实本地 Forge 服务，并只通过公开 HTTP API 完成 Direct SQL Evaluate exact-result → Enforce review/hash-bound approve/只读执行 → Explain integrity/Evidence/limitations；默认展示实际 SQL 并等待人工批准，`--yes --json` 提供机器可读 CI 证明，`--serve` 在验证完成后保持 Dashboard 可浏览。Dashboard 从 QueryRun 真相源读取最近治理运行，并用同一 Explain 投影显示状态、Evidence 数与 integrity；Evidence 漂移只显示失败关闭，不创建第二状态。实际 CLI smoke 验证两行上限截断、七类 Evidence、三项显式限制及 Dashboard 同一 QueryRun；Browser 在 1440px 与 390px 页面验证无水平溢出。聚焦回归 `69 passed`，Python 全套 `675 passed / 28 skipped`，Pi `118 passed`，TypeScript typecheck 与 Python compileall 通过。该内部证明只关闭 R0.5 工程收敛，不构成 R0.6 外部开发者采用证据；Runtime Governance Coverage 保持 3/14（21.4%）。

R0.6 当前证据状态：公开采用入口已收敛为“fresh clone → `forge quickstart --workdir ...` → 人工审核 SQL → 提交 GitHub Quickstart adoption report”，并已发布为公开 revision `0b4fd36b7175c09dc3375d839c5aba888aacb900`。Quickstart 先验证写 SQL 以 `assurance/readonly_violation` 失败关闭，再完成只读 Evaluate → Enforce → Explain → Dashboard，并把版本、有限环境字段、阶段结果、显式限制和 SHA-256 checksum 写入 `run_receipt`；Forge 不发送 telemetry，checksum 只用于漂移检测/去重，不证明提交者身份。维护者从公开 HTTPS 远端全新克隆 `main@5bbdabe5ceca10fd7128a825d277e5e4534d69e7`，在空工作目录完成 bootstrap + Quickstart 共 36 秒，终态与 checksum 复算通过；[Issue #9](https://github.com/shisuidata/Forge/issues/9) 已公开招募非实现者试跑，#8 保持为 framework-neutral external-agent adapter 贡献入口。公开 GitHub 清单中的 9 个 Issue 与 1 个 Pull Request 仍均为维护者身份，stars/forks 不计入采用证据；当前无合格外部回执，因此 R0.6 与 R0 退出门禁保持未通过。

R0.6 兼容性维护：`REQ-2026-09-03-027` 允许用公共 Benchmark 历史失败证据修复确定性 Compiler 缺陷，但不得发起未授权模型调用、改变输入/输出 Contract、按数据集写特例、宣称完整 500 题新分数或替代外部采用门禁。首个切片仅处理未投影聚合别名的排序展开，并要求保持可见结果列不变、离线复算公共失败样本和通过全量回归。

R0.6 兼容性维护关闭证据：Compiler 现在只在非集合查询中把未投影 `agg.as` 排序引用展开为原聚合表达式，不向结果追加隐藏列；已投影别名保持不变。公开 BIRD 历史 Run `hbr_453ac77d1fc34478b39e0d19dc5b6741` 离线回放中，case 988 run 3 从执行失败变为 Official EA exact match；case 1011 的两个同类候选恢复可执行但仍因输出列契约不匹配保持失败。聚焦测试 `63 passed / 2 skipped`，Python 全套 `678 passed / 28 skipped`，Pi `118 passed`，TypeScript typecheck、Python compileall 与 diff check 通过；没有新模型调用，完整 500 题分数和 R0.6 外部采用门禁均不变。

R0.6 Accuracy Lab 复验：`REQ-2026-09-03-028` 已获用户明确授权，使用 Ready Pi OAuth 模型 `openai-codex/gpt-5.6-sol` 对完整 BIRD Mini-Dev 500 题重新生成 Forge JSON 与 Direct SQL 双臂候选，共 1000 次调用。运行冻结 temperature 0、8192 max output tokens、同一 ContextSnapshot 和模型/catalog revision，以 Official EA 为主指标；不得复用旧候选、运行中改代码/Prompt、按结果重试选优或把基础设施失败计作理论结论。

R0.6 Accuracy Lab Structured Output 定向复验：`REQ-2026-09-04-029` 把 Pi Benchmark 的 Forge 生成从文本 JSON 切为单一 terminating custom tool，完整 Forge Schema 负责参数校验，Prompt 只保留结果契约、粒度、过滤、比例和 SQLite 表达式语义；默认 `ROUND(..., 4)` 已删除。当前递归/union Schema 不兼容 Provider strict 子集，因此 `strict: prefer` 确定性降级为普通 tool schema，未伪装成 strict `response_format`。同一 GPT-5.6 revision 与相同 ContextSnapshot 的 20 道历史 Forge 失分题中，20/20 取得结构化对象，Official EA 从 0/20 到 9/20；收益集中在舍入 5/6，实体/粒度语义 0/4。Python 全套 `684 passed / 26 skipped`，Pi `118 passed`，TypeScript typecheck 与 Python compileall 通过。该有偏诊断不改写完整 500 题 53.20% 基线，也不替代 R0.6 外部采用门禁。

R0.6 Structured Forge Compiler 兼容性维护关闭：`REQ-2026-09-04-030` 统一修复 quoted qualified identifier 绑定、按方言渲染关系/字段/输出 alias，以及 HAVING 条件两侧聚合 alias 展开，同时保持 raw subquery 与显式聚合 SQL 表达式逃生口。旧 Structured Tool 的相同 20 个候选离线重评由 9/20 EA、10/20 Contract、15/20 Execution 提升至 14/20、15/20、20/20；五题变化均为候选不变的确定性修复。按相同模型 revision、Prompt/Schema 和 ContextSnapshot 重新生成的新候选，经最终 Compiler 离线重评为 Forge 17/20 EA、18/20 Contract、20/20 Execution/Compile，同期 Direct 为 14/20 EA；但两次生成候选不同且样本有选择偏差，不宣称总体优势。Compiler 聚焦 `112 passed`，Python 全套 `697 passed / 26 skipped`，Pi `118 passed`，TypeScript typecheck 与 Python compileall 通过；完整 500 题基线和 R0.6 外部采用门禁不变。

R0.6 Accuracy Lab DeepSeek 复验阻塞证据：`REQ-2026-09-04-032` 先后按用户选择尝试火山 Coding Plan 与官方 DeepSeek。Benchmark Runtime 的扩展注册时序已修复并通过 Pi 118 个测试与 TypeScript typecheck；火山 canary 仍由 `429 AccountQuotaExceeded` 阻断。官方入口降低到并发 1 后，nominal full Run `pbr_9d5e4263afdb46cd8636d4b6599f0708` 写入 500 个 case，但每臂只有 78 个候选、422 个候选缺失，独立探针返回 `402 Insufficient Balance`。该 Run 失败关闭，不形成新分数、不改写三轮有效完整基线，也不替代 R0.6 External Adoption Evidence 门禁；额度恢复且双臂 canary 通过后才允许从隔离状态库重跑。

R0.6 Accuracy Lab 离线归因关闭：`REQ-2026-09-05-034` 固定 Sol Run `pbr_6778bf9d34ae42fba0b070a5f9c154ba`，完成209题/373个EA失败arm的Agent辅助初步审查及500题候选/SQL哈希证据。清单见 `benchmark-sol-error-triage-2026-09-05.json`；86个候选错误、150个参考冲突、133个意图/输出歧义、4个未定是初筛标签，不是人审裁决或可扣除的错误数。Official EA 62.6% vs62.8%不变；未发新基准调用、未改Prompt/Compiler/Gold/评分器。下一实验建议以值/实体绑定为先，结合范围、粒度及输出契约；须另行明确实施范围，不扩展平台或替代外部采用证据。

R0 未通过前，不恢复 M1A、G1、Q1、H6 或更广企业平台实现。Atlas candidate 可在不扩张产品范围的前提下单独复验；不得因产品切割删除已有 Contract、测试、审计或失败关闭边界。

明确假设：

- 采用“近期详、远期粗”，不为远期阶段提前设计全部存储和服务边界。
- 当前优先保护私有单机部署兼容，但企业 profile 必须严格默认拒绝。
- 近期不拆 Governance、Economics、Context 独立微服务。
- 不引入新的通用 Agent、渠道或无边界业务系统写操作。
- 所有真实 OAuth 登录、生产凭证、客户数据源和权限变更均需要用户单独明确授权。

### 15.1 Accuracy Optimization Program（REQ-2026-09-05-035）

> 状态：ACC-0/ACC-1A已验证；REQ-037元数据修复、REQ-038严格输出及REQ-039别名纠错已完成。用户批准的REQ-040二十次Luna新生成已按原始响应统一离线复核：当前配置Forge仍3/5，处理组tokens多41.06%，没有稳定收益结论。Pi递归归一化/失败状态与token丢失已修复。H独立审核未完成；不自动扩大量测、产品或部署，后续优先澄清具体语义与输出契约。

#### 15.1.1 目标、基线与完成定义

- **目标**：提高固定范围内的正确答案产出，降低被当作有效答案交付的静默错误；不以执行率、格式通过率、拒答率或测试数量替代准确率。
- **当前基线**：Sol固定候选Forge EA 313/500（62.6%）、Direct 314/500（62.8%），Execution 496/500与500/500。REQ-034的86个候选错误、150个参考冲突、133个歧义、4个未定均为失败arm初判；不是新正确率或可承诺收益。
- **证据定位**：当前500题已多次用于修复与分析，明确降为回归集；不能将其中重新抽出的50题称为独立留出集。Oracle Evidence仅代表官方辅助条件，真实业务能力必须另测。
- **完成定义**：至少有一个保留的改动在未用于本项目调参的样本上复现净收益；安全门禁不退化，正确产出、覆盖率、误拒绝、token与时延均有分母明确的证据；受影响的现有调用路径完成验证。没有有效增益时可以得出“不采用该优化”，但不能宣称准确率提升。
- **暂不设80%/90%承诺**：历史样本存在参考争议，且尚无独立真实业务基线。先冻结可验证目标与基线，再从实验估计收益；不能累加错题覆盖数预测最终得分。

#### 15.1.2 指标与数据分层

在具备独立正确性标签的固定评价范围内，设总请求数N、交付正确答案数C、交付错误答案数W；拒答/澄清不计入C或W，但保留在N。分别报告：端到端正确产出C/N、回答覆盖率(C+W)/N、条件准确率C/(C+W)、静默错误W/N、澄清/拒答率。零分母记N/A而非100%。门禁另报“被拦截的原本正确候选/所有已知正确候选”，防止靠拒答提高条件准确率。无独立标签的请求只报告可观察状态，不伪造C/W。

Official EA沿用官方tuple-set语义，另报版本化Contract Accuracy（列身份/顺序、行序、重复、NULL、数值/单位）；两者不合成分数。Compile/Execution用于定位，Evaluation Suite的pass_rate表示预期行为满足率，不是答案准确率。另记录每题及每个正确产出的tokens、调用数、平均/P95端到端时延；无法获得实际结算金额时不虚构费用。

| 数据层 | 用途 | 规则 |
|---|---|---|
| D：定向开发/冒烟集 | 验证某一机制是否命中问题 | ACC-2默认候选为16道值绑定诊断题加34道预先冻结的正确对照，按领域/难度/操作覆盖选择；有多重争议的题只对明确错误做诊断。50题是开发集，不报告总体准确率 |
| R：现有500题回归集 | 发现旧正确答案回退及全类别影响 | 原始候选、Gold和旧分数不变；分别报告变好/变差case，不只看净分 |
| H：独立留出集 | 检验泛化和正式增益 | 从未参与本项目调参的合法公开切片或独立编写的业务问题中，在实验前冻结题目、指标与标签；与D/R按题意/SQL模板/重复实体问题检查重叠。规模按目标最小增益和统计把握预先确定，不看结果后扩样凑显著性；不承诺模型预训练未见 |
| P：真实证据条件集 | 验证非Oracle条件及实际采用 | 使用获授权、可脱敏、由业务责任人确认口径的真实问题，记录澄清与失败；无数据授权时仅完成公开/合成smoke，不宣称生产准确率或外部采用 |
| S：安全与边界样本 | 防止静默越权/误放行 | 只读、ACL、未知字段、证据冲突、跨层作用域及错误拒绝；不得因主分上涨放宽门禁 |

争议标签必须带来源、理由、审核状态和版本。独立人工/业务Owner尚未裁定时维持unknown；继续在官方R集报告原始EA，但不把这些题纳入“已裁定语义正确率”的可判定分母；该分母及unknown覆盖率必须事先冻结并同时披露。不得根据某版本得分临时剔除题目，不将Gold差异改成模型正确证据。

#### 15.1.3 阶段总表与依赖

| 阶段 | 目标 | 当前状态 | 交付与退出条件 |
|---|---|---|---|
| ACC-0 固定基线与错题归因 | 209题/373个失败arm归因及500题候选/SQL哈希；争议标签保留 | 已完成 | 已验证覆盖与复算；不将初判改写为官方得分 |
| ACC-1 评测口径、样本分层与新基线 | 版本化结果契约、可复算评价基线、开发/回归/留出清单 | ACC-1A已验证；H独立标签门禁未完成 | 旧EA不变；新比较器同候选双臂重评已完成；D/R/S已冻结，H不得伪造 |
| ACC-2 字段取值与实体绑定 | 验证确有增量的权威取值/实体证据 | 原format+PK停止；CSV绑定修复完成，Luna五题诊断未达晋级条件 | 值绑定有局部改善，但完整答案/回退与token门槛未过；不扩大调用 |
| ACC-3 范围、粒度与输出契约 | 显式分母/时间/关联范围、实体粒度、输出字段、单位/并列约定 | 待ACC-2结论 | 已裁定语义切片上净改善；未定义语义保留澄清，不由Compiler猜测 |
| ACC-4 确定性诊断与语义门禁验证 | 作用域/数值/排序及有依据的粒度检查，先离线影子评估 | 待ACC-1，可提前离线研究 | 拒绝错误与误拒正确分别计数；未校准规则不启用硬拒绝；安全门禁不降级 |
| ACC-5 有界修正实验 | Pi管理至多一次修正，保留初稿、反馈、终稿、revision及成本 | 条件项，可能跳过 | 同预算对照下终稿正确率净改善；无越权/放宽/按Gold选优；否则禁用并保留单次生成 |
| ACC-6 独立验证与现有路径适配 | 留出集报告、Oracle/无Evidence/生产证据消融、现有真实调用路径验证 | 待候选冻结 | 增益可复现、成本与覆盖率符合预声明门槛；外部/业务验收独立记录 |

主线：ACC-0 → ACC-1 → ACC-2 → ACC-3 → 最终配置冻结 → ACC-6。ACC-4依赖ACC-1，可与生成改动分开研究，但不得在同一实验中同时更换Prompt和门禁。ACC-5仅在ACC-3/4之后按证据决定启用或跳过，不是所有请求必经步骤。每轮结束都写明保留/回退/证据不足；证据不足不自动晋级。

#### 15.1.4 ACC-1：先使评价可信

1. 固定原始500题候选、Gold、源码/revision及完整性；复用REQ-034清单，不再逐题重做初判。优先独立核对阻塞当前实验的争议，不要求先人工解决全部150个参考冲突arm才能修比较器。
2. 修复可确定的比较器问题：已具备可靠列身份时先按列身份/显式契约对齐；值完全相同但映射不唯一不得草率推断语义身份，按既定契约判断值等价或返回inconclusive。区分值不匹配、缺列与映射不确定；按明确数值契约处理int/float等价，禁止通用宽容差掩盖真实错误。
3. 版本化评价规则，保留旧结果；在相同候选上对两臂重评，单独归因为“评价器变化”，不当成生成改善。复用`forge/benchmark_v2.py`、`forge/evaluate.py`、`forge/evaluation_runs.py`及现有Suite/Manifest/Regression Gate。
4. 冻结D/R/H/S清单、模型/上下文/候选契约/retry/timeout/指标版本和成本上限。现有Regression Gate对metric/assurance/registry变更会判不可比：不得关闭该门禁；先在新评价基准上重算旧候选建立新基线，再比较producer版本。新增元数据属于实验处理变量，另附输入差异，不伪装同revision。
5. **验收**：比较器最小复现消除已确定误判且不把不同数值/错列判等；旧Official EA保持不变；落盘结果可复算且缺候选/版本漂移不发布可比最终分数。按真实错误保留回归测试，不增加源文本断言。

#### 15.1.5 ACC-2：先优化有证据的取值与实体绑定

- **ACC-2A，元数据呈现**：REQ-037对实际公开元数据和D16的审计未发现format+PK可补充的有效取值证据，停止原追加假设，不将冗余type标签或复合键成员当作新的业务语义。普通CSV文件名大小写绑定Bug已独立修复；仅恢复来源中本来存在的信息。是否带来生成改善须按15.1.13的新实验提案验证，不能由输入完整性推断。
- **ACC-2B，权威值/实体绑定**：2A证明不足后，单独验证Registry已有枚举、别名、本地化实体和日期表示映射；先做有界离线元数据，不默认新增在线探库工具。缺少证据则unknown，不强制lower/LIKE或猜测缩写；元数据与Evidence冲突需显式暴露，不静默挑有利于Gold的一方。
- **代码入口**：`forge/hard_accuracy_benchmark.py`的结构投影/呈现、`forge/benchmark_v2.py` ContextSnapshot、`web/routes/benchmark_v2.py`及`agent/prompts.py`；生产复用路径在ACC-6核对`forge/context.py`与既有Registry，不创建平行知识库。
- **实验**：2A只变呈现，2B只变绑定证据，分别跑控制/处理条件；同一条件内Forge和Direct共享相同证据。依据16题错误定位，同时用冻结正确对照看回退；无新增信息时不能宣传检索改善。
- **验收/停止**：目标切片有独立确认的修复、正确对照无未解释的新错误且未扩大资源可见性；只改善历史错题、增加无关context或持续损失正确题则回退，不推进全量调用。目标覆盖16题不等于可获得3.2pp。

#### 15.1.6 ACC-3：明确业务口径，而不是加更多绝对化Prompt

- 分三个单变量切片：3A实体/时间/分母/NULL及JOIN范围；3B聚合、去重、Top-N及并列规则；3C最终输出字段/身份、单位、比例尺度、精度与排序。
- 复用现有ResultContract、Registry指标/关系/约定；字段来源与业务定义须可追溯。现有`required_output_semantics`和`expected_grain`主要来自词面规则，不是已确认语义，不能直接变硬规则。优先在现有一次生成中表达必要约束，不预设新增模型规划调用或新Task状态。
- 明确“定义缺失/冲突 → 提议或澄清”，不能从Gold反推规则、对所有COUNT加DISTINCT、强制INNER/LEFT、默认ROUND或偷换分母。业务Owner确认的定义才可版本化发布和复用，不自动把对话猜测写回Registry。
- **代码入口**：`agent/prompts.py`、`forge/benchmark_v2.py`、`registry/`既有指标/关系/约定与`forge/assurance.py`。若必须更改公开契约，实施前追踪全部调用与Python/Pi两侧验证，不额外创建旁路intent真相源。
- **验收**：独立标签切片的正确产出提高，缺维度、错分母、漏列、多列、错误并列范围按类下降；未定义口径的澄清单报，不能冒充回答正确。输出列格式变化不能悄悄放宽官方EA。

#### 15.1.7 ACC-4：先量化诊断能力，再决定拦截

- 优先检查有确定依据的跨CTE输出缺失、窗口同层WHERE、嵌套聚合、整数除法截断、按文本排序数值/日期及未知值/字段。数字类型或业务定义未知时返回诊断/unknown，不自动改SQL表达式。
- 固定候选离线评估existing Assurance，分别统计“错候选被检出”“原本正确被误拒”“仍放行的错误”和覆盖率。当前Benchmark只运行`assure_compiled_sql`，不把`assure_query`的中文/正则语义检查直接套到BIRD或宣称通用语义验证。
- SQL可确定的规则尽量在两种输入共享的验证层评估；Forge结构专属检查单列方法差异。所有明确只读/ACL门禁维持强制，不等待语义规则校准；尚未校准的启发式规则仅影子诊断，不悄悄扩大拒绝。
- **代码入口**：`forge/assurance.py`、`forge/compiler.py`、`web/routes/benchmark_v2.py`及Evaluation Contract。Compiler只修显式输入的确定性翻译错误，绝不替模型猜分组、补业务过滤或输出列。
- **验收**：每条准备启用的规则有正/负样本，安全负例零漏放；硬拒绝前所有已知正确候选误拒必须解释并解决或降为诊断。语义拒绝即使降低静默错误，也不能宣称提高答案EA；不为仅4道非执行题建设通用自动修复平台。

#### 15.1.8 ACC-5：只有有用的反馈才值得一次修正（条件项）

- 仅对已证实可修复的生成/作用域/类型错误，考虑Pi在原Task内至多追加一次生成；使用实际诊断和相同授权上下文，记录初稿、反馈、终稿、tokens与时延。初稿和终稿分别评分，不能按Gold或最好一次选结果。
- 业务歧义应澄清而非重猜；ACL、只读、Policy拒绝不能通过改权限/换凭证“修复”。Provider额度/余额/系统性失败直接停止，不作为语义重试，不自动重放高风险动作。
- 修正若改变SQL/Assurance/hash，必须重新走原有审批约束，不能复用旧批准；Benchmark只用隔离只读公开数据，生产执行权仍由QueryRun链授予。
- **代码入口**：`services/pi-orchestrator/src/benchmark-runtime.ts`与既有Pi任务执行链/Contract；Forge只返回诊断，不接管调度。任何额外调用有独立预算上限。
- **验收/跳过**：同等最大调用预算下，同时比较单次生成与诊断修正的初稿/终稿，报告新增正确、改坏正确、成本及P95；无净收益、需频繁人工猜测或成本不合适即禁用。不能把双臂理想Oracle选优67.2%当可实现收益。

#### 15.1.9 ACC-6：独立验证与有界现有路径适配

1. 冻结保留的配置，在H上按预注册样本/重复次数验证；H一旦用于挑Prompt、改规则或挑模型即转开发资料，下次发布需新留出或独立外部验证。原500题只做最终回归，不用单次temperature 0结果声称稳定因果。
2. 分开运行“官方Evidence”“仅Schema”“生产可检索证据”条件，保持同条件双臂证据一致，报告差值；不能拿有Oracle的Forge对比无Oracle的Direct。生产Evidence不足时的正确澄清/停止单独计量。
3. 只将已验证改动迁移到仍实际使用的生成/Context路径，包括`agent/llm.py`调用的旧`build_system`、Pi及公开候选接口涉及的路径；枚举调用者后做干净迁移，不留下两套相矛盾的默认精度/输出规则。非相关功能不借机重构。
4. 公开/合成数据先走已有Evaluate → Enforce → Explain smoke，验证修正后的候选、SQL、审批和Evidence一致；模型依赖路径须单独真实验证。真实P集必须获得数据授权及业务Owner确认，没有授权不部署、不宣称生产验证。
5. **关闭门禁**：报告H上的配对变好/变差、区间、成本与S安全结果；声称统计增益时预先指定的方法给出95%区间下界>0且达到预声明最小业务增益。未达成则记录证据不足/不采用，不反复复测到通过。真实采用按R0.6外部回执独立验收，不由内部准确率替代。

#### 15.1.10 每一轮如何执行、停止和回滚

- **实验卡先于调用**：写清假设、处理变量、数据/模型/Prompt/Schema/Context/Compiler/Assurance/评分器revision、对照、主要指标/分母、允许的成本/P95增幅、总调用上限、重复轮数、终止与回滚条件。成本阈值须在看结果前由用户确认；未设上限不启动付费实验，不临时扩大模型范围。
- **固定顺序**：离线重放/最小复现 → 少量canary → D集筛选 → 完整错误差异审查 → 保留或回退 → 候选稳定后R/H/P验证。仅跨Python/Pi Contract时同时验证两侧；UI改动才另做真实UI验证，不用无关全套测试替代行为证据。
- **调用算清**：比较“控制/处理 × Forge/Direct”时，每题每轮为4次生成；2题canary为8次，50题D集为200次，500题完整同设计为2000次。若只复算保存候选为0次；重复轮数乘入总量。REQ-033的50题模型横测是不同实验，不可混用分母或把它称留出集。本轮不启动这些调用。 上述是逻辑生成次数；Provider自动重试的实际请求数另记，canary、重复轮次、失败请求和重试消耗都计入实验总预算，不用成功候选数代替实际调用成本。
- **公平与稳定**：默认继续以已有Sol方法基线作为参考，运行前重新冻结可用revision；模型横测和方法优化分开。请求执行次序按预声明随机/交错协议控制供应商时变影响；记录temperature0仍可能波动，禁止按结果重试选优。
- **小样本准入**：D集只决定是否值得继续，不决定总体提升；要求目标机制有可复核改善且正确对照没有未解释回退。改善与回退相当或重复轮次不稳定则保持现状，审查原因而不是直接跑更多题。
- **版本边界**：候选不变的Compiler/评价维护可离线隔离因果；新增Context或Prompt必须新生成。现有门禁判不可比时保留差异，重建同评价基础的控制基线，不关闭校验或把历史Run原地覆盖。
- **停止与回滚**：出现只读/ACL回退、候选缺失、上下文截断、revision漂移、成本超限或系统性Provider失败立即停止该轮；保留已产生的原始记录，标记无效/不可比。模型binding回滚用既有revision能力；源码改动只回退本工作包，不reset用户工作区。若停止在ACC-2/3，仍可交付已验证部分，不自动开启ACC-5。
- **阶段交付模板**：目标与假设 → 具体改动/revision → 有效运行和完整性 → 变好/变差case → 安全/覆盖/成本 → 保留/回退/待证据 → 下一切片。只把真实完成项标done；争议结论不升级为事实。

#### 15.1.11 首个实施工作包

**ACC-1A：比较器诊断与评价版本维护，默认零新模型调用。**以REQ-034已保存的列指纹、int/float、列置换及重复行反例为依据，确定比较器修复边界，保留能防真实误判的回归测试；在同一批候选上重评两臂，冻结新评价基线与D/R/H/S清单。涉及独立业务裁定的部分保留待审核，不阻塞确定性比较器维护。完成后再申请ACC-2A的canary/开发集调用预算，不把整份规划当一次性执行全部阶段的授权。

#### 15.1.12 ACC-1A关闭证据与剩余门禁（REQ-2026-09-05-036）

- **已完成**：比较器修复与 `semantic-result-compare-v2`；公开Evaluate结果版本入hash，保留可靠列身份并区分false与inconclusive；Pi新Run冻结版本并在context/evaluate漂移时失败关闭，历史未知版本不伪造、不重写、不按新版恢复。Official EA和已有审批/只读/ACL边界不变。
- **同候选重评**：`benchmark-sol-metric-replay-2026-09-05.json` 记录全部500题/1000个候选，996个可执行结果哈希一致且逐项复现旧判定；Forge Contract 280→284，Direct 291→294，零回退、此集零inconclusive。Official EA仍313/500与314/500；新增7个通过仅为评价器纠错，另181个真错诊断由映射歧义改为值错误。未发起模型调用，未重跑Assurance或生成新时延结论。
- **D/R/S冻结**：`accuracy-evaluation-cohorts-2026-09-05.json` 以修复前归因证据冻结R500、D50（16错题+34旧基线对照，11库/33难度分层）及S27项。父执行器独立复算清单/hash/配额，S20比较器边界、7个既有安全引用/14参数化用例通过。完整验证为Python722 passed/26 skipped、Pi124 passed、TypeScript typecheck通过。
- **未完成的外部前置条件**：H只有冻结协议、无已证明独立且审定标签的case；现有500/旧hard已参与调参，Spider2剩余公开材料也未满足完整独立性与题意/模板隔离证明。P无业务数据授权及Owner确认。不将这些缺口包装成ACC-1或ACC-6完成，不把已有Gold文件等同于独立审核。
- **后续实证**：新增来源、原2A假设的停止依据和下一实验门禁见15.1.13。独立H审核未完成时是否允许D诊断先行，以及调用预算，仍须明确确认。未经预算授权不发canary，不自动推进后续阶段。

#### 15.1.13 元数据机制实证与CSV绑定修复（REQ-2026-09-05-037）

- **实证结论**：`accuracy-metadata-treatment-audit-2026-09-05.json` 覆盖11库/75表/798列；format+PK对D16的25个失败arm新增有效取值证据为0，停止原追加Prompt假设。通用CSV绑定修复已进入 `forge/hard_accuracy_benchmark.py`，按SQLite ASCII大小写规则匹配并对歧义失败关闭，未写题号/Gold特例。
- **行为证明**：`accuracy-metadata-binding-fix-2026-09-05.json` 记录48列已有元数据恢复；重算全部D50真实context与100份双臂control指令，只有md-034/046/052/065/077输入改变，表/关系范围不变。md-065的approved=true/false证据已进入两臂输入；这不是新答案、不是EA改善。修复前2个回归反例失败，修复后Python全套725 passed/26 skipped、TypeScript typecheck通过。
- **H来源证据**：`accuracy-holdout-source-audit-2026-09-05.json` 从官方完整dev的1534题机械排除523题，冻结1011候选/964依赖组件；父执行器盲态复核哈希、集合、稳定ID和题面重叠。最终H未抽样，159条曝光SQL解析/qualification失败记录、独立题意/Gold/依赖审核及完整数据快照仍未闭环，不能称为独立H。
- **准备阶段提案**：5道输入受影响的D题，2条件×2臂，原提案20次生成，包含前2题canary；不等同于完整D50或泛化验证。后续用户授权、参数失败补额和Luna实测结果见15.1.14。
- **调用前风险**：现有Pi Run的temperature=0/max_output_tokens=8192是记录值，未在session调用显式绑定；离线实际Codex适配器探针即使传maxTokens=8192也不写输出上限，session默认仍允许自动重试。尚无可承诺的8192-token成本硬上限；必须先明确实际参数/请求计数/重试策略，再获得预算授权。模型真实能力与扩展覆写未验证，不追溯伪造历史请求参数。
- **准备轮边界**：新模型调用0；Official EA仍313/500 vs314/500。未变更Gold/Prompt/Compiler/生产配置，未commit/push/部署。

#### 15.1.14 Luna五题机制验证结果

- **实际调用**：Pi现有OAuth入口与 `PiBenchmarkRuntime`，固定Luna。首次显式temperature被接口拒绝，两请求HTTP400、无答案；用户确认“继续，最多22次”后，改用Provider默认采样，两条件一致。最终22次生成HTTP请求：2次参数失败+20个有效候选，无自动重试；不将失败canary包装成准确率结果。
- **五题实测**：Official EA与Contract v2均为Forge 3/5→2/5，Direct 3/5→3/5；Forge执行5/5→4/5，Direct保持5/5。无新增评价通过，Forge新增1个回退；这是全部5道输入受影响开发题，不是Luna总体准确率。证据：`benchmark-luna-binding-2026-09-05.json`。
- **错误变化**：md-065两臂approved从Yes改为true，空结果变为非空；但费用类型分组与Gold的event.type仍有语义/参考冲突，未改判。md-077 Forge日期恢复ISO格式，查到与Gold相同的两个人及金额，但姓名合并成一列，仍按原输出契约判失败；Direct日期仍错。md-052处理组Forge把SQL式别名写入scan/table，编译器复现“JOIN 条件引用了未声明的表：b, e。”，不能归因为稳定的元数据负效应。
- **代价与决定**：双臂合计tokens 28,611→38,709（+35.29%），超过预设20%晋级阈值；不扩大调用、不宣称准确率改善。保留元数据完整性修复；下一短闭环优先处理已复现的Forge别名表示错误，参考/输出契约争议单独处理，不机械推进阶段表。
- **复核与边界**：20个候选的评分/编译结果只读复现，5个Gold缓存与真实SQL执行一致；实际payload、session、请求记录和Pi run_id已封存。Raw Pi中的temperature=0/max_output_tokens=8192仍是旧声明字段，实际请求不传二者；不伪造历史参数。未改Gold/比较器/运行器生产源码，未commit/push/部署；独立H与外部采用门禁仍未通过。

#### 15.1.15 原生严格输出验收

- **用户确认与实施**：REQ-2026-09-05-038要求实际strict:true而非prefer。沿用Pi Benchmark会话与onPayload钩子，从完整Forge Schema派生严格传输Schema；递归、CTE、子查询与集合运算保留。可选占位null在进入现有编译评估链路前移除，SQL NULL保留；Direct仍为文本SQL。
- **失败关闭**：不支持的API或显式supportsStrictMode:false拒绝；禁止第二次Forge生成请求，Provider与Session自动重试关闭；历史prefer及Schema/Prompt漂移不得续跑混分。
- **实证**：用户批准最多2次Luna请求；实际md-034双臂各1次、均HTTP200、零自动重试。Forge真实payload含strict:true、指定函数tool_choice及parallel_tool_calls:false；原始参数独立通过严格Schema校验，规范化后完成编译执行。证据：`benchmark-luna-strict-output-2026-09-05.json`。
- **边界与决定**：TypeScript typecheck和129项Pi测试通过；仅Luna/Codex服务端完成实证。结构合法不等于别名或业务语义正确，不将1题成功称为准确率提升，不改写历史500题/独立H/外部采用门禁，不扩大量测。

#### 15.1.16 隐式别名确定性修复

- **范围与根因**：用户确认继续处理md-052；REQ-039修复Compiler渲染接受event e、引用校验却只识别event AS e的不一致。共用解析器支持省略AS的表/限定表/括号子查询别名，不改变Prompt、Schema或模型输出；不放宽未知引用与重复绑定检查。
- **验证**：修复前3个行为反例失败；修复后Compiler/Benchmark聚焦141项、5方言解析通过。20个原Luna候选与冻结HTTP上下文离线复算，仅处理组md-052 Forge的SQL改变；编译/Assurance/执行/EA/Contract通过，结果September Speaker。
- **结果与边界**：处理组Forge 2/5→3/5，其余三组仍3/5；零回退、零模型调用。证据：`benchmark-luna-alias-fix-2026-09-05.json`。不覆盖历史运行、不重评历史500题；修复Compiler不代表元数据有准确率增量，也不解除H与外部采用门禁。

#### 15.1.17 严格输出二十次新生成配对

- **授权与输入**：REQ-040，用户明确选择完整配对20次；同5题、两元数据条件、双臂一次生成，当前strict Schema与Compiler一致。10份上下文和20份用户指令冻结。
- **实际执行**：总20次HTTP200、零Provider自动重试、零替换调用。第10次发生Pi递归子Schema强制转换栈溢出，本地第二轮尝试被拦截；用户确认仅继续剩余10次。原失败记录不覆盖，所有原始响应经同一最终本地链路复核。
- **运行器修复**：strict原始参数在prepareArguments中校验并归一化，再交Pi canonical Schema校验；Provider Schema不变、未修改依赖。缺失候选不发布completed，失败token不再清零或从合计排除；132项Pi测试/typecheck通过。原故障臂3819 tokens从session恢复，候选仍因CTE未投影total被Assurance拒绝。
- **结果与代价**：旧元数据Forge EA/Contract 2/5、Direct3/5；修复后元数据两臂3/5。tokens 24021→33883（+41.06%），总57904。当前配置Forge与前轮固定候选复算同为3/5；md-052四臂均通过，md-065分组参考争议和md-077姓名列契约未解决。证据：`benchmark-luna-strict-paired-2026-09-05.json`。
- **决定**：不以5题或跨生成差异宣称总体/严格输出单因素优势，不更新历史500题成绩，不扩大调用；下一步先明确业务分组和结果列契约，H与外部采用门禁不变。



#### 15.1.18 当前配置跨库十题扩展回归

- **用户确认**：REQ-041，选择10题/20次请求；不是自动扩大REQ-040，也不解除H审核门禁。
- **冻结与目的**：从R500按固定哈希跨10库各取1题，排除最近5题；只依赖库/题号，不依据答案。当前配置单条件双臂，新生成后不修改代码、Prompt、元数据或评分。
- **门禁**：最多20次，零自动重试、零替换，异常停止；只读公开数据、Pi SDK认证。独立复核原始候选、EA/Contract、执行和真实消耗；不将无旧配置对照的小样本结果宣称优化增益或泛化。
- **完成证据**：20次HTTP200、零Provider重试/替换，10个Forge参数严格/真实Pi校验、20个原候选独立复算通过，源码与10库hash不变。Forge EA6/10、Contract5/10、Compile/Execution10/10；Direct EA/Contract7/10、Execution10/10。tokens 51077 vs29953（+70.52%），总81030。证据：`benchmark-luna-expanded-regression-2026-09-05.json`。
- **暴露问题与决定**：目标聚合未投影、JOIN改变分母、日期格式/粒度和姓名列契约仍有失败；md-454漏Top-5但当前数据EA通过，隔离六校反例证明逻辑不等价。原评分不改、无新代码调优或扩大调用；将样本当故障证据，不当业务规则来源，不宣称泛化或优化单因素增益。

#### 15.1.19 BIRD查询准确性基础建设（REQ-044）

- **已确认方向**：Mini-Dev固定快照与官方EX作为主要公开查询回归/阶段验收基准，不以冲榜为当前主线；查询正确性不能由审批、证据或拒绝替代。高覆盖、低静默错误和低用户负担共同约束。
- **验证节奏**：日常使用相关问题、预先固定正确对照与跨库样本；阶段验收运行R500全量并报告按库/难度、改善/回退和成本。H独立验证、安全与外部采用门禁保留。不得把子集/已曝光成绩当独立泛化，不以Gold反推产品规则。
- **当前实施切片**：离线诊断聚合表达与最终投影不一致，区分生成、DSL约定与确定性编译问题；先验证合法查询反例，避免“自动补列”或“未使用即拒绝”损害正确请求。
- **阶段验收规则**：固定模型/数据/评分/预算与对照，逐项证明改善和回退；EX、Contract、覆盖、静默错误和成本分别报告。暂不设无依据的百分比门槛；不自动扩大模型调用、切换输入路径或实施新Agent循环。
- **首个诊断完成**：3份原始生成的select已漏掉目标表达式，Compiler忠实复现；6种合成行为通过，机械补齐所有agg别名导致4个合法结果契约被破坏。确认生成表达问题，不把原因泛化为Compiler计算缺陷；Prompt是否是原因未获因果证明。
- **单变量实验完成**：用户另批20次新生成；5题×两指令条件×Forge/Direct，全部HTTP200、零Provider重试/替换。Forge EX/Contract4/5→4/5，3个正确对照保持通过；Direct指令完全相同却4/5→5/5，保留生成波动事实。20个原始候选经运行链路与BIRD官方EX核心分别复算零差异，10份strict/Pi参数校验通过。
- **决定与代价**：候选Prompt不采纳，源码/Schema/Gold/评分不改；不以旧失败题本轮恢复宣称收益，不追加拒绝、补列或付费抽样。双臂tokens36284→36262，总72546，Forge自身+0.25%。证据：`benchmark-luna-projection-instructions-2026-09-05.json`。首个表达说明假设闭环为未获收益，后续须另选有证据的通用机制并单独冻结实验。

#### 15.1.20 评测标准与工程化协议（REQ-045）

- **用户确认实施**：将BIRD基础能力基准、高覆盖/低错误/低交互负担和实验规则固化为可执行工程；本次不发起新模型调用或部署。
- **实施切片**：复用CLI/Benchmark链路提供不可变协议清单、分层选题、离线重放和严格对比；Pi原生绑定清单、明确预算、隔离上下文、保存真实候选/使用量并禁额外回合。增加质量指标与字段日期证据审计，不新建Orchestrator。
- **门禁已落地**：固定88个公开数据资产；Gold-only评分，SHM/空WAL不当数据漂移，非空WAL/journal拒绝。已评分失败保留EX=0与分母且可比较，未评分/漂移/旧诊断失败关闭；固定候选跨Compiler/Schema对比绑定原始output hash，生成源码/SDK/model/预算保留。缺usage显式未知，不从成本消失。
- **验收完成**：实际D5/R500冻结及验证、10个历史候选重放、合成失败分母/可比与诊断不可比命令、质量指标与日期审计、HTTP协议门禁、实际Web预算及未知成本展示均通过。Python786 passed/26 skipped，Pi143 passed，typecheck通过，Pi门禁接入CI；零外部模型调用、无部署。证据：`benchmark-standards-engineering-2026-09-05.json`。
- **后续边界**：这是可复用测试工程，不是新准确率收益；现行Prompt和官方EX不变，H审核与R0.6外部采用仍未关闭。任何新生成实验按标准单独声明假设/变量与预算。
- **首次标准真实运行（另批40次）**：Luna D20/seed42覆盖11库，Pi派发40次、返回39候选；md-472 Forge 120秒超时使Run failed，md-340 Gold在原30秒限制内不可评分。离线重放与compare按不完整记录拒绝晋级，20题中Forge确认正确10/失败9/未评分1，Direct9/10/1；已观测149300 tokens加一次未知消耗，不发布正式准确率或收益。40份raw hash与37份已评分候选判定核对一致。证据：`benchmark-luna-standard-d20-2026-09-05.json`。先处理Gold预检及未评分状态贯通；不自动重试、放宽Gold/评分、扩大R500或改变H/外部采用门禁。

#### 15.1.21 评分就绪与混合回归（REQ-046）

- **已授权**：先修复Gold生成前预检、未知评分与全分母，再运行一轮Luna20题/最多40次，不自动追加轮次。上轮任一臂EX/Contract失败或未知全部延续；余位按70% Luna未测、30% Luna已测随机抽取，整数四舍五入，互斥且不放回。
- **本轮冻结口径**：已有Luna留存生成覆盖33题；上轮11题必须延续，余9位为6新/3旧，seed43。历史其他模型完整500题曝光仍保留，不称独立holdout。复用CLI freeze、Pi原生protocol和原候选replay，不新增调度或任务状态。
- **门禁**：默认require_all在Gold预检失败时拒绝整轮；用户追加批准skip_unscorable，将Gold阻塞留分母、零生成，并固定缩减预算和阻塞报告，预检漂移需重新冻结。失败题超规模或池不足仍拒绝，不换题/比例/预算。SQL/Gold/数据与30s/120s限制不默改。
- **验证与本轮闭环**：Python803 passed/26 skipped，Pi149 passed/typecheck与实际界面通过。20题中Gold跳过1题，实际38次派发/38候选，无生成超时与补跑，tokens139490。EX确认正确8/9、Contract5/8，各1题未知；新题6/6 EX但Contract4/6与5/6，不能仅看EX或跨混合样本宣称提升。38响应hash与40臂重放一致，compare拒绝正式基线。
- **每轮交付与下一步**：用户要求每轮测试后出简报；本轮见`benchmark-luna-mixed-d20-2026-09-05.md`和同名JSON。Gold规划器问题仍阻塞，md-046/md-249 Forge再生成回退、md-259 Direct恢复。下轮按规则15延续＋4新＋1旧，未自动追加模型调用；H与R0.6门禁不变。
- **同配置续测seed44/45**：分别获用户“好的，那就继续”“继续，”授权，各38派发全部返回，tokens分别134585/137028；Gold阻塞均留20题分母。最新seed45为16延续/3新/1旧，EX确认正确6/6、Contract3/4，各1未知；相同15道可评分延续题EX2→4/4→4，Contract0→1/2→2。Forge md-289完整恢复，md-429仅EX恢复，未形成代码优化因果证据。两轮均38响应hash与40臂重放一致，源码/模型/运行版本/数据指纹不变。最新简报：`benchmark-luna-mixed-d20-seed45-2026-09-05.md`，同名JSON及seed44历史报告保留。下一轮17延续＋2新＋1旧，未自动追加调用，优先离线归因持续错题。
- **离线归因闭环（用户另批继续，零新生成）**：17延续题主因6道生成/作用域错误、6道参考冲突、4道输出契约歧义、1道Gold阻塞；不改官方判分、不发布纠正后准确率。独立重执行91条分片SQL与原38候选全链路重评一致，源码/数据不变。md-447实际为QUALIFY外层缺少源列作用域而非虚构字段；搬条件和换Gold编码的手工反事实不作为收益。未满足狭窄确定性修复门槛，未改Compiler/Prompt/Schema/Gold/评分；下一候选聚焦带来源的实际日期格式上下文，机制/预算另行冻结，不自动新增模型调用。见`benchmark-luna-offline-triage-2026-09-05.md/.json`。

- **恢复滚动seed46（2026-09-06，用户另批“继续吧”）**：20题为17延续/2新/1旧，日期off，md-340留分母零调用；38派发/38候选、136237 tokens且用量完整。EX确认正确6/4、Contract3/2，各1未知；同一16道可评分延续题EX3→4/3→2、Contract0→1/1→0。Forge md-249恢复；Direct md-082 Contract、md-429 EX回退；新md-044与旧md-034双臂通过，md-222双臂失败。38响应hash、40臂评分与SQL一致；md-340 Forge编译状态投影not_applicable/pending差异保留，不影响未知评分。共享输入/生成契约/模型/数据未变，但日期机制已使协议升v2及三处源码指纹不同，不宣称正式跨轮可比或优化收益。简报：`benchmark-luna-mixed-d20-seed46-2026-09-06.md/.json`。累计Luna曝光48题；下轮18延续＋1新＋1旧，未自动启动，未在本轮移除expected_grain。
- **滚动seed48（2026-09-06，另批“执行本轮38次”）**：采用REQ-048预定off父Run，19延续＋新md-313，日期/粒度off，Gold md-340保留且零派发。实际32派发/30候选后因md-234 Forge的120秒生成超时失败，md-447 Forge随后取消；md-472/481/483共6臂未派发，无补跑。两臂EX均4正确/12失败/4未知、Contract均1/15/4；仅新题双臂全过，Direct md-429 EX恢复、md-259 EX/Contract回退。观察107160 tokens，另2次用量未知，不作完整准确率或成本收益结论。
- **seed48生成前维护与闭环**：首次POST在创建Run前因1.0→1序列化导致冻结hash漂移，零调用；删除未使用的selection.remaining_new_fraction并重新冻结，题目/上下文/38次预算不变，HTTP协议200、创建Run202。修复前回归失败，修复后Python819 passed/26 skipped、Pi150 passed/typecheck通过；32留存响应hash、40臂核心评分重放、28可执行SQL独立EX一致。具体失败和非评分投影差异保留原证据，compare拒绝未评分基线，服务已停止。简报：`benchmark-luna-mixed-d20-seed48-2026-09-06.md/.json`。后续父Run为本轮，19延续＋1新，累计Luna曝光50题；不自动使用本轮6次或旧24次余额，H/R0.6门禁不变。

#### 15.1.22 严格归因与日期格式单变量实验（REQ-047）

- **已确认范围**：先收紧争议归因，再准备共享实际日期格式上下文；本次不含未声明预算的新生成。歧义必须同时有场景正证据、实质答案差异、且不能靠现有数据事实/确认语义/通常语言习惯消除。
- **归因修正**：md-020/429为输出粒度或冗余错误，md-249/273为表示差异，不再要求业务澄清。md-153时间口径和md-234同址赛道实体合并保留中等置信度澄清；md-472极值过滤、md-481可从元数据确定年份不算真歧义。独立候选错误与原官方评分保留。证据：`.forge/benchmarks/luna-date-context-20260905/ambiguity-review.json`。
- **机制与证据**：37个日期字段的有限只读样本中，3个声明冲突、33个未识别到明确布局声明、1个样本相容；不保证整列。新增date_context off/observed窄开关，默认off，仅投影本题可见字段，同一后缀给Forge和Direct；不改CSV/Gold/Compiler/Schema/评分，不自动转换日期。协议绑定事实及来源hash，源码、样本上限、ContextSnapshot或未声明指令漂移仍拒绝。
- **实验已冻结**：Luna五题md-483（日期失败目标）、md-077（日期相关探针）、md-085/231（原正确日期对照）、md-289（原正确无日期跨库对照），两条件均无Gold阻塞。至少一臂md-483须相对新基线改善日期绑定并新增EX/Contract通过，其他目标臂和正确对照不得回退；不满足则无推广证据，默认仍off，不补样追分。每条件每臂仅一次生成，无法排除采样/时序波动，不作稳定因果或泛化结论。
- **离线验收完成**：实际CLI冻结/预检/历史诊断重放通过，过期源码清单拒绝；两臂日期后缀相同，原snapshot/schema未变，无日期对照后缀为空。10个历史候选判分不变，原seed45的38个可评分候选重评不变；Python810 passed/26 skipped、Pi149 passed/typecheck通过。此处只有工程与评分保真证据，没有新生成收益。
- **调用已闭环**：用户在预算选项中明确选择“执行完整20次”；两条件各10派发、共20候选，零补跑/替换，重试策略0，用量全部已知。基线Run `pbr_716a9e146af7424a858e735647680aec`，处理Run `pbr_f90aeca4d5544fd59aa74f6809db7886`；本轮临时本地服务已停止。
- **实跑结果与决定**：Forge EX/Contract2/5→3/5、Direct3/5→3/5；唯一新增通过是未获得日期补充且输入相同的md-289，不能计为日期收益。md-483两臂从YYMMDD改为ISO、md-077 Forge日期绑定恢复，仍因结果粒度/姓名表示与参考不同未通过；预声明目标题门槛未满足，默认off、不推广、不补样。两条件tokens34621→38944（+12.49%），总73565。
- **最终证据**：20臂原始hash及离线重放一致；19个可执行候选和5个Gold经只读SQLite与官方EX集合核心独立复算一致，1个Assurance拒绝保留。源码/数据/模型/生成Contract及冻结上下文一致，比较门禁通过但不构成稳定因果。简报：`benchmark-luna-date-context-2026-09-05.md`，同名JSON保存逐题候选及证据hash。下一步仅建议离线复核输出契约，不改Gold追分；日常17+2+1、H与R0.6门禁不变。
- **输出契约复核闭环（2026-09-06，用户继续，零新调用）**：26条结果查询、5个完整DDL且外键有效的隔离反例、8份冻结Prompt来源核对完成。md-483的expected_grain=grouped来自each/per正则并进入两臂输入；每月单位也触发grouped，不能当业务确认。当前一账户一贷款不是DDL保证，合成数据区分总计3与账户2/1。md-077的CSV明确支持拼接全名；Gold活动关联把14/12个参与记录扩为26行再去重为2，隔离反例揭示范围、重复、不可逆姓名和NULL边界。原候选日期错误、官方评分和原20次结论保留。见`benchmark-luna-output-contract-review-2026-09-06.md/.json`。
- **Gold诊断与下一候选**：允许主动分析Gold定位错因并总结通用改进；禁止泄漏本题答案进入被测生成、篡改原始分数及把调参集冒充独立H。对照Gold不是作弊，也不意味着每个参考JOIN都是业务规则。优先考虑将未经确认的expected_grain移出生成契约，保留问题与事实；不添加更多per特判或按题号硬编码。此次仅完成诊断、未修改实现，新的生成消融仍须单独冻结与授权；日期观察默认off及H/R0.6门禁不变。

#### 15.1.23 未确认粒度声明切换与单变量消融（REQ-048）

- **范围已接受**：用户“好的，那就继续吧”授权共性问题的单变量工程推进；删除正常ResultContract中的expected_grain，不自动授权新的模型调用。
- **实现与边界**：Python/Pi Contract干净切换，无grouped/scalar默认替换；原each/per规则只在显式grain_context=question_heuristic对照中生成带question hash、business_confirmed=false的共享后缀，不进入评分ContextSnapshot。正常off；两条件相同源码/评分上下文/Schema/日期设置。此新基线不逐字复原seed46，不能以历史候选冒充配对基线。
- **工程准备验证完成**：单位表达误判已复现并移除；seed46原40臂SQL与EX/Contract重评不变，20题输入仅删除粒度字段及派生hash；4次真实HTTP评分与旧v2协议409通过。Python813 passed/26 skipped、Pi149 passed/typecheck通过。准备阶段零新增模型调用，后续付费运行另计。
- **冻结与实际授权**：seed47为18延续＋新md-199＋旧md-040，沿用完整Luna曝光历史；日期off，Gold md-340留分母零生成。用户随后明确选择“执行完整76次”，两条件各38次预算，无重试/补跑/换题；预先指定off处理组为下一轮父Run，对照计入曝光历史。
- **生成终态**：对照Run `pbr_fb550c000ff34a559d1f79e7738107ec`仅14派发/10候选，md-153、md-199两臂均空响应，24臂未派发且不可resume；off Run `pbr_cb5bcb3341a24f619b7e8a75bbd83236`完成38派发/38候选，仅因Gold未知不构成完整Run。合计52派发/48候选，观测172108 tokens，另4次用量未知；无补跑。
- **结果与门禁**：共同有效5题的两臂EX均3/5、Contract均1/5，零恢复、零回退，不能以此幸存子集满足完整配对验收。off的EX正确均4、Contract Forge1/Direct2，各1道Gold未评分；Forge91339 tokens、Direct50198，未显示准确率或成本优势。保留工程切换，不宣布生成收益，不晋级正式基线或H/R0.6。
- **后续维护已验证**：用户“好的”接受先修复两项维护问题，不新增模型调用。replay只对无候选且显式scored=false的记录保留未评分/原执行状态，真实生成失败仍计失败；新Pi raw_output.assistant保存content、stopReason、errorMessage，失败日志带Provider原因。两个修复前反例均失败，修复后Python814 passed/26 skipped、Pi150 passed/typecheck通过；本地合成Provider失败经真实Pi工作器、SQLite与日志读取验证，未调用真实Provider。历史丢失的4次错误原因不追补。
- **证据与后续边界**：准备及付费证据保持原样；后续维护证据为`benchmark-luna-grain-maintenance-2026-09-06.json`。用当前源码新冻结的diagnostic上下文重放原80臂：24个未派发臂恢复未评分，80臂SQL/EX/Contract/scored/execution与原Pi一致，原output/raw_output不变；Gold及生成失败的compile投影差异另列。源码指纹已改变，旧付费冻结不得续跑或强行比较，diagnostic/未评分仍不晋级。下一轮19延续＋1新＋0复核，Luna已见49/未见451；未启动下一轮，剩余24次授权未补跑。

#### 15.1.24 SQLite 标量极值 CTE 执行瓶颈（REQ-049）

- **范围已接受并闭环**：用户明确选择“修复执行瓶颈”，接受零模型调用与该优化SQL的SQLite≥3.35边界。只对参与INNER/CROSS JOIN的简单非递归列MIN/MAX标量CTE使用窄物化策略，不改原候选、Prompt、Schema、Gold或评分。
- **回归与边界**：修复前MIN/MAX两个固定VM预算反例失败，修复后通过，保留并列极值、NULL、空输入和标识符引用；未投影聚合保留下推。13类不应优化的结构与原源码输出一致，24个旧Forge候选的120项五方言编译结果只改变md-199的SQLite SQL。
- **实际收益**：原80臂完整离线重放，原output/raw_output均不变；只有off/md-199/Forge从30秒execution_timeout恢复为Assurance/执行/EX/Contract通过，独立本机只读烟测79.26ms。off Forge EX正确4→5、Contract1→2，各仍1未知；其他79臂核心状态/评分/SQL不变，Direct及对照不变。
- **验证与证据**：Python818 passed/26 skipped，Pi150 passed/typecheck通过；新diagnostic协议有效，旧源码协议与diagnostic晋级仍失败关闭。完整简报及证据索引为`benchmark-luna-scalar-cte-fix-2026-09-06.json`；旧Run、旧冻结与付费结果不改写，未启动新模型调用。
- **后续门禁**：收益限定于同候选Compiler修复，不能宣称新模型准确率、独立泛化或完整粒度配对收益。预定off父Run的续测已按新冻结与独立38次授权执行seed48，结果见15.1.21；本节固定候选修复与后续新生成分开记录，旧24次未派发预算不自动沿用。H与R0.6外部采用门禁不变。

#### 15.1.25 生成终止后的证据封存（REQ-050）

- **已接受的维护与闭环**：用户要求继续优化；只修复已确认的生命周期竞态，不增加模型调用或改变停止策略。原catch封存早于finally等待abort，可能遗漏最终assistant及usage；现共享同一abort Promise，在取消完成后封存，随后退订与dispose。
- **行为门禁**：120秒deadline、单次派发、零重试、失败即停、迟到输出拒绝与未知用量不变；不改Prompt/Schema/Compiler/Gold，不自动补列或按题号修分。原finally已经等待abort，本次不扩大生成窗口；不增加逐片全量复制或第二套响应状态。
- **验证证据**：deadline回归修复前失败、修复后Run重开仍保留最终响应/usage/hash且不执行迟到SQL。真实Pi SDK合成流显示最终化先后0→1条assistant、0→93个合成tokens，网络0；不是实际Luna用量恢复。Pi151 passed/typecheck、Python819 passed/26 skipped，原40臂diagnostic重放与旧工件hash不变，compare拒绝晋级。报告：`benchmark-luna-cancellation-evidence-fix-2026-09-06.json`。
- **后续边界**：这项修复改善归因可靠性，不提升已有候选成绩，也不能补回旧md-234缺失内容或两次未知usage。Pi runtime指纹变化，旧Run不跨版本续跑；下一轮仍需独立冻结授权，H/R0.6门禁不变。

#### 15.1.26 生成流阶段定向诊断（REQ-051）

- **已接受并完成**：用户另批4次；md-234/md-447两个独立单题双臂Run按题串行，保持Luna、Prompt、Schema、120秒、零重试及日期/粒度off。观察器仅聚合SDK可见事件；先做正常/取消合成流零网络烟测，不新增生产遥测或状态源。
- **真实结果**：4次生成与执行均成功，EX/Contract均0/4；19831 tokens、未知用量0。Forge生成11.18/23.60秒，工具参数可见包络3.27/7.46秒；未复现超时，不能把可见thinking包络解释为全部推理或证明REQ-050有延迟收益。
- **归因边界**：md-234的Gold不含题目所求次数，Direct另有按circuitId分组差异；md-447的候选DOCType与Gold DOC仅输出表示不同，离线换列得到相同Gold行多重集。反事实不采纳、不改原评分，不因追分删列或硬换编码。
- **证据与门禁**：4份Prompt与seed48对应臂hash一致；4份响应hash、4臂原候选重放及独立EX核验通过，57条日志与Pi完整快照封存，服务已停。未修改生产源码/生成策略、未追加请求；不是seed49，滚动父Run及H/R0.6门禁不变。简报：`benchmark-luna-generation-stages-2026-09-06.md`及同名JSON。

#### 15.1.27 保留CTE字段绑定与统一SQL保障（REQ-052）

- **已完成维护**：用户要求继续优化，零模型调用。删除Compiler的CTE限定名剥离与主CTE裸列猜测，不自动补聚合/投影或按Gold换字段。5个真实SQLite结果反例修复前失败、修复后通过；窗口测试从前缀断言改为结果行/Top-N验证。
- **公共边界闭环**：烟测发现CTE未输出字段在SELECT/WHERE中可被旧Forge JSON字段门禁放过。`assure_query`复用已有`assure_compiled_sql`，使用同一已限定Registry并保留先前门禁证据；Assurance升v8，两个漏拒反例已关闭，歧义拒绝对照保持。既有关系/业务Policy不放宽，Evaluate不授予执行权。
- **验证与收益边界**：40个历史Forge候选200项五方言结果不变、5份原Run hash不变；新v8诊断中md-032/md-199四臂SQL/EX/Contract不变，缺列继续拒绝。真实CLI/HTTP与返回SQL的隔离执行通过；Python825 passed/26 skipped、Pi151 passed/typecheck，服务已停止。不是新生成收益或完整R500复评，不解除旧协议漂移、H或R0.6门禁；滚动父Run仍seed48。
- **交付**：`benchmark-luna-cte-binding-fix-2026-09-06.md`及同名JSON。公共CTE→物理表关系和右CTE裸列的既有前置限制如实保留，不将Compiler合法等同全部业务Policy通过。

#### 15.1.28 当前栈回归收口与枚举绑定增量审计（REQ-053）

- **已完成范围**：R500固定候选回归与单一枚举机制审计闭环，新增被测模型调用0；不启动seed49。1000候选/历史SQL及500 Gold SQL hash核对，源码与公开数据不变。
- **回归**：Forge EX311/187/2、Contract282/216/2；Direct EX312/186/2、Contract292/206/2（正确/失败/未知）。无旧正确误拒、无已评分正确/错误转换；md-340/md-393四臂仅因30秒Gold超时由历史通过转未知。14变化臂全审，三个原错误在共享SQL门禁提前拒绝；诊断complete=false，不混同公共JSON全路径或v8单因素收益。
- **机制审计与决定**：当前两臂gasstations.Segment只有字段说明，缺精确枚举；公开只读全列5716行5个值提供真实输入增量。三个仅改字面量的反事实恢复参考结果，但四份原候选仍1/4正确，不改实际成绩；历史完整Prompt未知。仅提出带来源的分类值证据处理，不自动lower/LIKE或在线探库。
- **下一候选及门禁**：md009/md026＋正确对照md002/md018，4题×2条件×2臂为16次未授权提案；先冻结变量/实际模型及成本时延门槛，预算另批。当前不改生产源码、Prompt、Schema、Gold、评分或超时/重试；进程已退出，H/R0.6保持独立。简报：`benchmark-r500-v8-binding-audit-2026-09-06.md/.json`。

#### 15.1.29 限定列观察取值证据处理（REQ-054）

- 离线实现及单独获批16次Luna配对生成已完成；未通过目标四臂全对门槛，value_context保持默认off，不补跑/扩样。不启动seed49或改变H/R0.6门禁。
- **协议边界**：两臂同后缀、按可见限定列投影；记录查询、限制、覆盖和来源hash，独立绑定证据及完整指令。升bird-protocol-v4，旧冻结不续跑；只允许mode变化，列、界限、源码和原上下文仍校验。
- **验收**：默认off逐字不变；实际只读采集与越域/高基数/长文本/混合类型/大小写边界验证，Python/Pi消费及真实CLI/HTTP烟测。四题原/处理两条件冻结后交付证据，再决定是否授权生成；不改Gold/评分/生产执行权。
- 实现证明：真实四题CLI冻结、HTTP正负例、Pi完整指令消费/漂移阻断及原Sol八候选双条件回放通过；Python873 passed/26 skipped、Pi151 passed/typecheck通过。准备阶段与依赖误同步见`benchmark-luna-value-context-ready-2026-09-06.md/.json`，该文件保留授权前快照。
- 获批配对证明：16派发/16候选、未知标签/用量0；EX/Contract Forge2/4→3/4、Direct3/4→4/4，目标1/4→3/4而非要求4/4，控制无回退。35572 tokens、处理组+17.85%，成本线通过不能替代准确率线。md-009 Forge取值已正确但漏接CTE FROM/JOIN，v8未前置拒绝、SQLite失败；原失败保留，不补关系。16响应hash/回放/独立SQLite一致，216日志及真相源快照封存、两服务关闭。见`benchmark-luna-value-context-2026-09-06.md/.json`；FROM作用域校验仅为后续维护建议，不在本实验改码。

#### 15.1.30 FROM作用域绑定维护（REQ-055）

- 用户已接受零模型调用维护；复用共享assure_compiled_sql，拒绝未接入当前FROM/JOIN或合法相关祖先的限定关系。
- 不自动补JOIN，不改生成/Compiler/Gold/评分；原REQ-054失败与默认off结论不回填。保护合法相关/别名/CTE，并核对R500＋本轮16候选的误拒风险。
- 验收：先失败回归，再共享入口拒绝/合法SQL执行、CLI/HTTP及两侧回归、固定历史门禁对照；封存证据。旧冻结随Assurance源码修订失效，不续跑，不新增模型调用。
- 已完成（2026-09-07）：Assurance v9新增实际绑定/相关环境检查，修复前反例失败、修复后通过。1016历史SQL仅新增拒绝md-009处理组Forge；原R500三拒绝不变，旧正确新增误拒0。16原候选/SQL/EX/Contract均不变，只将一个执行错误前移为Assurance拒绝。
- 证明与限制：65个SQLite边界无新增合法误拒，18项其他方言/Scope检查仅静态；真实CLI/HTTP及合法返回SQL执行通过，Python887 passed/26 skipped、Pi151 passed/typecheck。13份历史来源hash不变，旧冻结及诊断compare仍拒绝，服务关闭；保持off、不新增调用/生成结论。完整简报与证据为`benchmark-luna-from-scope-fix-2026-09-07.md/.json`。

#### 15.1.31 分母范围范例实验准备（REQ-056）

- 用户接受首个窄切片：只评估一条合成Forge JSON范例，说明分母应遵循问题范围、不能被展示关联意外缩小；不是永远取全表。其他语义、CTE接口、返修等候选不同时实施。
- 复用现有prompt变量与forge_prompt_revision作为有限内置范例的选择/绑定，不另造Prompt平台或新Task状态；默认Prompt逐字不变，Direct指令/Schema/Context/Compiler/Assurance/Gold/评分不变。
- 零调用准备已完成：一条范例、8组隔离SQLite夹具/16次正反执行；默认500题输入hash不变，仅候选Forge指令增加2209 UTF-8 bytes。四题md-259/079/000/312双条件冻结与Gold预检通过，CLI/鉴权HTTP及Pi生产类合成消费/持久化/漂移失败关闭通过；Python889 passed/26 skipped、Pi155 passed/typecheck。合成会话不是模型效果。
- 用户后续明确批准16次，实际16派发/16候选完成：Forge EX/Contract2/4→3/4，Direct2/4不变；md-259完整恢复、md-000恢复，但md-079别名/投影错误回退。总53058 tokens、未知0，处理组+6.03%，成本/P95护栏通过；预声明零回退门槛失败，不启用默认，不补跑/扩样/自动返修或启动seed49。
- 16原响应hash/重放/独立SQLite与218日志、原生Pi状态快照一致，25准备工件及源码/数据/输入hash不变，服务已停。准备证据保留，正式结果见`benchmark-luna-denominator-scope-2026-09-07.json`；后续仅记录CTE别名/中间投影一致性为独立候选，尚无新实现/调用授权。

#### 15.1.32 CTE接口诊断与别名绑定维护（REQ-057）

- 用户接受继续验证CTE中间接口；确认另一个Compiler错误：本层聚合别名会覆盖显式限定源列，并跨SQL词法/查询边界替换。先修确定性编译错误，不增加Prompt补丁或自动返修。
- 复用SQLGlot解析、本层Column引用和原文跨度，仅展开真实本地别名；显式限定、字符串/注释、函数/类型名、子查询内部及插入SQL均不猜测改写。统一HAVING/QUALIFY与已有窗口/排序接口的相关漏展开。
- 已验证：12项回归先红后绿，42组独立边界全通过；SQLGlot下限27通过相同矩阵。548份历史候选2740次五方言编译不变；另3份合成探针仅限定列反例改变。上一轮16份原候选诊断输出/评分/失败分类不变，md-079仍拒绝。
- 实际CLI/API正负例及返回SQL通过；Python901 passed/26 skipped、Pi155 passed/typecheck，文档同步后相关107 passed。旧协议拒绝、新诊断独立冻结，默认Prompt/控制输入和数据hash不变；服务已停，不重试Gold阻塞或启动seed49。
- 结论：修复的是Compiler静默改写，不是模型CTE接口不一致；不宣称准确率收益，不启用自动补列/返修。报告：`benchmark-cte-expression-binding-fix-2026-09-07.json`。

#### 15.1.33 生成端CTE接口范例准备及开发对照（REQ-058）

- 只注册独立可选CTE接口合成范例，默认不启用，不叠加分母范例；不改Compiler/Schema/评分或增加返修。原零调用准备报告benchmark-cte-interface-ready-2026-09-07.json保留。
- 准备阶段已验证102项独立SQLite正反边界＋2项约束，默认500题四类hash不变，候选Forge仅增加2124字节；原Sol8候选双条件16项诊断不变。最终合成回执16派发、修正测试回执后累计48派发，真实模型0；原Python/Pi验证属准备证据，不当作本轮重跑。
- 用户对固定md-079/199目标＋md-312/386控制、最多16次新预算明确回复“继续”后，原生Pi完成控制8次、处理8次；单回合、零重试/替换，16全评分、未知用量0。
- Forge EX/Contract3/4→3/4（md-199新增正确，md-079回退）；Direct3/4→2/4（md-079回退），md-312/386两臂均正确。两个目标全对和零回退门槛失败，不能用CTE/编译通过代替完整答案；Direct输入不变，波动不归因于Forge范例。
- 总67367 tokens，控制32300→处理35067（+8.57%）；Forge每EX正确答案6792.67→7683（+13.11%）、生成P95 14.3516→20.312秒（+41.53%），后两项护栏失败。P95为每组4次最近秩最大值，不代表生产时延。
- 原16候选CLI重放、独立SQLite、响应hash及215日志核对一致；19准备工件/原报告及源码、数据、输入不变。首个启动请求在运行创建前因临时Pi服务密钥缺失拒绝、派发0；修正隔离配置后双协议门禁通过，不重放任何模型调用。
- 结论：不采纳默认，预算余0，不补跑/扩样/返修或启动seed49；原可选上下文off与seed48父Run不变。本阶段不改生产源码/Contract。正式报告benchmark-luna-cte-interface-2026-09-07.json，原生封存和清理见实验目录generation-*回执。

#### 15.1.34 发布核对与既有权限漏洞维护（2026-09-07）

- 用户明确要求push新版本、包含每轮报告并更新README；本次是main提交交付，不自动创建tag、发布包或部署。71份报告逐一索引，原字节留本地；公开副本的最小脱敏与hash映射见report-publication-2026-09-07.json。
- 维护既有全局CTE名称过滤造成的表权限漏检：按SQLGlot Scope来源和MappingSchema方言归一化校验物理表；合法CTE/derived保留，扁平Registry未授权的schema/catalog限定来源拒绝。Assurance v10使旧v9 QueryRun审批漂移失败关闭，必须重新prepare/审核；不迁移旧证据。
- 合成回归与实际API/SQLite维护零被测模型调用；旧报告、未知用量、失败/未完成实验及H/R0.6门禁不变，不把本次维护记为模型准确率增益。版本验证结果集中于release-verification-2026-09-07.json。
- 首次push的CI暴露外部DATA旁仓依赖，随后加强隔离又暴露六项HTTP测试默认状态目录依赖；只修复测试夹具与独立配置，不跳过用例、不改生产失败关闭语义。首轮失败和隔离更正保留于同一发布验证报告。
