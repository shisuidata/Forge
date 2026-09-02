# Forge 企业演进阶段性实施计划 v1.2

> 状态：`REQ-2026-09-03-025` 为当前产品主线；`REQ-2026-08-26-024` SQL Accuracy Benchmark 已验证并作为反证基线；`REQ-2026-08-25-023` 已吸收为历史短期切口。当前阶段为 R0 Open-source Trust Runtime Product Cut / Adoption Baseline；Runtime Governance Coverage 仍为 0% · Last updated: 2026-09-03
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
| R0 Open-source Trust Runtime | 当前开始 | `REQ-2026-09-03-025`：以 Evaluate/Enforce/Explain 收敛外部开发者入口；Direct SQL 一等输入；Golden Path 与开源采用证据优先 |
| S0–S4 真实用户短期产品闭环 | 已被 R0 吸收为历史验证路线 | `REQ-2026-08-25-023`：Design Partner、Enterprise Reference 与 Thin Founder Sandbox 的证据分工继续有效，但不再是唯一主动计划 |
| W3 Web 产品骨架与交互框架 | 已吸收进 SP3–SP5 | `REQ-2026-08-24-014`：`821065f` 隔离 Product Shell 已证明页面骨架，但错误移除了连续 Chat；“分析工作台”修订也被判定过窄。先以 N1 北极星重建产品地图，确认后再修订原型；W3B 不进入 |
| H1 Analysis 延迟与进度修复 | 已完成 | `REQ-2026-08-24-005`：Artifact-first Adapter、Provider failure 分类、StageAttempt deadline/phase 时间元数据和 Web elapsed/slow 提示；107 行真实 smoke 从临界 229/240s 降至 119s，不改变 SQL、审批或 Task 真相源 |
| H2 长文本语义化阅读体验 | 已完成并部署 | `REQ-2026-08-24-006`：NAS `9fca1ea` health/readiness/认证门禁通过；隔离 ReportStore HTML/PDF/PPTX exporter 全部 ready，无 SQL/Task 重放 |
| H3 Golden Journey 双验收 | 已完成，产品 FAIL | `REQ-2026-08-24-007`：物理链路 PASS；桌面旅程/可信交付 FAIL。发现 PDF 路径泄漏、same-page Publication 空白、Chart grain 误导 3 个 P0 |
| H4 Golden Journey P0 Closure | 已完成并验证 | `REQ-2026-08-24-008`：真实 NAS PDF 路径清除、same-page Report/Publication 可见、重复 label Chart fail-closed；同一 Golden Journey 复验 262.399s，物理与三个 P0 门禁 PASS |
| H5 Evidence-bound Chart Storytelling | Editorial revision 暂定保留，生产 R1 未批准 | 已改为连续报告并建立受控强调规范；用户要求先保留当前形态，后续再做视觉与语言精修。Atlas 只发布隔离静态预览，不等于接入生产 Renderer |
| D1 Atlas 隔离报告预览 | 已完成 | `REQ-2026-08-24-013`：`929e8d4` 固定构建物独立发布到 `192.168.8.10:18005`；生产源码仍为干净 `d2b0fd9`，Forge/Pi 未重启；阶段差距重评估完成 |
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
- 固定 commit `821065f` 发布到 Atlas `/home/elazer/services/forge-previews/web-shell-821065f/`，`forge-web-shell-preview.service` 仅绑定 `192.168.8.10:18006`。生产 Jinja、Forge/Pi、Store 和 `d2b0fd9` checkout 未修改。
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
- Atlas 隔离预览已完成：`/home/elazer/services/forge-previews/editorial-929e8d4/` 为不可写固定构建物，`forge-report-preview.service` 仅绑定 LAN `192.168.8.10:18005`；远端 browser gate 通过，生产 Forge/Pi active、源码仍为干净 `d2b0fd9`。生产 readiness 保留已知的内网 HTTP `secure_cookie` fail，本次未改认证或 HTTPS。
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
R0.1 Unified Input Contract：Direct SQL / Forge JSON
  → R0.2 Evaluate：Exact Result / Regression / Failure Taxonomy
  → R0.3 Enforce：Policy / Assurance / Read-only / Approval
  → R0.4 Explain：Evidence / Lineage / Limitations
  → R0.5 Public Golden Path：README / Quickstart / CLI-API / Dashboard
  → R0.6 External Adoption Evidence
```

R0 未通过前，不恢复 M1A、G1、Q1、H6 或更广企业平台实现。Atlas candidate 可在不扩张产品范围的前提下单独复验；不得因产品切割删除已有 Contract、测试、审计或失败关闭边界。

明确假设：

- 采用“近期详、远期粗”，不为远期阶段提前设计全部存储和服务边界。
- 当前优先保护私有单机部署兼容，但企业 profile 必须严格默认拒绝。
- 近期不拆 Governance、Economics、Context 独立微服务。
- 不引入新的通用 Agent、渠道或无边界业务系统写操作。
- 所有真实 OAuth 登录、生产凭证、客户数据源和权限变更均需要用户单独明确授权。
