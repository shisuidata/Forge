# Forge 企业演进阶段性实施计划 v1.1

> 状态：产品方向与计划已确认；M0.1–M0.3 已完成，待 Contract 评审；运行时 M1 尚未批准 · Last updated: 2026-08-24
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
- 每个工作包遵循“先更新计划 → 再实现 → 验证 → 回写状态/风险”。

中期产品定位：

> **Forge 是面向数据团队、供人和企业 Agent 共同使用的可信数据任务控制与执行平台。**

采用原则：

```text
GTM：Data-Team Led
体验：Business Accessible
架构：Agent Native
治理：Human Accountable
```

质量策略是 `100% Governed` 的受支持过程边界，而不是开放世界端到端 `100% Correct`。治理覆盖率必须以版本化 Action Catalog 中 `support_status=supported` 的 Action 为分母；目录外高风险 Action 失败关闭，不纳入能力声明。

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
| M0.2 Governance Contract 内核 | 已完成，待评审 | 已新增 6 个内核 Contract、共享 fixture、Python/TypeScript parity test；未接运行时 |
| M0.3 Governance Coverage Catalog | 已完成，待评审 | v1.0.0 收录 14 个 supported/governed Action；当前 100% 仅表示 Contract 分母完整 |
| M0.4 其余 Contract 草案 | 未开始 | 不阻塞 M1A；需 M0 内核评审后再开始 |
| M0.5 Fixture、威胁模型与迁移设计 | 未开始 | 不修改生产授权行为 |
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
- `AgentMandate v1`
  - 绑定 delegator、accountable principal、purpose、task、capabilities、resource scope、budget ref、approval policy、expiry。
  - `can_delegate` 必须显式给出，默认语义为 `false`。
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
| AgentMandate | Pi Governance | Mandate Store | Purpose、Task、能力、资源、责任主体、有效期；不含凭证 |
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

每个 Action 标记 Owner、执行者、风险级别、需要的 Principal/Mandate/Policy/Decision、真相源和失败策略。未进入支持目录的高风险 Action 必须 fail closed；对外只声明已纳入目录的治理覆盖率。

### M0.4 其余 Contract 草案

以下 Contract 在 M0 只形成 Schema、fixture 和威胁边界，不进入生产运行时，也不阻塞 M1A：

- Coordination：`TaskParticipant`、`DecisionRequest`、`DecisionRecord`、`ExecutionPlan v2`。
- Economics：`BudgetContract`、`UsageRecord`、`CostCatalogRevision`、`OutcomeRecord`。
- Context：`SourceRef`、`ClaimRecord`、`EvidenceLink`、`ConflictSet`、`MemoryProposal`、`ContextBundle`。
- OAuth：`ModelBackend`、`AuthSlotRef`、`ModelCompatibilityResult`、`ModelFallbackPolicy`、`QueryPlanningEnvelope`、`ForgeQueryCandidateSubmission`。

### M0.5 Fixture、威胁模型与迁移设计

使用现有 Web 和飞书 Query Task 各制作一条完整 fixture，覆盖：

- Human requester → Pi service actor → Forge execution。
- Agent actor → accountable human/org principal → task-scoped mandate。
- SQL Action、PolicyDecision、审批和 QueryRun lineage。
- 空的 Economics/Context 扩展位也必须显式表示，不能靠任意 metadata。

威胁模型覆盖请求身份篡改、Service Key 重放、delegation 扩权、默认允许 ACL、跨租户枚举、Context 跨 Purpose 泄露、预算绕过和旧审批迁移。

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
- `agent/contracts/` 新增 `PrincipalContext v1`、`AgentMandate v1`、`PolicyDecision v1`、`ResourceRef v1`、`DatasourceBinding v1`、`RegistryBinding v1` 及共享有效/无效 fixture。
- `services/pi-orchestrator/src/governance-contracts.ts` 提供对应 TypeBox 类型；Python JSON Schema 与 TypeScript 使用同一 fixture corpus 做行为 parity。
- Governance Action Catalog v1.0.0 收录 14 个 supported Action，固定 Owner、Executor、风险、Required Context、Truth Source 与失败策略；目录外高风险 Action 语义为 fail closed。
- 新增 Contract 与 Catalog 文档，明确 Owner、真相源、最小披露和“Contract Coverage 不等于运行时 enforcement”。
- 未修改 Task API、TaskRun/QueryRun 数据库 Schema、现有授权逻辑、数据库行为或 OAuth Runtime。

验证：

- Python：`544 passed, 24 skipped`。
- Pi Orchestrator：`88 passed`。
- TypeScript typecheck：通过。
- `npm audit --omit=dev`：0 vulnerabilities。
- JSON 解析、文档链接和全局 `git diff --check`：通过。
- TypeScript/Python LSP 未配置；分别使用 `tsc --noEmit`、Python 全量测试和契约测试替代。

遗留风险与下一步：

- M0.4 Coordination/Economics/Context/OAuth 草案和 M0.5 完整 Query fixture、Threat Model、迁移设计尚未开始。
- v1 Schema 负责稳定形状和最小类型不变量；跨对象 Organization/Workspace 一致性、时间先后、撤销状态和 delegation chain 连续性需在 M0.5 明确为语义门禁，并由 M1 PEP 失败关闭执行。
- 新 Contract 和 Catalog 当前没有生产调用方；未经本轮 Contract 评审不得接入 M1A。

## 3. M1A：服务身份、Delegation 与默认拒绝（近期，详细）

### 3.1 行为与接口变化

1. Pi 除 health 外的 API 全部要求服务身份；Web、Channel、Admin、Automation 使用不同 credential 和 scope。
2. 浏览器或渠道请求中的 `org_id/team_id/user_id` 不再是授权依据；严格模式从已认证身份和服务端 Identity/Membership 映射生成 `PrincipalContext`。
3. Pi → Forge 请求携带 task-scoped、短期、audience-bound 的 Delegation Envelope；Forge验证服务身份、Task、Mandate、expiry 和 resource scope。
4. TaskRun v2 保存 `principal_context_ref/hash`、`mandate_ref/revision`，不保存 token 或完整认证材料。
5. 企业/生产 profile 改为 deny-by-default；无明确 Policy/Binding 时拒绝准备或执行查询。
6. 错误语义固定：未认证返回 401；已认证但越权按接口风险返回 403 或统一 404，跨租户资源枚举统一 404。

### 3.2 真相源

- Pi Governance 模块：Principal、Membership、Service Identity、Agent Mandate 和 Task delegation。
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
