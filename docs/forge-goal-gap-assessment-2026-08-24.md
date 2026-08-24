# Forge 目标差距重评估（2026-08-24）

> 评估对象：当前本地 `929e8d4`、Atlas 生产基线 `d2b0fd9`、M0–M7 唯一主动计划。百分比只表示对已定义目标的阶段估计，不是准确度、可靠性或商业成功率。
>
> **后续优先级修订**：本评估最初建议 M1A 作为下一工作包；用户随后确认没有稳定产品骨架就无法持续人工测试和指导方向。根据 `REQ-2026-08-24-014`，当前先执行 W3 Web Product Shell，M1A 顺延为 W3 后首个后端治理工作包。长期差距判断不变。

## 1. 先区分两个目标

### 近期产品目标

让一个数据团队能够把真实问题推进为：澄清 → 受控取数 → SQL 审批 → 只读执行 → Evidence-bound 分析 → 报告交付，并在失败、重试和重复消息下保持可追踪。

**当前估计完成 65%–70%，仍差 30%–35%。**

已经成立的是物理主链、查询安全、Task/Artifact、真实模型分析、报告导出和三个 P0 的失败关闭。主要缺口是报告候选尚未进入生产、可复用报告只有 Contract、关键 P1 体验未关闭、生产 Runtime Governance 仍未执行新 Contract。

### 长期企业目标

让人和企业 Agent 在明确 Principal、Mandate、Policy、Evidence、Decision、Budget 和责任主体下，共同执行可信数据任务，并能证明成本、权限、结果和行动的一致性。

**当前估计完成 30%–35%，仍差 65%–70%。**

原因不是核心代码不足，而是企业闭环中最关键的运行时授权、多人职责、成本账本、平台 Assurance、HA/SSO 和第二场景均未完成。M0 Contract Ready 不能折算为生产治理已完成。

## 2. 分项状态

| 维度 | 阶段估计 | 已有证据 | 最大缺口 / 反证 |
|---|---:|---|---|
| 可信查询执行 | 80%–85% | Registry、Forge JSON、Compiler、Assurance、hash 审批、只读执行、QueryRun、幂等；真实 Golden Journey 仅一次审批/执行 | 生产 ACL 仍可默认允许；单 Datasource/Registry Binding；企业资源 Policy 未统一 |
| Pi 任务编排 | 70%–75% | TaskRun、ExecutionPlan、Artifact、StageAttempt、lease、timeout、恢复、渠道统一事件 | Principal/Mandate 尚未进入普通 Task runtime；多人 Participant/Decision 尚无运行时 |
| 分析与报告交付 | 45%–55% | Artifact-first 分析、HTML/PDF/PPTX、长文可读性、P0 关闭、ECharts Editorial candidate | Chart v2 period-delta Contract 不完整；候选未进生产；内容与视觉仍需真实用户迭代；H3 P1 尚存 |
| Governance | 20%–25% | Contract Coverage 100%、40 个负向 mutation、M0.5 review trace | Runtime Governance Coverage 明确为 0%；共享管理员、单 team、默认允许 ACL 仍存在 |
| Coordination | 30%–40% | 单任务状态、审批点、跨渠道 projection、Artifact lineage | 无 Owner/Requester/Steward/Approver 分离；无 DecisionRecord/obligation/supersedes runtime |
| Economics | 5%–10% | Stage latency、timeout、模型 revision 与部分执行元数据 | 无真实 token/cost ledger、Budget reserve/settle、人工审核成本、Cost per Trusted Outcome |
| Assurance 闭环 | 45%–55% | 查询 Gate、Evidence lineage、失败关闭、真实 Golden Journey、报告泄漏检查 | 任务级 QualityContract、统一 Audit envelope、silent error/abstention/override 指标尚未形成 |
| 企业运维与交付 | 20%–30% | Atlas 单机服务、备份/回滚、readiness、认证、只读库 | 内网 HTTP、Secure Cookie fail、SQLite/本地对象、无 OIDC/SCIM/HA/RPO/RTO/OTel/KMS |
| 可复用报告资产 | 10%–15% | Definition/SemanticQuery/Binding/Criteria/Run 设计与 reuse decision 规则 | 无 Store、editor、manual rerun、compatibility planner runtime 或 Run history |

## 3. 当前最重要的判断

1. **Forge 已经证明“可信数据任务主链可以跑通”，但还没有证明“企业治理闭环已经成立”。** Golden Journey 的物理成功是真实资产；Runtime Governance Coverage=0 也是同样真实的反证。
2. **下一阶段不应继续把主要精力投入报告微调。** 当前 Editorial candidate 可以保留作为设计基线，但它不是当前最大的系统风险。
3. **也不应直接扩张到通用 Agent 平台。** Governance、Economics 和第二消费者尚未成立；现在扩张只会放大职责和交付债务。
4. **可复用报告仍是最有价值的产品深化方向之一，但不能用旧 SQL/旧 Prompt 重放实现。** H6 必须建立在 M1A 的 Principal/Mandate 和 Forge compatibility decision 之上。

## 4. 用户确认后的下一阶段顺序

### P0：W3A 产品地图与高保真交互骨架

先定义工作台、新建任务、任务列表/详情、报告、数据资产和管理的信息架构，在隔离原型中覆盖完整桌面旅程和所有关键状态，发布 Atlas 供逐页人工门禁。原型允许明确标记的 fixture，不连接生产副作用。

### P1：W3B/W3C 生产 Product Shell

将用户通过的骨架接入现有 Pi/Forge/Report 真相源，打通可寻址的任务详情与报告库；再收口数据资产和管理入口。每个按钮必须真实可用或明确禁用，不建立第二套任务状态。

### P2：M1A 运行时治理

Product Shell 稳定后，只做 Service Identity、PrincipalContext、task-scoped DelegatedMandate、默认拒绝和 QueryRun lineage；不顺带做完整 RBAC、OAuth、Economics 或报告 Renderer。随后重跑 Web Human → Pi Service → Forge → SQL Approval → Report 的治理 Golden Journey。

### P3：H6 最小可复用报告切片

只实现 Definition/Criteria/Run Contract、手动“用最新数据更新”、两个时间点 fixture 和不可变 Run history。不做自动调度、不继承旧审批、不做免审批 SQL 重放。

### P4：再决定 M1B 与 Economics 的先后

若出现第二个真实团队/Workspace，优先 M1B Membership/Policy/多 Binding；若模型与人工审核成本已经成为交付阻力，先做 M2 最小 Usage Ledger。不得同时全面开工。

## 5. 近期明确不做

- 不继续把 H5 视觉候选接入生产，直到 period-delta Contract 与同版本兼容矩阵获批。
- 不做多引擎平台、通用 Agent Marketplace、复杂 BPMN 或单一全局 Memory Store。
- 不做自动定时报告和免逐次审批执行。
- 不以 Contract Coverage、测试数量或页面完成度替代 Runtime Governance、真实用户结果和付费验证。

## 6. 可证伪条件

本评估在以下情况发生时必须重算：

- M1A 真实 runtime 垂直切片通过跨租户、篡改、重放和默认拒绝门禁；
- 第二个真实团队或第二类非 SQL 消费者进入；
- H6 manual rerun 在真实数据上证明 Definition 可跨 Schema/时间安全复用；
- 出现客户愿意为治理、协同或可复用报告付费的证据；
- Golden Journey 出现新的 silent error、权限绕过或不可解释执行。
