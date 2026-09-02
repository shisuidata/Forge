# Forge 当前项目状态

> 状态：当前事实投影 · Last updated: 2026-09-03
>
> 本页是人和 Coding Agent 进入仓库后的第一入口。它只投影当前有效状态，不替代需求历史、主动计划或架构文档。

## 1. 当前产品定义

Forge 当前验证的是：

> **面向企业 Data Agent 的开源可信数据运行时：让既有 Agent 的数据访问可验证、可约束、可追溯。**

长期产品角色是企业可信数据平台；近期开发者入口收敛为 `Evaluate → Enforce → Explain`：先评测模型、Prompt、RAG、语义和 Agent 版本，再在运行时执行 Policy/Assurance/只读/审批门禁，最终返回带语义、来源、版本、限制和 lineage 的 Evidence。

自然语言问数、Chat、Product Shell 和 Forge JSON 都不是产品边界。Direct SQL、Forge JSON 与后续 Semantic Query 是可替换输入；Forge 的核心责任位于生成后的验证、可信执行、Evidence 和 Audit。Forge 不承诺开放世界 100% 正确。

## 2. 稳定职责边界

- **Pi**：唯一主 Orchestrator 和 Task 真相源。
- **Forge**：可信数据执行层；保留校验、拒绝和失败关闭能力。
- **DATA Skills**：专业方法层，不持有任务主状态，不直接获得数据库执行权。
- **Web / 飞书 / 钉钉**：渠道与投影层，不创建第二套业务真相源。
- **人工责任**：高风险动作、语义确认和生产权限变更不能被 UI 或 Agent 隐式越权。

## 3. 当前需求与计划

- 当前有效产品需求：`REQ-2026-09-03-025`；`REQ-2026-08-25-023` 已吸收为历史短期切口，`REQ-2026-08-26-024` Benchmark 工作包已验证完成。
- 唯一主动计划：`forge-enterprise-evolution-plan.md`。
- 当前产品主线：**R0 Open-source Trust Runtime Product Cut / Adoption Baseline**。
- 当前已完成工程证据：Accuracy Lab 支持持久 snapshot、SSE、重启恢复、Pi-native 双 AgentSession、共享 ContextSnapshot、Official EA 与 ResultContract 双评价。完整 Run `pbr_1f735d433a284366bfe6526146511792` 完成 500/500 cases、1000/1000 calls：Forge EA 45.40%、Direct SQL 56.40%，Forge Delta -11.00pp。
- 当前切入要求：公开入口必须让外部开发者沿单一 Golden Path 完成“现有 Agent/样例输出 → Evaluate → 失败定位 → Policy Gate → Evidence”；Direct SQL 不得被迫先转换为 Forge JSON。
- 当前不扩张通用 Product Shell、SaaS Connector、非 SQL Action、Economics/Outcome Ledger 或完整企业权限平台；真实客户数据、生产凭证和高风险数据源仍需单独授权。

## 4. 已完成且可复用的工程基础

- M0 Contract 评审与 Product Projection Contract。
- Pi Task/ExecutionPlan/Artifact/StageAttempt 运行骨架。
- Forge Registry、Assurance、Compiler、Executor、QueryRun 与审批哈希。
- SP0–SP5 Product Spine 和完整 Product Shell 基础。
- Web、报告投影、Registry Studio、受控 Skills 与多渠道 Presentation 基础。
- Accuracy Benchmark Runtime 与 `/admin/benchmark`：持久 run/case/call 真相源、SSE 实时只读投影、部分/最终成绩区分和有界准确性声明。

这些完成项是后续验证的基础，不等于目标市场、产品体验或企业平台假设已经验证。

## 5. 未关闭的验收与采用事实

- 尚无外部开发者独立完成新 Trust Runtime Golden Path 的证据，也没有外部 Adapter、Rule、Dataset 或真实 failure case 贡献。
- Direct SQL 尚未作为与 Forge JSON 平级的公开输入完成产品化切割。
- W2 主体内容规则、Product Spine 与完整 Product Shell 的 Atlas candidate 仍有历史人工复验项，但不再主导当前产品路线。
- Runtime Governance Coverage 仍为 0%；Contract Coverage 不能替代生产执行覆盖。

这些事实必须保留为反证；不得用页面数量、内部测试、自有题集或 stars/forks 代替真实外部运行与采用证据。

## 6. 文档权威顺序

遇到冲突时，按以下顺序处理：

1. 用户当前明确决定。
2. 本页的当前状态投影。
3. [`requirements-pool.md`](requirements-pool.md) 中最新且已接受的需求与决策。
4. [`forge-enterprise-evolution-plan.md`](forge-enterprise-evolution-plan.md) 的主动阶段和门禁。
5. [`product-north-star.md`](product-north-star.md) 与 [`platform-architecture.md`](platform-architecture.md) 的稳定产品/架构边界。
6. 历史计划、评审、证据和 Devlog；仅作溯源，不恢复为当前任务。

状态发生实质变化时，同步更新本页、对应 Requirement 和主动计划；不要把执行进度写进 AGENTS.md。

## 7. OMP 继续开发入口

进入仓库后：

1. 先读本页；只按任务需要读取相关源码、测试和文档章节。
2. 先查看工作区状态，保留用户已有未提交修改；不要 reset、覆盖或清理未知工作。
3. 普通 Bug 和已确认行为的修复可直接定位、测试、修复；新产品/体验/架构需求先进入需求池。
4. 修改产品职责或当前阶段前，先更新主动计划；修改稳定职责边界时再更新架构。
5. 运行覆盖变更行为的最小测试；跨 Python/Pi Contract 时同时验证两侧。
6. 未经用户明确要求，不 commit、push、部署、处理生产凭证或接入真实客户数据。

常用命令：

```bash
# Python
.venv/bin/python -m pytest tests -q

# Pi Orchestrator
npm --prefix services/pi-orchestrator run typecheck
npm --prefix services/pi-orchestrator test

# 本地 Web
uvicorn main:app --host 0.0.0.0 --port 8000
```
