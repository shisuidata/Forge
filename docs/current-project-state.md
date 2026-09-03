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
- 当前产品主线：**R0 Open-source Trust Runtime Product Cut / Adoption Baseline**；R0.1–R0.5 已完成，当前实施切片为 **R0.6 External Adoption Evidence**。
- R0.1 Unified Input Contract 已完成：`query-candidate-v1` 将 Direct SQL 与 Forge JSON 建模为互斥候选；两者进入同一 QueryRun 审批/执行链并绑定 `input_kind`、candidate/assurance/policy/registry revision 与 SQL hash。Direct SQL 在服务端执行只读、语法、Registry/ACL 和字段校验，不经 Forge JSON 转换。
- R0.2 Evaluate 已完成：版本化 `POST /api/v1/evaluate` 与 `forge evaluate` 统一 Direct SQL/Forge JSON 的 Policy、Failure Taxonomy、Exact Result、lineage 与响应内 Evidence refs；`evaluation-suite-v1` 和 `evaluation-run-manifest-v1` 持久化完整输入修订、原始 outcomes 与可复算聚合，并提供不可比失败关闭和跨 producer 版本 Regression release gate。所有 Evaluate 路径均不执行 SQL、不授予执行权。
- R0.3 Enforce 已完成：版本化 `POST /api/v1/enforce/query-runs`、`GET /api/v1/enforce/query-runs/{query_run_id}`、`POST /api/v1/enforce/query-runs/{query_run_id}/approve` 与 `forge enforce` 将 Principal、Purpose、Task、可选 DelegatedMandate、Resource Scope、Policy、Assurance、Registry 和只读凭证绑定进 QueryRun；回读绑定创建凭证，只有独立 reviewer credential 提交匹配 SQL/Assurance/Enforcement hashes 后才能执行。上下文、Policy、Registry、权限或只读条件漂移均失败关闭。
- R0.4 Explain 已完成：版本化 `GET /api/v1/explain/query-runs/{query_run_id}`、`forge explain` 与 `explain-query-response-v1` 从同一 QueryRun 投影结果、实际 SQL、Registry 语义/来源、Principal/Policy/Approval、Assurance、Evidence、lineage、版本和显式限制。来源上下文、审批与结果在写入时 hash-bound；篡改失败关闭，历史未锚定 QueryRun 只返回 `integrity=partial`，不伪造证据。
- R0.5 Public Golden Path 已完成：`forge quickstart` 使用隔离 SQLite 与真实本地服务串联公开 Evaluate → Enforce → Explain API，无需 API Key、LLM、Embedding、Pi、Forge JSON、已有数据库或 `.env`；默认展示实际 SQL 并等待批准，`--serve` 保持 Dashboard 可浏览，`--yes --json` 提供合成数据 CI 证明。Dashboard 只读投影同一 QueryRun 的执行状态与 Evidence integrity，不复制真相源。
- 当前已完成工程证据：Accuracy Lab 完整 Run `pbr_1f735d433a284366bfe6526146511792` 完成 500/500 cases、1000/1000 calls：Forge EA 45.40%、Direct SQL 56.40%，Forge Delta -11.00pp。R0.6 采用入口聚焦回归 `14 passed`，Python 全套 `676 passed / 28 skipped`，Pi `118 passed`，TypeScript typecheck 与 Python compileall 通过；实际人工批准与 `--yes --json` 两条 `forge quickstart` 均完成失败关闭、Evaluate、Enforce、Explain 与 Dashboard 链路。
- R0.6 证据采集准备已完成：Quickstart 先用写 SQL 证明 `assurance/readonly_violation` 失败关闭，再运行已审核只读查询；`summary.json` 生成不含 hostname、username、路径、SQL rows、凭证或私有 schema 的 `run_receipt` 与 SHA-256 漂移校验。公开 Quickstart adoption Issue 表单收集 tested revision、fresh-clone setup time、首个失败/困惑点、回执及开发者对 Policy、Evidence 和限制的独立解释；Forge 不发送 telemetry，checksum 不证明身份。
- 当前切入要求：R0.6 必须取得外部开发者独立完成 Golden Path 或提交 Adapter、Rule、Dataset、真实 failure case 的采用证据，并记录 setup time、失败点与修复闭环；内部 smoke、页面数量、自有题集和测试通过不能替代外部证据。可测试公开 revision 已发布为 `0b4fd36b7175c09dc3375d839c5aba888aacb900`；下一步是由未参与实现的外部开发者 fresh clone 试跑并提交公开回执。
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

- 公开 GitHub 信号盘点中，现有 8 个 Issue 与 1 个 Pull Request 均由维护者身份提交；11 stars 与 1 fork 仅是传播信号。尚无可确认的外部开发者 Golden Path 回执，也没有外部 Adapter、Rule、Dataset 或真实 failure case 贡献。
- R0.6 的失败关闭样例、隐私有界回执与公开提交表单已在 revision `0b4fd36b7175c09dc3375d839c5aba888aacb900` 发布并通过内部 smoke，但仍没有外部独立完成记录；R0.6 外部采用门禁未通过。
- W2 主体内容规则、Product Spine 与完整 Product Shell 的 Atlas candidate 仍有历史人工复验项，但不再主导当前产品路线。
- Governance Action Catalog 的 14 个 supported Action 中仅 `query.prepare`、`query.approve`、`query.execute` 已完成 v1 Runtime Enforcement，覆盖率为 3/14（21.4%）；Explain 是只读证据投影，不新增 Action Runtime Enforcement，Contract Coverage 不能替代其余运行时执行覆盖。

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
