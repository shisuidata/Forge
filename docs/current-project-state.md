# Forge 当前项目状态

> 状态：当前事实投影 · Last updated: 2026-10-22
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
- `REQ-2026-09-23-070` 有界 Agent 治理执行切片已在隔离合成环境完成工程验收；Agent 是独立凭证主体，管理员签发并可撤销单任务/单数据源/总表范围及自主子集授权，非自主表只由指定真人审核。Pi 仍拥有 Task，Forge 同库线性化 Mandate 与 QueryRun v2 claim；Web/渠道只作受信入口与投影。历史 v1 QueryRun 保留只读审计，公开 v1 执行及原始 SQL 入口已关闭（410），旧凭证与未验证历史身份不自动升级。此工程验收不改变 R0.6 外部采用与 H 独立审核门禁。
- `REQ-2026-09-23-071` 产品工作台实用型视觉优化已在本地完成：产品 Shell 改为无衬线/冷中性界面，待办与阻断前置，SQL 审核和授权先于计划；隔离合成投影的桌面/手机浏览器目视检查与产品页/BFF 聚焦测试通过。未部署 NAS、未提交真实审批，也不改变 R0.6 外部采用门禁。
- `REQ-2026-10-16-072` 官网重设计已在本地完成：独立 Astro 首页、暖白/墨黑/锻造橙品牌与 Starlight 文档主题，当前介绍/Quickstart/Benchmark 入口、合成示意和显式验证边界；28页构建及真实产物桌面/手机、交互/搜索/链接/自动无障碍检查通过。未提交、部署、重启 NAS 或关闭 Runtime P0/R0.6/H；详细验证见 `website/README.md`。
- `REQ-2026-10-16-073` 已完成确认的生成式蚊子品牌接入、全部 26 篇官网文档审核及 Cloudflare 静态发布：`https://forge.ss-data.cc/`，独立 Worker `forge-website`。HTTPS、134 项线上资产与本地构建一致、桌面/手机、搜索和导航已验证；只发布官网，不部署 Runtime、不重启 NAS、不提交/推送 Git，P0/R0.6/H 不变。版本、hash 和回退边界见 `website/README.md`。
- 2026-10-22 已修复 2026-10-04 评估的全部 P0/P1 发布阻断，并修复同期发现的控制台 AI 面板注入与审批/执行租约到期滞留（零模型调用）。逐项修复、提交与验证见[2026-10-22 公开测试摘要](archive/engineering/latest-test-report-2026-10-22.md)：受测提交 `e50b0a0` 远端 CI 通过，导出目录与 wheel/sdist 全新安装验证通过。升级影响：Assurance 升 `query-assurance-v13`，旧版本 QueryRun 需重新 prepare/审批；Pi 状态库迁移至 schema v5，旧版 Pi 无法再打开。2026-10-25 补齐 Prepare 中断遗留：Forge 新增仅创建者可用的 `GET /api/v2/enforce/query-runs?create_idempotency_key=`，Pi 重试 Prepare 前按被中断尝试的幂等键找回其 QueryRun，仍待审/待执行即取消，并记 `query.uncertain_prepare_settled`；不改变同一 Task 可有多条 QueryRun 的语义。仍开放：只读校验的副作用函数是拒绝清单，数据库只读角色仍是最终边界。不改变 R0.6/H。
- 唯一主动计划：`forge-enterprise-evolution-plan.md`。
- 当前产品主线：**R0 Open-source Trust Runtime Product Cut / Adoption Baseline**；R0.1–R0.5 已完成，当前实施切片为 **R0.6 External Adoption Evidence**。
- R0.1 Unified Input Contract 已完成：`query-candidate-v1` 将 Direct SQL 与 Forge JSON 建模为互斥候选；两者进入同一 QueryRun 审批/执行链并绑定 `input_kind`、candidate/assurance/policy/registry revision 与 SQL hash。Direct SQL 在服务端执行只读、语法、Registry/ACL 和字段校验，不经 Forge JSON 转换。
- R0.2 Evaluate 已完成：版本化 `POST /api/v1/evaluate` 与 `forge evaluate` 统一 Direct SQL/Forge JSON 的 Policy、Failure Taxonomy、Exact Result、lineage 与响应内 Evidence refs；`evaluation-suite-v1` 和 `evaluation-run-manifest-v1` 持久化完整输入修订、原始 outcomes 与可复算聚合，并提供不可比失败关闭和跨 producer 版本 Regression release gate。所有 Evaluate 路径均不执行 SQL、不授予执行权。
- R0.3 Enforce 已完成：版本化 `POST /api/v1/enforce/query-runs`、`GET /api/v1/enforce/query-runs/{query_run_id}`、`POST /api/v1/enforce/query-runs/{query_run_id}/approve` 与 `forge enforce` 将 Principal、Purpose、Task、可选 DelegatedMandate、Resource Scope、Policy、Assurance、Registry 和只读凭证绑定进 QueryRun；回读绑定创建凭证，只有独立 reviewer credential 提交匹配 SQL/Assurance/Enforcement hashes 后才能执行。上下文、Policy、Registry、权限或只读条件漂移均失败关闭。
- R0.4 Explain 已完成：版本化 `GET /api/v1/explain/query-runs/{query_run_id}`、`forge explain` 与 `explain-query-response-v1` 从同一 QueryRun 投影结果、实际 SQL、Registry 语义/来源、Principal/Policy/Approval、Assurance、Evidence、lineage、版本和显式限制。来源上下文、审批与结果在写入时 hash-bound；篡改失败关闭，历史未锚定 QueryRun 只返回 `integrity=partial`，不伪造证据。
- R0.5 Public Golden Path 已完成：`forge quickstart` 使用隔离 SQLite 与真实本地服务串联公开 Evaluate → Enforce → Explain API，无需 API Key、LLM、Embedding、Pi、Forge JSON、已有数据库或 `.env`；默认展示实际 SQL 并等待批准，`--serve` 保持 Dashboard 可浏览，`--yes --json` 提供合成数据 CI 证明。Dashboard 只读投影同一 QueryRun 的执行状态与 Evidence integrity，不复制真相源。
- 当前执行接入以 `/api/v2/governance/*`、`/api/v2/enforce/query-runs`、`/api/v2/explain/query-runs/{id}` 和 `forge quickstart [--agent]` 为准；上文 R0.3–R0.5 的 v1 路径与免凭证描述仅为历史阶段记录。隔离真实 HTTP Forge↔Pi 自主/指定审核人两链、撤销拒绝与浏览器工作台已验证；Python 1011 passed / 26 skipped、Pi 212 passed/typecheck、非 editable wheel Quickstart 通过。NAS 初次曾以合成 SQLite 独立预览，浏览器两条任务完成；随后按下一条用户决策收敛为唯一运行实例。
- **NAS 单服务原则（2026-09-23）**：用户明确要求 NAS 永远只运行最新 Forge/Pi，一套服务，无旧版与新版并行部署。已停用、禁用并删除旧 M4.1 与 product-spine 两组 user-systemd 服务定义及其运行程序；当前仅 forge-api.service 与 forge-pi.service 启用并运行，入口为内网 18009（Pi 18010）。当前 wheel SHA256 7f5abf7dd40babc96d98bb3b2ebe11ee43d287dba85ecf2295cb559f1a19e596，Pi 包 SHA256 3835dc2974b2eb63d5b3cb33532e33ce69971fa35ea169371da0a30002e9e001；新版仍仅合成 numbers/secrets、合成身份，不具备旧数据/模型能力，readiness 因无模型和内网 HTTP cookie 检查为 fail。旧状态库/敏感配置/备份与源码内数据独立保留、不可由新服务读取；旧代码和 venv 已删除。真实身份/数据迁移未获单独授权，不能将此切换当作生产门禁通过。
- **NAS 更新（2026-10-25）**：按用户要求更新为 `a2955d4`（wheel SHA256 f148a863…5ff5，Pi 源码包 9121a9cb…2c2f；Pi 依赖锁文件未变，未重装 node_modules）。更新前已在 `~/services/forge-m4.1/backups/agent-governed-pre-a2955d4-20261025T040041Z/` 保存 state、旧 dist、旧 Pi 源码与 pip 清单；NAS 自带 Python 3.12 上普通与 `--agent` Quickstart 通过，启动后 Forge health ok、Pi live 200、Pi 状态库由 schema v4 迁移至 v5（不可降级），登录页无 CDN 引用。Pi readiness 仍 fail：环境配置指向的 `拾穗 DATA/skills` 路径已不存在，属既有配置问题，未修改 NAS 环境文件。用户要求服务不常驻：验证后已停止并关闭 `forge-api`/`forge-pi` 开机自启，需要时手动 `systemctl --user start forge-api forge-pi`。上条“当前仅 forge-api.service 与 forge-pi.service 启用并运行”已由本条取代。
- 准确率现状（逐轮记录见[状态历史](archive/engineering/project-state-history-2026-10-22.md)）：共三轮有效 500 题、1000-call 双臂运行。最新 Structured GPT-5.6 候选经最终 Compiler 重评为 Official EA Forge 313/500 vs Direct SQL 314/500（Forge only 22、Direct only 23，exact p=1.0），只支持统计持平，不支持 Forge 准确率优势；该轮 Forge 生成多用 59.57% tokens。此后 Luna 小样本诊断与开发对照均未达到预注册门槛或不构成总体收益结论，日期、粒度、取值、分母、CTE 接口与关键 DSL 说明等可选上下文全部默认 off，滚动父 Run 仍为 seed48；DeepSeek 完整复验因额度失败关闭，不计成绩；H 独立审核未完成。
- R0.6 证据采集准备已完成：Quickstart 先用写 SQL 证明 `assurance/readonly_violation` 失败关闭，再运行已审核只读查询；`summary.json` 生成不含 hostname、username、路径、SQL rows、凭证或私有 schema 的 `run_receipt` 与 SHA-256 漂移校验。公开 Quickstart adoption Issue 表单收集 tested revision、fresh-clone setup time、首个失败/困惑点、回执及开发者对 Policy、Evidence 和限制的独立解释；Forge 不发送 telemetry，checksum 不证明身份。
- 当前切入要求：R0.6 必须取得外部开发者独立完成 Golden Path 或提交 Adapter、Rule、Dataset、真实 failure case 的采用证据，并记录 setup time、失败点与修复闭环；内部 smoke、页面数量、自有题集和测试通过不能替代外部证据。可测试公开 revision 已发布，独立试跑招募见 [GitHub #9](https://github.com/shisuidata/Forge/issues/9)；下一步等待未参与实现的外部开发者 fresh clone 试跑并提交公开回执。
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

- 2026-09-08按GitHub API记录类型分离：8个Issue（不含PR）、2个PR；#10为本会话创建的候选交付，不计独立外部贡献。此前“9个Issue与1个PR”投影已纠正，原始记录不追改。stars/forks仅是传播信号；尚无可确认的外部Golden Path回执或外部Adapter、Rule、Dataset、真实failure case贡献。
- R0.6 的失败关闭样例、隐私有界回执与公开提交表单已发布；[Issue #9](https://github.com/shisuidata/Forge/issues/9) 是维护者创建的外部试跑招募入口，不是采用证据。当前仍没有外部独立完成记录，R0.6 外部采用门禁未通过。
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

2026-09-09 起本项目退出 Maestri 开发工作流（REQ-068），直接在 `main` 工作，不加载或恢复旧角色任务；退出核对见[状态历史](archive/engineering/project-state-history-2026-10-22.md#maestri-退出核对原第-7-节)。

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
