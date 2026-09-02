# Forge 项目协作规则

默认使用中文沟通；代码、API、命令和 symbol 保持原语言。

Forge 是 Agent-native 的可信数据执行层。进入仓库后，先读 [`docs/current-project-state.md`](docs/current-project-state.md)；它是当前产品、阶段、门禁和未关闭验收项的简明投影。

## 上下文读取

不要默认扫描全部历史文档。按以下顺序读取：

1. `docs/current-project-state.md`
2. 与任务直接相关的源码、测试和文档章节
3. 产品方向问题：`docs/product-north-star.md`
4. 当前阶段或实施门禁：`docs/forge-enterprise-evolution-plan.md`
5. 职责边界：`docs/platform-architecture.md`
6. 新需求及历史决策：`docs/requirements-pool.md` 中相关 Requirement

文档分类和历史材料见 `docs/README.md`。历史计划、评审、证据和 Devlog 只用于溯源，不能自动恢复为当前待办。

## 当前产品与阶段

- 当前产品切口：面向已有数据库或数仓的小型数据团队的可信业务问数助手，在真实提问中逐步沉淀并安全复用业务语义。
- 当前有效需求：`REQ-2026-08-25-023`。
- 唯一主动计划：`docs/forge-enterprise-evolution-plan.md`。
- 当前只推进 **S0 Design Partner / Problem Baseline**；没有证据和用户确认，不实施 S1–S3、M1A、Agent Runtime、更多 Connector 或企业平台扩张。
- 不承诺开放世界 100% 正确；通过语义、来源、权限、Evidence、确定性编译、审批和失败关闭减少静默错误。

## 稳定架构边界

- Pi 是唯一主 Orchestrator 和 Task 真相源。
- Forge 是可信数据执行层，并保留校验、拒绝和失败关闭能力。
- DATA Skills 是专业方法层，不持有任务主状态，不直接获得数据库执行权。
- Web、飞书、钉钉是渠道与投影层，不创建第二套业务真相源。
- 不新增双写任务状态、第二套主调度流程或绕过审批的执行路径。

## 工作流

1. 开始前查看工作区状态，保留用户已有未提交修改；不要 reset、覆盖或清理未知工作。
2. 普通 Bug、测试修复和已确认行为的维护可直接定位、修复并验证。
3. 新产品、体验、架构或业务需求先追加到 `docs/requirements-pool.md`，保留原始表达，并完成价值、边界、风险、替代方案和机会成本评估。
4. 只有用户明确接受的需求才能进入主动计划；涉及职责迁移时再更新架构。
5. 修改前复用现有模块、Contract 和测试模式；不得建立旁路真相源或第二套约定。
6. 修改后运行覆盖实际行为的最小验证；跨 Python/Pi Contract 时同时验证两侧。
7. 回写当前状态、主动计划和 Requirement 的必要变化；不要把易变进度、测试数量或候选地址写进本文件。

## 工程入口

- Python API 与产品层：`main.py`、`web/`
- Forge Runtime：`forge/`
- Agent 与 Contract：`agent/`
- Registry：`registry/`
- Pi Orchestrator：`services/pi-orchestrator/`
- Python tests：`tests/`
- 产品与架构事实：`docs/`

常用验证命令：

```bash
.venv/bin/python -m pytest tests -q
npm --prefix services/pi-orchestrator run typecheck
npm --prefix services/pi-orchestrator test
```

按变更范围先跑最小测试，再决定是否扩大；不要用硬编码测试数量判断完成。

## 安全与版本管理

- 不读取、记录或提交 `.env`、凭证、客户数据和生产 Secret。
- 不主动修改生产配置、签名、外部服务或真实数据源。
- 未经用户明确要求，不 commit、push、tag、发布或部署。
- 高风险副作用不自动重放；真实客户数据、生产凭证和权限变更必须单独获得用户授权。
