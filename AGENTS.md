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

- 产品、当前有效需求及实施阶段以 `docs/current-project-state.md` 为准；不在本文件复制易变的需求编号或阶段进度。
- 唯一主动计划：`docs/forge-enterprise-evolution-plan.md`；没有证据和用户确认，不自行晋级或扩展 Agent Runtime、更多 Connector 及企业平台范围。
- 不承诺开放世界 100% 正确；通过语义、来源、权限、Evidence、确定性编译、审批和失败关闭减少静默错误。

## 稳定架构边界

- Pi 是唯一主 Orchestrator 和 Task 真相源。
- Forge 是可信数据执行层，并保留校验、拒绝和失败关闭能力。
- DATA Skills 是专业方法层，不持有任务主状态，不直接获得数据库执行权。
- Web、飞书、钉钉是渠道与投影层，不创建第二套业务真相源。
- 不新增双写任务状态、第二套主调度流程或绕过审批的执行路径。

## 工作流

日常开发、维护和文档工作默认只在 `main` 分支进行，不自行新建功能、评审或临时分支。只有用户明确要求时才采用其他分支；发现遗留非 `main` 检出时，先核对用户改动及分支关系，按明确授权安全迁回，不 reset、强推或删除未知工作。commit、push 与版本发布仍分别遵守下方授权规则。

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

<!-- omp-chatgpt-migration:begin -->
## 本地 App 上下文入口

- 在 ChatGPT App 的本地工作任务中使用本项目目录；默认中文回复。
- 涉及该文件描述的模块时按需读取 `CLAUDE.md`；工具专属命令需要适配，当前 AGENTS.md 优先。
- 当前代码、测试、项目既有规则和最新用户指令优先。迁移上下文不改变产品运行时的 Pi/OMP 依赖或职责，也不授权提交、推送、发布或生产操作。
- 历史 `local://`、`skill://` 或 OMP 工具名不保证在本会话可用；Skill 可直接读本地文件，其他能力先核对真实接口，不伪造调用。
- 涉及 OMP 历史计划或 `local://` 引用时读取 `.agents/omp-context/PLANS_INDEX.md`（仅维护者本机存在，含本机路径，不纳入版本控制）；保留的不同版本须结合当前正式计划核对，不按时间自动选主版本。
<!-- omp-chatgpt-migration:end -->
