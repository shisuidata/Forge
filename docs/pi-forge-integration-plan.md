# Pi × Forge × 拾穗 DATA 集成计划

> 状态：实施中 · Last updated: 2026-08-21
>
> 本文是本次平台改造的**计划真相源**。总体架构见 [`platform-architecture.md`](platform-architecture.md)，现有外部 Agent 安全边界见 [`agent-integration.md`](agent-integration.md)。

## 0. 计划治理

### 0.1 强制更新规则

本次改造期间，需求沟通与代码实施遵循以下顺序：

```text
用户确认需求或改变方向
→ 对照平台架构和当前计划
→ 先更新计划文档中的边界、阶段、门禁或风险
→ 再开始代码修改
→ 完成验证
→ 回写完成状态、验证结果、遗留风险和下一步
```

强制要求：

1. 新需求影响职责边界时，同时更新 `platform-architecture.md` 和本文。
2. 新需求只影响实施顺序或验收标准时，至少更新本文。
3. 代码实现不得先于相关计划更新；紧急故障修复除外，但修复后必须立即补记。
4. 每次完成一个可交付步骤，都要更新“当前执行状态”，不能只在对话中宣告完成。
5. 新旧要求冲突时，以用户最近确认的要求为准，同时记录被替代的决策，避免旧方案重新进入主路径。
6. 发现代码与计划不一致时先暂停扩展，明确以代码修计划还是以计划改代码。
7. 所有阶段都必须写清楚职责归属、验收门禁、兼容路径和退出条件。

### 0.2 已确认的需求基线

- Pi 是任务底座和唯一主 Orchestrator。
- Forge 是唯一可信数据执行层，保留安全否决权。
- 拾穗 DATA Skills 提供专业方法、分析和输出能力。
- Web、飞书、钉钉是轻量渠道，不实现业务流程。
- 高风险动作由用户批准。
- Forge 旧 Pipeline 只允许作为迁移期兼容路径，不能与 Pi 长期双写任务状态。
- 所有工作必须职责唯一、边界清楚，并通过 Artifact Contract 连接。

### 0.3 当前执行状态

| 工作项 | 状态 | 验证/说明 |
|---|---|---|
| 平台四层架构与职责迁移矩阵 | 已完成，待提交 | `platform-architecture.md` |
| TaskRun 与四类核心 Artifact Schema | 已完成，待提交 | `tests/test_artifact_contracts.py`，10 个契约测试 + 文档测试共 11 passed |
| 拾穗 DATA Pi Package | 已完成，待提交 | 23 Skills，Pi discovery 0 diagnostics，npm 门禁通过 |
| 首批四个 Skill 的 Runtime 白名单 | 已完成，待提交 | 精确加载 4 Skills；全局 Skills、Extensions、Context 均禁用 |
| 受限 Pi Orchestrator Service 骨架 | 已完成，待提交 | Runtime、Task/Event Store、Task API、能力清单、健康检查；TypeScript typecheck + 16 tests passed |
| `forge_prepare_query` Tool | 已完成，待提交 | 只接现有 `/api/prepare-query`；用户身份由 TaskContext 注入；拒绝任何可执行标记或结果集；Forge 端身份绑定留到 Phase 2 |
| Phase 1 Integration Spike | 已完成，待提交 | Web → Pi TaskRun/Event → Forge prepare-query → 不可执行 SQL 预览已跑通 |
| Web Task Event、审批与结果展示 | 已完成，待提交 | Web 仅代理 Pi Task API；审核时不可编辑 SQL；批准后展示 8 步事件与有界结果；管理员身份由服务端固定映射；Playwright 三服务 E2E 通过 |
| Pi 模型与四个 Skill 的真实任务执行 | 四个 MVP Skills 已接入 | Coding Plan 真实执行四个 Skills 均通过；分析只接受实际 QueryRun row evidence，报告 finding 必须逐字来自 AnalysisArtifact，Markdown 服务端确定性渲染 |
| Phase 3 分析与报告闭环 | 已完成，待提交 | 唯一一次补查闭环完成：用户选择 suggested_query → 幂等 child TaskRun → Forge 审批执行 → 父任务合并两个 QueryResult 重新分析；Coding Plan 多 QueryResult 冒烟和 Web 三服务 E2E 通过；34 TS tests、Forge full suite 383 passed |
| Phase 3.5 Pi 状态持久化 | 已完成，待提交 | Node SQLite + WAL 持久化 Task/Event/Artifact；生产 Server 默认持久化，内存 Store 仅测试；跨 Store、Application 和 Server 重启恢复通过；40 TS tests、Forge full suite 383 passed |
| Phase 3.6 Stage Attempt 与异步恢复 | 已完成，待提交 | 全部长耗时 Stage 已绑定 Attempt/Lease/timeout；可选 `async: true` 返回 202，Web 已使用 Task/Event/Artifact/Attempt polling；过期 lease 对账和异步三服务 E2E 通过；46 TS tests、Forge full suite 383 passed |
| Phase 4 飞书与钉钉渠道 | 进行中：飞书 gated path | ChannelEvent、独立服务鉴权、只读身份映射、SQLite 幂等入站、ChannelPresentation Renderer 已完成；`FEISHU_PI_ENABLED` 新路径已覆盖消息 → SQL 审核 → hash 绑定批准 → 结果，默认关闭；待真实飞书凭证 smoke、补充信息/取消/补查和钉钉复用 |
| Phase 4.4 Model Control Plane | 基础热加载已实现，版本控制待建设 | Forge 查询模型使用不可变 `ModelConfigSnapshot` + mtime/revision cache；Web 保存后新任务无需重启；缺失/错误配置失败关闭。待建设 Profile Store、真实验证、CAS 激活、回滚及 Pi Stage 绑定 |
| Phase 4.5 Registry Studio | 需求已确认，待设计实施 | 结构层以增强 Canonical Schema 为真相源，同时提供表格、DDL、ER 图和 JSON 投影视图；编辑先形成版本化草案和差异审核，绝不从 UI 直接执行数据库 DDL |
| Phase 2.5 前置 Skill 结构化执行 | 已完成，待提交 | 火山方舟 Coding Plan `ark-code-latest` readiness=`ready`；真实澄清生成 `ClarificationArtifact/needs_input`，真实指标审查生成 `MetricDefinitionArtifact/needs_confirmation`；Key 仅从既有 `ARK_API_KEY` 环境变量注入，未回显或复制 |
| Phase 2 QueryRun 审批执行闭环 | 已完成，待提交 | Forge 持久化 QueryRun；独立 Pi 服务认证；hash/身份/Registry/过期/只读/幂等门禁；Web 审批与结果展示 E2E 通过；Forge full suite 380 passed |
| Forge 内部 QueryRun 审批 API | 已完成，待提交 | create/get/approve/cancel/result；外部 `/api/prepare-query` 语义未改变 |
| Forge 旧 Pipeline 退出新主路径 | 已完成，兼容入口待退役 | 新 Web/Pi 链路不调用 `agent/pipeline.py`；旧 `/api/chat` 等仅作 feature-flag 回滚兼容，Phase 4 渠道迁移完成后进入废弃期 |

### 0.4 决策记录

| 日期 | 决策 | 影响 |
|---|---|---|
| 2026-08-21 | 采用 Pi + Forge + 拾穗 DATA Skills + 多渠道四层架构 | Forge 从单体 Agent 演进为可信执行层 |
| 2026-08-21 | 结构层支持表格、DDL、ER 图和 JSON 多视图 | Canonical Schema 仍是唯一真相源；各视图不得各自保存状态，编辑只修改 Registry 草案，不直接执行数据库 DDL |
| 2026-08-21 | 模型切换不得依赖服务重启 | 模型配置改为版本化 ModelProfile + ActiveBinding；切换前验证，切换后新任务生效，在途任务固定旧 revision，可审计回滚 |
| 2026-08-21 | Pi 拥有流程调度权，Forge 拥有可信执行权与否决权 | 禁止形成双 Orchestrator |
| 2026-08-21 | 计划文档随需求确认和实施结果持续更新 | 文档更新成为开发门禁，而非事后总结 |
| 2026-08-21 | Pi Runtime 默认关闭内置工具，仅显式加载四个 MVP Skills | 客户运行环境不继承个人 Pi 配置，也不具备文件或 Shell 权限 |
| 2026-08-21 | 首个 Forge Tool 只包装现有 `/api/prepare-query` | Pi 可取得待审核 SQL，但不能批准、执行或接收查询结果 |
| 2026-08-21 | Integration Spike 的 Web 只消费 Pi Task/Event API | 渠道不再直接选择 Forge Pipeline；Phase 1 页面不提供执行按钮 |
| 2026-08-21 | QueryRun 审批使用独立 Pi 服务密钥和 hash 绑定 | Pi 调度审批，Forge 独立验证并执行；外部 prepare-query 权限不变 |
| 2026-08-21 | Web 只允许批准原 SQL，不提供编辑后直执行 | 审核内容与实际执行内容保持一致 |
| 前置 Skill 使用阶段固定的 Structured Output Tool | Skill 全文进入隔离 Session 的 system prompt；模型只能提交对应 Artifact，服务端生成身份与时间元数据并校验，不依赖自由文本 JSON |
| 模型 Runtime 必须显式配置 Provider/Model 并使用服务专用 agentDir | 缺失或不可用时 readiness 降级；禁止静默继承个人 `~/.pi/agent` |
| Pi 专用模型采用火山方舟 Coding Plan `ark-code-latest` | OpenAI-compatible `/api/coding/v3`；模型模板只引用环境变量 `$ARK_API_KEY`，不把 Key 写入仓库或 Artifact |
| 报告 Markdown 不再由模型自由生成 | 模型提交结构化报告与 `SERVER_RENDERED` 哨兵；服务端只从证据绑定字段确定性渲染 Markdown，防止正文偷偷新增结论 |
| 报告 finding 不允许从 hypothesis 晋升 | `key_findings.statement` 必须逐字匹配 AnalysisArtifact finding，证据引用必须属于同一 AnalysisArtifact 和 QueryRun |
| 分析补查最多一次且必须来自模型建议 | 用户只能选择最新 incomplete AnalysisArtifact 的 suggested_query；child TaskRun 绑定父任务并完整经过 SQL 审核，创建和恢复均使用幂等键 |
| Pi 正式状态使用单一 SQLite 真相源 | Task、Event、Artifact 共用一个数据库文件；WAL + busy timeout；Task transition 使用 status CAS，Event sequence 在事务内生成；内存 Store 仅用于测试和显式注入 |
| Pi Orchestrator 运行时最低 Node 22.19 | 状态层复用 `node:sqlite`，避免新增 native 第三方依赖；锁定 Node 版本并在升级前运行持久化与重启测试 |
| 长耗时 Stage 使用持久化 Attempt/Lease | 每次模型或 Forge 调用先创建唯一 Attempt，记录 retry status 与 lease；成功/失败终止 Attempt，进程崩溃后仅回收过期 lease，不盲目重放副作用 |

## 1. 集成目标

第一阶段要验证的不是“Pi 能不能调用一个 HTTP API”，而是下面这条完整价值链能否稳定成立：

```text
模糊业务问题
→ 需求澄清
→ 指标口径审查
→ Forge 生成待审核 SQL
→ 用户批准
→ Forge 只读执行
→ 业务归因分析
→ 分析报告
```

成功后，Forge 将从可信问数 Agent 扩展为可信 AI 数据任务平台：

- Pi 是任务底座和编排 Runtime。
- Forge 是唯一可信执行层。
- 拾穗 DATA Skills 是专业方法和交付层。
- Web、飞书、钉钉是可替换的渠道层。

## 2. 当前基础与差距

### 2.1 可直接复用

Forge：

- `/api/prepare-query` 可以安全地产生待审核 SQL。
- `/api/chat`、`/api/approve` 已有内部审核执行链路。
- Registry、Retriever、Forge JSON、Lint、Compiler 和 Executor 已完成主链。
- Pipeline、Artifact、Memory、Audit 和 Feedback 已有代码基础。

拾穗 DATA：

- 23 个正式 Skill。
- 每个 Skill 有使用边界、输入、流程、输出格式和质量标准。
- 已有结构测试、用例测试、fixture 校验和人工评测规范。

Pi：

- 支持 Agent Skills 标准和渐进加载。
- 支持 Extensions、自定义工具、SDK、RPC、事件流和 Session。
- SDK 可以禁用内置工具，只暴露 Forge 受控工具。

### 2.2 当前差距

1. Forge 的 `agent/pipeline.py` 仍硬编码分析、可视化和报告 prompt，尚未以拾穗 DATA Skills 为能力源。
2. TaskRun 与首批 Artifact Schema 已定义，但当前分析 Stage 仍依赖自由文本和 JSON 提取，尚未接入这些契约。
3. `/api/prepare-query` 按设计不能进入 `/api/approve` 队列，不能直接支持外部 Pi 的完整审批执行闭环。
4. Forge 当前内部 `/api/approve` 依赖用户 pending state，不适合作为稳定的跨服务 QueryRun API。
5. 拾穗 DATA 已具备 Pi Package manifest 和本地发现配置，但生产部署尚未从本地路径升级为固定 Git commit 或不可变包版本。
6. Web、飞书和未来钉钉尚未共享统一 TaskRun / Channel Event Contract。
7. Pi Session 与 Forge Memory 的长期状态归属需要明确，避免双写和冲突。
8. 如果保留 Forge 现有 Pipeline 并在外层增加 Pi，会形成两个 Orchestrator，导致状态、重试、补查和责任归属冲突。

### 2.3 迁移原则：不是外套一层 Pi

每迁移一项能力，都必须明确四个问题：

1. 谁创建和推进状态？
2. 谁有权执行副作用？
3. 谁保存正式事实和审计？
4. 旧实现何时退出主路径？

统一答案：

| 工作类型 | 调度者 | 执行者 | 真相源 |
|---|---|---|---|
| 多轮澄清、Skill 选择、Stage 推进 | Pi | Pi Runtime | Pi TaskRun |
| QueryPlan 生成、Compile、Lint | Pi 发起 | Forge | Forge QueryRun / Audit |
| SQL 审批等待 | Pi 暂停与恢复 | 渠道收集、Forge 验证 | Forge Approval Record |
| 数据库只读查询 | Pi 发起 | Forge Executor | Forge QueryRun / Audit |
| 补查 | Pi 根据 Artifact 和用户决定 | Forge 执行新 QueryRun | Pi TaskRun + Forge QueryRun |
| 分析、报告和渠道渲染 | Pi | Skills / Renderer | Artifact Store |
| 指标与组织知识入库 | Pi 编排审核 | Forge Registry | Forge Registry History |
| 定时知识收集 | Pi Scheduler | 受控采集工具 | Candidate Store；确认后进入 Forge |

Forge 内部只允许一次能力调用范围内的有限技术重试，不允许据此推进下一业务 Stage。Pi 也不能接管 Forge 的安全校验或数据库执行。

## 3. 第一条垂直切片

### 3.1 场景

固定使用一个可复算 Demo 场景：

> 最近两周新用户首购转化为什么下降？请拆分渠道和终端，并输出给业务负责人阅读的分析报告。

### 3.2 首批 Skills

仅接入四个 Skill：

1. `data-requirement-clarifier`
2. `metric-definition-reviewer`
3. `business-root-cause-analysis`
4. `data-analysis-report-writer`

暂不在 MVP 中接入全部 23 个 Skill。

### 3.3 MVP 成功标准

- 用户可以在 Web 中完成一次多轮澄清。
- 指标定义以结构化 Artifact 传给 Forge，不依赖复制粘贴自由文本。
- Forge 生成的 SQL 必须显示给用户审批。
- 批准的 SQL 与实际执行 SQL hash 一致。
- 查询使用只读数据源，并受 timeout、row cap 和 ACL 约束。
- 分析结论中的每个核心发现都能引用 QueryResult 证据。
- 数据不足时返回 `incomplete + suggested_queries`，不编造结论。
- 一个 TaskRun 可以从审批等待状态恢复。
- Pi、Forge 和 Web 日志可以通过 `task_run_id` 关联。
- 现有 Forge 查询流程和 `/api/prepare-query` 外部安全边界不回归。

## 4. 推荐部署边界

### 4.1 Pi Orchestrator Service

新增独立 Node.js 服务，使用 `@earendil-works/pi-coding-agent` SDK：

```text
services/pi-orchestrator/
├── src/
│   ├── server.ts
│   ├── runtime.ts
│   ├── task-store.ts
│   ├── skill-router.ts
│   ├── artifacts/
│   ├── tools/forge.ts
│   └── renderers/
├── tests/
├── package.json
└── tsconfig.json
```

第一阶段建议放在 Forge 仓库中，减少跨仓库联调成本；接口稳定后再决定是否拆成独立仓库。不要在第一阶段迁移 Forge Python 核心。

Pi Runtime 配置：

- `noTools: "builtin"` 或等效设置。
- 只注册白名单 Forge Tools。
- Session 使用租户隔离目录或独立持久化实现。
- 固定并锁定 Pi SDK 版本。
- 模型凭证由服务端注入，不读取用户本机 Pi 配置。
- 不加载全局个人 Skills、Extensions 或 AGENTS.md。

### 4.2 Forge Service

Forge 继续作为 Python/FastAPI 服务，保留：

- Registry 与 Retriever。
- LLM Structured Output 查询生成。
- Compiler、Lint、Executor。
- SQL 审批、ACL 和 Audit。
- QueryRun 持久化。

### 4.3 Skills Package

拾穗 DATA 保持独立仓库，增加 Pi Package manifest：

```json
{
  "name": "@shisuidata/data-skills",
  "private": true,
  "version": "0.1.0",
  "keywords": ["pi-package"],
  "pi": {
    "skills": ["./skills"]
  }
}
```

具体是否发布到 npm 在垂直切片完成后决定。第一阶段可以固定 Git commit 或本地只读路径加载，但生产环境必须使用明确版本，不能跟随可变的工作区目录。

## 5. 服务契约

### 5.1 Pi 对渠道的 Task API

建议最小接口：

```text
POST /v1/tasks
POST /v1/tasks/{task_run_id}/messages
POST /v1/tasks/{task_run_id}/actions
GET  /v1/tasks/{task_run_id}
GET  /v1/tasks/{task_run_id}/events
```

创建任务：

```json
{
  "channel": "web",
  "channel_conversation_id": "...",
  "identity": {
    "org_id": "org_demo",
    "team_id": "team_growth",
    "user_id": "user_123"
  },
  "message": "最近两周新用户首购转化为什么下降？"
}
```

渠道动作：

```json
{
  "action": "approve_query",
  "artifact_id": "ar_review_...",
  "query_run_id": "qr_...",
  "sql_hash": "sha256:..."
}
```

Task API 返回渠道无关事件，由各渠道 Renderer 转换为页面、卡片或 Markdown。

### 5.2 Pi 对 Forge 的受控工具

第一阶段只提供：

```text
forge_prepare_query
forge_get_query_run
forge_approve_query
forge_cancel_query
forge_get_query_result
forge_submit_feedback
```

工具调用必须自动携带：

```text
task_run_id
correlation_id
org_id
team_id
user_id
service_identity
```

禁止向模型暴露：

- Forge 服务密钥。
- 数据库 URL 和密码。
- 原始渠道令牌。
- 超出当前用户权限的 Registry 或结果集。

### 5.3 Forge QueryRun API

保留现有 `/api/prepare-query` 作为“外部 Agent 只生成 SQL、不执行”的 v1 安全契约，不改变其语义。

为受信任的内部 Pi Orchestrator 新增独立、带 scope 的 QueryRun API。建议目标契约：

```text
POST /api/internal/query-runs
GET  /api/internal/query-runs/{query_run_id}
POST /api/internal/query-runs/{query_run_id}/approve
POST /api/internal/query-runs/{query_run_id}/cancel
GET  /api/internal/query-runs/{query_run_id}/result
```

创建 QueryRun：

```json
{
  "task_run_id": "tr_...",
  "question": "按渠道和终端比较最近两周新用户首购转化率",
  "intent": {},
  "org_id": "org_demo",
  "team_id": "team_growth",
  "user_id": "user_123",
  "dialect": "postgresql"
}
```

待审核响应：

```json
{
  "query_run_id": "qr_...",
  "status": "needs_review",
  "forge_json": {},
  "sql": "SELECT ...",
  "sql_hash": "sha256:...",
  "dialect": "postgresql",
  "registry_version": "...",
  "expires_at": "...",
  "review_required": true
}
```

批准请求：

```json
{
  "approver_user_id": "user_123",
  "sql_hash": "sha256:...",
  "approved_at": "..."
}
```

Forge 必须服务端复核：

- QueryRun 仍处于 `needs_review`。
- `approver_user_id` 与任务身份和权限一致。
- `sql_hash` 与待审核 SQL 一致。
- 数据源、租户和 Registry 上下文未漂移。
- 审批未过期。
- 部署允许执行且数据库只读确认通过。

### 5.4 幂等性

所有写操作支持 `Idempotency-Key`：

- 重复创建任务不产生两个 TaskRun。
- 渠道重复投递按钮事件不执行两次 SQL。
- 重试批准请求返回同一个最终 QueryRun 状态。

## 6. Artifact Contract

第一阶段至少定义四类 JSON Schema：

### 6.1 ClarificationArtifact

```json
{
  "goal": "定位新用户首购转化下降的主要贡献因素",
  "known_facts": [],
  "assumptions": [],
  "open_questions": [],
  "dimensions": ["channel", "device"],
  "time_range": {},
  "acceptance_criteria": []
}
```

### 6.2 MetricDefinitionArtifact

```json
{
  "metric_name": "新用户首购转化率",
  "numerator": "时间窗内完成首购的新用户数",
  "denominator": "同一 Cohort 的有效新用户数",
  "grain": "day/channel/device",
  "window": "registration + N days",
  "filters": [],
  "status": "needs_confirmation"
}
```

### 6.3 QueryResultArtifact

```json
{
  "query_run_id": "qr_...",
  "sql_hash": "sha256:...",
  "columns": [],
  "rows": [],
  "row_count": 0,
  "truncated": false,
  "dialect": "postgresql",
  "registry_version": "...",
  "execution_ms": 0,
  "executed_at": "..."
}
```

### 6.4 AnalysisArtifact

```json
{
  "status": "complete",
  "summary": "...",
  "findings": [
    {
      "statement": "...",
      "evidence_refs": ["qr_...#row:1-4"],
      "confidence": "high"
    }
  ],
  "hypotheses": [],
  "recommendations": [],
  "limitations": [],
  "suggested_queries": []
}
```

生产实现不能只依赖 Markdown 模板或正则抽取 JSON；关键 Artifact 应在模型输出阶段使用 JSON Schema，并在进入下一 Stage 前校验。

## 7. 实施阶段

### Phase 0：契约与基线

交付：

- 固化本文中的 TaskRun、QueryRun 和四类 Artifact Schema。
- 为垂直切片准备固定 Demo Schema、数据和预期事实。
- 记录当前 Forge Web 查询链路回归基线。
- 固定 Pi SDK 和拾穗 DATA Skills commit。

门禁：

- Artifact Schema 有单元测试。
- Demo 场景事实可以通过确定性脚本复算。
- 现有 Forge 测试通过。

### Phase 1：只生成 SQL 的 Integration Spike

交付：

- 创建最小 Pi Orchestrator Service。
- 只加载四个目标 Skill。
- 注册 `forge_prepare_query`；`forge_get_query_run` 留到 Phase 2 的 QueryRun API 完成后再加入。
- 复用现有 `/api/prepare-query`，只展示 SQL，不执行。
- Web 能展示 Pi 流式输出和 ReviewRequest。

目的：

- 验证 Pi SDK、Skill 加载、上下文和 Forge API 的集成方式。
- 不在此阶段放宽现有外部 Agent 安全边界。
- 验证 TaskRun 只由 Pi 推进，Forge 不再为这条新链路选择后续 Pipeline。

退出条件：

- 新链路不调用 Forge `IntentRouter` 或 `PipelineRunner`。
- Pi 只通过受控工具调用 Forge 查询能力。
- 同一个任务不存在 Pi TaskRun 与 Forge Pipeline state 两套可写流程状态。

### Phase 2：QueryRun 审批执行闭环

交付：

- Forge 内部 QueryRun API 与持久化。
- SQL hash、过期、幂等和服务 scope 校验。
- Web 审批按钮。
- QueryResultArtifact。
- 端到端 Audit 关联。

门禁：

- 未批准、hash 不匹配、身份不匹配和过期请求全部拒绝。
- 重复批准不会重复执行。
- SQL 修改后原批准失效。
- 数据库写语句和多语句继续被阻止。

### Phase 2.5：前置 Skill 结构化执行

交付：

- Pi 模型真实执行 `data-requirement-clarifier` 和 `metric-definition-reviewer`。
- 每个 Stage 使用独立隔离 Session，只注入目标 Skill 全文和对应终止型 Structured Output Tool。
- 服务端生成 `artifact_id/task_run_id/producer/created_at`，模型只能填写 Schema 约束的 payload。
- Artifact 校验通过后才能推进 TaskRun；缺少工具调用、重复提交或非法结构一律失败关闭。
- 模型配置只从 Pi Orchestrator 专用进程环境与 `agentDir` 加载，不读取个人全局 Pi 配置。

门禁：

- 模糊需求能够生成 `ClarificationArtifact` 并进入 `needs_input` 或 `ready_for_query`。
- 指标口径能够生成 `MetricDefinitionArtifact`，未确认口径不得进入 QueryRun。
- Session 没有文件、Shell、数据库和 Forge 执行工具。
- 固定 Session 模拟测试覆盖成功、未调用 Artifact Tool、重复提交和 Schema 拒绝。
- 未配置专用模型 Runtime 时健康检查明确降级，不能静默使用全局凭证。
- 使用专用测试模型完成至少一次真实 Provider 冒烟，确认 Provider 支持严格 Tool Schema 和终止型提交。

### Phase 3：分析与报告闭环

> 当前状态：已完成。唯一一次补查只能选择 AnalysisArtifact 中已有的 suggested_query；child TaskRun 绑定 parent_task_run_id，完整经过 Forge SQL 审核；父任务消费 child QueryResult 后合并证据重新分析。创建和恢复接口均支持幂等重放。

已验证：

- Coding Plan 真实 Analysis/Report 冒烟通过。
- Web → Pi → fake Forge → Coding Plan Analysis/Report 三服务 E2E 通过，事件流完整推进到 `report.completed`。
- Web → Pi parent/child TaskRun → fake Forge → Coding Plan 合并分析补查 E2E 通过，父事件流推进到第二次 `analysis.completed`。
- 无效 row evidence、跨 QueryRun evidence、报告新增 finding、过度确定因果措辞和模型自由 Markdown 均失败关闭。

交付：

- 接入归因分析和报告 Skill。
- AnalysisArtifact 使用 JSON Schema。
- 每条核心发现必须包含证据引用。
- 支持 `incomplete + suggested_queries`。
- 支持用户发起一次补查；补查仍经过 Forge 审批。
- 新链路中的分析、报告和补查调度全部由 Pi 完成，不再进入 Forge `agent/pipeline.py`。

门禁：

- 不允许无证据核心结论。
- 不把相关性描述为确定因果。
- 数据不足时不编造数字。
- 报告能够回溯到 QueryRun。
- Forge 分析/报告 Pipeline 不再是新链路主路径，并有明确兼容期和删除条件。

### Phase 3.5：Pi 状态持久化

> 当前状态：已完成。生产 Server 默认使用持久化 SQLite Store；等待审批及其他安全暂停点可跨实例恢复。执行中的 Stage 不会被自动重放，进入 Phase 3.6 处理。

本阶段不改变 Pi / Forge 职责，只替换 Pi 的正式状态介质：

1. 新增 `PI_ORCHESTRATOR_STATE_DB`，默认位于服务专用 `.runtime/state/orchestrator.sqlite3`，不得写入个人 `~/.pi/agent`。
2. TaskRun、TaskEvent、Artifact 必须存入同一个 SQLite 数据库；启用 WAL、foreign keys 和 busy timeout。
3. Task status transition 必须保留 optimistic compare-and-set；并发进程不能同时推进同一状态。
4. Event 的 `(task_run_id, sequence)` 必须唯一，sequence 在数据库事务内生成。
5. Artifact 写入前继续执行现有 Schema 和 producer 校验，读取后返回 defensive copy。
6. 生产 Server 默认使用持久化 Store；`InMemory*Store` 只保留给单元测试和显式依赖注入。
7. 重启测试至少覆盖：TaskRun、父子 lineage、事件顺序、Artifact、等待审批和 incomplete 安全暂停状态可恢复。
8. `clarifying/querying/analyzing/rendering` 等执行中崩溃不能伪装成成功；本阶段记录为后续 startup reconciliation 门禁，在没有 Stage lease / attempt 前不自动重放模型或 SQL。
9. 数据库文件和 WAL/SHM 必须被 Git 忽略；API Key 和模型文本日志不得进入状态库之外的旁路文件。

已验证：

- Store reopen 恢复 TaskRun、parent/child lineage、递增 Event sequence 和 Artifact。
- 新 Application 实例恢复 `waiting_for_query_approval` 和原 ReviewRequest。
- 默认 Server 关闭并以同一数据库重启后可读取原 TaskRun。
- 两个 SQLite 连接并发推进同一 TaskRun 时，status CAS 使后到者失败关闭。
- Task 创建和 status + `task.status_changed` 使用共享事务；Event sequence 在 `BEGIN IMMEDIATE` 事务内生成。
- TypeScript typecheck、40 个 Orchestrator tests、npm audit 及 Forge full suite 383 passed / 25 skipped。

当前边界：schema version 为 1，遇到未来版本会拒绝启动；`clarifying/querying/analyzing/rendering` 中途崩溃仍保留执行中状态，不自动重放，等待 Phase 3.6 的 Stage Attempt / lease / reconciliation。

### Phase 3.6：Stage Attempt、超时与异步恢复

> 当前状态：已完成。同步响应保留为迁移期兼容；Web 主路径已使用 `202 Accepted + polling`，不再依赖 300 秒单次 HTTP 请求。

本阶段解决 SQLite 持久化之后仍存在的“任务已进入执行中状态，但进程在外部调用期间崩溃”问题。

实施顺序：

1. 在 Pi 状态库新增 `stage_attempts`，记录 `attempt_id/task_run_id/stage/status/attempt_number/idempotency_key/retry_status/lease_expires_at/error`。
2. 同一 TaskRun 同时最多一个 `running` Attempt；attempt number 在事务内递增。
3. `clarifying/querying/analyzing/rendering` 及补查分析、Forge prepare/approve 都必须绑定 Attempt。
4. 模型调用与 Forge 调用使用 Stage 级 AbortSignal timeout；timeout 记录为 `timed_out`，不能只依赖渠道 HTTP timeout。
5. 成功结果落 Artifact、状态转换、Attempt 完成和关键事件尽量在同一事务提交。
6. startup reconciliation 只回收 lease 已过期的 Attempt；根据 Attempt 固化的 `retry_status` CAS 恢复 TaskRun，并记录 `stage.attempt_interrupted`。不得自动重新执行 SQL。
7. API 增加 Attempt 查询和进度事件；长耗时 Skill Endpoint 后续改为 `202 Accepted`，渠道通过 Task/Event polling 恢复，不维持超长同步请求。
8. 幂等重放必须返回同一 Attempt 或当前 Task 状态，不创建并发模型 Session。

当前已完成：

- SQLite schema v2 和 `stage_attempts` 唯一约束、递增 attempt number、幂等键、单 running attempt 门禁。
- Analysis、Report、QueryRun approval 创建持久化 Attempt，并在成功/失败/超时时终止。
- Stage timeout 与 lease 分离，配置强制 `lease > timeout`。
- Server 启动和定时扫描只回收过期 lease，CAS 恢复 retry status，不调用模型或 SQL。
- `GET /v1/tasks/{id}/attempts` 提供只读进度与诊断信息。
- 过期 lease、活跃 lease、双连接单次回收、跨 Application 审批恢复并完成 Attempt 的测试通过。
- 当前验证：TypeScript typecheck、43 个 Orchestrator tests、29 个 Web/Artifact/QueryRun 专项测试、npm audit 0 vulnerabilities、`git diff --check` 通过。

已完成：

- Clarification、Metric Review、Forge prepare/approve、Analysis、Supplemental Analysis、Report 均绑定持久化 Attempt/Lease/timeout。
- 长 Stage 支持请求体 `async: true` 并返回 `202 Accepted` 与 Task/Event/Artifact/Attempt polling URL；同步模式仅作兼容。
- Web 主链路的 prepare、approve、analysis、supplemental analysis 和 report 全部使用异步 polling。
- polling 能识别 failed、timed_out、interrupted，并回到可重试交互，不以渠道 HTTP timeout 判断任务事实。
- Web → Pi → fake Forge → Coding Plan 异步补查三服务 Playwright E2E 通过，父事件流推进到第二次分析完成。
- 当前验证：46 个 Orchestrator tests、Forge 383 passed / 25 skipped、npm audit 0 vulnerabilities、TypeScript typecheck 与 `git diff --check` 通过。

门禁：

- 崩溃测试覆盖 analyze、report 和 QueryRun approval 的 lease 过期恢复。
- 活跃 lease 不被第二实例回收；过期 lease 只能被一个实例成功回收。
- timeout、人工失败、进程中断三类结果可区分。
- startup reconciliation 不执行 SQL、不调用模型，只修复 Pi 状态并写审计事件。
- Web 渠道不再依赖 300 秒同步调用作为最终恢复机制。

### Phase 4：飞书与钉钉渠道

> 当前状态：进行中。第一步先建立可信且可复用的渠道边界，不能直接把现有飞书 Bot 从 Forge Agent 改成另一个会编排业务的 Bot。

当前已完成：

- SQLite schema v3 增加 `(channel,event_id)` 唯一 ChannelEvent Store；重复飞书消息和按钮回调返回原 TaskRun，不重复 prepare 或 approve。
- `X-Channel-Service-Key` 与 `PI_CHANNEL_SERVICE_KEYS` 独立鉴权；未知外部身份失败关闭，卡片 action 只信任飞书 callback operator。
- 服务端只读 identity map 将飞书 `open_id` / 钉钉 `user_id` 映射为 `org_id/team_id/user_id`，Task action 再次验证渠道、会话和任务所有者。
- 渠道无关 `ChannelPresentation` 可渲染 progress、needs_input、query_review、query_result、analysis、report、error；SQL 审批 action 固化 QueryRun 和 SQL hash。
- `web/pi_channel.py` 只封装 Pi Channel API 与 Card JSON；正式薄适配器 `web/feishu_pi.py` 不导入 Forge Agent、Executor、Registry、Memory 或数据库。
- `FEISHU_PI_ENABLED=true` 时 HTTP webhook 加载 `web/feishu_pi.py`；WebSocket 使用 `python -m web.feishu_pi`。旧卡片明确失效，关闭开关可回滚旧路径。
- 跨语言 Pi Feishu client → ChannelEvent → fake Forge → approve → query_result E2E 通过。
- 当前验证：50 个 Orchestrator tests、Forge 387 passed / 25 skipped、npm audit 0 vulnerabilities、Python/TypeScript 编译和 `git diff --check` 通过。

M4.1 Dev 环境部署决定（2026-06-22）：

- 现在适合先部署到 SSH host `dev` 的 Ubuntu/NAS，目标是验证 Linux 运行时、服务边界、SQLite 持久化、重启恢复和内网服务连通性；这不是 Phase 4 最终验收。
- 部署前只做只读环境盘点，不覆盖现有服务、凭证或数据；使用独立目录、端口、状态库和测试身份映射。
- 首次部署保持 `FEISHU_PI_ENABLED=false`，先完成 health、fake Forge/隔离数据源和跨进程 smoke；真实飞书开关必须在确认测试应用、open_id 映射和专用 Channel Key 后单独开启。
- 不复制本地 `.env`、`.runtime/models.json` 或数据库凭证；NAS 所需密钥只能由目标机已有环境或人工注入。
- M4.1 通过条件：Orchestrator readiness、ChannelEvent 幂等、SQL hash 审批、只读结果、进程重启恢复；不包含尚未开放的补充信息、取消、补查和钉钉。

M4.1 Dev 部署结果（2026-08-21）：

- Forge feature commit `ba5ebe1` 已推送，已合并并推送 `main` merge commit `e0b104b`。
- Ubuntu/NAS `dev` 使用独立目录 `~/services/forge-m4.1`，代码固定为 `e0b104b`；未覆盖已有服务。
- 使用独立 user systemd services：`forge-m41-fake.service` 和 `forge-m41-pi.service`；只监听 `127.0.0.1:18000/14310`。
- 目标机验证：Pi 50/50、Forge 383 passed / 2 skipped、TypeScript typecheck、npm audit 0 vulnerabilities。
- 隔离 fake Forge 冒烟通过：readiness ok、消息首次 202/重复 200、同一 TaskRun、SQL review、hash 审批首次 202/重复 200、query_result。
- Pi 进程重启后 PID 已变化，SQLite WAL 中的 TaskRun 恢复为 `ready_for_analysis`，presentation 仍为 `query_result`。
- 缺失 Channel Key 和未知飞书身份均返回 403；目标机密钥仅生成并保存在 mode 600 配置文件，未写入仓库或日志。
- 限制：当前 Forge 为 fake、模型目录使用不可调用的测试占位凭证，未访问真实数据库、模型或飞书；readiness 只证明目录/配置完整，不证明模型凭证有效。

M4.1 数据源决定（2026-08-21）：

- Dev 环境直接使用 Forge 仓库自带测试数据集，不引入客户数据或生产数据库。
- 测试 Registry 与 SQLite 数据库必须复制到 `~/services/forge-m4.1/state/test-dataset`，运行期只读；不得修改 Git checkout 中的 fixture。
- 该配置只允许 `dev` profile。PoC/生产 readiness 仍必须拒绝 `tests/datasets/*` benchmark Registry，不能因本次测试放宽门禁。
- 先用真实 Forge QueryRun/Compiler/Executor 替换 fake Forge；模型凭证仍必须由目标机环境独立注入，不从本地复制。若目标机没有有效凭证，可使用仅限 M4.1 的确定性 OpenAI-compatible test model 验证编译和执行链路，但不得把它计为真实模型验收。
- 数据集采用“先评估、后最小补齐”：先对 `tests/datasets/large` 的表、行数、时间跨度、NULL/空集、关联完整性和 40 题覆盖做清单；只有当前渠道场景缺少证据时，才在独立 mock fixture 中补充，不直接篡改 benchmark 原始数据。
- M4.1 至少需要覆盖：正常聚合、空结果、NULL、结果截断、SQL hash 不匹配、重复审批、分析证据引用和一次补查所需的父子结果。若 large fixture 已覆盖则直接复用；否则新增版本化 `m4-channel` fixture 和生成脚本。

M4.1 测试数据评估与真实执行结果（2026-08-21）：

- `tests/datasets/large` 有 200 张表，40 个基准问题，覆盖多表聚合、复杂过滤、HAVING、TopN、窗口、时序、ANTI/SEMI JOIN 和综合查询；大量事实表超过 1,500 行，多表包含真实 NULL 分布。
- 当前渠道里程碑所需的数据行为均已覆盖，无需立即扩充 fixture：正常聚合返回 6 行；空集 0 行；NULL 保留；超过上限时返回 200 行且 `truncated=true`；hash 不匹配返回 409；正确重试和同幂等键重放通过。
- 数据库和 Registry 已复制到 NAS `state/test-dataset`，原始文件 mode 400、数据库使用 SQLite `mode=ro`；写入尝试被拒绝且冒烟前后 SHA-256 不变。Runtime cache 使用独立目录，不写 fixture。
- NAS 已启动真实 `forge-m41-api.service`（127.0.0.1:18001）和确定性测试模型 `forge-m41-llm.service`（127.0.0.1:18002）；Pi 已切换到真实 Forge QueryRun/Compiler/Executor，原 `forge-m41-fake.service` 已停用。
- 真实跨服务链路再次通过：ChannelEvent → 确定性测试模型 → Forge JSON → Compiler → SQL review → hash 审批 → SQLite 只读执行 → 6 行 query_result；重复消息和审批仍幂等。
- 结论：large fixture 足够 M4.1 的渠道、审批、执行和持久化测试，暂不制造新数据。它是广覆盖随机合成数据，不适合作为“已知根因”准确率金标；进入归因分析/一次补查里程碑时，如需断言特定因果结论，再新增小型、版本化且有 ground truth 的 `m4-channel` fixture，不污染现有 40 题 benchmark。

M4.1 内网 Web 暴露决定：

- Forge Web 可从 `127.0.0.1:18001` 改为只监听 NAS 的固定内网 IP，不监听 `0.0.0.0`，不通过公网反向代理或 Cloudflare 暴露。
- Pi Orchestrator `14310`、确定性测试模型 `18002` 和 SQLite 文件继续只允许 loopback；浏览器不能直接访问 Pi、模型或数据库。
- 内网 Web 必须开启 Forge Web 认证，生成独立测试管理员密码并仅保存在目标机 mode 600 环境文件；不得在计划、Git、日志或回复中回显。
- Web 设置 `PI_ORCHESTRATOR_ENABLED=true`，仅由 Forge Web 服务端代理到 `127.0.0.1:14310`。部署后验证匿名 `/tasks` 被重定向到登录页、认证后可访问，且内网之外无新增监听。

M4.1 内网 Web 部署结果：

- Web 只监听 `192.168.8.10:18001`，访问地址为 `http://192.168.8.10:18001/tasks`；匿名请求返回 302 登录跳转。
- Pi `127.0.0.1:14310` 与确定性测试模型 `127.0.0.1:18002` 继续只监听 loopback，Pi readiness 返回 200。
- Forge Web 认证和 Pi Web 代理已开启；独立随机管理员密码只保存在 NAS mode 600 `config/forge.env`，未回显或提交。
- Pi 由 enabled user systemd unit 常驻，`Linger=yes`、`Restart=on-failure`、当前 `active`；机器重启或用户退出后仍由 systemd 管理。持久状态在 SQLite，模型 Session 只在 Stage 执行时按需创建，不是常驻对话进程。

M4.1 问候语错误生成 SQL 修复决定：

- 现象：输入 `hello` 仍生成固定 SQL。直接原因是 M4.1 确定性测试模型对所有输入返回同一 `generate_forge_query`；产品层根因是 Forge 在调用模型前没有拒绝纯问候语。
- 修复必须放在 Forge `prepare_query` 的模型前置门禁，而不是仅修改测试模型：标准化后的纯问候语返回 `needs_clarification`，不得调用模型、不得生成 SQL 或 QueryRun review。
- 保持范围最小，只识别明确的纯问候语，不用“长度过短”等启发式误伤“用户数”“GMV”等合法短查询。
- 增加单元测试和 NAS 真实 ChannelEvent 回归：`hello` presentation 必须为 `needs_input` 且无 SQL 审批 action；标准数据问题仍为 `query_review`。
- 已完成：Forge commit `3323f7f` / main merge `2453bc9` 部署到 NAS；完整 Python suite 393 passed / 25 skipped；真实 ChannelEvent `hello` 返回 `needs_input`、0 actions、无 SQL。部署时同时修正 Pi 到内网 Forge 地址，避免 Forge 改为内网 IP 后仍请求 loopback。
- 当前模型边界：NAS Forge 使用确定性 M4.1 测试模型；Pi 的 Coding Plan catalog 仍是 `volcengine-coding-plan / ark-code-latest`，但 NAS 仅有不可调用占位 Key。尚未完成真实 Coding Plan 模型验收，也未确认 `ark-code-latest` 当前是否映射到用户所说的 DeepSeek V4 Flash。

M4.1 用户配置接管与服务重启规则：

- 用户已在 NAS 配置 LLM，并要求确认测试数据库连接后重启服务。操作前只检查配置项是否存在、来源优先级、文件权限和 readiness，不读取或回显 Key/连接密码。
- 数据库继续使用项目 `large` fixture 的独立只读副本；如环境变量已配置则不重复覆盖。Web 设置与 systemd `EnvironmentFile` 冲突时必须明确唯一生效来源，避免页面显示“已保存”但进程仍使用旧值。
- 重启顺序为测试模型/真实外部模型依赖 → Forge API → Pi Orchestrator；重启后验证 Forge database/readonly/llm readiness、Pi readiness、`hello` 不生成 SQL和标准查询审核链路。

剩余：

- 使用真实飞书测试应用完成消息、卡片 operator、更新卡片 smoke。
- 完成 `provide_input`、`cancel_task`、`request_supplement` 的 child Task lineage 与卡片交互后再开放这些按钮。
- 飞书稳定后新增钉钉 SDK Adapter；只允许复用 ChannelEvent/Presentation，不复制业务状态机。

第一批实施契约：

1. 定义统一 `ChannelEvent`：`event_id/channel/event_type/external_user_id/conversation_id/message_id/task_run_id/payload`。
2. 渠道调用 Pi 使用独立 `X-Channel-Service-Key`；不得复用 Forge Pi service key。
3. 外部 `open_id/user_id` 必须通过服务端只读 identity map 映射到 `org_id/team_id/user_id`；未知身份失败关闭。
4. 入站 `(channel,event_id)` 持久化唯一，重复投递返回原处理结果，不创建第二个 TaskRun 或重复批准。
5. 定义渠道无关 `ChannelPresentation` 和 Action：`provide_input/approve_query/cancel_task/request_supplement/render_report`；Renderer 只消费 Task/Event/Artifact，不推进状态。
6. 飞书适配器只做 SDK 收发、identity key 转交、卡片渲染和 action 回传；禁止继续调用 `agent.process()`、`agent.approve()` 或 `_execute_sql()`。
7. 迁移使用 `FEISHU_PI_ENABLED` feature flag；新路径验证通过前保留旧 Bot 回滚能力，但两条路径不得同时处理同一事件。
8. 钉钉在飞书契约稳定后接入，不新增第二套业务状态机。

交付：

- 抽取 Channel Adapter 和 Renderer。
- 飞书与钉钉统一消费 Task Event。
- 实现审批、取消、补充信息和继续任务按钮。
- 渠道重试使用幂等键。

门禁：

- 三个渠道使用同一 TaskRun 状态机。
- 不在 Bot 内复制 Skill 路由和 Forge 查询逻辑。
- 同一用户的权限在各渠道保持一致。

### Phase 4.3.1：SQL 引用完整性与失败状态修复（2026-08-21 现场反馈）

现场截图显示旧 `/chat` 路径把以下 SQL 提交执行：引用未加入 `FROM/JOIN` 的 `dim_city.city_name`、对 `dwd_order_detail` 做恒真自连接、且“订单总额”缺少聚合。SQLite 返回 `no such column` 后，前端仍显示绿色“已执行”。

本修复优先于后续控制面建设：

1. 定位生成 Artifact、Compiler/Validator 与 `/chat` 状态渲染，确认错误是模型输出、Registry 关系缺失还是编译门禁漏洞。
2. Forge 在审核前校验所有字段引用均绑定已声明表实例；禁止无 alias 的同表自连接和恒真同字段连接；失败关闭为可理解的 SQL 准备错误，不创建可执行 QueryRun。
3. 执行失败不得显示“已执行”；页面显示“执行失败”，用户可返回修改或重新生成，不泄露完整堆栈和内部连接信息。
4. 增加截图对应回归测试，并确认 `/tasks` 主路径与旧 `/chat` 兼容路径行为一致。

门禁：模型生成的结构化 JSON 即使通过 JSON Schema，也必须通过引用完整性和关系语义校验；Compiler 不得把未绑定表引用编译成 SQL 后交给数据库发现错误。

实施结果：

- Compiler 新增查询作用域引用完整性校验，递归覆盖 CTE、集合操作和条件子查询；未绑定表引用、重复无 alias 自连接、同字段恒真 JOIN、JOIN 条件引用未知表或未引用待连接表均在生成 SQL 前拒绝。
- Agent 会把上述确定性错误反馈给模型进入受控重试；截图对应案例的回归测试确认首次错误输出不会进入审核，修正为 `dwd_order_detail → dim_city + SUM + GROUP BY` 后才生成 SQL review。
- Executor 对数据库错误做有界分类，原始异常只进服务端日志；Web/API 不再返回 SQLAlchemy 堆栈和内部错误链接。
- 旧 `/chat` 只有执行成功才显示绿色“执行成功”；失败显示红色“执行失败”、action=`execution_failed`，且失败数据不再恢复旧 Pipeline 分析阶段。
- 本地验证：Python 405 passed / 25 skipped，Pi Orchestrator 50 passed；Python LSP 未配置，已用 compileall、自动测试与 scoped diff check 替代。
- NAS `762fe2c` 真实回归：截图原问题“各城市的订单总额是多少？”生成了包含 `dim_city`、`SUM(total_amount)`、`GROUP BY city_name` 的可执行 SQL，审批后返回 14 行且无执行错误；不再出现未绑定表和恒真自连接。
- 剩余语义风险：当前模型选择 `订单明细 → 用户 → 地址 → 城市`，若用户存在多个地址可能重复放大金额。引用完整性门禁只能保证 SQL 结构合法，不能证明业务 join 粒度正确；该问题纳入模型准确率验证与 Registry 关系约束，不在 Compiler 中猜测修复。
- 紧急补丁复审发现并已修复两个收口项：JOIN ON 现在必须整体同时引用待连接表和至少一个既有作用域表，阻断 `joined.a = joined.b` 隐性笛卡尔积；旧 Pipeline 执行失败后持久标记 `failed`，不会被下一次成功审批错误恢复。复审无剩余阻断项；Python 407 passed / 25 skipped，Pi Orchestrator 50 passed。main `689b833` 已部署 NAS，Forge/Pi 均 active，可继续 Model Control Plane。

### Phase 4.3.2：统一 Query Assurance Pipeline（系统化融合既有校验）

方向确认：不能继续用单点补丁追逐模型错误。Forge 既有的动态 Tool Schema、Compiler、Convention Lint、Registry、字段约定、歧义规则、只读校验、QueryRun 审批和测试基准必须融合成一个可版本化、可审计、所有入口共用的查询保障流水线。

目标流水线：

```text
模型输出 Forge JSON
→ Contract Gate（动态 JSON Schema / Structured Output）
→ Registry Gate（表、字段、权限、关系与 Registry revision）
→ Scope & Type Gate（查询作用域、JOIN 连通性、聚合/窗口/CTE 类型规则）
→ Convention Gate（业务口径、字段约定、歧义规则、粒度与危险模式）
→ Deterministic Compile
→ SQL Safety Gate（单条只读、方言、行数、超时、执行账户）
→ QueryAssuranceReport
→ 人工 hash 审批
→ 只读执行
→ Evidence / Audit / Feedback
```

实施要求：

1. 新建 Forge 内部统一 `QueryAssuranceService`，输出结构化 `QueryAssuranceReport`，包含每个 Gate 的版本、状态、诊断、Registry revision、model revision 和最终 SQL hash。
2. `/api/prepare-query`、Pi QueryRun、旧 `/chat` 兼容入口、飞书及后续钉钉必须调用同一服务；禁止各入口自行拼接 `lint + compile + execute`。
3. 动态 Tool Schema 负责约束模型生成空间，但不得作为唯一验证；服务端必须再次依据实际 Registry 校验表、字段、ACL 和 Join 关系。
4. Convention Lint 从针对题目的散落函数升级为版本化 Policy Bundle；区分 `error/warning/info`，生产 error 必须失败关闭，warning 必须进入审核界面。
5. QueryRun 固定 assurance policy revision、Registry revision 和 model revision；审批绑定 assurance report 与 SQL hash，任一输入变化后必须重新准备和审批。
6. 40 题 EA、危险 JOIN、NULL、窗口、CTE、方言和权限用例成为模型激活及 Policy 发布的共同回归门禁。
7. Raw SQL 只作为管理员显式兼容能力，仍经过 SQL Safety Gate；不能伪装为 Forge JSON 已保障查询。

第一批实施：抽取统一 Report/Service，接管 Agent 两条生成路径，补 Registry 引用与 ACL 服务端校验，并保持现有 retry 语义。完成后再接 QueryRun 固定 revision 和 Model Control Plane 激活门禁。

第一批实施结果：

- 新增 `forge/assurance.py`，统一执行 `contract_registry_acl → convention_policy → scope_type_compile → sql_safety` 四个 Gate。
- 生成不可变 `QueryAssuranceReport`，固定 assurance/policy/Registry/model revision、逐 Gate 诊断、最终 SQL 与 SHA-256；Registry 不存在或损坏时失败关闭。
- `prepare_query()` 与旧 `process()` 已移除散落的 `lint + compile` 组合，统一调用 Assurance Service，并保持受控模型重试；旧会话保存 `pending_assurance`。
- 服务端使用完整 Registry 和用户 ACL 二次校验真实表/字段，不再只依赖模型 Tool Schema；错误诊断有界，不回显字段枚举和权限清单。
- LLM 每次响应携带实际 snapshot 的 model revision，Assurance Report 不再事后猜测当前配置。
- 回归覆盖：成功报告、未知字段、表 ACL、Registry 缺失、模型重试及 SQL hash；完整 Python 411 passed / 25 skipped，Pi Orchestrator 50 passed。
- NAS 已部署 main `44930a6`。真实 `/api/prepare-query` 回归“各城市的订单总额”经历 2 次受控修正后进入 `needs_review`，四个 Gate 全部 passed，并返回 Registry revision、实际 model revision 和 SQL hash。
- QueryRun 保障绑定已实现：SQLite 采用兼容迁移新增完整 Assurance Report/hash、Assurance/Policy/Model/Registry revisions；`needs_review` 缺少报告或报告 SQL/hash 不一致时直接转 failed。
- 审批现在同时校验 `sql_hash + assurance_report_hash + approver + expiry + Registry version + Assurance revision + Policy revision`；错误 report hash 与策略漂移均有回归测试。模型切换不改变已经生成的 SQL，因此在途 QueryRun 固定原 model revision，不与当前 active model 比较。
- Forge 内部 QueryRun API、Pi Client、Task Event、渠道审批动作和 QueryResultArtifact 已贯通 Assurance lineage；旧卡片缺少 Assurance hash 时失败关闭。
- 当前验证：Python 414 passed / 25 skipped；Pi typecheck 通过，50 tests passed。main `81656a1` 已部署 NAS，SQLite 兼容迁移成功；真实 ChannelEvent → review action 已携带 Assurance hash，审批 202 后进入 `query_result`。
- 真实复杂问题“各城市订单总额”在当前模型多次自修正时触发 Pi/Forge 请求 timeout。修复方向已确认并完成：建立端到端统一 Deadline Budget，单次模型调用、受控重试、Forge HTTP Client、Pi Stage timeout 与 lease 使用严格包含关系。
- Deadline/Retry Budget：查询准备总预算默认 210s，Forge HTTP 220s，Pi Stage 240s，lease 300s；Pi 启动时强制 `Forge HTTP < Stage < lease`。每次 LLM 调用使用 `min(模型配置 timeout, 当前剩余预算 - 5s 收尾预留)`，预算不足不再启动重试。
- OpenAI/Anthropic timeout 统一为有界 `LLMRequestTimeoutError`；`prepare_query` 返回稳定 `timed_out`，QueryRun 持久化该状态；Pi 将 Attempt 标记 `timed_out` 并恢复 `ready_for_query/query_prepare_retry`，渠道显示“查询准备超时，可重试”。
- 当前验证：Python 417 passed / 25 skipped；Pi typecheck 通过，52 tests passed。NAS 复杂问题在 45.3s 内进入 Assurance-bound review，证明 timeout 层级修复有效。
- 该回归同时发现并修复裸 `select` symbol 来源校验，`repurchase_rate` 这类未由真实字段或 agg/window 定义的名称无法再进入审核。
- 第二次回归暴露的动态 Tool Schema / 服务端契约混用已修复：模型生成约束、Compiler 静态契约、Registry/ACL 和 Alias/Scope Gate 已拆分，合法聚合/窗口 alias 不再被当作物理字段拒绝。
- 第三次真实回归 44.5s 内进入 review，但 SQL 回答了“购买用户数”而非“各城市订单总额”。定位到 Pi 的 `prepare_query` 仍调用旧 Agent `memory.build(user_id)`，把同一渠道用户的历史 EMS 注入了新 Task，造成跨 Task 意图污染。Pi Task 是唯一会话边界，prepare-only 路径现已只使用当前 Task 问题和 Registry/组织知识；旧 `process()` 才保留兼容会话记忆。
- NAS main `79c41f6` 隔离历史后再次回归：44.5s 内正确生成“城市 + SUM(订单金额) + GROUP BY”，Deadline 修复和意图隔离均生效；但模型选择了未经 Registry 确认的 `dim_region.region_id = dim_city.province_id` 关系，因此未执行。该结果进一步确认当前模型不能激活为生产默认，下一阶段必须以显式关系图和 EA/Join-grain 门禁阻断未确认关系。
- Registry Relationship Gate 已完成：Canonical Schema 增加版本化 `relationships`，每条关系记录左右字段、基数、状态与来源；只有数据库声明或人工确认的关系可进入生产查询，`inferred/unconfirmed` 只用于草案展示。Assurance 对每个物理 JOIN 做精确关系匹配，并在聚合已有侧度量时拒绝 one-to-many fan-out；关系同时按检索到的表裁剪后注入模型上下文。缺少关系元数据时，含物理 JOIN 的生产查询失败关闭，单表和 CTE 内部数据流不受影响。
- `forge sync` 现可导入数据库声明的单字段外键并保留人工确认关系；复合外键在 Canonical Schema 支持原子复合关系前不拆分导入，避免只用部分键也被信任。Assurance revision 已升级为 `query-assurance-v2`，旧 v1 审核必须重新准备。
- 当前验证：Python 433 passed / 25 skipped；Pi typecheck 通过，52 tests passed。NAS main `c6d68ca`：错误的 `dim_region.region_id = dim_city.province_id` 被 `relationship_grain` 拒绝，反向用户→订单聚合被 fan-out Gate 拒绝；真实复杂问题在 130.7s 内改用两条 confirmed many-to-one 关系进入审核。SQL 仍遗漏 `dim_region.level = 'city'`，因此未批准执行，继续作为 Model Profile EA/语义限定词门禁失败样本。

### Phase 4.4：Model Control Plane（无需重启的模型切换）

现状问题：

- Python `Config` 是进程导入时创建的全局单例，Web 保存 `forge.yaml` 后运行进程仍使用旧值，只能靠重启刷新。
- systemd `EnvironmentFile` 的 `LLM_*` 会覆盖 Web 写入的 YAML，出现“页面显示已保存，实际仍调用旧模型”。
- Forge 查询规划模型与 Pi Skill 模型分属两份配置，用户无法看清当前哪个 Stage 使用哪个模型。
- M4.1 已出现 `anthropic + Coding Plan base URL + deepseek-v4-flash` 组合请求 400，说明仅保存 provider/model/base_url 而不验证协议兼容性是不够的。

目标模型：

```text
ModelProfile（provider/protocol/base_url/model/capabilities/secret_ref）
        ↓ validate
ModelProfileRevision（不可变）
        ↓ atomic activate (CAS)
ActiveModelBinding（scope + stage → revision）
        ↓ snapshot
TaskRun / QueryRun / StageAttempt.model_revision
```

Scope 至少支持：

- `forge.query_planning`：自然语言到 Forge JSON。
- `pi.clarification`、`pi.metric_review`、`pi.analysis`、`pi.report`：允许统一默认，也允许按 Stage 覆盖。
- `org/team` 默认；Task 级临时覆盖只能由授权管理员设置并写审计。

切换流程：

1. 用户新建或编辑 ModelProfile 草案；API Key 只写 Secret Store/Vault，Profile 保存 `secret_ref`。
2. 服务端做协议校验、模型能力检查和最小无副作用 smoke；例如 Structured Output / Tool Calling 必须真实通过。
3. 验证成功后生成不可变 revision；用户点击激活时使用 expected current revision 做 CAS。
4. 新 Task/QueryRun 读取一次 ActiveBinding 并固定 revision；在途任务和 retry 继续使用原 revision，避免中途换模型改变结果。
5. 客户端/连接池按 revision 缓存并有界淘汰；不重启进程，不修改全局单例。
6. 激活失败不影响当前模型；支持一键回滚到上一 revision，完整记录操作者、时间、验证结果和影响 scope。

门禁：

- 不允许每次 LLM 调用重新读取 YAML；使用进程内 revision cache + 持久化 Store + 原子失效通知。
- 不允许把 API Key 放进 Web 响应、SQLite Artifact、Task metadata 或日志。
- `readiness` 分开报告 active revision 是否可用，以及 draft validation 是否失败；不能只检查“Key 非空”。
- Provider、协议和 base URL 必须成套验证，不能把 Anthropic SDK 指向只兼容 OpenAI Chat Completions 的地址。
- 模型切换不是普通用户对话动作；需要组织管理员权限和审计。
- 数据库连接、Registry revision 和执行账户不随模型切换热更新；本阶段只处理模型路由。

当前已完成：

- 新增热加载 `ModelConfigSnapshot`：每次新 LLM 调用获取一次不可变快照；配置未变化时命中内存 cache，`forge.yaml` mtime/size 或 LLM 环境变量变化时自动生成新 revision。
- `agent/llm.py` 不再在生产调用路径直接读取全局 `cfg.LLM_*`；同一调用始终使用同一 snapshot。
- Web 保存 LLM 后原子替换 YAML、失效 cache，新任务即时生效，不再显示“必须重启”。页面显示当前 provider/model/source/revision，并提示环境变量覆盖。
- 缺少 LLM 时明确返回“尚未配置 LLM”，不调用模型、不生成 Forge JSON 或 SQL；配置/协议错误返回有界错误，不回显 Provider 原始响应和密钥。
- 数据库设置页显示进程实际生效的脱敏 URL、环境变量来源和只读确认，避免 YAML 为空时误判“数据库未配置”。
- 当前验证：完整 Python suite 402 passed / 25 skipped；热加载、缺失、部分配置、未知 Provider、环境覆盖、Web 无重启保存和错误脱敏均有自动测试。
- NAS 已部署 main `ae67a7c`。Forge 查询模型从错误的 `anthropic + /api/coding` 改为 `openai + /api/coding/v3 + deepseek-v4-flash`，修改前后 Forge PID 均为 `3814124`，证明新任务热加载无需重启。
- 配置错误时真实 ChannelEvent 返回有界“LLM 配置错误”，不回显 Provider 原始响应；修正后下一条任务进入 `query_review`，批准后通过只读数据库执行。
- 当前质量风险：`deepseek-v4-flash` 本次真实输出未满足用户问题，错误增加 2026 时间过滤、遗漏渠道和 GMV，最终结果为空。热切换与协议兼容已通过，但该模型尚未通过 Forge 40 题准确率门禁，不能作为默认生产模型；需要在模型中心展示验证结果并阻止未达标模型直接激活。

仍待实施：

1. 将当前文件 revision cache 升级为持久化 ModelProfile / Revision / ActiveBinding Store。
2. 建立 SQLite `model_profiles/model_profile_revisions/active_model_bindings/model_switch_audit`。

当前实施切片（已完成基础控制面）：Forge `forge.query_planning` scope 已具备持久化 Revision/Binding/Audit。Revision 只保存非密配置和 `secret_ref`，支持 `env:` 与严格 mode 600 `file:` Secret；真实 Tool Calling smoke 和质量/性能门禁均通过后才可 CAS activate，rollback 也使用 expected binding version。`get_model_config()` 优先读取 active binding，无 active 时兼容回退现有环境/YAML；同一查询准备的全部受控重试固定一次 Model snapshot，避免切换中途改变 QueryRun。

当前验证：Python 442 passed / 25 skipped；Pi typecheck 通过，52 tests passed。NAS main `24d9bab` 已部署，Model Control SQLite 为 mode 600；现有 YAML Key 没有对应 `env:`/专用 mode 600 Secret 文件，迁移脚本因此失败关闭并阻止激活，未读取、复制或回显旧 Key，legacy fallback 仍可在 43.7s 内生成审核 SQL。

QualityValidationRun 已完成基础实现：后台逐题固定候选 revision，持久化 case 结果和准确率、Assurance 通过率、平均重试、P95 延迟、超时率；在显式只读 benchmark database 上比较 generated/reference SQL 结果集。API 使用 `202 + run_id + polling`，启动时只把遗留 running 标记 interrupted，不自动重放模型或 SQL。Validation 固定 Registry/Assurance/Policy lineage，激活和回滚时再次校验；lineage 漂移必须重跑。当前验证：Python 445 passed / 25 skipped；Pi typecheck 通过，52 tests passed。NAS main `33194ea` 已部署，40 cases 与显式只读 benchmark probe 正常，Model DB mode 600。

用户已明确授权将本轮提供的 Coding Plan Key 仅写入 NAS mode 600 `forge.env` 的 `LLM_API_KEY`，Revision 继续只保存 `env:LLM_API_KEY`，不把 Key 写入 SQLite、Artifact、日志或响应。配置已完成，Secret 扫描确认未进入 Model DB 或 worker script；真实 Tool Calling/Structured Output smoke 通过（1.94s）。持久化 40 题 Run `mvr_b08feedf6ffa49adb6cf20216f161a74` 已由独立 systemd transient worker 启动，当前 running；未达质量门禁不得激活。
3. 实现真实 Provider validate/activate/rollback API；配置保存与激活分离，失败保持旧 active revision。
4. Forge QueryRun 保存 `model_revision`；再将同一机制接入 Pi StageAttempt。
5. 增加并发切换、在途任务固定、失败回滚、进程重启恢复和 secret redaction E2E。

架构全景图：`docs/architecture-diagrams/forge-platform-architecture.html`。当前包含 11 个中文视角：产品架构、技术分层、元数据模型、端到端流程、关键闭环、实现与部署、控制面演进、模型运行时、安全信任、状态一致性、API 集成。NAS 已部署到 `/admin/architecture`：匿名访问 302，登录后 200。

### Phase 4.5：Registry Studio（结构层多视图与安全编辑）

目标：让结构层既能由 `forge sync` 从数据库同步，也能用数据库工程师熟悉的方式查看和维护，而不制造 JSON、DDL、ER 三套互相漂移的真相源。

当前差距：

- `schema.registry.json` 主要只有表名、字段名和部分 enum，缺少稳定生成 DDL/ER 所需的类型、nullable、default、主键、唯一约束、外键、索引、注释、schema/catalog 和视图信息。
- `registry/sync.py` 当前删除消失的表/列并直接写文件，缺少 drift preview、版本、审批和回滚，不适合直接承载可视化编辑。

目标 Canonical Schema：

- 数据源：`datasource/catalog/schema/dialect`。
- 表：名称、类型（table/view）、注释、业务别名、标签。
- 字段：顺序、原始类型、规范类型、nullable、default、PK、unique、注释、enum。
- 约束：primary key、foreign keys、unique constraints、indexes。
- 关系：数据库声明关系与人工推断关系分开；推断关系必须标记 `inferred/unconfirmed`。
- 元数据：schema version、registry revision、source fingerprint、synced_at、editor、change reason。

同一 Canonical Schema 的投影视图：

1. **表格视图**：按数据源/schema/table 浏览字段和约束，支持搜索、过滤和批量补注释。
2. **DDL 视图**：按方言确定性渲染 `CREATE TABLE`；允许导入/编辑 DDL 形成 Registry 草案，但默认不连接数据库执行。
3. **ER 图**：只根据外键和已确认关系生成；支持布局和关系草案，布局信息与结构元数据分离。
4. **JSON 视图**：高级用户查看 Canonical Schema 原文，仍经过 Schema 校验和权限检查。

安全编辑闭环：

```text
数据库 introspection / DDL import / 表格编辑 / ER 关系编辑
→ RegistryDraft
→ schema validation
→ deterministic diff
→ 人工审核
→ 发布 RegistryRevision
→ QueryRun 绑定新 revision
```

门禁：

- UI 中的“编辑 DDL”不等于执行 DDL；Phase 4.5 不提供数据库 migration 执行器。
- `forge sync` 先生成 drift proposal；删除表/列、类型收窄、nullable 收紧、PK/FK 变化均需明确审核。
- 发布使用 optimistic revision / CAS，防止两人覆盖；保存完整前后 diff、操作者和回滚点。
- ER 图不允许仅凭同名 `*_id` 自动晋升为正式外键；只能作为待确认建议。
- DDL parser/generator 先支持 SQLite、PostgreSQL、MySQL 的受控子集；未知方言语法保留为 unsupported diagnostics，不静默丢失。
- 结构层权限继续由 Forge ACL 控制；Pi 可编排“解释/审查结构变更”任务，但不是 Registry 真相源，也不直接落盘。

建议实施顺序：

1. 定义 `canonical-schema.schema.json`、revision/draft/diff 契约和兼容迁移器。
2. 扩展 `forge sync` introspection，先只读生成 proposal，保持旧 registry reader 兼容。
3. 实现确定性 DDL renderer、DDL parser 合约测试和 round-trip fixture。
4. 实现 Web 表格视图与 revision 审批。
5. 实现大图可缩放、可聚焦的 ER 视图及确认关系交互。
6. 最后开放受控编辑，不把结构编辑与生产数据库 migration 混在同一阶段。

### Phase 5：扩展 Skills 与组织能力

按价值逐批接入：

1. 漏斗、留存、EDA、A/B。
2. SQL Review、数据质量、表设计、血缘。
3. 看板、PPT、日报周报和数据文档。
4. 团队级 Skill 配置、Registry 版本与跨会话任务恢复。

每增加一个 Skill，都需要 Artifact Schema、固定测试用例和至少一条端到端评测。

## 8. 测试策略

### 8.1 单元测试

- Skill Router 选择与显式 Skill 固定。
- Artifact Schema 校验与版本兼容。
- SQL hash 与审批过期。
- 渠道身份映射。
- Renderer 降级行为。

### 8.2 合约测试

- Pi Forge Tool 与 Forge API 请求/响应。
- QueryRun 状态转换。
- 幂等键和重复渠道事件。
- 错误码和重试语义。

### 8.3 集成测试

- `needs_clarification` 不生成 SQL。
- `needs_review` 不自动执行。
- 用户批准后只执行审核的 SQL。
- QueryResult 能进入分析 Skill。
- `incomplete` 能形成补查建议并暂停。

### 8.4 E2E 测试

至少覆盖：

1. Web 完整成功链路。
2. 用户取消 SQL。
3. SQL hash 不匹配。
4. 用户跨会话恢复审批。
5. 查询超时和结果截断。
6. 分析证据不足。
7. 渠道重复投递。

### 8.5 质量评测

复用拾穗 DATA 的人工评测标准，并增加平台维度：

- 专业方法是否正确。
- 事实是否来自查询结果。
- 推断边界是否明确。
- 审批是否真实发生。
- 渠道呈现是否遗漏关键信息。
- 失败是否可恢复和回放。

## 9. 非目标

第一条垂直切片不做：

- 一次接入全部 Skills。
- 让 Pi 直接访问数据库。
- 让外部 `/api/prepare-query` 自动获得执行权限。
- 自动执行补查或无限 Agent 循环。
- 替换 Forge Compiler、Registry、Executor 或 Audit。
- 重写全部 Forge Web UI。
- 同时重构多租户、计费和部署系统。
- 将个人全局 Pi 配置带入客户环境。

## 10. 主要风险与控制

| 风险 | 控制方式 |
|---|---|
| Pi 是 coding harness，默认权限过大 | 使用 SDK、关闭内置工具、只注册白名单 Forge Tools |
| Skill 是说明文本，输出不稳定 | 关键输出增加 JSON Schema 和运行时校验 |
| Python 与 Node 双服务增加复杂度 | 第一阶段同仓库、固定协议、容器健康检查和合约测试 |
| Pi Session 与 Forge Memory 双写 | 按架构文档明确真相源，长期知识只进入 Forge |
| Skill 自动选择不稳定 | 关键 Workflow 显式固定 Skill ID，不仅依赖模型自发加载 |
| 审批事件被重放 | SQL hash、过期时间、幂等键、批准人和 QueryRun 状态校验 |
| 渠道身份可伪造 | 服务端身份映射，Forge 不信任客户端直接传入的 user_id |
| 分析结论幻觉 | 证据引用、限制声明、固定评测和人工反馈闭环 |
| Pi 或 Skill 升级造成行为漂移 | 锁定版本，升级前跑契约测试和端到端评测 |
| Pi SQLite 状态库损坏或备份遗漏 WAL | WAL、foreign keys、busy timeout、schema version 门禁；生产挂载持久卷并使用 SQLite 在线备份或停服备份 |
| Forge 在 `executing` 状态进程崩溃 | 当前不会重复执行，但需要增加超时回收与人工恢复 runbook |
| Coding Plan 分析/报告阶段延迟波动 | Stage Attempt、lease、timeout 和 202 + polling 已落地；同步接口仅作兼容，渠道主路径不得依赖长连接 |
| `node:sqlite` 在当前 Node 版本仍输出 ExperimentalWarning | 服务最低 Node 22.19，部署锁定已验证 Node 版本；Node 升级必须先跑 SQLite 重启、并发 CAS 和全套回归 |

## 11. 职责迁移门禁

每个 Phase 评审时必须检查：

- 是否出现两个组件都能推进同一状态。
- 是否出现 Pi 和 Forge 都保存同一份可写 pending state。
- 是否出现渠道层自行选择 Skill 或拼装业务 prompt。
- 是否出现 Skill、Renderer 或 Pi 直接访问数据库。
- 是否出现 Forge 在没有 Pi Task 指令时自动发起补查或下游分析。
- 是否为旧主路径定义了 feature flag、兼容期限和退出条件。

若任一答案为“是”且没有明确兼容期，该阶段不能合并到主路径。

## 12. 回滚策略

新链路以 feature flag 启用：

```text
PI_ORCHESTRATOR_ENABLED=false
```

迁移期间：

- Forge 原有 `/api/chat`、Web 查询和飞书路径保持可用。
- 新 TaskRun 与旧 Session 数据分开存储。
- Pi Orchestrator 不修改 Registry 和数据库。
- 新链路故障时渠道回退到原有 Forge 查询入口。
- 只有在垂直切片满足验收标准后，才逐步替换硬编码的 Analysis/Report Stage。

## 13. 第一轮开发清单

按顺序执行：

1. 定义并测试 TaskRun、Clarification、MetricDefinition、QueryResult、Analysis JSON Schema。
2. 为拾穗 DATA 增加 Pi Package manifest，并固定四个首批 Skill。
3. 创建受限 Pi Orchestrator Service 骨架。
4. 实现 `forge_prepare_query` Tool，对接现有 `/api/prepare-query`。
5. 在 Web 中展示 Pi 任务事件和待审核 SQL。
6. 完成 Integration Spike 评审后，再实现内部 QueryRun 审批执行 API。

第一轮完成的判定点是：

> 用户从 Web 输入一个模糊业务问题，Pi 使用拾穗 DATA Skill 完成澄清和指标审查，Forge 返回可审核 SQL；系统全程没有给 Pi 数据库权限，也没有改变现有 `/api/prepare-query` 的安全语义。
