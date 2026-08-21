# Forge AI 数据任务平台架构

> 状态：已确认目标架构 · Last updated: 2026-08-21。本文描述 Pi、Forge、拾穗 DATA Skills 与渠道之间的长期边界，不表示所有模块均已实现。

## 1. 产品定位

Forge 的目标产品形态是面向数据团队的**可信 AI 数据任务平台**。它不只回答“这条 SQL 怎么写”，还要把一个模糊业务问题推进为可审核、可执行、可解释、可交付的数据任务。

整体能力由四层组成：

```text
Web / 飞书 / 钉钉
        │
        ▼
渠道适配层
· 身份映射 · 消息收发 · 卡片审批 · 结果渲染
        │
        ▼
Pi Agent Platform
· 会话 · 任务识别 · Skill 选择 · 编排 · 中断与恢复
        │
        ├──────────────────┐
        ▼                  ▼
拾穗 DATA Skills          Forge Trusted Execution
· 需求澄清                · Registry / Schema RAG
· 指标审查                · Forge JSON / Lint / Compiler
· 专项分析                · SQL 审核 / 只读执行
· 报告与表达              · ACL / Audit / Feedback
```

对外产品仍可统一使用 **Forge** 品牌。Pi 和拾穗 DATA Skills 是内部能力层，不要求最终用户理解其存在。

## 2. 核心原则

1. **Pi 负责任务，不负责数据真相。** Pi 管理会话、路由和工作流，但不持有数据库凭证，也不直接执行 SQL。
2. **Forge 负责可信执行。** 所有数据库访问、语义解析、SQL 编译、权限、审批和审计都经过 Forge。
3. **Skills 负责专业方法。** Skill 定义一类数据任务应收集什么、验证什么、如何分析以及怎样交付，不绕过 Forge 查询数据。
4. **渠道保持轻薄。** Web、飞书和钉钉只处理身份、交互和渲染，不复制业务流程。
5. **高风险操作必须审核。** SQL 执行、指标入库、组织知识提升等动作必须绑定明确的批准人和审计记录。
6. **阶段之间传递 Artifact。** Agent 与 Skill 不通过隐式自由文本耦合，关键阶段输出采用可版本化的结构化数据。
7. **一个任务贯穿一个追踪 ID。** 渠道、Pi、Skill 和 Forge 日志通过 `task_run_id` 与 `correlation_id` 串联。
8. **调度权与执行权分离。** Pi 决定任务何时进入哪个阶段；Forge 独立判断一项数据操作是否允许以及如何安全执行。
9. **Forge 不再自我编排完整任务。** Forge 可以在一次受控能力调用内部完成确定性子步骤，但不能自行选择下一个 Skill、发起补查、切换渠道或推进整个业务流程。

## 3. 控制面与执行面

目标架构采用明确的双层控制：

```text
Pi Control Plane
· 创建 TaskRun
· 选择 Workflow / Skill
· 调度 Stage
· 等待用户输入和审批
· 决定继续、暂停、回退或结束
             │ 受控 Tool / API 调用
             ▼
Forge Execution Plane
· 校验身份、权限和输入契约
· 生成 QueryPlan
· Compile / Lint
· 验证审批
· 只读执行
· 返回事实 Artifact 和审计记录
```

这意味着“基于 Pi 调度执行”不等于“把数据库执行搬进 Pi”：

- Pi 拥有**流程控制权**，没有数据库执行权。
- Forge 拥有**数据操作执行权和否决权**，没有完整任务调度权。
- 用户保留高风险操作的最终批准权。

任何需要多个阶段、等待用户、选择 Skill、补查、重试业务步骤或跨渠道恢复的工作，都由 Pi 管理。Forge 内部只保留一次能力调用所必需的有限重试，例如 Provider 瞬时错误重试或 Forge JSON 编译纠错；这类重试必须有上限、可审计，且不能越过审批边界。

## 4. 四层职责

### 4.1 渠道层

渠道包括 Forge Web、飞书 Bot 和钉钉 Bot，负责：

- 将渠道身份映射为 `org_id / team_id / user_id`。
- 接收文本、文件、按钮和表单输入。
- 展示澄清问题、SQL、表格、图表和报告。
- 收集确认、取消、修改、纠错和追问。
- 传递稳定的 `task_run_id`，支持跨渠道恢复同一个任务。

渠道层不负责：

- 选择业务指标口径。
- 拼接 prompt 或实现分析方法。
- 保存数据库凭证。
- 直接调用数据库。

### 4.2 Pi Agent Platform

Pi 是任务底座和 Agent Runtime，负责：

- 维护当前会话和短期上下文。
- 识别任务类型并选择 Skill 或 Workflow。
- 发起澄清、暂停、恢复、回退和分支。
- 调用受控的 Forge Tools。
- 将上一步 Artifact 交给下一阶段。
- 记录 Stage 状态、耗时、失败和模型用量。

生产运行时只暴露业务所需的自定义工具。默认的 `bash`、`write`、`edit` 和任意文件读取能力不进入客户运行环境。

Pi 不负责：

- 直接连接数据库。
- 绕过 Forge 编译器生成并执行任意 SQL。
- 将未经确认的对话内容提升为组织事实。
- 成为 Registry、审计日志或长期业务记忆的真相源。

### 4.3 Forge Trusted Execution

Forge 是唯一可信执行层，负责：

- 数据源连接与只读账号管理。
- Schema 同步、Registry、业务上下文和 ACL。
- 自然语言意图到 Forge JSON。
- JSON Schema、Lint、Compiler 和方言适配。
- 生成待审核 SQL，并确保审核内容与实际执行内容一致。
- 查询超时、结果行数上限、敏感数据和表权限控制。
- QueryRun、审批、执行、Audit、Feedback 与回放。
- 结构层 Canonical Schema、版本、差异、草案审核与回滚。
- 从同一 Canonical Schema 确定性投影表格、DDL、ER 图和 JSON 视图。

Forge 返回事实型 Artifact，不承担所有业务分析和写作方法。

结构层的多视图不是多真相源：数据库 introspection、DDL import、表格编辑和 ER 关系编辑都只能形成 `RegistryDraft`，经 Schema 校验、确定性 diff 和人工审核后发布为 `RegistryRevision`。DDL 编辑默认只改变 Registry 草案，不直接向数据库执行 migration。ER 中根据命名推断的关系必须保持 `inferred/unconfirmed`，不能冒充真实外键。

### 4.4 拾穗 DATA Skills

拾穗 DATA Skills 是专业方法和交付能力层，负责：

- 需求澄清与验收标准。
- 指标定义和口径审查。
- 归因、漏斗、留存、EDA、A/B 等分析方法。
- 数据质量、血缘、表设计、SQL Review 等工程方法。
- 分析报告、PPT、日报周报和数据文档等表达交付。

Skill 输出必须区分：

- 已知事实。
- 合理推断。
- 未验证假设。
- 待确认问题。
- 建议动作。

需要数据库证据的 Skill 必须通过 Pi 调用 Forge，不允许自己访问数据源。

## 5. 任务与 Artifact 模型

### 5.1 TaskRun

`TaskRun` 是跨层任务的顶级标识，由 Pi 平台创建和编排：

```json
{
  "task_run_id": "tr_01...",
  "org_id": "org_...",
  "team_id": "team_...",
  "user_id": "user_...",
  "channel": "web",
  "intent": "business_root_cause_analysis",
  "status": "waiting_for_query_approval",
  "current_stage": "query_review",
  "created_at": "...",
  "updated_at": "..."
}
```

建议状态：

```text
created
→ clarifying
→ ready_for_query
→ waiting_for_query_approval
→ querying
→ analyzing
→ rendering
→ completed
```

异常和控制状态：

```text
needs_input / incomplete / cancelled / failed / expired
```

### 5.2 核心 Artifact

| Artifact | 生产者 | 主要消费者 | 用途 |
|---|---|---|---|
| `IntentArtifact` | Pi | Skill Router | 任务类型、目标、初始约束 |
| `ClarificationArtifact` | 需求澄清 Skill | Pi、用户 | 已知信息、缺口、验收标准 |
| `MetricDefinitionArtifact` | 指标审查 Skill | Forge、用户 | 指标公式、粒度、窗口、边界 |
| `QueryPlanArtifact` | Forge | Pi、渠道 | Forge JSON、SQL、方言、Registry 版本 |
| `ReviewRequestArtifact` | Forge | 渠道、用户 | 审批对象、SQL hash、过期时间 |
| `QueryResultArtifact` | Forge | 分析 Skill | 列、行、口径、执行元数据 |
| `AnalysisArtifact` | 分析 Skill | 报告/图表 Skill | 证据、洞察、假设、建议、缺口 |
| `ChartArtifact` | 可视化能力 | 渠道、报告 Skill | 图表规格、数据引用、标注 |
| `RenderedOutputArtifact` | 输出 Skill | 渠道 | Markdown、Web、飞书或钉钉表示 |

所有 Artifact 至少包含：

```json
{
  "artifact_id": "ar_...",
  "artifact_type": "analysis",
  "schema_version": 1,
  "task_run_id": "tr_...",
  "producer": "business-root-cause-analysis",
  "created_at": "...",
  "payload": {}
}
```

Artifact 采用 Schema-on-Read，并通过 `schema_version` 保证向后兼容。原始 Artifact 不原地覆盖；修改和重跑产生新版本，保留来源关系。

## 6. 查询审批与信任边界

查询执行必须满足：

```text
用户问题
→ Forge 生成 QueryPlan
→ Forge 生成 ReviewRequest
→ 渠道展示 SQL
→ 用户确认
→ Forge 验证批准信息
→ Forge 使用只读连接执行
→ 返回 QueryResult
```

审批记录至少绑定：

```text
query_run_id
approver_user_id
org_id / team_id
datasource_id
sql_hash
registry_version
approved_at
expires_at
```

以下任一变化都使原审批失效：

- SQL 内容变化。
- 数据源变化。
- 用户或团队上下文变化。
- Registry 版本变化且影响查询计划。
- 审批过期。

Pi 只能提交计划和转交批准结果，不能自行伪造批准，也不能持有 Forge 数据库凭证。

## 7. 身份、权限与租户

渠道身份先进入统一身份映射：

```text
web session / feishu open_id / dingtalk user_id
                    ↓
          org_id + team_id + user_id
                    ↓
       Pi Task Context + Forge ACL Context
```

规则：

- 渠道提供的 `user_id` 不能直接作为可信权限依据。
- Forge 必须校验服务身份和最终用户身份。
- Pi 的服务凭证只允许调用受限业务 API，不代表数据库超级用户。
- 表、字段、数据源和 Registry 的可见性由 Forge 根据租户上下文裁决。

## 8. 状态与记忆归属

| 数据 | 真相源 | 说明 |
|---|---|---|
| 当前对话和推理上下文 | Pi Session | 可压缩、可过期，不作为组织事实 |
| TaskRun 与 Stage 状态 | Pi Task Store | 支持暂停、恢复和渠道切换 |
| QueryRun、SQL、审批、执行 | Forge | 可信查询审计真相源 |
| 结构层 Canonical Schema、DDL/ER 投影、Revision/Draft | Forge Registry | Canonical Schema 是唯一真相源；DDL/ER/表格/JSON 只做确定性投影和受控草案编辑 |
| ER 布局 | Forge Registry UI Metadata | 只保存坐标、分组和视图偏好，不改变表、字段或关系事实 |
| 指标、歧义、字段约定 | Forge Registry | 需要版本、审核和回滚 |
| 组织业务上下文 | Forge Registry / SMP | 确认后才能提升为正式知识 |
| Skill 定义与测试 | 拾穗 DATA 仓库 | 独立版本管理和发布门禁 |
| 最终报告和图表 | Artifact Store | 关联 TaskRun 与数据来源 |

Pi Session 不替代 Forge EMS/SMP/Registry。跨会话长期知识必须经过候选、确认和入库流程。

## 9. 标准业务流程

以“最近两周新用户首购转化为什么下降”为例：

1. 渠道创建用户消息并完成身份映射。
2. Pi 创建 `TaskRun`，选择需求澄清 Skill。
3. Skill 生成 `ClarificationArtifact`；缺少口径时向用户提问。
4. 指标审查 Skill 生成 `MetricDefinitionArtifact`。
5. Pi 调用 Forge `prepare_query`，传入任务 ID、身份和已确认口径。
6. Forge 返回 QueryPlan 和 ReviewRequest。
7. 渠道展示 SQL，用户确认或修改需求。
8. Forge 验证 `sql_hash`、身份和权限后执行。
9. Forge 返回带口径和执行元数据的 QueryResult。
10. 归因分析 Skill 生成证据、假设树和补查建议。
11. 若需补查，创建关联 QueryRun，继续经过审批；不得后台无限循环。
12. 报告 Skill 生成渠道无关的 RenderedOutput。
13. Web、飞书或钉钉适配器完成最终展示。

## 10. 渠道无关输出

Skill 先生成语义结果，不直接拼装具体渠道组件：

```json
{
  "title": "新用户首购转化下降分析",
  "summary": "...",
  "sections": [],
  "tables": [],
  "charts": [],
  "actions": [
    {"type": "approve_query", "query_run_id": "qr_..."}
  ]
}
```

随后由 Renderer 转换为：

- Web 页面或流式 Chat 组件。
- 飞书互动卡片和长文。
- 钉钉互动卡片和 Markdown。

渠道能力不一致时允许降级，但不能改变事实、审批对象和查询状态。

## 11. 可观测性

每次任务应能按 `task_run_id` 查看：

- 原始用户问题和渠道。
- 选择了哪些 Skill 及其版本。
- 每个 Stage 的输入和输出 Artifact。
- Forge QueryRun、SQL hash、审批人和执行状态。
- 模型、Token、耗时和失败原因。
- 用户反馈和后续修正规则。

日志不得记录数据库密码、模型 API Key、渠道密钥或未经授权的完整敏感结果集。

## 12. 当前 Forge 职责迁移映射

目标不是在现有 Forge Pipeline 外再套一层 Pi，而是消除重复调度。当前模块应按下表演进：

| 当前模块/能力 | 目标归属 | 处理方式 |
|---|---|---|
| `agent/pipeline.py` Pipeline 路由与 Stage 推进 | Pi | 在 Pi Task Runtime 稳定后迁移并停止作为主编排器 |
| `agent/agent.py` 通用对话循环、pending state | Pi + Forge QueryRun | 对话和任务状态归 Pi；查询准备、审批状态改为 Forge QueryRun |
| `agent/llm.py` Registry 注入与 Forge JSON 生成 | Forge | 保留为查询规划能力，不承担通用 Skill 路由 |
| `agent/prompts.py` 通用分析/表达 prompt | 拾穗 DATA Skills | 逐步替换为版本化 Skill 和 Artifact Schema |
| `agent/memory` EMS/WMB 会话状态 | Pi | 当前会话、断点和工作记忆迁移到 Pi Task/Session |
| `agent/memory` 已确认业务知识 | Forge Registry/SMP | 保留正式知识，但写入必须经过候选和确认 |
| `agent/knowledge.py` 文档/RSS/URL 收集流程 | Pi | Pi 调度收集与审核；Forge 接收确认后的知识候选 |
| `agent/feishu.py` Bot 对话流程 | 渠道适配层 + Pi | Bot 只做消息与卡片，任务推进由 Pi 完成 |
| `agent/tenant.py` 用户到团队映射 | 身份层 | 渠道完成身份解析；Forge仍独立执行 ACL 校验 |
| `forge/retriever.py`、Registry | Forge | 保留；结构层升级为版本化 Canonical Schema，表格/DDL/ER/JSON 均从它投影 |
| `forge/compiler.py`、`forge/lint.py` | Forge | 保留 |
| `forge/executor.py` | Forge | 保留，且仍是唯一数据库执行入口 |
| `agent/audit.py` 查询审计 | Forge | 保留并增加 `task_run_id` 关联 |
| 图表和报告 Stage 调度 | Pi | Pi 选择 Skill 和顺序；确定性渲染器可以作为受控工具 |
| Web `/api/chat` 任务入口 | Pi | 渐进切换到 Pi Task API |
| Forge `/api/prepare-query` | Forge | 保持查询能力 API，不承担任务编排 |

迁移完成后的硬约束：

- Forge 不根据关键词自行选择 `query / analyze / visualize / report` Pipeline。
- Forge 不在分析结果不足时自行发起下一次业务查询。
- Forge 不直接向飞书、钉钉推进多轮任务。
- Pi 不复制 Registry 检索、Forge JSON 生成、Compiler、Lint 或 Executor。
- 同一职责在目标架构中只能有一个主实现；旧实现只允许作为有明确下线时间的兼容路径。

## 13. 当前实现基础

当前已有基础：

- Forge `agent/pipeline.py`：Pipeline、Stage、Artifact 和断点状态雏形。
- Forge `/api/prepare-query`：外部 Agent 生成待审核 SQL的安全边界。
- Forge Web `/api/chat`、`/api/approve`：内部查询与审批路径。
- Forge Registry、Compiler、Executor、Audit、Feedback 和 Memory。
- 拾穗 DATA 的正式 Skills、示例、测试用例和发布门禁。
- Pi 的 Skills、Extensions、SDK 和 RPC 能力。

目标架构需要新增：

- 独立、受限的 Pi Orchestrator Runtime。
- 稳定的 TaskRun 与 Artifact Contract。
- Pi 到 Forge 的 QueryRun 级审批协议。
- 拾穗 DATA Skills 的 Pi Package 发布方式。
- Web、飞书、钉钉共享的渠道适配接口。

迁移期间不删除 Forge 当前查询主链；新链路先以受控垂直切片并行验证，达到验收标准后再逐步替换硬编码的分析和报告 Stage。
