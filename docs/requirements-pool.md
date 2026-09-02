# Forge 需求池

> 状态：追加式需求与决策真相源 · Last updated: 2026-09-03
>
> 本文件记录所有产品、体验、架构和业务需求，包括未采纳、延期、拒绝和被替代的需求。需求池不等于实施计划；只有经过澄清、评估并由用户确认的需求，才能进入 [`forge-enterprise-evolution-plan.md`](forge-enterprise-evolution-plan.md)。当前状态的简明投影见 [`current-project-state.md`](current-project-state.md)。

## 0. 当前有效需求

- 当前有效产品需求：`REQ-2026-09-03-025`；`REQ-2026-08-25-023` 已吸收为历史短期切口，`REQ-2026-08-26-024` 已作为 Benchmark 证据验证完成。
- 当前阶段：R0 Open-source Trust Runtime Product Cut / Adoption Baseline。
- 当前产品身份：面向企业 Data Agent 的开源可信数据运行时；近期以 Evaluate、Enforce、Explain 三条路径建立开发者采用证据。
- Direct SQL、Forge JSON 和后续 Semantic Query 都是可替换输入；Forge 的产品边界位于生成后的验证、运行时约束、可信执行、Evidence 与 Audit，不再把 Forge JSON 准确率作为产品身份。
- 当前不扩张通用 Product Shell、SaaS Connector、非 SQL Action、Economics/Outcome Ledger 或完整企业治理平台；这些能力须等待开源采用和真实运行证据。
- 当前计划与未关闭验收项以 [`current-project-state.md`](current-project-state.md) 和 [`forge-enterprise-evolution-plan.md`](forge-enterprise-evolution-plan.md) 为准。

## 1. 需求门禁

```text
用户提出需求
→ captured：原样保留意图，不承诺实施
→ clarifying：澄清问题、用户、场景、结果与边界
→ assessed：评估价值、风险、替代方案、依赖和机会成本
→ 用户确认 accept / amend / defer / reject
→ accepted / accepted_with_changes 才能写入 Plan
→ 如改变职责边界，再更新 Architecture
→ planned → implementing → verified
```

### 1.1 状态

| 状态 | 含义 |
|---|---|
| `captured` | 已记录，尚未澄清 |
| `clarifying` | 正在确认真实问题和预期结果 |
| `assessed` | 已完成评估，等待用户决策 |
| `accepted` | 用户确认按原需求采纳 |
| `accepted_with_changes` | 用户确认按修订方案采纳 |
| `deferred` | 需求成立，但当前不实施 |
| `rejected` | 经评估明确不采纳，保留原因 |
| `planned` | 已进入主动 Plan 并有实施门禁 |
| `implementing` | 正在实施 |
| `verified` | 已完成并通过验收 |
| `superseded` | 已被后续需求或决策替代 |

### 1.2 强制规则

1. 新需求可以立即以 `captured` 写入本池，但不能直接写入 Plan 或 Architecture。
2. 在澄清和评估前，不把用户的初始方案等同于正确解法；必须考虑更正、缩小、延期或拒绝。
3. 评估至少覆盖：用户价值、产品公理、职责归属、安全/隐私、复杂度、复用、替代方案、机会成本和不做的后果。
4. 只有用户明确确认后，状态才能进入 `accepted` 或 `accepted_with_changes`。
5. `deferred/rejected/superseded` 永久保留，不删除历史理由。
6. 普通 Bug 若只是恢复已确认行为，可走简化评估；安全事故和数据损坏允许先止损，但必须立即补录。
7. 即使用户要求“直接实现”，也先给出简短评估；用户再次确认后才进入 Plan。
8. Requirement、Plan、Architecture 各有唯一职责：Requirement 保存需求与决策，Plan 保存已批准实施，Architecture 保存稳定职责边界。

## 2. 需求模板

```text
ID / 标题 / 日期 / 状态
原始需求
真实问题与目标结果
目标用户与场景
澄清记录
价值与架构评估
风险与依赖
替代方案
建议结论与可证伪条件
用户确认
关联 Plan / Architecture / 实现 / 验证
```

---

## REQ-2026-08-24-001：Web 对话右侧任务 DAG 与实时任务流

- **提出日期**：2026-08-24
- **当前状态**：`verified`
- **原始需求**：Web 对话页面右侧提供一个 DAG 图和当前正在执行的实时任务流。

### 真实问题与目标结果

用户在对话中看不到系统计划、当前步骤、等待原因、失败位置和剩余交付物，需要跳转 `/tasks`，破坏对话连续性。目标是在不离开对话的情况下理解任务如何被推进，并增强长任务的可控感和可信度。

### 评估

- **用户价值**：高；直接提升 Coordination 可见性、Assurance 透明度和等待体验。
- **战略一致性**：符合 Task/Artifact/Evidence/Decision 协作原则。
- **现有复用**：可复用 Pi `ExecutionPlanArtifact`、`TaskEvent`、`StageAttempt` 和既有 `/tasks` 观察能力。
- **职责边界**：必须是 Pi 真相源的只读 projection；Web 不创建第二状态机，不推进 Task。
- **主要风险**：原始事件过度技术化、右栏挤压对话、DAG 与时间流信息过载、补查 Parent/Child 混淆、高频轮询、内部信息泄露。

### 备选方案

1. 完整原始 DAG + 原始事件：信息过载且存在披露风险，拒绝。
2. 只显示线性进度：无法表达依赖和补查，价值不足。
3. **业务化 DAG + 可折叠实时流**：复用现有真相源并控制复杂度，采纳。

### 用户确认后的方案

- 桌面端 `/chat` 右侧显示业务化 DAG 和可折叠实时任务流。
- 窄屏降级为抽屉。
- DAG 来自最新有效 `ExecutionPlanArtifact`，展示业务标题、依赖和状态。
- 实时流展示有界 TaskEvent/StageAttempt，不展示原始 payload、Prompt、hidden CoT、Secret、内部 hash/path 或完整异常。
- 当前最新或用户选中的 Web Task 为观察焦点；补查 child 可成为执行焦点，但不改写 parent 状态。
- 页面关闭、折叠和切换观察焦点不影响任务执行。

### 用户确认

- **确认日期**：2026-08-24
- **决策**：接受需求池机制；本需求按“业务 DAG + 可折叠实时流、移动端抽屉、默认不展示原始技术事件”方案采纳。

### 关联

- **Plan**：`forge-enterprise-evolution-plan.md` W1。
- **Architecture**：`platform-architecture.md` 渠道层与可观测性边界。
- **实现**：`web/router.py` Web-chat-scoped flow projection；`web/templates/chat.html` 桌面右栏、业务 DAG、实时流和移动抽屉。
- **验证**：Python 全量 `546 passed / 24 skipped`；Pi `88 passed`、typecheck 通过；桌面与 390px 移动端 Playwright 通过，0 console/page error；网站构建通过；`git diff --check` 通过。
- **剩余风险**：当前使用增量 polling 而非推送；最大 12 个 PlanStep 的窄栏布局已受控，未来更大 Work Graph 需重新评估可视化策略。

---

## REQ-2026-08-24-002：修正 M0 Governance Contract 评审阻断项

- **提出日期**：2026-08-24
- **当前状态**：`verified`
- **来源**：M0.2/M0.3 正式代码审查。
- **原始需求**：在进入 M1A 前修复 Governance Coverage、Service Delegation、Task binding、递归委托和 Human Action mandate 语义问题。

### 评估

- **必要性**：高；这些是身份与授权 Contract 的基础语义，带问题冻结 v1 会迫使 M1 PEP 采用错误权限模型。
- **风险**：当前尚无运行时调用方，修订成本低；若延期到 M1 后再改，会增加数据迁移、兼容和安全风险。
- **替代方案**：只在 M0.5 增加语义校验不能解决 Service delegation 无 Contract、`can_delegate=true` 不可表达和 Coverage 指标混淆，拒绝该替代方案。

### 已确认修订方案

1. 将 `Contract Coverage` 与 `Runtime Governance Coverage` 分离，当前不得用字段完整冒充运行时已治理。
2. 将仅支持 Agent 的初版 Mandate 泛化为 task-scoped `DelegatedMandate v1`，同时覆盖 Service/Agent delegate。
3. active Mandate 必须绑定 Task 和 Audience；v1 `can_delegate=false`，递归委托留给有 parent/subset 语义的未来版本。
4. Human 直接 Action 使用 Membership/Role/Policy/Decision；只有 Service/Agent 代表 Principal 行动时才要求 DelegatedMandate。
5. 补齐共享正反 fixture 和 Python/TypeScript parity tests。

### 用户确认

- **确认日期**：2026-08-24
- **决策**：用户在代码审查后确认继续修复，并要求先合并到 `main`、后续直接在 `main` 开发。

### 关联

- **Plan**：`forge-enterprise-evolution-plan.md` M0.2/M0.3 评审修订。
- **实现**：`DelegatedMandate v1`、Governance Action Catalog v1.1.0、Python/TypeScript 共享 fixture 与 parity/引用一致性测试。
- **验证**：Python `548 passed / 24 skipped`；Pi `89 passed`、TypeScript typecheck 通过；npm audit 0 vulnerabilities；JSON、文档和 `git diff --check` 通过。
- **剩余边界**：M0.5 仍需验证跨对象 Organization/Workspace、时间顺序、撤销状态和完整 Query lineage；M1A 未批准，Runtime Governance Coverage 保持 0%。

---

## REQ-2026-08-24-003：完成 M0.5 Contract Review Closure

- **提出日期**：2026-08-24
- **当前状态**：`verified`
- **原始需求**：在不进入运行时授权改造的前提下，完成 Governance Contract 的跨对象语义验证、完整 Query fixture、Threat Model、迁移/回滚设计和正式评审结论。

### 评估与已确认方案

- **必要性**：高；JSON Schema/TypeBox 只证明对象形状，尚不能证明 Principal、Mandate、Policy、Binding、Approval 与 Query lineage 在同一 Task 上连续。
- **职责边界**：本工作包只增加 review fixture、语义验证、测试与文档；不修改 Task API、数据库 Schema、现有 PEP、QueryRun 行为或 OAuth Runtime。
- **范围**：Web Human、Feishu Human 和 Agent 三类完整 review trace；Organization/Workspace、时间、delegation、Task/Audience/Capability/Resource、Policy/Binding、approval/hash lineage 语义门禁；威胁模型；legacy single-user 迁移和 rollback 设计。
- **不做**：不实现 M1A，不把 review fixture 冒充生产 Event/Decision Contract，不提前建设 Coordination/Economics/Context/OAuth Runtime。
- **建议**：M0.5 优先于宽泛的 M0.4 草案；M0.4 保留但不阻塞 M1A，按首次真实消费者 Just-in-Time 细化。

### 用户确认

- **确认日期**：2026-08-24
- **决策**：用户确认按建议顺序执行：先固化当前成果，再完成 M0.5，输出正式 Contract Review verdict，并停在 M1A 授权门前。

### 关联

- **Plan**：`forge-enterprise-evolution-plan.md` M0.5。
- **实现**：`governance-review-fixtures.v1.json`、Python/TypeScript 跨 Contract 语义验证、40 个共享负向 mutation、`governance-contract-review-2026-08-24.md` Threat Model 与迁移/回滚设计。
- **验证**：Python `550 passed / 24 skipped`；Pi `91 passed`；TypeScript typecheck 通过；npm audit 0 vulnerabilities；JSON 和 `git diff --check` 通过。
- **Verdict**：`Approved for M1A proposal`；仅允许提出 M1A 工作包，不代表 M1A 已批准，Runtime Governance Coverage 保持 0%。

---

## REQ-2026-08-24-004：将 NAS Forge 部署更新到当前稳定代码

- **提出日期**：2026-08-24
- **当前状态**：`verified`
- **原始需求**：将 NAS 上的 Forge 部署更新到最新代码，以便用户查看当前 Web Chat 和任务流情况。

### 当前环境与评估

- NAS SSH host `dev` 的部署仓库为 `~/services/forge-m4.1/source`，当前固定在 `3bd20a6`，工作树干净。
- `forge-m41-api.service` 与 `forge-m41-pi.service` 当前均为 user systemd active；Forge 监听 NAS 内网地址，Pi 监听 loopback。
- 本地 `main` 当前为 `c660029`，相对 NAS 增加企业演进/Governance Contract baseline、Web 任务 DAG/实时流、Contract 评审修订和 M0.5 review-only 语义门禁。
- 只读预检显示 NAS 当前无 `running` StageAttempt；仍有等待审批、待分析、待报告和 needs-input Task，重启不得推进或重放这些任务。
- Governance Contract 与 M0.5 validator 当前无生产调用方，部署后的可见行为变化主要是 `/chat` 右侧只读任务视图；不涉及数据库迁移或凭证修改。

### 建议部署方案

1. 部署前检查无 running 高风险 Action，并备份状态库/配置元数据；不读取或复制 Secret 内容。
2. 未获 push 授权前，通过临时 Git bundle 将已提交的本地 `main` fast-forward 到 NAS，保持 NAS Git 历史和明确 rollback commit，不用 rsync 覆盖源码。
3. manifests 未变化时不重装依赖；重启 Forge/Pi user services。
4. 验证 API/Pi health、认证登录、`/chat`、Web ChannelEvent 与 `/flow` 权限/去敏；不自动执行客户 SQL。
5. 失败时回滚到 `3bd20a6` 并重启；状态数据不随代码 rollback 覆盖。

### 风险与门禁

- 重启可能中断正在运行的 Stage；部署前必须确认空闲或等待 lease 安全收口。
- 不修改 NAS mode-600 env、Identity Map、模型凭证、数据库 URL 或 Registry 数据。
- 不 push GitHub；若用户希望远端也同步，需要另行明确授权。

### 用户确认与实施

- **确认日期**：2026-08-24
- **决策**：`accepted_with_changes`——用户确认按“Git bundle fast-forward + 状态备份 + 空闲检查 + health/Web smoke + 可回滚、不 push”方案部署。
- **实施状态**：已完成。用户已通过认证后的 `/chat` 发起并观察真实任务；部署链路、DAG 与实时流可用。观察中发现的 Analysis 延迟作为独立 `REQ-2026-08-24-005` 处理，不回滚本部署需求。

### 部署结果（2026-08-24）

- 部署前 NAS 为 `3bd20a6`、工作树干净、Forge/Pi active、`running` StageAttempt 为 0；7 个可变 SQLite Store 使用 online backup 写入 NAS mode-700 目录 `~/services/forge-m4.1/backups/deploy-20260824T074830Z/`，配置只记录文件名/mode/size，不复制或读取 Secret 内容。
- 未 push GitHub；通过临时 Git bundle 将 NAS `main` fast-forward 到 `e4e3cb0`，bundle 随后从 NAS `/tmp` 删除。依赖 manifest 未变化，没有重装依赖或修改 env、Identity Map、Registry、数据库 URL/凭证。
- `forge-m41-api.service`、`forge-m41-pi.service` 重启后均 active；Forge `/health`、Pi live/readiness 均为 `ok`。
- NAS 是固定内网 HTTP 的 dev profile：Forge dev readiness 为 `warn`，唯一原因是 `AUTH_COOKIE_SECURE=false`；这是当前无 HTTPS 的内网部署所需设置。prod profile 会对此返回 fail，不能把该 NAS 状态声明为合格公网生产部署。
- 匿名 smoke：`/ → /chat`，`/chat` 与 `/tasks` 跳转登录，`/login` 200，未认证 `/flow` 返回 401。未使用、读取或回显管理员密码；认证后的 DAG/实时流由用户登录后完成最终视觉确认。
- 回滚点保持 `3bd20a6`；本次未发生回滚，等待审批/分析/报告的既有 Task 未被推进或重放。

---

## REQ-2026-08-24-005：修复 Analysis Stage 临界超时与“假死”体验

- **提出日期**：2026-08-24
- **当前状态**：`verified`
- **原始需求**：NAS 最新 Web 任务在分析阶段长时间没有变化，看起来一直卡在同一个位置；用户确认修复。

### 诊断证据

- 最新 Task 的 `business_root_cause_analysis` 用时 `229.106s` 后成功，距离 `240s` Stage timeout 仅约 11 秒；前一条同输入任务在 `240.051s` 超时。
- 当前 revision `sha256:f6f3…` 最近五次 Analysis 为：124s 成功、229s 成功、240s 超时、两次约 29s 未提交 Artifact；此前 revision 多为 43–136s。
- 两次慢任务输入相同，仅 107 行/3 列、QueryResult JSON 约 4.2KB；不是数据库或结果集过大。检查期间 Provider 连接持续接收数据，Pi/Forge 进程与 SQLite 正常。
- 直接原因是 Analysis 没有独立 ActiveModelBinding，回退到全局 `volcengine-coding-plan/ark-code-latest`；模型 Tool submission 延迟不稳定。UI 在 `attempt_started` 与终态之间没有可见 elapsed/deadline，因而表现为假死。

### 评估与已确认方案

1. **Artifact-first 适配**：生产 Analysis Adapter 明确覆盖 Skill 中面向人工阅读的 Markdown 输出示例，要求模型不要先写正文，直接把方法映射到唯一 `submit_analysis_artifact`；限制 finding/hypothesis/suggested-query 数量，减少无效长输出。
2. **失败分类**：Pi SDK `prompt()` 的 Provider 失败可能只进入 session state 而不 reject；Adapter 必须提取有界 `quota/rate_limit/auth/provider/context/aborted/unknown` 类别，禁止把 Provider 错误误报成“未提交 Artifact”并继续无效 correction。
3. **可观测性**：StageAttempt 增加兼容可空的 `deadline_at / progress_phase / first_model_activity_at / tool_submitted_at`，只记录时间与阶段，不记录 Prompt、模型正文或 hidden CoT。
4. **真实进度体验**：Web 根据服务端 started/deadline 显示 elapsed、剩余安全窗口和慢响应提示；只展示业务阶段名，不伪造百分比、不新增 Task 状态或心跳事件流。
5. **Binding 门禁修正**：通用 Tool capability gate 不能证明复杂 Analysis Artifact Tool 可用。两个通用 gate 通过的候选在真实 Analysis smoke 中均未提交 Artifact，已自动回滚；在新增 `analysis_artifact_gate` 前不得激活独立 `pi.analysis` Binding，也不得把 generic gate 冒充场景门禁。
6. **失败边界**：超时继续回到 `analysis_retry`，不重放 SQL；当前兼容路径继续固定实际 model revision，不自动切换候选。

### 用户确认

- **确认日期**：2026-08-24
- **决策**：用户在查看诊断后明确要求修复。

### 实施发现

- 首个 `deepseek-official/deepseek-v4-flash` 候选 generic capability gate 通过，但 Pi credential 不可用，未进入真实任务并回滚。
- 第二个 `openai/deepseek-v4-flash` 和受限输出的 `volcengine-coding-plan/ark-code-latest` 均通过 generic Tool smoke，但真实 `submit_analysis_artifact` smoke 未提交 Artifact；所有 Binding、catalog 和 NAS 代码已恢复到部署前 `e4e3cb0`，服务健康、无 SQL/Task 重放。
- 该结果否证“通用 Tool smoke 足以批准 Analysis Binding”，修复方向改为 Artifact-first adapter、Provider failure 分类、真实进度和场景专用 gate；不能为了满足计划而强行激活失败候选。

### 实施与验证结果

- Analysis Adapter 增加 Artifact-first 映射：Skill Markdown 仅作方法参考，模型直接调用终止型 Tool；findings/hypotheses/suggested queries 分别限制为最多 6/4/5 条。
- Pi SDK session error 现在分类为 `quota_exhausted / rate_limited / authentication_failed / context_limit / provider_unavailable / aborted / unknown_provider_error`；Provider 失败不再被误报为 Artifact omission，也不再发起无效 correction。
- StageAttempt 新增向后兼容可空 deadline/progress 时间字段；旧 SQLite user_version 不变。Web 显示业务阶段、elapsed、剩余安全窗口和 60s 慢响应提示，不记录或展示 Prompt/模型正文。
- 通用 gate 通过但真实 Analysis smoke 失败的候选均已回滚，NAS 保持无 `pi.analysis` Binding 和原 model catalog；证明 generic gate 不能替代 `analysis_artifact_gate`。
- 原兼容模型在新 Adapter 下的隔离、无 SQL 真实 smoke：2 行输入 `33.292s` 完成，107 行/3 列输入 `119.232s` 完成；均提交合法 Artifact，并观察到 `model_responding → artifact_submitted`。对比修复前同规模 `229.106s` 成功/`240.051s` 超时，已退出临界超时区，但大结果分析仍是剩余性能风险。
- 自动验证：Python `550 passed / 24 skipped`；Pi `93 passed`、TypeScript typecheck 通过、npm audit 0 vulnerabilities；Web 定向测试与桌面 Playwright 通过且 0 console/page error；NAS `45fcc87` Forge/Pi health/readiness 均正常。
- **剩余边界**：不宣称任意 107 行分析都稳定低于 120s；后续独立 Binding 必须先通过真实 Analysis Artifact 场景门禁。当前修复不重放 SQL、不自动切模型、不修改 Secret。

### 关联

- **Plan**：`forge-enterprise-evolution-plan.md` H1。
- **NAS backup**：`~/services/forge-m4.1/backups/h1-analysis-v2-20260824T083911Z/`。
- **UI 验收图**：本地 `/tmp/forge-analysis-progress.png`。

---

## REQ-2026-08-24-006：对话与报告的长文本可读性和语义化强调

- **提出日期**：2026-08-24
- **当前状态**：`verified`
- **原始需求**：像截图中的长文本，应通过加粗、下划线、斜体、强调色或 callout 提升阅读体验；不仅用于对话，也用于报告。

### 真实问题与目标结果

当前 Web Chat 将知识回答压成“summary + 同级 bullet”，浏览器 Markdown renderer 只支持链接、一级列表和代码块：`**强调**`、标题、编号/嵌套列表、inline code 与 blockquote 都不会形成视觉层级。截图中的默认口径、适用场景、歧义、字段和澄清要求因此混在同一阅读平面。

业务报告虽然已有固定章节，但 executive summary、结论、建议和限制仍主要依赖同质卡片/列表；PPTX 也以普通大段 bullet 为主。目标不是“让模型自由装饰文本”，而是让用户能快速扫描：定义、事实、结论、风险/歧义、限制、建议和下一步在所有输出渠道中有稳定层级。

### 两个子需求

1. **对话长文本**：支持安全、受限的富文本层级，包括 H2/H3、加粗、斜体、inline code、链接、编号/嵌套列表、引用，以及 `info / success / warning / limitation` 语义 callout；改善行宽、段距、列表缩进和移动端排版。
2. **业务报告长文本**：Web 报告与 PDF 使用同一语义化视觉系统；executive summary、关键发现、建议、限制/风险、证据说明映射为固定组件和强调色。PPTX 至少保持相同的信息优先级与风险/建议区分，不退化为整页同级长 bullet。

### 评估

- **用户价值**：高。直接降低指标口径、分析结论和报告的扫描成本，也减少重要歧义与限制被漏读的风险。
- **架构一致性**：Renderer 负责渠道表示，Artifact 保持事实与语义真相；Web/PDF/PPTX 不应各自发明结论或改变证据边界。
- **安全与可信边界**：不得允许模型输出任意 HTML、CSS、颜色或脚本。Markdown 只实现安全子集，使用 DOM `textContent`/属性白名单生成节点；callout 和强调色由服务端/Renderer 根据结构化语义决定。
- **可访问性**：普通下划线容易与链接混淆，因此不建议作为任意强调方式；下划线只保留给链接。强调使用字重、斜体、左边框、背景和可读标签，并验证对比度、键盘、打印及 reduced-motion。
- **现有复用**：Chat 可扩展现有无依赖 `renderMarkdown`；报告可复用 `RenderedOutputArtifact / AdvisoryArtifact / AnalysisArtifact` 已有 `summary/findings/recommendations/limitations/open_questions/confidence/priority`，以及 `_business_html` 的确定性投影和 PDF 打印链路。
- **关键缺口**：截图中的知识回答目前只投影 advisory 的 summary/findings，忽略 recommendations、assumptions、limitations、open questions 与 deliverables；仅补 CSS 无法恢复这些语义。需要先完善 Renderer 映射，再做样式。
- **复杂度**：中。Chat 安全 Markdown 与语义投影约为一个垂直切片；Web/PDF 报告可共用 HTML；PPTX 需独立验证分页、溢出和字体，不应假设 CSS 自动同步。

### 备选方案

1. **允许模型直接输出 HTML/颜色**：表达自由，但存在注入、品牌漂移、可访问性和跨渠道不一致，拒绝。
2. **仅增强 Markdown 语法和 CSS**：改动小，但 Artifact 中已有的限制、建议等语义仍会丢失，且 callout 只能靠文本约定猜测，不足。
3. **语义块优先 + 安全 Markdown 子集**：Renderer 将既有结构化字段投影为固定章节/callout；字段内部再支持安全 inline Markdown。兼顾可信、可读和跨渠道一致，建议采纳。
4. **立即新增通用 RichText Artifact DSL**：长期最完整，但当前只有 Web Chat 与 Report 两个消费者，过早冻结新 DSL；本轮不建议。

### 建议方案与验收边界

- 采用方案 3，分成同一需求下两个可独立验收的切片：`R1 Chat readability`、`R2 Report readability`。
- 第一轮不新增任意 HTML，也不让模型选择颜色；不使用普通下划线强调。
- Chat 先完整投影 advisory/analysis 的语义字段，并实现安全 Markdown 子集与 callout 组件；报告再复用同一 design token 和语义映射到 Web/PDF，最后验证 PPTX。
- 至少用截图对应的指标口径长文、分析长文、限制/歧义 callout、移动端、打印/PDF 和一份 PPTX 做视觉回归；同时测试 HTML/script 被当作纯文本、外链安全属性和既有 SQL code block 不回归。
- **可证伪条件**：若视觉层级只能靠 Renderer 猜关键词，或 Web/PDF/PPTX 同一语义产生冲突表达，则暂停扩展并重新评估版本化 Presentation Block Contract，而不是继续堆正则。

### 用户确认

- **确认日期**：2026-08-24
- **决策**：`accepted_with_changes`。用户确认按建议边界实施：报告覆盖业务 Web、PDF 与 PPTX，技术报告只做基础排版；普通下划线不作为任意强调，颜色/callout 由结构化语义和 design token 决定；按 `R1 Chat → 视觉确认 → R2 Web/PDF/PPTX` 顺序推进。

### R1 实施状态（视觉确认通过）

- Channel Renderer 已完整投影 Advisory 的 summary、findings、recommendations、assumptions、limitations、open questions 和 deliverables；Analysis 的方法、结论和限制使用同一层级，技术字段自动进入 inline code，结构化字段不能注入 block-level callout。
- Web Chat 无新增依赖地支持安全 H2/H3、strong、emphasis、inline/fenced code、站内/HTTP(S) 链接、ordered/unordered/nested list 和固定标签 callout；所有节点通过 DOM `textContent`/安全属性创建，任意 HTML/script 作为纯文本。
- 全局移动导航改为可关闭抽屉，390px Chat 不再被固定侧栏挤压；链接是唯一普通下划线，callout 使用 `info/warning/limitation/success` design token。
- 验证：Python `550 passed / 24 skipped`；Pi `94 passed`、TypeScript typecheck、npm audit 0 vulnerabilities；Web 定向 `16 passed`；桌面/390px Playwright 0 console/page error、无横向溢出，script fixture 未执行。
- **视觉候选**：`/tmp/forge-chat-readability-desktop.png`、`/tmp/forge-chat-readability-mobile.png`。
- **视觉确认**：用户于 2026-08-24 确认 R1 视觉方向并要求继续 R2。R1 门禁通过。
- **后续范围修订**：用户在 Golden Journey 评审期间明确当前产品暂不考虑移动端页面。已实现的响应式能力保留，但不再作为当前产品验收门禁，不继续为移动端追加需求或修复。

### R2 实施与验证结果

- 业务 Web 报告采用确定性 editorial hierarchy：深色 Executive Summary、方法范围、编号发现卡、confidence/evidence 标签、图表、数据表、priority 建议卡、下一步和 limitation 风险区；模型仍不能输出任意 HTML/CSS 或改写证据。
- PDF 与 Web 使用同一 HTML/CSS，增加 A4 print color、可分页 section/card/table 规则和重复表头；Playwright 实际生成 `424KB` A4 PDF，toolbar 在打印媒介隐藏。
- PPTX 使用固定 16:9 design token；摘要、方法、发现、图表、建议、限制和下一步各自分页。长标题、图表标题和正文按有界片段拆页，测试确认 300 字发现及 220/180 字标题内容未丢失，单 text shape 不超过 160 字；confidence/priority 使用文字与颜色双编码。
- 技术报告只改善 heading、SQL/code、table、长字段换行、移动和打印排版，没有加入业务化 callout。
- 安全与兼容：HTML 全字段 escape；恶意 `<img onerror>` 作为文本；Report Bundle、分享 ACL、下载审计、idempotency、forbidden reasoning 和文件 mode 未改变。已发布不可变报告不会被原地重写，新样式只用于新生成 revision。
- 验证：Python `551 passed / 24 skipped`；Pi `94 passed`、TypeScript typecheck、npm audit 0 vulnerabilities；报告专项 `7 passed`；Web/移动/print/technical Playwright 0 console/page error、0 横向溢出；PPTX 构建和 Quick Look 封面通过。
- 视觉与产物：`/tmp/forge-report-readability-desktop.png`、`/tmp/forge-report-readability-mobile.png`、`/tmp/forge-report-readability-print.png`、`/tmp/forge-report-technical.png`、`/tmp/forge-report-readability.pdf`、`/tmp/forge-report-readability/artifacts/rp_visual001/v1/report.pptx`。
- **剩余部署边界**：本地无系统级 `google-chrome/chromium` 命令，ReportStore 内置 PDF subprocess 未在本机直接执行；已用同一 Chromium print engine 的 Playwright 生成并验证 PDF。

### NAS 部署确认

- **确认日期**：2026-08-24
- **用户决策**：明确要求部署当前 R1/R2 到 NAS。
- **部署方案**：沿用已验证的 Git bundle fast-forward；部署前确认无 running StageAttempt 并在线备份可变 SQLite；不读取或修改 Secret、Identity Map、Registry、数据库 URL/凭证；不重装未变化依赖；重启 Forge API/Pi 后验证 health/readiness、认证门禁和目标机隔离报告 HTML/PDF/PPTX exporter。
- **回滚**：代码回滚到 NAS 当前 commit；状态库只在部署异常且确有必要时使用部署前备份，不自动推进、重放 SQL 或覆盖现有不可变报告。
- **部署结果**：部署前 NAS `caa8b69`、工作树干净、Forge/Pi active、running StageAttempt=0；10 个可变 SQLite 使用 online backup 保存到 `~/services/forge-m4.1/backups/readability-20260824T094102Z/`。
- 未 push 远端；通过临时 Git bundle fast-forward 到 `9fca1ea`，bundle 已从本地/NAS `/tmp` 删除。依赖 manifests 未变化，未重装依赖，未读取或修改 Secret、Identity Map、Registry、数据库 URL/凭证。
- Forge API/Pi 重启后 active，Forge health 与 Pi readiness 均为 `ok`；匿名 `/chat`=302、匿名 `/flow`=401；部署后 running StageAttempt=0、NAS 工作树干净。
- NAS 使用 `/usr/bin/google-chrome` 完成无客户数据的隔离 ReportStore smoke：HTML `published`、PDF `ready`（468,786 bytes）、PPTX `ready`（42,333 bytes）；临时目录退出后自动删除，无 SQL/Task 重放。
- **回滚点**：`caa8b69`；本次未触发回滚。当前需求恢复为 `verified`，认证后的 Chat/报告业务视觉可由用户继续人工观察。

### 关联

- **Plan**：`forge-enterprise-evolution-plan.md` H2。
- **实现**：R1 `7a88c70`；R2 `06ccb0d`。
- **Architecture**：不改变职责边界；继续由渠道 Renderer 和确定性 Report Renderer 负责表示，Artifact/Pi/Forge 真相源不变。

---

## REQ-2026-08-24-007：完整问数旅程的物理链路与逐阶段视觉验收

- **提出日期**：2026-08-24
- **当前状态**：`verified`
- **原始需求**：不能只验证局部 fixture 或物理执行；需要跑通一个完整问数流程，同时从前端逐步检查每个环节是否符合预期、是否能让用户满意，并实际使用视觉能力和自动化测试能力。

### 问题确认

该批评成立。此前验证分别覆盖了 Chat 长文本 fixture、隔离 ReportStore、Provider/Artifact smoke、状态/API 自动测试和 NAS health，但没有形成一条“同一个用户问题、同一个 TaskRun”贯穿以下全部节点的认证后浏览器证据：

```text
提出业务问题
→ 计划与进度
→ SQL review
→ 人工批准
→ 只读执行
→ 查询结果
→ Analysis
→ 报告生成
→ Web/PDF/PPTX
```

因此此前能证明组件和局部边界可用，不能充分证明完整旅程的状态衔接、等待体验、动作可理解性和最终交付体验均令人满意；不得把局部 Playwright 截图冒充端到端产品验收。

### 建议测试方案

采用“真实链路 + 隔离副作用 + 逐阶段视觉审查”的 Acceptance Journey，而不是在生产认证上开后门：

1. **隔离环境**：在 NAS 仅监听 loopback 的临时目录启动当前 commit 的 Forge/Pi/Web；使用版本化只读测试数据库、独立 Task/Query/Report Store 和测试 Principal。AUTH 只允许在该临时 loopback Web 中关闭，通过 SSH tunnel 供本地 Playwright 使用；生产 Web 认证不变。
2. **真实能力**：优先使用当前真实模型 runtime 的既有 secret reference，由进程环境引用但不读取/回显 Secret；真实执行 Skills、Forge Assurance、Compiler、SQL review、只读测试 SQL、Analysis 和 Report exporter。模型不可用时记录为真实失败，不用 deterministic fake 冒充通过；可另跑 deterministic control 以区分基础设施与模型问题。
3. **一个 Golden Journey**：问题固定为测试 Registry 可复算且能产生查询、分析和报告的业务问题；Playwright 以明确 test principal 驱动一次 SQL approval。该批准只授权隔离测试数据上的一次只读 SQL，不代表生产授权。
4. **逐阶段证据**：每个状态保存 screenshot、DOM/ARIA 摘要、Task/Event/Attempt/Artifact/QueryRun 有界快照和耗时；不得保存 Prompt、模型正文、hidden CoT、Secret、原始服务凭证或无关完整结果集。
5. **视觉审查**：使用视觉模型逐张检查桌面端的信息层级、下一步是否明确、风险/审批是否醒目、等待是否可理解、错误是否可恢复，以及表格/代码/长文本是否易读；同时用 Playwright 断言 action、焦点、ARIA、console/page error。移动端不属于当前验收范围。
6. **物理链路审查**：验证同一 TaskRun 的 PlanStep、StageAttempt、QueryRun、SQL/Assurance hash、批准、执行次数、QueryResult evidence、Analysis/Report lineage 和 Publication 一致；重复点击不得重复执行 SQL，页面轮询不得推进状态。
7. **结果产物**：输出逐阶段 Pass/Fail/Blocked 旅程报告、截图 contact sheet、状态时序、发现清单和 P0/P1/P2 修复建议。视觉不满意即记为产品失败，不能因后端状态成功而判通过。

### 风险、替代方案与边界

- **直接读取 NAS 管理员密码/cookie**：违反 Secret 边界，拒绝。
- **临时关闭生产认证**：会制造安全窗口，拒绝。
- **只用 fake model 跑 E2E**：可验证状态机但不能代表真实等待、Tool submission 和输出质量，不能作为最终通过。
- **直接污染生产 Task/Query/Audit Store**：会把测试记录混入正式真相源；首选独立 Store。若环境变量无法安全隔离，应暂停，不靠清理审计记录补救。
- **成本与耗时**：真实模型至少经历查询规划/分析/报告等调用，可能需要 5–15 分钟并消耗当前模型额度；只能设置有界一次主旅程，不做无限重试。
- **验收边界**：一条 Golden Journey 只能证明代表性主链，不证明所有业务问题均正确；后续至少还需 needs-input、取消/拒绝、超时/重试三个 edge journey，但应在主链暴露的问题修复后再扩展。

### 建议结论

建议 `accepted_with_changes`：先执行一条隔离但真实的 NAS Golden Journey，逐阶段做物理与视觉双验收；发现问题先进入需求池，不在测试脚本里掩盖。主链稳定后再提出 edge-journey 工作包。

### 用户确认

- **确认日期**：2026-08-24
- **决策**：`accepted_with_changes`。用户明确确认执行 Golden Journey，并接受以下有界授权：NAS loopback 临时隔离环境、现有模型 credential 引用、版本化只读测试数据库、独立状态库、test principal 自动批准并执行一次测试 SQL；不读取 Secret、不修改生产认证、不访问生产数据库、不写生产 Task/Audit Store。
- **范围修订**：当前产品暂不考虑移动端页面。H3 只以桌面端作为产品 Pass/Fail 门禁；已采集的移动截图只保留为非门禁诊断证据，不进入 finding 优先级和修复范围。

### 关联

- **Plan**：`forge-enterprise-evolution-plan.md` H3。
- **Architecture**：不改变 Pi/Forge/渠道职责；测试环境只验证现有 Contract、状态和 Renderer，不成为第二套生产状态机。
- **执行结果**：同一 TaskRun 完成 Query→Approval→Execution→Analysis→Report，物理不变量全部 PASS；桌面产品旅程与可信业务结果 FAIL。Analysis 183.265s，完整 Task 349.028s；PDF/PPTX 均 ready，重复 ChannelEvent 未重放 SQL。
- **正式评审**：[`golden-journey-acceptance-2026-08-24.md`](golden-journey-acceptance-2026-08-24.md)。Verdict：`Physical chain PASS / Trusted product outcome FAIL`。
- **证据**：NAS `~/services/forge-m4.1/e2e/golden-20260824T101129Z/`；本地 `/tmp/forge-golden-journey-evidence/`。隔离服务已停止，临时 service/channel keys 已删除，生产 Forge/Pi health 正常。

---

## REQ-2026-08-24-008：关闭 Golden Journey 的 P0 可信交付缺陷

- **提出日期**：2026-08-24
- **当前状态**：`verified`
- **来源**：`REQ-2026-08-24-007` Golden Journey 正式评审。
- **原始需求**：完整物理链路虽成功，但当前产品仍可能交付泄漏内部路径的 PDF、误导性 Chart，以及在真实同页操作中看不到报告完成卡片；必须在扩展 edge journey 前关闭。

### P0 范围

1. **PDF 内部路径泄漏**：NAS 实际 PDF 的默认 Chrome footer 暴露 `file:///home/.../index.html`、服务目录、报告 ID/revision，并带浏览器日期/标题页眉。必须关闭默认页眉页脚，并对真实目标 exporter 做内容级负向回归，不能只检查 status/size。
2. **同页报告完成卡片不可见**：用户在长 Analysis 底部 focus/click 报告 action 后，Report running 与 Publication complete 的主区为空；刷新后 Artifact/链接存在。需要修复 Chat/Flow 的 viewport scroll containment 和短卡片替换后的滚动锚点，并以同一卡片连续路径回归。
3. **ChartArtifact grain 误导**：`buildChartPayload` 从 sample 自动选首个字符串维度/数值列，不验证唯一标签、业务 grain 或聚合；实际 107 行只有 10 个可见品类标签，报告图表却表现为品类排名。重复 label 时必须使用稳定 key、确定性聚合或抑制图表；Critical Data Quality 不能继续发布误导图。

### P1 后续切片

- Critical Data Quality 的 decision-readiness 门禁；当前虽然在 prose 写了限制，Analysis/Report 仍标记 complete。
- SQL Review 增加“修改需求/重新生成”路径、业务解释、数据范围和风险摘要，但继续禁止批准后直接编辑 SQL。
- QueryResult 增加业务列名、单位、格式和异常标识；科学计数/超大值在 Analysis 前可见。
- 主进度卡显示真实阶段、elapsed/deadline 含义、可离开提示和下一交付物；不伪造 ETA/百分比。
- 长 Analysis 的下一 action 可发现；报告风险前置、Executive Summary 分块、桌面锚点/明细折叠；PPTX 封面不再截断摘要。

### 评估

- **用户/安全价值**：P0。PDF 路径泄漏是明确安全边界违反；Chart 误导和完成卡不可见直接破坏可信交付。
- **职责**：PDF 属 Forge deterministic Report Renderer；Chart grain 属 Pi deterministic Chart builder + Report projection；同页可见性属 Web Renderer/scroll ownership。不得通过模型 prompt 猜测修复。
- **建议顺序**：P0-A PDF leak → P0-B same-page completion → P0-C Chart grain/quality gate；三者各自有最小回归，再重跑同一 Golden Journey。P1 另按门禁拆分，不与 P0 混成大重写。
- **不做**：当前不处理移动端；不改变 Pi/Forge 边界；不修改生产认证/数据库；不直接编辑已发布不可变报告；不因测试数据异常而美化/隐藏原始 QueryResult。
- **可证伪门禁**：真实 NAS PDF 不含 `file://`/`/home/`/浏览器默认 header；same-page 长 Analysis→Report→Publication 无刷新可见；重复可见维度的 Chart fixture 被聚合、加 key 或拒绝，绝不静默画前 N 行。

### 决策与结果

用户于 2026-08-24 明确确认修复 Golden Journey P0。已按 P0-A PDF leak → P0-B same-page completion → P0-C Chart grain/quality gate 的顺序完成并部署；P1 继续保留在需求池，没有自动扩大本轮范围。

- Python `553 passed / 24 skipped`；Pi `96 passed`；typecheck、npm audit、桌面 Playwright 和实际 NAS Chrome PDF 内容扫描均通过。
- 同一 Golden Journey 重跑 262.399s 完成，1 次审批/1 次执行，重复消息未创建第二个 QueryRun；Report/PDF/PPTX ready，数据源保持 mode `0400` 且无 WAL/SHM。
- Report running 与 Publication complete 同页可见；PDF 不再包含 `file://`/`/home/`/默认 header；重复品类 label 使 ChartArtifact fail-closed 为 0，不再发布误导图。
- 正式证据：[`golden-journey-p0-closure-2026-08-24.md`](golden-journey-p0-closure-2026-08-24.md)。

---

## REQ-2026-08-24-009：专业报告的多图叙事、现代图表与证据绑定交互

- **提出日期**：2026-08-24
- **当前状态**：`editorial_report_revision_active`
- **原始需求**：专业报告目前过于模板化，图表数量少、样式不够现代、交互有限；图表需要有标注，让图更生动、更有业务价值。

### 第一性原理评估

- **需求成立，但“更多图表”不是独立价值目标**。一张图必须回答一个不同的决策问题；把同一组数据重复画成柱图、折线图和饼图只会制造视觉噪声与虚假丰富度。
- **当前 P0 fail-closed 不能回退**。维度 grain、聚合、单位或 Evidence 不可靠时，宁可不画图；现代样式和动画不能掩盖错误数据。
- **模型不能自由输出 HTML/CSS/脚本/颜色**。Pi/Skill 只能提交受 Schema 约束、Evidence-bound 的 Chart/Annotation proposal；Forge deterministic Renderer 决定布局、视觉 token、交互和 PDF/PPTX 降级。
- **媒介需要分层**：HTML 可以支持 hover/focus tooltip、系列显隐、图表/明细切换、证据定位和有界筛选；PDF/PPTX 必须投影为静态、完整、可理解的注释，不能依赖 hover 才能读懂。
- **标注必须有来源**：Top/Bottom、异常、拐点、目标线、同比差异和关键贡献可由确定性规则生成；业务解释标注必须引用 Analysis finding 和 QueryResult evidence，不能由 Renderer 创造结论。

### 建议产品切片

1. 定义 `ChartArtifact v2`：增加 `purpose/grain/encoding/series/transform/annotation/evidence/quality_status`，只支持有界类型和确定性变换。
2. 引入“图表计划”而不是固定一个图：根据 QueryResult + Analysis 选择 1–4 个互补视图，例如排名、趋势、结构占比、差异/贡献；数据不支持时允许 0–1 个，不凑数量。
3. 建立统一 Chart Design System：编辑式排版、业务友好单位、直接标签、克制配色、可访问对比度和低噪声网格；禁止 3D、装饰性渐变和无意义动画。
4. HTML 增加渐进式交互，且键盘/ARIA 可用；默认首屏已经完整，不把核心结论藏在 tooltip。
5. Annotation 与 finding/evidence 双向定位；技术报告记录 annotation rule、source rows 和 transform lineage。
6. PDF/PPTX 从同一 ChartArtifact v2 确定性投影，保持数据、标注和结论一致；旧 Report revision 不重写。

### 验收与反证

- 每张图必须声明决策目的、grain、单位、变换和 Evidence；无法声明则拒绝生成。
- 同一数据不得仅为增加数量重复成多种图型；每张图需有非重复问题和阅读结论。
- HTML 交互在禁用 JS、键盘操作和打印时仍保留核心信息；PDF/PPTX 不丢关键 Annotation。
- 图表值、标签、排序、Top-N/Other 聚合与 QueryResult 可复算一致；重复 label、截断结果、Critical Quality 必须失败关闭或显式降级。
- 用至少两个真实场景证伪“固定模板”：横截面品类比较 + 时间趋势/多系列结构。单场景不能证明通用 Chart Planner。

### Skills 与 Prompt 同步边界

- 当前生产 `business-root-cause-analysis` 与 `data-analysis-report-writer` Skills 仍固定在已发布 Skills package revision；Chart v1 主要由 Pi deterministic builder 从 QueryResult 生成，并不是现有报告 Skill 自由生成图表。
- **R0 视觉候选阶段不修改生产 Skills/Prompt**：候选由固定 fixture + ChartArtifact v2 Contract + deterministic candidate renderer 生成，避免视觉尚未确认时让生产模型输出和 Artifact 漂移。
- **R1 进入生产前必须同步修改并固定 revision**：分析 Skill 增加 chart-worthy finding、quality status 和 evidence-bound annotation candidate；报告 Skill/Tool 增加非重复 decision question、Chart Story Plan 与跨媒介叙事顺序；Pi Structured Artifact Tool、`skill-executor` 约束、Skills package revision、Compatibility Gate、负向 fixture 和 Renderer 必须同一版本门禁通过。
- Prompt/Skill 只决定结构化语义和证据，不得控制 HTML/CSS/script、颜色或任意图表库参数；视觉与交互仍由 deterministic Renderer 负责。
- 若 Skills package、Structured Tool Schema、Renderer 支持版本不一致，H5 Runtime 必须失败关闭，不能回退为自由文本猜测或静默使用旧 Chart v1 冒充 v2。

### 边界与建议

- **优先级**：P1，高业务价值，但不应混入正在收口的 H4 P0 安全修复。
- **建议顺序**：先完成 H4 并复验 fail-closed，再以 H5 先做 Contract/fixture/静态视觉候选；用户确认视觉和交互方向后实现 Skills/Prompt/Structured Tool/Renderer 的同版本生产切换，不直接大改报告全栈。
- **当前不做**：移动端；自由 Vega/Plotly/任意脚本注入；模型直接决定颜色/CSS；为凑图数自动补查数据库；原地修改已发布报告。

### 决策

用户于 2026-08-24 明确确认 H5 图表叙事方案。H5 第一门只实施 `ChartArtifact v2 Contract + 横截面/时间趋势两个真实 fixture + HTML/PDF/PPTX 视觉候选`；候选必须先由用户做视觉与信息价值确认，未经确认不进入完整交互 Runtime、不替换生产 ChartArtifact v1、不部署到生产报告主链。

R0 已完成并记录于 `docs/chart-storytelling-r0-evidence-2026-08-24.md`：ChartArtifact v2 Python/TypeScript Contract 与 QueryResult semantic gate、双正向 fixture、8 个负向 fail-closed case、自包含 HTML、5 页 PDF 和 5 页 PPTX 候选均通过自动化检查；但用户视觉门禁明确判定 **FAIL**。首屏深绿色“从图表堆砌，到决策叙事”是候选说明而不是报告内容，占据接近整屏却没有任何决策价值；交互全部藏在首屏以下，主视觉标签又像按钮但不可操作，导致用户感知为“看不到任何交互”。

下一版 R0 必须删除报告内的候选宣传 Hero 和伪按钮式元信息；首屏直接呈现报告主题、数据范围/新鲜度、质量、执行摘要和第一项决策内容。图表必须提供即时可见的 tooltip、明确的 series 控制、图表/数据表切换和 Annotation → Evidence 操作反馈，不能依赖原生 SVG `<title>` 或让用户先滚动一整屏。

用户进一步明确“产品不要重复造轮子”。因此当前手写 SVG/JavaScript Renderer 只能作为一次性 Contract harness，禁止演化为生产图表引擎。H5 修订版必须先做 library-first 选型门禁：优先比较 Apache ECharts、Vega/Vega-Lite 与 AntV G2；只有用户接受商业授权时才把 Highcharts/AG Charts 纳入最终候选。Forge 只保留不可替代的 `ChartArtifact v2 → allowlisted library spec` 薄适配、Evidence bridge、质量 Gate 和跨媒介一致性测试，不自研 tooltip、legend、zoom、selection、annotation geometry 或 chart layout。修订版再次通过用户门禁前，R1 生产 Skills/Prompt/Tool/Renderer 同版本切换仍未批准。

用户已确认继续执行开源 engine bake-off。第一门只在隔离的开发工具包中，用相同的品类横截面和月度多系列 fixture 比较 ECharts、Vega/Vega-Lite、AntV G2 的交互可发现性、Evidence event bridge、Annotation、SVG/静态导出、无障碍 fallback、bundle/加载成本和 CSP 风险；不把三个库接入生产 Pi package，不并行维护三个生产 Renderer。

Bake-off 已完成，证据见 `docs/chart-engine-bakeoff-2026-08-24.md`。三者在严格 CSP 下均完成 4 SVG、tooltip/legend、datum → Evidence、table fallback 和静态 PDF；ECharts 在本轮拥有最小 engine bundle、最低渲染延迟和最直接的 Annotation/Evidence adapter。Vega-Lite 的声明式治理优势成立，但需要 CSP interpreter/AST、layer lineage 更复杂，且曾暴露 annotation layer 继承主数据导致重复 mark 的隐蔽风险；G2 bundle/渲染成本最高且无抵消优势。当前建议只让 ECharts 进入 focused visual candidate，Vega-Lite 作为治理参考，G2 停止；用户确认前仍不接生产 Runtime。

用户已于 2026-08-24 确认继续 ECharts focused candidate。该门继续位于隔离工具包，不接生产 Runtime：删除 bake-off 实验室宣传与多引擎切换外壳，首屏直接呈现真实报告摘要和第一决策图；将第四张图从渠道存量结构改为四月至六月的**增量贡献拆解**，明确 baseline、总增量 174K、直营增量 87K/50% 及对应 Evidence；排名图增加前两名差距表达，不以单色高亮制造赢家错觉；抽出 allowlisted Chart Story → ECharts Option mapper，禁止 Artifact/模型注入自由 Option、formatter、颜色或脚本。HTML 必须保留 tooltip、series 控制、Evidence 和 table fallback，print/PDF/PPTX 必须静态自足。只有 focused candidate 再次通过用户视觉与信息价值门禁，才提出 R1 同版本生产切换。

Focused candidate 已完成，证据见 `docs/chart-storytelling-echarts-focused-evidence-2026-08-24.md`。页面已按 `REQ-2026-08-24-011` 删除所有宣传/实验说明并首屏直达报告；4→6 月增量图严格复算 `87K + 53K + 34K = 174K`，直营占 50%；排名差距、零基线趋势、Pareto threshold、tooltip、series toggle、Evidence、table、无 JS 核心结论和 5 页 PDF/PPTX 均通过。当前阻断是用户视觉确认，以及 ChartArtifact v2 尚不能完整声明 period-delta/output-grain；该 Contract 缺口不得由 Renderer 猜测，必须在 R1 兼容矩阵中同步解决。

用户随后明确判定 focused candidate 视觉仍为 **FAIL**：去掉宣传文案不等于成为专业报告；当前双栏大标题、深色执行摘要、导航卡片、大圆角章节和大面积彩色侧栏仍属于 Landing Page composition。修订方向见 `REQ-2026-08-24-012`：保留 ECharts 交互与专业解释，整体改为 Editorial Report，并对 strong/emphasis/superseded/link/code/mark/callout 建立受控语义格式。

---

## REQ-2026-08-24-010：将一次性报告保存为可复用分析定义并持续更新

- **提出日期**：2026-08-24
- **当前状态**：`assessed`
- **原始需求**：报告、图表和分析应抽象成可复用方案；数天或数月后数据变化时，用户可以从现有报告核心内容重新取数，并更新判断标准，需要明确的产品入口和功能。

### 第一性原理结论

这不应实现为“修改旧报告”或“复制一份 Prompt”，而应拆成两个对象：

```text
ReusableReportDefinition（可演进、版本化）
        ↓ 每次运行
ReportRun / ReportRevision（不可变快照）
```

- **Definition 是方法资产**：目标、Audience、Owner、参数、指标、grain、Datasource/Registry binding、判断标准、Chart Story、Skills/Model policy、交付渠道与更新策略。
- **Run 是证据快照**：固定 TaskRun、QueryRun、审批、数据时间、Registry/Skill/Model/Policy revision、Analysis、Chart、Report 和 Export；历史 Run 永不原地改写。
- “更新数据”创建新 Run；“更新判断标准/目标线/阈值”先创建 Definition 新 revision，展示 diff 并确认，再运行。不能让新标准悄悄重解释历史报告。

### 建议产品入口

1. **报告页主操作**：
   - `保存为可复用报告`：从当前不可变 Report Bundle 提取 Definition Draft，用户确认名称、Owner、参数和判断标准；
   - `用最新数据更新`：基于当前 Definition revision 创建新 TaskRun，显示数据范围、变化的 Registry/标准和审批点；
   - `调整判断标准`：进入版本化 Criteria editor，展示前后 diff，不直接改当前 Report。
2. **可复用报告库**：显示 Definition、Owner、状态、最近数据时间、最近结论、Definition revision、Run history 和下一建议更新时间；支持暂停/归档，不删除历史 Evidence。
3. **更新向导**：选择时间范围/参数 → 检查数据源与 Registry drift → 预览指标/标准/Chart Story → SQL review/授权 → 新 Run → 与上一 Run 做差异摘要。
4. **历史对比**：新旧 Run 的数据、判断标准、结论、Annotation 和质量状态分别比较；区分“数据变化导致结论变化”与“标准变化导致结论变化”。

### Contract 建议

- `ReusableReportDefinition v1`：`definition_id/revision/title/purpose/audience/owner/parameters/metric_refs/grain/semantic_query/criteria/chart_story/skill_policy/delivery_policy/bindings/status`。
- `SemanticQuerySpec v1`：保存稳定语义 ID，而不是只保存物理表列名：`intent/metrics/dimensions/grain/filters/time_semantics/relationships/order/limit/parameters/expected_shape`；可包含已通过 Forge Assurance 的 Forge JSON semantic plan，但不能把模型自由文本或旧 Prompt 当查询真相源。
- `RegistryBindingSet v1`：记录 semantic ID → 当前 datasource/table/column/relationship 的物理绑定、Registry revision、binding status 与兼容性；表结构变化后可重新绑定。
- `CompiledQuerySnapshot v1`：每次 Run 固定当时的 Forge JSON、参数化 SQL、bound parameters、dialect、SQL hash、Registry/Assurance/Policy revision 和审批；它用于复现，不是未来运行的唯一源。
- `JudgementCriteria v1`：目标值、阈值、比较基线、方向、适用 scope、生效时间、Owner/approver 和 revision；禁止只有自然语言没有可计算字段。
- `ReportRun v1`：`definition_revision + semantic_query_revision + binding_snapshot + compiled_query_snapshot + parameter_snapshot + task/query/report lineage + data_as_of + criteria_revision + outcome/diff`。
- 用户确认这里指**参数化 SQL**：SQL 文本与 literal/bound parameters 分离保存。`CompiledQuerySnapshot` 必须同时固定 parameter schema、非敏感 parameter snapshot、dialect、SQL hash 和 semantic/metric/binding IDs；Secret parameter 只保存不可逆引用或受控 SecretRef，不进入 Artifact 明文。参数化 SQL 解决安全复现与参数更新，不解决 Schema 漂移；Schema 漂移仍由 SemanticQuerySpec + RegistryBindingSet 处理。

### 动态复用策略

产品不能把“复用旧 SQL 还是重新生成”交给模型自由判断，而应由 Forge deterministic compatibility planner 输出一种可审计策略：

1. `reuse_compiled_sql`：Datasource/dialect、Registry 物理 binding、字段类型、关系、Policy 和 SQL Assurance 仍兼容，且只有已声明 parameter 变化；旧 SQL 仍需重新过当前 Safety/Authorization，旧审批不自动延续。
2. `rebind_and_recompile`：SemanticQuerySpec 未变，但表/字段物理 binding 发生可证明的 rename/move/type-compatible drift；基于 stable semantic ID 重新绑定并确定性编译新 SQL，再 Assurance/审批。
3. `replan_from_semantics`：指标、关系、grain 或当前 Registry 无法直接重绑；使用原 SemanticQuerySpec + 当前 Registry 生成新的不可信 Forge JSON candidate，再进入完整 Assurance 和人工 review。
4. `blocked_needs_input`：Semantic ID 缺失、多个 binding 冲突、标准含义改变或结果 shape 不兼容；失败关闭并要求用户/数据团队确认，不能猜。

每次选择必须生成 `QueryReuseDecision`：记录候选策略、兼容性 diagnostics、旧/新 Registry diff、实际 SQL 是否变化、是否需要审批和最终责任人。SQL reuse 是优化，不是产品真相；SemanticQuerySpec 才是跨 Schema 演进的主要复用资产。

### 职责与安全边界

- Pi 创建/调度 ReportRun，维护等待、恢复和对比流程；Forge 继续独立准备、Assure、审批和执行查询；Web 只提供 Definition/Run projection 和操作入口。
- Skills/Prompt 版本必须固定在 Definition 或 Run；升级 Skill 形成可见 migration proposal，不静默改变同一 Definition 的分析方法。
- 数据源、Registry、指标、ACL、判断标准或 Chart Contract 漂移时必须提示并失败关闭或要求新 revision；不得用旧审批执行新 SQL。
- 定时运行属于后续切片。v1 先支持手动“用最新数据更新”；自动计划需要 Owner、Budget、通知、失败策略和有界授权，不能默认自动执行高风险副作用。

### 验收与反证

- 同一 Definition 连续运行两次，产生两个不可变 ReportRun，旧 URL/PDF/PPTX 和 Evidence hash 不变。
- 只更新数据时 criteria revision 不变；只更新标准时 data snapshot 不冒充变化来源；UI 明确区分两类 diff。
- Schema/Registry/Skill/Criteria 漂移均不能静默复用旧 Query approval。
- 用户能从报告页在 3 个动作内发起更新，并在报告库看到 Definition 与 Run history；失败可恢复，不重复执行 SQL。
- 若第二个真实报告场景无法复用同一 Definition Contract，或用户不能理解 Definition/Run 区别，应停止抽象，不建设通用模板平台。

### 建议顺序与边界

- **优先级**：P1，商业价值高。这会把一次性 AI 报告升级为组织可持续使用的“分析产品”。
- **建议顺序**：H5 R0 视觉门禁完成后，先做 H6 Contract/双场景/入口原型；用户确认 Definition/Run 心智后，再实现手动 rerun。自动调度和免逐次审批不进入第一版。
- **不做**：原地修改旧 Report revision；保存自由 Prompt 当模板；静默升级 Skill/标准；后台自动重放 SQL；移动端。

### 待用户确认

是否接受把该能力作为独立 H6：第一门只做 `ReusableReportDefinition + JudgementCriteria + ReportRun Contract`、两个跨时间 fixture，以及报告页/报告库/更新向导桌面原型；通过心智与视觉门禁后，再实现手动“用最新数据更新”的生产链路？

---

## REQ-2026-08-24-011：Web 页面只呈现主体内容，禁止宣传口号与营销文案

- **提出日期**：2026-08-24
- **当前状态**：`implemented_pending_visual_confirmation`
- **原始需求**：任何 Forge Web 页面都不应出现宣传 slogan、口号或带营销意味的内容；页面只需要把主体内容描述清楚。

### 产品规则

用户页面的文案分母只包含：

- 当前任务、对象或报告的明确标题；
- 数据范围、新鲜度、质量、状态和限制；
- 业务事实、分析结论、Evidence 和可执行建议；
- 用户下一步、审批风险、错误恢复和必要帮助；
- 法律、安全、权限或数据质量所需的最小披露。

以下内容禁止进入终端用户页面：

- 品牌 slogan、价值主张、宣传 Hero、Campaign 标题；
- “从 X 到 Y”“重新定义”“更智能/更可信/更专业”等营销式对仗或自我评价；
- 用大面积首屏解释 Forge、候选方案、技术选型或产品价值；
- 为营造气氛而存在、不能帮助用户理解当前任务或采取行动的文案；
- 将开发门禁、engine bake-off、Contract revision 或 Renderer 说明混入业务报告正文。

允许显示 Forge 产品名、页面功能名和准确的技术/状态标签，但这些标签不能扩写成宣传语。开发证据、选型说明和产品论证只保留在 `docs/`、测试证据或明确的管理员/开发工具中，不进入普通用户工作流。

### 实施与门禁

1. H5 ECharts focused candidate 立即移除候选宣传、英文氛围标签、技术边界 footer 和任何自我评价；只保留报告标题、执行摘要、决策问题、图表、Evidence、数据质量与操作。
2. 对 `web/templates/` 做一次只读文案审计，列出疑似宣传/口号与其实际页面；不得仅修隔离候选后宣称全站合规。
3. 对确认属于营销内容的生产文案做小范围确定性替换，不改变 Task、Artifact、审批、身份或导航逻辑；帮助文本只有在直接降低操作风险时保留。
4. 增加静态回归：最终用户模板和候选不得出现已拒绝的宣传短语；但不能用宽泛关键词误杀真实业务报告中的“增长”“价值”等领域内容。
5. 桌面视觉检查确认首屏主体内容立即可见，页面层级不依赖宣传 Hero 填充。

### 边界

- 本规则约束产品 Web 页面，不要求删除 README、架构文档、选型报告或管理员开发诊断中的必要说明。
- 不把“去营销化”误解为删除执行摘要、业务判断或建议；只要这些内容由当前数据和 Evidence 支持，它们就是主体内容。
- 不因此新增移动端工作或重写 design system。

### 实施结果

- 已审计 `web/templates/` 的 19 个模板、Web 暴露的 Architecture Atlas 和 H5 candidate，正式记录见 `docs/web-product-content-audit-2026-08-24.md`。
- 已清理 `/chat` slogan 与营销式空状态、`/tasks` integration/架构宣传和口号标题、Registry Studio 控制面 eyebrow、全局/登录页 `AI SQL Agent` 描述、登录页“私有化部署”展示，以及 Architecture Atlas 中的产品主张；保留直接降低操作风险的 SQL 审批、DDL Draft、Binding/Revision 与架构事实等说明。
- H5 focused candidate 同步删除“可信数据报告”、英文氛围标签、候选/Renderer/版本说明，Web 正文只保留报告内容、Evidence、质量、限制和操作。
- 新增 `tests/test_web_product_content.py` 固定明确拒绝短语，定向 Web 测试 76 passed；H5 浏览器 gate 0 console/page error，首图在 1600×1000 首屏内开始可见。
- 当前等待用户对去宣传后的实际页面视觉确认；确认前不宣称最终视觉门禁 `verified`。

---

## REQ-2026-08-24-012：建立编辑式专业报告排版与受控语义强调规范

- **提出日期**：2026-08-24
- **当前状态**：`accepted`
- **原始需求**：报告不能像宣传单页或营销落地页；必须在内容、排版、视觉和交互上同时专业。保留现代交互式图表和专业解释；加粗、斜体、删除线、下划线、callout 等样式必须受规范控制，保证易读性。

### 对当前候选的正式判定

当前 ECharts focused candidate 在数据语义和交互上进步，但视觉门禁仍为 **FAIL**：

- 大标题双栏首屏、深绿色 Executive Brief、四个导航卡片仍沿用 Landing Page Hero/Feature Grid 语法；
- 每一节都是大圆角 Card + 彩色侧栏 Callout，像产品卖点陈列，而不是连续的报告论证；
- 装饰纹理、强调色面积和大号数字过多，视觉在“推销结论”，而不是帮助读者审核结论；
- Evidence、图表与解释虽然存在，但章节、图注、方法、论证和限制尚未形成专业文档阅读节奏。

不能通过继续换颜色、减少一句文案或把圆角缩小来关闭。需要从 Landing Page composition 切换为 Editorial Report composition。

### 专业报告视觉语法

1. **文档而非落地页**：使用白色/近白文档画布、明确页边距、紧凑报告头、标题/副标题/作者或生成信息、数据范围、质量和 revision；禁止宣传 Hero、Feature 卡片、氛围纹理和装饰性大色块。
2. **连续论证结构**：执行摘要 → 目录/结论索引 → 方法与数据边界 → 编号章节 → 图表 → 图注 → 解释 → Evidence/限制。章节通过字号、留白、细分隔线和编号组织，不依赖每节一个营销卡片。
3. **图表是正文的一部分**：保留 ECharts tooltip、legend/series toggle、datum → Evidence、table fallback；图表标题、单位、轴起点、Annotation、source note 和 figure number 在静态首屏中已完整，不能依赖 hover 才理解。
4. **解释必须专业**：每张图后固定包含“观察 / 判断 / 限制或下一步”中的适用项；事实、推断和建议不能混写；关键数字与 Evidence 在同一阅读块内。
5. **克制的视觉 token**：正文以黑、灰、白为主；品牌绿只用于链接、图表主系列和小范围状态，coral 只用于风险/负偏差。减少圆角、阴影、胶囊标签和渐变，不用色块营造高级感。

### Inline emphasis 与 Callout 规范

模型/Artifact 不得自由输出 HTML/CSS、颜色或任意 class。Renderer 只投影版本化语义 token：

| 语义 | 允许表现 | 禁止用途 |
|---|---|---|
| `strong` | 加粗关键结论、字段名或有证据的关键数字 | 整段加粗、用粗体制造口号 |
| `emphasis` | 斜体术语、假设或轻度语气强调 | 用斜体承载关键数值或风险 |
| `superseded` | 删除线显示被新 revision 明确替代的标准/值，并紧邻显示新值 | 删除普通错误、隐藏历史责任 |
| `link/evidence` | 下划线只用于可点击链接、Evidence 定位和引用跳转 | 对不可点击普通文字加下划线 |
| `code/identifier` | 等宽样式显示 SQL、字段、ID、revision | 用等宽字体装饰普通正文 |
| `mark` | 低饱和背景强调极少量待审数字或定义，必须有语义来源 | 模型自行选择荧光颜色 |
| `callout.info` | 方法、口径或范围说明；细边框/浅底 | 大面积占据首屏 |
| `callout.decision` | Evidence 支持的决策结论 | 宣传口号或自我评价 |
| `callout.warning` | 数据质量、风险或审批提醒 | 用暖色强调普通内容 |
| `callout.limitation` | 假设、限制和不可推断范围 | 藏在页尾或仅 hover 可见 |

普通下划线继续保留给链接/Evidence，避免与可点击性冲突；这不是拒绝下划线，而是把下划线纳入可验证的交互规范。删除线只表达明确 superseded lineage，不作为修辞。

### 本轮实施范围

- 继续只修改隔离的 ECharts focused candidate，不接生产 Runtime。
- 将现有页面重构为桌面 Editorial Report：去除深色摘要卡、四导航卡片、大圆角章节卡、装饰纹理和营销式彩色侧栏；保留数据与四张图。
- 增加紧凑报告信息、方法/范围、figure caption、观察/判断/限制结构和一个受控 inline-style/callout specimen；specimen 必须绑定实际报告内容，不做 design-system 宣传区。
- HTML 继续验证 tooltip、series toggle、Evidence、table、键盘和 no-JS 核心结论；PDF/PPTX 静态完整。
- 用户再次做桌面视觉与阅读门禁。通过前不提出 H5 R1 生产切换。

### 验收与反证

- 1600×1000 首屏必须看起来像报告封面/报告第一页，而不是产品首页；报告标题、元数据、摘要和正文开头均可见。
- 去除 JS 后仍能按章节阅读结论、图注、Evidence 和限制。
- 强调 token 有固定语义与数量边界；不存在任意 inline style、模型 class、不可点击下划线或无 lineage 删除线。
- 每张图的交互可发现，但 controls 不抢占标题和正文；PDF/PPTX 不依赖交互。
- 若用户仍首先感知为 Landing Page、宣传册、Dashboard 卡片墙或模板拼装，本轮继续 FAIL，不以自动测试通过代替视觉门禁。

### 用户补充：内容专业不等于术语密度

- 专业性的依据是事实准确、证据可复算、推理可检查和边界诚实，不是专业词、英文缩写或咨询式表达的数量。
- 能用准确普通中文说清楚时，不使用 jargon；必要术语首次出现时说明其具体含义。
- 正文顺序优先为“数据中看到什么 → 可以作出什么有限判断 → 当前不能说明什么 → 需要补充什么”，不得把相关性写成因果。
- `Evidence`、`Revision`、`Ready`、`baseline/comparison` 等内部技术语言不得占据业务正文；必要 ID 放入数据来源明细。
- 不得通过强语气、粗体密度、Callout 数量或图表标注制造确定性。无法由当前 Evidence 支持时必须降级表达或明确未知。

---

## REQ-2026-08-24-013：Atlas 隔离报告预览部署与阶段差距重评估

- **提出日期**：2026-08-24
- **当前状态**：`completed`
- **需求**：将当前 Editorial Report 候选部署到 Atlas，随后基于唯一主动计划和长期目标重新评估已完成能力、剩余差距与下一步。

### 部署边界

- 当前候选只作为 Atlas 内部、只读静态预览发布；不接入生产 Report Renderer，不修改 ChartArtifact、Skills、Prompt、数据库、Registry、Identity Map 或 Secret。
- 预览必须使用本地固定构建产物，不依赖 CDN；发布前完成 candidate tests/build/audit、浏览器交互、no-JS、PDF/PPTX 与泄漏检查。
- 使用 Atlas 现有 LAN 管理入口和独立目录；不得覆盖 `~/services/forge-m4.1/source` 或重启生产 Forge/Pi。
- 记录部署路径、访问地址、版本、回滚/删除方式和验证结果。独立预览部署不代表 H5 R1 通过或生产报告能力已升级。

### 重评估范围

- 以“概率机器在不拥有最终责任能力时安全参与组织认知、决策与行动”为长期问题，检查 Governance、Coordination、Economics、Assurance 是否形成真实闭环。
- 区分已验证、仅 Contract-ready、仅候选、未开始和被阻断，不能用代码量或测试数量替代产品完成度。
- 给出当前目标完成度的分项估计、关键证据、最大反证、下一阶段优先级与明确不做项。

### 实施结果

- 当前 Editorial candidate 固定到本地 commit `929e8d4`；candidate `6 passed`，build/audit、4 SVG/0 Canvas、tooltip、legend toggle、数据来源定位、3 行增量表、no-JS、5 页 PDF、5 页 PPTX 和 0 browser error 通过。完整仓库回归为 Python `564 passed / 24 skipped`、Pi `103 passed`、typecheck 与 audit 通过。
- Atlas 的 `primary SSH entry` 入口在 SSH banner 阶段超时；使用同一主机的现有 LAN 管理入口 `internal operations entry`（`preview.internal.invalid`）完成发布。未读取 Secret。
- 静态文件发布在 `/srv/forge/previews/editorial-929e8d4/`，`current` symlink 指向该不可写 revision；systemd user service `forge-report-preview.service` 仅绑定 `preview.internal.invalid:18005`。
- 访问地址：`http://preview.internal.invalid:18005/`。Atlas browser gate 再次通过；三个文件 SHA-256 与本地构建完全一致。
- 生产 `forge-m41-api.service` 与 `forge-m41-pi.service` 保持 active，`~/services/forge-m4.1/source` 仍为干净的 `d2b0fd9`，未重启、未覆盖。生产 readiness 仍只有已知 `secure_cookie` fail：当前为内网 HTTP，未在本次预览部署中修改 HTTPS/Auth 配置。
- 回滚/删除只需停止并 disable `forge-report-preview.service`，删除 `current` symlink、独立 revision 目录和该 user unit；不涉及 Forge 状态恢复。
- 目标差距正式重评估见 `docs/forge-goal-gap-assessment-2026-08-24.md`：近期可信数据任务产品约完成 65%–70%，长期企业目标约完成 30%–35%；当时建议下一工作包为 M1A，后续已被用户确认的 `REQ-2026-08-24-014` 调整为先完成 Web 产品骨架，M1A 顺延为首个后端治理工作包。

---

## REQ-2026-08-24-014：Web 产品骨架与可人工测试交互框架优先

- **提出日期**：2026-08-24
- **当前状态**：`w3a_product_direction_reassessment`
- **用户决策**：先从整体产品框架和 Web 前端页面开始，把信息架构、页面骨架、核心交互和可见状态搭到可用、可人工测试的程度；治理、成本等内在能力适当后排。原因是没有可操作的产品外壳，用户无法持续人工测试、指导产品走向或判断后端能力是否真正形成产品价值。

### 评估结论

方向成立，但采用 **Interaction-first、Contract-backed**，不是“先做一套假前端”：

- Web 可以先于完整后端展示产品结构和所有关键状态，但不得建立第二套 Task/Artifact/Approval 真相源；
- 已实现能力连接真实接口；未实现能力必须显示明确状态、限制或“尚未开放”，不能提供会伪造成功的按钮；
- 演示数据只允许存在于隔离 R0 原型并清楚标注，不得混入生产 Store、Audit 或真实任务；
- 先搭骨架不等于冻结领域 Contract。页面状态必须从现有 Task、Artifact、QueryRun、Report 和 Registry 概念投影，避免后端完成后推翻全部交互。

### 当前 Web 审计反证

- 20 个 Jinja 模板虽然共享 `base.html`，但 `/chat`、`/tasks`、管理后台和报告使用多套视觉语法，缺少统一产品框架；
- 左侧导航平铺约 16 个入口，把最终用户工作流、Registry、内部 Pipeline、Memory、架构图和系统设置混在同一层级；
- `/admin/dashboard` 是系统健康概览，不是用户工作台；任务创建、任务监控、对话、报告和审计之间缺少清晰的信息架构；
- `/tasks` 同页堆叠创建、事件、SQL、结果、分析和报告，缺少可寻址 Task Detail；报告有 detail/share API，但没有用户可发现的 Report Library；
- Tailwind/Marked 依赖 CDN，页面级内联 CSS/JS 较多，设计 token、组件状态和交互反馈不统一；
- 当前页面可以运行，但还不能作为稳定的产品测试框架。

### W3 第一版目标信息架构（已被用户门禁退回）

以下结构是 `821065f` 原型的历史设计依据，不再作为下一版已确认 IA；北极星完成后需重新评估 Conversation、Task、Decision、Delivery、Data Governance 和 Agent Runtime 的产品投影。

第一版主导航曾定义为：

1. **工作台**：等待处理、进行中任务、最近报告、系统阻断；
2. **新建任务**：对话式提出问题、补充目标和选择交付物；
3. **任务**：Task inbox、筛选、Task Detail、计划、审批、结果、分析、报告和活动；
4. **报告**：Report Library、Report Detail、下载/分享；Reusable Definition 在 H6 前显示为未开放，不伪造；
5. **数据资产**：结构、指标、语义规则、Registry Draft/Revision；
6. **管理**：团队、审计、模型/渠道/数据库和系统设置。Pipeline、Memory、Architecture 等开发/诊断入口不再占据主导航第一层。

已有 URL 优先兼容，通过导航分组和新聚合页面逐步迁移，不立即删除旧路由。

### W3 分门实施

#### W3A：产品地图与高保真交互骨架

- 输出页面地图、对象关系、路由兼容矩阵、关键用户旅程和每页状态/动作清单；
- 建立隔离、桌面优先的高保真 Web shell 原型，覆盖工作台、新建任务、任务列表/详情、SQL 审批、查询结果、分析、报告、数据资产和管理框架；
- 原型允许使用固定 fixture，但页面必须显式标记“演示数据”，副作用按钮不得连接生产；
- 建立本地 design tokens、排版、表单、按钮、表格、状态、空/loading/error/forbidden、drawer/dialog、callout 与 Evidence 交互规范；不使用 CDN，不使用 slogan 或营销 Hero；
- 部署到 Atlas 独立预览端口，由用户进行逐页人工门禁。用户确认 IA 和交互方向前，不大范围改写生产模板。

#### W3B：生产 Product Shell 与核心旅程

- 将通过门禁的 shell 接入 Jinja/Web，统一本地资源、主导航、页面头、Workspace/身份上下文、内容宽度、反馈和可访问性；
- 新增可寻址 Task Detail 和 Report Library projection，连接现有 Pi/Forge/Report 真相源；
- 打通“新建任务 → 查看计划 → 补充信息/审批 SQL → 查看结果/分析/报告 → 回到任务/报告列表”的桌面 Golden Path；
- 每个可见按钮必须真实可用或明确 disabled+原因；刷新、后退和深链接不丢失当前对象；
- 生产切换使用单一 feature flag 和明确回滚点，不长期维护两套 Product Shell。

#### W3C：数据资产与管理信息架构收口

- 将 Schema、Metrics、Semantic、Staging、Registry Studio 收口到“数据资产”二级导航；
- 将 Team、Audit、Model、Channel、Database、System 收口到“管理”；
- Legacy Pipeline、Session、Memory 与 Architecture 作为诊断入口，不再与日常任务并列；
- 本阶段只重组入口和交互，不顺带实现 M1B、M2、M3 或通用 Memory Service。

### 人工与自动验收

- 桌面端至少覆盖 1440×900 和 1600×1000；当前不把移动端加入 Pass/Fail；
- 用户无需知道 Pi、Forge JSON、Artifact type 或内部 stage code，也能找到当前任务、风险、下一步和最终报告；
- 从任意主页面最多两次导航到达新建任务、等待审批、失败任务和最近报告；
- 所有页面具备 loading、empty、ready、partial、needs_input、waiting_approval、failed、forbidden/offline 中适用状态；
- Playwright 验证导航、键盘/focus、深链接、刷新恢复、dialog/drawer、无死按钮、0 console/page error 和无横向溢出；
- 人工门禁优先判断：产品结构是否容易理解、下一步是否明显、状态是否可信、页面之间是否像同一个产品。自动测试通过不能替代用户判断。

### 优先级调整

- W3A 成为唯一主动下一工作包；W3A 用户门禁通过后进入 W3B，随后 W3C。
- M1A 不取消，顺延为 Product Shell 核心旅程稳定后的首个后端治理工作包；涉及真实跨用户/跨团队生产开放前仍必须完成。
- H5 生产 R1、H6 runtime、M1B–M7 暂停新增实现，只保留已有 Contract、证据和 backlog。

### W3A 实施结果

- **当前状态**：用户门禁 `CHANGE`，现为 `product_direction_reassessment`。隔离原型位于 `tools/web-product-shell-prototype/`，固定 commit `821065f`；生产 Jinja、Pi/Forge package、Task/Approval/Report/Registry Store 均未修改。
- 一级信息架构覆盖工作台、新建任务、任务、报告、数据资产和管理；hash route 可深链接到 Task Detail、SQL、分析、报告和数据资产 tabs。
- 固定 fixture 覆盖 `waiting_approval / needs_input / analyzing / rendering / completed / failed`，原型控制可额外查看 querying/offline；所有页面固定显示演示数据边界，源码不包含 `fetch/XMLHttpRequest/WebSocket`。
- SQL 审批原型在最终确认中重复显示任务、数据源、范围、系统限制、完整 SQL、4 项检查和演示无副作用边界；确认按钮需显式勾选后才启用。
- 自动验证：prototype tests `5 passed`、build、npm audit 0；Python 全量 `564 passed / 24 skipped`（Web 定向 `19 passed`）；Pi typecheck 与 `103 passed`。Playwright 在 1440×900 与 1600×1000 走通工作台、新建、搜索、Task Detail、审批 dialog、状态切换、back/forward/reload、报告库/详情、数据资产和管理；0 console error、0 横向溢出、0 生产请求。
- Atlas 发布：`/srv/forge/previews/web-shell-821065f/`，user service `forge-web-shell-preview.service` 仅绑定 `preview.internal.invalid:18006`；远端两个 viewport 复验通过。生产 Forge/Pi 与原报告预览服务保持 active，生产源码仍为干净 `d2b0fd9`。
- 正式证据见 `docs/web-product-shell-w3a-evidence-2026-08-24.md`。在用户逐页给出 IA/交互门禁前，不进入 W3B，不把 fixture 行为接生产。

### 用户门禁反馈：Chat 与既有产品定位未被正确投影

用户判定当前 W3A **CHANGE**：原型把“新建任务”做成一次性 brief form，并从产品骨架中移除了连续 Chat；随后提出的“分析工作台”修订又把 Forge 误缩为分析场景。两者都没有完整反映此前已确认的产品方向。

已从近期项目会话重新确认的约束：

- 2026-08-21 已确认 Pi 是唯一任务底座和主 Orchestrator、Forge 是可信执行层、拾穗 DATA Skills 是专业方法层，Web/飞书/钉钉是渠道；产品不是单一分析页面。
- 2026-08-22 已确认入口需要处理 `conversation / knowledge / query`，并向 `action / workflow`、Structured Intent Router 和 deliverable-driven Plan 演进；Chat 是多种数据任务的通用交互面，不等于“探索性分析”。
- 2026-08-23 已明确恢复 `/chat` 为一等 Web 渠道并与 `/tasks` 任务监控分离；后续产品审计又确认 Data Agent 是默认体验和市场切口，不是产品最终边界。
- 中期定位保持“面向数据团队、供人和企业 Agent 共同使用的可信数据任务控制与执行平台”；`Data-Team Led / Business Accessible / Agent Native / Human Accountable` 继续有效。
- 对话负责人的连续交互，Task/Artifact/Evidence/Decision/Action 才是可恢复、可审计的协同真相源；两者不能互相替代。
- 长期 Context/Memory、企业 AI Infra 和非 SQL 场景仍是待验证假设，不得提前投影成已完成的通用控制台。

当前只确认原 W3A IA 不通过，尚未确认新的一级导航、页面命名和 Conversation/Session/Task 关系。W3A 退回 `product_direction_reassessment`；完成近期对话复盘并由用户确认修订后的产品地图前，不实施 Chat revision、不进入 W3B。

---

## REQ-2026-08-25-015：沉淀 Forge 产品北极星指导文档

- **提出日期**：2026-08-25
- **当前状态**：`verified`
- **用户决策**：基于 2026-08-21 至 2026-08-25 关于 100% 准确率、人—Agent/人—人共识、企业 Agent 数据底座、产品定位和 Web 方向的完整讨论，沉淀一份长期重要指导文档，防止后续把 Forge 再次缩成 Text-to-SQL、单一分析工作台，或未经验证扩张为通用 AI Infra。

### 评估结论

需求成立，采用独立 `docs/product-north-star.md`，不把战略判断混入功能 Plan，也不取代现有文档职责：

- `product-north-star.md` 回答 Forge 为什么存在、服务谁、提供什么、如何处理正确性/共识/事实与产品边界；
- `product-axioms.md` 继续保存不可轻易违反的稳定公理；
- `platform-architecture.md` 继续保存 Pi/Forge/Skills/渠道职责和目标架构；
- `forge-enterprise-evolution-plan.md` 继续是唯一实施计划；
- `ai-native-enterprise-thesis.md` 继续保留完整论证、反证和待验证假设。

文档必须明确：

1. 开放世界端到端 100% 正确理论上不可普遍实现；系统不变量和确定性投影仍必须追求严格保证；核心目标是降低 Silent Error，而非用单一 Accuracy 掩盖 Coverage、Clarification、Safe Abstention 和不确定性。
2. 组织不追求认知完全一致，而追求任务范围内合法、可验证、可执行的一致性；冲突、作用域、Decision authority 和责任必须是一等对象。
3. Forge 的中期方向是面向数据团队建设、供人和企业 Agent 使用的可信数据运行时与任务控制/执行平台；它为其他 Agent 提供有来源、语义、权限、版本和 Evidence 的数据事实能力，而不是自行制造组织真相。
4. Chat/Channel 是人的连续交互面；Task、Artifact、Evidence、Decision 和 Action 是协同真相源；Agent-facing Runtime 主要通过受控 API/Tool 使用。
5. 数据库、数仓、CRM、文档和身份系统继续持有各自业务真相；统一的是可信访问、Context、语义、Policy 和 Evidence 协议，不建立第二套事实主库。
6. Data Agent 是入口，结构化数据任务是第一验证场景；通用 Memory Store、完整企业 AI Infra 和更广 Action Plane 仍需第二场景、第二消费者和真实价值证明。

完成后将文档加入 README 导航，并让仓库 `AGENTS.md` 把它列为产品/体验/架构方向变更的必读依据；不修改 Runtime、API、数据库或生产部署。

### 实施与验证结果

- 新增 [`product-north-star.md`](product-north-star.md)，沉淀北极星命题、定位分层、四类质量、Silent Error、可执行一致性、信息分型、Agent Data Runtime、Conversation/Task 关系、四平面、体验投影、非目标、审查问题、可证伪假设和讨论时间线。
- `README.md` 加入首要文档入口并将顶部定位对齐为可信数据运行时与数据任务平台；`platform-architecture.md` 同步产品定义但不改变 Pi/Forge/Skills/渠道职责。
- `AGENTS.md` 将北极星加入相关任务必读清单；旧“弱模型生成可信 SQL”描述降为结构化查询验证切片，删除“生成错误物理上不可能”和“Registry 消灭业务逻辑错误”等绝对化表述。
- 未修改 Runtime、API、数据库、Web 原型或 Atlas/生产部署；W3A 仍处于 `product_direction_reassessment`，北极星不自动批准新 IA。
- 验证：文档链接与 Web 内容静态测试 `4 passed`；北极星 8 项必要章节断言通过；`git diff --check` 通过。

---

## REQ-2026-08-25-016：基于产品北极星重建产品设计与阶段计划

- **提出日期**：2026-08-25
- **当前状态**：`assessed_awaiting_user_confirmation`
- **用户决策**：基于已确认的产品北极星和此前讨论过程，重新规划 Forge 的未来产品设计与实施顺序；不能只修订导航或恢复 Chat，也不能继续沿用已被用户门禁退回的 W3 第一版信息架构。

### 需要解决的问题

1. 如何把“面向人的 Control Plane”和“面向企业 Agent 的 Trusted Data Runtime”设计为同一个 Forge，而不是两个产品或两套真相源；
2. Conversation、Task、Plan、Decision、Artifact、Evidence、Data Asset、Agent Client 和 Outcome 的产品关系与页面投影；
3. Chat 如何保持连续交互，但不成为 Task、权限或组织事实真相源；
4. Web 如何同时服务 Requester、Steward、Approver、Auditor 和 Agent Operator，又不把所有后台能力平铺给每个用户；
5. 现有单用户私有化能力、Runtime Governance Coverage=0、外部 Agent 仅 `prepare-query` 的现实边界如何进入路线图；
6. W3、M1A、Agent Runtime、Data Trust Control Plane、Assurance、Reusable Deliverables、Economics 和第二场景的正确先后顺序。

### 评估原则

- 先形成独立产品设计与路线提案，由用户进行 `PASS / CHANGE / REMOVE`；提案确认前不修改 W3A 原型或生产 Web。
- W3A 第一版和 `docs/web-product-shell-plan-2026-08-24.md` 保留为历史反例，不原地伪装成已确认的新设计。
- 新计划必须标明哪些是已有能力、哪些是产品 projection、哪些需要新 Contract/Store/Runtime，不能用 fixture 或页面入口冒充已实现。
- Agent-facing Runtime 在 M1A Service Identity/Principal/Mandate/Default Deny 前不得开放执行能力；现有外部 `prepare-query` 安全语义保持不变。
- 产品设计优先验证人和 Agent 获取可信数据事实的完整 Journey，而不是页面数量、Chat 消息数或 Agent 数量。

### 评估产出与建议

正式提案见 [`product-design-roadmap-2026-08-25.md`](product-design-roadmap-2026-08-25.md)。建议采用：

- **一个产品、三个产品面**：Human Work Surface、Shared Trust & Data Foundation、Agent-facing Trusted Data Runtime；三者共享同一 Task/Artifact/Evidence/Decision/Action 真相链。
- **核心对象**：Conversation 提供人的连续性；Data Task Contract 提供执行边界；Decision 绑定精确 Action；Artifact 承载结果；Evidence 说明依据；Data Asset 提供正式语义；Agent Client 必须绑定 Principal/Mandate。
- **导航提案**：按“工作 / 信任 / 接入 / 系统”分组；工作面包含工作台、对话、任务和近期报告/后续交付，信任面包含数据资产与治理审计，接入面在 M1A 后开放 Agents & Apps。
- **阶段顺序提案**：N2 用户门禁 → W3A.2 北极星驱动原型 → W3B Human Control Plane → M1A Runtime Trust → R1 Agent Data Runtime MVP → G1 Data Trust Control Plane → Q1 Quality/Assurance → H6 Reusable Deliverables → 按证据选择 M1B/M2/M3 → M5 第二场景。
- **关键门禁**：Agent Runtime 执行能力不得早于 M1A；W3B 只接已有 Human truth sources；G1/Q1 不用页面或 Contract 冒充 Runtime；R1 必须通过真实 Agent Golden Journey。

旧 [`web-product-shell-plan-2026-08-24.md`](web-product-shell-plan-2026-08-24.md) 已标记为历史第一版；当前原型和生产代码均未修改。2026-08-25 用户进一步确认短期实施必须采用“底层真实框架 → 前端真实投影 → 持续人工测试”的顺序；完整导航和长期阶段仍可在真实测试中修订。

---

## REQ-2026-08-25-017：短期 Product Spine 底层优先实施计划

- **提出日期**：2026-08-25
- **当前状态**：`sp5_automated_gate_passed_user_atlas_review_pending`
- **用户决策**：下一步先完成短期实施计划。计划必须从底层框架开始，再到前端展示；前端要成为用户持续测试、发现问题、补充功能和逐步丰富细节的真实抓手，不能继续以脱离 Runtime 的高保真 fixture 原型作为主路径。
- **实施批准**：2026-08-25 用户明确批准按 `SP0 → SP5` 顺序实施并逐包门禁；SP0–SP5 自动化与真实集成门禁现已完成，最终用户 Atlas 主观门禁仍待确认。

### 初始边界

- “底层框架”指支撑近期真实产品 Journey 的最小 Product Spine：对象关系、真相源、持久化/Projection、API、状态、权限边界、Event/Artifact/Evidence lineage 和恢复语义；不等于先建设完整企业 AI Infra。
- 前端必须连接真实 Pi/Forge/Report/Registry 能力；尚未实现的能力明确 disabled 或不进入短期导航。
- 短期先服务单用户私有化部署和 Human Control Plane，形成可持续人工测试闭环；Agent Runtime、完整 Decision Runtime、Economics、多 Workspace 和通用 Memory 不并行扩张。
- Conversation 是短期默认交互入口，Task/Approval/Artifact/Evidence/Report 是结构化测试抓手；不能新建第二套 Task、Approval 或 Report Store。
- 计划需要列出每个工作包的底层改动、前端投影、验收 Journey、失败关闭条件、依赖与停止点。

### 代码基线评估

- Pi SQLite 已真实持久化 TaskRun、TaskEvent、Artifact、StageAttempt、ChannelEvent 和 Skill Policy；每条 Channel message 已带 `channel_conversation_id` 并创建 TaskRun，因此 Conversation v1 可以先由现有真相源只读投影，不需要新的可写 Conversation Store。
- 当前 `ChannelEventStore` 只支持 claim/complete/fail/get，`TaskStore` 只支持 scope list/get；缺少 Conversation list/detail 和统一 Task Detail Projection。
- 当前 Web 已分别代理 task/events/artifacts/attempts/presentation/actions，但页面自行拼装；需要版本化、bounded、去敏的 Product Projection 和 BFF。
- `ReportStore` 已有不可变 get/share/download/export，但没有 scope-aware list；报告库必须先补底层索引能力。
- 当前 `base.html` 仍依赖 CDN，`/tasks` 同时承担创建、监控、审批、结果、分析和报告；前端重构应在 Backend Gate 后进行，并使用独立本地静态 Product Shell。

### 实施计划产出

详细计划见 [`short-term-product-spine-plan-2026-08-25.md`](short-term-product-spine-plan-2026-08-25.md)：

```text
SP0 Contract / Truth Source Closure
→ SP1 Pi Conversation + Task Product Projection
→ SP2 Product BFF + scoped Report Index
→ Backend Gate
→ SP3 local-asset Product Shell
→ SP4 real Workspace / Conversation / Task / Report / Data pages
→ SP5 Human Golden Journeys + isolated Atlas candidate
```

- 总风险范围：14–24 个工作日，按工作包逐项验收，不把时间范围当承诺。
- 短期最终抓手：统一 Web 内完成并恢复 Conversation → Task → SQL Review → QueryResult → Analysis → Report，同时覆盖 knowledge-only、needs_input、partial/offline 和失败恢复。
- 实施前门禁：用户确认工作包顺序、单用户 Human Control Plane 边界、短期导航和逐包验收方式。
- 当前未修改 Runtime、API、数据库、模板或部署。
- 计划验证：文档链接/Web 内容测试 `4 passed`；SP0→SP5 顺序、无第二真相源、真实前端与短期 scope 结构断言通过；`git diff --check` 通过。

### SP0 Contract/Truth Source Closure（已完成）

- 新增 TypeBox 真相定义 `services/pi-orchestrator/src/product-projections.ts`；SP0 固定 `ActionCapabilityV1`、Conversation Summary/Detail、Task Detail、Workspace、Report Summary，SP2 在实际 Task list BFF 前补充同版本 `TaskSummaryV1`，避免前端消费未注册的 raw TaskRun。
- 新增生成脚本和跨语言 Schema：`agent/contracts/product-projection-v1.schema.json`；Python package 注册 `product_projection_v1`。
- 新增共享正反 fixtures 与 TypeScript/Python semantic parity，覆盖 ready/needs_input/waiting_decision/running/partial/failed/completed、cross-scope、额外 Secret-like 字段、超长字段、source revision、Action/Task 状态、table shape、Artifact lineage、Workspace count 和 Report URL。
- 新增 [`product-projection-contracts.md`](product-projection-contracts.md)，固定状态语言、bounds、redaction、Query Review 和 SP1 入口条件。
- 验证：Pi `108 passed`、Python 全量 `567 passed / 24 skipped`、SP0 Python 定向最终 `19 passed`、TypeScript typecheck、npm audit 0 vulnerabilities、JSON/Schema 解析、生成 Schema 同步、`git diff --check` 通过。
- Code review 未发现阻断缺陷；LSP 未配置 TypeScript server，使用严格 `tsc --noEmit` 替代。SP0 未修改数据库、API、Task 状态机、页面或部署。

用户已批准全序列且 SP0 门禁通过，SP1 已完成并通过门禁，现进入 SP2。

### SP1 Pi Product Projection（已完成）

- 增加 scope/user/channel Conversation read queries、parent/child queries、opaque list/detail cursor 和单调 Task timestamp；无 DB Schema 变更。
- 增加 `ProductProjectionService`，从真实 Task/Event/Attempt/Artifact/Presentation 构建 Conversation Summary/Detail 与 Task Detail；Query Review 保留精确 SQL/hash/Assurance/expiry，损坏 lineage 失败关闭。
- 增加 authenticated read-only Pi API：`GET /v1/conversations`、`GET /v1/conversations/{conversation_id}`、`GET /v1/tasks/{task_run_id}/detail`；响应 `no-store`，scope mismatch 不披露对象。
- 10K Task/1K Conversation 首版 list 约 `19,040 ms`，定位为相关子查询 + N+1 JSON scan；改为 scoped CTE + window rank + grouped first/latest 后平均约 `26.8 ms`，因此未新增 schema v5。
- 验证：Pi `114 passed`、Python `569 passed / 24 skipped`、typecheck、npm audit、101-entry pagination、SQLite restart、auth/scope/no-store/bad cursor、2 MB boundary 和 `git diff --check` 通过。
- 正式证据：[`product-spine-sp1-evidence-2026-08-25.md`](product-spine-sp1-evidence-2026-08-25.md)。

SP2 已完成并通过 Backend Gate，现进入 SP3。

### SP2 Product BFF + Report Index（已完成）

- `ReportStore.list` 增加 scope/status/cursor/limit 与 `idx_reports_scope_updated`，不复制报告状态。
- 新增 `/api/product/workspace|conversations|tasks|reports|data-summary` BFF；使用现有 Session/API auth、configured scope、`web_admin` 和 Pi Channel Service Key。
- Conversation/Task 在 BFF 再执行 Python Contract + scope gate；Report/Workspace 执行 semantic gate；Pi invalid JSON、scope mismatch 和 Report/Registry offline 均 bounded/partial。
- SP2 在暴露 Task list 前补充同版本 `TaskSummaryV1`，避免前端消费 raw TaskRun metadata；Task list 明确 bounded/truncated，Report list 支持 cursor。
- Registry revision 改为内容 SHA-256；Workspace 在 Pi/Report/Registry 或读取上限发生时显示 partial/offline，不伪装 ready。
- 验证：Python `575 passed / 24 skipped`、SP2 定向 `34 passed`、Pi `114 passed`、typecheck、npm audit 0 vulnerabilities、scope/auth/cursor/offline tests 和 `git diff --check` 通过。
- 正式证据：[`product-spine-sp2-evidence-2026-08-25.md`](product-spine-sp2-evidence-2026-08-25.md)。

Backend Gate 已通过，SP3 已完成，现进入 SP4。

### SP3 Product Shell Foundation（已完成）

- 新增 `product_base.html` 和本地 `product.css`/`product-shell.js`，`main.py` 挂载 `/static`；0 CDN、0 inline style/script。
- 短期导航只保留工作台、对话、任务、报告、数据资产和管理入口；无 Agents/Economics/Pipeline/Memory/Architecture，无独立“新建任务”。
- 建立统一 panel/status/button/field/table/notice/empty/partial/offline/skeleton/code/evidence 组件及 skip-link/focus/reduced-motion/mobile-nav 基础。
- Shell JS 只处理导航和状态标签，不 fetch、不写 localStorage、不持有业务状态。
- 验证：Python `581 passed / 24 skipped`、SP3/Docs 定向 `10 passed`、Pi `114 passed`、typecheck、npm audit、static serving、template render 和 `git diff --check` 通过。
- 证据：[`product-spine-sp3-evidence-2026-08-25.md`](product-spine-sp3-evidence-2026-08-25.md)。

SP4 已完成并通过 SP5 入口门禁，现进入 SP5。

### SP4 Real Product Pages（已完成）

- `/workspace`、`/chat`、`/tasks`、`/tasks/{id}`、`/reports`、`/data` 已接真实 Product BFF；Chat message/Task action 继续复用原 typed ChannelEvent endpoints。
- SQL Review 从 Task Detail 读取精确 Query/hash/Assurance；补查参数未进入 Contract 时明确 disabled；无 Conversation ID 的 Task 不绕过边界。
- 使用本地安全 DOM Markdown renderer，无 `innerHTML/eval`，不复制业务状态。
- 首轮视觉审查发现审批操作不在主路径、pending 错标“可用”、状态层级混淆；均已修正，复审无 P0。
- 验证：Python `583 passed / 24 skipped`、Product/Web/Docs `33 passed`、Pi `114 passed`、typecheck/npm audit/JS syntax/Playwright 1440×900 与 1600×1000/0 error/0 overflow/chat send/dialog/`git diff --check` 通过。
- 证据：[`product-spine-sp4-evidence-2026-08-25.md`](product-spine-sp4-evidence-2026-08-25.md)；截图 `/tmp/forge-sp4-task-detail.png`。

### SP5 Human Golden Gate + Atlas Candidate（自动门禁已通过）

- Candidate 固定为 `product-spine-5dcd4715941a`，URL 为 `http://preview.internal.invalid:18007/`；Pi/QueryRun/Report/Registry/Artifact 状态独立，测试 SQLite 为 candidate 内 mode `0400` 副本，认证已开启，生产 Forge/Pi 保持 active。
- 固定渠道指标问题在最终 candidate 配置下连续 3 次完成 Conversation → SQL Review → 单次只读执行 → Analysis → Report → Report Library；每次均为 1 个 QueryRun、1 个 `query.completed`、4 个 succeeded StageAttempt，PDF/PPTX ready。
- 重复消息返回原任务；过期重复批准返回 409 且不重放 SQL。等待审批与完成态通过 restart recovery；Pi offline 时 Workspace 返回 partial，Report/Data 继续可读。
- Live Gate 修复 insecure-HTTP ID、瞬时 ready 轮询、同源 HTTP Report URL、空 Attempt error、长 SQL Grid overflow 和完成态历史审核误标权限；复杂查询 Assurance 拒绝与 Analysis `incomplete` 作为 fail-closed 反证保留。
- 最终验证：Python `583 passed / 24 skipped`、Pi `115 passed`、typecheck/npm audit/JS syntax/`git diff --check`、双桌面 12 routes/0 external request/0 error/0 overflow 全部通过。
- 正式证据：[`product-spine-sp5-evidence-2026-08-25.md`](product-spine-sp5-evidence-2026-08-25.md)。自动门禁 PASS；用户仍需对稳定 Atlas URL 给出 `PASS / CHANGE / REMOVE`，在此之前不选择 M1A/G1/Q1/H6，也不替换生产。

## REQ-2026-08-25-018：Atlas 网站性能严重不可接受

- **提出日期**：2026-08-25
- **当前状态**：`candidate_fix_verified_user_retest_pending`
- **用户反馈**：Atlas 候选网站性能非常差，要求直接在真实候选环境中自行测试、量化、定位并修复。
- **归属**：`REQ-2026-08-25-017` 的 SP5 用户 Atlas `CHANGE` 反馈；在性能门禁通过前，Product Spine 不得标记为最终接受，也不得替换生产。
- **简化 Bug 评估**：这是已批准 Product Spine 的真实体验缺陷，不引入新产品职责或真相源；按 P0 处理。先建立真实浏览器与 HTTP 分段基线，区分页面静态加载、Product BFF、Pi Projection、轮询和模型长任务耗时，再做最小根因修复。
- **验收边界**：必须以候选环境的真实页面与真实 API 复测；同时报告首屏、关键 Product API、页面切换和已有任务读取的耗时。不能用缩短假数据链路、放宽 Assurance、隐藏 loading 或减少 Evidence 代替性能修复。
- **根因**：`agent.contracts.validate_contract` 每次校验都重新执行 `validator_for + check_schema + validator construction`。Product BFF 对 100 条 Task 和 20–50 份 Report 逐项校验，把单次页面读取放大为数十秒；Pi 直连同批读取仅 `44.4–55.9 ms`，不是 SQLite Projection 或网络瓶颈。
- **修复**：按 Contract name 缓存已检查、可复用的 jsonschema validator；Schema、语义、scope、redaction、Evidence 和 Assurance 行为不变。增加 100 Task + 50 Report 的页面批量校验预算回归测试。
- **真实复验**：修复前 Workspace `15,727 ms`、Conversations `20,970 ms`、Tasks `28,985 ms`，Reports/Data 并发超过 `30 s`；修复后稳定 API 分别为 `258.8 / 134.1 / 87.0 / 54.5 / 103.2 ms`。六个真实页面严格等待 DOM 内容替换后的完成时间为 `83.7–586.9 ms`，DOM ready 为 `17.7–53.5 ms`；0 console/page error、0 横向溢出。
- **候选**：性能修复源码 revision `product-spine-d0aa8c9e3a0e` 已在原隔离状态与只读数据边界上切换至稳定 URL `http://preview.internal.invalid:18007/`；生产 Forge/Pi 未替换。契约/BFF/Shell 定向回归 `41 passed`。
- **剩余门禁**：自动性能门禁已通过，等待用户重新体验；用户未确认前，SP5 仍不标记为最终接受。

## REQ-2026-08-25-019：Conversation 查询结果缺少数据表

- **提出日期**：2026-08-25
- **当前状态**：`candidate_fix_verified_user_retest_pending`
- **用户反馈**：Conversation 已显示“查询结果”“可用”和“共 107 行”，但结果区域没有任何 Table；该位置应直接展示有界查询结果表。
- **归属**：`REQ-2026-08-25-017` 的 SP5 用户 Atlas `CHANGE` 反馈；属于真实 QueryResult → Product Projection → Conversation 渲染主链缺陷。
- **简化 Bug 评估**：按 P0 处理。主交付只显示行数、不显示行数据会让查询结果不可用，并破坏 Evidence 可检查性。必须定位 Table 是在 Pi Presentation、Conversation Projection 还是 Web DOM Renderer 丢失。
- **验收边界**：Conversation 和 Task Detail 对同一 QueryResult 必须显示一致的有界列与行；保留总行数、截断状态和 Evidence lineage。不能通过拼接 Markdown 表、前端读取原始 Artifact、扩大敏感结果披露或复制 QueryResult 状态修复。
- **根因**：Pi Channel Renderer 和 Conversation/Task Product Projection 均已携带 `presentation.table`；丢失发生在 `web/static/product/product-pages.js`。`renderConversation()` 只调用 `renderMarkdown()` 并单独渲染 fields，没有复用 Task Detail 已使用的 `renderPresentation()`，因此 Table 和 truncated notice 被 DOM 层静默丢弃。
- **修复**：Conversation entry 统一复用 `renderPresentation()`，删除重复 fields 渲染；不改变 QueryResult、Projection Contract、行列边界、scope 或 Evidence lineage。
- **真实复验**：新建真实 Conversation `web_conv_66e18f83b37539de3637d8699df5b422`，Task `tr_c0c65389a9a344e9b711cfa68909f6eb` 经 SQL 审核和一次只读执行后返回 107 行。Conversation DOM 显示 `category_name / total_sales` 两列、20 行有界预览、“共 107 行”和“结果已截断”；0 横向溢出。
- **候选**：最终源码/缓存版本 `product-spine-6a23e71276e5` 已发布到稳定 URL `http://preview.internal.invalid:18007/`，Product 页面统一引用 `product-pages.js?v=2`，避免浏览器继续使用缺陷脚本；继续复用原隔离状态和 mode `0400` 只读数据，生产 Forge/Pi 未替换。浏览器行为回归从失败转为通过，页面/缓存契约 `11 passed`，JS syntax PASS。
- **剩余门禁**：自动 Table 门禁通过，等待用户重新体验；SP5 仍不标记为最终接受。

## REQ-2026-08-25-020：按未来产品方向补全前端产品面

- **提出日期**：2026-08-25
- **当前状态**：`candidate_implemented_user_atlas_review_pending`
- **用户意图**：对照 Forge 未来产品方向和功能版图，系统梳理当前前端还应补充的页面、信息架构、状态与交互。底层能力可以暂时不可用，但前端产品面需要先完整，成为后续持续测试和补功能的统一抓手。
- **初始边界**：允许展示未来能力入口、信息结构、空态和 disabled 状态；不允许伪造成功数据、可点击死按钮、复制 Task/Decision/Report/Registry 真相源，或用前端状态冒充未实现 Runtime。
- **待评估**：对照 Product North Star、Human Control Plane、Agent-facing Trusted Data Runtime、Shared Trust/Data Foundation、现有 Product Spine 和已批准非目标，提出保留/补充/暂缓清单、Route 与页面矩阵、状态投影、实施顺序和用户门禁。
- **现状审计**：当前 Product Shell 只有工作台、对话、任务列表/详情、报告库、数据资产聚合和旧管理入口。真实 Human Query 主链已成立，但 Decision、Evidence/Assurance、Governance、Agent Access、完整 Data Trust、Outcome/Feedback 和统一部署设置没有正式产品页面；现有 Admin 仍是另一套视觉与导航。
- **产品判断**：可以前端先行，但必须建设“Capability-aware Product Shell”，不是 fixture 产品。未实现能力允许拥有稳定 Route、页面职责、对象关系、状态、空态和 disabled 操作；页面必须明确 `available / partial / planned / blocked`、依赖的 Runtime 阶段和为什么不可用。
- **建议 IA**：
  - 工作：工作台、对话、任务、交付；
  - 信任：数据资产、治理与审计；
  - 接入：Agents & Apps；
  - 系统：管理。
- **建议补充的产品页**：
  1. `交付中心`：Report、Export、重要 Artifact、revision、来源、限制；Reusable Definition/Run History 先 planned；
  2. `Decision Inbox/History`：先投影真实 SQL Approval，通用 Decision 暂 planned；
  3. `Evidence & Assurance`：Evidence 链、Query Assurance、来源/范围/截断/限制、失败样本；
  4. `Data Trust`：Datasource、Schema、Metric、Relationship/Grain、Knowledge、Draft/Revision、Quality/Freshness、Conflict/Proposal；
  5. `治理与审计`：Policy、Mandate、Action Audit、Runtime Governance Coverage；
  6. `Agents & Apps`：Agent Client、Owner/Purpose、Mandate、Capabilities、调用记录、Human takeover；M1A/R1 前全部明确 disabled；
  7. `管理中心`：Workspace/Team、Model、Skill、Channel、Database、System readiness，统一进入 Product Shell；
  8. `Outcome/Feedback`：结果接受、纠错、复用、Knowledge/Rule Proposal；Runtime 未实现时 planned。
- **共享前端基础**：全局搜索/命令入口、Workspace/身份上下文、通知与待办、面包屑与深链接、Evidence Drawer、Diff/Revision Viewer、Table/Chart/Code/Report Viewer、统一 loading/empty/needs_input/waiting_decision/running/partial/failed/forbidden/offline/superseded 状态、404/403/offline 页面和键盘/焦点/响应式门禁。
- **推荐实施顺序**：
  - `F0 Product Map/Shell`：完整四组导航、稳定 Route、页面职责、Capability Status 和 disabled 规则；
  - `F1 Existing Truth Wiring`：把现有 Query Approval/Audit、Registry/Knowledge、Report/Artifact、Model/Channel/System 接入新页面；
  - `F2 Future Surface`：Agents & Apps、Policy/Mandate、Quality/Conflict、Reusable Definition、Outcome 页面只完成真实结构和 planned/blocked 状态；
  - 后端阶段完成后再逐页从 `planned` 切为 `available`，不重做 IA。
- **明确不做**：不伪造 Agent Client、Decision、Policy、Quality 或 Outcome 记录；不创建前端业务状态库；不把所有 Admin 页面原样换皮；不在页面内实现第二套 Task/Approval/Report 状态机。
- **待用户决策**：选择完整未来产品壳、Human+Trust 优先，或只补当前主链。确认后才进入主动计划和前端实现。
- **用户决策**：选择“完整未来产品壳”。按四组 IA 一次性补齐稳定 Route、页面职责、Capability Status 和 disabled 规则；现有能力接真实入口，未来能力只展示 planned/blocked，不使用 fixture。
- **实施结果**：
  - Product Shell 一级导航已固定为工作/信任/接入/系统四组共 8 个入口；
  - 新增 `/deliverables`、`/governance`、`/runtime`、`/manage`、`/search`、`/inbox` 及 Decision/Evidence/Policy/Audit、Agent Client/API/Activity、Quality/Conflict、Reusable/Outcome 子 Route；
  - 交付接真实 Report Library/PDF/PPTX，治理接真实 Query Approval/Audit，数据资产接 Schema/Metric/Semantic/Registry/Staging/Knowledge，管理接 Team/Model/Skill/Channel/Database/Readiness；
  - 未实现能力统一显示 `available / partial / planned / blocked`、依赖阶段和不可用原因；Agent execute、Credential、通用 Decision、Policy PEP、Quality、Outcome、Reusable Definition 均未被伪装为可用；
  - 增加默认 Workspace 上下文、全局 Search 入口、待办 Inbox、Evidence Drawer/Diff Viewer 产品边界，以及统一 404/403/offline 页面。
- **候选与验证**：`product-spine-2ceffbcf1600` 已发布到 `http://preview.internal.invalid:18007/`，继续复用隔离状态和只读测试数据，生产 Forge/Pi 未替换。23 个有效/状态 Route、404 产品页、1440×900/1600×1000/390px、移动 8 项导航、现有 107 行 QueryResult Table 主链均通过；有效页面 0 console/page error、0 横向溢出。定向 `17 passed` + 浏览器行为 `1 passed`，JS syntax PASS。
- **剩余门禁**：完整未来 Product Shell 自动门禁 PASS，等待用户 Atlas `PASS / CHANGE / REMOVE`；前端完成不改变 Runtime Governance Coverage=0%。

## REQ-2026-08-25-021：Product Chat 缺少任务状态侧边栏

- **提出日期**：2026-08-25
- **当前状态**：`candidate_fix_verified_user_retest_pending`
- **用户反馈**：当前对话页面没有展示关联任务的状态侧边栏。
- **归属**：已批准 W1“Web 对话实时任务视图”和完整 Product Shell 的回归缺陷。Conversation 应承接连续交互，同时让用户看见当前关联 Task、Plan、状态、阻断和下一步。
- **简化 Bug 评估**：按 P0 处理。任务在后台推进但对话页不可见，会让 SQL 审批、执行、分析和报告阶段失去可观察性，并使用户误判系统卡住或任务完成。
- **验收边界**：必须复用 Pi Product Projection/Task Detail 真相源，只读展示当前 Conversation 最新关联 Task；不得恢复旧 Web 自建状态、重复轮询多个 raw Store、推进 Task 或暴露 Prompt/hidden CoT/Secret。桌面右栏与移动抽屉都必须可用。
- **根因**：完整 Product Shell 的 `product_chat.html` 只保留 Conversation Index 与 Conversation Workspace 两列；`product-pages.js` 只渲染 Conversation 与列表，没有消费已存在的 `/api/product/tasks/{task_run_id}` Task Detail Projection。W1 任务可观察面在 Shell 重建时被遗漏。
- **修复**：桌面 `/chat` 恢复第三列“当前任务”只读侧栏；切换 Conversation 或轮询时读取最新关联 Task Detail，并使用 epoch 丢弃旧请求。侧栏展示 Task 状态、计划进度、下一步 Action、Artifact/Evidence、最近 Activity 和任务详情深链接。
- **移动体验**：≤72rem 降级为右侧 Drawer；`aria-expanded`、backdrop、背景滚动锁定、Escape 关闭和 focus 返回均已实现。桌面侧栏常驻且不显示无效 Toggle。
- **真实复验**：Conversation `web_conv_66e18f83b37539de3637d8699df5b422` 展示完成态 Task、4 个 PlanStep、Artifact/Evidence 摘要和最近 Activity；1440×900 无横向溢出。390×844 Drawer 打开时 backdrop 可见且页面锁定滚动，Escape 后完全关闭；有效路径 0 console/page error。
- **候选与测试**：`product-spine-1de35ae9acc3` 已发布到稳定 URL，CSS `v5`、Product Pages JS `v5`。定向 `17 passed`，Product Conversation 浏览器行为 `2 passed`，JS syntax PASS；生产 Forge/Pi 未替换。
- **剩余门禁**：自动侧栏门禁 PASS，等待用户 Atlas 复验。

## REQ-2026-08-25-022：Chat 任务侧栏轮询时持续闪烁

- **提出日期**：2026-08-25
- **当前状态**：`candidate_fix_verified_user_retest_pending`
- **用户反馈**：任务状态侧栏会持续刷新闪烁。
- **归属**：`REQ-2026-08-25-021` 侧栏恢复后的 P0 体验回归。
- **简化 Bug 评估**：按 P0 处理。轮询用于保持可观察性，不能每轮清空并重建已稳定内容；持续闪烁会让任务状态不可读，并造成系统仍在重载或异常的错误感知。
- **验收边界**：同一 Task 的相同 Projection 轮询不得改变 Sidebar DOM；Projection 真实变化时才更新，保持滚动位置。后台刷新失败时保留最后一次有效状态，不以 loading/error 覆盖已显示内容。切换到不同 Task 时仍可显示首次 loading。
- **根因**：`loadConversationTask()` 在每次 2.5 秒轮询前都把已显示 Sidebar 替换成 loading，响应后再 `replaceChildren()` 重建全部内容；即使同一 Task 的 Product Projection 完全相同，DOM 也会被销毁重建。
- **修复**：为 Sidebar 建立不包含 `projection_meta.generated_at` 等易变字段的语义 fingerprint。相同 Task + 相同 fingerprint 直接 no-op；真实状态、Plan、Action、Artifact、Activity 或 Review 变化时才重绘。切换 Task 才显示首次 loading。
- **稳定性**：真实变化重绘前保存 `scrollTop`，完成后恢复；刷新失败且已有有效内容时保留最后状态并通过 aria-live 提示，不再覆盖为 loading/error；request epoch 继续阻止旧响应覆盖新 Task。
- **验证**：浏览器测试在同一 Task 至少两次轮询之间写入 DOM stability probe，确认节点身份不变；随后注入真实 Activity 变化，确认页面更新且滚动位置保持。桌面/移动 Drawer 与 QueryResult Table 回归继续通过。
- **候选与测试**：`product-spine-beb59d1a56f7` 已发布；Product Pages JS `v6`。资源/页面契约 `11 passed`，Product Conversation 浏览器行为 `2 passed`，JS syntax PASS。
- **剩余门禁**：自动无闪烁门禁 PASS，等待用户 Atlas 复验。

---

## REQ-2026-08-25-023：以任务驱动语义治理重排 Forge 下一阶段产品路线

- **提出日期**：2026-08-25
- **当前状态**：`accepted_with_changes`
- **原始需求**：用户确认 Forge 不能只依靠 JSON DSL 生产可靠 SQL；准确数据还依赖高质量元数据、业务语义、关系、Grain、歧义澄清和持续治理。完整前置治理成本过高，因此希望利用 AI 完成治理劳动并持续维护，同时处理人机与人人之间的语义分歧；在此目标下重新决定产品下一步。

### 澄清记录（2026-08-25）

- 用户认可“任务驱动语义治理 × 证据驱动可信执行”的长期方向，但明确要求进入新一轮产品讨论，不能把该长期机制直接当作短期路线。
- 后续必须分开回答：长期希望 Forge 成为什么；短期究竟为哪个人、在哪个重复场景中完成什么完整工作。
- 短期产品的验收不只是架构闭环或 Golden Journey 物理通过，还必须同时满足外部用户觉得好用、用户本人愿意持续使用；因此需要先明确首位使用者、首个高频 Job、首次价值时刻、可接受接入/治理成本和重复使用理由。
- 先前 F0–F5 阶段建议保留为待讨论候选，不视为用户确认，不进入主动 Plan。
- 本轮选择：第一使用者是用户本人；第一高频工作是“随时问业务数据”；第一条真实旅程使用用户自己的经营数据。由此短期产品必须先成为可持续 dogfood 的私有经营数据助手，而不是从企业管理员、Steward 后台或 Agent API 开始。
- 用户暂未选择“好用”的优先标准；首次价值、允许的打断频率、接入成本、结果交付形态和重复使用理由仍需下一轮结合真实经营问题确定，不能由现有架构指标代替。
- 用户进一步指出：个人经营数据量和企业代表性有限，单独使用可能无法验证企业级体量；但真实业务与本人高度相关，能产生实际提效、使用动机和产品体感，纯 Mock/Demo 无法建立同等信心。
- 评估结论是不能在“个人真实数据”和“企业级模拟数据”之间二选一。个人真实 Workspace 用于验证是否愿意持续使用、交互摩擦、语义纠正和复用价值；版本化 Enterprise Reference Workspace 用于验证规模、复杂 Join、脏数据、漂移、冲突、权限和可复算负向边界；后续真实 Design Partner 用于验证企业组织和采用代表性。三者提供不同证据，任何单一数据集都不能替代另外两类。
- 用户确认个人经营数据不仅规模小、收入和流量事件有限，而且分散在多个外部平台，统一采集本身成本较高。该事实削弱了“Founder Dogfood 作为短期主要产品真相”的前提：若为 dogfood 先建设大量 SaaS Connector/ETL，会把 Forge 错误地转向数据集成产品；若不集成，又缺少足够高频问题验证持续使用。
- 修订建议：个人数据降为 `Thin Founder Sandbox`，只在无需定制采集或可用现成导出/同步时验证交互、纠正和 Evidence；Enterprise Reference 继续负责确定性系统门禁；真实 Design Partner 从后续阶段前移为短期产品价值的主要证据。短期首位目标用户应重新评估为“已有数据库/数仓和真实问数需求的小型数据团队”，不要求 Forge 先解决源系统采集。

### 真实问题与目标结果

Forge 已有可信查询纵向切片和完整 Product Shell，但当前路线把长期企业 Trust Infrastructure、治理对象和未来页面同时展开，掩盖了最需要验证的产品机制：企业是否能在不先完成完整治理项目的前提下，通过真实数据任务发现最小语义缺口，由 AI 生成有来源的候选，经有权主体作出有作用域的决定，并让已确认 revision 立即约束后续编译、审批、执行和 Evidence。

目标不是建设全量企业知识库，也不是退回更准确的 Text-to-SQL，而是闭合两个共享 Registry 和 Evidence 的循环：

```text
可信执行循环：问题 → 语义绑定 → Forge IR → Assurance → Approval → Execution → Evidence
语义学习循环：缺口/冲突 → AI Proposal → Human Decision → Registry Revision → 后续任务复用
```

### 目标用户与首个场景

- 短期第一目标用户修订为：已经拥有数据库或数仓、存在真实临时问数需求、但语义治理不完整的小型数据团队。第一阶段只覆盖一个数据 Domain 和一名能够确认口径的 Data Owner/Steward，不要求大型企业完整治理或多租户上线。
- 短期核心 Job 是“随时提出真实业务问题，在不先完成完整 Registry 的前提下获得可追溯答案；必要时只处理会改变结果的最小语义缺口，并让确认结果被后续任务安全复用”。
- 产品价值主要由真实 Design Partner 的重复使用验证；版本化 Enterprise Reference Workspace 负责规模、复杂 Join、脏数据、漂移、冲突、权限和 Ground Truth 门禁；个人经营数据仅作为可选 `Thin Founder Sandbox`，不得驱动 Connector/ETL 范围扩张。
- 首个产品门禁是一条真实团队的完整旅程：连接一个现有可查询数据源，提出真实问题，完成最小澄清、可信执行、直接答案、按需 Evidence、语义纠正与复用；内部 `Semantic Gap Golden Journey` 是该体验的机制验证，不是用户产品定义。

### 价值与架构评估

- **用户价值**：把高成本、前置、集中式治理改为随真实任务发生的最小充分治理；第一次确认立即服务当前任务，后续复用降低澄清和人工维护成本。
- **产品公理**：符合“模型输出首先是 Claim”“AI 可承担治理劳动但不能获得治理 Authority”“不建立第二业务真相源”“Evidence 不足时诚实失败”。
- **职责归属**：Pi 继续负责 Conversation、Task、澄清和 Decision wait；Forge Registry/Assurance/Compiler/Executor 负责语义 revision、运行时绑定和可信执行；Skills 只提取、比较、解释和提交 Proposal，不绕过 Forge 或自行发布组织知识。
- **安全与隐私**：AI 自动采集限于已授权的 Schema、文档引用、查询历史和有界 Profile；不得回显 Secret、自动扩大数据范围、从一次对话发布全局知识或静默覆盖冲突定义。
- **复杂度与复用**：优先复用现有 definition mode、clarification、Registry Draft/Revision/Diff/Publish/Rollback、disambiguation、field convention、Task/Artifact/Evidence 和精确 Query Approval；首个切片不先抽象通用 Claim Store、Consensus Service 或独立 Memory Service。
- **机会成本**：本阶段暂停新增 Product Shell 页面、通用 Decision Center、Economics/Outcome Ledger、Reusable Report、更多渠道和非 SQL Action；继续扩张这些方向会延后核心假设验证。
- **不做的后果**：Forge 会停留在“已有治理结果上的可信执行器”，客户仍需先完成高成本治理；或退化为展示宏大对象模型但无法形成低成本治理飞轮的平台。

### 替代方案

1. **退回纯可信问数/SQL Agent**：交付边界小，但无法解决业务语义和数据质量上游约束，拒绝。
2. **先建设完整企业数据治理与知识平台**：覆盖广，但价值出现晚、接入成本高、与成熟数据目录/治理/知识产品正面重叠，拒绝。
3. **AI 自动生成并发布全部治理结果**：维护成本最低，但模型会获得事实和权力，冲突与错误可能直接污染运行时，拒绝。
4. **任务驱动渐进治理 + 运行时强绑定**：只处理当前任务的最小充分语义，AI 自动发现和提案，人负责高影响确认，确认结果立即进入 Runtime；建议采用。

### 已确认的短期阶段与门禁

1. **S0 Design Partner 与问题基线**：先确定一个符合边界的真实小型数据团队、一个数据 Domain、一个现有数据库/数仓、一名业务语义 Owner，以及一组真实历史问题和持续新增问题。未获得真实问题与合法测试边界前，不实施新 Runtime 能力。
2. **S1 Direct Trusted Answer**：围绕首批真实问题完成“连接现有数据源 → 直接提问 → 最小必要澄清 → 只读可信执行 → 业务答案/表格 → Evidence 按需展开”。默认不要求完整治理、完整报告或展示内部 Task/DAG；高风险和证据不足仍失败关闭。
3. **S2 Semantic Learning Loop**：让用户在 Conversation 中纠正口径并选择 task-local 或 Domain-level 作用域；AI 形成有来源 Proposal，Owner Review/Publish 后进入 Registry revision；第二次任务安全复用，语义或 Datasource drift 后旧绑定和旧审批失效。
4. **S3 三环境验证**：同一 Runtime 同时通过 Design Partner 真实重复使用、Enterprise Reference 确定性与负向门禁、可选 Thin Founder Sandbox 交互体验。个人数据不新增通用 Connector；Reference 不能代替真实用户价值。
5. **S4 短期产品退出门禁**：目标用户会在没有演示脚本驱动时再次提出真实问题；至少一次语义纠正被后续任务正确复用；至少一次歧义、证据不足或 drift 被正确停止；用户能从答案回到口径、数据范围、SQL 和 Evidence；新增人工治理成本能由复用或 Silent Error 减少解释。
6. **后续边界**：S4 通过后才重新评估 M1A、单一 Agent Consumer、更多数据域和企业 Trust Infrastructure。当前暂停新增 Product Shell 页面、通用 Decision Center、Economics/Outcome Ledger、Reusable Report、更多渠道、非 SQL Action 和通用 SaaS Connector。

### 首个切片的可证伪条件

- 正常任务仍要求企业先整理大范围 Registry 才能开始；
- AI Proposal 缺少来源、作用域、Owner、差异或影响说明；
- task-local 确认不能立即推进当前任务，或发布 revision 不能减少第二次澄清；
- Registry revision 未进入 DSL/Assurance/Approval/Evidence lineage，只停留在治理后台；
- 冲突定义被静默合并，或 AI 能未经授权发布组织级知识；
- Schema/语义漂移后旧审批继续执行；
- 人工治理成本没有随复用下降，且不能用 Silent Error 减少证明额外成本的价值。

### 建议结论

采用方案 4“任务驱动渐进治理 + 运行时强绑定”，并将短期产品收敛为：

> **面向已有数据库/数仓的小型数据团队的可信业务问数助手；不要求先完成完整数据治理，在真实提问中逐步沉淀和复用业务语义。**

长期“可信数据与知识底座”方向不变；短期产品价值以 Design Partner 为主要证据，Enterprise Reference 提供确定性系统证据，Thin Founder Sandbox 只提供辅助体验证据。Forge 当前不承担外部平台数据采集与通用 ETL。

### 用户确认

- **确认日期**：2026-08-25
- **决策**：`accepted_with_changes`。用户明确同意按照“已有数据库/数仓的小型数据团队 + Design Partner 主要证据 + Enterprise Reference 系统门禁 + Thin Founder Sandbox 辅助体验”的方向重新制定短期目标。
- **计划边界**：本次确认批准短期目标与阶段重排，不自动批准 S1–S3 Runtime 实现、真实客户数据接入、生产凭证、外部平台 Connector、M1A 或 Agent Runtime；各实施切片继续按需求池门禁单独评估和确认。


---

## REQ-2026-08-26-024：火山方舟 Coding Plan SQL Benchmark 实时监控

- **提出日期**：2026-08-26
- **当前状态**：`verified`
- **原始需求**：用户要求“用已经配置的火山方舟 Coding Plan benchmark Forge 的 SQL 准确性；开发一个 Web 页面实时监控测试进度和成绩，测试情况与数据实时同步”，并明确要求立即实施。

### 真实问题与目标结果

现有 `tests/accuracy` 已有 Ark Coding Plan 的 Method AI、40 题 large 数据集、三次重复生成和 EA 结果，但执行仍以 CLI 与落盘 JSON 为主。运行期间看不到可信的当前进度、部分成绩、分类表现、失败原因和模型/数据集边界，容易把历史结果或未完成结果误认为本次结论。

本需求交付一个内部 Accuracy Benchmark 控制与观测面：使用已配置的 Ark Coding Plan 运行现有 Method AI；Benchmark Runtime 持久化 run/case/call 状态；Web 页面实时投影同一份状态，持续显示进度、当前 EA、Run Accuracy、编译成功率、分类成绩、延迟和失败明细。页面不是第二套测试真相源，不修改模型绑定，不持有凭证，不把部分成绩伪装成最终成绩。

### 价值、边界与风险评估

- **用户价值**：让 SQL 准确性测试从黑盒 CLI 变成可观察实验；用户可以实时判断测试是否正常、当前成绩如何、错误集中在哪类问题。
- **产品一致性**：直接服务 Forge“准确；无法证明时真实不装”的核心个性。页面必须区分 running partial score 与 completed final score，并展示数据集、方法、模型 revision、运行次数和失败边界。
- **职责边界**：Benchmark Runtime 是测试任务真相源；Web 只创建、读取和订阅 Benchmark Run，不复制进度状态。该工作包不改变 Pi Task 真相源，不进入客户查询执行路径。
- **数据与安全**：只使用仓库内 Enterprise Reference large fixture；不接真实客户数据。API 和页面不得返回 API Key、Secret、原始 Provider 错误、隐藏 Prompt 或内部凭证路径。
- **运行风险**：Ark 请求存在额度、限流、超时和网络失败；运行必须保留失败状态、已完成 case 和可恢复观测，不自动重放已完成调用。
- **准确性风险**：40 题 EA 只能证明固定数据集、固定 Registry、固定模型和代码 revision 下的有界结果，不能宣称开放世界或真实企业 SQL 100% 准确。
- **机会成本**：新增一个内部观测页面和有界 Runtime；不借机扩张 Product Shell、Agent Runtime、Channel、Connector 或企业治理对象。

### 替代方案

1. **继续使用 CLI + 结束后查看 JSON**：实现最少，但不能满足实时进度和成绩同步，拒绝。
2. **页面直接轮询 `runs.json`**：开发快，但运行状态、并发、错误和最终性没有可靠契约，进程重启后容易产生歧义，拒绝。
3. **持久化 Benchmark Run + 实时只读投影**：Runner 每完成一次调用就原子记录；页面通过实时事件流接收同一 snapshot；符合单一真相源与失败关闭原则，采用。

### 已确认实施边界

- 默认测试现有 `method_ai`：Ark Coding Plan、large 40 题、每题 3 次、最多 2 次编译修复。
- 新建 Benchmark Run 时固定 method、dataset、model/revision、代码/Registry lineage 和总调用数。
- 每次调用完成后持久化 case/run 结果并增量更新 partial metrics；终态生成 final metrics。
- 页面实时显示：run 状态、总进度、API 调用进度、Case EA、Run Accuracy、编译成功率、分类成绩、最新完成项、失败项和耗时。
- 页面重连后从持久化 snapshot 恢复；不得依赖浏览器内存作为测试状态。
- 同一运行不得被重复启动；服务进程重启时将未完成 run 标记 interrupted，不自动重放外部模型调用。
- 页面明确标识“固定 Enterprise Reference Benchmark”，不得把 partial 或历史结果渲染为本次最终结论。

### 验收标准

1. 用户可以从 Web 页面启动一次 Method AI Benchmark，并看到本次 run ID、Coding Plan 模型、large 数据集和运行参数。
2. 测试执行期间，进度、已完成调用、已完成用例、部分 EA、Run Accuracy、编译失败和分类成绩无需刷新页面即可更新。
3. 每条 case/run 完成后，服务端持久状态与页面展示一致；重新打开页面可以恢复当前或最近 run。
4. 测试完成后显示 final 状态与最终 EA/Run Accuracy；失败或 interrupted 状态保留已完成证据并显示有界原因。
5. Web 响应不包含模型 Secret、API Key、隐藏 Prompt 或未去敏 Provider 错误。
6. 定向测试覆盖 run 状态转换、增量指标、实时流、重连恢复和终态；浏览器实际运行验证页面进度和成绩同步。

### 用户确认

- **确认日期**：2026-08-26
- **决策**：`accepted`。用户明确批准该有界内部 Benchmark Runtime 与实时 Web 页面，并要求立即实施。
- **计划边界**：只批准 Accuracy Benchmark 运行与观测，不批准新 Agent Runtime、客户数据接入、Connector、Channel 或 Product Shell 扩张。


### 实施与验证结果（2026-08-26）

- 新增持久化 Benchmark Runtime：SQLite 记录 run/case/call，原子推进 sequence；同一时刻只允许一个 active run；进程重启将未完成运行标记 interrupted，绝不自动重放模型调用。
- 新增受认证的 `/admin/benchmark` Accuracy Lab、JSON snapshot API 与 SSE 事件流。浏览器只投影服务端 snapshot；重连或服务重启后恢复最近结果。
- 页面显示部分/最终证据、调用/用例进度、Case EA、Run Accuracy、编译成功率、P95、分类成绩、事件流、40 题矩阵、模型/代码/Registry lineage 和固定数据集免责声明；API 不返回 Secret、API Key、Prompt 或原始 Provider 错误。
- 真实 Ark Coding Plan 运行：`abr_b410ab2b05ef40d88050b1b9be1eb097`，`ark-code-latest`，large 40 题 × 3 runs，120/120 调用完成。
- 最终成绩：Case EA `100.0% (40/40)`；Run Accuracy `98.3% (118/120)`；Compile Success `100.0% (120/120)`；P95 latency `58,945 ms`。
- 非全对用例：Case 23“品牌评价相对平均分偏差” `2/3`；Case 38“品类内 Top3 商品销售额占比” `2/3`。后者一次生成 SQL 可编译但执行比较失败；页面明确保留 mixed 状态，没有把 40/40 Case EA 呈现成每次都正确。
- 分类 Run Accuracy：窗口聚合 `93.3%`、综合复杂查询 `93.3%`；其余六类 `100.0%`。
- 验证：Benchmark + Web API 定向回归 `61 passed`；真实浏览器验证 1 → 27 → 72 → 112 → 120 调用实时推进，终态封存；服务重启后同一 run、120 calls、40 cases 和失败边界恢复；1440px 与 390px 页面无阻断视觉缺陷。
- 结果边界：该成绩只适用于当前 dirty code revision、固定 large fixture、固定 Registry、`ark-code-latest` 和 Method AI；不代表开放世界、真实客户或任意 Schema 下 SQL 100% 准确。


### 后续修订：Hard Benchmark 与 Forge / Direct SQL 对照（2026-08-26）

- **当前状态**：`verified`
- **用户原始补充**：页面需要实时日志输出和结果查看；现有 40 题可能过于简单，需要重新设计更难、但必须有答案的题，并配套结构层与语义层；使用同一模型直接生成 SQL 作为对照；重新设计 Web 页面并展示这些内容。用户指出这些能力过去已有设计，应优先复用而不是另造一套口径。
- **资产复用结论**：仓库已有 `tests/benchmark` 的 Forge vs Direct 双臂设计、`method_b_large_sem` 直接 SQL + 语义库对照、large 200-table Schema、Registry metrics/disambiguations/field conventions/relationships 和可执行 SQLite fixture。新实现复用这些方法论与资产。
- **公平对照**：Forge 与 Direct SQL 共享 Ark Coding Plan model revision、hard question、结构层、Oracle Evidence 和 SQLite 数据库，并使用同一 BIRD Execution Accuracy 判定。系统提示、输出格式、Forge 编译器与最多 2 次编译修复属于被测路径差异，必须披露，不能声称上下文完全相同。
- **Hard Dataset**：新增独立 hard suite。每题必须包含可执行 `reference_sql`、非空或业务上明确的答案、结构/语义依赖和难点标签；覆盖多 CTE、相关子查询、窗口、时序、占比、复购/留存、退款和复杂 Grain。
- **页面增强**：实时日志必须显示 run/method/case/stage/result；用户可打开题目查看问题、难度、结构依赖、语义定义、参考 SQL、Forge SQL、Direct SQL、执行结果摘要和有界错误。页面显示双臂实时成绩与差异，不只显示总分。
- **边界**：不使用真实客户数据；不以题目数量或复杂度伪装真实企业代表性；Reference SQL 必须先执行验证；页面不返回 Secret、Prompt 或未去敏 Provider 错误。


### Hard Benchmark 实施与验证结果（2026-08-26）

- 题目不再由本 Agent 生成。采用 BIRD-SQL 官方 Mini-Dev challenging split；来源：`https://bird-bench.github.io/`、`https://github.com/bird-bench/mini_dev`、`https://huggingface.co/datasets/birdsql/bird_mini_dev`，许可证 CC BY-SA 4.0。
- 当前 12 题均来自官方 challenging 记录，字段逐项核对无改写；但它只覆盖 challenging 的 12/102 和数据库的 2/11。原“Gold 可执行且非空”的选择说明不足以解释同两库另 6 道同样合格题为何未入选，已撤销并降级为固定诊断样本，不作为代表性或 leaderboard-comparable split。
- 结构层来自官方 `dev_tables.json` 与每表 `database_description/*.csv`；语义层来自每题 Oracle `evidence`；答案来自官方 Gold SQL。页面可查看完整问题、Evidence、结构/字段说明、Gold SQL/结果、Forge SQL/Forge JSON/结果和 Direct SQL/结果。
- 双臂共享 ark-code-latest、问题、结构层、Oracle Evidence 和 SQLite database；路径专属系统提示和 Forge 编译修复预算不同。评分仅比较同库执行结果，不比较 SQL 文本。
- NAS 生产部署：`internal operations entry` / `preview.internal.invalid`，最终源码 commit `f2e3755`，`forge-m41-api.service` 与 `forge-m41-pi.service` active；备份点 `~/services/forge-m4.1/backups/accuracy-bird-20260825T191348Z/`。Accuracy Lab 地址：`http://preview.internal.invalid:18001/admin/benchmark`，保持既有认证门禁。
- NAS 真实 run：`hbr_9a78d73cc64642709b03d4dc8aef978a`，72/72 调用、12/12 双臂用例完成，147 条持久实时日志。此前本机 run 只作开发诊断，不再作为部署验收结论。
- **EA 标准修订**：主判定严格复刻 BIRD 官方逻辑：set(gold_result_tuples) == set(predicted_result_tuples)。结果值与 tuple 列顺序精确比较；忽略行顺序和重复 multiplicity；不做数值误差、大小写或空白归一化。Execution Success 与延迟单列。
- 旧比较器使用 0.1% 相对误差、0.005 绝对误差与文本归一化，导致 72 次观测中的 11 个假阳性；旧的 Forge 30.56% / Direct 33.33% 与 Case EA 结论作废。
- NAS run hbr_9a78d73cc64642709b03d4dc8aef978a 官方 EA 重算：Forge Mean EA 5.56% (2/36)、First-run EA 0.00%、Pass@3 16.67%、Consistent@3 0.00%；Direct Mean EA 27.78% (10/36)、First-run EA 33.33%、Pass@3 50.00%、Consistent@3 8.33%。Direct 领先 22.22pp；Execution Success 与 P95 仍分别为 Forge 91.67% / 116,851 ms、Direct 100% / 23,518 ms。
- 页面主指标改为 Official BIRD EA；First-run EA、Pass@3、Executable 和 P95 分列，API projection 使用 scoring standard bird_execution_accuracy_exact_set_v1。历史观测在启动时重执行生成 SQL并迁移 verdict，避免把旧近似判断继续显示为官方 EA。
- 后续公共验证门禁：完整 Mini-Dev 500 题、11 个数据库、每题一次生成，以 Official EA 为主；102 道 challenging 全集只作难题切片。3-run 只报告 Mean EA / Pass@3 / Consistent@3 稳定性，不再把 Pass@3 命名为 Case EA。
- 结果边界：当前仍是提供 Oracle Evidence 的 12 题诊断子集；不代表完整 Mini-Dev、无 Evidence、其他数据库、任意 Schema 或真实客户环境准确率。

- **NAS EA 修订验证**：生产源码 commit e076573，API/Pi active，目标回归 8 passed，JavaScript syntax check 通过；72/72 历史观测写入 bird_execution_accuracy_exact_set_v1，最新 run 的独立 SQL 重执行得到 Forge 2/36、Direct 11/36，与持久 verdict 完全一致。页面 v4 资源 200，真实浏览器显示 Forge 5.56%、Direct 30.56%、Delta -25.00pp，无横向溢出或指标遮挡。
- NAS 三轮完整运行聚合：Forge 7/108 (6.48%)，Direct 28/108 (25.93%)。该聚合只说明固定 12 题诊断样本上的稳定性，仍不替代完整 Mini-Dev。

### 后续修订：完整 EA 数据看板（2026-08-26）

- **用户原始补充**：继续完善功能，但不要启动 Benchmark 模型测试；功能本身需要测好。前端不要花哨，直接采用实时数据看板形态，提供详细图表与日志明细，使用户能在页面完成分析。
- **实施边界**：接入完整 BIRD Mini-Dev 500 题与 11 个数据库作为下一正式套件；默认每题每臂一次，共 1000 次模型调用。当前只完成资产、运行契约、分析接口和页面，不创建新 run、不调用模型；未来启动必须在页面明确确认 1000 次外部模型调用。
- **看板信息架构**：克制的浅色运维看板；首屏显示 Official EA、First-run EA、执行成功率、延迟和差值；图表覆盖历史 EA 趋势、当前运行累计 EA、逐题双臂命中、延迟分布和错误构成；日志支持 method/stage/level/case/search 筛选与分页。
- **数据真相源**：图表、日志和逐题详情全部来自 Benchmark Store/API/SSE，不在浏览器生成第二套结果；SQL 仅作诊断详情，不参与 EA。
- **验收**：NAS 接口与真实浏览器验证通过，桌面和移动无横向溢出；确认 hard_benchmark_runs 与 observations 数量在本次改造前后不增加，证明未触发模型测试。

- **实施结果**：NAS commit 4056986。完整官方资产 800,943,648 bytes 已下载并展开为 11 个 SQLite 数据库；suite preview 返回 500 cases、11 databases、1000 expected model calls。
- **分析接口**：新增 hard run history 与日志筛选分页；snapshot v3 按 run.suite_id 绑定完整或历史套件，并为 observation 投影 completed_at，供累计 EA 与延迟图表使用。
- **页面结果**：真实浏览器显示历史 EA、累计 EA、延迟分布、结果构成、逐题双臂分析和 100 条分页日志；method/stage 筛选得到 36/36 条 Forge evaluated 日志，逐题详情含 Question、Evidence、Gold SQL/结果和双臂 SQL/结果。
- **安全与验证**：未确认启动请求返回 409 与 expected_model_calls=1000；确认弹窗明确显示 500 题、11 库、1000 次调用。本次未点击确认。NAS 9 passed，JS syntax 通过，API/Pi active，源码 clean；部署前后均为 6 runs、246 observations、0 active。

### 缺陷修订：完整套件启动卡住（2026-08-26）

- **用户报告**：在 Web 上启动完整测试后页面卡住。
- **复现证据**：API health 3 秒无响应；主进程持续占用 CPU；Benchmark Store 没有新增 run，仍为 6 runs / 246 observations / 0 active，证明阻塞发生在模型调用和 run 持久化之前。
- **根因**：start_hard_benchmark_run 在 async 请求内同步调用 create_hard_run；create_hard_run 先顺序执行 500 条 Gold SQL，遇到 codebase_community 长查询后长期占用事件循环。execute_result 没有 SQLite progress timeout，并使用不会自动 close connection 的 Connection context manager。
- **修复**：创建请求只加载元数据并先写 queued run；Gold SQL 预检移入 asyncio.to_thread 后台运行阶段。预检 4-way 并行、单 SQL 30 秒超时、显式 finally close、失败取消 pending futures；每 10 题持久化 gold_validation 进度，预检失败则标记 failed 且不调用模型。Snapshot 读取完整套件不再触发同步 Gold 校验。
- **恢复与验证**：旧阻塞进程无法响应 SIGTERM，确认无 run/observation 后由 systemd 定向 SIGKILL 并恢复 API。NAS 11 passed、JS syntax 通过；health 200 / 3.5ms，真实浏览器未确认启动 409 / 31ms；生产 head b9d6673 clean，API/Pi active。原请求没有可恢复 run，未自动重放，仍为 6 runs / 246 observations / 0 active。

### 候选改造：Pi-native RAG 双 Sub-Agent Benchmark（2026-08-26）

- **用户原始表达**：Benchmark 应基于已集成的 Pi Agent。每个 case 先执行一次 RAG 任务，使用原始自然语言、分析出的候选表/字段语义与 RAG 召回结果；召回不足时调整参数直到满足查询需要。冻结同一上下文后，并行派生两个 Sub-Agent：Forge JSON 路径与 Direct SQL 路径。两边生成 SQL、执行、与标准答案做结合原始问题语义的结果比较。页面展示完整 DAG；RAG 节点下方并排双实时日志；关键过程结果在对比表实时更新；保留关键卡片、进度、当前问题、供应商/模型；支持暂停和停止；固定使用 deepseek-v4-flash，不再使用火山 auto 模型。用户要求在此基础上扩展遗漏需求并先制定实施计划。
- **核心意图**：不只比较最终 EA，而是把 Retrieval → Context Sufficiency → Generation/Compile → Execution → Evaluation 的每层质量、成本、速度和失败暴露出来，能够定位 Forge 相对 Direct 的真实增益或损失发生在哪一层。
- **稳定架构约束**：Pi 仍是唯一 Orchestrator 和 Task 真相源；RAG、Forge JSON、Direct SQL 是同一 Benchmark TaskRun 下的 Stage/Child Attempt，不新增 Python 第二调度器。Forge 仍是唯一可信编译、只读校验与执行层；Sub-Agent 不直接获得数据库执行权。
- **公平性约束**：RAG/分析产出冻结成同一 ContextSnapshot，同时提供给两个生成分支。两臂固定同一 deepseek-v4-flash Model Revision、temperature、上下文、数据库快照和首轮输出预算；路径专属 Prompt、Forge 编译器和修复行为单独记账。Primary 同时报告 First-attempt A/B 与 Product-path A/B，避免 Forge retry 预算混入模型能力比较。
- **召回约束**：RAG 使用有界迭代而非“直到召回够数量”为止。每轮持久化 query、top_k、允许表、命中表/字段、score、FK 扩展和 sufficiency verdict；最多固定轮数。仍不足则 needs_clarification / retrieval_insufficient，失败关闭，不靠无限扩大 top_k 把全库塞入上下文。
- **评价约束**：保留 Official BIRD EA 作为可比较主指标；新增 Result Contract / Semantic Accuracy 作为解释指标。先从原始问题生成不可变 ResultContract（必需列语义、重复语义、排序是否有业务意义、Top-N、精度/舍入、NULL），再执行确定性列对齐与 multiset 对比。LLM adjudicator 只处理确定性比较无法判定的 case，必须输出证据，不能覆盖 Official EA。
- **运行控制**：pause 只阻止调度新 case，等待 in-flight StageAttempt 到安全点后进入 paused；stop 取消 queued、向 in-flight 传播 AbortSignal，迟到结果保留但标记 after_stop，不写成正常完成。控制命令幂等、CAS 状态转换、重启后保持 paused/stopped，不自动重放模型调用。
- **可观测要求**：每个节点记录 start/end、attempt、输入/输出 hash、ContextSnapshot ID、模型 revision、token usage、latency、error taxonomy 和安全摘要；不保存 Secret、hidden CoT、原始 Provider 错误或未去敏 Prompt。页面实时显示 DAG、RAG 轮次、双日志、case 对比表、当前问题、速度、tokens、compile/execute/evaluate 状态以及按数据库/难度/SQL 特征/RAG 覆盖/错误类型的图表。
- **当前基线**：完整 run hbr_3e28c9b723c3469eb14cd5614d0e0ca4 已完成 1000/1000。当前仍是 ark-code-latest：Forge 231/500、Direct 275/500；Forge execution failures 37、compile failures 2，Direct execution failures 12；P95 分别 31.27s / 14.95s。该 run 只作改造前基线，不满足 Pi-native、RAG lineage、token 或 deepseek-v4-flash 条件。
- **主要风险与替代方案**：直接在现有 Python ThreadPool 上加“伪 Sub-Agent”会制造第二调度器且暂停/恢复不可证明，应拒绝；用 LLM Judge 取代 Official EA 会引入不可审计主观误判，应只作有界 adjudication；每臂独立 RAG 会破坏公平，应共享冻结召回；使用 mutable model alias 会破坏复现，应绑定已验证 revision。deepseek-v4-flash 历史质量门禁仅 20% 且当前无 ActiveBinding，必须先通过 Pi Artifact-first readiness 与 Benchmark 专用绑定，不能绕过 Model Control Gate 强行激活。
- **机会成本**：该改造会暂缓继续优化单一 Forge Prompt/Compiler；收益是建立可定位、可暂停、可复现的 Benchmark 平台，后续模型、RAG 和编译器优化都能在同一证据链上比较。

### Pi-native Benchmark 实施结果（2026-08-26）

- 新增 Benchmark v2 Contract、ResultContract、确定性列 permutation / multiset / 有条件排序和显式舍入比较；Official EA 保持独立主指标。
- 新增字段级 RAG、FK 扩展、top_k 5/10/20 有界轮次、Context Sufficiency、共享不可变 ContextSnapshot hash；Gold SQL/Result 不进入生成上下文。
- Pi Runtime 使用根 TaskRun + Case scheduler；每个 Case 真实创建两个无工具 AgentSession 并 Promise.all 并行。Provider/Model 从 ready catalog 选择并在 Run 中固定 revision，不硬编码 deepseek。
- 新增持久 Run/Case/Log、tokens、latency、compile、execution、Official EA、Contract Accuracy 和 failure layer；新增 pause/resume/stop，Case claim 防重复，重启中断不自动重放。
- Web 新增供应商/模型/用例规模选择、当前 Case、七张核心卡片、真实 DAG、RAG 详情、Forge/Direct 并排日志、实时 Case 对比表、四组诊断图和控制按钮。
- Python /runs 与 /hard-runs POST 已退役为 410；历史 GET、日志和旧 Run 保持只读。
- 自动验证：本地 Pi 117 tests passed；生产基线 Pi 105 tests passed；Python Benchmark v2 定向 10 passed；JS syntax 通过。
- 真实验证：deepseek-v4-flash 2-case Canary 完成 4/4；补充完整 Forge DSL system prompt 后，Forge JSON 可以编译并执行。Pause/Resume 3-case 验证完成 6/6 且无重复；Stop 3-case 验证停止于 4 calls，1 Case 保持 pending。
- 完整 Run pbr_1f735d433a284366bfe6526146511792 已完成 500 cases / 1000 calls，固定 openai/deepseek-v4-flash revision sha256:f75be09a。固定 500 分母结果：Forge EA 45.40%、Contract 39.80%、Execution 73.00%、Tokens 3,506,756、平均生成 29.11s；Direct EA 56.40%、Contract 50.80%、Execution 91.20%、Tokens 2,386,708、平均生成 16.46s；Delta -11.00pp。运行持久化 500 Case 和 3,002 logs。

- **用户后续纠正**：模型供应商和模型都必须在每次测试前可调整，不能把 deepseek-v4-flash 硬编码为唯一模型。实现已改为读取 Pi ModelRuntime ready catalog；页面联动选择 Provider/Model，Run 创建时验证 readiness 并冻结 provider/model/catalog revision。deepseek-v4-flash 只是本轮验收所选模型，不修改生产 ActiveBinding。

### Benchmark 页面可视化与日志修订（2026-08-26）

- **用户后续反馈**：供应商与模型应合并选择，并默认选中最近一次运行模型；Benchmark 页面整体中文化、克制且紧凑；关键指标、DAG、日志和图表尽量首屏可见；用例表只占约三分之二宽度，余下空间承载更丰富的堆叠趋势、流程损失和失败构成；日志要展示 RAG 与 Sub-Agent 的真实过程，而不是只有最终结果。
- **页面实现**：模型选择改为按 Provider 分组的单一 Selector，共投影 ready catalog 的 40 个模型，并在载入最近 Run 后同步选中其冻结模型。管理导航、状态、控制、指标和诊断文案中文化；BIRD 原始问题保持原文，避免修改 Benchmark 语义。
- **信息架构**：桌面首屏同时展示 7 个核心指标、Pi DAG、Forge/Direct 双日志、共享日志和准确率堆叠趋势；用例对比表固定约 66.2% 宽，右侧展示流程损失桑基图与 Forge/Direct 失败圆环；下方保留准确率分层、Token/速度和数据库维度。移动端改为 2 列紧凑指标卡，表格在 356px 有界容器内横向滚动，图表不再撑破页面。
- **视觉与动效**：采用 Forge 绿、Direct 紫、召回青、错误珊瑚的语义配色；面板进入、进度、柱状增长和桑基流线只使用短时 transform/opacity/路径动效；prefers-reduced-motion 下关闭动效并保留实色桑基流线，不因减弱动效丢失信息。
- **详细日志**：每个 Case 持久记录 RAG 轮次的 top_k、表/字段、概念覆盖率和充分性；两条生成分支记录模型 revision、AgentSession、Prompt 安全摘要、首个流式事件、每 250 个片段的有界采样、响应字符与 tokens、输出解析、Forge 评价请求和最终 EA/Contract。日志不保存 Prompt 正文、Secret、hidden CoT 或未去敏 Provider 错误。
- **真实冒烟**：生产 Run pbr_f197173128514a458fd5654d9a299492 使用 openai/deepseek-v4-flash 完成 1 case / 2 calls；共享日志包含 rag.round，双臂日志包含 generation.model/session/prompt/stream/completed、output.parse、evaluation.request/evaluation。Forge 执行成功但 EA/Contract 为 0，Direct EA/Contract 为 1，页面没有隐藏该差异。
- **验证与部署**：本地 Pi 117 passed；NAS 生产基线 Pi 105 passed；Python 定向 10 passed；JS syntax 通过。1440px 页面 0 横向溢出、表格宽度占比 0.662、2 个图例、8 条桑基流线、2 个失败圆环和 122 条历史日志成功渲染；390px 页面 0 横向溢出，表格/桑基/圆环均约束在 356px。视觉复核通过。生产 head dee4f82，API/Pi active，源码 clean；部署前备份位于 ~/services/forge-m4.1/backups/benchmark-ui-20260826/。


---

## REQ-2026-09-03-025：以开源 Trust Runtime 收敛 Forge 产品方向

- **提出日期**：2026-09-03
- **当前状态**：`accepted_with_changes`
- **用户原始表达**：用户在重新审视 Forge 后，将未来 3–6 个月的产品角色选择为“企业可信数据平台”，首要成功证据选择为“开源影响力增长”；在进一步讨论后确认，Forge 应成为企业 Data Agent 共用的可信执行边界，而不是继续扩张为宽泛的问数应用。

### 真实问题与目标结果

Forge 已形成 Registry、Compiler、Assurance、只读执行、QueryRun、Approval、Evidence、Audit、Pi Task 和 Benchmark 等工程资产，但尚未形成与其复杂度匹配的外部采用证据。完整 BIRD Mini-Dev 对照中，Forge JSON 路径的 Official EA、Execution Success、Token 和延迟均弱于同模型 Direct SQL，否证了“Forge JSON 天然比直接 SQL 更准确”作为近期产品身份的前提。同时，自然语言问数、语义层和 Agent Analytics 已由仓库原生平台与成熟开源项目广泛覆盖。

目标是保留“企业可信数据平台”的长期身份，同时把近期入口收敛为开发者可理解、可独立采用的开源 Trust Runtime：让既有 Data Agent 在访问数据库时可验证、可约束、可追溯，而不是要求用户先迁移到另一套 Chat、BI 或治理后台。

### 已确认产品定义

> **Forge 是面向企业 Data Agent 的开源可信数据运行时：在 Agent 与数据库之间完成有界语义绑定、策略与安全校验、可信执行、Evidence 封装、Audit 和回归评测。**

近期产品承诺分为三条路径：

1. **Evaluate**：导入真实问题、标准结果或已有 Agent 输出，对模型、Prompt、RAG、语义和方法版本执行可复现 A/B、Exact Result Comparison、失败分层和回归门禁。
2. **Enforce**：在运行时绑定 Principal、Purpose、Task、Policy 和 Resource Scope；执行只读、安全、审批与 drift 检查，证据不足时失败关闭。
3. **Explain**：返回结果及其语义、数据源/快照、实际 SQL、版本、限制、Policy/Assurance、Approval、Evidence 和 lineage。

### 目标用户与首个 Job

- 第一用户：正在建设或维护内部 Data Agent 的 Data/AI Engineer、数据架构师和数据平台团队。
- 首个 Job：模型、Prompt、RAG、语义层或 Agent 流程发生变化后，在上线前发现结果回归、Silent Error 和权限风险，并留下可复算发布证据。
- 普通业务问数用户仍可通过上游 Agent 或 Human Control Plane 使用 Forge，但不再是近期安装、文档和开源采用的第一入口。

### 架构与实施边界

- Forge JSON 降为可替换 Planner Adapter；Direct SQL 必须成为一等输入，后续 MAY 接入 Semantic Query。所有路径共享 Forge Assurance、Executor、Evidence 和 Audit。
- Pi 继续是默认部署中的唯一主 Orchestrator 和 Task 真相源；本需求不批准第二调度器或旁路执行权。
- Forge 继续保留独立校验、拒绝和失败关闭能力；上游 Agent、Skills、Chat 和 MCP Client 不直接获得数据库执行权。
- 优先复用现有 Benchmark Runtime、Registry、Assurance、Compiler、QueryRun、Approval、Evidence 和 Audit；不以重新设计全部平台对象作为启动条件。
- 当前暂停新增通用 Product Shell 页面、报告 Renderer、SaaS Connector、非 SQL Action、Economics/Outcome Ledger 和完整企业权限平台。
- 不接真实客户数据、生产凭证或高风险数据源，除非另立需求并完成隐私、授权和运行门禁。

### 开源采用门禁

- 新用户可以从公开 README 和 Quickstart 独立完成“现有 Agent/样例输出 → Evaluate → 失败定位 → Policy Gate → Evidence”的单一路径。
- 上游 Agent 不需要采用 Forge JSON 才能获得验证、执行和 Evidence 能力。
- 公共 Benchmark 必须透明披露数据集、模型、上下文、评分、失败和版本边界，不能用自有题集成绩替代公共泛化证据。
- 首轮采用证据优先观察外部独立 Quickstart、真实 failure case、Adapter/Rule/Dataset 贡献和下游集成；stars/forks 是传播指标，不替代实际运行证据。

### 替代方案与机会成本

1. **继续做完整可信问数应用**：体验直观，但与 Snowflake、Databricks、Cube、WrenAI 等正面竞争，且要求 Connector、语义维护和业务用户分发同时成立；不作为近期主线。
2. **只做 Benchmark 内容项目**：能提升传播，但无法兑现 Runtime 与可信执行资产；保留为开源增长手段，不作为产品终局。
3. **直接建设完整企业 Trust Platform**：长期想象力最大，但会在采用证据前扩张身份、权限、治理、HA 和销售复杂度；拒绝立即全面实施。
4. **开源 Trust Runtime 切入、企业平台演进**：先以 Evaluate/Enforce/Explain 建立开发者采用，再由真实消费者证据批准更广企业能力；采用。

### 用户确认

- **确认日期**：2026-09-03
- **决策**：`accepted_with_changes`。用户选择“企业可信数据平台 + 开源影响力增长”，并确认以“所有 Data Agent 共用的可信执行边界”作为近期切入口。
- **计划边界**：批准产品定位与主动计划重排；不自动批准真实客户接入、生产凭证、通用 Connector、非 SQL Action、完整企业权限系统或新的独立 Runtime 服务。

### R0.1 实施证据（2026-09-03）

- 新增共享 `query-candidate-v1` JSON Schema：`direct_sql` 与 `forge_json` 是互斥输入，均可记录 producer revision。
- Direct SQL 不转换为 Forge JSON；它在服务端通过只读、SQL 解析、Registry/ACL 和字段校验后，与 Forge JSON 进入同一 QueryRun 审批、执行及 hash/revision lineage。
- QueryRun 和 Pi 事件持久传递 `input_kind`、candidate revision；相同 SQL 的两条路径共享 SQL hash、assurance/policy/registry/candidate revision，输入来源仍可区分。
- 非只读 SQL、未授权表、未知字段、歧义或附加字段候选均失败关闭；既有自然语言 → Forge JSON 路径保持兼容。
- 验证：Python 全套 `612 passed / 28 skipped`；Pi `118 passed`；TypeScript typecheck 通过。该证据只关闭 R0.1，不代表 R0 Golden Path 或外部采用门禁已通过。
