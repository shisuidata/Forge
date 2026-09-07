# Forge 需求池

> 状态：追加式需求与决策真相源 · Last updated: 2026-09-04
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

### R0.2 Evaluate 实施与关闭证据（2026-09-03）

- 新增版本化 `POST /api/v1/evaluate` 与 `forge evaluate`；Direct SQL 和 Forge JSON 使用同一请求/响应信封，上游调用方不需要理解 Pi 或迁移到 Forge JSON。
- Evaluate 复用 `query-candidate-v1`、Registry Assurance 与 `benchmark-failure-v1`，输出 Policy verdict、有界失败分类、编译 SQL、request/SQL/Assurance lineage 和响应内 Evidence refs；非法、非只读、越界和结果不一致均失败关闭。
- 调用方可提交 expected/actual result 执行 Exact Result Comparison；入口不连接数据源、不执行 SQL，`execution_authorized` 固定为 false，`allowed_tables` 仅是本次评测条件而非授权。
- 新增 `evaluation-suite-v1` 与 `evaluation-run-manifest-v1`：持久化完整 suite、dataset/producer/prompt/retrieval/retry/timeout/evaluator/metric/Registry 等修订、原始 case outcomes 和可复算 aggregate；可按 suite revision 回放，按 run ID 导出。
- 跨 producer/model/Prompt 版本可使用 baseline release gate；dataset、case selection、evaluation basis、Policy、evaluator、Registry 或 dialect 不一致时明确标记 `not_comparable` 并失败关闭。默认不允许新增失败或 pass rate 下降。
- README、Benchmark 文档和公开本地 fixture `examples/evaluation-suite-v1.json` 已提供完整 CLI 路径。持久运行、suite revision 回放、baseline gate 和 manifest 导出实际 CLI/API smoke 均通过；R0.2 聚焦测试 `42 passed`，Python 全套 `649 passed / 28 skipped`。
- 该证据关闭 R0.2 Evaluate；R0 Golden Path、外部独立 Quickstart 与真实采用证据仍未关闭。

### R0.3 Enforce 实施与关闭证据（2026-09-03）

- 新增 `enforce-query-request-v1`、`enforce-query-approval-v1` 与 `enforce-query-response-v1`，公开输入显式绑定 Principal、Purpose、Task、可选 DelegatedMandate、Resource Scope、candidate 与 dialect。
- 新增 `POST /api/v1/enforce/query-runs`、`GET /api/v1/enforce/query-runs/{query_run_id}`、`POST /api/v1/enforce/query-runs/{query_run_id}/approve` 和 `forge enforce`；复用现有 QueryRun 存储与状态机，不创建第二套执行真相源。
- Human 直接调用必须自行承担责任；Agent/Service 调用必须提供唯一、有效且与 actor、accountable human、task、purpose、audience、capability、scope 和 budget 匹配的 mandate。Resource Scope 只支持当前 datasource 与可选 table 子集，不能扩张配置中的数据源或租户 ACL。
- 创建阶段只准备并持久化候选、实际 SQL、Assurance、Policy 和全部上下文 hashes，不执行数据库；审批必须来自独立 reviewer credential，并匹配 accountable human、SQL hash、Assurance hash 与 Enforcement Context hash。重复审批不会重复执行。
- 执行前重新校验 Principal/Mandate 有效期、Policy/Registry/Assurance/candidate/resource hashes、执行开关和只读凭证；任何 drift 或异常均失败关闭并返回有界失败，不泄漏原始数据库错误。
- Governance Action Catalog 升至 v1.2.0；仅 `query.prepare`、`query.approve`、`query.execute` 标记为 runtime `enforced`，覆盖 3/14（21.4%），不得外推为其余 Action 已治理。
- 聚焦回归 `67 passed`，Python 全套 `663 passed / 28 skipped`，Pi `118 passed`，TypeScript typecheck 通过。真实本地服务经 CLI 完成创建 → hash-bound 审批执行 → GET 回读，结果按 2 行上限截断；R0.3 关闭，当前进入 R0.4 Explain。
- 该证据不关闭完整 IAM、生产部署、R0.4 Explain、R0.5 Public Golden Path 或 R0.6 外部采用门禁。

### R0.4 Explain 实施与关闭证据（2026-09-03）

- 新增稳定 `explain-query-response-v1`、`GET /api/v1/explain/query-runs/{query_run_id}` 与 `forge explain`，覆盖 `review_required`、`denied`、`executing`、`completed`、`failed`、`cancelled`、`expired` QueryRun 状态。
- Explain 复用 QueryRun 作为唯一执行真相源，不创建 Artifact 旁路或第二状态库；公开投影收敛结果、实际 SQL、候选、Registry 表列语义、数据源/资源范围、Principal/Purpose/Policy/Approval、Assurance、版本、Evidence、lineage、integrity 与显式 limitations。
- QueryRun 创建时固化来源/语义上下文，审批和完成时分别固化 approval/result hash；candidate、SQL、Assurance、Policy、Enforcement Context、source context、approval 或 result 漂移时返回有界错误并失败关闭。当前 Registry 后续变化不改写历史 Explain。
- 历史未固化 Explain/approval/result hash 的 QueryRun 不伪造已验证证据，只返回 `integrity.status=partial`、未验证组件和对应限制；所有响应明确披露无数据库 snapshot、语义正确性未被证明、未完成执行或结果截断等适用边界。
- Explain 读取绑定创建 QueryRun 的凭证，不公开 session hash、内部 SQLite 结构或原始数据库错误；独立 reviewer credential 无权回读创建凭证的 Explain。
- Explain/Enforce 聚焦回归 `24 passed`，Python 全套 `673 passed / 28 skipped`，Pi `118 passed`，TypeScript typecheck 与 Python compileall 通过。真实本地服务经 CLI 完成 Enforce 创建 → hash-bound 审批执行 → Explain，验证 2 行截断结果、7 类 Evidence 和 3 项显式限制。
- Explain 是只读证据投影，不新增 Governance Action Catalog Action 或 Runtime Enforcement；覆盖仍为 3/14（21.4%）。R0.4 关闭，当前进入 R0.5 Public Golden Path；完整 IAM、生产部署和 R0.6 外部采用门禁仍未关闭。

### R0.5 Public Golden Path 实施与关闭证据（2026-09-03）

- 默认英文 README 与中文入口都把 `forge quickstart` 作为首个可运行路径；开发者无需 API Key、LLM、Embedding、Pi、Forge JSON、已有数据库或 `.env` 即可完成 Direct SQL Trust Runtime 链路。
- Quickstart 创建隔离合成 SQLite/Registry 并启动真实 Forge 服务，只调用公开 Evaluate → Enforce → Explain HTTP API；默认展示实际 SQL 并等待人工批准，`--yes --json` 用于合成数据 CI，`--workdir` 保留数据库、QueryRun、日志与摘要，`--serve` 保持 Dashboard 可浏览。
- Dashboard 不创建第二执行状态，只读取最近的 Enforce QueryRun 并通过同一 Explain 投影显示 question、执行状态、Evidence 数、limitations 数与 integrity；证据漂移显示有界错误并失败关闭。
- 实际 `forge quickstart --yes --serve` 验证 exact-result comparison、review stop、hash-bound approval、只读两行上限截断、七类 Evidence、三项显式限制和同一 QueryRun Dashboard 投影。Browser 在 1440px 与 390px 实际页面确认 Golden Path、治理运行可见且无水平溢出。
- R0.5 聚焦回归 `69 passed`，Python 全套 `675 passed / 28 skipped`，Pi `118 passed`，TypeScript typecheck 与 Python compileall 通过。
- Runtime Governance Coverage 仍为 3/14（21.4%）；内部 Quickstart 与 Browser smoke 只关闭工程收敛，不构成外部采用。R0.5 关闭，当前进入 R0.6 External Adoption Evidence；完整 IAM、生产部署和更广平台扩张仍未关闭。

### R0.6 External Adoption Evidence 准备与当前阻塞（2026-09-03）

- 公开 GitHub 信号盘点显示：现有 9 个 Issue 与 1 个 Pull Request 均由维护者身份提交；11 stars 与 1 fork 仅是传播指标。当前没有可确认的非维护者 Golden Path 回执或 Adapter、Rule、Dataset、真实 failure case、下游集成贡献。Issue #9 是维护者创建的外部试跑招募入口，本身不计入采用证据。
- `forge quickstart` 现在先提交写 SQL，并要求公开 Evaluate 以 `stage=assurance`、`code=readonly_violation` 失败关闭；随后才运行已审核的只读 Evaluate → Enforce → Explain → Dashboard 链路，避免成功路径掩盖拒绝边界。
- `summary.json` 新增 `run_receipt`：记录 Forge version、OS/arch/Python、起止时间、运行时长及有界阶段结果，并用 canonical JSON 的 SHA-256 checksum 检测回执漂移。回执排除 hostname、username、文件路径、SQL rows、凭证和私有 schema；Forge 不发送 telemetry。Checksum 不证明身份，公开 GitHub 提交者才提供 provenance。
- 新增 Quickstart adoption Issue 表单，并在 README、中文 README、CONTRIBUTING 和 CLI 终态指向同一提交入口。表单要求 tested release/commit、fresh-clone setup time、首个失败或困惑步骤、回执、开发者对 Policy verdict/Evidence integrity/limitations 的独立解释，以及 fresh clone、非实现者、SQL 审核和无敏感信息确认。
- 实际人工批准与 `--yes --json` Quickstart 均完成失败关闭、exact-result Evaluate、hash-bound Enforce、七类 Evidence、三项限制和 Dashboard 投影；聚焦回归 `14 passed`，Python 全套 `676 passed / 28 skipped`，Pi `118 passed`，TypeScript typecheck、Python compileall 与 Issue YAML 结构检查通过。另从公开 HTTPS 远端全新克隆 `main@5bbdabe5ceca10fd7128a825d277e5e4534d69e7`，在空工作目录完成 bootstrap + Quickstart 共 36 秒并重算 receipt checksum 一致。
- 上述代码与入口已发布，[Issue #9](https://github.com/shisuidata/Forge/issues/9) 已公开招募独立试跑，#8 保持为 framework-neutral external-agent adapter 贡献入口；但发布、维护者 fresh-clone 和维护者 Issue 都不是外部采用证据。当前仍没有非维护者回执，R0.6 与 R0 退出门禁保持未通过。下一动作是等待未参与实现的外部开发者独立试跑、提交回执和摩擦；若失败，必须完成 issue → fix → rerun 闭环后再评估门禁。

---

## REQ-2026-09-03-026：默认英文 README 与中文本地化入口

- **提出日期**：2026-09-03
- **当前状态**：`verified`
- **用户原始表达**：Forge 应该更加国际化，例如默认使用英文 README。

### 真实问题与目标结果

Forge 的公开定位面向国际开源开发者，但 GitHub 仓库首页默认展示中文，英文版本位于非默认文件；同时存在一份过期的中文 README 副本，容易形成两个中文事实源。目标是让国际访问者默认看到英文项目定位、Quickstart、能力边界和贡献入口，同时保留完整、易发现且与英文版对应的简体中文入口。

### 评估与实施边界

- **用户价值**：高；减少国际开发者理解成本，并使英文项目描述、Topics、Release 与仓库首页保持一致。
- **职责边界**：只调整公开文档入口与语言导航，不改变 Forge 产品定义、Runtime Contract、主动计划或工程行为。
- **一致性要求**：根 `README.md` 为英文；中文使用明确 locale 文件 `README.zh-CN.md`；两者首屏互链；移除 `README_EN.md` 和过期 `README_CN.md`，避免并行事实源。
- **风险**：文档重命名可能产生失效链接；必须更新仓库内引用并运行文档链接检查。
- **替代方案**：保留中文根 README、仅增加英文链接，无法改变 GitHub 默认呈现；保留三个 README 会继续制造漂移，均不采纳。
- **机会成本**：不在本次扩张为全量英文文档翻译；内部规划与历史文档继续使用中文，以免产生大规模双写维护成本。

### 用户确认

- **确认日期**：2026-09-03
- **决策**：接受默认英文 README，并保留简体中文本地化入口。

### 关联

- **实现**：根 `README.md`、`README.zh-CN.md` 及仓库内语言导航链接。
- **Plan / Architecture**：无需更新；该需求不改变当前 R0 主线或稳定职责边界。
- **验证**：根 README locale contract 检查通过；`tests/test_docs_links.py` 为 `1 passed`；`git diff --check` 通过。

---

## REQ-2026-09-03-027：基于公共失败证据修复确定性准确率缺陷

- **提出日期**：2026-09-03
- **当前状态**：`verified`
- **用户原始表达**：在复盘本地提高准确率的工作后，用户确认继续推进原始 Execution Accuracy 弱项修复。

### 真实问题与目标结果

完整 BIRD Mini-Dev 500 题运行中 Forge JSON 路径 Official EA 为 45.40%，低于同模型 Direct SQL 的 56.40%。现有完整运行只保留聚合指标；本地可审计的 12 题三轮公共诊断记录进一步暴露一个确定性编译缺陷：模型生成了 `agg`，用其别名排序，但为遵守问题输出契约没有把聚合列放进 `select` 时，Compiler 仍输出 `ORDER BY alias`，SQLite 报 `no such column`。目标是在不改变结果列、不猜测业务语义的前提下，恢复该合法聚合排序意图，并用公共失败样本离线复算实际结果。

### 评估与实施边界

- **用户价值**：高；该缺陷把语义足够完整的候选降级为执行失败，且存在于 Top-N、最短/最长、按聚合排序等通用查询。
- **职责归属**：属于 Forge JSON Planner Adapter 的确定性 Compiler 规范化；不改变 Trust Runtime 的产品身份、Pi 编排、QueryRun 真相源或 Direct SQL 路径。
- **安全与隐私**：只分析公开 BIRD 数据和本地历史产物，不访问客户数据、生产凭证或外部模型。
- **首个切片**：仅当 `sort.col` 命中本层未投影的 `agg.as` 时，把排序引用展开为同一聚合表达式；已经投影的别名保持不变，禁止为了可执行性向结果中泄漏隐藏聚合列。
- **不做**：不把 12 题诊断分数包装成完整 500 题新成绩；不根据题号、表名或自然语言写特例；不修补比例粒度、时间字符串解析、OWNER 关系或输出列形状等仍需语义证据的问题；不发起新的模型调用。
- **替代方案**：把聚合别名自动追加到 `select` 会改变 Exact Result 的列契约，拒绝；只依赖模型重试浪费调用且不能保证收敛，拒绝；用文本替换生成 SQL 绕过 Compiler Contract，拒绝。
- **可证伪条件**：若展开后仍不可执行、改变可见列、破坏已投影别名行为，或公共失败样本结果仍不匹配 Gold，则该切片不成立。

### 用户确认

- **确认日期**：2026-09-03
- **决策**：用户在获知当前准确率证据与建议优先项后回复“好的，继续”；按上述保守切片实施。

### 关联

- **Plan**：R0.6 期间的兼容性维护，不改变 R0 阶段顺序或外部采用门禁。
- **Architecture**：无需更新；Compiler 仍只做确定性转换，Assurance/Executor 边界不变。
- **实现**：`forge/compiler.py` 在普通查询的 `ORDER BY` 中，将命中本层未投影 `agg.as` 的引用展开为同一 `_agg_expr`；集合运算和已投影聚合别名保持原行为。`tests/test_compiler.py` 用真实内存 SQLite 验证可执行结果、排序和单列输出契约，并覆盖已投影别名不回归。
- **验证**：聚焦 Compiler/Hard Benchmark 为 `63 passed / 2 skipped`；Python 全套 `678 passed / 28 skipped`；Pi `118 passed`；TypeScript typecheck、Python compileall 和 `git diff --check` 通过。公开 BIRD 本地历史 Run `hbr_453ac77d1fc34478b39e0d19dc5b6741` 离线回放中，case 988 run 3 从执行失败变为 Official EA exact match；case 1011 run 1/3 恢复可执行，但输出分别为 1/2 列而 Gold 为 3 列，继续判为不正确。
- **剩余边界**：这是对历史候选的确定性离线复算，不是重新生成或完整 500 题重跑；完整基线仍为 Forge 45.40%、Direct SQL 56.40%。比例缩放/粒度、时间字符串解析、关系角色和输出形状等语义错误保持未修复。

---

## REQ-2026-09-03-028：用 GPT-5.6 完整重测 Forge JSON 准确率理论

- **提出日期**：2026-09-03
- **当前状态**：`verified`
- **用户原始表达**：用户指出历史 Forge JSON 重新编译只能验证 Compiler，不能验证“LLM 生成 Forge JSON 比直接生成 SQL 更准确”的理论基础，并选择用当前 Codex 的 GPT-5.6 完整重跑 500 题双臂测试。

### 真实问题与目标结果

`REQ-2026-09-03-027` 证明了一个确定性 Compiler 缺陷可以挽救历史候选，但没有重新执行 LLM 生成，不能回答 Forge JSON Planner Adapter 的核心准确率假设。目标是使用同一模型、同一 500 题、同一 Schema/Evidence/ContextSnapshot 和一次生成策略，分别新生成 Forge JSON 与 Direct SQL，以 BIRD Official Execution Accuracy 判断当前实现下 Forge JSON 是否更准确。

### 已确认测试设计与边界

- **模型**：`openai-codex/gpt-5.6-sol`，使用现有 Pi OAuth Runtime；只检查非敏感 readiness，不读取、复制或记录 credential。
- **规模**：完整 BIRD Mini-Dev 500 题；每题 Forge JSON 与 Direct SQL 各一次新生成，共 1000 次模型调用。
- **固定参数**：temperature 0、max output tokens 8192、同一模型/catalog revision；每题双臂共享 question、Oracle Evidence、召回后的 Schema 和 ContextSnapshot，Gold SQL/Gold Result 对模型隐藏。
- **主指标**：BIRD Official EA exact set comparison；辅助指标为 Contract Accuracy、Execution Success、失败阶段、Token 与延迟。可执行不等于正确。
- **判定**：只有 Forge Official EA 高于 Direct SQL，才支持本轮模型与数据集上的准确率优势；持平或更低均不支持。单次 500 题结果不外推到所有模型或开放世界查询。
- **运行安全**：使用隔离本地运行状态和公开 BIRD SQLite，不接客户数据或生产数据源；认证、配额或 Provider 系统性失败时停止，不能把基础设施失败包装成方法准确率。
- **不做**：不复用历史候选，不用旧 JSON 重编译替代新生成，不切换模型或在运行中修改 Prompt/Compiler，不按结果补题或重试选优。

### 用户确认

- **确认日期**：2026-09-03
- **决策**：用户选择“完整 500 题”，并指定尝试当前 Codex GPT-5.6；已知该运行需要 1000 次模型调用，视为本轮明确调用授权。

### 运行结果与结论

- **完整运行**：`pbr_76da9a18d96c4e13b2b810ba111bd599` 于 2026-09-04 完成 500/500 cases、1000/1000 新模型调用；使用 Pi AgentSession、现有 `openai-codex` OAuth 与 `gpt-5.6-sol`，固定 model revision `sha256:64aabcc80506d63ad711bdc89b9f3a29fe8a275d62dab5b97c04d5af222724a0`。1000 个调用均产出候选；Provider 自动重试 3 次，没有调用级失败。
- **主结果**：Forge JSON → SQL Official EA 53.20% (266/500)，Direct SQL 62.20% (311/500)，Forge Delta **-9.00pp**。Contract Accuracy 分别为 48.00% 与 56.80%；Execution Success 分别为 94.40% 与 99.80%。
- **配对结果**：both correct 244、Forge only 22、Direct only 67、both wrong 167。89 个不一致对上的双侧 exact McNemar/binomial p = `1.899849174722082e-06`。两臂均执行成功的 471 题中，Direct only 50、Forge only 22；因此差距不能只归因于 Compiler 或执行失败。
- **难度与覆盖**：simple 为 67.57% vs 74.32%（-6.76pp），moderate 为 54.40% vs 60.40%（-6.00pp），challenging 为 29.41% vs 49.02%（-19.61pp）。Forge 只在 11 个数据库中的 2 个各领先 1 题，在其余 9 个落后。
- **失败结构**：Forge 有 1 个编译失败，最终执行 passed/failed/skipped 为 472/6/22；Direct 为 499/1/0。Direct 独赢的 67 题中，17 题来自 Forge 非执行，50 题是 Forge 已执行但结果错误。抽样可见 Forge 的额外失分包括复杂字段名未正确引用、保留字表名未转义、额外输出列、日期边界、比例缩放/舍入和关系语义；Forge 独赢样本则显示结构化规则在部分 Top-N、结果列和精度要求上有帮助，但不足以形成总体优势。
- **成本**：Forge 使用 5,443,601 total tokens，Direct 使用 3,558,117，Forge 多 52.99%；平均生成时延 9,269.59 ms vs 6,650.82 ms（+39.37%），P95 19,637.05 ms vs 14,129.74 ms（+38.98%）。
- **结论**：在本轮固定模型、完整 BIRD Mini-Dev 和一次生成条件下，“让同一 LLM 先生成 Forge JSON 会比直接生成 SQL 更准确”的广义理论**不成立**。Forge JSON 仍可作为可替换候选输入和治理载体，但不能作为当前产品的准确率承诺；产品价值继续落在生成后的 Evaluate、Policy、Assurance、只读执行、Evidence 与 Audit。

### 关联

- **Plan**：R0.6 期间的 Accuracy Lab 理论复验；不替代外部采用门禁。
- **Architecture**：不改变职责边界；Pi 继续负责模型双臂运行，Forge 负责 Context、Compiler、Assurance、执行和评分。
- **实现与验证**：修复官方 BIRD ZIP 完整运行目录解析并补充 layout 回归；完整运行结果由持久 Case 重聚合，并逐一读取 1000 条候选记录、对其中已生成 SQL 独立重执行，均得到 Forge 266/500、Direct 311/500。Python 全套 `683 passed / 26 skipped`，Pi `118 passed`，TypeScript typecheck、Python compileall 与 `git diff --check` 通过。运行状态隔离在 `.forge/gpt56-pi.sqlite3`，未读取、复制或记录 OAuth credential。

---

## REQ-2026-09-04-029：用 Structured Output 精简 Forge 生成提示并复测历史失分题

- **提出日期**：2026-09-04
- **当前状态**：`verified`
- **用户原始表达**：用户要求尝试 Structured Output，基于结构化输出模式精简模型生成 Forge JSON 的 Prompt，并小批量重跑上一轮没有做对的题。

### 价值与目标结果

上一轮 Forge 分支只靠文本 Prompt 要求 JSON，再由服务端 `json.loads`；大量 DSL 语法说明与展示规则增加输入成本，其中默认 `ROUND(..., 4)` 与 BIRD exact result contract 冲突。目标是让 Pi 通过 schema-bound Structured Output 直接取得 Forge 对象，把 Prompt 收敛为必要的语义决策规则，并在上一轮 Forge 失分题上验证结构合法性、Official EA、执行率、Token 与时延变化。

### 方案、边界与风险

- 当前 `createAgentSession` 不暴露独立的 Provider `response_format/json_schema`。采用 Pi SDK 的单一 terminating custom tool，以完整 Forge JSON Schema 约束并校验工具参数。Forge Schema 包含递归 `$ref` 与对象 `oneOf`，超出 Pi/OpenAI strict 子集，因此只请求 `constrainedSampling: {type: "json_schema", strict: "prefer"}`；当前会确定性降级为普通 tool schema，而不是伪装成 Provider-native strict JSON。若模型未完成有效工具调用则失败关闭，不回退到文本 JSON。
- 结构 Schema 负责形状与枚举，Prompt 只保留 SQLite 方言、结果列契约、过滤/聚合粒度、百分比/比率、复杂标识符与原始表达式边界；删除 JSON 拼写教程、重复格式规则和默认四位舍入。
- 先运行上一轮 Forge 错题的有界分层样本；样本是定向诊断，不代表完整 500 题新成绩。完整重跑仍需另行确认模型调用预算。
- Direct SQL 分支、共享 ContextSnapshot、Gold 隐藏、Compiler、Assurance、Executor 与 Official EA 不变；运行必须冻结模型 revision、Prompt revision、Structured Output mode 与 case IDs。
- 风险：工具 Schema 本身仍消耗输入 token；旧错题抽样存在选择偏差；Structured Output 只能减少格式错误，不能自动修复实体、粒度和业务语义。替代方案是继续文本 JSON 或绕开 Pi 直连 Provider，前者不能验证本需求，后者破坏 Pi 主 Orchestrator 边界，均不采用。

### 关联

- **Requirement**：承接 `REQ-2026-09-03-028` 的完整 GPT-5.6 双臂结果与失分分析。
- **Plan**：R0.6 Accuracy Lab 的有界方法复验，不替代 External Adoption Evidence 门禁。
- **Architecture**：Pi 继续持有模型 Session 与结构化工具调用；Forge 继续负责 Contract、Compiler、Assurance、执行和评分。
- **实现与验证**：Pi Benchmark Forge 分支已切为 `emit_forge_query` terminating custom tool；完整 Forge Schema 去除说明性 annotation 后作为参数契约，Prompt revision 为 `forge-structured-benchmark-v1`，运行投影持久化 output mode、Prompt revision、Schema SHA-256 与 strict request。样本在调用前冻结为舍入 `md-000/012/046/079/322/458`、非执行 `md-083/139/439/440/448/495`、输出契约 `md-203/240/297/319`、语义/粒度 `md-014/198/420/482`。Runs `pbr_d14f45bf31df4dfdb536c39f0fa75905` 与 `pbr_00295fcb34f1474a9f5d33253c4a3efa` 共完成 20 cases / 40 新调用；相同 GPT-5.6 revision 与 20/20 相同 ContextSnapshot 下，Structured Forge Official EA 由历史 0/20 到 9/20，20/20 得到校验后对象，舍入类修复 5/6；total tokens -4.52%，平均生成时延 -6.96%，但 prompt tokens +25.54%。Python 全套 `684 passed / 26 skipped`，Pi `118 passed`，TypeScript typecheck 与 Python compileall 通过；隔离服务已停止，OAuth credential 未被读取、复制或记录。
---

## REQ-2026-09-04-030：修复 Structured Forge 暴露的确定性 Compiler 缺陷并复测

- **提出日期**：2026-09-04
- **当前状态**：`verified`
- **用户原始表达**：在 Structured Output 定向实验后，用户回复“好的，继续”，确认按建议先修复 Compiler 的 alias、复杂标识符引用和聚合 alias 展开，再重跑同一 20 题。

### 简化评估与已确认范围

- **真实问题**：`REQ-2026-09-04-029` 的新候选中，`md-083/md-495` 因双引号限定表名未进入引用完整性校验而编译失败，`md-322/md-448` 因含空格输出 alias 未引用而 SQL 解析失败，`md-139` 因 HAVING 只展开 `col`、未展开 `col2` 而执行失败；这些都是相同 Forge 对象必然复现的确定性 Compiler 缺陷。
- **目标结果**：引用校验理解合法的双引号/反引号限定标识符；输出 alias 按目标方言安全引用；HAVING 聚合 alias 在条件树两侧一致展开；相同 20 题离线复算与重新生成复测均保留独立证据。
- **职责边界**：只修改 Forge Contract 校验后的确定性 SQL 编译，不改 Prompt、模型、ContextSnapshot、Assurance、Executor、Gold 隐藏或 Direct SQL 分支，不用编译器猜测实体、粒度或业务语义。
- **风险与约束**：标识符规范化只能用于引用绑定比较，SQL 中仍保留原始已引用标识符；alias 引用必须区分 MySQL/BigQuery 反引号与 SQLite/PostgreSQL/Snowflake 双引号；不得以字符串特判五个 case。
- **替代方案**：继续通过 Prompt 要求模型避免空格 alias 或去掉合法引用，会把确定性语法职责推回随机生成层，拒绝；直接跑 500 题会重复已知失败且成本更高，先复测同一 20 题。
- **用户确认**：2026-09-04，接受上述顺序与边界。

### 关联

- **Requirement**：承接 `REQ-2026-09-04-029` 的结构化生成实验。
- **Plan**：R0.6 Accuracy Lab 期间的 Compiler 兼容性维护，不改变产品阶段、架构边界或外部采用门禁。
- **实现**：`forge/compiler.py` 统一了按方言渲染关系、字段与输出 alias 的标识符路径；引用绑定接受双引号/反引号限定名；HAVING 条件树的 `col`/`col2` 均展开聚合 alias；raw subquery 与聚合 SQL 表达式继续保留显式逃生口。`tests/test_compiler.py` 与 `tests/test_compiler_extended.py` 覆盖 SQLite/PostgreSQL/MySQL/BigQuery/Snowflake alias、复杂/保留字 source identifier、双侧 HAVING 与 raw expression 回归。
- **确定性复算**：对 Runs `pbr_d14f45bf31df4dfdb536c39f0fa75905` / `pbr_00295fcb34f1474a9f5d33253c4a3efa` 保存的相同 20 个 Forge 对象离线重评，Official EA 由 9/20 升至 14/20、Contract Accuracy 由 10/20 升至 15/20、Execution Success 由 15/20 升至 20/20；`md-083/139/322/448/495` 五题均由确定性 Compiler 修复，未重新生成候选。
- **重新生成复测**：相同模型 revision、temperature、max output、Prompt/Schema 与 20/20 ContextSnapshot 下，Runs `pbr_cdfe12f9e77745eb8a81dd37b0e56571` / `pbr_e91cc3b0ba4645d2a2447daf9732e2cc` 的保存候选先得到 15/20 EA、16/20 Contract、18/20 Execution；最终 source identifier 渲染对同一候选再确定性修复 `md-448/495`，终值为 17/20 EA、18/20 Contract、20/20 Execution/Compile。同期新 Direct SQL 为 14/20 EA；但该有偏 20 题样本且两次生成候选不同，不能把 9/20 → 17/20 解释为纯 Compiler 因果或总体优势。完整 500 题 53.20% vs 62.20% 基线不变。
- **验证**：Compiler 聚焦 `112 passed`；Python 全套 `697 passed / 26 skipped`；Pi `118 passed`；TypeScript typecheck 与 Python compileall 通过。LSP diagnostics 因仓库未配置 Python language server 不可用。

---

## REQ-2026-09-04-031：使用 GPT-5.6 重跑完整 500 题双臂基准

- **提出日期**：2026-09-04
- **当前状态**：`verified`
- **用户原始表达**：在 20 题定向修复显示 Forge 85% vs Direct SQL 70% 后，用户确认“我们可以直接进一步测试了”，并补充“还是用 GPT 5.6”。

### 简化评估与已确认范围

- **目标结果**：在完整 BIRD Mini-Dev 500 题上测量当前 Structured Forge + 最终 Compiler 与 Direct SQL 的端到端 Official EA、Contract、Execution、tokens、时延和配对胜负，判断定向收益能否扩展。
- **实验设计**：固定 `openai-codex/gpt-5.6-sol`、temperature 0、max output 8192、同一模型/catalog revision 与 ContextSnapshot；每题每臂一次生成，不按结果重试或选优；Forge 使用当前 terminating custom tool Schema 与最终 Compiler，Direct SQL 保持既有分支。
- **因果边界**：先对历史 GPT-5.6 500 题保存候选离线重评，隔离 Compiler 的确定性影响；再执行 500 题 / 1000-call 新双臂运行。新运行同时包含生成波动、Structured Tool、Prompt 和 Compiler 变化，不能全部归因于 Compiler。
- **风险与约束**：运行成本和耗时较高；temperature 0 仍不保证完全确定；运行开始后不修改代码、Prompt、Schema、评分器或样本，不把部分结果包装为最终分数。
- **用户确认**：2026-09-04，明确授权继续完整测试，并指定继续使用 GPT-5.6。

### 关联

- **Requirement**：承接 `REQ-2026-09-03-028` 完整 GPT-5.6 基线与 `REQ-2026-09-04-029/030` Structured Tool、Compiler 定向修复。
- **Plan**：R0.6 Accuracy Lab 的完整复验；不替代 External Adoption Evidence 门禁。
- **完整运行**：Run `pbr_6778bf9d34ae42fba0b070a5f9c154ba` 完成 500/500 cases、1000/1000 新调用；固定 `openai-codex/gpt-5.6-sol` revision `sha256:64aabcc80506d63ad711bdc89b9f3a29fe8a275d62dab5b97c04d5af222724a0`、temperature 0、max output 8192，500/500 ContextSnapshot hash 与历史 GPT-5.6 Run 一致。Forge 500/500 都取得 schema-bound tool 对象，没有文本 JSON/空输出。
- **原始封存结果**：运行时 Compiler 下 Forge 为 57.40% EA（287/500）、51.40% Contract（257/500）、87.80% Execution（439/500）、96.40% Compile（482/500）；Direct 为 62.80% EA（314/500）、58.20% Contract（291/500）、100% Execution。配对 Forge only 20、Direct only 47，双侧 exact p=0.001307；该状态仍显著落后。
- **确定性修复与重评**：原始候选暴露 relation `AS` alias/self-join 绑定、raw SQL 内层 alias 作用域、比较谓词 scalar subquery、排序 raw expression、semi/anti 多条件 SimpleCondition、HAVING 复合 aggregate alias 与空 GROUP BY 的确定性 Compiler 缺陷。修复后对**相同 500 个 Forge 对象**离线重评，无新增模型调用：EA 62.60%（313/500）、Contract 56.00%（280/500）、Execution 99.20%（496/500）、Compile 100%（500/500）；相对封存结果净增 26 EA、23 Contract、57 Execution，零 EA 回退。
- **最终配对结论**：Direct 候选与评分不变，为 62.80% EA（314/500）、58.20% Contract（291/500）、100% Execution。Official EA 配对为 both correct 291、Forge only 22、Direct only 23、both wrong 164，双侧 exact p=1.0；Contract 配对 p=0.1608。当前证据是统计不可区分的近似持平，不支持 Forge 更准确，也不再支持本轮 Direct 显著领先。
- **因果边界**：历史文本 Forge 候选在同一最终 Compiler 下只由 266 升至 269；新 Structured 候选为 313，较历史同编译器净增 44（62 gains / 18 losses，p=8.14e-7），但该差异合并了 Structured Tool、精简 Prompt、移除默认舍入和一次生成波动，不能拆成单因素因果。temperature 0 下 Forge 新旧候选 0/500 完全相同，Direct 仅 237/500 完全相同；Direct EA 311→314 的 17 gains / 14 losses（p=0.7201）提供生成波动对照。
- **成本与剩余失败**：新 Forge 使用 4,905,368 tokens、平均生成 9,791.49 ms、P95 18,269.9 ms；Direct 为 3,074,132、6,454.98 ms、13,601.8 ms，Forge 分别多 59.57%、51.69%、34.32%。最终仅 4 个 Forge 候选未执行：`md-006/119` 漏投影下游需要的 CTE 字段，`md-339` 把 window alias 用在同层 WHERE，`md-405` 同层嵌套 aggregate；均是生成语义错误，不以 Compiler 猜测修复。
- **产品结论**：完整 500 题已把定向收益扩展为一次近似准确率持平，但 Forge 的 token/时延成本仍明显更高；结果只适用于固定 GPT-5.6 revision、Oracle Evidence、当前 Structured Prompt/Schema、最终 Compiler 与一次生成，不替代 R0.6 External Adoption Evidence 门禁。
- **工程验证**：Compiler 聚焦 `119 passed`，Benchmark 集成 `18 passed`，Python 全套 `704 passed / 26 skipped`，Pi `118 passed`；TypeScript typecheck、Python compileall 与 `forge/schema.json` 解析通过。完成 Run 与 500 条持久 Case 已从隔离的 `.forge/gpt56-full-rerun-pi.sqlite3` 读取复算；未读取、复制或记录 OAuth credential。

---

## REQ-2026-09-04-032：使用 DeepSeek 重跑完整 Structured 500 题双臂基准

- **提出日期**：2026-09-04
- **当前状态**：`superseded`
- **用户原始表达**：GPT-5.6 完整 Structured 复验完成后，用户要求“好的，我们再用 DeepSeek 来测试一下吧”。

### 简化评估与已确认范围

- **目标结果**：先按用户选择使用火山 Coding Plan 的 `volc-ark-coding/deepseek-v4-flash-ga-260731`；套餐额度阻断后按用户选择回退 `deepseek-official/deepseek-v4-flash`。在完整 BIRD Mini-Dev 500 题上复测当前 Structured Forge + 最终 Compiler 与 Direct SQL，测量 Official EA、Contract、Execution、Compile、tokens、时延和配对胜负。历史 Run 使用的 provider alias 为 `openai/deepseek-v4-flash`，因此跨历史比较保留 provider/revision 漂移限制，不宣称严格同端点复现。
- **实验设计**：固定当前代码、`forge-structured-benchmark-v1` Prompt、schema-bound terminating tool、temperature 0、max output 8192、500 题与 Oracle Evidence；每题每臂一次生成，共 1000 次调用，不按结果重试或选优。运行前由 Pi ModelRuntime 确认模型 ready 并冻结 catalog revision。
- **对照设计**：新双臂共享同一 ContextSnapshot；与历史 DeepSeek Run `pbr_1f735d433a284366bfe6526146511792` 比较 Structured/Compiler 变化，与最新 GPT-5.6 Run `pbr_6778bf9d34ae42fba0b070a5f9c154ba` 比较模型差异。跨历史候选的变化包含生成波动，不能作为单因素 Compiler 或模型因果。
- **职责与安全边界**：Pi 继续持有 AgentSession、模型目录与运行状态，Forge 负责 Context、Contract、Compiler、Assurance、执行和评分；使用隔离本地状态库，不修改生产配置、模型绑定或真实数据，不读取、复制或记录凭证。
- **风险与停止条件**：完整运行成本和耗时较高；temperature 0 仍不保证确定；运行期间不修改代码、Prompt、Schema、评分器或样本。模型不可用、上下文 revision 漂移、运行未完成或候选记录不完整时失败关闭，不报告部分结果为最终成绩。
- **用户确认**：2026-09-04，明确要求继续使用 DeepSeek 完整复测；确认官方 API 运行较慢后先选择火山 Coding Plan，遇套餐额度阻断后选择官方入口回退，并选择先以并发 4 加速；并发 4 出现 Provider 污染后，改以并发 1 完成稳定性 pilot 与 nominal full run。

### 关联

- **Requirement**：承接 `REQ-2026-09-04-031` 的完整 Structured GPT-5.6 复验。
- **Plan**：R0.6 Accuracy Lab 的模型横向复验；不替代 External Adoption Evidence 门禁。
- **实现与验证**：Pi Benchmark Runtime 已在首次列举/选择模型前用无工具、内存 Session 引导已配置扩展，使 `volc-ark-coding` 六个模型可被 `/v1/benchmarks/models` 正确列出为 ready；TypeScript typecheck 与 Pi `118 passed`。火山 canary `pbr_3027e7668ed44c09a1dc0bc8c2ecf89f` 两臂均无候选，独立 CLI 探针返回 `429 AccountQuotaExceeded`，套餐月额度将在 `2026-09-12 23:59:59 +0800` 重置。官方入口 canary `pbr_2d8dc382f81d4147af4678a53951b92a` 两臂有效；并发 4 Run `pbr_d4520dabc41d450fb724bc8192a06e80` 在 10/500 题时已有 27 次自动重试和候选缺失，已停止；并发 1 pilot `pbr_743b905dc5d644499b7976996bc752d2` 完成 10/10 题、20/20 arm。最终 nominal full Run `pbr_9d5e4263afdb46cd8636d4b6599f0708` 虽写入 500 个唯一 case、1000 个 arm 并被调度器标记 `completed`，实际仅保存 78 个 Forge object 和 78 条 Direct SQL，每臂 422 次候选缺失；`md-080`–`md-499` 两臂全部缺失，运行后独立 CLI 探针返回 `402 Insufficient Balance`。保存候选的独立重放与 Store 聚合一致（Forge Compile 75 / Execution 69 / EA 48；Direct Execution 77 / EA 54），证明不是存储损坏，但不能恢复缺失候选。原始 500 分母结果被 Provider 余额故障主导，按门禁失败关闭，不更新基准分数，不与历史 DeepSeek 或 GPT-5.6 作准确率比较。
- **后续决策**：2026-09-04，用户明确放弃火山与 DeepSeek 入口，不再等待额度恢复；本需求由 `REQ-2026-09-04-033` 的 Luna / Terra / Sol 横向基准取代，历史失败证据保留。

---

## REQ-2026-09-04-033：对比 GPT-5.6 Luna、Terra 与 Sol

- **提出日期**：2026-09-04
- **当前状态**：`implementing`
- **用户原始表达**：“那我们就放弃火山和 DeepSeek 了。我们来测试 Luna、Terra 以及 Sol 他们三个模型之间的区别吧，我们先从 Lunaa 开始吧。”

### 简化评估与已确认范围

- **真实目标**：在同一完整 BIRD Mini-Dev 500 题双臂协议下建立 Luna、Terra、Sol 的准确率、输出契约、执行成功率、token 与时延对照，先完成 Luna；用户的 “Lunaa” 按 Pi 目录中唯一匹配的 Luna 模型解释为 `openai-codex/gpt-5.6-luna`。
- **模型绑定**：Pi 目录确认三个模型分别为 `openai-codex/gpt-5.6-luna`、`openai-codex/gpt-5.6-terra`、`openai-codex/gpt-5.6-sol`。Luna / Terra context 为 272K，Sol 为 1.1M；本基准必须记录实际 Prompt/token 并确认输入未触及较小 context，避免把截断误当模型差异。
- **真实目标**：建立 Luna、Terra、Sol 的准确率、输出契约、执行成功率、token 与时延方向性对照。Luna 首轮完整 500 题实测耗时 47.3 分钟，用户明确认为等待过久，因此最终三模型矩阵改用同一 50 题确定性分层子集；用户的 “Lunaa” 按 Pi 目录中唯一匹配的 Luna 模型解释为 `openai-codex/gpt-5.6-luna`。
- **可比性**：准确率比较要求每个模型取得 500 个 Forge object、500 条 Direct SQL 并可独立重放。Provider 自动重试、候选缺失、context 截断和并发策略单独记录；系统性基础设施失败时整轮失败关闭。Sol 可先以已完成的 Run `pbr_6778bf9d34ae42fba0b070a5f9c154ba` 作为当前基线，最终三模型报告前再核对全部 invariants，任何漂移都要求重跑 Sol。
- **实验设计**：沿用当前 Structured Forge + 最终 Compiler 与 Direct SQL；以固定 seed `gpt-5.6-luna-terra-sol-bird50-v1` 对 case ID 做 SHA-256 排序，并分别取 simple 15、moderate 25、challenging 10 题，共 50 题且覆盖全部 11 个数据库。固定 Oracle Evidence、ContextSnapshot、`forge-structured-benchmark-v1` Prompt、`pi_tool_schema`、temperature 0、max output 8192、每题每臂一次生成且不按结果重试选优。每个模型冻结 provider/model/catalog/model/prompt/schema/compiler/evaluator revision。
- **可比性**：三个模型使用完全相同的 50 个 case ID、当前 Schema revision 和并发 2；各自要求 50 个 Forge object、50 条 Direct SQL并可独立重放。Provider 自动重试、候选缺失、context 截断和并发策略单独记录；系统性基础设施失败时整轮失败关闭。旧 Sol Run `pbr_6778bf9d34ae42fba0b070a5f9c154ba` 的 Forge Schema revision 不同，不能进入本次严格矩阵，Sol 必须按当前 50 题协议新跑。
- **顺序与边界**：Luna 50 题结果从已完成、同当前 Schema 的 500 题 Run 中按预先固定算法投影；随后只需新跑 Terra 与 Sol 的相同 50 题。使用隔离本地状态库，不修改生产模型绑定、外部服务或真实数据，不读取、复制或记录凭证。

- **用户确认**：2026-09-04，明确停止火山和 DeepSeek 路线，选择 Luna → Terra → Sol 横向测试并要求先从 Luna 开始。
- **Plan**：R0.6 Accuracy Lab 的模型横向证据，不替代 External Adoption Evidence 门禁。
- **用户确认**：2026-09-04，明确停止火山和 DeepSeek 路线，选择 Luna → Terra → Sol 横向测试并要求先从 Luna 开始；在重复全量运行到 276/500 时反馈“等太久了”，随后选择推荐的 50 题分层共同子集方案。


---

## REQ-2026-09-05-034：基于固定 Sol 候选裁定准确率优化方向

- **提出日期**：2026-09-05
- **当前状态**：`verified`
- **用户原始表达**：“来根据我们代码和过去的测试结果，判断一下我们接下来的准确率优化方向”；收到先做错题裁定、再验证值绑定与粒度口径的建议后，用户要求“继续”。
- **已接受范围**：先完成 P0 离线证据与逐题初步归因。固定 Sol Run `pbr_6778bf9d34ae42fba0b070a5f9c154ba` 的 500 题候选，覆盖 164 道共同错题、23 道 Direct 独对和 22 道 Forge 独对；另外分离 EA/Contract 口径差异。
- **价值与验收**：交付绑定候选、Compiler/Schema/比较器 hash 的可复核 JSON 清单；每个 EA 失败 arm 有具体 SQL/结果差异和证据依据，区分候选错误、参考冲突、意图歧义与未定。Agent 辅助归因不是独立人工裁决，不能当作纠正后的官方准确率。
- **边界**：不修改 Prompt、Compiler、评分器或 Gold，不生成新候选，不启动新模型基准，不恢复 DeepSeek，不改变 REQ-033 的模型横测，不读取凭证或真实客户数据。后续生成/语义门禁改动需单独明确实施范围。
- **风险与替代方案**：Gold 和 Evidence 可能冲突；结果错误码不是语义根因；人工/Agent 归因有判断误差。可直接重跑模型或堆 Prompt，但无法隔离现有失败原因；本次优先复用现有公开候选，保留未定项，不猜测。
- **机会成本与阶段**：仅为 R0.6 Accuracy Lab 的有限离线分析，不替代 External Adoption Evidence，不扩展 Runtime 职责或创建第二套任务真相源；诊断回放不是正式计时基准。
- **关闭证据**：`docs/benchmark-sol-error-triage-2026-09-05.json` 已落盘并验证 checksum、500个唯一case、1000个候选/SQL哈希、209题/373个失败arm覆盖及聚合复算。初判候选错误86、参考冲突150、意图/输出歧义133、未定4；主审修订与原始判断均保留。官方EA 313/500 vs314/500不变，未生成新候选；未定与冲突是完成审查后的认识边界，不标为已纠正。下一步建议和值绑定25个arm/16题等覆盖见 `docs/benchmarks.md`，不是涨分承诺或后续实施授权。

---

## REQ-2026-09-05-035：制定准确率优化的整体分阶段规划

- **提出日期**：2026-09-05
- **当前状态**：`verified`（整体规划已交付；各阶段实施单独受门禁约束）
- **用户原始表达**：“好的，下一步，我们需要一个整体的规划，来一步步完成我们准确率的优化工作”。
- **真实目标**：从已完成的固定候选归因出发，形成按证据推进、可回滚的准确率改进闭环；同时提升正确答案产出、降低静默错误，避免靠拒答、改Gold、样本选择或增加无界重试制造涨分。
- **已接受范围**：整体规划与文档落盘，覆盖评测治理、值/实体绑定、范围/粒度/输出契约、确定性诊断、有界修正的条件实验、独立验证及现有调用路径适配；每阶段列明依赖、改动位置、交付、验收与停止条件。
- **本次授权边界**：不自动实施规划中的运行时代码，不发起新模型基准、真实数据访问或部署；新增模型调用需在相应阶段冻结模型、样本、配置和调用/成本上限后明确授权。后续阶段仅在前置证据及门禁满足后进入，不是批量批准全部未来功能。
- **证据基础**：REQ-034已覆盖209题/373个EA失败arm；值/实体绑定为已初判候选错误中最大单类（25arm/16题）。初判不是人审裁决或收益预测；保留Sol Official EA 62.6% vs Direct 62.8%的当前基线。
- **职责与非目标**：Pi继续负责唯一Task/生成/有界修正调度；Forge负责校验、只读执行、审批与Evidence；Registry承载经确认的业务定义。复用现有Benchmark Store、Evaluation Suite/Manifest及Regression Gate，不建立第二套任务/评测真相源。不扩更多Connector、企业权限平台或开放世界自主治理。
- **风险**：当前500题已用于多轮调优；Oracle Evidence不等于真实业务证据；列指纹评分缺陷、参考冲突及隐式粒度会误导优化；temperature 0不保证确定性；生成器适配会增加token/延迟；安全拦截会改变覆盖率，不能单看已回答准确率。
- **替代方案与机会成本**：继续模型横测、扩大Schema召回、强制多候选投票均不能隔离现有主因；当前先补评价契约和高证据价值的元数据/值绑定，再决定是否值得加入额外推理。保持有限工作包，准确率实验不替代R0.6外部采用闭环。
- **规划真相源**：只写入`docs/forge-enterprise-evolution-plan.md`的“15.1 Accuracy Optimization Program”子节；不新增第二份主动路线图。REQ-033模型横测单独保留，不阻塞离线评价维护，也不混为生成方法增益。
- **完成标准**：形成ACC-0至ACC-6的阶段表、统一指标及数据分层、实验协议、晋级/停止/回滚规则、现有代码入口与首个可启动工作包；规划完成不标记任何尚未运行实验为完成。
- **规划交付**：已在唯一主动计划15.1节写入ACC-0至ACC-6、D/R/H/P/S数据分层、端到端正确产出/覆盖率/静默错误口径、版本可比性与实验调用上限、逐阶段验收/停止/回滚条件，依赖无环。2题/50题/500题的双条件双臂单轮预算分别8/200/2000次逻辑生成，仅为计划计算；本规划不授予新模型调用或全部后续实现的默认授权。ACC-0见REQ-034，首个ACC-1A实施及关闭证据见REQ-036；ACC-1整体仍待独立H标签门禁，ACC-2至ACC-6未实施。

---

## REQ-2026-09-05-036：实施ACC-1A比较器诊断与评价版本维护

- **提出日期**：2026-09-05
- **当前状态**：`verified`（ACC-1A维护已完成；不等于ACC-1整体或独立准确率验证完成）
- **用户原始表达**：收到整体准确率规划及“下一可启动工作包ACC-1A，零新模型调用”后，用户确认“好的，继续吧”。
- **已接受范围**：修复确定性的结果比较误判，区分数值/列数错误与列映射不确定；保留Official EA和历史Run；版本化比较器并绑定公开Evaluate及Pi Benchmark调用；同候选重评两臂，冻结D/R/S清单与H选择/审核协议。
- **实现约束**：int/float按精确数值等价，不转换全部数字为float；不引入通用容差；已知唯一列名优先，值等价但身份不唯一不伪造列身份；未能证明映射的结果为inconclusive，不视作通过。现有重复、顺序、NULL及显式舍入契约保留。
- **版本与安全**：新比较器revision进入评价证据/哈希；新Benchmark运行冻结revision并拒绝中途漂移；历史缺revision记录只显示未知，不原地伪造或重算。Pi继续是唯一运行真相源，不改Prompt/Compiler/Gold，不新增模型调用、真实数据访问、生产配置或部署。
- **验证**：最小反例先失败后通过，正确/错误/不确定边界回归；公开API与Python/Pi版本漂移失败关闭；500题原始候选双评分离线复算及不可变旧证据校验。
- **风险与替代方案**：列的边际值相同不能证明行关系正确；不能仅比较每列值集合或任意选一个排列。优先用已有契约及有界比较，不为复杂歧义建立指数搜索。继续用旧比较器会保留已证实假阴性；放宽全部精度会隐藏真错，均不采用。
- **留出边界**：若当前公开资产无法支持独立H及可靠标签，只冻结选择/审核协议并显式not_ready，不从已调优500题伪造H；该外部前置条件不阻塞确定性比较器维护，但不宣称ACC-1或ACC-6全门禁通过。
- **关联**：REQ-035 / 唯一主动计划15.1.11；不替代R0.6外部采用要求。
- **关闭证据**：新比较器修复重复同值列、int/float精确指纹、值错误诊断、宽结果唯一置换及显式set策略；可靠列名不丢失，不确定映射不伪装通过。公开响应metric_revision入evaluation ID；新Pi Run冻结版本，漂移失败关闭；既有EvaluationRun跨版本仍不可比，旧记录不改写。
- **固定候选结果**：`docs/benchmark-sol-metric-replay-2026-09-05.json` 校验1000个候选/SQL、500个Gold、996个成功执行结果哈希并逐项复现旧评分。EA仍Forge313/500、Direct314/500，Contract由280/291变为284/294，零回退；新增7个通过和181个错误诊断更正均属评价器变化，不宣称模型提升。
- **样本与验证**：`docs/accuracy-evaluation-cohorts-2026-09-05.json` 冻结R500、D50（16错误+34旧正确指标对照，11库/33难度分层）、S27和H审核协议；独立重算选择/hash通过，S20比较器边界及7安全引用/14用例通过。Python全套722 passed/26 skipped，Pi124 passed，TypeScript typecheck通过；核心6个修复前失败反例已消除。
- **保留门禁**：H无已证明独立且完成标签审核的样本，P未获业务授权，明确not_ready；不能据此启动独立增益宣称或跳过预算进入ACC-2。全程零新模型调用，未改Prompt/Compiler/Gold或原始Run，未发布/部署。


---

## REQ-2026-09-05-037：推进独立验证证据与首个准确率机制实验

- **当前状态**：`verified`（离线审计、元数据绑定修复及Luna五题诊断已完成；未证明整体准确率提升，独立H审核未完成）
- **用户原始表达**：“好的，继续，别忘记我们本次的目标”。
- **目标**：提高真实问数正确产出、减少静默错误；不把评价器修正、公开来源、更多文档或测试数量当作生成改善。
- **本轮范围**：为独立H查找新增合法公开来源并实际核验重叠/可执行条件；对ACC-2A字段format/键信息的遗漏与D16错误关联做数据核查，冻结单变量实验卡、版本和预算。沿用现有模块与D/R/S清单，不修改旧证据、不另建产品路线。
- **边界与取舍**：新候选调用须预先明确预算；独立审核不能由同一实现者自行认证。公开Gold只提供原始参考，不等同于审核完成。若2A没有新增有用信息，停止该假设，不因已有规划而强行加Prompt；换机制前明确证据与范围。
- **验收**：交付可复算的新增来源/重叠审计和实际元数据输入差异；给出可执行、可停止的下一实验，不重复仅列待办。未满足H门禁不宣称泛化，新模型调用和生产部署不得默许。
- **关联**：REQ-035准确率整体计划、REQ-036评价器基线、主动计划15.1；R0.6外部采用另计。
- **证据驱动的范围澄清**：离线审计发现仅追加现有format/PK对D16没有新增值绑定证据，停止原呈现假设。`student_club`的CSV文件名大小写与schema不同却导致48列描述/取值静默丢失；本轮按普通Bug维护修复描述绑定，遵循SQLite ASCII标识符大小写规则、冲突失败关闭，不按题号或Gold写特例。先证明已有权威取值进入真实上下文；未生成新答案前不宣称准确率增益。
- **实证结论**：`accuracy-metadata-treatment-audit-2026-09-05.json` 覆盖11库/75表/798列；format+PK对D16的25个失败arm新增有效取值证据为0，停止原追加Prompt假设。通用CSV绑定修复已进入 `forge/hard_accuracy_benchmark.py`，按SQLite ASCII大小写规则匹配并对歧义失败关闭，未写题号/Gold特例。
- **行为证明**：`accuracy-metadata-binding-fix-2026-09-05.json` 记录48列已有元数据恢复；重算全部D50真实context与100份双臂control指令，只有md-034/046/052/065/077输入改变，表/关系范围不变。md-065的approved=true/false证据已进入两臂输入；这不是新答案、不是EA改善。修复前2个回归反例失败，修复后Python全套725 passed/26 skipped、TypeScript typecheck通过。
- **H来源证据**：`accuracy-holdout-source-audit-2026-09-05.json` 从官方完整dev的1534题机械排除523题，冻结1011候选/964依赖组件；父执行器盲态复核哈希、集合、稳定ID和题面重叠。最终H未抽样，159条曝光SQL解析/qualification失败记录、独立题意/Gold/依赖审核及完整数据快照仍未闭环，不能称为独立H。
- **最初实验提案**：D中全部5道输入受影响题，2条件×2臂，20次有效配对生成，含前2题canary；后续用户明确授权Luna实测及两次失败请求补额，执行结论见下。
- **调用前风险**：现有Pi Run的temperature=0/max_output_tokens=8192是记录值，未在session调用显式绑定；离线实际Codex适配器探针即使传maxTokens=8192也不写输出上限，session默认仍允许自动重试。尚无可承诺的8192-token成本硬上限；必须先明确实际参数/请求计数/重试策略，再获得预算授权。模型真实能力与扩展覆写未验证，不追溯伪造历史请求参数。
- **准备轮边界**：新模型调用0；Official EA仍313/500 vs314/500。未变更Gold/Prompt/Compiler/生产配置，未commit/push/部署。
- **新实验授权**：用户确认“好的，那就用 luna 测试一波准确性吧”，并在两次参数失败后确认“继续，最多22次”。固定 `openai-codex/gpt-5.6-luna`，完成5题×修复前后×Forge/Direct；总计22次请求含失败canary。不等待H审核做开发诊断，但不宣称整体泛化、不扩大到500题或三模型横测。
- **实际调用**：Pi现有OAuth入口与 `PiBenchmarkRuntime`，固定Luna。首次显式temperature被接口拒绝，两请求HTTP400、无答案；用户确认“继续，最多22次”后，改用Provider默认采样，两条件一致。最终22次生成HTTP请求：2次参数失败+20个有效候选，无自动重试；不将失败canary包装成准确率结果。
- **五题实测**：Official EA与Contract v2均为Forge 3/5→2/5，Direct 3/5→3/5；Forge执行5/5→4/5，Direct保持5/5。无新增评价通过，Forge新增1个回退；这是全部5道输入受影响开发题，不是Luna总体准确率。证据：`benchmark-luna-binding-2026-09-05.json`。
- **错误变化**：md-065两臂approved从Yes改为true，空结果变为非空；但费用类型分组与Gold的event.type仍有语义/参考冲突，未改判。md-077 Forge日期恢复ISO格式，查到与Gold相同的两个人及金额，但姓名合并成一列，仍按原输出契约判失败；Direct日期仍错。md-052处理组Forge把SQL式别名写入scan/table，编译器复现“JOIN 条件引用了未声明的表：b, e。”，不能归因为稳定的元数据负效应。
- **代价与决定**：双臂合计tokens 28,611→38,709（+35.29%），超过预设20%晋级阈值；不扩大调用、不宣称准确率改善。保留元数据完整性修复；下一短闭环优先处理已复现的Forge别名表示错误，参考/输出契约争议单独处理，不机械推进阶段表。
- **复核与边界**：20个候选的评分/编译结果只读复现，5个Gold缓存与真实SQL执行一致；实际payload、session、请求记录和Pi run_id已封存。Raw Pi中的temperature=0/max_output_tokens=8192仍是旧声明字段，实际请求不传二者；不伪造历史参数。未改Gold/比较器/运行器生产源码，未commit/push/部署；独立H与外部采用门禁仍未通过。

## REQ-2026-09-05-038：真正启用严格Structured Outputs

- **状态**：`verified`。
- **用户原始表达**：“所以要用的，你觉得呢”；在明确应验证并启用strict:true、不得把prefer当成已启用后，用户确认“好的”。
- **范围**：Forge Benchmark分支实际发送strict:true，保留完整Forge DSL，使用Pi现有会话和请求钩子；Direct SQL不改变。严格传输Schema从现有schema.json派生，规范化传输层null后仍进入原有Compiler/Assurance/Evaluate。
- **边界**：不把结构合法等同答案正确，不为通过严格转换器删除CTE/子查询/集合运算；不静默退回prefer，不改Gold，不扩展模型横测。历史普通工具运行不得以严格模式继续生成混合成绩。
- **实证约束**：安装版Pi的makeStrictJsonSchema直接拒绝definitions/$ref/结构化联合；使用现有onPayload钩子绑定Provider原生严格Schema，避免修改依赖或重写Agent循环。服务端是否接受须另以有界canary实证；离线验证不冒充Luna支持。
- **验收**：实际请求包含strict:true；递归查询和SQL NULL语义保留；不支持的API、参数漂移、Schema漂移失败关闭；新旧生成契约可区分，结果可复核。
- **价值与替代**：消除把prefer降级误称为严格输出的问题；不继续使用普通工具回退，也不为适配Pi转换器缩减DSL。现有onPayload+派生传输Schema避免依赖补丁和第二套Agent循环。
- **实测**：用户另行批准最多2次Luna调用；实际2次、零自动重试。md-034双臂HTTP200，Forge发送strict:true、指定函数tool_choice、parallel_tool_calls:false；未经过SDK强制转换的原始参数通过Schema校验，规范化后通过编译与执行。证据：`benchmark-luna-strict-output-2026-09-05.json`。
- **验证与代价**：TypeScript typecheck与129项Pi测试通过，含递归/SQL NULL及历史续跑隔离；本题Forge 4185 tokens、Direct 2182 tokens。仅Luna/Codex路径完成服务端验证，不外推其他兼容API；结构约束不保证别名或业务语义正确，也不据1题宣称准确率提升。

## REQ-2026-09-05-039：修复隐式关系别名的引用校验

- **状态与确认**：`verified`；用户在严格输出验收后回复“好的，那就继续吧”，继续已指出的md-052别名问题。本轮仅做确定性维护和离线复算，不新增模型调用。
- **根因与价值**：关系渲染会透传合法的event e，但共用解析器只识别event AS e，引用校验因此误报e/b未声明。这是Compiler内部不一致，应纠正此前笼统归为模型别名表示错误的归因。
- **实现与边界**：在共用关系解析器支持普通/限定/引用表名及括号子查询的省略AS别名；保留显式AS、引号与SQL尾部子句。未声明引用、别名遮蔽和重复绑定仍拒绝；不改Prompt、Schema、Gold、评价器或Assurance门禁。
- **替代与机会成本**：不要求模型重生成，不放宽引用检查，不按题号修补；用原候选隔离Compiler收益，避免扩大付费量测。
- **验收**：修复前3个行为回归失败，修复后Compiler/Benchmark聚焦141项通过；5种方言解析通过。20个原候选、原HTTP上下文离线复算，只有处理组md-052 Forge的SQL变化；编译/Assurance/执行/EA/Contract均通过，结果September Speaker。处理组Forge 2/5→3/5，其他三组保持3/5，零回退、零新增模型请求。证据：`benchmark-luna-alias-fix-2026-09-05.json`。
- **结论**：这是独立记录的固定候选Compiler纠错，不覆盖历史Run；元数据处理后仍无准确率增量证据，历史500题未重评。

## REQ-2026-09-05-040：Luna严格输出五题完整配对复测

- **状态**：`verified`。用户原话“好的，我们再测一遍”，明确选择“完整配对，20次请求”；第10次后本地故障中断，用户另行确认“继续剩余10次”，未补跑任何已发请求。
- **范围**：md-065/034/046/052/077，旧元数据与修复后元数据两条件、Forge/Direct双臂，各一次新生成；两条件均启用当前strict:true与隐式别名Compiler修复。复用Pi OAuth与Pi Benchmark，不建立新Orchestrator。
- **预算与安全**：实际20次生成HTTP请求、全部200，零Provider自动重试、零替换调用；默认采样，无输出token硬上限。不直接读取/复制凭证，认证由Pi SDK处理；不改Gold，不扩展题目或Provider。10份HTTP上下文与20份用户指令匹配原冻结输入。
- **价值、替代与限制**：与刚完成的固定候选离线复算不同，本轮观察修复后的新生成表现；用户选择完整配对而非仅当前配置10次或单题2次。代价为20次调用；仅5道选定开发题，跨历史比较含严格协议与生成波动，不能宣称总体准确率或严格输出单因素因果。
- **验收**：20份原始响应经同一最终归一化/Compiler/Assurance/Evaluate离线复核；10个Forge原始参数通过严格Schema及真实Pi规范参数校验。旧元数据Forge EA/Contract 2/5、Direct 3/5；修复后元数据两臂均3/5。证据：`benchmark-luna-strict-paired-2026-09-05.json`。
- **中断与修复**：Pi对脱离根上下文的递归子Schema执行强制转换，md-046触发栈溢出；改在prepareArguments中先严格校验并归一化，再让Pi校验canonical Schema，发送给模型的Schema不变。缺失候选不再发布completed，生成失败保留token并纳入总消耗。3个修复前失败回归在修复后通过，Pi全套132项/typecheck通过；未修改依赖。
- **故障账本**：原md-046 Forge Run错误地completed且记0 tokens；原记录保留，新证据从session恢复3819 tokens及原候选。第二轮Agent尝试在HTTP前被拦截；恢复候选仍因CTE漏投影total被Assurance拒绝，不替换或改判。
- **结果与代价**：当前配置Forge仍3/5，与前轮Compiler离线纠错后相同；md-052四臂均通过，md-065分组/参考争议、md-077姓名列契约差异仍在。控制组24021 tokens、处理组33883（+41.06%），总57904；小样本不证明稳定收益或严格输出单因素因果，不扩大调用。

## REQ-2026-09-05-041：当前配置十题跨数据库扩展回归

- **状态与原话**：`verified`。用户“嗯，那就测试下当前优化的结果吧”，明确选择“10题，20次请求”。此前强调所有优化应修通用问题，不迎合测试答案。
- **价值与范围**：检查当前配置在另外10题上的新生成表现；从已曝光R500排除最近5题，按固定SHA-256种子先选10库、每库1题，不参考历史对错/Gold。冻结代码、元数据、输入与评分后，Luna Forge/Direct各一次。
- **预算与风险**：最多20次请求，Provider默认采样、无输出token硬上限、零自动重试/替换；沿用Pi OAuth，不直接读取凭证，只读公开数据库。生成或协议故障停止，保留原响应与实际消耗。
- **边界与替代**：用户未选择1000次全量生成或零调用离线回归。此处没有旧配置同模型对照，不能识别优化因果；已曝光样本不冒充H或总体泛化，不改Gold/Prompt/门禁，不按结果换题、扩样或调代码。
- **验收与结果**：20次HTTP200、零Provider重试/替换，20个原始候选独立复算与存储评分及tokens一致；10个Forge原始参数通过strict Schema及真实Pi校验。Forge EA6/10、Contract5/10、Compile/Execution10/10；Direct EA/Contract7/10、Execution10/10。tokens 51077 vs29953（Forge多70.52%），总81030；10库文件与冻结源码hash未变。证据：`benchmark-luna-expanded-regression-2026-09-05.json`。
- **通用问题证据**：md-436/259 Forge缺目标聚合投影；md-259 Direct关联后分母744而参考总体750；md-483双臂用YYMMDD比较ISO日期，另有粒度差异；md-241两臂姓名合列，人员本身一致。md-454 Forge漏Top-5条件却通过当前数据EA；隔离六校反例返回6行而非5行，确认结果偶合，未改官方得分。该题Contract失败仅为行序，不伪称已拦截漏条件。
- **结论与限制**：本轮结构/编译/执行链路无故障，但不证明逻辑正确、优化增益或总体优势。没有修改代码、Gold、Prompt或评分；不以分数为由扩样、补跑。临时启动脚本的JSON tuple/list比较和tsx loader问题在零模型调用预检阶段处理，不是生成重试。

## REQ-2026-09-05-042：评估Forge是否以BIRD Text-to-SQL冲榜为目标

- **状态**：方向原则已确认，实施方案待讨论。用户回复“好的，那么我们就按照这个方向继续讨论吧”，接受持续对标、成熟后参评但不以冲榜为当前开发主线；不授权新调用、运行期改动、正式提交或部署。
- **用户原话**：“我说的 agent是我们正在开发的 forge，项目是 bird 这个项目，我在想在text to sql这个能力上，我们的 forge 是否需要冲榜”。用户指BIRD为评测项目、Forge为被测Agent/系统，不是在问是否采用Agent SDK。
- **价值**：正式参评可提供外部可比的Text-to-SQL能力证据，并检验Forge系统整体而非单个模型或DSL；传播与采用收益仍待验证。
- **边界**：Mini-Dev子集/已曝光500题只作回归，不冒充隐藏test榜单。EX不覆盖权限、审批、Evidence与业务歧义；Forge JSON不是必须赢过Direct SQL的产品身份。
- **风险与机会成本**：按名次优化可能诱导样本过拟合、增加调用预算或维护榜单专用路径，挤占通用语义、可靠执行及真实消费者验证。单模型或多次采样带来的收益不能归因为Forge。
- **替代与建议**：先维持官方口径的公开可复现基准，使用同模型/相同可用信息及预声明预算对照简单基线，验证独立样本与质量/成本/安全边界；成熟后把正式参评作为外部验收。当前不建议把高名次设为产品主线或承诺指标。
- **当前证据**：历史固定候选500题Forge EX313/500、Direct314/500，Forge多59.57% tokens；最新Luna跨库10题6/10 vs7/10，Forge多70.52% tokens。前者为旧模型/已曝光回归，后者为小样本，均不支持当前系统有稳定Text-to-SQL准确率优势，也不能当榜单排名。

## REQ-2026-09-05-043：高覆盖、低静默错误与低交互负担共同约束

- **状态**：用户明确提出产品目标，具体实施与验收阈值待讨论；不授权新调用、扩展Runtime或部署。与北极星4.3及第10节一致，不以拒绝制造高准确率。
- **用户原话**：“我们要做到 forge 应对各种需求的稳定，在需求的处理上当然需要做优化，比如拒绝或者和用户澄清需求。但这种‘拒绝’的情况应该尽可能的较少，不然用户的使用成本过高，forge 这个产品的价值也无法体现。”
- **价值与范围**：针对多样化、合法且在产品能力范围内的数据需求，提高可靠完成覆盖率，减少可避免拒绝、重复澄清和用户返工；不能靠只支持少数固定问题回避能力建设。
- **处理原则提案**：已有已确认口径直接复用；系统可在权限内取得的信息自行补齐；可恢复技术错误在明确预算内处理，不让用户承担DSL/SQL细节；仅对实质影响结果且不能自行消除的业务歧义做最小必要澄清。权限禁止、不可恢复的数据缺口等保持明确停止，不以降低拒绝率为由猜测或越权。
- **风险与边界**：自动补全不等于猜业务口径，更多重试不等于可靠；新增策略须证明成功率收益与成本，不默认实施无限循环、隐式权限扩张或自动写回未经确认的语义。Pi保持唯一调度与任务真相源。
- **评价提案**：正确完成率与静默错误共同报告；补充无需用户补充的正确完成率、澄清后完成率、可避免拒绝率、澄清轮次及每个正确任务的用户/时间/模型成本。必要的权限拒绝单列，不能通过分母调整掩盖能力失败；无独立标签不伪造语义正确率。
- **替代与机会成本**：拒绝优先会把系统能力不足转嫁给用户，回答优先可能增加静默错误；建议在权限、质量和明确成本约束下提高任务完成能力，而不是预先偏向两端。暂不承诺未验证的百分比阈值或启动新评测。
- **错误归因边界讨论（待确认，未授权实施）**：用户原话“是的，所以我们对错误也是需要有一些判断，如果一些错误是来源于语义理解问题（问题本身就有歧义，问题表达不明确），我们是否需要修复？”需要区分输入欠明确、可发现事实缺失、模型违背明确要求、查询转换/执行缺陷，以及参考答案冲突，不能统称语义理解失败。
- **判断与处置提案**：将纯歧义从确定缺陷修复队列中单列，不为猜中Gold修Compiler；歧义判断须指出至少两种有依据的解释、会改变的结果及现有上下文不能消除分歧的原因，证据不足标未定。产品应先查权限内数据事实、复用已确认语义；只有残余分歧实质影响结果才做最小澄清，不把“常见口径”冒充用户授权。歧义不豁免候选独立错误，例如历史评分口径未定也不能据此合理化全部跨日期笛卡尔积。
- **价值、风险与机会成本**：优先修明确且可复现的错误，减少迎合Gold和反复抽样成本；但防止滥贴歧义标签、过度澄清或新增拒绝掩盖能力不足。官方EX/Contract、完整分母和既定混合抽样规则保持不变，归因标签不改判、不代表人审真值；若后续要停止重复生成争议题，需另行确认抽样策略，不在本次讨论中静默排除。与全部硬修或全部拒绝相比，建议选择证据分流与最小必要交互；当前不启动新生成、语义写回或Runtime扩展。
- **常识与实质歧义的甄别讨论（待确认）**：用户原话“所以如何甄别一些语义的歧义是被允许的，有些其实是人类常识，压根不用考虑（比如日期格式这些）”。补充提案：不能只因能想象两种解释就认定歧义；替代解释须有当前任务的积极证据，而非牵强可能性。优先使用明确要求、已确认语义、可发现数据事实与稳定语言惯例；多源证据冲突显式保留，不用常识覆盖明确约束。
- **操作门槛提案**：实际日期存储布局属于可发现事实，不转嫁为业务澄清；患者人数默认患者实体粒度，不能将检验记录数无依据地视作同等合理解释。只有仍有至少两种场景中有依据的口径、会实质改变答案或风险、且不能由权限内事实或已确认约定消除时，才进入最小澄清。日期布局混杂或03/04等无法唯一解析时，标记数据表示证据不足，不冒充模型可凭常识猜定。既有归因标签仍属待审核诊断，不凭本次讨论改Gold、官方评分、抽样规则或直接重判全部争议题。

## REQ-2026-09-05-044：以BIRD建立并验收查询准确性基础

- **状态**：`accepted`。用户原话“所以我在想的就是 bird bench 是否可以帮助我们完成在准确性这一块达到一定程度，来作为基础能力”，在确认“主要训练场和验收基准、不是唯一企业能力证明”的建议后回复“好的，同意，我们继续”。
- **价值与目标**：优先提高信息充分时的查询正确完成能力，再研究信息不充分时的低负担交互；不以拒绝、审批或可追溯性替代准确性建设。BIRD是主要公开回归与阶段验收基准，不只作可选参考，也不以榜单名次为当前目标。
- **评测边界**：固定Mini-Dev SQLite数据/Gold/官方EX核心口径；开发用相关题加固定正确及跨库对照，阶段验收回到R500全量。已曝光R不证明泛化，独立H审核继续；Contract、安全、覆盖率与成本单列，官方EX不改写。
- **验收规则**：记录总体及按库/难度/错误类型表现、逐题改善/回退、正确完成成本；发现逻辑偶合时补隔离反例，不为提高分数改Gold或按题号修复。能力百分比阈值待可信基线与任务范围确定，不凭空承诺。
- **首个可执行切片**：以最近聚合目标未投影的原始候选作故障探针，离线区分生成表达、DSL契约和Compiler根因，验证合法隐藏聚合/输出选择等反例；不自动补业务表达式，不增加硬拒绝，不默认新增模型请求。
- **风险、替代与机会成本**：只跑定向错题会高估收益，先扩功能/重试会混淆因果，只防错不提高完成能力会增加用户负担；采用单根因短反馈与全量阶段回归，优先修系统可解决的问题。后续新生成实验须另行冻结处理变量和调用预算。
- **首个离线切片完成**：3个原始候选重新编译/只读执行，SQL与冻结结果一致；遗漏已存在于生成select，不是Compiler丢弃正确投影。6个合成行为证明同层比例、CTE导出、隐藏HAVING/ORDER聚合、隐藏QUALIFY排名及未使用定义均可合法工作；自动追加所有agg别名会破坏其中4个结果契约。证据：`.forge/benchmarks/bird-foundation-projection-20260905/diagnosis.json`。
- **候选优化设计**：只在现有紧凑Forge生成指令中明确每层select实际输出、CTE中间量导出及请求输出完整性，保留隐藏辅助表达式；不改Compiler/Schema/Gold/评分，不加拒绝。冻结5题（2个原失败探针+3个正确对照）、两条件双臂20次调用，后经用户授权完成。
- **新生成授权与冻结**：用户另选“执行20次配对验证”。实际运行前确认两条件的结构/Evidence/ContextSnapshot/Direct指令一致，仅Forge紧凑指令及配套Prompt revision不同。API和Pi revision仅在隔离进程中覆盖，版本漂移门禁未关闭，仓库生效源码未改；默认采样、零重试/替换、20次硬上限。冻结材料：`.forge/benchmarks/luna-projection-instructions-20260905/`。
- **配对验收完成**：20次HTTP200、零Provider重试/替换；20个原始候选经Forge链路与固定版本BIRD官方EX核心函数分别重放，判分零差异；10个Forge strict/真实Pi校验、冻结源码与5库hash通过。Forge EX/Contract均4/5→4/5，零新增通过、零正确回退，3个正确对照保持通过。未变指令的Direct为4/5→5/5，说明本轮存在生成波动，不归因为处理收益。
- **结果解释与决定**：md-436两条件生成相同正确SQL，不能把旧失败的恢复归功于新指令；md-259控制组分母范围错，候选组输出三个中间计数而非百分比/计数，仍不正确。候选Prompt不采纳，保留现行版本，不重复抽样追分、不加拒绝或自动补列。tokens控制组36284、候选组36262，总72546；Forge自身23660→23719（+0.25%）。证据：`benchmark-luna-projection-instructions-2026-09-05.json`。准确性基础建设继续，但该Prompt假设未获收益证据，历史500题成绩不变。

## REQ-2026-09-05-045：标准、规则与可复现测试工程落地

- **状态**：`verified`。用户原话“把之前讨论的那些都落地吧，标准，规则，测试工程落地”。已完成工程实施与零付费模型调用验证，不扩大生成或生产流程。
- **范围与价值**：把REQ-042/043/044从讨论和一次性配方固化为主要BIRD回归标准、可执行实验规则、CLI冻结/重放/对比及Pi原生门禁；降低重复实验和误报收益的成本。固定官方EX/Gold/数据快照，D/R/H/S边界和单变量可比性；独立记录覆盖、拒绝、澄清、未知标签与每个正确任务的成本。
- **工程边界**：复用Forge CLI、现有ContextSnapshot/Compiler/Assurance/EX/Contract与Pi Benchmark真相源，不新增第二套调度或任务状态。冻结清单只是不可变输入和审计证据。生成前必须确认调用数/版本，单臂至多一次生成，禁重试和隐式上下文，保留失败与消耗；历史未锚定数据不冒充可比。
- **上下文与回归**：官方结构/字段描述/Evidence进入上下文，Gold仅评分；增加字段级日期事实审计，报告缺失/冲突与采样边界，不按题号纠正值或默默覆盖元数据。官方EX一致性、Top-5偶合、合法隐藏聚合与误拒/未知指标边界进入自动回归和CI。
- **替代与风险**：仅补文档无法约束真实调用，继续临时脚本难以复现；独立重写执行器会产生旁路。采用最小公共协议与现有运行链路，明确旧记录兼容只读、不可比失败关闭，避免通过放宽评分、排除失败或新增拒绝提升表面指标。
- **验收完成**：Python全套786 passed/26 skipped，Pi全套143 passed与typecheck通过；CI新增Pi双门禁。实际CLI完成D5/R500冻结、完整88资产校验、10个历史原候选只读重放（双臂EX/Contract仍4/5）、历史不可比非零退出，以及含已评分编译失败的可比合成重放。真实HTTP验证协议/评分200、未鉴权401、预算/上下文篡改409；实际页面确认取消零POST、接受携带2N预算、缺usage的总卡片/逐题/图表显示未知。
- **交付与限制**：标准见`benchmarks.md`，固定数据目录清单见`tests/datasets/bird_mini_dev_standard.json`；证据见`benchmark-standards-engineering-2026-09-05.json`。日期审计37个候选字段：3个已观察布局冲突、33个未识别到支持的明确布局、1个样本相容；不改源元数据/Prompt。全量只冻结与校验，未新跑R500生成；H与外部采用门禁不变，标准落地不构成准确率提升。
- **用户追加实跑授权**：原话“好的，那我们用 luna 模型测试一波，还是没准备好？”，明确选择“20题，最多40次”。固定D20/seed42/11库，模型openai-codex/gpt-5.6-luna；经Pi原生Runtime派发40次，39份候选，无补跑或替换。
- **实跑结果与未关闭项**：md-472 Forge在120秒超时、用量未知，Run failed；md-340 Gold单独执行可重现30秒SQL超时，两臂未评分，replay complete=false、compare非零拒绝。20题完整分母：Forge已确认正确10/失败9/未知1，Direct正确9/失败10/未知1；EX与Contract本轮已知通过数相同，不作正式准确率/优势结论。已报告tokens 93556 vs55744，另有一次未知消耗。40份原始hash一致、37份有候选且已评分的EX/Contract重放无差异。证据：`benchmark-luna-standard-d20-2026-09-05.json`。工程验收不等于首次真实基线完整；需先补齐Gold预检与未评分状态到Pi汇总的贯通，再另行授权生成，不改Gold、放宽超时或扩样追分。

## REQ-2026-09-05-046：评分门禁修复与Luna混合回归

- **状态**：`verified`（工程修复与获批诊断已验证，Gold执行问题仍保留）。用户原话“好的，修复，并重复测试，每次带上上次失败的题，其他的随机挑选未测试过的题（70%），和已测试过的题（30%）”；另明确“未测试过”指Luna尚未测试，而非从未被任何模型测试。
- **范围与价值**：修复Gold评分就绪预检、未评分状态与完整分母贯通；复用既有freeze/Pi链路固定每轮失败延续及新旧题混合选择，减少重复暴露和漏回归。不创建第二套任务真相源。
- **本轮边界**：继承20题/最多40次授权，只运行一轮，不自动扩大。上轮任一臂EX失败或未评分共11题全部延续，剩余9题按70%四舍五入为6道Luna未测、3道已测且不在延续集合的题；固定seed，记录来源和原始结果，不以拒绝/换题/扩预算消除失败。后续轮次按同一规则，缺少候选池或失败题超出规模时明确阻塞，不静默换比例。
- **风险与替代**：500题均有其他模型曝光，不冒充holdout；混合选题的整轮比例不是同比准确率，失败恢复与随机新题成绩分开报告。Gold/数据/30秒SQL与120秒生成限制不默改；默认Gold预检失败拒绝整轮，显式跳过策略须单独获批。修复不扩大Compiler/Prompt优化或产品范围。
- **验收**：Python/Pi及实际界面核对未评分=null、失败保留分母、Gold失败先于生成；冻结选择可重现且全部延续失败题，抽样分层互斥。实际调用及原候选离线重放封存，不追加替换调用。
- **Gold阻塞的追加决定**：用户选择“保留阻塞，跑其余19题”。本轮20题清单不变，md-340保留在报告和分母内，跳过两次生成，其他19题最多38次，无替换；以后Gold不可评题沿用显式策略。freeze绑定阻塞报告与缩减预算，preflight复验一致才生成；阻塞变化需重新冻结，不静默复活或换题。结果只作不完整诊断，不能发布正式准确率。
- **每轮交付要求**：用户原话“每次测试完成，都要出一个测试简要报告”。每轮无论成功、失败或阻塞均交付简报：模型/Run ID、题目组成与seed、授权/实际调用、EX与Contract正确/失败/未评分、tokens与未知消耗、上轮失败题恢复及持续失败、回退和下一步；链接原始证据，不用换题后的整轮比例宣称优化收益。
- **实跑与验收结果**：Run `pbr_056afb6c56794221b9ada4be62cfab61`，20题中md-340零调用Gold阻塞，其他19题38次派发/38候选，tokens139490且无未知，未补跑。EX确认正确8/9、Contract5/8，各1题未知，完整准确率为null；40条评分重放一致、38份raw hash一致，compare拒绝不完整基线。新6题双臂EX6/6，Contract4/6与5/6；旧失败Direct md-259恢复、Forge md-429仅EX恢复，Forge md-046/md-249再生成回退，不作优化因果结论。Python803 passed/26 skipped、Pi149 passed/typecheck及实际界面通过。简报：`benchmark-luna-mixed-d20-2026-09-05.md`，完整证据同名JSON。
- **保留限制**：md-340原Gold在SQLite3.53.4/3.51.0/3.50.2上均存在重复聚合的执行计划；只读设置与临时副本ANALYZE未解决，不改Gold、源库或30秒限制。下一轮规则推导为15延续（含阻塞）＋4新＋1旧，未发起新轮次。
- **下一轮续测授权**：用户回复“好的，那就继续”，按既定规则执行一轮seed44，20题为15延续＋4道Luna未测＋1道已测；同配置、不改Prompt/Compiler/评分，Gold md-340继续留分母零调用，最多38次、不补跑，结束交付简报。
- **seed44续测结果**：Run `pbr_d613cfab8ad24638a4520b51d760ffc9`完成38次派发/38候选，20题含md-340零生成Gold阻塞，tokens134585完整。EX确认正确Forge6/Direct8，Contract4/6，各1题未知；同一14题可评分延续组EX3→3/4→4，Contract0→1/3→2。Forge md-046恢复、Direct md-259回退，md-429的EX通过在两臂之间波动但Contract仍不通过。模型/生成契约/源码/运行时/数据库指纹不变，38响应hash及40臂重放一致，compare拒绝不完整基线。简报和证据：`benchmark-luna-mixed-d20-seed44-2026-09-05.md/.json`。累计Luna曝光43题；下一轮16延续＋3新＋1旧，未自动执行。
- **seed45续测授权**：用户回复“继续，”，按既定规则执行一轮20题（16延续＋3道Luna新题＋1道已测），seed45；Gold md-340留分母零生成，最多38次调用、无补跑，配置不改，结束交付简报。
- **seed45续测结果**：Run `pbr_d6db1ab5661b46b892238df238303dd3`完成38派发/38候选，20题含Gold零生成阻塞1题，tokens137028完整。EX确认正确6/6、Contract3/4，各1未知；同一15题可评分延续组EX2→4/4→4，Contract0→1/2→2。Forge md-289完整恢复、md-429仅EX恢复；共享题无EX/Contract回退，md-447仍出现字段引用错误。新题双臂1/3，复测md-085双臂通过。模型/生成契约/源码/运行时/数据指纹未变，38响应hash与40臂重放一致，不作优化因果结论。简报及证据：`benchmark-luna-mixed-d20-seed45-2026-09-05.md/.json`；累计Luna曝光46题，下轮17延续＋2新＋1旧，未自动执行。
- **离线归因续做授权**：用户在讨论停止同配置重抽、先离线归因并仅在有证据时做通用确定性修复后，回复“好的，那就继续”。本次不授权新模型调用、改Gold或按题号修复。
- **离线归因结果**：17延续题按诊断主因分为生成/作用域错误6、参考冲突6、输出契约歧义4、Gold阻塞1；存在次要交叉问题，不是人审裁决或纠正后准确率。独立重执行91条分片SQL证据一致，原38候选经现有链路重评SQL和EX/Contract均未变化，另2条Gold未知臂保留；源码/数据指纹一致。md-032参考反向排除会议、md-418参考越过分子限制；同时确认md-082患者粒度、md-259分母/公式、md-300作者绑定、md-483日期绑定等生成错误。
- **修复决定与后续假设**：md-447字段确实存在，问题是QUALIFY包装后作用域不可见；移至filter只恢复执行，另换DOC编码才与Gold匹配，两个手工反事实不能算系统收益。不通过自动去重/换JOIN/换字段或把QUALIFY无条件移到WHERE修分；本轮未满足狭窄确定性代码修复门槛，未改Compiler/Prompt/Schema/Gold/评分。下一候选仅考虑有来源的实际日期格式上下文，保留原始元数据冲突，处理变量与模型预算需另行冻结授权。简报：`benchmark-luna-offline-triage-2026-09-05.md`，完整证据同名JSON；原混合抽样规则及H/R500/外部采用边界不变。

- **seed46恢复授权（2026-09-06）**：用户在核对滚动规则后回复“继续吧”，恢复seed45之后的一轮20题，17延续＋2道Luna新题＋1道已测，seed46；纳入全部留存Luna曝光，包括日期专项，但专项不替代滚动父轮。日期off，Gold md-340仍留分母零生成，最多38次、不重试/换题/补跑，不自动追加轮次。
- **seed46结果**：Run `pbr_cf21f96d67b9484f97c4157357d5230d`，38派发/38候选，136237 tokens完整。EX确认正确Forge6/Direct4，Contract3/2，各1题未知；同一16道可评分延续题EX3→4/3→2、Contract0→1/1→0。Forge md-249恢复；Direct md-082 Contract与md-429 EX回退；md-447 Forge恢复执行仍错，md-483 Forge原表达式引号不闭合而被Assurance解析拒绝。新md-044、旧md-034双臂通过，新md-222双臂失败。原17道延续无双臂完整恢复，下一轮18延续＋1新＋1旧；累计Luna曝光48题，未自动执行。
- **seed46证据与限制**：38响应hash、40臂评分/SQL/执行状态重放一致；Gold md-340 Forge的compile_status在Pi为not_applicable、replay为pending，保留投影差异，双方仍零派发且评分null。共享提示词/Schema/ContextSnapshot、生成契约、模型和数据与seed45相同；协议v2及三处中间日期机制源码指纹不同，不能作为正式配对或优化因果证据。运行后validate通过，replay/compare拒绝不完整基线；本轮未改代码/Gold/原分数，临时服务已停止。简报与证据：`benchmark-luna-mixed-d20-seed46-2026-09-06.md/.json`。
- **seed48授权与冻结（2026-09-06）**：用户“好的继续”后明确选择“执行本轮38次”。沿REQ-048预定off父Run与全部53份不同run_id的Luna历史，20题=19延续＋新md-313＋0复核，seed48，日期/粒度off；Gold md-340保留零生成，余1位按70%/30%最近整数为1/0。旧24次未派发不沿用，无重试/换题/补跑。
- **seed48协议维护**：首次提交在Run创建前被Python/JavaScript的1.0→1序列化差异拒绝，零模型调用。删除没人消费的selection.remaining_new_fraction，新冻结保留原整数计数、case IDs、上下文和38次预算；不改hash算法或抽样规则、不覆盖旧冻结。修复前HTTP回归409，修复后真实协议200、Run创建202；Python819 passed/26 skipped、Pi150 passed/typecheck通过。
- **seed48结果**：Run `pbr_f24226875eaf4a56b5fb560d61b94dc2`实际32派发/30候选；md-234 Forge生成120秒超时，md-447 Forge因失败收敛取消并保存aborted / Request was aborted。md-472/481/483共6臂未启动；md-340 Gold未知，全部保留20题分母。两臂EX均4/12/4、Contract均1/15/4（正确/失败/未评分）；观察107160 tokens，另2臂未知用量，不补零。唯一新题md-313双臂全过，19延续仍无双臂完整恢复。相同15道已评分延续题EX3→3/3→3、Contract0→0/1→0；Direct md-429 EX恢复、md-259 EX/Contract回退。md-199新Forge候选缺少best_ms投影，与REQ-049旧候选已通过的确定性修复分开记录。
- **seed48证据与后续**：32份留存raw_output hash、32条dispatch/payload日志及446总日志封存；40臂原候选不变，核心评分/SQL/执行状态重放一致，独立16 Gold＋28可执行候选的官方EX核心复算一致。生成失败在候选重评中归为空输出，错误原因/不可重试属性以原Pi日志为准；Gold编译投影差异也保留，不改变分数。19共享题输入hash和模型一致但源码/运行时不同，不作因果比较。运行后validate有效，replay/compare拒绝不完整基线，隔离服务已停。简报：`benchmark-luna-mixed-d20-seed48-2026-09-06.md/.json`；下一轮19延续＋1新＋0复核，累计Luna曝光50/未见450；剩余预算不自动滚存，未启动下一轮。

## REQ-2026-09-05-047：严格归因复核与实际日期格式单变量实验

- **状态**：`validated`（严格归因复核、日期开关与20次配对诊断均已闭环；未达到推广门槛）。用户最初回复“同意，那就继续吧”授权归因复核与离线准备，随后在明确5题×两条件×双臂预算后选择“执行完整20次”；不授权追加轮次。
- **价值与范围**：复核纯输出争议及混合歧义标签，要求场景依据、实质差异和无法自行消除三个门槛；复用现有日期审计，把带来源、样本计数与局限的实际格式观察作为显式实验输入，验证可发现事实是否能改善绑定。
- **实施边界**：日期证据同时提供给Forge与Direct，不改原始CSV、Gold、语义口径、Compiler、评分或查询候选；既有ContextSnapshot的检索与结果契约不变。现有协议只允许Forge Prompt等因素，不能伪装成Prompt修改绕过共享context/evaluator约束；扩展一个严格限定的日期证据开关并绑定来源与内容hash，两条件使用相同代码/数据/模型。
- **风险、替代与机会成本**：有限非NULL样本不是整列格式保证，混合/未知布局与冲突必须显式披露，不能自动转换日期或按题号修复。仅给Forge补事实会混淆双臂可用信息，任意context变量又会放宽评价器门禁；选择窄开关、共享事实与固定源码，避免新增运行时调度或通用画像平台。
- **验收与调用门禁**：实际只读审计、上下文信息边界、来源/开关/样本漂移拒绝、离线冻结与预检、固定候选及正确/跨库对照均需验证。先封存题目、两条件和成功/回退标准，再单独确认模型预算；保持历史官方成绩与混合抽样规则，不自动扣除争议分母。
- **准备验收完成**：9题严格复核撤回4道纯输出歧义（2道输出粒度错误、2道表示差异），保留md-153/md-234中等置信度业务澄清与独立候选错误；不改历史判分。只读日期审计37字段，5题off/observed冻结与预检均就绪，双臂共享事实且snapshot/schema不变；10个历史候选两条件诊断重放不变。Python810 passed/26 skipped、Pi149 passed/typecheck通过。
- **实跑闭环**：用户随后明确选择“执行完整20次”。两条件共20派发/20候选，零补跑/替换，tokens73565、未知用量0。Forge EX/Contract2/5→3/5，Direct3/5→3/5；唯一新增通过md-289输入未变、无日期补充，不归因于处理。md-483双臂与md-077 Forge日期绑定改善但未通过完整结果契约，预声明推广门槛未满足；默认off，不扩样。20候选重放/hash及19个可执行候选独立官方核心复算一致，源码/数据/模型/生成Contract不变。简报与证据：`benchmark-luna-date-context-2026-09-05.md/.json`。
- **继续复核（2026-09-06）**：用户回复“继续”，完成md-483粒度及md-077姓名输出的零新生成调查。26条结果查询、5个实际DDL且外键有效的隔离反例、8份Prompt来源核对通过；发现本地each/per正则把grouped写入生成契约，姓名CSV支持拼接而Gold另有活动关联/DISTINCT。只修正归因说明，不修改运行时、Gold、候选或原分数。证据：`benchmark-luna-output-contract-review-2026-09-06.md/.json`。
- **Gold使用讨论**：用户原话：“但是我们在分析问题的时候可以用分析 gold sql 来寻找我们出错的原因，你觉得这样算作弊吗”。本轮明确：允许并应主动对照Gold做离线诊断和开发/回归调优，这不算作弊；限制的是向被测生成泄漏答案、人工改候选/Gold后冒充原始成绩，以及调参后仍宣称独立H。可以学习通用JOIN/筛选/聚合/投影规律，同时核对题意和来源，不机械照抄参考SQL。下一狭窄修复候选是移除未经确认的expected_grain，尚未在本次调查中实施或授权新增生成。

## REQ-2026-09-06-048：移除未确认粒度声明与共享提示单变量消融

- **状态**：executed_incomplete（工程切换已验证；76次授权中实际52派发，对照中途失败，配对结果不完整；报告已归档，不宣布准确率收益）。
- **原始问题与价值**：each/per正则把单位表达也标为grouped，并作为expected_grain进入两臂ResultContract。移除未经确认的粒度声明，避免把启发式包装为业务事实；不承诺因此自动提高准确率。
- **实施边界**：从正常Python/Pi ResultContract删除expected_grain和自动推断；不改问题/Evidence、检索范围、Compiler、其他比较规则、Gold或数据。默认不提供粒度建议；不是将grouped改成固定scalar。
- **实验设计**：复用已有参数型冻结与双臂指令后缀路径，增加仅显式消融可用的grain_context=question_heuristic/off。旧关键词规则只作为问题来源、未获业务确认的实验提示，置于生成指令而非ResultContract；两条件使用同一源码和完全相同的评分ContextSnapshot。此新基线不声称逐字复原seed46，只检验标注来源后的启发式建议相对省略的效果；历史成绩不得充当配对新基线。
- **风险、替代与机会成本**：直接用历史候选对比新生成混入时序/采样变化；放开任意context/source漂移会削弱可信比较，故仅允许窄参数及其共享后缀变化。保留显式实验对照不是生产兼容分支；避免更复杂关键词、题号特判或直接学习本题Gold输出。单次生成仍不足以证明稳定因果或泛化。
- **验证门禁**：复现单位表达误判；Python/Pi Contract干净切换，默认上下文不带粒度断言；日期、Schema、评分上下文和源码漂移仍拒绝。seed46原40臂用当前上下文零新生成重评，SQL与EX/Contract不变；保持Gold未知。实际CLI和HTTP冻结/上下文/评分链路及两侧测试均需通过。
- **选题与授权**：用户先回复“好的，那就继续吧”，接受狭窄工程切换；随后在预算选项中明确选择“执行完整76次”。本轮按seed46失败全部延续，20题=18延续+新md-199+旧md-040，seed47；日期off，Gold md-340留分母零调用。两条件各38次上限；实际52次派发后终态停止，剩余24次未使用，不自动重试、补跑或换题。
- **结果解释**：先报告共同可评分题的EX/Contract恢复与回退、原已正确臂的保护情况，以及tokens/未知用量；Gold未知继续阻止完整准确率和正式基线晋级。无新增通过或有未解释回退不宣布准确率收益；H与R0.6门禁独立保留。
- **工程验收与冻结**：Python813 passed/26 skipped、Pi149 passed/typecheck；seed46原40臂SQL与EX/Contract不变、20题上下文仅移除粒度字段及派生hash，4次真实HTTP评分与旧协议409通过。seed47冻结18延续＋新md-199＋旧md-040；两条件均预检ready，各38次，Gold md-340保留。该准备阶段新增模型调用0；原准备证据`benchmark-luna-grain-context-preparation-2026-09-06.json`保持不变。
- **付费执行结果**：对照Run `pbr_fb550c000ff34a559d1f79e7738107ec`为14派发/10候选，4次不完整响应，24臂未派发；处理Run `pbr_cb5bcb3341a24f619b7e8a75bbd83236`为38派发/38候选。总计52派发、48候选、观测172108 tokens，另4次用量未知；未补跑、未替换。对照各臂EX为3正确/4失败/13未评分、Contract为1/6/13；处理EX两臂均4/15/1、Contract Forge1/18/1与Direct2/17/1。
- **配对结论与滚动账本**：共同有效5题的两臂EX/Contract零恢复、零回退；完整配对缺失，不能宣布粒度提示移除的准确率收益。相对seed46沿用题，Forge md-249回退、Direct md-259恢复，仅为非因果滚动观察。预先指定off处理组为下一父Run，对照也计入暴露；下轮19延续＋1新＋0复核，Luna已见49/未见451，未启动下一轮。
- **付费实验复核时的缺陷**：52派发的原始响应hash和日志一致，56个核心评分/SQL/execution记录匹配（含4个Gold）；当时原生replay将另24个未派发臂误记为scored生成失败，报告以原始Pi状态为准。Pi当时未保存Provider stopReason/errorMessage，无法认定4次空响应根因。这两项维护缺陷已由后述零调用修复处理；Gold compile投影差异另列，原付费配对不晋级。
- **证据交付**：`benchmark-luna-grain-context-2026-09-06.md/.json`保存完整付费结果、原始Run、双冻结和逐臂审计。目录访问恢复时两条件bird validate均valid、model_calls=0，完成仓库回写；当时未修改运行时代码或原始候选。隔离服务已停止。随后维护改变源码指纹，须使用新的诊断冻结，不重写该历史校验回执；H与R0.6门禁独立保留。
- **后续维护闭环（零调用）**：用户“好的”接受两项维护修复。无候选且显式scored=false的replay记录保留未评分，真实生成失败仍计失败；新Pi原始响应与错误日志保留Provider终止原因及错误详情，并经Run重开验证。修复前两个反例失败，修复后Python814 passed/26 skipped、Pi150 passed/typecheck通过。当前源码新冻结diagnostic重放80臂全部匹配原Pi的SQL/评分/execution，24个未派发臂不再误记为失败，48个有效候选的输出/原始响应/评分不变，compare仍拒绝晋级。证据：`benchmark-luna-grain-maintenance-2026-09-06.json`；原付费JSON与Run不改写，旧源码冻结不续跑，4次历史Provider根因无法追补。

## REQ-2026-09-06-049：SQLite 简单标量极值 CTE 的重复计算瓶颈

- **状态**：verified。用户明确选择“修复执行瓶颈”，接受零模型调用、原候选复核和SQLite≥3.35边界；实现、原候选重放与兼容边界均已验证，不授权新的模型生成。
- **用户原始表达**：“好的，所以我们进行下一步吧”。该表达授权继续推进与调查，不解释为自动增加模型预算或已接受尚未说明的Compiler兼容性变化。
- **问题与证据**：上一轮off Run `pbr_cb5bcb3341a24f619b7e8a75bbd83236`的md-199 Forge候选因execution_timeout失败。原SQL以coroutine计算全局MIN并在外层循环中扫描；保持结果列、筛选含义和JOIN关系，只增加AS MATERIALIZED的只读SQL反事实约87.61ms完成，结果与Gold相同。另一个标量子查询反事实约64.79ms完成并匹配Gold；均不计为原候选收益。2000行隔离夹具中，普通CTE达到500000条VM指令预算仍未完成，物化版本约30000条完成且保留两个并列最小值，含NULL输入。
- **价值与假设**：优先验证可确定性复现的执行瓶颈，尝试让同一份逻辑正确Forge JSON在既有预算内执行；比再次付费随机生成更直接。单题与微型夹具不能证明所有CTE应物化，也不代表总体准确率提升。
- **建议实施边界**：只在SQLite编译目标中，针对参与JOIN、无过滤/分组/窗口/集合操作、直接列MIN/MAX的简单非递归标量CTE，验证窄物化策略。原Forge JSON、Gold、Prompt、输出列、并列值、NULL语义与评分不变；不按题号/表名硬编码，不加自动重试或通用SQL改写，不把全部CTE强制物化。其他四种方言不引入SQLite提示。
- **兼容性与风险**：AS MATERIALIZED要求SQLite3.35.0及以上；本机为3.53.4。物化是优化屏障，可能损失下推或增加临时存储，不能推广到分组、递归、复杂表达式或有不确定函数的CTE。保持编译确定性，不依据本机SQLite版本偷偷改变相同输入的编译输出；用户接受后须明确文档中的目标引擎要求。来源：https://sqlite.org/lang_with.html#materialization_hints 。
- **替代方案与机会成本**：继续粒度单变量新配对需重新冻结19延续＋1新题并另批模型预算，原76次中的24次不自动补跑；它不能直接解决当前执行瓶颈。只提醒模型改用标量子查询依赖再次生成，不能作为同候选Compiler收益；无条件重排JOIN或强制所有CTE物化可能改变语义或性能，不采用。
- **验收门禁**：先保留修复前失败的固定VM预算回归，再确认同候选md-199在原执行预算内完成且EX/Contract通过；覆盖并列极值、NULL、空输入以及不应优化的结构。全部原候选离线复核，明确SQL变化和评分恢复/回退；不覆写历史Run或旧冻结、不混版本续跑，不发起新模型调用。
- **调查结论补充**：md-447的漏引号错误已存在于原始filter.col表达式；现有Prompt已有标识符引用规则。只补引号后可执行，但仍与Gold结果不同，不新增重复Prompt规则，也不将人工改写记作优化收益。
- **原始证据**：`.forge/benchmarks/luna-scalar-cte-investigation-20260906/evidence.json`，保存只读执行计划、真实数据库反事实、VM预算夹具及来源限制。
- **实施闭环**：仅对INNER/CROSS JOIN中的简单列MIN/MAX标量CTE使用窄物化，未扩展DSL或推断业务含义。两个修复前预算反例失败、修复后通过；NULL、空输入、并列值、引用别名与非聚合下推保持。13种排除结构与原源码编译一致，24旧Forge候选共120项五方言编译结果仅md-199/SQLite改变。
- **同候选结果**：原80臂output/raw_output不变，只有off/md-199/Forge从30秒超时恢复为Assurance/执行/EX/Contract通过，独立本机烟测79.26ms；其余79臂核心状态/评分/SQL不变。off Forge EX正确4→5、Contract1→2，各仍1未知。Python818 passed/26 skipped、Pi150 passed/typecheck通过。新diagnostic有效，旧冻结与诊断晋级拒绝，旧付费结果及后续预算不变；不宣称新模型收益。
- **最终简报与证据索引**：`benchmark-luna-scalar-cte-fix-2026-09-06.json`；测试日志、全部80臂审计、五方言对照、边界烟测与冻结回执均有来源哈希。

## REQ-2026-09-06-050：生成终止后的证据封存顺序

- **状态**：verified。本次按用户继续优化的授权处理已确认行为的维护缺陷，不增加模型预算或改变生成失败策略。
- **用户原始表达**：“然后，那就继续去优化它吧”。
- **问题与证据**：seed48的md-234 Forge在120秒上限前有至少6250个流事件，却只封存assistant=[]；md-447的取消保留了部分消息。修复前代码在catch中计算raw_output/hash/usage，然后在finally等待AgentSession.abort完成；真实SDK的abort是等待idle的异步操作，最终assistant与usage可在此期间才提交。不能由历史缺失证据判断模型在思考、生成参数还是阻塞。
- **价值与边界**：使超时/取消后的原始响应、已报告用量、hash与重开Run一致，供后续生成质量优化归因。先用确定性取消夹具复现，再让取消完成先于失败封存；同一会话取消操作复用，不提前退订末尾事件。不把部分或迟到响应提升为有效候选，不修改120秒上限、失败即停、授权计数、重试、Prompt、Schema、Compiler、Gold或评分。
- **风险与替代**：取消本身可能缓慢；原代码finally已经等待同一abort，本次不额外增加生成等待或放宽deadline。逐片复制/保存完整流可能引入平方级拷贝和大量日志，暂不采用；依赖SDK最终消息提交，而不是新增第二套响应真相源。历史未保存内容及usage不可追补。仅等待最终化不保证Provider提供usage，未知仍保留。
- **未采用的优化**：md-032/199的未输出列引用在原JSON中已存在，自动改名/补列会篡改意图；其他返回候选没有确认的Compiler语义偏差。此前投影Prompt强化未达到配对收益门槛，不重新采用或按Gold特判。延长超时、继续失败Run或追加模型调用需独立评估与授权。
- **验收**：修复前deadline回归失败，修复后Run重开仍留存终止消息、最后实际usage与可复算hash；超时仍失败且零重试，未启动臂不被派发。覆盖真实SDK取消顺序的零模型调用烟测，现有Pi全套/typecheck与Python相关验证；原40臂只作零调用诊断重放，候选/SQL/EX/Contract不变，不追改历史失败或晋级完整基线。
- **修复与验证闭环**：同一会话复用abort Promise；catch在取消完成后才读取响应/hash/usage，finally随后退订和dispose，不复制逐片流或增加第二套状态。deadline回归修复前assistant=[]而失败，修复后重开Run仍保留终止消息、最终已知usage、日志/hash；有效SQL文本若在取消时到达仍不送评分，后续题仍未派发。真实Pi 0.84.2 SDK的合成流烟测证实：取消刚发出时0条消息/0 tokens，等待完成后1条aborted消息/93个合成tokens；无网络、无真实模型调用，93不是Luna补账。
- **原候选与证据**：Pi151 passed、typecheck、Python819 passed/26 skipped；seed48原40臂diagnostic重放的output/raw_output、SQL、EX、Contract、执行/失败状态均不变，旧Run/冻结/重放hash未变。Python源码未改、原协议CLI校验仍有效；Pi runtime指纹已变，旧Run不得跨版本resume。compare继续拒绝不完整诊断；旧未知usage和缺失响应不回填。报告：`benchmark-luna-cancellation-evidence-fix-2026-09-06.json`。仅闭环诊断可靠性，不宣布准确率、成本收益或外部采用验收。

## REQ-2026-09-06-051：生成流阶段的最小定向诊断

- **状态**：verified（诊断闭环，未识别旧超时根因）。用户明确选择“执行4次诊断”；实际4次、零重试，准备阶段与离线复核零模型调用。
- **用户原始表达**：“好的，那就继续做吧”。承接REQ-050后取得可靠终止证据、区分可见推理与工具参数阶段的建议，不解释为放开预算或直接修改生成策略。
- **价值与设计**：只选seed48中Forge超时md-234及被连带取消md-447，各一个单题双臂Run，按题串行、两臂仍走原Pi。独立Run消除跨题取消干扰；同模型、Prompt、Schema、数据与120秒上限，日期/粒度off，每臂一次，总上限4。不是滚动seed49，不替代seed48父Run或借机排除其他失败题。
- **观测边界**：用单次实验的只读SDK订阅记录各事件类型首末时间、计数和delta长度，及终止原因；不保存逐片内容/累计快照，不改流、工具调用、Prompt或评分。观察器源码及输入hash留证，不新增生产遥测、API或任务真相源。不可把首次可见事件前的等待等同内部推理；可见thinking片段也不覆盖服务端全部思考。
- **风险、替代与机会成本**：四次生成可能无法重现偶发超时；一轮只作诊断，不为复现追加请求。扩大到20题消耗更多且混入滚动目标；只看旧空响应无法补回丢失内容，直接延长timeout或缩减DSL无根因证据。保持独立Run而不改失败即停策略；认证/额度拒绝时不继续尝试。
- **门禁**：先固定两个协议、确认Gold可评分和共享输入一致；观察器先用真实SDK合成流做零网络烟测，确认正常返回/取消与用量不被改写，再单独确认4次预算。实际生成后保存全部原始Run、日志、阶段摘要和候选重放；失败和未知不消失。没有足够根因证据不采纳Prompt/Schema/超时改动，结束必须交付简报。
- **实际结果**：两个单题Run均completed，4个候选可执行，EX/Contract均0/4；观察19831 tokens、未知用量0。Forge md-234生成11.18秒、md-447为23.60秒，工具参数可见区间约3.27/7.46秒；未复现超时/取消。4份Prompt hash与seed48对应臂一致，不代表新随机响应能证明延迟收益。
- **结果差异**：md-234题目要求次数而Gold仅含2行地点/坐标；Forge加COUNT为2行4列，Direct按circuitId为3行4列。只读删COUNT可让两臂EX匹配，但Direct仍有重复坐标，不采纳此删列。md-447两臂57行选DOCType、Gold选DOC；只替换该投影的离线反事实与Gold行多重集完全相同，不改实际评分、不自动归类为业务歧义。
- **验收与决定**：4份响应hash、4臂原候选重放与独立只读EX一致；57条日志、4条阶段记录、完整Pi快照已封存，服务停止。未修改生产源码/Prompt/Schema/Compiler/Gold/120秒策略，不追加模型请求；seed48仍是滚动父Run，不晋级H或基线。简报：`benchmark-luna-generation-stages-2026-09-06.md`，机器证据及25份工件hash见同名JSON。

## REQ-2026-09-06-052：保留CTE字段绑定的确定性编译修复

- **状态**：verified；确定性Compiler与公共Assurance修复完成，零新增模型调用。
- **用户原始表达**：“好的，那我们继续优化”。承接REQ-051后优先离线定位可复现根因，不解释为自动授权新模型预算。
- **事实与价值**：seed48 md-032/md-199确实引用CTE未输出字段，不能机械补列；检查该链路发现Compiler剥离显式CTE限定名，把合法投影、聚合和窗口分区编译成JOIN同名列歧义，三条路径已在隔离SQLite复现。相邻CTE投影补前缀还会把输出别名或仅属于右侧CTE的裸列错误绑定到主CTE。
- **决策与边界**：删除CTE专属的前缀剥离/猜测，复用统一关系绑定、别名展开和Assurance校验；显式select仍定义输出，不自动补聚合列、不猜缺失字段或参考答案。不改Prompt、Schema、Gold、评分、120秒/单派发策略或Pi主状态。
- **风险与替代**：部分旧SQL文本会保留原限定名；原来依赖猜测的歧义裸列不再静默选主CTE，需如实报告影响。只加JOIN类型特判会继续遗漏别名与窗口/聚合路径；扩大生成或改提示词无法修复确定性转换错误。维护收益是合法输入不被Compiler破坏，不承诺BIRD得分提升或泛化。
- **验收**：先固定源码/原候选hash及失败反例；用真实SQLite结果验证投影、聚合/分组、窗口和CTE别名作用域，并保留缺列/越域拒绝。按五方言重编译原固定候选，对SQLite变化项独立复算；运行受影响测试与实际公共入口烟测，保留旧Run/分数并交付简报。
- **调用链补充**：公共Evaluate负例暴露既有CTE字段盲区：`picked.missing`可能被允许送审，虽不授予执行权仍违反字段保障。保留限定名后必须同步保证此边界；Forge JSON的末尾校验复用已有`assure_compiled_sql`，而不是再造CTE符号解析器，Assurance修订升至v8。关系/业务Policy不放宽；合法SQL不等于通过所有业务门禁。
- **闭环结果**：删除CTE前缀剥离与主CTE裸列猜测；5个合法查询的SQLite结果反例修复前失败、修复后通过。公共SELECT/WHERE中的CTE未投影字段两个漏拒反例已拒绝，歧义裸列拒绝对照保持；Forge JSON复用编译SQL门禁，Assurance v8，保留原关系/Policy及前置失败证据。
- **原候选与验证**：5份历史Run的40个Forge候选共200项五方言编译结果不变，原文件hash未变；v8新diagnostic冻结下md-032/md-199四臂SQL/EX/Contract不变，两个Forge缺列仍拒绝。实际CLI/HTTP合法窗口allow_review、越域/缺列deny且均不授权执行；返回SQL在自建内存数据库得到预期行。Python825 passed/26 skipped、Pi151 passed/typecheck通过，服务已停。
- **限制与交付**：不改Prompt/Schema/Gold/评分/生成策略，不声称BIRD准确率或性能收益；既有公共CTE→物理表关系与右CTE裸列前置限制未放宽。不是seed49，父Run与H/R0.6门禁不变。简报：`benchmark-luna-cte-binding-fix-2026-09-06.md`及同名JSON。

## REQ-2026-09-06-053：v8固定候选回归收口与枚举绑定证据审计

- **状态**：verified；离线调查完成，新增被测模型调用0；取值证据处理仅提案，未实施或授权生成。
- **用户原始表达**：“所以你觉得下一步我们应该是做什么？是继续做测试，还是换测试方案继续测？还是说，我们要去在其他方面优化我们的准确率？”接受建议：“好的，按照你的方案继续吧”。
- **价值与方案**：暂停无明确变量的滚动重生成；复用R500及既有错题归因，在当前Compiler/Assurance v8下重放500题1000份原候选，量化新拒绝/旧正确误拒/仍放行错误。再审计一个确有出处的值绑定机制，不重复全量归因。
- **审计切片**：在看本轮结果前选择md-009/md-026共同的gasstations.Segment大小写枚举绑定；核对实际存储值、原候选及已有输入，区别缺失证据与已有证据未使用，不以Gold值回灌生成，不自动lower/LIKE或改候选。
- **风险、替代与机会成本**：当前栈与历史跨多次修订，不能把差异全部归因于v8，也不能当模型增益；R500已曝光，未知/超时留分母。重抽难题继续生成只能观察波动；堆无增量Context会增加成本。若已有输入已包含枚举或无权威来源，停止追加假设，不扩大调用。
- **验收与边界**：保存原候选、源码与数据hash，运行既有离线CLI并审查变化项；公共只读数据实际执行证明绑定事实。原Gold/评分/Prompt/Schema/生成策略/旧Run不改，不启动seed49、不推进H/R0.6；交付测试简报及机器证据。无新行为时不新增永久测试；若发现源代码Bug，单独登记维护而不混入收益归因。
- **回归结果**：1000候选/1000历史SQL/500 Gold SQL hash核对，当前源码与数据不变。Forge EX311/187/2、Contract282/216/2；Direct EX312/186/2、Contract292/206/2（正确/失败/未知）。旧正确候选误拒0、已评分正确/错误转换0；md-340/md-393两臂因30秒Gold超时由历史通过转未知，不降为错误。14臂变化已全审查；三个原缺列Forge提前拒绝，两个SQL变化仍无判分收益。
- **绑定结果**：当前两臂Segment元数据仅chain segment、无枚举；公开只读全列5716行有5个精确值。md009 Direct及md026双臂仅替换Discount/Premium字面量的三个反事实与Gold相同，原实际成绩仍1/4；无模型增益。历史完整Prompt未知，不声称原模型已看到或忽略枚举；合成反例否定自动lower/LIKE。
- **决定与交付**：不再盲修Compiler或重生成；提出双臂对称的限定列观察取值证据处理，尚未应用。候选D4为md009/md026＋历史正确对照md002/md018，2条件×2臂最多16次生成，仅提案、预算另批。当前CLI完整重放因4未知返回complete=false/exit1，不能晋级；Benchmark仅共享SQL门禁，不冒充公共JSON全链。进程均退出，简报及机器证据：`benchmark-r500-v8-binding-audit-2026-09-06.md/.json`。

## REQ-2026-09-06-054：限定列观察取值证据的单变量实验开关

- 状态：实现与获批16次配对实验均已完成；验收未通过，value_context保持默认off，不补跑、不扩样。
- **用户原始表达**：在REQ-053确认当前Segment取值证据缺失后，用户：“好的，我们继续优化”。
- **价值与方案**：复用日期/粒度实验的freeze→context→Pi路径，增加默认off的value_context单因素；显式选择数据库/表/列，仅观察当前可见的这一列，为两臂提供相同精确取值和来源。限定md009/md026及md002/md018四题开发实验，不凭题号修复SQL。
- **边界**：只适用于已固定公开SQLite实验，不增加生产探库权限或第二套知识库/调度。只读、5秒采集、默认至多16个值/每值256字节；超界、非文本、失败不泄漏部分值，不自动lower/LIKE或改原值。字段限定、观察覆盖与快照局限显式记录。
- **风险与替代**：高基数/长文本会膨胀Context，数据中的指令不得当命令；同名列不得串域，完整快照取值不是未来业务枚举定义。人工补CSV会污染固定数据，泛化RAG或自动改写均扩大范围；采用带hash的显式实验投影，原Schema/Gold/评分/Compiler不变。
- **验收**：默认off实际指令逐字不变；处理组两臂后缀一致，限定列/来源/上下文完整性及参数漂移失败关闭，旧冻结不冒充新协议。实际CLI/HTTP与Pi消费验证、越域/高基数/大小写边界回归，冻结四题两条件并交付简报；未单独批准预算不生成。
- 实施与证据：`forge/benchmark_metadata.py`限定列有界只读采集，`bird-protocol-v4`冻结/CLI/HTTP贯通；Pi继续消费完整冻结指令、无需第二套生成路径。md-009/md-026/md-018每臂+1642字节，md-002同名异表不附加；off八指令与旧版逐字一致，原八候选两条件回放不改SQL/评分。真实CLI/HTTP/Pi合成消费及漂移拒绝通过；Python873 passed/26 skipped、Pi151 passed/typecheck通过，服务/临时脚本已清理。开发环境uv误同步的三个依赖及前后版本已披露，最终两条件使用同一当前环境。见`benchmark-luna-value-context-ready-2026-09-06.md/.json`；不宣称模型收益，不推进seed49/H/R0.6。
- 追加授权：交互确认“批准16次对照”。固定md-009/md-026目标与md-002/md-018控制，双臂×off/observed；处理目标四臂EX/Contract全过且同期至少新增1臂，控制无回退、用量完整、处理总tokens≤1.25×off。120秒、零重试，生成/基础设施失败即停，不补跑/扩样；金额未知，token门槛不是费用硬上限。授权工件：`.forge/benchmarks/categorical-value-context-20260906/generation-authorization.json`。
- 配对结果：实际16派发/16候选、全可评分；Forge EX/Contract 2/4→3/4，Direct 3/4→4/4。目标四臂1/4→3/4，未达预声明4/4；控制四臂两条件均通过。35572 tokens、未知用量0，处理总tokens+17.85%低于25%线，但准确率门槛失败。md-009 Forge已用Discount，却漏接cze/svk的FROM/JOIN；共享Assurance v8通过后SQLite报no such column: cze.cnt，保留失败，不自动补JOIN。16响应hash/原候选回放/独立SQLite判分一致，216日志与Pi快照封存、服务已停。见`benchmark-luna-value-context-2026-09-06.md/.json`。下一窄维护候选为FROM作用域漏拒，未在本实验修改源码或追加调用。

## REQ-2026-09-06-055：共享SQL门禁的FROM作用域绑定维护

- 状态：已验证完成（2026-09-07），零模型调用。
- 用户原始表达：助手建议“先做零模型调用的FROM作用域校验维护，而不是继续增加Prompt或放宽评分”，用户回复“好的”。
- 价值与根因：REQ-054 md-009 Forge声明cze/svk却未在外层FROM/JOIN引用，仍通过v8。SQLGlot的Scope.sources包含可选CTE，不能替代实际绑定；在现有共享SQL保障中补足可见关系检查，把真实执行错误前移到拒绝。
- 边界：保护合法CTE、别名、相关子查询、递归及方言行为；不自动补JOIN，不改Compiler/Schema/Prompt/Gold/评分，也不恢复16次预算或推进seed49/H/R0.6。
- 风险与替代：全局名称匹配或无条件禁止外层引用会误拒合法相关查询；依赖SQLite报错过晚，自动补关系则猜测业务意图。复用SQLGlot作用域模型及共享Assurance，无第二套解析器/执行旁路。
- 验收：修复前失败回归、修复后真实CLI/HTTP拒绝原案例；合法查询隔离执行一致，历史R500和REQ-054固定候选门禁差异全审，旧正确不误拒；保留原候选/分数/未知与完整证据，回写简报。
- 结果：Assurance v9按实际绑定与使用环境校验；1016份历史SQL仅新增拒绝原md-009处理组Forge，R500门禁不变、旧正确新增误拒0。16原输出/SQL/EX/Contract不变；65个SQLite边界及18项静态方言检查通过，公共CLI/HTTP前置拒绝、合法返回SQL只读执行通过。Python887 passed/26 skipped、Pi151 passed/typecheck，13份原来源hash不变，服务关闭。
- 限制与证据：这是执行前拒绝维护，不是生成收益或评分改判；非SQLite方言/LATERAL仅静态检查，原有SQLGlot限制未放宽。旧冻结及诊断晋级仍拒绝，value_context保持off、父Run仍seed48。见`benchmark-luna-from-scope-fix-2026-09-07.md/.json`。

## REQ-2026-09-07-056：问题到Forge JSON生成链路的优化候选评估

- 状态：用户授权的16次分母范例配对生成已完成；目标题恢复但正确对照回退，未过预声明门槛，不采纳默认、不补跑/扩样。其他优化候选仍未实施，R0.6门禁不变。

### 用户原始表达

> 根据过去错误的一些信息，你觉得在我们的 Forge JSON 生成阶段，其实我们优化的主要还是从用户提出问题、到生产出 Forge JSON 的这个过程。
>
> 在整个过程当中去做优化的话，你觉得还有什么可以优化的项？

- 后续确认：用户回复“好的，继续”。按上一轮建议只推进分母范例、正反对照和冻结准备；实际生成另行提交明确调用预算。
### 事实与价值

- 目标是增加正确Forge JSON/完整答案产出，不以Schema通过、执行成功或提前拒绝替代EX。REQ-055只前移错误，原16候选EX不变。
- REQ-034历史Sol初判的46个Forge候选错误中，值绑定13、关联/过滤7、输出契约7、排序/限制6、粒度5、算术4、作用域4；不是独立人审、当前Luna分布或可承诺收益。后续归因更正优先于早期歧义标签。
- 当前Pi Benchmark使用一次strict emit_forge_query、单回合、零重试。评估时agent/prompts.py的旧build_system有按需示例，build_structured_benchmark_system只有紧凑语义约束与上下文，不走旧示例分支；本次保留默认，只注册下文的独立候选revision。
- 本轮零模型调用实际构造md-009/082/259/483上下文，分别包含5/3/10/8张表且sufficiency=sufficient；当前小库全表纳入。ResultContract.required_output_semantics仍为问题/Evidence词集合，不是已确认的对象、分母、粒度和输出映射。不能把结构覆盖视为业务语义充分，也不能先假定缺表。
- 历史投影指令实验Forge4/5→4/5，无采用证据；取值证据有局部改善但未过采纳门槛，日期证据未产生可归因的完整答案新增通过。二者保持off，不能重新包装为已证明有效的默认优化。

### 候选、优先级与风险

1. **P0：有来源的答案约束。** 复用现有Context/Registry/ResultContract，整理计量对象、范围、分母、时间边界、展示粒度、输出字段、单位和排序。区分用户明示、已有权威定义与未确认推断；不把词袋或each/per正则升级成业务事实，不新建Intent真相源或第二套DSL。首先评估一次生成内的组织方式，不预设额外规划调用。
2. **P0：集合、JOIN与聚合语义。** 针对md-082实体被明细放大、md-259分母被内连接缩小等独立错误，表达关联是用于存在性过滤、取展示属性还是参与计量。基数从真实键约束/已确认关系推导，不凭字段名称或复合键成员标记猜唯一性；不通用补DISTINCT或换JOIN。
3. **P0：字段、实体与值绑定。** 将有增量的限定字段来源、精确存储值、时间表示、编码/标签及已确认业务定义提供给生成器。缺口先从获授权来源解决；只有影响答案且无法消解的业务分歧才澄清。现有value/date候选保持off，下一实验不同时开启。
4. **P1：按查询结构提供少量已验证范例。** 当前严格输出链可以独立评估1–2个合成/脱敏的结构例，而不是继续追加笼统禁令。重点区分CTE中间select的下游接口与最外层select的答案接口，以及独立分母/窗口后筛选；不按题号注入Gold，不强制简单查询使用CTE。旧例须先核对当前DSL/Compiler，不能直接搬过来视为已验证。
5. **P1条件项：一次确定性诊断返修。** 只对合法上下文内的作用域/类型/结构错误，评估Pi原Task最多追加一次生成；沿用现有Failure/Evidence并明确缺少的局部诊断，不把generic错误码假装成完整修复方案。初稿/终稿与成本分别保留，复检全部门禁，SQL变化使旧审批失效；不修权限拒绝、不按Gold择优。当前单回合协议不允许，必须独立协议与调用预算。
6. **P2：上下文降噪和预算。** 先量化重复说明、诊断trace/hash进入模型输入的必要性，完整审计材料仍保留；有用语义/字段不能为省token被删。当前小库样本不是未检索到表，不先上通用GraphRAG、向量库或裁掉DSL能力。大库检索须另证实召回缺口。

### 建议的首个窄实验与验收

- 首选只验证一个“分母不随展示关联被缩小”的合成查询范例；以md-259为已知开发探针，补充预先冻结的正反对照：分母确为全集、分母明确要求存在关联、NULL/重复明细及不涉及比例的正确题。不把选择该题视为H，也不硬编码其分母或答案。CTE输出接口范例作为另一个独立候选，不同轮叠加。
- 只更改范例这一Prompt变量；同模型、源码、Schema、上下文、评分、超时与单次派发。结构上不能满足分母或输出要求时，不由Compiler补救。若同时改变业务定义、观察值或修正轮数，则拆成另一实验。
- 先确认合成范例按现有DSL可编译且结果符合独立预期，再冻结控制/处理、调用数、成本/P95门槛和停止条件，由用户另批生成。该零调用准备已在下文完成，真实生成尚未授权。
- 主指标是完整分母的最终EX/Contract及新增正确/改坏正确；同时记录分母、输出遗漏、误拒、未评分、tokens/正确答案和P95。多重错误的题仅修一个子步骤不能算完整成功；不按子集或历史最好一次选优。开发信号通过后才做R/H验证，不承诺80%/90%。

### 替代方案、机会成本与职责

- 更大模型、全量RAG、统一两阶段规划、多Agent投票、无限重试及微调均非首选；会混淆信息缺口与推理错误，增加调用/维护成本，当前没有稳定收益或足够独立标签证据。只堆投影提醒已有无收益反例。
- 语义/检索改善尽可能对Direct与Forge共享；单独记录Forge DSL范例的适配收益，不以偏置上下文制造格式优势。Forge仍是可信执行层，Pi仍是唯一调度与Task真相源，DATA Skills只供方法，不直接取得数据库执行权。
- 原候选评估轮只改需求池，未改生成源码或调用模型；分析依据为REQ-034、044、047/048、054/055及对应简报。用户接受后的首个准备切片见下文，未推进外部采用。

### 首个分母范例的零调用准备结果（2026-09-07）

- 准备已完成，真实被测模型调用0。只增加一个虚构accounts/invoices范例；8组完整PK/FK SQLite夹具、16次实际Prompt范例/反向对照的编译→Assurance→执行符合独立集合预期。覆盖NULL、重复明细、无关联、零分子、空总体；反向例与夹具结果不进入Prompt。
- 默认500题四类输入hash逐项不变；候选仅增加Forge指令2209 UTF-8 bytes，Direct/Schema/Context及数据、Compiler、Assurance不变。复用prompt单因素和有限内置forge_prompt_revision；HTTP严格校验冻结，Pi绑定/持久化实际revision，compare仅放行与各自manifest一致的Prompt revision差异，其他运行合同仍固定。
- 已冻结md-259（分母目标）、md-079（明确女性子集分母）、md-000（EUR/CZK普通比率）、md-312（非比例正确题），控制/处理Gold均ready。后两种分母对照的正确历史来自Sol，不冒充当前Luna表现；四题都是开发探针，不是H。md-259的分母与漏投影来自不同历史候选，最终必须按完整EX/Contract验收。
- 实际CLI/鉴权HTTP与Pi生产类消费通过；Pi生成用合成会话而非真实Provider，16次合成派发、外部网络尝试0，双条件持久化重开一致，上下文漂移在派发前停止。Python889 passed/26 skipped，Pi155 passed/typecheck通过；不把这些结果写成准确率提升。
- 提案为4题×2条件×2臂，最多16次Luna调用，当前授权仍0。控制后处理、原生Pi双臂、case concurrency=1；120秒、单回合、零Provider重试/替换，任一条件运行失败即停止后续生成，不沿用旧预算或启动seed49。
- 预声明开发门槛：md-259 Forge须由本次控制EX/Contract均失败变为处理均通过；所有控制正确不得回退（含Direct稳定性对照），16臂评分/用量完整。处理总tokens≤控制1.20倍、Forge每EX正确答案tokens不增、Forge生成P95≤1.25倍；零正确分母的成本指标未定义、不算通过。4个延迟样本的P95只作护栏。门槛不满足/无法判断即保留默认且不补跑；通过也仅允许讨论另批R/H确认，不自动启用。
- 机器准备报告：`benchmark-denominator-scope-ready-2026-09-07.json`；精确协议、预声明门槛和复现证据：`.forge/benchmarks/denominator-scope-ready-20260907/`。未改Gold/历史评分或外部采用门禁，滚动父Run仍seed48。

- 生成授权：用户在明确16次提案后回复“好的”；独立授权记录为`.forge/benchmarks/denominator-scope-ready-20260907/generation-authorization.json`。不沿用旧预算；原准备报告/experiment-card保留为生成前证据。

### 实际配对生成结果（2026-09-07）

- 用户授权16次，原生Pi实际16派发/16候选，双Run completed、评分及用量未知0，无补跑/替换。控制Run `pbr_eb3c228f529e47bbb14e7a5e78e14554`，处理Run `pbr_9450b18fa549445ea38d1c38b6468ed6`；固定原四题、单回合、零Provider重试策略和同一model/runtime/SDK，未改变准备版本源码。
- 完整四题分母下Forge EX/Contract均2/4→3/4，Direct均2/4→2/4；两条件Forge执行均3/4、Direct均4/4。Forge新增通过md-259/md-000，回退md-079；Direct没有标签变化。compare可比，但不能据此宣称总体或稳定因果收益。
- md-259控制组关联后总体741，且只返回741/212/118三个计数，没有按要求输出比例/数量两列；处理组把总体独立为750，输出28.266666666666666与118，EX/Contract完整通过。独立读取英雄/阵营/出版商基础记录并用Python集合计数一致。
- md-079处理组分母仍限定SEX=F，回退不是“总取全表”：CTE聚合声明别名`n_last_presence_placeholder_wrong? no`，却select `n`。原响应确实如此，Assurance以unknown_schema_reference拒绝，原SQL独立执行报no such column: n；不修候选、不改判。md-000由错误投影`select:["1"]`恢复为EUR/CZK比率。Direct两组md-079仍漏乘100，md-259仍被INNER JOIN缩小分母。
- 已观测tokens总53058（控制25753、处理27305，+6.03%），未知用量0；Forge每EX正确答案tokens 8724.5→6333（-27.41%），Forge生成P95 21.68s→16.93s，按每条件4项nearest-rank计算，仅作本样本护栏、不当稳定性能结论。成本与延迟门槛通过，真实账单金额未知。
- **预声明“正确对照不得回退”门槛失败，不采纳为默认。** 默认Prompt及date/grain/value的off状态保留，不补跑、扩大、自动返修或开始seed49。其他门槛通过和净多答对1题不能覆盖此失败。
- 16份原始响应hash、218条完整日志、16原候选重放、16条原SQL独立SQLite核对与导出的原生Pi快照一致；两项原Assurance拒绝保留（其中`SELECT "1"`可被SQLite字符串回退执行但答案仍错）。25份准备工件及冻结源码/数据/上下文hash不变；服务端口关闭，原生状态/快照封存。
- 正式机器简报：`benchmark-luna-denominator-scope-2026-09-07.json`。下一候选为单独评估CTE聚合别名与中间投影接口一致性，不再叠加分母提醒；这里只记录方向，未实现或授权新的生成/返修预算。父Run仍seed48，H与外部采用门禁不变。

## REQ-2026-09-07-057：CTE接口诊断中的表达式别名绑定维护

- 状态：已验证并封存的确定性Compiler维护；本轮CTE接口诊断完成。没有新提示词实验、自动返修或被测模型调用。

### 用户原始表达与承接

> 继续

- 承接REQ-056最终建议：下一项单独验证CTE聚合别名与中间投影接口一致性。上一轮16次预算已耗尽，本轮授权/实际被测模型调用均0。

### 事实、价值与最小处理

- REQ-056处理组md-079是模型声明别名与select不一致，原SQL缺n；仍应拒绝，不猜别名或补列。合法显式CTE输出按现有DSL能够运行。
- 新隔离反例：源列facts.n为700/800，另声明SUM(facts.amount) AS n但未选择该聚合；select表达式facts.n被_select_exprs/_expand_aliases改为SUM，静默变成15。这是Compiler改错含义，不能靠Prompt掩盖，也不同于REQ-052已经删除的CTE结构规范化阶段猜测。
- 同一字符串替换还跨越引号、字符串/注释、函数/类型标识、子查询作用域，并可再次改写已经插入的聚合表达式。复用仓库已有SQLGlot，以AST中的本层未限定Column定位原字符串跨度，只替换实际本地别名引用；保留其他文本及显式限定，不序列化整个表达式。
- 同一路径核对HAVING/QUALIFY和窗口/排序的别名使用；已独立复现FIRST_VALUE聚合实参与隐藏聚合排序表达式的漏展开，复用相同接口规则修复，不增加新DSL。

### 边界、风险与替代方案

- 不改Prompt/Schema/Gold/评分/Provider预算，不恢复已失败分母范例，不给错误候选补列/改别名，不自动加入第二次生成。原候选与历史结果保留。
- 不把不合法的限定引用去前缀“救活”；词法和作用域错误可能改为更早拒绝。当前声明的一层别名展开不进入原始SQL子查询，也不递归修改插入SQL。
- regex补几个边界仍会误改SQL语义；全表达式AST重序列化会扩大格式/方言变化。使用AST定位+最小文本替换，解析失败关闭，不降级回正则猜测。
- 先冻结当前独立SQLite边界与551份已有查询的2755次五方言编译；修复后逐项复比、人工核查变化项，并对近期16候选做独立诊断重评。已知Gold阻塞题不重复执行，不借Compiler维护宣称模型增益。
- 证据目录：`.forge/benchmarks/cte-interface-diagnostic-20260907/`；42组边界的独立预期已核对（含预期拒绝），修复前16通过/26失败；历史固定查询无改写。生产入口和所有受影响调用点随后验证，只有复核后才封存维护结论。

### 已验证结果（2026-09-07）

- 已完成确定性维护。12项可观察SQLite回归先红后绿；独立42组边界从16通过/26失败变为42通过，所有原始输入保留。额外确认SQLGlot 26.0.0缺少标识符跨度，27.0.0完整42组通过；依赖下限改为27，上限仍<30。
- 551份固定查询×5方言的2755次编译，仅合成限定列反例在5方言变化；500份Sol、8份上一轮候选和40份近期Luna的2740项SQL/错误完全不变。22个未保存Forge输出单列，不补造。没有重新执行R500 Gold或重试md-340/md-393。
- 新协议下16份原候选零调用诊断重放，输出、EX、Contract与失败分类逐项不变；REQ-056处理组md-079仍unknown_schema_reference。旧冻结因Compiler/依赖来源变化拒绝，新记录明确diagnostic，不覆盖原Run、旧协议或分母实验结论。
- 真实forge evaluate CLI经本机main:app正例allow_review，返回SQL在合成SQLite中得到700/800；未导出CTE字段负例deny。两者execution_authorized=false。隔离API已停、18778无监听，测试创建的运行时DB已清理。
- Python901 passed/26 skipped，Pi155 passed/typecheck通过；文档同步后Compiler与文档相关107 passed。现行语义/README/课程及网站镜像已更新，历史Devlog不改写。默认Prompt和原控制组上下文hash、数据hash不变，日期/粒度/取值off，父Run仍seed48。
- 正式证据：`benchmark-cte-expression-binding-fix-2026-09-07.json`。此次没有修复模型声明/引用不一致，也不宣称BIRD准确率提升；下一步若做生成端CTE接口干预，应另立窄实验与调用授权，不能复用已耗尽的16次预算。

## REQ-2026-09-07-058：生成端CTE输出接口范例的隔离实验

- 状态：零调用准备与独立授权后的16次开发对照均已封存；预声明门槛失败，默认不启用。本轮预算已用尽，不补跑、替换或扩样。

### 原始表达与承接

> 好的，继续吧

承接REQ-057：Compiler确定性绑定已修复，md-079声明/引用不一致与seed48 md-199未导出best_ms仍是原模型候选错误。不得由Compiler猜别名、补列或将拒绝率代替最终准确性。

### 单因素、价值与边界

- 只增加独立可选的CTE接口合成范例revision，默认紧凑Prompt逐字不变；不叠加分母范例或已失败的投影提醒，不更改Schema/Context/Compiler/Assurance/Gold/评分或Pi单次派发。
- 一张虚构readings表演示分组计数、排名CTE与外层筛选：本层agg别名用于窗口排序，agg/window结果经显式select导出，下游只引用导出名；最终只返回题目要求的频道及记录数，不输出辅助排名。不强制简单查询使用CTE，不向Prompt注入题号/真实字段/Gold/夹具答案。
- 目的：观察具体结构范例能否减少声明→导出→引用断裂。REQ-044已证明笼统投影提醒没有Forge增益；本次不是重复该处理，也不预设范例一定有效。REQ-056已经出现范例诱发回退，故默认不采纳并保留简单查询与复杂正确对照。
- 通过现有forge_prompt_revision/prompt单因素注册和冻结；不增加新配置平台、Task状态、生成步骤或返修通道。无服务端自动补救，也不按Gold择优。

### 固定实验提案与机会成本

- 四题固定为md-079（REQ-056变体引发的不一致，当前默认不一定失败）、md-199（seed48 CTE漏导出）、md-312（简单查询正确控制）、md-386（历史复杂正确控制）。全部为已曝光D探针，不是H。现有候选只作来源与当前栈诊断，不能代替本轮新控制。
- 冻结提案：同一Luna/model/runtime下4题×2条件×2臂=最多16次；冻结时授权/实际均0，随后独立授权与执行见末尾结果。控制后处理，单回合、case concurrency=1、120秒、零Provider重试/替换；任何运行失败停止，不补跑、不改变样本、不启动seed49。
- 预声明开发门槛：处理组两个Forge目标均须EX/Contract通过，且至少一个目标相对本次控制从两指标失败变为均通过；控制中任何正确的EX/Contract不得回退（含Direct）；16臂评分/用量完整。处理总tokens≤控制1.20倍、Forge每EX正确答案tokens不增、Forge生成P95≤1.25倍；分母为0或指标未知视为不能通过。Direct输入不变，波动仍计入完整报告。
- 所有成本/稳定性门槛与样本在新生成前固定；通过也只提供进一步确认的开发信号，不自动启用、不更新500题成绩或绕过H/R0.6。相对扩大模型/多轮返修，这个候选便于隔离但仍有提示词过拟合和上下文增量成本。

### 准备验收

- 运行真实范例、同构反例和独立SQLite预期；保护并列、空输入、作用域、重命名导出、合法隐藏辅助量及简单无CTE查询。
- 500题默认输入hash不变；双条件CLI冻结/Gold预检、HTTP与Pi生产消费/漂移门禁通过且无Provider调用。封存原候选、协议、来源、实际指令和预算提案，生成另获授权。

### 零调用准备已完成（2026-09-07）

- 只在agent/prompts.py注册独立的forge-structured-benchmark-cte-interface-v1；默认500题Forge/Direct/Schema/Context四类hash逐项不变，候选只增加Forge指令2124 UTF-8 bytes。原分母变体四题指令也未变；其不采纳结论与旧证据保留。
- 从实际Prompt解析DDL/JSON，17种结构×6个夹具共102项（60合法/42预期错误）全部符合独立Python与SQL预期，另2项DDL约束验证通过；父执行器完整重放一致。覆盖并列/空输入、漏导出、别名不一致、重命名、限定源列、合法隐藏辅助量及不使用CTE的对照；反例不进入模型输入。
- 四题两条件CLI冻结/Gold预检通过，归一化后仅Prompt变量不同。8份原Sol候选在两条件16次诊断中输出/SQL/EX/Contract/失败分类不变，均通过，仅是固定候选管线证明，不是新Luna成绩。
- 实际鉴权HTTP及生产Pi Runtime完成两条件消费、全部实际指令hash/原候选核对、新进程持久化重开与双层漂移派发前阻断。最终回执16次合成派发；测试回执修正导致另两轮重跑，合计48次合成派发，真实模型调用始终0。更正仅涉及测试网络回执持久化与并发会话hash归属，不改生产代码；最终外部网络尝试0。
- Python901 passed/26 skipped、Pi155 passed/typecheck通过。服务18779已停，无监听，临时状态已删除；无临时可执行脚本或新永久测试，复现源码保留在证据JSON。
- 准备封存时状态：ready_for_separate_generation_authorization，当时16次仅为独立新预算提案，尚未授权/派发；此历史准备报告保留，不以合成验证宣称准确率收益。精确卡片与协议在`.forge/benchmarks/cte-interface-ready-20260907/`，准备报告`benchmark-cte-interface-ready-2026-09-07.json`。

- 生成授权：用户在完整冻结提案后回复“继续”，独立授权最多16次（每条件8次）；记录于`.forge/benchmarks/cte-interface-ready-20260907/generation-authorization.json`。原准备报告和experiment-card保留为授权前证据，任一运行失败即停止，不追加调用。

### 新授权16次开发对照完成（2026-09-07）

- 独立授权16次，实际16次、两个原生Pi Run均完成；控制8次后处理8次，零Provider重试/替换，16候选全评分、未知用量0。run_id为pbr_5ac3895ee65b4d3cb16f5a16b4e281ee与pbr_d706f8085689482a86768623598443cb，原协议/响应/日志保留。
- Forge EX/Contract均3/4→3/4：md-199从执行超时变为完整正确四列；md-079从正确百分比变为962、1023两项计数，发生回退。Direct均3/4→2/4：md-079漏乘100；md-199两组均缺毫秒列。md-312/386两臂均保持正确。Direct输入逐字一致，其波动不能归因于Forge范例，但按预声明仍计入稳定性失败。
- 总67367 tokens：控制32300、处理35067（+8.57%，通过≤20%）；Forge每EX正确答案6792.67→7683 tokens（+13.11%，失败）；Forge生成P95为14.3516→20.312秒（+41.53%，超过≤25%）。P95按各组4次最近秩取最大值，不代表生产P95。两个目标全对、零回退及上述两项护栏失败，不采纳默认。
- 16原候选离线CLI重放、独立只读SQLite的EX集合/Contract多重集核对一致；控制md-199 Forge在独立30秒限制下仍超时，不改原失败成绩。16原响应hash、215日志和冻结输入逐项匹配，19份准备工件、原准备报告及源码/数据/输入hash不变。
- 首次创建请求因临时启动配置漏配独立Pi服务密钥而返回500；后端门禁401、账本为空、派发0。只修正本地配置，双协议门禁200后才创建首个Run；未重放已创建运行或模型调用。此启动拒绝单独封存，不隐去、不计作额外模型尝试。
- 预算已用尽，余量0；不补跑、不扩样、不自动返修或启用范例，各可选上下文仍off，父Run仍seed48。本阶段未改生产源码/Contract，不新增永久测试；正式报告benchmark-luna-cte-interface-2026-09-07.json，封存与服务清理回执位于原实验目录的generation-*工件。

## REQ-2026-09-07-059：仓库版本推送、逐轮报告公开与README校正

### 原始表达

> 好的，我们应该可以 push 个新的版本了，包括我们的每次测试的报告，并更新 readme

### 价值、边界与决策

- 用户明确授权本次commit/push。价值是把累计源码、跨Python/Pi Contract维护、全部可定位实验报告和公开说明交付为同一份可审查仓库版本，不再让README领先于证据。
- 采用origin/main普通提交与推送；保留包版本0.1.0和既有v0.1.0-beta tag，不额外创建tag、GitHub Release、发布包或手工部署。远端提交与CI结果以Git/GitHub记录为准。
- 逐一索引55份Accuracy/Benchmark文件与16份早期测试/验收报告；早期缺失运行仅从文档重建并标注unknown，不读取运行数据库补造。原字节留在被忽略的本地归档；20份公开副本做最小路径/联系字段处理，逐项保留分数、分母和原件/公开版hash映射。
- 替代方案是只推源码和最新好结果，成本更低但会遗漏失败、未知和未完成记录；不采用。复制整个运行目录虽便于维护者复查，却会暴露凭证、会话和数据库，也不采用。
- 机会成本与风险：完整报告增加仓库体积；本轮集中做公开交付和回归门禁，不新增付费模型实验、提示词默认采纳、业务功能或平台扩张。数据来源、许可和公开边界集中记录，不把模式扫描称为独立安全认证。

### 发布阻断维护与验证证据

- 版本审阅发现既有全局CTE名称过滤可能把同名未授权物理表当作CTE跳过。已用按scope来源和方言归一化的物理关系校验修复；扁平Registry不授权schema/catalog限定来源，合法CTE/derived不受误伤。
- Assurance升级query-assurance-v10；旧v9 QueryRun审批必须以assurance_revision_drift失败关闭，重新prepare/审核，不迁移旧证据。此项是既有安全行为维护，不是模型成绩提升。
- Python/Pi全量、定向回归、跨方言静态校验与真实合成SQLite/API冒烟、Quickstart、站点构建及文档链接的实测结果与跳过范围见[发布验证报告](release-verification-2026-09-07.json)。本轮被测模型调用0。
- 全部报告入口见[文档导航](README.md)，原件/公开副本映射见[报告公开说明](report-publication-2026-09-07.json)。产品需求与H/R0.6门禁不因仓库推送自动晋级。

### 远端CI阻断与测试隔离修复

- 首个提交609386b已push；CI运行34068683259的Python与数据库兼容性通过，Pi为150通过/5失败。根因是四项Skill/runtime测试依赖旁边的拾穗DATA目录，health测试因此返回503，不归因于Node版本。
- 改用临时Skill包和真实Pi加载器验证白名单与单Skill隔离；去掉生产Skill文案及固定20数量断言，保留缺失包失败关闭，不改生产源码、跳过用例或放宽readiness。
- 加强沙箱后另发现六项HTTP测试会写默认.runtime/state，已改用每测试独立配置。初轮本地门禁未覆盖此目录，不能声称初轮完全没有运行目录访问；未读取或清理该既有目录。
- 禁止访问旁仓、真实.runtime及凭证，并禁止对外联网后，定向20通过、完整Pi155通过、typecheck通过；本轮模型调用仍0。首次失败、隔离更正和本地结果追加在发布验证报告中，后续远端结论以对应提交的CI为准。
