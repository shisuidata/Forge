# Forge 需求池

> 状态：主动需求真相源 · Last updated: 2026-08-24
>
> 本文件记录所有产品、体验、架构和业务需求，包括未采纳、延期、拒绝和被替代的需求。需求池不等于实施计划；只有经过澄清、评估并由用户确认的需求，才能进入 [`forge-enterprise-evolution-plan.md`](forge-enterprise-evolution-plan.md)。

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
- **当前状态**：`accepted`
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
- 若“带引号的 SQL”指参数化/带引用 SQL，则必须把 literal 与 SQL 分离保存，并引用 semantic/metric/binding ID；quoted identifier 只能解决方言和保留字，不能解决 Schema 漂移。该术语需用户进一步确认后再固化命名。

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
