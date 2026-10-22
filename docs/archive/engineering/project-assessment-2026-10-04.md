# Forge 全面项目评估报告

> 评估日期：2026-10-04。状态：评估结论与建议，不是新的实施计划或需求授权。
>
> 评估对象：本地 `main` 工作区的当前实现，包括既有未提交修改；不能将本报告当作公开 Release、远端 CI 或 NAS 当前运行实例的认证。开始时工作区有 103 个 unstaged、22 个 untracked 项，均保留。

## 一、结论先行

**Forge 值得继续投入，但应收敛到可信数据执行核心；当前是核心链路已跑通、尚未完成安全与外部采用验收的工程验证阶段，不是生产就绪的企业平台。**

最有价值的资产不是 Forge JSON，而是：Registry、统一 QueryRun、生成后 Assurance、身份与授权绑定、审批与执行一致性、Evidence，以及真实失败样本与可复核评测体系。`Evaluate → Enforce → Explain` 比“更聪明的问数工具”更符合现有证据，也更有机会成为既有 Agent 的独立组件。

本轮同时确认了两项发布阻断：

1. **Direct SQL 的检查与实际执行文本不一致。** 特定输入能让检查漏掉后续语句及资源引用；空引用集合又可进入自主执行授权。已复现授权漏检，未证明真实 PostgreSQL/MySQL 数据泄露。
2. **Pi 事件去重缺少主体作用域。** 不同合法主体提交相同事件 ID，可取得先前主体的任务投影。已在隔离合成环境复现跨主体返回。

因此，**不建议将当前实现用于真实生产或共享多租户数据访问**。本地合成数据演示与受控开发验证可以继续，但应显式保留上述限制。

全套测试通过与上述结论并不冲突：测试证明已覆盖行为的稳定性，未覆盖的对抗性输入、跨身份幂等和故障窗口仍可能破坏核心不变量。

### 成熟度判断

| 维度 | 当前判断 | 主要依据 |
|---|---|---|
| 产品定位 | 清晰，尚未完成用户价值验证 | Trust Runtime 边界明确；外部独立采用门禁未过 |
| 核心执行能力 | 可运行，有发布阻断 | Evaluate/Enforce/Explain 与 v2 有界授权链路跑通；存在 SQL 授权漏检 |
| 架构职责 | 正确方向，局部恢复协议未闭合 | Pi 持有 Task，Forge 持有 QueryRun；跨服务终态和报告恢复存在缺口 |
| 工程质量 | 自动化基础较强，安全反例覆盖不足 | Python/Pi 全套通过，wheel 可独立安装；合成反例仍成功触发缺陷 |
| 产品体验 | 新工作台可用，入口与部署环境不一致 | 本地 CSS 工作台正常；旧 Dashboard 在外网不可用时严重失真 |
| 企业生产能力 | 尚未验证 | 缺真实身份/权限迁移、独立安全审核、HA/负载及恢复证据 |
| 开源采用与商业化 | 未验证 | 无可确认的外部 Golden Path、持续使用或付费转化证据 |

不提供综合百分制评分：不同维度的证据等级不一致，给出精确分数会掩盖发布阻断。

## 二、评估范围与证据口径

### 本轮实际做了什么

- 阅读当前状态、北极星、主动计划与职责边界；检查 Python Runtime、Web/BFF、Pi 编排/持久化、测试、打包与 CI 配置。
- 在隔离状态路径、合成身份与合成 SQLite 环境运行测试、CLI 和真实本地 HTTP 服务。
- 运行公开 Quickstart 的人工审核、自主授权、指定审核人与授权撤销路径。
- 构建 wheel，并在独立虚拟环境和非源码工作目录完成安装后烟测。
- 打开真实浏览器检查旧 Dashboard 与 `/workspace` 桌面/手机表现，执行工作台 WCAG A/AA 自动检查。
- 用隔离合成反例检查 SQL 校验、跨主体事件去重、凭证撤销、注销和部分故障恢复。

### 本轮未做什么

未读取生产 Secret、未连接真实客户数据、未调用付费模型、未修改运行代码或生产配置、未提交/推送/发布/部署。未实测真实 PostgreSQL/MySQL 权限和副作用，未验证 NAS 当前状态或远端 CI，未重跑历史 BIRD 生成实验，未进行负载、HA 或独立渗透测试。

证据标记：

- **运行证据**：本轮实际执行或合成反例观察。
- **源码证据**：当前代码可定位的行为；不自动等于真实生产事故。
- **历史证据**：仓库既有报告与状态投影；不是本轮复测。
- **[INFERENCE]**：由实现推导的风险，尚未完成对应运行反例。

## 三、产品价值与商业判断

### 3.1 近期切入成立，长期平台仍是假设

第一用户应是已有 Data Agent、需要控制数据执行风险的 Data/AI Engineer 和数据平台团队。Forge 应回答四个问题：

1. 这个候选能否被允许执行？
2. 它采用了哪些数据、语义、权限和版本？
3. 审批是否绑定实际执行内容，撤销和漂移是否有效？
4. 错误发生后能否解释、复核和追责？

这比要求用户迁移到一套新的聊天产品更契合当前模块边界。Pi 工作台可以作为参考消费者，但不应成为独立 Runtime 被采用的必要条件。

**差异化机会**是数据访问专用的、可落地到执行路径的治理与 Evidence，而不是页面数量、DSL 自身或“100% 正确”。机会尚未被市场证明：用户是否愿意承担 Registry 维护、审批和集成成本，仍需独立采用证据。

### 3.2 不应以 Forge JSON 准确率优势作为主卖点

历史 Structured GPT-5.6 的同候选 Compiler 重评结果为：

| 指标 | Forge | Direct SQL |
|---|---:|---:|
| 500 题 Exact Result | 313/500，62.60% | 314/500，62.80% |
| 仅本臂正确 | 22 | 23 |
| 配对 exact p | 两臂比较 p=1.0 | — |

对应历史生成成本中，Forge tokens 多 59.57%，平均生成时间高 51.69%。Compiler 重评不是新生成，也不能把新实现正确性归因到所有模型和数据域。

结论：该历史证据支持统计持平，不支持总体准确率优势；同时不支持成本优势。小样本恢复、失败归因和确定性修复有价值，但不能替代独立泛化或商业收益。

**保留 Forge JSON 作为可选 Planner Adapter；产品主张放在生成后的治理、执行与解释。**

### 3.3 商业验证缺口

当前状态明确：R0.6 外部独立采用与 H 独立审核未通过。维护者 Quickstart、内部测试、stars/forks 与维护者招募 Issue 都不是外部采用。

下一步应获取实际消费者的证据，而不是预估收入：

- 独立安装与首个受控查询能否完成，失败在哪里；
- 接入既有 Agent 的代码/运维成本，是否需要采用 Pi；
- 对合法查询的误拒率、审批负担、延迟和每个被接受结果的成本；
- 是否持续使用，是否愿意提供真实失败样本；
- 是否愿意为部署支持、治理运维或明确服务承诺付费。

付费 Pilot 应晚于安全修复与真实只读权限验证。当前没有依据承诺市场规模、客户转化率或收入。

## 四、架构评价

### 4.1 应保留的设计

- **控制与执行分离**：Pi 是唯一 Task 真相源；Forge 是 QueryRun 与受控数据库执行的真相源。DATA Skills 不获得裸数据库执行权，Web/渠道只投影。
- **输入可替换**：Direct SQL 与 Forge JSON 进入统一 QueryRun；没有把生成协议绑成产品身份。
- **授权绑定实际动作**：创建凭证、SQL/Assurance/Enforcement hashes、Registry/Policy revisions、Task、Datasource 和 Mandate 都参与执行门禁。
- **失败与未知可见**：Explain 暴露截断、语义未证明、实时数据无快照等限制；历史未锚定证据不伪装成完整可信。
- **有界授权**：单任务/单数据源/表范围与自主子集、有效期、指定真人审核、撤销和不可再委托，比超级 API Key 更适合 Agent。
- **可独立运行的核心**：安装后烟测验证核心不依赖 Web 或旧 Agent 才能导入，Quickstart 不需要 LLM 或 Pi。

这是项目最值得保留的工程方向，不需要另建 Orchestrator、任务表或执行旁路来解决现有缺陷。

### 4.2 必须区分的边界

| 当前能力 | 不能据此宣称 |
|---|---|
| 表级 ACL | 同表内行级隔离、租户 RLS 或完备列级策略 |
| Direct SQL/JSON 共用 QueryRun | 两种输入具有相同的业务语义校验强度 |
| 查询结果与 Evidence hash 绑定 | 具备独立签名、可信时间或可抵抗同库管理员篡改 |
| 保存历史查询结果 | 实时源数据已有不可变快照，查询可以在未来原样复算 |
| Registry revision 漂移检查 | 已验证真实数据库 Schema、Role 和会话权限漂移 |
| 行数上限和超时 | 完整控制 CPU、内存、结果字节与租户公平性 |
| Contract 定义完整 | 所有 Action 都有 Runtime Enforcement |

历史 Action Catalog 的运行时覆盖投影是 3/14；新 v2 查询治理切片不应被自动外推为所有 Action 已实现。指定真人审核也不自动构成严格独立的四眼审核：当前允许签发人与指定审核人为同一人，必须与 H 独立审核门禁分开。

### 4.3 维护性与容量

静态量测中，`forge/compiler.py` 约 1763 行；`forge/cli.py` 994 行、`forge/enforce.py` 867 行、`forge/query_runs.py` 763 行。Pi `application.ts` 约 2275 行、`sqlite-store.ts` 1086 行、`server.ts` 1022 行。

文件长度本身不是缺陷。真正风险是多个 Stage 各自处理取消、超时、恢复和发布，已经出现行为不一致。应先明确并验证状态转换与副作用恢复不变量，再按职责拆分；不建议先做大规模重构。

SQLite/WAL 与 `BEGIN IMMEDIATE` 对单实例工程验证是保守选择，不必现在换成分布式系统。但本轮没有并发吞吐、锁等待、队列积压、备份恢复或高可用实测。**[INFERENCE]** 全量事件/Artifact 读取与缺少分页会随任务增长放大延迟和内存；应由真实负载证明，再调整索引与分页。

Pi 默认 Skills 依赖相邻外部 DATA 目录，模型 revision 不等于 Skill 文本版本。跨环境任务复现需要同时交付和标识 Skill 资产，而不能只记录模型。

## 五、安全与授权：发布阻断及重要缺陷

本节反例均使用合成数据或纯内存状态，没有访问真实受限数据。

### F1 · P0：Direct SQL 校验漏检，检查对象与执行对象不一致

**源码链路**：[`forge/executor.py`](../../../forge/executor.py):86–92 先用正则去注释再去字符串；[`forge/assurance.py`](../../../forge/assurance.py):224–237 用 `parse_one` 检查首条语句，272–282 保留原始 SQL；[`forge/enforce.py`](../../../forge/enforce.py):415–423 与 [`forge/query_runs.py`](../../../forge/query_runs.py):628–637 可将空引用集合判为自主授权。

合成反例：

```sql
SELECT '/*'; SELECT n FROM private_data; SELECT '*/' AS x
```

在 ACL 仅允许 `numbers`、`autonomous_tables=[]` 时，prepare → claim 观察到 `referenced_tables=[]`、自主授权且 claim 成功，原始多语句文本仍保留。独立检查另确认以下输入也通过当前只读/Assurance 检查：

```sql
SELECT '/*'; DELETE FROM numbers; SELECT '*/' AS x
SELECT n INTO created_table FROM numbers
SELECT nextval('synthetic_sequence')
```

**影响边界**：已确认授权检查漏检，并非已证明生产写入或泄露。真实 SQLite driver 对多语句执行返回 `ProgrammingError`/`execution_failed`；未实测 PostgreSQL/MySQL 的这些副作用。驱动拒绝不能替代 Runtime 的 ACL 与只读不变量；只读数据库角色也不能替代表级授权。

**修复验收要求**：检查完整 AST，明确只接受单条支持的无副作用查询；作用域资源提取与执行同一已校验文本绑定；覆盖字符串/注释、嵌套查询、CTE、`SELECT INTO` 和有副作用函数；对支持数据库验证真实只读会话与 Role。不能仅补一个正则或依赖配置中的 `readonly_confirmed` 标志。

### F2 · P0：Pi 幂等事件键没有身份作用域

**源码链路**：[`sqlite-store.ts`](../../../services/pi-orchestrator/src/sqlite-store.ts):526–528、574–580、1016–1025 的去重主键仅包含 `channel,event_id`；[`application.ts`](../../../services/pi-orchestrator/src/application.ts):579–590 在 duplicate 分支读取旧 Task/Presentation，未重新检查当前主体及 payload；[`server.ts`](../../../services/pi-orchestrator/src/server.ts):443–446 验证当前 actor，但没有在无显式 task ID 的去重返回路径校验旧 owner；[`web/routes/pi.py`](../../../web/routes/pi.py):140–156 将客户端 message ID 转成 event ID。

**运行反例**：合成 `org_a/user_a` 先提交 Web 事件；合法 `org_b/user_b` 再提交相同 event ID，观察到 `duplicate=true`、相同 Task，返回的组织是 `org_a`。

**影响边界**：需要获知、复用或碰撞事件 ID，不是已经证明可以任意枚举所有任务；但跨主体投影返回本身已破坏隔离。

**修复验收要求**：幂等键按受信租户/主体命名空间定义，duplicate 返回前重新授权；绑定请求 fingerprint，拒绝相同 key 的不同输入；消息和任务 action 分别定义幂等语义。仅把事件 ID 随机化不能修复授权边界。

### F3 · P1：凭证撤销与 Mandate 管理之间存在竞争窗口

[`forge/governance_mandates.py`](../../../forge/governance_mandates.py):47–54 的 `_actor` 重查身份但未重查 credential；签发/撤销事务见 109–111、208–210。

合成流程“认证 admin → 撤销该 key → 使用原 `AuthenticatedPrincipal` 签发”仍产生 active Mandate。新 HTTP 请求携带已撤销 key 会被拒绝，本问题限定为认证至事务提交之间的 TOCTOU 窗口，不是所有撤销失效。

**验收要求**：在授权变更的线性化事务内重查当前 credential 与主体状态；撤销必须约束该提交窗口。

### F4 · P1：注销只清 Cookie，没有撤销服务端 Session

[`web/router.py`](../../../web/router.py):165–169 的 logout 返回 303 并清 Cookie。运行反例确认旧 Session token 在注销后仍可认证，直到约一小时 TTL。

**验收要求**：注销时撤销服务端 Session，并证明旧 token 无法继续使用。清浏览器 Cookie 不等于使已复制 token 失效。

### 已验证的安全基础

当前实现具备注册凭证解析、无效 API Key 不退回 Cookie、治理入口不因关闭一般 Web 认证而自动匿名开放、同源/CSRF 检查、HttpOnly/SameSite，以及 Mandate 期限/资源子集/身份约束。QueryRun claim 事务绑定凭证、审批 hashes、授权/ACL/revisions 后再提交 worker；过期 lease 失败而不盲重放。这些设计应保留，但不能抵消 F1/F2。

## 六、编排可靠性与恢复

### R1 · P1：报告发布不确定时重新生成，破坏原发布幂等

[`application.ts`](../../../services/pi-orchestrator/src/application.ts):1824–1935 已持久化 bundle 后执行 publish，1981–1983 对未知结果/超时回到 `ready_for_report`；新 command 在 1755–1799 重新生成，随机 Artifact IDs 改变 bundle hash，但 task-derived report ID 和 revision 1 不变。[`forge/reporting.py`](../../../forge/reporting.py):297–302 拒绝同版本的不同 bundle。

合成模型返回相同正文、并复刻远端 hash 规则的反例观察到两次模型生成、两次 publish、不同 bundle hash，重试失败。该证据不是完整真实远端 HTTP 丢响应测试。

**验收要求**：恢复原已持久化 bundle；先查询远端发布状态，确认成功则收敛，确认未成功才用相同身份/内容重送；未知副作用不得通过重新生成掩盖。另测进程退出、超时和响应丢失。

### R2 · P1：重启恢复忽略 QueryRun 失败/取消终态

`application.ts`:1364–1389 的恢复分支处理远端 executing/completed，忽略 failed/cancelled。合成反例中，本地 `waiting_for_query_approval` 已回读远端 failed，仍停留在原状态。

**验收要求**：明确 Forge QueryRun 每个终态如何映射到 Pi Task/StageAttempt，恢复后消除悬挂待办，保留失败证据，不重新执行。

### R3 · P1：prepare 阶段取消语义与其他 Stage 不一致

`application.ts`:1181–1185 把调用方 abort 转成 failed 终态；合成反例确认 Task/Attempt 均为 failed，而不是其他 Stage 使用的 interrupted。

**验收要求**：区分用户取消、依赖失败和执行不确定性；采用一致的恢复与重试协议，不仅修改显示文案。

### R4 · P2：[INFERENCE] 执行 lease 与审批到期可能留下滞留状态

[`main.py`](../../../main.py):54–63 在 startup reconcile；`query_runs.py`:513–526 跳过尚未到期 lease，240–249 的回读不再次 reconcile。如果 worker 失去后在 lease 到期前重启，之后可能一直保留 executing，直到再次重启或显式 reconcile。本轮没有完整复现该时间窗口。

审批 TTL 到期主要抛错，没有同步持久化 expired/inbox 过滤状态，也有造成无效待办的风险。应通过进程故障和可控时钟场景确认，不将推断写成已发生事故。

## 七、工程质量与交付

### 7.1 本轮运行结果

| 验证 | 观察结果 | 范围限制 |
|---|---|---|
| Python 全套 | 1011 passed / 26 skipped；18.55 秒；2 条第三方 deprecation warnings | 跳过项不等于通过；不覆盖真实生产环境 |
| Pi TypeScript | `npm --prefix services/pi-orchestrator run typecheck` 通过 | 类型检查不是授权证明 |
| Pi 测试 | 212 passed / 0 failed；约 4.34 秒 | 合成故障反例仍发现未覆盖路径 |
| 安全聚焦测试 | 子评估 96 passed | 不能替代 F1/F2 对抗性反例 |
| 人工审核 Quickstart | `--yes --json` 完成真实本地 HTTP Evaluate → Enforce → Explain | 自动确认仅用于合成环境，未验证真实人的审核判断 |
| Agent Quickstart | `--agent --yes --json` 自主/指定 reviewer 两链 completed；撤销后 prepare 返回 409 `mandate_inactive` | 无模型/Pi，不代表全部 Agent 产品链路 |
| wheel 构建与安装 | `uv build --wheel` 成功，独立 venv 安装并运行 installed package smoke | 本轮未构建/烟测 sdist、未运行远端 CI |
| 安装后功能 | compile、PoC init/validate、包资源与 Quickstart 通过，Explain verified | 不代表真实数据源接入 |

本机 `.venv` 没有 `build/setuptools/pip/wheel`，`python -m build` 不可用；改用 `uv build` 得到发行验证。不应把本机缺失工具误判成产品构建失败。

Quickstart 还观察到写入被 `readonly_violation` 拒绝、结果行数截断，以及 Explain 显式暴露 `semantic_correctness_unproven`、`live_data_no_snapshot`、`authorization_is_not_semantic_correctness`、`result_truncated`。这些诚实限制比泛化“可信结果”标签更重要。

本轮临时日志位于 `/tmp/forge-assessment-python.log` 和 `/tmp/forge-assessment-pi.log`；仅是本机临时证据，不是仓库永久附件或公开可下载工件。

### 7.2 自动化的价值与缺口

CI 配置已有 Python 3.11、安装包 smoke、Pi/Node 22、PostgreSQL 16/MySQL 8 兼容性任务，是较好的工程基础。这里是配置审查，不冒充本轮远端或实库测试通过。

接下来最值得新增的是可击穿消费方不变量的回归：完整 SQL effect/ACL、跨主体幂等、撤销并发、注销失效、远端发布丢响应、终态恢复。不是继续增加默认值、字段拷贝或源码文字断言。

## 八、产品体验与文档

### 8.1 新工作台比旧入口更稳，但尚未完成真实业务验收

真实浏览器下 `/workspace` 使用本地 CSS，桌面显示正常。390px 手机宽度稳定后 `scrollWidth=390`，主内容无横向溢出；Pi 断开时明确展示 partial/依赖不可用，而不是伪造任务完成。

本轮没有运行真实模型或任务审批交互，因此不能评价完整自然语言问数体验。默认 `/chat` 在 Pi 关闭时输入禁用，是正确的依赖状态，不是问数链路已完成验证。

axe-core 4.13 的工作台 WCAG A/AA 自动检查发现 1 类规则、2 个低对比节点：`kbd` 约 3.06:1，`.status` 约 4.34:1，所需 4.5:1。其余 19 项通过不能替代键盘、屏幕阅读器和全部页面检查。

### 8.2 离线/受限网络部署有明确 UI 缺陷

[`web/templates/base.html`](../../../web/templates/base.html):7–8 与 [`login.html`](../../../web/templates/login.html):7 依赖外部 Tailwind/marked CDN。仅允许本地请求的真实浏览器中，旧 Dashboard 虽返回两个 completed QueryRun 与 verified Evidence，但出现 1424px SVG 和约 26139px 页面高度，布局严重失真。

这是**外部 CDN 不可达时**的已复现缺陷，不是声称常规联网环境必坏。企业内网、离线 Quickstart 与部署入口应该使用随包本地资源；登录页面也不能依赖外网才能正确呈现。

`dashboard.html`:44 仍显示已退休的 `/api/v1/enforce/query-runs`，会误导当前 v2 使用者。

### 8.3 文档资产丰富，但当前投影过重且入口漂移

状态文档约 46KB、主动计划约 188KB、需求池约 346KB。历史记录是核心资产，不建议删除；问题在于“当前状态”混入过多历史，使当前门禁与最新 API 难以快速定位。

README 仍有旧 LLM 中心/raw SQL fallback 图示；Governance Contract 文档仍保留 M0 未运行表述。生产部署文档已注明旧快速部署不适用于 v2，但后文仍有旧 `AUTH_API_KEYS` 操作流程。不同角色文档应标明历史/当前，不应让用户自行拼装有效入口。

建议保持原历史材料字节与结论，缩短当前投影；统一当前 v2、凭证初始化、授权、审批、Explain、失败与安装流程。报告归档不自动授权修改主计划。

## 九、建议优先级与验收门槛

以下是评估建议，不表示用户已接受新需求，也不自动进入主动计划。

| 顺序 | 事项 | 最小验收证据 |
|---|---|---|
| P0 | F1 完整 SQL/资源/副作用验证 | 反例先失败后通过；支持方言边界；真实只读 Role/会话；未授权表在 prepare 和 claim 均失败关闭 |
| P0 | F2 跨主体幂等隔离 | 合法不同租户/用户相同 event ID 不返回旧 Task；same-key changed-payload 拒绝；同主体同输入仍只生成一个 Task |
| P1 | F3/F4 撤销与 Session | 事务提交前撤销生效；logout 后旧 token 无法认证；正常新请求行为保持 |
| P1 | R1/R2/R3 副作用与状态恢复 | 真实本地 HTTP 丢响应/进程退出；不重复生成/发布；所有远端终态收敛；取消可解释且可安全恢复 |
| P1 | 当前 v2 入口与离线 UI | 非源码安装环境、禁外网仍完整呈现；文档命令可运行；Dashboard 不展示退休执行入口 |
| P1 | 独立外部 Golden Path | 非维护者独立完成；记录安装/接入耗时、阻断、合法误拒与 Evidence 消费；收集真实 failure case |
| P2 | R4、容量与运行手册 | 可控 lease/TTL 故障；备份恢复；负载与资源上限测量；由证据决定存储/队列改造 |
| 后续 | 付费 Pilot 与更广平台 | 明确消费者持续使用、可接受成本、治理价值与付费责任边界 |

**推荐顺序：安全不变量 → 故障恢复 → 可独立交付与采用 → 商业验证。**

暂不建议优先增加 Connector、通用 Agent 平台、第二套主状态或继续为 Forge JSON 追整体分数。机会成本是现有真实安全和采用门禁被推迟；这些扩展也没有第二消费者证据支持。

## 十、最终判断

Forge 不是只有界面和概念的样品：本轮证明了核心 HTTP 链路、受控授权撤销、Explain 和独立 wheel 的实际可运行性；版本化 Contract、审批绑定、失败证据和历史负结果积累也具备复用价值。

但“可运行”距离“可信且值得企业采用”还有三道独立门：

1. SQL 与主体授权不变量真正关闭，不能依靠驱动偶然拒绝或事件 ID 难猜；
2. 跨服务副作用与进程故障后状态能够安全收敛；
3. 外部用户证明这套治理的收益大于接入、审核和维护成本。

**继续做，但应把资源从横向扩张收回到可信执行与独立采用。先修复会破坏产品承诺的缺陷，再用真实消费者证明产品；不要用新增功能、测试数量或历史小样本成绩代替这两件事。**

### 事实入口

- [当前项目状态](../../current-project-state.md)
- [产品北极星](../../product-north-star.md)
- [唯一主动计划](../../forge-enterprise-evolution-plan.md)
- [职责边界](../../platform-architecture.md)
- [Benchmark 方法与口径](../../benchmarks.md)
- [既有工程评估基线](architecture-quality-review-2026-09-07.json)

本报告未修复上述缺陷，也未将建议写入主动计划或宣布门禁晋级。运行代码与生产状态保持原样。
