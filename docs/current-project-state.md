# Forge 当前项目状态

> 状态：当前事实投影 · Last updated: 2026-09-07
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
- 当前已完成工程证据：Accuracy Lab 已完成三轮 500 题、1000-call 公共双臂运行。最新 Structured GPT-5.6 Run `pbr_6778bf9d34ae42fba0b070a5f9c154ba` 的运行时封存为 Forge 57.40%、Direct SQL 62.80%；修复确定性 Compiler 后对相同 Forge 候选重评为 62.60% vs 62.80%、Delta -0.20pp。此前文本 GPT-5.6 Run `pbr_76da9a18d96c4e13b2b810ba111bd599` 为 53.20% vs 62.20%，`openai/deepseek-v4-flash` Run `pbr_1f735d433a284366bfe6526146511792` 为 45.40% vs 56.40%。R0.6 采用入口聚焦回归 `14 passed`，Python 全套 `704 passed / 26 skipped`，Pi `118 passed`，TypeScript typecheck 与 Python compileall 通过；实际人工批准与 `--yes --json` 两条 `forge quickstart` 均完成失败关闭、Evaluate、Enforce、Explain 与 Dashboard 链路。另从公开 HTTPS 远端全新克隆 `main@5bbdabe5ceca10fd7128a825d277e5e4534d69e7`，在空工作目录完成 bootstrap + Quickstart 共 36 秒并重算回执 checksum 一致；该维护者 smoke 只证明公开路径可运行，不计入外部采用。
- `REQ-2026-09-04-032` DeepSeek 完整复验当前失败关闭，不构成第四轮有效 500 题运行：火山 Coding Plan canary 返回 `429 AccountQuotaExceeded`；官方入口 nominal full Run `pbr_9d5e4263afdb46cd8636d4b6599f0708` 虽有 500 个 case 终态，实际每臂仅 78 个候选、422 个候选缺失，运行后独立探针返回 `402 Insufficient Balance`。原始 48/500 vs 54/500 不进入当前得分，也不与历史 DeepSeek 或 GPT-5.6 比较；当前仍只有三轮有效 500 题、1000-call 双臂证据。
- `REQ-2026-09-03-028` 已验证：GPT-5.6 完整配对运行中 Forge only 22、Direct only 67（双侧 exact p=1.90e-6）；两臂均执行成功的 471 题仍为 22 vs 50。Forge 使用的 tokens 多 52.99%，平均生成时延高 39.37%。这直接否定当前实现和该固定模型下的广义 Forge JSON 准确率优势，不改变 Forge 作为生成后可信运行时的产品边界。
- `REQ-2026-09-04-029` 已验证定向结果：Pi Benchmark Forge 分支改为完整 Forge Schema 的 terminating custom tool，Prompt 只保留语义规则并移除默认四位舍入。相同 GPT-5.6 revision、相同 ContextSnapshot 的 20 道历史 Forge 错题中，Structured Forge 修复 9 题（0/20 → 9/20），其中舍入类 5/6；20/20 均生成通过工具校验的对象。Prompt 本文缩短 44.54%，但工具 Schema 使 provider prompt tokens 增加 25.54%；total tokens 降 4.52%、平均生成时延降 6.96%。该定向样本不代表完整 500 题成绩，语义类 0/4；当时剩余的 Compiler/identifier 非执行已由 `REQ-2026-09-04-030` 处理，仍不能宣称总体优势。
- `REQ-2026-09-04-030` 已验证：quoted qualified identifier 绑定、source/output identifier 方言渲染和 HAVING 双侧聚合 alias 展开已修复。旧 Structured Tool 的相同候选离线重评从 9/20 EA、10/20 Contract、15/20 Execution 提升到 14/20、15/20、20/20；重新生成的同一 20 题候选经最终 Compiler 重评为 Forge 17/20 EA、18/20 Contract、20/20 Execution/Compile，同期 Direct 为 14/20 EA。后者跨生成且样本有选择偏差；完整 500 题 53.20% vs 62.20% 基线不变。
- `REQ-2026-09-04-031` 已验证：完整 Structured GPT-5.6 运行 500/500 返回工具校验对象；最终 Compiler 对相同候选离线重评从 287/500 EA、257/500 Contract、439/500 Execution 提升到 313/500、280/500、496/500，Compile 500/500，零 EA 回退。Direct 为 314/500 EA；最终配对 Forge only 22、Direct only 23，exact p=1.0，支持统计持平而非 Forge 优势。Forge 仍多 59.57% tokens、51.69% 平均生成时间；历史与新候选不同，Structured Tool、Prompt、舍入规则和生成波动不能拆成单因素因果。
- `REQ-2026-09-05-034` 已完成 P0 固定候选离线归因：209道至少一臂EA失败的题、373个失败arm均有可复核记录，清单为 `docs/benchmark-sol-error-triage-2026-09-05.json`。Agent辅助初判86个arm存在候选错误、150个参考冲突、133个意图/输出歧义、4个未定；不是人审裁决、不是纠正后准确率，官方313/500 vs314/500不变。可确认错误以值/实体绑定最多（25个arm/16题）；下一步优先验证字段格式/枚举及范围、粒度和输出契约，不盲扩Compiler或模型调用。未修改运行时代码、Gold或Prompt，不替代外部采用门禁。
- `REQ-2026-09-05-035` 整体准确率规划保留为参考，详见唯一主动计划15.1节。ACC-0/ACC-1A已验证，H独立审核未完成；ACC-2首次Luna五题诊断已完成但未达到晋级条件，后续按具体错因做短反馈闭环，不机械推进阶段或扩大模型调用。500题只作回归集；R0.6外部采用门禁独立保留。
- `REQ-2026-09-05-036` 已验证：比较器升级至 `semantic-result-compare-v2`，公开Evaluate与Pi Benchmark绑定评价版本，未知映射失败关闭、版本漂移不发布completed混合成绩。原500题同候选Official EA仍313/500 vs314/500；Contract为284/500 vs294/500，相对v1新增4/3、零回退，仅为评价器纠错。`benchmark-sol-metric-replay-2026-09-05.json` 保存全量证据；`accuracy-evaluation-cohorts-2026-09-05.json` 冻结D50/R500/S27及H审核协议；后续进展见REQ-037。
- `REQ-2026-09-05-037` 已完成元数据审计、CSV绑定修复与Luna五题诊断：原format+PK对D16没有新增有效取值证据，停止该假设；通用大小写绑定修复恢复48列已有元数据。20个有效Luna候选中，Official EA/Contract均为Forge 3/5→2/5、Direct 3/5→3/5；零新增通过、一个Forge编译回退，不能宣称准确率提升。md-065取值绑定改善，md-077 Forge日期/人员金额改善，但仍有参考与输出契约差异，不改判。
- REQ-037的Luna实测共22次请求（2次temperature参数被拒+20次有效生成；补额获用户确认），复用Pi OAuth与Pi Benchmark Runtime，默认采样、无自动重试。20个候选结果与5个Gold均完成只读复算；双臂tokens 28,611→38,709（+35.29%），不扩大调用。保留元数据完整性修复；该轮md-052失败已由后续REQ-039定位为Compiler别名解析不一致并修复。原始证据：`benchmark-luna-binding-2026-09-05.json`。
- `REQ-2026-09-05-038` 已验证：Forge Benchmark改为Provider原生strict:true函数工具，保留完整DSL并去除传输占位null，不静默回退；历史prefer或Schema版本漂移禁止续跑。用户批准的1题双臂canary实际2次Luna请求、零重试，HTTP200且Forge完成编译执行；原始参数独立通过严格Schema校验。仅证明该协议链路可用，不代表准确率提升或其他Provider已验证。证据：`benchmark-luna-strict-output-2026-09-05.json`。
- `REQ-2026-09-05-039` 已验证：Compiler共用关系解析器支持省略AS的表/子查询别名，保留引号和作用域拒绝。20个原Luna候选离线复算仅处理组md-052 Forge的SQL改变，编译/Assurance/执行/EA/Contract通过；处理组Forge 2/5→3/5，其余三组3/5不变，零回退、零模型调用。修复前3个行为反例失败，修复后141项聚焦测试与5方言解析通过。证据：`benchmark-luna-alias-fix-2026-09-05.json`；不重写历史500题或宣称元数据准确率优势。
- `REQ-2026-09-05-040` 已完成20次Luna配对复测及全部原响应离线复核：旧元数据Forge EA/Contract 2/5、Direct 3/5，修复后元数据两臂3/5。处理组tokens 33883 vs24021（+41.06%）；无总体准确率或稳定收益结论。第10次发生Pi递归子Schema强制转换栈溢出，已改为校验前严格归一化；缺失候选失败关闭且保留tokens，132项Pi测试/typecheck通过。用户确认仅继续剩余10次；原故障响应恢复3819 tokens，仍因CTE漏投影被Assurance拒绝，未补跑。证据：`benchmark-luna-strict-paired-2026-09-05.json`；下一步优先澄清费用分组/姓名输出契约，不继续盲目扩大模型调用。
- `REQ-2026-09-05-041` 已完成当前配置10库10题扩展回归：用户批准20次请求、全部HTTP200、零Provider重试/替换；Forge EA6/10、Contract5/10、Compile/Execution10/10，Direct EA/Contract7/10、Execution10/10。tokens 51077 vs29953（+70.52%），总81030；20个原始候选独立复算、10个strict/Pi参数校验、源码与10库hash一致。暴露聚合投影遗漏、JOIN分母、日期/粒度和姓名列契约问题；md-454漏Top-5却过当前EA，隔离六校反例证明逻辑不等价，评分未改。证据：`benchmark-luna-expanded-regression-2026-09-05.json`。已曝光样本、无旧配置对照，不证明泛化或优化因果；本轮未改代码/Gold/Prompt/评分，不扩大调用。
- `REQ-2026-09-05-044` 已确认以BIRD固定快照/官方EX建设查询准确性基础，日常小样本、阶段R500全量，高覆盖/低静默错误/低用户负担共同约束。首个投影诊断重现3个原始候选、6种合法合成行为，机械补列破坏4个结果契约。用户另批20次Luna配对生成：Forge EX/Contract4/5→4/5，Direct4/5→5/5，3个正确对照保持通过；20份原始候选经Forge与官方EX核心复算零差异，零Provider重试/替换。总72546 tokens，Forge自身+0.25%；未变Direct指令仍出现波动，旧失败恢复不归因于候选。候选Prompt不采用，保持源码/Schema/Gold/评分，不加拒绝/补列或扩样追分。证据：`benchmark-luna-projection-instructions-2026-09-05.json`；5题不代表整体80%，历史500题不变。
- `REQ-2026-09-05-045` 已工程落地并验证：`forge benchmark bird freeze/validate/replay/compare/audit-context`与`forge benchmark quality`可复用；仓库固定500题/11库/88资产校验清单，D/R/H/S和EX/Contract/业务标签分离。Pi绑定不可变输入、2N显式授权、单回合/零重试/120秒/隔离上下文及源码/SDK/model版本，保留失败和未知成本；原候选/源码漂移不可冒充收益。实际CLI、HTTP与Web验收通过，Python786 passed/26 skipped、Pi143 passed/typecheck通过，CI新增Pi门禁。十个历史原候选重放仍双臂4/5，零付费新调用、无Prompt采纳或新准确率结论。标准见`benchmarks.md`，证据见`benchmark-standards-engineering-2026-09-05.json`。
- REQ-045首次标准Luna D20实跑：用户另批最多40次，Pi实际派发40次、返回39份候选；md-472 Forge 120秒超时且usage未知，运行failed、不补跑。原候选离线重放发现md-340两臂Gold均在30秒超时，评分不完整，compare拒绝纳入可比基线。完整20题分母下Forge已确认正确10、失败9、未评分1；Direct正确9、失败10、未评分1，不发布正式准确率或增益。已报告149300 tokens，另有一次未知消耗；37份已评分且有候选记录与运行判定一致，40份raw hash一致。证据：`benchmark-luna-standard-d20-2026-09-05.json`。Pi局部19题指标及未评分false字段不能当完整成绩；下一步先处理零模型调用的Gold预检与评分状态贯通，不直接扩至R500。
- REQ-046已验证评分门禁与混合回归：同模型历史曝光33题，seed43冻结11延续/6新/3旧。用户明确批准Gold不可评题留分母但零调用；md-340跳过两次生成，其余19题实际38次派发/38候选、无补跑、用量完整139490 tokens。EX确认正确Forge8/Direct9，Contract5/8，各1题未知；新6题EX双臂6/6但Contract4/6与5/6，不能宣称整体提升。md-259 Direct恢复；md-429 Forge仅EX恢复、Contract仍失败；Forge md-046/md-249再生成回退。40条臂判定重放一致、38份raw hash一致，compare因Gold未知拒绝正式基线。Python803 passed/26 skipped、Pi149 passed/typecheck与真实页面通过。简报与证据：`benchmark-luna-mixed-d20-2026-09-05.md/.json`；每轮必须交付简报。Gold规划器问题仍在，不改原SQL/库/超时；下一轮按规则延续15题（含Gold阻塞），余5位4新1旧，本次未自动启动。
- REQ-046历史续测seed45已完成（前轮seed44简报保留）：用户授权20题为16延续＋3新＋1旧，md-340留分母零调用，实际38派发/38候选，tokens137028且用量完整。EX确认正确6/6、Contract3/4，各1题未知；同一15道可评分延续题EX为Forge2→4/Direct4→4，Contract0→1/2→2。Forge md-289恢复，md-429仅恢复EX且Contract仍失败；共享题无EX/Contract回退，但md-447再次被字段校验拒绝。同配置重生成，不作优化因果结论。38响应hash、40臂重放及源码/模型/运行版本/数据指纹一致，compare拒绝正式基线。简报：`benchmark-luna-mixed-d20-seed45-2026-09-05.md`，证据同名JSON；下轮17延续＋2新＋1旧，未自动启动，优先离线归因持续错题。
- REQ-046后续已完成零新生成离线归因：17延续题主因分为6道生成/作用域错误、6道参考冲突、4道输出契约歧义、1道Gold阻塞；不是纠正后准确率。91条分片SQL独立重执行一致，原38候选的SQL与EX/Contract重评不变，源码/数据指纹一致。md-447源字段实际存在，错误为QUALIFY外层作用域不可见，撤回“虚构字段”推断；未采用自动迁移筛选或按Gold换列。未满足狭窄确定性修复门槛，本轮不改Compiler/Prompt/Gold/评分。简报及证据：`benchmark-luna-offline-triage-2026-09-05.md/.json`；下一候选优先实际日期格式上下文，须单独冻结变量/预算，不启动新生成。
- REQ-047严格归因复核已完成：撤回md-020/249/273/429四道“输出契约歧义”，转为两道输出粒度错误、两道表示差异；md-153时间口径和md-234同址实体合并保留中等置信度业务澄清，独立候选错误与原官方评分保留。日期观察以默认off的窄开关同时提供给两臂，绑定来源、样本局限和上下文hash；不改CSV、Gold、Compiler或评分。
- REQ-047日期上下文配对实跑已闭环：用户另批完整20次，Pi实际20派发/20候选，零重试策略/补跑/替换，用量完整73565 tokens。Forge EX/Contract2/5→3/5、Direct3/5→3/5；唯一新增通过md-289没有日期补充且输入未变，不计日期收益。md-483双臂及md-077 Forge改用正确ISO日期，但仍有聚合粒度/姓名列表示与参考的差异，目标无新增通过；默认off、不推广、不扩额。20臂重放/原始响应hash、19个可执行候选独立官方EX核心复算及来源一致性通过，1个Assurance拒绝保留。简报与证据：`benchmark-luna-date-context-2026-09-05.md/.json`。下一步先离线核对输出契约；日常17+2+1、H与R0.6门禁不变。
- REQ-047后续输出契约复核已完成（2026-09-06，零新Luna调用）：md-483的grouped提示由本地each/per正则生成并进入两臂Prompt，与Gold总计冲突，不是业务确认；真实粒度分歧不豁免独立日期错误。md-077的CSV明确支持姓名拼接，Gold另加活动关联/DISTINCT，当前快照掩盖范围与重复差异。26条结果查询、5个完整DDL且外键有效的隔离夹具、8份Prompt来源核对完成；不改运行时代码、Gold或原成绩。简报：`benchmark-luna-output-contract-review-2026-09-06.md/.json`。允许主动分析Gold总结通用改进，禁止答案泄漏、改分和伪装独立泛化；下一狭窄候选是移除未经确认的expected_grain，不扩关键词特判或自动追加生成。
- REQ-046历史滚动续测seed46已完成（2026-09-06）：用户“继续吧”授权17延续＋2新＋1旧，日期off；md-340留20题分母零调用，实际38派发/38候选、136237 tokens且用量完整。EX确认正确Forge6/Direct4，Contract3/2，各1未知。同一16道可评分延续题EX3→4/3→2、Contract0→1/1→0；Forge md-249恢复，Direct md-082 Contract与md-429 EX回退，md-447恢复执行但仍错、md-483被SQL解析拒绝。38响应hash与40臂评分/SQL重放一致；保留md-340 Forge编译状态not_applicable/pending投影差异。共享输入/生成契约/数据未变，但协议v2及中间日期机制源码指纹不同，不作正式配对或优化因果结论。简报：`benchmark-luna-mixed-d20-seed46-2026-09-06.md/.json`；下轮18延续＋1新＋1旧，累计Luna曝光48题，未自动启动。
- REQ-048付费配对实验仍未完成：正常Python/Pi ResultContract已删除未确认的expected_grain；显式grain_context对照不作为业务事实，也不逐字复原seed46。用户授权76次，实际52派发、48候选、4次空响应，24臂未派发且未补跑。共同有效5题的两臂EX/Contract均无变化；off组EX正确均4、Contract Forge1/Direct2，各1道Gold未评分。已观测172108 tokens，另4次用量未知，不能宣称准确率或成本收益。原运行及配对证据JSON不变；简报：`benchmark-luna-grain-context-2026-09-06.md/.json`。
- REQ-048后续维护已验证（零新增模型调用）：replay保留output=null且scored=false的未评分状态，真实生成失败仍计失败；新Pi原始响应保存content/stopReason/errorMessage，错误详情在Run重开后可查。修复前两个回归反例均失败，修复后Python814 passed/26 skipped、Pi150 passed/typecheck通过。使用当前源码新冻结的diagnostic上下文重放上一轮80臂，24个未派发臂不再被误判；80臂SQL/评分/execution与原Pi一致，原输出和raw_output不变，compare仍拒绝晋级。历史4次空响应根因无法追补；旧源码冻结不得续跑。证据：`benchmark-luna-grain-maintenance-2026-09-06.json`。预定off父Run的19延续＋1新已在后续seed48执行；H/R0.6门禁不变。
- REQ-049 SQLite标量极值CTE执行瓶颈已修复（零模型调用）：仅对简单列MIN/MAX且参与INNER/CROSS JOIN的非递归标量CTE采用窄物化，优化SQL要求SQLite≥3.35。md-199原Forge JSON不变，从30秒超时恢复为Assurance/执行/EX/Contract通过，独立只读烟测79.26ms。原80臂重放仅此臂恢复，另79臂核心状态/评分/SQL不变；24个旧Forge候选的120项五方言编译结果只改变此SQLite SQL。两个修复前预算反例及NULL/空输入/下推边界通过，Python818 passed/26 skipped、Pi150 passed/typecheck通过。简报：`benchmark-luna-scalar-cte-fix-2026-09-06.json`。原付费结果不改写，新diagnostic有效、旧冻结与晋级仍拒绝；不代表新模型收益。后续seed48已按独立授权执行19延续＋1新，H/R0.6门禁不变。
- REQ-046最新滚动seed48已交付简报（2026-09-06）：用户另批38次，19延续＋新md-313，日期/粒度均off。实际32派发/30候选后失败：md-234 Forge生成120秒超时，md-447 Forge被取消；3题6臂未启动，md-340 Gold留分母零调用，无补跑。完整20题下两臂EX均4正确/12失败/4未知、Contract均1/15/4；仅新md-313双臂全过。Direct md-429恢复EX、md-259回退EX/Contract；md-199新Forge候选引用CTE未输出的best_ms，与REQ-049已修复旧候选不同。观察107160 tokens，另2臂用量未知。生成前删除冗余浮点比例字段修复Python/JS协议hash漂移，题目/上下文/预算不变；Python819 passed/26 skipped、Pi150 passed/typecheck，真实HTTP往返通过。32留存响应hash、40臂核心评分重放、28可执行候选独立EX一致；差异和446日志封存。简报：`benchmark-luna-mixed-d20-seed48-2026-09-06.md/.json`。服务已停，不完整基线不晋级；累计Luna曝光50题，下轮19延续＋1新，未授权自动执行。
- REQ-050生成终止证据竞态已修复（零模型调用）：同一会话复用abort Promise，等待取消完成后再封存失败的raw_output/hash/usage，末尾事件处理完才退订与dispose；120秒上限、单派发/零重试、失败即停和评分不变。修复前deadline回归丢失末尾响应，修复后持久化与重开一致；迟到SQL不送评分、后续题不派发。真实Pi SDK合成流烟测确认最终消息/usage晚于abort发出，网络请求0。Pi151 passed/typecheck、Python819 passed/26 skipped，seed48原40臂诊断重放与旧工件hash不变。报告：`benchmark-luna-cancellation-evidence-fix-2026-09-06.json`。历史md-234内容与两臂未知usage不补造，不宣称准确率收益或自动启动新生成。
- REQ-051生成阶段定向诊断已完成：用户单独授权4次，md-234/md-447各一个独立双臂Run；实际4次均生成/执行成功，EX/Contract均0/4，19831 tokens且无未知用量。Forge耗时11.18/23.60秒，工具参数可见区间3.27/7.46秒，未复现旧超时，不能宣称延迟改善。4份Prompt与seed48对应臂hash一致、4个原候选重放/独立EX一致；md-234暴露次数请求与Gold列集合差异，md-447为DOCType/DOC表示差异，仅作未采纳的只读反事实。未修改生产源码或生成策略、未追加调用，服务已停；不是seed49，滚动父Run仍为seed48。见`benchmark-luna-generation-stages-2026-09-06.md`及同名JSON。
- REQ-052 CTE字段绑定维护已验证（零模型调用）：删除显式限定名前缀剥离及裸列强绑主CTE；5个合法投影/聚合/窗口/CTE输出反例恢复预期SQLite结果。公共Forge JSON末尾复用`assure_compiled_sql`，Assurance升v8，两个CTE未投影字段漏拒关闭，原关系/Policy与执行授权边界保留。40个旧Forge候选200项五方言编译结果及5份原Run hash不变；md-032/md-199四臂诊断重放评分/SQL不变，缺列不补。真实CLI/HTTP正负例和返回SQL烟测通过，Python825 passed/26 skipped、Pi151 passed/typecheck通过，服务已停。不宣称BIRD收益或解决全部公共CTE前置限制；新诊断单独冻结，父Run仍seed48。见`benchmark-luna-cte-binding-fix-2026-09-06.md/.json`。
- REQ-053离线闭环（零被测模型调用）：R500原1000候选在当前Compiler/Assurance v8下诊断，旧正确误拒0、已评分正确/错误转换0；EX Forge311/187/2、Direct312/186/2，Contract282/216/2、292/206/2（正确/失败/未知），md-340/md-393两臂Gold超时保留未知。14变化臂全审，原候选/源码/数据hash保留；不是完整可比成绩或v8单因素收益。gasstations.Segment当前上下文确缺5个存储值，三个字面量反事实证明大小写机制，但原四臂仍1/4，不计模型提升。提出带来源的分类取值证据处理与4题16次对照提案，未实施、未授权调用；无新源码/Prompt/Schema改动，进程已退出。见`benchmark-r500-v8-binding-audit-2026-09-06.md/.json`。
- REQ-054取值证据默认off实现及获批16次Luna对照已完成：v4只对可见限定列附相同观察后缀，不改SQL/Gold/评分。Forge EX/Contract2/4→3/4、Direct3/4→4/4，目标四臂1/4→3/4未达预声明4/4，控制四臂两条件均通过。35572 tokens、未知用量0，处理组+17.85%；保持off，不补跑/扩样。md-009 Forge已用Discount但漏接cze/svk的FROM/JOIN，共享Assurance v8漏拒此样本，SQLite报unknown_column；不自动补JOIN。16响应hash、原候选回放与独立SQLite评分一致，216日志/Pi快照已封存，服务已停；Python873 passed/26 skipped、Pi151 passed/typecheck通过。准备期依赖误同步另行披露，最终两条件同环境。下一窄维护候选为FROM作用域校验；父Run仍seed48，H/R0.6未推进。见`benchmark-luna-value-context-2026-09-06.md/.json`。
- REQ-055 FROM作用域漏拒已修复（零模型调用）：共享Assurance升v9，区分可用CTE与实际FROM/JOIN绑定，并按使用位置保留合法祖先相关环境；不补关系、不改SQL。1016份历史SQL仅新增拒绝REQ-054处理组md-009 Forge，R500原997通过/3拒绝不变，旧正确新增误拒0。16原候选诊断重放的输出/SQL/EX/Contract全不变，仅该臂从执行失败前移为Assurance拒绝。65个SQLite边界无新增合法误拒，另18项仅静态方言/Scope检查；真实CLI/鉴权HTTP及合法返回SQL通过。Python887 passed/26 skipped、Pi151 passed/typecheck；13份历史来源hash不变，旧冻结和诊断晋级拒绝，服务已停。取值上下文保持off、父Run仍seed48，非准确率收益。见`benchmark-luna-from-scope-fix-2026-09-07.md/.json`。
- H来源仍仅有1011个机械候选/964依赖组件，最终H未选；独立审核、曝光SQL解析失败与完整快照缺口未关闭。Pi历史原始Run中的temperature=0/max_output_tokens=8192不代表有效请求设置，当时实际payload不传二者；新Run已显式记录provider_default/null。历史500题分数和H/外部采用门禁不因小样本或标准落地改变。
- `REQ-2026-09-03-027` 已验证：Compiler 仅在非集合查询中把未投影的聚合排序别名展开为原聚合表达式，保持可见结果列和已投影别名不变。公开 BIRD 历史候选离线回放中，case 988 run 3 从 `no such column` 变为 Official EA exact match；case 1011 的两个同类候选恢复可执行但仍因输出列契约不匹配保持 EA 失败。未发起新模型调用，也未改写完整 500 题 45.40% 基线。
- R0.6 证据采集准备已完成：Quickstart 先用写 SQL 证明 `assurance/readonly_violation` 失败关闭，再运行已审核只读查询；`summary.json` 生成不含 hostname、username、路径、SQL rows、凭证或私有 schema 的 `run_receipt` 与 SHA-256 漂移校验。公开 Quickstart adoption Issue 表单收集 tested revision、fresh-clone setup time、首个失败/困惑点、回执及开发者对 Policy、Evidence 和限制的独立解释；Forge 不发送 telemetry，checksum 不证明身份。
- 当前切入要求：R0.6 必须取得外部开发者独立完成 Golden Path 或提交 Adapter、Rule、Dataset、真实 failure case 的采用证据，并记录 setup time、失败点与修复闭环；内部 smoke、页面数量、自有题集和测试通过不能替代外部证据。可测试公开 revision 已发布，独立试跑招募见 [GitHub #9](https://github.com/shisuidata/Forge/issues/9)；下一步等待未参与实现的外部开发者 fresh clone 试跑并提交公开回执。
- 当前不扩张通用 Product Shell、SaaS Connector、非 SQL Action、Economics/Outcome Ledger 或完整企业权限平台；真实客户数据、生产凭证和高风险数据源仍需单独授权。

- REQ-056分母范例16次Luna对照已完成：16派发/16候选、未知用量0；Forge EX/Contract2/4→3/4，Direct2/4→2/4。md-259恢复全体750分母及完整答案，md-000恢复；正确对照md-079出现CTE聚合别名/投影不一致，Assurance拒绝，预声明零回退门槛失败，故默认不启用。总53058 tokens，处理组+6.03%；成本/P95护栏通过但不覆盖回退。16原响应hash/重放/独立SQLite核对及218日志、原生Pi快照已封存；25准备工件和源码/数据/输入hash不变，服务已停。无补跑/扩样/自动返修或seed49，日期/粒度/取值仍off、父Run仍seed48。见`benchmark-luna-denominator-scope-2026-09-07.json`。
- REQ-057表达式别名绑定维护已完成（零被测模型调用）：去掉SELECT expr限定名前缀剥离，改为SQLGlot AST本层Column单次替换，保留常量/注释、函数/类型名、子查询和插入SQL；修复相关HAVING、FIRST_VALUE/LAST_VALUE与隐藏聚合排序漏展开。12项回归先红后绿、42组独立边界全通过；依赖下限27经完整矩阵验证。548份历史候选2740项五方言编译不变，上一轮16份候选诊断的输出/EX/Contract/失败分类不变，md-079缺失别名仍拒绝，非生成准确率收益。真实CLI/API正负例和返回SQL通过，Python901 passed/26 skipped、Pi155 passed/typecheck；服务已停。默认Prompt/控制上下文hash不变，各可选上下文off，父Run仍seed48。见`benchmark-cte-expression-binding-fix-2026-09-07.json`。
- REQ-058 CTE接口范例16次独立授权开发对照已完成，门槛失败、默认不启用。Forge EX/Contract3/4→3/4：md-199新增正确、md-079回退；Direct3/4→2/4，md-079漏乘100，其输入不变，不能归因于Forge范例。总67367 tokens、未知用量0；处理总量+8.57%，Forge每正确答案+13.11%、生成P95+41.53%，后两项护栏失败。16原候选重放/独立SQLite及响应hash、215日志已核对，19准备工件和源码/数据/输入不变；首次本地鉴权拒绝发生于运行创建前、模型派发0，另有回执。预算余0，不补跑/扩样/自动返修，各可选上下文off、父Run仍seed48。准备证据保留；生成报告`benchmark-luna-cte-interface-2026-09-07.json`。
- 2026-09-07发布核对：71份历史测试/实验报告逐一建索引，公开副本仅脱敏本机路径/个人联系样本及修正私有链接；原件和hash留在本地，成绩不变。发现并修复既有嵌套CTE名称遮蔽物理表权限漏洞，Assurance升v10；旧v9 QueryRun须重新prepare/审批。旧生成报告仍是历史版本证据，不据此重写成绩或声称新版本准确率收益。发布验证见`release-verification-2026-09-07.json`，公开处理见`report-publication-2026-09-07.json`。

## 4. 已完成且可复用的工程基础

- M0 Contract 评审与 Product Projection Contract。
- Pi Task/ExecutionPlan/Artifact/StageAttempt 运行骨架。
- Forge Registry、Assurance、Compiler、Executor、QueryRun 与审批哈希。
- SP0–SP5 Product Spine 和完整 Product Shell 基础。
- Web、报告投影、Registry Studio、受控 Skills 与多渠道 Presentation 基础。
- Accuracy Benchmark Runtime 与 `/admin/benchmark`：持久 run/case/call 真相源、SSE 实时只读投影、部分/最终成绩区分和有界准确性声明。

这些完成项是后续验证的基础，不等于目标市场、产品体验或企业平台假设已经验证。

## 5. 未关闭的验收与采用事实

- 公开 GitHub 信号盘点中，现有 9 个 Issue 与 1 个 Pull Request 均由维护者身份提交；11 stars 与 1 fork 仅是传播信号。尚无可确认的外部开发者 Golden Path 回执，也没有外部 Adapter、Rule、Dataset 或真实 failure case 贡献。
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
