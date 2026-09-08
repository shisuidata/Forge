# Forge 文档导航

> 目的：区分当前事实、稳定约束、主动计划和历史材料，避免 Coding Agent 把旧计划恢复成当前任务。

## 进入项目先读

| 文档 | 角色 |
|---|---|
| [`current-project-state.md`](current-project-state.md) | 当前产品、阶段、门禁、未关闭验收项和 OMP 入口；默认第一入口 |
| [`product-north-star.md`](product-north-star.md) | 稳定产品价值、正确性边界与非目标 |
| [`forge-enterprise-evolution-plan.md`](forge-enterprise-evolution-plan.md) | 唯一主动计划；当前为 R0 Open-source Trust Runtime Product Cut / Adoption Baseline |
| [`platform-architecture.md`](platform-architecture.md) | Pi、Forge、Skills、Channel 的稳定职责边界 |
| [`requirements-pool.md`](requirements-pool.md) | 追加式需求与决策历史；当前有效产品需求为 `REQ-2026-09-03-025` |

按任务需要读取，不要在每次会话中扫描全部历史文档。

## 长期知识资产

测试证据、失败结论和沟通中形成的决策是项目核心资产，不是实现完成后即可清理的附属物。归档改变文档角色，不降低其保留优先级。

| 资产 | 保存位置与用途 |
|---|---|
| 测试结论、反例与验证报告 | [Benchmark归档](archive/benchmarks/)、[工程归档](archive/engineering/)；下方逐文件索引保留原实验、离线复算、失败与未完成记录。 |
| 用户表达、授权、取舍与决策 | [需求池](requirements-pool.md)保留沟通原话与决策依据；[产品公理](product-axioms.md)、[北极星](product-north-star.md)及[架构边界](platform-architecture.md)承接已确认的稳定结论。 |
| 方法、设计推演与复盘 | [Benchmark方法](benchmarks.md)、[架构课程](architecture-course/index.md)、[设计讨论](pipeline-architecture.md)、[Devlog](devlog/)；设计讨论中的历史方案不是当前实施授权。 |
| 被替代路线与否决依据 | [历史计划](archive/plans/)及下方历史产品／架构评审；保留当时背景，不恢复为当前待办。 |

当前状态页是可更新的摘要，不能替代原报告或需求历史。原始表达、证据结论与后续提炼分别保留，未知和缺失不补造；执行下方维护规则。

## 稳定工程说明

| 文档 | 角色 |
|---|---|
| [`architecture.md`](architecture.md) | 当前系统架构的精简入口 |
| [`architecture-course/index.md`](architecture-course/index.md) | 从可信问数原理到生产架构的系统教材 |
| [`how-it-works.md`](how-it-works.md) | DSL、Registry、Assurance 和执行流程 |
| [`product-projection-contracts.md`](product-projection-contracts.md) | Product Projection v1 只读 Contract |
| [`production-deployment.md`](production-deployment.md) | 生产部署边界与操作说明 |
| [`agent-integration.md`](agent-integration.md) | 外部 Agent 的受控集成边界 |
| [`benchmarks.md`](benchmarks.md) | BIRD固定快照、D/R/H/S、EX/业务质量口径、预算/可比性规则与可复现CLI |

## 战略参考，不是自动实施授权

- [`product-design-roadmap-2026-08-25.md`](product-design-roadmap-2026-08-25.md)：长期产品面、对象与方向地图。
- [`ai-native-enterprise-thesis.md`](ai-native-enterprise-thesis.md)：企业 AI Native 假设与待验证论证。
- [`product-axioms.md`](product-axioms.md)：产品和工程决策公理。
- [`commercial-readiness.md`](commercial-readiness.md)、[`commercialization-plan.md`](commercialization-plan.md)：商业化参考，不覆盖当前 R0 开源采用门禁。


## 历史计划与证据归档

归档只按文档角色和已完成／已替代状态划分，不按日期机械搬移；以下材料用于溯源，不是当前待办：

- [`archive/benchmarks/`](archive/benchmarks/)：已完成的 Accuracy / Benchmark 报告、准备快照、负结果与历史失败分析；逐轮索引见下文。另保留早期 [Method K 公开报告](archive/benchmarks/report_2026-03-15_185406.md)。
- [`archive/engineering/`](archive/engineering/)：工程、测试、发布、验收与评估报告；[REQ-060 原始评估](archive/engineering/architecture-quality-review-2026-09-07.json)与[实施报告](archive/engineering/architecture-quality-implementation-2026-09-07.json)各自保留原始 JSON 字节，不覆盖基线。
- [`archive/plans/`](archive/plans/)：已被替代的 [Pi 集成计划](archive/plans/pi-forge-integration-plan.md)、[Product Spine 计划](archive/plans/short-term-product-spine-plan-2026-08-25.md)、[Web Product Shell 计划](archive/plans/web-product-shell-plan-2026-08-24.md)与[早期 EA 优化计划](archive/plans/optimization_plan_2026-03-19.md)。
- [`devlog/`](devlog/) 与 [`architecture-course/`](architecture-course/) 保持原位；长期产品路线图及上述战略参考也保持原位，不因带日期而自动归档。

归档 Markdown 仅重定位必要的可点击链接，历史命令、代码／报告字符串、原始路径与验证结论按当时上下文保留。JSON 内的路径和 hash 是历史证据，不是当前工作目录相对路径；不得为让旧路径可执行而迁移本地数据、补造结果或创建旧路径包装文件。

文件名中的日期、完成项或旧路线不能替代文档顶部状态和 [`current-project-state.md`](current-project-state.md)。

文件结构职责见[CONTRIBUTING](../CONTRIBUTING.md#repository-layout)；本轮保留／迁移／归档／待确认决策、逐文件路径与SHA-256及本地验证见[REQ-061治理记录](file-structure-governance-2026-09-07.json)。旧本地数据与缓存不随目录归位自动搬迁。

## 候选分支交付（2026-09-08）

- [REQ-063候选交付记录](candidate-delivery-2026-09-08.json)：公开范围审查、既有接口文档维护、候选分支／PR与远端CI；不代表已合并、Release、部署或外部采用。
- [Python／打包原始审查](archive/engineering/candidate-python-publication-audit-2026-09-08.json)：67份当前文件及审查边界；原始静态审查不重写为后续CI结果。
- [Pi／Web／工具原始审查](archive/engineering/candidate-pi-web-publication-audit-2026-09-08.json)：179份当前文件、公开前身与40份合成Gold CSV；保留继承的低风险观察。
- [文档原始扫描](archive/engineering/candidate-documentation-publication-audit-2026-09-08.json)：107份文档与原始疑似项；最终处置、三份原件SHA-256及后续变更见候选交付记录。三份原件均按原字节保留，不构成独立安全认证。

## 本次仓库版本交付（2026-09-07）

- [发布验证报告](archive/engineering/release-verification-2026-09-07.json)：本地Python/Pi、Quickstart、站点构建、Assurance v10升级边界及未覆盖范围；远端CI以对应commit为准。
- [报告公开说明](archive/engineering/report-publication-2026-09-07.json)：逐文件原件/公开版hash、最小脱敏规则、数据来源与许可边界。

以下是历史实验与验收报告；本次发布报告与公开说明不计为新的模型实验。

## Accuracy / Benchmark 逐轮报告

本节REQ-028–058历史部分覆盖当时所有 `accuracy-*.json` 与 `benchmark-*` 正式报告、准备清单和同名简报：**40份JSON＋15份Markdown，共55个文件**（含1份早期运行文档重建汇总）；后续REQ-064–066材料单列追加，不回写历史计数。同一实验的JSON与简报不是两轮；新生成、旧候选离线重放、准备快照及失败未完成记录分别标明。下方另列16份早期测试/工程验收报告，避免因文件名前缀遗漏。

### 公开报告与本地原始证据边界

- 相对链接指向仓库报告或公开投影，不承诺其依赖的原始运行数据随仓库提供。公开处理规则、原件/公开副本SHA-256映射见 [报告公开说明](archive/engineering/report-publication-2026-09-07.json)；历史报告内的原始hash仍绑定原件，不能当作脱敏副本hash。
- `.forge/`、`artifact://`、`local://`、个人绝对路径及内部预览地址仅是维护者本地证据定位，不是公网下载链接或全新克隆可复现承诺。原候选、会话、日志、原始数据库与第三方数据集不随索引复制；不得为补齐链接而提交凭证或运行目录。
- 原报告审查发现个人工作目录路径及公共BIRD样本中的个人联系字段；公开来源不等于可直接再披露，公开投影与原件分开核对。未检出实际密钥不等于完成独立安全认证；数据来源、许可与脱敏边界以公开说明为准。
- 结果口径遵循 [Benchmark标准](benchmarks.md)：EX/Contract分开，Gold阻塞保留未知与完整分母，未派发不伪装成有效候选；已知tokens不是含未知usage的完整成本，也不是结算金额。表内请求/派发遵循原报告计数，不替代独立Provider HTTP实测。
- 准备报告保留当时“未授权/待预算”的历史状态；后续获批结果另列。离线修复不计新模型收益，已曝光小样本不计H或泛化证据，不回填/改写失败轮次或历史评分。

### REQ-028–033 早期运行记录

汇总入口：[早期逐轮汇总 JSON](archive/benchmarks/benchmark-historical-runs-2026-09-07.json) 仅从 [需求池](requirements-pool.md)、[Benchmark说明](benchmarks.md) 和 [当前状态](current-project-state.md) 重建历史记录，**不是新复测或原始运行导出**。保留已知Run ID、来源、离线/新生成边界；缺失分数、用量与Run ID为null，不访问本地数据库补造。

| 需求 | 证据类别与保留结论 |
|---|---|
| REQ-028 | 500题/1000次新生成：Forge 266/500 vs Direct 311/500，Forge更高token/时延，未支持准确率优势。 |
| REQ-029 | 1题canary＋19题，共40次新生成定向诊断：Forge历史0/20→9/20；有选择偏差，不是完整500题结论。 |
| REQ-030 | 旧候选离线9/20→14/20；另一次新生成经最终Compiler重评17/20 vs14/20，不能合并为纯Compiler因果。 |
| REQ-031 | 完整500题新生成封存287/500 vs314/500；相同候选离线修复313/500 vs314/500，近似持平且Forge成本更高。 |
| REQ-032 | 六次入口/canary/pilot/full尝试分别保留；nominal full每臂仅78候选、422缺失，48/500 vs54/500不算有效完整成绩。 |
| REQ-033 | 需求历史提到Luna首轮500题完成及重复运行276/500时停止，缺Run ID/完整评分；后续50题三模型矩阵仍无完成证据。与当前有效三轮口径并列披露，不认证第四轮或补造横向成绩。 |

REQ-035为规划；REQ-042/043为目标与方向讨论，不是遗漏的模型实跑报告，参见 [需求池](requirements-pool.md) 与 [主动计划](forge-enterprise-evolution-plan.md)。

### REQ-034–058 报告文件清单

| 需求 | 报告文件 | 类别 | 结果与限制（报告时点） |
|---|---|---|---|
| REQ-034 | [`archive/benchmarks/benchmark-sol-error-triage-2026-09-05.json`](archive/benchmarks/benchmark-sol-error-triage-2026-09-05.json) | 旧候选离线归因 | 209题、373失败臂；Agent初判而非人审裁决或纠正后准确率。 |
| REQ-036 | [`archive/benchmarks/benchmark-sol-metric-replay-2026-09-05.json`](archive/benchmarks/benchmark-sol-metric-replay-2026-09-05.json) | 旧候选离线重评 | Official EA仍313/500 vs314/500；Contract v2为284/500 vs294/500，只是评价器纠错。 |
| REQ-036 | [`archive/benchmarks/accuracy-evaluation-cohorts-2026-09-05.json`](archive/benchmarks/accuracy-evaluation-cohorts-2026-09-05.json) | 冻结清单／准备 | D50/R500/S27冻结；H/P未就绪，独立审核门禁未完成。 |
| REQ-037 | [`archive/benchmarks/accuracy-holdout-source-audit-2026-09-05.json`](archive/benchmarks/accuracy-holdout-source-audit-2026-09-05.json) | 离线来源审计／准备 | 1011条候选池不等于H；最终H样本0，标签和独立审核未就绪。 |
| REQ-037 | [`archive/benchmarks/accuracy-metadata-treatment-audit-2026-09-05.json`](archive/benchmarks/accuracy-metadata-treatment-audit-2026-09-05.json) | 离线假设审计 | format+PK对D16没有新增有效取值证据，不运行该假设。 |
| REQ-037 | [`archive/benchmarks/accuracy-metadata-binding-fix-2026-09-05.json`](archive/benchmarks/accuracy-metadata-binding-fix-2026-09-05.json) | 离线输入修复 | 恢复48列已有元数据；新生成0，不宣称准确率收益。 |
| REQ-037 | [`archive/benchmarks/accuracy-csv-binding-experiment-card-2026-09-05.json`](archive/benchmarks/accuracy-csv-binding-experiment-card-2026-09-05.json) | 实验卡／已回填诊断 | 保留准备与授权历史；22请求、20候选，诊断未晋级，不重复计作一轮。 |
| REQ-037 | [`archive/benchmarks/benchmark-luna-binding-2026-09-05.json`](archive/benchmarks/benchmark-luna-binding-2026-09-05.json) | 新生成／负结果 | 22请求含2次参数拒绝；Forge 3/5→2/5、Direct 3/5→3/5；有效生成tokens 28611→38709。 |
| REQ-038 | [`archive/benchmarks/benchmark-luna-strict-output-2026-09-05.json`](archive/benchmarks/benchmark-luna-strict-output-2026-09-05.json) | 新生成 canary | 2次Luna请求验证strict协议链路；不是准确率提升或跨Provider证明。 |
| REQ-039 | [`archive/benchmarks/benchmark-luna-alias-fix-2026-09-05.json`](archive/benchmarks/benchmark-luna-alias-fix-2026-09-05.json) | 旧候选离线维护 | 新生成0；仅处理组md-052 Forge恢复，处理组2/5→3/5；不重写旧成绩。 |
| REQ-040 | [`archive/benchmarks/benchmark-luna-strict-paired-2026-09-05.json`](archive/benchmarks/benchmark-luna-strict-paired-2026-09-05.json) | 新生成＋原响应恢复 | 20请求；Forge 2/5→3/5、Direct 3/5→3/5；保留栈溢出、3819 tokens原响应恢复及Assurance拒绝，无补跑。 |
| REQ-041 | [`archive/benchmarks/benchmark-luna-expanded-regression-2026-09-05.json`](archive/benchmarks/benchmark-luna-expanded-regression-2026-09-05.json) | 新生成扩展回归 | 20请求、81030 tokens；Forge EA6/10、Contract5/10，Direct均7/10；无旧配置对照或泛化结论。 |
| REQ-044 | [`archive/benchmarks/benchmark-luna-projection-instructions-2026-09-05.json`](archive/benchmarks/benchmark-luna-projection-instructions-2026-09-05.json) | 新生成／不采用 | 20请求、72546 tokens；Forge 4/5→4/5、Direct 4/5→5/5；候选Prompt不采用。 |
| REQ-045 | [`archive/benchmarks/benchmark-standards-engineering-2026-09-05.json`](archive/benchmarks/benchmark-standards-engineering-2026-09-05.json) | 离线工程验证 | 新生成0；CLI/协议与十个旧候选重放，不是新准确率成绩。 |
| REQ-045 | [`archive/benchmarks/benchmark-luna-standard-d20-2026-09-05.json`](archive/benchmarks/benchmark-luna-standard-d20-2026-09-05.json) | 新生成／失败未完成 | 40派发、39候选；1次超时usage未知，已知149300 tokens非完整总量；md-340 Gold阻塞，不发布正式准确率。 |
| REQ-046 | [`archive/benchmarks/benchmark-luna-mixed-d20-2026-09-05.json`](archive/benchmarks/benchmark-luna-mixed-d20-2026-09-05.json) · [简报](archive/benchmarks/benchmark-luna-mixed-d20-2026-09-05.md) | 新生成 seed43／评分不完整 | 38派发/38候选、139490 tokens；md-340留20题分母零调用，Gold未知，run failed。 |
| REQ-046 | [`archive/benchmarks/benchmark-luna-mixed-d20-seed44-2026-09-05.json`](archive/benchmarks/benchmark-luna-mixed-d20-seed44-2026-09-05.json) · [简报](archive/benchmarks/benchmark-luna-mixed-d20-seed44-2026-09-05.md) | 新生成 seed44／评分不完整 | 38派发/38候选、134585 tokens；Gold阻塞；延续题两臂EX无净增，恢复与回退并存。 |
| REQ-046 | [`archive/benchmarks/benchmark-luna-mixed-d20-seed45-2026-09-05.json`](archive/benchmarks/benchmark-luna-mixed-d20-seed45-2026-09-05.json) · [简报](archive/benchmarks/benchmark-luna-mixed-d20-seed45-2026-09-05.md) | 新生成 seed45／评分不完整 | 38派发/38候选、137028 tokens；Gold阻塞；同配置再生成，不是优化因果。 |
| REQ-046 | [`archive/benchmarks/benchmark-luna-offline-triage-2026-09-05.json`](archive/benchmarks/benchmark-luna-offline-triage-2026-09-05.json) · [简报](archive/benchmarks/benchmark-luna-offline-triage-2026-09-05.md) | 旧候选离线归因 | 17延续题含1题Gold阻塞；不是纠正后准确率，部分归因由REQ-047后续复核收紧。 |
| REQ-047 | [`archive/benchmarks/benchmark-luna-date-context-2026-09-05.json`](archive/benchmarks/benchmark-luna-date-context-2026-09-05.json) · [简报](archive/benchmarks/benchmark-luna-date-context-2026-09-05.md) | 新生成／门槛失败 | 20请求、73565 tokens；Forge 2/5→3/5、Direct 3/5→3/5；新增通过无日期补充，不计日期收益，默认off。 |
| REQ-047 | [`archive/benchmarks/benchmark-luna-output-contract-review-2026-09-06.json`](archive/benchmarks/benchmark-luna-output-contract-review-2026-09-06.json) · [简报](archive/benchmarks/benchmark-luna-output-contract-review-2026-09-06.md) | 旧候选离线契约复核 | 新生成0；粒度提示不等于业务确认，Gold范围/表示差异保留；不改历史评分。 |
| REQ-046 | [`archive/benchmarks/benchmark-luna-mixed-d20-seed46-2026-09-06.json`](archive/benchmarks/benchmark-luna-mixed-d20-seed46-2026-09-06.json) · [简报](archive/benchmarks/benchmark-luna-mixed-d20-seed46-2026-09-06.md) | 新生成 seed46／评分不完整 | 38派发/38候选、136237 tokens；Gold阻塞，Direct出现回退，不是完整基线。 |
| REQ-048 | [`archive/benchmarks/benchmark-luna-grain-context-preparation-2026-09-06.json`](archive/benchmarks/benchmark-luna-grain-context-preparation-2026-09-06.json) | 准备快照 | 新生成0；冻结两条件，准备状态不等于已完成实验；后续执行另列。 |
| REQ-048 | [`archive/benchmarks/benchmark-luna-grain-context-2026-09-06.json`](archive/benchmarks/benchmark-luna-grain-context-2026-09-06.json) · [简报](archive/benchmarks/benchmark-luna-grain-context-2026-09-06.md) | 新生成／配对未完成 | 授权76，实际52派发/48候选、24臂未派发；已知172108 tokens＋4次未知；共同有效5题无增益，Gold阻塞保留。 |
| REQ-048 | [`archive/benchmarks/benchmark-luna-grain-maintenance-2026-09-06.json`](archive/benchmarks/benchmark-luna-grain-maintenance-2026-09-06.json) | 旧候选离线维护 | 新生成0；修复未派发/未评分状态及失败证据保存，80臂重放；不回填旧未知usage。 |
| REQ-049 | [`archive/benchmarks/benchmark-luna-scalar-cte-fix-2026-09-06.json`](archive/benchmarks/benchmark-luna-scalar-cte-fix-2026-09-06.json) | 旧候选离线Compiler维护 | 新生成0；原md-199 Forge候选从超时恢复，其他79臂不变；不是新生成准确率收益。 |
| REQ-046 | [`archive/benchmarks/benchmark-luna-mixed-d20-seed48-2026-09-06.json`](archive/benchmarks/benchmark-luna-mixed-d20-seed48-2026-09-06.json) · [简报](archive/benchmarks/benchmark-luna-mixed-d20-seed48-2026-09-06.md) | 新生成 seed48／失败未完成 | 授权38，实际32派发/30候选、6臂未启动；已知107160 tokens＋2次未知；Gold阻塞，不补跑或回填。 |
| REQ-050 | [`archive/benchmarks/benchmark-luna-cancellation-evidence-fix-2026-09-06.json`](archive/benchmarks/benchmark-luna-cancellation-evidence-fix-2026-09-06.json) | 离线运行时维护 | 新生成0；修复取消终止证据竞态，保留超时/零重试/失败评分；不恢复旧未知usage。 |
| REQ-051 | [`archive/benchmarks/benchmark-luna-generation-stages-2026-09-06.json`](archive/benchmarks/benchmark-luna-generation-stages-2026-09-06.json) · [简报](archive/benchmarks/benchmark-luna-generation-stages-2026-09-06.md) | 新生成定向诊断 | 4请求、19831 tokens；均可执行但EX/Contract 0/4，未复现旧超时，不宣称时延改善。 |
| REQ-052 | [`archive/benchmarks/benchmark-luna-cte-binding-fix-2026-09-06.json`](archive/benchmarks/benchmark-luna-cte-binding-fix-2026-09-06.json) · [简报](archive/benchmarks/benchmark-luna-cte-binding-fix-2026-09-06.md) | 旧候选离线Compiler/Assurance维护 | 新生成0；合法CTE绑定与漏字段拒绝修复；原候选评分不变，不自动补列。 |
| REQ-053 | [`archive/benchmarks/benchmark-r500-v8-binding-audit-2026-09-06.json`](archive/benchmarks/benchmark-r500-v8-binding-audit-2026-09-06.json) · [简报](archive/benchmarks/benchmark-r500-v8-binding-audit-2026-09-06.md) | 旧候选离线R500诊断／不完整 | 1000旧候选、996臂可评分；md-340/md-393两臂Gold超时，已评分无对错转换；不覆盖旧313/314或晋级完整可比成绩。 |
| REQ-054 | [`archive/benchmarks/benchmark-luna-value-context-ready-2026-09-06.json`](archive/benchmarks/benchmark-luna-value-context-ready-2026-09-06.json) · [简报](archive/benchmarks/benchmark-luna-value-context-ready-2026-09-06.md) | 准备／离线实现 | 新生成0、默认off；准备时未授权的16次提案与后续获批结果分开，不改历史准备状态。 |
| REQ-054 | [`archive/benchmarks/benchmark-luna-value-context-2026-09-06.json`](archive/benchmarks/benchmark-luna-value-context-2026-09-06.json) · [简报](archive/benchmarks/benchmark-luna-value-context-2026-09-06.md) | 新生成／门槛失败 | 16请求、35572 tokens、未知usage 0；Forge 2/4→3/4、Direct 3/4→4/4，目标3/4未达4/4，默认off。 |
| REQ-055 | [`archive/benchmarks/benchmark-luna-from-scope-fix-2026-09-07.json`](archive/benchmarks/benchmark-luna-from-scope-fix-2026-09-07.json) · [简报](archive/benchmarks/benchmark-luna-from-scope-fix-2026-09-07.md) | 旧候选离线Assurance维护 | 新生成0；1016历史SQL仅新增拒绝漏FROM绑定臂，输出/SQL/EX/Contract不变；非准确率收益。 |
| REQ-056 | [`archive/benchmarks/benchmark-denominator-scope-ready-2026-09-07.json`](archive/benchmarks/benchmark-denominator-scope-ready-2026-09-07.json) | 准备快照 | 新生成0；16次提案待独立授权的历史快照，不等于实跑结果。 |
| REQ-056 | [`archive/benchmarks/benchmark-luna-denominator-scope-2026-09-07.json`](archive/benchmarks/benchmark-luna-denominator-scope-2026-09-07.json) | 新生成／门槛失败 | 16请求、53058 tokens、未知usage 0；Forge 2/4→3/4、Direct 2/4→2/4；正确对照回退，默认不启用。 |
| REQ-057 | [`archive/benchmarks/benchmark-cte-expression-binding-fix-2026-09-07.json`](archive/benchmarks/benchmark-cte-expression-binding-fix-2026-09-07.json) | 旧候选离线Compiler维护 | 新生成0；548候选五方言2740项编译不变，上一轮16候选评分不变，缺失别名仍拒绝；非生成收益。 |
| REQ-058 | [`archive/benchmarks/benchmark-cte-interface-ready-2026-09-07.json`](archive/benchmarks/benchmark-cte-interface-ready-2026-09-07.json) | 准备快照 | 新生成0；CTE接口范例与16次独立授权提案，保留准备时状态；不替代生成报告。 |
| REQ-058 | [`archive/benchmarks/benchmark-luna-cte-interface-2026-09-07.json`](archive/benchmarks/benchmark-luna-cte-interface-2026-09-07.json) | 新生成／门槛失败 | 16请求、67367 tokens、未知usage 0；Forge EX/Contract 3/4→3/4、Direct 3/4→2/4；目标/零回退及Forge每正确答案tokens、生成P95护栏失败，默认不启用。 |

REQ-058当时结论：Forge目标一题恢复、一题回退，Direct输入不变仍发生回退，不能归因于Forge范例；处理总tokens +8.57%、Forge每正确答案tokens +13.11%、生成P95 +41.53%，后两项护栏失败。**默认不启用，不补跑、不扩样、不自动返修；历史滚动父Run仍seed48。** 准备快照与最终负结果均保留，不把16次开发对照视为新的完整基线。

### REQ-064 生成优化研究（不计新模型实验）

- [调研与离线复算](archive/benchmarks/forge-json-generation-research-2026-09-08.json)：旧500题配对统计、当前strict Schema说明/键序审计、500候选重排键的编译烟测及六项一手方法。新增模型/SQL/Gold执行均0；不是新EX或优化采用证据。
- [原始需求与评估](requirements-pool.md#req-2026-09-08-064以超过direct-sql的ex为目标重评forge-json生成优化)：优先单变量DSL说明，再研究答案规划；键序与有界返修分别验证，研究时64次开发设计仅为提案，后续准备及实跑见REQ-065。主动计划、H与R0.6不变；上方55份历史报告计数与负结果保留。

### REQ-065–066 关键DSL说明实验与限定样本里程碑

- [零调用准备卡](archive/benchmarks/benchmark-luna-schema-descriptions-ready-2026-09-08.json)：默认off的五字段说明、v5跨语言冻结与严格比较；45项SQLite语义检查、500题默认输入、原候选/真实SDK合成HTTP闭环。当时真实生成未授权的历史状态保留，不替代后续实跑。
- [64次真实ABBA报告](archive/benchmarks/benchmark-luna-schema-descriptions-2026-09-08.json) · [技术简报](archive/benchmarks/benchmark-luna-schema-descriptions-2026-09-08.md)：8题各两重复，处理Forge EX/Contract12/16 vsDirect10/16，两重复各6/8 vs5/8；原响应/重放一致。无同一目标题稳定恢复且有控制回退，采用门槛未过，保持off。
- [里程碑文案草稿](devlog/2026-09-08-forge-schema-descriptions-milestone.md)：解释本轮五字段接口说明、局部得分突破及限制；未发布，不声称总体或历史首次优势。
- [本次公开处理记录](archive/engineering/schema-descriptions-publication-2026-09-08.json)：三份JSON的原件/公开副本hash与最小路径脱敏；原始SSE、状态库、凭据和数据集不入Git。用户仅授权当前分支commit/push，不发布版本。

### 其他历史测试与工程验收报告

以下16份报告保持其当时范围；通过、失败及待人审门禁不能相互覆盖，也不与当前BIRD单次生成协议混算：

| 报告文件 | 范围与限制 |
|---|---|
| [`archive/benchmarks/benchmark_failure_analysis_2026-03-18.md`](archive/benchmarks/benchmark_failure_analysis_2026-03-18.md) | 历史large 40题失败归因；不是BIRD或当前模型成绩。 |
| [`archive/engineering/test-report-2026-05-05.md`](archive/engineering/test-report-2026-05-05.md) | 历史DeepSeek两轮large 40题及API/E2E；GLM-5.1因余额不足未完成。 |
| [`archive/engineering/test-report-2026-05-06.md`](archive/engineering/test-report-2026-05-06.md) | 历史Method AF、40题×3次；EA(any)/EA(all)/Run ACC分开，不与BIRD单次协议比較。 |
| [`archive/engineering/test-report-2026-07-13.md`](archive/engineering/test-report-2026-07-13.md) | 历史Method AI、40题×3次120/120；不承诺开放世界100%正确。 |
| [`archive/engineering/delivery-assessment-2026-05-07.md`](archive/engineering/delivery-assessment-2026-05-07.md) | 历史受控交付评估；缺客户业务域等条件，不替代当前R0.6。 |
| [`archive/engineering/chart-engine-bakeoff-2026-08-24.md`](archive/engineering/chart-engine-bakeoff-2026-08-24.md) | 隔离图表引擎选型验证，不是生产Runtime批准。 |
| [`archive/engineering/chart-storytelling-r0-evidence-2026-08-24.md`](archive/engineering/chart-storytelling-r0-evidence-2026-08-24.md) | 自动门禁通过、用户视觉/交互门禁失败。 |
| [`archive/engineering/chart-storytelling-echarts-focused-evidence-2026-08-24.md`](archive/engineering/chart-storytelling-echarts-focused-evidence-2026-08-24.md) | 候选自动门禁通过，等待用户视觉/信息价值确认。 |
| [`archive/engineering/golden-journey-acceptance-2026-08-24.md`](archive/engineering/golden-journey-acceptance-2026-08-24.md) | 物理链路PASS、可信产品结果FAIL；仅桌面范围。 |
| [`archive/engineering/golden-journey-p0-closure-2026-08-24.md`](archive/engineering/golden-journey-p0-closure-2026-08-24.md) | 三项声明P0关闭；P1、移动端和丰富可视化不在结论内。 |
| [`archive/engineering/web-product-shell-w3a-evidence-2026-08-24.md`](archive/engineering/web-product-shell-w3a-evidence-2026-08-24.md) | 自动门禁PASS、用户IA/交互门禁待确认；隔离原型。 |
| [`archive/engineering/product-spine-sp1-evidence-2026-08-25.md`](archive/engineering/product-spine-sp1-evidence-2026-08-25.md) | SP1只读Product Projection历史验证。 |
| [`archive/engineering/product-spine-sp2-evidence-2026-08-25.md`](archive/engineering/product-spine-sp2-evidence-2026-08-25.md) | SP2 Product BFF与带scope的Report Index历史验证。 |
| [`archive/engineering/product-spine-sp3-evidence-2026-08-25.md`](archive/engineering/product-spine-sp3-evidence-2026-08-25.md) | SP3 Product Shell基础历史验证。 |
| [`archive/engineering/product-spine-sp4-evidence-2026-08-25.md`](archive/engineering/product-spine-sp4-evidence-2026-08-25.md) | SP4真实页面通过SP5入口，不替代完整采用验收。 |
| [`archive/engineering/product-spine-sp5-evidence-2026-08-25.md`](archive/engineering/product-spine-sp5-evidence-2026-08-25.md) | 自动化/真实链路PASS；用户Atlas CHANGE修复后仍待复验。 |

### 历史产品与架构讨论

这些评审保留沟通中形成的约束、取舍和未完成条件，不以当时估计或批准范围替代当前状态：

| 文档 | 保存价值与边界 |
|---|---|
| [产品方向与架构复审](archive/engineering/product-direction-architecture-review-2026-08-24.md) | 四平面框架与产品公理的评审依据；保留从判断到架构决策的过程。 |
| [目标差距重评估](archive/engineering/forge-goal-gap-assessment-2026-08-24.md) | 历史目标差距和优先级取舍；阶段百分比不是当前完成度、准确率或商业成功率。 |
| [Governance Contract正式评审](archive/engineering/governance-contract-review-2026-08-24.md) | Contract评审范围与威胁模型；当时只批准作为M1A提案输入，不批准实施或证明Runtime覆盖。 |
| [Web主体内容审计](archive/engineering/web-product-content-audit-2026-08-24.md) | 主体内容与宣传文案的判定依据及历史检查范围；版本化旧报告不原地重写。 |

## 维护规则

- 新产品、体验、架构或业务需求先追加到 `requirements-pool.md`；不覆盖原始表达。
- 用户接受后才进入主动计划；只有影响稳定职责边界时才修改 `platform-architecture.md`。
- 实施进度、验证结果和风险写回主动计划及当前状态页；不要堆进 `AGENTS.md`。
- 历史文档不删除；被替代时明确标记历史/已吸收/未批准。
- 测试数量、候选地址和临时工作区状态属于易变证据，不作为长期导航文案。
- 结论应保留问题、条件／版本、证据出处、结果、限制，以及采纳或否决的理由；成功、负结果、未知和未完成均保留，不能只留下最佳数字。
- 后续纠错或新实跑另记日期、依据及与原结论的关系；不回填历史成绩，不覆盖hash绑定的原始证据。可读提炼链接原报告，不能用总结替换原文。
- 重要沟通按需求池既有模式追加原话、判断与明确授权；不能仅留在临时聊天或artifact链接中，也不把局部记录说成完整会话备份。
- 代码重构、目录治理、方案被否决或新版本测试通过，都不是删除历史知识的理由。公开脱敏副本与受限原件分开，不因长期保留而复制凭证、客户数据或私有会话到公开仓库。
- 本地落盘、Git提交与异地备份是不同事实。只有实际执行并核验后才能宣称相应状态；未经明确授权不commit、push、外传或迁移原始运行数据。
