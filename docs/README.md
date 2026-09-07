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

## 历史计划与证据

以下文档保留用于溯源，不是当前待办：

- `pi-forge-integration-plan.md`
- `short-term-product-spine-plan-2026-08-25.md`
- `product-spine-sp*-evidence-2026-08-25.md`
- `product-direction-architecture-review-2026-08-24.md`
- `governance-contract-review-2026-08-24.md`
- `delivery-assessment-*.md`
- `devlog/`

文件名中的日期、完成项或旧路线不能替代文档顶部状态和 [`current-project-state.md`](current-project-state.md)。

## 本次仓库版本交付（2026-09-07）

- [发布验证报告](release-verification-2026-09-07.json)：本地Python/Pi、Quickstart、站点构建、Assurance v10升级边界及未覆盖范围；远端CI以对应commit为准。
- [报告公开说明](report-publication-2026-09-07.json)：逐文件原件/公开版hash、最小脱敏规则、数据来源与许可边界。

以下是历史实验与验收报告；本次发布报告与公开说明不计为新的模型实验。

## Accuracy / Benchmark 逐轮报告

本节覆盖所有 `accuracy-*.json` 与 `benchmark-*` 正式报告、准备清单和同名简报：**40份JSON＋15份Markdown，共55个文件**（含1份早期运行文档重建汇总）。同一实验的JSON与简报不是两轮；新生成、旧候选离线重放、准备快照及失败未完成记录分别标明。下方另列16份早期测试/工程验收报告，避免因文件名前缀遗漏。

### 公开报告与本地原始证据边界

- 相对链接指向仓库报告或公开投影，不承诺其依赖的原始运行数据随仓库提供。公开处理规则、原件/公开副本SHA-256映射见 [报告公开说明](report-publication-2026-09-07.json)；历史报告内的原始hash仍绑定原件，不能当作脱敏副本hash。
- `.forge/`、`artifact://`、`local://`、个人绝对路径及内部预览地址仅是维护者本地证据定位，不是公网下载链接或全新克隆可复现承诺。原候选、会话、日志、原始数据库与第三方数据集不随索引复制；不得为补齐链接而提交凭证或运行目录。
- 原报告审查发现个人工作目录路径及公共BIRD样本中的个人联系字段；公开来源不等于可直接再披露，公开投影与原件分开核对。未检出实际密钥不等于完成独立安全认证；数据来源、许可与脱敏边界以公开说明为准。
- 结果口径遵循 [Benchmark标准](benchmarks.md)：EX/Contract分开，Gold阻塞保留未知与完整分母，未派发不伪装成有效候选；已知tokens不是含未知usage的完整成本，也不是结算金额。表内请求/派发遵循原报告计数，不替代独立Provider HTTP实测。
- 准备报告保留当时“未授权/待预算”的历史状态；后续获批结果另列。离线修复不计新模型收益，已曝光小样本不计H或泛化证据，不回填/改写失败轮次或历史评分。

### REQ-028–033 早期运行记录

汇总入口：[早期逐轮汇总 JSON](benchmark-historical-runs-2026-09-07.json) 仅从 [需求池](requirements-pool.md)、[Benchmark说明](benchmarks.md) 和 [当前状态](current-project-state.md) 重建历史记录，**不是新复测或原始运行导出**。保留已知Run ID、来源、离线/新生成边界；缺失分数、用量与Run ID为null，不访问本地数据库补造。

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
| REQ-034 | [`benchmark-sol-error-triage-2026-09-05.json`](benchmark-sol-error-triage-2026-09-05.json) | 旧候选离线归因 | 209题、373失败臂；Agent初判而非人审裁决或纠正后准确率。 |
| REQ-036 | [`benchmark-sol-metric-replay-2026-09-05.json`](benchmark-sol-metric-replay-2026-09-05.json) | 旧候选离线重评 | Official EA仍313/500 vs314/500；Contract v2为284/500 vs294/500，只是评价器纠错。 |
| REQ-036 | [`accuracy-evaluation-cohorts-2026-09-05.json`](accuracy-evaluation-cohorts-2026-09-05.json) | 冻结清单／准备 | D50/R500/S27冻结；H/P未就绪，独立审核门禁未完成。 |
| REQ-037 | [`accuracy-holdout-source-audit-2026-09-05.json`](accuracy-holdout-source-audit-2026-09-05.json) | 离线来源审计／准备 | 1011条候选池不等于H；最终H样本0，标签和独立审核未就绪。 |
| REQ-037 | [`accuracy-metadata-treatment-audit-2026-09-05.json`](accuracy-metadata-treatment-audit-2026-09-05.json) | 离线假设审计 | format+PK对D16没有新增有效取值证据，不运行该假设。 |
| REQ-037 | [`accuracy-metadata-binding-fix-2026-09-05.json`](accuracy-metadata-binding-fix-2026-09-05.json) | 离线输入修复 | 恢复48列已有元数据；新生成0，不宣称准确率收益。 |
| REQ-037 | [`accuracy-csv-binding-experiment-card-2026-09-05.json`](accuracy-csv-binding-experiment-card-2026-09-05.json) | 实验卡／已回填诊断 | 保留准备与授权历史；22请求、20候选，诊断未晋级，不重复计作一轮。 |
| REQ-037 | [`benchmark-luna-binding-2026-09-05.json`](benchmark-luna-binding-2026-09-05.json) | 新生成／负结果 | 22请求含2次参数拒绝；Forge 3/5→2/5、Direct 3/5→3/5；有效生成tokens 28611→38709。 |
| REQ-038 | [`benchmark-luna-strict-output-2026-09-05.json`](benchmark-luna-strict-output-2026-09-05.json) | 新生成 canary | 2次Luna请求验证strict协议链路；不是准确率提升或跨Provider证明。 |
| REQ-039 | [`benchmark-luna-alias-fix-2026-09-05.json`](benchmark-luna-alias-fix-2026-09-05.json) | 旧候选离线维护 | 新生成0；仅处理组md-052 Forge恢复，处理组2/5→3/5；不重写旧成绩。 |
| REQ-040 | [`benchmark-luna-strict-paired-2026-09-05.json`](benchmark-luna-strict-paired-2026-09-05.json) | 新生成＋原响应恢复 | 20请求；Forge 2/5→3/5、Direct 3/5→3/5；保留栈溢出、3819 tokens原响应恢复及Assurance拒绝，无补跑。 |
| REQ-041 | [`benchmark-luna-expanded-regression-2026-09-05.json`](benchmark-luna-expanded-regression-2026-09-05.json) | 新生成扩展回归 | 20请求、81030 tokens；Forge EA6/10、Contract5/10，Direct均7/10；无旧配置对照或泛化结论。 |
| REQ-044 | [`benchmark-luna-projection-instructions-2026-09-05.json`](benchmark-luna-projection-instructions-2026-09-05.json) | 新生成／不采用 | 20请求、72546 tokens；Forge 4/5→4/5、Direct 4/5→5/5；候选Prompt不采用。 |
| REQ-045 | [`benchmark-standards-engineering-2026-09-05.json`](benchmark-standards-engineering-2026-09-05.json) | 离线工程验证 | 新生成0；CLI/协议与十个旧候选重放，不是新准确率成绩。 |
| REQ-045 | [`benchmark-luna-standard-d20-2026-09-05.json`](benchmark-luna-standard-d20-2026-09-05.json) | 新生成／失败未完成 | 40派发、39候选；1次超时usage未知，已知149300 tokens非完整总量；md-340 Gold阻塞，不发布正式准确率。 |
| REQ-046 | [`benchmark-luna-mixed-d20-2026-09-05.json`](benchmark-luna-mixed-d20-2026-09-05.json) · [简报](benchmark-luna-mixed-d20-2026-09-05.md) | 新生成 seed43／评分不完整 | 38派发/38候选、139490 tokens；md-340留20题分母零调用，Gold未知，run failed。 |
| REQ-046 | [`benchmark-luna-mixed-d20-seed44-2026-09-05.json`](benchmark-luna-mixed-d20-seed44-2026-09-05.json) · [简报](benchmark-luna-mixed-d20-seed44-2026-09-05.md) | 新生成 seed44／评分不完整 | 38派发/38候选、134585 tokens；Gold阻塞；延续题两臂EX无净增，恢复与回退并存。 |
| REQ-046 | [`benchmark-luna-mixed-d20-seed45-2026-09-05.json`](benchmark-luna-mixed-d20-seed45-2026-09-05.json) · [简报](benchmark-luna-mixed-d20-seed45-2026-09-05.md) | 新生成 seed45／评分不完整 | 38派发/38候选、137028 tokens；Gold阻塞；同配置再生成，不是优化因果。 |
| REQ-046 | [`benchmark-luna-offline-triage-2026-09-05.json`](benchmark-luna-offline-triage-2026-09-05.json) · [简报](benchmark-luna-offline-triage-2026-09-05.md) | 旧候选离线归因 | 17延续题含1题Gold阻塞；不是纠正后准确率，部分归因由REQ-047后续复核收紧。 |
| REQ-047 | [`benchmark-luna-date-context-2026-09-05.json`](benchmark-luna-date-context-2026-09-05.json) · [简报](benchmark-luna-date-context-2026-09-05.md) | 新生成／门槛失败 | 20请求、73565 tokens；Forge 2/5→3/5、Direct 3/5→3/5；新增通过无日期补充，不计日期收益，默认off。 |
| REQ-047 | [`benchmark-luna-output-contract-review-2026-09-06.json`](benchmark-luna-output-contract-review-2026-09-06.json) · [简报](benchmark-luna-output-contract-review-2026-09-06.md) | 旧候选离线契约复核 | 新生成0；粒度提示不等于业务确认，Gold范围/表示差异保留；不改历史评分。 |
| REQ-046 | [`benchmark-luna-mixed-d20-seed46-2026-09-06.json`](benchmark-luna-mixed-d20-seed46-2026-09-06.json) · [简报](benchmark-luna-mixed-d20-seed46-2026-09-06.md) | 新生成 seed46／评分不完整 | 38派发/38候选、136237 tokens；Gold阻塞，Direct出现回退，不是完整基线。 |
| REQ-048 | [`benchmark-luna-grain-context-preparation-2026-09-06.json`](benchmark-luna-grain-context-preparation-2026-09-06.json) | 准备快照 | 新生成0；冻结两条件，准备状态不等于已完成实验；后续执行另列。 |
| REQ-048 | [`benchmark-luna-grain-context-2026-09-06.json`](benchmark-luna-grain-context-2026-09-06.json) · [简报](benchmark-luna-grain-context-2026-09-06.md) | 新生成／配对未完成 | 授权76，实际52派发/48候选、24臂未派发；已知172108 tokens＋4次未知；共同有效5题无增益，Gold阻塞保留。 |
| REQ-048 | [`benchmark-luna-grain-maintenance-2026-09-06.json`](benchmark-luna-grain-maintenance-2026-09-06.json) | 旧候选离线维护 | 新生成0；修复未派发/未评分状态及失败证据保存，80臂重放；不回填旧未知usage。 |
| REQ-049 | [`benchmark-luna-scalar-cte-fix-2026-09-06.json`](benchmark-luna-scalar-cte-fix-2026-09-06.json) | 旧候选离线Compiler维护 | 新生成0；原md-199 Forge候选从超时恢复，其他79臂不变；不是新生成准确率收益。 |
| REQ-046 | [`benchmark-luna-mixed-d20-seed48-2026-09-06.json`](benchmark-luna-mixed-d20-seed48-2026-09-06.json) · [简报](benchmark-luna-mixed-d20-seed48-2026-09-06.md) | 新生成 seed48／失败未完成 | 授权38，实际32派发/30候选、6臂未启动；已知107160 tokens＋2次未知；Gold阻塞，不补跑或回填。 |
| REQ-050 | [`benchmark-luna-cancellation-evidence-fix-2026-09-06.json`](benchmark-luna-cancellation-evidence-fix-2026-09-06.json) | 离线运行时维护 | 新生成0；修复取消终止证据竞态，保留超时/零重试/失败评分；不恢复旧未知usage。 |
| REQ-051 | [`benchmark-luna-generation-stages-2026-09-06.json`](benchmark-luna-generation-stages-2026-09-06.json) · [简报](benchmark-luna-generation-stages-2026-09-06.md) | 新生成定向诊断 | 4请求、19831 tokens；均可执行但EX/Contract 0/4，未复现旧超时，不宣称时延改善。 |
| REQ-052 | [`benchmark-luna-cte-binding-fix-2026-09-06.json`](benchmark-luna-cte-binding-fix-2026-09-06.json) · [简报](benchmark-luna-cte-binding-fix-2026-09-06.md) | 旧候选离线Compiler/Assurance维护 | 新生成0；合法CTE绑定与漏字段拒绝修复；原候选评分不变，不自动补列。 |
| REQ-053 | [`benchmark-r500-v8-binding-audit-2026-09-06.json`](benchmark-r500-v8-binding-audit-2026-09-06.json) · [简报](benchmark-r500-v8-binding-audit-2026-09-06.md) | 旧候选离线R500诊断／不完整 | 1000旧候选、996臂可评分；md-340/md-393两臂Gold超时，已评分无对错转换；不覆盖旧313/314或晋级完整可比成绩。 |
| REQ-054 | [`benchmark-luna-value-context-ready-2026-09-06.json`](benchmark-luna-value-context-ready-2026-09-06.json) · [简报](benchmark-luna-value-context-ready-2026-09-06.md) | 准备／离线实现 | 新生成0、默认off；准备时未授权的16次提案与后续获批结果分开，不改历史准备状态。 |
| REQ-054 | [`benchmark-luna-value-context-2026-09-06.json`](benchmark-luna-value-context-2026-09-06.json) · [简报](benchmark-luna-value-context-2026-09-06.md) | 新生成／门槛失败 | 16请求、35572 tokens、未知usage 0；Forge 2/4→3/4、Direct 3/4→4/4，目标3/4未达4/4，默认off。 |
| REQ-055 | [`benchmark-luna-from-scope-fix-2026-09-07.json`](benchmark-luna-from-scope-fix-2026-09-07.json) · [简报](benchmark-luna-from-scope-fix-2026-09-07.md) | 旧候选离线Assurance维护 | 新生成0；1016历史SQL仅新增拒绝漏FROM绑定臂，输出/SQL/EX/Contract不变；非准确率收益。 |
| REQ-056 | [`benchmark-denominator-scope-ready-2026-09-07.json`](benchmark-denominator-scope-ready-2026-09-07.json) | 准备快照 | 新生成0；16次提案待独立授权的历史快照，不等于实跑结果。 |
| REQ-056 | [`benchmark-luna-denominator-scope-2026-09-07.json`](benchmark-luna-denominator-scope-2026-09-07.json) | 新生成／门槛失败 | 16请求、53058 tokens、未知usage 0；Forge 2/4→3/4、Direct 2/4→2/4；正确对照回退，默认不启用。 |
| REQ-057 | [`benchmark-cte-expression-binding-fix-2026-09-07.json`](benchmark-cte-expression-binding-fix-2026-09-07.json) | 旧候选离线Compiler维护 | 新生成0；548候选五方言2740项编译不变，上一轮16候选评分不变，缺失别名仍拒绝；非生成收益。 |
| REQ-058 | [`benchmark-cte-interface-ready-2026-09-07.json`](benchmark-cte-interface-ready-2026-09-07.json) | 准备快照 | 新生成0；CTE接口范例与16次独立授权提案，保留准备时状态；不替代生成报告。 |
| REQ-058 | [`benchmark-luna-cte-interface-2026-09-07.json`](benchmark-luna-cte-interface-2026-09-07.json) | 新生成／门槛失败 | 16请求、67367 tokens、未知usage 0；Forge EX/Contract 3/4→3/4、Direct 3/4→2/4；目标/零回退及Forge每正确答案tokens、生成P95护栏失败，默认不启用。 |

最新REQ-058：Forge目标一题恢复、一题回退，Direct输入不变仍发生回退，不能归因于Forge范例；处理总tokens +8.57%、Forge每正确答案tokens +13.11%、生成P95 +41.53%，后两项护栏失败。**默认不启用，不补跑、不扩样、不自动返修；历史滚动父Run仍seed48。** 准备快照与最终负结果均保留，不把16次开发对照视为新的完整基线。

### 其他历史测试与工程验收报告

以下16份报告保持其当时范围；通过、失败及待人审门禁不能相互覆盖，也不与当前BIRD单次生成协议混算：

| 报告文件 | 范围与限制 |
|---|---|
| [`benchmark_failure_analysis_2026-03-18.md`](benchmark_failure_analysis_2026-03-18.md) | 历史large 40题失败归因；不是BIRD或当前模型成绩。 |
| [`test-report-2026-05-05.md`](test-report-2026-05-05.md) | 历史DeepSeek两轮large 40题及API/E2E；GLM-5.1因余额不足未完成。 |
| [`test-report-2026-05-06.md`](test-report-2026-05-06.md) | 历史Method AF、40题×3次；EA(any)/EA(all)/Run ACC分开，不与BIRD单次协议比較。 |
| [`test-report-2026-07-13.md`](test-report-2026-07-13.md) | 历史Method AI、40题×3次120/120；不承诺开放世界100%正确。 |
| [`delivery-assessment-2026-05-07.md`](delivery-assessment-2026-05-07.md) | 历史受控交付评估；缺客户业务域等条件，不替代当前R0.6。 |
| [`chart-engine-bakeoff-2026-08-24.md`](chart-engine-bakeoff-2026-08-24.md) | 隔离图表引擎选型验证，不是生产Runtime批准。 |
| [`chart-storytelling-r0-evidence-2026-08-24.md`](chart-storytelling-r0-evidence-2026-08-24.md) | 自动门禁通过、用户视觉/交互门禁失败。 |
| [`chart-storytelling-echarts-focused-evidence-2026-08-24.md`](chart-storytelling-echarts-focused-evidence-2026-08-24.md) | 候选自动门禁通过，等待用户视觉/信息价值确认。 |
| [`golden-journey-acceptance-2026-08-24.md`](golden-journey-acceptance-2026-08-24.md) | 物理链路PASS、可信产品结果FAIL；仅桌面范围。 |
| [`golden-journey-p0-closure-2026-08-24.md`](golden-journey-p0-closure-2026-08-24.md) | 三项声明P0关闭；P1、移动端和丰富可视化不在结论内。 |
| [`web-product-shell-w3a-evidence-2026-08-24.md`](web-product-shell-w3a-evidence-2026-08-24.md) | 自动门禁PASS、用户IA/交互门禁待确认；隔离原型。 |
| [`product-spine-sp1-evidence-2026-08-25.md`](product-spine-sp1-evidence-2026-08-25.md) | SP1只读Product Projection历史验证。 |
| [`product-spine-sp2-evidence-2026-08-25.md`](product-spine-sp2-evidence-2026-08-25.md) | SP2 Product BFF与带scope的Report Index历史验证。 |
| [`product-spine-sp3-evidence-2026-08-25.md`](product-spine-sp3-evidence-2026-08-25.md) | SP3 Product Shell基础历史验证。 |
| [`product-spine-sp4-evidence-2026-08-25.md`](product-spine-sp4-evidence-2026-08-25.md) | SP4真实页面通过SP5入口，不替代完整采用验收。 |
| [`product-spine-sp5-evidence-2026-08-25.md`](product-spine-sp5-evidence-2026-08-25.md) | 自动化/真实链路PASS；用户Atlas CHANGE修复后仍待复验。 |

## 维护规则

- 新产品、体验、架构或业务需求先追加到 `requirements-pool.md`；不覆盖原始表达。
- 用户接受后才进入主动计划；只有影响稳定职责边界时才修改 `platform-architecture.md`。
- 实施进度、验证结果和风险写回主动计划及当前状态页；不要堆进 `AGENTS.md`。
- 历史文档不删除；被替代时明确标记历史/已吸收/未批准。
- 测试数量、候选地址和临时工作区状态属于易变证据，不作为长期导航文案。
