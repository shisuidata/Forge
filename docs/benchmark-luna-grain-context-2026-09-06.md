# Luna grain_context 配对实验简报（2026-09-06）

关联需求：REQ-2026-09-06-048。状态：**配对实验未完成；工程切换与复核已验证；仓库报告及状态回写完成。**

## 结论

尚无证据证明去掉未确认粒度提示能提高准确率。对照组中途失败；双方均获得有效候选的5题，Forge/Direct的EX与Contract全部保持不变。不能把处理组19题的结果与对照组7题的结果直接作增益比较。

普通ResultContract已删除把问题措辞当作业务确认的expected_grain字段。这修正了契约表达，不等于证明了问数准确率或成本优势。

## 设计与预算

- 同一20题、seed47：18题沿用 + 新题md-199 + 旧题复核md-040。保留md-340 Gold不可评分，不换题、不给它调用模型。
- 唯一实验参数：grain_context。对照question_heuristic给双方相同的“来自问题措辞、未经业务确认”提示；处理off不提供该提示。date_context在两边均为off。
- 对照是显式、有来源的实验控制，**不是seed46原prompt的逐字复现**。不能归因比较旧源码Run与本轮。
- 授权上限76次，每条件38次；先对照、后处理。实际52次派发、48份有效候选、4次不完整响应；24个臂未派发。没有重试、替换或补跑。
- 预先指定off处理组为下一轮父Run；不按哪组分数高来挑父Run。对照也计入Luna暴露历史。

| 条件 | Run | 派发 / 有效候选 | 结束状态 |
|---|---|---:|---|
| question_heuristic | pbr_fb550c000ff34a559d1f79e7738107ec | 14 / 10 | failed；无法resume；24臂未派发 |
| off | pbr_cb5bcb3341a24f619b7e8a75bbd83236 | 38 / 38 | failed；仅因冻结的Gold题不可评分而不完整 |

## 指标与成本

所有行保留20题分母。以下写“正确 / 失败 / 未评分”，不把缺失项填成错误或从分母删除。EX为既有official_ea口径，Contract为contract_accuracy；两者分列。

| 条件 | 臂 | EX | Contract | 观测Token | 用量未知的已派发臂 |
|---|---|---|---|---:|---:|
| 对照 | Forge | 3 / 4 / 13 | 1 / 6 / 13 | 20,432 | 2 |
| 对照 | Direct | 3 / 4 / 13 | 1 / 6 / 13 | 10,139 | 2 |
| off | Forge | 4 / 15 / 1 | 1 / 18 / 1 | 91,339 | 0 |
| off | Direct | 4 / 15 / 1 | 2 / 17 / 1 | 50,198 | 0 |

- 总计**172,108个已观测Token，另有4次派发用量未知**；实际总量不能写成172,108或把未知补零。
- off合计141,537 Token；Forge比Direct多约82%，EX正确题数持平，Contract少1题。此为失败富集的本批观察，不能外推总体准确率或长期成本。
- Forge有效候选中17/19执行成功：md-199执行超时，md-447 SQL解析失败。Direct为19/19执行成功；执行成功不等于答案正确。
- 对照耗时29.706秒；off耗时161.604秒，来自各Run开始、结束时间差；不是单请求延迟。

## 配对子集与逐题结果

两组两臂都有有效候选的题：md-020、md-032、md-040、md-082、md-127。每条件每臂均EX 3/5、Contract 1/5：10个配对臂中零恢复、零回退。该子集有幸存样本选择，不能满足完整配对验收门禁。

处理组逐题结果，单元格顺序为EX / Contract：

| Case | 组别 | Forge | Direct |
|---|---|---|---|
| md-020 | 沿用 | 正确 / 失败 | 正确 / 失败 |
| md-032 | 沿用 | 失败 / 失败 | 失败 / 失败 |
| md-040 | 旧题复核 | 正确 / 正确 | 正确 / 正确 |
| md-082 | 沿用 | 正确 / 失败 | 正确 / 失败 |
| md-127 | 沿用 | 失败 / 失败 | 失败 / 失败 |
| md-153 | 沿用 | 失败 / 失败 | 失败 / 失败 |
| md-199 | 新题 | 失败 / 失败 | 失败 / 失败 |
| md-222 | 沿用 | 失败 / 失败 | 失败 / 失败 |
| md-234 | 沿用 | 失败 / 失败 | 失败 / 失败 |
| md-249 | 沿用 | 失败 / 失败 | 失败 / 失败 |
| md-259 | 沿用 | 失败 / 失败 | 正确 / 正确 |
| md-273 | 沿用 | 失败 / 失败 | 失败 / 失败 |
| md-300 | 沿用 | 失败 / 失败 | 失败 / 失败 |
| md-340 | 沿用 | 未评分 / 未评分 | 未评分 / 未评分 |
| md-418 | 沿用 | 失败 / 失败 | 失败 / 失败 |
| md-429 | 沿用 | 正确 / 失败 | 失败 / 失败 |
| md-447 | 沿用 | 失败 / 失败 | 失败 / 失败 |
| md-472 | 沿用 | 失败 / 失败 | 失败 / 失败 |
| md-481 | 沿用 | 失败 / 失败 | 失败 / 失败 |
| md-483 | 沿用 | 失败 / 失败 | 失败 / 失败 |

相对seed46的18道沿用题（含1道Gold未评分），Forge md-249由EX/Contract均正确变成均失败；Direct md-259反向恢复。Forge md-483从解析失败变为可执行但仍答错，md-447从可执行但答错变为解析失败。以上是滚动观察，**不是粒度开关的因果收益**。

## 原始响应与重评审计

- 共80个条件×题×臂记录：48个有效候选、4个已派发生成失败、24个未派发、4个Gold阻断。
- 52次派发的原始响应hash和日志响应匹配。两个Run与各自冻结Manifest经canonical JSON比较一致，实际model和generation_contract一致。
- 对48个有效候选及4个已派发失败的SQL、EX、Contract和execution复核一致；另外4个Gold记录的核心评分与执行状态仍未评分。合计56个核心记录匹配。
- **发现replay状态缺陷：24个未派发的对照臂被native replay改成scored生成失败。** 报告坚持使用原始Pi状态：scored=false、EX/Contract=null、execution=pending；不接受重评伪造的失败。原生replay文件原样保留，不通过修改证据掩盖差异。
- Gold md-340的Forge compile投影另有not_applicable/pending差异，不计入上述核心评分/SQL/execution匹配声明。
- 原生compare拒绝完整比较，原因含unscored/diagnostic及unscored candidate。没有正式baseline/gain。
- 对照md-153、md-199的两臂均为空响应（assistant=[[]]、tool_arguments=null），日志仅有Incomplete model response。Pi丢失了Provider stopReason/errorMessage，无法证明是配额、限流或服务故障；off随后38次返回也不能倒推出根因。

## 工程验证与未关闭门禁

准备阶段：Python 813 passed / 26 skipped；Pi149 passed；typecheck通过；实际HTTP md-249双条件重评通过，旧v2 context被409拒绝；seed46的40个原候选在当前context下重评，原输出、SQL、EX、Contract不变。详细证据与原始Run、Manifest、native replay、逐臂审计在配套JSON中。

付费生成结束后，仓库目录曾返回EPERM，当时仅完成内存中冻结/实际Run的来源比较，并如实记录文件系统校验受阻。用户恢复权限后，已补齐两条件bird validate：均valid=true、model_calls=0，protocol_revision与各自冻结记录一致。没有重跑生成或修改原始候选。原始Run、日志、replay与run-integrity保留于.forge/benchmarks/luna-grain-context-20260906/；恢复校验回执见resumed-validation.json。

本轮隔离服务已通过确认PID后SIGTERM停止；PID 59606、59650、59607已退出，18767/18768不再监听。Supervisor报告退出失败是主动终止后的登记结果，不是新增模型调用失败。

本轮复核发现的两个维护问题已在下述零调用维护中修复；原付费候选、运行记录、JSON证据与本轮准确率结论不变。历史丢失的Provider错误信息不能补造。

## 后续维护验证（零新增模型调用）

- replay对output=null且显式scored=false的记录保留未评分及原执行/失败状态，不再把未派发臂计为模型错误。实际生成失败和Gold未知的原有语义保留。
- 新Pi raw_output.assistant以对象保存content、stopReason和errorMessage；不完整响应的日志包含可用的Provider错误详情。本地合成错误经真实Pi工作器和SQLite持久化验证，重开Run后仍可查询；没有调用真实Provider。
- 修复前两个回归反例均失败；修复后Python814 passed / 26 skipped、Pi150 passed及typecheck通过。
- 使用当前源码新冻结的diagnostic上下文实际重放80个原始臂：24个未派发臂恢复未评分，80臂的SQL、scored、EX、Contract和execution与原Pi一致，原output/raw_output不变。该一致性声明不包含历史Gold及生成失败的compile投影差异；原native replay文件保留作为修复前证据。
- 当前源码指纹已经变化，旧付费冻结不能续跑或冒充同版本基线。重放仍标记diagnostic、complete=false，compare拒绝；本次维护没有产生新准确率或成本收益。
- 维护证据：[benchmark-luna-grain-maintenance-2026-09-06.json](benchmark-luna-grain-maintenance-2026-09-06.json)。原付费JSON保留当时发现问题的状态，本节和维护证据记录后续修复。

## 滚动续测与交付状态

- 下一轮父Run：pbr_cb5bcb3341a24f619b7e8a75bbd83236（预先指定off）。
- 仅md-040两臂EX与Contract全部正确，其余19题继续沿用。下轮20题应为**19沿用 + 1新题 + 0复核**，按剩余槽位及当前比例取整；没有启动下一轮。
- Luna暴露累计49题、未见451题；本轮只有md-199是新暴露。
- 本简报与完整JSON已回写docs/benchmark-luna-grain-context-2026-09-06.{md,json}，同步current-project-state、主动计划、REQ-048与benchmarks中的已知重放限制。原会话附件保留当时权限阻断状态，准备证据和原始运行记录不变。本次恢复仅完成文档归档与零调用验证，保留用户已有修改；没有使用剩余24次授权补跑。
