# Luna 混合续测简报 · seed48 · 2026-09-06

## 结论

本轮在32／38次派发后失败终止，留下30个有效候选；6次未派发，不补跑。唯一新题md-313双臂全过，19道延续题没有双臂完整恢复。Direct md-429恢复EX，但md-259回退EX/Contract。Gold阻塞、未派发和未知用量保留；不构成完整准确率基线或优化收益证据。

## 范围与消耗

- 模型：openai-codex/gpt-5.6-luna；Run：pbr_f24226875eaf4a56b5fb560d61b94dc2。
- 父轮：REQ-048预先指定的off Run pbr_cb5bcb3341a24f619b7e8a75bbd83236。汇入全部53份不同run_id的Luna原始历史，冻结前已曝光49题、未曝光451题。
- 20题＝19道失败/未知延续＋1道Luna新题md-313＋0道复核，seed48。余1位按既定70%/30%四舍五入为1/0，不改变比例。date_context与grain_context均off。
- 用户明确选择“执行本轮38次”。Gold md-340预声明零生成、留分母；不挪用上轮未派发的24次预算。本轮实际32次Pi派发、30份候选、2次生成失败、6次预算内调用未派发，无重试、补跑或替换。
- 运行约173.4秒，不含预检；已观察107,160 tokens，另2个Forge臂用量未知，不能补零或计算完整成本。未独立逐笔观测Provider HTTP请求数，tokens不是结算账单。

## 成绩（完整20题分母）

| 指标 | Forge JSON | Direct SQL |
|---|---:|---:|
| 官方EX：正确 / 失败 / 未评分 | 4 / 12 / 4 | 4 / 12 / 4 |
| Contract：正确 / 失败 / 未评分 | 1 / 15 / 4 | 1 / 15 / 4 |
| 已派发臂候选返回 | 14/16 | 16/16 |
| 已派发臂SQL执行成功 | 12/16 | 16/16 |
| 已观察tokens | 64,085 | 43,075 |
| 缺失usage臂 | 2 | 0 |

生成失败计失败；Gold阻塞和未派发计未评分。4道未评分为md-340、md-472、md-481、md-483，不缩为16题发布准确率。Forge另外两份已返回候选md-032/md-199被Assurance字段引用校验拒绝。

## 终止证据

1. md-234 Forge达到120秒生成上限。最后留存进度为至少6,250个流事件，但没有完整assistant消息或最终候选；流事件数不是tokens，不能认定为无Provider响应。
2. Run失败收敛会取消在途controller；随后md-447 Forge记录stopReason=aborted、errorMessage=Request was aborted及部分assistant内容。不是将两臂都解释为独立120秒超时。
3. md-472、md-481、md-483未启动；md-340始终零派发。未延长上限或恢复运行。

## 逐题结果

表格每格依次为EX / Contract；“错”保留原评分，不代表已经排除参考或业务口径冲突。

| 题目 | Forge | Direct | 说明 |
|---|---|---|---|
| md-020 | 过 / 错 | 过 / 错 | 两臂仅EX通过，Contract失败。 |
| md-032 | 错 / 错 | 错 / 错 | Forge被Assurance字段引用校验拒绝；Direct结果不符。 |
| md-082 | 过 / 错 | 过 / 错 | 两臂仅EX通过，Contract失败。 |
| md-127 | 错 / 错 | 错 / 错 | 双臂EX/Contract均未通过；保留原候选，不修补评分。 |
| md-153 | 错 / 错 | 错 / 错 | 双臂EX/Contract均未通过；保留原候选，不修补评分。 |
| md-199 | 错 / 错 | 错 / 错 | Forge引用CTE未输出的best_ms；Direct结果列数不符。旧固定Forge候选经REQ-049已全过，不能与本轮新生成混淆。 |
| md-222 | 错 / 错 | 错 / 错 | 双臂EX/Contract均未通过；保留原候选，不修补评分。 |
| md-234 | 错 / 错 | 错 / 错 | Forge到达120秒生成超时，未得到最终候选；Direct候选评分失败。 |
| md-249 | 错 / 错 | 错 / 错 | 双臂EX/Contract均未通过；保留原候选，不修补评分。 |
| md-259 | 错 / 错 | 错 / 错 | Direct相对父Run从EX/Contract均过回退为均失败。 |
| md-273 | 错 / 错 | 错 / 错 | 双臂EX/Contract均未通过；保留原候选，不修补评分。 |
| md-300 | 错 / 错 | 错 / 错 | 双臂EX/Contract均未通过；保留原候选，不修补评分。 |
| md-313 | 过 / 过 | 过 / 过 | 唯一Luna新题，双臂EX/Contract全过。 |
| md-340 | 未知 / 未知 | 未知 / 未知 | 预声明Gold execution_timeout；留分母，两臂零派发、未评分。 |
| md-418 | 错 / 错 | 错 / 错 | 双臂EX/Contract均未通过；保留原候选，不修补评分。 |
| md-429 | 过 / 错 | 过 / 错 | Direct EX由失败恢复；两臂Contract仍失败。 |
| md-447 | 错 / 错 | 错 / 错 | Forge在本轮失败收敛时被取消，记录aborted / Request was aborted；Direct评分失败。 |
| md-472 | 未知 / 未知 | 未知 / 未知 | Run终止前未启动，两臂零派发、未评分。 |
| md-481 | 未知 / 未知 | 未知 / 未知 | Run终止前未启动，两臂零派发、未评分。 |
| md-483 | 未知 / 未知 | 未知 / 未知 | Run终止前未启动，两臂零派发、未评分。 |

## 相邻轮变化与可比边界

- 同一15道本轮已评分延续题，相对父Run原始成绩：Forge EX 3→3、Contract 0→0；Direct EX 3→3、Contract 1→0。
- Direct md-429由EX失败恢复为通过，但Contract仍失败；md-259由EX/Contract都过回退为都错。其余延续题没有新增EX/Contract通过；未启动的3题单列为未知，不算回退成模型答错。
- md-199旧Forge候选在REQ-049同候选修复后EX/Contract均通过；本轮重新生成的CTE未输出best_ms却在JOIN中引用，被Assurance拒绝。这是新旧候选差异，不是已修复的物化路径再次超时，也不能抹去旧候选的确定性修复证据。
- 19道共享题的两臂指令、Schema和Context Snapshot hash均相同，模型相同；Compiler、重放维护及Pi错误留存使跨轮源码/运行时指纹不同。上述仅为滚动观察，不是正式配对或模型能力趋势。

## 生成前协议维护与验证

首次提交在生成前暴露Python/JavaScript序列化差异：冗余派生字段selection.remaining_new_fraction在Python为1.0，经JavaScript转为1，导致冻结hash不一致。Pi首次POST返回500，内部protocol返回409，未创建Run、零模型调用。

从新冻结中删除这个无人消费的派生比例字段，保留整数计数和case IDs，不改抽样算法。重新冻结仅改变该字段及对应源码hash；题目、上下文、模型与38次预算不变。真实HTTP协议往返转为200；唯一成功创建Run的POST为202，revision为sha256:7f0cd56f430e4ac0c5cf86e27e0446211e9f7d9e41463111b5d9988014113feb。旧冻结和失败回执不覆盖。

- 回归反例修复前409失败，修复后通过；Python全量819 passed / 26 skipped，Pi150 passed，typecheck通过。没有追加无关功能或修改Prompt、Gold、评分、生成超时。
- 32份留存raw_output记录（包括空/部分响应）的hash均可重算；32条dispatch日志、32条有效payload日志及446条总日志封存。不是32份完整模型回答。
- 40条臂的原output/raw_output不变；评分状态、EX、Contract、SQL、execution_status均与原Pi一致。离线重放零模型调用。
- 保留非评分差异：md-340 Forge的compile_status为Pi not_applicable / replay pending；两个真实生成失败臂在候选重评中归为generation_empty、compile failed，而Pi原记录为agent_failed、compile pending。具体超时、取消及不可重试原因以原Run和日志为准，不覆盖原始失败信息。
- 另外独立只读执行16条Gold与28条可执行候选SQL，按BIRD原始tuple-set核心比较，全部与Pi官方EX一致；两份Assurance拒绝不绕过校验执行。
- 运行后freeze validate通过；replay因不完整以1退出，compare即使自比也拒绝未评分记录。隔离Pi/Forge服务已停止，18767/18768端口无监听。

## 下一步

按规则仍为19道延续＋1道Luna新题＋0复核；Luna累计实际曝光50题、未曝光450题。未自动启动下一轮，未将本轮未派发的6次或上轮24次预算滚存。

优先离线分析生成超时与持续候选错误；如需改变生成上限、重试或继续策略，须作为独立行为变更冻结评估，不能在当前失败Run中静默补齐。当前结果不支持准确率或成本提升结论，H与R0.6外部采用门禁不变。

[完整证据](benchmark-luna-mixed-d20-seed48-2026-09-06.json)；原始运行、日志、双冻结、授权和重放：.forge/benchmarks/luna-mixed-d20-seed48-20260906/。
