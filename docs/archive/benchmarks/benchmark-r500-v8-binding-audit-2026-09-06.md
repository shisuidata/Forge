# R500回归收口与枚举绑定审计（2026-09-06）

## 决定

REQ-053离线调查完成，**新增被测模型调用0次**。

- 当前栈未误拒R500中的旧正确候选；已评分答案没有正确/错误转换。本轮不需要再改Compiler追分。
- 找到一个确有输入增量的候选：为`gasstations.Segment`提供带来源的精确分类值。当前两臂上下文缺少这些值，不是重复追加format/PK。
- 尚未修改Prompt或生成任何新答案；离线改字面量的反事实不算准确率提升。后续只建议一个单变量开发实验，不自动启动。

## 固定候选回归

来源为Structured GPT-5.6 Sol Run `pbr_6778bf9d34ae42fba0b070a5f9c154ba`的已保存报告，不是Luna新运行。原候选1000份、历史SQL1000份、Gold SQL500份hash均核对；当前源码及公开数据指纹在运行后不变。

使用既有`forge benchmark bird freeze/replay`、当前Compiler与Assurance v8，完整执行500题×2臂的诊断流程：

| 指标 | Forge | Direct SQL |
|---|---:|---:|
| EX正确 / 失败 / 未知 | 311 / 187 / 2 | 312 / 186 / 2 |
| Contract正确 / 失败 / 未知 | 282 / 216 / 2 | 292 / 206 / 2 |
| 候选执行成功 | 496 | 500 |
| 旧EX或Contract正确候选被前置门禁拒绝 | 0 | 0 |
| 已评分答案新增通过 / 正确回退 | 0 / 0 | 0 / 0 |

**不发布新的完整准确率百分比。** md-340、md-393两臂候选均执行成功，但Gold在当前30秒限制下超时：原先通过的四个判分改为未知，不是答案退步。保留500题分母，不增时限、不以缓存替代。

Freeze CLI exit0；Replay写完全部1000臂后因4臂未评分返回exit1、`complete=false`。这是完整诊断交付，但不是完整可晋级成绩。

### 全部变化项已审查

共14个臂有SQL、诊断字段或状态变化：

- md-006/md-119/md-339 Forge：原SQL仍分别缺少`consumption_rank/patient_count/comment_rank`；当前提前在Assurance拒绝。只读执行原SQL重现相同错误，非误拒。
- md-159 Forge：当前Compiler表达原候选的MIN(date)，由三行变为一行，但日期仍与Gold不同；EX/Contract继续失败。旧归因中“未选最早日期”的描述不能直接套到当前编译结果，不改写历史标签。
- md-340 Forge：增加MATERIALIZED；原/现SQL补充执行结果相同，不是相对历史基线新增可执行，Gold仍未知。
- md-109 Direct、md-319/459双臂：只是统一评价器补充`official_ea_mismatch`诊断，判分不变。
- md-405 Forge：原有同层`MAX(COUNT(*))`执行错误仍在；诊断归类变化，不是新回归。
- 其余变化为上述两个Gold超时题的未知状态。

### 可比性边界

历史基线未重跑Assurance，使用120秒诊断及已校验Gold缓存；本轮使用当前30秒只读执行。Compiler、上下文等经历多次维护，500个ResultContract均移除了未经确认的expected_grain。**差异不能全归因于v8，更不是模型增益。**

本轮Benchmark走`assure_compiled_sql`，不等于公共`assure_query`的全部JSON关系/意图前置规则。R500已参与调参，不证明独立泛化或真实业务静默错误率；原完整Pi请求未独立恢复，候选来源明确绑定保存报告。

## 枚举绑定机制

在看本轮回归结果前，选定md-009/md-026这一共同机制。

### 输入缺什么

- CSV的`Segment.value_description`为空，字段绑定正常；当前实际调用上下文构造入口，两臂只有`Segment text — chain segment`。
- 对公开SQLite以`mode=ro + query_only`完整检查5716行，得到5个精确值：`Discount`、`Noname`、`Other`、`Premium`、`Value for money`。
- 大小写敏感等值筛选`discount/premium`均匹配0行。相同拼写事实已见于历史人工诊断，但**当前模型输入没有它们**：新意在输入证据增量，不是发现新数据。

### 机制实证，不改实际成绩

| 原错误候选 | 原结果 | 仅替换一个字面量后的结果 |
|---|---|---|
| md-009 Direct：discount→Discount | NULL | 176 |
| md-026 Forge：premium→Premium | 0 | 35.68181818181818 |
| md-026 Direct：premium→Premium | 0 | 35.68181818181818 |

三个反事实都与本次新执行Gold相同，其余SQL字节不变；原md-009 Forge仍正确。**四份原候选实际仍为1/4正确**，没有“模型提升至4/4”的结论。

父执行器另实测四份原SQL均通过共享v8 SQL门禁，说明语法/字段合法不代表取值正确。合成SQLite证明`Premium`与`premium`可同时是不同合法类别，不能自动lower/LIKE合并。

当前完整两臂输入与R500冻结内容交叉核对一致。历史完整Prompt/CSV字节不可恢复，不能从snapshot hash推断原模型曾缺失还是忽略枚举；完整文本hash与语义snapshot hash也不能互相冒充。

## 下一候选，仅提案

建议只补充这一限定列的**观察取值证据**：标明数据库/列、来源hash、提取查询、覆盖与快照局限，双臂一致。它不是未来业务域的权威穷举，不授予在线探库权限，不改候选、Gold、评分或硬拒绝规则。

拟用md-009/md-026作目标题；md-002作同名不同表列对照（`customers.Segment=SME`），md-018作同表其他字段对照。两道对照在历史双臂EX/Contract均正确，选择不依赖本轮得分。

4题×2条件×2臂＝**16次生成**，包括任何canary、零重试；仅是未授权提案。先冻结实际模型/输入和成本时延门槛，再单独申请预算。小样本只决定是否值得继续，不据此发布总体收益。

本轮仅写调查证据和必要文档；没有源代码/Prompt/Schema改动，无新增永久测试或全套测试重跑。所有自建进程已退出；seed48与H/R0.6门禁保持不变。

[完整机器报告与21项工件校验和](benchmark-r500-v8-binding-audit-2026-09-06.json) · 原始工件（仅本地，未随仓库发布：`../.forge/benchmarks/r500-v8-binding-audit-20260906/`）
