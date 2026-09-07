# Luna 混合续测简报 · seed46 · 2026-09-06

## 结论

本轮完成38次生成与原候选复核。17道延续题仍各有至少一臂失败或未知；Forge md-249恢复EX/Contract，Direct md-082回退Contract、md-429回退EX。Gold阻塞仍保留，本轮不是完整准确率基线，也不是优化收益证据。

## 范围与消耗

- 模型：openai-codex/gpt-5.6-luna；Run：pbr_cf21f96d67b9484f97c4157357d5230d。
- 用户“继续吧”授权恢复一轮滚动测试；20题＝17道seed45失败/未知延续＋2道Luna新题＋1道已测题，seed46。余3位按70%/30%最近整数分配为2/1。
- 新题：md-044、md-222；已测题：md-034。历史曝光包括历次Luna记录和日期专项两条件；专项实验不替代seed45这个滚动父轮。
- Gold md-340零生成、保留分母；预算38次，实际38次Pi派发、38份候选，无生成失败、补跑或替换；Provider重试策略为0。
- 运行约124.9秒，不含预检；已报告136,237 tokens，无缺失usage。未独立观测Provider HTTP请求数；tokens不是结算账单。
- 日期上下文保持off。本轮未修改Prompt、Compiler、Schema、Gold、评分或超时；未经业务确认的expected_grain也未在此轮移除。

## 成绩（完整20题分母）

| 指标 | Forge JSON | Direct SQL |
|---|---:|---:|
| 官方EX：正确 / 失败 / 未评分 | 6 / 13 / 1 | 4 / 15 / 1 |
| Contract：正确 / 失败 / 未评分 | 3 / 16 / 1 | 2 / 17 / 1 |
| 可生成题候选返回 | 19/19 | 19/19 |
| 可生成题SQL执行成功 | 18/19 | 19/19 |
| 已报告tokens | 88,102 | 48,135 |

Gold未知不算模型答错；完整准确率为null。运行状态failed来自预声明的Gold跳过导致诊断不完整，不是缺失生成。失败题富集且样本改变，不用整轮总数与前轮直接比较收益。

## 分层与相邻轮变化

| 题组 | Forge EX / Contract通过 | Direct EX / Contract通过 |
|---|---:|---:|
| 延续17题（含1题Gold未知） | 4 / 1 | 2 / 0 |
| 新题2题 | 1 / 1 | 1 / 1 |
| 已测1题 | 1 / 1 | 1 / 1 |

- 同一16道可评分延续题，上轮→本轮EX为Forge 3→4、Direct 3→2；Contract为Forge 0→1、Direct 1→0。
- Forge md-249恢复EX/Contract，但Direct仍失败，因此这道题仍须延续。
- Direct md-082从EX/Contract都通过变为仅EX通过；md-429的EX从通过变为失败，Contract继续失败。
- Forge md-447从Assurance字段作用域拒绝恢复为可执行，结果仍不匹配；md-483从可执行退为SQL解析拒绝。后者原始select表达式引号不闭合，且两臂仍使用YYMMDD字面量；未修补候选。
- 新md-044、复测md-034均双臂EX/Contract通过；新md-222均失败。md-222 Forge原候选返回constructorId而非题目所问的数量，Direct虽返回COUNT仍未通过官方评分；不凭这一简报直接归因为Gold错误。
- md-020、md-082两臂及md-429 Forge本轮EX通过而Contract失败，不能视为完整结果正确。

## 可比边界与验证

17道共享题的Forge/Direct提示词、Schema Context及Context Snapshot与seed45相同；模型快照、生成契约、Python运行版本及全部数据指纹相同。但中间已实施日期上下文机制，冻结协议由v1升为v2，forge/benchmark_metadata.py、forge/bird_benchmark.py、web/routes/benchmark_v2.py源码指纹变化。因此只报告共同题的观察转移，不声称跨轮源码完全相同或构成正式配对优化证据。

- 实际CLI freeze/preflight通过；运行后validate通过，Pi使用同一冻结manifest/revision。
- 38份原始响应hash及generation.completed日志一致；40条臂的评分状态、EX、Contract、SQL和执行状态与离线重放一致；505条日志完整封存。
- 保留一项非评分投影差异：md-340 Forge的compile_status在Pi为not_applicable、replay为pending。两处都是零派发、跳过执行、SQL/EX/Contract为null；未静默抹平差异，也未在此轮改代码。
- replay因评分不完整以1退出；当前重放记录即使与自身比较，compare也拒绝不完整基线。两者均零模型调用。
- 此轮无源码修改，未重跑完整单元测试；证明来自实际生成、原候选重放和指纹核对。临时Pi/Forge服务已停止。

## 下一轮

按既定规则为**18道延续（含Gold阻塞）＋1道Luna新题＋1道已测题**：本轮17道延续全部保留，再加入md-222。余2位按70%/30%最近整数为1/1；不是改变抽样比例。Luna累计曝光48题，尚有452题未测。本轮未自动启动下一轮。

未达到日期上下文推广门槛的决定不变；本轮也没有实施expected_grain移除。任何单变量改动须独立冻结并与新基线比较，不能将本轮再生成波动算作修复收益；H与R0.6外部采用门禁不变。

[完整证据](benchmark-luna-mixed-d20-seed46-2026-09-06.json)；原始运行、日志、冻结和重放：.forge/benchmarks/luna-mixed-d20-seed46-20260906/。
