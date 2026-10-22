---
title: 基准测试
description: BIRD 500 题固定候选重放、成本与边界，以及历史自有用例和 Spider2-Lite 参考
---

## 先看结论：运行时价值不等于准确率优势

Forge 的产品主线是 **Evaluate → Enforce → Explain**：评估候选、约束执行、解释同一 QueryRun 的证据。Direct SQL 与 Forge JSON 是可选候选格式；公开实验没有证明 Forge JSON 具有总体准确率优势，更不能把自有小样本的 100% 外推为陌生 Schema 或生产环境的保证。

### 历史 BIRD Mini-Dev：500 题，同候选 Compiler replay

以下为 GPT-5.6 完整配对实验的历史记录，不是本轮网站更新的新测试。原始生成结果与后续确定性 Compiler 离线重评分别列出，避免把修复后的重放误称为一次新生成。

| 阶段 | Forge Official EA | Direct SQL Official EA | 证据边界 |
|---|---:|---:|---|
| Structured 候选的原始运行封存 | 287/500（57.4%） | 314/500（62.8%） | 保留原始运行判定 |
| 同一 Structured 候选，最终 Compiler replay | **313/500（62.6%）** | **314/500（62.8%）** | 固定候选离线重评，未重新调用模型生成 |

最终配对中 Forge-only 22 题、Direct-only 23 题，双侧 exact McNemar/binomial **p = 1.0**；差异为 -0.2 个百分点，未显示显著差异，**不支持 Forge 更准确的结论**。

成本也必须同时报告：原始 Structured 生成中，Forge 相比 Direct SQL 使用 **多 59.57% 的 tokens**、**多 51.69% 的平均生成时间**。这些是原始生成成本，不是离线 Compiler replay 的耗时。

结论只适用于该固定 GPT-5.6 revision、Oracle Evidence、Structured Prompt/Schema、最终 Compiler 与一次生成策略。后续比较器修正后的 Contract 为 284/500 对 294/500；Contract 是独立指标，不能替换或改写上面的 Official EA。完整方法、版本和原始成绩见[仓库基准报告](https://github.com/shisuidata/Forge/blob/main/docs/benchmarks.md)。

### 工程测试不是准确率评测，也不是生产认证

公开证据：[最新公开测试摘要](https://github.com/shisuidata/Forge/blob/main/docs/archive/engineering/latest-test-report-2026-10-22.md)记录了 **2026-10-22** 对公开 `main` 提交 `e50b0a0` 的验证：Python **1049 passed / 26 skipped**（仓库检出），Pi **226 passed**，远端 CI 五项通过，wheel 与 sdist 全新安装通过。它不是对之后的提交、Release 或当前部署的认证。

2026-10-04 评估所列两项 **P0 安全发布阻断**（Direct SQL 授权检查、Pi 跨主体事件去重隔离）已修复并有回归测试；[历史摘要](https://github.com/shisuidata/Forge/blob/main/docs/archive/engineering/latest-test-report-2026-10-14.md)保留当时的发现。真实数据库只读角色、真实身份与独立安全审核仍未验证，不能以测试总数宣称生产就绪、真实客户采用或共享多租户安全。

---

## 历史参考：自有 40 题与 Spider2-Lite

以下完整保留本站原有实验成绩，供了解探索过程，**不是当前统一排行榜**。各实验模型、数据规模、运行次数、重试策略和指标不同，不能跨表直接排名；LLM Judge、EA、Case EA(any/all) 与 Run ACC 也不能互换。历史模型画像与选型建议只适用于对应实验，不作为当前通用采购或部署建议。

### 历史得分总览

| 基准 | 题数 | 指标 | 得分 |
|---|---|---|---|
| 自有用例（Method AI，Ark Coding Plan，large，3 runs） | 40 | Run ACC | **100.0%** |
| 自有用例（Method AF，DeepSeek V4 Pro，large，3 runs） | 40 | Run ACC | **97.5%** |
| 自有用例（Method AF，DeepSeek V4 Pro，large，3 runs） | 40 | Case EA(all) | **92.5%** |
| 自有用例（Method M，Claude，small） | 40 | Execution Accuracy | **95.0%** |
| 自有用例（Method O，DeepSeek V3，small） | 40 | EA | **95.0%** |
| 自有用例（Method J+Sem，Claude） | 40 | LLM Judge | **8.82 / 10** |
| 自有用例（Method R，M2.7，large，retry=2） | 40 | EA | **72.5%** |
| 自有用例（Method N，DeepSeek V3，large） | 40 | EA | **65.0%** |
| Spider2-Lite SQLite | 123 | EA | **9.2%** |
| Spider2-Lite SQLite | 123 | 编译成功率 | **97.6%** |

---

## 历史版本演化（LLM 评分 0-10）

| 版本 | 核心改动 | LLM 评分 | 编译失败率 |
|---|---|---|---|
| **A** | 基线（SQL 风格 DSL） | 7.63 | 3.8% |
| **B** | 对照组：直接生成 SQL | 8.38 | 0.0% |
| **D** | JSON DSL + Structured Output | 8.46 | 1.2% |
| **G** | 数量词语义优化 | 8.69 | 0.0% |
| **J** | HAVING 精准化 + 人均模式 | 8.65 | 0.5% |
| **J+Sem** | J + 运行时语义消歧库 | **8.82** | **0.0%** |

---

## 历史 Forge DSL vs 直接 SQL（跨模型 EA 对比）

### MiniMax-M2.5（中等能力模型）

| 方法 | EA | 正确题数 |
|---|---|---|
| **Forge (DSL)** | **65.0%** | 26/40 |
| 直接 SQL | 57.5% | 23/40 |

### Forge J+Sem vs 直接 SQL（Claude Sonnet，LLM Judge）

| 分类 | 直接 SQL | Forge J+Sem | 差值 |
|---|---|---|---|
| ANTI/SEMI JOIN | 7.80 | **8.60** | **+0.80** |
| 排名 & TopN | 8.36 | **9.00** | +0.64 |
| 时序导航 | 8.40 | **9.00** | +0.60 |
| **总体** | **8.38** | **8.82** | **+0.44** |

在这组历史样本中，ANTI/SEMI JOIN 差距最大：直接生成 SQL 出现 `NOT IN` 的 NULL 语义错误，Forge 的 `anti` join 用于约束这一类翻译。该局部结果不能证明所有生成错误已被消除。

---

## 历史四强横评

测试环境：large 数据集，40 题，200 张表电商数仓。

### 总体得分

| Method | 模型 | Case EA (any) | Run ACC |
|---|---|---|---|
| **R** | MiniMax M2.7 | **72.5%** | 54.2% |
| **N** | DeepSeek V3.2 | **65.0%** | **58.3%** |
| **T** | Claude Sonnet 4.6 | 57.5% | 57.5% |
| **L** | MiniMax M2.5 | 52.5% | 41.7% |

> Case EA (any)：至少 1 次运行正确的题数比例（能力上限）。Run ACC：全部运行中正确的比例（稳定性）。

### 模型画像

- **DeepSeek V3.2**：综合最强。各类别均衡，综合复杂查询唯一非零，稳定性最好（损耗仅 6.7pp）
- **MiniMax M2.7**：EA 最高。较 M2.5 提升 12.5pp，进步集中在 JOIN/窗口/时序/ANTI 类
- **Claude Sonnet 4.6**：基础扎实。HAVING/JOIN 类第一，弱点在 TopN 和时序

### 选型建议

| 场景 | 推荐 |
|---|---|
| 追求最高准确率 | **DeepSeek V3.2** |
| 成本敏感 / 中等复杂度 | **MiniMax M2.7** |
| 隐私合规 / 私有部署 | **Claude Sonnet 4.6** |

## 历史商业化基线（2026-07-13）

2026-07-13 的商业化 readiness 分支进一步把评测口径拉到真实 provider smoke 和重复运行稳定性：

| Method | Provider / Model | Case EA(any) | Case EA(all) | Run ACC | 编译失败 |
|---|---|---:|---:|---:|---:|
| Method AF | DeepSeek V4 Pro | 40/40 | 37/40 | 117/120 | 0/120 |
| Method AI | 火山方舟 Coding Plan / `ark-code-latest` | 40/40 | 40/40 | 120/120 | 0/120 |

这两组结果不能直接推导出“任意陌生 Schema 100% 准确”。商业交付仍要按客户域运行 `schema sync -> golden questions -> accuracy run -> failure triage -> Registry 修正 -> 回归测试`。

---

## 历史 Spider2-Lite

| 指标 | 值 |
|---|---|
| 测试用例 | 123 个 SQLite 用例 |
| 编译成功率 | **97.6%** |
| EA | **9.2%** |

EA 低说明 Spider2 的日期序列生成、复杂自关联、统计建模等模式与 Forge 当前目标范围存在明显差异。它也是重要的泛化边界证据：编译成功不代表算法和执行结果正确。客户覆盖率必须通过客户自己的 golden questions 测量，不能从该基准外推。

---

> 本页为精简版。完整内容参见 [docs/benchmarks.md](https://github.com/shisuidata/Forge/blob/main/docs/benchmarks.md)。先体验运行时可前往[五分钟上手](/guides/quickstart/)；产品边界见[Forge 介绍](/guides/introduction/)。
