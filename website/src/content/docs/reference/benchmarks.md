---
title: 基准测试
description: Forge 在自有用例和 Spider2-Lite 上的准确率评测
---

## 当前得分总览

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

## 版本演化（LLM 评分 0-10）

| 版本 | 核心改动 | LLM 评分 | 编译失败率 |
|---|---|---|---|
| **A** | 基线（SQL 风格 DSL） | 7.63 | 3.8% |
| **B** | 对照组：直接生成 SQL | 8.38 | 0.0% |
| **D** | JSON DSL + Structured Output | 8.46 | 1.2% |
| **G** | 数量词语义优化 | 8.69 | 0.0% |
| **J** | HAVING 精准化 + 人均模式 | 8.65 | 0.5% |
| **J+Sem** | J + 运行时语义消歧库 | **8.82** | **0.0%** |

---

## Forge DSL vs 直接 SQL（跨模型 EA 对比）

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

ANTI/SEMI JOIN 差距最大：直接生成 SQL 频繁产生 `NOT IN`，遇到 NULL 时静默返回错误结果；Forge 的 `anti` join 从根源消灭了这类错误。

---

## 四强横评

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

## 当前商业化基线

2026-07-13 的商业化 readiness 分支进一步把评测口径拉到真实 provider smoke 和重复运行稳定性：

| Method | Provider / Model | Case EA(any) | Case EA(all) | Run ACC | 编译失败 |
|---|---|---:|---:|---:|---:|
| Method AF | DeepSeek V4 Pro | 40/40 | 37/40 | 117/120 | 0/120 |
| Method AI | 火山方舟 Coding Plan / `ark-code-latest` | 40/40 | 40/40 | 120/120 | 0/120 |

这两组结果不能直接推导出“任意陌生 Schema 100% 准确”。商业交付仍要按客户域运行 `schema sync -> golden questions -> accuracy run -> failure triage -> Registry 修正 -> 回归测试`。

---

## Spider2-Lite

| 指标 | 值 |
|---|---|
| 测试用例 | 123 个 SQLite 用例 |
| 编译成功率 | **97.6%** |
| EA | **9.2%** |

EA 低是因为 Spider2 的查询分布与 Forge 的设计目标存在系统性错位——日期序列生成、复杂自关联、统计建模等属于「算法逻辑错误」，超出 Forge 能力边界。在真实企业场景中，超过 80% 的日常查询落在 Forge DSL 覆盖范围内。

---

> 本页为精简版。完整内容参见 [docs/benchmarks.md](https://github.com/shisuidata/Forge/blob/main/docs/benchmarks.md)。
