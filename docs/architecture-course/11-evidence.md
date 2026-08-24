---
title: 11｜基准、证据与诚实边界
summary: 正确阅读 120/120、Spider2 与兼容性证据
---

# 基准、证据与诚实边界

## 1. 四个常用指标

| 指标 | 含义 | 回答的问题 |
|---|---|---|
| Case EA(any) | 一题多次运行，至少一次执行结果正确 | 模型“有时能不能做对” |
| Case EA(all) | 一题多次运行，每次都正确 | 用例级稳定性 |
| Run ACC | 全部运行中正确次数占比 | 总体重复执行可靠性 |
| 编译失败率 | 生成无法通过 Schema/Compiler 的比例 | 结构生成稳定性 |

只报告 EA(any) 会隐藏不稳定性；只报告编译成功会把“能生成 SQL”误当成“答案正确”。

## 2. 当前推荐证据卡

**Method AI，2026-07-13**：

- 数据集：large 电商数仓 40 题；
- Provider/模型：火山方舟 Coding Plan，`ark-code-latest`；
- 运行：40 × 3，最多 2 轮 JSON/compile/lint 修正；
- Case EA(any)：40/40；
- Case EA(all)：40/40；
- Run ACC：120/120；
- 生成/编译失败：0/120。

原始说明见[测试报告](https://github.com/shisuidata/Forge/blob/main/docs/test-report-2026-07-13.md)。

**正确解读**：当前代码、模型、Provider、Registry、规则和 40 题数据集组合在该次实验中全部通过。

**错误解读**：Forge 对任意客户、任意陌生 Schema、任意问题都是 100%。

## 3. 为什么需要客户域 accuracy suite

业务题的难点与客户 Schema、指标口径、数据质量和常用表达强相关。每个 PoC 应建立 30—100 条 golden questions：

- P0：核心财务/经营指标；
- P1：高频明细、排名、趋势、漏斗；
- P2：长尾探索；
- 每题保存 reference SQL、预期列、结果或等价判断；
- 每次 Registry、lint、prompt、Compiler 或模型变更后回归。

## 4. 自有业务题与 Spider2-Lite

Spider2-Lite SQLite 子集历史结果：编译成功率 97.6%，EA 9.2%。其大量题目涉及日期序列、复杂同比、多层嵌套、自关联和统计模式，与 Forge 的日常业务查询目标不同。

这组低分不是应该隐藏的数字，它说明：

- DSL 可以表达/编译很多结构，不代表模型选择了正确算法；
- 自有 40 题高分不能外推到学术复杂查询；
- 需要明确 refusal、澄清或转人工的能力边界。

## 5. 弱模型与强模型

历史实验中，中等模型在部分场景从 DSL 获得明显收益；强模型与直接 SQL 的差距可能接近持平。可能原因是强模型本身减少了语法错误。

因此技术主张应调整为：

- DSL 对弱模型、私有模型和高风险结构仍有价值；
- Registry 对任何模型都重要，因为它承载私有业务知识；
- 审核、权限和审计对任何模型都重要；
- 应持续做同模型、同 Provider、同数据集的公平 A/B。

## 6. 证据层级

```text
代码存在
  < mock/单元测试
  < CI/真实 smoke
  < 客户 accuracy suite
  < 受控生产运行
  < 多客户、跨版本长期证据
```

“implemented”不能自动升级为“production_verified”。每条对外兼容声明都应能回答：在哪个版本、哪个环境、用什么测试验证？

## 7. 易变数字治理

教材只集中维护证据卡，详细分类与历史演化链接到：

- [基准测试总览](https://github.com/shisuidata/Forge/blob/main/docs/benchmarks.md)
- [最新 Method AI 报告](https://github.com/shisuidata/Forge/blob/main/docs/test-report-2026-07-13.md)
- [兼容性矩阵](https://github.com/shisuidata/Forge/blob/main/docs/compatibility-matrix.md)

更新基准时，先更新原始报告，再更新证据卡和 README，避免多处数字漂移。
