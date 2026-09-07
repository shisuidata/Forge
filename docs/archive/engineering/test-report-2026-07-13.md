# Forge 测试报告（2026-07-13）

## 结论

本轮 Method AI 在 large 40 题、每题 3 次的 Execution Accuracy 测试中达到
`120/120`，Run ACC 为 `100.0%`，生成/编译失败率为 `0.0%`。

相对同一 Ark Coding Plan Provider 的 Method AH 首轮 `113/120（94.2%）`，提升
`5.8` 个百分点；相对历史交付基线 Method AF 的 `117/120（97.5%）`，提升
`2.5` 个百分点。

## 环境

- 数据集：`tests/datasets/large/cases.json`
- 数据库：SQLite `tests/datasets/large/database.db`
- Provider：火山方舟 Coding Plan（OpenAI-compatible）
- 模型：`ark-code-latest`
- 运行次数：40 题 × 3 runs
- 重试：每次最多 2 轮 JSON/compile/lint 修正

API Key 仅通过进程环境变量传入，未写入仓库、配置文件或测试结果。

## 结果

| 指标 | Method AH | Method AI |
|---|---:|---:|
| Case EA(any) | 39/40（97.5%） | 40/40（100.0%） |
| Case EA(all) | 35/40（87.5%） | 40/40（100.0%） |
| Run ACC | 113/120（94.2%） | 120/120（100.0%） |
| 生成/编译失败 | 2/120（1.7%） | 0/120（0.0%） |

Method AI 的 8 个查询类别均为 `15/15`。

## 有效改动

1. 编译器不再通过外层 `SELECT *` 泄漏 `qualify` 使用的内部排名列。
2. lint 固定三类明细查询的最小结果列，模型可在重试中按明确反馈自修正。
3. 品类占比分母按用户看到的 `category_name` 汇总，避免同名品类按内部 ID 拆分。
4. OpenAI-compatible 基准输出上限由 4096 提升到 8192，复杂 JSON 未再最终截断。

## 边界

- Method AH 与 Method AI 是同 Provider、同模型、同数据集对比。
- Method AF 使用 DeepSeek V4 Pro；其账户在本轮全量复测到 108/120 时返回 HTTP 402，
  因此 Method AF 与 Method AI 不是严格同模型 A/B。
- 该结果证明当前 40 题业务基准的可重复执行准确率，不代表任意陌生 Schema 均为 100%。
