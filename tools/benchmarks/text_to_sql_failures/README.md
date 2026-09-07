# Text-to-SQL 失败案例测试集

用于验证 AI 在 SQL 生成上的典型失败模式，是 AQL DSL 设计的靶心。

## 测试 Schema

```sql
users        (id, name, city, created_at, is_vip)
orders       (id, user_id, status, total_amount, created_at)
order_items  (id, order_id, product_id, quantity, unit_price)
products     (id, name, category, cost_price)
```

## 失败模式分类

| 分类 | 文件 | 核心陷阱 |
|---|---|---|
| A. JOIN 陷阱 | `A-join-traps.md` | INNER vs LEFT、NULL 语义 |
| B. 聚合陷阱 | `B-aggregation-traps.md` | GROUP BY/HAVING 混用、窗口函数 |
| C. 时间陷阱 | `C-time-traps.md` | 日期序列填充、同比计算 |
| D. 语义陷阱 | `D-semantic-traps.md` | 业务指标定义歧义 |
| E. 方言陷阱 | `E-dialect-traps.md` | MySQL vs PostgreSQL 写法差异 |

## 使用方式

1. 用自然语言 prompt 问 LLM（GPT-4 / Claude）
2. 记录生成的 SQL 到对应文件的 `ai_output` 字段
3. 对比 `expected_sql`，分析错误类型
4. 用结果指导 AQL DSL 的设计决策
