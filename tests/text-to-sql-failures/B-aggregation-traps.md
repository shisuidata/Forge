# B. 聚合陷阱

## B1 — 多条件叠加的平均客单价

**自然语言 Prompt**
> 统计每个城市中，VIP 用户的平均客单价，只统计状态为"已完成"的订单

**核心陷阱**
多个过滤条件叠加时，WHERE / HAVING / JOIN ON 的位置容易混淆；过滤条件放错位置会影响聚合结果

**Expected SQL**
```sql
SELECT
  u.city,
  AVG(o.total_amount) AS avg_order_value
FROM users u
JOIN orders o ON o.user_id = u.id
WHERE u.is_vip = true
  AND o.status = 'completed'
GROUP BY u.city;
```

**AI 常见错误**
```sql
-- 错误：把 status 过滤放到 HAVING，语义变成"城市平均值符合条件"而非"只算符合条件的订单"
SELECT u.city, AVG(o.total_amount) AS avg_order_value
FROM users u
JOIN orders o ON o.user_id = u.id
WHERE u.is_vip = true
GROUP BY u.city
HAVING o.status = 'completed';  -- HAVING 中引用非聚合字段，多数引擎直接报错
```

**AQL 设计启示**
- 过滤条件需区分「行级过滤」（WHERE）和「聚合后过滤」（HAVING）
- DSL 应强制用户在 filter 层声明过滤时机

**测试记录**
| 模型 | 日期 | 是否正确 | 错误类型 |
|---|---|---|---|
| | | | |

---

## B2 — 高于自身历史均值的订单

**自然语言 Prompt**
> 找出每个用户中，订单金额高于该用户历史平均订单金额的订单

**核心陷阱**
需要在同一查询中同时使用明细级和聚合级数据，AI 经常搞混聚合层级，或者写出无法执行的 SQL

**Expected SQL**
```sql
-- 方案一：窗口函数
SELECT *
FROM (
  SELECT
    *,
    AVG(total_amount) OVER (PARTITION BY user_id) AS user_avg
  FROM orders
) t
WHERE total_amount > user_avg;

-- 方案二：子查询
SELECT o.*
FROM orders o
JOIN (
  SELECT user_id, AVG(total_amount) AS user_avg
  FROM orders
  GROUP BY user_id
) avg_table ON avg_table.user_id = o.user_id
WHERE o.total_amount > avg_table.user_avg;
```

**AI 常见错误**
```sql
-- 错误：直接在 WHERE 中嵌套聚合函数（SQL 不允许）
SELECT * FROM orders
WHERE total_amount > AVG(total_amount);
```

**AQL 设计启示**
- 「与自身聚合值比较」是高频场景，应作为 DSL 的原语支持
- 窗口函数 vs 子查询的选择应由编译器决定，不应暴露给 AI

**测试记录**
| 模型 | 日期 | 是否正确 | 错误类型 |
|---|---|---|---|
| | | | |
