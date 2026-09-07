# E. 方言陷阱

## E1 — 每个用户的最近一次订单

**自然语言 Prompt（MySQL 版）**
> 查询每个用户最近一次订单的详情（使用 MySQL）

**自然语言 Prompt（PostgreSQL 版）**
> 查询每个用户最近一次订单的详情（使用 PostgreSQL）

**核心陷阱**
MySQL 和 PostgreSQL 对「每组取一条」的写法有显著差异；AI 常混用，或生成在目标引擎上无法执行的 SQL

**Expected SQL（MySQL）**
```sql
-- 方案：子查询取 MAX 再 JOIN
SELECT o.*
FROM orders o
JOIN (
  SELECT user_id, MAX(created_at) AS last_order_time
  FROM orders
  GROUP BY user_id
) latest ON latest.user_id = o.user_id AND latest.last_order_time = o.created_at;
```

**Expected SQL（PostgreSQL）**
```sql
-- 方案一：DISTINCT ON（PG 独有）
SELECT DISTINCT ON (user_id) *
FROM orders
ORDER BY user_id, created_at DESC;

-- 方案二：窗口函数（跨方言通用）
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) AS rn
  FROM orders
) t WHERE rn = 1;
```

**AI 常见错误**
- 在 MySQL 中使用 `DISTINCT ON`（MySQL 不支持）
- 在 PG 中使用 `LIMIT 1` 子查询但忘记 correlated subquery 性能问题
- 方言未指定时随机选一种

**AQL 设计启示**
- 「每组取 TopN」是高频操作，应作为 DSL 原语：`top_n: { per: user_id, order_by: created_at DESC, n: 1 }`
- 编译器根据目标方言选择最优实现，AI 无需感知

**测试记录**
| 模型 | 目标方言 | 日期 | 是否正确 | 错误类型 |
|---|---|---|---|---|
| | MySQL | | | |
| | PostgreSQL | | | |

---

## E2 — 品类销售额及占比

**自然语言 Prompt**
> 统计各商品品类的销售额，并计算每个品类占总销售额的百分比

**核心陷阱**
需要窗口函数 `SUM() OVER ()`；AI 常漏写 `OVER`，或错误地用子查询导致性能差，或 `OVER()` 括号内写了错误的 PARTITION

**Expected SQL**
```sql
SELECT
  p.category,
  SUM(oi.quantity * oi.unit_price) AS category_revenue,
  ROUND(
    SUM(oi.quantity * oi.unit_price) * 100.0
    / SUM(SUM(oi.quantity * oi.unit_price)) OVER (),
    2
  ) AS pct_of_total
FROM order_items oi
JOIN products p ON p.id = oi.product_id
GROUP BY p.category
ORDER BY category_revenue DESC;
```

**AI 常见错误**
```sql
-- 错误一：漏写 OVER()，变成普通聚合，报错
SUM(SUM(...)) -- 嵌套聚合不合法

-- 错误二：OVER() 里加了 PARTITION BY category，变成自身除以自身，结果全是 100%
/ SUM(...) OVER (PARTITION BY p.category)
```

**AQL 设计启示**
- 「占比」计算是窗口函数最常见的误用场景，应封装为 DSL 的 `percent_of_total` 聚合类型
- 嵌套聚合的合法性校验应在 DSL 编译阶段完成，而非等到 SQL 执行报错

**测试记录**
| 模型 | 日期 | 是否正确 | 错误类型 |
|---|---|---|---|
| | | | |
