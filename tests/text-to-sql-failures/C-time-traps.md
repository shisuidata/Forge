# C. 时间陷阱

## C1 — 连续日期序列（含零值填充）

**自然语言 Prompt**
> 统计最近 30 天每天的新增用户数，没有新用户注册的日期也要显示，值为 0

**核心陷阱**
需要生成连续日期序列与数据做 LEFT JOIN；AI 几乎必然忽略日期填充，只返回有数据的日期

**Expected SQL（PostgreSQL）**
```sql
SELECT
  d.date,
  COUNT(u.id) AS new_users
FROM generate_series(
  CURRENT_DATE - INTERVAL '29 days',
  CURRENT_DATE,
  INTERVAL '1 day'
) AS d(date)
LEFT JOIN users u ON DATE(u.created_at) = d.date
GROUP BY d.date
ORDER BY d.date;
```

**Expected SQL（MySQL）**
```sql
-- MySQL 无 generate_series，需要用数字表或递归 CTE 生成日期序列
WITH RECURSIVE dates AS (
  SELECT CURDATE() - INTERVAL 29 DAY AS date
  UNION ALL
  SELECT date + INTERVAL 1 DAY FROM dates WHERE date < CURDATE()
)
SELECT d.date, COUNT(u.id) AS new_users
FROM dates d
LEFT JOIN users u ON DATE(u.created_at) = d.date
GROUP BY d.date
ORDER BY d.date;
```

**AI 常见错误**
```sql
-- 错误：直接 GROUP BY，缺失日期不显示
SELECT DATE(created_at) AS date, COUNT(*) AS new_users
FROM users
WHERE created_at >= CURDATE() - INTERVAL 30 DAY
GROUP BY DATE(created_at)
ORDER BY date;
```

**AQL 设计启示**
- 「时间序列填充」是时间聚合的常见需求，应作为 DSL 的 `fill_gaps: true` 选项
- 日期序列生成完全是方言差异，必须由编译器处理

**测试记录**
| 模型 | 日期 | 是否正确 | 错误类型 |
|---|---|---|---|
| | | | |

---

## C2 — 同比计算

**自然语言 Prompt**
> 统计今年每个月的销售额，以及与去年同月相比的增长率

**核心陷阱**
同比需要同时查询两个时间段，AI 容易搞错 JOIN 方向、用错日期截断函数，或漏掉去年某月无数据的情况

**Expected SQL**
```sql
WITH monthly AS (
  SELECT
    DATE_TRUNC('month', created_at) AS month,
    SUM(total_amount) AS revenue
  FROM orders
  WHERE EXTRACT(YEAR FROM created_at) IN (EXTRACT(YEAR FROM NOW()), EXTRACT(YEAR FROM NOW()) - 1)
  GROUP BY DATE_TRUNC('month', created_at)
)
SELECT
  cy.month,
  cy.revenue AS this_year,
  py.revenue AS last_year,
  ROUND((cy.revenue - COALESCE(py.revenue, 0)) / NULLIF(py.revenue, 0) * 100, 2) AS growth_rate
FROM monthly cy
LEFT JOIN monthly py
  ON DATE_TRUNC('month', cy.month - INTERVAL '1 year') = py.month
WHERE EXTRACT(YEAR FROM cy.month) = EXTRACT(YEAR FROM NOW())
ORDER BY cy.month;
```

**AI 常见错误**
- 用 `YEAR()` 而不是 `DATE_TRUNC`（方言问题）
- 同比 JOIN 条件写错（月份对不上）
- 忘记处理去年同月无数据时的 NULL

**AQL 设计启示**
- 同比/环比是高频分析模式，可作为 DSL 的 `compare: { type: yoy, metric: revenue }` 原语
- NULL 处理策略（`COALESCE` / `NULLIF`）应在 DSL 层声明

**测试记录**
| 模型 | 日期 | 是否正确 | 错误类型 |
|---|---|---|---|
| | | | |
