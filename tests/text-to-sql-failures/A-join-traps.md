# A. JOIN 陷阱

## A1 — 包含零单用户的订单统计

**自然语言 Prompt**
> 统计所有用户的订单数量，没有下单的用户也要显示，订单数为 0

**核心陷阱**
AI 容易使用 INNER JOIN，导致从未下单的用户被过滤掉

**Expected SQL**
```sql
SELECT
  u.id,
  u.name,
  COUNT(o.id) AS order_count
FROM users u
LEFT JOIN orders o ON o.user_id = u.id
GROUP BY u.id, u.name;
```

**AI 常见错误**
```sql
-- 错误：INNER JOIN 丢失零单用户
SELECT u.id, u.name, COUNT(o.id) AS order_count
FROM users u
JOIN orders o ON o.user_id = u.id
GROUP BY u.id, u.name;
```

**AQL 设计启示**
- JOIN type 必须显式声明，不能有默认值
- `COUNT` 的目标字段需明确（`COUNT(o.id)` vs `COUNT(*)`）

**测试记录**
| 模型 | 日期 | 是否正确 | 错误类型 |
|---|---|---|---|
| | | | |

---

## A2 — 找出无明细的异常订单

**自然语言 Prompt**
> 找出所有没有对应订单明细的异常订单

**核心陷阱**
需要 LEFT JOIN + IS NULL 反连接模式；AI 常用 NOT IN 子查询，当 order_items.order_id 存在 NULL 时语义错误

**Expected SQL**
```sql
SELECT o.*
FROM orders o
LEFT JOIN order_items oi ON oi.order_id = o.id
WHERE oi.id IS NULL;
```

**AI 常见错误**
```sql
-- 错误：NOT IN 遇到 NULL 会返回空结果
SELECT * FROM orders
WHERE id NOT IN (SELECT order_id FROM order_items);
```

**AQL 设计启示**
- 反连接（anti-join）应作为 DSL 的一等公民，而不是靠 AI 推导 NOT IN / LEFT JOIN IS NULL
- NULL 语义需在 DSL 层显式处理

**测试记录**
| 模型 | 日期 | 是否正确 | 错误类型 |
|---|---|---|---|
| | | | |
