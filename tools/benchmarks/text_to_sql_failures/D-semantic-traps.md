# D. 语义陷阱

## D1 — 业务指标定义歧义（复购率）

**自然语言 Prompt**
> 统计复购率，复购用户定义为下过 2 次及以上订单的用户

**核心陷阱**
「复购率」的分母定义不唯一：是总注册用户？还是有过至少1次下单的用户？AI 会自己选一个，不告知用户

**Expected SQL（分母 = 有过下单记录的用户）**
```sql
SELECT
  COUNT(CASE WHEN order_count >= 2 THEN 1 END) * 1.0 / COUNT(*) AS repurchase_rate
FROM (
  SELECT user_id, COUNT(*) AS order_count
  FROM orders
  GROUP BY user_id
) t;
```

**Expected SQL（分母 = 全部注册用户）**
```sql
SELECT
  COUNT(CASE WHEN t.order_count >= 2 THEN 1 END) * 1.0 / COUNT(u.id) AS repurchase_rate
FROM users u
LEFT JOIN (
  SELECT user_id, COUNT(*) AS order_count
  FROM orders
  GROUP BY user_id
) t ON t.user_id = u.id;
```

**AI 常见问题**
- 默默选择其中一种，不告知用户存在歧义
- 两种都可能生成，结果数值差异显著

**AQL 设计启示**
- 复购率、留存率等指标应在「指标注册层」预定义，包含分子/分母的精确定义
- DSL 引用指标名而非让 AI 重新推导计算逻辑

**测试记录**
| 模型 | 日期 | 选的分母 | 是否告知歧义 |
|---|---|---|---|
| | | | |

---

## D2 — 加权平均价格（毛利率）

**自然语言 Prompt**
> 计算每个商品的实际毛利率，公式为（售价 - 成本）/ 售价

**核心陷阱**
「售价」不是商品表的固定价格，而是 order_items 中的实际成交均价（unit_price 加权平均）；AI 常直接用商品表字段或简单平均

**Expected SQL**
```sql
SELECT
  p.id,
  p.name,
  p.cost_price,
  AVG(oi.unit_price) AS avg_sell_price,
  ROUND((AVG(oi.unit_price) - p.cost_price) / NULLIF(AVG(oi.unit_price), 0) * 100, 2) AS gross_margin_pct
FROM products p
JOIN order_items oi ON oi.product_id = p.id
GROUP BY p.id, p.name, p.cost_price;
```

**AI 常见错误**
```sql
-- 错误：用了商品表里不存在的 price 字段（幻觉）
SELECT name, (price - cost_price) / price AS margin FROM products;
```

**AQL 设计启示**
- 「售价」这类需要跨表计算的派生字段，应在 schema 注册时定义为计算字段
- AI 操作 DSL 时只引用字段名，不负责推导计算路径

**测试记录**
| 模型 | 日期 | 是否正确 | 错误类型 |
|---|---|---|---|
| | | | |
