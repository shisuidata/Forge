## CTE 用法（多步聚合）

`cte` 仅用于"子查询结果需要再次 join 或 filter"的场景，不用于简单聚合。

**关键**：有 `cte` 的 Forge JSON 仍然必须有顶层 `scan` 和 `select`。\
`cte` 定义命名子查询，主查询（`scan`/`filter`/`agg`/`select`）把这些名字当表用。

**每个 CTE 项必须有 `name` 和 `query` 两个字段**，`query` 内是一个完整的 Forge JSON（含 scan 和 select）。

### 单 CTE 示例

```json
{
  "cte": [
    {
      "name": "order_counts",
      "query": {
        "scan": "orders",
        "group": ["orders.user_id"],
        "agg": [{"fn": "count_all", "as": "order_count"}],
        "select": ["orders.user_id", "order_count"]
      }
    }
  ],
  "scan": "order_counts",
  "filter": [{"col": "order_count", "op": "gte", "val": 2}],
  "select": ["order_counts.user_id", "order_count"]
}
```

### 比值计算示例（退款率 = 退款数 / 总订单数）

计算两个聚合值的比值，必须用 CTE 先算各自数量，再在主查询 `select` 中用 `expr` 相除。
**`fn: 'expr'` 不存在，绝对不能用在 `agg` 里。**

```json
{
  "cte": [
    {
      "name": "category_stats",
      "query": {
        "scan": "orders",
        "joins": [{"type": "inner", "table": "order_items", "on": {"left": "orders.id", "right": "order_items.order_id"}}],
        "group": ["order_items.category_id"],
        "agg": [
          {"fn": "count_all", "as": "total_orders"},
          {"fn": "count", "col": "orders.refund_id", "as": "refund_orders"}
        ],
        "select": ["order_items.category_id", "total_orders", "refund_orders"]
      }
    }
  ],
  "scan": "category_stats",
  "select": [
    "category_stats.category_id",
    "total_orders",
    "refund_orders",
    {"expr": "refund_orders * 1.0 / total_orders", "as": "refund_rate"}
  ]
}
```

❌ 错误：`{"fn": "expr", "col": "CAST(count(x) AS FLOAT)/count_all()", "as": "rate"}` — `fn: 'expr'` 不在枚举内

### 双 CTE 示例（两个子查询再 JOIN）

```json
{
  "cte": [
    {
      "name": "sales",
      "query": {
        "scan": "order_items",
        "group": ["order_items.product_id"],
        "agg": [{"fn": "sum", "col": "order_items.unit_price", "as": "total_sales"}],
        "select": ["order_items.product_id", "total_sales"]
      }
    },
    {
      "name": "refunds",
      "query": {
        "scan": "refund_items",
        "group": ["refund_items.product_id"],
        "agg": [{"fn": "sum", "col": "refund_items.refund_amount", "as": "total_refund"}],
        "select": ["refund_items.product_id", "total_refund"]
      }
    }
  ],
  "scan": "sales",
  "joins": [
    {
      "type": "left",
      "table": "refunds",
      "on": {"left": "sales.product_id", "right": "refunds.product_id"}
    }
  ],
  "select": ["sales.product_id", "total_sales", "total_refund"]
}
```

### 严格禁止的错误写法

```json
// ❌ 错误 1：CTE 是对象而非数组
{"cte": {"user_totals": {"scan": "orders", ...}}, "scan": "user_totals"}

// ❌ 错误 2：CTE 项直接展开，缺少 query 包装层
{"cte": [{"name": "user_totals", "scan": "orders", "group": [...], "select": [...]}], "scan": "user_totals"}

// ❌ 错误 3：使用 ctes（不存在，应为 cte）
{"ctes": [...], "scan": "..."}
```

```json
// ✅ 正确：每个 CTE 项必须是 {name, query: {完整Forge子查询}}
{
  "cte": [
    {
      "name": "user_totals",
      "query": {
        "scan": "orders",
        "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
        "group": ["orders.user_id"],
        "agg": [{"fn": "sum", "col": "orders.total_amount", "as": "total_spent"}],
        "select": ["orders.user_id", "total_spent"]
      }
    }
  ],
  "scan": "user_totals",
  "filter": [{"col": "total_spent", "op": "gte", "val": 5000}],
  "select": ["user_totals.user_id", "total_spent"]
}
```

### 高于平均值（CROSS JOIN 标量 CTE）

找出"高于所有用户平均消费"的用户：必须用两个 CTE + CROSS JOIN。
**❌ 绝对不能** 在 WHERE 中直接写 `avg_cte.avg_val`（avg_cte 不在 FROM 链中会报 'no such column'）。

```json
{
  "cte": [
    {
      "name": "user_totals",
      "query": {
        "scan": "orders",
        "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
        "group": ["orders.user_id"],
        "agg": [{"fn": "sum", "col": "orders.total_amount", "as": "total_spent"}],
        "select": ["orders.user_id", "total_spent"]
      }
    },
    {
      "name": "avg_val",
      "query": {
        "scan": "user_totals",
        "agg": [{"fn": "avg", "col": "user_totals.total_spent", "as": "avg_spent"}],
        "select": ["avg_spent"]
      }
    }
  ],
  "scan": "user_totals",
  "joins": [
    {"type": "inner", "table": "users", "on": {"left": "user_totals.user_id", "right": "users.id"}},
    {"type": "cross", "table": "avg_val"}
  ],
  "filter": [{"col": "user_totals.total_spent", "op": "gt", "col2": "avg_val.avg_spent"}],
  "select": ["users.name", "user_totals.total_spent"],
  "sort": [{"col": "user_totals.total_spent", "dir": "desc"}]
}
```

**规则**：`avg_val` 只有1行，CROSS JOIN 后每行都能访问 `avg_val.avg_spent`，用 `col2` 做列对列比较。

**记忆口诀**：`cte` = 数组；每项 = `{name, query}`；`query` 内 = 完整 Forge（含 scan + select）
