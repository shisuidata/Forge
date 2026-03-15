## CTE 用法（多步聚合）

`cte` 仅用于"子查询结果需要再次 join 或 filter"的场景，不用于简单聚合。

**关键**：有 `cte` 的 Forge JSON 仍然必须有顶层 `scan` 和 `select`。\
`cte` 定义命名子查询，主查询（`scan`/`filter`/`agg`/`select`）把这些名字当表用。

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

❌ 错误（只有 cte，无顶层 scan）：`{"cte": [...]}`\
✅ 正确：`{"cte": [...], "scan": "cte名称", "select": [...]}`
