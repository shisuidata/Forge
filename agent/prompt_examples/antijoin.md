## ANTI / SEMI JOIN 用法

`anti` = "不存在于右表"；`semi` = "存在于右表"。绝对禁用 NOT IN（NULL 陷阱）。

⚠️ **`scan` 必须是主表（保留行的那张表）**：
- `scan` = 你想保留数据行的表（"从哪张表查"）
- `joins[].table` + `type: anti` = 你要排除的那张表
- 常见错误：把被排除的表写成 `scan`，主表反而放在 join 里 → 结果完全相反

```json
// ❌ 错误：scan 写了被排除的表
{"scan": "orders", "joins": [{"type": "anti", "table": "users", "on": {...}}]}
// 上面查的是"在 users 中不存在的 orders"，而不是"没下单的用户"

// ✅ 正确：scan 是你想保留的主表（users），orders 是被排除的表
{"scan": "users", "joins": [{"type": "anti", "table": "orders", "on": {...}}]}
```

### 关键：`filter` 字段

当需要"从未做过某类操作"（而非"完全没有记录"）时，用 `anti` join 的 `filter` 字段指定条件。
`filter` 里的条件作用于右表，编译为 NOT EXISTS 子查询的 WHERE 子句。

### 示例 1：从未写过差评的用户

```json
{
  "scan": "dim_user",
  "joins": [
    {
      "type": "anti",
      "table": "dwd_comment_detail",
      "on": {"left": "dim_user.user_id", "right": "dwd_comment_detail.user_id"},
      "filter": [{"col": "dwd_comment_detail.comment_type", "op": "eq", "val": "差评"}]
    }
  ],
  "select": ["dim_user.user_name", "dim_user.register_date"],
  "sort": [{"col": "dim_user.register_date", "dir": "asc"}]
}
```

编译结果：`WHERE NOT EXISTS (SELECT 1 FROM dwd_comment_detail WHERE user_id = dim_user.user_id AND comment_type = '差评')`

### 示例 2：从未下单的用户（无额外条件时不需要 filter）

```json
{
  "scan": "users",
  "joins": [
    {
      "type": "anti",
      "table": "orders",
      "on": {"left": "users.id", "right": "orders.user_id"}
    }
  ],
  "select": ["users.name", "users.created_at"]
}
```

### 示例 3：有差评但无图片的商品（HAVING + ANTI）

先用 CTE 统计，再过滤：

```json
{
  "cte": [
    {
      "name": "bad_review_stats",
      "query": {
        "scan": "dwd_comment_detail",
        "filter": [{"col": "dwd_comment_detail.comment_type", "op": "eq", "val": "差评"}],
        "group": ["dwd_comment_detail.product_id"],
        "agg": [
          {"fn": "count_all", "as": "total_bad_reviews"},
          {"fn": "count", "col": "dwd_comment_detail.image_url", "as": "reviews_with_image"}
        ],
        "select": ["dwd_comment_detail.product_id", "total_bad_reviews", "reviews_with_image"]
      }
    }
  ],
  "scan": "dim_product",
  "joins": [
    {
      "type": "inner",
      "table": "bad_review_stats",
      "on": {"left": "dim_product.product_id", "right": "bad_review_stats.product_id"}
    }
  ],
  "having": [{"col": "reviews_with_image", "op": "eq", "val": 0}],
  "select": ["dim_product.product_name"]
}
```

❌ 错误（LEFT JOIN 忘加 filter 条件，会把完全没有评论的用户也包含进去）：
```json
{"type": "left", "table": "dwd_comment_detail", "on": {...}}
// → WHERE dwd_comment_detail.user_id IS NULL  ← 过滤掉了所有评论，不是只过滤差评
```

✅ 正确：用 `anti` + `filter` 精准指定"哪类记录不存在"

### 示例 4：有 2023 年已完成订单但 2024 年没有已完成订单的用户（时间段 anti-join）

**模式**："有 X 时期记录 但 Y 时期没有记录" → CTE 分别聚合两个时期，主查询 anti-join 排除 Y 时期存在的用户。

```json
{
  "cte": [
    {
      "name": "users_2023",
      "query": {
        "scan": "orders",
        "filter": [
          {"col": "orders.status", "op": "eq", "val": "completed"},
          {"col": "orders.created_at", "op": "gte", "val": {"$date": "2023-01-01"}},
          {"col": "orders.created_at", "op": "lt",  "val": {"$date": "2024-01-01"}}
        ],
        "group": ["orders.user_id"],
        "agg": [{"fn": "sum", "col": "orders.total_amount", "as": "total_2023"}],
        "select": ["orders.user_id", "total_2023"]
      }
    }
  ],
  "scan": "users_2023",
  "joins": [
    {
      "type": "inner",
      "table": "users",
      "on": {"left": "users_2023.user_id", "right": "users.id"}
    },
    {
      "type": "anti",
      "table": "orders",
      "on": {"left": "users_2023.user_id", "right": "orders.user_id"},
      "filter": [
        {"col": "orders.status", "op": "eq", "val": "completed"},
        {"col": "orders.created_at", "op": "gte", "val": {"$date": "2024-01-01"}}
      ]
    }
  ],
  "select": ["users.name", "users_2023.total_2023"],
  "sort": [{"col": "users_2023.total_2023", "dir": "desc"}]
}
```
