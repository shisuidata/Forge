## Forge JSON 标准示例

以下是一个包含多表关联、过滤、分组、聚合、排序的完整示例，请严格参照此格式。

**需求**：统计各品类已完成订单的总销售额和订单数，只看金额大于1000的品类，按销售额降序取前5。

```json
{
  "scan": "orders",
  "joins": [
    {
      "type": "inner",
      "table": "order_items",
      "on": {"left": "orders.id", "right": "order_items.order_id"}
    },
    {
      "type": "inner",
      "table": "products",
      "on": {"left": "order_items.product_id", "right": "products.id"}
    }
  ],
  "filter": [
    {"col": "orders.status", "op": "eq", "val": "completed"}
  ],
  "group": ["products.category"],
  "agg": [
    {"fn": "sum", "col": "order_items.unit_price", "as": "total_sales"},
    {"fn": "count_all", "as": "order_count"}
  ],
  "having": [
    {"col": "total_sales", "op": "gt", "val": 1000}
  ],
  "select": ["products.category", "total_sales", "order_count"],
  "sort": [{"col": "total_sales", "dir": "desc"}],
  "limit": 5
}
```

**格式要点（对照 SQL 习惯）**：

| Forge JSON 字段 | ❌ SQL 习惯（错误） | ✅ Forge 写法（正确） |
|---|---|---|
| 关联表 | `"join": {...}` | `"joins": [{"type":..., "table":..., "on":{...}}]` |
| 连接条件 | `"on": "a.id = b.id"` | `"on": {"left": "a.id", "right": "b.id"}` |
| 过滤条件 | `"where": {...}` 或 `"filter": {"and":[...]}` | `"filter": [{...}, {...}]`（平铺数组，多项自动 AND） |
| 分组 | `"group_by": [...]` 或 `"groupby": [...]` | `"group": [...]` |
| 排序 | `"order_by": [...]` 或 `[{"col": "x"}]`（**缺 dir**） | `"sort": [{"col": "字段", "dir": "desc"}]`（**dir 是必填项，asc 或 desc，绝不能省略**） |
| 比较运算符 | `"op": "<"` 或 `"op": ">"` | `"op": "lt"` / `"op": "gt"` |
| 按月分组 | `{"col": "created_at", "fn": "month"}` | `{"expr": "STRFTIME('%Y-%m', orders.created_at)", "as": "month"}` |
| 条件聚合数量 | `{"fn": "count_all", "filter": [...]}` | `{"fn": "count", "col": "table.id", "filter": [...], "as": "n"}` |

⚠️ **sort.dir 是必填字段**：每个 sort 节点必须同时包含 `col` 和 `dir`（值为 `"asc"` 或 `"desc"`），缺少 `dir` 会导致编译失败。

```json
// ❌ 错误：缺少 dir
"sort": [{"col": "total_sales"}]

// ✅ 正确：必须指定方向
"sort": [{"col": "total_sales", "dir": "desc"}]
```
