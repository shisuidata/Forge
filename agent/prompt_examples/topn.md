## per-group TopN（分组内排名过滤）

用 qualify 字段过滤窗口函数结果，实现"每组取前 N 名"：

```json
{
  "scan": "products",
  "select": ["products.name", "products.category", "products.cost_price", "cost_rank"],
  "window": [{"fn": "dense_rank", "partition": ["products.category"],
              "order": [{"col": "products.cost_price", "dir": "desc"}], "as": "cost_rank"}],
  "qualify": [{"col": "cost_rank", "op": "lte", "val": 3}]
}
```
