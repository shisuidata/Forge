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

### 聚合后的分组内 TopN

当问题是"各品类内按销量/销售额排名前 3 的商品"时，不能直接对明细行打排名。必须：

1. 先用 CTE 按商品和品类聚合出销量/销售额
2. 在主查询对聚合结果打 `row_number` / `rank` / `dense_rank`
3. 用 `qualify` 过滤排名

```json
{
  "cte": [
    {
      "name": "product_sales",
      "query": {
        "scan": "dwd_order_item_detail",
        "joins": [
          {"type": "inner", "table": "dim_product", "on": {"left": "dwd_order_item_detail.product_id", "right": "dim_product.product_id"}},
          {"type": "inner", "table": "dim_category", "on": {"left": "dim_product.category_id", "right": "dim_category.category_id"}}
        ],
        "group": ["dim_product.product_id", "dim_product.product_name", "dim_category.category_name"],
        "agg": [{"fn": "sum", "col": "dwd_order_item_detail.quantity", "as": "total_qty"}],
        "select": ["dim_product.product_id", "dim_product.product_name", "dim_category.category_name", "total_qty"]
      }
    }
  ],
  "scan": "product_sales",
  "window": [
    {
      "fn": "row_number",
      "partition": ["category_name"],
      "order": [{"col": "total_qty", "dir": "desc"}],
      "as": "rn"
    }
  ],
  "qualify": [{"col": "rn", "op": "lte", "val": 3}],
  "select": ["category_name", "product_name", "total_qty", "rn"],
  "sort": [{"col": "category_name", "dir": "asc"}, {"col": "rn", "dir": "asc"}]
}
```

❌ 错误：只写 `sort + limit: 3`。这会取全局前 3，不是每个品类前 3。

❌ 错误：有 `window` 排名但没有 `qualify`。这会返回每个品类的全部商品。

❌ 错误：在明细表上直接排名。必须先聚合到商品粒度，再排名。

当问题同时要求“商品销售额在品类中的占比”时，分母必须与用户看到的品类展示粒度一致：

1. 第一层 CTE 按 `product_id + product_name + category_name` 聚合 `product_revenue`
2. 第二层在聚合结果上计算
   `SUM(product_revenue) OVER (PARTITION BY category_name)` 和
   `ROW_NUMBER() OVER (PARTITION BY category_name ORDER BY product_revenue DESC)`
3. 最外层过滤 `rn <= N`，只输出 `category_name、product_name、product_revenue、pct`

不要另建按 `category_id` 分组的品类总额 CTE。一个展示名称可能对应多个内部 ID，
按 ID 计算分母会把同名品类拆开，得到错误占比。
