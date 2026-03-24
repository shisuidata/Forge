## 窗口函数

窗口函数放在顶层 `window` 数组，**不要放在 `agg` 里**。有三类：

| 类型 | fn | 必须有 col？ | 说明 |
|------|----|----|------|
| 排名 | row_number / rank / dense_rank / percent_rank / cume_dist / ntile | ❌ 不需要 | 只计排名 |
| 聚合窗口 | sum / avg / count / min / max | ✅ 必须有 | 指定聚合哪一列 |
| 导航 | lag / lead / first_value / last_value | ✅ 必须有 | 取上下行的值 |

**❌ 最常见错误：WindowAgg 忘写 `col`**
```json
// ❌ 错误：sum window 缺 col
{"fn": "sum", "partition": ["user_id"], "order": [{"col": "created_at", "dir": "asc"}], "as": "running_total"}

// ✅ 正确：必须指定要累加哪一列
{"fn": "sum", "col": "orders.total_amount", "partition": ["orders.user_id"], "order": [{"col": "orders.created_at", "dir": "asc"}], "as": "running_total"}
```

**格式规则**：
- `partition` 是字符串数组（❌ `"partition": "字段"` → ✅ `"partition": ["字段"]`）
- `order` 是排序对象数组（❌ `"order": {"col":"..."}` → ✅ `"order": [{"col":"...", "dir":"asc"}]`）

## 按月统计 + LAG/LEAD（必须用 CTE）

**关键限制**：SQLite 的窗口函数 ORDER BY 不能引用 SELECT 别名（如 `month`）。
按月统计 + 环比/预测，必须先用 CTE 算好月份，再在外层做窗口函数。

```json
{
  "cte": [
    {
      "name": "monthly",
      "query": {
        "scan": "orders",
        "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
        "group": [
          "orders.channel_id",
          {"expr": "STRFTIME('%Y-%m', orders.created_at)", "as": "month"}
        ],
        "agg": [{"fn": "count_all", "as": "order_count"}],
        "select": ["orders.channel_id", "month", "order_count"]
      }
    }
  ],
  "scan": "monthly",
  "window": [{
    "fn": "lag",
    "col": "monthly.order_count",
    "offset": 1,
    "default": null,
    "partition": ["monthly.channel_id"],
    "order": [{"col": "monthly.month", "dir": "asc"}],
    "as": "prev_month_count"
  }],
  "select": ["monthly.channel_id", "monthly.month", "monthly.order_count", "prev_month_count"]
}
```

❌ 错误（window ORDER BY 引用 SELECT 别名）：以下两种都会报 "no such column"：
- 同层查询里 `order: [{"col": "month"}]` — `month` 是 STRFTIME 别名
- `order: [{"col": "refund_rate"}]` — `refund_rate` 是本层 `select` 中计算的 expr 别名

**规则**：`window[].order` 只能引用 FROM 表的原始列或 CTE 子查询里已 SELECT 出来的列，不能引用本层 SELECT 新定义的 expr 别名。解决方法：把需要排序的计算列放进 CTE 先算好。

## 时序导航（LAG / LEAD）

LAG/LEAD **必须有 partition**，否则会跨用户取行，语义错误。
LAG/LEAD 的 `order` **必须用 `"dir": "asc"`**（时间升序，才能取到"上一条"）；用 `desc` 会取到错误方向的行。

### 模式 1：明细行 LAG（取上一笔金额）

对原始明细行直接打 LAG，返回每一行（不是只返回最后一行）。

```json
{
  "scan": "orders",
  "joins": [{"type": "inner", "table": "users", "on": {"left": "orders.user_id", "right": "users.id"}}],
  "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
  "window": [{
    "fn": "lag",
    "col": "orders.total_amount",
    "offset": 1,
    "default": null,
    "partition": ["orders.user_id"],
    "order": [{"col": "orders.created_at", "dir": "asc"}],
    "as": "prev_amount"
  }],
  "select": ["users.name", "orders.created_at", "orders.total_amount", "prev_amount"]
}
```

### 模式 2：LAG + 时间间隔计算（SQLite julianday）

求相邻两次下单间隔天数：先用 LAG 取上一行时间，再用 `julianday()` 做差。
**SQLite 不支持日期直接相减**，必须用 `julianday(a) - julianday(b)` 得到天数差。

```json
{
  "scan": "orders",
  "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
  "window": [{
    "fn": "lag",
    "col": "orders.created_at",
    "offset": 1,
    "default": null,
    "partition": ["orders.user_id"],
    "order": [{"col": "orders.created_at", "dir": "asc"}],
    "as": "prev_order_dt"
  }],
  "select": [
    "orders.user_id", "orders.created_at", "prev_order_dt",
    {"expr": "ROUND(julianday(orders.created_at) - julianday(prev_order_dt), 1)", "as": "days_gap"}
  ]
}
```
