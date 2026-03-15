## 复合过滤条件（OR 内嵌 AND）

filter 是**数组**，`{"or":[...]}` 是数组的一个元素，不能把 `{"or":[...]}` 直接作为 filter 的值。

表达 `(A AND B) OR C`：

```json
"filter": [
  {
    "or": [
      {"col": "users.name", "op": "like", "val": "%明%"},
      {"and": [
        {"col": "users.created_at", "op": "gte", "val": "2024-01-01"},
        {"col": "users.is_vip",     "op": "eq",  "val": 1}
      ]}
    ]
  }
]
```

❌ 错误写法：`"filter": {"or": [...]}` （filter 不能是 dict）
