---
title: 工作原理
description: Forge 的执行流程、DSL 能力表与 Schema RAG 检索机制
---

## 执行流程

以「统计复购率，复购用户定义为下过 2 次及以上订单的用户」为例：

### Step 1 — Registry 构建系统 Prompt

`forge sync` 直连数据库，自动采样低基数列枚举值：

```
Database schema:
  users: id, name, city, created_at, is_vip[0/1]
  orders: id, user_id, status[cancelled/completed], total_amount, created_at
  order_items: id, order_id, product_id, quantity, unit_price
  products: id, name, category[Books/Clothing/Electronics], cost_price
```

`status[cancelled/completed]` 让 LLM 知道正确的字符串拼写，消灭一类幻觉。

### Step 2 — LLM 生成 Forge JSON（Structured Output）

```json
{
  "cte": [{
    "name": "user_orders",
    "query": {
      "scan": "orders",
      "group": ["orders.user_id"],
      "agg": [{"fn": "count_all", "as": "order_count"}],
      "select": ["orders.user_id", "order_count"]
    }
  }],
  "scan": "user_orders",
  "agg": [
    {"fn": "count_all", "as": "total_users"},
    {"fn": "count", "col": "CASE WHEN order_count >= 2 THEN 1 END", "as": "repeat_users"}
  ],
  "select": [{"expr": "repeat_users * 1.0 / total_users", "as": "repurchase_rate"}]
}
```

当 Provider 严格执行动态 Tool Schema 时，`fn` 等固定值域与多数表列引用会在生成阶段受到枚举约束。为支持 CTE、CASE/算术表达式而保留的字符串位置，以及 Provider 兼容降级路径，仍需编译、lint 和审核兜底。

### Step 3 — 确定性编译

```sql
WITH user_orders AS (
  SELECT orders.user_id, COUNT(*) AS order_count
  FROM orders
  GROUP BY orders.user_id
)
SELECT COUNT(CASE WHEN order_count >= 2 THEN 1 END) * 1.0 / COUNT(*) AS repurchase_rate
FROM user_orders
```

同样的 Forge JSON 永远产生同样的 SQL。编译前，`_expand_aliases()` 将 SELECT 中引用的 agg alias 展开为完整表达式，规避 SQL alias 作用域陷阱。

### Step 4 — 用户审核 + 执行

用户看到的就是会执行的那个 SQL，无运行时变换。审核通过，Forge 直连数据库执行，展示结果。

---

## DSL 能力

| 特性 | 详情 |
|---|---|
| **JOIN 类型** | `inner / left / right / full / anti / semi`，类型必须显式声明 |
| **anti join** | 替代 `NOT IN`，从根源消灭 NULL 陷阱 |
| **聚合函数** | `count / count_all / count_distinct / sum / avg / min / max / group_concat` |
| **agg FILTER** | `SUM(...) FILTER (WHERE ...)`，SQLite/PG 原生支持 |
| **CASE WHEN in agg** | `{"fn":"count","col":"CASE WHEN x>=2 THEN 1 END"}` |
| **窗口函数（排名）** | `row_number / rank / dense_rank / percent_rank / cume_dist / ntile(n)` |
| **窗口函数（导航）** | `lag / lead / first_value / last_value`，支持 offset、default、frame |
| **窗口帧** | `ROWS BETWEEN ... AND ...`，支持滑动平均、累计求和 |
| **qualify** | 窗口结果过滤（per-group TopN），编译为包装子查询 |
| **CTE** | 多步聚合、派生指标，支持 recursive CTE |
| **日期** | `$date` 字面量 + `$preset` 相对日期（8 种预设） |
| **集合运算** | `union / union_all / intersect / except` |
| **IN 子查询** | `col IN (SELECT ...)` |
| **方言适配** | SQLite / MySQL / PostgreSQL（日期、字符串聚合、FILTER、FULL JOIN） |
| **alias 展开** | SELECT expr 中引用 agg/window alias 自动展开 |

---

## Schema 向量检索（RAG）

当 Registry 包含几十/几百张表时，Forge 通过两级检索方案只注入最相关的表。

### 流程

```
用户问题
  ↓
SchemaRetriever.retrieve(question, top_k=5)
  ├── 已建索引 + embed_fn 可用 → 向量检索（cosine similarity）
  └── 否则 → BM25-lite 关键词降级（自动触发）
  ↓
top-k 相关表的 DDL schema（仅注入 prompt 中这几张表）
```

### 两级检索

| 模式 | 原理 | 触发条件 |
|---|---|---|
| **向量检索** | L2 归一化 cosine 相似度 | 已建索引 + embed_fn 可用 |
| **BM25-lite 降级** | TF x IDF + 中文 bigram 分词 | 无 embedding API 时自动启用 |

### 压缩效果

| 场景 | 全量 schema token | 检索精简后 | 减少 |
|---|---|---|---|
| 4 张表，top_k=5 | ~230 | ~230（自动全取） | 0% |
| 50 张表，top_k=5 | ~2,800 | ~560 | **~80%** |

> 在真实企业场景的几十张表 schema 中，每次查询只注入 5 张最相关的表，prompt 中 schema 部分可压缩 80% 以上。

---

> 本页为精简版。完整内容参见 [docs/how-it-works.md](https://github.com/shisuidata/Forge/blob/main/docs/how-it-works.md)。
