# 工作原理与 DSL 能力

---

## 执行流程详解

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

JSON Schema 在 token 生成层强制约束：`fn` 只能是枚举值，`scan` 只能是 Registry 里的表名。

### Step 3 — 确定性编译

```python
compile_query(forge_json)  # 同样的输入永远产生同样的 SQL
```

编译前，`_expand_aliases()` 将 SELECT 中引用的 agg alias 展开为完整表达式，规避 SQL alias 作用域陷阱：

```sql
WITH user_orders AS (
  SELECT orders.user_id, COUNT(*) AS order_count
  FROM orders
  GROUP BY orders.user_id
)
SELECT COUNT(CASE WHEN order_count >= 2 THEN 1 END) * 1.0 / COUNT(*) AS repurchase_rate
FROM user_orders
```

### Step 4 — 用户审核 → 执行

用户看到的就是会执行的那个 SQL，无运行时变换。审核通过，Forge 直连数据库执行，展示结果。

---

## DSL 能力

| 特性 | 详情 |
|---|---|
| **JOIN 类型** | `inner / left / right / full / anti / semi`，类型必须显式声明 |
| **anti join** | 替代 `NOT IN`，从根源消灭 NULL 陷阱 |
| **聚合函数** | `count / count_all / count_distinct / sum / avg / min / max / group_concat` |
| **agg FILTER 子句** | `{"fn":"sum","col":"...","filter":[...]}` → `SUM(...) FILTER (WHERE ...)`，SQLite/PG 原生支持 |
| **CASE WHEN in agg** | `{"fn":"count","col":"CASE WHEN x>=2 THEN 1 END"}` |
| **窗口函数（排名/分布）** | `row_number / rank / dense_rank / percent_rank / cume_dist / ntile(n)` |
| **窗口函数（值/导航）** | `lag / lead / first_value / last_value`，支持 offset、default、frame |
| **窗口帧（frame）** | `{"unit":"rows","start":"6 preceding","end":"current_row"}` → `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`；支持滑动平均、累计求和 |
| **qualify** | 窗口结果过滤（per-group TopN），编译为包装子查询 |
| **CTE** | 多步聚合、派生指标，支持 recursive CTE |
| **日期截断分组** | `group` 支持 `{"expr":"STRFTIME('%Y-%m',col)","as":"month"}`，别名直接在 select 中引用 |
| **日期** | `$date` 字面量 + `$preset` 相对日期（8 种预设） |
| **SELECT DISTINCT** | 顶层加 `"distinct": true` |
| **集合运算** | `union / union_all / intersect / except`，主查询的 sort/limit 应用于整体 |
| **IN 子查询** | `{"col":"users.id","op":"in","val":{"subquery":{...}}}` → `col IN (SELECT ...)` |
| **方言适配** | SQLite / MySQL / PostgreSQL（日期函数、字符串聚合、FULL JOIN 检测、FILTER 子句可用性校验） |
| **alias 展开** | SELECT expr 中引用 agg/window alias 自动展开，消灭 alias 作用域错误 |

---

## Schema 向量检索（RAG）

当 Registry 包含几十/几百张表时，把完整 schema 放进 prompt 会撑爆 context window，且大量无关表会干扰 LLM 意图识别。Forge 通过 `forge/retriever.py` 内置了一套两级检索方案。

### 工作流程

```
用户问题
  ↓
SchemaRetriever.retrieve(question, embed_fn, top_k=5)
  ├── 已建索引 + embed_fn 可用 → 向量检索（cosine similarity）
  └── 否则 → BM25-lite 关键词降级（自动触发，无需配置）
  ↓
top-k 相关表的 DDL schema（仅注入 prompt 中这几张表）
  ↓
LLM 生成 Forge JSON（context 更短，干扰更少）
```

### 表描述构建（索引的基础）

检索质量的上限取决于「每张表的描述文本写得有多好」。Forge 把 Registry 中每张表的元信息拼成富文本：

```
Table: orders. Description: 订单主表. Columns: id, user_id, status (completed, cancelled), total_amount, created_at
```

**关键：枚举值写进描述里。** 当用户问「已完成订单」，`status (completed, cancelled)` 让 embedding 能正确命中 `orders` 表，而不是找错表。

### 两级检索

| 模式 | 原理 | 触发条件 | 召回率（4 张表，top_k=5） |
|---|---|---|---|
| **向量检索** | L2 归一化 cosine 相似度 | 已建索引 + embed_fn 可用 | 100% |
| **BM25-lite 降级** | TF×IDF + 中文 bigram 分词 | 无 embedding API 时自动启用 | 92.9% |

BM25-lite 的中文分词策略：纯中文块同时生成原串（精确匹配权重高）+ 字符级 bigram（模糊匹配），让「品类」能从「商品品类分类」中命中。

### Embedding API 兼容性

`make_embed_fn` 工厂函数屏蔽了不同 API 的格式差异：

| API | 请求格式 | 响应格式 |
|---|---|---|
| 标准 OpenAI | `{"input": ["..."]}` | `{"data": [{"embedding": [...]}]}` |
| MiniMax | `{"texts": ["..."], "type": "db"/"query"}` | `{"vectors": [[...], [...]]}` |

MiniMax 区分 `db`（建索引用）和 `query`（查询用）两种嵌入类型，混用会降低召回率。Forge 自动为索引构建和查询检索使用正确的 type。

### 向量索引缓存

- 首次运行：调用 embedding API 批量嵌入所有表描述，L2 归一化后缓存到 `.forge/schema_embeddings.pkl`
- 后续查询：直接加载缓存（毫秒级），表集合变更时自动失效重建
- `top_k >= 表总数` 时跳过检索，直接返回全部表（避免在小 schema 上浪费 API 调用）

### Schema 压缩效果

| 场景 | 全量 schema token | 检索精简后 | 减少 |
|---|---|---|---|
| 4 张表，top_k=5 | ~230 | ~230（自动全取） | 0% |
| 50 张表，top_k=5 | ~2,800 | ~560 | **~80%** |

> 在真实企业场景的几十张表 schema 中，每次查询只注入 5 张最相关的表，prompt 中 schema 部分可压缩 80% 以上。
