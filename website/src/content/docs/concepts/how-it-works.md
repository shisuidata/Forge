---
title: 工作原理
description: Evaluate → Enforce → Explain、可选 Forge JSON 编译与检索边界
---

## 当前运行时主链

Forge 接收 Direct SQL 或 Forge JSON **候选**，核心职责不是替上游 Agent 做所有生成，而是验证、约束执行和解释证据：

```text
候选 → Evaluate（不执行、不授予执行权）
     → Enforce（身份、用途、任务、资源范围、运行时检查与适用审核）
     → QueryRun 受控执行 → Explain（同一运行的证据与限制）
```

公开源码的 Golden Path 使用 v1 的合成 Quickstart；独立主体、可撤销有界授权及指定审核人的 v2 路径属于维护者本地未提交候选，不是公开 `main` 已发布能力。v1/原始 SQL 入口退休（410）也是该本地候选的变更，不能外推到公开版本。Direct SQL 是候选格式，不是绕过门禁的 fallback。详见[架构设计](/reference/architecture/)。

:::caution[机制不等于安全认证]
[最新公开测试摘要](https://github.com/shisuidata/Forge/blob/main/docs/archive/engineering/latest-test-report-2026-10-14.md)记录的是 2026-10-04 本地合成环境评估，仍保留 Direct SQL 授权检查与 Pi 跨主体事件去重隔离两项 P0 发布阻断。不要将该快照接入真实生产或共享多租户数据。编译成功、获得授权和 Evidence 完整性通过都不证明业务答案正确。
:::

## 可选生成路径：Forge JSON 示例

以「统计复购率，复购用户定义为下过 2 次及以上订单的用户」为例：

### Step 1 — Registry 构建系统 Prompt

在获准的隔离测试数据源上，`forge sync` 可读取结构并采样低基数列枚举值；公开源码默认只输出 drift proposal，修改 Registry 需要审阅后显式 `--apply`。结构与样本值本身也可能敏感，发送给模型前必须确认数据权限与 Provider 边界。下面均为合成电商示意：

```
Database schema:
  users: id, name, city, created_at, is_vip[0/1]
  orders: id, user_id, status[cancelled/completed], total_amount, created_at
  order_items: id, order_id, product_id, quantity, unit_price
  products: id, name, category[Books/Clothing/Electronics], cost_price
```

`status[cancelled/completed]` 提供字符串拼写提示，但不保证模型不会选错值或误解业务口径。

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

在固定编译器版本与方言下，同样的 Forge JSON 产生同样的 SQL。`_expand_aliases()` 仅展开本层表达式中的未限定 agg/window 列引用。SQLGlot AST 定位原文后单次替换，保留限定源列、常量、函数/类型名和子查询内部作用域；CTE缺失输出不猜测补齐。

### Step 4 — 执行门禁、审核与解释

编译产物仍是候选，不会因为生成了 SQL 就直接执行。Enforce 将实际 SQL、上下文与版本绑定到 QueryRun；公开 v1 合成演示等待终端批准，本地 v2 候选进一步引入独立主体与指定审核人。满足执行条件后才使用只读数据库连接执行，并施加超时和结果上限。Explain 回读同一运行的 SQL、结果和限制；任何候选修改或绑定漂移都不能复用旧批准。

---

## DSL 能力

| 特性 | 详情 |
|---|---|
| **JOIN 类型** | `inner / left / right / full / anti / semi / cross`，类型必须显式声明 |
| **anti join** | 显式表达排除关系，避免相应 `NOT IN` 子查询的 NULL 陷阱；仍需验证业务含义 |
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

### 压缩示意（不是客户实测）

| 场景 | 全量 schema token | 检索精简后 | 减少 |
|---|---|---|---|
| 4 张表，top_k=5 | ~230 | ~230（自动全取） | 0% |
| 50 张表，top_k=5 | ~2,800 | ~560 | **~80%** |

这些数值仅说明裁剪 Schema 的可能量级，不是企业客户或生产采用证据。top-k 也可能漏掉必要的 JOIN 表，必须同时评估召回与最终查询质量；检索裁剪不是授权边界。

---

> 本页为精简版。完整内容参见 [docs/how-it-works.md](https://github.com/shisuidata/Forge/blob/main/docs/how-it-works.md)。
