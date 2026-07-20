---
title: DSL 语义
description: Forge DSL 的形式化定义、能力边界与不可能性保证
---

## Forge DSL 是什么

Forge DSL 是一种**机器生成、确定性编译**的查询中间表示（IR）。LLM 生成 Forge DSL（JSON），编译器将其翻译为 SQL。两步职责严格分离。

---

## 与 SQL 执行语义的对应

Forge DSL 的字段顺序直接对应 SQL 的**执行顺序**（而不是书写顺序）：

| 执行阶段 | SQL 子句 | Forge DSL 字段 |
|---|---|---|
| 1 | FROM | `scan` |
| 2 | JOIN | `joins` |
| 3 | WHERE | `filter` |
| 4 | GROUP BY | `group` |
| 5 | Aggregation | `agg` |
| 6 | HAVING | `having` |
| 7 | Window functions | `window` |
| 8 | QUALIFY | `qualify` |
| 9 | SELECT | `select` |
| 10 | ORDER BY | `sort` |
| 11 | LIMIT / OFFSET | `limit`, `offset` |

这消除了 SQL 中书写顺序与执行顺序不一致导致的认知错误。

---

## 表达能力

### 关系运算

| 关系代数操作 | Forge DSL | 备注 |
|---|---|---|
| 选择（WHERE） | `filter` | 比较、范围、IN、LIKE、IS NULL、OR/AND |
| 投影（SELECT） | `select` | 列引用、表达式、别名 |
| 内连接 | `joins[type=inner]` | 等值和多条件 |
| 左/右/全外连接 | `joins[type=left/right/full]` | MySQL 不支持 FULL |
| 反连接 | `joins[type=anti]` | 编译为 LEFT JOIN + IS NULL |
| 半连接 | `joins[type=semi]` | 编译为 WHERE EXISTS |
| 聚合 | `group` + `agg` | count/sum/avg/min/max/distinct/concat |
| 集合运算 | `union / intersect / except` | |

### 超越基础关系代数

| 能力 | Forge DSL | 备注 |
|---|---|---|
| 窗口函数 | `window` | 排名/分布/聚合/导航四类 |
| 窗口帧 | `window.frame` | 滑动平均、累计求和 |
| per-group TopN | `qualify` | 编译为包装子查询 |
| CTE | `cte` | 支持递归 |
| IN 子查询 | `filter[op=in, val.subquery]` | |
| 相对日期 | `$preset` | 8 种预设，编译器适配方言 |
| 条件聚合 | `agg.filter` | SQLite/PG 支持 |

### 刻意排除

| 能力 | 原因 |
|---|---|
| CROSS JOIN | 强制声明 JOIN 意图，消灭意外笛卡儿积 |
| DDL / DML | Forge 是只读查询语言 |
| 存储过程 | 超出查询范围 |
| PIVOT / LATERAL JOIN | 暂未支持 |

---

## 不可能性保证

以下错误类型**在物理层面不可能出现**在合法的 Forge DSL 中：

| 错误类型 | SQL 中的问题 | Forge 如何消灭 |
|---|---|---|
| **无类型 JOIN** | 裸 `JOIN` 等价于 INNER，但意图可能是 LEFT | `type` 字段是必填枚举值 |
| **NOT IN + NULL** | 子查询含 NULL 时结果集静默为空 | `anti` join 编译为 LEFT JOIN + IS NULL |
| **幻觉列名/表名** | LLM 生成不存在的列名 | Structured Output 在 token 生成层强制枚举 |
| **WHERE vs HAVING 混淆** | 聚合条件放在 WHERE | `filter` 和 `having` 是独立字段 |
| **GROUP BY 歧义** | MySQL/PG 跨方言行为不一致 | `_coerce` 自动补齐 GROUP BY |
| **Alias 作用域错误** | 同层 SELECT 中引用 agg alias | `_expand_aliases()` 自动展开 |

---

## 方言适配

| 特性 | SQLite | MySQL | PostgreSQL |
|---|---|---|---|
| $preset 日期 | DATE('now',...) | CURDATE()/DATE_SUB | CURRENT_DATE/INTERVAL |
| 字符串聚合 | GROUP_CONCAT | GROUP_CONCAT SEPARATOR | STRING_AGG |
| FILTER (WHERE) | 支持 | 不支持 | 支持 |
| FULL OUTER JOIN | 支持 | 不支持 | 支持 |

---

## 与 SemQL/IRNet 的对比

| 维度 | SemQL（IRNet 2019） | Forge DSL |
|---|---|---|
| 表达形式 | 树状 S-expression | JSON（LLM 友好） |
| 约束方式 | 文法规则 | JSON Schema + Structured Output |
| 容错机制 | 无 | 14 个 `_coerce` 修复 |
| 方言支持 | 无 | SQLite / MySQL / PG / BQ / SF |
| 目标场景 | 学术 benchmark | 企业私有化部署 |

IRNet 在 Spider benchmark 上比直接生成 SQL 提升 19.5pp。Forge DSL 是同一理念的工程化实现。

---

> 本页为精简版。完整内容参见 [docs/dsl-semantics.md](https://github.com/shisuidata/Forge/blob/main/docs/dsl-semantics.md)。
