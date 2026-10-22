---
title: DSL 语义
description: Forge JSON 的查询语义、结构约束与不能据此推导的保证
---

## Forge DSL 是什么

Forge DSL 是一种**机器生成、确定性编译**的查询中间表示（IR）。编译器将支持范围内的 Forge JSON 翻译为 SQL；Direct SQL 是另一种候选格式，不必经过 DSL。两者都仍需评估与执行门禁，见[架构设计](/reference/architecture/)。

---

## 与 SQL 执行语义的对应

Forge DSL 的字段对应 SQL 的**逻辑求值阶段**（不是数据库优化器的物理执行顺序；JSON 对象键的排列顺序也不决定执行顺序）：

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

这种组织方式有助于区分行过滤、聚合和窗口阶段，但不能消除模型的业务或算法错误。

---

## 表达能力

### 关系运算

| 关系代数操作 | Forge DSL | 备注 |
|---|---|---|
| 选择（WHERE） | `filter` | 比较、范围、IN、LIKE、IS NULL、OR/AND |
| 投影（SELECT） | `select` | 列引用、表达式、别名 |
| 内连接 | `joins[type=inner]` | 等值和多条件 |
| 左/右/全外连接 | `joins[type=left/right/full]` | MySQL 不支持 FULL |
| 反连接 | `joins[type=anti]` | 编译为安全的反连接语义 |
| 半连接 | `joins[type=semi]` | 编译为 WHERE EXISTS |
| 显式笛卡儿积 | `joins[type=cross]` | 用于引用标量 CTE；无 `on`，不作为普通 JOIN 默认值 |
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
| 隐式/无意图的笛卡儿积 | 只允许显式 `cross` 场景，合理性仍由 lint、审核与测试判断 |
| DDL / DML | Forge 是只读查询语言 |
| 存储过程 | 超出查询范围 |
| PIVOT / LATERAL JOIN | 暂未支持 |

---

## 结构约束与边界（不是不可能性证明）

动态 Schema 与严格 Structured Output 可以约束相应字段的结构和枚举；表达式、Provider 兼容性、业务口径、源数据和算法不在此保证内。合法 JSON、可编译 SQL 都不证明安全或语义正确。

| 错误类型 | SQL 中的问题 | 约束如何帮助，仍有什么边界 |
|---|---|---|
| **无类型 JOIN** | 裸 JOIN 默认 INNER，意图可能是 LEFT | `type` 必填可要求显式选择，但仍可能选错类型 |
| **NOT IN + NULL** | 子查询含 NULL 时可能静默空集 | `anti` 表达反连接语义，不能替代业务意图判断 |
| **幻觉列名/表名** | 引用不存在的表列 | 严格动态 Schema 约束覆盖的位置；字符串表达式仍需检查 |
| **WHERE vs HAVING** | 聚合条件误入 WHERE | `filter` 和 `having` 分字段，模型仍可能误填 |
| **GROUP BY** | 方言与聚合粒度差异 | 部分确定性规范化可修正结构，但不能猜测正确业务粒度 |
| **同层聚合别名引用** | 同层 SELECT 引用 alias 可能无效 | 仅展开本层未限定引用，不修复缺失的 CTE 输出 |

`_expand_aliases()` 仅展开本层表达式中的未限定 agg/window 列引用。SQLGlot AST 定位原文后单次替换，保留限定源列、常量、函数/类型名和子查询内部作用域；CTE缺失输出不猜测补齐。

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
| 容错机制 | 无 | 部分确定性规范化；需行为回归验证 |
| 方言支持 | 无 | SQLite / MySQL / PG，以及 BQ / SF 编译路径 |
| 目标场景 | 学术 benchmark | 受控数据查询；不代表已有生产采用 |

这是一项历史设计思路对照，不是同模型、同数据的性能比较，也不能据此推导 Forge 的准确率优势。实际成绩与安全发布阻断见[基准与验证边界](/reference/benchmarks/)。

---

> 本页为精简版。完整内容参见 [docs/dsl-semantics.md](https://github.com/shisuidata/Forge/blob/main/docs/dsl-semantics.md)。
