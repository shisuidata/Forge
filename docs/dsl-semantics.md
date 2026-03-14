# Forge DSL 形式化语义

> 本文档描述 Forge DSL 的表达边界、不可能性保证、与关系代数的对应关系。

---

## 一、Forge DSL 是什么

Forge DSL 是一种**机器生成、确定性编译**的查询中间表示（Intermediate Representation, IR）。

它不是 SQL 的语法糖，也不是简化版 SQL——它是一个**语义层**，与 SQL 的关系是：

```
自然语言  →  Forge DSL（JSON）  →  SQL（确定性）
```

LLM 生成 Forge DSL，编译器将其翻译为 SQL。两个步骤的职责严格分离：

| 组件 | 职责 | 特点 |
|---|---|---|
| LLM | 理解用户意图，生成 Forge DSL | 有创造性，可能出错 |
| 编译器 | Forge DSL → SQL | 确定性，无随机性，可测试 |

---

## 二、与 SQL 执行语义的对应

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
| 8 | QUALIFY（per-group filter） | `qualify` |
| 9 | SELECT | `select` |
| 10 | ORDER BY | `sort` |
| 11 | LIMIT / OFFSET | `limit`, `offset` |

**这消除了 SQL 中书写顺序与执行顺序不一致导致的认知错误。**

SQL 的书写顺序是 SELECT → FROM → WHERE → GROUP BY → HAVING → ORDER BY，
但实际执行顺序恰好相反。这是 SQL 最常见的初学者错误来源之一（Chamberlin 1974 的设计取舍）。
Forge DSL 强制要求按执行语义组织，LLM 无法写出执行顺序错误的查询。

---

## 三、表达能力：能表达什么

Forge DSL 覆盖"无歧义的声明式关系查询"的核心子集。下表列出能力边界：

### 3.1 关系运算

| 关系代数操作 | SQL 对应 | Forge DSL | 备注 |
|---|---|---|---|
| 选择（σ） | WHERE | `filter` | 支持比较、范围、IN、LIKE、IS NULL、OR/AND 嵌套 |
| 投影（π） | SELECT | `select` | 支持列引用、计算表达式、别名 |
| 笛卡儿积 × | CROSS JOIN | 不支持 | 刻意省略，强迫声明 JOIN 意图 |
| 内连接（⋈） | INNER JOIN | `joins[type=inner]` | 支持等值和多条件 |
| 左外连接 | LEFT JOIN | `joins[type=left]` | |
| 右外连接 | RIGHT JOIN | `joins[type=right]` | |
| 全外连接 | FULL OUTER JOIN | `joins[type=full]` | MySQL 不支持，编译时报错 |
| 反连接（not exists） | NOT IN / NOT EXISTS | `joins[type=anti]` | 编译为 LEFT JOIN + IS NULL，消灭 NULL 陷阱 |
| 半连接（exists） | EXISTS | `joins[type=semi]` | 编译为 WHERE EXISTS |
| 聚合（γ） | GROUP BY + agg | `group` + `agg` | 支持 count/sum/avg/min/max/distinct/concat |
| 集合并 | UNION / UNION ALL | `union` | |
| 集合交 | INTERSECT | `intersect` | |
| 集合差 | EXCEPT | `except` | |

### 3.2 超越基础关系代数的能力

| 能力 | SQL 对应 | Forge DSL | 备注 |
|---|---|---|---|
| 窗口函数 | OVER (PARTITION BY ... ORDER BY ...) | `window` | 排名/分布/聚合/导航四类 |
| 窗口帧 | ROWS/RANGE BETWEEN ... AND ... | `window.frame` | 支持滑动平均、累计求和 |
| per-group TopN | QUALIFY / 子查询 | `qualify` | 编译为包装子查询 |
| 公共表表达式 | WITH ... AS | `cte` | 支持递归 CTE |
| 去重 | SELECT DISTINCT | `distinct: true` | |
| IN 子查询 | col IN (SELECT ...) | `filter[op=in, val.subquery]` | |
| 相对日期 | 方言相关函数 | `$preset` | 8 种预设，编译器适配方言 |
| 条件聚合 | SUM(...) FILTER (WHERE ...) | `agg.filter` | SQLite/PG；MySQL/BQ/SF 不支持 |
| 日期截断分组 | STRFTIME / DATE_TRUNC | `group[GroupExpr]` | 表达式透传，alias 在 SELECT 中引用 |

### 3.3 不支持的能力（刻意排除）

| 能力 | 原因 |
|---|---|
| CROSS JOIN | 强制要求声明 JOIN 意图，消灭意外笛卡儿积 |
| 裸子查询（非 IN/EXISTS） | 用 CTE 代替，结构更清晰可读 |
| DDL（CREATE/ALTER/DROP） | Forge 是只读查询语言 |
| DML（INSERT/UPDATE/DELETE） | 同上 |
| 存储过程 / 函数定义 | 超出查询范围 |
| PIVOT / UNPIVOT | 复杂度过高，暂未支持 |
| LATERAL JOIN | 暂未支持 |

---

## 四、不可能性保证（Impossibility Guarantees）

这是 Forge DSL 最核心的价值主张。以下错误类型**在物理层面不可能出现**在合法的 Forge DSL 中：

### 4.1 无类型 JOIN

SQL 允许：
```sql
SELECT * FROM orders JOIN users ON orders.user_id = users.id
```
这等价于 INNER JOIN，但 LLM 经常在应该用 LEFT JOIN 时写成这种形式。

Forge DSL 中，`type` 字段是枚举值且必填（JSON Schema 强制），不存在"无类型 JOIN"。

### 4.2 NOT IN + NULL 陷阱

SQL 中：
```sql
SELECT * FROM orders WHERE user_id NOT IN (SELECT id FROM banned_users)
-- 如果 banned_users 包含 NULL 行，结果集为空（静默！）
```

Forge DSL 中不存在 `NOT IN` 原语。反连接只能通过 `type: "anti"` 表达，
编译器将其翻译为 `LEFT JOIN ... WHERE right_key IS NULL`，消灭 NULL 陷阱。

### 4.3 幻觉列名/表名

SQL 中，LLM 可以生成任何字符串作为列名（`SELECT orders.amount` 当列名实际是 `total_amount`）。

Forge DSL 通过 `schema_builder.py` 动态注入 Registry 中的合法枚举值到 JSON Schema，
Structured Output 在 token 生成层强制枚举约束——不在 Registry 中的列名**在生成阶段就被阻止**。

### 4.4 WHERE vs HAVING 混淆

SQL 中，将聚合条件放在 WHERE 子句（`WHERE COUNT(*) > 5`）会产生语法错误或逻辑错误。

Forge DSL 有 `filter`（WHERE）和 `having`（HAVING）两个独立字段，
编译器根据字段位置确定性地生成正确的子句。混淆不可能发生。

### 4.5 GROUP BY 非聚合列歧义

SQL 中（MySQL 旧版），`SELECT * FROM t GROUP BY city` 是合法的但返回不确定行，
而在 PostgreSQL 中同样的 SQL 会直接报错。

Forge DSL 的 `_coerce` Fix 5 在编译前自动将 SELECT 中的非聚合列补齐到 GROUP BY，
确保跨方言行为一致。

### 4.6 Alias 作用域错误

SQL 中：
```sql
SELECT COUNT(*) AS cnt, cnt * 2 AS double_cnt  -- 错误：cnt 未定义
FROM orders
```

Forge DSL 的 `_expand_aliases()` 在编译前将 SELECT expr 中引用的 agg/window 别名
展开为完整表达式，消灭整类 alias 作用域错误。

---

## 五、与 SemQL/IRNet 的对比

Forge DSL 与 2019 年 ACL 论文 IRNet 提出的 SemQL 在设计哲学上高度一致：
都用"中间语义表示 + 确定性编译"路径取代"直接生成 SQL"。

| 维度 | SemQL（IRNet 2019） | Forge DSL |
|---|---|---|
| 表达形式 | 树状 S-expression | JSON（LLM 友好） |
| 约束方式 | 文法规则（特殊 token） | JSON Schema + Structured Output |
| 编译方式 | 规则映射 | Python 确定性编译器 |
| 容错机制 | 无 | 14 个 `_coerce` 修复 |
| 方言支持 | 无 | SQLite / MySQL / PostgreSQL / BigQuery / Snowflake |
| Registry 集成 | 无 | 内置（结构层 + 语义层） |
| 目标场景 | 学术 benchmark | 企业私有化部署 |

IRNet 在 Spider benchmark 上比直接生成 SQL 提升了 19.5 个百分点——这从学术角度验证了 IR 方法的有效性，Forge DSL 是同一理念的工程化实现。

---

## 六、方言适配矩阵

| 特性 | SQLite | MySQL | PostgreSQL | BigQuery | Snowflake |
|---|---|---|---|---|---|
| $preset 日期 | ✅ DATE('now',...) | ✅ CURDATE()/DATE_SUB | ✅ CURRENT_DATE/INTERVAL | ✅ CURRENT_DATE()/DATE_SUB | ✅ CURRENT_DATE()/DATEADD |
| 字符串聚合 | ✅ GROUP_CONCAT | ✅ GROUP_CONCAT SEPARATOR | ✅ STRING_AGG | ✅ STRING_AGG | ✅ LISTAGG |
| FILTER (WHERE) | ✅ | ❌ | ✅ | ❌ | ❌ |
| FULL OUTER JOIN | ✅ | ❌ | ✅ | ✅ | ✅ |
| RIGHT JOIN | ✅ | ✅ | ✅ | ❌ | ✅ |
| GROUP BY date expr | STRFTIME | DATE_FORMAT | DATE_TRUNC | DATE_TRUNC(..., MONTH) | DATE_TRUNC('month',...) |

---

## 七、语义完备性的边界

Forge DSL 的目标不是覆盖 SQL 的全部能力，而是覆盖**"企业日常分析查询"的 80% 场景**，同时在这个范围内提供生成错误的物理不可能性保证。

Spider 2.0（ICLR 2025 Oral）的研究表明，当前最强模型在真实企业查询上仍只有 21% 的准确率——Forge DSL 的方向是：在可覆盖的查询子集上达到接近 100% 的生成正确率，而不是追求覆盖所有 SQL 特性但每种都有错误风险。

这是一个有意识的设计取舍：**已知边界内的确定性，优于未知范围内的概率性。**
