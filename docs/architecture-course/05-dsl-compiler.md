---
title: 05｜Forge DSL 与确定性编译
summary: 理解中间表示、校验、coerce、alias 展开与方言适配
---

# Forge DSL 与确定性编译

## 1. DSL 不是给人手写的“简化 SQL”

Forge DSL 是面向机器生成的 JSON 中间表示（IR）：

```text
自然语言 → Forge JSON → SQL
            概率系统    确定性系统
```

它的价值不是比 SQL 更短，而是把“理解意图”和“生成可执行语法”分离。

## 2. 字段接近 SQL 的执行语义

| 执行阶段 | DSL | SQL |
|---|---|---|
| 数据来源 | `scan` | FROM |
| 关系组合 | `joins` | JOIN |
| 行过滤 | `filter` | WHERE |
| 分组/聚合 | `group` / `agg` | GROUP BY / aggregate |
| 聚合过滤 | `having` | HAVING |
| 窗口 | `window` | OVER |
| 窗口后过滤 | `qualify` | 包装子查询/QUALIFY 语义 |
| 输出 | `select` | SELECT |
| 排序分页 | `sort` / `limit` / `offset` | ORDER BY/LIMIT/OFFSET |

## 3. 编译管线

```text
输入 JSON
  → deepcopy 与 _coerce
  → JSON Schema validation
  → CTE/集合运算递归编译
  → 聚合与窗口表达式构建
  → alias 展开
  → 方言日期/字符串/连接能力检查
  → SQL 字符串
```

`_coerce` 用来兼容模型常见但可无歧义修正的格式偏差。它不是“猜业务”，只应做确定性修复。无法无歧义修正时必须报错并反馈模型。

## 4. 三个系统性案例

### 4.1 Anti Join 与 NULL

```sql
-- 风险写法：子查询出现 NULL 时可能得到意外空集
WHERE user_id NOT IN (SELECT user_id FROM refunds)
```

DSL 用 `type: "anti"` 明确表达排除关系，Compiler 生成 anti/NOT EXISTS 语义，避免模型自由选择危险模式。

### 4.2 WHERE 与 HAVING

行条件进入 `filter`，聚合结果条件进入 `having`。字段位置让 Compiler 决定 SQL 子句，减少将 `COUNT(*)` 放进 WHERE 的错误。

### 4.3 Alias 作用域

```sql
SELECT COUNT(*) AS cnt, cnt * 2 AS double_cnt
```

许多数据库不允许同层 SELECT 引用同层 alias。`_expand_aliases()` 将 `cnt` 展开为原表达式，再生成合法 SQL。

## 5. 能力边界

当前 DSL 包含：

- `inner/left/right/full/anti/semi/cross` JOIN；
- 聚合、条件聚合、GROUP BY、HAVING；
- 排名、导航和聚合窗口、frame、qualify；
- CTE 与 recursive CTE；
- UNION/INTERSECT/EXCEPT；
- IN 子查询、日期字面量与相对日期；
- SQLite、PostgreSQL、MySQL 主要方言，以及 BigQuery/Snowflake 编译路径。

### CROSS JOIN 的真实边界

旧文档曾写“刻意不支持 CROSS JOIN”，但当前 [`schema.json`](https://github.com/shisuidata/Forge/blob/main/forge/schema.json) 明确允许 `cross`。它用于引用标量 CTE（如“与全局均值比较”），不要求 `on`。这不意味着鼓励任意笛卡儿积：Lint、示例和审核仍应限制其使用。

### 表达式透传

`select.expr`、部分聚合表达式和 CASE 能力保留字符串表达力。因此不能宣称所有合法 DSL 都完全杜绝字段幻觉或 SQL 注入式表达式风险；最终仍要 Compiler 校验、只读检查、审核和数据库权限。

## 6. 动态 Schema 的约束强弱

`schema_builder.py` 会把已知表和列做成枚举，但并非所有位置都能严格枚举：

- `scan`、`joins.table`、多数 `col` 可严格约束；
- 聚合或计算表达式为支持 CASE/算术，可能保留 string；
- 枚举字段值目前主要作为描述提示，并非所有值都形成 token 枚举；
- Provider 必须正确支持 tools/JSON Schema，约束才能在生成时生效。

因此更准确的表述是：

> 在动态 Schema 覆盖且 Provider 严格执行 Structured Output 的字段上，非法候选可在生成阶段被阻止；其余字段由校验、编译、lint 和审核继续兜底。

## 7. 方言支持不等于数据库交付支持

Compiler 能生成某种方言，只证明“翻译器存在”。生产支持还需要：

1. `forge sync` 能正确 introspect；
2. Executor 能连接并设置资源限制；
3. 方言特性有测试或真实 smoke；
4. 客户查询集通过；
5. 有数据库权限、备份和运维方案。

详情见[兼容性矩阵](https://github.com/shisuidata/Forge/blob/main/docs/compatibility-matrix.md)。

## 8. 深入阅读

- [DSL 形式化语义](https://github.com/shisuidata/Forge/blob/main/docs/dsl-semantics.md)
- [`forge/compiler.py`](https://github.com/shisuidata/Forge/blob/main/forge/compiler.py)
- [`tests/test_compiler.py`](https://github.com/shisuidata/Forge/blob/main/tests/test_compiler.py)
