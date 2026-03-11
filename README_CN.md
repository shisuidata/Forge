# Forge

**面向 AI Agent 的查询语言，确定性编译为 SQL。**

[English](README.md)

---

> 让 AI 操作「意图」而不是「执行细节」

Forge 是自然语言与 SQL 之间的防抖层，由机器生成、确定性编译：

```
自然语言
    ↓  LLM 生成
  Forge DSL        ← AI 操作这一层
    ↓  Schema 校验 + 语义校验
    ↓  确定性编译
  SQL（目标方言）
    ↓  执行引擎
   查询结果
```

## 解决的问题

| Text-to-SQL 的痛点 | Forge 的应对 |
|---|---|
| 字段名/表名幻觉 | Schema 注册，只允许引用已知实体 |
| JOIN 类型推断错误 | 强制显式声明，无默认值 |
| 方言语法差异 | 编译器统一处理，DSL 无方言 |
| 错误在执行时才发现 | 解析阶段即可校验 |
| 业务语义歧义 | 预定义指标层，名称唯一映射 |

## 工作原理

LLM 通过 Structured Output 生成 Forge JSON——token 级别的约束让格式错误在生成阶段就不可能出现。编译器随后确定性地转换为 SQL，人工审核后再执行。

```json
{
  "scan": "users",
  "joins": [{"type": "left", "table": "orders", "on": {"left": "users.id", "right": "orders.user_id"}}],
  "group": ["users.id", "users.name"],
  "agg":   [{"fn": "count", "col": "orders.id", "as": "order_count"}],
  "select": ["users.name", "order_count"]
}
```

编译结果：

```sql
SELECT users.name, COUNT(orders.id) AS order_count
FROM users
LEFT JOIN orders ON users.id = orders.user_id
GROUP BY users.id, users.name
```

## 项目结构

```
forge/
  schema.json     — Forge DSL 格式定义（JSON Schema）
  compiler.py     — Forge JSON → SQL 编译器
  cli.py          — 命令行入口
tests/
  test_compiler.py
  text-to-sql-failures/   — AI 生成 SQL 的典型失败案例（设计靶心）
schema.registry.json      — 数据库 Schema 注册表
```

## 快速开始

```bash
pip install jsonschema
echo '{"scan":"orders","select":["orders.id","orders.status"]}' | PYTHONPATH=. python3 -m forge.cli -
```

## 状态

🌱 早期设计阶段
