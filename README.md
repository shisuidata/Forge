# Forge

**An Agent-Native DSL that compiles deterministically to SQL.**

[中文文档](README_CN.md)

---

> Let AI operate on **intent**, not **execution details**.

Forge sits between natural language and SQL as a stable, machine-generated intermediate layer:

```
Natural Language
    ↓  LLM generates
  Forge DSL        ← AI operates here
    ↓  Schema validation + semantic validation
    ↓  Deterministic compilation
  SQL (target dialect)
    ↓  Query engine
  Results
```

## Why Forge

| Text-to-SQL pain point | Forge's answer |
|---|---|
| Field/table hallucinations | Schema registry — only known entities allowed |
| Wrong JOIN type inferred | Explicit declaration required, no defaults |
| Dialect syntax differences | Compiler handles all dialects, DSL is dialect-free |
| Errors only caught at execution | Caught at parse/compile time |
| Business metric ambiguity | Pre-defined metric layer with unique name mapping |

## How It Works

The LLM generates Forge JSON using [Structured Output](https://platform.openai.com/docs/guides/structured-outputs) — token-level constraints make it physically impossible to produce malformed queries. The compiler then translates deterministically to SQL, which a human reviews before execution.

```json
{
  "scan": "users",
  "joins": [{"type": "left", "table": "orders", "on": {"left": "users.id", "right": "orders.user_id"}}],
  "group": ["users.id", "users.name"],
  "agg":   [{"fn": "count", "col": "orders.id", "as": "order_count"}],
  "select": ["users.name", "order_count"]
}
```

Compiles to:

```sql
SELECT users.name, COUNT(orders.id) AS order_count
FROM users
LEFT JOIN orders ON users.id = orders.user_id
GROUP BY users.id, users.name
```

## Project Structure

```
forge/
  schema.json     — Forge DSL format definition (JSON Schema)
  compiler.py     — Forge JSON → SQL compiler
  cli.py          — CLI entry point
tests/
  test_compiler.py
  text-to-sql-failures/   — Real AI SQL failure cases (design targets)
schema.registry.json      — Database schema for validation
```

## Quick Start

```bash
pip install jsonschema
echo '{"scan":"orders","select":["orders.id","orders.status"]}' | PYTHONPATH=. python3 -m forge.cli -
```

## Status

🌱 Early design stage
