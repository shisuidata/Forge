---
name: forge-query
description: Use this skill when the user asks a natural-language data question against a known Forge Registry. Generate Forge JSON, compile it deterministically to SQL, and stop for review unless execution is explicitly authorized.
---

# Forge Query

Translate a natural-language data question into Forge JSON, compile it to SQL, and present the exact SQL for review.

## Preconditions

1. Work from the Forge repository root.
2. Read `registry/data/schema.registry.json` and the relevant semantic Registry files.
3. Use only registered tables, columns, relationships, metrics, and disambiguation rules.
4. If the Registry does not support the requested meaning, ask for clarification or fail explicitly. Do not invent schema or business semantics.

## Workflow

1. Parse the user's intent and identify missing semantics.
2. Generate Forge JSON using the supported DSL.
3. Write it to a temporary JSON file.
4. Compile with:

   ```bash
   .venv/bin/python -m forge.cli compile /tmp/forge-query.json
   ```

5. If compilation fails, fix the Forge JSON and retry at most twice.
6. Show the compiled SQL and relevant assumptions.
7. Stop before execution unless the user explicitly authorizes the normal Forge review/execution flow. Never bypass approval or execute directly against a database.

## Core Forge JSON Shape

```json
{
  "scan": "table",
  "joins": [
    {
      "type": "inner|left|right|full|anti|semi",
      "table": "other_table",
      "on": {"left": "table.id", "right": "other_table.table_id"}
    }
  ],
  "filter": [
    {"col": "table.column", "op": "eq|neq|gt|gte|lt|lte|in|like|is_null|is_not_null|between", "val": "value"}
  ],
  "group": ["table.column"],
  "agg": [
    {"fn": "count|count_all|count_distinct|sum|avg|min|max", "col": "table.column", "as": "alias"}
  ],
  "having": [{"col": "alias", "op": "gt", "val": 0}],
  "select": ["table.column", "alias"],
  "sort": [{"col": "alias", "dir": "asc|desc"}],
  "limit": 100
}
```

## Guardrails

- JOIN `type` and sort `dir` are required.
- Use `filter` for row-level predicates and `having` for aggregate predicates.
- `count_all` has no `col`; other aggregate functions require one.
- Qualify columns with `table.column` when joins are present.
- Avoid `NOT IN` + `NULL`; use a supported anti-join when semantics match.
- `between` uses `lo` and `hi`, not `val`.
- A valid JSON Schema shape is not evidence that the business meaning is correct.
