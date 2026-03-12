"""
动态 Tool Schema 生成器。

核心思路（Slot Filling in Constrained State Space）：
    LLM 的错误率 ∝ 输出空间大小。
    把 col/table 字段从自由字符串变成 schema-derived 枚举，
    使格式错误从概率问题变成不可能事件。

约束策略：
    ┌──────────────────────────────────┬────────────────────────────────────┐
    │ 字段                              │ 约束方式                            │
    ├──────────────────────────────────┼────────────────────────────────────┤
    │ scan / joins.table               │ 严格枚举（已知表名）                 │
    │ filter/group/sort/window 的 col  │ 严格枚举（table.col 全集）           │
    │ agg.col                          │ 保持 string（支持算术表达式）         │
    │ agg.fn / join.type / sort.dir    │ 严格枚举（固定值域）                  │
    │ val                              │ oneOf 类型系统（string/number/bool/  │
    │                                  │   null/array/$date/$preset）        │
    │ select                           │ oneOf string | expr 对象             │
    └──────────────────────────────────┴────────────────────────────────────┘

接口：
    build_tool_schema(registry) -> dict
        registry: schema.registry.json 解析后的 dict
        返回：generate_forge_query 工具的 input_schema dict
"""
from __future__ import annotations


def _parse_registry(registry: dict) -> tuple[list[str], list[str], dict[str, list]]:
    """
    解析 registry，返回：
        table_names:  所有表名列表
        col_refs:     所有 "table.col" 引用列表
        col_enums:    {"table.col": [合法值, ...]}，仅含有枚举定义的列
    """
    tables_info = registry.get("tables", registry)
    table_names: list[str] = list(tables_info.keys())
    col_refs: list[str] = []
    col_enums: dict[str, list] = {}

    for table, info in tables_info.items():
        cols = info.get("columns", info) if isinstance(info, dict) else info

        if isinstance(cols, dict):
            # 新格式：{"col_name": {"enum": [...], ...}}
            for col_name, meta in cols.items():
                ref = f"{table}.{col_name}"
                col_refs.append(ref)
                if isinstance(meta, dict) and meta.get("enum"):
                    col_enums[ref] = meta["enum"]
        else:
            # 旧格式（forge sync 输出）：["col1", "col2", ...]
            for col_name in cols:
                col_refs.append(f"{table}.{col_name}")

    return table_names, col_refs, col_enums


def build_tool_schema(registry: dict) -> dict:
    """
    从 schema registry 动态生成 generate_forge_query 工具的 input_schema。

    Args:
        registry: schema.registry.json 解析后的 dict

    Returns:
        JSON Schema dict，可直接用作 Anthropic tool 的 input_schema
        或 OpenAI function 的 parameters。
    """
    table_names, col_refs, col_enums = _parse_registry(registry)

    # ── val 的 oneOf 类型系统 ───────────────────────────────────────────────────
    # 把已知枚举值注入到 string 分支的 description，给模型以提示但不强制约束
    enum_hint = ""
    if col_enums:
        parts = [f"{col}: {vals}" for col, vals in col_enums.items()]
        enum_hint = f" Known enum values — {'; '.join(parts)}."

    scalar_val = {
        "oneOf": [
            {"type": "string",
             "description": "String value." + enum_hint},
            {"type": "number"},
            {"type": "boolean"},
            {"type": "null",
             "description": "Use for LAG/LEAD default when no prior/next row should yield NULL."},
            {"type": "array",
             "description": "List of values for 'in' operator.",
             "items": {"type": ["string", "number"]}},
            {"type": "object",
             "required": ["$date"],
             "additionalProperties": False,
             "description": "Date literal.",
             "properties": {
                 "$date": {"type": "string",
                           "description": "ISO-8601 date: YYYY-MM-DD"}
             }},
            {"type": "object",
             "required": ["$preset"],
             "additionalProperties": False,
             "description": "Relative date preset.",
             "properties": {
                 "$preset": {
                     "type": "string",
                     "enum": ["today", "yesterday", "last_7_days", "last_30_days",
                              "this_month", "last_month", "this_quarter", "this_year"]
                 }
             }},
        ]
    }

    bound_val = {
        "oneOf": [
            {"type": "number"},
            {"type": "string"},
            {"type": "object",
             "required": ["$date"],
             "additionalProperties": False,
             "properties": {"$date": {"type": "string"}}}
        ]
    }

    # ── 基础 schema 块 ─────────────────────────────────────────────────────────

    col_field = {
        "type": "string",
        "enum": col_refs,
        "description": "Column reference in table.col format.",
    } if col_refs else {"type": "string"}

    sort_key = {
        "type": "object",
        "required": ["col", "dir"],
        "additionalProperties": False,
        "properties": {
            "col": col_field,
            "dir": {"type": "string", "enum": ["asc", "desc"]},
        },
    }

    simple_condition = {
        "type": "object",
        "required": ["col", "op"],
        "additionalProperties": False,
        "properties": {
            "col": col_field,
            "op": {
                "type": "string",
                "enum": ["eq", "neq", "gt", "gte", "lt", "lte",
                         "in", "like", "is_null", "is_not_null", "between"],
            },
            "val": scalar_val,
            "lo": {**bound_val,
                   "description": "Lower bound for 'between'. Use {\"$date\":\"...\"} for dates."},
            "hi": {**bound_val,
                   "description": "Upper bound for 'between'."},
        },
    }

    and_condition = {
        "type": "object",
        "required": ["and"],
        "additionalProperties": False,
        "properties": {
            "and": {
                "type": "array",
                "items": simple_condition,
                "minItems": 2,
            },
        },
    }

    or_condition = {
        "type": "object",
        "required": ["or"],
        "additionalProperties": False,
        "properties": {
            "or": {
                "type": "array",
                "items": {"oneOf": [simple_condition, and_condition]},
                "minItems": 2,
            },
        },
    }

    condition = {"oneOf": [simple_condition, or_condition]}

    # ── Aggregation ────────────────────────────────────────────────────────────

    agg_with_col = {
        "type": "object",
        "required": ["fn", "col", "as"],
        "additionalProperties": False,
        "properties": {
            "fn": {
                "type": "string",
                "enum": ["count", "count_distinct", "sum", "avg", "min", "max"],
            },
            "col": {
                "type": "string",
                "description": (
                    "Column or arithmetic expression, e.g. 'orders.total_amount' "
                    "or 'order_items.quantity * order_items.unit_price'."
                ),
            },
            "as": {"type": "string"},
        },
    }

    agg_count_all = {
        "type": "object",
        "required": ["fn", "as"],
        "additionalProperties": False,
        "description": "COUNT(*) — no col field. Use when counting all rows, not a specific column.",
        "properties": {
            "fn": {"type": "string", "enum": ["count_all"]},
            "as": {"type": "string"},
        },
    }

    # ── Window functions ───────────────────────────────────────────────────────

    window_ranking = {
        "type": "object",
        "required": ["fn", "as"],
        "additionalProperties": False,
        "description": (
            "Ranking functions — no input column. "
            "row_number: unique integers, no ties. "
            "rank: ties share same number, next rank skips (1,1,3). "
            "dense_rank: ties share same number, no gap (1,1,2)."
        ),
        "properties": {
            "fn": {"type": "string", "enum": ["row_number", "rank", "dense_rank"]},
            "partition": {"type": "array", "items": col_field},
            "order": {"type": "array", "items": sort_key},
            "as": {"type": "string"},
        },
    }

    window_agg = {
        "type": "object",
        "required": ["fn", "col", "as"],
        "additionalProperties": False,
        "description": "Aggregate window: SUM/AVG/COUNT/MIN/MAX OVER (...).",
        "properties": {
            "fn": {"type": "string", "enum": ["sum", "avg", "count", "min", "max"]},
            "col": col_field,
            "partition": {"type": "array", "items": col_field},
            "order": {"type": "array", "items": sort_key},
            "as": {"type": "string"},
        },
    }

    window_nav = {
        "type": "object",
        "required": ["fn", "col", "as"],
        "additionalProperties": False,
        "description": (
            "Navigation: LAG / LEAD. partition is required to avoid cross-user row access. "
            "default: set to a meaningful value (e.g. 'first_order') when the question requires it; "
            "omit or use null when no prior/next row should yield NULL."
        ),
        "properties": {
            "fn": {"type": "string", "enum": ["lag", "lead"]},
            "col": col_field,
            "offset": {"type": "integer", "minimum": 1},
            "default": scalar_val,
            "partition": {"type": "array", "items": col_field},
            "order": {"type": "array", "items": sort_key, "minItems": 1},
            "as": {"type": "string"},
        },
    }

    # ── select item ────────────────────────────────────────────────────────────

    select_item = {
        "oneOf": [
            {
                "type": "string",
                "description": "Column reference, agg alias, or window alias.",
            },
            {
                "type": "object",
                "required": ["expr", "as"],
                "additionalProperties": False,
                "description": "Computed expression, e.g. {\"expr\": \"quantity * unit_price\", \"as\": \"revenue\"}.",
                "properties": {
                    "expr": {
                        "type": "string",
                        "description": "Arithmetic or CASE expression, passed verbatim into SQL.",
                    },
                    "as": {"type": "string"},
                },
            },
        ]
    }

    # ── joins.on: single equality or multi-condition array ─────────────────────

    on_single = {
        "type": "object",
        "required": ["left", "right"],
        "additionalProperties": False,
        "description": "Equality join (most common).",
        "properties": {
            "left": col_field,
            "right": col_field,
        },
    }

    on_multi = {
        "type": "array",
        "description": "Multi-condition join (inner/left/right/full only). Conditions are AND-combined.",
        "items": simple_condition,
        "minItems": 2,
    }

    # ── Top-level schema ───────────────────────────────────────────────────────

    return {
        "type": "object",
        "required": ["scan", "select"],
        "additionalProperties": False,
        "properties": {
            "scan": {
                "type": "string",
                "enum": table_names,
                "description": "Primary table (scan target).",
            },
            "joins": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["type", "table", "on"],
                    "additionalProperties": False,
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": ["inner", "left", "right", "full", "anti", "semi"],
                            "description": "inner=default; anti=NOT IN alternative; semi=EXISTS alternative.",
                        },
                        "table": {"type": "string", "enum": table_names},
                        "on": {"oneOf": [on_single, on_multi]},
                    },
                },
            },
            "filter": {
                "type": "array",
                "description": "Row-level filters (WHERE). Top-level items are AND-combined.",
                "items": condition,
            },
            "group": {
                "type": "array",
                "description": "Group-by keys.",
                "items": col_field,
            },
            "agg": {
                "type": "array",
                "description": "Aggregate expressions. count_all has no col; all others require col.",
                "items": {"oneOf": [agg_with_col, agg_count_all]},
            },
            "having": {
                "type": "array",
                "description": (
                    "Post-aggregate filters (HAVING). "
                    "col must be an agg alias (defined in agg[].as), not a raw column or expression. "
                    "Top-level items are AND-combined."
                ),
                "items": condition,
            },
            "select": {
                "type": "array",
                "description": "Output columns. Can be table.col refs, agg aliases, window aliases, or {\"expr\":\"...\",\"as\":\"alias\"} computed expressions.",
                "items": select_item,
                "minItems": 1,
            },
            "window": {
                "type": "array",
                "items": {"oneOf": [window_ranking, window_agg, window_nav]},
            },
            "qualify": {
                "type": "array",
                "description": "Post-window filter for per-group TopN (e.g. rank <= 3).",
                "items": simple_condition,
                "minItems": 1,
            },
            "sort": {
                "type": "array",
                "items": sort_key,
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "description": "Maximum rows to return. Use the exact number from the user's question.",
            },
            "offset": {
                "type": "integer",
                "minimum": 0,
                "description": "Rows to skip (for pagination).",
            },
            "explain": {
                "type": "string",
                "description": "Your intent description. Not compiled. Helps error recovery.",
            },
            "cte": {
                "type": "array",
                "description": "Reserved for future CTE support. Do not use yet.",
            },
        },
    }
