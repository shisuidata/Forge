"""
Forge → SQL compiler.
Input:  Forge JSON (validated against schema.json)
Output: SQL string (dialect-neutral for now)
"""

from __future__ import annotations
import json
import pathlib
from typing import Any

import jsonschema

_SCHEMA_PATH = pathlib.Path(__file__).parent / "schema.json"
_SCHEMA = json.loads(_SCHEMA_PATH.read_text())

_JOIN_KEYWORDS = {
    "inner": "INNER JOIN",
    "left":  "LEFT JOIN",
    "right": "RIGHT JOIN",
    "full":  "FULL OUTER JOIN",
}

_OP_SYMBOLS = {
    "eq":  "=",
    "neq": "!=",
    "gt":  ">",
    "gte": ">=",
    "lt":  "<",
    "lte": "<=",
}


# ── public entry point ────────────────────────────────────────────────────────

def compile_query(forge: dict) -> str:
    """Validate a Forge JSON dict and compile it to SQL."""
    jsonschema.validate(forge, _SCHEMA)
    return _compile(forge)


# ── compiler ──────────────────────────────────────────────────────────────────

def _compile(q: dict) -> str:
    clauses: list[str] = []

    # SELECT
    clauses.append("SELECT " + ", ".join(_select_exprs(q)))

    # FROM
    clauses.append("FROM " + q["scan"])

    # JOINs — anti/semi joins inject extra WHERE / WHERE EXISTS conditions
    extra_where: list[str] = []
    for join in q.get("joins", []):
        sql, injected = _join(join)
        if sql:
            clauses.append(sql)
        extra_where.extend(injected)

    # WHERE
    where_parts = [_condition(c) for c in q.get("filter", [])] + extra_where
    if where_parts:
        clauses.append("WHERE " + " AND ".join(where_parts))

    # GROUP BY
    if "group" in q:
        clauses.append("GROUP BY " + ", ".join(q["group"]))

    # HAVING
    having_parts = [_condition(c) for c in q.get("having", [])]
    if having_parts:
        clauses.append("HAVING " + " AND ".join(having_parts))

    # ORDER BY
    if "sort" in q:
        sort_exprs = [f"{s['col']} {s['dir'].upper()}" for s in q["sort"]]
        clauses.append("ORDER BY " + ", ".join(sort_exprs))

    # LIMIT
    if "limit" in q:
        clauses.append(f"LIMIT {q['limit']}")

    return "\n".join(clauses)


# ── SELECT expressions ────────────────────────────────────────────────────────

def _select_exprs(q: dict) -> list[str]:
    # Build alias → SQL expression map from agg definitions
    agg_map: dict[str, str] = {
        agg["as"]: _agg_expr(agg) for agg in q.get("agg", [])
    }
    exprs = []
    for col in q["select"]:
        if col in agg_map:
            exprs.append(f"{agg_map[col]} AS {col}")
        else:
            exprs.append(col)
    return exprs


def _agg_expr(agg: dict) -> str:
    fn = agg["fn"]
    if fn == "count_all":
        return "COUNT(*)"
    if fn == "count_distinct":
        return f"COUNT(DISTINCT {agg['col']})"
    return f"{fn.upper()}({agg['col']})"


# ── JOIN ──────────────────────────────────────────────────────────────────────

def _join(join: dict) -> tuple[str | None, list[str]]:
    """Return (JOIN clause or None, extra WHERE conditions)."""
    jtype  = join["type"]
    table  = join["table"]
    left   = join["on"]["left"]
    right  = join["on"]["right"]

    if jtype == "anti":
        # LEFT JOIN … WHERE right_key IS NULL
        return (
            f"LEFT JOIN {table} ON {left} = {right}",
            [f"{right} IS NULL"],
        )

    if jtype == "semi":
        # No JOIN clause — inject WHERE EXISTS
        return (
            None,
            [f"EXISTS (SELECT 1 FROM {table} WHERE {left} = {right})"],
        )

    keyword = _JOIN_KEYWORDS[jtype]
    return f"{keyword} {table} ON {left} = {right}", []


# ── conditions ────────────────────────────────────────────────────────────────

def _condition(cond: dict) -> str:
    if "or" in cond:
        return "(" + " OR ".join(_condition(c) for c in cond["or"]) + ")"

    col = cond["col"]
    op  = cond["op"]

    if op == "is_null":
        return f"{col} IS NULL"
    if op == "is_not_null":
        return f"{col} IS NOT NULL"
    if op == "between":
        return f"{col} BETWEEN {_val(cond['lo'])} AND {_val(cond['hi'])}"
    if op == "in":
        items = ", ".join(_val(v) for v in cond["val"])
        return f"{col} IN ({items})"
    if op == "like":
        return f"{col} LIKE {_val(cond['val'])}"

    symbol = _OP_SYMBOLS[op]
    return f"{col} {symbol} {_val(cond['val'])}"


# ── value formatting ──────────────────────────────────────────────────────────

def _val(v: Any) -> str:
    if isinstance(v, dict) and "$date" in v:
        return f"'{v['$date']}'"          # date literal — dialect layer can refine later
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)
