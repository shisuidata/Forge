"""
Forge compiler tests.

Cases marked with the source failure doc (A1, B1, …) are derived from
tests/text-to-sql-failures/ and represent real AI-generated SQL mistakes
that Forge is designed to make structurally impossible.
"""

import pytest
import jsonschema
from forge.compiler import compile_query


# ── helpers ───────────────────────────────────────────────────────────────────

def sql(q: dict) -> str:
    """Compile and normalise whitespace for easy comparison."""
    return " ".join(compile_query(q).split())


# ── A. JOIN traps ─────────────────────────────────────────────────────────────

def test_a1_left_join_preserves_zero_order_users():
    """
    A1: Users with no orders must appear (order_count = 0).
    Classic AI mistake: INNER JOIN drops them silently.
    Forge forces explicit join type — 'left' is the only way to express this.
    """
    result = sql({
        "scan": "users",
        "joins": [{"type": "left", "table": "orders",
                   "on": {"left": "users.id", "right": "orders.user_id"}}],
        "group": ["users.id", "users.name"],
        "agg":   [{"fn": "count", "col": "orders.id", "as": "order_count"}],
        "select": ["users.name", "order_count"],
    })
    assert result == (
        "SELECT users.name, COUNT(orders.id) AS order_count "
        "FROM users "
        "LEFT JOIN orders ON users.id = orders.user_id "
        "GROUP BY users.id, users.name"
    )


def test_a2_anti_join_replaces_not_in():
    """
    A2: Orders with no matching order_items.
    Classic AI mistake: NOT IN fails silently when order_items.order_id contains NULL.
    Forge has no NOT IN — anti join is the only primitive for this pattern.
    """
    result = sql({
        "scan": "orders",
        "joins": [{"type": "anti", "table": "order_items",
                   "on": {"left": "orders.id", "right": "order_items.order_id"}}],
        "select": ["orders.id", "orders.status", "orders.total_amount"],
    })
    assert result == (
        "SELECT orders.id, orders.status, orders.total_amount "
        "FROM orders "
        "LEFT JOIN order_items ON orders.id = order_items.order_id "
        "WHERE order_items.order_id IS NULL"
    )


def test_inner_join():
    result = sql({
        "scan": "orders",
        "joins": [{"type": "inner", "table": "users",
                   "on": {"left": "orders.user_id", "right": "users.id"}}],
        "select": ["users.name", "orders.total_amount"],
    })
    assert "INNER JOIN users ON orders.user_id = users.id" in result


def test_semi_join_compiles_to_exists():
    result = sql({
        "scan": "orders",
        "joins": [{"type": "semi", "table": "users",
                   "on": {"left": "orders.user_id", "right": "users.id"}}],
        "select": ["orders.id"],
    })
    assert "EXISTS (SELECT 1 FROM users WHERE orders.user_id = users.id)" in result
    assert "JOIN" not in result


# ── B. Aggregation traps ──────────────────────────────────────────────────────

def test_b1_filter_goes_to_where_not_having():
    """
    B1: Filtering on row attributes (is_vip, status) must happen in WHERE,
    not HAVING. Forge uses separate 'filter' and 'having' keys — the
    position in the schema enforces the correct SQL placement.
    """
    result = sql({
        "scan": "orders",
        "joins": [{"type": "inner", "table": "users",
                   "on": {"left": "orders.user_id", "right": "users.id"}}],
        "filter": [
            {"col": "users.is_vip",    "op": "eq", "val": True},
            {"col": "orders.status",   "op": "eq", "val": "completed"},
        ],
        "group":  ["users.city"],
        "agg":    [{"fn": "avg", "col": "orders.total_amount", "as": "avg_order_value"}],
        "select": ["users.city", "avg_order_value"],
    })
    assert "WHERE users.is_vip = TRUE AND orders.status = 'completed'" in result
    assert "HAVING" not in result


def test_count_field_vs_count_all_are_distinct():
    """count(col) ignores NULLs; count_all() counts every row. Must compile differently."""
    q_count = sql({
        "scan": "orders",
        "agg": [{"fn": "count", "col": "orders.id", "as": "n"}],
        "select": ["n"],
    })
    q_count_all = sql({
        "scan": "orders",
        "agg": [{"fn": "count_all", "as": "n"}],
        "select": ["n"],
    })
    assert "COUNT(orders.id)" in q_count
    assert "COUNT(*)"         in q_count_all


def test_count_distinct():
    result = sql({
        "scan": "orders",
        "agg": [{"fn": "count_distinct", "col": "orders.user_id", "as": "unique_users"}],
        "select": ["unique_users"],
    })
    assert "COUNT(DISTINCT orders.user_id) AS unique_users" in result


# ── filter operators ──────────────────────────────────────────────────────────

def test_filter_operators():
    cases = [
        ({"col": "orders.amount", "op": "gt",  "val": 100},   "orders.amount > 100"),
        ({"col": "orders.amount", "op": "gte", "val": 100},   "orders.amount >= 100"),
        ({"col": "orders.amount", "op": "lt",  "val": 100},   "orders.amount < 100"),
        ({"col": "orders.amount", "op": "lte", "val": 100},   "orders.amount <= 100"),
        ({"col": "orders.status", "op": "neq", "val": "x"},   "orders.status != 'x'"),
        ({"col": "orders.status", "op": "in",  "val": ["a", "b"]}, "orders.status IN ('a', 'b')"),
        ({"col": "orders.name",   "op": "like","val": "%foo%"},"orders.name LIKE '%foo%'"),
        ({"col": "orders.x",      "op": "is_null"},            "orders.x IS NULL"),
        ({"col": "orders.x",      "op": "is_not_null"},        "orders.x IS NOT NULL"),
        ({"col": "orders.amount", "op": "between", "lo": 10, "hi": 50}, "orders.amount BETWEEN 10 AND 50"),
    ]
    for cond, expected in cases:
        result = sql({"scan": "orders", "filter": [cond], "select": ["orders.id"]})
        assert expected in result, f"Expected '{expected}' in: {result}"


def test_date_literal():
    result = sql({
        "scan": "orders",
        "filter": [{"col": "orders.created_at", "op": "gte", "val": {"$date": "2024-01-01"}}],
        "select": ["orders.id"],
    })
    assert "orders.created_at >= '2024-01-01'" in result


def test_or_condition():
    result = sql({
        "scan": "orders",
        "filter": [{"or": [
            {"col": "orders.status", "op": "eq", "val": "completed"},
            {"col": "orders.status", "op": "eq", "val": "pending"},
        ]}],
        "select": ["orders.id"],
    })
    assert "(orders.status = 'completed' OR orders.status = 'pending')" in result


def test_multiple_filters_are_and_combined():
    result = sql({
        "scan": "orders",
        "filter": [
            {"col": "orders.status", "op": "eq", "val": "completed"},
            {"col": "orders.amount", "op": "gt", "val": 0},
        ],
        "select": ["orders.id"],
    })
    assert "WHERE orders.status = 'completed' AND orders.amount > 0" in result


# ── sort / limit ──────────────────────────────────────────────────────────────

def test_sort_requires_explicit_direction():
    result = sql({
        "scan": "orders",
        "select": ["orders.id"],
        "sort": [
            {"col": "orders.created_at", "dir": "desc"},
            {"col": "orders.id",         "dir": "asc"},
        ],
    })
    assert "ORDER BY orders.created_at DESC, orders.id ASC" in result


def test_limit():
    result = sql({"scan": "orders", "select": ["orders.id"], "limit": 50})
    assert "LIMIT 50" in result


# ── having ────────────────────────────────────────────────────────────────────

def test_having_filters_after_group():
    result = sql({
        "scan": "orders",
        "group":  ["orders.user_id"],
        "agg":    [{"fn": "count", "col": "orders.id", "as": "n"}],
        "having": [{"col": "n", "op": "gt", "val": 5}],
        "select": ["orders.user_id", "n"],
    })
    assert "HAVING n > 5" in result
    # HAVING must come after GROUP BY
    assert result.index("GROUP BY") < result.index("HAVING")


# ── schema validation ─────────────────────────────────────────────────────────

def test_missing_scan_raises():
    with pytest.raises((ValueError, jsonschema.ValidationError)):
        compile_query({"select": ["orders.id"]})


def test_missing_select_raises():
    with pytest.raises((ValueError, jsonschema.ValidationError)):
        compile_query({"scan": "orders"})


def test_invalid_join_type_raises():
    """'join' without a type — or an unknown type — must be rejected at validation."""
    with pytest.raises((ValueError, jsonschema.ValidationError)):
        compile_query({
            "scan": "orders",
            "joins": [{"type": "JOIN", "table": "users",   # 'JOIN' not in enum
                       "on": {"left": "orders.user_id", "right": "users.id"}}],
            "select": ["orders.id"],
        })


def test_join_without_type_raises():
    with pytest.raises((ValueError, jsonschema.ValidationError)):
        compile_query({
            "scan": "orders",
            "joins": [{"table": "users",
                       "on": {"left": "orders.user_id", "right": "users.id"}}],
            "select": ["orders.id"],
        })


def test_invalid_sort_direction_raises():
    with pytest.raises((ValueError, jsonschema.ValidationError)):
        compile_query({
            "scan": "orders",
            "select": ["orders.id"],
            "sort": [{"col": "orders.id", "dir": "DESC"}],  # must be lowercase
        })


def test_empty_select_raises():
    with pytest.raises((ValueError, jsonschema.ValidationError)):
        compile_query({"scan": "orders", "select": []})


# ── clause ordering ───────────────────────────────────────────────────────────

def test_clause_order_in_full_query():
    """SELECT … FROM … JOIN … WHERE … GROUP BY … HAVING … ORDER BY … LIMIT"""
    result = compile_query({
        "scan": "orders",
        "joins":  [{"type": "inner", "table": "users",
                    "on": {"left": "orders.user_id", "right": "users.id"}}],
        "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
        "group":  ["users.city"],
        "agg":    [{"fn": "sum", "col": "orders.total_amount", "as": "revenue"}],
        "having": [{"col": "revenue", "op": "gt", "val": 1000}],
        "select": ["users.city", "revenue"],
        "sort":   [{"col": "revenue", "dir": "desc"}],
        "limit":  5,
    })
    keywords = ["SELECT", "FROM", "INNER JOIN", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT"]
    positions = [result.index(kw) for kw in keywords]
    assert positions == sorted(positions), "SQL clauses are out of order"
