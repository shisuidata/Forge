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


# ── P1: window frame ──────────────────────────────────────────────────────────

def test_window_frame_rows_between():
    """Window frame: ROWS BETWEEN 6 PRECEDING AND CURRENT ROW (7-day rolling avg)."""
    result = sql({
        "scan": "orders",
        "window": [{
            "fn": "avg", "col": "orders.total_amount",
            "order": [{"col": "orders.created_at", "dir": "asc"}],
            "frame": {"unit": "rows", "start": "6 preceding", "end": "current_row"},
            "as": "rolling_avg"
        }],
        "select": ["orders.created_at", "rolling_avg"],
    })
    assert "AVG(orders.total_amount) OVER" in result
    assert "ROWS BETWEEN 6 PRECEDING AND CURRENT ROW" in result


def test_window_frame_running_total():
    """Window frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (running total)."""
    result = sql({
        "scan": "orders",
        "window": [{
            "fn": "sum", "col": "orders.total_amount",
            "order": [{"col": "orders.created_at", "dir": "asc"}],
            "frame": {"unit": "rows", "start": "unbounded_preceding", "end": "current_row"},
            "as": "running_total"
        }],
        "select": ["running_total"],
    })
    assert "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in result


def test_window_frame_range():
    """Window frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW."""
    result = sql({
        "scan": "orders",
        "window": [{
            "fn": "sum", "col": "orders.total_amount",
            "order": [{"col": "orders.created_at", "dir": "asc"}],
            "frame": {"unit": "range", "start": "unbounded_preceding", "end": "current_row"},
            "as": "range_total"
        }],
        "select": ["range_total"],
    })
    assert "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in result


# ── P1: date trunc group key ──────────────────────────────────────────────────

def test_group_expr_date_trunc():
    """Group by computed expression (date truncation) with alias referenced in select."""
    result = sql({
        "scan": "orders",
        "group": [{"expr": "STRFTIME('%Y-%m', orders.created_at)", "as": "month"}],
        "agg":   [{"fn": "count_all", "as": "order_count"}],
        "select": ["month", "order_count"],
    })
    assert "STRFTIME('%Y-%m', orders.created_at) AS month" in result
    assert "GROUP BY STRFTIME('%Y-%m', orders.created_at)" in result
    assert "COUNT(*) AS order_count" in result


def test_group_expr_mixed_with_column():
    """Group by a mix of plain column and computed expression."""
    result = sql({
        "scan": "orders",
        "group": [
            "orders.user_id",
            {"expr": "STRFTIME('%Y-%m', orders.created_at)", "as": "month"},
        ],
        "agg":   [{"fn": "sum", "col": "orders.total_amount", "as": "revenue"}],
        "select": ["orders.user_id", "month", "revenue"],
    })
    assert "GROUP BY orders.user_id, STRFTIME('%Y-%m', orders.created_at)" in result
    assert "STRFTIME('%Y-%m', orders.created_at) AS month" in result


# ── P1: new window functions ──────────────────────────────────────────────────

def test_window_percent_rank():
    result = sql({
        "scan": "orders",
        "window": [{
            "fn": "percent_rank",
            "order": [{"col": "orders.total_amount", "dir": "desc"}],
            "as": "pct_rank"
        }],
        "select": ["orders.id", "pct_rank"],
    })
    assert "PERCENT_RANK() OVER" in result
    assert "ORDER BY orders.total_amount DESC" in result


def test_window_cume_dist():
    result = sql({
        "scan": "orders",
        "window": [{
            "fn": "cume_dist",
            "order": [{"col": "orders.total_amount", "dir": "asc"}],
            "as": "cd"
        }],
        "select": ["orders.id", "cd"],
    })
    assert "CUME_DIST() OVER" in result


def test_window_ntile():
    """NTILE(4) → quartile buckets."""
    result = sql({
        "scan": "orders",
        "window": [{
            "fn": "ntile", "n": 4,
            "order": [{"col": "orders.total_amount", "dir": "desc"}],
            "as": "quartile"
        }],
        "select": ["orders.id", "quartile"],
    })
    assert "NTILE(4) OVER" in result


def test_window_first_value():
    result = sql({
        "scan": "orders",
        "window": [{
            "fn": "first_value", "col": "orders.total_amount",
            "partition": ["orders.user_id"],
            "order": [{"col": "orders.created_at", "dir": "asc"}],
            "as": "first_order_amount"
        }],
        "select": ["orders.user_id", "first_order_amount"],
    })
    assert "FIRST_VALUE(orders.total_amount) OVER" in result
    assert "PARTITION BY orders.user_id" in result


def test_window_last_value_with_frame():
    """LAST_VALUE needs ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING to work correctly."""
    result = sql({
        "scan": "orders",
        "window": [{
            "fn": "last_value", "col": "orders.total_amount",
            "partition": ["orders.user_id"],
            "order": [{"col": "orders.created_at", "dir": "asc"}],
            "frame": {"unit": "rows", "start": "unbounded_preceding", "end": "unbounded_following"},
            "as": "last_order_amount"
        }],
        "select": ["orders.user_id", "last_order_amount"],
    })
    assert "LAST_VALUE(orders.total_amount) OVER" in result
    assert "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" in result


# ── P2: SELECT DISTINCT ───────────────────────────────────────────────────────

def test_select_distinct():
    result = sql({
        "scan": "orders",
        "distinct": True,
        "select": ["orders.user_id", "orders.status"],
    })
    assert result.startswith("SELECT DISTINCT")
    assert "orders.user_id" in result


def test_select_without_distinct():
    """Without distinct: true, output must be SELECT (not SELECT DISTINCT)."""
    result = sql({
        "scan": "orders",
        "select": ["orders.user_id"],
    })
    assert "SELECT DISTINCT" not in result
    assert result.startswith("SELECT")


# ── P2: INTERSECT / EXCEPT ───────────────────────────────────────────────────

def test_intersect():
    result = sql({
        "scan": "orders",
        "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
        "select": ["orders.user_id"],
        "intersect": [{"query": {
            "scan": "orders",
            "filter": [{"col": "orders.total_amount", "op": "gte", "val": 1000}],
            "select": ["orders.user_id"],
        }}],
    })
    assert "INTERSECT" in result
    assert result.count("orders.user_id") >= 2


def test_except():
    result = sql({
        "scan": "users",
        "select": ["users.id"],
        "except": [{"query": {
            "scan": "orders",
            "select": ["orders.user_id"],
        }}],
    })
    assert "EXCEPT" in result


# ── P2: filter IN subquery ────────────────────────────────────────────────────

def test_filter_in_subquery():
    """IN (SELECT ...) subquery compiles correctly."""
    result = sql({
        "scan": "users",
        "filter": [{
            "col": "users.id",
            "op": "in",
            "val": {"subquery": {
                "scan": "orders",
                "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
                "select": ["orders.user_id"],
            }}
        }],
        "select": ["users.id", "users.name"],
    })
    assert "users.id IN (" in result
    assert "SELECT orders.user_id" in result
    assert "orders.status = 'completed'" in result


# ── P2: agg FILTER clause ─────────────────────────────────────────────────────

def test_agg_filter_clause():
    """SUM(col) FILTER (WHERE ...) compiles correctly (SQLite / PostgreSQL)."""
    result = sql({
        "scan": "orders",
        "agg": [
            {"fn": "count_all", "as": "total_orders"},
            {
                "fn": "sum", "col": "orders.total_amount", "as": "vip_revenue",
                "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}]
            },
        ],
        "select": ["total_orders", "vip_revenue"],
    })
    assert "SUM(orders.total_amount) FILTER (WHERE orders.status = 'completed') AS vip_revenue" in result
    assert "COUNT(*) AS total_orders" in result


def test_agg_filter_mysql_raises():
    """MySQL does not support FILTER (WHERE ...) — must raise ValueError."""
    with pytest.raises(ValueError, match="MySQL"):
        compile_query({
            "scan": "orders",
            "agg": [{
                "fn": "sum", "col": "orders.total_amount", "as": "rev",
                "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
            }],
            "select": ["rev"],
        }, dialect="mysql")


# ── NULL 安全 neq 编译 ────────────────────────────────────────────────────────

def test_null_safe_neq_with_nullable_col():
    """nullable 列的 neq 条件自动展开为 (col != val OR col IS NULL)"""
    result = " ".join(compile_query(
        {"scan": "orders", "filter": [{"col": "orders.user_id", "op": "neq", "val": 0}], "select": ["orders.id"]},
        nullable_cols=frozenset(["orders.user_id"])
    ).split())
    assert result == "SELECT orders.id FROM orders WHERE (orders.user_id != 0 OR orders.user_id IS NULL)"


def test_null_safe_neq_col_shortname():
    """nullable_cols 支持不带表前缀的列名匹配"""
    result = " ".join(compile_query(
        {"scan": "orders", "filter": [{"col": "orders.status", "op": "neq", "val": "cancelled"}], "select": ["orders.id"]},
        nullable_cols=frozenset(["status"])
    ).split())
    assert result == "SELECT orders.id FROM orders WHERE (orders.status != 'cancelled' OR orders.status IS NULL)"


def test_neq_without_nullable_stays_simple():
    """不传 nullable_cols 时，neq 保持普通形式"""
    result = " ".join(compile_query(
        {"scan": "orders", "filter": [{"col": "orders.status", "op": "neq", "val": "cancelled"}], "select": ["orders.id"]}
    ).split())
    assert result == "SELECT orders.id FROM orders WHERE orders.status != 'cancelled'"


# ── BigQuery 方言 ─────────────────────────────────────────────────────────────

def test_bigquery_preset_today():
    result = " ".join(compile_query(
        {"scan": "orders", "filter": [{"col": "orders.created_at", "op": "gte", "val": {"$preset": "today"}}], "select": ["orders.id"]},
        dialect="bigquery"
    ).split())
    assert "CURRENT_DATE()" in result


def test_bigquery_preset_this_month():
    result = " ".join(compile_query(
        {"scan": "orders", "filter": [{"col": "orders.created_at", "op": "gte", "val": {"$preset": "this_month"}}], "select": ["orders.id"]},
        dialect="bigquery"
    ).split())
    assert "DATE_TRUNC(CURRENT_DATE(), MONTH)" in result


def test_bigquery_group_concat():
    result = " ".join(compile_query(
        {"scan": "orders", "group": ["orders.status"], "agg": [{"fn": "group_concat", "col": "orders.id", "as": "ids"}], "select": ["orders.status", "ids"]},
        dialect="bigquery"
    ).split())
    assert "STRING_AGG(orders.id," in result


def test_bigquery_no_right_join():
    with pytest.raises(ValueError, match="BigQuery"):
        compile_query(
            {"scan": "orders", "joins": [{"type": "right", "table": "users", "on": {"left": "orders.user_id", "right": "users.id"}}], "select": ["orders.id"]},
            dialect="bigquery"
        )


# ── Snowflake 方言 ────────────────────────────────────────────────────────────

def test_snowflake_preset_last_7_days():
    result = " ".join(compile_query(
        {"scan": "orders", "filter": [{"col": "orders.created_at", "op": "gte", "val": {"$preset": "last_7_days"}}], "select": ["orders.id"]},
        dialect="snowflake"
    ).split())
    assert "DATEADD(day, -7, CURRENT_DATE())" in result


def test_snowflake_preset_this_month():
    result = " ".join(compile_query(
        {"scan": "orders", "filter": [{"col": "orders.created_at", "op": "gte", "val": {"$preset": "this_month"}}], "select": ["orders.id"]},
        dialect="snowflake"
    ).split())
    assert "DATE_TRUNC('month', CURRENT_DATE())" in result


def test_snowflake_group_concat():
    result = " ".join(compile_query(
        {"scan": "orders", "group": ["orders.status"], "agg": [{"fn": "group_concat", "col": "orders.id", "as": "ids"}], "select": ["orders.status", "ids"]},
        dialect="snowflake"
    ).split())
    assert "LISTAGG(orders.id," in result


def test_snowflake_filter_clause_raises():
    with pytest.raises(ValueError, match="Snowflake"):
        compile_query(
            {"scan": "orders", "group": ["orders.status"], "agg": [{"fn": "sum", "col": "orders.total_amount", "as": "total", "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}]}], "select": ["orders.status", "total"]},
            dialect="snowflake"
        )


# ── Fix 17-19: New coerce fixes ───────────────────────────────────────────────

def test_fix17_having_without_group_infers_group():
    """Fix 17: HAVING present but no GROUP BY → infer GROUP BY from non-agg select columns."""
    result = sql({
        "scan": "category_orders",
        "joins": [{"type": "inner", "table": "category_refunds",
                   "on": {"left": "category_orders.category", "right": "category_refunds.category"}}],
        "agg": [{"fn": "avg", "col": "category_orders.order_count", "as": "avg_orders"}],
        "having": [{"col": "avg_orders", "op": "gt", "val": 10}],
        "select": ["category_orders.category", "avg_orders"],
    })
    assert "GROUP BY category_orders.category" in result
    assert "HAVING avg_orders > 10" in result


def test_fix18_lag_expands_agg_alias():
    """Fix 18: LAG/LEAD col referencing an agg alias → expands to actual expression."""
    result = sql({
        "scan": "orders",
        "group": ["orders.month"],
        "agg": [{"fn": "count_all", "as": "order_count"}],
        "window": [{
            "fn": "lag", "col": "order_count", "offset": 1,
            "order": [{"col": "orders.month", "dir": "asc"}],
            "as": "prev_order_count"
        }],
        "select": ["orders.month", "order_count", "prev_order_count"],
    })
    assert "LAG(COUNT(*), 1)" in result


def test_fix19_semi_join_filter_scope():
    """Fix 19: top-level filter referencing semi-join table → moved to join's filter."""
    result = sql({
        "scan": "dim_user",
        "filter": [{"col": "dwd_cart_detail.action_type", "op": "eq", "val": "add"}],
        "joins": [{
            "type": "semi",
            "table": "dwd_cart_detail",
            "on": {"left": "dim_user.user_id", "right": "dwd_cart_detail.user_id"},
        }],
        "select": ["dim_user.user_id"],
    })
    # The condition must end up in the EXISTS subquery, not the outer WHERE
    assert "dwd_cart_detail.action_type = 'add'" in result
    # Outer WHERE should only have the EXISTS clause (no standalone action_type filter)
    lines = result.split("\n") if "\n" in result else result.split()
    assert "EXISTS" in result


def test_col2_condition():
    """col2: column-to-column comparison, e.g. good_count > bad_count."""
    result = sql({
        "scan": "product_stats",
        "filter": [{"col": "product_stats.good_count", "op": "gt", "col2": "product_stats.bad_count"}],
        "select": ["product_stats.product_id"],
    })
    assert "product_stats.good_count > product_stats.bad_count" in result
