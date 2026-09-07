"""
Forge compiler tests.

Cases marked with the source failure doc (A1, B1, …) are derived from
tools/benchmarks/text_to_sql_failures/ and represent real AI-generated SQL mistakes
that Forge is designed to make structurally impossible.
"""

import sqlite3

import jsonschema
import pytest
from forge.compiler import compile_query


# ── helpers ───────────────────────────────────────────────────────────────────

def sql(q: dict) -> str:
    """Compile and normalise whitespace for easy comparison."""
    return " ".join(compile_query(q).split())


# ── A. JOIN traps ─────────────────────────────────────────────────────────────

def test_rejects_unbound_table_reference_before_sql_execution():
    with pytest.raises(ValueError, match="未加入 FROM/JOIN.*dim_city"):
        compile_query({
            "scan": "dwd_order_detail",
            "joins": [{
                "type": "inner",
                "table": "fact_order",
                "on": {
                    "left": "dwd_order_detail.order_id",
                    "right": "fact_order.order_id",
                },
            }],
            "select": ["dim_city.city_name", "dwd_order_detail.total_amount"],
        })


def test_rejects_unaliased_self_join():
    with pytest.raises(ValueError, match="不支持无别名自连接"):
        compile_query({
            "scan": "dwd_order_detail",
            "joins": [{
                "type": "inner",
                "table": "dwd_order_detail",
                "on": {
                    "left": "dwd_order_detail.order_id",
                    "right": "dwd_order_detail.order_id",
                },
            }],
            "select": ["dwd_order_detail.order_id"],
        })


def test_rejects_join_that_does_not_connect_existing_scope():
    with pytest.raises(ValueError, match="必须连接待连接表与当前查询中的已有表"):
        compile_query({
            "scan": "orders",
            "joins": [{
                "type": "inner",
                "table": "users",
                "on": {"left": "users.id", "right": "users.manager_id"},
            }],
            "select": ["orders.id"],
        })


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


def test_sort_by_unselected_aggregate_expands_expression_without_leaking_column():
    query = {
        "scan": "orders",
        "agg": [{"fn": "min", "col": "orders.amount", "as": "min_amount"}],
        "group": ["orders.user_id"],
        "select": ["orders.user_id"],
        "sort": [{"col": "min_amount", "dir": "asc"}],
    }

    compiled = sql(query)
    assert compiled.startswith("SELECT orders.user_id FROM orders")
    assert "ORDER BY MIN(orders.amount) ASC" in compiled
    assert "AS min_amount" not in compiled

    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE orders (user_id INTEGER, amount INTEGER)")
        connection.executemany(
            "INSERT INTO orders VALUES (?, ?)",
            [(1, 30), (1, 10), (2, 5), (2, 50)],
        )
        cursor = connection.execute(compiled)
        assert [column[0] for column in cursor.description] == ["user_id"]
        assert cursor.fetchall() == [(2,), (1,)]


def test_sort_by_projected_aggregate_keeps_visible_alias():
    result = sql({
        "scan": "orders",
        "agg": [{"fn": "min", "col": "orders.amount", "as": "min_amount"}],
        "group": ["orders.user_id"],
        "select": ["orders.user_id", "min_amount"],
        "sort": [{"col": "min_amount", "dir": "asc"}],
    })

    assert "MIN(orders.amount) AS min_amount" in result
    assert "ORDER BY min_amount ASC" in result
    assert "ORDER BY MIN(orders.amount)" not in result


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
    assert "HAVING COUNT(orders.id) > 5" in result  # alias expanded to expr to avoid SQLite ambiguity
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
    assert "HAVING AVG(category_orders.order_count) > 10" in result  # alias expanded


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


def test_quoted_qualified_identifiers_bind_and_execute():
    query = {
        "scan": "Examination",
        "joins": [{
            "type": "inner",
            "table": "Patient",
            "on": {
                "left": '"Examination"."ID"',
                "right": '"Patient"."ID"',
            },
        }],
        "filter": [{"col": '"Examination"."RVVT"', "op": "eq", "val": "+"}],
        "select": ['"Patient"."ID"'],
    }
    compiled = compile_query(query)

    with sqlite3.connect(":memory:") as connection:
        connection.execute('CREATE TABLE "Examination" ("ID" INTEGER, "RVVT" TEXT)')
        connection.execute('CREATE TABLE "Patient" ("ID" INTEGER)')
        connection.executemany(
            'INSERT INTO "Examination" VALUES (?, ?)',
            [(1, "+"), (2, "-")],
        )
        connection.executemany('INSERT INTO "Patient" VALUES (?)', [(1,), (2,)])
        assert connection.execute(compiled).fetchall() == [(1,)]


def test_quoted_reserved_join_table_binds_and_executes():
    query = {
        "scan": "account",
        "joins": [{
            "type": "inner",
            "table": '"order"',
            "on": {
                "left": "account.account_id",
                "right": '"order".account_id',
            },
        }],
        "filter": [{"col": '"order".amount', "op": "eq", "val": 3539}],
        "select": ["account.frequency", '"order".k_symbol'],
    }
    compiled = compile_query(query)

    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE account (account_id INTEGER, frequency TEXT)")
        connection.execute(
            'CREATE TABLE "order" (account_id INTEGER, amount INTEGER, k_symbol TEXT)'
        )
        connection.execute("INSERT INTO account VALUES (3, 'monthly')")
        connection.execute("INSERT INTO \"order\" VALUES (3, 3539, 'insurance')")
        assert connection.execute(compiled).fetchall() == [("monthly", "insurance")]


@pytest.mark.parametrize(
    ("dialect", "expected_alias"),
    [
        ("sqlite", '"eligible free rate"'),
        ("postgresql", '"eligible free rate"'),
        ("snowflake", '"eligible free rate"'),
        ("mysql", "`eligible free rate`"),
        ("bigquery", "`eligible free rate`"),
    ],
)
def test_unsafe_output_alias_uses_target_dialect_quoting(dialect, expected_alias):
    query = {
        "scan": "metrics",
        "select": [{"expr": "metrics.value * 100.0", "as": "eligible free rate"}],
        "sort": [{"col": "eligible free rate", "dir": "desc"}],
    }
    compiled = compile_query(query, dialect=dialect)

    assert f"AS {expected_alias}" in compiled
    assert f"ORDER BY {expected_alias} DESC" in compiled

    if dialect == "sqlite":
        with sqlite3.connect(":memory:") as connection:
            connection.execute("CREATE TABLE metrics (value REAL)")
            connection.executemany("INSERT INTO metrics VALUES (?)", [(0.25,), (0.5,)])
            cursor = connection.execute(compiled)
            assert [column[0] for column in cursor.description] == ["eligible free rate"]
            assert cursor.fetchall() == [(50.0,), (25.0,)]


def test_reserved_word_output_alias_is_quoted():
    compiled = compile_query({
        "scan": "metrics",
        "select": [{"expr": "metrics.value", "as": "order"}],
    })
    assert 'AS "order"' in compiled


def test_having_expands_aggregate_aliases_on_both_operands():
    query = {
        "scan": "matches",
        "group": ["matches.league"],
        "agg": [
            {"fn": "avg", "col": "matches.home_goals", "as": "avg_home_goals"},
            {"fn": "avg", "col": "matches.away_goals", "as": "avg_away_goals"},
        ],
        "having": [{
            "col": "avg_home_goals",
            "op": "gt",
            "col2": "avg_away_goals",
        }],
        "select": ["matches.league"],
    }
    compiled = compile_query(query)

    assert "HAVING AVG(matches.home_goals) > AVG(matches.away_goals)" in compiled
    with sqlite3.connect(":memory:") as connection:
        connection.execute(
            "CREATE TABLE matches (league TEXT, home_goals INTEGER, away_goals INTEGER)"
        )
        connection.executemany(
            "INSERT INTO matches VALUES (?, ?, ?)",
            [("home", 3, 1), ("home", 1, 1), ("away", 0, 2)],
        )
        assert connection.execute(compiled).fetchall() == [("home",)]


def test_unquoted_reserved_source_table_is_quoted_and_executes():
    query = {
        "scan": "account",
        "joins": [{
            "type": "inner",
            "table": "order",
            "on": {
                "left": "account.account_id",
                "right": "order.account_id",
            },
        }],
        "filter": [{"col": "order.amount", "op": "eq", "val": 3539}],
        "select": ["account.frequency", "order.k_symbol"],
    }
    compiled = compile_query(query)

    assert 'INNER JOIN "order" ON account.account_id = "order".account_id' in compiled
    assert 'WHERE "order".amount = 3539' in compiled
    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE account (account_id INTEGER, frequency TEXT)")
        connection.execute(
            'CREATE TABLE "order" (account_id INTEGER, amount INTEGER, k_symbol TEXT)'
        )
        connection.execute("INSERT INTO account VALUES (3, 'monthly')")
        connection.execute('INSERT INTO "order" VALUES (?, ?, ?)', (3, 3539, "insurance"))
        assert connection.execute(compiled).fetchall() == [("monthly", "insurance")]


def test_complex_source_column_is_quoted_in_select_and_sort():
    query = {
        "scan": "state_stats",
        "select": ["State", "Enrollment (K-12)"],
        "sort": [{"col": "Enrollment (K-12)", "dir": "desc"}],
    }
    compiled = compile_query(query)

    assert 'SELECT State, "Enrollment (K-12)"' in compiled
    assert 'ORDER BY "Enrollment (K-12)" DESC' in compiled
    with sqlite3.connect(":memory:") as connection:
        connection.execute(
            'CREATE TABLE state_stats (State TEXT, "Enrollment (K-12)" INTEGER)'
        )
        connection.executemany(
            'INSERT INTO state_stats VALUES (?, ?)',
            [("A", 100), ("B", 200)],
        )
        assert connection.execute(compiled).fetchall() == [("B", 200), ("A", 100)]


@pytest.mark.parametrize("alias_separator", [" AS ", " "])
def test_raw_subquery_scan_remains_an_expression(alias_separator):
    compiled = compile_query({
        "scan": f"(SELECT 1 AS value){alias_separator}source",
        "select": ["source.value"],
    })


    with sqlite3.connect(":memory:") as connection:
        assert connection.execute(compiled).fetchall() == [(1,)]


def test_conditional_aggregate_expression_remains_sql():
    query = {
        "scan": "metrics",
        "agg": [{
            "fn": "sum",
            "expr": "CASE WHEN category = 'x' THEN amount ELSE 0 END",
            "as": "x_total",
        }],
        "select": ["x_total"],
    }
    compiled = compile_query(query)

    assert "SUM(CASE WHEN category = 'x' THEN amount ELSE 0 END)" in compiled
    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE metrics (category TEXT, amount INTEGER)")
        connection.executemany(
            "INSERT INTO metrics VALUES (?, ?)",
            [("x", 10), ("y", 20), ("x", 5)],
        )
        assert connection.execute(compiled).fetchall() == [(15,)]


@pytest.mark.parametrize("alias_separator", [" AS ", " "])
def test_relation_aliases_support_self_join_execution(alias_separator):
    query = {
        "scan": f"connected{alias_separator}c",
        "joins": [
            {
                "type": "inner",
                "table": f"atom{alias_separator}a1",
                "on": {"left": "c.atom_id", "right": "a1.atom_id"},
            },
            {
                "type": "inner",
                "table": f"atom{alias_separator}a2",
                "on": {"left": "c.atom_id2", "right": "a2.atom_id"},
            },
        ],
        "select": ["a1.element", "a2.element"],
    }
    compiled = compile_query(query)


    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE connected (atom_id TEXT, atom_id2 TEXT)")
        connection.execute("CREATE TABLE atom (atom_id TEXT, element TEXT)")
        connection.executemany("INSERT INTO atom VALUES (?, ?)", [("1", "p"), ("2", "n")])
        connection.execute("INSERT INTO connected VALUES ('1', '2')")
        assert connection.execute(compiled).fetchall() == [("p", "n")]


def test_quoted_relation_names_and_aliases_preserve_identifier_boundaries():
    with sqlite3.connect(":memory:") as connection:
        connection.execute('CREATE TABLE "source AS data" (id INTEGER, parent_id INTEGER)')
        connection.executemany('INSERT INTO "source AS data" VALUES (?, ?)', [(1, None), (2, 1)])
        unaliased = {"scan": '"source AS data"', "select": ['"source AS data".id'], "sort": [{"col": "id", "dir": "asc"}]}
        assert connection.execute(compile_query(unaliased)).fetchall() == [(1,), (2,)]
        aliased = {
            "scan": 'main."source AS data" "parent row"',
            "joins": [{"type": "inner", "table": '"source AS data" "child row"',
                       "on": {"left": '"parent row".id', "right": '"child row".parent_id'}}],
            "select": ['"parent row".id', '"child row".id'],
        }
        assert connection.execute(compile_query(aliased)).fetchall() == [(1, 2)]


@pytest.fixture
def scalar_extreme_connection():
    connection = sqlite3.connect(":memory:")
    connection.executescript("""
        CREATE TABLE samples (driver INTEGER, race INTEGER, value INTEGER);
        CREATE TABLE drivers (id INTEGER PRIMARY KEY);
        CREATE TABLE races (id INTEGER PRIMARY KEY);
    """)
    connection.executemany("INSERT INTO drivers VALUES (?)", ((i,) for i in range(100)))
    connection.executemany("INSERT INTO races VALUES (?)", ((i,) for i in range(10)))
    connection.executemany(
        "INSERT INTO samples VALUES (?, ?, ?)",
        ((i % 100, i % 10, None if i == 0 else -1 if i in (2, 7)
          else 3000 if i in (19, 21) else i) for i in range(2000)),
    )
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def scalar_extreme_query():
    return {
        "scan": "samples",
        "cte": [{"name": "best", "query": {
            "scan": "samples",
            "agg": [{"fn": "min", "col": "samples.value", "as": "extreme"}],
            "select": ["extreme"],
        }}],
        "joins": [
            {"type": "inner", "table": "drivers",
             "on": {"left": "samples.driver", "right": "drivers.id"}},
            {"type": "inner", "table": "races",
             "on": {"left": "samples.race", "right": "races.id"}},
            {"type": "inner", "table": "best",
             "on": {"left": "samples.value", "right": "best.extreme"}},
        ],
        "select": ["samples.value", "drivers.id", "races.id"],
    }


def _execute_with_step_budget(connection, query, budget):
    compiled = compile_query(query)
    def interrupt():
        nonlocal budget
        budget -= 1000
        return budget <= 0
    connection.set_progress_handler(interrupt, 1000)
    try:
        return connection.execute(compiled).fetchall()
    finally:
        connection.set_progress_handler(None, 0)


@pytest.mark.parametrize("fn, explicit_projection, expected", [
    ("min", True, [(-1, 2, 2), (-1, 7, 7)]),
    ("max", False, [(3000, 19, 9), (3000, 21, 1)]),
])
def test_joined_scalar_extreme_preserves_ties_with_bounded_work(
    scalar_extreme_connection, scalar_extreme_query, fn, explicit_projection, expected,
):
    body = scalar_extreme_query["cte"][0]["query"]
    body["agg"][0]["fn"] = fn
    if explicit_projection:
        body["select"] = [{"expr": f"{fn.upper()}(samples.value)", "as": "extreme"}]
    rows = _execute_with_step_budget(
        scalar_extreme_connection, scalar_extreme_query, 500_000,
    )
    assert sorted(rows) == expected


def test_scalar_extreme_keeps_null_row_for_null_and_empty_input(
    scalar_extreme_connection, scalar_extreme_query,
):
    cte = scalar_extreme_query["cte"][0]
    cte["name"] = "best time"
    cte["query"]["scan"] = 'samples AS "source data"'
    cte["query"]["agg"][0]["col"] = '"source data".value'
    query = {
        "cte": [cte], "scan": "drivers",
        "joins": [{"type": "cross", "table": '"best time" AS b'}],
        "filter": [{"col": "drivers.id", "op": "eq", "val": 0}],
        "select": ["b.extreme"],
    }
    scalar_extreme_connection.execute("UPDATE samples SET value = NULL")
    assert _execute_with_step_budget(scalar_extreme_connection, query, 500_000) == [(None,)]
    scalar_extreme_connection.execute("DELETE FROM samples")
    assert _execute_with_step_budget(scalar_extreme_connection, query, 500_000) == [(None,)]


def test_unused_aggregate_does_not_block_nonaggregate_cte_pushdown(
    scalar_extreme_connection, scalar_extreme_query,
):
    scalar_extreme_connection.execute("CREATE INDEX samples_value ON samples(value)")
    scalar_extreme_query["cte"][0]["query"]["select"] = [
        {"expr": "samples.value", "as": "extreme"},
    ]
    scalar_extreme_query["filter"] = [{"col": "samples.value", "op": "eq", "val": 3}]
    assert _execute_with_step_budget(
        scalar_extreme_connection, scalar_extreme_query, 5_000,
    ) == [(3, 3, 3)]



def test_implicit_alias_does_not_keep_the_original_table_in_scope():
    with pytest.raises(ValueError):
        compile_query({"scan": "nodes n", "select": ["nodes.id"]})


def test_mixed_alias_syntax_cannot_redeclare_a_visible_relation():
    with pytest.raises(ValueError):
        compile_query({
            "scan": "nodes n",
            "joins": [{"type": "inner", "table": "nodes AS n",
                       "on": {"left": "n.id", "right": "n.parent_id"}}],
            "select": ["n.id"],
        })


def test_relation_suffix_is_not_reinterpreted_as_an_implicit_alias():
    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE nodes (id INTEGER)")
        connection.execute("CREATE INDEX node_index ON nodes(id)")
        connection.execute("INSERT INTO nodes VALUES (7)")
        compiled = compile_query({"scan": "nodes INDEXED BY node_index", "select": ["id"]})
        assert connection.execute(compiled).fetchall() == [(7,)]
    with pytest.raises(ValueError):
        compile_query({"scan": "nodes WHERE", "select": ['"WHERE".id']})


def test_inner_aliases_in_raw_subquery_do_not_leak_into_outer_scope():
    query = {
        "scan": "outer_table",
        "select": [{
            "expr": "(SELECT MAX(inner_t.value) FROM inner_table AS inner_t)",
            "as": "maximum_value",
        }],
    }
    compiled = compile_query(query)

    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE outer_table (id INTEGER)")
        connection.execute("CREATE TABLE inner_table (value INTEGER)")
        connection.executemany("INSERT INTO outer_table VALUES (?)", [(1,), (2,)])
        connection.executemany("INSERT INTO inner_table VALUES (?)", [(3,), (5,)])
        assert connection.execute(compiled).fetchall() == [(5,), (5,)]


def test_scalar_subquery_comparison_executes():
    query = {
        "scan": "scores",
        "filter": [{
            "col": "score",
            "op": "gt",
            "val": {
                "subquery": {
                    "scan": "scores",
                    "agg": [{"fn": "avg", "col": "score", "as": "average_score"}],
                    "select": ["average_score"],
                },
            },
        }],
        "select": ["user_id"],
        "sort": [{"col": "user_id", "dir": "asc"}],
    }
    compiled = compile_query(query)

    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE scores (user_id INTEGER, score INTEGER)")
        connection.executemany("INSERT INTO scores VALUES (?, ?)", [(1, 1), (2, 5), (3, 9)])
        assert connection.execute(compiled).fetchall() == [(3,)]


def test_sort_sql_expression_executes_without_identifier_quoting():
    query = {
        "scan": "event",
        "joins": [{
            "type": "inner",
            "table": "budget",
            "on": {"left": "event.id", "right": "budget.event_id"},
        }],
        "select": ["event.name"],
        "sort": [{"col": "budget.spent / budget.amount", "dir": "desc"}],
        "limit": 1,
    }
    compiled = compile_query(query)

    assert "ORDER BY budget.spent / budget.amount DESC" in compiled
    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE event (id INTEGER, name TEXT)")
        connection.execute("CREATE TABLE budget (event_id INTEGER, spent REAL, amount REAL)")
        connection.executemany("INSERT INTO event VALUES (?, ?)", [(1, "A"), (2, "B")])
        connection.executemany("INSERT INTO budget VALUES (?, ?, ?)", [(1, 50, 100), (2, 200, 100)])
        assert connection.execute(compiled).fetchall() == [("B",)]


def test_multi_condition_semi_join_accepts_simple_conditions():
    query = {
        "scan": "users",
        "joins": [{
            "type": "semi",
            "table": "audit",
            "on": [
                {"col": "users.id", "op": "eq", "col2": "audit.user_id"},
                {"col": "users.org_id", "op": "eq", "col2": "audit.org_id"},
            ],
        }],
        "select": ["users.id"],
        "sort": [{"col": "users.id", "dir": "asc"}],
    }
    compiled = compile_query(query)

    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE users (id INTEGER, org_id INTEGER)")
        connection.execute("CREATE TABLE audit (user_id INTEGER, org_id INTEGER)")
        connection.executemany("INSERT INTO users VALUES (?, ?)", [(1, 10), (2, 20)])
        connection.executemany("INSERT INTO audit VALUES (?, ?)", [(1, 10), (1, 10), (2, 99)])
        assert connection.execute(compiled).fetchall() == [(1,)]


def test_having_expression_expands_hidden_aggregate_aliases():
    query = {
        "scan": "sales",
        "group": ["category"],
        "agg": [
            {"fn": "sum", "col": "amount", "as": "total_amount"},
            {"fn": "count_all", "as": "item_count"},
        ],
        "having": [{"col": "total_amount * 1.0 / item_count", "op": "gt", "val": 5}],
        "select": ["category"],
    }
    compiled = compile_query(query)

    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE sales (category TEXT, amount INTEGER)")
        connection.executemany("INSERT INTO sales VALUES (?, ?)", [("A", 4), ("A", 8), ("B", 3)])
        assert connection.execute(compiled).fetchall() == [("A",)]


def test_empty_group_does_not_emit_invalid_group_by_clause():
    query = {
        "scan": "items",
        "group": [],
        "agg": [{"fn": "count_all", "as": "item_count"}],
        "select": ["item_count"],
    }
    compiled = compile_query(query)

    assert "GROUP BY" not in compiled
    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE items (id INTEGER)")
        connection.executemany("INSERT INTO items VALUES (?)", [(1,), (2,)])
        assert connection.execute(compiled).fetchall() == [(2,)]


@pytest.mark.parametrize(("clauses", "expected"), [
    pytest.param({
        "select": ["picked.id", {"expr": "picked.amount", "as": "value"}, "dimensions.amount"],
        "sort": [{"col": "picked.id", "dir": "asc"}, {"col": "picked.amount", "dir": "asc"}],
    }, [(1, 10, 100), (1, 20, 100), (2, 70, 200)], id="projection"),
    pytest.param({
        "group": ["picked.id"],
        "agg": [{"fn": "sum", "col": "picked.amount", "as": "total"}],
        "select": ["picked.id", "total"],
        "sort": [{"col": "picked.id", "dir": "asc"}],
    }, [(1, 30), (2, 70)], id="aggregation"),
    pytest.param({
        "window": [{"fn": "row_number", "partition": ["picked.id"],
                    "order": [{"col": "picked.amount", "dir": "asc"}], "as": "position"}],
        "select": [{"expr": "picked.id + 0", "as": "fact_id"}, "position"],
        "sort": [{"col": "picked.id", "dir": "asc"}, {"col": "picked.amount", "dir": "asc"}],
    }, [(1, 1), (1, 2), (2, 1)], id="window-partition"),
])
def test_cte_qualified_columns_survive_join_with_same_named_physical_columns(clauses, expected):
    query = {
        "cte": [{"name": "picked", "query": {
            "scan": "facts", "select": ["facts.id", "facts.amount"],
        }}],
        "scan": "picked",
        "joins": [{"type": "inner", "table": "dimensions",
                   "on": {"left": "picked.id", "right": "dimensions.id"}}],
        **clauses,
    }
    with sqlite3.connect(":memory:") as connection:
        connection.executescript("""
            CREATE TABLE facts (id INTEGER, amount INTEGER);
            CREATE TABLE dimensions (id INTEGER, amount INTEGER);
            INSERT INTO facts VALUES (1, 10), (1, 20), (2, 70);
            INSERT INTO dimensions VALUES (1, 100), (2, 200);
        """)
        assert connection.execute(compile_query(query)).fetchall() == expected


def test_unique_joined_cte_column_is_not_rebound_to_main_cte():
    query = {
        "cte": [
            {"name": "picked", "query": {"scan": "facts", "select": ["facts.id"]}},
            {"name": "labels", "query": {"scan": "dimensions", "select": ["dimensions.id", "dimensions.label"]}},
        ],
        "scan": "picked",
        "joins": [{"type": "inner", "table": "labels",
                   "on": {"left": "picked.id", "right": "labels.id"}}],
        "select": ["label"],
        "sort": [{"col": "picked.id", "dir": "asc"}],
    }
    with sqlite3.connect(":memory:") as connection:
        connection.executescript("""
            CREATE TABLE facts (id INTEGER);
            CREATE TABLE dimensions (id INTEGER, label TEXT);
            INSERT INTO facts VALUES (1), (1), (2);
            INSERT INTO dimensions VALUES (1, 'north'), (2, 'south');
        """)
        assert connection.execute(compile_query(query)).fetchall() == [("north",), ("north",), ("south",)]


@pytest.fixture
def expression_binding_connection():
    with sqlite3.connect(":memory:") as connection:
        connection.executescript("""
            PRAGMA foreign_keys = ON;
            CREATE TABLE facts (id INTEGER PRIMARY KEY, n INTEGER, amount INTEGER, label TEXT);
            CREATE TABLE detail (id INTEGER PRIMARY KEY, fact_id INTEGER REFERENCES facts(id), n INTEGER);
            INSERT INTO facts VALUES (1, 700, 5, 'n'), (2, 800, 10, 'keep');
            INSERT INTO detail VALUES (1, 1, 40), (2, 2, 50);
        """)
        yield connection


@pytest.mark.parametrize("reference", ['facts.n', '"facts"."n"', 'facts /* boundary */ . n'])
def test_expression_binding_preserves_qualified_source_column(expression_binding_connection, reference):
    query = {"scan": "facts", "agg": [{"fn": "sum", "col": "facts.amount", "as": "n"}],
             "select": [{"expr": reference, "as": "value"}],
             "sort": [{"col": "facts.id", "dir": "asc"}]}
    assert expression_binding_connection.execute(compile_query(query)).fetchall() == [(700,), (800,)]


def test_expression_binding_distinguishes_window_alias_from_source(expression_binding_connection):
    query = {"scan": "facts", "window": [{"fn": "row_number", "order": [{"col": "facts.id", "dir": "asc"}], "as": "n"}],
             "select": [{"expr": "facts.n + n", "as": "value"}],
             "sort": [{"col": "facts.id", "dir": "asc"}]}
    assert expression_binding_connection.execute(compile_query(query)).fetchall() == [(701,), (802,)]


def test_expression_binding_preserves_literals_functions_and_types(expression_binding_connection):
    query = {"scan": "facts", "agg": [
        {"fn": "sum", "col": "facts.amount", "as": "total"},
        {"fn": "count_all", "as": "abs"}, {"fn": "count_all", "as": "REAL"}],
        "select": [{"expr": "CAST(abs(total) AS REAL)", "as": "value"},
                   {"expr": "'total abs REAL'", "as": "label"}]}
    assert expression_binding_connection.execute(compile_query(query)).fetchall() == [(15.0, "total abs REAL")]


def test_expression_binding_does_not_enter_scalar_subqueries(expression_binding_connection):
    query = {"scan": "facts", "agg": [{"fn": "sum", "col": "facts.amount", "as": "n"}],
             "select": [
                 {"expr": "n + (SELECT n FROM detail WHERE detail.fact_id = 1)", "as": "value"},
                 {"expr": "(WITH n AS (SELECT 8 AS n) SELECT n FROM n) + n", "as": "cte_value"}]}
    assert expression_binding_connection.execute(compile_query(query)).fetchall() == [(55, 23)]


def test_expression_binding_does_not_rewrite_inserted_aggregate_sql(expression_binding_connection):
    query = {"scan": "facts", "agg": [
        {"fn": "sum", "col": "facts.n", "as": "total_value"},
        {"fn": "min", "col": "facts.amount", "as": "n"}],
        "select": [{"expr": "total_value", "as": "value"}]}
    assert expression_binding_connection.execute(compile_query(query)).fetchall() == [(1500,)]


def test_expression_binding_distinguishes_quoted_aliases_and_numbers(expression_binding_connection):
    query = {"scan": "facts", "agg": [
        {"fn": "count_all", "as": "1"}, {"fn": "sum", "col": "facts.amount", "as": "gross total"}],
        "select": [{"expr": '"1" + "gross total" + 1', "as": "value"}]}
    assert expression_binding_connection.execute(compile_query(query)).fetchall() == [(18,)]


def test_expression_binding_does_not_rescue_missing_cte_output(expression_binding_connection):
    query = {"cte": [{"name": "picked", "query": {"scan": "facts", "select": ["facts.id"]}}],
             "scan": "picked", "agg": [{"fn": "sum", "col": "picked.id", "as": "n"}],
             "select": [{"expr": "picked.n", "as": "value"}]}
    with pytest.raises(sqlite3.OperationalError):
        expression_binding_connection.execute(compile_query(query)).fetchall()


def test_expression_binding_quoted_having_alias_uses_aggregate(expression_binding_connection):
    query = {"scan": "facts", "group": ["facts.label"],
             "agg": [{"fn": "sum", "col": "facts.amount", "as": "n"}],
             "select": ["facts.label", "n"], "having": [{"col": '"n"', "op": "gt", "val": 7}]}
    assert expression_binding_connection.execute(compile_query(query)).fetchall() == [("keep", 10)]


def test_expression_binding_first_value_accepts_aggregate_argument(expression_binding_connection):
    query = {"scan": "facts", "group": ["facts.label"],
             "agg": [{"fn": "sum", "col": "facts.amount", "as": "total"}],
             "window": [{"fn": "first_value", "col": "total", "order": [{"col": "facts.label", "dir": "asc"}], "as": "first_total"}],
             "select": ["facts.label", "total", "first_total"],
             "sort": [{"col": "facts.label", "dir": "asc"}]}
    assert expression_binding_connection.execute(compile_query(query)).fetchall() == [("keep", 10, 10), ("n", 5, 10)]


def test_expression_binding_sort_can_use_hidden_aggregate(expression_binding_connection):
    query = {"scan": "facts", "group": ["facts.label"],
             "agg": [{"fn": "sum", "col": "facts.amount", "as": "total"}],
             "select": ["facts.label"], "sort": [{"col": "total + 1", "dir": "asc"}]}
    cursor = expression_binding_connection.execute(compile_query(query))
    assert [column[0] for column in cursor.description] == ["label"]
    assert cursor.fetchall() == [("n",), ("keep",)]
