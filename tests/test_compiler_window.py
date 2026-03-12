"""
Tests for window function support in the Forge compiler.
Covers WindowRanking, WindowAgg, WindowNav, and schema validation.
"""
import sys
from pathlib import Path

import pytest
import jsonschema

sys.path.insert(0, str(Path(__file__).parent.parent))

from forge.compiler import compile_query


def sql(q: dict) -> str:
    return " ".join(compile_query(q).split())


# ── WindowRanking ─────────────────────────────────────────────────────────────

def test_row_number_with_partition_and_order():
    result = sql({
        "scan": "orders",
        "window": [{"fn": "row_number", "partition": ["orders.user_id"],
                    "order": [{"col": "orders.created_at", "dir": "desc"}], "as": "rn"}],
        "select": ["orders.id", "rn"],
    })
    assert "ROW_NUMBER() OVER (PARTITION BY orders.user_id ORDER BY orders.created_at DESC) AS rn" in result


def test_rank_order_only_no_partition():
    result = sql({
        "scan": "orders",
        "window": [{"fn": "rank", "order": [{"col": "orders.total_amount", "dir": "desc"}], "as": "rnk"}],
        "select": ["orders.id", "rnk"],
    })
    assert "RANK() OVER (ORDER BY orders.total_amount DESC) AS rnk" in result


def test_dense_rank():
    result = sql({
        "scan": "orders",
        "window": [{"fn": "dense_rank",
                    "partition": ["orders.status"],
                    "order": [{"col": "orders.total_amount", "dir": "asc"}],
                    "as": "dr"}],
        "select": ["orders.status", "dr"],
    })
    assert "DENSE_RANK() OVER (PARTITION BY orders.status ORDER BY orders.total_amount ASC) AS dr" in result


def test_ranking_no_partition_no_order():
    """Ranking with empty OVER () is valid SQL for global rank."""
    result = sql({
        "scan": "orders",
        "window": [{"fn": "row_number", "as": "rn"}],
        "select": ["orders.id", "rn"],
    })
    assert "ROW_NUMBER() OVER () AS rn" in result


# ── WindowAgg ─────────────────────────────────────────────────────────────────

def test_sum_over_partition():
    result = sql({
        "scan": "orders",
        "window": [{"fn": "sum", "col": "orders.total_amount",
                    "partition": ["orders.user_id"], "as": "user_total"}],
        "select": ["orders.id", "user_total"],
    })
    assert "SUM(orders.total_amount) OVER (PARTITION BY orders.user_id) AS user_total" in result


def test_avg_over_partition_and_order():
    result = sql({
        "scan": "orders",
        "window": [{"fn": "avg", "col": "orders.total_amount",
                    "partition": ["orders.status"],
                    "order": [{"col": "orders.created_at", "dir": "asc"}],
                    "as": "running_avg"}],
        "select": ["orders.id", "running_avg"],
    })
    assert "AVG(orders.total_amount) OVER (PARTITION BY orders.status ORDER BY orders.created_at ASC) AS running_avg" in result


def test_count_over_partition():
    result = sql({
        "scan": "orders",
        "window": [{"fn": "count", "col": "orders.id",
                    "partition": ["orders.user_id"], "as": "user_order_count"}],
        "select": ["orders.user_id", "user_order_count"],
    })
    assert "COUNT(orders.id) OVER (PARTITION BY orders.user_id) AS user_order_count" in result


def test_min_max_over():
    result = sql({
        "scan": "orders",
        "window": [
            {"fn": "min", "col": "orders.total_amount", "partition": ["orders.user_id"], "as": "min_amt"},
            {"fn": "max", "col": "orders.total_amount", "partition": ["orders.user_id"], "as": "max_amt"},
        ],
        "select": ["orders.id", "min_amt", "max_amt"],
    })
    assert "MIN(orders.total_amount) OVER (PARTITION BY orders.user_id) AS min_amt" in result
    assert "MAX(orders.total_amount) OVER (PARTITION BY orders.user_id) AS max_amt" in result


# ── WindowNav (LAG / LEAD) ────────────────────────────────────────────────────

def test_lag_col_only():
    """LAG with just col — offset and default omitted."""
    result = sql({
        "scan": "orders",
        "window": [{"fn": "lag", "col": "orders.total_amount",
                    "order": [{"col": "orders.created_at", "dir": "asc"}],
                    "as": "prev_amount"}],
        "select": ["orders.id", "prev_amount"],
    })
    assert "LAG(orders.total_amount) OVER (ORDER BY orders.created_at ASC) AS prev_amount" in result


def test_lag_with_offset():
    result = sql({
        "scan": "orders",
        "window": [{"fn": "lag", "col": "orders.total_amount", "offset": 2,
                    "order": [{"col": "orders.created_at", "dir": "asc"}],
                    "as": "prev2"}],
        "select": ["orders.id", "prev2"],
    })
    assert "LAG(orders.total_amount, 2) OVER (ORDER BY orders.created_at ASC) AS prev2" in result


def test_lag_with_offset_and_default():
    result = sql({
        "scan": "orders",
        "window": [{"fn": "lag", "col": "orders.total_amount", "offset": 1, "default": 0,
                    "order": [{"col": "orders.created_at", "dir": "asc"}],
                    "as": "prev_or_zero"}],
        "select": ["orders.id", "prev_or_zero"],
    })
    assert "LAG(orders.total_amount, 1, 0) OVER (ORDER BY orders.created_at ASC) AS prev_or_zero" in result


def test_lead_with_partition_and_order():
    result = sql({
        "scan": "orders",
        "window": [{"fn": "lead", "col": "orders.total_amount", "offset": 1, "default": 0,
                    "partition": ["orders.user_id"],
                    "order": [{"col": "orders.created_at", "dir": "asc"}],
                    "as": "next_amount"}],
        "select": ["orders.id", "next_amount"],
    })
    assert "LEAD(orders.total_amount, 1, 0) OVER (PARTITION BY orders.user_id ORDER BY orders.created_at ASC) AS next_amount" in result


# ── multiple window expressions ───────────────────────────────────────────────

def test_multiple_window_exprs_in_select():
    result = sql({
        "scan": "orders",
        "window": [
            {"fn": "row_number", "partition": ["orders.user_id"],
             "order": [{"col": "orders.created_at", "dir": "desc"}], "as": "rn"},
            {"fn": "sum", "col": "orders.total_amount",
             "partition": ["orders.user_id"], "as": "cum_total"},
        ],
        "select": ["orders.id", "rn", "cum_total"],
    })
    assert "ROW_NUMBER()" in result
    assert "SUM(orders.total_amount)" in result
    assert "cum_total" in result


# ── mixed: window + regular agg in same query ─────────────────────────────────

def test_window_alongside_plain_columns():
    result = sql({
        "scan": "orders",
        "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
        "window": [{"fn": "row_number",
                    "partition": ["orders.user_id"],
                    "order": [{"col": "orders.total_amount", "dir": "desc"}],
                    "as": "rn"}],
        "select": ["orders.id", "orders.user_id", "orders.total_amount", "rn"],
    })
    assert "WHERE orders.status = 'completed'" in result
    assert "ROW_NUMBER() OVER (PARTITION BY orders.user_id ORDER BY orders.total_amount DESC) AS rn" in result


# ── multi-column PARTITION BY ─────────────────────────────────────────────────

def test_multi_column_partition_by():
    result = sql({
        "scan": "orders",
        "window": [{"fn": "rank",
                    "partition": ["orders.user_id", "orders.status"],
                    "order": [{"col": "orders.total_amount", "dir": "desc"}],
                    "as": "rnk"}],
        "select": ["orders.id", "rnk"],
    })
    assert "PARTITION BY orders.user_id, orders.status" in result


# ── schema validation ─────────────────────────────────────────────────────────

def test_ranking_fn_rejects_col_field():
    """row_number / rank / dense_rank must not have col — schema rejects it."""
    with pytest.raises((ValueError, jsonschema.ValidationError)):
        compile_query({
            "scan": "orders",
            "window": [{"fn": "row_number", "col": "orders.id",
                        "order": [{"col": "orders.id", "dir": "asc"}], "as": "rn"}],
            "select": ["rn"],
        })


def test_window_agg_requires_col():
    """SUM/AVG/COUNT/MIN/MAX over window require col."""
    with pytest.raises((ValueError, jsonschema.ValidationError)):
        compile_query({
            "scan": "orders",
            "window": [{"fn": "sum", "as": "s"}],
            "select": ["s"],
        })


def test_window_nav_requires_col():
    with pytest.raises((ValueError, jsonschema.ValidationError)):
        compile_query({
            "scan": "orders",
            "window": [{"fn": "lag", "as": "p",
                        "order": [{"col": "orders.id", "dir": "asc"}]}],
            "select": ["p"],
        })


def test_window_expr_requires_as():
    with pytest.raises((ValueError, jsonschema.ValidationError)):
        compile_query({
            "scan": "orders",
            "window": [{"fn": "row_number",
                        "order": [{"col": "orders.id", "dir": "asc"}]}],
            "select": ["orders.id"],
        })


def test_invalid_window_fn_rejected():
    with pytest.raises((ValueError, jsonschema.ValidationError)):
        compile_query({
            "scan": "orders",
            "window": [{"fn": "ntile", "as": "bucket",
                        "order": [{"col": "orders.id", "dir": "asc"}]}],
            "select": ["bucket"],
        })


def test_nav_offset_must_be_positive():
    with pytest.raises((ValueError, jsonschema.ValidationError)):
        compile_query({
            "scan": "orders",
            "window": [{"fn": "lag", "col": "orders.id", "offset": 0,
                        "order": [{"col": "orders.id", "dir": "asc"}], "as": "p"}],
            "select": ["p"],
        })


# ── qualify（窗口函数结果过滤 / per-group TopN）────────────────────────────────

def test_qualify_per_group_topn():
    """每个品类成本排名前3的商品（DENSE_RANK + qualify）"""
    result = compile_query({
        "scan": "products",
        "select": ["products.name", "products.category", "products.cost_price", "cost_rank"],
        "window": [{"fn": "dense_rank",
                    "partition": ["products.category"],
                    "order": [{"col": "products.cost_price", "dir": "desc"}],
                    "as": "cost_rank"}],
        "qualify": [{"col": "cost_rank", "op": "lte", "val": 3}],
    })
    assert "SELECT * FROM (" in result
    assert "DENSE_RANK() OVER" in result
    assert "cost_rank" in result
    assert ") AS _q" in result
    assert "WHERE cost_rank <= 3" in result


def test_qualify_row_number_topn():
    """每个用户消费排名第一的订单（ROW_NUMBER + qualify = 1）"""
    result = compile_query({
        "scan": "orders",
        "joins": [{"type": "inner", "table": "users",
                   "on": {"left": "orders.user_id", "right": "users.id"}}],
        "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
        "select": ["users.name", "orders.total_amount", "orders.created_at", "rn"],
        "window": [{"fn": "row_number",
                    "partition": ["orders.user_id"],
                    "order": [{"col": "orders.total_amount", "dir": "desc"}],
                    "as": "rn"}],
        "qualify": [{"col": "rn", "op": "eq", "val": 1}],
    })
    assert "SELECT * FROM (" in result
    assert "ROW_NUMBER() OVER" in result
    assert "PARTITION BY orders.user_id" in result
    assert "WHERE rn = 1" in result


def test_qualify_does_not_affect_non_qualify_queries():
    """无 qualify 字段时输出不变（不包一层子查询）"""
    result = compile_query({
        "scan": "products",
        "select": ["products.name", "products.category", "rk"],
        "window": [{"fn": "rank",
                    "partition": ["products.category"],
                    "order": [{"col": "products.cost_price", "dir": "desc"}],
                    "as": "rk"}],
    })
    assert "SELECT * FROM" not in result
    assert result.startswith("SELECT")
