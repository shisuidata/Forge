"""
Extended compiler tests — complex queries, all join types, all aggregations,
edge cases not covered by test_compiler.py.
"""
import sys
from pathlib import Path

import pytest
import jsonschema

sys.path.insert(0, str(Path(__file__).parent.parent))

from forge.compiler import compile_query


def sql(q: dict) -> str:
    return " ".join(compile_query(q).split())


# ── all join types ────────────────────────────────────────────────────────────

def test_right_join():
    result = sql({
        "scan": "orders",
        "joins": [{"type": "right", "table": "users",
                   "on": {"left": "orders.user_id", "right": "users.id"}}],
        "select": ["users.name", "orders.id"],
    })
    assert "RIGHT JOIN users ON orders.user_id = users.id" in result


def test_full_join():
    result = sql({
        "scan": "orders",
        "joins": [{"type": "full", "table": "users",
                   "on": {"left": "orders.user_id", "right": "users.id"}}],
        "select": ["users.id", "orders.id"],
    })
    assert "FULL OUTER JOIN users ON orders.user_id = users.id" in result


def test_semi_join_no_join_keyword():
    """Semi-join compiles to EXISTS, not JOIN."""
    result = sql({
        "scan": "users",
        "joins": [{"type": "semi", "table": "orders",
                   "on": {"left": "users.id", "right": "orders.user_id"}}],
        "select": ["users.id", "users.name"],
    })
    assert "WHERE EXISTS (SELECT 1 FROM orders WHERE users.id = orders.user_id)" in result
    assert "JOIN" not in result


# ── three-way join ─────────────────────────────────────────────────────────────

def test_three_way_join():
    result = sql({
        "scan": "order_items",
        "joins": [
            {"type": "inner", "table": "orders",
             "on": {"left": "order_items.order_id", "right": "orders.id"}},
            {"type": "inner", "table": "users",
             "on": {"left": "orders.user_id", "right": "users.id"}},
        ],
        "select": ["users.name", "orders.id", "order_items.unit_price"],
    })
    assert "INNER JOIN orders ON order_items.order_id = orders.id" in result
    assert "INNER JOIN users ON orders.user_id = users.id" in result
    # join order preserved: orders JOIN appears before users JOIN
    assert result.index("INNER JOIN orders") < result.index("INNER JOIN users")


# ── all aggregation functions ──────────────────────────────────────────────────

def test_all_aggregation_functions():
    cases = [
        ({"fn": "sum",            "col": "orders.total_amount", "as": "s"}, "SUM(orders.total_amount) AS s"),
        ({"fn": "avg",            "col": "orders.total_amount", "as": "a"}, "AVG(orders.total_amount) AS a"),
        ({"fn": "min",            "col": "orders.total_amount", "as": "mn"},"MIN(orders.total_amount) AS mn"),
        ({"fn": "max",            "col": "orders.total_amount", "as": "mx"},"MAX(orders.total_amount) AS mx"),
        ({"fn": "count",          "col": "orders.id",           "as": "c"}, "COUNT(orders.id) AS c"),
        ({"fn": "count_distinct", "col": "orders.user_id",      "as": "u"}, "COUNT(DISTINCT orders.user_id) AS u"),
        ({"fn": "count_all",                                     "as": "n"}, "COUNT(*) AS n"),
    ]
    for agg, expected in cases:
        result = sql({"scan": "orders", "agg": [agg], "select": [agg["as"]]})
        assert expected in result, f"Expected '{expected}' in: {result}"


def test_multiple_aggregations():
    result = sql({
        "scan": "orders",
        "group": ["orders.status"],
        "agg": [
            {"fn": "count_all",            "as": "total"},
            {"fn": "sum", "col": "orders.total_amount", "as": "revenue"},
            {"fn": "avg", "col": "orders.total_amount", "as": "avg_val"},
        ],
        "select": ["orders.status", "total", "revenue", "avg_val"],
    })
    assert "COUNT(*) AS total" in result
    assert "SUM(orders.total_amount) AS revenue" in result
    assert "AVG(orders.total_amount) AS avg_val" in result


# ── filter operator edge cases ─────────────────────────────────────────────────

def test_in_operator_with_integers():
    result = sql({
        "scan": "orders",
        "filter": [{"col": "orders.user_id", "op": "in", "val": [1, 2, 3]}],
        "select": ["orders.id"],
    })
    assert "orders.user_id IN (1, 2, 3)" in result


def test_between_with_floats():
    result = sql({
        "scan": "orders",
        "filter": [{"col": "orders.total_amount", "op": "between", "lo": 100.5, "hi": 999.9}],
        "select": ["orders.id"],
    })
    assert "orders.total_amount BETWEEN 100.5 AND 999.9" in result


def test_boolean_true_false():
    result_true  = sql({"scan":"users","filter":[{"col":"users.is_vip","op":"eq","val":True}],"select":["users.id"]})
    result_false = sql({"scan":"users","filter":[{"col":"users.is_vip","op":"eq","val":False}],"select":["users.id"]})
    assert "users.is_vip = TRUE"  in result_true
    assert "users.is_vip = FALSE" in result_false


def test_nested_or_inside_and():
    """AND of multiple OR groups."""
    result = sql({
        "scan": "orders",
        "filter": [
            {"or": [
                {"col": "orders.status", "op": "eq", "val": "completed"},
                {"col": "orders.status", "op": "eq", "val": "pending"},
            ]},
            {"col": "orders.total_amount", "op": "gt", "val": 0},
        ],
        "select": ["orders.id"],
    })
    assert "(orders.status = 'completed' OR orders.status = 'pending')" in result
    assert "orders.total_amount > 0" in result
    assert "WHERE" in result


def test_multiple_having_conditions():
    result = sql({
        "scan": "orders",
        "group":  ["orders.user_id"],
        "agg":    [
            {"fn": "count_all",                                     "as": "n"},
            {"fn": "sum", "col": "orders.total_amount", "as": "total"},
        ],
        "having": [
            {"col": "n",     "op": "gte", "val": 2},
            {"col": "total", "op": "gt",  "val": 1000},
        ],
        "select": ["orders.user_id", "n", "total"],
    })
    # Compiler expands aliases back to function expressions in HAVING
    assert "HAVING" in result
    assert ">= 2" in result
    assert "> 1000" in result


# ── multiple GROUP BY ─────────────────────────────────────────────────────────

def test_multiple_group_by_columns():
    result = sql({
        "scan": "orders",
        "joins": [{"type": "inner", "table": "users",
                   "on": {"left": "orders.user_id", "right": "users.id"}}],
        "group":  ["users.city", "orders.status"],
        "agg":    [{"fn": "count_all", "as": "n"}],
        "select": ["users.city", "orders.status", "n"],
    })
    assert "GROUP BY users.city, orders.status" in result


# ── no joins (simple scan) ────────────────────────────────────────────────────

def test_simple_scan_no_join():
    result = sql({
        "scan": "products",
        "select": ["products.id", "products.name", "products.cost_price"],
    })
    assert result == "SELECT products.id, products.name, products.cost_price FROM products"


def test_scan_with_all_clauses_no_join():
    result = sql({
        "scan": "orders",
        "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
        "group":  ["orders.status"],
        "agg":    [{"fn": "count_all", "as": "n"}],
        "having": [{"col": "n", "op": "gt", "val": 10}],
        "select": ["orders.status", "n"],
        "sort":   [{"col": "n", "dir": "desc"}],
        "limit":  5,
    })
    assert "FROM orders" in result
    assert "JOIN" not in result
    assert "WHERE" in result
    assert "GROUP BY" in result
    assert "HAVING" in result
    assert "ORDER BY" in result
    assert "LIMIT 5" in result


# ── clause ordering (comprehensive) ──────────────────────────────────────────

def test_anti_join_then_filter():
    """Anti-join produces WHERE right_key IS NULL; additional filter should AND with it."""
    result = sql({
        "scan": "orders",
        "joins": [{"type": "anti", "table": "order_items",
                   "on": {"left": "orders.id", "right": "order_items.order_id"}}],
        "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
        "select": ["orders.id"],
    })
    assert "order_items.order_id IS NULL" in result
    assert "orders.status = 'completed'" in result
    assert "WHERE" in result


# ── schema validation edge cases ──────────────────────────────────────────────

def test_count_all_coerces_col_field():
    """count_all with a 'col' field is auto-corrected by _coerce() to COUNT(*)."""
    sql = compile_query({
        "scan": "orders",
        "agg": [{"fn": "count_all", "col": "orders.id", "as": "n"}],
        "select": ["n"],
    })
    assert "COUNT(*) AS n" in sql


# ── Fix 6: having with inline fn field ────────────────────────────────────────

def test_having_fn_coerced_to_alias():
    """having 中出现 fn 字段（内联聚合）自动替换为 agg alias。"""
    result = sql({
        "scan": "orders",
        "joins": [{"type": "inner", "table": "users",
                   "on": {"left": "orders.user_id", "right": "users.id"}}],
        "group":  ["users.city"],
        "agg":    [{"fn": "avg", "col": "orders.total_amount", "as": "avg_amount"}],
        "having": [{"col": "orders.total_amount", "fn": "avg", "op": "gt", "val": 800}],
        "select": ["users.city", "avg_amount"],
    })
    # Compiler expands fn field to full expression in HAVING
    assert "HAVING" in result
    assert "> 800" in result
    assert "AVG" in result


# ── select expr objects ────────────────────────────────────────────────────────

def test_select_expr_object():
    """select 中可以使用 expr 对象输出计算列。"""
    result = sql({
        "scan": "order_items",
        "select": [
            "order_items.order_id",
            {"expr": "order_items.quantity * order_items.unit_price", "as": "revenue"},
        ],
    })
    assert "order_items.quantity * order_items.unit_price AS revenue" in result
    assert "order_items.order_id" in result


# ── joins.on multi-condition ───────────────────────────────────────────────────

def test_join_multi_condition():
    """joins.on 支持多条件数组。"""
    result = sql({
        "scan": "orders",
        "joins": [{
            "type": "inner",
            "table": "budgets",
            "on": [
                {"col": "orders.dept_id", "op": "eq", "val": 1},
                {"col": "orders.year",    "op": "eq", "val": 2024},
            ],
        }],
        "select": ["orders.id"],
    })
    assert "INNER JOIN budgets ON" in result
    assert "orders.dept_id = 1" in result
    assert "orders.year = 2024" in result
    assert " AND " in result


def test_anti_join_multi_condition_raises():
    """anti join 不允许多条件 on。"""
    with pytest.raises((ValueError, jsonschema.ValidationError, Exception)):
        compile_query({
            "scan": "users",
            "joins": [{
                "type": "anti",
                "table": "orders",
                "on": [
                    {"col": "users.id",   "op": "eq", "val": 1},
                    {"col": "users.city", "op": "eq", "val": "北京"},
                ],
            }],
            "select": ["users.id"],
        })


# ── offset ─────────────────────────────────────────────────────────────────────

def test_offset():
    """offset 字段编译为 OFFSET 子句。"""
    result = sql({
        "scan": "orders",
        "select": ["orders.id"],
        "sort":   [{"col": "orders.created_at", "dir": "desc"}],
        "limit":  10,
        "offset": 20,
    })
    assert "LIMIT 10" in result
    assert "OFFSET 20" in result
    # OFFSET 必须在 LIMIT 之后
    assert result.index("LIMIT") < result.index("OFFSET")


# ── explain field is ignored ───────────────────────────────────────────────────

def test_explain_field_ignored():
    """explain 字段不参与编译输出。"""
    result = sql({
        "scan": "orders",
        "select": ["orders.id"],
        "explain": "查询所有订单ID",
    })
    assert "explain" not in result.lower()
    assert "查询所有订单ID" not in result
    assert "SELECT orders.id FROM orders" == result


# ── val type system ────────────────────────────────────────────────────────────

def test_val_date_object():
    """{"$date": "..."} 编译为带引号的日期字符串。"""
    result = sql({
        "scan": "orders",
        "filter": [{"col": "orders.created_at", "op": "gte", "val": {"$date": "2024-01-01"}}],
        "select": ["orders.id"],
    })
    assert "orders.created_at >= '2024-01-01'" in result


def test_val_null():
    """null 编译为 NULL。"""
    result = sql({
        "scan": "orders",
        "window": [{
            "fn": "lag", "col": "orders.total_amount", "offset": 1, "default": None,
            "partition": ["orders.user_id"],
            "order": [{"col": "orders.created_at", "dir": "asc"}],
            "as": "prev",
        }],
        "select": ["orders.id", "prev"],
    })
    assert "LAG(orders.total_amount, 1, NULL)" in result


def test_agg_without_as_raises():
    with pytest.raises((ValueError, jsonschema.ValidationError)):
        compile_query({
            "scan": "orders",
            "agg": [{"fn": "sum", "col": "orders.total_amount"}],  # missing "as"
            "select": ["total"],
        })


def test_invalid_filter_op_raises():
    with pytest.raises((ValueError, jsonschema.ValidationError)):
        compile_query({
            "scan": "orders",
            "filter": [{"col": "orders.id", "op": "not_exists", "val": 1}],
            "select": ["orders.id"],
        })


def test_limit_must_be_positive():
    with pytest.raises((ValueError, jsonschema.ValidationError, Exception)):
        compile_query({"scan": "orders", "select": ["orders.id"], "limit": -1})


# ── determinism ───────────────────────────────────────────────────────────────

def test_same_input_always_same_output():
    q = {
        "scan": "orders",
        "joins": [{"type": "inner", "table": "users",
                   "on": {"left": "orders.user_id", "right": "users.id"}}],
        "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
        "group":  ["users.city"],
        "agg":    [{"fn": "sum", "col": "orders.total_amount", "as": "rev"}],
        "select": ["users.city", "rev"],
        "sort":   [{"col": "rev", "dir": "desc"}],
        "limit":  10,
    }
    assert compile_query(q) == compile_query(q)
    assert compile_query(q) == compile_query(q)


# ── OR 嵌套 AND 复合条件 ──────────────────────────────────────────────────────

def test_or_with_and_nested():
    """(name LIKE '%明%') OR (created_at >= '2024-01-01' AND is_vip = 1)"""
    result = sql({
        "scan": "users",
        "filter": [
            {"or": [
                {"col": "users.name", "op": "like", "val": "%明%"},
                {"and": [
                    {"col": "users.created_at", "op": "gte", "val": "2024-01-01"},
                    {"col": "users.is_vip",     "op": "eq",  "val": 1},
                ]},
            ]}
        ],
        "select": ["users.name", "users.city", "users.is_vip"],
    })
    assert "WHERE" in result
    assert "users.name LIKE '%明%'" in result
    assert "users.created_at >= '2024-01-01'" in result
    assert "users.is_vip = 1" in result
    assert " OR " in result
    assert " AND " in result


def test_or_with_multiple_and_branches():
    """(status='completed' AND amount>500) OR (status='pending' AND is_vip=1)"""
    result = sql({
        "scan": "orders",
        "joins": [{"type": "inner", "table": "users",
                   "on": {"left": "orders.user_id", "right": "users.id"}}],
        "filter": [
            {"or": [
                {"and": [
                    {"col": "orders.status",       "op": "eq",  "val": "completed"},
                    {"col": "orders.total_amount", "op": "gt",  "val": 500},
                ]},
                {"and": [
                    {"col": "orders.status", "op": "eq", "val": "pending"},
                    {"col": "users.is_vip",  "op": "eq", "val": 1},
                ]},
            ]}
        ],
        "select": ["orders.id", "orders.status", "orders.total_amount"],
    })
    assert "orders.status = 'completed'" in result
    assert "orders.total_amount > 500" in result
    assert "orders.status = 'pending'" in result
    assert "users.is_vip = 1" in result
    assert result.count(" OR ") == 1
    assert result.count(" AND ") >= 2


def test_or_mixed_simple_and_and_branch():
    """simple OR (A AND B) 混合分支"""
    result = sql({
        "scan": "users",
        "filter": [
            {"or": [
                {"col": "users.city", "op": "eq", "val": "北京"},
                {"and": [
                    {"col": "users.is_vip",    "op": "eq",  "val": 1},
                    {"col": "users.created_at","op": "gte", "val": "2023-01-01"},
                ]},
            ]}
        ],
        "select": ["users.name", "users.city"],
    })
    assert "users.city = '北京'" in result
    assert "users.is_vip = 1" in result
    assert " OR " in result


def test_and_branch_rejected_at_top_level():
    """and 分支不能直接出现在 filter 顶层（必须在 or 内），schema 应报错"""
    import pytest, jsonschema
    with pytest.raises((ValueError, jsonschema.ValidationError)):
        compile_query({
            "scan": "users",
            "filter": [
                {"and": [
                    {"col": "users.is_vip", "op": "eq", "val": 1},
                    {"col": "users.city",   "op": "eq", "val": "北京"},
                ]}
            ],
            "select": ["users.name"],
        })


# ── UNION / UNION ALL ─────────────────────────────────────────────────────────

def test_union_all_two_branches():
    """UNION ALL 两个分支合并结果集"""
    result = sql({
        "scan": "orders",
        "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
        "select": ["orders.user_id", "orders.total_amount"],
        "union": [{"mode": "union_all", "query": {
            "scan": "orders",
            "filter": [{"col": "orders.status", "op": "eq", "val": "refunded"}],
            "select": ["orders.user_id", "orders.total_amount"],
        }}],
    })
    assert "UNION ALL" in result
    assert result.count("SELECT") == 2
    assert "completed" in result
    assert "refunded" in result


def test_union_distinct():
    """UNION（去重）"""
    result = sql({
        "scan": "users",
        "filter": [{"col": "users.city", "op": "eq", "val": "Beijing"}],
        "select": ["users.id"],
        "union": [{"mode": "union", "query": {
            "scan": "users",
            "filter": [{"col": "users.city", "op": "eq", "val": "Shanghai"}],
            "select": ["users.id"],
        }}],
    })
    assert "UNION\n" in result or "UNION ALL" not in result
    assert "Beijing" in result
    assert "Shanghai" in result


def test_union_sort_limit_applies_to_whole():
    """sort/limit 在 UNION 时应出现在最后（作用于整体）"""
    result = sql({
        "scan": "products",
        "select": ["products.name", "products.cost_price"],
        "filter": [{"col": "products.category", "op": "eq", "val": "A"}],
        "union": [{"mode": "union_all", "query": {
            "scan": "products",
            "filter": [{"col": "products.category", "op": "eq", "val": "B"}],
            "select": ["products.name", "products.cost_price"],
        }}],
        "sort": [{"col": "products.cost_price", "dir": "desc"}],
        "limit": 5,
    })
    # ORDER BY 和 LIMIT 必须在 UNION ALL 之后
    union_pos = result.index("UNION ALL")
    order_pos = result.index("ORDER BY")
    limit_pos = result.index("LIMIT 5")
    assert union_pos < order_pos < limit_pos


def test_union_with_cte():
    """UNION 与 CTE 结合：WITH 子句对所有分支可见"""
    result = sql({
        "cte": [{"name": "active", "query": {
            "scan": "users",
            "filter": [{"col": "users.is_vip", "op": "eq", "val": 1}],
            "select": ["users.id", "users.city"],
        }}],
        "scan": "active",
        "select": ["active.city"],
        "union": [{"mode": "union_all", "query": {
            "scan": "active",
            "filter": [{"col": "active.city", "op": "eq", "val": "Beijing"}],
            "select": ["active.city"],
        }}],
    })
    assert result.startswith("WITH ")
    assert "UNION ALL" in result


# ── GROUP_CONCAT ──────────────────────────────────────────────────────────────

def test_group_concat_no_separator():
    """GROUP_CONCAT 不带分隔符"""
    result = sql({
        "scan": "order_items",
        "group": ["order_items.order_id"],
        "agg": [{"fn": "group_concat", "col": "order_items.product_id", "as": "product_ids"}],
        "select": ["order_items.order_id", "product_ids"],
    })
    assert "GROUP_CONCAT(order_items.product_id) AS product_ids" in result
    assert "GROUP BY order_items.order_id" in result


def test_group_concat_with_separator():
    """GROUP_CONCAT 带自定义分隔符"""
    result = sql({
        "scan": "products",
        "group": ["products.category"],
        "agg": [{"fn": "group_concat", "col": "products.name", "separator": " | ", "as": "names"}],
        "select": ["products.category", "names"],
    })
    assert "GROUP_CONCAT(products.name, ' | ') AS names" in result


# ── 递归 CTE (WITH RECURSIVE) ─────────────────────────────────────────────────

def test_recursive_cte_basic():
    """WITH RECURSIVE：员工层级查询"""
    result = sql({
        "cte": [{
            "name": "org_tree",
            "recursive": True,
            "query": {
                "scan": "users",
                "filter": [{"col": "users.id", "op": "eq", "val": 1}],
                "select": ["users.id", "users.name", {"expr": "0", "as": "depth"}],
            },
            "recursive_term": {
                "scan": "users",
                "joins": [{"type": "inner", "table": "org_tree",
                           "on": {"left": "users.id", "right": "org_tree.id"}}],
                "select": ["users.id", "users.name", {"expr": "org_tree.depth + 1", "as": "depth"}],
            },
            "recursive_union": "union_all",
        }],
        "scan": "org_tree",
        "select": ["org_tree.id", "org_tree.name", "org_tree.depth"],
        "sort": [{"col": "org_tree.depth", "dir": "asc"}],
    })
    assert result.startswith("WITH RECURSIVE")
    assert "org_tree AS (" in result
    assert "UNION ALL" in result
    assert "org_tree.depth + 1" in result


def test_recursive_cte_union_distinct():
    """WITH RECURSIVE：使用 UNION（去重）的递归 CTE"""
    from forge.compiler import compile_query
    result = compile_query({
        "cte": [{
            "name": "path",
            "recursive": True,
            "query": {
                "scan": "users",
                "filter": [{"col": "users.id", "op": "eq", "val": 1}],
                "select": ["users.id"],
            },
            "recursive_term": {
                "scan": "users",
                "joins": [{"type": "inner", "table": "path",
                           "on": {"left": "users.id", "right": "path.id"}}],
                "select": ["users.id"],
            },
            "recursive_union": "union",
        }],
        "scan": "path",
        "select": ["path.id"],
    })
    assert result.startswith("WITH RECURSIVE")
    assert "UNION\n" in result  # UNION (not UNION ALL)
    assert "UNION ALL" not in result


def test_non_recursive_cte_still_uses_with():
    """普通 CTE 不加 RECURSIVE 前缀"""
    result = sql({
        "cte": [{"name": "top_users", "query": {
            "scan": "users",
            "select": ["users.id"],
            "limit": 10,
        }}],
        "scan": "top_users",
        "select": ["top_users.id"],
    })
    assert result.startswith("WITH ")
    assert "RECURSIVE" not in result
