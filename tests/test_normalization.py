from __future__ import annotations

from forge.normalization import complete_unambiguous_ratio_alias


def test_completes_one_unambiguous_window_ratio_alias():
    query = {
        "scan": "product_sales",
        "window": [
            {
                "fn": "sum",
                "col": "product_sales.product_revenue",
                "partition": ["product_sales.category_name"],
                "as": "category_total",
            }
        ],
        "select": [
            "product_sales.category_name",
            "product_sales.product_name",
            "product_sales.product_revenue",
            "pct_of_category",
        ],
    }

    completed = complete_unambiguous_ratio_alias(query, "商品销售额在所属品类中的占比")

    assert completed["select"][-1] == {
        "expr": "ROUND(product_sales.product_revenue * 1.0 / category_total, 4)",
        "as": "pct_of_category",
    }
    assert query["select"][-1] == "pct_of_category"


def test_does_not_guess_when_ratio_inputs_are_ambiguous():
    query = {
        "scan": "stats",
        "window": [
            {"fn": "sum", "col": "stats.a", "as": "total_a"},
            {"fn": "sum", "col": "stats.b", "as": "total_b"},
        ],
        "select": ["stats.a", "stats.b", "pct"],
    }

    assert complete_unambiguous_ratio_alias(query, "计算占比") == query


def test_does_not_complete_without_explicit_ratio_intent():
    query = {
        "scan": "stats",
        "window": [{"fn": "sum", "col": "stats.amount", "as": "total"}],
        "select": ["stats.amount", "pct"],
    }

    assert complete_unambiguous_ratio_alias(query, "查询金额") == query
