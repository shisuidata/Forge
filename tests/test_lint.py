from __future__ import annotations

from forge.lint import lint_conventions


def test_lint_category_question_requires_readable_name():
    forge_json = {
        "scan": "dim_category",
        "select": ["dim_category.category_id"],
    }

    warnings = lint_conventions(forge_json, "统计各品类销售额")

    assert any("category_name" in warning for warning in warnings)


def test_lint_category_group_by_name_requires_id():
    forge_json = {
        "scan": "dim_category",
        "select": ["dim_category.category_name"],
        "group": ["dim_category.category_name"],
        "agg": [{"fn": "count_all", "as": "cnt"}],
    }

    warnings = lint_conventions(forge_json, "统计各品类数量")

    assert any("category_id" in warning and "分组" in warning for warning in warnings)


def test_lint_category_window_partition_name_does_not_require_id():
    forge_json = {
        "scan": "product_sales",
        "select": ["category_name", "product_name", "rn"],
        "window": [
            {
                "fn": "row_number",
                "partition": ["category_name"],
                "order": [{"col": "total_qty", "dir": "desc"}],
                "as": "rn",
            }
        ],
        "qualify": [{"col": "rn", "op": "lte", "val": 3}],
    }

    warnings = lint_conventions(forge_json, "各品类内按销量排名前3的商品")

    assert not any("category_id" in warning and "分组" in warning for warning in warnings)


def test_lint_category_id_is_allowed_when_user_explicitly_asks_for_id():
    forge_json = {
        "scan": "dim_category",
        "select": ["dim_category.category_id"],
    }

    warnings = lint_conventions(forge_json, "统计各品类 ID 的数量")

    assert not any("category_name" in warning for warning in warnings)


def test_lint_lag_lead_time_order_must_be_ascending():
    forge_json = {
        "scan": "dwd_order_detail",
        "select": ["dwd_order_detail.user_id", "previous_order_dt"],
        "window": [
            {
                "fn": "lag",
                "col": "dwd_order_detail.order_dt",
                "partition": ["dwd_order_detail.user_id"],
                "order": [{"col": "dwd_order_detail.order_dt", "dir": "desc"}],
                "as": "previous_order_dt",
            }
        ],
    }

    warnings = lint_conventions(forge_json, "每个用户相邻两次下单时间间隔")

    assert any("ASC" in warning and "DESC" in warning for warning in warnings)


def test_lint_lag_lead_temporal_question_requires_order():
    forge_json = {
        "scan": "dwd_order_detail",
        "select": ["dwd_order_detail.user_id", "previous_order_dt"],
        "window": [
            {
                "fn": "lag",
                "col": "dwd_order_detail.order_dt",
                "partition": ["dwd_order_detail.user_id"],
                "as": "previous_order_dt",
            }
        ],
    }

    warnings = lint_conventions(forge_json, "每个用户上一笔订单时间")

    assert any("必须声明时间排序" in warning for warning in warnings)


def test_lint_refund_rate_requires_refund_detail_join():
    forge_json = {
        "scan": "dwd_order_detail",
        "select": ["refund_rate"],
        "agg": [
            {"fn": "count_distinct", "col": "dwd_order_detail.refund_id", "as": "refund_orders"},
            {"fn": "count_distinct", "col": "dwd_order_detail.order_id", "as": "total_orders"},
        ],
    }

    warnings = lint_conventions(forge_json, "统计各年龄段退款率")

    assert any("dwd_refund_detail" in warning for warning in warnings)


def test_lint_refund_rate_accepts_refund_detail_reference():
    forge_json = {
        "scan": "dwd_order_detail",
        "joins": [
            {
                "type": "left",
                "table": "dwd_refund_detail",
                "on": [
                    {
                        "left": "dwd_refund_detail.order_id",
                        "op": "eq",
                        "right": "dwd_order_detail.order_id",
                    }
                ],
            }
        ],
        "select": ["refund_rate"],
        "agg": [
            {"fn": "count_distinct", "col": "dwd_refund_detail.order_id", "as": "refund_orders"},
            {"fn": "count_distinct", "col": "dwd_order_detail.order_id", "as": "total_orders"},
        ],
    }

    warnings = lint_conventions(forge_json, "统计退款率")

    assert not any("退款率应使用 dwd_refund_detail" in warning for warning in warnings)


def test_lint_per_group_topn_requires_qualify_when_rank_exists():
    forge_json = {
        "scan": "product_sales",
        "select": ["category_name", "product_name", "total_qty", "rn"],
        "window": [
            {
                "fn": "row_number",
                "partition": ["category_name"],
                "order": [{"col": "total_qty", "dir": "desc"}],
                "as": "rn",
            }
        ],
    }

    warnings = lint_conventions(forge_json, "各品类内按销售数量排名前3的商品")

    assert any("缺少 qualify" in warning for warning in warnings)


def test_lint_per_group_topn_rejects_global_limit_only():
    forge_json = {
        "scan": "product_sales",
        "select": ["category_name", "product_name", "total_qty"],
        "sort": [{"col": "total_qty", "dir": "desc"}],
        "limit": 3,
    }

    warnings = lint_conventions(forge_json, "各品类内按销售数量排名前3的商品")

    assert any("不能只用全局 sort/limit" in warning for warning in warnings)
