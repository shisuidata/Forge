from __future__ import annotations

from forge.lint import lint_conventions


def test_lint_category_question_requires_readable_name():
    forge_json = {
        "scan": "dim_category",
        "select": ["dim_category.category_id"],
    }

    warnings = lint_conventions(forge_json, "统计各品类销售额")

    assert any("category_name" in warning for warning in warnings)


def test_lint_category_group_by_name_does_not_require_id():
    forge_json = {
        "scan": "dim_category",
        "select": ["dim_category.category_name"],
        "group": ["dim_category.category_name"],
        "agg": [{"fn": "count_all", "as": "cnt"}],
    }

    warnings = lint_conventions(forge_json, "统计各品类数量")

    assert not any("category_id" in warning and "分组" in warning for warning in warnings)


def test_lint_category_display_grain_rejects_id_partition_when_name_requested():
    warnings = lint_conventions(
        {
            "scan": "product_sales",
            "select": ["category_name", "product_name", "rn"],
            "window": [
                {
                    "fn": "row_number",
                    "partition": ["category_id", "category_name"],
                    "order": [{"col": "total_qty", "dir": "desc"}],
                    "as": "rn",
                }
            ],
            "qualify": [{"col": "rn", "op": "lte", "val": 3}],
        },
        "各品类内按销量排名前3的商品，显示品类名、商品名和排名",
    )

    assert any("PARTITION BY" in warning and "category_id" in warning for warning in warnings)


def test_lint_all_bad_reviews_without_images_requires_anti_semantics():
    warnings = lint_conventions(
        {
            "scan": "dwd_comment_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dim_product",
                    "on": {
                        "left": "dwd_comment_detail.product_id",
                        "right": "dim_product.product_id",
                    },
                }
            ],
            "filter": [{"col": "dwd_comment_detail.comment_type", "op": "eq", "val": "差评"}],
            "group": ["dim_product.product_id", "dim_product.product_name"],
            "agg": [
                {
                    "fn": "count",
                    "col": "dwd_comment_detail.comment_id",
                    "filter": [{"col": "dwd_comment_detail.has_image", "op": "eq", "val": 1}],
                    "as": "has_image_count",
                }
            ],
            "having": [{"col": "has_image_count", "op": "eq", "val": 0}],
            "select": ["dim_product.product_name"],
        },
        "找出有差评记录但所有差评均无图片的商品，显示商品名称",
    )

    assert any("反存在语义" in warning for warning in warnings)


def test_lint_all_bad_reviews_without_images_rejects_comment_self_join():
    warnings = lint_conventions(
        {
            "scan": "dwd_comment_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dim_product",
                    "on": {
                        "left": "dwd_comment_detail.product_id",
                        "right": "dim_product.product_id",
                    },
                },
                {
                    "type": "left",
                    "table": "dwd_comment_detail",
                    "on": {
                        "left": "dim_product.product_id",
                        "right": "dwd_comment_detail.product_id",
                    },
                },
            ],
            "filter": [{"col": "dwd_comment_detail.comment_type", "op": "eq", "val": "差评"}],
            "select": ["dim_product.product_name"],
        },
        "找出有差评记录但所有差评均无图片的商品，显示商品名称",
    )

    assert any("自连接" in warning and "歧义列" in warning for warning in warnings)


def test_lint_all_bad_reviews_without_images_requires_stable_anti_join_key():
    warnings = lint_conventions(
        {
            "cte": [
                {
                    "name": "bad_products",
                    "query": {
                        "scan": "dwd_comment_detail",
                        "joins": [
                            {
                                "type": "inner",
                                "table": "dim_product",
                                "on": {
                                    "left": "dwd_comment_detail.product_id",
                                    "right": "dim_product.product_id",
                                },
                            }
                        ],
                        "filter": [
                            {"col": "dwd_comment_detail.comment_type", "op": "eq", "val": "差评"}
                        ],
                        "group": ["dim_product.product_name"],
                        "select": ["dim_product.product_name"],
                    },
                },
                {
                    "name": "bad_with_image",
                    "query": {
                        "scan": "dwd_comment_detail",
                        "joins": [
                            {
                                "type": "inner",
                                "table": "dim_product",
                                "on": {
                                    "left": "dwd_comment_detail.product_id",
                                    "right": "dim_product.product_id",
                                },
                            }
                        ],
                        "filter": [
                            {"col": "dwd_comment_detail.comment_type", "op": "eq", "val": "差评"},
                            {"col": "dwd_comment_detail.has_image", "op": "eq", "val": 1},
                        ],
                        "group": ["dim_product.product_name"],
                        "select": ["dim_product.product_name"],
                    },
                },
            ],
            "scan": "bad_products",
            "joins": [
                {
                    "type": "anti",
                    "table": "bad_with_image",
                    "on": {
                        "left": "bad_products.product_name",
                        "right": "bad_with_image.product_name",
                    },
                }
            ],
            "distinct": True,
            "select": ["bad_products.product_name"],
        },
        "找出有差评记录但所有差评均无图片的商品，显示商品名称",
    )

    assert any("product_id" in warning and "反连接" in warning for warning in warnings)


def test_lint_all_bad_reviews_without_images_requires_distinct_product_name():
    warnings = lint_conventions(
        {
            "cte": [
                {
                    "name": "bad_review_products",
                    "query": {
                        "scan": "dwd_comment_detail",
                        "filter": [{"col": "comment_type", "op": "eq", "val": "差评"}],
                        "group": ["product_id"],
                        "select": ["product_id"],
                    },
                },
                {
                    "name": "bad_with_image_products",
                    "query": {
                        "scan": "dwd_comment_detail",
                        "filter": [
                            {"col": "comment_type", "op": "eq", "val": "差评"},
                            {"col": "has_image", "op": "eq", "val": 1},
                        ],
                        "group": ["product_id"],
                        "select": ["product_id"],
                    },
                },
            ],
            "scan": "bad_review_products",
            "joins": [
                {
                    "type": "anti",
                    "table": "bad_with_image_products",
                    "on": {
                        "left": "bad_review_products.product_id",
                        "right": "bad_with_image_products.product_id",
                    },
                },
                {
                    "type": "inner",
                    "table": "dim_product",
                    "on": {
                        "left": "bad_review_products.product_id",
                        "right": "dim_product.product_id",
                    },
                },
            ],
            "select": ["dim_product.product_name"],
        },
        "找出有差评记录但所有差评均无图片的商品，显示商品名称",
    )

    assert any("去重商品名称" in warning or "distinct: true" in warning for warning in warnings)


def test_lint_all_bad_reviews_without_images_accepts_distinct_product_name():
    warnings = lint_conventions(
        {
            "cte": [
                {
                    "name": "bad_review_products",
                    "query": {
                        "scan": "dwd_comment_detail",
                        "filter": [{"col": "comment_type", "op": "eq", "val": "差评"}],
                        "group": ["product_id"],
                        "select": ["product_id"],
                    },
                }
            ],
            "scan": "bad_review_products",
            "joins": [
                {
                    "type": "inner",
                    "table": "dim_product",
                    "on": {
                        "left": "bad_review_products.product_id",
                        "right": "dim_product.product_id",
                    },
                }
            ],
            "distinct": True,
            "select": ["dim_product.product_name"],
        },
        "找出有差评记录但所有差评均无图片的商品，显示商品名称",
    )

    assert not any("去重商品名称" in warning or "同名商品" in warning for warning in warnings)


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


def test_lint_category_refund_rate_rejects_order_id_product_id_join():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_detail",
            "joins": [
                {
                    "type": "left",
                    "table": "dwd_refund_detail",
                    "on": {
                        "left": "dwd_order_detail.order_id",
                        "right": "dwd_refund_detail.order_id",
                    },
                },
                {
                    "type": "inner",
                    "table": "dim_product",
                    "on": {
                        "left": "dwd_order_detail.order_id",
                        "right": "dim_product.product_id",
                    },
                },
                {
                    "type": "inner",
                    "table": "dim_category",
                    "on": {
                        "left": "dim_product.category_id",
                        "right": "dim_category.category_id",
                    },
                },
            ],
            "group": ["dim_category.category_name"],
            "agg": [
                {"fn": "count_distinct", "col": "dwd_order_detail.order_id", "as": "total_orders"},
                {"fn": "count_distinct", "col": "dwd_refund_detail.order_id", "as": "refund_orders"},
            ],
            "select": ["dim_category.category_name", "total_orders", "refund_orders"],
        },
        "统计各品类的退款率（退款订单数/总订单数），找出退款率超过15%的品类",
    )

    assert any("dwd_order_detail.order_id" in warning and "dim_product.product_id" in warning for warning in warnings)


def test_lint_refund_rate_requires_visible_numerator_denominator_when_question_defines_ratio():
    warnings = lint_conventions(
        {
            "scan": "age_stats",
            "select": [
                "age_group",
                {"expr": "refund_orders * 1.0 / total_orders", "as": "refund_rate"},
            ],
        },
        "各年龄段用户的退款率（退款订单数/总订单数），并按退款率降序排列",
    )

    assert any("total_orders" in warning and "refund_orders" in warning for warning in warnings)


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


def test_lint_order_count_requires_distinct_after_joining_items():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dwd_order_item_detail",
                    "on": {
                        "left": "dwd_order_detail.order_id",
                        "right": "dwd_order_item_detail.order_id",
                    },
                }
            ],
            "group": ["dwd_order_detail.order_status"],
            "agg": [
                {"fn": "count_all", "as": "order_count"},
            ],
            "select": ["dwd_order_detail.order_status", "order_count"],
        },
        "统计各状态的订单数",
    )

    assert any("count_distinct" in warning and "dwd_order_detail.order_id" in warning for warning in warnings)


def test_lint_order_count_accepts_distinct_order_id_after_joining_items():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dwd_order_item_detail",
                    "on": {
                        "left": "dwd_order_detail.order_id",
                        "right": "dwd_order_item_detail.order_id",
                    },
                }
            ],
            "group": ["dwd_order_detail.order_status"],
            "agg": [
                {"fn": "count_distinct", "col": "dwd_order_detail.order_id", "as": "order_count"},
            ],
            "select": ["dwd_order_detail.order_status", "order_count"],
        },
        "统计各状态的订单数",
    )

    assert not any("订单明细表" in warning for warning in warnings)


def test_lint_consumption_ranking_requires_completed_orders():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dwd_order_item_detail",
                    "on": {
                        "left": "dwd_order_detail.order_id",
                        "right": "dwd_order_item_detail.order_id",
                    },
                }
            ],
            "group": ["dwd_order_detail.user_id"],
            "agg": [{"fn": "sum", "col": "dwd_order_item_detail.actual_amount", "as": "total_spent"}],
            "select": ["dwd_order_detail.user_id", "total_spent"],
        },
        "统计各用户消费排名",
    )

    assert any("order_status" in warning and "已完成" in warning for warning in warnings)


def test_lint_member_category_spend_rejects_vip_level_id_grouping():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_item_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dim_user",
                    "on": {
                        "left": "dwd_order_item_detail.user_id",
                        "right": "dim_user.user_id",
                    },
                },
                {
                    "type": "inner",
                    "table": "dim_vip_level",
                    "on": {
                        "left": "dim_user.vip_level_id",
                        "right": "dim_vip_level.vip_level_id",
                    },
                },
                {
                    "type": "inner",
                    "table": "dim_product",
                    "on": {
                        "left": "dwd_order_item_detail.product_id",
                        "right": "dim_product.product_id",
                    },
                },
                {
                    "type": "inner",
                    "table": "dim_category",
                    "on": {
                        "left": "dim_product.category_id",
                        "right": "dim_category.category_id",
                    },
                },
            ],
            "filter": [{"col": "dim_vip_level.level_name", "op": "in", "val": ["钻石", "铂金"]}],
            "group": [
                "dim_vip_level.vip_level_id",
                "dim_vip_level.level_name",
                "dim_category.category_id",
                "dim_category.category_name",
            ],
            "agg": [{"fn": "sum", "col": "dwd_order_item_detail.actual_amount", "as": "total_spent"}],
            "select": ["dim_vip_level.level_name", "dim_category.category_name", "total_spent"],
        },
        "钻石和铂金会员用户在各品类的消费总额，按会员等级和品类分组",
    )

    assert any("vip_level_id" in warning and "行数和金额膨胀" in warning for warning in warnings)


def test_lint_member_category_spend_rejects_order_header_user_grain():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dim_user",
                    "on": {
                        "left": "dwd_order_detail.user_id",
                        "right": "dim_user.user_id",
                    },
                },
                {
                    "type": "inner",
                    "table": "dwd_order_item_detail",
                    "on": {
                        "left": "dwd_order_detail.order_id",
                        "right": "dwd_order_item_detail.order_id",
                    },
                },
                {
                    "type": "inner",
                    "table": "dim_product",
                    "on": {
                        "left": "dwd_order_item_detail.product_id",
                        "right": "dim_product.product_id",
                    },
                },
                {
                    "type": "inner",
                    "table": "dim_category",
                    "on": {
                        "left": "dim_product.category_id",
                        "right": "dim_category.category_id",
                    },
                },
            ],
            "group": ["dim_category.category_id", "dim_category.category_name"],
            "agg": [{"fn": "sum", "col": "dwd_order_item_detail.actual_amount", "as": "total_spent"}],
            "select": ["dim_category.category_name", "total_spent"],
        },
        "钻石和铂金会员用户在各品类的消费总额，按会员等级和品类分组",
    )

    assert any("dwd_order_item_detail.user_id" in warning for warning in warnings)


def test_lint_member_category_spend_rejects_order_header_amount():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dwd_order_item_detail",
                    "on": {
                        "left": "dwd_order_detail.order_id",
                        "right": "dwd_order_item_detail.order_id",
                    },
                }
            ],
            "agg": [{"fn": "sum", "col": "dwd_order_detail.total_amount", "as": "total_consumption"}],
            "select": ["total_consumption"],
        },
        "钻石和铂金会员用户在各品类的消费总额，按会员等级和品类分组",
    )

    assert any("actual_amount" in warning and "重复计算" in warning for warning in warnings)


def test_lint_brand_diamond_avg_item_requires_audit_fields():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_item_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dwd_order_detail",
                    "on": {
                        "left": "dwd_order_item_detail.order_id",
                        "right": "dwd_order_detail.order_id",
                    },
                },
                {
                    "type": "inner",
                    "table": "dim_user",
                    "on": {
                        "left": "dwd_order_detail.user_id",
                        "right": "dim_user.user_id",
                    },
                },
                {
                    "type": "inner",
                    "table": "dim_vip_level",
                    "on": {
                        "left": "dim_user.vip_level_id",
                        "right": "dim_vip_level.vip_level_id",
                    },
                },
                {
                    "type": "inner",
                    "table": "dim_product",
                    "on": {
                        "left": "dwd_order_item_detail.product_id",
                        "right": "dim_product.product_id",
                    },
                },
                {
                    "type": "inner",
                    "table": "dim_brand",
                    "on": {
                        "left": "dim_product.brand_id",
                        "right": "dim_brand.brand_id",
                    },
                },
            ],
            "filter": [
                {"col": "dwd_order_detail.order_status", "op": "eq", "val": "已完成"},
                {"col": "dim_vip_level.level_name", "op": "eq", "val": "钻石"},
            ],
            "group": ["dim_brand.brand_id", "dim_brand.brand_name"],
            "agg": [{"fn": "avg", "col": "dwd_order_item_detail.actual_amount", "as": "avg_item_price"}],
            "select": ["dim_brand.brand_name", "avg_item_price"],
        },
        "统计各品牌钻石会员用户在已完成订单中的平均商品实付单价（AVG item actual_amount），按均价降序排列",
    )

    assert any("level_name" in warning for warning in warnings)
    assert any("order_count" in warning for warning in warnings)


def test_lint_brand_diamond_avg_item_accepts_audit_fields():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_item_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dwd_order_detail",
                    "on": {
                        "left": "dwd_order_item_detail.order_id",
                        "right": "dwd_order_detail.order_id",
                    },
                }
            ],
            "filter": [
                {"col": "dwd_order_detail.order_status", "op": "eq", "val": "已完成"},
                {"col": "dim_vip_level.level_name", "op": "eq", "val": "钻石"},
            ],
            "group": ["dim_brand.brand_id", "dim_brand.brand_name", "dim_vip_level.level_name"],
            "agg": [
                {"fn": "avg", "col": "dwd_order_item_detail.actual_amount", "as": "avg_item_price"},
                {"fn": "count_distinct", "col": "dwd_order_detail.order_id", "as": "order_count"},
            ],
            "select": [
                "dim_brand.brand_name",
                "dim_vip_level.level_name",
                "avg_item_price",
                "order_count",
            ],
        },
        "统计各品牌钻石会员用户在已完成订单中的平均商品实付单价（AVG item actual_amount），按均价降序排列",
    )

    assert not any("order_count" in warning or "level_name" in warning for warning in warnings)


def test_lint_generic_order_count_rejects_default_completed_filter():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dim_channel",
                    "on": {
                        "left": "dwd_order_detail.channel_id",
                        "right": "dim_channel.channel_id",
                    },
                }
            ],
            "filter": [{"col": "dwd_order_detail.order_status", "op": "eq", "val": "已完成"}],
            "group": [
                {"expr": "STRFTIME('%Y-%m', dwd_order_detail.order_dt)", "as": "month"},
                "dim_channel.channel_name",
            ],
            "agg": [{"fn": "count_distinct", "col": "dwd_order_detail.order_id", "as": "order_count"}],
            "select": ["month", "dim_channel.channel_name", "order_count"],
        },
        "每个渠道按月统计订单数，以及该渠道当月订单数在所有渠道当月总订单数中的占比",
    )

    assert any("没有指定" in warning and "order_status" in warning for warning in warnings)


def test_lint_explicit_completed_order_count_allows_status_filter():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_detail",
            "filter": [{"col": "dwd_order_detail.order_status", "op": "eq", "val": "已完成"}],
            "group": ["dwd_order_detail.channel_id"],
            "agg": [{"fn": "count_distinct", "col": "dwd_order_detail.order_id", "as": "order_count"}],
            "select": ["dwd_order_detail.channel_id", "order_count"],
        },
        "统计各渠道已完成订单数",
    )

    assert not any("不要默认添加" in warning for warning in warnings)


def test_lint_detail_vs_group_average_rejects_plain_group_by_avg():
    warnings = lint_conventions(
        {
            "scan": "dwd_comment_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dim_product",
                    "on": {
                        "left": "dwd_comment_detail.product_id",
                        "right": "dim_product.product_id",
                    },
                },
                {
                    "type": "inner",
                    "table": "dim_brand",
                    "on": {
                        "left": "dim_product.brand_id",
                        "right": "dim_brand.brand_id",
                    },
                },
            ],
            "group": ["dim_brand.brand_id", "dim_brand.brand_name"],
            "agg": [{"fn": "avg", "col": "dwd_comment_detail.rating", "as": "brand_avg_rating"}],
            "select": [
                "dim_brand.brand_name",
                "brand_avg_rating",
                "dwd_comment_detail.rating",
                {"expr": "dwd_comment_detail.rating - brand_avg_rating", "as": "rating_deviation"},
            ],
        },
        "每个品牌商品评分的平均分，以及每条评价相对品牌平均分的偏差",
    )

    assert any("保留每条明细行" in warning for warning in warnings)


def test_lint_detail_vs_group_average_accepts_window_avg():
    warnings = lint_conventions(
        {
            "scan": "dwd_comment_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dim_product",
                    "on": {
                        "left": "dwd_comment_detail.product_id",
                        "right": "dim_product.product_id",
                    },
                }
            ],
            "window": [
                {
                    "fn": "avg",
                    "col": "dwd_comment_detail.rating",
                    "partition": ["dim_product.brand_id"],
                    "as": "brand_avg_rating",
                }
            ],
            "select": [
                "dwd_comment_detail.comment_id",
                "dwd_comment_detail.rating",
                "brand_avg_rating",
                {"expr": "dwd_comment_detail.rating - brand_avg_rating", "as": "rating_deviation"},
            ],
        },
        "每个品牌商品评分的平均分，以及每条评价相对品牌平均分的偏差",
    )

    assert not any("保留每条明细行" in warning for warning in warnings)


def test_lint_brand_rating_deviation_rejects_unstable_output_contract():
    warnings = lint_conventions(
        {
            "scan": "dwd_comment_detail",
            "window": [{
                "fn": "avg",
                "col": "dwd_comment_detail.rating",
                "partition": ["dim_brand.brand_id"],
                "as": "brand_avg_rating",
            }],
            "select": [
                "dwd_comment_detail.comment_id",
                "dim_brand.brand_name",
                "dwd_comment_detail.rating",
                {"expr": "ROUND(brand_avg_rating, 4)", "as": "brand_avg_rating"},
                {"expr": "ROUND(rating - brand_avg_rating, 4)", "as": "deviation"},
            ],
            "sort": [{"col": "dim_brand.brand_name", "dir": "asc"}],
        },
        "每个品牌商品评分的平均分，以及每条评价相对品牌平均分的偏差",
    )

    assert any("brand_name、rating、brand_avg_rating、rating_deviation" in warning for warning in warnings)


def test_lint_order_detail_query_requires_item_id_and_item_date():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dwd_order_item_detail",
                    "on": {
                        "left": "dwd_order_detail.order_id",
                        "right": "dwd_order_item_detail.order_id",
                    },
                }
            ],
            "filter": [
                {"col": "dwd_order_detail.order_dt", "op": "gte", "val": {"$date": "2025-12-01"}},
            ],
            "select": ["dwd_order_detail.order_id", "dwd_order_detail.order_dt"],
            "sort": [{"col": "dwd_order_detail.order_dt", "dir": "desc"}],
        },
        "2025年12月内进口商品的订单明细，按下单时间降序",
    )

    assert any("order_item_id" in warning for warning in warnings)
    assert any("dwd_order_item_detail.order_dt" in warning for warning in warnings)


def test_lint_imported_brand_order_detail_rejects_unstable_output_contract():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_item_detail",
            "select": [
                "dwd_order_item_detail.order_item_id",
                "dwd_order_item_detail.order_id",
                "dwd_order_item_detail.product_id",
                "dwd_order_item_detail.quantity",
                "dwd_order_item_detail.actual_amount",
                "dwd_order_item_detail.order_dt",
            ],
        },
        "2025年12月内，国际品牌或国内知名品牌的进口商品的订单明细，按下单时间降序",
    )

    assert any("order_item_id、order_id、product_name、brand_name、actual_amount、order_dt" in warning for warning in warnings)


def test_lint_refund_record_rejects_unstable_output_contract():
    warnings = lint_conventions(
        {
            "scan": "dwd_refund_detail",
            "select": [
                "dwd_refund_detail.refund_id",
                "dwd_refund_detail.order_id",
                "dwd_refund_detail.user_id",
                "dwd_refund_detail.refund_amount",
                "dwd_refund_detail.refund_status",
                "dwd_refund_detail.complete_dt",
            ],
        },
        "找出退款状态为已退款且退款金额超过500元的退款记录，按退款金额降序",
    )

    assert any("refund_id、order_id、user_id、refund_amount、apply_dt" in warning for warning in warnings)


def test_lint_order_lookup_requires_order_id():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dim_user",
                    "on": {
                        "left": "dwd_order_detail.user_id",
                        "right": "dim_user.user_id",
                    },
                }
            ],
            "filter": [{"col": "dwd_order_detail.order_status", "op": "eq", "val": "已完成"}],
            "select": ["dim_user.user_name", "dwd_order_detail.total_amount"],
        },
        "找出客单价在500到2000之间的已完成订单",
    )

    assert any("order_id" in warning for warning in warnings)


def test_lint_order_lookup_does_not_trigger_for_category_metric_query():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dwd_order_item_detail",
                    "on": {
                        "left": "dwd_order_detail.order_id",
                        "right": "dwd_order_item_detail.order_id",
                    },
                },
                {
                    "type": "inner",
                    "table": "dim_product",
                    "on": {
                        "left": "dwd_order_item_detail.sku_id",
                        "right": "dim_product.sku_id",
                    },
                },
            ],
            "filter": [{"col": "dwd_order_detail.order_status", "op": "eq", "val": "已退款"}],
            "group_by": ["dim_product.category_name"],
            "select": ["dim_product.category_name", {"expr": "count", "as": "refund_order_count"}],
        },
        "统计各品类的退款率，找出退款率超过15%的品类",
    )

    assert not any("订单主键" in warning for warning in warnings)


def test_lint_cart_without_completed_order_requires_order_status_join():
    warnings = lint_conventions(
        {
            "scan": "dim_user",
            "joins": [
                {
                    "type": "inner",
                    "table": "dwd_cart_detail",
                    "on": {"left": "dim_user.user_id", "right": "dwd_cart_detail.user_id"},
                    "filter": [{"col": "dwd_cart_detail.action_type", "op": "eq", "val": "add"}],
                },
                {
                    "type": "anti",
                    "table": "dwd_order_item_detail",
                    "on": {
                        "left": "dwd_cart_detail.product_id",
                        "right": "dwd_order_item_detail.product_id",
                    },
                    "filter": [
                        {"col": "dwd_order_item_detail.user_id", "op": "eq", "col2": "dwd_cart_detail.user_id"},
                    ],
                },
            ],
            "select": ["dim_user.user_id", "dim_user.user_name"],
        },
        "找出曾将某商品加入购物车但该商品从未出现在其已完成订单中的用户",
    )

    assert any("order_status" in warning and "已完成" in warning for warning in warnings)
    assert not any("订单主键" in warning for warning in warnings)


def test_lint_cart_without_completed_order_requires_same_product_antijoin():
    warnings = lint_conventions(
        {
            "scan": "dim_user",
            "joins": [
                {
                    "type": "inner",
                    "table": "dwd_cart_detail",
                    "on": {"left": "dim_user.user_id", "right": "dwd_cart_detail.user_id"},
                    "filter": [{"col": "dwd_cart_detail.action_type", "op": "eq", "val": "add"}],
                },
                {
                    "type": "anti",
                    "table": "dwd_order_detail",
                    "on": {"left": "dim_user.user_id", "right": "dwd_order_detail.user_id"},
                    "filter": [{"col": "dwd_order_detail.order_status", "op": "eq", "val": "已完成"}],
                },
            ],
            "select": ["dim_user.user_id", "dim_user.user_name"],
        },
        "找出曾将某商品加入购物车但该商品从未出现在其已完成订单中的用户",
    )

    assert any("同一商品" in warning and "dwd_order_item_detail" in warning for warning in warnings)


def test_lint_category_monthly_orders_require_item_date_and_item_order_id():
    warnings = lint_conventions(
        {
            "cte": [
                {
                    "name": "monthly_category",
                    "query": {
                        "scan": "dwd_order_detail",
                        "joins": [
                            {
                                "type": "inner",
                                "table": "dwd_order_item_detail",
                                "on": {
                                    "left": "dwd_order_detail.order_id",
                                    "right": "dwd_order_item_detail.order_id",
                                },
                            },
                            {
                                "type": "inner",
                                "table": "dim_product",
                                "on": {
                                    "left": "dwd_order_item_detail.product_id",
                                    "right": "dim_product.product_id",
                                },
                            },
                        ],
                        "group": [
                            "dim_product.category_id",
                            {"expr": "STRFTIME('%Y-%m', dwd_order_detail.order_dt)", "as": "month"},
                        ],
                        "agg": [
                            {"fn": "count_distinct", "col": "dwd_order_detail.order_id", "as": "order_count"},
                        ],
                        "select": ["dim_product.category_id", "month", "order_count"],
                    },
                }
            ],
            "scan": "monthly_category",
            "select": ["monthly_category.category_id", "monthly_category.month", "monthly_category.order_count"],
        },
        "每个品类按月统计订单量，显示品类名、月份、当月订单量及下一个月的订单量",
    )

    assert any("dwd_order_item_detail.order_dt" in warning for warning in warnings)
    assert any("dwd_order_item_detail.order_id" in warning for warning in warnings)


def test_lint_category_monthly_lead_rejects_category_id_grain():
    warnings = lint_conventions(
        {
            "cte": [
                {
                    "name": "category_monthly_orders",
                    "query": {
                        "scan": "dwd_order_item_detail",
                        "joins": [
                            {
                                "type": "inner",
                                "table": "dim_product",
                                "on": {
                                    "left": "dwd_order_item_detail.product_id",
                                    "right": "dim_product.product_id",
                                },
                            },
                            {
                                "type": "inner",
                                "table": "dim_category",
                                "on": {
                                    "left": "dim_product.category_id",
                                    "right": "dim_category.category_id",
                                },
                            },
                        ],
                        "group": [
                            "dim_category.category_id",
                            "dim_category.category_name",
                            {"expr": "STRFTIME('%Y-%m', dwd_order_item_detail.order_dt)", "as": "month"},
                        ],
                        "agg": [
                            {
                                "fn": "count_distinct",
                                "col": "dwd_order_item_detail.order_id",
                                "as": "order_count",
                            },
                        ],
                        "select": ["dim_category.category_name", "month", "order_count"],
                    },
                }
            ],
            "scan": "category_monthly_orders",
            "window": [
                {
                    "fn": "lead",
                    "col": "category_monthly_orders.order_count",
                    "partition": ["category_monthly_orders.category_name"],
                    "order": [{"col": "category_monthly_orders.month", "dir": "asc"}],
                    "as": "next_month_order_count",
                }
            ],
            "select": ["category_name", "month", "order_count", "next_month_order_count"],
        },
        "每个品类按月统计订单量，显示品类名、月份、当月订单量及下一个月的订单量",
    )

    assert any("category_name + month" in warning and "category_id" in warning for warning in warnings)


def test_lint_rejects_qualified_window_alias_select():
    warnings = lint_conventions(
        {
            "scan": "product_sales",
            "window": [
                {
                    "fn": "row_number",
                    "partition": ["product_sales.category_name"],
                    "order": [{"col": "product_sales.product_sales", "dir": "desc"}],
                    "as": "rn",
                }
            ],
            "select": [
                "product_sales.category_name",
                "product_sales.product_name",
                "product_sales.rn",
                "rn",
            ],
        },
        "各品类内，按销售额排名前3的商品及其销售额在品类中的占比",
    )

    assert any("product_sales.rn" in warning and "窗口函数别名" in warning for warning in warnings)


def test_lint_rejects_internal_rank_alias_when_rank_not_requested_in_output():
    warnings = lint_conventions(
        {
            "scan": "product_sales",
            "window": [
                {
                    "fn": "row_number",
                    "partition": ["product_sales.category_name"],
                    "order": [{"col": "product_sales.product_sales", "dir": "desc"}],
                    "as": "rn",
                }
            ],
            "qualify": [{"col": "rn", "op": "lte", "val": 3}],
            "select": [
                "product_sales.category_name",
                "product_sales.product_name",
                "product_sales.product_sales",
                "rn",
            ],
        },
        "各品类内，按销售额排名前3的商品及其销售额在品类中的占比",
    )

    assert any("内部字段" in warning and "rn" in warning for warning in warnings)


def test_lint_allows_rank_output_when_question_explicitly_requests_it():
    query = {
        "scan": "orders",
        "window": [
            {
                "fn": "rank",
                "partition": ["orders.user_id"],
                "order": [{"col": "orders.total_amount", "dir": "desc"}],
                "as": "amount_rank",
            }
        ],
        "select": ["orders.user_id", "orders.total_amount", "amount_rank"],
    }

    for question in (
        "每笔订单金额排名，显示用户ID、金额和排名",
        "各品类前3商品，显示品类名、商品名、销量及排名",
    ):
        warnings = lint_conventions(query, question)
        assert not any("内部字段" in warning for warning in warnings)


def test_lint_does_not_hide_unfiltered_rank_output():
    warnings = lint_conventions(
        {
            "scan": "orders",
            "window": [{"fn": "rank", "as": "amount_rank"}],
            "select": ["orders.id", "amount_rank"],
        },
        "每笔订单金额排名",
    )

    assert not any("内部字段" in warning for warning in warnings)


def test_lint_hides_unrequested_dimension_id_when_name_is_displayed():
    warnings = lint_conventions(
        {
            "scan": "dim_product",
            "select": ["dim_product.product_id", "dim_product.product_name"],
        },
        "找出销售额最高的前10个商品",
    )

    assert any("product_id" in warning and "内部 ID" in warning for warning in warnings)


def test_lint_monthly_orders_default_to_order_date():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_detail",
            "group": [{"expr": "STRFTIME('%Y-%m', dwd_order_detail.complete_dt)", "as": "month"}],
            "select": ["month"],
        },
        "每个渠道按月统计已完成订单量",
    )

    assert any("order_dt" in warning and "complete_dt" in warning for warning in warnings)


def test_lint_rate_threshold_uses_unrounded_ratio():
    warnings = lint_conventions(
        {
            "scan": "stats",
            "select": [
                {"expr": "ROUND(refunds * 1.0 / orders, 4)", "as": "refund_rate"}
            ],
        },
        "找出退款率超过15%的品类",
    )

    assert any("阈值" in warning and "ROUND" in warning for warning in warnings)


def test_lint_category_topn_share_rejects_denominator_grouped_by_category_id():
    warnings = lint_conventions(
        {
            "cte": [
                {
                    "name": "product_sales",
                    "query": {
                        "scan": "dwd_order_item_detail",
                        "group": ["dim_product.product_id", "dim_category.category_name"],
                        "agg": [{"fn": "sum", "col": "actual_amount", "as": "product_revenue"}],
                        "select": ["dim_product.product_id", "dim_category.category_name", "product_revenue"],
                    },
                },
                {
                    "name": "category_totals",
                    "query": {
                        "scan": "dwd_order_item_detail",
                        "group": ["dim_category.category_id", "dim_category.category_name"],
                        "agg": [{"fn": "sum", "col": "actual_amount", "as": "category_total_sales"}],
                        "select": ["dim_category.category_id", "dim_category.category_name", "category_total_sales"],
                    },
                },
            ],
            "scan": "product_sales",
            "joins": [{
                "type": "inner",
                "table": "category_totals",
                "on": {"left": "product_sales.category_id", "right": "category_totals.category_id"},
            }],
            "window": [{
                "fn": "row_number",
                "partition": ["product_sales.category_name"],
                "order": [{"col": "product_sales.product_revenue", "dir": "desc"}],
                "as": "rn",
            }],
            "qualify": [{"col": "rn", "op": "lte", "val": 3}],
            "select": [
                "product_sales.category_name",
                "product_sales.product_name",
                "product_sales.product_revenue",
                {"expr": "product_revenue / category_total_sales", "as": "pct"},
            ],
        },
        "各品类内，按销售额排名前3的商品及其销售额在品类中的占比",
    )

    assert any("category_name" in warning and "分母" in warning for warning in warnings)


def test_lint_derived_metric_filter_rejects_having_without_aggregation():
    warnings = lint_conventions(
        {
            "cte": [
                {
                    "name": "category_stats",
                    "query": {
                        "scan": "orders",
                        "group": ["category_name"],
                        "agg": [
                            {"fn": "count_all", "as": "total_orders"},
                            {"fn": "count", "col": "refund_id", "as": "refund_orders"},
                        ],
                        "select": ["category_name", "total_orders", "refund_orders"],
                    },
                }
            ],
            "scan": "category_stats",
            "having": [{"col": "refund_rate", "op": "gt", "val": 0.15}],
            "select": [
                "category_name",
                "total_orders",
                "refund_orders",
                {"expr": "refund_orders * 1.0 / total_orders", "as": "refund_rate"},
            ],
        },
        "统计各品类的退款率（退款订单数/总订单数），找出退款率超过15%的品类",
    )

    assert any("filter" in warning and "HAVING" in warning for warning in warnings)


def test_lint_order_interval_requires_completed_status():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_detail",
            "window": [
                {
                    "fn": "lag",
                    "col": "dwd_order_detail.order_dt",
                    "partition": ["dwd_order_detail.user_id"],
                    "order": [{"col": "dwd_order_detail.order_dt", "dir": "asc"}],
                    "as": "prev_order_dt",
                }
            ],
            "select": ["dwd_order_detail.user_id", "dwd_order_detail.order_dt", "prev_order_dt"],
        },
        "每个用户相邻两次下单之间的时间间隔（天数），显示用户ID、下单时间和距上次下单天数",
    )

    assert any("order_status" in warning and "已完成" in warning for warning in warnings)


def test_lint_order_user_join_requires_order_detail_user_id():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_item_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dwd_order_detail",
                    "on": {
                        "left": "dwd_order_item_detail.order_id",
                        "right": "dwd_order_detail.order_id",
                    },
                },
                {
                    "type": "inner",
                    "table": "dim_user",
                    "on": {
                        "left": "dwd_order_item_detail.user_id",
                        "right": "dim_user.user_id",
                    },
                },
            ],
            "select": ["dim_user.user_name", "dwd_order_detail.order_id"],
        },
        "统计钻石会员用户在已完成订单中的平均商品实付单价",
    )

    assert any("dwd_order_detail.user_id" in warning for warning in warnings)


def test_lint_refund_rate_requires_distinct_order_denominator_and_order_user_join():
    warnings = lint_conventions(
        {
            "cte": [
                {
                    "name": "age_order_stats",
                    "query": {
                        "scan": "dwd_order_detail",
                        "joins": [
                            {
                                "type": "inner",
                                "table": "dim_user",
                                "on": {"left": "dwd_order_detail.user_id", "right": "dim_user.user_id"},
                            }
                        ],
                        "group": ["dim_user.age_group"],
                        "agg": [{"fn": "count_all", "as": "total_orders"}],
                        "select": ["dim_user.age_group", "total_orders"],
                    },
                },
                {
                    "name": "age_refund_stats",
                    "query": {
                        "scan": "dwd_refund_detail",
                        "joins": [
                            {
                                "type": "inner",
                                "table": "dim_user",
                                "on": {"left": "dwd_refund_detail.user_id", "right": "dim_user.user_id"},
                            }
                        ],
                        "group": ["dim_user.age_group"],
                        "agg": [
                            {"fn": "count_distinct", "col": "dwd_refund_detail.order_id", "as": "refund_orders"},
                        ],
                        "select": ["dim_user.age_group", "refund_orders"],
                    },
                },
            ],
            "scan": "age_order_stats",
            "select": ["age_group", "total_orders", "refund_orders"],
        },
        "各年龄段用户的退款率（退款订单数/总订单数），并按退款率降序排列",
    )

    assert any("COUNT(DISTINCT dwd_order_detail.order_id)" in warning for warning in warnings)
    assert any("dwd_refund_detail.order_id JOIN dwd_order_detail.order_id" in warning for warning in warnings)


def test_lint_refund_rate_rejects_split_numerator_denominator_ctes():
    warnings = lint_conventions(
        {
            "cte": [
                {
                    "name": "category_order_stats",
                    "query": {
                        "scan": "dwd_order_detail",
                        "group": ["dim_category.category_id", "dim_category.category_name"],
                        "agg": [
                            {"fn": "count_distinct", "col": "dwd_order_detail.order_id", "as": "total_orders"},
                        ],
                        "select": ["dim_category.category_id", "dim_category.category_name", "total_orders"],
                    },
                },
                {
                    "name": "category_refund_stats",
                    "query": {
                        "scan": "dwd_refund_detail",
                        "group": ["dim_category.category_id", "dim_category.category_name"],
                        "agg": [
                            {"fn": "count_distinct", "col": "dwd_refund_detail.order_id", "as": "refund_orders"},
                        ],
                        "select": ["dim_category.category_id", "dim_category.category_name", "refund_orders"],
                    },
                },
            ],
            "scan": "category_order_stats",
            "joins": [
                {
                    "type": "left",
                    "table": "category_refund_stats",
                    "on": {
                        "left": "category_order_stats.category_id",
                        "right": "category_refund_stats.category_id",
                    },
                }
            ],
            "select": ["category_name", "total_orders", "refund_orders"],
        },
        "统计各品类的退款率（退款订单数/总订单数），找出退款率超过15%的品类",
    )

    assert any("同一分组查询" in warning for warning in warnings)


def test_lint_refund_rate_rejects_percentage_output():
    warnings = lint_conventions(
        {
            "scan": "refund_stats",
            "select": [
                "age_group",
                {"expr": "ROUND(refund_orders * 100.0 / total_orders, 2)", "as": "refund_rate"},
            ],
        },
        "各年龄段用户的退款率（退款订单数/总订单数），并按退款率降序排列",
    )

    assert any("0~1 小数" in warning for warning in warnings)


def test_lint_percentage_display_rejects_percent_not_decimal_ratio():
    warnings = lint_conventions(
        {
            "scan": "monthly_channel_orders",
            "joins": [
                {
                    "type": "inner",
                    "table": "monthly_total_orders",
                    "on": {
                        "left": "monthly_channel_orders.month",
                        "right": "monthly_total_orders.month",
                    },
                }
            ],
            "select": [
                "monthly_channel_orders.month",
                "monthly_channel_orders.channel_order_count",
                {"expr": "ROUND(channel_order_count * 100.0 / total_order_count, 2)", "as": "channel_order_ratio"},
            ],
        },
        "每个渠道按月统计订单数，以及该渠道当月订单数在所有渠道当月总订单数中的占比",
    )

    assert any("0~1 小数" in warning and "不要乘以 100" in warning for warning in warnings)


def test_lint_ratio_hides_unrequested_window_denominator():
    warnings = lint_conventions(
        {
            "scan": "monthly_channel",
            "window": [
                {
                    "fn": "sum",
                    "col": "monthly_channel.order_count",
                    "partition": ["monthly_channel.month"],
                    "as": "month_total",
                }
            ],
            "select": [
                "monthly_channel.channel_name",
                "monthly_channel.month",
                "monthly_channel.order_count",
                "month_total",
                {
                    "expr": "ROUND(monthly_channel.order_count * 1.0 / month_total, 4)",
                    "as": "pct",
                },
            ],
        },
        "每个渠道按月统计订单数，以及当月订单数在所有渠道当月总订单数中的占比",
    )

    assert any("month_total" in warning and "中间汇总" in warning for warning in warnings)


def test_lint_percentage_display_accepts_decimal_ratio():
    warnings = lint_conventions(
        {
            "scan": "monthly_channel_orders",
            "select": [
                "monthly_channel_orders.month",
                {"expr": "ROUND(channel_order_count * 1.0 / total_order_count, 4)", "as": "channel_order_ratio"},
            ],
        },
        "每个渠道按月统计订单数，以及该渠道当月订单数在所有渠道当月总订单数中的占比",
    )

    assert not any("0~1 小数" in warning for warning in warnings)


def test_lint_unit_price_rejects_window_avg_qualify():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dim_user",
                    "on": {"left": "dwd_order_detail.user_id", "right": "dim_user.id"},
                }
            ],
            "window": [
                {
                    "fn": "avg",
                    "col": "dwd_order_detail.total_amount",
                    "partition": ["dwd_order_detail.user_id"],
                    "as": "avg_order_amount",
                }
            ],
            "qualify": [{"col": "avg_order_amount", "op": "between", "lo": 500, "hi": 2000}],
            "select": ["dwd_order_detail.order_id", "avg_order_amount"],
        },
        "找出客单价在500到2000之间、25-34岁或35-44岁女性用户的已完成订单",
    )

    assert any("窗口均值" in warning for warning in warnings)
    assert any("dim_user.user_id" in warning and "dim_user.id" in warning for warning in warnings)


def test_lint_average_order_value_allows_order_count_threshold():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_detail",
            "group": ["dim_channel.channel_name"],
            "agg": [
                {"fn": "count_all", "as": "order_count"},
                {"fn": "avg", "col": "dwd_order_detail.total_amount", "as": "avg_order_value"},
            ],
            "having": [{"col": "order_count", "op": "gt", "val": 10}],
            "select": ["dim_channel.channel_name", "order_count", "avg_order_value"],
        },
        "各渠道中，已完成订单数超过10笔的渠道，列出渠道名称、订单数和平均客单价，按订单数降序",
    )

    assert not any("客单价在X到Y之间" in warning for warning in warnings)


def test_lint_unit_price_order_lookup_requires_exact_output_contract():
    warnings = lint_conventions(
        {
            "scan": "dwd_order_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dim_user",
                    "on": {
                        "left": "dwd_order_detail.user_id",
                        "right": "dim_user.user_id",
                    },
                }
            ],
            "filter": [
                {"col": "dwd_order_detail.order_status", "op": "eq", "val": "已完成"},
                {"col": "dim_user.gender", "op": "eq", "val": "female"},
                {"col": "dwd_order_detail.total_amount", "op": "between", "lo": 500, "hi": 2000},
            ],
            "select": [
                "dwd_order_detail.order_id",
                "dim_user.user_name",
                "dwd_order_detail.total_amount",
                "dwd_order_detail.order_dt",
            ],
            "sort": [{"col": "dwd_order_detail.order_id", "dir": "asc"}],
        },
        "找出客单价在500到2000之间、25-34岁或35-44岁女性用户的已完成订单",
    )

    assert any("不要漏掉 age_group" in warning for warning in warnings)
    assert any("不要额外添加 ORDER BY" in warning for warning in warnings)


def test_lint_rejects_mixed_legacy_orders_with_large_schema_tables():
    warnings = lint_conventions(
        {
            "scan": "orders",
            "joins": [
                {
                    "type": "inner",
                    "table": "dim_user",
                    "on": {"left": "orders.user_id", "right": "dim_user.user_id"},
                }
            ],
            "select": ["orders.order_id", "dim_user.user_name", "orders.total_amount"],
        },
        "找出客单价在500到2000之间、25-34岁或35-44岁女性用户的已完成订单",
    )

    assert any("不要混入旧示例表 orders" in warning for warning in warnings)


def test_lint_good_review_with_images_requires_contract():
    warnings = lint_conventions(
        {
            "scan": "dwd_comment_detail",
            "filter": [
                {"col": "dwd_comment_detail.comment_dt", "op": "gte", "val": {"$date": "2025-12-01"}},
                {"col": "dwd_comment_detail.rating", "op": "in", "val": [4, 5]},
                {"col": "dwd_comment_detail.has_image", "op": "eq", "val": 1},
            ],
            "select": [
                "dwd_comment_detail.comment_id",
                "dwd_comment_detail.order_item_id",
                "dwd_comment_detail.user_id",
                "dwd_comment_detail.product_id",
                "dwd_comment_detail.rating",
                "dwd_comment_detail.comment_dt",
            ],
            "sort": [{"col": "dwd_comment_detail.rating", "dir": "desc"}],
        },
        "2025年12月以来，评分为4或5星且带图片的好评记录，按评分降序",
    )

    assert any("comment_type = '好评'" in warning for warning in warnings)
    assert any("最终输出列" in warning and "order_item_id" in warning for warning in warnings)
    assert any("comment_dt DESC" in warning for warning in warnings)


def test_lint_good_review_with_images_accepts_unqualified_single_table_columns():
    warnings = lint_conventions(
        {
            "scan": "dwd_comment_detail",
            "filter": [
                {"col": "comment_type", "op": "eq", "val": "好评"},
                {"col": "has_image", "op": "eq", "val": 1},
                {"col": "rating", "op": "in", "val": [4, 5]},
                {"col": "comment_dt", "op": "gte", "val": {"$date": "2025-12-01"}},
            ],
            "select": ["comment_id", "product_id", "user_id", "rating", "comment_dt"],
            "sort": [
                {"col": "rating", "dir": "desc"},
                {"col": "comment_dt", "dir": "desc"},
            ],
        },
        "2025年12月以来，评分为4或5星且带图片的好评记录，按评分降序",
    )

    assert not any("最终输出列" in warning or "稳定排序" in warning for warning in warnings)


def test_lint_refund_product_ranking_requires_refund_count_and_no_status_filter():
    warnings = lint_conventions(
        {
            "scan": "dwd_refund_detail",
            "joins": [
                {
                    "type": "inner",
                    "table": "dwd_order_item_detail",
                    "on": {
                        "left": "dwd_refund_detail.order_id",
                        "right": "dwd_order_item_detail.order_id",
                    },
                },
                {
                    "type": "inner",
                    "table": "dim_product",
                    "on": {
                        "left": "dwd_order_item_detail.product_id",
                        "right": "dim_product.product_id",
                    },
                },
            ],
            "filter": [{"col": "dwd_refund_detail.refund_status", "op": "eq", "val": "已退款"}],
            "group": ["dim_product.product_id", "dim_product.product_name"],
            "agg": [{"fn": "sum", "col": "dwd_refund_detail.refund_amount", "as": "total_refund"}],
            "select": ["dim_product.product_name", "total_refund"],
            "limit": 5,
        },
        "按退款总金额排名，找出退款金额最高的前5个商品",
    )

    assert any("refund_count" in warning for warning in warnings)
    assert any("refund_status" in warning for warning in warnings)


def test_lint_product_category_share_rejects_product_id_output_and_id_total():
    warnings = lint_conventions(
        {
            "cte": [
                {
                    "name": "product_sales",
                    "query": {
                        "scan": "dwd_order_item_detail",
                        "group": [
                            "dim_product.product_id",
                            "dim_product.product_name",
                            "dim_category.category_id",
                            "dim_category.category_name",
                        ],
                        "agg": [
                            {"fn": "sum", "col": "dwd_order_item_detail.actual_amount", "as": "product_sales"},
                        ],
                        "select": [
                            "dim_product.product_id",
                            "dim_product.product_name",
                            "dim_category.category_id",
                            "dim_category.category_name",
                            "product_sales",
                        ],
                    },
                },
                {
                    "name": "category_total",
                    "query": {
                        "scan": "product_sales",
                        "group": ["product_sales.category_id"],
                        "agg": [{"fn": "sum", "col": "product_sales.product_sales", "as": "category_total_sales"}],
                        "select": ["product_sales.category_id", "category_total_sales"],
                    },
                },
            ],
            "scan": "product_sales",
            "select": [
                "product_sales.product_id",
                "product_sales.product_name",
                "product_sales.category_name",
                "product_sales.product_sales",
                {"expr": "ROUND(product_sales.product_sales * 1.0 / category_total_sales, 4)", "as": "pct_of_category"},
            ],
        },
        "计算每个商品的销售额及其在所属品类总销售额中的占比",
    )

    assert any("不要额外输出 product_id" in warning for warning in warnings)
    assert any("category_name 展示粒度" in warning or "category_id" in warning for warning in warnings)


def test_lint_adjacent_review_lag_rejects_topn_prefilter():
    warnings = lint_conventions(
        {
            "cte": [
                {
                    "name": "recent_comments",
                    "query": {
                        "scan": "dwd_comment_detail",
                        "window": [
                            {
                                "fn": "row_number",
                                "partition": ["dwd_comment_detail.product_id"],
                                "order": [{"col": "dwd_comment_detail.comment_dt", "dir": "desc"}],
                                "as": "rn",
                            }
                        ],
                        "qualify": [{"col": "rn", "op": "lte", "val": 2}],
                        "select": [
                            "dwd_comment_detail.product_id",
                            "dwd_comment_detail.comment_dt",
                            "dwd_comment_detail.rating",
                        ],
                    },
                }
            ],
            "scan": "recent_comments",
            "window": [{"fn": "lag", "col": "recent_comments.rating", "as": "prev_rating"}],
            "select": ["recent_comments.product_id", "recent_comments.comment_dt", "prev_rating"],
        },
        "每个商品相邻两次评价的评分变化，显示商品ID、评价时间、当前评分和上一次评分",
    )

    assert any("不要先用 row_number/limit" in warning for warning in warnings)


def test_lint_adjacent_review_is_not_misclassified_as_per_group_topn():
    warnings = lint_conventions(
        {
            "scan": "dwd_comment_detail",
            "window": [
                {
                    "fn": "lag",
                    "col": "dwd_comment_detail.rating",
                    "partition": ["dwd_comment_detail.product_id"],
                    "order": [{"col": "dwd_comment_detail.comment_dt", "dir": "asc"}],
                    "as": "prev_rating",
                }
            ],
            "select": [
                "dwd_comment_detail.product_id",
                "dwd_comment_detail.comment_dt",
                "dwd_comment_detail.rating",
                "prev_rating",
            ],
        },
        "每个商品相邻两次评价的评分变化，显示商品ID、评价时间、当前评分和上一次评分",
    )

    assert not any("分组内 TopN" in warning for warning in warnings)


def test_lint_channel_monthly_mom_rejects_channel_id_partition_and_aliases():
    warnings = lint_conventions(
        {
            "cte": [
                {
                    "name": "monthly",
                    "query": {
                        "scan": "dwd_order_detail",
                        "group": [
                            "dim_channel.channel_id",
                            "dim_channel.channel_name",
                            {"expr": "STRFTIME('%Y-%m', dwd_order_detail.order_dt)", "as": "month"},
                        ],
                        "agg": [{"fn": "count_all", "as": "order_count"}],
                        "select": ["dim_channel.channel_id", "dim_channel.channel_name", "month", "order_count"],
                    },
                }
            ],
            "scan": "monthly",
            "window": [
                {
                    "fn": "lag",
                    "col": "monthly.order_count",
                    "partition": ["monthly.channel_id"],
                    "order": [{"col": "monthly.month", "dir": "asc"}],
                    "as": "prev_order_count",
                }
            ],
            "select": [
                "monthly.channel_name",
                "monthly.month",
                "monthly.order_count",
                "prev_order_count",
                {"expr": "monthly.order_count - prev_order_count", "as": "change"},
            ],
        },
        "每个渠道按月统计已完成订单量，以及与上月相比的环比变化量",
    )

    assert any("channel_name" in warning and "channel_id" in warning for warning in warnings)
    assert any("prev_month_count" in warning for warning in warnings)
    assert any("mom_change" in warning for warning in warnings)


def test_lint_cart_without_purchase_rejects_bare_user_id_output():
    warnings = lint_conventions(
        {
            "scan": "target_users",
            "joins": [
                {
                    "type": "inner",
                    "table": "dim_user",
                    "on": {"left": "target_users.user_id", "right": "dim_user.user_id"},
                }
            ],
            "select": ["user_id", "dim_user.user_name"],
        },
        "找出曾将某商品加入购物车（add）但该商品从未出现在其已完成订单中的用户，显示用户ID和用户名",
    )

    assert any("裸 user_id" in warning for warning in warnings)


def test_lint_cross_event_user_counts_requires_qualified_cte_outputs_and_distinct_counts():
    warnings = lint_conventions(
        {
            "cte": [
                {
                    "name": "add_counts",
                    "query": {
                        "scan": "dwd_cart_detail",
                        "agg": [{"fn": "count_all", "as": "add_count"}],
                        "select": ["dwd_cart_detail.user_id", "add_count"],
                    },
                },
                {
                    "name": "refund_counts",
                    "query": {
                        "scan": "dwd_refund_detail",
                        "agg": [{"fn": "count_all", "as": "refund_count"}],
                        "select": ["dwd_refund_detail.user_id", "refund_count"],
                    },
                },
            ],
            "scan": "add_counts",
            "joins": [
                {
                    "type": "inner",
                    "table": "refund_counts",
                    "on": {"left": "add_counts.user_id", "right": "refund_counts.user_id"},
                }
            ],
            "select": ["dim_user.user_name", "add_count", "refund_count"],
        },
        "2025年11月以来，既有加购行为（add）又有退款记录的用户，显示用户名、加购次数和退款次数",
    )

    assert any("add_counts.add_count" in warning and "refund_counts.refund_count" in warning for warning in warnings)
    assert any("COUNT(DISTINCT dwd_cart_detail.cart_id)" in warning for warning in warnings)
