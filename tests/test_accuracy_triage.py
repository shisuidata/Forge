from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "accuracy"))

from triage_failures import build_triage, render_markdown


def test_triage_classifies_missing_topn_qualify():
    cases = [
        {
            "id": 1,
            "category": "排名与TopN",
            "question": "每个品类销售额最高的前 3 个商品",
            "reference_sql": "SELECT * FROM ranked WHERE rn <= 3",
        }
    ]
    runs = {
        "1": {
            "runs": [
                {
                    "sql": "SELECT product_name, row_number() OVER (PARTITION BY category_name ORDER BY sales DESC) AS rn FROM product_sales",
                    "error": None,
                }
            ]
        }
    }
    ea = {
        "method": "x",
        "ea": 0.0,
        "run_accuracy": 0.0,
        "case_results": {
            "1": {
                "question": cases[0]["question"],
                "category": cases[0]["category"],
                "any_correct": False,
                "runs": [{"correct": False, "reason": "ref=9行, gen=100行"}],
            }
        },
    }

    triage = build_triage(cases, runs, ea)

    assert triage["total_failures"] == 1
    assert triage["failures"][0]["root_cause"] == "topn_filter"
    assert triage["root_cause_counts"] == {"topn_filter": 1}


def test_triage_does_not_flag_topn_when_outer_rank_filter_exists():
    cases = [
        {
            "id": 1,
            "category": "排名与TopN",
            "question": "每个品类销售额最高的前 3 个商品",
            "reference_sql": "SELECT * FROM ranked WHERE rn <= 3",
        }
    ]
    runs = {
        "1": {
            "runs": [
                {
                    "sql": """
                    SELECT * FROM (
                      SELECT product_name,
                             row_number() OVER (PARTITION BY category_name ORDER BY sales DESC) AS sales_rank
                      FROM product_sales
                    ) AS ranked
                    WHERE sales_rank <= 3
                    """,
                    "error": None,
                }
            ]
        }
    }
    ea = {
        "method": "x",
        "ea": 0.0,
        "run_accuracy": 0.0,
        "case_results": {
            "1": {
                "question": cases[0]["question"],
                "category": cases[0]["category"],
                "any_correct": False,
                "runs": [{"correct": False, "reason": "ref=9行, gen=10行"}],
            }
        },
    }

    triage = build_triage(cases, runs, ea)

    assert triage["failures"][0]["root_cause"] != "topn_filter"


def test_triage_renders_markdown_backlog():
    triage = {
        "method": "x",
        "ea": 0.5,
        "run_accuracy": 0.4,
        "total_failures": 1,
        "root_cause_counts": {"filter_semantics": 1},
        "category_root_cause_counts": {"复杂过滤": {"filter_semantics": 1}},
        "failures": [
            {
                "case_id": "2",
                "category": "复杂过滤",
                "question": "统计已完成订单",
                "root_cause_label": "复杂过滤/字段约定缺失",
                "reason": "ref=10行, gen=20行",
                "next_action": "把字段约定转成 lint 或 field_conventions 规则。",
                "generated_sql": "SELECT * FROM orders",
            }
        ],
    }

    md = render_markdown(triage)

    assert "Method x EA 失败归因" in md
    assert "| 复杂过滤/字段约定缺失 | 1 |" in md
    assert "Case 2" in md
    assert "SELECT * FROM orders" in md
