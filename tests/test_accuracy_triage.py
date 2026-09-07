from __future__ import annotations

from tools.benchmarks.accuracy.triage_failures import build_triage


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
