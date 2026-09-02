from __future__ import annotations

import pytest

from agent.prompts import _detect_needed_examples, build_system


def test_group_topn_question_injects_topn_example():
    question = "各品类内按销售数量排名前3的商品"

    assert "topn.md" in _detect_needed_examples(question)

    prompt = build_system("schema", question=question, mode="benchmark")

    assert "聚合后的分组内 TopN" in prompt
    assert "qualify" in prompt
    assert "只写 `sort + limit: 3`" in prompt
    assert "窗口意图必须落到 window" in prompt
    assert "请求的输出不得遗漏" in prompt
    assert "Do NOT use for ranking/TopN" not in prompt

@pytest.mark.parametrize(
    "question",
    [
        "Show consumption for enterprise customers before 2020",
        "Return the score recorded by each driver",
        "What is the anti-Cardiolipin antibody concentration?",
    ],
)
def test_unrelated_english_words_do_not_inject_boolean_examples(question):
    examples = _detect_needed_examples(question)

    assert "filter_or.md" not in examples
    assert "antijoin.md" not in examples


@pytest.mark.parametrize(
    ("question", "example"),
    [
        ("Customers who paid by card or cash", "filter_or.md"),
        ("List the top 3 products for each category", "topn.md"),
        ("Show the cumulative monthly revenue", "window_lag.md"),
        ("Calculate the year over year growth rate", "cte.md"),
        ("Customers without any completed order", "antijoin.md"),
    ],
)
def test_cross_language_intents_inject_only_relevant_examples(question, example):
    assert example in _detect_needed_examples(question)


def test_current_date_is_injected_only_for_relative_time_questions():
    assert "## 当前时间" not in build_system("schema", question="Orders in 2020")
    assert "## 当前时间" in build_system("schema", question="今年的订单")
    assert "## 当前时间" in build_system("schema", question="Orders this year")
