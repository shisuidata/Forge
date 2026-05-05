from __future__ import annotations

from agent.prompts import _detect_needed_examples, build_system


def test_group_topn_question_injects_topn_example():
    question = "各品类内按销售数量排名前3的商品"

    assert "topn.md" in _detect_needed_examples(question)

    prompt = build_system("schema", question=question, mode="benchmark")

    assert "聚合后的分组内 TopN" in prompt
    assert "qualify" in prompt
    assert "只写 `sort + limit: 3`" in prompt
