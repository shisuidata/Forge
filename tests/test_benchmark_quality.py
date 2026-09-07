import math

import pytest

from forge.benchmark_quality import summarize_quality


def test_all_requests_remain_in_denominators_and_clarified_answer_is_one_task():
    records = [
        {"case_id": "autonomous", "status": "answered", "correct": True, "answerable": True,
         "clarification_turns": 0, "user_interactions": 0, "model_calls": 1, "total_tokens": 100, "elapsed_ms": 10},
        {"case_id": "clarified", "status": "answered", "correct": True, "answerable": True,
         "clarification_turns": 2, "user_interactions": 2, "model_calls": 3, "total_tokens": 300, "elapsed_ms": 30},
        {"case_id": "wrong", "status": "answered", "correct": False,
         "clarification_turns": 0, "user_interactions": 0, "model_calls": 1, "total_tokens": 100, "elapsed_ms": 10},
        {"case_id": "blocked", "status": "refused", "refusal_reason": "permission", "answerable": True,
         "clarification_turns": 0, "user_interactions": 0, "model_calls": 0, "total_tokens": 0, "elapsed_ms": 1},
        {"case_id": "pending", "status": "needs_clarification", "clarification_turns": 1,
         "user_interactions": 0, "model_calls": 1, "total_tokens": 100, "elapsed_ms": 10},
        {"case_id": "failed", "status": "failed", "clarification_turns": 0,
         "user_interactions": 0, "model_calls": 1, "total_tokens": 100, "elapsed_ms": 20},
    ]
    result = summarize_quality(records, label_basis="independent_semantic")
    metrics = result["metrics"]
    assert result["request_count"] == 6
    for metric, count in {"correct_completion": 2, "answer_coverage": 3, "silent_error": 1,
                          "clarification": 2, "refusal": 1, "permission_refusal": 1,
                          "avoidable_refusal": 0, "autonomous_completion": 1, "failure": 1}.items():
        assert metrics[metric]["count"] == count
        assert metrics[metric]["denominator"] == 6
        assert metrics[metric]["rate"] == count / 6
    assert metrics["conditional_accuracy"]["rate"] == 2 / 3
    assert result["costs"]["model_calls"]["total"] == 7
    assert result["costs"]["model_calls"]["per_correct_completion"] == 3.5
    assert result["costs"]["total_tokens"]["per_correct_completion"] == 350
    assert result["costs"]["elapsed_ms"]["per_correct_completion"] == 40.5
    assert result["costs"]["user_interactions"]["per_correct_completion"] == 1


def test_official_ex_cannot_be_reported_as_semantic_silent_error():
    records = [{"case_id": "a", "status": "answered", "correct": False}]
    result = summarize_quality(records, label_basis="official_ex")
    assert result["metrics"]["silent_error"] is None
    assert result["metrics"]["EX_wrong_answer_proxy"]["rate"] == 1
    unknown = summarize_quality(records, label_basis="unknown")
    assert unknown["metrics"]["silent_error"] is None
    assert unknown["metrics"]["EX_wrong_answer_proxy"] is None
    assert unknown["metrics"]["correct_completion"]["rate"] is None
    assert unknown["metrics"]["answer_coverage"]["rate"] == 1


def test_partial_labels_and_costs_are_not_zero_imputed_or_dropped():
    result = summarize_quality([
        {"case_id": "known", "status": "answered", "correct": True, "model_calls": 2},
        {"case_id": "unjudged", "status": "answered", "correct": None, "model_calls": None},
    ], label_basis="independent_semantic")
    completion = result["metrics"]["correct_completion"]
    assert completion["denominator"] == 2
    assert completion["count"] is None and completion["rate"] is None
    assert completion["observed_count"] == 1
    assert completion["known_count"] == completion["missing_count"] == 1
    assert completion["state"] == "partial"
    calls = result["costs"]["model_calls"]
    assert calls["observed_total"] == 2
    assert calls["total"] is None and calls["per_correct_completion"] is None
    assert calls["known_count"] == calls["missing_count"] == 1
    assert result["costs"]["total_tokens"]["observed_total"] is None
    assert result["costs"]["total_tokens"]["state"] == "unknown"
    assert result["metrics"]["autonomous_completion"]["state"] == "unknown"
    assert result["observations"]["answerable"] == {"known_count": 0, "missing_count": 2}


def test_complete_costs_still_require_known_correct_count_for_unit_cost():
    result = summarize_quality([
        {"case_id": "a", "status": "answered", "correct": True, "model_calls": 1},
        {"case_id": "b", "status": "answered", "model_calls": 2},
    ], label_basis="independent_semantic")
    assert result["costs"]["model_calls"]["total"] == 3
    assert result["costs"]["model_calls"]["per_correct_completion"] is None


def test_permission_and_genuinely_unanswerable_refusals_are_not_capability_errors():
    result = summarize_quality([
        {"case_id": "permission", "status": "refused", "refusal_reason": "permission", "answerable": True},
        {"case_id": "capability", "status": "refused", "refusal_reason": "capability", "answerable": True},
        {"case_id": "unsupported", "status": "refused", "refusal_reason": "unsupported", "answerable": False},
        {"case_id": "no-data", "status": "refused", "refusal_reason": "missing_data", "answerable": False},
    ], label_basis="independent_semantic")
    assert result["metrics"]["permission_refusal"]["rate"] == 1 / 4
    assert result["metrics"]["avoidable_refusal"]["rate"] == 1 / 4
    assert result["metrics"]["conditional_accuracy"]["rate"] is None


@pytest.mark.parametrize("record", [
    {"refusal_reason": "capability"},
    {"refusal_reason": "unknown", "answerable": True},
    {"answerable": True},
])
def test_refusals_need_answerability_and_permission_evidence(record):
    result = summarize_quality([{"case_id": "a", "status": "refused", **record}], label_basis="independent_semantic")
    assert result["metrics"]["avoidable_refusal"]["state"] == "unknown"
    assert result["metrics"]["avoidable_refusal"]["count"] is None


def test_empty_input_and_zero_correct_answers_do_not_invent_unit_costs():
    empty = summarize_quality([], label_basis="unknown")
    assert empty["metrics"]["answer_coverage"]["rate"] is None
    assert empty["costs"]["model_calls"]["total"] is None
    failed = summarize_quality([{"case_id": "a", "status": "failed", "model_calls": 0}], label_basis="official_ex")
    assert failed["costs"]["model_calls"]["total"] == 0
    assert failed["costs"]["model_calls"]["per_correct_completion"] is None
    assert failed["metrics"]["correct_completion"]["rate"] == 0


@pytest.mark.parametrize("fields", [
    {"total_tokens": math.nan}, {"elapsed_ms": math.inf}, {"total_tokens": -1},
    {"model_calls": True}, {"user_interactions": -1}, {"clarification_turns": 1.5},
    {"model_calls": "1"}, {"total_tokens": False}, {"correct": 1}, {"answerable": "yes"},
    {"status": "completed"}, {"status": []}, {"refusal_reason": []},
    {"refusal_reason": "permission"}, {"correct": True, "answerable": False},
    {"status": "failed", "correct": True},
    {"status": "needs_clarification", "clarification_turns": 0},
])
def test_invalid_or_contradictory_observations_fail_closed(fields):
    with pytest.raises(ValueError):
        summarize_quality([{"case_id": "a", "status": "answered", **fields}], label_basis="official_ex")


def test_duplicate_tasks_and_invalid_basis_fail_closed():
    record = {"case_id": "a", "status": "answered"}
    with pytest.raises(ValueError):
        summarize_quality([record, record], label_basis="official_ex")
    with pytest.raises(ValueError):
        summarize_quality([record], label_basis="contract")
    with pytest.raises(ValueError):
        summarize_quality([{"case_id": " ", "status": "answered"}], label_basis="unknown")


def test_finite_cost_inputs_cannot_overflow_into_infinite_totals():
    with pytest.raises(ValueError):
        summarize_quality([
            {"case_id": "a", "status": "failed", "elapsed_ms": 1e308},
            {"case_id": "b", "status": "failed", "elapsed_ms": 1e308},
        ], label_basis="unknown")
