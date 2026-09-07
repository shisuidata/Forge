"""Request-level quality accounting; benchmark EX is not a semantic truth label."""

from collections.abc import Sequence
import math


_LABEL_BASES = {"official_ex", "independent_semantic", "unknown"}
_STATUSES = {"answered", "needs_clarification", "refused", "failed"}
_REFUSAL_REASONS = {"permission", "unsupported", "missing_data", "capability", "unknown"}
_INTEGER_FIELDS = ("clarification_turns", "user_interactions", "model_calls")
_COST_FIELDS = ("model_calls", "total_tokens", "elapsed_ms", "user_interactions")
_OBSERVATION_FIELDS = ("correct", "answerable", "refusal_reason", "clarification_turns", *_COST_FIELDS)


def _validate(records: Sequence[dict], label_basis: str) -> None:
    if not isinstance(label_basis, str) or label_basis not in _LABEL_BASES:
        raise ValueError("label_basis must be official_ex, independent_semantic, or unknown")
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
        raise ValueError("records must be a sequence of objects")
    seen = set()
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("each record must be an object")
        case_id = record.get("case_id")
        if not isinstance(case_id, str) or not case_id.strip() or case_id in seen:
            raise ValueError("case_id must be a nonempty unique string")
        seen.add(case_id)
        status = record.get("status")
        if not isinstance(status, str) or status not in _STATUSES:
            raise ValueError(f"{case_id}: invalid status")
        for field in ("correct", "answerable"):
            if record.get(field) is not None and type(record[field]) is not bool:
                raise ValueError(f"{case_id}: {field} must be boolean or null")
        reason = record.get("refusal_reason")
        if reason is not None and (not isinstance(reason, str) or reason not in _REFUSAL_REASONS):
            raise ValueError(f"{case_id}: invalid refusal_reason")
        if reason is not None and status != "refused":
            raise ValueError(f"{case_id}: refusal_reason requires refused status")
        if record.get("correct") is True and (status != "answered" or record.get("answerable") is False):
            raise ValueError(f"{case_id}: correct answer contradicts status or answerable label")
        for field in (*_INTEGER_FIELDS, "total_tokens", "elapsed_ms"):
            value = record.get(field)
            if value is None:
                continue
            types = (int,) if field in _INTEGER_FIELDS else (int, float)
            if type(value) not in types or value < 0 or (type(value) is float and not math.isfinite(value)):
                raise ValueError(f"{case_id}: {field} must be finite, nonnegative, and correctly typed")
        if status == "needs_clarification" and record.get("clarification_turns") == 0:
            raise ValueError(f"{case_id}: needs_clarification requires a requested clarification turn")


def _metric(values: list[bool | None]) -> dict:
    denominator = len(values)
    known = sum(value is not None for value in values)
    observed_count = sum(value is True for value in values)
    complete = known == denominator and denominator > 0
    return {
        "count": observed_count if complete else None,
        "observed_count": observed_count if known else None,
        "denominator": denominator,
        "rate": observed_count / denominator if complete else None,
        "known_count": known,
        "missing_count": denominator - known,
        "state": "known" if complete else "partial" if known else "unknown",
    }


def _all_true(*values: bool | None) -> bool | None:
    if False in values:
        return False
    return None if None in values else True


def summarize_quality(records: Sequence[dict], *, label_basis: str) -> dict:
    """Summarize one terminal record per request, without imputing missing observations.

    Clarification turns count requested rounds (including an unanswered request).
    User interactions count additional user actions, excluding the initial request.
    Rates with incomplete labels remain null; observed counts are not estimates.
    Total/per-correct costs include unsuccessful requests and require complete data.
    """
    _validate(records, label_basis)
    completion = []
    answers = []
    accuracy = []
    wrong_answers = []
    clarification = []
    refusals = []
    permission_refusals = []
    avoidable_refusals = []
    autonomous = []
    for record in records:
        answered = record["status"] == "answered"
        refused = record["status"] == "refused"
        correct = record.get("correct") if label_basis != "unknown" else None
        completed = correct if answered else False
        completion.append(completed)
        answers.append(answered)
        if answered:
            accuracy.append(correct)
        wrong_answers.append((not correct if correct is not None else None) if answered else False)
        turns = record.get("clarification_turns")
        interactions = record.get("user_interactions")
        clarification.append(True if record["status"] == "needs_clarification" else turns > 0 if turns is not None else None)
        refusals.append(refused)
        reason = record.get("refusal_reason")
        reason_known = reason not in (None, "unknown")
        permission_refusals.append((reason == "permission" if reason_known else None) if refused else False)
        answerable = record.get("answerable")
        if not refused or reason == "permission" or answerable is False:
            avoidable_refusals.append(False)
        elif not reason_known or answerable is None:
            avoidable_refusals.append(None)
        else:
            avoidable_refusals.append(True)
        autonomous.append(_all_true(completed, turns == 0 if turns is not None else None,
                                    interactions == 0 if interactions is not None else None))

    metrics = {
        "correct_completion": _metric(completion),
        "answer_coverage": _metric(answers),
        "conditional_accuracy": _metric(accuracy),
        "silent_error": _metric(wrong_answers) if label_basis == "independent_semantic" else None,
        "EX_wrong_answer_proxy": _metric(wrong_answers) if label_basis == "official_ex" else None,
        "clarification": _metric(clarification),
        "refusal": _metric(refusals),
        "permission_refusal": _metric(permission_refusals),
        "avoidable_refusal": _metric(avoidable_refusals),
        "autonomous_completion": _metric(autonomous),
        "failure": _metric([record["status"] == "failed" for record in records]),
    }
    observations = {}
    for field in _OBSERVATION_FIELDS:
        known = sum(record.get(field) is not None and not (field == "refusal_reason" and record[field] == "unknown")
                    for record in records)
        observations[field] = {"known_count": known, "missing_count": len(records) - known}
    costs = {}
    correct_count = metrics["correct_completion"]["count"]
    for field in _COST_FIELDS:
        values = [record[field] for record in records if record.get(field) is not None]
        observed_total = sum(values) if values else None
        if isinstance(observed_total, float) and not math.isfinite(observed_total):
            raise ValueError(f"{field}: aggregate exceeds finite numeric range")
        total = observed_total if len(values) == len(records) else None
        costs[field] = {
            "total": total,
            "observed_total": observed_total,
            "per_correct_completion": total / correct_count if total is not None and correct_count else None,
            "known_count": len(values),
            "missing_count": len(records) - len(values),
            "state": "known" if values and len(values) == len(records) else "partial" if values else "unknown",
        }
    return {
        "metric_revision": "request-quality-v1",
        "label_basis": label_basis,
        "correctness_scope": {
            "official_ex": "EX execution-result proxy only; not business correctness or SQL semantic equivalence",
            "independent_semantic": "independently labeled business semantic correctness",
            "unknown": "no established correctness basis",
        }[label_basis],
        "request_count": len(records),
        "metrics": metrics,
        "costs": costs,
        "observations": observations,
    }
