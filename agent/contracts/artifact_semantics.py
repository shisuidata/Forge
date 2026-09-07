"""Semantic checks for TypeBox-owned structured artifacts, after JSON Schema.

Keep these in agreement with structured-artifact-tools.ts via the shared
structured-artifact-fixtures.v1.json corpus. Context ownership (supplied evidence,
report source identity) is enforced separately by the executing stage.
"""
from __future__ import annotations

import calendar
import re
from typing import Any

from jsonschema import FormatChecker, ValidationError

STRUCTURED_ARTIFACT_CONTRACTS = frozenset({
    "clarification_artifact", "metric_definition_artifact", "query_result_artifact",
    "analysis_artifact", "advisory_artifact", "rendered_output_artifact",
})

structured_artifact_format_checker = FormatChecker()
_DATE_TIME = re.compile(
    r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):"
    r"([0-9]{2}(?:\.[0-9]+)?)(?:Z|([+-])([0-9]{2}):([0-9]{2}))$", re.IGNORECASE,
)


@structured_artifact_format_checker.checks("date-time")
def _date_time(value: Any) -> bool:
    # TypeBox IsDateTime semantics, including timezone offsets and leap seconds.
    # jsonschema otherwise silently skips this format without optional extras.
    if not isinstance(value, str):
        return True
    match = _DATE_TIME.match(value)
    if match is None:
        return False
    year, month, day, hour, minute = map(int, match.group(1, 2, 3, 4, 5))
    if not 1 <= month <= 12 or not 1 <= day <= calendar.monthrange(year, month)[1]:
        return False
    second = float(match.group(6))
    sign = -1 if match.group(7) == "-" else 1
    offset_hour, offset_minute = int(match.group(8) or 0), int(match.group(9) or 0)
    if offset_hour > 23 or offset_minute > 59:
        return False
    if hour <= 23 and minute <= 59 and second < 60:
        return True
    utc_minute = minute - offset_minute * sign
    utc_hour = hour - offset_hour * sign - (1 if utc_minute < 0 else 0)
    return utc_hour in {23, -1} and utc_minute in {59, -1} and second < 61

# ASCII word boundaries match JavaScript RegExp's \b (not Python Unicode \w).
_CAUSAL = re.compile(
    r"(可排除|已经排除|直接导致|证明了|确定(?:的)?原因|直接来源|必然导致|"
    r"\bcaused by\b|\bproves?\b|\brules? out\b|\bdefinitely\b)", re.IGNORECASE | re.ASCII,
)
_DISCLOSURE = re.compile(
    r"(?:</?think>|chain[- ]of[- ]thought|system prompt|tool call|"
    r"思考过程\s*[:：]|内部分析\s*[:：]|推理过程\s*[:：])", re.IGNORECASE,
)


def validate_artifact_semantics(name: str, instance: dict[str, Any]) -> None:
    payload = instance["payload"]
    texts: list[str] = []
    if name == "query_result_artifact":
        if payload["row_count"] != len(payload["rows"]):
            raise ValidationError("row_count must equal persisted rows length")
        if any(len(row) != len(payload["columns"]) for row in payload["rows"]):
            raise ValidationError("every result row must match the columns length")
    elif name == "analysis_artifact":
        method = payload["method_summary"]
        visible = [method["objective"], method["comparison_baseline"],
                   *method["dimensions"], *method["approach_steps"], payload["summary"],
                   *(item["statement"] for item in payload["findings"]),
                   *(item["statement"] for item in payload["hypotheses"]), *payload["limitations"]]
        if any(_DISCLOSURE.search(text) for text in visible):
            raise ValidationError("analysis contains hidden reasoning, prompt, or tool transcript disclosure")
        if payload["status"] == "incomplete" and not payload["suggested_queries"]:
            raise ValidationError("incomplete analysis requires at least one suggested query")
        texts = [payload["summary"], *(item["statement"] for item in payload["findings"])]
    elif name == "advisory_artifact":
        if payload["status"] == "incomplete" and not payload["open_questions"]:
            raise ValidationError("incomplete advisory requires at least one open question")
        texts = [payload["summary"], *(item["statement"] for item in payload["findings"])]
    elif name == "rendered_output_artifact":
        texts = [payload["executive_summary"],
                 *(text for item in payload["key_findings"]
                   for text in (item["statement"], item["interpretation"]))]
    if any(_CAUSAL.search(text) for text in texts):
        raise ValidationError("artifact uses unsupported causal certainty")
