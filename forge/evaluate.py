"""Versioned, non-executing evaluation for external query candidates."""
from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from typing import Any

from jsonschema import ValidationError

from agent.contracts import validate_contract
from forge.assurance import (
    ASSURANCE_REVISION,
    POLICY_REVISION,
    QUERY_CANDIDATE_REVISION,
    QueryAssuranceError,
    QueryAssuranceReport,
    assure_direct_sql,
    assure_query,
)
from forge.benchmark_v2 import build_result_contract, semantic_result_compare

EVALUATE_SCHEMA_VERSION = 1
EVALUATOR_REVISION = "evaluate-v1"
RESULT_COMPARATOR_REVISION = "semantic-result-compare-v1"
SUPPORTED_DIALECTS = {"auto", "sqlite", "postgresql", "mysql", "bigquery", "snowflake"}


def canonical_hash(value: Any) -> str:
    canonical = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _failure(stage: str, code: str, *, retryable: bool) -> dict[str, Any]:
    failure = {"stage": stage, "code": code, "retryable": retryable}
    validate_contract("benchmark_failure_v1", failure)
    return failure


def _safe_assurance(report: QueryAssuranceReport) -> dict[str, Any]:
    return {
        "status": report.status,
        "assurance_revision": report.assurance_revision,
        "policy_revision": report.policy_revision,
        "registry_revision": report.registry_revision,
        "producer_revision": report.model_revision,
        "input_kind": report.input_kind,
        "candidate_revision": report.candidate_revision,
        "sql_hash": report.sql_hash,
        "gates": [
            {
                "gate": gate.gate,
                "status": gate.status,
                "revision": gate.revision,
            }
            for gate in report.gates
        ],
    }


def _classify_assurance_failure(exc: QueryAssuranceError) -> tuple[str, str, bool]:
    gate = exc.report.gates[-1].gate if exc.report.gates else ""
    if gate == "sql_safety":
        return "assurance", "readonly_violation", False
    if gate == "sql_parse":
        return "assurance", "sql_parse_failed", True
    if gate in {"registry_acl", "registry_acl_alias"}:
        return "assurance", "unknown_schema_reference", True
    if gate == "scope_type_compile":
        return "compile", "compile_failed", True
    if gate == "contract_scope_type":
        return "candidate_contract", "candidate_contract_invalid", True
    return "assurance", "candidate_contract_invalid", True


def _result_comparison(request: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
    expected = request.get("expected_result")
    actual = request.get("actual_result")
    if expected is None and actual is None:
        return {
            "status": "not_requested",
            "correct": None,
            "verdict": "not_requested",
            "column_mapping": None,
            "contract_revision": None,
        }, None

    contract = build_result_contract(request["question"])
    expected_columns = expected["columns"]
    actual_columns = actual["columns"]
    expected_rows = [tuple(row) for row in expected["rows"]]
    actual_rows = [tuple(row) for row in actual["rows"]]

    malformed_width = (
        len(expected_columns) != len(actual_columns)
        or any(len(row) != len(expected_columns) for row in expected_rows)
        or any(len(row) != len(actual_columns) for row in actual_rows)
    )
    if malformed_width:
        verdict = {
            "correct": False,
            "verdict": "column_count_mismatch",
            "column_mapping": None,
            "failure_code": "result_column_count_mismatch",
        }
    elif (
        len(set(expected_columns)) == len(expected_columns)
        and set(expected_columns) == set(actual_columns)
    ):
        column_mapping = tuple(actual_columns.index(name) for name in expected_columns)
        aligned_actual_rows = [
            tuple(row[index] for index in column_mapping) for row in actual_rows
        ]
        verdict = semantic_result_compare(
            expected_rows,
            aligned_actual_rows,
            replace(contract, column_order_significant=True),
        )
        verdict["column_mapping"] = column_mapping
    else:
        verdict = semantic_result_compare(expected_rows, actual_rows, contract)

    comparison = {
        "status": "passed" if verdict["correct"] else "failed",
        "correct": verdict["correct"],
        "verdict": verdict["verdict"],
        "column_mapping": list(verdict["column_mapping"])
        if verdict["column_mapping"] is not None
        else None,
        "contract_revision": contract.revision,
    }
    failure = None
    if not verdict["correct"]:
        failure = _failure(
            "result_contract",
            verdict["failure_code"],
            retryable=True,
        )
    return comparison, failure


def _finalize(
    *,
    request: dict[str, Any],
    status: str,
    input_kind: str,
    producer_revision: str,
    policy_verdict: str,
    compiled_sql: str | None,
    assurance_report: QueryAssuranceReport | None,
    result_comparison: dict[str, Any],
    failure: dict[str, Any] | None,
) -> dict[str, Any]:
    request_hash = canonical_hash(request)
    full_assurance = assurance_report.to_dict() if assurance_report is not None else None
    assurance_hash = canonical_hash(full_assurance) if full_assurance is not None else None
    safe_assurance = _safe_assurance(assurance_report) if assurance_report is not None else None
    sql_hash = assurance_report.sql_hash if assurance_report is not None else None

    payload = {
        "schema_version": EVALUATE_SCHEMA_VERSION,
        "status": status,
        "candidate": {
            "input_kind": input_kind,
            "candidate_revision": QUERY_CANDIDATE_REVISION,
            "producer_revision": producer_revision,
        },
        "policy": {
            "verdict": policy_verdict,
            "review_required": policy_verdict == "allow_review",
            "execution_authorized": False,
        },
        "failure": failure,
        "result_comparison": result_comparison,
        "compiled_sql": compiled_sql,
        "assurance": safe_assurance,
        "lineage": {
            "request_hash": request_hash,
            "candidate_revision": QUERY_CANDIDATE_REVISION,
            "producer_revision": producer_revision,
            "assurance_revision": assurance_report.assurance_revision
            if assurance_report is not None
            else ASSURANCE_REVISION,
            "policy_revision": assurance_report.policy_revision
            if assurance_report is not None
            else POLICY_REVISION,
            "registry_revision": assurance_report.registry_revision
            if assurance_report is not None
            else None,
            "assurance_report_hash": assurance_hash,
            "sql_hash": sql_hash,
            "result_contract_revision": result_comparison["contract_revision"],
        },
    }
    evaluation_hash = canonical_hash(payload)
    evaluation_id = "ev_" + evaluation_hash.removeprefix("sha256:")[:24]
    refs = [f"{evaluation_id}#candidate"]
    if assurance_report is not None:
        refs.append(f"{evaluation_id}#assurance")
    if result_comparison["status"] not in {"not_requested", "not_run"}:
        refs.append(f"{evaluation_id}#result-comparison")
    return {
        "evaluation_id": evaluation_id,
        **payload,
        "evidence_refs": refs,
    }


def evaluate_query_candidate(request: dict[str, Any]) -> dict[str, Any]:
    """Evaluate a candidate without executing it or granting execution authority."""
    candidate = request.get("candidate")
    raw_input_kind = candidate.get("kind") if isinstance(candidate, dict) else None
    input_kind = raw_input_kind if raw_input_kind in {"direct_sql", "forge_json"} else "unknown"
    producer_revision = (
        str(candidate.get("producer_revision") or "external")
        if isinstance(candidate, dict)
        else "external"
    )
    not_run = {
        "status": "not_run",
        "correct": None,
        "verdict": "not_run",
        "column_mapping": None,
        "contract_revision": None,
    }

    try:
        validate_contract("query_candidate_v1", candidate)
    except (ValidationError, TypeError):
        return _finalize(
            request=request,
            status="failed",
            input_kind=input_kind,
            producer_revision=producer_revision,
            policy_verdict="deny",
            compiled_sql=None,
            assurance_report=None,
            result_comparison=not_run,
            failure=_failure(
                "candidate_contract",
                "candidate_contract_invalid",
                retryable=True,
            ),
        )

    dialect = str(request.get("dialect") or "auto")
    if dialect not in SUPPORTED_DIALECTS:
        return _finalize(
            request=request,
            status="failed",
            input_kind=input_kind,
            producer_revision=producer_revision,
            policy_verdict="deny",
            compiled_sql=None,
            assurance_report=None,
            result_comparison=not_run,
            failure=_failure("assurance", "dialect_unsupported", retryable=True),
        )

    try:
        if input_kind == "direct_sql":
            report = assure_direct_sql(
                candidate["sql"],
                dialect=dialect,
                allowed_tables=request.get("allowed_tables"),
                producer_revision=producer_revision,
            )
        else:
            report = assure_query(
                candidate["forge_json"],
                request["question"],
                dialect=dialect,
                allowed_tables=request.get("allowed_tables"),
                model_revision=producer_revision,
            )
    except QueryAssuranceError as exc:
        stage, code, retryable = _classify_assurance_failure(exc)
        return _finalize(
            request=request,
            status="failed",
            input_kind=input_kind,
            producer_revision=producer_revision,
            policy_verdict="deny",
            compiled_sql=exc.report.sql,
            assurance_report=exc.report,
            result_comparison=not_run,
            failure=_failure(stage, code, retryable=retryable),
        )

    comparison, comparison_failure = _result_comparison(request)
    return _finalize(
        request=request,
        status="failed" if comparison_failure else "passed",
        input_kind=input_kind,
        producer_revision=producer_revision,
        policy_verdict="allow_review",
        compiled_sql=report.sql,
        assurance_report=report,
        result_comparison=comparison,
        failure=comparison_failure,
    )
