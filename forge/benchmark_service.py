"""Offline benchmark business services, independent of HTTP adapters."""
from __future__ import annotations
from dataclasses import dataclass
import json
from typing import Any

from jsonschema import ValidationError
from agent.contracts import validate_contract
from agent.prompts import (
    STRUCTURED_BENCHMARK_PROMPT_REVISION,
    build_structured_benchmark_system,
)
from forge.assurance import QueryAssuranceError, assure_compiled_sql
from forge.benchmark_v2 import RESULT_COMPARATOR_REVISION
from forge.benchmark_v2 import ResultContract, build_context_snapshot, semantic_result_compare, snapshot_dict
from forge.compiler import compile_query
from forge.hard_accuracy_benchmark import (
    _clean_sql,
    _compare_results,
    _database_path,
    _direct_system,
    _forge_context,
    _json_safe,
    execute_result,
    structure_projection,
    structure_prompt,
)

@dataclass(frozen=True)
class CandidateEvaluation:
    case_id: str
    protocol_revision: str
    metric_revision: str
    arm: str
    output: Any
    context_snapshot: dict[str, Any]

class BenchmarkCaseNotFound(ValueError):
    pass

def _case(suite: dict[str, Any], case_id: str) -> dict[str, Any]:
    for item in suite["cases"]:
        if item["case_id"] == case_id:
            return item
    raise BenchmarkCaseNotFound("Benchmark case not found")

def _failure(stage: str, code: str, *, retryable: bool) -> dict[str, Any]:
    failure = {"stage": stage, "code": code, "retryable": retryable}
    validate_contract("benchmark_failure_v1", failure)
    return failure


def _failed_evaluation(
    req: CandidateEvaluation,
    *,
    compile_status: str | None,
    execution_status: str,
    stage: str,
    code: str,
    retryable: bool,
    sql: str | None = None,
    forge_json: dict[str, Any] | None = None,
    assurance: dict[str, Any] | None = None,
    scored: bool = True,
) -> dict[str, Any]:
    failure = _failure(stage, code, retryable=retryable)
    return {
        "arm": req.arm,
        "metric_revision": RESULT_COMPARATOR_REVISION,
        "compile_status": compile_status,
        "execution_status": execution_status,
        "scored": scored,
        "official_ea": False if scored else None,
        "contract_accuracy": False if scored else None,
        "failure": failure,
        "error_code": code,
        "sql": sql,
        "forge_json": forge_json,
        "assurance": assurance,
    }


def _registry_snapshot(structure: dict[str, Any]) -> dict[str, Any]:
    return {
        "tables": {
            str(table["name"]): {
                "columns": {
                    str(column["name"]): {}
                    for column in table.get("columns", [])
                }
            }
            for table in structure.get("tables", [])
        }
    }


def _assurance_failure_code(exc: QueryAssuranceError) -> str:
    gate = exc.report.gates[-1].gate if exc.report.gates else ""
    return {
        "sql_safety": "readonly_violation",
        "sql_parse": "sql_parse_failed",
        "registry_acl": "unknown_schema_reference",
    }.get(gate, "execution_failed")


def _execution_failure_code(exc: Exception) -> str:
    message = str(exc).lower()
    if "no such table" in message:
        return "unknown_table"
    if "no such column" in message:
        return "unknown_column"
    if "no such function" in message:
        return "dialect_unsupported"
    if "interrupted" in message or "timeout" in message or "timed out" in message:
        return "execution_timeout"
    return "execution_failed"


def check_gold_readiness(suite: dict[str, Any], case_ids: list[str]) -> list[dict[str, str]]:
    """Execute all selected Gold and return only answer-safe failure identities."""
    cases = {case["case_id"]: case for case in suite["cases"]}
    if any(case_id not in cases for case_id in case_ids):
        raise ValueError("Gold readiness selection contains an unknown case")
    failures = []
    for case_id in case_ids:
        case = cases[case_id]
        try:
            execute_result(_database_path(case["db_id"]), str(case["SQL"]))
        except Exception as exc:
            failures.append({
                "case_id": case_id,
                "db_id": case["db_id"],
                "code": _execution_failure_code(exc),
            })
    return failures

def build_context_response(
    suite: dict[str, Any], case: dict[str, Any], *,
    forge_prompt_revision: str = STRUCTURED_BENCHMARK_PROMPT_REVISION,
):
    structure = structure_projection(suite["tables"][case["db_id"]])
    snapshot = build_context_snapshot(case["question"], case["evidence"], structure)
    selected = set(snapshot.tables)
    filtered = {
        **structure,
        "tables": [table for table in structure["tables"] if table["name"] in selected],
        "relationships": [
            relation for relation in structure["relationships"]
            if relation["from"].split(".", 1)[0] in selected
            and relation["to"].split(".", 1)[0] in selected
        ],
    }
    schema_context = structure_prompt(filtered)
    return {
        "metric_revision": RESULT_COMPARATOR_REVISION,
        "case": {
            "case_id": case["case_id"],
            "question_id": case["question_id"],
            "db_id": case["db_id"],
            "difficulty": case["difficulty"],
            "question": case["question"],
            "evidence": case["evidence"],
        },
        "context_snapshot": snapshot_dict(snapshot),
        "schema_context": schema_context,
        "forge_prompt_revision": forge_prompt_revision,
        "forge_instructions": build_structured_benchmark_system(
            _forge_context(schema_context, case["evidence"]), prompt_revision=forge_prompt_revision,
        ),
        "direct_instructions": _direct_system(schema_context, case["evidence"]),
    }

def evaluate_candidate(req: CandidateEvaluation, suite: dict[str, Any]):
    """Shared offline/HTTP compiler → assurance → execution → EX/Contract path."""
    case = _case(suite, req.case_id)
    structure = structure_projection(suite["tables"][case["db_id"]])
    expected_context = build_context_snapshot(case["question"], case["evidence"], structure)
    if json.dumps(req.context_snapshot, sort_keys=True) != json.dumps(snapshot_dict(expected_context), sort_keys=True):
        raise ValueError("ContextSnapshot hash mismatch")
    contract = ResultContract(**req.context_snapshot["result_contract"])
    forge_json: dict[str, Any] | None = None
    sql: str | None = None

    raw_output = str(req.output or "").strip()
    if not raw_output:
        return _failed_evaluation(
            req,
            compile_status="failed" if req.arm == "forge" else "not_applicable",
            execution_status="skipped",
            stage="generation",
            code="generation_empty",
            retryable=True,
        )

    if req.arm == "forge":
        try:
            parsed_output = req.output if isinstance(req.output, dict) else json.loads(raw_output)
        except (json.JSONDecodeError, TypeError):
            return _failed_evaluation(
                req,
                compile_status="failed",
                execution_status="skipped",
                stage="parse",
                code="malformed_output",
                retryable=True,
            )
        forge_json = parsed_output if isinstance(parsed_output, dict) else None
        candidate = {
            "kind": "forge_json",
            "forge_json": parsed_output,
            "producer_revision": "benchmark-v2",
        }
        compile_status = "failed"
    else:
        sql = _clean_sql(raw_output)
        candidate = {
            "kind": "direct_sql",
            "sql": sql,
            "producer_revision": "benchmark-v2",
        }
        compile_status = "not_applicable"

    try:
        validate_contract("query_candidate_v1", candidate)
    except ValidationError:
        return _failed_evaluation(
            req,
            compile_status=compile_status,
            execution_status="skipped",
            stage="candidate_contract",
            code="candidate_contract_invalid",
            retryable=True,
            sql=sql,
            forge_json=forge_json,
        )

    if req.arm == "forge":
        try:
            sql = compile_query(forge_json)
            compile_status = "passed"
        except Exception:
            return _failed_evaluation(
                req,
                compile_status="failed",
                execution_status="skipped",
                stage="compile",
                code="compile_failed",
                retryable=True,
                forge_json=forge_json,
            )

    try:
        assurance_report = assure_compiled_sql(
            sql,
            dialect="sqlite",
            input_kind=candidate["kind"],
            allowed_tables=list(expected_context.tables),
            producer_revision=candidate["producer_revision"],
            registry_snapshot=_registry_snapshot(structure),
        )
    except QueryAssuranceError as exc:
        code = _assurance_failure_code(exc)
        return _failed_evaluation(
            req,
            compile_status=compile_status,
            execution_status="skipped",
            stage="assurance",
            code=code,
            retryable=code != "readonly_violation",
            sql=sql,
            forge_json=forge_json,
            assurance=exc.report.to_dict(),
        )

    try:
        predicted_rows, preview = execute_result(_database_path(case["db_id"]), sql)
    except Exception as exc:
        code = _execution_failure_code(exc)
        return _failed_evaluation(
            req,
            compile_status=compile_status,
            execution_status="failed",
            stage="execution",
            code=code,
            retryable=code != "execution_failed",
            sql=sql,
            forge_json=forge_json,
            assurance=assurance_report.to_dict(),
        )

    result_preview = {
        "columns": preview["columns"],
        "rows": [[_json_safe(value) for value in row] for row in predicted_rows[:20]],
        "row_count": len(predicted_rows),
        "truncated": len(predicted_rows) > 20,
    }
    # Gold is scoring-only; execute read-only instead of trusting historical caches.
    try:
        gold_rows, _ = execute_result(_database_path(case["db_id"]), str(case["SQL"]))
    except Exception:
        return {
            **_failed_evaluation(
                req, compile_status=compile_status, execution_status="passed",
                stage="gold", code="gold_execution_failed", retryable=False,
                sql=sql, forge_json=forge_json, assurance=assurance_report.to_dict(),
                scored=False,
            ),
            "result": result_preview,
        }
    official_ea = _compare_results(predicted_rows, gold_rows)
    semantic = semantic_result_compare(gold_rows, predicted_rows, contract)
    if not semantic["correct"]:
        failure = _failure("result_contract", semantic["failure_code"], retryable=True)
    elif not official_ea:
        failure = _failure("official_ea", "official_ea_mismatch", retryable=True)
    else:
        failure = None
    return {
        "arm": req.arm,
        "metric_revision": RESULT_COMPARATOR_REVISION,
        "compile_status": compile_status,
        "execution_status": "passed",
        "scored": True,
        "official_ea": official_ea,
        "contract_accuracy": semantic["correct"],
        "semantic_verdict": semantic["verdict"],
        "column_mapping": semantic["column_mapping"],
        "failure": failure,
        "error_code": failure["code"] if failure else None,
        "sql": sql,
        "forge_json": forge_json,
        "assurance": assurance_report.to_dict(),
        "result": result_preview,
    }