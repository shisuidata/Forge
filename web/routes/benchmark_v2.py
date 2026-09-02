"""Internal benchmark-v2 API for the authenticated Pi orchestrator."""
from __future__ import annotations

import json
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from agent.prompts import build_system
from forge.benchmark_v2 import ResultContract, build_context_snapshot, semantic_result_compare, snapshot_dict
from forge.compiler import compile_query
from forge.executor import validate_readonly_sql
from forge.hard_accuracy_benchmark import (
    _FULL_SUITE_ID,
    _clean_sql,
    _compare_results,
    _database_path,
    _direct_system,
    _forge_context,
    _json_safe,
    execute_result,
    get_hard_benchmark_service,
    load_suite,
    structure_projection,
    structure_prompt,
    validate_gold_cases,
)
from web.auth import require_pi_service_auth

router = APIRouter(
    prefix="/api/internal/benchmark-v2",
    dependencies=[Depends(require_pi_service_auth)],
)


class ContextRequest(BaseModel):
    case_id: str


class EvaluateRequest(BaseModel):
    case_id: str
    arm: Literal["forge", "direct"]
    output: Any
    context_snapshot: dict[str, Any]


def _suite():
    return load_suite(_FULL_SUITE_ID)


def _case(suite: dict[str, Any], case_id: str) -> dict[str, Any]:
    for item in suite["cases"]:
        if item["case_id"] == case_id:
            return item
    raise HTTPException(status_code=404, detail="Benchmark case not found")


@router.get("/suite")
def suite_projection():
    suite = _suite()
    return {
        "suite": suite["manifest"],
        "cases": [
            {
                "case_id": case["case_id"],
                "question_id": case["question_id"],
                "db_id": case["db_id"],
                "difficulty": case["difficulty"],
                "question": case["question"],
                "evidence": case["evidence"],
            }
            for case in suite["cases"]
        ],
    }


@router.post("/context")
def context_projection(req: ContextRequest):
    suite = _suite()
    case = _case(suite, req.case_id)
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
        "forge_instructions": build_system(
            _forge_context(schema_context, case["evidence"]),
            question=case["question"],
            mode="benchmark",
        ),
        "direct_instructions": _direct_system(schema_context, case["evidence"]),
    }


@router.post("/evaluate")
def evaluate_arm(req: EvaluateRequest):
    suite = _suite()
    case = _case(suite, req.case_id)
    expected_context = build_context_snapshot(
        case["question"],
        case["evidence"],
        structure_projection(suite["tables"][case["db_id"]]),
    )
    if req.context_snapshot.get("content_hash") != expected_context.content_hash:
        raise HTTPException(status_code=409, detail="ContextSnapshot hash mismatch")
    contract = ResultContract(**req.context_snapshot["result_contract"])
    forge_json = None
    compile_status: str | None = None
    if req.arm == "forge":
        try:
            forge_json = req.output if isinstance(req.output, dict) else json.loads(str(req.output))
            sql = compile_query(forge_json)
            compile_status = "passed"
        except Exception as exc:
            return {
                "arm": req.arm,
                "compile_status": "failed",
                "execution_status": "skipped",
                "official_ea": False,
                "contract_accuracy": False,
                "error_code": "compile_failed",
                "error": type(exc).__name__,
                "sql": None,
                "forge_json": forge_json,
            }
    else:
        sql = _clean_sql(str(req.output or ""))
        compile_status = "not_applicable"
    if not sql:
        return {
            "arm": req.arm,
            "compile_status": compile_status,
            "execution_status": "skipped",
            "official_ea": False,
            "contract_accuracy": False,
            "error_code": "generation_failed",
            "error": "empty output",
            "sql": None,
            "forge_json": forge_json,
        }
    try:
        validate_readonly_sql(sql)
        predicted_rows, preview = execute_result(_database_path(case["db_id"]), sql)
    except Exception as exc:
        return {
            "arm": req.arm,
            "compile_status": compile_status,
            "execution_status": "failed",
            "official_ea": False,
            "contract_accuracy": False,
            "error_code": "execution_failed",
            "error": type(exc).__name__,
            "sql": sql,
            "forge_json": forge_json,
        }
    answers = validate_gold_cases(suite)
    gold_rows = answers[case["case_id"]]["rows"]
    official_ea = _compare_results(predicted_rows, gold_rows)
    semantic = semantic_result_compare(gold_rows, predicted_rows, contract)
    return {
        "arm": req.arm,
        "compile_status": compile_status,
        "execution_status": "passed",
        "official_ea": official_ea,
        "contract_accuracy": semantic["correct"],
        "semantic_verdict": semantic["verdict"],
        "column_mapping": semantic["column_mapping"],
        "error_code": None if semantic["correct"] else "incorrect_result",
        "error": None,
        "sql": sql,
        "forge_json": forge_json,
        "result": {
            "columns": preview["columns"],
            "rows": [[_json_safe(value) for value in row] for row in predicted_rows[:20]],
            "row_count": len(predicted_rows),
            "truncated": len(predicted_rows) > 20,
        },
    }
