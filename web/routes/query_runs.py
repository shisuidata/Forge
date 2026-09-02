"""Internal QueryRun API used only by the authenticated Pi control plane."""
from __future__ import annotations

import re
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from forge.query_runs import (
    QueryRunError,
    approve_and_execute_query_run,
    cancel_query_run,
    create_query_run,
    get_query_run,
)
from web.auth import require_pi_service_auth

router = APIRouter(
    prefix="/api/internal/query-runs",
    dependencies=[Depends(require_pi_service_auth)],
)

_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_DIALECTS = {"auto", "sqlite", "postgresql", "mysql", "bigquery", "snowflake"}


class CreateQueryRunRequest(BaseModel):
    task_run_id: str
    org_id: str
    team_id: str
    user_id: str
    question: str
    dialect: Optional[str] = None
    candidate: Optional[dict[str, Any]] = None


class ApproveQueryRunRequest(BaseModel):
    approver_user_id: str
    sql_hash: str
    assurance_report_hash: str


class CancelQueryRunRequest(BaseModel):
    user_id: str


def _error(exc: QueryRunError) -> JSONResponse:
    return JSONResponse(
        {"status": "error", "error": str(exc)},
        status_code=exc.status_code,
    )


def _validate_create_request(req: CreateQueryRunRequest) -> QueryRunError | None:
    for name, value in (
        ("task_run_id", req.task_run_id),
        ("org_id", req.org_id),
        ("team_id", req.team_id),
        ("user_id", req.user_id),
    ):
        if _ID_PATTERN.fullmatch(value) is None:
            return QueryRunError(f"Invalid {name}", status_code=400)
    if not req.question.strip():
        return QueryRunError("question must not be empty", status_code=400)
    if req.dialect is not None and req.dialect not in _DIALECTS:
        return QueryRunError("Unsupported dialect", status_code=400)
    return None


def _review_payload(run: dict) -> dict:
    return {
        "query_run_id": run["query_run_id"],
        "task_run_id": run["task_run_id"],
        "status": run["status"],
        "question": run["question"],
        "user_id": run["user_id"],
        "datasource_id": run["datasource_id"],
        "input_kind": run["input_kind"],
        "candidate_revision": run["candidate_revision"],
        "forge_json": run["forge_json"],
        "sql": run["sql"],
        "sql_hash": run["sql_hash"],
        "dialect": run["dialect"],
        "registry_version": run["registry_version"],
        "assurance_report": run["assurance_report"],
        "assurance_report_hash": run["assurance_report_hash"],
        "assurance_revision": run["assurance_revision"],
        "policy_revision": run["policy_revision"],
        "model_revision": run["model_revision"],
        "assurance_registry_revision": run["assurance_registry_revision"],
        "review_required": run["status"] == "needs_review",
        "can_execute": False,
        "expires_at": run["expires_at"],
        "error": run["error"] or "",
    }


def _result_payload(run: dict) -> dict:
    return {
        "query_run_id": run["query_run_id"],
        "task_run_id": run["task_run_id"],
        "status": run["status"],
        "sql_hash": run["sql_hash"],
        "input_kind": run["input_kind"],
        "candidate_revision": run["candidate_revision"],
        "dialect": run["dialect"],
        "registry_version": run["registry_version"],
        "assurance_report_hash": run["assurance_report_hash"],
        "assurance_revision": run["assurance_revision"],
        "policy_revision": run["policy_revision"],
        "model_revision": run["model_revision"],
        "assurance_registry_revision": run["assurance_registry_revision"],
        "columns": run["result_columns"] or [],
        "rows": run["result_rows"] or [],
        "row_count": run["row_count"],
        "truncated": run["truncated"],
        "execution_ms": run["execution_ms"],
        "executed_at": run["updated_at"],
        "error": run["error"] or "",
    }


@router.post("")
async def create_internal_query_run(
    req: CreateQueryRunRequest,
    idempotency_key: str = Header(default="", alias="Idempotency-Key"),
):
    invalid = _validate_create_request(req)
    if invalid:
        return _error(invalid)
    try:
        run = await create_query_run(
            task_run_id=req.task_run_id,
            org_id=req.org_id,
            team_id=req.team_id,
            user_id=req.user_id,
            question=req.question,
            dialect=req.dialect,
            idempotency_key=idempotency_key,
            candidate=req.candidate,
        )
        return _review_payload(run)
    except QueryRunError as exc:
        return _error(exc)


@router.get("/{query_run_id}")
async def get_internal_query_run(query_run_id: str):
    run = await get_query_run(query_run_id)
    if run is None:
        return _error(QueryRunError("QueryRun not found", status_code=404))
    return _review_payload(run)


@router.post("/{query_run_id}/approve")
async def approve_internal_query_run(
    query_run_id: str,
    req: ApproveQueryRunRequest,
    idempotency_key: str = Header(default="", alias="Idempotency-Key"),
):
    try:
        run = await approve_and_execute_query_run(
            query_run_id=query_run_id,
            approver_user_id=req.approver_user_id,
            sql_hash=req.sql_hash,
            assurance_report_hash=req.assurance_report_hash,
            idempotency_key=idempotency_key,
        )
        return _result_payload(run)
    except QueryRunError as exc:
        return _error(exc)


@router.post("/{query_run_id}/cancel")
async def cancel_internal_query_run(
    query_run_id: str,
    req: CancelQueryRunRequest,
):
    try:
        run = await cancel_query_run(query_run_id, req.user_id)
        return _review_payload(run)
    except QueryRunError as exc:
        return _error(exc)


@router.get("/{query_run_id}/result")
async def get_internal_query_result(query_run_id: str):
    run = await get_query_run(query_run_id)
    if run is None:
        return _error(QueryRunError("QueryRun not found", status_code=404))
    if run["status"] != "completed":
        return _error(QueryRunError(f"QueryRun result is not ready: {run['status']}"))
    return _result_payload(run)
