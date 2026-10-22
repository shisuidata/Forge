"""Public governed QueryRun API; v1 is historical read-only audit."""
from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, Header
from fastapi.responses import JSONResponse
from jsonschema import ValidationError
from pydantic import BaseModel, ConfigDict, Field

from agent.contracts import validate_contract
from forge.enforce import EnforceContextError, project_enforce_query_run
from forge.governance import AuthenticatedPrincipal, GovernanceError
from forge.query_runs import (
    QueryRunError,
    cancel_governed_query_run,
    create_governed_query_run,
    execute_authorized_query_run,
    get_governed_query_run,
    get_query_run,
)
from web.auth import require_admin_principal, require_runtime_principal

router = APIRouter(tags=["Enforce"])
legacy_router = APIRouter(prefix="/api/v1/enforce", tags=["Enforce"])


class EnforceQueryRequestV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[2]
    mandate_id: str = Field(pattern=r"^md_[A-Za-z0-9_-]+$")
    task_run_id: str = Field(pattern=r"^tr_[A-Za-z0-9_-]+$", max_length=128)
    purpose: str = Field(min_length=1, max_length=1000)
    question: str = Field(min_length=1, max_length=10_000)
    candidate: dict[str, Any]
    dialect: Literal["auto", "sqlite", "postgresql", "mysql", "bigquery", "snowflake"]


class EnforceQueryActionV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[2]
    sql_hash: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")
    assurance_report_hash: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")
    enforcement_context_hash: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")


def _error(exc: EnforceContextError | QueryRunError | GovernanceError) -> JSONResponse:
    return JSONResponse(
        {"schema_version": 2, "status": "error", "failure": {
            "stage": "enforce", "code": exc.code,
            "retryable": exc.status_code >= 500 or exc.code in {
                "governance_drift", "registry_drift", "review_expired", "execution_unavailable"
            },
        }},
        status_code=exc.status_code,
    )


def _invalid(code: str) -> JSONResponse:
    return _error(QueryRunError("Request contract invalid", status_code=422, code=code))


@router.post("/api/v2/enforce/query-runs")
async def create_enforced_query_run(
    req: EnforceQueryRequestV2,
    idempotency_key: str = Header(default="", alias="Idempotency-Key"),
    actor: AuthenticatedPrincipal = Depends(require_runtime_principal),
):
    try:
        validate_contract("enforce_query_request_v2", req.model_dump())
    except ValidationError:
        return _invalid("enforce_request_invalid")
    try:
        run = await create_governed_query_run(
            actor=actor, mandate_id=req.mandate_id, task_run_id=req.task_run_id,
            purpose=req.purpose, question=req.question, dialect=req.dialect,
            candidate=req.candidate, idempotency_key=idempotency_key,
        )
        return project_enforce_query_run(run)
    except ValidationError:
        return _error(QueryRunError("Stored response contract invalid", code="enforcement_response_invalid"))
    except (EnforceContextError, QueryRunError, GovernanceError) as exc:
        return _error(exc)


@router.get("/api/v2/enforce/query-runs/{query_run_id}")
async def get_enforced_query_run(
    query_run_id: str,
    actor: AuthenticatedPrincipal = Depends(require_runtime_principal),
):
    try:
        run = await get_governed_query_run(query_run_id, actor)
        return project_enforce_query_run(run)
    except ValidationError:
        return _error(QueryRunError("Stored response contract invalid", code="enforcement_response_invalid"))
    except (EnforceContextError, QueryRunError, GovernanceError) as exc:
        return _error(exc)


async def _claim(
    *, query_run_id: str, req: EnforceQueryActionV2, action: str,
    idempotency_key: str, actor: AuthenticatedPrincipal,
    control_token: str | None = None,
):
    try:
        validate_contract("enforce_query_action_v2", req.model_dump())
    except ValidationError:
        return _invalid("enforce_action_invalid")
    try:
        if not idempotency_key:
            return _error(QueryRunError("Idempotency-Key required", status_code=400, code="idempotency_key_required"))
        if action == "approve" and actor.principal_type != "human" and not control_token:
            return _error(QueryRunError("Reviewer credential or Pi control token required", status_code=403, code="reviewer_not_authorized"))
        run = await execute_authorized_query_run(
            query_run_id=query_run_id, actor=actor, action=action,
            hashes={"sql_hash": req.sql_hash, "assurance_report_hash": req.assurance_report_hash,
                    "enforcement_context_hash": req.enforcement_context_hash},
            idempotency_key=idempotency_key, control_token=control_token,
        )
        return JSONResponse(project_enforce_query_run(run), status_code=200 if run["_claim_replayed"] else 202)
    except ValidationError:
        return _error(QueryRunError("Stored response contract invalid", code="enforcement_response_invalid"))
    except (EnforceContextError, QueryRunError, GovernanceError) as exc:
        return _error(exc)

@router.post("/api/v2/enforce/query-runs/{query_run_id}/execute")
async def execute_enforced_query_run(
    query_run_id: str, req: EnforceQueryActionV2,
    idempotency_key: str = Header(default="", alias="Idempotency-Key"),
    actor: AuthenticatedPrincipal = Depends(require_runtime_principal),
):
    return await _claim(query_run_id=query_run_id, req=req, action="execute",
                        idempotency_key=idempotency_key, actor=actor)


@router.post("/api/v2/enforce/query-runs/{query_run_id}/approve")
async def approve_enforced_query_run(
    query_run_id: str, req: EnforceQueryActionV2,
    idempotency_key: str = Header(default="", alias="Idempotency-Key"),
    control_token: str | None = Header(default=None, alias="X-Forge-Control-Token"),
    actor: AuthenticatedPrincipal = Depends(require_runtime_principal),
):
    return await _claim(query_run_id=query_run_id, req=req, action="approve",
                        idempotency_key=idempotency_key, actor=actor, control_token=control_token)


@router.post("/api/v2/enforce/query-runs/{query_run_id}/cancel")
async def cancel_enforced_query_run(
    query_run_id: str,
    actor: AuthenticatedPrincipal = Depends(require_runtime_principal),
):
    try:
        run = await cancel_governed_query_run(query_run_id, actor)
        return project_enforce_query_run(run)
    except (EnforceContextError, QueryRunError, GovernanceError) as exc:
        return _error(exc)


@legacy_router.post("/query-runs")
@legacy_router.post("/query-runs/{query_run_id}/approve")
async def legacy_enforce_write(
    actor: AuthenticatedPrincipal = Depends(require_runtime_principal),
    query_run_id: str | None = None,
):
    return _error(QueryRunError("Governed API v2 required", status_code=410, code="governed_api_v2_required"))


@legacy_router.get("/query-runs/{query_run_id}")
async def get_historical_enforced_query_run(
    query_run_id: str,
    actor: AuthenticatedPrincipal = Depends(require_admin_principal),
):
    try:
        run = await get_query_run(query_run_id)
        if run is None or run.get("enforce_schema_version") != 1 or (
            run["org_id"], run["workspace_id"]
        ) != (actor.organization_id, actor.workspace_id):
            raise QueryRunError("QueryRun not found", status_code=404, code="query_run_not_found")
        return project_enforce_query_run(run)
    except (EnforceContextError, QueryRunError, GovernanceError) as exc:
        return _error(exc)


router.include_router(legacy_router)
