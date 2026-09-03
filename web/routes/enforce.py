"""Public, versioned Enforce API backed by the existing QueryRun truth source."""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Literal

from fastapi import APIRouter, Depends, Header
from fastapi.responses import JSONResponse
from jsonschema import ValidationError
from pydantic import BaseModel, ConfigDict, Field

from agent.contracts import validate_contract
from forge.enforce import (
    EnforceContextError,
    prepare_governance_context,
    project_enforce_query_run,
)
from forge.query_runs import (
    QueryRunError,
    approve_and_execute_query_run,
    create_query_run,
    get_query_run,
)
from web.auth import (
    credential_binding_matches_principal_context,
    require_api_auth,
    require_enforce_reviewer_auth,
)

router = APIRouter(prefix="/api/v1/enforce", tags=["Enforce"])


class EnforceQueryRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    task_run_id: str = Field(pattern=r"^tr_[A-Za-z0-9_-]+$", max_length=128)
    purpose: str = Field(min_length=1, max_length=1000)
    question: str = Field(min_length=1, max_length=10_000)
    principal_context: dict[str, Any]
    delegated_mandate: dict[str, Any] | None
    resource_scope: list[dict[str, Any]] = Field(min_length=1, max_length=128)
    candidate: dict[str, Any]
    dialect: Literal["auto", "sqlite", "postgresql", "mysql", "bigquery", "snowflake"] = "auto"


class HumanPrincipalV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    principal_id: str = Field(min_length=1, max_length=256)
    principal_type: Literal["human"]


class EnforceQueryApprovalV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    approver_principal: HumanPrincipalV1
    sql_hash: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")
    assurance_report_hash: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")
    enforcement_context_hash: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")


def _error(exc: EnforceContextError | QueryRunError) -> JSONResponse:
    retryable = exc.code not in {
        "approver_not_authorized",
        "candidate_contract_invalid",
        "query_run_access_denied",
        "governance_contract_invalid",
        "human_accountability_required",
        "readonly_identity_unconfirmed",
    }
    return JSONResponse(
        {
            "schema_version": 1,
            "status": "error",
            "failure": {
                "stage": "enforce",
                "code": exc.code,
                "retryable": retryable,
            },
        },
        status_code=exc.status_code,
    )


def _bind_authentication(
    principal_context: dict[str, Any],
    auth_binding: dict[str, str | None],
) -> dict[str, Any]:
    bound = deepcopy(principal_context)
    authentication = bound.get("authentication_context")
    if not isinstance(authentication, dict):
        raise EnforceContextError(
            "governance_contract_invalid",
            "principal_context authentication_context is required",
        )
    authentication["method"] = auth_binding["method"]
    authentication["assurance_level"] = auth_binding["assurance_level"]
    authentication["session_id_hash"] = auth_binding["session_id_hash"]
    return bound


@router.post("/query-runs")
async def create_enforced_query_run(
    req: EnforceQueryRequestV1,
    idempotency_key: str = Header(default="", alias="Idempotency-Key"),
    auth_binding: dict[str, str | None] = Depends(require_api_auth),
):
    payload = req.model_dump()
    try:
        validate_contract("enforce_query_request_v1", payload)
        principal_context = _bind_authentication(req.principal_context, auth_binding)
        governance_context = prepare_governance_context(
            principal_context=principal_context,
            delegated_mandate=req.delegated_mandate,
            purpose=req.purpose,
            task_run_id=req.task_run_id,
            resource_scope=req.resource_scope,
        )
        accountable = principal_context["accountable_principal"]
        run = await create_query_run(
            task_run_id=req.task_run_id,
            org_id=principal_context["organization_id"],
            team_id=principal_context["workspace_id"],
            user_id=accountable["principal_id"],
            question=req.question,
            dialect=req.dialect,
            idempotency_key=idempotency_key,
            candidate=req.candidate,
            governance_context=governance_context,
        )
        return project_enforce_query_run(run)
    except ValidationError:
        return _error(
            EnforceContextError(
                "enforce_request_invalid",
                "Enforce request contract is invalid",
            )
        )
    except (EnforceContextError, QueryRunError) as exc:
        return _error(exc)


@router.get("/query-runs/{query_run_id}")
async def get_enforced_query_run(
    query_run_id: str,
    auth_binding: dict[str, str | None] = Depends(require_api_auth),
):
    run = await get_query_run(query_run_id)
    if run is None:
        return _error(
            QueryRunError(
                "QueryRun not found",
                status_code=404,
                code="query_run_not_found",
            )
        )
    if run.get("enforce_schema_version") != 1:
        return _error(
            EnforceContextError(
                "enforcement_context_missing",
                "Governed QueryRun not found",
                status_code=404,
            )
        )
    if not credential_binding_matches_principal_context(
        run.get("principal_context"), auth_binding
    ):
        return _error(
            EnforceContextError(
                "query_run_access_denied",
                "QueryRun belongs to a different authenticated credential",
                status_code=403,
            )
        )
    try:
        payload = project_enforce_query_run(run)
    except EnforceContextError as exc:
        return _error(exc)
    return payload


@router.post("/query-runs/{query_run_id}/approve")
async def approve_enforced_query_run(
    query_run_id: str,
    req: EnforceQueryApprovalV1,
    idempotency_key: str = Header(default="", alias="Idempotency-Key"),
    _auth: dict[str, str | None] = Depends(require_api_auth),
    _reviewer: str = Depends(require_enforce_reviewer_auth),
):
    try:
        validate_contract("enforce_query_approval_v1", req.model_dump())
        run = await approve_and_execute_query_run(
            query_run_id=query_run_id,
            approver_user_id=req.approver_principal.principal_id,
            sql_hash=req.sql_hash,
            assurance_report_hash=req.assurance_report_hash,
            enforcement_context_hash=req.enforcement_context_hash,
            idempotency_key=idempotency_key,
        )
        return project_enforce_query_run(run)
    except ValidationError:
        return _error(
            EnforceContextError(
                "approval_contract_invalid",
                "Approval request contract is invalid",
            )
        )
    except (EnforceContextError, QueryRunError) as exc:
        return _error(exc)
