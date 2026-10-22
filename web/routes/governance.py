"""Authenticated mandate management API."""
from __future__ import annotations

from agent.tenant import tenants
from config import cfg
from fastapi import APIRouter, Depends, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from forge.governance import AuthenticatedPrincipal, GovernanceError
from forge.governance_mandates import _registry_tables, get_mandate, issue_mandate, revoke_mandate
from forge.query_runs import QueryRunError, list_reviewer_query_runs
from web.auth import require_admin_principal, require_human_principal, require_runtime_principal

router = APIRouter(prefix="/api/v2/governance", tags=["Governance"])


class RevokeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_state_revision: int = Field(ge=1)
    reason: str = Field(min_length=1, max_length=1000)


def _error(exc: GovernanceError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"code": exc.code})


@router.get("/ui-context")
async def governance_ui_context(actor: AuthenticatedPrincipal = Depends(require_human_principal)):
    """Only the authenticated workspace receives options for issuing a mandate."""
    context = {"principal_id": actor.principal_id, "role": actor.role}
    if actor.role != "admin":
        return context
    try:
        members = tenants.get_team_members(actor.workspace_id)
        tables = sorted(set(tenants.get_allowed_tables(actor.workspace_id)) & _registry_tables())
    except GovernanceError as exc:
        return _error(exc)
    return {**context, "tables": tables,
            "humans": [{"principal_id": m["user_id"], "display_name": m["display_name"], "role": m["role"]}
                       for m in members if m["status"] == "active" and m["principal_type"] == "human"],
            "datasource_id": cfg.DATASOURCE_ID, "execution_actor_id": cfg.PI_EXECUTION_PRINCIPAL_ID}

@router.post("/mandates", status_code=201)
async def issue_mandate_route(
    body: dict,
    actor: AuthenticatedPrincipal = Depends(require_admin_principal),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
    try:
        return issue_mandate(actor=actor, request=body, idempotency_key=idempotency_key or "")
    except GovernanceError as exc:
        return _error(exc)


@router.get("/mandates/{mandate_id}")
async def get_mandate_route(
    mandate_id: str, actor: AuthenticatedPrincipal = Depends(require_runtime_principal),
):
    try:
        return get_mandate(actor=actor, mandate_id=mandate_id)
    except GovernanceError as exc:
        return _error(exc)


@router.post("/mandates/{mandate_id}/revoke")
async def revoke_mandate_route(
    mandate_id: str, body: RevokeRequest,
    actor: AuthenticatedPrincipal = Depends(require_admin_principal),
):
    try:
        return revoke_mandate(actor=actor, mandate_id=mandate_id, **body.model_dump())
    except GovernanceError as exc:
        return _error(exc)

@router.get("/reviews")
async def list_reviews_route(actor: AuthenticatedPrincipal = Depends(require_human_principal)):
    try:
        runs = await list_reviewer_query_runs(actor)
    except QueryRunError as exc:
        return _error(exc)
    return {"reviews": [{
        "query_run_id": run["query_run_id"], "task_run_id": run["task_run_id"],
        "question": run["question"], "sql": run["sql"], "sql_hash": run["sql_hash"],
        "assurance_report": run["assurance_report"],
        "assurance_report_hash": run["assurance_report_hash"],
        "enforcement_context_hash": run["enforcement_context_hash"],
        "mandate_id": run["mandate_id"], "expires_at": run["expires_at"],
    } for run in runs]}
