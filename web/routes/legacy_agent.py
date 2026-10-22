"""Compatibility endpoints without an executable legacy Agent path."""
from __future__ import annotations

from functools import partial
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from agent import audit
from agent.agent import prepare_query as agent_prepare_query
from agent.tenant import tenants
from forge.governance import AuthenticatedPrincipal
from web.auth import require_runtime_principal
from web.router_support import _run_sync

router = APIRouter()


class PrepareQueryRequest(BaseModel):
    question: str
    dialect: str | None = None


@router.post("/api/prepare-query", response_class=JSONResponse)
async def api_prepare_query(req: PrepareQueryRequest, actor: AuthenticatedPrincipal = Depends(require_runtime_principal)):
    """Prepare a candidate, never execute it or create approval state."""
    allowed_tables = tenants.get_allowed_tables_for_user(actor.principal_id)
    result = await _run_sync(
        partial(agent_prepare_query, allowed_tables=allowed_tables), actor.principal_id, req.question, req.dialect,
    )
    await audit.log(
        user_id=actor.principal_id,
        user_message=req.question,
        forge_json=result.get("forge_json"),
        sql=result.get("sql"),
        status="needs_external_review" if result.get("status") == "needs_review" else "error",
        error_message=result.get("error") or result.get("text") or None,
    )
    return result


@router.post("/api/chat")
@router.post("/api/approve")
@router.post("/api/cancel")
async def legacy_agent_execution_gone():
    return JSONResponse(
        status_code=410,
        content={"status": "deprecated", "error": "Create a governed Pi TaskRun and QueryRun v2."},
    )
