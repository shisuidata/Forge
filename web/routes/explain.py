"""Public, versioned Explain API backed by the existing QueryRun truth source."""
from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from forge.explain import ExplainError, project_explain_query_run
from forge.query_runs import get_query_run
from web.auth import (
    credential_binding_matches_principal_context,
    require_api_auth,
)

router = APIRouter(prefix="/api/v1/explain", tags=["Explain"])


def _error(code: str, *, status_code: int) -> JSONResponse:
    return JSONResponse(
        {
            "schema_version": 1,
            "status": "error",
            "failure": {
                "stage": "explain",
                "code": code,
                "retryable": False,
            },
        },
        status_code=status_code,
    )


@router.get("/query-runs/{query_run_id}")
async def explain_query_run(
    query_run_id: str,
    auth_binding: dict[str, str | None] = Depends(require_api_auth),
):
    run = await get_query_run(query_run_id)
    if run is None or run.get("enforce_schema_version") != 1:
        return _error("explain_query_run_not_found", status_code=404)
    if not credential_binding_matches_principal_context(
        run.get("principal_context"), auth_binding
    ):
        return _error("query_run_access_denied", status_code=403)
    try:
        return project_explain_query_run(run)
    except ExplainError as exc:
        return _error(exc.code, status_code=exc.status_code)
