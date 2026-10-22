"""Versioned, identity-scoped Explain API backed by QueryRun evidence."""
from __future__ import annotations

import asyncio
import sqlite3

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from forge.explain import ExplainError, project_explain_query_run
from forge.governance import AuthenticatedPrincipal, GovernanceError
from forge.governance_mandates import get_mandate
from forge.query_runs import QueryRunError, _validate_run_access, get_query_run
from web.auth import require_admin_principal, require_runtime_principal

router = APIRouter(tags=["Explain"])


def _error(code: str, *, status_code: int, version: int) -> JSONResponse:
    return JSONResponse(
        {"schema_version": version, "status": "error", "failure": {
            "stage": "explain", "code": code, "retryable": status_code == 503,
        }},
        status_code=status_code,
    )


@router.get("/api/v2/explain/query-runs/{query_run_id}")
async def explain_governed_query_run(
    query_run_id: str,
    actor: AuthenticatedPrincipal = Depends(require_runtime_principal),
):
    try:
        run = await get_query_run(query_run_id)
        if run is None or run.get("enforce_schema_version") != 2:
            return _error("explain_query_run_not_found", status_code=404, version=2)
        await asyncio.to_thread(_validate_run_access, run, actor)
        issued = await asyncio.to_thread(get_mandate, actor=actor, mandate_id=run["mandate_id"])
        return project_explain_query_run(run, issued_mandate=issued)
    except QueryRunError as exc:
        return _error("explain_query_run_not_found" if exc.status_code in {403, 404} else exc.code,
                      status_code=404 if exc.status_code in {403, 404} else exc.status_code, version=2)
    except GovernanceError as exc:
        return _error("explain_query_run_not_found" if exc.status_code in {403, 404} else exc.code,
                      status_code=404 if exc.status_code in {403, 404} else exc.status_code, version=2)
    except ExplainError as exc:
        return _error(exc.code, status_code=exc.status_code, version=2)
    except (sqlite3.Error, OSError):
        return _error("governance_unavailable", status_code=503, version=2)


@router.get("/api/v1/explain/query-runs/{query_run_id}")
async def explain_historical_query_run(
    query_run_id: str,
    actor: AuthenticatedPrincipal = Depends(require_admin_principal),
):
    try:
        run = await get_query_run(query_run_id)
        if (run is None or run.get("enforce_schema_version") != 1
                or run.get("org_id") != actor.organization_id
                or (run.get("workspace_id") or run.get("team_id")) != actor.workspace_id):
            return _error("explain_query_run_not_found", status_code=404, version=1)
        return project_explain_query_run(run)
    except ExplainError as exc:
        return _error(exc.code, status_code=exc.status_code, version=1)
    except (sqlite3.Error, OSError):
        return _error("governance_unavailable", status_code=503, version=1)
