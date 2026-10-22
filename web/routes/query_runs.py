"""Pi internal controls: legacy execution is closed; candidate generation is prepare-only."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from agent.agent import prepare_query
from forge.governance import GovernanceError, connect_governance, ensure_governance_schema, resolve_control_token
from web.auth import require_pi_service_auth
from web.pi_task_scope import task_scope, task_tables

router = APIRouter(prefix="/api/internal/query-runs", dependencies=[Depends(require_pi_service_auth)])
candidate_router = APIRouter(prefix="/api/internal/query-candidates", dependencies=[Depends(require_pi_service_auth)])
internal_governance_router = APIRouter(prefix="/api/internal/governance", dependencies=[Depends(require_pi_service_auth)])


def _gone():
    return JSONResponse(status_code=410, content={"error": "governed_api_v2_required"})


@router.post("")
@router.get("/{query_run_id}")
@router.post("/{query_run_id}/approve")
@router.post("/{query_run_id}/cancel")
@router.get("/{query_run_id}/result")
async def legacy_internal_query_runs(query_run_id: str = ""):
    return _gone()


class CandidateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    task_run_id: str = Field(min_length=1, max_length=128)
    mandate_id: str = Field(min_length=1, max_length=128)
    question: str = Field(min_length=1, max_length=2000)
    dialect: str | None = None


@candidate_router.post("")
async def create_candidate(payload: CandidateRequest):
    scope = await task_scope(payload.task_run_id)
    if scope.get("mandate_id") != payload.mandate_id:
        raise HTTPException(status_code=403, detail="mandate_not_attached")
    tables = task_tables(scope, require_mandate=True)
    result = prepare_query(scope["execution_actor_id"], payload.question, payload.dialect,
                           allowed_tables=tables)
    if result["status"] == "needs_review" and isinstance(result.get("forge_json"), dict):
        return {"candidate": {"kind": "forge_json", "forge_json": result["forge_json"]}}
    # Clarification and generation failure are not QueryRuns and never claim execution.
    return {"status": result["status"], "text": result.get("text") or "", "error": result.get("error") or ""}


class ResolveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    token: str = Field(min_length=1)
    method: str = Field(min_length=1)
    path: str = Field(min_length=1)
    body_hash: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")
    idempotency_key: str | None = None


@internal_governance_router.post("/control-tokens/resolve")
async def resolve_control_request(payload: ResolveRequest, service=Depends(require_pi_service_auth)):
    try:
        with connect_governance() as conn:
            ensure_governance_schema(conn)
            record, principal = resolve_control_token(
                conn, payload.token, payload.method, payload.path,
                payload.body_hash, payload.idempotency_key,
            )
            channel = external_user_id = None
            if record["channel_binding_id"]:
                channel, external_user_id = record["channel_binding_id"].split(":", 1)
                binding = conn.execute("SELECT status,principal_id,revision FROM governance_channel_bindings "
                                       "WHERE channel=? AND external_user_id=?", (channel, external_user_id)).fetchone()
                if (binding is None or binding["status"] != "active" or
                    binding["principal_id"] != principal.principal_id or
                    binding["revision"] != record["channel_binding_revision"]):
                    raise GovernanceError("control_principal_invalid", 403)
            return {"principal": asdict(principal), "action": record["action"],
                    "task_run_id": record["task_run_id"], "query_run_id": record["query_run_id"],
                    "approval_hashes": json.loads(record["approval_hashes_json"]) if record["approval_hashes_json"] else None,
                    "idempotency_key": record["idempotency_key"],
                    "channel": channel, "external_user_id": external_user_id}
    except GovernanceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.code) from exc
    except (OSError, sqlite3.Error) as exc:
        raise HTTPException(status_code=503, detail="governance_unavailable") from exc
