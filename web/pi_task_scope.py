"""Forge-side authoritative Task scope lookup for authenticated Pi internal calls."""
from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
from urllib.parse import quote

import httpx
from fastapi import HTTPException

from agent.tenant import tenants
from config import cfg
from forge.governance import GovernanceError, canonical_hash, connect_governance, ensure_governance_schema

_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


async def task_scope(task_run_id: str) -> dict:
    if not _ID.fullmatch(task_run_id):
        raise HTTPException(status_code=422, detail="invalid_task_run_id")
    if not cfg.PI_CHANNEL_SERVICE_KEY:
        raise HTTPException(status_code=503, detail="pi_control_unavailable")
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(
                cfg.PI_ORCHESTRATOR_URL.rstrip("/") + "/v1/internal/task-scopes/" + quote(task_run_id, safe=""),
                headers={"X-Channel-Service-Key": cfg.PI_CHANNEL_SERVICE_KEY},
            )
    except httpx.RequestError as exc:
        raise HTTPException(status_code=503, detail="pi_unavailable") from exc
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail="task_not_found")
    if response.status_code != 200:
        raise HTTPException(status_code=503, detail="pi_control_unavailable")
    try:
        scope = response.json()
        if not isinstance(scope, dict) or scope.get("task_id") != task_run_id or any(
            not isinstance(scope.get(k), str) or not scope[k] for k in ("org_id", "team_id", "user_id", "execution_actor_id", "status")
        ):
            raise ValueError("invalid scope")
        if scope["execution_actor_id"] != cfg.PI_EXECUTION_PRINCIPAL_ID:
            raise HTTPException(status_code=404, detail="task_not_found")
        requester = tenants.get_principal(scope["user_id"])
        actor = tenants.get_principal(scope["execution_actor_id"])
        if requester["principal_type"] != "human" or actor["principal_type"] != "service" or (
            requester["organization_id"], requester["workspace_id"]
        ) != (scope["org_id"], scope["team_id"]) or (
            actor["organization_id"], actor["workspace_id"]
        ) != (scope["org_id"], scope["team_id"]):
            raise HTTPException(status_code=404, detail="task_not_found")
        return scope
    except HTTPException:
        raise
    except GovernanceError as exc:
        if exc.status_code == 503:
            raise HTTPException(status_code=503, detail="governance_unavailable") from exc
        raise HTTPException(status_code=404, detail="task_not_found") from exc
    except (OSError, sqlite3.Error) as exc:
        raise HTTPException(status_code=503, detail="governance_unavailable") from exc
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=503, detail="pi_control_unavailable") from exc


def task_tables(scope: dict, *, require_mandate: bool = False) -> list[str]:
    """Current ACL, narrowed by the attached active mandate when present."""
    try:
        tables = set(tenants.get_allowed_tables_for_user(scope["execution_actor_id"]))
        tables.intersection_update(tenants.get_allowed_tables_for_user(scope["user_id"]))
        mandate_id = scope.get("mandate_id")
        if not mandate_id:
            if require_mandate:
                raise HTTPException(status_code=403, detail="mandate_required")
            return sorted(tables)
        with connect_governance() as conn:
            ensure_governance_schema(conn)
            mandate = conn.execute("SELECT * FROM issued_mandates WHERE mandate_id=?", (mandate_id,)).fetchone()
            if (mandate is None or mandate["status"] != "active" or
                datetime.fromisoformat(mandate["expires_at"]) <= datetime.now(timezone.utc) or
                any(mandate[k] != scope[v] for k, v in (("task_run_id", "task_id"), ("org_id", "org_id"),
                        ("workspace_id", "team_id"), ("accountable_id", "user_id"),
                        ("delegate_id", "execution_actor_id")))):
                raise HTTPException(status_code=403, detail="mandate_inactive")
            import json
            issuance = json.loads(mandate["issuance_json"])
            if canonical_hash(issuance) != mandate["issuance_hash"]:
                raise HTTPException(status_code=409, detail="mandate_integrity_failed")
            tables.intersection_update(tenants.get_allowed_tables_for_user(mandate["issuer_id"]))
            tables.intersection_update(issuance["policy"]["allowed_tables"])
        return sorted(tables)
    except HTTPException:
        raise
    except GovernanceError as exc:
        raise HTTPException(status_code=503 if exc.status_code == 503 else exc.status_code, detail=exc.code) from exc
    except (OSError, sqlite3.Error) as exc:
        raise HTTPException(status_code=503, detail="governance_unavailable") from exc
