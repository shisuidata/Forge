"""Controlled Pi → Forge semantic memory boundary."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from agent.memory.smp import SemanticMemoryPool
from web.auth import require_pi_service_auth

router = APIRouter()
_ALLOWED_CATEGORIES = {"user_profile", "correction", "confirmed_fact", "session_summary"}


def _pool() -> SemanticMemoryPool:
    return SemanticMemoryPool()


@router.post("/api/internal/memory/entries", dependencies=[Depends(require_pi_service_auth)])
async def write_memory(payload: dict[str, Any]):
    required = ("org_id", "team_id", "user_id", "operation", "category", "key")
    if any(not isinstance(payload.get(key), str) or not payload[key].strip() for key in required):
        raise HTTPException(status_code=422, detail="memory identity, operation, category and key are required")
    if payload["category"] not in _ALLOWED_CATEGORIES:
        raise HTTPException(status_code=422, detail="memory category is not allowed")
    if payload["operation"] not in {"upsert", "delete"}:
        raise HTTPException(status_code=422, detail="memory operation is invalid")
    if len(payload["key"]) > 128:
        raise HTTPException(status_code=422, detail="memory key is too long")
    pool = _pool()
    if payload["operation"] == "delete":
        deleted = pool.delete_user_entry(payload["user_id"], payload["category"], payload["key"])
        return {"status": "deleted" if deleted else "not_found", "key": payload["key"]}
    value = payload.get("value")
    serialized = str(value)
    if value is None or len(serialized.encode()) > 4_000:
        raise HTTPException(status_code=422, detail="memory value is required and bounded to 4 KB")
    expires_at = payload.get("expires_at")
    if expires_at is not None:
        if not isinstance(expires_at, str):
            raise HTTPException(status_code=422, detail="expires_at must be a date-time")
        try:
            expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            if expiry.tzinfo is None or expiry <= datetime.now(timezone.utc):
                raise ValueError
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="expires_at must be a future timezone-aware date-time") from exc
    confidence = payload.get("confidence", 1.0)
    if not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
        raise HTTPException(status_code=422, detail="confidence must be between 0 and 1")
    pool.upsert(
        category=payload["category"], key=payload["key"], value=value,
        user_id=payload["user_id"], scope="user",
        source_session=str(payload.get("source_session") or ""),
        source_revision=str(payload.get("source_revision") or ""),
        confidence=float(confidence), expires_at=expires_at,
    )
    return {
        "status": "confirmed", "scope": "user", "category": payload["category"],
        "key": payload["key"], "expires_at": expires_at,
    }
