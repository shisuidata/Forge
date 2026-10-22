"""Task-bound, ACL-filtered read-only knowledge retrieval for Pi."""
from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from forge.context import search_context
from web.auth import require_pi_service_auth
from web.pi_task_scope import task_scope, task_tables

router = APIRouter(prefix="/api/internal/context", dependencies=[Depends(require_pi_service_auth)])


class ContextSearchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    task_run_id: str = Field(min_length=1, max_length=128)
    question: str = Field(min_length=1, max_length=2000)
    limit: int = Field(default=8, ge=1, le=12)


@router.post("/search")
async def context_search(request: ContextSearchRequest) -> dict:
    scope = await task_scope(request.task_run_id)
    try:
        return search_context(
            question=request.question.strip(), user_id=scope["user_id"],
            team_id=scope["team_id"], limit=request.limit,
            allowed_tables=task_tables(scope),
        )
    except (OSError, sqlite3.Error) as exc:
        raise HTTPException(status_code=503, detail="context_store_unavailable") from exc
