"""Authenticated, read-only Context API used by Pi knowledge answers."""
from __future__ import annotations

import re

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from forge.context import search_context
from web.auth import require_pi_service_auth

router = APIRouter(
    prefix="/api/internal/context",
    dependencies=[Depends(require_pi_service_auth)],
)

_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


class ContextSearchRequest(BaseModel):
    org_id: str
    team_id: str
    user_id: str
    question: str = Field(min_length=1, max_length=2000)
    limit: int = Field(default=8, ge=1, le=12)


@router.post("/search")
async def context_search(request: ContextSearchRequest) -> dict:
    for name, value in (
        ("org_id", request.org_id),
        ("team_id", request.team_id),
        ("user_id", request.user_id),
    ):
        if _ID_PATTERN.fullmatch(value) is None:
            raise HTTPException(status_code=400, detail=f"Invalid {name}")
    return search_context(
        question=request.question.strip(),
        user_id=request.user_id,
        team_id=request.team_id,
        limit=request.limit,
    )
