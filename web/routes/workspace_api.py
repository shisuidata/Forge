"""Workspace feedback; legacy raw SQL is permanently non-executable."""
from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from agent import feedback
from forge.governance import AuthenticatedPrincipal
from web.auth import require_runtime_principal

router = APIRouter()


class FeedbackRequest(BaseModel):
    feedback_type: str = "wrong_result"
    message: str
    audit_id: int | None = None
    question: str | None = None
    sql: str | None = None
    expected: str | None = None


@router.post("/api/execute-raw")
async def raw_sql_gone():
    return JSONResponse(status_code=410, content={"error": "governed_api_v2_required"})


@router.post("/api/feedback", response_class=JSONResponse)
async def api_feedback(req: FeedbackRequest, actor: AuthenticatedPrincipal = Depends(require_runtime_principal)):
    try:
        feedback_id = await feedback.submit(
            user_id=actor.principal_id,
            audit_id=req.audit_id,
            question=req.question,
            sql=req.sql,
            feedback_type=req.feedback_type,
            message=req.message,
            expected=req.expected,
        )
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
    return {"ok": True, "feedback_id": feedback_id, "status": "pending"}
