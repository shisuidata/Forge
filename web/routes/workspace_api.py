"""Workspace APIs: manual raw-SQL execution and result feedback."""
from __future__ import annotations

import logging
import time

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from agent import audit
from agent import feedback
from config import cfg
from forge.executor import execute
from web.auth import require_api_auth
from web.router_support import _run_sync

logger = logging.getLogger(__name__)

router = APIRouter()


class ExecuteRawRequest(BaseModel):
    sql: str
    user_id: str = "web_user"


class FeedbackRequest(BaseModel):
    user_id: str = "web_user"
    feedback_type: str = "wrong_result"
    message: str
    audit_id: int | None = None
    question: str | None = None
    sql: str | None = None
    expected: str | None = None


@router.post("/api/execute-raw", response_class=JSONResponse)
async def api_execute_raw(req: ExecuteRawRequest, _auth=Depends(require_api_auth)):
    """直接执行用户编辑后的 SQL（跳过 Agent 编译）。"""
    result = {"text": "", "sql": req.sql, "action": "approved",
              "columns": None, "rows": None, "row_count": 0, "exec_error": None,
              "analysis": None, "chart_html": None, "error_code": None}
    execution_ms = None
    if not cfg.RAW_SQL_ENABLED:
        result["exec_error"] = "手动 SQL 执行已被配置禁用。"
        result["error_code"] = "raw_sql_disabled"
        result["action"] = "execution_failed"
        result["text"] = "SQL 执行失败，请重新生成或修改查询。"
        await audit.log(
            user_id=req.user_id,
            user_message="[手动编辑 SQL]",
            forge_json=None,
            sql=req.sql,
            status="error",
            error_message=result["exec_error"],
        )
        return result
    try:
        started = time.perf_counter()
        outcome = await _run_sync(execute, req.sql)
        execution_ms = int((time.perf_counter() - started) * 1000)
        result["columns"] = outcome.columns
        result["rows"] = [list(r) for r in outcome.rows]
        result["row_count"] = len(outcome.rows)
        if not outcome.success:
            result["exec_error"] = outcome.text
            result["error_code"] = outcome.error_code or "execution_failed"
    except Exception:
        logger.exception("Raw SQL execution failed")
        result["exec_error"] = "数据库查询失败，请检查 SQL 或联系管理员。"
        result["error_code"] = "execution_failed"

    if result["error_code"]:
        result["action"] = "execution_failed"
        result["text"] = "SQL 执行失败，请重新生成或修改查询。"

    await audit.log(
        user_id=req.user_id,
        user_message="[手动编辑 SQL]",
        forge_json=None,
        sql=req.sql,
        status="approved" if not result["error_code"] else "error",
        error_message=result["exec_error"],
        row_count=result["row_count"],
        execution_ms=execution_ms,
    )
    return result


@router.post("/api/feedback", response_class=JSONResponse)
async def api_feedback(req: FeedbackRequest, _auth=Depends(require_api_auth)):
    """提交 SQL/结果反馈，进入待处理队列。"""
    try:
        feedback_id = await feedback.submit(
            user_id=req.user_id,
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
