"""Legacy Agent API: deprecated chat/approve/cancel plus external prepare-query.

POST /api/chat, /api/approve and /api/cancel default to 410 and only respond
when cfg.LEGACY_AGENT_API_ENABLED is set (explicit rollback). /api/prepare-query
is the external-agent review contract and stays active.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from agent import audit
from agent.agent import process as agent_process
from agent.agent import prepare_query as agent_prepare_query
from agent.agent import approve as agent_approve
from agent.agent import cancel as agent_cancel
from config import cfg
from forge.executor import execute
from web.auth import require_api_auth
from web.router_support import _run_sync

logger = logging.getLogger(__name__)

router = APIRouter()


class ChatRequest(BaseModel):
    message: str
    user_id: str = "web_user"


class PrepareQueryRequest(BaseModel):
    question: str
    user_id: str = "external-agent"
    dialect: Optional[str] = None


@router.post("/api/chat", response_class=JSONResponse)
async def api_chat(req: ChatRequest, _auth=Depends(require_api_auth)):
    """Deprecated legacy Agent path; disabled unless an explicit rollback flag is set."""
    if not cfg.LEGACY_AGENT_API_ENABLED:
        return JSONResponse(
            {
                "status": "deprecated",
                "error": "Legacy Agent API is disabled; create a Pi TaskRun via /tasks.",
            },
            status_code=410,
        )
    from agent.pipeline import router as intent_router, runner as pipeline_runner

    pipeline_name = intent_router.route(req.message)

    if pipeline_name in ("analyze", "visualize", "report"):
        # Pipeline 模式：run() 返回 pending_approval 状态，等待用户确认 SQL
        run = await _run_sync(pipeline_runner.run, pipeline_name, req.user_id, req.message)
        # 取 generate stage 的结果（SQL + text）
        gen_stage = next((s for s in run.stages if s.stage == "generate"), None)
        art = gen_stage.artifact if gen_stage else None
        sql = getattr(art, "sql", None) if art else None
        forge_json = getattr(art, "forge_json", None) if art else None
        action = "sql_review" if sql else ("error" if run.status == "failed" else "message")
        text = (gen_stage.error or "Pipeline 启动失败") if run.status == "failed" else ""
        await audit.log(
            user_id=req.user_id, user_message=req.message,
            forge_json=forge_json, sql=sql,
            status="pending" if sql else "error",
            error_message=text or None,
        )
        return {"text": text, "sql": sql, "forge_json": forge_json, "action": action,
                "pipeline": pipeline_name}
    else:
        # 普通查询模式
        resp = await _run_sync(agent_process, req.user_id, req.message)
        status_map = {"sql_review": "pending", "error": "error", "metric_saved": "approved"}
        await audit.log(
            user_id=req.user_id,
            user_message=req.message,
            forge_json=resp.forge_json,
            sql=resp.sql,
            status=status_map.get(resp.action, "approved"),
            error_message=resp.text if resp.action == "error" else None,
        )
        return {
            "text": resp.text,
            "sql": resp.sql,
            "forge_json": resp.forge_json,
            "action": resp.action,
            "retry_count": getattr(resp, "retry_count", 0),
        }


@router.post("/api/prepare-query", response_class=JSONResponse)
async def api_prepare_query(req: PrepareQueryRequest, _auth=Depends(require_api_auth)):
    """外部 Agent 嵌入入口：只生成可审核 SQL，不创建可执行 pending state。"""
    result = await _run_sync(agent_prepare_query, req.user_id, req.question, req.dialect)
    status = "needs_external_review" if result.get("status") == "needs_review" else "error"
    await audit.log(
        user_id=req.user_id,
        user_message=req.question,
        forge_json=result.get("forge_json"),
        sql=result.get("sql"),
        status=status,
        error_message=result.get("error") or result.get("text") or None,
    )
    return result


@router.post("/api/approve", response_class=JSONResponse)
async def api_approve(req: ChatRequest, _auth=Depends(require_api_auth)):
    """Deprecated legacy approval path, available only for explicit rollback."""
    if not cfg.LEGACY_AGENT_API_ENABLED:
        return JSONResponse({"status": "deprecated", "error": "Legacy Agent API is disabled."}, status_code=410)
    resp = await _run_sync(agent_approve, req.user_id)
    result = {"text": resp.text, "sql": resp.sql, "action": resp.action,
              "columns": None, "rows": None, "row_count": 0, "exec_error": None,
              "analysis": None, "chart_html": None, "error_code": None}

    if resp.action == "approved" and resp.sql:
        # 1. 执行 SQL

        execution_ms = None
        try:
            started = time.perf_counter()
            outcome = await _run_sync(execute, resp.sql)
            execution_ms = int((time.perf_counter() - started) * 1000)
            result["columns"] = outcome.columns
            result["rows"] = [list(r) for r in outcome.rows]
            result["row_count"] = len(outcome.rows)
            if not outcome.success:
                result["exec_error"] = outcome.text
                result["error_code"] = outcome.error_code or "execution_failed"
        except Exception:
            logger.exception("Approved SQL execution failed")
            result["exec_error"] = "数据库查询失败，请检查 SQL 或联系管理员。"
            result["error_code"] = "execution_failed"

        if result["error_code"]:
            result["action"] = "execution_failed"
            result["text"] = "SQL 执行失败，请重新生成或修改查询。"

        # 2. 仅在查询成功时恢复兼容 Pipeline，失败结果不能进入分析阶段。
        try:
            from agent.memory import memory as _mem
            from agent.pipeline import runner as _runner, QueryResult, Artifact
            run_data = _mem.get_state(req.user_id, "pipeline_run")
            if result["error_code"] and run_data and run_data.get("status") == "pending_approval":
                run_data["status"] = "failed"
                run_data["error"] = "SQL 执行失败，Pipeline 已终止。"
                _mem.set_state(req.user_id, "pipeline_run", run_data)
            elif run_data and run_data.get("status") == "pending_approval":
                # 找到 generate stage artifact，注入 rows / columns
                stages = run_data.get("stages", [])
                for s in stages:
                    if s.get("stage") == "generate" and s.get("artifact"):
                        art = s["artifact"]
                        art["rows"]    = result["rows"] or []
                        art["columns"] = result["columns"]
                        art["row_count"] = len(result["rows"] or [])
                        s["artifact"] = art
                run_data["stages"] = stages
                run_data["status"] = "running"
                _mem.set_state(req.user_id, "pipeline_run", run_data)

                # resume pipeline（analyze / chart / report 阶段）
                pipeline_run = await _run_sync(_runner.resume, req.user_id)
                if pipeline_run:
                    # 收集分析报告
                    for sr in pipeline_run.stages:
                        art = sr.artifact
                        if art is None:
                            continue
                        if isinstance(art, dict):
                            art = Artifact.from_dict(art)
                        if art._type == "analysis_report":
                            result["analysis"] = {
                                "summary":          getattr(art, "summary", ""),
                                "insights":         getattr(art, "insights", []),
                                "key_metrics":      getattr(art, "key_metrics", {}),
                                "trend_direction":  getattr(art, "trend_direction", ""),
                                "anomalies":        getattr(art, "anomalies", []),
                                "recommendations":  getattr(art, "recommendations", []),
                            }
                        elif art._type == "chart_spec":
                            result["chart_html"] = getattr(art, "html", None)
                    result["action"] = "pipeline_complete"
        except Exception as exc:
            import logging
            logging.getLogger(__name__).warning("Pipeline resume failed: %s", exc)

        await audit.update_latest_pending(
            req.user_id,
            "error" if result["error_code"] else "approved",
            error_message=result["exec_error"],
            row_count=result["row_count"],
            execution_ms=execution_ms,
        )

    return result


@router.post("/api/cancel", response_class=JSONResponse)
async def api_cancel(req: ChatRequest, _auth=Depends(require_api_auth)):
    """Deprecated legacy cancellation path, available only for explicit rollback."""
    if not cfg.LEGACY_AGENT_API_ENABLED:
        return JSONResponse({"status": "deprecated", "error": "Legacy Agent API is disabled."}, status_code=410)
    resp = await _run_sync(agent_cancel, req.user_id)
    await audit.update_latest_pending(req.user_id, "cancelled")
    return {"text": resp.text, "action": resp.action}
