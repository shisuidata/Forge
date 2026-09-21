"""Pi Web Chat / Task API: root-level /api/pi/* routes over the Pi orchestrator.

All upstream I/O goes through the module-level `_pi_request`; tests monkeypatch
this attribute (formerly web.router._pi_request).
"""
from __future__ import annotations

import asyncio
import re
from typing import Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from config import cfg
from web.auth import require_api_auth
from web.diagnostics import error_response
from web.pi_client import pi_request as _pi_request
from web.pi_projections import (
    _bounded_web_execution_plan,
    _bounded_web_stage_attempts,
    _bounded_web_task_events,
    _pi_disabled_response,
    _pi_stage_payload,
    _valid_web_event_id,
    _web_action_event_id,
    _web_admin_can_observe,
    _web_admin_task_scopes,
)

router = APIRouter()


class PiTaskCreateRequest(BaseModel):
    message: str
    intent: str = "query_prepare"
    channel_conversation_id: Optional[str] = None


class PiWebChatMessageRequest(BaseModel):
    message: str
    conversation_id: str
    message_id: str


class PiWebChatActionRequest(BaseModel):
    action: str
    conversation_id: str
    message_id: str
    payload: dict = Field(default_factory=dict)


class PiPrepareQueryRequest(BaseModel):
    question: str
    dialect: Optional[str] = None
    idempotency_key: Optional[str] = None
    run_async: bool = False


class PiSkillStageRequest(BaseModel):
    message: str
    idempotency_key: Optional[str] = None
    run_async: bool = False


class PiAdvisorySkillRequest(BaseModel):
    skill_name: str
    prompt: str
    idempotency_key: str


class PiAnalyzeRequest(BaseModel):
    question: Optional[str] = None
    idempotency_key: Optional[str] = None
    run_async: bool = False


class PiRenderReportRequest(BaseModel):
    audience: str
    idempotency_key: Optional[str] = None
    run_async: bool = False


class PiSupplementRequest(BaseModel):
    suggested_query_index: int
    idempotency_key: str


class PiResumeAnalysisRequest(BaseModel):
    child_task_run_id: str
    idempotency_key: str
    run_async: bool = False


class PiApproveQueryRequest(BaseModel):
    query_run_id: str
    sql_hash: str
    idempotency_key: str
    run_async: bool = False


async def _pi_scoped_task_get(task_run_id: str, suffix: str = "") -> tuple[int, dict]:
    from web.diagnostics import error_body, upstream_error
    task_status, task_data = await _pi_request("GET", f"/v1/tasks/{task_run_id}")
    if task_status != 200:
        return upstream_error(task_status, task_data)
    task = task_data.get("task")
    if not isinstance(task, dict):
        return 502, error_body(502, "upstream_contract_invalid")
    if not _web_admin_can_observe(task):
        return 404, error_body(404)
    if not suffix:
        return task_status, task_data
    return await _pi_request("GET", f"/v1/tasks/{task_run_id}/{suffix}")


@router.post("/api/pi/chat/messages", response_class=JSONResponse)
async def api_pi_web_chat_message(
    req: PiWebChatMessageRequest,
    _auth=Depends(require_api_auth),
):
    """Submit one authenticated Web message through the shared ChannelEvent ingress."""
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    message = req.message.strip()
    if not message or len(message) > 20_000:
        return error_response(400, 'invalid_request')
    if not _valid_web_event_id(req.conversation_id) or not _valid_web_event_id(req.message_id):
        return error_response(400, 'invalid_request')
    status, data = await _pi_request(
        "POST",
        "/v1/channel-events",
        {
            "event_id": req.message_id,
            "channel": "web",
            "event_type": "message",
            "external_user_id": "web_admin",
            "conversation_id": req.conversation_id,
            "message_id": req.message_id,
            "task_run_id": None,
            "payload": {"text": message, "chat_type": "web"},
        },
    )
    return JSONResponse(data, status_code=status)


@router.get(
    "/api/pi/chat/tasks/{task_run_id}/presentation",
    response_class=JSONResponse,
)
async def api_pi_web_chat_presentation(
    task_run_id: str,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return error_response(400, 'invalid_request')
    task_status, task_data = await _pi_scoped_task_get(task_run_id)
    task = task_data.get("task") if isinstance(task_data, dict) else None
    if task_status != 200:
        return error_response(task_status, task_data.get("code"))
    if task.get("channel") != "web" or task.get("user_id") != "web_admin":
        return error_response(404)
    status, data = await _pi_request("GET", f"/v1/tasks/{task_run_id}/presentation")
    return JSONResponse(data, status_code=status)


@router.get(
    "/api/pi/chat/tasks/{task_run_id}/flow",
    response_class=JSONResponse,
)
async def api_pi_web_chat_task_flow(
    task_run_id: str,
    request: Request,
    _auth=Depends(require_api_auth),
):
    """Return a minimal read-only Plan/Event/Attempt projection for Web chat."""
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return error_response(400, 'invalid_request')
    raw_after = request.query_params.get("after", "0")
    if not raw_after.isdigit() or len(raw_after) > 12:
        return error_response(400, 'invalid_request')
    after = int(raw_after)
    task_status, task_data = await _pi_scoped_task_get(task_run_id)
    task = task_data.get("task") if isinstance(task_data, dict) else None
    if task_status != 200:
        return error_response(task_status, task_data.get("code"))
    if task.get("channel") != "web" or task.get("user_id") != "web_admin":
        return error_response(404)
    event_result, artifact_result, attempt_result = await asyncio.gather(
        _pi_request("GET", f"/v1/tasks/{task_run_id}/events?after={after}"),
        _pi_request("GET", f"/v1/tasks/{task_run_id}/artifacts"),
        _pi_request("GET", f"/v1/tasks/{task_run_id}/attempts"),
    )
    if any(status != 200 for status, _ in (event_result, artifact_result, attempt_result)):
        return error_response(502, 'upstream_unavailable')
    events = _bounded_web_task_events(event_result[1].get("events"))
    plan = _bounded_web_execution_plan(artifact_result[1].get("artifacts"))
    attempts = _bounded_web_stage_attempts(attempt_result[1].get("attempts"))
    return JSONResponse({
        "status": "ok",
        "task": {
            "task_run_id": task_run_id,
            "status": str(task.get("status") or "unknown")[:64],
            "current_stage": str(task.get("current_stage") or "")[:128],
            "updated_at": str(task.get("updated_at") or "")[:64],
        },
        "plan": plan,
        "events": events,
        "attempts": attempts,
        "last_event_sequence": max((event["sequence"] for event in events), default=after),
    })


@router.post(
    "/api/pi/chat/tasks/{task_run_id}/actions",
    response_class=JSONResponse,
)
async def api_pi_web_chat_action(
    task_run_id: str,
    req: PiWebChatActionRequest,
    _auth=Depends(require_api_auth),
):
    """Forward only presentation-declared Web actions through shared ChannelEvent handling."""
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return error_response(400, 'invalid_request')
    allowed_actions = {
        "provide_input", "approve_query", "cancel_task", "request_supplement",
        "analyze", "render_report", "confirm_memory",
    }
    if req.action not in allowed_actions:
        return error_response(400, 'invalid_request')
    if not _valid_web_event_id(req.conversation_id) or not _valid_web_event_id(req.message_id):
        return error_response(400, 'invalid_request')
    task_status, task_data = await _pi_scoped_task_get(task_run_id)
    task = task_data.get("task") if isinstance(task_data, dict) else None
    if task_status != 200:
        return error_response(task_status, task_data.get("code"))
    if task.get("channel") != "web" or task.get("user_id") != "web_admin":
        return error_response(404)
    presentation_status, presentation_data = await _pi_request(
        "GET", f"/v1/tasks/{task_run_id}/presentation"
    )
    if presentation_status != 200:
        return error_response(presentation_status, presentation_data.get("code"))
    if not isinstance(presentation_data.get("presentation"), dict):
        return error_response(502, "upstream_contract_invalid")
    presentation = (
        presentation_data.get("presentation")
        if presentation_status == 200 and isinstance(presentation_data, dict)
        else None
    )
    declared_actions = (
        presentation.get("actions", []) if isinstance(presentation, dict) else []
    )
    allowed_extra = {"text"} if req.action == "provide_input" else set()
    declared = None
    for item in declared_actions:
        if (
            not isinstance(item, dict)
            or item.get("type") != req.action
            or item.get("task_run_id") != task_run_id
        ):
            continue
        candidate_payload = item.get("payload")
        if not isinstance(candidate_payload, dict):
            candidate_payload = {}
        if (
            all(req.payload.get(key) == value for key, value in candidate_payload.items())
            and not (set(req.payload) - set(candidate_payload) - allowed_extra)
        ):
            declared = item
            break
    if declared is None:
        return error_response(409, 'conflict')
    if req.action == "provide_input" and not str(req.payload.get("text") or "").strip():
        return error_response(400, 'invalid_request')
    task_conversation_id = task.get("channel_conversation_id")
    if not isinstance(task_conversation_id, str) or not _valid_web_event_id(task_conversation_id):
        return error_response(409, 'conflict')
    action_message_id = f"web_card_{task_run_id}"
    event_id = _web_action_event_id(
        action_message_id,
        task_run_id,
        req.action,
        req.payload,
    )
    status, data = await _pi_request(
        "POST",
        "/v1/channel-events",
        {
            "event_id": event_id,
            "channel": "web",
            "event_type": "action",
            "external_user_id": "web_admin",
            "conversation_id": task_conversation_id,
            "message_id": action_message_id,
            "task_run_id": task_run_id,
            "payload": {"action": req.action, **req.payload},
        },
    )
    return JSONResponse(data, status_code=status)


@router.get("/api/pi/tasks", response_class=JSONResponse)
async def api_pi_list_tasks(
    channel: str | None = None,
    status: str | None = None,
    limit: int = 50,
    _auth=Depends(require_api_auth),
):
    """List the authenticated admin team's cross-channel TaskRuns."""
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    allowed_channels = {"web", "feishu", "dingtalk", "api"}
    allowed_statuses = {
        "created", "clarifying", "ready_for_query", "waiting_for_query_approval",
        "waiting_for_action_approval", "querying", "ready_for_analysis", "analyzing",
        "ready_for_report", "rendering", "completed", "needs_input", "incomplete",
        "cancelled", "failed", "expired",
    }
    if channel is not None and channel not in allowed_channels:
        return error_response(400, 'invalid_request')
    if status is not None and status not in allowed_statuses:
        return error_response(400, 'invalid_request')
    if limit < 1 or limit > 100:
        return error_response(400, 'invalid_request')
    scopes = _web_admin_task_scopes()
    if not scopes:
        return error_response(503, 'misconfigured')
    tasks_by_id: dict[str, dict] = {}
    for org_id, team_id in scopes:
        query = {
            "org_id": org_id,
            "team_id": team_id,
            "limit": str(limit),
            **({} if channel is None else {"channel": channel}),
            **({} if status is None else {"status": status}),
        }
        upstream_status, data = await _pi_request("GET", f"/v1/tasks?{urlencode(query)}")
        if upstream_status != 200:
            return JSONResponse(data, status_code=upstream_status)
        for task in data.get("tasks", []):
            if isinstance(task, dict) and isinstance(task.get("task_run_id"), str):
                tasks_by_id[task["task_run_id"]] = task
    tasks = sorted(
        tasks_by_id.values(),
        key=lambda task: (str(task.get("updated_at", "")), str(task.get("task_run_id", ""))),
        reverse=True,
    )[:limit]
    return JSONResponse({"tasks": tasks})


@router.post("/api/pi/tasks", response_class=JSONResponse)
async def api_pi_create_task(req: PiTaskCreateRequest, _auth=Depends(require_api_auth)):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    status, data = await _pi_request(
        "POST",
        "/v1/tasks",
        {
            "message": req.message,
            # Integration Spike 只有管理员 Web 渠道；不信任浏览器提交身份。
            # 正式 org/team/user 映射在 Phase 2 身份层完成。
            "user_id": "web_admin",
            "org_id": "org_default",
            "team_id": "team_default",
            "intent": req.intent,
            "channel": "web",
            "channel_conversation_id": req.channel_conversation_id,
        },
    )
    return JSONResponse(data, status_code=status)


@router.post(
    "/api/pi/tasks/{task_run_id}/prepare-query",
    response_class=JSONResponse,
)
async def api_pi_prepare_query(
    task_run_id: str,
    req: PiPrepareQueryRequest,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return error_response(400, 'invalid_request')
    status, data = await _pi_request(
        "POST",
        f"/v1/tasks/{task_run_id}/prepare-query",
        _pi_stage_payload(req),
    )
    return JSONResponse(data, status_code=status)


@router.post(
    "/api/pi/tasks/{task_run_id}/clarify",
    response_class=JSONResponse,
)
async def api_pi_clarify_task(
    task_run_id: str,
    req: PiSkillStageRequest,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return error_response(400, 'invalid_request')
    status, data = await _pi_request(
        "POST", f"/v1/tasks/{task_run_id}/clarify", _pi_stage_payload(req)
    )
    return JSONResponse(data, status_code=status)


@router.post(
    "/api/pi/tasks/{task_run_id}/review-metric",
    response_class=JSONResponse,
)
async def api_pi_review_metric(
    task_run_id: str,
    req: PiSkillStageRequest,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return error_response(400, 'invalid_request')
    status, data = await _pi_request(
        "POST", f"/v1/tasks/{task_run_id}/review-metric", _pi_stage_payload(req)
    )
    return JSONResponse(data, status_code=status)


@router.get("/api/pi/tasks/{task_run_id}", response_class=JSONResponse)
async def api_pi_task(
    task_run_id: str,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return error_response(400, 'invalid_request')
    upstream_status, data = await _pi_scoped_task_get(task_run_id)
    return JSONResponse(data, status_code=upstream_status)


@router.get(
    "/api/pi/tasks/{task_run_id}/attempts",
    response_class=JSONResponse,
)
async def api_pi_task_attempts(
    task_run_id: str,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return error_response(400, 'invalid_request')
    upstream_status, data = await _pi_scoped_task_get(task_run_id, "attempts")
    return JSONResponse(data, status_code=upstream_status)


@router.get(
    "/api/pi/tasks/{task_run_id}/artifacts",
    response_class=JSONResponse,
)
async def api_pi_task_artifacts(
    task_run_id: str,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return error_response(400, 'invalid_request')
    upstream_status, data = await _pi_scoped_task_get(task_run_id, "artifacts")
    return JSONResponse(data, status_code=upstream_status)


@router.post(
    "/api/pi/tasks/{task_run_id}/supplements",
    response_class=JSONResponse,
)
async def api_pi_create_supplement(
    task_run_id: str,
    req: PiSupplementRequest,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return error_response(400, 'invalid_request')
    status, data = await _pi_request(
        "POST", f"/v1/tasks/{task_run_id}/supplements", req.model_dump()
    )
    return JSONResponse(data, status_code=status)


@router.post(
    "/api/pi/tasks/{task_run_id}/resume-analysis",
    response_class=JSONResponse,
)
async def api_pi_resume_analysis(
    task_run_id: str,
    req: PiResumeAnalysisRequest,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return error_response(400, 'invalid_request')
    status, data = await _pi_request(
        "POST", f"/v1/tasks/{task_run_id}/resume-analysis", _pi_stage_payload(req)
    )
    return JSONResponse(data, status_code=status)


@router.post(
    "/api/pi/tasks/{task_run_id}/run-skill",
    response_class=JSONResponse,
)
async def api_pi_run_skill(
    task_run_id: str,
    req: PiAdvisorySkillRequest,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return error_response(400, 'invalid_request')
    status, data = await _pi_request(
        "POST", f"/v1/tasks/{task_run_id}/run-skill", req.model_dump(exclude_none=True)
    )
    return JSONResponse(data, status_code=status)


@router.post(
    "/api/pi/tasks/{task_run_id}/analyze",
    response_class=JSONResponse,
)
async def api_pi_analyze_task(
    task_run_id: str,
    req: PiAnalyzeRequest,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return error_response(400, 'invalid_request')
    status, data = await _pi_request(
        "POST", f"/v1/tasks/{task_run_id}/analyze", _pi_stage_payload(req)
    )
    return JSONResponse(data, status_code=status)


@router.post(
    "/api/pi/tasks/{task_run_id}/render-report",
    response_class=JSONResponse,
)
async def api_pi_render_report(
    task_run_id: str,
    req: PiRenderReportRequest,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return error_response(400, 'invalid_request')
    status, data = await _pi_request(
        "POST", f"/v1/tasks/{task_run_id}/render-report", _pi_stage_payload(req)
    )
    return JSONResponse(data, status_code=status)


@router.post(
    "/api/pi/tasks/{task_run_id}/approve-query",
    response_class=JSONResponse,
)
async def api_pi_approve_query(
    task_run_id: str,
    req: PiApproveQueryRequest,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return error_response(400, 'invalid_request')
    status, data = await _pi_request(
        "POST",
        f"/v1/tasks/{task_run_id}/approve-query",
        _pi_stage_payload(req),
    )
    return JSONResponse(data, status_code=status)


@router.get("/api/pi/tasks/{task_run_id}/events", response_class=JSONResponse)
async def api_pi_task_events(
    task_run_id: str,
    after: int = 0,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return error_response(400, 'invalid_request')
    task_status, task_data = await _pi_scoped_task_get(task_run_id)
    if task_status != 200:
        return JSONResponse(task_data, status_code=task_status)
    upstream_status, data = await _pi_request(
        "GET",
        f"/v1/tasks/{task_run_id}/events?after={max(after, 0)}",
    )
    return JSONResponse(data, status_code=upstream_status)
