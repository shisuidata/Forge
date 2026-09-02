"""Authenticated Product BFF over Pi, ReportStore, and Registry read models."""
from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import sqlite3
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote, urlencode

import httpx
import yaml
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from agent.contracts.product_projection_semantics import validate_product_projection
from config import cfg
from web.auth import require_api_auth
from web.pi_client import pi_request
from web.routes.reports import get_report_store

router = APIRouter(prefix="/api/product", dependencies=[Depends(require_api_auth)])

_SCOPE_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
_OBJECT_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]{1,256}$")
_SECRET_LINE = re.compile(
    r"(?:\b(?:api[_-]?key|password|secret|authorization)\s*[:=]\s*\S+|bearer\s+\S+|/(?:home|Users|tmp)/|Traceback \(most recent call last\))",
    re.IGNORECASE,
)
_TASK_STATUSES = {
    "created", "clarifying", "ready_for_query", "waiting_for_query_approval",
    "waiting_for_action_approval", "querying", "ready_for_analysis", "analyzing",
    "ready_for_report", "rendering", "completed", "needs_input", "incomplete",
    "cancelled", "failed", "expired",
}
_REPORT_STATUSES = {"publishing", "published", "failed"}


def _response(content: dict[str, Any], status_code: int = 200) -> JSONResponse:
    return JSONResponse(
        content,
        status_code=status_code,
        headers={"Cache-Control": "no-store"},
    )


def _configured_scopes() -> list[tuple[str, str]]:
    scopes: list[tuple[str, str]] = []
    for raw in cfg.PI_WEB_ADMIN_TASK_SCOPES.split(","):
        org_id, separator, team_id = raw.strip().partition(":")
        if (
            separator
            and _SCOPE_PATTERN.fullmatch(org_id)
            and _SCOPE_PATTERN.fullmatch(team_id)
        ):
            scopes.append((org_id, team_id))
    return scopes


def _scope(org_id: str | None, team_id: str | None) -> tuple[str, str, str]:
    scopes = _configured_scopes()
    if not scopes:
        raise HTTPException(status_code=503, detail="No valid Product scope is configured")
    if org_id is None and team_id is None and len(scopes) == 1:
        selected = scopes[0]
    elif org_id is None or team_id is None:
        raise HTTPException(status_code=400, detail="org_id and team_id must be provided together")
    elif not _SCOPE_PATTERN.fullmatch(org_id) or not _SCOPE_PATTERN.fullmatch(team_id):
        raise HTTPException(status_code=400, detail="Product scope is invalid")
    elif (org_id, team_id) not in set(scopes):
        raise HTTPException(status_code=404, detail="Product scope not found")
    else:
        selected = (org_id, team_id)
    return selected[0], selected[1], "web_admin"


def _safe_text(value: Any, maximum: int, fallback: str = "") -> str:
    if not isinstance(value, str):
        return fallback
    without_think = re.sub(r"<think>[\s\S]*?(?:</think>|$)", "", value, flags=re.IGNORECASE)
    visible = "\n".join(
        line for line in without_think.splitlines() if not _SECRET_LINE.search(line)
    ).strip()
    return (visible or fallback)[:maximum]


def _task_state(status: str) -> str:
    if status == "needs_input":
        return "needs_input"
    if status in {"waiting_for_query_approval", "waiting_for_action_approval"}:
        return "waiting_decision"
    if status in {"created", "clarifying", "querying", "analyzing", "rendering"}:
        return "running"
    if status == "incomplete":
        return "partial"
    if status in {"failed", "expired"}:
        return "failed"
    if status == "cancelled":
        return "cancelled"
    if status == "completed":
        return "completed"
    return "ready"


def _task_summary(task: dict[str, Any]) -> dict[str, Any] | None:
    task_id = task.get("task_run_id")
    status = task.get("status")
    if (
        not isinstance(task_id, str)
        or re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_id) is None
        or status not in _TASK_STATUSES
    ):
        return None
    created_at = task.get("created_at")
    updated_at = task.get("updated_at")
    if not isinstance(created_at, str) or not created_at or not isinstance(updated_at, str) or not updated_at:
        return None
    scope_values = [task.get("org_id"), task.get("team_id"), task.get("user_id"), task.get("channel")]
    if not all(isinstance(value, str) and value for value in scope_values):
        return None
    metadata = task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
    original_message = metadata.get("original_message")
    title = _safe_text(original_message, 200, str(task.get("intent") or "数据任务"))
    result = {
        "schema_version": 1,
        "projection_type": "task_summary_v1",
        "scope": {
            "org_id": task["org_id"],
            "team_id": task["team_id"],
            "user_id": task["user_id"],
            "channel": task["channel"],
        },
        "task_run_id": task_id,
        "conversation_id": task.get("channel_conversation_id")
        if isinstance(task.get("channel_conversation_id"), str)
        else None,
        "parent_task_run_id": task.get("parent_task_run_id")
        if isinstance(task.get("parent_task_run_id"), str)
        else None,
        "intent": _safe_text(task.get("intent"), 256, "data_task"),
        "title": title.splitlines()[0][:200] or "数据任务",
        "status": status,
        "display_state": _task_state(status),
        "current_stage": _safe_text(task.get("current_stage"), 256) or None,
        "created_at": created_at[:64],
        "updated_at": updated_at[:64],
        "href": f"/tasks/{task_id}",
        "projection_meta": {
            "availability": "ready",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source_revisions": [{"source": "pi_task_store", "revision": updated_at[:256]}],
            "unavailable_reasons": [],
            "redactions": ([{"field_path": "title", "reason_code": "sensitive_text_redacted"}]
                if isinstance(original_message, str) and _SECRET_LINE.search(original_message)
                else []),
        },
    }
    _validate_projection("task_summary_v1", result)
    return result


def _validate_projection(name: str, value: Any) -> None:
    errors = validate_product_projection(name, value)
    if errors:
        raise HTTPException(
            status_code=502,
            detail={"code": "upstream_contract_invalid", "reasons": errors[:8]},
        )


def _require_projection_scope(
    value: dict[str, Any],
    org_id: str,
    team_id: str,
    user_id: str,
    channel: str | None = None,
) -> None:
    expected = {"org_id": org_id, "team_id": team_id, "user_id": user_id}
    if channel is not None:
        expected["channel"] = channel
    scope = value.get("scope")
    if not isinstance(scope, dict) or any(scope.get(key) != item for key, item in expected.items()):
        raise HTTPException(
            status_code=502,
            detail={"code": "upstream_scope_mismatch"},
        )


def _pi_path(path: str, org_id: str, team_id: str, user_id: str, **query: Any) -> str:
    params = {
        "org_id": org_id,
        "team_id": team_id,
        "user_id": user_id,
        "channel": "web",
        **{key: value for key, value in query.items() if value is not None},
    }
    return f"{path}?{urlencode(params)}"


async def _pi_product_get(path: str) -> tuple[int, dict[str, Any]]:
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return 503, {"status": "offline", "error": "Pi Orchestrator is not enabled"}
    try:
        return await pi_request("GET", path)
    except httpx.HTTPError:
        return 502, {"status": "offline", "error": "Pi Orchestrator is unavailable"}


async def _tasks(
    org_id: str,
    team_id: str,
    user_id: str,
    *,
    limit: int = 100,
    status: str | None = None,
) -> tuple[bool, list[dict[str, Any]]]:
    code, body = await _pi_product_get(
        _pi_path("/v1/tasks", org_id, team_id, user_id, limit=limit, status=status)
    )
    raw = body.get("tasks") if isinstance(body, dict) else None
    if code != 200 or not isinstance(raw, list):
        return False, []
    summaries: list[dict[str, Any]] = []
    for item in raw:
        summary = _task_summary(item) if isinstance(item, dict) else None
        if summary is None:
            raise HTTPException(
                status_code=502,
                detail={"code": "upstream_contract_invalid", "resource": "task_list"},
            )
        _require_projection_scope(summary, org_id, team_id, user_id, "web")
        summaries.append(summary)
    return True, summaries


def _encode_cursor(updated_at: str, object_id: str) -> str:
    payload = json.dumps(
        {"updated_at": updated_at, "object_id": object_id},
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def _decode_cursor(cursor: str | None) -> tuple[str, str] | None:
    if cursor is None:
        return None
    if not cursor or len(cursor) > 512:
        raise HTTPException(status_code=400, detail="cursor is invalid")
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode())
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError, binascii.Error) as exc:
        raise HTTPException(status_code=400, detail="cursor is invalid") from exc
    if (
        not isinstance(payload, dict)
        or not isinstance(payload.get("updated_at"), str)
        or not isinstance(payload.get("object_id"), str)
        or len(payload["updated_at"]) > 64
        or _OBJECT_PATTERN.fullmatch(payload["object_id"]) is None
    ):
        raise HTTPException(status_code=400, detail="cursor is invalid")
    return payload["updated_at"], payload["object_id"]


def _report_summary(record: dict[str, Any], generated_at: str) -> dict[str, Any]:
    status = record.get("status")
    pdf_status = record.get("pdf_status")
    pptx_status = record.get("pptx_status")
    if status == "publishing":
        display_state = "running"
    elif status == "failed":
        display_state = "failed"
    else:
        display_state = "completed" if pdf_status == pptx_status == "ready" else "partial"
    result = {
        "schema_version": 1,
        "projection_type": "report_summary_v1",
        "scope": {
            "org_id": record.get("org_id"),
            "team_id": record.get("team_id"),
            "user_id": record.get("user_id"),
        },
        "report_id": record.get("report_id"),
        "task_run_id": record.get("task_run_id"),
        "revision": record.get("revision"),
        "title": _safe_text(record.get("title"), 200, "未命名报告"),
        "status": status,
        "display_state": display_state,
        "pdf_status": pdf_status,
        "pptx_status": pptx_status,
        "internal_url": record.get("internal_url"),
        "technical_url": record.get("technical_url"),
        "pdf_url": record.get("pdf_url"),
        "pptx_url": record.get("pptx_url"),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "projection_meta": {
            "availability": "ready",
            "generated_at": generated_at,
            "source_revisions": [{
                "source": "forge_report_store",
                "revision": f"{record.get('report_id')}:v{record.get('revision')}:{record.get('updated_at')}",
            }],
            "unavailable_reasons": [],
            "redactions": ([{"field_path": "error", "reason_code": "sensitive_error_redacted"}]
                if record.get("error") else []),
        },
    }
    _validate_projection("report_summary_v1", result)
    return result


def _list_reports(
    org_id: str,
    team_id: str,
    user_id: str,
    *,
    limit: int,
    status: str | None = None,
    cursor: str | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    before = _decode_cursor(cursor)
    rows = get_report_store().list(
        org_id=org_id,
        team_id=team_id,
        user_id=user_id,
        status=status,
        limit=limit + 1,
        before=before,
    )
    visible = rows[:limit]
    generated_at = datetime.now(timezone.utc).isoformat()
    reports = [_report_summary(row, generated_at) for row in visible]
    next_cursor = (
        _encode_cursor(visible[-1]["updated_at"], visible[-1]["report_id"])
        if len(rows) > limit and visible
        else None
    )
    return reports, next_cursor


def _registry_summary(org_id: str, team_id: str) -> dict[str, Any]:
    reasons: list[str] = []
    revision_parts: list[bytes] = []
    table_count = 0
    metric_count = 0
    try:
        schema_bytes = cfg.REGISTRY_PATH.read_bytes()
        revision_parts.extend([b"schema\0", schema_bytes])
        schema = json.loads(schema_bytes)
        tables = schema.get("tables", []) if isinstance(schema, dict) else []
        table_count = len(tables) if isinstance(tables, (list, dict)) else 0
    except (OSError, ValueError, json.JSONDecodeError):
        reasons.append("schema_registry_unavailable")
        revision_parts.append(b"schema:unavailable")
    try:
        metrics_bytes = cfg.METRICS_PATH.read_bytes()
        revision_parts.extend([b"metrics\0", metrics_bytes])
        metrics = yaml.safe_load(metrics_bytes) or {}
        if isinstance(metrics, dict):
            metric_values = metrics.get("metrics", metrics)
            metric_count = len(metric_values) if isinstance(metric_values, (list, dict)) else 0
    except (OSError, yaml.YAMLError):
        reasons.append("metric_registry_unavailable")
        revision_parts.append(b"metrics:unavailable")
    source_revision = "sha256:" + hashlib.sha256(b"\n".join(revision_parts)).hexdigest()
    return {
        "schema_version": 1,
        "scope": {"org_id": org_id, "team_id": team_id},
        "status": "partial" if reasons else "ready",
        "counts": {"tables": table_count, "metrics": metric_count},
        "links": [
            {"label": "数据结构", "href": "/admin/schema"},
            {"label": "业务指标", "href": "/admin/metrics"},
            {"label": "语义规则", "href": "/admin/semantic"},
            {"label": "Registry Studio", "href": "/admin/registry-studio"},
            {"label": "Staging 审核", "href": "/admin/staging"},
            {"label": "Knowledge 审核", "href": "/admin/knowledge"},
        ],
        "unavailable_reasons": reasons,
        "source_revision": source_revision,
    }


def _workspace_item(
    item_type: str,
    item_id: str,
    title: str,
    state: str,
    updated_at: str,
    href: str,
    reason: str | None,
) -> dict[str, Any]:
    return {
        "item_type": item_type,
        "item_id": item_id,
        "title": title[:200] or "未命名项目",
        "state": state,
        "updated_at": updated_at,
        "href": href,
        "reason": reason[:500] if isinstance(reason, str) else None,
    }


@router.get("/conversations")
async def list_conversations(
    org_id: str | None = None,
    team_id: str | None = None,
    limit: int = Query(default=20, ge=1, le=50),
    cursor: str | None = None,
):
    org_id, team_id, user_id = _scope(org_id, team_id)
    code, body = await _pi_product_get(
        _pi_path("/v1/conversations", org_id, team_id, user_id, limit=limit, cursor=cursor)
    )
    if code != 200:
        return _response(body, code if code in {400, 404, 409} else 502)
    conversations = body.get("conversations")
    if not isinstance(conversations, list):
        return _response({"status": "upstream_contract_invalid"}, 502)
    for item in conversations:
        _validate_projection("conversation_summary_v1", item)
        _require_projection_scope(item, org_id, team_id, user_id, "web")
    return _response(body)


@router.get("/conversations/{conversation_id}")
async def get_conversation(
    conversation_id: str,
    org_id: str | None = None,
    team_id: str | None = None,
    cursor: str | None = None,
):
    if _OBJECT_PATTERN.fullmatch(conversation_id) is None:
        return _response({"status": "invalid_request", "error": "conversation_id is invalid"}, 400)
    org_id, team_id, user_id = _scope(org_id, team_id)
    code, body = await _pi_product_get(
        _pi_path(
            f"/v1/conversations/{quote(conversation_id, safe='')}",
            org_id,
            team_id,
            user_id,
            cursor=cursor,
        )
    )
    if code != 200:
        return _response(body, code if code in {400, 404, 409} else 502)
    conversation = body.get("conversation")
    _validate_projection("conversation_detail_v1", conversation)
    _require_projection_scope(conversation, org_id, team_id, user_id, "web")
    return _response(body)


@router.get("/tasks")
async def list_tasks(
    org_id: str | None = None,
    team_id: str | None = None,
    status: str | None = None,
    limit: int = Query(default=50, ge=1, le=100),
):
    if status is not None and status not in _TASK_STATUSES:
        return _response({"status": "invalid_request", "error": "status is invalid"}, 400)
    org_id, team_id, user_id = _scope(org_id, team_id)
    available, tasks = await _tasks(org_id, team_id, user_id, limit=limit, status=status)
    if not available:
        return _response({"status": "offline", "error": "Pi Orchestrator is unavailable"}, 502)
    return _response({
        "schema_version": 1,
        "scope": {"org_id": org_id, "team_id": team_id, "user_id": user_id, "channel": "web"},
        "tasks": tasks,
        "bounded": True,
        "truncated_possible": len(tasks) == limit,
    })


@router.get("/tasks/{task_run_id}")
async def get_task(
    task_run_id: str,
    org_id: str | None = None,
    team_id: str | None = None,
):
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return _response({"status": "invalid_request", "error": "task_run_id is invalid"}, 400)
    org_id, team_id, user_id = _scope(org_id, team_id)
    code, body = await _pi_product_get(
        _pi_path(f"/v1/tasks/{task_run_id}/detail", org_id, team_id, user_id)
    )
    if code != 200:
        return _response(body, code if code in {400, 404, 409} else 502)
    detail = body.get("detail")
    _validate_projection("task_detail_projection_v1", detail)
    _require_projection_scope(detail, org_id, team_id, user_id, "web")
    return _response(body)


@router.get("/reports")
async def list_reports(
    org_id: str | None = None,
    team_id: str | None = None,
    status: str | None = None,
    limit: int = Query(default=50, ge=1, le=100),
    cursor: str | None = None,
):
    if status is not None and status not in _REPORT_STATUSES:
        return _response({"status": "invalid_request", "error": "status is invalid"}, 400)
    org_id, team_id, user_id = _scope(org_id, team_id)
    try:
        reports, next_cursor = await asyncio.to_thread(
            _list_reports,
            org_id,
            team_id,
            user_id,
            limit=limit,
            status=status,
            cursor=cursor,
        )
    except HTTPException:
        raise
    except (OSError, ValueError, sqlite3.Error):
        return _response({"status": "offline", "error": "Report index is unavailable"}, 502)
    return _response({
        "schema_version": 1,
        "reports": reports,
        "next_cursor": next_cursor,
    })


@router.get("/data-summary")
async def data_summary(org_id: str | None = None, team_id: str | None = None):
    org_id, team_id, _ = _scope(org_id, team_id)
    return _response(await asyncio.to_thread(_registry_summary, org_id, team_id))


@router.get("/workspace")
async def workspace(org_id: str | None = None, team_id: str | None = None):
    org_id, team_id, user_id = _scope(org_id, team_id)
    generated_at = datetime.now(timezone.utc).isoformat()
    task_result, report_result, registry = await asyncio.gather(
        _tasks(org_id, team_id, user_id, limit=100),
        asyncio.to_thread(_list_reports, org_id, team_id, user_id, limit=20),
        asyncio.to_thread(_registry_summary, org_id, team_id),
        return_exceptions=True,
    )
    dependencies: list[dict[str, Any]] = []
    source_revisions: list[dict[str, str]] = []
    tasks: list[dict[str, Any]] = []
    task_available = False
    if isinstance(task_result, tuple):
        task_available, tasks = task_result
    if task_available:
        latest_task_revision = max((task["updated_at"] for task in tasks), default="empty")
        source_revisions.append({"source": "pi_task_store", "revision": latest_task_revision})
        if len(tasks) == 100:
            dependencies.append(_workspace_item(
                "dependency", "task_list_bound", "任务列表达到读取上限", "partial",
                generated_at, "/tasks", "工作台只投影最近 100 个任务。",
            ))
    else:
        dependencies.append(_workspace_item(
            "dependency", "pi_orchestrator", "任务运行时暂不可用", "offline",
            generated_at, "/tasks", "对话和任务暂时无法读取。",
        ))
        source_revisions.append({"source": "pi_task_store", "revision": f"offline:{generated_at}"})

    reports: list[dict[str, Any]] = []
    report_available = not isinstance(report_result, BaseException)
    if report_available:
        reports, report_cursor = report_result
        latest_report_revision = max((report["updated_at"] for report in reports), default="empty")
        source_revisions.append({"source": "forge_report_store", "revision": latest_report_revision})
        if report_cursor is not None:
            dependencies.append(_workspace_item(
                "dependency", "report_list_bound", "报告列表达到读取上限", "partial",
                generated_at, "/reports", "工作台只投影最近 20 份报告。",
            ))
    else:
        dependencies.append(_workspace_item(
            "dependency", "report_store", "报告索引暂不可用", "offline",
            generated_at, "/reports", "任务仍可使用，报告列表暂时不可读取。",
        ))
        source_revisions.append({"source": "forge_report_store", "revision": f"offline:{generated_at}"})

    if isinstance(registry, BaseException):
        registry = _registry_summary(org_id, team_id)
    registry_available = isinstance(registry, dict) and registry.get("status") == "ready"
    if registry.get("status") != "ready":
        dependencies.append(_workspace_item(
            "dependency", "registry", "数据资产信息不完整", "partial",
            generated_at, "/data", "部分 Registry 文件不可读取。",
        ))
    source_revisions.append({"source": "forge_registry_store", "revision": registry["source_revision"]})

    needs_input = [task for task in tasks if task["display_state"] == "needs_input"]
    waiting = [task for task in tasks if task["display_state"] == "waiting_decision"]
    running = [task for task in tasks if task["display_state"] == "running"]
    failed = [task for task in tasks if task["display_state"] == "failed"]

    def task_items(items: list[dict[str, Any]], reason: str | None = None) -> list[dict[str, Any]]:
        return [
            _workspace_item(
                "task", task["task_run_id"], task["title"], task["display_state"],
                task["updated_at"], task["href"], reason,
            )
            for task in items[:20]
        ]

    report_items = [
        _workspace_item(
            "report", report["report_id"], report["title"], report["display_state"],
            report["updated_at"], report["internal_url"] or "/reports", None,
        )
        for report in reports[:20]
    ]
    availability = "ready" if not dependencies else (
        "offline" if not task_available and not report_available and not registry_available else "partial"
    )
    reasons = [
        item["item_id"] if item["item_id"].endswith("_bound")
        else item["item_id"] + "_unavailable"
        for item in dependencies
    ]
    projection = {
        "schema_version": 1,
        "projection_type": "workspace_projection_v1",
        "scope": {"org_id": org_id, "team_id": team_id, "user_id": user_id, "channel": "web"},
        "counts": {
            "needs_input": len(needs_input),
            "waiting_decision": len(waiting),
            "running": len(running),
            "failed": len(failed),
            "recent_reports": len(reports),
        },
        "needs_input": task_items(needs_input, "需要补充任务信息。"),
        "waiting_decision": task_items(waiting, "等待确认精确操作。"),
        "running": task_items(running),
        "failed": task_items(failed, "任务未完成，可查看恢复方式。"),
        "recent_reports": report_items,
        "dependencies": dependencies,
        "projection_meta": {
            "availability": availability,
            "generated_at": generated_at,
            "source_revisions": source_revisions,
            "unavailable_reasons": reasons,
            "redactions": [],
        },
    }
    _validate_projection("workspace_projection_v1", projection)
    return _response(projection)
