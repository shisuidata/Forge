"""Product Shell GET pages (top-level navigation surfaces)."""
from __future__ import annotations

import re

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from config import cfg
from web.auth import require_web_auth
from web.product_content import product_state_page, product_surface_page
from web.templates import templates

router = APIRouter()


@router.get("/chat", response_class=HTMLResponse)
async def chat_page(request: Request, _auth=Depends(require_web_auth)):
    """First-class Web channel backed by Pi ChannelEvent and TaskRun contracts."""
    return templates.TemplateResponse(
        request,
        "product_chat.html",
        {
            "active": "chat",
            "page_title": "对话",
            "page_eyebrow": "连续交互",
            "pi_enabled": cfg.PI_ORCHESTRATOR_ENABLED,
        },
    )


@router.get("/tasks", response_class=HTMLResponse)
async def task_workspace_page(request: Request, _auth=Depends(require_web_auth)):
    """Canonical Pi task UI with Artifact rendering and non-executable SQL review."""
    return templates.TemplateResponse(
        request,
        "product_tasks.html",
        {
            "active": "tasks",
            "page_title": "任务",
            "page_eyebrow": "执行与恢复",
            "pi_enabled": cfg.PI_ORCHESTRATOR_ENABLED,
        },
    )


@router.get("/workspace", response_class=HTMLResponse)
async def product_workspace_page(request: Request, _auth=Depends(require_web_auth)):
    return templates.TemplateResponse(
        request,
        "product_workspace.html",
        {"active": "workspace", "page_title": "工作台", "page_eyebrow": "当前工作"},
    )


@router.get("/tasks/{task_run_id}", response_class=HTMLResponse)
async def product_task_detail_page(
    task_run_id: str,
    request: Request,
    _auth=Depends(require_web_auth),
):
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return product_state_page(request, "not_found", 404)
    return templates.TemplateResponse(
        request,
        "product_task_detail.html",
        {
            "active": "tasks",
            "page_title": "任务详情",
            "page_eyebrow": "状态、证据与操作",
            "task_run_id": task_run_id,
        },
    )


@router.get("/deliverables", response_class=HTMLResponse)
@router.get("/reports", response_class=HTMLResponse)
async def product_report_library_page(request: Request, _auth=Depends(require_web_auth)):
    return templates.TemplateResponse(
        request,
        "product_reports.html",
        {"active": "deliverables", "page_title": "交付", "page_eyebrow": "报告、导出与复用"},
    )


@router.get("/deliverables/{surface_key}", response_class=HTMLResponse)
async def product_deliverable_surface_page(
    surface_key: str,
    request: Request,
    _auth=Depends(require_web_auth),
):
    return product_surface_page(request, f"deliverables/{surface_key}")


@router.get("/data", response_class=HTMLResponse)
async def product_data_page(request: Request, _auth=Depends(require_web_auth)):
    return templates.TemplateResponse(
        request,
        "product_data.html",
        {"active": "data", "page_title": "数据资产", "page_eyebrow": "结构、语义与质量"},
    )


@router.get("/data/{surface_key}", response_class=HTMLResponse)
async def product_data_surface_page(
    surface_key: str,
    request: Request,
    _auth=Depends(require_web_auth),
):
    return product_surface_page(request, f"data/{surface_key}")


@router.get("/governance", response_class=HTMLResponse)
async def product_governance_page(request: Request, _auth=Depends(require_web_auth)):
    return product_surface_page(request, "governance")


@router.get("/governance/{surface_key}", response_class=HTMLResponse)
async def product_governance_surface_page(
    surface_key: str,
    request: Request,
    _auth=Depends(require_web_auth),
):
    return product_surface_page(request, f"governance/{surface_key}")


@router.get("/runtime", response_class=HTMLResponse)
async def product_runtime_page(request: Request, _auth=Depends(require_web_auth)):
    return product_surface_page(request, "runtime")


@router.get("/runtime/{surface_key}", response_class=HTMLResponse)
async def product_runtime_surface_page(
    surface_key: str,
    request: Request,
    _auth=Depends(require_web_auth),
):
    return product_surface_page(request, f"runtime/{surface_key}")


@router.get("/manage", response_class=HTMLResponse)
async def product_manage_page(request: Request, _auth=Depends(require_web_auth)):
    return product_surface_page(request, "manage")


@router.get("/search", response_class=HTMLResponse)
async def product_search_page(request: Request, _auth=Depends(require_web_auth)):
    return product_surface_page(request, "search")


@router.get("/inbox", response_class=HTMLResponse)
async def product_inbox_page(request: Request, _auth=Depends(require_web_auth)):
    return product_surface_page(request, "inbox")


@router.get("/forbidden", response_class=HTMLResponse)
async def product_forbidden_state_page(request: Request, _auth=Depends(require_web_auth)):
    return product_state_page(request, "forbidden")


@router.get("/offline", response_class=HTMLResponse)
async def product_offline_state_page(request: Request, _auth=Depends(require_web_auth)):
    return product_state_page(request, "offline")
