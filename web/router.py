"""
Forge web UI — FastAPI router aggregator.

Routes
------
## Chat（查询对话）
GET  /chat                           → Web Chat（统一 Pi ChannelEvent / TaskRun）
POST /api/chat                       → 默认 410；仅显式回滚开关恢复旧 Agent API
POST /api/prepare-query              → 外部 Agent 生成可审核 SQL（不执行）
POST /api/approve                    → 确认 SQL
POST /api/cancel                     → 取消 SQL

## Admin（管理后台，挂载在 /admin 前缀下）
GET  /admin                          → redirect to /admin/dashboard
GET  /admin/schema                   → registry overview (tables)
POST /admin/metrics/metric           → add or update a metric definition
DELETE /admin/metrics/metric/{name}  → delete a metric
GET  /admin/semantic                 → 语义规则（歧义消除规则 + 字段使用约定）
GET  /admin/staging                  → staging 歧义确认队列
POST /admin/staging/promote/{name}   → 合并单条 staging 记录
POST /admin/staging/promote-all      → 合并全部 staging 记录
POST /admin/staging/discard/{name}   → 丢弃单条 staging 记录
GET  /admin/audit                    → recent audit log
GET  /admin/settings                 → current config (secrets masked)

Route implementations live in web/routes/* (REQ-2026-09-21-069 split);
shared helpers live in web/router_support.py, web/pi_projections.py,
web/admin_registry_store.py, web/product_content.py and web/admin_ai.py.
"""
from __future__ import annotations

import hmac

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from config import cfg
from web.auth import (
    require_web_auth,
    set_session_cookie,
    clear_session_cookie,
)
from web.templates import templates
from web.routes.enforce import router as enforce_router
from web.routes.explain import router as explain_router
from web.routes.query_runs import router as query_runs_router
from web.routes.evaluate import router as evaluate_router
from web.routes.context import router as context_router
from web.routes.reports import router as reports_router
from web.routes.memory import router as memory_router
from web.routes.settings import router as settings_router
from web.routes.registry_studio import router as registry_studio_router
from web.routes.product import router as product_router
from web.routes.deliveries import router as deliveries_router
from web.routes.accuracy_benchmark import router as accuracy_benchmark_router
from web.routes.benchmark_v2 import router as benchmark_v2_router
from web.routes.shell_pages import router as shell_pages_router
from web.routes.pi import router as pi_router
from web.routes.legacy_agent import router as legacy_agent_router
from web.routes.workspace_api import router as workspace_api_router
from web.admin_ai import router as admin_ai_router
from web.routes.admin_overview import router as admin_overview_router
from web.routes.admin_semantics import router as admin_semantics_router
from web.routes.admin_staging import router as admin_staging_router
from web.routes.admin_knowledge import router as admin_knowledge_router
from web.routes.admin_teams import router as admin_teams_router
from web.routes.admin_memory import router as admin_memory_router

# Chat / API 路由 — 挂载在根级别
chat_router = APIRouter()
chat_router.include_router(query_runs_router)
chat_router.include_router(evaluate_router)
chat_router.include_router(context_router)
chat_router.include_router(enforce_router)
chat_router.include_router(explain_router)
chat_router.include_router(reports_router)
chat_router.include_router(memory_router)
chat_router.include_router(product_router)
chat_router.include_router(benchmark_v2_router)
chat_router.include_router(deliveries_router)
chat_router.include_router(shell_pages_router)
chat_router.include_router(pi_router)
chat_router.include_router(legacy_agent_router)
chat_router.include_router(workspace_api_router)
chat_router.include_router(admin_ai_router)
# Admin 路由 — 挂载在 /admin 前缀下（全部路由需要 Web 登录验证）
router = APIRouter(dependencies=[Depends(require_web_auth)])
router.include_router(settings_router)
router.include_router(registry_studio_router)
router.include_router(accuracy_benchmark_router)
router.include_router(admin_overview_router)
router.include_router(admin_semantics_router)
router.include_router(admin_staging_router)
router.include_router(admin_knowledge_router)
router.include_router(admin_teams_router)
router.include_router(admin_memory_router)


# ── 认证路由（login / logout）─────────────────────────────────────────────────

def _safe_next_path(next_path: str) -> str:
    """Allow redirects only to local absolute paths."""
    if not next_path or not next_path.startswith("/") or next_path.startswith("//"):
        return "/chat"
    return next_path


@chat_router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, next: str = "/chat"):
    return templates.TemplateResponse(
        request, "login.html", {"error": None, "next": next}
    )


@chat_router.post("/login", response_class=HTMLResponse)
async def login_submit(
    request: Request,
    password: str = Form(...),
    next: str = Form(default="/chat"),
):
    expected = cfg.AUTH_ADMIN_PASSWORD
    # auth disabled 时任意密码均可通过；auth enabled 时必须配置并匹配密码
    if not cfg.AUTH_ENABLED or (expected and hmac.compare_digest(password, expected)):
        response = RedirectResponse(url=_safe_next_path(next), status_code=303)
        set_session_cookie(response, "admin")
        return response
    return templates.TemplateResponse(
        request,
        "login.html",
        {"error": "密码错误，请重试", "next": next},
        status_code=401,
    )


@chat_router.get("/logout")
async def logout():
    response = RedirectResponse(url="/login", status_code=302)
    clear_session_cookie(response)
    return response
