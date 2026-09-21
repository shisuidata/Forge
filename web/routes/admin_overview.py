"""Admin overview pages: root redirect, dashboard, schema, audit, sessions, pipelines."""
from __future__ import annotations

import json
import logging
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from agent import audit
from config import cfg
from web.admin_registry_store import (
    _load_conventions,
    _load_disambiguations,
    _load_metrics,
    _load_schema,
)
from web.pi_projections import _web_admin_task_scopes
from web.templates import templates

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_class=RedirectResponse)
async def admin_root():
    return RedirectResponse(url="/admin/dashboard", status_code=302)


@router.get("/architecture", response_class=HTMLResponse)
async def architecture_atlas():
    """Serve the standalone architecture atlas behind admin authentication."""
    path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "architecture-diagrams"
        / "forge-platform-architecture.html"
    )
    if not path.exists():
        return HTMLResponse("架构图尚未生成。", status_code=404)
    return HTMLResponse(path.read_text(encoding="utf-8"))


# ── Dashboard（概览）──────────────────────────────────────────────────────────

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    schema = _load_schema()
    tables = schema.get("tables", {})
    metrics = _load_metrics()
    disambiguations = _load_disambiguations()
    conventions = _load_conventions()

    health = {"db": False, "embedding": False}
    try:
        if cfg.DATABASE_URL:
            from sqlalchemy import create_engine, text as sa_text

            engine = create_engine(cfg.DATABASE_URL)
            with engine.connect() as conn:
                conn.execute(sa_text("SELECT 1"))
            health["db"] = True
    except Exception:
        pass
    health["embedding"] = bool(cfg.EMBED_API_KEY)

    audit_view = await audit.projection(scopes=_web_admin_task_scopes(), limit=5)
    today_count = audit_view["today"]
    recent_queries = audit_view["records"]
    governed_runs = []
    governed_runs_available = True
    try:
        from forge.explain import ExplainError, project_explain_query_run
        from forge.query_runs import list_governed_query_runs

        for run in await list_governed_query_runs(limit=8):
            try:
                explanation = project_explain_query_run(run)
                governed_runs.append(
                    {
                        "query_run_id": explanation["query_run_id"],
                        "question": explanation["statement"]["question"],
                        "status": explanation["status"],
                        "integrity": explanation["integrity"]["status"],
                        "evidence_count": len(explanation["evidence"]),
                        "limitation_count": len(explanation["limitations"]),
                        "updated_at": run["updated_at"],
                        "error_code": None,
                    }
                )
            except ExplainError as exc:
                governed_runs.append(
                    {
                        "query_run_id": run["query_run_id"],
                        "question": run["question"],
                        "status": "evidence_error",
                        "integrity": "failed",
                        "evidence_count": 0,
                        "limitation_count": 0,
                        "updated_at": run["updated_at"],
                        "error_code": exc.code,
                    }
                )
    except Exception:
        governed_runs = []
        governed_runs_available = False

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "table_count": len(tables),
            "metric_count": len(metrics),
            "rule_count": len(disambiguations) + len(conventions),
            "today_query_count": today_count,
            "health": health,
            "llm_model": cfg.LLM_MODEL or "",
            "embed_model": cfg.EMBED_MODEL or "",
            "registry_path": str(cfg.REGISTRY_PATH),
            "recent_queries": recent_queries,
            "governed_runs": governed_runs,
            "governed_runs_available": governed_runs_available,
            "governance_coverage": "3 / 14",
        },
    )

# ── 结构层（表 / 字段）─────────────────────────────────────────────────────────

@router.get("/schema", response_class=HTMLResponse)
async def schema_page(request: Request):
    schema = _load_schema()
    tables = schema.get("tables", {})
    return templates.TemplateResponse(
            request,
            "schema.html",
            {"tables": tables},
        )


# 兼容旧路由
@router.get("/registry", response_class=RedirectResponse)
async def registry_redirect():
    return RedirectResponse(url="/admin/schema", status_code=302)


@router.get("/audit", response_class=HTMLResponse)
async def audit_page(request: Request, status: str = "", q: str = "", page: int = 1):
    per_page = 50
    page = max(1, page)
    view = await audit.projection(scopes=_web_admin_task_scopes(), status=status, keyword=q, limit=per_page, offset=(page - 1) * per_page)
    return templates.TemplateResponse(request, "audit.html", {
        **view, "filter_status": status, "filter_q": q, "page": page,
        "total_pages": max(1, (view["available_total"] + per_page - 1) // per_page),
    })


@router.get("/sessions", response_class=HTMLResponse)
async def sessions_page(request: Request, sid: str = ""):
    """对话日志页面：Session 列表 + 单 Session 详情。"""
    from agent.memory import memory
    if sid:
        # 单个 session 详情
        messages = memory.ems.get_full_session(sid)
        return templates.TemplateResponse(
                request,
                "sessions.html",
                {"session_id": sid, "messages": messages, "sessions": []},
            )
    else:
        # session 列表（聚合所有用户）
        try:
            conn = memory.ems._ensure_conn()
            rows = conn.execute(
                "SELECT session_id, user_id, MIN(created_at) as started, MAX(created_at) as ended, COUNT(*) as msg_count "
                "FROM memory_ems WHERE role != 'state' "
                "GROUP BY session_id ORDER BY ended DESC LIMIT 50"
            ).fetchall()
            sessions = [
                {"session_id": r[0], "user_id": r[1], "started": r[2], "ended": r[3], "msg_count": r[4]}
                for r in rows
            ]
        except Exception:
            sessions = []
        return templates.TemplateResponse(
                request,
                "sessions.html",
                {"session_id": "", "messages": [], "sessions": sessions},
            )


@router.get("/pipelines", response_class=HTMLResponse)
async def pipelines_page(request: Request):
    """Pipeline 执行视图。从 EMS 聚合所有查询活动。"""
    from agent.memory import memory
    runs = []
    try:
        conn = memory.ems._ensure_conn()

        # 1. 读取 PipelineRunner 产生的记录
        pr_rows = conn.execute(
            "SELECT tool_output FROM memory_ems "
            "WHERE tool_name = 'pipeline_complete' AND tool_output IS NOT NULL "
            "ORDER BY id DESC LIMIT 30"
        ).fetchall()
        for row in pr_rows:
            try:
                data = json.loads(row[0])
                data["total_ms"] = sum(s.get("duration_ms", 0) for s in data.get("stages", []))
                runs.append(data)
            except (json.JSONDecodeError, TypeError):
                continue

        # 2. 读取直接走 agent.process() 的查询（按 session 聚合）
        query_rows = conn.execute(
            """SELECT
                e.session_id, e.user_id,
                u.content as question,
                e.tool_output as sql,
                e.action,
                e.created_at,
                u.created_at as asked_at
            FROM memory_ems e
            INNER JOIN (
                SELECT session_id, MAX(id) as last_user_id, content, created_at
                FROM memory_ems
                WHERE role = 'user' AND content != '' AND action IS NULL
                GROUP BY session_id
            ) u ON e.session_id = u.session_id
            WHERE e.tool_name = 'generate_forge_query' AND e.action = 'sql_review'
            ORDER BY e.id DESC LIMIT 50"""
        ).fetchall()

        seen_sessions = {r.get("run_id", "") for r in runs}
        for row in query_rows:
            sid, uid, question, sql, action, created_at, asked_at = row
            if sid in seen_sessions:
                continue
            seen_sessions.add(sid)

            # 查找该 session 里是否有 approve/cancel
            status_row = conn.execute(
                "SELECT action FROM memory_ems "
                "WHERE session_id = ? AND action IN ('approved','cancelled') "
                "ORDER BY id DESC LIMIT 1",
                (sid,),
            ).fetchone()
            final_status = "completed" if status_row and status_row[0] == "approved" else (
                "cancelled" if status_row and status_row[0] == "cancelled" else "pending_approval"
            )

            runs.append({
                "run_id": sid,
                "pipeline": "query",
                "user_id": uid or "",
                "team_id": "",
                "question": question or "",
                "status": final_status,
                "started_at": asked_at or created_at or "",
                "ended_at": created_at or "",
                "total_ms": 0,
                "stages": [
                    {"stage": "generate", "agent": "forge_query",
                     "status": "completed", "duration_ms": 0, "error": None},
                ],
            })

        # 按时间倒序
        runs.sort(key=lambda r: r.get("started_at", ""), reverse=True)
        runs = runs[:50]

    except Exception as exc:
        logger.warning("Pipeline page error: %s", exc)
        runs = []

    return templates.TemplateResponse(
            request,
            "pipelines.html",
            {"runs": runs},
        )
