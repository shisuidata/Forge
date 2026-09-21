"""Admin memory management: SMP entries and EMS stats over raw SQLite reads."""
from __future__ import annotations

import json

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from web.templates import templates

router = APIRouter()


def _get_smp_entries(limit: int = 200) -> list[dict]:
    """读取所有 SMP 条目（不限 user，管理员视图）。"""
    try:
        from agent.db import get_connection_raw
        conn = get_connection_raw()
        rows = conn.execute(
            "SELECT id, scope, user_id, category, key, value, confidence, updated_at "
            "FROM memory_smp ORDER BY scope, category, updated_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        result = []
        for r in rows:
            try:
                import json
                val = json.loads(r[5])
            except Exception:
                val = r[5]
            result.append({
                "id": r[0], "scope": r[1], "user_id": r[2], "category": r[3],
                "key": r[4], "value": val, "confidence": r[6], "updated_at": r[7],
            })
        return result
    except Exception:
        return []


def _get_ems_stats() -> dict:
    """读取 EMS 统计数据。"""
    try:
        from agent.db import get_connection_raw
        conn = get_connection_raw()
        total_sessions = conn.execute(
            "SELECT COUNT(DISTINCT session_id) FROM memory_ems"
        ).fetchone()[0] or 0
        total_events = conn.execute(
            "SELECT COUNT(*) FROM memory_ems"
        ).fetchone()[0] or 0
        # 按用户统计
        user_rows = conn.execute(
            "SELECT user_id, COUNT(DISTINCT session_id) as sessions, MAX(created_at) as last_active "
            "FROM memory_ems GROUP BY user_id ORDER BY last_active DESC LIMIT 50"
        ).fetchall()
        users = [{"user_id": r[0], "sessions": r[1], "last_active": r[2]} for r in user_rows]
        return {
            "total_sessions": total_sessions,
            "total_events": total_events,
            "active_users": len(users),
            "users": users,
        }
    except Exception:
        return {"total_sessions": 0, "total_events": 0, "active_users": 0, "users": []}


@router.get("/memory", response_class=HTMLResponse)
async def memory_page(request: Request, flash: str = ""):
    import json
    smp_entries = _get_smp_entries()
    ems_stats = _get_ems_stats()
    return templates.TemplateResponse(
        request, "memory.html",
        {"smp_entries": smp_entries, "ems_stats": ems_stats, "flash": flash, "json": json}
    )


@router.post("/memory/smp/delete/{entry_id}", response_class=RedirectResponse)
async def memory_smp_delete(entry_id: int):
    try:
        from agent.db import get_connection_raw
        conn = get_connection_raw()
        conn.execute("DELETE FROM memory_smp WHERE id = ?", (entry_id,))
        conn.commit()
    except Exception:
        pass
    return RedirectResponse(url="/admin/memory?flash=已删除", status_code=303)


@router.post("/memory/ems/clear/{user_id:path}", response_class=RedirectResponse)
async def memory_ems_clear_user(user_id: str):
    try:
        from agent.db import get_connection_raw
        conn = get_connection_raw()
        conn.execute("DELETE FROM memory_ems WHERE user_id = ?", (user_id,))
        conn.commit()
    except Exception:
        pass
    return RedirectResponse(url="/admin/memory?flash=已清空", status_code=303)


@router.post("/memory/ems/clear-all", response_class=RedirectResponse)
async def memory_ems_clear_all():
    try:
        from agent.db import get_connection_raw
        conn = get_connection_raw()
        conn.execute("DELETE FROM memory_ems")
        conn.commit()
    except Exception:
        pass
    return RedirectResponse(url="/admin/memory?flash=全部已清空", status_code=303)
