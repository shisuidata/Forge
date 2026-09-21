"""Admin team management: teams, table ACLs and members."""
from __future__ import annotations

import json

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from config import cfg
from web.templates import templates

router = APIRouter()


def _get_all_tables() -> list[str]:
    """从 schema.registry.json 读取所有表名。"""
    try:
        schema = json.loads(cfg.REGISTRY_PATH.read_text())
        return sorted(schema.get("tables", {}).keys())
    except Exception:
        return []


@router.get("/teams", response_class=HTMLResponse)
async def teams_page(request: Request, flash: str = ""):
    from agent.tenant import tenants
    teams = tenants.list_teams()
    all_tables = _get_all_tables()
    # 为每个团队附加当前 ACL
    for t in teams:
        t["allowed_tables"] = tenants.get_allowed_tables(t["team_id"])  # None = 无限制
    return templates.TemplateResponse(
        request, "teams.html",
        {"teams": teams, "all_tables": all_tables, "flash": flash}
    )


@router.post("/teams/create", response_class=RedirectResponse)
async def teams_create(
    team_id:      str = Form(...),
    display_name: str = Form(default=""),
):
    from agent.tenant import tenants
    tenants.create_team(team_id.strip(), display_name.strip() or team_id.strip())
    return RedirectResponse(url="/admin/teams?flash=团队已创建", status_code=303)


@router.post("/teams/{team_id}/acl", response_class=RedirectResponse)
async def teams_save_acl(team_id: str, request: Request):
    form = await request.form()
    # checkbox 多选：getlist
    tables = form.getlist("tables")
    from agent.tenant import tenants
    tenants.set_allowed_tables(team_id, list(tables))
    msg = f"已限制 {len(tables)} 张表" if tables else "权限已清除（不限制）"
    return RedirectResponse(url=f"/admin/teams?flash={msg}", status_code=303)


@router.get("/teams/{team_id}/members", response_class=HTMLResponse)
async def team_members_page(request: Request, team_id: str, flash: str = ""):
    from agent.tenant import tenants
    members = tenants.get_team_members(team_id)
    return templates.TemplateResponse(
        request, "team_members.html",
        {"team_id": team_id, "members": members, "flash": flash}
    )


@router.post("/teams/{team_id}/members/add", response_class=RedirectResponse)
async def team_members_add(
    team_id:      str,
    user_id:      str = Form(...),
    display_name: str = Form(default=""),
    role:         str = Form(default="member"),
):
    from agent.tenant import tenants
    tenants.set_team(user_id.strip(), team_id, display_name.strip(), role)
    return RedirectResponse(url=f"/admin/teams/{team_id}/members?flash=已添加", status_code=303)


@router.post("/teams/{team_id}/members/remove", response_class=RedirectResponse)
async def team_members_remove(
    team_id: str,
    user_id: str = Form(...),
):
    # 把用户移回 default 团队
    from agent.tenant import tenants
    tenants.set_team(user_id, "default")
    return RedirectResponse(url=f"/admin/teams/{team_id}/members?flash=已移除", status_code=303)
