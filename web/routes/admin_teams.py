"""Admin team management: teams, table ACLs and members."""
from __future__ import annotations

import json

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from config import cfg
from forge.governance import AuthenticatedPrincipal, connect_governance
from web.auth import csrf_token_for_session, require_admin_principal
from web.templates import templates

router = APIRouter()


def _session_token(request: Request, actor: AuthenticatedPrincipal) -> str:
    token = request.cookies.get("forge_session", "")
    if actor.authentication_method != "session" or not token:
        raise HTTPException(status_code=401, detail="session_required")
    return token


def _team_in_scope(team_id: str, actor: AuthenticatedPrincipal) -> None:
    from agent.tenant import tenants
    if not any(t["team_id"] == team_id and t["organization_id"] == actor.organization_id
               for t in tenants.list_teams()):
        raise HTTPException(status_code=404, detail="unknown_team")


def _get_all_tables() -> list[str]:
    """从 schema.registry.json 读取所有表名。"""
    try:
        schema = json.loads(cfg.REGISTRY_PATH.read_text())
        return sorted(schema.get("tables", {}).keys())
    except Exception:
        return []


@router.get("/teams", response_class=HTMLResponse)
async def teams_page(request: Request, flash: str = "", actor: AuthenticatedPrincipal = Depends(require_admin_principal)):
    token = _session_token(request, actor)
    from agent.tenant import tenants
    teams = [t for t in tenants.list_teams() if t["organization_id"] == actor.organization_id]
    all_tables = _get_all_tables()
    # 为每个团队附加当前 ACL
    for t in teams:
        t["allowed_tables"] = tenants.get_allowed_tables(t["team_id"])
    return templates.TemplateResponse(
        request, "teams.html",
        {"teams": teams, "all_tables": all_tables, "flash": flash,
         "csrf_token": csrf_token_for_session(token), "organization_id": actor.organization_id}
    )


@router.post("/teams/create", response_class=RedirectResponse)
async def teams_create(
    request: Request,
    team_id:      str = Form(...),
    display_name: str = Form(default=""),
    organization_id: str = Form(...),
    actor: AuthenticatedPrincipal = Depends(require_admin_principal),
):
    _session_token(request, actor)
    if organization_id.strip() != actor.organization_id:
        raise HTTPException(status_code=403, detail="organization_scope_mismatch")
    from agent.tenant import tenants
    tenants.create_team(team_id.strip(), display_name.strip() or team_id.strip(), organization_id=organization_id.strip())
    return RedirectResponse(url="/admin/teams?flash=团队已创建", status_code=303)


@router.post("/teams/{team_id}/acl", response_class=RedirectResponse)
async def teams_save_acl(team_id: str, request: Request, actor: AuthenticatedPrincipal = Depends(require_admin_principal)):
    _session_token(request, actor)
    _team_in_scope(team_id, actor)
    form = await request.form()
    # checkbox 多选：getlist
    tables = form.getlist("tables")
    if len(tables) != len(set(tables)) or not set(tables).issubset(_get_all_tables()):
        raise HTTPException(status_code=422, detail="invalid_table_scope")
    from agent.tenant import tenants
    tenants.set_allowed_tables(team_id, list(tables))
    msg = f"已限制 {len(tables)} 张表" if tables else "拒绝全部查询"
    return RedirectResponse(url=f"/admin/teams?flash={msg}", status_code=303)


@router.get("/teams/{team_id}/members", response_class=HTMLResponse)
async def team_members_page(request: Request, team_id: str, flash: str = "", actor: AuthenticatedPrincipal = Depends(require_admin_principal)):
    token = _session_token(request, actor)
    _team_in_scope(team_id, actor)
    from agent.tenant import tenants
    members = tenants.get_team_members(team_id)
    return templates.TemplateResponse(
        request, "team_members.html",
        {"team_id": team_id, "members": members, "flash": flash,
         "csrf_token": csrf_token_for_session(token)}
    )


@router.post("/teams/{team_id}/members/add", response_class=RedirectResponse)
async def team_members_add(
    team_id:      str,
    request: Request,
    user_id:      str = Form(...),
    display_name: str = Form(default=""),
    role:         str = Form(default="member"),
    principal_type: str = Form(...),
    actor: AuthenticatedPrincipal = Depends(require_admin_principal),
):
    _session_token(request, actor)
    _team_in_scope(team_id, actor)
    if principal_type not in {"human", "agent", "service"} or role not in {"member", "reviewer", "admin"} or (principal_type != "human" and role != "member"):
        raise HTTPException(status_code=422, detail="invalid_principal_role")
    with connect_governance() as conn:
        existing = conn.execute("SELECT t.organization_id FROM tenant_users u JOIN tenant_teams t ON t.team_id=u.team_id WHERE u.user_id=?", (user_id.strip(),)).fetchone()
        if existing and existing["organization_id"] != actor.organization_id:
            raise HTTPException(status_code=404, detail="unknown_member")
    from agent.tenant import tenants
    tenants.set_team(user_id.strip(), team_id, display_name.strip(), role,
                     principal_type=principal_type, status="active")
    return RedirectResponse(url=f"/admin/teams/{team_id}/members?flash=已添加", status_code=303)


@router.post("/teams/{team_id}/members/remove", response_class=RedirectResponse)
async def team_members_remove(
    team_id: str,
    request: Request,
    user_id: str = Form(...),
    actor: AuthenticatedPrincipal = Depends(require_admin_principal),
):
    _session_token(request, actor)
    _team_in_scope(team_id, actor)
    from agent.tenant import tenants
    member = next((m for m in tenants.get_team_members(team_id) if m["user_id"] == user_id), None)
    if member is None:
        raise HTTPException(status_code=404, detail="unknown_member")
    tenants.set_team(user_id, team_id, member["display_name"], member["role"],
                     principal_type=member["principal_type"], status="disabled")
    return RedirectResponse(url=f"/admin/teams/{team_id}/members?flash=已禁用", status_code=303)
