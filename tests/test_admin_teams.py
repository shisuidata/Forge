"""Team administration through a real registered human session."""
from __future__ import annotations

import re

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from agent.tenant import tenants
from config import cfg
from forge.governance_credentials import create_credential, create_session
from web.auth import require_admin_principal
from web.routes.admin_teams import router


@pytest.fixture
def admin_client(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "QUERY_RUN_DB_PATH", str(tmp_path / "governance.sqlite"))
    monkeypatch.setattr(cfg, "AUTH_ENABLED", True)
    registry = tmp_path / "schema.registry.json"
    registry.write_text('{"tables":{"numbers":{},"secrets":{}}}', encoding="utf-8")
    monkeypatch.setattr(cfg, "REGISTRY_PATH", registry)
    tenants.create_team("alpha", organization_id="org")
    tenants.create_team("other", organization_id="other-org")
    tenants.set_team("admin", "alpha", role="admin", principal_type="human", status="active")
    tenants.set_team("elsewhere", "other", role="admin", principal_type="human", status="active")
    _, session = create_session("admin")
    app = FastAPI()
    app.include_router(router, prefix="/admin", dependencies=[Depends(require_admin_principal)])
    with TestClient(app) as client:
        client.cookies.set("forge_session", session)
        yield client


def _form(client: TestClient, path: str) -> str:
    response = client.get(path)
    assert response.status_code == 200
    tokens = re.findall(r'name="csrf_token" value="([^"]+)"', response.text)
    assert tokens and len(set(tokens)) == 1
    return tokens[0]


def _post(client: TestClient, path: str, data: dict, csrf: str, *, origin: str = "http://testserver"):
    return client.post(path, data={**data, "csrf_token": csrf}, headers={"Origin": origin}, follow_redirects=False)


def test_admin_forms_are_session_bound_and_empty_acl_denies(admin_client):
    client = admin_client
    csrf = _form(client, "/admin/teams")
    page = client.get("/admin/teams").text
    assert "拒绝全部查询" in page
    assert "other-org" not in page
    assert 'name="organization_id" value="org" readonly' in page
    assert client.post("/admin/teams/create", data={"team_id": "new", "organization_id": "org"},
                       headers={"Origin": "http://testserver"}, follow_redirects=False).status_code == 403
    assert _post(client, "/admin/teams/create", {"team_id": "new", "organization_id": "org"}, csrf,
                 origin="https://evil.test").status_code == 403
    assert _post(client, "/admin/teams/create", {"team_id": "foreign", "organization_id": "other-org"}, csrf).status_code == 403
    assert _post(client, "/admin/teams/create", {"team_id": "new", "organization_id": "org"}, csrf).status_code == 303
    assert _post(client, "/admin/teams/alpha/acl", {"tables": "numbers"}, csrf).status_code == 303
    assert tenants.get_allowed_tables("alpha") == ["numbers"]
    assert _post(client, "/admin/teams/alpha/acl", {}, csrf).status_code == 303
    assert tenants.get_allowed_tables("alpha") == []
    assert "拒绝全部查询" in client.get("/admin/teams").text
    assert _post(client, "/admin/teams/alpha/acl", {"tables": "unregistered"}, csrf).status_code == 422
    assert _post(client, "/admin/teams/other/acl", {}, csrf).status_code == 404
    _, key = create_credential("admin")
    client.cookies.clear()
    assert client.get("/admin/teams", headers={"X-API-Key": key}).status_code == 401


def test_member_type_role_and_disable_are_enforced(admin_client):
    client = admin_client
    csrf = _form(client, "/admin/teams/alpha/members")
    path = "/admin/teams/alpha/members"
    assert _post(client, path + "/add", {"user_id": "worker", "principal_type": "agent", "role": "reviewer"}, csrf).status_code == 422
    assert _post(client, path + "/add", {"user_id": "worker", "principal_type": "service", "role": "admin"}, csrf).status_code == 422
    assert _post(client, path + "/add", {"user_id": "worker", "principal_type": "agent", "role": "member"}, csrf).status_code == 303
    assert _post(client, path + "/add", {"user_id": "reviewer", "principal_type": "human", "role": "reviewer"}, csrf).status_code == 303
    page = client.get(path).text
    assert "worker" in page and "agent / active" in page and "reviewer" in page
    assert _post(client, path + "/remove", {"user_id": "worker"}, csrf).status_code == 303
    assert any(m["user_id"] == "worker" and m["status"] == "disabled" for m in tenants.get_team_members("alpha"))
    assert "agent / disabled" in client.get(path).text
    assert _post(client, path + "/remove", {"user_id": "elsewhere"}, csrf).status_code == 404
