from __future__ import annotations

import hashlib
import secrets
import sqlite3
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from forge.governance import bind_channel, governance_transaction, revoke_channel
from forge.governance_credentials import (
    authenticate_channel, authenticate_credential, bind_credential, create_credential,
    create_session, revoke_credential, validate_credential_binding, validate_channel_binding,
)
from web.auth import (
    csrf_token_for_session, require_admin_principal, require_runtime_principal,
    set_governance_session_cookie,
)
from fastapi import Response
from config import cfg


@pytest.fixture
def store(tmp_path: Path, monkeypatch):
    path = tmp_path / "governance.sqlite"
    monkeypatch.setattr(cfg, "QUERY_RUN_DB_PATH", str(path))
    monkeypatch.setattr(cfg, "AUTH_ENABLED", True)
    with governance_transaction() as conn:
        conn.execute("INSERT INTO tenant_teams(team_id,organization_id,display_name) VALUES ('team','org','Team')")
        conn.executemany(
            "INSERT INTO tenant_users(user_id,team_id,display_name,role,principal_type,status) "
            "VALUES (?,'team',?,?,?,'active')",
            [("admin", "Admin", "admin", "human"), ("agent", "Agent", "member", "agent"),
             ("service", "Pi", "member", "service")],
        )
    return path


def _app():
    app = FastAPI()

    @app.get("/who")
    def who(principal=Depends(require_runtime_principal)):
        return {"id": principal.principal_id, "credential_id": principal.credential_id}

    @app.post("/admin")
    def admin(principal=Depends(require_admin_principal)):
        return {"id": principal.principal_id}

    return app


def test_keys_are_hashed_and_revocation_closes_parent_sessions(store):
    credential_id, token = create_credential("admin")
    assert len(token) >= 43
    with sqlite3.connect(store) as conn:
        persisted = conn.execute("SELECT token_hash FROM governance_credentials WHERE credential_id=?",
                                 (credential_id,)).fetchone()[0]
        assert persisted == "sha256:" + hashlib.sha256(token.encode()).hexdigest()
        assert token not in str(conn.execute("SELECT payload_json FROM governance_events").fetchall())
    assert authenticate_credential(token).principal_id == "admin"
    session_id, session_token = create_session("admin", credential_id)
    assert authenticate_credential(session_token, "session").credential_id == session_id
    with governance_transaction() as conn:
        assert validate_credential_binding(conn, credential_id, 1, "admin")
        assert not validate_credential_binding(conn, credential_id, 2, "admin")
        assert validate_credential_binding(conn, session_id, 1, "admin")
    revoke_credential(credential_id)
    with governance_transaction() as conn:
        assert not validate_credential_binding(conn, credential_id, 1, "admin")
        assert not validate_credential_binding(conn, session_id, 1, "admin")
    assert authenticate_credential(token) is None
    assert authenticate_credential(session_token) is None
    revoke_credential(credential_id)


def test_role_and_identity_changes_take_effect_without_reissuing(store):
    admin_id, admin_key = create_credential("admin")
    _, agent_key = create_credential("agent")
    _, service_key = create_credential("service", "pi_service")
    assert authenticate_credential(service_key).principal_type == "service"
    assert authenticate_credential(agent_key).role == "member"
    with TestClient(_app()) as client:
        assert client.post("/admin", headers={"X-API-Key": admin_key}).status_code == 200
        assert client.post("/admin", headers={"X-API-Key": agent_key}).status_code == 403
        assert client.post("/admin", headers={"X-API-Key": service_key}).status_code == 403
        assert client.get("/who", params={"api_key": admin_key}).status_code == 401
        assert client.get("/who", headers={"X-API-Key": "invalid"}).status_code == 401
        with governance_transaction() as conn:
            conn.execute("UPDATE tenant_users SET status='disabled',identity_revision=identity_revision+1 "
                         "WHERE user_id='admin'")
        assert client.get("/who", headers={"X-API-Key": admin_key}).status_code == 401
    assert authenticate_credential(admin_key) is None


def test_channel_binding_requires_explicit_active_mapping(store):
    assert authenticate_channel("feishu", "unknown") is None
    bind_channel("feishu", "external", "agent")
    identity = authenticate_channel("feishu", "external")
    assert identity.principal_id == "agent"
    assert identity.credential_id is None
    assert identity.channel_binding_id == "feishu:external"
    with governance_transaction() as conn:
        assert validate_channel_binding(conn, identity.channel_binding_id, identity.channel_binding_revision, "agent")
    revoke_channel("feishu", "external")
    with governance_transaction() as conn:
        assert not validate_channel_binding(conn, identity.channel_binding_id, identity.channel_binding_revision, "agent")
    assert authenticate_channel("feishu", "external") is None


def test_cookie_mutation_requires_same_origin_and_session_csrf(store):
    _, token = create_session("admin")
    response = Response()
    set_governance_session_cookie(response, token)
    cookie = response.headers["set-cookie"].split(";", 1)[0]
    with TestClient(_app()) as client:
        assert client.get("/who", headers={"Cookie": cookie}).json()["id"] == "admin"
        assert client.get("/who", headers={"Cookie": cookie, "X-API-Key": "invalid"}).status_code == 401
        base = {"Cookie": cookie, "Origin": "http://testserver"}
        assert client.post("/admin", headers=base).status_code == 403
        assert client.post("/admin", headers={**base, "Origin": "https://evil.test",
                                                      "X-CSRF-Token": csrf_token_for_session(token)}).status_code == 403
        assert client.post("/admin", headers={**base, "X-CSRF-Token": csrf_token_for_session(token)}).status_code == 200
        assert client.post("/admin", headers={**base, "Content-Type": "application/x-www-form-urlencoded"},
                           data={"csrf_token": csrf_token_for_session(token)}).status_code == 200
        revoke_credential(authenticate_credential(token, "session").credential_id)
        assert client.get("/who", headers={"Cookie": cookie}).status_code == 401


def test_bound_key_and_expired_session(store):
    from forge.governance import GovernanceError
    with pytest.raises(GovernanceError, match="credential_entropy_insufficient"):
        bind_credential("agent", "api_key", "weak")
    key = secrets.token_urlsafe(32)
    credential_id = bind_credential("agent", "api_key", key)
    assert authenticate_credential(key).credential_id == credential_id
    _, token = create_session("admin", ttl_seconds=1)
    with governance_transaction() as conn:
        conn.execute("UPDATE governance_credentials SET expires_at='2000-01-01T00:00:00+00:00' "
                     "WHERE token_hash=?", ("sha256:" + hashlib.sha256(token.encode()).hexdigest(),))
    assert authenticate_credential(token, "session") is None
