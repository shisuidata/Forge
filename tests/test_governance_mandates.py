"""Synthetic mandate lifecycle and authority boundaries."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from agent.tenant import TenantStore
from forge.governance import AuthenticatedPrincipal, GovernanceError, connect_governance
from forge.governance_mandates import get_mandate, issue_mandate, revoke_mandate


@pytest.fixture
def scope(tmp_path, monkeypatch):
    from config import cfg
    monkeypatch.setattr(cfg, "QUERY_RUN_DB_PATH", str(tmp_path / "runs.db"))
    monkeypatch.setattr(cfg, "REGISTRY_PATH", tmp_path / "registry.json")
    monkeypatch.setattr(cfg, "DATASOURCE_ID", "synthetic")
    (tmp_path / "registry.json").write_text(json.dumps({"tables": {"numbers": {}, "secrets": {}}}))
    store = TenantStore()
    store.create_team("w", organization_id="org")
    store.create_team("other", organization_id="other-org")
    for name, kind, role, team in (
        ("admin", "human", "admin", "w"), ("reviewer", "human", "reviewer", "w"),
        ("accountable", "human", "member", "w"), ("agent", "agent", "member", "w"),
        ("stranger", "human", "admin", "other"), ("alien", "agent", "member", "other"),
    ):
        store.set_team(name, team, role=role, principal_type=kind, status="active")
    store.set_allowed_tables("w", ["numbers", "secrets"])
    store.set_allowed_tables("other", ["numbers"])

    def actor(name):
        p = store.get_principal(name)
        return AuthenticatedPrincipal(principal_id=name, principal_type=p["principal_type"],
                                      organization_id=p["organization_id"], workspace_id=p["workspace_id"],
                                      role=p["role"], identity_revision=p["identity_revision"],
                                      credential_id=None, credential_revision=None,
                                      authentication_method="test", authenticated_at=datetime.now(timezone.utc).isoformat())

    def request(**changes):
        return {"schema_version": 1, "task_run_id": "tr_synthetic", "delegate_principal_id": "agent",
                "accountable_principal_id": "accountable", "reviewer_principal_id": "reviewer",
                "purpose": "Synthetic bounded query", "datasource_id": "synthetic",
                "allowed_tables": ["numbers", "secrets"], "autonomous_tables": ["numbers"],
                "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(), **changes}
    return store, actor, request


def test_issue_replay_revoke_and_history(scope):
    _, actor, request = scope
    body = request()
    issued = issue_mandate(actor=actor("admin"), request=body, idempotency_key="first")
    assert issued["status"] == "active"
    assert issued["delegated_mandate"]["accountable_principal"]["principal_id"] == "accountable"
    assert issue_mandate(actor=actor("admin"), request=body, idempotency_key="first") == issued
    with pytest.raises(GovernanceError, match="idempotency_key_conflict"):
        issue_mandate(actor=actor("admin"), request={**body, "purpose": "different"}, idempotency_key="first")
    assert get_mandate(actor=actor("agent"), mandate_id=issued["mandate_id"]) == issued
    with pytest.raises(GovernanceError, match="mandate_not_found"):
        get_mandate(actor=actor("stranger"), mandate_id=issued["mandate_id"])
    with pytest.raises(GovernanceError, match="mandate_revision_conflict"):
        revoke_mandate(actor=actor("admin"), mandate_id=issued["mandate_id"], expected_state_revision=2, reason="stop")
    revoked = revoke_mandate(actor=actor("admin"), mandate_id=issued["mandate_id"], expected_state_revision=1, reason="stop")
    assert revoked["issuance_hash"] == issued["issuance_hash"]
    assert revoked["status"] == "revoked" and revoked["state_revision"] == 2
    assert revoke_mandate(actor=actor("admin"), mandate_id=issued["mandate_id"], expected_state_revision=1, reason="stop") == revoked
    assert get_mandate(actor=actor("agent"), mandate_id=issued["mandate_id"]) == revoked
    with connect_governance() as conn:
        assert conn.execute("SELECT COUNT(*) FROM governance_events WHERE event_type='mandate.revoked'").fetchone()[0] == 1


def test_bounds_role_scope_and_acl(scope):
    store, actor, request = scope
    for user in ("reviewer", "agent"):
        with pytest.raises(GovernanceError, match="admin_required"):
            issue_mandate(actor=actor(user), request=request(), idempotency_key=user)
    for change, error in (
        ({"delegate_principal_id": "alien"}, "mandate_scope_mismatch"),
        ({"accountable_principal_id": "agent"}, "invalid_mandate_humans"),
        ({"reviewer_principal_id": "accountable"}, "invalid_mandate_humans"),
        ({"allowed_tables": ["unknown"], "autonomous_tables": []}, "mandate_acl_exceeded"),
        ({"autonomous_tables": ["unknown"]}, "invalid_mandate_request"),
        ({"expires_at": (datetime.now(timezone.utc) + timedelta(hours=25)).isoformat()}, "invalid_mandate_expiry"),
    ):
        with pytest.raises(GovernanceError, match=error):
            issue_mandate(actor=actor("admin"), request={**request(), **change}, idempotency_key=error)
    store.set_allowed_tables("w", ["numbers"])
    with pytest.raises(GovernanceError, match="mandate_acl_exceeded"):
        issue_mandate(actor=actor("admin"), request=request(), idempotency_key="narrow")


def test_expiry_convergence_and_immutable_issuance(scope, monkeypatch):
    import forge.governance_mandates as mandates

    _, actor, request = scope
    body = request()
    first = issue_mandate(actor=actor("admin"), request=body, idempotency_key="first")
    with pytest.raises(GovernanceError, match="active_mandate_exists"):
        issue_mandate(actor=actor("admin"), request=request(), idempotency_key="second")
    monkeypatch.setattr(mandates, "_now", lambda: datetime.now(timezone.utc) + timedelta(hours=2))
    assert issue_mandate(actor=actor("admin"), request=body, idempotency_key="first")["status"] == "expired"
    second_body = {**request(), "expires_at": (datetime.now(timezone.utc) + timedelta(hours=3)).isoformat()}
    second = issue_mandate(actor=actor("admin"), request=second_body, idempotency_key="second")
    assert second["mandate_id"] != first["mandate_id"]
    expired = get_mandate(actor=actor("admin"), mandate_id=first["mandate_id"])
    assert expired["status"] == "expired" and expired["issuance_hash"] == first["issuance_hash"]
    with connect_governance() as conn:
        assert conn.execute("SELECT state_revision FROM issued_mandates WHERE mandate_id=?", (first["mandate_id"],)).fetchone()[0] == 2



def test_http_mandate_scope_and_strict_revoke_body(scope):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from web.auth import require_admin_principal, require_runtime_principal
    from web.routes.governance import router

    _, actor, request = scope
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[require_admin_principal] = lambda: actor("admin")
    app.dependency_overrides[require_runtime_principal] = lambda: actor("stranger")
    with TestClient(app) as client:
        created = client.post("/api/v2/governance/mandates", json=request(),
                              headers={"Idempotency-Key": "http"})
        assert created.status_code == 201
        mandate_id = created.json()["mandate_id"]
        assert client.get(f"/api/v2/governance/mandates/{mandate_id}").status_code == 404
        assert client.post(f"/api/v2/governance/mandates/{mandate_id}/revoke",
                           json={"expected_state_revision": 1, "reason": "stop", "revoked_by": "stranger"}).status_code == 422
        revoked = client.post(f"/api/v2/governance/mandates/{mandate_id}/revoke",
                              json={"expected_state_revision": 1, "reason": "stop"})
        assert revoked.status_code == 200 and revoked.json()["status"] == "revoked"


def test_registry_unknown_and_issuance_tamper_fail_closed(scope):
    store, actor, request = scope
    store.set_allowed_tables("w", ["numbers", "secrets", "ghost"])
    with pytest.raises(GovernanceError, match="mandate_unknown_table"):
        issue_mandate(actor=actor("admin"), request={**request(), "allowed_tables": ["ghost"],
                                                       "autonomous_tables": []}, idempotency_key="ghost")
    issued = issue_mandate(actor=actor("admin"), request=request(), idempotency_key="valid")
    with connect_governance() as conn:
        conn.execute("UPDATE issued_mandates SET accountable_id=? WHERE mandate_id=?",
                     ("reviewer", issued["mandate_id"]))
    with pytest.raises(GovernanceError, match="mandate_integrity_failed"):
        get_mandate(actor=actor("admin"), mandate_id=issued["mandate_id"])