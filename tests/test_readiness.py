"""Deployment gates protect governed execution without requiring Pi or an LLM."""
from __future__ import annotations

from config import cfg
from forge.governance import bootstrap
from forge.governance_credentials import create_credential, revoke_credential
from forge.readiness import readiness_payload


def _checks(profile="prod"):
    return {check["name"]: check for check in readiness_payload(profile)["checks"]}


def _configure(monkeypatch, tmp_path):
    for name in ("REGISTRY_PATH", "METRICS_PATH", "DISAMBIGUATIONS_PATH", "CONVENTIONS_PATH"):
        path = tmp_path / name.lower()
        path.write_text('{"tables":{"numbers":{"columns":{"n":{}}}}}' if name == "REGISTRY_PATH" else "{}")
        monkeypatch.setattr(cfg, name, path)
    monkeypatch.setattr(cfg, "QUERY_RUN_DB_PATH", str(tmp_path / "governance.db"))
    monkeypatch.setattr(cfg, "AUDIT_DB_PATH", str(tmp_path / "audit.db"))
    monkeypatch.setattr(cfg, "AUTH_ENABLED", True)
    monkeypatch.setattr(cfg, "AUTH_ADMIN_PRINCIPAL_ID", "admin")
    monkeypatch.setattr(cfg, "DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setattr(cfg, "DATABASE_READONLY_CONFIRMED", True)
    monkeypatch.setattr(cfg, "EXECUTION_ENABLED", True)
    monkeypatch.setattr(cfg, "EXECUTION_MAX_ROWS", 100)
    monkeypatch.setattr(cfg, "EXECUTION_TIMEOUT_SECONDS", 5)
    monkeypatch.setattr(cfg, "RAW_SQL_ENABLED", False)
    monkeypatch.setattr(cfg, "LEGACY_AGENT_API_ENABLED", False)
    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", False)
    monkeypatch.setattr(cfg, "PI_CHANNEL_SERVICE_KEY", "")
    monkeypatch.setattr(cfg, "PI_EXECUTION_PRINCIPAL_ID", "pi")
    monkeypatch.setattr(cfg, "LLM_API_KEY", "")
    monkeypatch.setattr(cfg, "AUTH_COOKIE_SECURE", True)


def _bootstrap():
    bootstrap({"schema_version": 1, "teams": [{
        "team_id": "team", "organization_id": "org", "display_name": "Demo",
        "allowed_tables": ["numbers"]}], "principals": [
        {"principal_id": identity, "team_id": "team", "display_name": identity,
         "principal_type": kind, "role": role, "status": "active"}
        for identity, kind, role in (("admin", "human", "admin"),
                                     ("agent", "agent", "member"),
                                     ("pi", "service", "member"))], "channel_bindings": []})
    create_credential("admin")
    return create_credential("agent")[0]


def test_readiness_requires_bootstrapped_authority_and_auth_in_every_profile(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    assert _checks()["governance_store"]["status"] == "fail"
    assert not (tmp_path / "governance.db").exists()
    _bootstrap()
    assert _checks()["governance_store"]["status"] == "ok"
    assert _checks()["identity_bindings"]["status"] == "ok"
    assert _checks()["llm"]["status"] == "ok"  # Direct SQL Agent needs no model.
    for profile in ("dev", "poc", "prod"):
        monkeypatch.setattr(cfg, "AUTH_ENABLED", False)
        assert _checks(profile)["auth"]["status"] == "fail"
    monkeypatch.setattr(cfg, "AUTH_ENABLED", True)
    assert _checks()["database_readonly"]["status"] == "ok"
    assert "尚未独立验证" in _checks()["database_readonly"]["message"]


def test_readiness_identity_credential_and_pi_are_conditional(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    _bootstrap()
    assert _checks()["pi_control_auth"]["status"] == "ok"
    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    assert _checks()["pi_control_auth"]["status"] == "fail"
    monkeypatch.setattr(cfg, "PI_CHANNEL_SERVICE_KEY", "synthetic-control-key")
    pi_key_id, _ = create_credential("pi", kind="pi_service")
    assert _checks()["pi_control_auth"]["status"] == "ok"
    assert _checks()["llm"]["status"] == "fail"
    monkeypatch.setattr(cfg, "LLM_API_KEY", "synthetic-model-key")
    assert _checks()["llm"]["status"] == "ok"
    revoke_credential(pi_key_id)
    assert _checks()["pi_control_auth"]["status"] == "fail"


def test_readiness_rejects_legacy_execution_and_missing_agent_binding(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    agent_key_id = _bootstrap()
    monkeypatch.setattr(cfg, "LEGACY_AGENT_API_ENABLED", True)
    assert _checks("dev")["legacy_execution_closed"]["status"] == "fail"
    monkeypatch.setattr(cfg, "LEGACY_AGENT_API_ENABLED", False)
    monkeypatch.setattr(cfg, "RAW_SQL_ENABLED", True)
    assert _checks("prod")["legacy_execution_closed"]["status"] == "fail"
    monkeypatch.setattr(cfg, "RAW_SQL_ENABLED", False)
    revoke_credential(agent_key_id)
    assert _checks()["identity_bindings"]["status"] == "fail"
    monkeypatch.setattr(cfg, "AUTH_ADMIN_PRINCIPAL_ID", "not-an-admin")
    assert _checks()["identity_bindings"]["status"] == "fail"
