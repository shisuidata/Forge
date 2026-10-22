"""Governance invariants whose failure would grant access without authority."""
from __future__ import annotations

import pytest

from agent.tenant import TenantStore
from forge.governance import GovernanceError, PrincipalAccessError, bootstrap, connect_governance


@pytest.fixture
def governed_db(tmp_path, monkeypatch):
    from config import cfg
    monkeypatch.setattr(cfg, "QUERY_RUN_DB_PATH", str(tmp_path / "query_runs.db"))
    return TenantStore()


def test_unknown_disabled_and_empty_acl_never_grant_all_tables(governed_db):
    store = governed_db
    with pytest.raises(PrincipalAccessError):
        store.get_team("unknown")
    store.create_team("w", organization_id="org")
    store.set_team("x", "w", principal_type="agent", status="active")
    assert store.get_allowed_tables_for_user("x") == []
    store.set_allowed_tables("w", ["numbers"])
    assert store.get_allowed_tables_for_user("x") == ["numbers"]
    identity_revision = store.get_principal("x")["identity_revision"]
    store.set_team("x", "w", principal_type="agent", status="disabled")
    with pytest.raises(PrincipalAccessError):
        store.get_allowed_tables_for_user("x")
    with connect_governance() as conn:
        row = conn.execute("SELECT identity_revision FROM tenant_users WHERE user_id='x'").fetchone()
    assert row[0] > identity_revision


def test_principal_type_immutable_and_nonhuman_cannot_be_reviewer(governed_db):
    store = governed_db
    store.create_team("w", organization_id="org")
    store.set_team("x", "w", principal_type="agent", status="active")
    with pytest.raises(GovernanceError, match="principal_type_immutable"):
        store.set_team("x", "w", principal_type="human", status="active")
    with pytest.raises(PrincipalAccessError):
        store.set_team("x", "w", role="reviewer", principal_type="agent", status="active")
    assert store.get_principal("x")["role"] == "member"


def test_bootstrap_conflict_preserves_original_membership(governed_db):
    manifest = {
        "schema_version": 1,
        "teams": [{"team_id": "w", "organization_id": "org", "display_name": "W", "allowed_tables": []}],
        "principals": [{"principal_id": "a", "team_id": "w", "display_name": "A",
                        "principal_type": "human", "role": "admin", "status": "active"}],
        "channel_bindings": [],
    }
    bootstrap(manifest)
    bootstrap(manifest)
    with pytest.raises(GovernanceError, match="governance_already_bootstrapped"):
        bootstrap({**manifest, "teams": [{**manifest["teams"][0], "allowed_tables": ["secrets"]}]})
    assert governed_db.get_allowed_tables_for_user("a") == []
    with connect_governance() as conn:
        count = conn.execute("SELECT COUNT(*) FROM governance_events WHERE event_type='bootstrap'").fetchone()[0]
    assert count == 1

def test_channel_rebinding_requires_explicit_revocation(governed_db):
    from forge.governance import bind_channel, revoke_channel
    store = governed_db
    store.create_team("w", organization_id="org")
    for name in ("a", "b"):
        store.set_team(name, "w", principal_type="human", status="active")
    bind_channel("feishu", "external", "a")
    with pytest.raises(GovernanceError, match="channel_binding_revoke_required"):
        bind_channel("feishu", "external", "b")
    revoke_channel("feishu", "external")
    bind_channel("feishu", "external", "b")
    with connect_governance() as conn:
        row = conn.execute("SELECT principal_id,status,revision FROM governance_channel_bindings WHERE external_user_id=?", ("external",)).fetchone()
    assert (row["principal_id"], row["status"], row["revision"]) == ("b", "active", 3)

def test_legacy_tenant_tables_are_not_silently_upgraded(governed_db):
    from forge.governance import ensure_governance_schema
    with connect_governance() as conn:
        conn.execute("CREATE TABLE tenant_teams (team_id TEXT PRIMARY KEY, display_name TEXT)")
        conn.execute("INSERT INTO tenant_teams VALUES ('old','Old')")
    with pytest.raises(GovernanceError, match="governance_schema_migration_required"):
        ensure_governance_schema()
    with connect_governance() as conn:
        assert conn.execute("SELECT display_name FROM tenant_teams WHERE team_id='old'").fetchone()[0] == "Old"

def test_offline_migration_never_imports_undeclared_identity_or_expands_acl(governed_db, tmp_path):
    import hashlib
    import json
    import sqlite3
    from forge.governance_migrate import MigrationError, migrate
    source = tmp_path / "legacy.db"
    with sqlite3.connect(source) as conn:
        conn.executescript("""CREATE TABLE tenant_teams(team_id TEXT PRIMARY KEY,display_name TEXT,created_at TEXT);
            CREATE TABLE tenant_users(user_id TEXT PRIMARY KEY,team_id TEXT,display_name TEXT,role TEXT,created_at TEXT,updated_at TEXT);
            CREATE TABLE team_table_acl(team_id TEXT,table_name TEXT);""")
        conn.execute("INSERT INTO tenant_teams VALUES ('w','W','2026-01-01')")
        conn.execute("INSERT INTO tenant_users VALUES ('a','w','A','admin','2026-01-01','2026-01-01')")
    manifest = {"schema_version": 1, "teams": [{"team_id": "w", "organization_id": "org", "display_name": "W", "allowed_tables": []}],
                "principals": [{"principal_id": "a", "team_id": "w", "display_name": "A", "principal_type": "human", "role": "admin", "status": "active"}],
                "channel_bindings": []}
    manifest_path = tmp_path / "identity.json"
    manifest_path.write_text(json.dumps(manifest))
    source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    migrate(source_memory_db=str(source), identity_manifest=str(manifest_path), apply=True)
    assert governed_db.get_allowed_tables_for_user("a") == []
    assert hashlib.sha256(source.read_bytes()).hexdigest() == source_hash
    with sqlite3.connect(source) as conn:
        conn.execute("INSERT INTO tenant_users VALUES ('omitted','w','O','member','2026-01-01','2026-01-01')")
    changed_source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    with pytest.raises(MigrationError, match="manifest_does_not_cover_legacy_rows"):
        migrate(source_memory_db=str(source), identity_manifest=str(manifest_path), apply=True)
    assert hashlib.sha256(source.read_bytes()).hexdigest() == changed_source_hash
    with connect_governance() as conn:
        assert conn.execute("SELECT COUNT(*) FROM tenant_users").fetchone()[0] == 1
