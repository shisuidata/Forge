"""Explicit, offline migration of legacy memory tenant rows into QueryRun governance.

The source is opened read-only; the identity manifest is an operator decision, not
an inference from the old unrestricted-ACL convention. No runtime code reads it.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from urllib.parse import quote

from jsonschema import ValidationError

from agent.contracts import validate_contract
from config import cfg
from .governance import append_event, canonical_hash, governance_transaction


class MigrationError(ValueError):
    pass


_LEGACY_COLUMNS = {
    "tenant_teams": {"team_id", "display_name", "created_at"},
    "tenant_users": {"user_id", "team_id", "display_name", "role", "created_at", "updated_at"},
    "team_table_acl": {"team_id", "table_name"},
}
_TARGET_TABLES = ("tenant_teams", "tenant_users", "team_table_acl", "governance_channel_bindings")


def _read_legacy(conn: sqlite3.Connection) -> tuple[dict, dict, dict]:
    for table, columns in _LEGACY_COLUMNS.items():
        actual = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        if not columns.issubset(actual):
            raise MigrationError(f"legacy_schema_mismatch: {table}")
    teams = {row["team_id"]: dict(row) for row in conn.execute("SELECT team_id,display_name FROM tenant_teams")}
    users = {row["user_id"]: dict(row) for row in conn.execute("SELECT user_id,team_id,display_name,role FROM tenant_users")}
    acl: dict[str, set[str]] = {team: set() for team in teams}
    for row in conn.execute("SELECT team_id,table_name FROM team_table_acl"):
        if row["team_id"] not in acl:
            raise MigrationError("orphan_legacy_acl")
        acl[row["team_id"]].add(row["table_name"])
    if any(user["team_id"] not in teams for user in users.values()):
        raise MigrationError("orphan_legacy_principal")
    return teams, users, acl


def _reconcile_source(manifest: dict, teams: dict, users: dict, acl: dict) -> list[str]:
    declared_teams = {t["team_id"]: t for t in manifest["teams"]}
    declared_users = {p["principal_id"]: p for p in manifest["principals"]}
    if set(teams) != set(declared_teams) or set(users) != set(declared_users):
        raise MigrationError("manifest_does_not_cover_legacy_rows")
    for team_id, old in teams.items():
        proposed = declared_teams[team_id]
        if old["display_name"] is not None and old["display_name"] != proposed["display_name"]:
            raise MigrationError(f"legacy_team_conflict: {team_id}")
        if acl[team_id] and acl[team_id] != set(proposed["allowed_tables"]):
            raise MigrationError(f"legacy_acl_conflict: {team_id}")
    for user_id, old in users.items():
        proposed = declared_users[user_id]
        if (old["team_id"] != proposed["team_id"] or old["role"] != proposed["role"] or
                old["display_name"] is not None and old["display_name"] != proposed["display_name"]):
            raise MigrationError(f"legacy_principal_conflict: {user_id}")
    return sorted(team_id for team_id, tables in acl.items() if not tables)


def _target_rows(conn: sqlite3.Connection, table: str, key_columns: tuple[str, ...], columns: tuple[str, ...]) -> dict:
    names = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if not names:
        return {}
    if not set(columns).issubset(names):
        raise MigrationError(f"target_schema_mismatch: {table}")
    return {tuple(row[column] for column in key_columns): tuple(row[column] for column in columns)
            for row in conn.execute(f"SELECT {','.join(columns)} FROM {table}")}


def _reconcile_target(conn: sqlite3.Connection, manifest: dict) -> dict[str, list[tuple]]:
    team_rows = [(t["team_id"], t["organization_id"], t["display_name"]) for t in manifest["teams"]]
    principal_rows = [(p["principal_id"], p["team_id"], p["display_name"], p["role"], p["principal_type"], p["status"])
                      for p in manifest["principals"]]
    acl_rows = [(t["team_id"], name) for t in manifest["teams"] for name in t["allowed_tables"]]
    binding_rows = [(b["channel"], b["external_user_id"], b["principal_id"], "active")
                    for b in manifest["channel_bindings"]]
    specs = {
        "tenant_teams": (("team_id",), ("team_id", "organization_id", "display_name"), team_rows),
        "tenant_users": (("user_id",), ("user_id", "team_id", "display_name", "role", "principal_type", "status"), principal_rows),
        "team_table_acl": (("team_id", "table_name"), ("team_id", "table_name"), acl_rows),
        "governance_channel_bindings": (("channel", "external_user_id"),
                                        ("channel", "external_user_id", "principal_id", "status"), binding_rows),
    }
    additions = {}
    for table, (keys, columns, desired) in specs.items():
        existing = _target_rows(conn, table, keys, columns)
        new = []
        for row in desired:
            key = row[:len(keys)]
            if key in existing:
                if existing[key] != row:
                    raise MigrationError(f"target_conflict: {table}: {key}")
            else:
                new.append(row)
        additions[table] = new
    # A destination ACL entry absent from the explicit manifest must never
    # silently broaden one of the migrated teams (including empty ACL teams).
    migrated = {team["team_id"] for team in manifest["teams"]}
    permitted = set(acl_rows)
    existing_acl = _target_rows(conn, "team_table_acl", ("team_id", "table_name"), ("team_id", "table_name"))
    if any(key[0] in migrated and key not in permitted for key in existing_acl):
        raise MigrationError("target_acl_conflict")
    return additions


def migrate(*, source_memory_db: str, identity_manifest: str, apply: bool) -> dict:
    """Reconcile all legacy tenant rows; apply inserts atomically or report only."""
    source_path = Path(source_memory_db).expanduser().resolve(strict=True)
    target_path = Path(cfg.QUERY_RUN_DB_PATH).expanduser().resolve()
    if source_path == target_path:
        raise MigrationError("source_and_target_must_differ")
    manifest = json.loads(Path(identity_manifest).read_text(encoding="utf-8"))
    try:
        validate_contract("governance_bootstrap_v1", manifest)
    except ValidationError as exc:
        raise MigrationError(f"invalid_identity_manifest: {exc.message}") from exc
    # URI mode=ro refuses to create a missing source; no PRAGMA/schema writes.
    with closing(sqlite3.connect(f"file:{quote(source_path.as_posix(), safe='/')}?mode=ro", uri=True)) as source:
        source.row_factory = sqlite3.Row
        source.execute("PRAGMA query_only = ON")
        source.execute("BEGIN")
        teams, users, acl = _read_legacy(source)
        unrestricted = _reconcile_source(manifest, teams, users, acl)
        if apply:
            with governance_transaction() as target:
                additions = _reconcile_target(target, manifest)
                _insert(target, additions, manifest)
        elif target_path.exists():
            with closing(sqlite3.connect(f"file:{quote(target_path.as_posix(), safe='/')}?mode=ro", uri=True)) as target:
                target.row_factory = sqlite3.Row
                target.execute("PRAGMA query_only = ON")
                additions = _reconcile_target(target, manifest)
        else:
            additions = {
                "tenant_teams": list(manifest["teams"]),
                "tenant_users": list(manifest["principals"]),
                "team_table_acl": [(t["team_id"], name) for t in manifest["teams"] for name in t["allowed_tables"]],
                "governance_channel_bindings": list(manifest["channel_bindings"]),
            }
    return {"status": "applied" if apply else "dry_run", "manifest_hash": canonical_hash(manifest),
            "source_counts": {"teams": len(teams), "principals": len(users),
                              "acl_rows": sum(map(len, acl.values()))},
            "legacy_empty_acl_teams": unrestricted,
            "insert_counts": {table: len(rows) for table, rows in additions.items()}}


def _insert(conn: sqlite3.Connection, additions: dict[str, list[tuple]], manifest: dict) -> None:
    statements = {
        "tenant_teams": "INSERT INTO tenant_teams (team_id,organization_id,display_name) VALUES (?,?,?)",
        "tenant_users": "INSERT INTO tenant_users (user_id,team_id,display_name,role,principal_type,status) VALUES (?,?,?,?,?,?)",
        "team_table_acl": "INSERT INTO team_table_acl (team_id,table_name) VALUES (?,?)",
        "governance_channel_bindings": "INSERT INTO governance_channel_bindings (channel,external_user_id,principal_id,status) VALUES (?,?,?,?)",
    }
    if not any(additions.values()):
        return
    for table in _TARGET_TABLES:
        conn.executemany(statements[table], additions[table])
    append_event(conn, "legacy.migrated", "local_operator", "governance_store",
                 {"manifest_hash": canonical_hash(manifest), "counts": {table: len(rows) for table, rows in additions.items()}})
