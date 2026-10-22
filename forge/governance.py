"""Authoritative identity, ACL and mandate storage beside QueryRun claims.

No database is opened on import. Every mutation uses BEGIN IMMEDIATE so a
revocation and an execution claim have a single observable ordering.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from agent.contracts import validate_contract
from config import cfg


class GovernanceError(Exception):
    def __init__(self, code: str, status_code: int = 409):
        super().__init__(code)
        self.code = code
        self.status_code = status_code


class PrincipalAccessError(GovernanceError):
    def __init__(self) -> None:
        super().__init__("principal_access_denied", 403)


@dataclass(frozen=True)
class AuthenticatedPrincipal:
    principal_id: str
    principal_type: str
    organization_id: str
    workspace_id: str
    role: str
    identity_revision: int
    credential_id: str | None
    credential_revision: int | None
    authentication_method: str
    authenticated_at: str
    channel_binding_id: str | None = None
    channel_binding_revision: int | None = None


def governance_db_path() -> str:
    path = Path(cfg.QUERY_RUN_DB_PATH).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


_DDL = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS tenant_teams (
  team_id TEXT PRIMARY KEY, organization_id TEXT NOT NULL,
  display_name TEXT NOT NULL, acl_revision INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS tenant_users (
  user_id TEXT PRIMARY KEY, team_id TEXT NOT NULL REFERENCES tenant_teams(team_id),
  display_name TEXT NOT NULL, role TEXT NOT NULL CHECK(role IN ('admin','reviewer','member')),
  principal_type TEXT NOT NULL CHECK(principal_type IN ('human','agent','service')),
  status TEXT NOT NULL CHECK(status IN ('active','disabled')),
  identity_revision INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  CHECK(principal_type = 'human' OR role = 'member')
);
CREATE INDEX IF NOT EXISTS idx_tenant_team ON tenant_users(team_id);
CREATE TABLE IF NOT EXISTS team_table_acl (
  team_id TEXT NOT NULL REFERENCES tenant_teams(team_id),
  table_name TEXT NOT NULL, PRIMARY KEY(team_id,table_name)
);
CREATE TABLE IF NOT EXISTS governance_credentials (
  credential_id TEXT PRIMARY KEY, token_hash TEXT NOT NULL UNIQUE,
  principal_id TEXT NOT NULL REFERENCES tenant_users(user_id),
  kind TEXT NOT NULL CHECK(kind IN ('api_key','session','pi_service')),
  status TEXT NOT NULL CHECK(status IN ('active','revoked')),
  revision INTEGER NOT NULL DEFAULT 1,
  parent_credential_id TEXT REFERENCES governance_credentials(credential_id),
  created_at TEXT NOT NULL, expires_at TEXT
);
CREATE TABLE IF NOT EXISTS governance_channel_bindings (
  channel TEXT NOT NULL CHECK(channel IN ('web','feishu','dingtalk')),
  external_user_id TEXT NOT NULL,
  principal_id TEXT NOT NULL REFERENCES tenant_users(user_id),
  revision INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL CHECK(status IN ('active','revoked')),
  PRIMARY KEY(channel,external_user_id)
);
CREATE TABLE IF NOT EXISTS issued_mandates (
  mandate_id TEXT PRIMARY KEY, issuer_id TEXT NOT NULL REFERENCES tenant_users(user_id),
  delegate_id TEXT NOT NULL REFERENCES tenant_users(user_id),
  accountable_id TEXT NOT NULL REFERENCES tenant_users(user_id),
  reviewer_id TEXT NOT NULL REFERENCES tenant_users(user_id),
  org_id TEXT NOT NULL, workspace_id TEXT NOT NULL,
  task_run_id TEXT NOT NULL, issuance_json TEXT NOT NULL, issuance_hash TEXT NOT NULL,
  issuance_revision INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL CHECK(status IN ('active','revoked','expired')),
  state_revision INTEGER NOT NULL DEFAULT 1, issued_at TEXT NOT NULL,
  expires_at TEXT NOT NULL, revoked_at TEXT, revoked_by TEXT,
  create_key TEXT NOT NULL, request_hash TEXT NOT NULL,
  UNIQUE(issuer_id,create_key)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_active_delegate_task
 ON issued_mandates(delegate_id,task_run_id) WHERE status = 'active';
CREATE TABLE IF NOT EXISTS governance_events (
  event_id TEXT PRIMARY KEY, event_type TEXT NOT NULL, actor_id TEXT NOT NULL,
  subject_id TEXT NOT NULL, occurred_at TEXT NOT NULL,
  payload_json TEXT NOT NULL, payload_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS governance_control_tokens (
  token_id TEXT PRIMARY KEY, token_hash TEXT NOT NULL UNIQUE,
  principal_id TEXT NOT NULL REFERENCES tenant_users(user_id),
  credential_id TEXT REFERENCES governance_credentials(credential_id),
  credential_revision INTEGER, channel_binding_id TEXT, channel_binding_revision INTEGER,
  method TEXT NOT NULL, path TEXT NOT NULL, body_hash TEXT NOT NULL,
  action TEXT NOT NULL, task_run_id TEXT, query_run_id TEXT,
  approval_hashes_json TEXT, idempotency_key TEXT,
  issued_at TEXT NOT NULL, expires_at TEXT NOT NULL,
  consumed_execution_key TEXT,
  CHECK((credential_id IS NULL) != (channel_binding_id IS NULL))
);
"""


def canonical_hash(value: object) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_governance_schema(conn: sqlite3.Connection | None = None) -> None:
    if conn is None:
        with connect_governance() as owned:
            ensure_governance_schema(owned)
        return
    conn.executescript(_DDL)
    for table, required in {
        "tenant_teams": {"organization_id", "acl_revision"},
        "tenant_users": {"principal_type", "status", "identity_revision"},
    }.items():
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        if not required.issubset(columns):
            raise GovernanceError("governance_schema_migration_required", 503)


@contextmanager
def connect_governance() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(governance_db_path(), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def governance_transaction() -> Iterator[sqlite3.Connection]:
    with connect_governance() as conn:
        ensure_governance_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.rollback()
            raise
        else:
            conn.commit()


def append_event(conn: sqlite3.Connection, event_type: str, actor: str, subject: str, payload: dict) -> None:
    conn.execute(
        "INSERT INTO governance_events VALUES (?,?,?,?,?,?,?)",
        ("ge_" + uuid.uuid4().hex, event_type, actor, subject, _now(),
         json.dumps(payload, ensure_ascii=False, sort_keys=True), canonical_hash(payload)),
    )


def bootstrap(manifest: dict) -> None:
    validate_contract("governance_bootstrap_v1", manifest)
    digest = canonical_hash(manifest)
    with governance_transaction() as conn:
        prior = conn.execute("SELECT payload_hash FROM governance_events WHERE event_type = 'bootstrap' LIMIT 1").fetchone()
        if prior:
            if prior["payload_hash"] == digest:
                return
            raise GovernanceError("governance_already_bootstrapped")
        if conn.execute("SELECT 1 FROM tenant_users LIMIT 1").fetchone() or conn.execute("SELECT 1 FROM tenant_teams LIMIT 1").fetchone():
            raise GovernanceError("governance_store_not_empty")
        for team in manifest["teams"]:
            conn.execute("INSERT INTO tenant_teams (team_id,organization_id,display_name) VALUES (?,?,?)",
                         (team["team_id"],team["organization_id"],team["display_name"]))
            conn.executemany("INSERT INTO team_table_acl VALUES (?,?)",
                             ((team["team_id"], table) for table in team["allowed_tables"]))
        for principal in manifest["principals"]:
            conn.execute("INSERT INTO tenant_users (user_id,team_id,display_name,role,principal_type,status) VALUES (?,?,?,?,?,?)",
                         tuple(principal[key] for key in ("principal_id","team_id","display_name","role","principal_type","status")))
        for binding in manifest["channel_bindings"]:
            conn.execute("INSERT INTO governance_channel_bindings (channel,external_user_id,principal_id,status) VALUES (?,?,?,'active')",
                         tuple(binding[key] for key in ("channel","external_user_id","principal_id")))
        append_event(conn,"bootstrap","local_operator","governance_store",manifest)

def bind_channel(channel: str, external_user_id: str, principal_id: str) -> None:
    if channel not in {"feishu", "dingtalk"} or not external_user_id:
        raise GovernanceError("channel_binding_invalid", 422)
    with governance_transaction() as conn:
        principal = conn.execute("SELECT status FROM tenant_users WHERE user_id=?", (principal_id,)).fetchone()
        if not principal or principal["status"] != "active":
            raise PrincipalAccessError()
        prior = conn.execute("SELECT principal_id,status FROM governance_channel_bindings WHERE channel=? AND external_user_id=?",
                             (channel,external_user_id)).fetchone()
        if prior and prior["status"] == "active":
            if prior["principal_id"] == principal_id:
                return
            raise GovernanceError("channel_binding_revoke_required")
        conn.execute("INSERT INTO governance_channel_bindings (channel,external_user_id,principal_id,status) VALUES (?,?,?,'active') "
                     "ON CONFLICT(channel,external_user_id) DO UPDATE SET principal_id=excluded.principal_id,status='active',revision=revision+1",
                     (channel,external_user_id,principal_id))
        append_event(conn,"channel.bound",principal_id,f"{channel}:{external_user_id}",{"principal_id":principal_id})


def revoke_channel(channel: str, external_user_id: str) -> None:
    if channel not in {"feishu", "dingtalk"} or not external_user_id:
        raise GovernanceError("channel_binding_invalid", 422)
    with governance_transaction() as conn:
        row = conn.execute("SELECT principal_id,status FROM governance_channel_bindings WHERE channel=? AND external_user_id=?",
                           (channel,external_user_id)).fetchone()
        if not row:
            raise GovernanceError("channel_binding_not_found", 404)
        if row["status"] == "revoked":
            return
        conn.execute("UPDATE governance_channel_bindings SET status='revoked',revision=revision+1 WHERE channel=? AND external_user_id=?",
                     (channel,external_user_id))
        append_event(conn,"channel.revoked",row["principal_id"],f"{channel}:{external_user_id}",{"principal_id":row["principal_id"]})

_CONTROL_ACTIONS = {"task.read", "task.write", "task.attach_mandate", "query.approve"}


def issue_control_token(principal: AuthenticatedPrincipal, method: str, path: str,
                        body_bytes: bytes, action: str, task_run_id: str | None = None,
                        query_run_id: str | None = None, approval_hashes: dict | None = None,
                        idempotency_key: str | None = None) -> str:
    """Mint a short-lived, body-bound request proof only after Forge authentication."""
    import secrets
    from datetime import timedelta
    from forge.governance_credentials import validate_channel_binding, validate_credential_binding

    if action not in _CONTROL_ACTIONS or not method or not path.startswith("/") or not isinstance(body_bytes, bytes):
        raise GovernanceError("control_scope_invalid", 422)
    if action == "query.approve" and (not query_run_id or not idempotency_key or not approval_hashes):
        raise GovernanceError("approval_scope_required", 422)
    token = secrets.token_urlsafe(32)
    now = datetime.now(timezone.utc)
    with governance_transaction() as conn:
        binding = (validate_credential_binding(conn, principal.credential_id, principal.credential_revision, principal.principal_id)
                   if principal.credential_id else bool(principal.channel_binding_id) and validate_channel_binding(
                       conn, principal.channel_binding_id, principal.channel_binding_revision, principal.principal_id))
        identity = conn.execute("SELECT status,identity_revision FROM tenant_users WHERE user_id=?",
                                (principal.principal_id,)).fetchone()
        if not binding or identity is None or identity["status"] != "active" or identity["identity_revision"] != principal.identity_revision:
            raise GovernanceError("control_principal_invalid", 403)
        conn.execute("INSERT INTO governance_control_tokens (token_id,token_hash,principal_id,credential_id,credential_revision,"
                     "channel_binding_id,channel_binding_revision,method,path,body_hash,action,task_run_id,query_run_id,"
                     "approval_hashes_json,idempotency_key,issued_at,expires_at,consumed_execution_key) "
                     "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL)",
                     ("ct_" + uuid.uuid4().hex, "sha256:" + hashlib.sha256(token.encode()).hexdigest(),
                      principal.principal_id, principal.credential_id, principal.credential_revision,
                      principal.channel_binding_id, principal.channel_binding_revision, method.upper(), path,
                      "sha256:" + hashlib.sha256(body_bytes).hexdigest(), action, task_run_id, query_run_id,
                      json.dumps(approval_hashes, sort_keys=True) if approval_hashes else None,
                      idempotency_key, now.isoformat(), (now + timedelta(seconds=60)).isoformat()))
    return token


def resolve_control_token(conn: sqlite3.Connection, token: str, method: str, path: str,
                          body_hash: str, idempotency_key: str | None = None) -> tuple[sqlite3.Row, AuthenticatedPrincipal]:
    from forge.governance_credentials import validate_channel_binding, validate_credential_binding

    digest = "sha256:" + hashlib.sha256(token.encode()).hexdigest()
    record = conn.execute("SELECT * FROM governance_control_tokens WHERE token_hash=?", (digest,)).fetchone()
    if (record is None or record["method"] != method.upper() or record["path"] != path
            or record["body_hash"] != body_hash or record["idempotency_key"] != idempotency_key
            or datetime.fromisoformat(record["expires_at"]) <= datetime.now(timezone.utc)):
        raise GovernanceError("control_token_invalid", 403)
    binding = (validate_credential_binding(conn, record["credential_id"], record["credential_revision"], record["principal_id"])
               if record["credential_id"] else validate_channel_binding(
                   conn, record["channel_binding_id"], record["channel_binding_revision"], record["principal_id"]))
    principal = conn.execute("SELECT u.principal_type,u.status,u.role,u.identity_revision,u.team_id,t.organization_id "
                             "FROM tenant_users u JOIN tenant_teams t ON t.team_id=u.team_id WHERE u.user_id=?",
                             (record["principal_id"],)).fetchone()
    if not binding or principal is None or principal["status"] != "active":
        raise GovernanceError("control_principal_invalid", 403)
    return record, AuthenticatedPrincipal(
        principal_id=record["principal_id"], principal_type=principal["principal_type"],
        organization_id=principal["organization_id"], workspace_id=principal["team_id"],
        role=principal["role"], identity_revision=principal["identity_revision"],
        credential_id=record["credential_id"], credential_revision=record["credential_revision"],
        authentication_method="service_key" if record["channel_binding_id"] else "api_key",
        authenticated_at=record["issued_at"], channel_binding_id=record["channel_binding_id"],
        channel_binding_revision=record["channel_binding_revision"],
    )
