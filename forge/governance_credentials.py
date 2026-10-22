"""Credential and channel identity lookups against the QueryRun governance store.

Raw tokens are returned only at creation and never persisted. Every mutation uses
BEGIN IMMEDIATE, the same serialization boundary as execution claims.
"""
from __future__ import annotations

import base64
import hashlib
import secrets
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone

from forge.governance import (
    AuthenticatedPrincipal, GovernanceError, PrincipalAccessError,
    append_event, connect_governance, ensure_governance_schema, governance_transaction,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _hash(token: str) -> str:
    return "sha256:" + hashlib.sha256(token.encode("utf-8")).hexdigest()


def _principal(conn: sqlite3.Connection, principal_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT u.user_id,u.principal_type,u.status,u.role,u.identity_revision,"
        "u.team_id,t.organization_id FROM tenant_users u JOIN tenant_teams t "
        "ON t.team_id=u.team_id WHERE u.user_id=?", (principal_id,),
    ).fetchone()
    if row is None or row["status"] != "active":
        raise PrincipalAccessError()
    return row


def _identity(row: sqlite3.Row, *, credential: sqlite3.Row | None = None,
              channel_binding_id: str | None = None, channel_binding_revision: int | None = None,
              method: str = "api_key") -> AuthenticatedPrincipal:
    return AuthenticatedPrincipal(
        principal_id=row["user_id"], principal_type=row["principal_type"],
        organization_id=row["organization_id"], workspace_id=row["team_id"],
        role=row["role"], identity_revision=row["identity_revision"],
        credential_id=credential["credential_id"] if credential is not None else None,
        credential_revision=credential["revision"] if credential is not None else None,
        authentication_method=method, authenticated_at=_now().isoformat(),
        channel_binding_id=channel_binding_id, channel_binding_revision=channel_binding_revision,
    )


def _check_kind(row: sqlite3.Row, kind: str) -> None:
    if kind not in {"api_key", "pi_service"}:
        raise GovernanceError("credential_kind_invalid", 422)
    if kind == "pi_service" and row["principal_type"] != "service":
        raise GovernanceError("pi_service_principal_required", 422)


def _insert(conn: sqlite3.Connection, principal_id: str, kind: str, token: str,
            *, expires_at: str | None = None, parent_id: str | None = None) -> str:
    credential_id = "gc_" + uuid.uuid4().hex
    conn.execute(
        "INSERT INTO governance_credentials (credential_id,token_hash,principal_id,kind,status,revision,"
        "parent_credential_id,created_at,expires_at) VALUES (?,?,?,?,'active',1,?,?,?)",
        (credential_id, _hash(token), principal_id, kind, parent_id, _now().isoformat(), expires_at),
    )
    append_event(conn, "credential.created", principal_id, credential_id,
                 {"credential_id": credential_id, "kind": kind, "parent_credential_id": parent_id})
    return credential_id


def create_credential(principal_id: str, kind: str = "api_key") -> tuple[str, str]:
    """Issue a 256-bit API/service key; caller must deliver its plaintext securely."""
    with governance_transaction() as conn:
        _check_kind(_principal(conn, principal_id), kind)
        token = secrets.token_urlsafe(32)
        credential_id = _insert(conn, principal_id, kind, token)
    return credential_id, token


def bind_credential(principal_id: str, kind: str, token: str) -> str:
    """Explicitly bind an existing high-entropy key; never log or store plaintext."""
    try:
        decoded = base64.b64decode(token + "=" * (-len(token) % 4), altchars=b"-_", validate=True)
    except (ValueError, base64.binascii.Error):
        raise GovernanceError("credential_entropy_insufficient", 422) from None
    if len(decoded) < 32 or len(token) > 512:
        raise GovernanceError("credential_entropy_insufficient", 422)
    with governance_transaction() as conn:
        _check_kind(_principal(conn, principal_id), kind)
        if conn.execute("SELECT 1 FROM governance_credentials WHERE token_hash=?", (_hash(token),)).fetchone():
            raise GovernanceError("credential_already_bound")
        return _insert(conn, principal_id, kind, token)


def _valid_credential(conn: sqlite3.Connection, token_hash: str, kind: str | None) -> sqlite3.Row | None:
    row = conn.execute("SELECT * FROM governance_credentials WHERE token_hash=?", (token_hash,)).fetchone()
    if row is None or row["status"] != "active" or (kind is not None and row["kind"] != kind):
        return None
    if row["expires_at"] and datetime.fromisoformat(row["expires_at"]) <= _now():
        return None
    if row["kind"] == "session":
        if not row["expires_at"]:
            return None
        if row["parent_credential_id"]:
            parent = conn.execute("SELECT status,kind,principal_id,expires_at FROM governance_credentials "
                                  "WHERE credential_id=?", (row["parent_credential_id"],)).fetchone()
            if (parent is None or parent["status"] != "active" or parent["kind"] != "api_key"
                    or parent["principal_id"] != row["principal_id"]
                    or (parent["expires_at"] and datetime.fromisoformat(parent["expires_at"]) <= _now())):
                return None
    return row


def authenticate_credential(token: str, kind: str | None = None,
                            *, conn: sqlite3.Connection | None = None) -> AuthenticatedPrincipal | None:
    """Resolve an active key/session and its current identity, optionally within a claim transaction."""
    if not token:
        return None
    if conn is None:
        with connect_governance() as owned:
            ensure_governance_schema(owned)
            return authenticate_credential(token, kind, conn=owned)
    credential = _valid_credential(conn, _hash(token), kind)
    if credential is None:
        return None
    try:
        row = _principal(conn, credential["principal_id"])
    except PrincipalAccessError:
        return None
    if credential["kind"] == "pi_service" and row["principal_type"] != "service":
        return None
    if credential["kind"] == "session" and row["principal_type"] != "human":
        return None
    return _identity(row, credential=credential,
                     method="session" if credential["kind"] == "session" else credential["kind"])


def create_session(principal_id: str, parent_credential_id: str | None = None,
                   ttl_seconds: int = 3600) -> tuple[str, str]:
    """Create a short human session; API-key login sessions retain their key parent."""
    if not 0 < ttl_seconds <= 3600:
        raise GovernanceError("session_ttl_invalid", 422)
    with governance_transaction() as conn:
        principal = _principal(conn, principal_id)
        if principal["principal_type"] != "human":
            raise PrincipalAccessError()
        if parent_credential_id:
            parent = conn.execute("SELECT * FROM governance_credentials WHERE credential_id=?",
                                  (parent_credential_id,)).fetchone()
            if (parent is None or parent["principal_id"] != principal_id or parent["kind"] != "api_key"
                    or parent["status"] != "active" or
                    (parent["expires_at"] and datetime.fromisoformat(parent["expires_at"]) <= _now())):
                raise PrincipalAccessError()
        token = secrets.token_urlsafe(32)
        credential_id = _insert(conn, principal_id, "session", token,
                                parent_id=parent_credential_id,
                                expires_at=(_now() + timedelta(seconds=ttl_seconds)).isoformat())
    return credential_id, token


def revoke_credential(credential_id: str) -> None:
    """Revoke a key and all derived sessions in one claim-ordered transaction."""
    with governance_transaction() as conn:
        row = conn.execute("SELECT principal_id,status FROM governance_credentials WHERE credential_id=?",
                           (credential_id,)).fetchone()
        if row is None:
            raise GovernanceError("credential_not_found", 404)
        if row["status"] == "revoked":
            return
        affected = conn.execute(
            "SELECT credential_id FROM governance_credentials WHERE credential_id=? OR parent_credential_id=?",
            (credential_id, credential_id),
        ).fetchall()
        conn.execute("UPDATE governance_credentials SET status='revoked',revision=revision+1 "
                     "WHERE (credential_id=? OR parent_credential_id=?) AND status='active'",
                     (credential_id, credential_id))
        append_event(conn, "credential.revoked", row["principal_id"], credential_id,
                     {"credential_id": credential_id, "affected_ids": [r["credential_id"] for r in affected]})


def authenticate_channel(channel: str, external_user_id: str,
                         *, conn: sqlite3.Connection | None = None) -> AuthenticatedPrincipal | None:
    """Resolve an already verified platform identity; never auto-bind unknown users."""
    if channel not in {"feishu", "dingtalk"} or not external_user_id:
        return None
    if conn is None:
        with connect_governance() as owned:
            ensure_governance_schema(owned)
            return authenticate_channel(channel, external_user_id, conn=owned)
    binding = conn.execute("SELECT * FROM governance_channel_bindings WHERE channel=? AND external_user_id=?",
                           (channel, external_user_id)).fetchone()
    if binding is None or binding["status"] != "active":
        return None
    try:
        row = _principal(conn, binding["principal_id"])
    except PrincipalAccessError:
        return None
    return _identity(row, channel_binding_id=f"{channel}:{external_user_id}",
                     channel_binding_revision=binding["revision"], method="service_key")


def validate_credential_binding(conn: sqlite3.Connection, credential_id: str,
                                revision: int, principal_id: str) -> bool:
    """Recheck an authenticated credential inside the caller's claim transaction."""
    row = conn.execute("SELECT * FROM governance_credentials WHERE credential_id=?",
                       (credential_id,)).fetchone()
    if row is None or row["principal_id"] != principal_id or row["revision"] != revision:
        return False
    if row["status"] != "active" or (row["expires_at"] and datetime.fromisoformat(row["expires_at"]) <= _now()):
        return False
    try:
        principal = _principal(conn, principal_id)
    except PrincipalAccessError:
        return False
    if row["kind"] == "session":
        if principal["principal_type"] != "human" or not row["expires_at"]:
            return False
        if row["parent_credential_id"]:
            parent = conn.execute("SELECT principal_id,kind,status,expires_at FROM governance_credentials WHERE credential_id=?",
                                  (row["parent_credential_id"],)).fetchone()
            if parent is None or parent["principal_id"] != principal_id or parent["kind"] != "api_key" or parent["status"] != "active":
                return False
            if parent["expires_at"] and datetime.fromisoformat(parent["expires_at"]) <= _now():
                return False
    return row["kind"] != "pi_service" or principal["principal_type"] == "service"


def validate_channel_binding(conn: sqlite3.Connection, channel_binding_id: str,
                             revision: int, principal_id: str) -> bool:
    """Recheck a verified channel identity in the transaction that claims execution."""
    try:
        channel, external_user_id = channel_binding_id.split(":", 1)
    except ValueError:
        return False
    if channel not in {"feishu", "dingtalk"}:
        return False
    row = conn.execute("SELECT principal_id,revision,status FROM governance_channel_bindings "
                       "WHERE channel=? AND external_user_id=?", (channel, external_user_id)).fetchone()
    if row is None or row["principal_id"] != principal_id or row["revision"] != revision or row["status"] != "active":
        return False
    try:
        _principal(conn, principal_id)
    except PrincipalAccessError:
        return False
    return True
