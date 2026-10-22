"""Transactional, bounded mandate issuance and revocation."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from jsonschema import ValidationError

from agent.contracts import validate_contract
from config import cfg
from forge.governance import (
    AuthenticatedPrincipal, GovernanceError, append_event, canonical_hash,
    connect_governance, ensure_governance_schema, governance_transaction,
)
from forge.governance_credentials import validate_channel_binding, validate_credential_binding

POLICY_REVISION = "bounded-agent-policy-v1"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _instant(value: str) -> datetime:
    try:
        instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if instant.tzinfo is None:
            raise ValueError("timezone required")
        return instant.astimezone(timezone.utc)
    except (ValueError, TypeError) as exc:
        raise GovernanceError("invalid_mandate_expiry", 422) from exc


def _principal(conn: Any, principal_id: str) -> Any:
    row = conn.execute(
        "SELECT u.user_id,u.principal_type,u.role,u.status,u.identity_revision,u.team_id workspace_id,"
        "t.organization_id FROM tenant_users u JOIN tenant_teams t ON t.team_id=u.team_id "
        "WHERE u.user_id=?", (principal_id,),
    ).fetchone()
    if row is None or row["status"] != "active":
        raise GovernanceError("principal_access_denied", 403)
    return row


def _actor(conn: Any, actor: AuthenticatedPrincipal) -> Any:
    row = _principal(conn, actor.principal_id)
    # Recheck the presented credential inside this transaction so a revocation
    # committed after authentication also blocks this commit.
    if actor.credential_id is not None:
        bound = validate_credential_binding(conn, actor.credential_id, actor.credential_revision, actor.principal_id)
    elif actor.channel_binding_id is not None:
        bound = validate_channel_binding(conn, actor.channel_binding_id, actor.channel_binding_revision, actor.principal_id)
    else:
        bound = False
    if not bound:
        raise GovernanceError("credential_revoked", 401)
    if (row["identity_revision"] != actor.identity_revision or
            row["organization_id"] != actor.organization_id or
            row["workspace_id"] != actor.workspace_id or
            row["principal_type"] != actor.principal_type or row["role"] != actor.role):
        raise GovernanceError("governance_drift", 409)
    return row


def _admin(row: Any) -> None:
    if row["principal_type"] != "human" or row["role"] != "admin":
        raise GovernanceError("admin_required", 403)


def _stored(row: Any, now: datetime) -> dict:
    issuance = json.loads(row["issuance_json"])
    if canonical_hash(issuance) != row["issuance_hash"]:
        raise GovernanceError("mandate_integrity_failed", 409)
    delegated = issuance["delegated_mandate"]
    if (issuance["mandate_id"] != row["mandate_id"] or
            issuance["issuer_principal_id"] != row["issuer_id"] or
            delegated["delegate_principal"]["principal_id"] != row["delegate_id"] or
            delegated["accountable_principal"]["principal_id"] != row["accountable_id"] or
            issuance["policy"]["reviewer_principal_id"] != row["reviewer_id"] or
            delegated["organization_id"] != row["org_id"] or
            delegated["workspace_id"] != row["workspace_id"] or
            delegated["task_run_id"] != row["task_run_id"] or
            delegated["issued_at"] != row["issued_at"] or
            delegated["expires_at"] != row["expires_at"]):
        raise GovernanceError("mandate_integrity_failed", 409)
    issued = {**issuance, "issuance_hash": row["issuance_hash"],
              "status": "expired" if row["status"] == "active" and _instant(row["expires_at"]) <= now else row["status"],
              "state_revision": row["state_revision"], "revoked_at": row["revoked_at"], "revoked_by": row["revoked_by"]}
    validate_contract("issued_mandate_v1", issued)
    return issued


def _registry_tables() -> set[str]:
    try:
        raw = json.loads(Path(cfg.REGISTRY_PATH).read_text(encoding="utf-8"))
        tables = raw.get("tables", raw)
        if not isinstance(tables, dict):
            raise ValueError("registry tables must be an object")
        return {name for name, description in tables.items() if isinstance(description, dict)}
    except (OSError, ValueError, TypeError) as exc:
        raise GovernanceError("registry_unavailable", 503) from exc


def issue_mandate(*, actor: AuthenticatedPrincipal, request: dict, idempotency_key: str) -> dict:
    if not idempotency_key or len(idempotency_key) > 256:
        raise GovernanceError("idempotency_key_required", 422)
    try:
        validate_contract("mandate_issue_v1", request)
    except ValidationError as exc:
        raise GovernanceError("invalid_mandate_request", 422) from exc
    allowed = request["allowed_tables"]
    autonomous = request["autonomous_tables"]
    if len(allowed) > 127 or not set(autonomous).issubset(allowed):
        raise GovernanceError("invalid_mandate_tables", 422)
    now = _now()
    fingerprint = canonical_hash(request)
    with governance_transaction() as conn:
        issuer = _actor(conn, actor)
        _admin(issuer)
        prior = conn.execute("SELECT * FROM issued_mandates WHERE issuer_id=? AND create_key=?",
                             (actor.principal_id, idempotency_key)).fetchone()
        if prior:
            if prior["request_hash"] != fingerprint:
                raise GovernanceError("idempotency_key_conflict", 409)
            return _stored(prior, now)
        expiry = _instant(request["expires_at"])
        if expiry <= now or expiry > now + timedelta(hours=24):
            raise GovernanceError("invalid_mandate_expiry", 422)
        if request["datasource_id"] != cfg.DATASOURCE_ID:
            raise GovernanceError("datasource_scope_mismatch", 422)
        delegate = _principal(conn, request["delegate_principal_id"])
        accountable = _principal(conn, request["accountable_principal_id"])
        reviewer = _principal(conn, request["reviewer_principal_id"])
        scope = (issuer["organization_id"], issuer["workspace_id"])
        if any((person["organization_id"], person["workspace_id"]) != scope
               for person in (delegate, accountable, reviewer)):
            raise GovernanceError("mandate_scope_mismatch", 403)
        if delegate["principal_type"] not in {"agent", "service"}:
            raise GovernanceError("invalid_delegate", 422)
        if accountable["principal_type"] != "human" or reviewer["principal_type"] != "human" or reviewer["role"] not in {"admin", "reviewer"}:
            raise GovernanceError("invalid_mandate_humans", 422)
        upper = {row[0] for row in conn.execute(
            "SELECT table_name FROM team_table_acl WHERE team_id=? INTERSECT "
            "SELECT table_name FROM team_table_acl WHERE team_id=?",
            (issuer["workspace_id"], delegate["workspace_id"]))}
        if not set(allowed).issubset(upper):
            raise GovernanceError("mandate_acl_exceeded", 403)
        if not set(allowed).issubset(_registry_tables()):
            raise GovernanceError("mandate_unknown_table", 422)
        old = conn.execute("SELECT * FROM issued_mandates WHERE delegate_id=? AND task_run_id=? AND status='active'",
                           (delegate["user_id"], request["task_run_id"])).fetchone()
        if old:
            if _instant(old["expires_at"]) > now:
                raise GovernanceError("active_mandate_exists", 409)
            conn.execute("UPDATE issued_mandates SET status='expired',state_revision=state_revision+1 WHERE mandate_id=?", (old["mandate_id"],))
            append_event(conn, "mandate.expired", actor.principal_id, old["mandate_id"],
                         {"mandate_id": old["mandate_id"], "state_revision": old["state_revision"] + 1})
        mandate_id = "md_" + uuid.uuid4().hex
        issued_at = now.isoformat()
        org_id, workspace_id = scope
        resource_scope = [{"schema_version": 1, "resource_type": "datasource", "resource_id": request["datasource_id"],
                           "organization_id": org_id, "workspace_id": workspace_id,
                           "parent_resource_id": None, "resource_revision": None}]
        resource_scope.extend({"schema_version": 1, "resource_type": "table", "resource_id": table,
                               "organization_id": org_id, "workspace_id": workspace_id,
                               "parent_resource_id": request["datasource_id"], "resource_revision": None}
                              for table in allowed)
        delegated = {"schema_version": 1, "mandate_id": mandate_id, "revision": 1,
                     "delegate_principal": {"principal_id": delegate["user_id"], "principal_type": delegate["principal_type"]},
                     "delegator_principal": {"principal_id": issuer["user_id"], "principal_type": "human"},
                     "accountable_principal": {"principal_id": accountable["user_id"], "principal_type": "human"},
                     "organization_id": org_id, "workspace_id": workspace_id, "purpose": request["purpose"],
                     "task_run_id": request["task_run_id"], "audience": "forge",
                     "capabilities": ["query.prepare", "query.execute"], "resource_scope": resource_scope,
                     "budget_ref": None, "approval_policy_ref": POLICY_REVISION, "can_delegate": False,
                     "status": "active", "issued_at": issued_at, "expires_at": request["expires_at"]}
        issuance = {"schema_version": 1, "mandate_id": mandate_id, "issuance_revision": 1,
                    "issuer_principal_id": actor.principal_id, "delegated_mandate": delegated,
                    "policy": {"revision": POLICY_REVISION, "reviewer_principal_id": reviewer["user_id"],
                               "allowed_tables": allowed, "autonomous_tables": autonomous}}
        digest = canonical_hash(issuance)
        conn.execute("INSERT INTO issued_mandates (mandate_id,issuer_id,delegate_id,accountable_id,reviewer_id,org_id,workspace_id,task_run_id,issuance_json,issuance_hash,issuance_revision,status,state_revision,issued_at,expires_at,revoked_at,revoked_by,create_key,request_hash) VALUES (?,?,?,?,?,?,?,?,?,?,1,'active',1,?,?,NULL,NULL,?,?)",
                     (mandate_id, actor.principal_id, delegate["user_id"], accountable["user_id"], reviewer["user_id"],
                      org_id, workspace_id, request["task_run_id"], json.dumps(issuance, sort_keys=True), digest,
                      issued_at, request["expires_at"], idempotency_key, fingerprint))
        append_event(conn, "mandate.issued", actor.principal_id, mandate_id,
                     {"mandate_id": mandate_id, "issuance_hash": digest})
        return {**issuance, "issuance_hash": digest, "status": "active", "state_revision": 1,
                "revoked_at": None, "revoked_by": None}


def _visible(row: Any, principal: Any) -> bool:
    return ((principal["principal_type"] == "human" and principal["role"] == "admin" and
             row["org_id"] == principal["organization_id"] and row["workspace_id"] == principal["workspace_id"])
            or (principal["user_id"] in {row["issuer_id"], row["accountable_id"], row["reviewer_id"]} and
                principal["principal_type"] == "human" and row["org_id"] == principal["organization_id"] and
                row["workspace_id"] == principal["workspace_id"])
            or (principal["user_id"] == row["delegate_id"] and row["org_id"] == principal["organization_id"] and
                row["workspace_id"] == principal["workspace_id"]))


def get_mandate(*, actor: AuthenticatedPrincipal, mandate_id: str) -> dict:
    with connect_governance() as conn:
        ensure_governance_schema(conn)
        principal = _actor(conn, actor)
        row = conn.execute("SELECT * FROM issued_mandates WHERE mandate_id=?", (mandate_id,)).fetchone()
        if row is None or not _visible(row, principal):
            raise GovernanceError("mandate_not_found", 404)
        return _stored(row, _now())


def revoke_mandate(*, actor: AuthenticatedPrincipal, mandate_id: str,
                   expected_state_revision: int, reason: str) -> dict:
    if not isinstance(expected_state_revision, int) or isinstance(expected_state_revision, bool) or expected_state_revision < 1 or not isinstance(reason, str) or not reason.strip() or len(reason) > 1000:
        raise GovernanceError("invalid_revoke_request", 422)
    with governance_transaction() as conn:
        principal = _actor(conn, actor)
        _admin(principal)
        row = conn.execute("SELECT * FROM issued_mandates WHERE mandate_id=?", (mandate_id,)).fetchone()
        if row is None or row["org_id"] != principal["organization_id"] or row["workspace_id"] != principal["workspace_id"]:
            raise GovernanceError("mandate_not_found", 404)
        if row["status"] == "revoked":
            event = conn.execute("SELECT payload_json FROM governance_events WHERE event_type='mandate.revoked' AND subject_id=? ORDER BY occurred_at DESC LIMIT 1", (mandate_id,)).fetchone()
            if event and json.loads(event["payload_json"])["reason"] == reason:
                return _stored(row, _now())
            raise GovernanceError("mandate_revision_conflict", 409)
        if row["state_revision"] != expected_state_revision:
            raise GovernanceError("mandate_revision_conflict", 409)
        now = _now()
        if row["status"] != "active" or _instant(row["expires_at"]) <= now:
            raise GovernanceError("mandate_expired", 409)
        revoked_at = now.isoformat()
        conn.execute("UPDATE issued_mandates SET status='revoked',state_revision=state_revision+1,revoked_at=?,revoked_by=? WHERE mandate_id=?",
                     (revoked_at, actor.principal_id, mandate_id))
        append_event(conn, "mandate.revoked", actor.principal_id, mandate_id,
                     {"mandate_id": mandate_id, "reason": reason, "state_revision": row["state_revision"] + 1})
        result = dict(row)
        result.update(status="revoked", state_revision=row["state_revision"] + 1,
                      revoked_at=revoked_at, revoked_by=actor.principal_id)
        return _stored(result, now)
