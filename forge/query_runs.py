"""Persistent QueryRun lifecycle for the trusted Pi control plane.

Pi owns TaskRun orchestration. Forge owns this QueryRun record, approval
validation, read-only execution, and the resulting audit evidence.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiosqlite


from config import cfg
from forge.governance import governance_db_path

_DDL = """
CREATE TABLE IF NOT EXISTS query_runs (
    query_run_id            TEXT PRIMARY KEY,
    task_run_id             TEXT NOT NULL,
    org_id                  TEXT NOT NULL,
    team_id                 TEXT NOT NULL,
    user_id                 TEXT NOT NULL,
    datasource_id           TEXT NOT NULL,
    question                TEXT NOT NULL,
    input_kind              TEXT NOT NULL DEFAULT 'forge_json',
    candidate_revision      TEXT NOT NULL DEFAULT 'query-candidate-v1',
    candidate_payload       TEXT,
    candidate_hash          TEXT,
    enforce_schema_version  INTEGER,
    workspace_id            TEXT,
    purpose                 TEXT,
    principal_context       TEXT,
    principal_context_hash  TEXT,
    delegated_mandate       TEXT,
    delegated_mandate_hash  TEXT,
    resource_scope          TEXT,
    resource_scope_hash     TEXT,
    policy_decision         TEXT,
    policy_decision_hash    TEXT,
    enforcement_context_hash TEXT,
    explain_context        TEXT,
    explain_context_hash   TEXT,
    status                  TEXT NOT NULL,
    forge_json              TEXT,
    sql                     TEXT,
    sql_hash                TEXT,
    dialect                 TEXT NOT NULL,
    registry_version        TEXT NOT NULL,
    assurance_report        TEXT,
    assurance_report_hash   TEXT,
    assurance_revision      TEXT,
    policy_revision         TEXT,
    model_revision          TEXT,
    assurance_registry_revision TEXT,
    create_idempotency_key  TEXT UNIQUE,
    approval_idempotency_key TEXT UNIQUE,
    approver_user_id        TEXT,
    approved_at             TEXT,
    expires_at              TEXT NOT NULL,
    result_columns          TEXT,
    create_request_hash TEXT,
    mandate_id TEXT,
    mandate_issuance_revision INTEGER,
    mandate_issuance_hash TEXT,
    execution_mode TEXT,
    reviewer_principal_id TEXT,
    authorization_policy_revision TEXT,
    authorization_record TEXT,
    authorization_hash TEXT,
    governance_revision_snapshot TEXT,
    execution_idempotency_key TEXT UNIQUE,
    execution_claim_id TEXT,
    executed_at TEXT,
    result_rows             TEXT,
    result_hash            TEXT,
    approval_hash          TEXT,
    row_count               INTEGER NOT NULL DEFAULT 0,
    truncated               INTEGER NOT NULL DEFAULT 0,
    execution_ms            INTEGER,
    error                   TEXT,
    execution_owner         TEXT,
    execution_lease_expires_at TEXT,
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_query_runs_task ON query_runs(task_run_id);
"""


_SCHEMA_LOCK = asyncio.Lock()
_SCHEMA_READY_PATHS: set[str] = set()
_PROCESS_OWNER = f"forge-{uuid.uuid4().hex}"


class QueryRunError(Exception):
    """Bounded domain error with a stable HTTP-friendly status code."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int = 409,
        code: str = "query_run_conflict",
    ):
        super().__init__(message)
        self.status_code = status_code
        self.code = code

def _db_path() -> str:
    return governance_db_path()


async def _ensure_schema() -> None:
    path = _db_path()
    if path in _SCHEMA_READY_PATHS:
        return
    async with _SCHEMA_LOCK:
        if path in _SCHEMA_READY_PATHS:
            return
        async with aiosqlite.connect(path) as db:
            await db.executescript(_DDL)
            cursor = await db.execute("PRAGMA table_info(query_runs)")
            columns = {row[1] for row in await cursor.fetchall()}
            migrations = {
                "input_kind": "TEXT NOT NULL DEFAULT 'forge_json'",
                "candidate_revision": "TEXT NOT NULL DEFAULT 'query-candidate-v1'",
                "candidate_payload": "TEXT",
                "candidate_hash": "TEXT",
                "enforce_schema_version": "INTEGER",
                "workspace_id": "TEXT",
                "purpose": "TEXT",
                "principal_context": "TEXT",
                "principal_context_hash": "TEXT",
                "delegated_mandate": "TEXT",
                "delegated_mandate_hash": "TEXT",
                "resource_scope": "TEXT",
                "resource_scope_hash": "TEXT",
                "policy_decision": "TEXT",
                "mandate_id": "TEXT",
                "mandate_issuance_revision": "INTEGER",
                "create_request_hash": "TEXT",
                "mandate_issuance_hash": "TEXT",
                "execution_mode": "TEXT",
                "reviewer_principal_id": "TEXT",
                "authorization_policy_revision": "TEXT",
                "authorization_record": "TEXT",
                "authorization_hash": "TEXT",
                "governance_revision_snapshot": "TEXT",
                "execution_idempotency_key": "TEXT",
                "execution_claim_id": "TEXT",
                "executed_at": "TEXT",
                "policy_decision_hash": "TEXT",
                "enforcement_context_hash": "TEXT",
                "explain_context": "TEXT",
                "explain_context_hash": "TEXT",
                "assurance_report": "TEXT",
                "assurance_report_hash": "TEXT",
                "approval_hash": "TEXT",
                "result_hash": "TEXT",
                "assurance_revision": "TEXT",
                "policy_revision": "TEXT",
                "model_revision": "TEXT",
                "assurance_registry_revision": "TEXT",
                "execution_owner": "TEXT",
                "execution_lease_expires_at": "TEXT",
            }
            for name, sql_type in migrations.items():
                if name not in columns:
                    await db.execute(f"ALTER TABLE query_runs ADD COLUMN {name} {sql_type}")
            await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_query_runs_execution_key ON query_runs(execution_idempotency_key) WHERE execution_idempotency_key IS NOT NULL")
            await db.commit()
        _SCHEMA_READY_PATHS.add(path)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _sql_hash(sql: str) -> str:
    return "sha256:" + hashlib.sha256(sql.encode("utf-8")).hexdigest()


def _artifact_hash(value: dict[str, Any]) -> str:
    canonical = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def current_registry_version() -> str:
    """Hash the effective Registry inputs without exposing their contents."""
    digest = hashlib.sha256()
    paths = [
        cfg.REGISTRY_PATH,
        cfg.METRICS_PATH,
        cfg.DISAMBIGUATIONS_PATH,
        cfg.CONVENTIONS_PATH,
        cfg.BUSINESS_CONTEXT_PATH,
    ]
    for raw_path in paths:
        path = Path(raw_path)
        digest.update(str(path).encode("utf-8"))
        if path.exists() and path.is_file():
            digest.update(path.read_bytes())
        else:
            digest.update(b"<missing>")
    return "sha256:" + digest.hexdigest()


def _decode_row(row: aiosqlite.Row) -> dict[str, Any]:
    result = dict(row)
    for field in (
        "forge_json",
        "candidate_payload",
        "principal_context",
        "delegated_mandate",
        "authorization_record",
        "governance_revision_snapshot",
        "resource_scope",
        "policy_decision",
        "assurance_report",
        "result_columns",
        "result_rows",
        "explain_context",
    ):
        value = result.get(field)
        result[field] = json.loads(value) if value is not None else None
    result["truncated"] = bool(result.get("truncated"))
    return result


_LEASE_EXPIRED = ("status='executing' AND execution_lease_expires_at IS NOT NULL "
                  "AND execution_lease_expires_at <= ?")
_REVIEW_EXPIRED = "enforce_schema_version=2 AND status IN ('ready','review_required') AND expires_at <= ?"


async def _settle_time_decided_query_runs(query_run_id: str | None = None) -> None:
    """Persist outcomes that only the clock decides, so reads never report a stale live state or inbox item.

    A read-only probe skips the common case, and autocommit makes each UPDATE finish on the aiosqlite
    thread, so no lock spans an await (see ``_read_rows``).
    """
    now = _now().isoformat()
    scope, args = ("", ()) if query_run_id is None else (" AND query_run_id = ?", (query_run_id,))
    async with aiosqlite.connect(_db_path(), isolation_level=None) as db:
        if not await db.execute_fetchall(
            f"SELECT 1 FROM query_runs WHERE (({_LEASE_EXPIRED}) OR ({_REVIEW_EXPIRED})){scope} LIMIT 1",
            (now, now, *args),
        ):
            return
        await db.execute(
            "UPDATE query_runs SET status='failed', error='execution_interrupted_or_lease_expired', updated_at=?, "
            f"execution_owner=NULL, execution_lease_expires_at=NULL WHERE {_LEASE_EXPIRED}{scope}",
            (now, now, *args),
        )
        await db.execute(f"UPDATE query_runs SET status='expired', updated_at=? WHERE {_REVIEW_EXPIRED}{scope}",
                         (now, now, *args))


async def _read_rows(sql: str, parameters: tuple) -> list[dict[str, Any]]:
    """Run a read in one aiosqlite call.

    Splitting execute and fetch across awaits leaves the statement holding SQLite's shared lock while the
    coroutine waits for the event loop; a synchronous governance write on that loop then deadlocks until the
    busy timeout. One call steps the statement to completion on the aiosqlite thread.
    """
    async with aiosqlite.connect(_db_path()) as db:
        db.row_factory = aiosqlite.Row
        return [_decode_row(row) for row in await db.execute_fetchall(sql, parameters)]


async def get_query_run(query_run_id: str) -> dict[str, Any] | None:
    await _ensure_schema()
    await _settle_time_decided_query_runs(query_run_id)
    rows = await _read_rows("SELECT * FROM query_runs WHERE query_run_id = ?", (query_run_id,))
    return rows[0] if rows else None
async def list_governed_query_runs(actor, limit: int = 8) -> list[dict[str, Any]]:
    """Return recent v2 QueryRuns in one authenticated admin workspace."""
    if actor.principal_type != "human" or actor.role != "admin":
        raise QueryRunError("Admin required", status_code=403, code="admin_principal_required")
    await _ensure_schema()
    await _settle_time_decided_query_runs()
    runs = await _read_rows(
        "SELECT * FROM query_runs WHERE enforce_schema_version=2 AND org_id=? AND workspace_id=? "
        "ORDER BY created_at DESC, query_run_id DESC LIMIT ?",
        (actor.organization_id, actor.workspace_id, min(max(int(limit), 1), 50)),
    )
    for run in runs:
        await asyncio.to_thread(_validate_run_access, run, actor)
    return runs


async def _get_by_create_key(key: str) -> dict[str, Any] | None:
    await _ensure_schema()
    rows = await _read_rows("SELECT * FROM query_runs WHERE create_idempotency_key = ?", (key,))
    return rows[0] if rows else None




def _validate_actor_binding(conn, actor) -> None:
    from forge.governance_credentials import validate_channel_binding, validate_credential_binding

    if actor.credential_id is not None:
        valid = validate_credential_binding(conn, actor.credential_id, actor.credential_revision, actor.principal_id)
    elif actor.channel_binding_id is not None:
        valid = validate_channel_binding(conn, actor.channel_binding_id, actor.channel_binding_revision, actor.principal_id)
    else:
        valid = False
    if not valid:
        raise QueryRunError("Authenticated binding is no longer active", status_code=401, code="credential_revoked")


def _validate_governance_snapshot(conn, run: dict[str, Any], revisions: dict) -> dict:
    from forge.governance import canonical_hash
    from agent.contracts import validate_contract

    principal_context = run["principal_context"]
    if isinstance(principal_context, str):
        principal_context = json.loads(principal_context)
    mandate = conn.execute("SELECT * FROM issued_mandates WHERE mandate_id=?", (run["mandate_id"],)).fetchone()
    if mandate is None or mandate["status"] != "active" or datetime.fromisoformat(mandate["expires_at"]) <= _now():
        raise QueryRunError("Mandate inactive", code="mandate_inactive")
    issuance = json.loads(mandate["issuance_json"])
    if (canonical_hash(issuance) != mandate["issuance_hash"]
            or mandate["issuance_hash"] != run["mandate_issuance_hash"]
            or mandate["issuance_revision"] != run["mandate_issuance_revision"]
            or mandate["task_run_id"] != run["task_run_id"]
            or mandate["delegate_id"] != principal_context["actor_principal"]["principal_id"]
            or mandate["reviewer_id"] != run["reviewer_principal_id"]
            or mandate["org_id"] != run["org_id"] or mandate["workspace_id"] != run["workspace_id"]):
        raise QueryRunError("Mandate changed", code="governance_drift")
    try:
        validate_contract("issued_mandate_v1", {**issuance, "issuance_hash": mandate["issuance_hash"],
            "status": mandate["status"], "state_revision": mandate["state_revision"],
            "revoked_at": mandate["revoked_at"], "revoked_by": mandate["revoked_by"]})
    except Exception as exc:
        raise QueryRunError("Mandate integrity failed", code="governance_drift") from exc
    ids = {"issuer": mandate["issuer_id"], "delegate": mandate["delegate_id"],
           "accountable": mandate["accountable_id"], "reviewer": mandate["reviewer_id"]}
    for role, principal_id in ids.items():
        person = conn.execute("SELECT u.principal_type,u.role,u.status,u.identity_revision,u.team_id,t.organization_id,t.acl_revision "
                              "FROM tenant_users u JOIN tenant_teams t ON t.team_id=u.team_id WHERE u.user_id=?",
                              (principal_id,)).fetchone()
        if (person is None or person["status"] != "active"
                or person["organization_id"] != run["org_id"] or person["team_id"] != run["workspace_id"]
                or person["identity_revision"] != revisions["identities"][role]
                or person["acl_revision"] != revisions["acls"][person["team_id"]]
                or (role == "issuer" and (person["principal_type"] != "human" or person["role"] != "admin"))
                or (role == "delegate" and person["principal_type"] not in {"agent", "service"})
                or (role == "accountable" and person["principal_type"] != "human")
                or (role == "reviewer" and (person["principal_type"] != "human" or person["role"] not in {"admin", "reviewer"}))):
            raise QueryRunError("Principal or ACL changed", code="governance_drift")
    allowed = set(issuance["policy"]["allowed_tables"])
    for principal_id in (mandate["issuer_id"], mandate["delegate_id"]):
        team = conn.execute("SELECT team_id FROM tenant_users WHERE user_id=?", (principal_id,)).fetchone()[0]
        actual = {row[0] for row in conn.execute("SELECT table_name FROM team_table_acl WHERE team_id=?", (team,))}
        if not allowed.issubset(actual):
            raise QueryRunError("ACL narrowed", code="governance_drift")
    return issuance


def _validate_run_access(run: dict[str, Any], actor) -> None:
    from forge.governance import connect_governance, ensure_governance_schema
    with connect_governance() as conn:
        ensure_governance_schema(conn)
        _validate_actor_binding(conn, actor)
        principal = conn.execute("SELECT status,principal_type,role,team_id FROM tenant_users WHERE user_id=?",
                                 (actor.principal_id,)).fetchone()
        if (principal is None or principal["status"] != "active" or principal["team_id"] != run["workspace_id"]
                or actor.organization_id != run["org_id"]):
            raise QueryRunError("QueryRun not found", status_code=404, code="query_run_not_found")
        human = principal["principal_type"] == "human"
        permitted = (actor.principal_id == run["principal_context"]["actor_principal"]["principal_id"]
                     and run.get("mandate_id") is not None)
        permitted = permitted or (human and actor.principal_id in {
            run["user_id"], run["reviewer_principal_id"], run["delegated_mandate"]["delegator_principal"]["principal_id"]})
        permitted = permitted or (human and principal["role"] == "admin")
        if not permitted:
            raise QueryRunError("QueryRun not found", status_code=404, code="query_run_not_found")
        if not human:
            mandate = conn.execute("SELECT status,expires_at FROM issued_mandates WHERE mandate_id=?",
                                   (run["mandate_id"],)).fetchone()
            if mandate is None or mandate["status"] != "active" or datetime.fromisoformat(mandate["expires_at"]) <= _now():
                raise QueryRunError("QueryRun not found", status_code=404, code="query_run_not_found")


async def create_governed_query_run(
    *, actor, mandate_id: str, task_run_id: str, purpose: str, question: str,
    dialect: str | None, candidate: dict[str, Any], idempotency_key: str,
) -> dict[str, Any]:
    """Persist a bounded preparation; SQL is never executed by this operation."""
    from forge.evaluate import canonical_hash
    from forge.enforce import EnforceContextError, prepare_governed_query

    if not idempotency_key:
        raise QueryRunError("Idempotency-Key is required", status_code=400, code="idempotency_key_required")
    fingerprint = canonical_hash({"actor": actor.principal_id, "mandate_id": mandate_id,
                                  "task_run_id": task_run_id, "purpose": purpose, "question": question,
                                  "dialect": dialect, "candidate": candidate})
    await _ensure_schema()
    existing = await _get_by_create_key(idempotency_key)
    if existing is not None:
        if existing.get("create_request_hash") != fingerprint or existing.get("enforce_schema_version") != 2:
            raise QueryRunError("Idempotency-Key is bound to another request", code="idempotency_key_conflict")
        await asyncio.to_thread(_validate_run_access, existing, actor)
        return existing
    try:
        prepared = await asyncio.to_thread(
            prepare_governed_query, actor=actor, mandate_id=mandate_id, task_run_id=task_run_id,
            purpose=purpose, question=question, dialect=dialect, candidate=candidate,
        )
    except EnforceContextError as exc:
        raise QueryRunError(str(exc), status_code=exc.status_code, code=exc.code) from exc
    now = _now().isoformat()
    context = prepared["context"]
    mandate = prepared["mandate"]
    report = prepared["assurance_report"]
    from forge.explain import build_explain_context
    explain_context = await asyncio.to_thread(
        build_explain_context, sql=prepared["sql"], dialect=prepared["dialect"],
        registry_revision=prepared["registry_version"], datasource_id=cfg.DATASOURCE_ID,
        resource_scope=context["resource_scope"],
    )
    row = {
        "query_run_id": "qr_" + uuid.uuid4().hex, "task_run_id": task_run_id,
        "org_id": actor.organization_id, "team_id": actor.workspace_id,
        "workspace_id": actor.workspace_id,
        "user_id": mandate["delegated_mandate"]["accountable_principal"]["principal_id"],
        "datasource_id": cfg.DATASOURCE_ID, "question": question,
        "input_kind": candidate["kind"], "candidate_revision": "query-candidate-v1",
        "candidate_payload": json.dumps(candidate, ensure_ascii=False, sort_keys=True),
        "candidate_hash": prepared["candidate_hash"], "enforce_schema_version": 2,
        "purpose": purpose, "principal_context": json.dumps(context["principal_context"], ensure_ascii=False, sort_keys=True),
        "principal_context_hash": context["principal_context_hash"],
        "delegated_mandate": json.dumps(context["delegated_mandate"], ensure_ascii=False, sort_keys=True),
        "delegated_mandate_hash": context["delegated_mandate_hash"],
        "resource_scope": json.dumps(context["resource_scope"], ensure_ascii=False, sort_keys=True),
        "resource_scope_hash": context["resource_scope_hash"],
        "policy_decision": json.dumps(prepared["policy_decision"], ensure_ascii=False, sort_keys=True),
        "policy_decision_hash": prepared["policy_decision_hash"],
        "enforcement_context_hash": prepared["enforcement_context_hash"],
        "explain_context": json.dumps(explain_context, ensure_ascii=False, sort_keys=True),
        "explain_context_hash": _artifact_hash(explain_context),
        "status": prepared["status"], "forge_json": json.dumps(candidate["forge_json"], ensure_ascii=False) if candidate["kind"] == "forge_json" else None,
        "sql": prepared["sql"], "sql_hash": report.get("sql_hash") if prepared["sql"] else None,
        "dialect": prepared["dialect"], "registry_version": prepared["registry_version"],
        "assurance_report": json.dumps(report, ensure_ascii=False, sort_keys=True),
        "assurance_report_hash": prepared["assurance_report_hash"],
        "assurance_revision": report["assurance_revision"], "policy_revision": report["policy_revision"],
        "model_revision": report["model_revision"], "assurance_registry_revision": report["registry_revision"],
        "create_idempotency_key": idempotency_key, "create_request_hash": fingerprint,
        "expires_at": prepared["expires_at"], "error": None if prepared["status"] != "denied" else "assurance_denied",
        "created_at": now, "updated_at": now, "mandate_id": mandate_id,
        "mandate_issuance_revision": mandate["issuance_revision"],
        "mandate_issuance_hash": mandate["issuance_hash"],
        "execution_mode": prepared["execution_mode"],
        "reviewer_principal_id": mandate["policy"]["reviewer_principal_id"],
        "authorization_policy_revision": mandate["policy"]["revision"],
        "governance_revision_snapshot": json.dumps(prepared["governance_revision_snapshot"], sort_keys=True),
    }

    def commit() -> str:
        from forge.governance import governance_transaction
        with governance_transaction() as conn:
            existing_row = conn.execute("SELECT * FROM query_runs WHERE create_idempotency_key=?",
                                        (idempotency_key,)).fetchone()
            if existing_row is not None:
                return existing_row["query_run_id"]
            _validate_governance_snapshot(conn, row, prepared["governance_revision_snapshot"])
            if current_registry_version() != prepared["registry_version"]:
                raise QueryRunError("Registry drift", code="registry_drift")
            keys = tuple(row)
            conn.execute(f"INSERT INTO query_runs ({', '.join(keys)}) VALUES ({', '.join('?' for _ in keys)})",
                         tuple(row[key] for key in keys))
            return row["query_run_id"]

    query_run_id = await asyncio.to_thread(commit)
    created = await get_query_run(query_run_id)
    assert created is not None
    if created.get("create_request_hash") != fingerprint or created.get("enforce_schema_version") != 2:
        raise QueryRunError("Idempotency-Key is bound to another request", code="idempotency_key_conflict")
    await asyncio.to_thread(_validate_run_access, created, actor)
    return created

async def complete_query_run(
    query_run_id: str, *, columns: list[str], rows: list[list[Any]], row_count: int,
    truncated: bool, execution_ms: int, claim_id: str | None = None,
) -> dict[str, Any]:
    await _ensure_schema()
    now = _now().isoformat()
    normalized_columns = json.loads(json.dumps(columns, ensure_ascii=False, default=str))
    normalized_rows = json.loads(json.dumps(rows, ensure_ascii=False, default=str))
    result_hash = _artifact_hash({"columns": normalized_columns, "rows": normalized_rows,
                                  "row_count": row_count, "truncated": bool(truncated),
                                  "execution_ms": execution_ms, "executed_at": now})
    # Autocommit: the write lock must not span an await before commit (see ``_read_rows``).
    async with aiosqlite.connect(_db_path(), isolation_level=None) as db:
        cursor = await db.execute(
            """UPDATE query_runs SET status='completed',result_columns=?,result_rows=?,
               result_hash=?,row_count=?,truncated=?,execution_ms=?,updated_at=?,executed_at=?,
               execution_owner=NULL,execution_lease_expires_at=NULL
               WHERE query_run_id=? AND status='executing' AND execution_owner=?
               AND (? IS NULL OR execution_claim_id=?)""",
            (json.dumps(normalized_columns, ensure_ascii=False), json.dumps(normalized_rows, ensure_ascii=False),
             result_hash, row_count, int(truncated), execution_ms, now, now,
             query_run_id, _PROCESS_OWNER, claim_id, claim_id),
        )
        if cursor.rowcount != 1:
            raise QueryRunError("QueryRun execution ownership expired; result was not persisted")
    run = await get_query_run(query_run_id)
    assert run is not None
    return run



async def fail_query_run(query_run_id: str, error: str, *, claim_id: str | None = None) -> None:
    await _ensure_schema()
    async with aiosqlite.connect(_db_path(), isolation_level=None) as db:
        await db.execute(
            """
            UPDATE query_runs SET status = 'failed', error = ?, updated_at = ?,
                execution_owner = NULL, execution_lease_expires_at = NULL
            WHERE query_run_id = ? AND status = 'executing' AND execution_owner = ?
              AND (? IS NULL OR execution_claim_id = ?)
            """,
            (error, _now().isoformat(), query_run_id, _PROCESS_OWNER, claim_id, claim_id),
        )


async def reconcile_expired_query_run_executions() -> int:
    """Fail only expired execution leases; never replay SQL after restart."""
    await _ensure_schema()
    now = _now().isoformat()
    async with aiosqlite.connect(_db_path(), isolation_level=None) as db:
        cursor = await db.execute(
            """
            UPDATE query_runs
            SET status = 'failed',
                error = 'execution_interrupted_or_lease_expired',
                updated_at = ?, execution_owner = NULL,
                execution_lease_expires_at = NULL
            WHERE status = 'executing'
              AND (execution_lease_expires_at IS NULL OR execution_lease_expires_at <= ?)
            """,
            (now, now),
        )
        return cursor.rowcount


def _claim_governed_sync(query_run_id: str, actor, action: str, hashes: dict,
                         idempotency_key: str, control_token: str | None) -> tuple[str, dict]:
    from forge.governance import governance_transaction, canonical_hash, resolve_control_token
    from forge.assurance import ASSURANCE_REVISION, POLICY_REVISION, QueryAssuranceError, assure_compiled_sql
    from forge.enforce import AUTHORIZATION_POLICY_REVISION, build_enforcement_context_hash, _scope_hash

    with governance_transaction() as conn:
        row = conn.execute("SELECT * FROM query_runs WHERE query_run_id=?", (query_run_id,)).fetchone()
        if row is None or row["enforce_schema_version"] != 2:
            raise QueryRunError("QueryRun not found", status_code=404, code="query_run_not_found")
        run = _decode_row(row)
        _validate_actor_binding(conn, actor)
        if any(not hmac.compare_digest(str(run.get(key) or ""), str(hashes.get(key) or ""))
               for key in ("sql_hash", "assurance_report_hash", "enforcement_context_hash")):
            raise QueryRunError("Reviewed hashes changed", code="review_hash_mismatch")
        authorizer = actor
        ticket = None
        if control_token is not None:
            if action != "approve" or actor.principal_id != cfg.PI_EXECUTION_PRINCIPAL_ID or actor.principal_type != "service":
                raise QueryRunError("Pi service authorization required", status_code=403, code="control_token_invalid")
            token_hash = "sha256:" + hashlib.sha256(control_token.encode()).hexdigest()
            ticket = conn.execute("SELECT * FROM governance_control_tokens WHERE token_hash=?", (token_hash,)).fetchone()
            if ticket is None:
                raise QueryRunError("Control token invalid", status_code=403, code="control_token_invalid")
            try:
                ticket, authorizer = resolve_control_token(conn, control_token, ticket["method"], ticket["path"],
                                                           ticket["body_hash"], idempotency_key)
            except Exception as exc:
                raise QueryRunError("Control token invalid", status_code=403, code="control_token_invalid") from exc
            if (ticket["action"] != "query.approve" or ticket["task_run_id"] != run["task_run_id"]
                    or ticket["query_run_id"] != query_run_id or ticket["consumed_execution_key"] is not None
                    or json.loads(ticket["approval_hashes_json"]) != hashes):
                raise QueryRunError("Approval ticket scope mismatch", status_code=403, code="control_token_invalid")
        if run["execution_idempotency_key"] == idempotency_key and run["status"] in {"executing", "completed", "failed"}:
            record = run["authorization_record"] or {}
            if (record.get("action") != action or record.get("authorizing_principal_id") != authorizer.principal_id):
                raise QueryRunError("Execution key belongs to another authorization", code="idempotency_key_conflict")
            return "replay", run
        if run["status"] == "expired":
            raise QueryRunError("QueryRun expired", code="review_expired")
        if run["status"] != ("ready" if action == "execute" else "review_required"):
            raise QueryRunError("QueryRun cannot be claimed", code="query_run_status_conflict")
        if datetime.fromisoformat(run["expires_at"]) <= _now():
            raise QueryRunError("QueryRun expired", code="review_expired")
        if action == "execute":
            if run["execution_mode"] != "autonomous" or actor.principal_id != run["principal_context"]["actor_principal"]["principal_id"]:
                raise QueryRunError("Autonomous actor required", status_code=403, code="actor_not_authorized")
        elif action == "approve":
            if (run["execution_mode"] != "human_review" or authorizer.principal_id != run["reviewer_principal_id"]
                    or authorizer.principal_type != "human" or authorizer.role not in {"reviewer", "admin"}):
                raise QueryRunError("Designated reviewer required", status_code=403, code="reviewer_not_authorized")
        else:
            raise QueryRunError("Unknown action", status_code=422, code="action_invalid")
        issuance = _validate_governance_snapshot(conn, run, run["governance_revision_snapshot"])
        if (run["registry_version"] != current_registry_version() or run["datasource_id"] != cfg.DATASOURCE_ID
                or run["assurance_revision"] != ASSURANCE_REVISION or run["policy_revision"] != POLICY_REVISION
                or run["authorization_policy_revision"] != AUTHORIZATION_POLICY_REVISION):
            raise QueryRunError("Registry or policy drift", code="governance_drift")
        if not cfg.EXECUTION_ENABLED or not cfg.DATABASE_READONLY_CONFIRMED:
            raise QueryRunError("Read-only execution unavailable", status_code=503, code="execution_unavailable")
        candidate = run["candidate_payload"]
        if (canonical_hash(candidate) != run["candidate_hash"]
                or canonical_hash(run["assurance_report"]) != run["assurance_report_hash"]
                or canonical_hash(run["policy_decision"]) != run["policy_decision_hash"]
                or canonical_hash(run["principal_context"]) != run["principal_context_hash"]
                or canonical_hash(run["delegated_mandate"]) != run["delegated_mandate_hash"]
                or _scope_hash(run["resource_scope"]) != run["resource_scope_hash"]):
            raise QueryRunError("Persisted context changed", code="enforcement_context_drift")
        report = run["assurance_report"]
        if (report["status"] != "passed" or report["sql"] != run["sql"]
                or report["sql_hash"] != run["sql_hash"] or run["sql_hash"] != _sql_hash(run["sql"])):
            raise QueryRunError("SQL integrity changed", code="assurance_context_drift")
        context_hash_record = {
            "enforce_schema_version": 2, "task_run_id": run["task_run_id"], "purpose": run["purpose"],
            "candidate_revision": run["candidate_revision"], "candidate_hash": run["candidate_hash"],
            "sql_hash": run["sql_hash"], "assurance_revision": run["assurance_revision"],
            "assurance_report_hash": run["assurance_report_hash"], "policy_revision": run["policy_revision"],
            "policy_decision_hash": run["policy_decision_hash"], "registry_version": run["registry_version"],
            "principal_context_hash": run["principal_context_hash"], "delegated_mandate_hash": run["delegated_mandate_hash"],
            "resource_scope_hash": run["resource_scope_hash"], "datasource_id": run["datasource_id"],
            "mandate_issuance_hash": run["mandate_issuance_hash"],
            "mandate_issuance_revision": run["mandate_issuance_revision"],
            "governance_revision_snapshot": run["governance_revision_snapshot"], "execution_mode": run["execution_mode"],
            "reviewer_principal_id": run["reviewer_principal_id"],
            "authorization_policy_revision": run["authorization_policy_revision"],
            "referenced_tables": report["referenced_tables"],
        }
        if build_enforcement_context_hash(context_hash_record) != run["enforcement_context_hash"]:
            raise QueryRunError("Enforcement context changed", code="enforcement_context_drift")
        allowed = set(run["delegated_mandate"]["resource_scope"][i]["resource_id"]
                      for i, resource in enumerate(run["delegated_mandate"]["resource_scope"])
                      if resource["resource_type"] == "table")
        if (not set(report["referenced_tables"]).issubset(allowed)
                or run["policy_decision"]["effect"] != ("allow" if action == "execute" else "conditional")
                or run["policy_decision"]["subject_principal_id"] != run["principal_context"]["actor_principal"]["principal_id"]):
            raise QueryRunError("Policy integrity changed", code="policy_context_drift")
        try:
            confirmed = assure_compiled_sql(run["sql"], dialect=run["dialect"],
                                            allowed_tables=sorted(allowed), input_kind=run["input_kind"])
        except QueryAssuranceError as exc:
            raise QueryRunError("Current SQL assurance denied", code="assurance_drift") from exc
        if (confirmed.sql_hash != run["sql_hash"] or set(confirmed.referenced_tables) != set(report["referenced_tables"])):
            raise QueryRunError("Physical references changed", code="assurance_drift")
        autonomy = set(issuance["policy"]["autonomous_tables"])
        if (set(confirmed.referenced_tables).issubset(autonomy)) != (action == "execute"):
            raise QueryRunError("Execution classification changed", code="policy_context_drift")
        now = _now().isoformat()
        claim_id = "claim_" + uuid.uuid4().hex
        authorization = {"schema_version": 1, "kind": "mandate" if action == "execute" else "human_approval",
                         "action": action, "actor_principal_id": run["principal_context"]["actor_principal"]["principal_id"],
                         "authorizing_principal_id": authorizer.principal_id,
                         "issuer_principal_id": run["delegated_mandate"]["delegator_principal"]["principal_id"],
                         "accountable_principal_id": run["user_id"],
                         "reviewer_principal_id": None if action == "execute" else authorizer.principal_id,
                         "mandate_id": run["mandate_id"], "mandate_issuance_revision": run["mandate_issuance_revision"],
                         "mandate_issuance_hash": run["mandate_issuance_hash"],
                         "authorization_policy_revision": run["authorization_policy_revision"],
                         "sql_hash": run["sql_hash"], "assurance_report_hash": run["assurance_report_hash"],
                         "enforcement_context_hash": run["enforcement_context_hash"],
                         "authorized_at": now, "execution_claim_id": claim_id,
                         "credential_id": authorizer.credential_id, "channel_binding_id": authorizer.channel_binding_id}
        lease = (_now() + timedelta(seconds=max(1, int(cfg.EXECUTION_TIMEOUT_SECONDS)) + 10)).isoformat()
        try:
            conn.execute("UPDATE query_runs SET status='executing', execution_idempotency_key=?, execution_claim_id=?, "
                         "authorization_record=?,authorization_hash=?, approver_user_id=?,approved_at=?,approval_hash=?, "
                         "execution_owner=?,execution_lease_expires_at=?,updated_at=? WHERE query_run_id=?",
                         (idempotency_key, claim_id, json.dumps(authorization, sort_keys=True), canonical_hash(authorization),
                          authorizer.principal_id if action == "approve" else None, now if action == "approve" else None,
                          canonical_hash(authorization) if action == "approve" else None,
                          _PROCESS_OWNER, lease, now, query_run_id))
        except Exception as exc:
            if isinstance(exc, __import__("sqlite3").IntegrityError):
                raise QueryRunError("Execution key already used", code="idempotency_key_conflict") from exc
            raise
        if ticket is not None:
            conn.execute("UPDATE governance_control_tokens SET consumed_execution_key=? WHERE token_id=? AND consumed_execution_key IS NULL",
                         (idempotency_key, ticket["token_id"]))
        return "claimed", authorization


async def execute_authorized_query_run(*, query_run_id: str, actor, action: str, hashes: dict,
                                       idempotency_key: str, control_token: str | None = None) -> dict:
    """Commit permission before starting one non-replayable SQL worker."""
    if not idempotency_key:
        raise QueryRunError("Idempotency-Key required", status_code=400, code="idempotency_key_required")
    await _ensure_schema()
    state, authorization = await asyncio.to_thread(_claim_governed_sync, query_run_id, actor, action,
                                                    hashes, idempotency_key, control_token)
    if state == "claimed":
        task = asyncio.create_task(_run_governed_worker(query_run_id, authorization["execution_claim_id"]))
        _ACTIVE_GOVERNED_WORKERS.add(task)
        task.add_done_callback(_ACTIVE_GOVERNED_WORKERS.discard)
    result = await get_query_run(query_run_id)
    assert result is not None
    result["_claim_replayed"] = state == "replay"
    return result


_ACTIVE_GOVERNED_WORKERS: set[asyncio.Task] = set()


async def _run_governed_worker(query_run_id: str, claim_id: str) -> None:
    from forge.executor import execute
    run = await get_query_run(query_run_id)
    if run is None or run["status"] != "executing" or run["execution_claim_id"] != claim_id:
        return
    started = time.perf_counter()
    try:
        result = await asyncio.to_thread(execute, run["sql"], cfg.EXECUTION_MAX_ROWS)
        if not result.success:
            await fail_query_run(query_run_id, result.error_code or "execution_failed", claim_id=claim_id)
            return
        await complete_query_run(query_run_id, columns=result.columns, rows=[list(row) for row in result.rows],
                                 row_count=len(result.rows), truncated=result.truncated,
                                 execution_ms=int((time.perf_counter() - started) * 1000), claim_id=claim_id)
    except Exception:
        await fail_query_run(query_run_id, "execution_failed", claim_id=claim_id)

async def get_governed_query_run(query_run_id: str, actor) -> dict[str, Any]:
    run = await get_query_run(query_run_id)
    if run is None or run.get("enforce_schema_version") != 2:
        raise QueryRunError("QueryRun not found", status_code=404, code="query_run_not_found")
    await asyncio.to_thread(_validate_run_access, run, actor)
    return run


async def get_governed_query_run_by_create_key(idempotency_key: str, actor) -> dict[str, Any]:
    """Let only the creator recover the run behind a create response it never received."""
    run = await _get_by_create_key(idempotency_key) if idempotency_key else None
    if (run is None or run.get("enforce_schema_version") != 2
            or run["principal_context"]["actor_principal"]["principal_id"] != actor.principal_id):
        raise QueryRunError("QueryRun not found", status_code=404, code="query_run_not_found")
    return await get_governed_query_run(run["query_run_id"], actor)


async def cancel_governed_query_run(query_run_id: str, actor) -> dict[str, Any]:
    """Cancel only a still-unclaimed run with the same authenticated scope."""
    from forge.governance import governance_transaction

    await _ensure_schema()
    run = await get_governed_query_run(query_run_id, actor)
    def cancel() -> None:
        with governance_transaction() as conn:
            _validate_actor_binding(conn, actor)
            row = conn.execute("SELECT * FROM query_runs WHERE query_run_id=?", (query_run_id,)).fetchone()
            if row is None or row["enforce_schema_version"] != 2:
                raise QueryRunError("QueryRun not found", status_code=404, code="query_run_not_found")
            if row["status"] not in {"ready", "review_required"}:
                raise QueryRunError("Claimed QueryRun cannot be cancelled", code="query_run_status_conflict")
            principal = conn.execute("SELECT status,principal_type,role,team_id FROM tenant_users WHERE user_id=?",
                                     (actor.principal_id,)).fetchone()
            if principal is None or principal["status"] != "active" or principal["team_id"] != row["workspace_id"]:
                raise QueryRunError("QueryRun not found", status_code=404, code="query_run_not_found")
            permitted = actor.principal_id in {row["user_id"], row["reviewer_principal_id"]}
            permitted = permitted or (principal["principal_type"] == "human" and principal["role"] == "admin")
            permitted = permitted or actor.principal_id == run["principal_context"]["actor_principal"]["principal_id"]
            if not permitted:
                raise QueryRunError("QueryRun not found", status_code=404, code="query_run_not_found")
            conn.execute("UPDATE query_runs SET status='cancelled',updated_at=? WHERE query_run_id=?",
                         (_now().isoformat(), query_run_id))
    await asyncio.to_thread(cancel)
    result = await get_query_run(query_run_id)
    assert result is not None
    return result

async def list_reviewer_query_runs(actor, limit: int = 50) -> list[dict[str, Any]]:
    if actor.principal_type != "human" or actor.role not in {"reviewer", "admin"}:
        raise QueryRunError("Reviewer required", status_code=403, code="reviewer_not_authorized")
    await _ensure_schema()
    await _settle_time_decided_query_runs()
    rows = await _read_rows(
        "SELECT * FROM query_runs WHERE enforce_schema_version=2 AND status='review_required' "
        "AND reviewer_principal_id=? AND org_id=? AND workspace_id=? "
        "ORDER BY created_at DESC LIMIT ?",
        (actor.principal_id, actor.organization_id, actor.workspace_id, min(max(limit, 1), 50)),
    )
    for row in rows:
        await asyncio.to_thread(_validate_run_access, row, actor)
    return rows
