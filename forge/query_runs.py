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
from typing import Any, Callable

import aiosqlite
from jsonschema import ValidationError

from agent.contracts import validate_contract

from config import cfg

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
    path = Path(cfg.QUERY_RUN_DB_PATH).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


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


async def get_query_run(query_run_id: str) -> dict[str, Any] | None:
    await _ensure_schema()
    async with aiosqlite.connect(_db_path()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM query_runs WHERE query_run_id = ?",
            (query_run_id,),
        )
        row = await cursor.fetchone()
        return _decode_row(row) if row else None
async def list_governed_query_runs(limit: int = 8) -> list[dict[str, Any]]:
    """Return recent governed QueryRuns for read-only administrative projections."""
    await _ensure_schema()
    bounded_limit = min(max(int(limit), 1), 50)
    async with aiosqlite.connect(_db_path()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT * FROM query_runs
            WHERE enforce_schema_version = 1
            ORDER BY created_at DESC, query_run_id DESC
            LIMIT ?
            """,
            (bounded_limit,),
        )
        return [_decode_row(row) for row in await cursor.fetchall()]


async def _get_by_create_key(key: str) -> dict[str, Any] | None:
    await _ensure_schema()
    async with aiosqlite.connect(_db_path()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM query_runs WHERE create_idempotency_key = ?",
            (key,),
        )
        row = await cursor.fetchone()
        return _decode_row(row) if row else None



def _prepare_supplied_candidate(
    user_id: str,
    question: str,
    dialect: str | None,
    candidate: dict[str, Any],
    scope_allowed_tables: list[str] | None = None,
) -> dict[str, Any]:
    from agent.agent import resolve_dialect
    from agent.tenant import tenants
    from forge.assurance import QueryAssuranceError, assure_direct_sql, assure_query

    resolved_dialect = resolve_dialect(dialect)
    tenant_allowed_tables = tenants.get_allowed_tables_for_user(user_id)
    if tenant_allowed_tables is not None and scope_allowed_tables is not None:
        allowed_tables = sorted(set(tenant_allowed_tables) & set(scope_allowed_tables))
    elif scope_allowed_tables is not None:
        allowed_tables = scope_allowed_tables
    else:
        allowed_tables = tenant_allowed_tables
    input_kind = candidate["kind"]
    producer_revision = str(candidate.get("producer_revision") or "external")
    forge_json = candidate.get("forge_json") if input_kind == "forge_json" else None
    try:
        if input_kind == "direct_sql":
            assurance = assure_direct_sql(
                candidate["sql"],
                dialect=resolved_dialect,
                allowed_tables=allowed_tables,
                producer_revision=producer_revision,
            )
        else:
            assurance = assure_query(
                forge_json,
                question,
                dialect=resolved_dialect,
                allowed_tables=allowed_tables,
                model_revision=producer_revision,
            )
    except QueryAssuranceError as exc:
        return {
            "status": "error",
            "input_kind": input_kind,
            "forge_json": forge_json,
            "sql": None,
            "dialect": resolved_dialect,
            "assurance_report": exc.report.to_dict(),
            "error": str(exc),
        }

    return {
        "status": "needs_review",
        "input_kind": input_kind,
        "forge_json": forge_json,
        "sql": assurance.sql,
        "dialect": resolved_dialect,
        "assurance_report": assurance.to_dict(),
        "error": "",
    }


async def create_query_run(
    *,
    task_run_id: str,
    org_id: str,
    team_id: str,
    user_id: str,
    question: str,
    dialect: str | None,
    idempotency_key: str,
    candidate: dict[str, Any] | None = None,
    prepare_fn: Callable[[str, str, str | None], dict[str, Any]] | None = None,
    governance_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from forge.evaluate import canonical_hash

    if not idempotency_key:
        raise QueryRunError(
            "Idempotency-Key is required",
            status_code=400,
            code="idempotency_key_required",
        )
    normalized_candidate = None
    if candidate is not None:
        try:
            validate_contract("query_candidate_v1", candidate)
        except ValidationError as exc:
            raise QueryRunError(
                "Invalid query candidate",
                status_code=400,
                code="candidate_contract_invalid",
            ) from exc
        normalized_candidate = dict(candidate)
        normalized_candidate["producer_revision"] = str(
            candidate.get("producer_revision") or "external"
        )

    existing = await _get_by_create_key(idempotency_key)
    if existing:
        mismatch = existing["task_run_id"] != task_run_id
        if governance_context is not None:
            mismatch = mismatch or any(
                (
                    existing.get("enforce_schema_version") != 1,
                    existing.get("purpose") != governance_context["purpose"],
                    existing.get("principal_context_hash")
                    != governance_context["principal_context_hash"],
                    existing.get("delegated_mandate_hash")
                    != governance_context["delegated_mandate_hash"],
                    existing.get("resource_scope_hash")
                    != governance_context["resource_scope_hash"],
                    existing.get("candidate_hash")
                    != canonical_hash(normalized_candidate),
                )
            )
        elif existing.get("enforce_schema_version") is not None:
            mismatch = True
        if mismatch:
            raise QueryRunError(
                "Idempotency-Key is already bound to another request",
                code="idempotency_key_conflict",
            )
        return existing

    registry_version_before = current_registry_version()
    if candidate is None:
        if prepare_fn is None:
            from agent.agent import prepare_query as prepare_fn
        prepared = await asyncio.to_thread(prepare_fn, user_id, question, dialect)
    else:
        prepared = await asyncio.to_thread(
            _prepare_supplied_candidate,
            user_id,
            question,
            dialect,
            normalized_candidate,
            governance_context.get("allowed_tables") if governance_context else None,
        )
    registry_version_after = current_registry_version()
    status_map = {
        "needs_review": "needs_review",
        "needs_clarification": "needs_clarification",
        "timed_out": "timed_out",
        "error": "failed",
    }
    status = status_map.get(prepared.get("status"), "failed")
    sql = prepared.get("sql")
    now = _now()
    ttl = max(1, int(cfg.QUERY_RUN_REVIEW_TTL_SECONDS))
    expires_at = now + timedelta(seconds=ttl)
    query_run_id = "qr_" + uuid.uuid4().hex
    input_kind = str(prepared.get("input_kind") or "forge_json")
    forge_json = prepared.get("forge_json")
    assurance_report = prepared.get("assurance_report")
    if normalized_candidate is None and isinstance(assurance_report, dict):
        normalized_candidate = {
            "kind": input_kind,
            "producer_revision": str(
                assurance_report.get("model_revision") or "external"
            ),
        }
        if input_kind == "forge_json":
            normalized_candidate["forge_json"] = forge_json
        elif sql:
            normalized_candidate["sql"] = sql
    if status == "needs_review" and registry_version_before != registry_version_after:
        status = "failed"
        sql = None
        error = "Registry changed while preparing the query; prepare it again"
    elif status == "needs_review" and not isinstance(assurance_report, dict):
        status = "failed"
        sql = None
        error = "Query assurance report is required for review"
    elif status == "needs_review" and (
        assurance_report.get("status") != "passed"
        or assurance_report.get("input_kind") != input_kind
        or assurance_report.get("candidate_revision") != "query-candidate-v1"
        or assurance_report.get("sql") != sql
        or assurance_report.get("sql_hash") != _sql_hash(sql or "")
        or (input_kind == "forge_json" and not isinstance(forge_json, dict))
        or (input_kind == "direct_sql" and forge_json is not None)
    ):
        status = "failed"
        sql = None
        error = "Query assurance report does not match the prepared candidate"
    else:
        error = prepared.get("error") or (
            prepared.get("text") if status != "needs_review" else None
        )
    assurance_report_hash = (
        _artifact_hash(assurance_report) if isinstance(assurance_report, dict) else None
    )
    candidate_hash = (
        canonical_hash(normalized_candidate)
        if isinstance(normalized_candidate, dict)
        else None
    )
    candidate_revision = (
        assurance_report.get("candidate_revision", "query-candidate-v1")
        if isinstance(assurance_report, dict)
        else "query-candidate-v1"
    )

    explain_context = None
    explain_context_hash = None
    policy_decision = None
    policy_decision_hash = None
    enforcement_context_hash = None
    if governance_context is not None:
        from forge.explain import build_explain_context
        from forge.enforce import (
            ENFORCE_SCHEMA_VERSION,
            build_enforcement_context_hash,
            build_policy_decision,
        )

        policy_decision = build_policy_decision(
            governance_context,
            assurance_report=assurance_report,
            evaluated_at=now,
            expires_at=expires_at,
        )
        policy_decision_hash = canonical_hash(policy_decision)
        hash_record = {
            "task_run_id": task_run_id,
            "purpose": governance_context["purpose"],
            "candidate_revision": candidate_revision,
            "candidate_hash": candidate_hash,
            "sql_hash": _sql_hash(sql) if sql else None,
            "assurance_revision": assurance_report.get("assurance_revision")
            if isinstance(assurance_report, dict) else None,
            "assurance_report_hash": assurance_report_hash,
            "policy_revision": assurance_report.get("policy_revision")
            if isinstance(assurance_report, dict) else None,
            "policy_decision_hash": policy_decision_hash,
            "registry_version": registry_version_after,
            "principal_context_hash": governance_context["principal_context_hash"],
            "delegated_mandate_hash": governance_context["delegated_mandate_hash"],
            "resource_scope_hash": governance_context["resource_scope_hash"],
            "datasource_id": cfg.DATASOURCE_ID,
        }
        enforcement_context_hash = build_enforcement_context_hash(hash_record)
        enforce_schema_version = ENFORCE_SCHEMA_VERSION
        explain_context = build_explain_context(
            sql=sql,
            dialect=str(prepared.get("dialect") or dialect or ""),
            registry_revision=registry_version_after,
            datasource_id=cfg.DATASOURCE_ID,
            resource_scope=governance_context["resource_scope"],
        )
        explain_context_hash = _artifact_hash(explain_context)
    else:
        enforce_schema_version = None

    columns = (
        "query_run_id, task_run_id, org_id, team_id, user_id, datasource_id, "
        "question, input_kind, candidate_revision, candidate_payload, candidate_hash, "
        "enforce_schema_version, workspace_id, purpose, principal_context, "
        "principal_context_hash, delegated_mandate, delegated_mandate_hash, "
        "resource_scope, resource_scope_hash, policy_decision, policy_decision_hash, "
        "enforcement_context_hash, explain_context, explain_context_hash, status, "
        "forge_json, sql, sql_hash, dialect, "
        "registry_version, assurance_report, assurance_report_hash, "
        "assurance_revision, policy_revision, model_revision, "
        "assurance_registry_revision, create_idempotency_key, expires_at, error, "
        "created_at, updated_at"
    )
    values = (
        query_run_id,
        task_run_id,
        org_id,
        team_id,
        user_id,
        cfg.DATASOURCE_ID,
        question,
        input_kind,
        candidate_revision,
        json.dumps(normalized_candidate, ensure_ascii=False, sort_keys=True)
        if normalized_candidate is not None else None,
        candidate_hash,
        enforce_schema_version,
        governance_context["principal_context"]["workspace_id"]
        if governance_context else None,
        governance_context["purpose"] if governance_context else None,
        json.dumps(governance_context["principal_context"], ensure_ascii=False, sort_keys=True)
        if governance_context else None,
        governance_context["principal_context_hash"] if governance_context else None,
        json.dumps(governance_context["delegated_mandate"], ensure_ascii=False, sort_keys=True)
        if governance_context and governance_context["delegated_mandate"] is not None else None,
        governance_context["delegated_mandate_hash"] if governance_context else None,
        json.dumps(governance_context["resource_scope"], ensure_ascii=False, sort_keys=True)
        if governance_context else None,
        governance_context["resource_scope_hash"] if governance_context else None,
        json.dumps(policy_decision, ensure_ascii=False, sort_keys=True)
        if policy_decision is not None else None,
        policy_decision_hash,
        enforcement_context_hash,
        json.dumps(explain_context, ensure_ascii=False, sort_keys=True)
        if explain_context is not None else None,
        explain_context_hash,
        status,
        json.dumps(forge_json, ensure_ascii=False) if forge_json is not None else None,
        sql,
        _sql_hash(sql) if sql else None,
        prepared.get("dialect") or dialect or "",
        registry_version_after,
        json.dumps(assurance_report, ensure_ascii=False, sort_keys=True)
        if isinstance(assurance_report, dict) else None,
        assurance_report_hash,
        assurance_report.get("assurance_revision")
        if isinstance(assurance_report, dict) else None,
        assurance_report.get("policy_revision")
        if isinstance(assurance_report, dict) else None,
        assurance_report.get("model_revision")
        if isinstance(assurance_report, dict) else None,
        assurance_report.get("registry_revision")
        if isinstance(assurance_report, dict) else None,
        idempotency_key,
        expires_at.isoformat(),
        error,
        now.isoformat(),
        now.isoformat(),
    )

    await _ensure_schema()
    try:
        async with aiosqlite.connect(_db_path()) as db:
            placeholders = ", ".join("?" for _ in values)
            await db.execute(
                f"INSERT INTO query_runs ({columns}) VALUES ({placeholders})",
                values,
            )
            await db.commit()
    except aiosqlite.IntegrityError:
        existing = await _get_by_create_key(idempotency_key)
        if existing:
            return existing
        raise

    created = await get_query_run(query_run_id)
    assert created is not None
    return created

async def claim_query_run_for_execution(
    *,
    query_run_id: str,
    approver_user_id: str,
    sql_hash: str,
    assurance_report_hash: str,
    idempotency_key: str,
    enforcement_context_hash: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Atomically claim a reviewed QueryRun before executing outside the DB lock."""
    if not idempotency_key:
        raise QueryRunError(
            "Idempotency-Key is required",
            status_code=400,
            code="idempotency_key_required",
        )
    await _ensure_schema()
    async with aiosqlite.connect(_db_path()) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")
        cursor = await db.execute(
            "SELECT * FROM query_runs WHERE query_run_id = ?",
            (query_run_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            await db.rollback()
            raise QueryRunError(
                "QueryRun not found",
                status_code=404,
                code="query_run_not_found",
            )
        run = _decode_row(row)

        if not run.get("sql_hash") or not hmac.compare_digest(run["sql_hash"], sql_hash):
            await db.rollback()
            raise QueryRunError(
                "SQL hash does not match the reviewed query",
                code="sql_hash_mismatch",
            )
        if not run.get("assurance_report_hash") or not hmac.compare_digest(
            run["assurance_report_hash"], assurance_report_hash
        ):
            await db.rollback()
            raise QueryRunError(
                "Assurance report hash does not match the reviewed query",
                code="assurance_hash_mismatch",
            )
        if run.get("enforce_schema_version") is not None:
            if not enforcement_context_hash or not hmac.compare_digest(
                str(run.get("enforcement_context_hash") or ""),
                enforcement_context_hash,
            ):
                await db.rollback()
                raise QueryRunError(
                    "Enforcement context hash does not match the reviewed query",
                    code="enforcement_context_hash_mismatch",
                )

        if run["status"] == "completed" and hmac.compare_digest(
            str(run.get("approval_idempotency_key") or ""), idempotency_key
        ):
            await db.rollback()
            return "replay", run
        if run["status"] != "needs_review":
            await db.rollback()
            raise QueryRunError(
                f"QueryRun cannot be approved from status: {run['status']}",
                code="query_run_status_conflict",
            )
        if not hmac.compare_digest(run["user_id"], approver_user_id):
            await db.rollback()
            raise QueryRunError(
                "Approver is not authorized for this QueryRun",
                status_code=403,
                code="approver_not_authorized",
            )
        if _now() > datetime.fromisoformat(run["expires_at"]):
            await db.execute(
                "UPDATE query_runs SET status = 'expired', updated_at = ? WHERE query_run_id = ?",
                (_now().isoformat(), query_run_id),
            )
            await db.commit()
            raise QueryRunError("QueryRun review has expired", code="review_expired")
        if run["registry_version"] != current_registry_version():
            await db.rollback()
            raise QueryRunError(
                "Registry changed after review; prepare the query again",
                code="registry_drift",
            )
        if run["datasource_id"] != cfg.DATASOURCE_ID:
            await db.rollback()
            raise QueryRunError(
                "Datasource changed after review; prepare the query again",
                code="datasource_drift",
            )
        from forge.assurance import ASSURANCE_REVISION, POLICY_REVISION
        if run.get("assurance_revision") != ASSURANCE_REVISION:
            await db.rollback()
            raise QueryRunError(
                "Query assurance revision changed; prepare the query again",
                code="assurance_revision_drift",
            )
        if run.get("policy_revision") != POLICY_REVISION:
            await db.rollback()
            raise QueryRunError(
                "Query policy revision changed; prepare the query again",
                code="policy_revision_drift",
            )
        if run.get("enforce_schema_version") is not None:
            from forge.enforce import EnforceContextError, validate_stored_enforcement_context

            try:
                validate_stored_enforcement_context(run)
            except EnforceContextError as exc:
                await db.rollback()
                raise QueryRunError(
                    str(exc),
                    status_code=exc.status_code,
                    code=exc.code,
                ) from exc
        if not cfg.EXECUTION_ENABLED:
            await db.rollback()
            raise QueryRunError(
                "SQL execution is disabled",
                status_code=503,
                code="execution_disabled",
            )
        if not cfg.DATABASE_READONLY_CONFIRMED:
            await db.rollback()
            raise QueryRunError(
                "Database read-only account is not confirmed",
                status_code=503,
                code="readonly_identity_unconfirmed",
            )

        now_dt = _now()
        now = now_dt.isoformat()
        approval_hash = _artifact_hash(
            {
                "approver_principal": {
                    "principal_id": approver_user_id,
                    "principal_type": "human",
                },
                "approved_at": now,
                "sql_hash": run.get("sql_hash"),
                "assurance_report_hash": run.get("assurance_report_hash"),
                "enforcement_context_hash": run.get("enforcement_context_hash"),
            }
        )
        lease_expires_at = (
            now_dt + timedelta(seconds=max(int(cfg.EXECUTION_TIMEOUT_SECONDS), 1) + 10)
        ).isoformat()
        try:
            await db.execute(
                """
                UPDATE query_runs
                SET status = 'executing', approval_idempotency_key = ?,
                    approver_user_id = ?, approved_at = ?, approval_hash = ?,
                    updated_at = ?, execution_owner = ?, execution_lease_expires_at = ?
                WHERE query_run_id = ?
                """,
                (
                    idempotency_key,
                    approver_user_id,
                    now,
                    approval_hash,
                    now,
                    _PROCESS_OWNER,
                    lease_expires_at,
                    query_run_id,
                ),
            )
            await db.commit()
        except aiosqlite.IntegrityError as exc:
            await db.rollback()
            raise QueryRunError(
                "Approval Idempotency-Key is already in use",
                code="approval_idempotency_key_conflict",
            ) from exc

    claimed = await get_query_run(query_run_id)
    assert claimed is not None
    return "claimed", claimed

async def complete_query_run(
    query_run_id: str,
    *,
    columns: list[str],
    rows: list[list[Any]],
    row_count: int,
    truncated: bool,
    execution_ms: int,
) -> dict[str, Any]:
    await _ensure_schema()
    now = _now().isoformat()
    normalized_columns = json.loads(
        json.dumps(columns, ensure_ascii=False, default=str)
    )
    normalized_rows = json.loads(json.dumps(rows, ensure_ascii=False, default=str))
    result_hash = _artifact_hash(
        {
            "columns": normalized_columns,
            "rows": normalized_rows,
            "row_count": row_count,
            "truncated": bool(truncated),
            "execution_ms": execution_ms,
            "executed_at": now,
        }
    )
    async with aiosqlite.connect(_db_path()) as db:
        cursor = await db.execute(
            """
            UPDATE query_runs
            SET status = 'completed', result_columns = ?, result_rows = ?,
                result_hash = ?, row_count = ?, truncated = ?, execution_ms = ?,
                updated_at = ?, execution_owner = NULL, execution_lease_expires_at = NULL
            WHERE query_run_id = ? AND status = 'executing' AND execution_owner = ?
            """,
            (
                json.dumps(normalized_columns, ensure_ascii=False),
                json.dumps(normalized_rows, ensure_ascii=False),
                result_hash,
                row_count,
                int(truncated),
                execution_ms,
                now,
                query_run_id,
                _PROCESS_OWNER,
            ),
        )
        await db.commit()
        if cursor.rowcount != 1:
            raise QueryRunError("QueryRun execution ownership expired; result was not persisted")
    run = await get_query_run(query_run_id)
    assert run is not None
    return run


async def approve_and_execute_query_run(
    *,
    query_run_id: str,
    approver_user_id: str,
    sql_hash: str,
    assurance_report_hash: str,
    idempotency_key: str,
    enforcement_context_hash: str | None = None,
) -> dict[str, Any]:
    claim_status, run = await claim_query_run_for_execution(
        query_run_id=query_run_id,
        approver_user_id=approver_user_id,
        sql_hash=sql_hash,
        assurance_report_hash=assurance_report_hash,
        idempotency_key=idempotency_key,
        enforcement_context_hash=enforcement_context_hash,
    )
    if claim_status == "replay":
        return run

    from forge.executor import execute_with_metadata

    started = time.perf_counter()
    try:
        result = await asyncio.to_thread(
            execute_with_metadata,
            run["sql"],
            cfg.EXECUTION_MAX_ROWS,
        )
    except Exception as exc:
        await fail_query_run(query_run_id, "execution_failed")
        raise QueryRunError(
            "Query execution failed",
            status_code=500,
            code="execution_failed",
        ) from exc
    execution_ms = int((time.perf_counter() - started) * 1000)
    if result.text.startswith("⚠"):
        await fail_query_run(query_run_id, "execution_failed")
        raise QueryRunError(
            "Query execution failed",
            status_code=500,
            code="execution_failed",
        )

    return await complete_query_run(
        query_run_id,
        columns=result.columns,
        rows=[list(row) for row in result.rows],
        row_count=len(result.rows),
        truncated=result.truncated,
        execution_ms=execution_ms,
    )

async def fail_query_run(query_run_id: str, error: str) -> None:
    await _ensure_schema()
    async with aiosqlite.connect(_db_path()) as db:
        await db.execute(
            """
            UPDATE query_runs SET status = 'failed', error = ?, updated_at = ?,
                execution_owner = NULL, execution_lease_expires_at = NULL
            WHERE query_run_id = ? AND status = 'executing' AND execution_owner = ?
            """,
            (error, _now().isoformat(), query_run_id, _PROCESS_OWNER),
        )
        await db.commit()


async def reconcile_expired_query_run_executions() -> int:
    """Fail only expired execution leases; never replay SQL after restart."""
    await _ensure_schema()
    now = _now().isoformat()
    async with aiosqlite.connect(_db_path()) as db:
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
        await db.commit()
        return cursor.rowcount


async def cancel_query_run(query_run_id: str, user_id: str) -> dict[str, Any]:
    await _ensure_schema()
    async with aiosqlite.connect(_db_path()) as db:
        cursor = await db.execute(
            """
            UPDATE query_runs SET status = 'cancelled', updated_at = ?
            WHERE query_run_id = ? AND user_id = ? AND status = 'needs_review'
            """,
            (_now().isoformat(), query_run_id, user_id),
        )
        await db.commit()
        if cursor.rowcount != 1:
            raise QueryRunError("QueryRun cannot be cancelled", status_code=409)
    run = await get_query_run(query_run_id)
    assert run is not None
    return run
