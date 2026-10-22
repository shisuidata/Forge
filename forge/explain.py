"""Stable Evidence, lineage, and limitation projection for governed QueryRuns."""
from __future__ import annotations

import hashlib
import hmac
import json
from pathlib import Path
from typing import Any
from datetime import datetime, timezone

from jsonschema import ValidationError
from sqlglot import exp, parse_one
from sqlglot.errors import SqlglotError

from agent.contracts import validate_contract
from config import cfg
from forge.enforce import (
    ENFORCE_SCHEMA_VERSION,
    build_enforcement_context_hash,
    canonical_hash,
    project_enforce_query_run,
)

EXPLAIN_SCHEMA_VERSION = 1
EXPLAIN_REVISION = "explain-query-v1"
_SQLGLOT_DIALECTS = {"postgresql": "postgres"}


class ExplainError(Exception):
    """Bounded public Explain failure."""

    def __init__(self, code: str, message: str, *, status_code: int = 409):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def _same_hash(left: str | None, right: str | None) -> bool:
    return bool(left and right) and hmac.compare_digest(left, right)


def _sql_hash(sql: str) -> str:
    return "sha256:" + hashlib.sha256(sql.encode("utf-8")).hexdigest()


def _extract_sql_references(
    sql: str | None,
    dialect: str,
) -> tuple[list[str], list[tuple[str, str]], list[str], str]:
    if not sql:
        return [], [], [], "unavailable"
    try:
        expression = parse_one(sql, read=_SQLGLOT_DIALECTS.get(dialect, dialect or None))
    except (SqlglotError, ValueError):
        return [], [], [], "unavailable"

    cte_names = {cte.alias_or_name for cte in expression.find_all(exp.CTE)}
    physical_table_nodes = [
        table
        for table in expression.find_all(exp.Table)
        if table.name and table.name not in cte_names
    ]
    physical_tables = sorted({table.name for table in physical_table_nodes})
    aliases = {
        table.alias_or_name: table.name
        for table in physical_table_nodes
        if table.alias_or_name
    }
    aliases.update({table.name: table.name for table in physical_table_nodes})

    columns: set[tuple[str, str]] = set()
    unresolved: set[str] = set()
    for column in expression.find_all(exp.Column):
        table_name = aliases.get(column.table) if column.table else None
        if table_name is None and not column.table and len(physical_tables) == 1:
            table_name = physical_tables[0]
        if table_name and column.name:
            columns.add((table_name, column.name))
        elif column.name:
            unresolved.add(column.sql())
    if any(True for _ in expression.find_all(exp.Star)):
        unresolved.add("*")

    status = "complete" if physical_tables and not unresolved else "partial"
    return physical_tables, sorted(columns), sorted(unresolved), status


def _registry_tables() -> tuple[dict[str, Any], bool]:
    try:
        raw = json.loads(Path(cfg.REGISTRY_PATH).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return {}, False
    tables = raw.get("tables", raw) if isinstance(raw, dict) else {}
    return (tables if isinstance(tables, dict) else {}), True


def build_explain_context(
    *,
    sql: str | None,
    dialect: str,
    registry_revision: str,
    datasource_id: str,
    resource_scope: list[dict[str, Any]],
) -> dict[str, Any]:
    """Capture bounded source and schema semantics at QueryRun preparation time."""
    physical_tables, columns, unresolved, extraction_status = _extract_sql_references(
        sql, dialect
    )
    registry_tables, registry_available = _registry_tables()
    bindings: list[dict[str, Any]] = []
    for table_name in physical_tables:
        table = registry_tables.get(table_name)
        description = table.get("description") if isinstance(table, dict) else None
        bindings.append(
            {
                "binding_type": "table",
                "identifier": table_name,
                "description": str(description) if description is not None else None,
                "registry_revision": registry_revision,
            }
        )
    for table_name, column_name in columns:
        table = registry_tables.get(table_name)
        table_columns = table.get("columns", {}) if isinstance(table, dict) else {}
        column = table_columns.get(column_name) if isinstance(table_columns, dict) else None
        description = column.get("description") if isinstance(column, dict) else None
        bindings.append(
            {
                "binding_type": "column",
                "identifier": f"{table_name}.{column_name}",
                "description": str(description) if description is not None else None,
                "registry_revision": registry_revision,
            }
        )
    if not registry_available and extraction_status == "complete":
        extraction_status = "partial"
    return {
        "schema_version": EXPLAIN_SCHEMA_VERSION,
        "registry_revision": registry_revision,
        "datasource_id": datasource_id,
        "resource_scope": resource_scope,
        "physical_tables": physical_tables,
        "bindings": bindings,
        "unresolved_columns": unresolved,
        "extraction_status": extraction_status,
        "registry_snapshot_available": registry_available,
    }


def build_approval_record(run: dict[str, Any]) -> dict[str, Any] | None:
    approved_at = run.get("approved_at")
    approver = run.get("approver_user_id")
    if not approved_at or not approver:
        return None
    return {
        "approver_principal": {
            "principal_id": approver,
            "principal_type": "human",
        },
        "approved_at": approved_at,
        "sql_hash": run.get("sql_hash"),
        "assurance_report_hash": run.get("assurance_report_hash"),
        "enforcement_context_hash": run.get("enforcement_context_hash"),
    }


def build_result_record(run: dict[str, Any]) -> dict[str, Any] | None:
    if run.get("status") != "completed":
        return None
    return {
        "columns": run.get("result_columns") or [],
        "rows": run.get("result_rows") or [],
        "row_count": run.get("row_count") or 0,
        "execution_ms": run.get("execution_ms"),
        "truncated": bool(run.get("truncated")),
        "executed_at": run.get("executed_at") if run.get("enforce_schema_version") == 2 else run.get("updated_at"),
    }

def _validate_hash_anchors(run: dict[str, Any]) -> tuple[list[str], list[str]]:
    if run.get("enforce_schema_version") not in {ENFORCE_SCHEMA_VERSION, 2}:
        raise ExplainError(
            "explain_query_run_not_found",
            "Governed QueryRun not found",
            status_code=404,
        )

    candidate = run.get("candidate_payload")
    try:
        validate_contract("query_candidate_v1", candidate)
    except (ValidationError, TypeError) as exc:
        raise ExplainError(
            "candidate_evidence_drift", "Persisted candidate evidence is invalid"
        ) from exc
    if not _same_hash(canonical_hash(candidate), run.get("candidate_hash")):
        raise ExplainError(
            "candidate_evidence_drift", "Persisted candidate hash does not match"
        )
    if run.get("enforce_schema_version") == 2:
        for field in ("principal_context", "delegated_mandate"):
            value = run.get(field)
            if not isinstance(value, dict) or not _same_hash(canonical_hash(value), run.get(f"{field}_hash")):
                raise ExplainError("mandate_evidence_drift", "Persisted governance context does not match")
        if not _same_hash(canonical_hash(sorted(run.get("resource_scope") or [], key=canonical_hash)),
                          run.get("resource_scope_hash")):
            raise ExplainError("source_context_drift", "Persisted resource scope does not match")

    sql = run.get("sql")
    if sql is not None and not _same_hash(_sql_hash(sql), run.get("sql_hash")):
        raise ExplainError("sql_evidence_drift", "Persisted SQL hash does not match")
    if sql is None and run.get("sql_hash") is not None:
        raise ExplainError("sql_evidence_drift", "Persisted SQL evidence is inconsistent")

    assurance = run.get("assurance_report")
    if assurance is not None and not _same_hash(
        canonical_hash(assurance), run.get("assurance_report_hash")
    ):
        raise ExplainError(
            "assurance_evidence_drift", "Persisted Assurance hash does not match"
        )
    if assurance is None and run.get("assurance_report_hash") is not None:
        raise ExplainError(
            "assurance_evidence_drift", "Persisted Assurance evidence is inconsistent"
        )

    policy = run.get("policy_decision")
    try:
        validate_contract("policy_decision_v1", policy)
    except (ValidationError, TypeError) as exc:
        raise ExplainError("policy_evidence_drift", "Persisted Policy evidence is invalid") from exc
    if not _same_hash(canonical_hash(policy), run.get("policy_decision_hash")):
        raise ExplainError("policy_evidence_drift", "Persisted Policy hash does not match")
    context_record = run if run.get("enforce_schema_version") == 1 else {
        **run, "referenced_tables": (assurance or {}).get("referenced_tables", []),
    }
    if not _same_hash(build_enforcement_context_hash(context_record), run.get("enforcement_context_hash")):
        raise ExplainError("enforcement_context_drift", "Persisted enforcement context hash does not match")

    verified = ["candidate", "policy", "enforcement_context"]
    if sql is not None:
        verified.append("sql")
    if assurance is not None:
        verified.append("assurance")
    unverified: list[str] = []

    explain_context = run.get("explain_context")
    explain_context_hash = run.get("explain_context_hash")
    if explain_context is None and explain_context_hash is None:
        if run.get("enforce_schema_version") == 2:
            raise ExplainError("source_context_drift", "Governed source context anchor is missing")
        unverified.append("source_context")
    elif not isinstance(explain_context, dict) or not _same_hash(
        canonical_hash(explain_context), explain_context_hash
    ):
        raise ExplainError(
            "source_context_drift", "Persisted source context hash does not match"
        )
    else:
        if (
            explain_context.get("registry_revision") != run.get("registry_version")
            or explain_context.get("datasource_id") != run.get("datasource_id")
            or canonical_hash(sorted(explain_context.get("resource_scope") or [], key=canonical_hash))
            != run.get("resource_scope_hash")
        ):
            raise ExplainError("source_context_drift", "Persisted source context bindings do not match")
        verified.append("source_context")

    approval = build_approval_record(run)
    if run.get("enforce_schema_version") == 2:
        if run.get("execution_mode") == "autonomous" and (approval is not None or run.get("approval_hash") is not None):
            raise ExplainError("approval_evidence_drift", "Autonomous execution cannot carry human approval")
        if run.get("execution_mode") == "human_review" and run.get("authorization_record") is not None:
            if not approval or not _same_hash(run.get("approval_hash"), run.get("authorization_hash")):
                raise ExplainError("approval_evidence_drift", "Human approval is not bound to authorization")
            verified.append("approval")
    elif approval is not None:
        if run.get("approval_hash") is None:
            unverified.append("approval")
        elif not _same_hash(canonical_hash(approval), run.get("approval_hash")):
            raise ExplainError("approval_evidence_drift", "Persisted approval hash does not match")
        else:
            verified.append("approval")

    result = build_result_record(run)
    if result is not None:
        if run.get("result_hash") is None:
            if run.get("enforce_schema_version") == 2:
                raise ExplainError("result_evidence_drift", "Governed result anchor is missing")
            unverified.append("result")
        elif not _same_hash(canonical_hash(result), run.get("result_hash")):
            raise ExplainError(
                "result_evidence_drift", "Persisted result hash does not match"
            )
        else:
            verified.append("result")
    return verified, unverified


def _evidence_item(
    run_id: str,
    evidence_type: str,
    summary: str,
    content_hash: str,
) -> dict[str, Any]:
    source_ref = f"query-run:{run_id}#{evidence_type}"
    evidence_id = "ev_" + canonical_hash(
        {"source_ref": source_ref, "content_hash": content_hash}
    ).removeprefix("sha256:")[:24]
    return {
        "evidence_id": evidence_id,
        "evidence_type": evidence_type,
        "summary": summary,
        "source_ref": source_ref,
        "content_hash": content_hash,
    }


def _limitations(
    run: dict[str, Any],
    enforce_projection: dict[str, Any],
    explain_context: dict[str, Any],
    unverified: list[str],
) -> list[dict[str, str]]:
    limitations = [
        {
            "code": "semantic_correctness_unproven",
            "severity": "warning",
            "description": "Schema bindings and deterministic gates do not prove that the SQL answers the intended business question.",
        },
        {
            "code": "live_data_no_snapshot",
            "severity": "warning",
            "description": "The datasource is queried live; this QueryRun does not capture a database snapshot or transaction revision.",
        },
    ]
    if run.get("enforce_schema_version") == 2:
        limitations.append({
            "code": "authorization_is_not_semantic_correctness", "severity": "warning",
            "description": "Authorization permits this bounded execution; it does not prove the SQL answers the question.",
        })
    if explain_context.get("extraction_status") != "complete":
        limitations.append(
            {
                "code": "semantic_binding_partial",
                "severity": "warning",
                "description": "Some SQL references could not be bound to a persisted Registry table or column description.",
            }
        )
    if enforce_projection["status"] != "completed":
        limitations.append(
            {
                "code": "execution_not_completed",
                "severity": "info",
                "description": "No completed execution result exists for this QueryRun.",
            }
        )
    if enforce_projection.get("result") and enforce_projection["result"]["truncated"]:
        limitations.append(
            {
                "code": "result_truncated",
                "severity": "warning",
                "description": "The persisted result reached the configured row limit and is incomplete.",
            }
        )
    descriptions = {
        "source_context": (
            "source_context_unanchored",
            "This QueryRun predates the persisted Explain source-context hash.",
        ),
        "approval": (
            "approval_unanchored",
            "This QueryRun predates the persisted approval evidence hash.",
        ),
        "result": (
            "result_unanchored",
            "This QueryRun predates the persisted result evidence hash.",
        ),
    }
    for component in unverified:
        code, description = descriptions[component]
        limitations.append(
            {"code": code, "severity": "warning", "description": description}
        )
    return limitations

def _verify_governed_evidence(run: dict[str, Any], issued: dict[str, Any]) -> dict | None:
    issuance = {key: issued[key] for key in (
        "schema_version", "mandate_id", "issuance_revision", "issuer_principal_id",
        "delegated_mandate", "policy",
    )}
    if (not _same_hash(canonical_hash(issuance), issued.get("issuance_hash"))
            or not _same_hash(issued.get("issuance_hash"), run.get("mandate_issuance_hash"))
            or issued["mandate_id"] != run.get("mandate_id")
            or issued["issuance_revision"] != run.get("mandate_issuance_revision")
            or issuance["delegated_mandate"] != run.get("delegated_mandate")):
        raise ExplainError("mandate_evidence_drift", "Issued mandate differs from the prepared run")
    if (issued["issuer_principal_id"] != run["delegated_mandate"]["delegator_principal"]["principal_id"]
            or issued["policy"]["reviewer_principal_id"] != run.get("reviewer_principal_id")
            or issued["policy"]["revision"] != run.get("authorization_policy_revision")
            or run["principal_context"]["actor_principal"] != run["delegated_mandate"]["delegate_principal"]
            or run["principal_context"]["accountable_principal"] != run["delegated_mandate"]["accountable_principal"]
            or run["task_run_id"] != run["delegated_mandate"]["task_run_id"]):
        raise ExplainError("mandate_evidence_drift", "Mandate identities or scope differ")
    mode = run.get("execution_mode")
    expected_authority = (issued["issuer_principal_id"] if mode == "autonomous"
                          else run["reviewer_principal_id"])
    decision = run["policy_decision"]
    if (mode not in {"autonomous", "human_review", None}
            or decision["decision_authority"] != {"principal_id": expected_authority, "principal_type": "human"}
            or decision["effect"] != ({"autonomous": "allow", "human_review": "conditional", None: "deny"}[mode])
            or decision["mandate_id"] != run["mandate_id"]
            or decision["subject_principal_id"] != run["principal_context"]["actor_principal"]["principal_id"]):
        raise ExplainError("policy_evidence_drift", "Policy authority or execution mode differs")
    authorization = run.get("authorization_record")
    if authorization is None:
        if (run.get("authorization_hash") is not None or run.get("execution_claim_id") is not None
                or run.get("approval_hash") is not None or run.get("approved_at") is not None
                or run.get("approver_user_id") is not None or run.get("result_hash") is not None
                or run["status"] in {"executing", "completed"}):
            raise ExplainError("authorization_evidence_drift", "Execution has no authorization anchor")
    else:
        expected_kind = "mandate" if run["execution_mode"] == "autonomous" else "human_approval"
        expected_authorizer = (run["principal_context"]["actor_principal"]["principal_id"]
                               if expected_kind == "mandate" else run["reviewer_principal_id"])
        expected = {
            "schema_version": 1, "kind": expected_kind,
            "action": "execute" if expected_kind == "mandate" else "approve",
            "actor_principal_id": run["principal_context"]["actor_principal"]["principal_id"],
            "authorizing_principal_id": expected_authorizer,
            "issuer_principal_id": issued["issuer_principal_id"],
            "accountable_principal_id": run["user_id"],
            "reviewer_principal_id": None if expected_kind == "mandate" else run["reviewer_principal_id"],
            "mandate_id": run["mandate_id"], "mandate_issuance_revision": run["mandate_issuance_revision"],
            "mandate_issuance_hash": run["mandate_issuance_hash"],
            "authorization_policy_revision": run["authorization_policy_revision"],
            "sql_hash": run["sql_hash"], "assurance_report_hash": run["assurance_report_hash"],
            "enforcement_context_hash": run["enforcement_context_hash"],
            "execution_claim_id": run["execution_claim_id"],
        }
        if (not isinstance(authorization, dict)
                or not _same_hash(canonical_hash(authorization), run.get("authorization_hash"))
                or any(authorization.get(key) != value for key, value in expected.items())
                or not authorization.get("authorized_at")
                or not (authorization.get("credential_id") or authorization.get("channel_binding_id"))
                or (expected_kind == "human_approval" and (run.get("approver_user_id") != expected_authorizer
                    or run.get("approved_at") != authorization["authorized_at"]))):
            raise ExplainError("authorization_evidence_drift", "Authorization record is not bound to claim")
    return authorization

def project_explain_query_run(run: dict[str, Any], *, issued_mandate: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build and validate a deterministic public explanation from one QueryRun."""
    verified, unverified = _validate_hash_anchors(run)
    governed = run.get("enforce_schema_version") == 2
    authorization = None
    if governed:
        if issued_mandate is None:
            raise ExplainError("mandate_evidence_drift", "Authoritative issuance is required")
        authorization = _verify_governed_evidence(run, issued_mandate)
        verified.append("mandate")
        if authorization is not None:
            verified.append("authorization")
        status = "review_required" if run["status"] == "needs_review" else run["status"]
        failure = ({"stage": "execution", "code": "execution_failed", "retryable": True}
                   if status == "failed" and authorization else
                   {"stage": "enforce", "code": "preparation_failed", "retryable": False}
                   if status in {"failed", "denied"} else None)
        enforce_projection = {"status": status, "failure": failure, "result": build_result_record(run)}
    else:
        enforce_projection = project_enforce_query_run(run)
    explain_context = run.get("explain_context")
    if not isinstance(explain_context, dict):
        physical_tables, _, unresolved, extraction_status = _extract_sql_references(
            run.get("sql"), str(run.get("dialect") or "")
        )
        explain_context = {
            "physical_tables": physical_tables,
            "bindings": [],
            "unresolved_columns": unresolved,
            "extraction_status": (
                "partial" if extraction_status == "complete" else extraction_status
            ),
        }

    approval = build_approval_record(run)
    if approval is not None:
        approval = {**approval, "approval_hash": run.get("approval_hash")}
    result = build_result_record(run)

    evidence = [
        _evidence_item(
            run["query_run_id"],
            "candidate",
            "The upstream candidate is preserved and hash-bound.",
            run["candidate_hash"],
        ),
        _evidence_item(
            run["query_run_id"],
            "policy",
            "The execution decision is bound to the persisted Policy decision.",
            run["policy_decision_hash"],
        ),
    ]
    optional_evidence = (
        ("query", "This is the exact SQL prepared for review and execution.", run.get("sql_hash")),
        ("assurance", "The deterministic gate outcomes are preserved.", run.get("assurance_report_hash")),
        (
            "source",
            "Datasource, resource scope, tables, and Registry semantics are preserved.",
            run.get("explain_context_hash") or run.get("resource_scope_hash"),
        ),
        ("approval", "Human approval is bound to the reviewed hashes.", run.get("approval_hash")),
        ("result", "The persisted execution result is hash-bound.", run.get("result_hash")),
    )
    evidence.extend(
        _evidence_item(run["query_run_id"], kind, summary, content_hash)
        for kind, summary, content_hash in optional_evidence
        if content_hash
    )
    if governed:
        evidence.append(_evidence_item(run["query_run_id"], "mandate",
            "The original issued mandate is independently hash-bound.", run["mandate_issuance_hash"]))
        if authorization is not None:
            evidence.append(_evidence_item(run["query_run_id"], "authorization",
                "The execution claim records the actual authorizing principal.", run["authorization_hash"]))

    principal = run["principal_context"]
    mandate = run.get("delegated_mandate")
    payload = {
        "schema_version": EXPLAIN_SCHEMA_VERSION,
        "query_run_id": run["query_run_id"],
        "task_run_id": run["task_run_id"],
        "status": enforce_projection["status"],
        "statement": {
            "question": run["question"],
            "purpose": run["purpose"],
            "actual_sql": run.get("sql"),
            "dialect": run.get("dialect") or "",
            "result": result,
            "failure": enforce_projection["failure"],
        },
        "semantics": {
            "input_kind": run["input_kind"],
            "candidate_revision": run["candidate_revision"],
            "producer_revision": run["candidate_payload"].get(
                "producer_revision", "external"
            ),
            "candidate": run["candidate_payload"],
            "bindings": explain_context.get("bindings") or [],
            "extraction_status": explain_context.get(
                "extraction_status", "unavailable"
            ),
        },
        "sources": {
            "datasource_id": run["datasource_id"],
            "resource_scope": run["resource_scope"],
            "physical_tables": explain_context.get("physical_tables") or [],
            "registry_revision": run["registry_version"],
            "context_hash": run.get("explain_context_hash"),
            "data_snapshot_ref": None,
        },
        "governance": {
            "actor_principal": principal["actor_principal"],
            "accountable_principal": principal["accountable_principal"],
            "mandate_id": mandate.get("mandate_id") if mandate else None,
            "policy_decision": run["policy_decision"],
            "approval": approval,
        },
        "assurance": run.get("assurance_report"),
        "lineage": {
            "explain_revision": EXPLAIN_REVISION,
            "candidate_hash": run["candidate_hash"],
            "sql_hash": run.get("sql_hash"),
            "assurance_report_hash": run.get("assurance_report_hash"),
            "policy_decision_hash": run["policy_decision_hash"],
            "registry_revision": run["registry_version"],
            "enforcement_context_hash": run["enforcement_context_hash"],
            "explain_context_hash": run.get("explain_context_hash"),
            "approval_hash": run.get("approval_hash"),
            "result_hash": run.get("result_hash"),
        },
        "evidence": evidence,
        "limitations": _limitations(run, enforce_projection, explain_context, unverified),
        "generated_at": run["updated_at"],
    }
    if governed:
        payload["schema_version"] = 2
        payload["governance"].update({
            "issuer_principal": {"principal_id": issued_mandate["issuer_principal_id"], "principal_type": "human"},
            "authorization": authorization,
        })
        payload["lineage"].update({
            "explain_revision": "explain-query-v2",
            "mandate_issuance_hash": run["mandate_issuance_hash"],
            "authorization_hash": run.get("authorization_hash"),
        })
    explanation_hash = canonical_hash(payload)
    payload["integrity"] = {
        "status": "partial" if unverified else "verified",
        "explanation_hash": explanation_hash,
        "verified_hashes": verified,
        "unverified_components": unverified,
    }
    if governed:
        payload["governance"]["current_authorization"] = {
            "status": issued_mandate["status"],
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
    try:
        validate_contract("explain_query_response_v2" if governed else "explain_query_response_v1", payload)
    except ValidationError as exc:
        raise ExplainError(
            "explain_projection_invalid",
            "Persisted QueryRun cannot be projected through Explain v1",
            status_code=500,
        ) from exc
    return payload
