"""Fail-closed governance bindings for the public Enforce query workflow."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hmac
from typing import Any

from jsonschema import ValidationError

from agent.contracts import validate_contract
from config import cfg
from forge.assurance import POLICY_REVISION
from forge.evaluate import canonical_hash
from forge.governance import AuthenticatedPrincipal, GovernanceError
from forge.governance_mandates import get_mandate
from agent.tenant import tenants

ENFORCE_SCHEMA_VERSION = 1
ENFORCE_REVISION = "enforce-query-v1"
GOVERNED_ENFORCE_REVISION = "enforce-query-v2"
AUTHORIZATION_POLICY_REVISION = "bounded-agent-policy-v1"
_REQUIRED_AGENT_CAPABILITIES = {"query.prepare", "query.execute"}


class EnforceContextError(ValueError):
    """A stable, public failure raised before execution authorization."""

    def __init__(self, code: str, message: str, *, status_code: int = 400):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_time(value: Any, code: str) -> datetime:
    if not isinstance(value, str):
        raise EnforceContextError(code, "Governance timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EnforceContextError(code, "Governance timestamp is invalid") from exc
    if parsed.tzinfo is None:
        raise EnforceContextError(code, "Governance timestamp must include a timezone")
    return parsed


def _active(start: Any, end: Any, at: datetime, code: str) -> None:
    start_at = _parse_time(start, code)
    end_at = _parse_time(end, code)
    if not start_at <= at < end_at:
        raise EnforceContextError(code, "Governance context is not active")


def _principal_key(value: dict[str, Any]) -> tuple[Any, Any]:
    return value.get("principal_id"), value.get("principal_type")


def _scope_hash(resource_scope: list[dict[str, Any]]) -> str:
    ordered = sorted(resource_scope, key=canonical_hash)
    return canonical_hash(ordered)


def _same_hash(actual: str | None, expected: str | None) -> bool:
    return bool(actual and expected) and hmac.compare_digest(actual, expected)


def prepare_governance_context(
    *,
    principal_context: dict[str, Any],
    delegated_mandate: dict[str, Any] | None,
    purpose: str,
    task_run_id: str,
    resource_scope: list[dict[str, Any]],
    at: datetime | None = None,
    require_active: bool = True,
) -> dict[str, Any]:
    """Validate and normalize the principal, delegation, purpose, task, and scope."""
    try:
        validate_contract("principal_context_v1", principal_context)
        for resource in resource_scope:
            validate_contract("resource_ref_v1", resource)
        if delegated_mandate is not None:
            validate_contract("delegated_mandate_v1", delegated_mandate)
    except (ValidationError, TypeError) as exc:
        raise EnforceContextError(
            "governance_contract_invalid",
            "Principal, mandate, or resource scope contract is invalid",
        ) from exc

    if not purpose.strip():
        raise EnforceContextError("purpose_missing", "purpose must not be empty")
    if not resource_scope:
        raise EnforceContextError("resource_scope_missing", "resource_scope must not be empty")

    current = at or _now()
    if require_active:
        _active(
            principal_context["issued_at"],
            principal_context["expires_at"],
            current,
            "principal_context_inactive",
        )
    authenticated_at = _parse_time(
        principal_context["authentication_context"]["authenticated_at"],
        "authentication_time_invalid",
    )
    issued_at = _parse_time(principal_context["issued_at"], "principal_context_inactive")
    if authenticated_at > issued_at:
        raise EnforceContextError(
            "authentication_time_invalid",
            "authentication must precede the principal context",
        )

    organization_id = principal_context["organization_id"]
    workspace_id = principal_context["workspace_id"]
    actor = principal_context["actor_principal"]
    accountable = principal_context["accountable_principal"]
    if accountable["principal_type"] != "human":
        raise EnforceContextError(
            "human_accountability_required",
            "Public Enforce v1 requires one accountable human principal",
        )

    canonical_scope = [deepcopy(resource) for resource in resource_scope]
    resource_keys = [canonical_hash(resource) for resource in canonical_scope]
    if len(resource_keys) != len(set(resource_keys)):
        raise EnforceContextError("resource_scope_duplicate", "resource_scope contains duplicates")

    for resource in canonical_scope:
        if (
            resource["organization_id"] != organization_id
            or resource["workspace_id"] != workspace_id
        ):
            raise EnforceContextError(
                "resource_scope_mismatch",
                "Every resource must match the principal organization and workspace",
            )
        if resource["resource_type"] not in {"datasource", "table"}:
            raise EnforceContextError(
                "resource_type_unsupported",
                "Public query Enforce v1 supports datasource and table resources only",
            )

    datasources = [
        resource for resource in canonical_scope if resource["resource_type"] == "datasource"
    ]
    if len(datasources) != 1 or datasources[0]["resource_id"] != cfg.DATASOURCE_ID:
        raise EnforceContextError(
            "datasource_scope_mismatch",
            "resource_scope must contain exactly the configured datasource",
        )
    datasource = datasources[0]
    table_resources = [
        resource for resource in canonical_scope if resource["resource_type"] == "table"
    ]
    if any(resource["parent_resource_id"] != cfg.DATASOURCE_ID for resource in table_resources):
        raise EnforceContextError(
            "table_scope_mismatch",
            "Every table resource must be parented by the configured datasource",
        )

    actor_type = actor["principal_type"]
    chain = principal_context["delegation_chain"]
    if actor_type == "human":
        if _principal_key(actor) != _principal_key(accountable):
            raise EnforceContextError(
                "principal_accountability_mismatch",
                "A human actor must be the accountable principal",
            )
        if chain or delegated_mandate is not None:
            raise EnforceContextError(
                "delegation_unexpected",
                "A directly accountable human must not provide an agent mandate",
            )
    else:
        if delegated_mandate is None or len(chain) != 1:
            raise EnforceContextError(
                "delegation_required",
                "A service or agent actor requires one active delegated mandate",
            )
        mandate = delegated_mandate
        delegation = chain[0]
        if require_active:
            _active(
                mandate["issued_at"],
                mandate["expires_at"],
                current,
                "mandate_inactive",
            )
            _active(
                delegation["issued_at"],
                delegation["expires_at"],
                current,
                "delegation_inactive",
            )
        if mandate["status"] != "active":
            raise EnforceContextError("mandate_inactive", "Delegated mandate is not active")
        if (
            mandate["organization_id"] != organization_id
            or mandate["workspace_id"] != workspace_id
        ):
            raise EnforceContextError("mandate_scope_mismatch", "Mandate scope does not match")
        if _principal_key(mandate["delegate_principal"]) != _principal_key(actor):
            raise EnforceContextError("mandate_actor_mismatch", "Mandate delegate does not match actor")
        if _principal_key(mandate["accountable_principal"]) != _principal_key(accountable):
            raise EnforceContextError(
                "mandate_accountability_mismatch",
                "Mandate accountability does not match the principal context",
            )
        if _principal_key(mandate["delegator_principal"]) != _principal_key(accountable):
            raise EnforceContextError(
                "mandate_delegator_mismatch",
                "Public Enforce v1 requires the accountable human to delegate",
            )
        if (
            delegation["mandate_id"] != mandate["mandate_id"]
            or delegation["delegator_principal_id"]
            != mandate["delegator_principal"]["principal_id"]
            or delegation["delegate_principal_id"]
            != mandate["delegate_principal"]["principal_id"]
        ):
            raise EnforceContextError(
                "delegation_chain_mismatch",
                "Delegation chain does not match the delegated mandate",
            )
        if mandate["task_run_id"] != task_run_id:
            raise EnforceContextError("mandate_task_mismatch", "Mandate task does not match")
        if mandate["purpose"] != purpose:
            raise EnforceContextError("mandate_purpose_mismatch", "Mandate purpose does not match")
        if mandate["audience"] != "forge":
            raise EnforceContextError("mandate_audience_mismatch", "Mandate audience must be forge")
        if not _REQUIRED_AGENT_CAPABILITIES.issubset(set(mandate["capabilities"])):
            raise EnforceContextError(
                "mandate_capability_missing",
                "Mandate must allow query.prepare and query.execute",
            )
        if _scope_hash(mandate["resource_scope"]) != _scope_hash(canonical_scope):
            raise EnforceContextError(
                "mandate_resource_out_of_scope",
                "Requested resources do not exactly match the delegated mandate",
            )
        if mandate["budget_ref"] is not None:
            raise EnforceContextError(
                "budget_unresolved",
                "Public Enforce v1 cannot execute an unresolved budget reference",
            )

    normalized_principal = deepcopy(principal_context)
    normalized_mandate = deepcopy(delegated_mandate)
    return {
        "principal_context": normalized_principal,
        "principal_context_hash": canonical_hash(normalized_principal),
        "delegated_mandate": normalized_mandate,
        "delegated_mandate_hash": canonical_hash(normalized_mandate)
        if normalized_mandate is not None
        else None,
        "purpose": purpose,
        "resource_scope": canonical_scope,
        "resource_scope_hash": _scope_hash(canonical_scope),
        "datasource_resource": deepcopy(datasource),
        "allowed_tables": sorted(resource["resource_id"] for resource in table_resources)
        or None,
    }

def _governed_snapshot(actor: AuthenticatedPrincipal, mandate: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read every authority whose revision must remain stable through preparation."""
    delegated = mandate["delegated_mandate"]
    ids = {
        "issuer": mandate["issuer_principal_id"],
        "delegate": delegated["delegate_principal"]["principal_id"],
        "accountable": delegated["accountable_principal"]["principal_id"],
        "reviewer": mandate["policy"]["reviewer_principal_id"],
    }
    principals = {role: tenants.get_principal(identifier) for role, identifier in ids.items()}
    scope = (delegated["organization_id"], delegated["workspace_id"])
    if any((person["organization_id"], person["workspace_id"]) != scope for person in principals.values()):
        raise EnforceContextError("governance_drift", "Principal scope changed", status_code=409)
    if (principals["issuer"]["principal_type"] != "human" or principals["issuer"]["role"] != "admin"
            or principals["delegate"]["principal_type"] not in {"agent", "service"}
            or principals["accountable"]["principal_type"] != "human"
            or principals["reviewer"]["principal_type"] != "human"
            or principals["reviewer"]["role"] not in {"admin", "reviewer"}):
        raise EnforceContextError("governance_drift", "Mandate principals changed", status_code=409)
    if (principals["delegate"]["principal_id"] != actor.principal_id
            or principals["delegate"]["identity_revision"] != actor.identity_revision
            or principals["delegate"]["principal_type"] != actor.principal_type
            or principals["delegate"]["role"] != actor.role
            or scope != (actor.organization_id, actor.workspace_id)):
        raise EnforceContextError("mandate_actor_mismatch", "Mandate is not assigned to this actor", status_code=403)
    if (delegated["delegate_principal"]["principal_type"] != actor.principal_type
            or delegated["delegator_principal"] != {"principal_id": ids["issuer"], "principal_type": "human"}
            or delegated["accountable_principal"]["principal_type"] != "human"):
        raise EnforceContextError("mandate_integrity_failed", "Mandate principal snapshot is inconsistent", status_code=409)
    revision = {
        "identities": {role: person["identity_revision"] for role, person in principals.items()},
        "acls": {person["workspace_id"]: person["acl_revision"] for person in principals.values()},
    }
    return principals, revision


def _governed_policy(
    *, mandate: dict[str, Any], principals: dict[str, dict[str, Any]],
    report: dict[str, Any], mode: str | None, evaluated_at: datetime, expires_at: datetime,
) -> dict[str, Any]:
    delegated = mandate["delegated_mandate"]
    resource = next(item for item in delegated["resource_scope"] if item["resource_type"] == "datasource")
    suffix = canonical_hash({"mandate": mandate["issuance_hash"], "report": report,
                             "mode": mode}).removeprefix("sha256:")[:24]
    obligations = []
    if mode:
        for kind, enforcement, description in (
            ("read_only", "database", "Execute with the confirmed read-only database identity."),
            ("audit", "forge", "Persist authorization and result lineage."),
            ("expiry", "forge", "Claim execution before the mandate expires."),
        ):
            obligations.append({"obligation_id": f"obl_{kind}_{suffix}",
                                "obligation_type": kind, "enforcement_point": enforcement,
                                "description": description})
        if mode == "human_review":
            obligations.append({"obligation_id": f"obl_approval_{suffix}",
                                "obligation_type": "approval", "enforcement_point": "forge",
                                "description": "Require exact hash-bound approval from the named reviewer."})
    authority_role = "issuer" if mode == "autonomous" else "reviewer"
    decision = {
        "schema_version": 1, "policy_decision_id": f"pd_{suffix}", "revision": 1,
        "subject_principal_id": principals["delegate"]["principal_id"],
        "mandate_id": mandate["mandate_id"], "action": "query.execute", "resource": resource,
        "effect": "allow" if mode == "autonomous" else "conditional" if mode else "deny",
        "reason_code": ("policy.query.autonomous" if mode == "autonomous" else
                        "policy.query.human_approval_required" if mode else "policy.query.assurance_denied"),
        "reason": ("The issued mandate permits autonomous execution of the referenced tables."
                   if mode == "autonomous" else "The designated reviewer must approve the exact query."
                   if mode else "Query Assurance denied execution; approval cannot override denial."),
        "obligations": obligations, "policy_revision": AUTHORIZATION_POLICY_REVISION,
        "decision_authority": {"principal_id": principals[authority_role]["principal_id"],
                               "principal_type": "human"},
        "evaluated_at": evaluated_at.isoformat(), "expires_at": expires_at.isoformat(),
    }
    validate_contract("policy_decision_v1", decision)
    return decision


def prepare_governed_query(
    *, actor: AuthenticatedPrincipal, mandate_id: str, task_run_id: str,
    purpose: str, question: str, dialect: str | None, candidate: dict[str, Any],
) -> dict[str, Any]:
    """Prepare a v2 QueryRun without granting execution or trusting request scope."""
    from forge.assurance import QueryAssuranceError, assure_direct_sql, assure_query
    from forge.dialects import DialectResolutionError, resolve_dialect
    from forge.query_runs import current_registry_version

    try:
        validate_contract("query_candidate_v1", candidate)
    except (ValidationError, TypeError) as exc:
        raise EnforceContextError("candidate_contract_invalid", "Invalid query candidate") from exc
    if not task_run_id or not purpose.strip() or not question.strip():
        raise EnforceContextError("governance_contract_invalid", "Task, purpose and question are required")
    try:
        mandate = get_mandate(actor=actor, mandate_id=mandate_id)
        validate_contract("issued_mandate_v1", mandate)
        delegated = mandate["delegated_mandate"]
        now = _now()
        if (mandate["status"] != "active" or delegated["status"] != "active"
                or not _parse_time(delegated["issued_at"], "mandate_inactive") <= now
                < _parse_time(delegated["expires_at"], "mandate_inactive")):
            raise EnforceContextError("mandate_inactive", "Mandate is not active", status_code=409)
        if delegated["task_run_id"] != task_run_id:
            raise EnforceContextError("mandate_task_mismatch", "Mandate task does not match", status_code=403)
        if delegated["purpose"] != purpose:
            raise EnforceContextError("mandate_purpose_mismatch", "Mandate purpose does not match", status_code=403)
        if (delegated["audience"] != "forge" or delegated["can_delegate"]
                or delegated["budget_ref"] is not None
                or not _REQUIRED_AGENT_CAPABILITIES.issubset(delegated["capabilities"])
                or mandate["policy"]["revision"] != AUTHORIZATION_POLICY_REVISION):
            raise EnforceContextError("mandate_integrity_failed", "Mandate does not grant bounded query execution", status_code=409)
        principals, revisions = _governed_snapshot(actor, mandate)
        policy_tables = mandate["policy"]["allowed_tables"]
        resource_tables = {item["resource_id"] for item in delegated["resource_scope"]
                           if item["resource_type"] == "table"}
        if (not policy_tables or set(policy_tables) != resource_tables
                or not set(mandate["policy"]["autonomous_tables"]).issubset(policy_tables)):
            raise EnforceContextError("mandate_integrity_failed", "Mandate table policy is inconsistent", status_code=409)
        allowed = sorted(set(policy_tables)
                         & set(tenants.get_allowed_tables_for_user(principals["issuer"]["principal_id"]))
                         & set(tenants.get_allowed_tables_for_user(actor.principal_id)))
        if (len([r for r in delegated["resource_scope"] if r["resource_type"] == "datasource"]) != 1
                or not any(r["resource_type"] == "datasource" and r["resource_id"] == cfg.DATASOURCE_ID
                           for r in delegated["resource_scope"])):
            raise EnforceContextError("datasource_scope_mismatch", "Mandate datasource is not configured", status_code=409)
        if len(allowed) != len(policy_tables):
            raise EnforceContextError("governance_drift", "Table permission was narrowed", status_code=409)
        try:
            resolution = resolve_dialect(dialect, configured_dialect=cfg.SQL_DIALECT,
                                         database_url=cfg.DATABASE_URL)
        except DialectResolutionError as exc:
            raise EnforceContextError(exc.code, str(exc)) from exc
        registry_before = current_registry_version()
        producer = candidate.get("producer_revision", "external")
        try:
            if candidate["kind"] == "direct_sql":
                assurance = assure_direct_sql(candidate["sql"], dialect=resolution.resolved,
                                              allowed_tables=allowed, producer_revision=producer)
            else:
                assurance = assure_query(candidate["forge_json"], question,
                                         dialect=resolution.resolved, allowed_tables=allowed,
                                         model_revision=producer)
            report = assurance.to_dict()
        except QueryAssuranceError as exc:
            report = exc.report.to_dict()
        report["dialect_resolution"] = resolution.to_dict()
        references = report.get("referenced_tables")
        mode = None
        if report["status"] == "passed":
            if not isinstance(references, (list, tuple)) or any(not isinstance(t, str) for t in references):
                raise EnforceContextError("assurance_context_invalid", "Assurance did not report physical tables", status_code=409)
            if not set(references).issubset(allowed):
                raise EnforceContextError("assurance_context_invalid", "Assurance referenced unauthorized tables", status_code=409)
            mode = ("autonomous" if set(references).issubset(mandate["policy"]["autonomous_tables"])
                    else "human_review")
        latest = get_mandate(actor=actor, mandate_id=mandate_id)
        latest_principals, latest_revisions = _governed_snapshot(actor, latest)
        if (current_registry_version() != registry_before or latest["status"] != "active"
                or latest["issuance_hash"] != mandate["issuance_hash"]
                or latest["state_revision"] != mandate["state_revision"]
                or latest_revisions != revisions or latest_principals != principals
                or allowed != sorted(set(policy_tables)
                                     & set(tenants.get_allowed_tables_for_user(principals["issuer"]["principal_id"]))
                                     & set(tenants.get_allowed_tables_for_user(actor.principal_id)))):
            raise EnforceContextError("governance_drift", "Governance or Registry changed during preparation", status_code=409)
        expires_at = min(_parse_time(delegated["expires_at"], "mandate_inactive"),
                         now + timedelta(seconds=max(1, int(cfg.QUERY_RUN_REVIEW_TTL_SECONDS))))
        principal_context = {
            "schema_version": 1, "principal_context_id": "pc_" + mandate_id.removeprefix("md_") + "_" + canonical_hash({"actor": actor.principal_id, "at": now.isoformat()})[-16:],
            "actor_principal": delegated["delegate_principal"],
            "accountable_principal": delegated["accountable_principal"],
            "organization_id": delegated["organization_id"], "workspace_id": delegated["workspace_id"],
            "authentication_context": {
                "method": (actor.authentication_method if actor.authentication_method in {"local", "oidc", "service_key", "oauth", "workload_identity"} else "service_key"),
                "assurance_level": "service_asserted" if actor.authentication_method == "service_key" else "single_factor",
                "authenticated_at": actor.authenticated_at, "session_id_hash": None,
            },
            "delegation_chain": [{"delegation_id": "dlg_" + mandate_id.removeprefix("md_"),
                                  "delegator_principal_id": principals["issuer"]["principal_id"],
                                  "delegate_principal_id": actor.principal_id,
                                  "mandate_id": mandate_id, "issued_at": delegated["issued_at"],
                                  "expires_at": delegated["expires_at"]}],
            "issued_at": now.isoformat(), "expires_at": expires_at.isoformat(),
        }
        validate_contract("principal_context_v1", principal_context)
        policy = _governed_policy(mandate=mandate, principals=principals, report=report,
                                  mode=mode, evaluated_at=now, expires_at=expires_at)
        candidate_hash = canonical_hash(candidate)
        report_hash = canonical_hash(report)
        policy_hash = canonical_hash(policy)
        context = {
            "principal_context": principal_context, "principal_context_hash": canonical_hash(principal_context),
            "delegated_mandate": delegated, "delegated_mandate_hash": canonical_hash(delegated),
            "resource_scope": delegated["resource_scope"], "resource_scope_hash": _scope_hash(delegated["resource_scope"]),
            "allowed_tables": allowed, "datasource_resource": policy["resource"],
            "purpose": purpose,
        }
        hash_record = {
            "enforce_schema_version": 2, "task_run_id": task_run_id, "purpose": purpose,
            "candidate_revision": "query-candidate-v1", "candidate_hash": candidate_hash,
            "sql_hash": report.get("sql_hash"), "assurance_revision": report["assurance_revision"],
            "assurance_report_hash": report_hash, "policy_revision": report["policy_revision"],
            "policy_decision_hash": policy_hash, "registry_version": registry_before,
            "principal_context_hash": context["principal_context_hash"],
            "delegated_mandate_hash": context["delegated_mandate_hash"],
            "resource_scope_hash": context["resource_scope_hash"], "datasource_id": cfg.DATASOURCE_ID,
            "mandate_issuance_hash": mandate["issuance_hash"], "mandate_issuance_revision": mandate["issuance_revision"],
            "governance_revision_snapshot": revisions, "execution_mode": mode,
            "reviewer_principal_id": principals["reviewer"]["principal_id"],
            "authorization_policy_revision": AUTHORIZATION_POLICY_REVISION,
            "referenced_tables": references if mode else (),
        }
        return {
            "status": "ready" if mode == "autonomous" else "review_required" if mode else "denied",
            "execution_mode": mode, "mandate": mandate, "context": context,
            "candidate": candidate, "candidate_hash": candidate_hash,
            "sql": report.get("sql") if mode else None, "dialect": resolution.resolved,
            "assurance_report": report, "assurance_report_hash": report_hash,
            "policy_decision": policy, "policy_decision_hash": policy_hash,
            "enforcement_context_hash": build_enforcement_context_hash(hash_record),
            "hash_record": hash_record, "governance_revision_snapshot": revisions,
            "registry_version": registry_before, "expires_at": expires_at.isoformat(),
        }
    except GovernanceError as exc:
        raise EnforceContextError(exc.code, str(exc), status_code=exc.status_code) from exc




def build_policy_decision(
    context: dict[str, Any],
    *,
    assurance_report: dict[str, Any] | None,
    evaluated_at: datetime,
    expires_at: datetime,
) -> dict[str, Any]:
    """Build the immutable query.execute decision for one prepared QueryRun."""
    passed = bool(assurance_report and assurance_report.get("status") == "passed")
    actor = context["principal_context"]["actor_principal"]
    accountable = context["principal_context"]["accountable_principal"]
    mandate = context["delegated_mandate"]
    policy_revision = (
        assurance_report.get("policy_revision") if assurance_report else POLICY_REVISION
    )
    basis = {
        "subject_principal_id": actor["principal_id"],
        "mandate_id": mandate["mandate_id"] if mandate else None,
        "resource": context["datasource_resource"],
        "policy_revision": policy_revision,
        "evaluated_at": evaluated_at.isoformat(),
    }
    decision_suffix = canonical_hash(basis).removeprefix("sha256:")[:24]
    obligations: list[dict[str, Any]] = []
    if passed:
        obligations = [
            {
                "obligation_id": f"obl_approval_{decision_suffix}",
                "obligation_type": "approval",
                "enforcement_point": "forge",
                "description": "Execute only the exact approved enforcement context hash.",
            },
            {
                "obligation_id": f"obl_readonly_{decision_suffix}",
                "obligation_type": "read_only",
                "enforcement_point": "database",
                "description": "Use the configured and explicitly confirmed read-only identity.",
            },
            {
                "obligation_id": f"obl_audit_{decision_suffix}",
                "obligation_type": "audit",
                "enforcement_point": "forge",
                "description": "Persist preparation, approval, execution, and result lineage.",
            },
        ]
    decision = {
        "schema_version": 1,
        "policy_decision_id": f"pd_{decision_suffix}",
        "revision": 1,
        "subject_principal_id": actor["principal_id"],
        "mandate_id": mandate["mandate_id"] if mandate else None,
        "action": "query.execute",
        "resource": context["datasource_resource"],
        "effect": "conditional" if passed else "deny",
        "reason_code": (
            "policy.query.human_approval_required"
            if passed
            else "policy.query.assurance_denied"
        ),
        "reason": (
            "The reviewed query may execute only after exact hash-bound human approval."
            if passed
            else "Query Assurance denied execution before human review."
        ),
        "obligations": obligations,
        "policy_revision": policy_revision,
        "decision_authority": accountable,
        "evaluated_at": evaluated_at.isoformat(),
        "expires_at": expires_at.isoformat(),
    }
    validate_contract("policy_decision_v1", decision)
    return decision


def build_enforcement_context_hash(run: dict[str, Any]) -> str:
    """Bind v2 authority and scope while keeping the historical v1 digest stable."""
    revision = GOVERNED_ENFORCE_REVISION if run.get("enforce_schema_version") == 2 else ENFORCE_REVISION
    payload = {
        "enforce_revision": revision,
        "task_run_id": run.get("task_run_id"),
        "purpose": run.get("purpose"),
        "candidate_revision": run.get("candidate_revision"),
        "candidate_hash": run.get("candidate_hash"),
        "sql_hash": run.get("sql_hash"),
        "assurance_revision": run.get("assurance_revision"),
        "assurance_report_hash": run.get("assurance_report_hash"),
        "policy_revision": run.get("policy_revision"),
        "policy_decision_hash": run.get("policy_decision_hash"),
        "registry_revision": run.get("registry_version"),
        "principal_context_hash": run.get("principal_context_hash"),
        "delegated_mandate_hash": run.get("delegated_mandate_hash"),
        "resource_scope_hash": run.get("resource_scope_hash"),
        "datasource_id": run.get("datasource_id"),
    }
    if revision == GOVERNED_ENFORCE_REVISION:
        payload.update({
            "mandate_issuance_hash": run.get("mandate_issuance_hash"),
            "mandate_issuance_revision": run.get("mandate_issuance_revision"),
            "governance_revision_snapshot": run.get("governance_revision_snapshot"),
            "execution_mode": run.get("execution_mode"),
            "reviewer_principal_id": run.get("reviewer_principal_id"),
            "authorization_policy_revision": run.get("authorization_policy_revision"),
            "referenced_tables": sorted(run.get("referenced_tables") or []),
        })
    return canonical_hash(payload)


def validate_stored_enforcement_context(
    run: dict[str, Any], *, at: datetime | None = None
) -> None:
    """Revalidate persisted governance and all approval-bound hashes before execution."""
    if run.get("enforce_schema_version") != ENFORCE_SCHEMA_VERSION:
        raise EnforceContextError("enforcement_context_missing", "QueryRun is not governed")
    context = prepare_governance_context(
        principal_context=run.get("principal_context"),
        delegated_mandate=run.get("delegated_mandate"),
        purpose=str(run.get("purpose") or ""),
        task_run_id=str(run.get("task_run_id") or ""),
        resource_scope=run.get("resource_scope") or [],
        at=at,
        require_active=True,
    )
    for field in (
        "principal_context_hash",
        "delegated_mandate_hash",
        "resource_scope_hash",
    ):
        if context[field] is None and run.get(field) is None:
            continue
        if not _same_hash(context[field], run.get(field)):
            raise EnforceContextError(
                "enforcement_context_drift",
                "Persisted governance context no longer matches its reviewed hash",
            )

    candidate = run.get("candidate_payload")
    try:
        validate_contract("query_candidate_v1", candidate)
    except (ValidationError, TypeError) as exc:
        raise EnforceContextError(
            "candidate_context_drift",
            "Persisted candidate is not valid",
        ) from exc
    if not _same_hash(canonical_hash(candidate), run.get("candidate_hash")):
        raise EnforceContextError(
            "candidate_context_drift",
            "Persisted candidate no longer matches its reviewed hash",
        )

    policy = run.get("policy_decision")
    try:
        validate_contract("policy_decision_v1", policy)
    except (ValidationError, TypeError) as exc:
        raise EnforceContextError(
            "policy_context_drift",
            "Persisted Policy decision is not valid",
        ) from exc
    mandate = context["delegated_mandate"]
    expected_mandate_id = mandate["mandate_id"] if mandate else None
    principal = context["principal_context"]
    if (
        policy["effect"] != "conditional"
        or policy["action"] != "query.execute"
        or policy["subject_principal_id"]
        != principal["actor_principal"]["principal_id"]
        or policy["mandate_id"] != expected_mandate_id
        or policy["resource"] != context["datasource_resource"]
        or policy["decision_authority"] != principal["accountable_principal"]
        or policy["policy_revision"] != run.get("policy_revision")
        or {item["obligation_type"] for item in policy["obligations"]}
        != {"approval", "read_only", "audit"}
    ):
        raise EnforceContextError(
            "policy_context_drift",
            "Persisted Policy decision no longer authorizes the reviewed query",
        )
    _active(
        policy["evaluated_at"],
        policy["expires_at"],
        at or _now(),
        "policy_decision_inactive",
    )
    if not _same_hash(canonical_hash(policy), run.get("policy_decision_hash")):
        raise EnforceContextError(
            "policy_context_drift",
            "Persisted Policy decision no longer matches its reviewed hash",
        )
    if not _same_hash(
        build_enforcement_context_hash(run), run.get("enforcement_context_hash")
    ):
        raise EnforceContextError(
            "enforcement_context_drift",
            "Enforcement context no longer matches its reviewed hash",
        )


def _failure_for_run(run: dict[str, Any]) -> dict[str, Any] | None:
    status = run["status"]
    if status == "expired":
        return {"stage": "approval", "code": "review_expired", "retryable": True}
    if status == "cancelled":
        return {"stage": "approval", "code": "review_cancelled", "retryable": True}
    if status != "failed":
        return None
    if run.get("approved_at") or run.get("authorization_record"):
        return {"stage": "execution", "code": "execution_failed", "retryable": True}
    report = run.get("assurance_report") or {}
    gates = report.get("gates") or []
    gate = gates[-1].get("gate") if gates else ""
    stage, code, retryable = {
        "sql_safety": ("assurance", "readonly_violation", False),
        "sql_parse": ("assurance", "sql_parse_failed", True),
        "registry_acl": ("assurance", "unknown_schema_reference", True),
        "registry_acl_alias": ("assurance", "unknown_schema_reference", True),
        "scope_type_compile": ("compile", "compile_failed", True),
        "contract_scope_type": ("candidate_contract", "candidate_contract_invalid", True),
    }.get(gate, ("enforce", "preparation_failed", True))
    return {"stage": stage, "code": code, "retryable": retryable}


def project_enforce_query_run(run: dict[str, Any]) -> dict[str, Any]:
    """Project one governed QueryRun through the public Enforce response contract."""
    if run.get("enforce_schema_version") == 2:
        return _project_governed_query_run(run)
    if run.get("enforce_schema_version") != ENFORCE_SCHEMA_VERSION:
        raise EnforceContextError("enforcement_context_missing", "QueryRun is not governed", status_code=404)
    public_status = {
        "needs_review": "review_required",
        "failed": "failed" if run.get("approved_at") else "denied",
    }.get(run["status"], run["status"])
    authorized = bool(run.get("approved_at"))
    if authorized:
        verdict = "allow"
    elif run["status"] == "needs_review":
        verdict = "conditional"
    else:
        verdict = "deny"
    result = None
    if run["status"] == "completed":
        result = {
            "columns": run.get("result_columns") or [],
            "rows": run.get("result_rows") or [],
            "row_count": run.get("row_count") or 0,
            "truncated": bool(run.get("truncated")),
            "execution_ms": run.get("execution_ms"),
            "executed_at": run["updated_at"],
        }
    candidate = run["candidate_payload"]
    payload = {
        "schema_version": ENFORCE_SCHEMA_VERSION,
        "query_run_id": run["query_run_id"],
        "task_run_id": run["task_run_id"],
        "status": public_status,
        "decision": {
            "verdict": verdict,
            "review_required": run["status"] == "needs_review",
            "execution_authorized": authorized,
        },
        "context": {
            "principal_context": run["principal_context"],
            "delegated_mandate": run["delegated_mandate"],
            "purpose": run["purpose"],
            "resource_scope": run["resource_scope"],
        },
        "candidate": {
            "input_kind": run["input_kind"],
            "candidate_revision": run["candidate_revision"],
            "producer_revision": candidate.get("producer_revision", "external"),
            "candidate_hash": run["candidate_hash"],
        },
        "review": {
            "sql": run["sql"],
            "sql_hash": run["sql_hash"],
            "assurance_report_hash": run["assurance_report_hash"],
            "enforcement_context_hash": run["enforcement_context_hash"],
            "expires_at": run["expires_at"],
        },
        "policy_decision": run["policy_decision"],
        "assurance": run["assurance_report"],
        "lineage": {
            "candidate_revision": run["candidate_revision"],
            "candidate_hash": run["candidate_hash"],
            "assurance_revision": run["assurance_revision"],
            "assurance_report_hash": run["assurance_report_hash"],
            "policy_revision": run["policy_revision"],
            "policy_decision_hash": run["policy_decision_hash"],
            "registry_revision": run["registry_version"],
            "enforcement_context_hash": run["enforcement_context_hash"],
        },
        "result": result,
        "failure": _failure_for_run(run),
    }
    validate_contract("principal_context_v1", payload["context"]["principal_context"])
    if payload["context"]["delegated_mandate"] is not None:
        validate_contract("delegated_mandate_v1", payload["context"]["delegated_mandate"])
    validate_contract("policy_decision_v1", payload["policy_decision"])
    validate_contract("enforce_query_response_v1", payload)
    return payload

def _project_governed_query_run(run: dict[str, Any]) -> dict[str, Any]:
    """Expose stored authority and claim evidence without treating readiness as permission."""
    status = run["status"]
    authorization = run.get("authorization_record")
    candidate = run["candidate_payload"]
    result = None
    if status == "completed":
        result = {
            "columns": run.get("result_columns") or [],
            "rows": run.get("result_rows") or [],
            "row_count": run.get("row_count") or 0,
            "truncated": bool(run.get("truncated")),
            "execution_ms": run.get("execution_ms"),
            "executed_at": run["executed_at"],
        }
    payload = {
        "schema_version": 2,
        "query_run_id": run["query_run_id"],
        "task_run_id": run["task_run_id"],
        "status": status,
        "decision": {
            "verdict": run["policy_decision"]["effect"],
            "review_required": status == "review_required",
            "execution_authorized": authorization is not None,
            "execution_mode": run["execution_mode"],
        },
        "context": {
            "principal_context": run["principal_context"],
            "delegated_mandate": run["delegated_mandate"],
            "purpose": run["purpose"],
            "resource_scope": run["resource_scope"],
        },
        "candidate": {
            "input_kind": run["input_kind"],
            "candidate_revision": run["candidate_revision"],
            "producer_revision": candidate.get("producer_revision", "external"),
            "candidate_hash": run["candidate_hash"],
        },
        "review": {
            "sql": run["sql"],
            "sql_hash": run["sql_hash"],
            "assurance_report_hash": run["assurance_report_hash"],
            "enforcement_context_hash": run["enforcement_context_hash"],
            "expires_at": run["expires_at"],
        },
        "policy_decision": run["policy_decision"],
        "assurance": run["assurance_report"],
        "lineage": {
            "candidate_revision": run["candidate_revision"],
            "candidate_hash": run["candidate_hash"],
            "assurance_revision": run["assurance_revision"],
            "assurance_report_hash": run["assurance_report_hash"],
            "policy_revision": run["policy_revision"],
            "policy_decision_hash": run["policy_decision_hash"],
            "registry_revision": run["registry_version"],
            "enforcement_context_hash": run["enforcement_context_hash"],
            "mandate_issuance_hash": run["mandate_issuance_hash"],
            "authorization_policy_revision": run["authorization_policy_revision"],
            "authorization_hash": run["authorization_hash"],
        },
        "result": result,
        "failure": _failure_for_run(run) if status != "denied" else {
            "stage": "assurance", "code": "assurance_denied", "retryable": False,
        },
        "authorization": authorization,
    }
    validate_contract("principal_context_v1", payload["context"]["principal_context"])
    validate_contract("delegated_mandate_v1", payload["context"]["delegated_mandate"])
    validate_contract("policy_decision_v1", payload["policy_decision"])
    validate_contract("enforce_query_response_v2", payload)
    return payload
