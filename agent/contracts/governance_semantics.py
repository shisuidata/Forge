"""M0.5 cross-contract review semantics.

This module validates test/review traces that compose the versioned Governance
contracts. It is deliberately not wired into production authorization. M1 must
implement the same fail-closed invariants at its policy enforcement points.
"""
from __future__ import annotations

from datetime import datetime
import re
from typing import Any

from jsonschema import ValidationError

from . import validate_contract

_CONTRACT_FIELDS = {
    "principal_context": "principal_context_v1",
    "delegated_mandate": "delegated_mandate_v1",
    "policy_decision": "policy_decision_v1",
    "datasource_binding": "datasource_binding_v1",
    "registry_binding": "registry_binding_v1",
}
_ACCOUNTABLE_TYPES = {"human", "team", "organization"}
_HASH_PATTERN = re.compile(r"^sha256:[a-f0-9]{64}$")


def _time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _active_at(start: Any, end: Any, at: datetime) -> bool:
    start_time = _time(start)
    end_time = _time(end) if end is not None else None
    return start_time is not None and start_time <= at and (end_time is None or at < end_time)


def _principal_key(principal: dict[str, Any]) -> tuple[Any, Any]:
    return principal.get("principal_id"), principal.get("principal_type")


def _resource_key(resource: dict[str, Any]) -> tuple[Any, ...]:
    return (
        resource.get("resource_type"),
        resource.get("resource_id"),
        resource.get("organization_id"),
        resource.get("workspace_id"),
        resource.get("resource_revision"),
    )


def validate_governance_review_trace(trace: dict[str, Any]) -> list[str]:
    """Return stable fail-closed reason codes for one M0.5 review trace."""
    errors: list[str] = []

    def reject(code: str, condition: bool) -> None:
        if condition and code not in errors:
            errors.append(code)

    for field, contract_name in _CONTRACT_FIELDS.items():
        try:
            validate_contract(contract_name, trace.get(field))
        except (ValidationError, TypeError):
            reject(f"contract.{field}", True)
    if errors:
        return errors

    principal = trace["principal_context"]
    mandate = trace["delegated_mandate"]
    policy = trace["policy_decision"]
    datasource = trace["datasource_binding"]
    registry = trace["registry_binding"]
    action = trace.get("action_request", {})
    approval = trace.get("approval_snapshot", {})
    query = trace.get("query_lineage", {})
    request = trace.get("request_binding", {})
    extensions = trace.get("extensions", {})
    at = _time(trace.get("as_of"))
    if at is None:
        return ["trace.as_of_invalid"]

    org = principal["organization_id"]
    workspace = principal["workspace_id"]
    actor = principal["actor_principal"]
    accountable = principal["accountable_principal"]

    reject("principal.scope_mismatch", any(
        item.get("organization_id") != org or item.get("workspace_id") != workspace
        for item in (mandate, datasource, registry)
    ))
    reject("principal.accountability_mismatch", _principal_key(mandate["accountable_principal"]) != _principal_key(accountable))
    reject(
        "principal.accountable_scope_mismatch",
        accountable.get("principal_type") == "organization" and accountable.get("principal_id") != org,
    )
    reject("principal.not_active", not _active_at(principal["issued_at"], principal["expires_at"], at))
    authenticated_at = _time(principal["authentication_context"]["authenticated_at"])
    principal_issued_at = _time(principal["issued_at"])
    reject(
        "principal.authentication_time_invalid",
        authenticated_at is None or principal_issued_at is None or authenticated_at > principal_issued_at,
    )

    chain = principal["delegation_chain"]
    reject("delegation.chain_invalid", len(chain) != 1)
    if len(chain) == 1:
        delegation = chain[0]
        reject("delegation.mandate_mismatch", delegation["mandate_id"] != mandate["mandate_id"])
        reject(
            "delegation.principal_mismatch",
            delegation["delegator_principal_id"] != mandate["delegator_principal"]["principal_id"]
            or delegation["delegate_principal_id"] != mandate["delegate_principal"]["principal_id"],
        )
        reject("delegation.not_active", not _active_at(delegation["issued_at"], delegation["expires_at"], at))
        delegation_issued = _time(delegation["issued_at"])
        delegation_expires = _time(delegation["expires_at"])
        principal_issued = _time(principal["issued_at"])
        principal_expires = _time(principal["expires_at"])
        mandate_issued = _time(mandate["issued_at"])
        mandate_expires = _time(mandate["expires_at"])
        reject(
            "delegation.interval_mismatch",
            None in {delegation_issued, delegation_expires, principal_issued, principal_expires, mandate_issued, mandate_expires}
            or not (
                max(principal_issued, mandate_issued) <= delegation_issued
                < delegation_expires <= min(principal_expires, mandate_expires)
            ),
        )

    reject("mandate.actor_mismatch", _principal_key(mandate["delegate_principal"]) != _principal_key(actor))
    reject("mandate.status_invalid", mandate["status"] != "active")
    reject("mandate.not_active", not _active_at(mandate["issued_at"], mandate["expires_at"], at))
    reject("mandate.task_mismatch", mandate["task_run_id"] != action.get("task_run_id"))
    reject("mandate.audience_mismatch", mandate["audience"] != action.get("audience"))
    reject("mandate.purpose_mismatch", mandate["purpose"] != action.get("purpose"))
    reject("mandate.capability_missing", action.get("action") not in mandate["capabilities"])
    action_resource = action.get("resource", {})
    reject(
        "resource.scope_mismatch",
        action_resource.get("organization_id") != org
        or action_resource.get("workspace_id") != workspace
        or policy["resource"].get("organization_id") != org
        or policy["resource"].get("workspace_id") != workspace
        or any(
            item.get("organization_id") != org or item.get("workspace_id") != workspace
            for item in mandate["resource_scope"]
        ),
    )
    reject(
        "mandate.resource_out_of_scope",
        _resource_key(action_resource) not in {_resource_key(item) for item in mandate["resource_scope"]},
    )
    reject("budget.unresolved", mandate["budget_ref"] is not None)

    reject("policy.subject_mismatch", policy["subject_principal_id"] != actor["principal_id"])
    reject("policy.mandate_mismatch", policy["mandate_id"] != mandate["mandate_id"])
    reject("policy.action_mismatch", policy["action"] != action.get("action"))
    reject("policy.resource_mismatch", _resource_key(policy["resource"]) != _resource_key(action_resource))
    reject("policy.effect_not_allow", policy["effect"] != "allow")
    reject("policy.not_active", not _active_at(policy["evaluated_at"], policy["expires_at"], at))
    reject(
        "policy.revision_mismatch",
        policy["policy_revision"] != datasource["policy_revision"]
        or policy["policy_revision"] != registry["policy_revision"],
    )
    obligation_types = {item["obligation_type"] for item in policy["obligations"]}
    reject("policy.approval_obligation_missing", action.get("action") == "query.execute" and "approval" not in obligation_types)
    reject("policy.approval_ref_mismatch", mandate["approval_policy_ref"] != action.get("approval_policy_ref"))

    reject("binding.datasource_not_active", datasource["status"] != "active" or not _active_at(datasource["valid_from"], datasource["valid_until"], at))
    reject("binding.datasource_scope_mismatch", datasource["datasource"]["organization_id"] != org or datasource["datasource"]["workspace_id"] != workspace)
    reject("binding.registry_not_active", registry["status"] != "active" or not _active_at(registry["valid_from"], registry["valid_until"], at))
    reject("binding.registry_scope_mismatch", registry["registry"]["organization_id"] != org or registry["registry"]["workspace_id"] != workspace)
    reject("binding.datasource_link_mismatch", registry["datasource_binding_id"] != datasource["datasource_binding_id"] or registry["registry"]["parent_resource_id"] != datasource["datasource"]["resource_id"])
    reject("binding.registry_revision_mismatch", registry["registry_revision"] != registry["registry"]["resource_revision"])

    reject("approval.status_invalid", approval.get("status") != "approved")
    reject("approval.authority_invalid", approval.get("authority", {}).get("principal_type") not in _ACCOUNTABLE_TYPES)
    reject("approval.authority_mismatch", _principal_key(approval.get("authority", {})) != _principal_key(policy["decision_authority"]))
    reject("approval.action_invalid", approval.get("action") != "query.approve")
    reject("approval.task_mismatch", approval.get("task_run_id") != action.get("task_run_id"))
    reject("approval.query_mismatch", approval.get("query_run_id") != query.get("query_run_id"))
    reject("approval.binding_mismatch", approval.get("datasource_binding_id") != datasource["datasource_binding_id"] or approval.get("registry_binding_id") != registry["registry_binding_id"])
    reject("approval.policy_mismatch", approval.get("policy_revision") != policy["policy_revision"])
    reject(
        "approval.hash_invalid",
        not _HASH_PATTERN.fullmatch(str(approval.get("sql_hash", "")))
        or not _HASH_PATTERN.fullmatch(str(approval.get("assurance_report_hash", ""))),
    )
    reject("approval.not_active", not _active_at(approval.get("decided_at"), approval.get("expires_at"), at))
    reject("approval.time_mismatch", approval.get("decided_at") != query.get("approved_at"))

    reject("query.task_mismatch", query.get("task_run_id") != action.get("task_run_id"))
    reject("query.scope_mismatch", query.get("organization_id") != org or query.get("workspace_id") != workspace)
    reject("query.datasource_mismatch", query.get("datasource_id") != datasource["datasource"]["resource_id"])
    reject("query.registry_mismatch", query.get("registry_revision") != registry["registry_revision"])
    reject("query.policy_mismatch", query.get("policy_revision") != policy["policy_revision"])
    reject("query.approver_mismatch", query.get("approver_principal_id") != approval.get("authority", {}).get("principal_id"))
    reject("query.sql_hash_mismatch", query.get("sql_hash") != approval.get("sql_hash"))
    reject("query.assurance_hash_mismatch", query.get("assurance_report_hash") != approval.get("assurance_report_hash"))
    reject("query.status_invalid", query.get("status") != "completed")
    reject("query.action_sequence_invalid", query.get("actions") != ["query.prepare", "query.approve", "query.execute"])
    prepared_at = _time(query.get("prepared_at"))
    approved_at = _time(query.get("approved_at"))
    executed_at = _time(query.get("executed_at"))
    reject(
        "query.time_order_invalid",
        prepared_at is None or approved_at is None or executed_at is None
        or not (prepared_at <= approved_at <= executed_at <= at),
    )

    reject(
        "request.binding_missing",
        not isinstance(request.get("request_id"), str)
        or not request["request_id"].startswith("req_")
        or not _HASH_PATTERN.fullmatch(str(request.get("idempotency_key_hash", ""))),
    )
    reject("request.task_mismatch", request.get("task_run_id") != action.get("task_run_id"))
    reject("request.audience_mismatch", request.get("audience") != action.get("audience"))
    reject("request.not_active", not _active_at(request.get("issued_at"), request.get("expires_at"), at))
    approval_time = _time(approval.get("decided_at"))
    policy_time = _time(policy.get("evaluated_at"))
    request_time = _time(request.get("issued_at"))
    reject(
        "request.time_order_invalid",
        approval_time is None or policy_time is None or request_time is None or executed_at is None
        or not (approval_time <= policy_time <= request_time <= executed_at <= at),
    )

    reject("extensions.not_explicit", set(extensions) != {"economics", "context"} or extensions.get("economics") is not None or extensions.get("context") is not None)
    return errors
