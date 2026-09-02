"""Cross-field semantic validation for Product Projection v1."""
from __future__ import annotations

from typing import Any

from jsonschema import ValidationError

from . import validate_contract

PRODUCT_PROJECTION_NAMES = {
    "action_capability_v1",
    "conversation_summary_v1",
    "conversation_detail_v1",
    "task_summary_v1",
    "task_detail_projection_v1",
    "workspace_projection_v1",
    "report_summary_v1",
}


def _meta(value: dict[str, Any], errors: list[str]) -> None:
    meta = value.get("projection_meta")
    if not isinstance(meta, dict):
        return
    availability = meta.get("availability")
    reasons = meta.get("unavailable_reasons")
    reasons = reasons if isinstance(reasons, list) else []
    if availability == "ready" and reasons:
        errors.append("meta.ready_has_unavailable_reason")
    if availability in {"partial", "offline"} and not reasons:
        errors.append("meta.unavailable_reason_required")
    revisions = meta.get("source_revisions")
    revisions = revisions if isinstance(revisions, list) else []
    keys = [
        (revision.get("source"), revision.get("revision"))
        for revision in revisions
        if isinstance(revision, dict)
    ]
    if len(keys) != len(set(keys)):
        errors.append("meta.duplicate_source_revision")


def _action(value: dict[str, Any], errors: list[str]) -> None:
    if value.get("availability") == "enabled" and value.get("reason_code") is not None:
        errors.append("action.enabled_has_reason")
    if value.get("availability") == "disabled" and value.get("reason_code") is None:
        errors.append("action.disabled_reason_required")
    if value.get("action_type") == "approve_query" and value.get("requires_confirmation") is not True:
        errors.append("action.query_approval_requires_confirmation")


def _presentation(value: dict[str, Any], errors: list[str]) -> None:
    table = value.get("table")
    if not isinstance(table, dict):
        return
    columns = table.get("columns")
    rows = table.get("rows")
    if not isinstance(columns, list) or not isinstance(rows, list):
        return
    if any(not isinstance(row, list) or len(row) != len(columns) for row in rows):
        errors.append("presentation.table_shape_mismatch")


def _expected_task_state(status: str) -> str:
    if status == "needs_input":
        return "needs_input"
    if status in {"waiting_for_query_approval", "waiting_for_action_approval"}:
        return "waiting_decision"
    if status in {"created", "clarifying", "querying", "analyzing", "rendering"}:
        return "running"
    if status == "incomplete":
        return "partial"
    if status in {"failed", "expired"}:
        return "failed"
    if status == "cancelled":
        return "cancelled"
    if status == "completed":
        return "completed"
    return "ready"


def validate_product_projection(name: str, value: Any) -> list[str]:
    """Return stable shape/semantic reason codes for one Product Projection."""
    if name not in PRODUCT_PROJECTION_NAMES:
        raise ValueError(f"Unknown Product Projection contract: {name}")
    if not isinstance(value, dict) or value.get("projection_type") != name:
        return ["contract.invalid"]
    try:
        validate_contract("product_projection_v1", value)
    except ValidationError:
        return ["contract.invalid"]
    projection = value
    errors: list[str] = []
    _meta(projection, errors)

    if name == "action_capability_v1":
        _action(projection, errors)

    if name == "conversation_detail_v1":
        summary = projection["summary"]
        _meta(summary, errors)
        if projection["scope"] != summary["scope"]:
            errors.append("conversation.scope_mismatch")
        for entry in projection["entries"]:
            _presentation(entry["presentation"], errors)
            for capability in entry["actions"]:
                _action(capability, errors)
                if capability["task_run_id"] != entry["task"]["task_run_id"]:
                    errors.append("conversation.action_task_mismatch")

    if name == "task_summary_v1":
        if projection["display_state"] != _expected_task_state(projection["status"]):
            errors.append("task.display_state_mismatch")

    if name == "task_detail_projection_v1":
        task = projection["task"]
        _presentation(projection["presentation"], errors)
        if task["display_state"] != _expected_task_state(task["status"]):
            errors.append("task.display_state_mismatch")
        for capability in projection["actions"]:
            _action(capability, errors)
            if capability["task_run_id"] != task["task_run_id"]:
                errors.append("task.action_task_mismatch")
        if task["status"] == "waiting_for_query_approval":
            if projection["review_request"] is None:
                errors.append("task.query_review_required")
            if not any(
                capability["action_type"] == "approve_query"
                for capability in projection["actions"]
            ):
                errors.append("task.query_approval_action_required")
        artifact_ids = {artifact["artifact_id"] for artifact in projection["artifacts"]}
        if any(
            artifact_id not in artifact_ids
            for artifact_id in projection["presentation"]["source_artifact_ids"]
        ):
            errors.append("task.presentation_artifact_missing")
        if projection["relations"]["parent_task_run_id"] != task["parent_task_run_id"]:
            errors.append("task.parent_relation_mismatch")

    if name == "workspace_projection_v1":
        for section in (
            "needs_input",
            "waiting_decision",
            "running",
            "failed",
            "recent_reports",
        ):
            if projection["counts"][section] < len(projection[section]):
                errors.append(f"workspace.{section}_count_underflow")

    if name == "report_summary_v1":
        status = projection["status"]
        if status == "published" and projection["internal_url"] is None:
            errors.append("report.published_url_required")
        allowed_states = (
            {"running"}
            if status == "publishing"
            else {"failed"}
            if status == "failed"
            else {"completed", "partial"}
        )
        if projection["display_state"] not in allowed_states:
            errors.append("report.display_state_mismatch")
        if projection["pdf_status"] == "ready" and projection["pdf_url"] is None:
            errors.append("report.ready_pdf_url_required")
        if projection["pptx_status"] == "ready" and projection["pptx_url"] is None:
            errors.append("report.ready_pptx_url_required")

    return list(dict.fromkeys(errors))
