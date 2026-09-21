"""Bounded, read-only projections of Pi TaskRun data for the Web layer.

Moved from web/router.py (REQ-2026-09-21-069). Bodies unchanged. All helpers
here are pure: none of them performs upstream I/O.
"""
from __future__ import annotations

import hashlib
import json
import re

from pydantic import BaseModel

from config import cfg
from web.diagnostics import error_response


def _valid_web_event_id(value: str) -> bool:
    return re.fullmatch(r"web_[A-Za-z0-9_-]{8,128}", value) is not None


def _web_action_event_id(
    message_id: str,
    task_run_id: str,
    action: str,
    payload: dict,
) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(
        f"{message_id}\n{task_run_id}\n{action}\n{canonical}".encode()
    ).hexdigest()
    return f"web_action_{digest[:40]}"


def _web_admin_task_scopes() -> list[tuple[str, str]]:
    scopes: list[tuple[str, str]] = []
    for raw_scope in cfg.PI_WEB_ADMIN_TASK_SCOPES.split(","):
        org_id, separator, team_id = raw_scope.strip().partition(":")
        if (
            separator
            and re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", org_id)
            and re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", team_id)
        ):
            scopes.append((org_id, team_id))
    return scopes


def _web_admin_can_observe(task: dict) -> bool:
    return (task.get("org_id"), task.get("team_id")) in set(_web_admin_task_scopes())


def _bounded_web_execution_plan(artifacts: object) -> dict | None:
    """Project the latest ExecutionPlan without leaking arbitrary Artifact payloads."""
    if not isinstance(artifacts, list):
        return None
    candidates: list[tuple[int, dict]] = []
    for artifact in artifacts:
        if not isinstance(artifact, dict) or artifact.get("artifact_type") != "execution_plan":
            continue
        payload = artifact.get("payload")
        if not isinstance(payload, dict) or not isinstance(payload.get("steps"), list):
            continue
        revision = payload.get("plan_revision")
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
            continue
        candidates.append((revision, payload))
    if not candidates:
        return None
    _, payload = max(candidates, key=lambda item: item[0])
    steps = []
    for raw_step in payload["steps"][:12]:
        if not isinstance(raw_step, dict):
            continue
        step_id = raw_step.get("step_id")
        title = raw_step.get("title")
        capability = raw_step.get("capability")
        status = raw_step.get("status")
        dependencies = raw_step.get("depends_on")
        if not all(isinstance(value, str) for value in (step_id, title, capability, status)):
            continue
        if not isinstance(dependencies, list) or not all(isinstance(value, str) for value in dependencies):
            continue
        steps.append({
            "step_id": step_id[:64],
            "title": title[:200],
            "capability": capability[:64],
            "depends_on": dependencies[:12],
            "required": raw_step.get("required") is True,
            "status": status[:32],
        })
    return {
        "plan_revision": payload["plan_revision"],
        "status": str(payload.get("status") or "active")[:32],
        "route_kind": str(payload.get("route_kind") or "unknown")[:32],
        "goal": str(payload.get("goal") or "")[:500],
        "steps": steps,
    }


def _bounded_web_task_events(events: object) -> list[dict]:
    if not isinstance(events, list):
        return []
    bounded = []
    for event in events[:200]:
        if not isinstance(event, dict):
            continue
        sequence = event.get("sequence")
        event_type = event.get("event_type")
        created_at = event.get("created_at")
        if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
            continue
        if not isinstance(event_type, str) or not isinstance(created_at, str):
            continue
        bounded.append({
            "sequence": sequence,
            "event_type": event_type[:128],
            "created_at": created_at[:64],
        })
    return bounded


def _bounded_web_stage_attempts(attempts: object) -> list[dict]:
    from diagnostics import safe_id
    if not isinstance(attempts, list):
        return []
    bounded = []
    for attempt in attempts[-50:]:
        if not isinstance(attempt, dict):
            continue
        required = ("attempt_id", "stage", "status", "started_at", "updated_at")
        if not all(isinstance(attempt.get(field), str) for field in required):
            continue
        bounded.append({
            "attempt_id": str(attempt["attempt_id"])[:128],
            "stage": str(attempt["stage"])[:128],
            "status": str(attempt["status"])[:32],
            "attempt_number": attempt.get("attempt_number") if isinstance(attempt.get("attempt_number"), int) else 0,
            "started_at": str(attempt["started_at"])[:64],
            "updated_at": str(attempt["updated_at"])[:64],
            "finished_at": str(attempt["finished_at"])[:64] if isinstance(attempt.get("finished_at"), str) else None,
            "deadline_at": str(attempt["deadline_at"])[:64] if isinstance(attempt.get("deadline_at"), str) else None,
            "progress_phase": (
                str(attempt["progress_phase"])[:32]
                if attempt.get("progress_phase") in {
                    "waiting_for_model", "model_responding", "artifact_submitted"
                }
                else None
            ),
            "first_model_activity_at": (
                str(attempt["first_model_activity_at"])[:64]
                if isinstance(attempt.get("first_model_activity_at"), str)
                else None
            ),
            "tool_submitted_at": (
                str(attempt["tool_submitted_at"])[:64]
                if isinstance(attempt.get("tool_submitted_at"), str)
                else None
            ),
            "model_revision": safe_id(attempt.get("model_revision")),
            "skill_policy_version": attempt.get("skill_policy_version") if type(attempt.get("skill_policy_version")) is int else None,
            "request_id": safe_id(attempt.get("request_id")),
            "usage_status": attempt.get("usage_status") if attempt.get("usage_status") in {"known", "not_started", "unknown"} else "unknown",
            "safe_error": ({"code": "stage_failed", "message": "阶段未完成，请通过 Attempt ID 查询诊断。"} if attempt.get("error") else None),
        })
    return bounded


def _pi_stage_payload(request: BaseModel) -> dict:
    payload = request.model_dump(exclude_none=True)
    if payload.pop("run_async", False):
        payload["async"] = True
    return payload


def _pi_disabled_response():
    return error_response(503, 'disabled')
