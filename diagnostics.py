"""Payload-free diagnostic context shared by domain and channel adapters."""
from __future__ import annotations

import json
import logging
import re
from contextvars import ContextVar
from uuid import uuid4

request_id: ContextVar[str | None] = ContextVar("request_id", default=None)
_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


def safe_id(value: object) -> str | None:
    return value if isinstance(value, str) and _ID.fullmatch(value) else None


def current_request_id() -> str:
    value = request_id.get()
    if value is None:
        value = f"req_{uuid4().hex}"
        request_id.set(value)
    return value


def record(event: str, **fields: object) -> None:
    """Only accept diagnostic identifiers and bounded scalar measurements, never payloads."""
    allowed = {
        "task_run_id", "query_run_id", "attempt_id", "command_id", "delivery_id",
        "correlation_id", "stage", "model_revision", "skill_policy_version",
        "usage_status", "input_tokens", "output_tokens", "error_code", "status_code",
        "duration_ms", "delivery_status", "operation", "source",
    }
    data: dict[str, object] = {"event": safe_id(event) or "diagnostic", "request_id": current_request_id()}
    for key, value in fields.items():
        if key in allowed and (value is None or isinstance(value, (int, float)) or safe_id(value)):
            data[key] = value
    logging.getLogger("forge.diagnostics").info(json.dumps(data, separators=(",", ":")))


def model_usage(usage: object, *, anthropic: bool = False) -> dict[str, object]:
    def value(name: str) -> object:
        return usage.get(name) if isinstance(usage, dict) else getattr(usage, name, None)

    input_tokens = value("input_tokens" if anthropic else "prompt_tokens")
    output_tokens = value("output_tokens" if anthropic else "completion_tokens")
    known = all(type(n) is int and n >= 0 for n in (input_tokens, output_tokens))
    return {
        "usage_status": "known" if known else "unknown",
        "input_tokens": input_tokens if known else None,
        "output_tokens": output_tokens if known else None,
    }
