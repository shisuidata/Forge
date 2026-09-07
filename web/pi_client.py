"""Shared HTTP client for the private Pi Orchestrator control plane."""
from __future__ import annotations

from typing import Any

import httpx

from config import cfg
from diagnostics import current_request_id, record
from web.diagnostics import error_body, upstream_error

async def pi_request(
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    """Call Pi with the dedicated channel credential and bounded JSON fallback."""
    url = f"{cfg.PI_ORCHESTRATOR_URL}{path}"
    timeout = httpx.Timeout(cfg.PI_ORCHESTRATOR_TIMEOUT_SECONDS)
    headers = (
        {"X-Channel-Service-Key": cfg.PI_CHANNEL_SERVICE_KEY}
        if cfg.PI_CHANNEL_SERVICE_KEY
        else {}
    )
    headers["X-Request-ID"] = current_request_id()
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.request(method, url, json=payload, headers=headers)
    except httpx.HTTPError:
        record("pi_response", error_code="upstream_unavailable", status_code=502)
        return 502, error_body(502)
    try:
        data = response.json()
    except ValueError:
        return 502, error_body(502, "upstream_contract_invalid")
    if not isinstance(data, dict):
        return 502, error_body(502, "upstream_contract_invalid")
    if response.status_code >= 400:
        status, failure = upstream_error(response.status_code, data)
        record("pi_response", status_code=status, error_code=failure["code"])
        return status, failure
    task = data.get("task")
    record("pi_response", status_code=response.status_code,
           command_id=payload.get("event_id") if payload else None,
           task_run_id=task.get("task_run_id") if isinstance(task, dict) else None)
    return response.status_code, data
