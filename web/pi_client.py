"""Shared HTTP client for the private Pi Orchestrator control plane."""
from __future__ import annotations

import json
import sqlite3
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit

import httpx

from config import cfg
from diagnostics import current_request_id, record
from forge.governance import AuthenticatedPrincipal, GovernanceError, issue_control_token
from web.diagnostics import error_body, upstream_error


async def pi_request(
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    *,
    principal: AuthenticatedPrincipal,
    action: str,
    task_run_id: str | None = None,
    query_run_id: str | None = None,
    approval_hashes: dict[str, str] | None = None,
    idempotency_key: str | None = None,
) -> tuple[int, dict[str, Any]]:
    """Mint a scoped ticket for exactly the bytes transmitted to Pi."""
    if not cfg.PI_CHANNEL_SERVICE_KEY:
        return 503, error_body(503, "pi_control_auth_unavailable")
    parsed = urlsplit(path)
    if parsed.scheme or parsed.netloc or not parsed.path.startswith("/") or parsed.fragment:
        raise ValueError("Pi path must be relative to the configured service")
    path = parsed.path + ("?" + urlencode(sorted(parse_qsl(parsed.query, keep_blank_values=True))) if parsed.query else "")
    body = (json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
            if payload is not None else b"")
    try:
        token = issue_control_token(principal, method.upper(), path, body, action,
                                    task_run_id=task_run_id, query_run_id=query_run_id,
                                    approval_hashes=approval_hashes, idempotency_key=idempotency_key)
    except GovernanceError as exc:
        return exc.status_code, error_body(exc.status_code, exc.code)
    except (sqlite3.Error, OSError):
        return 503, error_body(503, "governance_unavailable")
    headers = {"X-Channel-Service-Key": cfg.PI_CHANNEL_SERVICE_KEY,
               "X-Forge-Control-Token": token,
               "X-Request-ID": current_request_id()}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(cfg.PI_ORCHESTRATOR_TIMEOUT_SECONDS)) as client:
            response = await client.request(method, f"{cfg.PI_ORCHESTRATOR_URL}{path}", content=body, headers=headers)
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


async def pi_benchmark_request(method: str, path: str, payload: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
    """Benchmark-only service exchange; never usable for Task routes."""
    if not path.startswith("/v1/benchmarks") or (len(path) > len("/v1/benchmarks") and path[len("/v1/benchmarks")] not in "/?"):
        raise ValueError("benchmark endpoint required")
    headers = {"X-Channel-Service-Key": cfg.PI_CHANNEL_SERVICE_KEY,
               "X-Request-ID": current_request_id()}
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(cfg.PI_ORCHESTRATOR_TIMEOUT_SECONDS)) as client:
            response = await client.request(method, f"{cfg.PI_ORCHESTRATOR_URL}{path}",
                                            json=payload, headers=headers)
        data = response.json()
    except httpx.HTTPError:
        return 502, error_body(502)
    except ValueError:
        return 502, error_body(502, "upstream_contract_invalid")
    if not isinstance(data, dict):
        return 502, error_body(502, "upstream_contract_invalid")
    if response.status_code >= 400:
        return upstream_error(response.status_code, data)
    return response.status_code, data
