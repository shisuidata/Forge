"""Shared HTTP client for the private Pi Orchestrator control plane."""
from __future__ import annotations

from typing import Any

import httpx

from config import cfg


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
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.request(method, url, json=payload, headers=headers)
    try:
        data = response.json()
    except ValueError:
        return 502, {
            "status": "upstream_error",
            "error": "Pi Orchestrator returned invalid JSON",
        }
    if not isinstance(data, dict):
        return 502, {
            "status": "upstream_error",
            "error": "Pi Orchestrator returned a non-object response",
        }
    return response.status_code, data
