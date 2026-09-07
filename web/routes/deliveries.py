"""Authenticated receipt inspection and presentation-only redelivery."""
from __future__ import annotations

import asyncio
import re
import sqlite3

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from web.auth import require_api_auth
from web.channel_delivery import DeliveryBusy, get_delivery_store
from web.diagnostics import error_response
from web.pi_client import pi_request
from web.routes.product import _configured_scopes

router = APIRouter(prefix="/api/pi", dependencies=[Depends(require_api_auth)])


async def _visible_task(task_run_id: str):
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return error_response(404)
    status, data = await pi_request("GET", f"/v1/tasks/{task_run_id}")
    if status != 200:
        return error_response(status, data.get("code"))
    task = data.get("task")
    if not isinstance(task, dict):
        return error_response(502, "upstream_contract_invalid")
    if task.get("channel") != "feishu" or (task.get("org_id"), task.get("team_id")) not in _configured_scopes():
        return error_response(404)
    return None


@router.get("/tasks/{task_run_id}/deliveries")
async def task_deliveries(task_run_id: str):
    denied = await _visible_task(task_run_id)
    if denied is not None:
        return denied
    try:
        receipts = await asyncio.to_thread(get_delivery_store().for_task, task_run_id)
        return JSONResponse({"status": "ok", "task_run_id": task_run_id, "deliveries": receipts}, headers={"Cache-Control": "no-store"})
    except (sqlite3.Error, OSError):
        return error_response(503, "delivery_store_unavailable")


@router.post("/deliveries/{delivery_id}/retry")
async def retry_delivery(delivery_id: str):
    if re.fullmatch(r"delivery_[a-f0-9]{32}", delivery_id) is None:
        return error_response(404)
    try:
        receipt = await asyncio.to_thread(get_delivery_store().get, delivery_id)
        if receipt is None:
            return error_response(404)
        denied = await _visible_task(receipt["task_run_id"])
        if denied is not None:
            return denied
        # Import SDK only when an explicitly requested delivery is performed.
        from web.feishu_pi import deliver_receipt
        result = await asyncio.to_thread(deliver_receipt, delivery_id)
        return JSONResponse({"status": "ok", "delivery": result}, headers={"Cache-Control": "no-store"})
    except DeliveryBusy:
        return error_response(409, "delivery_not_retryable")
    except (sqlite3.Error, OSError):
        return error_response(503, "delivery_store_unavailable")
