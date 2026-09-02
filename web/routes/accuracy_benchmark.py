"""Authenticated Accuracy Lab pages, control APIs, and SSE projections."""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from agent.model_config import LLMConfigurationError, LLMNotConfiguredError
from forge.accuracy_benchmark import (
    AccuracyBenchmarkError,
    BenchmarkConfig,
    get_accuracy_benchmark_service,
)
from forge.hard_accuracy_benchmark import (
    HardBenchmarkConfig,
    HardBenchmarkError,
    get_hard_benchmark_service,
)
from web.pi_client import pi_request

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/benchmark", tags=["accuracy-benchmark"])
templates = Jinja2Templates(directory=str(Path(__file__).parents[1] / "templates"))
_benchmark_tasks: set[asyncio.Task] = set()
_TERMINAL = {"completed", "failed", "interrupted"}


def _service():
    return get_accuracy_benchmark_service()


def _hard_service():
    return get_hard_benchmark_service()


async def _execute(run_id: str, snapshot) -> None:
    try:
        await asyncio.to_thread(_service().run, run_id, snapshot)
    except Exception as exc:
        logger.warning(
            "Accuracy Benchmark failed: run=%s error=%s", run_id, type(exc).__name__
        )


async def _execute_hard(run_id: str, snapshot) -> None:
    try:
        await asyncio.to_thread(_hard_service().run, run_id, snapshot)
    except Exception as exc:
        logger.warning(
            "Hard Accuracy Benchmark failed: run=%s error=%s",
            run_id,
            type(exc).__name__,
        )


def _launch(run_id: str, snapshot) -> None:
    task = asyncio.create_task(_execute(run_id, snapshot))
    _benchmark_tasks.add(task)
    task.add_done_callback(_benchmark_tasks.discard)


def _launch_hard(run_id: str, snapshot) -> None:
    task = asyncio.create_task(_execute_hard(run_id, snapshot))
    _benchmark_tasks.add(task)
    task.add_done_callback(_benchmark_tasks.discard)


@router.get("", response_class=HTMLResponse)
async def benchmark_page(request: Request):
    model: dict[str, str] | None = None
    model_error: str | None = None
    suite: dict[str, Any] | None = None
    suite_error: str | None = None
    try:
        model = _service().model_summary()
    except (LLMNotConfiguredError, LLMConfigurationError, AccuracyBenchmarkError):
        model_error = "当前 Ark Coding Plan 不可用于 Benchmark。"
    try:
        suite = _hard_service().suite_preview()
    except HardBenchmarkError:
        suite_error = "BIRD Mini-Dev 官方数据不可用。"
    return templates.TemplateResponse(
        request,
        "accuracy_benchmark.html",
        {
            "active": "benchmark",
            "model": model,
            "model_error": model_error,
            "hard_suite": suite,
            "suite_error": suite_error,
        },
    )


async def _pi_json(method: str, path: str, payload: dict[str, Any] | None = None):
    status, data = await pi_request(method, path, payload)
    return JSONResponse(data, status_code=status, headers={"Cache-Control": "no-store"})


@router.get("/pi-runs/models")
async def pi_benchmark_models():
    return await _pi_json("GET", "/v1/benchmarks/models")


@router.post("/pi-runs")
async def start_pi_benchmark(request: Request):
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="JSON object required")
    return await _pi_json("POST", "/v1/benchmarks", payload)


@router.get("/pi-runs")
async def pi_benchmark_history(limit: int = 20):
    return await _pi_json("GET", f"/v1/benchmarks?limit={limit}")


@router.get("/pi-runs/latest")
async def latest_pi_benchmark():
    return await _pi_json("GET", "/v1/benchmarks/latest")


@router.get("/pi-runs/{run_id}")
async def get_pi_benchmark(run_id: str):
    return await _pi_json("GET", f"/v1/benchmarks/{run_id}")


@router.get("/pi-runs/{run_id}/logs")
async def get_pi_benchmark_logs(
    run_id: str,
    arm: str | None = None,
    stage: str | None = None,
    case_id: str | None = None,
    search: str | None = None,
    limit: int = 100,
    offset: int = 0,
):
    params = {"limit": str(limit), "offset": str(offset)}
    for key, value in (("arm", arm), ("stage", stage), ("case_id", case_id), ("search", search)):
        if value:
            params[key] = value
    from urllib.parse import urlencode
    return await _pi_json("GET", f"/v1/benchmarks/{run_id}/logs?{urlencode(params)}")


@router.post("/pi-runs/{run_id}/{action}")
async def control_pi_benchmark(run_id: str, action: str):
    if action not in {"pause", "resume", "stop"}:
        raise HTTPException(status_code=404, detail="Unsupported control")
    return await _pi_json("POST", f"/v1/benchmarks/{run_id}/{action}", {})


# Legacy 40-case Method AI run remains readable for historical comparison.
@router.post("/runs")
async def start_benchmark_run():
    return JSONResponse(
        {"status": "retired", "error": "New Benchmark runs are owned by Pi; use /admin/benchmark/pi-runs."},
        status_code=410,
    )


@router.get("/runs/latest")
async def latest_benchmark_run():
    return JSONResponse({"run": _service().latest()}, headers={"Cache-Control": "no-store"})


@router.get("/runs/{run_id}")
async def get_benchmark_run(run_id: str):
    snapshot = _service().snapshot(run_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Benchmark Run 不存在。")
    return JSONResponse(snapshot, headers={"Cache-Control": "no-store"})


@router.post("/hard-runs")
async def start_hard_benchmark_run(request: Request):
    return JSONResponse(
        {"status": "retired", "error": "Python Benchmark scheduling is read-only; use Pi Benchmark."},
        status_code=410,
    )


@router.get("/hard-runs/latest")
async def latest_hard_benchmark_run():
    return JSONResponse(
        {"run": _hard_service().latest()}, headers={"Cache-Control": "no-store"}
    )


@router.get("/hard-runs/history")
async def hard_benchmark_history(limit: int = 20):
    return JSONResponse(
        {"runs": _hard_service().history(limit=limit)},
        headers={"Cache-Control": "no-store"},
    )


@router.get("/hard-runs/{run_id}/logs")
async def hard_benchmark_logs(
    run_id: str,
    limit: int = 200,
    offset: int = 0,
    method_id: str | None = None,
    stage: str | None = None,
    level: str | None = None,
    case_id: str | None = None,
    search: str | None = None,
):
    result = _hard_service().logs(
        run_id,
        limit=limit,
        offset=offset,
        method_id=method_id,
        stage=stage,
        level=level,
        case_id=case_id,
        search=(search or "")[:100] or None,
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Hard Benchmark Run 不存在。")
    return JSONResponse(result, headers={"Cache-Control": "no-store"})

@router.get("/hard-runs/{run_id}")
async def get_hard_benchmark_run(run_id: str):
    snapshot = _hard_service().snapshot(run_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Hard Benchmark Run 不存在。")
    return JSONResponse(snapshot, headers={"Cache-Control": "no-store"})


async def _event_stream(
    request: Request,
    run_id: str,
    snapshot_fn: Callable[[str], dict[str, Any] | None],
):
    last_sequence = -1
    idle_ticks = 0
    while True:
        if await request.is_disconnected():
            return
        snapshot = snapshot_fn(run_id)
        if snapshot is None:
            yield "event: error\ndata: {\"error\":\"Benchmark Run 不存在。\"}\n\n"
            return
        sequence = int(snapshot["sequence"])
        if sequence != last_sequence:
            payload = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
            yield f"id: {sequence}\nevent: snapshot\ndata: {payload}\n\n"
            last_sequence = sequence
            idle_ticks = 0
        else:
            idle_ticks += 1
            if idle_ticks >= 20:
                yield ": keepalive\n\n"
                idle_ticks = 0
        if snapshot["status"] in _TERMINAL:
            return
        await asyncio.sleep(0.5)


async def benchmark_event_stream(request: Request, run_id: str):
    async for event in _event_stream(request, run_id, _service().snapshot):
        yield event


async def hard_benchmark_event_stream(request: Request, run_id: str):
    async for event in _event_stream(request, run_id, _hard_service().snapshot):
        yield event


def _stream_response(generator) -> StreamingResponse:
    return StreamingResponse(
        generator,
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/runs/{run_id}/events")
async def stream_benchmark_run(request: Request, run_id: str):
    if _service().snapshot(run_id) is None:
        raise HTTPException(status_code=404, detail="Benchmark Run 不存在。")
    return _stream_response(benchmark_event_stream(request, run_id))


@router.get("/hard-runs/{run_id}/events")
async def stream_hard_benchmark_run(request: Request, run_id: str):
    if _hard_service().snapshot(run_id) is None:
        raise HTTPException(status_code=404, detail="Hard Benchmark Run 不存在。")
    return _stream_response(hard_benchmark_event_stream(request, run_id))
