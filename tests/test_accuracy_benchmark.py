from __future__ import annotations

import json
from pathlib import Path

import pytest

from agent.model_config import ModelConfigSnapshot
from forge.accuracy_benchmark import (
    AccuracyBenchmarkError,
    AccuracyBenchmarkStore,
    BenchmarkConfig,
)
from web.routes import accuracy_benchmark as benchmark_routes


def snapshot() -> ModelConfigSnapshot:
    return ModelConfigSnapshot(
        provider="openai",
        model="ark-code-latest",
        api_key="test-secret-never-projected",
        base_url="https://ark.cn-beijing.volces.com/api/coding/v3",
        tool_choice="auto",
        timeout_seconds=120,
        revision="revision-test",
        source="test",
    )


def result(
    case_id: str,
    run_index: int,
    *,
    correct: bool,
    compiled: bool = True,
    category: str = "聚合",
) -> dict:
    return {
        "case_id": case_id,
        "run_index": run_index,
        "category": category,
        "difficulty": 2,
        "question": f"问题 {case_id}",
        "compiled": compiled,
        "correct": correct,
        "attempts": 1,
        "latency_ms": 100.0 * run_index,
        "error_code": None if correct else "incorrect_result",
        "sql_hash": "sha256:" + case_id * 32,
    }


def create_run(store: AccuracyBenchmarkStore, *, total_cases: int = 2) -> str:
    return store.create_run(
        BenchmarkConfig(runs_per_case=2, workers=1),
        snapshot(),
        method_label="Method AI",
        total_cases=total_cases,
        lineage={"code_revision": "test", "schema_revision": "sha256:schema"},
    )


def test_benchmark_store_projects_incremental_and_final_metrics(tmp_path: Path):
    store = AccuracyBenchmarkStore(tmp_path / "benchmark.db")
    run_id = create_run(store)
    store.mark_running(run_id)

    store.record_call(run_id, result("1", 1, correct=True))
    partial = store.snapshot(run_id)
    assert partial is not None
    assert partial["status"] == "running"
    assert partial["score_phase"] == "partial"
    assert partial["progress"] == {
        "total_cases": 2,
        "completed_cases": 0,
        "total_calls": 4,
        "completed_calls": 1,
        "percent": 0.25,
    }
    assert partial["metrics"]["run_accuracy"] == 1.0
    assert "api_key" not in json.dumps(partial).lower()
    assert "test-secret-never-projected" not in json.dumps(partial)

    store.record_call(run_id, result("1", 2, correct=True))
    store.record_call(run_id, result("2", 1, correct=False, compiled=False))
    store.record_call(run_id, result("2", 2, correct=True))
    store.complete(run_id, status="completed")

    final = store.snapshot(run_id)
    assert final is not None
    assert final["status"] == "completed"
    assert final["score_phase"] == "final"
    assert final["progress"]["completed_cases"] == 2
    assert final["metrics"]["case_ea"] == 1.0
    assert final["metrics"]["all_runs_case_ea"] == 0.5
    assert final["metrics"]["run_accuracy"] == 0.75
    assert final["metrics"]["compile_success_rate"] == 0.75
    assert final["metrics"]["p95_latency_ms"] == 200.0
    assert final["categories"][0]["completed_cases"] == 2
    assert final["cases"][1]["last_error_code"] == "incorrect_result"
    assert final["cases"][1]["error_message"]


def test_benchmark_store_rejects_parallel_active_runs_and_reconciles(tmp_path: Path):
    store = AccuracyBenchmarkStore(tmp_path / "benchmark.db")
    run_id = create_run(store, total_cases=1)
    with pytest.raises(AccuracyBenchmarkError, match="已有运行中的 Benchmark"):
        create_run(store, total_cases=1)

    store.mark_running(run_id)
    assert store.reconcile_interrupted() == 1
    interrupted = store.snapshot(run_id)
    assert interrupted is not None
    assert interrupted["status"] == "interrupted"
    assert interrupted["error_code"] == "process_restarted"


class FakeService:
    def __init__(self, run: dict | None = None):
        self.run = run

    def model_summary(self):
        return {
            "provider": "openai",
            "name": "ark-code-latest",
            "revision": "rev-test",
            "source": "test",
            "endpoint_host": "ark.cn-beijing.volces.com",
        }

    def latest(self):
        return self.run

    def snapshot(self, run_id: str):
        if self.run and self.run["run_id"] == run_id:
            return self.run
        return None

    def suite_preview(self):
        return {
            "manifest": {
                "suite": "bird-mini-dev-full-v1",
                "title": "BIRD-SQL Mini-Dev Full",
                "selection": {"cases": 500, "databases": 11},
                "expected_model_calls": 1000,
            },
            "cases": [{"question_id": 1, "difficulty": "challenging"}],
            "structures": {"demo": {"tables": []}},
        }

    def create(self, config=None):
        prefix = "hbr_test" if config.__class__.__name__.startswith("Hard") else "abr_test"
        return prefix, snapshot()

    def history(self, limit=20):
        return [self.run] if self.run else []

    def logs(self, run_id, **filters):
        return {"run_id": run_id, "total": 0, "limit": 100, "offset": 0, "items": []}


@pytest.mark.asyncio
async def test_benchmark_page_and_start_api_require_explicit_model_call_confirmation(
    client, monkeypatch: pytest.MonkeyPatch
):
    service = FakeService()
    monkeypatch.setattr(benchmark_routes, "_service", lambda: service)
    monkeypatch.setattr(benchmark_routes, "_hard_service", lambda: service)
    launched = []
    monkeypatch.setattr(
        benchmark_routes, "_launch", lambda run_id, model: launched.append((run_id, model.model))
    )
    hard_launched = []
    monkeypatch.setattr(
        benchmark_routes, "_launch_hard",
        lambda run_id, model: hard_launched.append((run_id, model.model)),
    )

    page = await client.get("/admin/benchmark")
    assert page.status_code == 200
    assert "SQL 准确率测试台" in page.text
    assert "RAG" in page.text
    assert "子智能体" in page.text
    assert "准确率实验室" in page.text
    rejected = await client.post("/admin/benchmark/hard-runs", json={})
    assert rejected.status_code == 410
    assert rejected.json()["status"] == "retired"
    assert hard_launched == []

    legacy = await client.post("/admin/benchmark/runs", json={})
    assert legacy.status_code == 410
    assert legacy.json()["status"] == "retired"


@pytest.mark.asyncio
async def test_benchmark_snapshot_api_and_sse_return_same_terminal_projection(
    client, monkeypatch: pytest.MonkeyPatch
):
    run = {
        "schema_version": 1,
        "projection_type": "accuracy_benchmark_run_v1",
        "run_id": "abr_terminal",
        "status": "completed",
        "score_phase": "final",
        "sequence": 7,
        "progress": {"total_cases": 40, "completed_cases": 40, "total_calls": 120, "completed_calls": 120, "percent": 1.0},
        "metrics": {},
        "categories": [],
        "cases": [],
        "recent": [],
    }
    service = FakeService(run)
    monkeypatch.setattr(benchmark_routes, "_service", lambda: service)

    response = await client.get("/admin/benchmark/runs/abr_terminal")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == run

    class ConnectedRequest:
        async def is_disconnected(self):
            return False

    stream = benchmark_routes.benchmark_event_stream(ConnectedRequest(), "abr_terminal")
    event = await anext(stream)
    assert "event: snapshot" in event
    assert json.loads(event.split("data: ", 1)[1]) == run
    with pytest.raises(StopAsyncIteration):
        await anext(stream)


@pytest.mark.asyncio
async def test_accuracy_benchmark_static_assets_are_mounted(client):
    css = await client.get("/static/accuracy-benchmark.css?v=15")
    assert css.status_code == 200
    assert css.headers["content-type"].startswith("text/css")
    assert ".pi-benchmark" in css.text

    javascript = await client.get("/static/accuracy-benchmark.js?v=15")
    assert javascript.status_code == 200
    assert "javascript" in javascript.headers["content-type"]
    assert "model-choice" in javascript.text
