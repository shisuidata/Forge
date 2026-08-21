"""Web channel adapter tests for the Pi Task API Integration Spike."""
from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_task_workspace_renders_hash_bound_queryrun_approval(client: AsyncClient):
    response = await client.get("/tasks")
    assert response.status_code == 200
    assert "Pi 任务控制台" in response.text
    assert "批准并只读执行" in response.text
    assert "sql-editor" not in response.text


@pytest.mark.asyncio
async def test_pi_task_proxy_fails_closed_when_disabled(client: AsyncClient, monkeypatch):
    from config import cfg

    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", False)
    response = await client.post(
        "/api/pi/tasks",
        json={"message": "查询订单", "user_id": "web-user"},
    )
    assert response.status_code == 503
    assert response.json()["status"] == "disabled"


@pytest.mark.asyncio
async def test_web_proxy_forwards_task_creation_without_business_routing(
    client: AsyncClient, monkeypatch
):
    from config import cfg
    import web.router as router_mod

    calls = []

    async def fake_pi_request(method, path, payload=None):
        calls.append((method, path, payload))
        return 201, {
            "task": {"task_run_id": "tr_web_001", "status": "created"},
            "events": [{"sequence": 1, "event_type": "task.created", "payload": {}}],
        }

    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(router_mod, "_pi_request", fake_pi_request)

    response = await client.post(
        "/api/pi/tasks",
        json={
            "message": "查询订单",
            "intent": "query_prepare",
        },
    )

    assert response.status_code == 201
    assert calls == [
        (
            "POST",
            "/v1/tasks",
            {
                "message": "查询订单",
                "user_id": "web_admin",
                "org_id": "org_default",
                "team_id": "team_default",
                "intent": "query_prepare",
                "channel": "web",
                "channel_conversation_id": None,
            },
        )
    ]


@pytest.mark.asyncio
async def test_web_proxy_exposes_review_request_but_no_execution_endpoint(
    client: AsyncClient, monkeypatch
):
    from config import cfg
    import web.router as router_mod

    async def fake_pi_request(method, path, payload=None):
        assert method == "POST"
        assert path == "/v1/tasks/tr_web_001/prepare-query"
        return 200, {
            "task": {"task_run_id": "tr_web_001", "status": "waiting_for_query_approval"},
            "result": {
                "status": "needs_review",
                "sql": "SELECT 1",
                "review_required": True,
                "can_execute": False,
            },
            "events": [
                {
                    "sequence": 4,
                    "event_type": "query.review_requested",
                    "payload": {"sql": "SELECT 1", "can_execute": False},
                }
            ],
        }

    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(router_mod, "_pi_request", fake_pi_request)

    response = await client.post(
        "/api/pi/tasks/tr_web_001/prepare-query",
        json={"question": "查询订单", "dialect": "sqlite"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["result"]["can_execute"] is False
    assert data["events"][0]["event_type"] == "query.review_requested"


@pytest.mark.asyncio
async def test_web_proxy_forwards_structured_skill_stage(client: AsyncClient, monkeypatch):
    from config import cfg
    import web.router as router_mod

    calls = []

    async def fake_pi_request(method, path, payload=None):
        calls.append((method, path, payload))
        return 200, {
            "task": {"task_run_id": "tr_web_001", "status": "needs_input"},
            "artifact": {
                "artifact_id": "ar_demo",
                "artifact_type": "clarification",
                "schema_version": 1,
                "task_run_id": "tr_web_001",
                "producer": "skill:data-requirement-clarifier",
                "created_at": "2026-08-21T00:00:00Z",
                "payload": {"status": "needs_input"},
            },
            "events": [],
        }

    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(router_mod, "_pi_request", fake_pi_request)
    response = await client.post(
        "/api/pi/tasks/tr_web_001/clarify",
        json={"message": "最近转化为什么下降"},
    )
    assert response.status_code == 200
    assert calls == [
        (
            "POST",
            "/v1/tasks/tr_web_001/clarify",
            {"message": "最近转化为什么下降"},
        )
    ]
    assert response.json()["artifact"]["artifact_type"] == "clarification"


@pytest.mark.asyncio
async def test_web_proxy_forwards_analysis_and_report_stages(client: AsyncClient, monkeypatch):
    from config import cfg
    import web.router as router_mod

    calls = []

    async def fake_pi_request(method, path, payload=None):
        calls.append((method, path, payload))
        artifact_type = "analysis" if path.endswith("/analyze") else "rendered_output"
        return 200, {
            "task": {"task_run_id": "tr_web_001", "status": "ready_for_report"},
            "artifact": {"artifact_type": artifact_type, "payload": {}},
            "events": [],
        }

    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(router_mod, "_pi_request", fake_pi_request)
    analyzed = await client.post(
        "/api/pi/tasks/tr_web_001/analyze",
        json={"question": "分析下降集中点", "run_async": True},
    )
    reported = await client.post(
        "/api/pi/tasks/tr_web_001/render-report",
        json={"audience": "业务负责人"},
    )
    supplemented = await client.post(
        "/api/pi/tasks/tr_web_001/supplements",
        json={"suggested_query_index": 0, "idempotency_key": "supplement-web-001"},
    )
    resumed = await client.post(
        "/api/pi/tasks/tr_web_001/resume-analysis",
        json={
            "child_task_run_id": "tr_child_001",
            "idempotency_key": "resume-web-001",
        }
    )
    task = await client.get("/api/pi/tasks/tr_web_001")
    attempts = await client.get("/api/pi/tasks/tr_web_001/attempts")
    assert analyzed.status_code == 200
    assert reported.status_code == 200
    assert supplemented.status_code == 200
    assert task.status_code == 200
    assert attempts.status_code == 200
    assert resumed.status_code == 200
    assert calls == [
        (
            "POST",
            "/v1/tasks/tr_web_001/analyze",
            {"question": "分析下降集中点", "async": True},
        ),
        ("POST", "/v1/tasks/tr_web_001/render-report", {"audience": "业务负责人"}),
        (
            "POST",
            "/v1/tasks/tr_web_001/supplements",
            {"suggested_query_index": 0, "idempotency_key": "supplement-web-001"},
        ),
        (
            "POST",
            "/v1/tasks/tr_web_001/resume-analysis",
            {
                "child_task_run_id": "tr_child_001",
                "idempotency_key": "resume-web-001",
            },
        ),
        ("GET", "/v1/tasks/tr_web_001", None),
        ("GET", "/v1/tasks/tr_web_001/attempts", None),
    ]


@pytest.mark.asyncio
async def test_web_proxy_forwards_hash_bound_approval(client: AsyncClient, monkeypatch):
    from config import cfg
    import web.router as router_mod

    calls = []

    async def fake_pi_request(method, path, payload=None):
        calls.append((method, path, payload))
        return 200, {
            "task": {"task_run_id": "tr_web_001", "status": "completed"},
            "result": {
                "status": "completed",
                "query_run_id": "qr_web_001",
                "columns": ["n"],
                "rows": [[1]],
                "row_count": 1,
                "truncated": False,
                "execution_ms": 2,
            },
            "events": [],
        }

    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(router_mod, "_pi_request", fake_pi_request)
    response = await client.post(
        "/api/pi/tasks/tr_web_001/approve-query",
        json={
            "query_run_id": "qr_web_001",
            "sql_hash": "sha256:" + "a" * 64,
            "idempotency_key": "approve-web-001",
        },
    )
    assert response.status_code == 200
    assert calls[0][1] == "/v1/tasks/tr_web_001/approve-query"
    assert calls[0][2]["sql_hash"].startswith("sha256:")


@pytest.mark.asyncio
async def test_web_proxy_rejects_invalid_task_id(client: AsyncClient, monkeypatch):
    from config import cfg

    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    response = await client.post(
        "/api/pi/tasks/not-a-task/prepare-query",
        json={"question": "查询订单"},
    )
    assert response.status_code == 400
