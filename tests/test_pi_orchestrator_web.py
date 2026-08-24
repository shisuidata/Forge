"""Web channel adapter tests for the Pi Task API Integration Spike."""
from __future__ import annotations

import json

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_task_workspace_renders_hash_bound_queryrun_approval(client: AsyncClient):
    response = await client.get("/tasks")
    assert response.status_code == 200
    assert "AI 数据任务" in response.text
    assert "批准并只读执行" in response.text
    assert 'id="task-mode"' in response.text
    assert "专业 Advisory Skill" in response.text
    assert 'value="funnel-analysis"' in response.text
    assert "sql-editor" not in response.text
    assert "实时任务监控" in response.text
    assert 'id="task-inbox"' in response.text
    assert 'id="task-attempts"' in response.text


@pytest.mark.asyncio
async def test_web_task_list_uses_server_owned_scope_and_cross_channel_filters(
    client: AsyncClient, monkeypatch
):
    from config import cfg
    import web.router as router_mod

    calls = []

    async def fake_pi_request(method, path, payload=None):
        calls.append((method, path, payload))
        return 200, {"tasks": [{"task_run_id": "tr_feishu_001", "channel": "feishu"}]}

    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(router_mod, "_pi_request", fake_pi_request)
    response = await client.get("/api/pi/tasks?channel=feishu&limit=20")

    assert response.status_code == 200
    assert response.json()["tasks"][0]["channel"] == "feishu"
    assert calls == [(
        "GET",
        "/v1/tasks?org_id=org_default&team_id=team_default&limit=20&channel=feishu",
        None,
    )]


@pytest.mark.asyncio
async def test_web_task_detail_fails_closed_outside_admin_scope(client: AsyncClient, monkeypatch):
    from config import cfg
    import web.router as router_mod

    async def fake_pi_request(method, path, payload=None):
        return 200, {"task": {
            "task_run_id": "tr_other_001", "org_id": "org_other", "team_id": "team_other",
        }}

    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(router_mod, "_pi_request", fake_pi_request)
    response = await client.get("/api/pi/tasks/tr_other_001/events")
    assert response.status_code == 404
    assert response.json() == {"status": "not_found"}


@pytest.mark.asyncio
async def test_task_event_stream_is_incremental_and_uses_poll_backoff(client: AsyncClient):
    response = await client.get("/tasks")
    assert response.status_code == 200
    source = response.text
    assert "const renderedEventKeys = new Set()" in source
    assert "if (additions.length === 0) return" in source
    # One clear for the first render and one only when the operator switches TaskRun.
    assert source.count("eventsPanel.replaceChildren();") == 2
    assert "Math.min(2000, Math.round(pollDelay * 1.5))" in source
    assert "prefers-reduced-motion: reduce" in source
    assert 'id="result-chart"' in source
    assert "renderResultChart(result)" in source
    assert "采用推荐补查并继续分析" in source
    assert "批准补查 SQL 并继续分析" in source
    assert "await runAnalysis();" in source
    assert "refreshTaskInbox()" in source
    assert "refreshSelectedTask()" in source
    assert "safeLogPayload(event.payload)" in source
    assert "task.metadata?.original_message" in source


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
        artifact_type = (
            "analysis" if path.endswith("/analyze")
            else "advisory" if path.endswith("/run-skill")
            else "rendered_output"
        )
        return 200, {
            "task": {
                "task_run_id": "tr_web_001", "status": "ready_for_report",
                "org_id": "org_default", "team_id": "team_default",
            },
            "artifact": {"artifact_type": artifact_type, "payload": {}},
            "events": [],
        }

    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(router_mod, "_pi_request", fake_pi_request)
    analyzed = await client.post(
        "/api/pi/tasks/tr_web_001/analyze",
        json={"question": "分析下降集中点", "run_async": True},
    )
    advisory = await client.post(
        "/api/pi/tasks/tr_web_001/run-skill",
        json={
            "skill_name": "sql-reviewer",
            "prompt": "审查这段 SQL",
            "idempotency_key": "skill-web-001",
        },
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
    assert advisory.status_code == 200
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
        (
            "POST",
            "/v1/tasks/tr_web_001/run-skill",
            {
                "skill_name": "sql-reviewer",
                "prompt": "审查这段 SQL",
                "idempotency_key": "skill-web-001",
            },
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
async def test_web_chat_message_uses_shared_channel_event_with_server_identity(
    client: AsyncClient, monkeypatch
):
    from config import cfg
    import web.router as router_mod

    calls = []

    async def fake_pi_request(method, path, payload=None):
        calls.append((method, path, payload))
        return 202, {
            "status": "accepted",
            "task": {"task_run_id": "tr_web_chat_001", "channel": "web"},
            "presentation": {"kind": "progress", "task_run_id": "tr_web_chat_001"},
        }

    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(router_mod, "_pi_request", fake_pi_request)
    response = await client.post(
        "/api/pi/chat/messages",
        json={
            "message": "统计本月销售额",
            "conversation_id": "web_conv_12345678",
            "message_id": "web_msg_12345678",
        },
    )

    assert response.status_code == 202
    assert calls == [("POST", "/v1/channel-events", {
        "event_id": "web_msg_12345678",
        "channel": "web",
        "event_type": "message",
        "external_user_id": "web_admin",
        "conversation_id": "web_conv_12345678",
        "message_id": "web_msg_12345678",
        "task_run_id": None,
        "payload": {"text": "统计本月销售额", "chat_type": "web"},
    })]


@pytest.mark.asyncio
async def test_web_chat_task_flow_is_scoped_and_minimally_disclosed(
    client: AsyncClient, monkeypatch
):
    from config import cfg
    import web.router as router_mod

    task = {
        "task_run_id": "tr_web_chat_001", "channel": "web", "user_id": "web_admin",
        "org_id": "org_default", "team_id": "team_default", "status": "analyzing",
        "current_stage": "business_root_cause_analysis", "updated_at": "2026-08-24T08:00:03Z",
    }
    plan = {
        "plan_revision": 2, "status": "active", "route_kind": "query",
        "goal": "分析销售额变化", "required_deliverables": ["query_result", "analysis"],
        "supersedes_artifact_id": "ar_plan_001",
        "steps": [
            {"step_id": "step_query", "capability": "query", "title": "准备并审批查询", "depends_on": [], "required": True, "status": "completed", "deliverable": "query_result"},
            {"step_id": "step_analyze", "capability": "analysis", "title": "分析查询结果", "depends_on": ["step_query"], "required": True, "status": "running", "deliverable": "analysis"},
        ],
    }

    async def fake_pi_request(method, path, payload=None):
        assert method == "GET"
        if path == "/v1/tasks/tr_web_chat_001":
            return 200, {"task": task}
        if path == "/v1/tasks/tr_web_chat_001/events?after=3":
            return 200, {"events": [{
                "event_id": "te_004", "task_run_id": "tr_web_chat_001", "sequence": 4,
                "event_type": "stage.attempt_started", "created_at": "2026-08-24T08:00:03Z",
                "payload": {"prompt": "must not leak", "model_revision": "secret-lineage"},
            }]}
        if path == "/v1/tasks/tr_web_chat_001/artifacts":
            return 200, {"artifacts": [{
                "artifact_id": "ar_plan_002", "artifact_type": "execution_plan",
                "schema_version": 1, "task_run_id": "tr_web_chat_001", "producer": "pi",
                "created_at": "2026-08-24T08:00:02Z", "payload": plan,
            }]}
        if path == "/v1/tasks/tr_web_chat_001/attempts":
            return 200, {"attempts": [{
                "attempt_id": "sa_001", "task_run_id": "tr_web_chat_001", "stage": "analysis",
                "status": "running", "attempt_number": 1, "started_at": "2026-08-24T08:00:03Z",
                "updated_at": "2026-08-24T08:00:03Z", "finished_at": None,
                "error": "must not leak", "model_revision": "must-not-leak",
            }]}
        raise AssertionError(path)

    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(cfg, "PI_WEB_ADMIN_TASK_SCOPES", "org_default:team_default")
    monkeypatch.setattr(router_mod, "_pi_request", fake_pi_request)
    response = await client.get("/api/pi/chat/tasks/tr_web_chat_001/flow?after=3")

    assert response.status_code == 200
    body = response.json()
    assert body["plan"]["plan_revision"] == 2
    assert body["plan"]["steps"][1]["depends_on"] == ["step_query"]
    assert body["events"] == [{
        "sequence": 4, "event_type": "stage.attempt_started",
        "created_at": "2026-08-24T08:00:03Z",
    }]
    assert body["attempts"][0]["status"] == "running"
    serialized = json.dumps(body)
    assert "must not leak" not in serialized
    assert "model_revision" not in serialized
    assert "prompt" not in serialized


@pytest.mark.asyncio
async def test_web_chat_task_flow_rejects_cross_channel_task(client: AsyncClient, monkeypatch):
    from config import cfg
    import web.router as router_mod

    async def fake_pi_request(method, path, payload=None):
        return 200, {"task": {
            "task_run_id": "tr_feishu_001", "channel": "feishu", "user_id": "feishu_owner",
            "org_id": "org_default", "team_id": "team_default",
        }}

    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(cfg, "PI_WEB_ADMIN_TASK_SCOPES", "org_default:team_default")
    monkeypatch.setattr(router_mod, "_pi_request", fake_pi_request)
    response = await client.get("/api/pi/chat/tasks/tr_feishu_001/flow")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_web_chat_action_must_match_current_presentation(
    client: AsyncClient, monkeypatch
):
    from config import cfg
    import web.router as router_mod

    calls = []
    task = {
        "task_run_id": "tr_web_chat_001", "channel": "web", "user_id": "web_admin",
        "org_id": "org_default", "team_id": "team_default",
        "channel_conversation_id": "web_conv_server_owned",
    }
    presentation = {
        "task_run_id": "tr_web_chat_001", "kind": "query_review",
        "actions": [{
            "type": "approve_query", "task_run_id": "tr_web_chat_001",
            "payload": {"query_run_id": "qr_001", "sql_hash": "sha256:abc", "assurance_report_hash": "sha256:def"},
        }],
    }

    async def fake_pi_request(method, path, payload=None):
        calls.append((method, path, payload))
        if path == "/v1/tasks/tr_web_chat_001":
            return 200, {"task": task}
        if path.endswith("/presentation"):
            return 200, {"presentation": presentation}
        return 202, {"status": "accepted", "task": task, "presentation": {"kind": "progress"}}

    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(router_mod, "_pi_request", fake_pi_request)
    rejected = await client.post(
        "/api/pi/chat/tasks/tr_web_chat_001/actions",
        json={
            "action": "approve_query", "conversation_id": "web_conv_12345678",
            "message_id": "web_card_12345678",
            "payload": {"query_run_id": "qr_other", "sql_hash": "sha256:abc", "assurance_report_hash": "sha256:def"},
        },
    )
    assert rejected.status_code == 409

    accepted = await client.post(
        "/api/pi/chat/tasks/tr_web_chat_001/actions",
        json={
            "action": "approve_query", "conversation_id": "web_conv_12345678",
            "message_id": "web_card_12345678",
            "payload": presentation["actions"][0]["payload"],
        },
    )
    assert accepted.status_code == 202
    forwarded = calls[-1]
    assert forwarded[0:2] == ("POST", "/v1/channel-events")
    assert forwarded[2]["channel"] == "web"
    assert forwarded[2]["payload"]["query_run_id"] == "qr_001"


@pytest.mark.asyncio
async def test_web_chat_cannot_act_on_cross_channel_task(client: AsyncClient, monkeypatch):
    from config import cfg
    import web.router as router_mod

    async def fake_pi_request(method, path, payload=None):
        return 200, {"task": {
            "task_run_id": "tr_feishu_001", "channel": "feishu", "user_id": "feishu_owner",
            "org_id": "org_default", "team_id": "team_default",
        }}

    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(router_mod, "_pi_request", fake_pi_request)
    response = await client.post(
        "/api/pi/chat/tasks/tr_feishu_001/actions",
        json={
            "action": "cancel_task", "conversation_id": "web_conv_12345678",
            "message_id": "web_card_12345678", "payload": {},
        },
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_web_proxy_rejects_invalid_task_id(client: AsyncClient, monkeypatch):
    from config import cfg

    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    response = await client.post(
        "/api/pi/tasks/not-a-task/prepare-query",
        json={"question": "查询订单"},
    )
    assert response.status_code == 400
