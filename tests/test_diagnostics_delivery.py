from __future__ import annotations

import json
import sqlite3

import httpx
import pytest
from fastapi import FastAPI

from diagnostics import model_usage
from web.channel_delivery import DeliveryBusy, DeliveryReceiptStore
from web.diagnostics import RequestCorrelationMiddleware


@pytest.mark.asyncio
@pytest.mark.parametrize("upstream_status, payload, expected, code", [
    (200, {"task": {"channel": "web", "user_id": "web_admin", "org_id": "org_default", "team_id": "team_default"}}, 200, None),
    (200, {}, 502, "upstream_contract_invalid"),
    (403, {"error": "secret scope"}, 404, "not_found"),
    (404, {"error": "secret scope"}, 404, "not_found"),
    (500, {"error": "secret exception"}, 502, "upstream_unavailable"),
    (503, {"error": "secret exception"}, 503, "upstream_unavailable"),
    (200, "invalid-json", 502, "upstream_contract_invalid"),
])
async def test_task_error_boundary_preserves_failure_class_without_private_details(monkeypatch, upstream_status, payload, expected, code):
    from config import cfg
    from web import pi_client
    from web.router import chat_router

    seen = []
    def upstream(request):
        seen.append(request.headers.get("X-Request-ID"))
        if request.url.path.endswith("/presentation"):
            return httpx.Response(200, json={"presentation": {"kind": "progress"}})
        return httpx.Response(upstream_status, content=payload) if isinstance(payload, str) else httpx.Response(upstream_status, json=payload)

    client_type = httpx.AsyncClient
    monkeypatch.setattr(pi_client.httpx, "AsyncClient", lambda **kwargs: client_type(**kwargs, transport=httpx.MockTransport(upstream)))
    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(cfg, "PI_WEB_ADMIN_TASK_SCOPES", "org_default:team_default")
    monkeypatch.setattr(cfg, "AUTH_ENABLED", False)
    app = FastAPI()
    app.add_middleware(RequestCorrelationMiddleware)
    app.include_router(chat_router)
    async with client_type(transport=httpx.ASGITransport(app), base_url="http://synthetic") as client:
        response = await client.get("/api/pi/chat/tasks/tr_probe/presentation", headers={"X-Request-ID": "req_probe"})
    assert response.status_code == expected
    assert response.headers["X-Request-ID"] == "req_probe"
    assert set(seen) == {"req_probe"}
    assert "secret" not in response.text
    if code:
        assert response.json()["code"] == code
        assert response.json()["request_id"] == "req_probe"


def test_unknown_delivery_redelivers_presentation_without_resubmitting_task(tmp_path, monkeypatch):
    from web import feishu_pi
    store = DeliveryReceiptStore(tmp_path / "delivery.db")
    calls = {"submit": 0, "send": 0, "read": 0}
    presentation = {"kind": "analysis", "title": "Synthetic result", "markdown": "Complete", "fields": [], "table": None, "actions": []}
    class Pi:
        def submit_message(self, **kwargs):
            calls["submit"] += 1
            return {"task": {"task_run_id": "tr_synthetic"}}
        def wait_for_presentation(self, task_run_id):
            return presentation
        def get_presentation(self, task_run_id):
            calls["read"] += 1
            return presentation
    def send(*args, **kwargs):
        calls["send"] += 1
        if calls["send"] == 1:
            raise TimeoutError("provider acknowledgement lost")
        return "remote_message"
    monkeypatch.setattr(feishu_pi, "get_delivery_store", lambda: store)
    monkeypatch.setattr(feishu_pi, "_get_pi_client", Pi)
    monkeypatch.setattr(feishu_pi, "_send_card", send)
    feishu_pi._process_message("private_recipient", "private_conversation", "cmd_original", "synthetic question", "p2p")
    first = store.for_task("tr_synthetic")[0]
    assert first["status"] == "unknown"
    assert first["retry_available"]
    assert "private_" not in json.dumps(first)
    restored = DeliveryReceiptStore(tmp_path / "delivery.db")
    monkeypatch.setattr(feishu_pi, "get_delivery_store", lambda: restored)
    delivered = feishu_pi.deliver_receipt(first["delivery_id"])
    assert delivered["status"] == "delivered"
    assert delivered["attempts"] == 2
    assert calls == {"submit": 1, "send": 2, "read": 1}
    with pytest.raises(DeliveryBusy):
        feishu_pi.deliver_receipt(first["delivery_id"])
    assert calls["send"] == 2


@pytest.mark.asyncio
async def test_audit_projection_is_read_only_scoped_and_explicit_when_partial(tmp_path, monkeypatch):
    from agent import audit
    query_path, legacy_path = tmp_path / "queries.db", tmp_path / "legacy.db"
    monkeypatch.setattr(audit.cfg, "QUERY_RUN_DB_PATH", str(query_path))
    monkeypatch.setattr(audit.cfg, "AUDIT_DB_PATH", str(legacy_path))
    with sqlite3.connect(query_path) as db:
        db.execute("CREATE TABLE query_runs (query_run_id, task_run_id, user_id, created_at, status, model_revision, row_count, execution_ms, org_id, team_id)")
        db.executemany("INSERT INTO query_runs VALUES (?, ?, 'u', '2026-09-07T12:00:00Z', 'completed', 'model_rev', 0, 5, ?, ?)",
                       [("qr_visible", "tr_visible", "org", "team"), ("qr_hidden", "tr_hidden", "other", "team")])
    before = query_path.read_bytes()
    view = await audit.projection(scopes=[("org", "team")], keyword="tr_visible")
    assert view["availability"] == "partial"
    assert view["total"] is None and view["today"] is None
    assert [row["id"] for row in view["records"]] == ["qr_visible"]
    assert view["records"][0]["usage_status"] == "unknown"
    assert not legacy_path.exists()
    assert query_path.read_bytes() == before


def test_usage_missing_is_not_reported_as_zero():
    assert model_usage(None) == {"usage_status": "unknown", "input_tokens": None, "output_tokens": None}
    assert model_usage({"prompt_tokens": 0, "completion_tokens": 0}) == {"usage_status": "known", "input_tokens": 0, "output_tokens": 0}
    assert model_usage({"prompt_tokens": 12})["usage_status"] == "unknown"


@pytest.mark.asyncio
async def test_delivery_receipts_hide_scope_and_do_not_expose_recipients(tmp_path, monkeypatch):
    from config import cfg
    from web.routes import deliveries
    store = DeliveryReceiptStore(tmp_path / "delivery.db")
    receipt = store.create(task_run_id="tr_hidden", command_id="cmd_original", target_id="private_recipient", conversation_id="private_conversation")
    monkeypatch.setattr(deliveries, "get_delivery_store", lambda: store)
    monkeypatch.setattr(cfg, "AUTH_ENABLED", False)
    monkeypatch.setattr(cfg, "PI_WEB_ADMIN_TASK_SCOPES", "org:team")
    scope = {"org_id": "other", "team_id": "team"}
    async def upstream(*args):
        return 200, {"task": {"channel": "feishu", **scope}}
    monkeypatch.setattr(deliveries, "pi_request", upstream)
    app = FastAPI()
    app.add_middleware(RequestCorrelationMiddleware)
    app.include_router(deliveries.router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://synthetic") as client:
        hidden = await client.get("/api/pi/tasks/tr_hidden/deliveries")
        denied = await client.post("/api/pi/deliveries/" + receipt["delivery_id"] + "/retry")
        assert hidden.status_code == denied.status_code == 404
        assert "other" not in hidden.text and "private_" not in denied.text
        assert store.get(receipt["delivery_id"])["attempts"] == 0
        scope["org_id"] = "org"
        visible = await client.get("/api/pi/tasks/tr_hidden/deliveries")
        assert visible.json()["deliveries"][0]["delivery_id"] == receipt["delivery_id"]
        assert "private_" not in visible.text


def test_process_alive_does_not_claim_channel_connected(tmp_path, monkeypatch):
    from web.feishu_runtime import FeishuRuntimeSupervisor
    supervisor = FeishuRuntimeSupervisor(tmp_path / "synthetic-settings.yaml")
    monkeypatch.setattr(supervisor, "_settings", lambda: {"enabled": True, "credentials_configured": True, "channel_key_configured": True})
    class Process:
        def poll(self):
            return None
    supervisor._process = Process()
    assert supervisor.status().process_running
    assert supervisor.status().connection_status == "unknown"
    supervisor._process = None
    assert supervisor.status().connection_status == "disconnected"
