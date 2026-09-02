from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import httpx
import pytest

from config import cfg
from web.routes import product


class FakeReportStore:
    def __init__(self, rows: list[dict]):
        self.rows = rows

    def list(self, **options):
        rows = [
            row
            for row in self.rows
            if row["org_id"] == options["org_id"]
            and row["team_id"] == options["team_id"]
            and row["user_id"] == options["user_id"]
            and (options.get("status") is None or row["status"] == options["status"])
        ]
        rows.sort(key=lambda row: (row["updated_at"], row["report_id"]), reverse=True)
        before = options.get("before")
        if before:
            rows = [
                row
                for row in rows
                if (row["updated_at"], row["report_id"]) < before
            ]
        return rows[: options["limit"]]


def report(report_id: str = "rp_product_001") -> dict:
    return {
        "report_id": report_id,
        "task_run_id": "tr_product_001",
        "org_id": "org_demo",
        "team_id": "team_demo",
        "user_id": "web_admin",
        "revision": 1,
        "bundle_hash": "sha256:" + "a" * 64,
        "title": "渠道支付金额分析",
        "status": "published",
        "pdf_status": "ready",
        "pptx_status": "ready",
        "error": None,
        "created_at": "2026-08-25T08:00:00+00:00",
        "updated_at": "2026-08-25T08:02:00+00:00",
        "internal_url": f"/reports/{report_id}",
        "technical_url": f"/reports/{report_id}/technical",
        "pdf_url": f"/reports/{report_id}/download/pdf",
        "pptx_url": f"/reports/{report_id}/download/pptx",
    }


@pytest.fixture(autouse=True)
def product_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setattr(cfg, "PI_WEB_ADMIN_TASK_SCOPES", "org_demo:team_demo")
    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    schema = tmp_path / "schema.json"
    metrics = tmp_path / "metrics.yaml"
    schema.write_text(json.dumps({"tables": [{"name": "orders"}]}), encoding="utf-8")
    metrics.write_text("metrics:\n  gmv:\n    definition: paid amount\n", encoding="utf-8")
    monkeypatch.setattr(cfg, "REGISTRY_PATH", schema)
    monkeypatch.setattr(cfg, "METRICS_PATH", metrics)


@pytest.mark.asyncio
async def test_product_bff_requires_auth_when_auth_is_enabled(client, monkeypatch):
    monkeypatch.setattr(cfg, "AUTH_ENABLED", True)
    monkeypatch.setattr(cfg, "AUTH_API_KEYS", ["product-key"])
    assert (await client.get("/api/product/workspace")).status_code == 401


@pytest.mark.asyncio
async def test_product_report_list_is_scoped_versioned_and_cursor_bounded(client, monkeypatch):
    rows = [report("rp_product_002"), report("rp_product_001")]
    rows[1]["updated_at"] = "2026-08-25T08:01:00+00:00"
    monkeypatch.setattr(product, "get_report_store", lambda: FakeReportStore(rows))

    first = await client.get("/api/product/reports?limit=1")
    assert first.status_code == 200
    assert first.headers["cache-control"] == "no-store"
    body = first.json()
    assert body["schema_version"] == 1
    assert len(body["reports"]) == 1
    assert body["reports"][0]["projection_type"] == "report_summary_v1"
    assert "error" not in body["reports"][0]
    assert body["next_cursor"]

    second = await client.get(
        "/api/product/reports",
        params={"limit": 1, "cursor": body["next_cursor"]},
    )
    assert second.status_code == 200
    assert [item["report_id"] for item in second.json()["reports"]] == ["rp_product_001"]
    assert (await client.get(
        "/api/product/reports?org_id=org_other&team_id=team_demo"
    )).status_code == 404


@pytest.mark.asyncio
async def test_product_conversation_and_task_bff_validate_and_minimize_upstream(client, monkeypatch):
    fixtures = json.loads(
        Path("agent/contracts/product-projection-fixtures.v1.json").read_text(encoding="utf-8")
    )
    summary = deepcopy(fixtures["valid"]["conversation_summary_v1"][0]["value"])
    summary["scope"] = {
        "org_id": "org_demo",
        "team_id": "team_demo",
        "user_id": "web_admin",
        "channel": "web",
    }

    async def fake_pi(method: str, path: str, payload=None):
        assert method == "GET"
        if path.startswith("/v1/conversations"):
            return 200, {"schema_version": 1, "conversations": [summary], "next_cursor": None}
        if path.startswith("/v1/tasks"):
            return 200, {"tasks": [{
                "task_run_id": "tr_product_001",
                "org_id": "org_demo",
                "team_id": "team_demo",
                "user_id": "web_admin",
                "channel": "web",
                "channel_conversation_id": "web_conv_product",
                "intent": "query",
                "status": "needs_input",
                "current_stage": "clarification",
                "parent_task_run_id": None,
                "created_at": "2026-08-25T08:00:00Z",
                "updated_at": "2026-08-25T08:01:00Z",
                "metadata": {"original_message": "password=do-not-project"},
            }]}
        raise AssertionError(path)

    monkeypatch.setattr(product, "pi_request", fake_pi)
    conversations = await client.get("/api/product/conversations")
    assert conversations.status_code == 200
    assert conversations.json()["conversations"][0]["conversation_id"] == "web_conv_001"

    tasks = await client.get("/api/product/tasks")
    assert tasks.status_code == 200
    task = tasks.json()["tasks"][0]
    assert task["title"] == "query"
    assert "metadata" not in task
    assert "password" not in json.dumps(task)

    invalid = deepcopy(summary)
    invalid["api_key"] = "must-fail"

    async def invalid_pi(method: str, path: str, payload=None):
        return 200, {"schema_version": 1, "conversations": [invalid], "next_cursor": None}

    monkeypatch.setattr(product, "pi_request", invalid_pi)
    rejected = await client.get("/api/product/conversations")
    assert rejected.status_code == 502
    assert rejected.json()["detail"]["code"] == "upstream_contract_invalid"

    wrong_scope = deepcopy(summary)
    wrong_scope["scope"]["org_id"] = "org_other"

    async def wrong_scope_pi(method: str, path: str, payload=None):
        return 200, {"schema_version": 1, "conversations": [wrong_scope], "next_cursor": None}

    monkeypatch.setattr(product, "pi_request", wrong_scope_pi)
    scope_rejected = await client.get("/api/product/conversations")
    assert scope_rejected.status_code == 502
    assert scope_rejected.json()["detail"]["code"] == "upstream_scope_mismatch"


@pytest.mark.asyncio
async def test_workspace_degrades_when_pi_is_offline_without_hiding_reports(client, monkeypatch):
    monkeypatch.setattr(product, "get_report_store", lambda: FakeReportStore([report()]))

    async def offline_pi(method: str, path: str, payload=None):
        raise httpx.ConnectError("offline")

    monkeypatch.setattr(product, "pi_request", offline_pi)
    response = await client.get("/api/product/workspace")
    assert response.status_code == 200
    body = response.json()
    assert body["projection_type"] == "workspace_projection_v1"
    assert body["projection_meta"]["availability"] == "partial"
    assert body["counts"]["recent_reports"] == 1
    assert body["recent_reports"][0]["item_type"] == "report"
    assert any(item["item_id"] == "pi_orchestrator" for item in body["dependencies"])
    assert "pi_orchestrator_unavailable" in body["projection_meta"]["unavailable_reasons"]


@pytest.mark.asyncio
async def test_data_summary_returns_partial_instead_of_fabricating_counts(client, monkeypatch, tmp_path):
    monkeypatch.setattr(cfg, "REGISTRY_PATH", tmp_path / "missing-schema.json")
    response = await client.get("/api/product/data-summary")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "partial"
    assert body["counts"]["tables"] == 0
    assert body["source_revision"].startswith("sha256:")
    assert "schema_registry_unavailable" in body["unavailable_reasons"]
    repeated = await client.get("/api/product/data-summary")
    assert repeated.json()["source_revision"] == body["source_revision"]
