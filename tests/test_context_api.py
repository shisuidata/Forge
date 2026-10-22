from __future__ import annotations

import json

import pytest
from httpx import AsyncClient


@pytest.fixture
def context_env(tmp_path, monkeypatch):
    from config import cfg
    import forge.context as context
    from web.routes import context as context_route
    from agent.tenant import tenants
    from forge.governance_credentials import create_credential

    schema = tmp_path / "schema.json"
    schema.write_text(json.dumps({
        "tables": {
            "orders": {
                "description": "订单事实表",
                "columns": {"total_amount": {"description": "订单支付金额"}},
            }
        }
    }, ensure_ascii=False))
    metrics = tmp_path / "metrics.yaml"
    metrics.write_text("revenue:\n  label: 销售额\n  formula: orders.total_amount\n")
    empty = tmp_path / "empty.yaml"
    empty.write_text("{}\n")
    monkeypatch.setattr(cfg, "REGISTRY_PATH", schema)
    monkeypatch.setattr(cfg, "METRICS_PATH", metrics)
    monkeypatch.setattr(cfg, "DISAMBIGUATIONS_PATH", empty)
    monkeypatch.setattr(cfg, "CONVENTIONS_PATH", empty)
    monkeypatch.setattr(cfg, "BUSINESS_CONTEXT_PATH", empty)
    monkeypatch.setattr(cfg, "QUERY_RUN_DB_PATH", str(tmp_path / "governance.db"))
    monkeypatch.setattr(cfg, "AUTH_ENABLED", True)
    monkeypatch.setattr(cfg, "PI_EXECUTION_PRINCIPAL_ID", "context_pi_service")
    tenants.create_team("context_team", organization_id="context_org")
    tenants.set_team("context_pi_service", "context_team", principal_type="service", status="active")
    _, service_key = create_credential("context_pi_service", kind="pi_service")
    async def scoped_task(task_run_id):
        return {"task_id": task_run_id, "user_id": "user_demo", "team_id": "team_demo"}
    monkeypatch.setattr(context_route, "task_scope", scoped_task)
    monkeypatch.setattr(context_route, "task_tables", lambda scope: ["orders"])
    monkeypatch.setattr(context, "_memory_documents", lambda user_id, team_id: [])
    return {"X-API-Key": service_key}


@pytest.mark.asyncio
async def test_context_api_returns_bounded_registry_evidence(client: AsyncClient, context_env):
    response = await client.post(
        "/api/internal/context/search",
        headers=context_env,
        json={
            "task_run_id": "tr_context_1",
            "question": "订单表有哪些字段",
            "limit": 8,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["bounded"] is True
    assert body["evidence_count"] >= 1
    assert any(item["source_type"] == "schema" for item in body["evidence"])
    assert all(item["evidence_ref"].startswith("ctx_") for item in body["evidence"])
    assert all(len(item["content"]) <= 1200 for item in body["evidence"])
    assert all(item["verification_level"] == "verified" for item in body["evidence"])
    assert all(item["scope"] == "organization" for item in body["evidence"])
    assert all(item["source_revision"].startswith("sha256:") for item in body["evidence"])
    assert "rows" not in body


@pytest.mark.asyncio
async def test_context_api_requires_pi_service_auth(client: AsyncClient, context_env):
    response = await client.post(
        "/api/internal/context/search",
        json={
            "org_id": "org_demo", "team_id": "team_demo", "user_id": "user_demo",
            "question": "订单表有哪些字段",
        },
    )
    assert response.status_code == 401
