from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture
def memory_env(governed_client, tmp_path, monkeypatch):
    from config import cfg
    from agent.tenant import tenants
    from forge.governance_credentials import create_credential
    from web.routes import context, memory
    import agent.db as db

    if db._engine is not None:
        db._engine.dispose()
    db._engine = None
    monkeypatch.setattr(cfg, "MEMORY_DB_URL", "")
    monkeypatch.setattr(cfg, "MEMORY_DB_PATH", str(tmp_path / "memory.db"))
    tenants.set_team("user_demo", "fixture_team", principal_type="human", status="active")
    tenants.set_team("pi_memory", "fixture_team", principal_type="service", status="active")
    monkeypatch.setattr(cfg, "PI_EXECUTION_PRINCIPAL_ID", "pi_memory")
    _, service_key = create_credential("pi_memory", kind="pi_service")
    async def scope(task_run_id):
        from fastapi import HTTPException
        if task_run_id != "tr_demo":
            raise HTTPException(status_code=404, detail="task_not_found")
        return {"task_id": task_run_id, "org_id": "fixture_org", "team_id": "fixture_team",
                "user_id": "user_demo", "execution_actor_id": "pi_memory", "mandate_id": None}
    monkeypatch.setattr(memory, "task_scope", scope)
    monkeypatch.setattr(context, "task_scope", scope)
    yield {"X-API-Key": service_key}
    if db._engine is not None:
        db._engine.dispose()
    db._engine = None


@pytest.mark.asyncio
async def test_pi_memory_is_scoped_expiring_retrievable_and_soft_deletable(governed_client, memory_env):
    client = governed_client
    expires = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
    payload = {
        "task_run_id": "tr_demo",
        "operation": "upsert", "category": "session_summary", "key": "task_demo",
        "value": {"question": "上次分析哪个地区销售额最高", "summary": "华东最高"},
        "source_session": "tr_demo", "source_revision": "ar_analysis_demo",
        "confidence": 1.0, "expires_at": expires,
    }
    saved = await client.post(
        "/api/internal/memory/entries", json=payload,
        headers=memory_env,
    )
    assert saved.status_code == 200
    assert saved.json()["scope"] == "user"

    found = await client.post(
        "/api/internal/context/search",
        json={"task_run_id": "tr_demo", "question": "上次哪个地区销售额最高", "limit": 8},
        headers=memory_env,
    )
    assert found.status_code == 200
    memory = next(item for item in found.json()["evidence"] if item["source_type"] == "semantic_memory")
    assert memory["scope"] == "user"
    assert memory["verification_level"] == "contextual"
    assert memory["source_revision"] == "ar_analysis_demo"
    assert memory["expires_at"] == expires

    deleted = await client.post(
        "/api/internal/memory/entries",
        json={**payload, "operation": "delete"},
        headers=memory_env,
    )
    assert deleted.json()["status"] == "deleted"
    missing = await client.post(
        "/api/internal/context/search",
        json={"task_run_id": "tr_demo", "question": "上次哪个地区销售额最高", "limit": 8},
        headers=memory_env,
    )
    assert all(item["source_type"] != "semantic_memory" for item in missing.json()["evidence"])


@pytest.mark.asyncio
async def test_context_marks_conflicting_memory_scopes_instead_of_silently_choosing(governed_client, memory_env):
    client = governed_client
    from agent.memory.smp import SemanticMemoryPool

    pool = SemanticMemoryPool()
    pool.upsert("confirmed_fact", "revenue", "个人口径", user_id="user_demo", scope="user")
    pool.upsert_team("fixture_team", "confirmed_fact", "revenue", "团队口径")
    response = await client.post(
        "/api/internal/context/search",
        json={"task_run_id": "tr_demo", "question": "revenue", "limit": 12},
        headers=memory_env,
    )
    matches = [item for item in response.json()["evidence"] if item["title"] == "confirmed_fact:revenue"]
    assert len(matches) == 2
    assert all(item["verification_level"] == "conflicted" for item in matches)


@pytest.mark.asyncio
async def test_memory_api_rejects_org_scope_escalation_and_missing_auth(governed_client, memory_env):
    client = governed_client
    payload = {
        "task_run_id": "tr_demo",
        "operation": "upsert", "category": "confirmed_fact", "key": "metric_demo",
        "value": "个人确认内容", "scope": "org",
    }
    unauthorized = await client.post("/api/internal/memory/entries", json=payload, headers={"X-API-Key": "unbound"})
    assert unauthorized.status_code == 401
    # Client-supplied scope is not part of the task-bound contract.
    rejected = await client.post("/api/internal/memory/entries", json=payload, headers=memory_env)
    assert rejected.status_code == 422
    saved = await client.post("/api/internal/memory/entries", json={k: v for k, v in payload.items() if k != "scope"}, headers=memory_env)
    assert saved.status_code == 200 and saved.json()["scope"] == "user"
    other_task = await client.post("/api/internal/memory/entries", json={**{k: v for k, v in payload.items() if k != "scope"}, "task_run_id": "tr_other"}, headers=memory_env)
    assert other_task.status_code == 404
