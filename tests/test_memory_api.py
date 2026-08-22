from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture
def memory_env(tmp_path, monkeypatch):
    from config import cfg
    import agent.db as db

    if db._engine is not None:
        db._engine.dispose()
    db._engine = None
    monkeypatch.setattr(cfg, "MEMORY_DB_URL", "")
    monkeypatch.setattr(cfg, "MEMORY_DB_PATH", str(tmp_path / "memory.db"))
    monkeypatch.setattr(cfg, "PI_SERVICE_API_KEYS", ["pi-memory-secret"])
    yield
    if db._engine is not None:
        db._engine.dispose()
    db._engine = None


@pytest.mark.asyncio
async def test_pi_memory_is_scoped_expiring_retrievable_and_soft_deletable(client, memory_env):
    expires = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
    payload = {
        "org_id": "org_demo", "team_id": "team_demo", "user_id": "user_demo",
        "operation": "upsert", "category": "session_summary", "key": "task_demo",
        "value": {"question": "上次分析哪个地区销售额最高", "summary": "华东最高"},
        "source_session": "tr_demo", "source_revision": "ar_analysis_demo",
        "confidence": 1.0, "expires_at": expires,
    }
    saved = await client.post(
        "/api/internal/memory/entries", json=payload,
        headers={"X-Pi-Service-Key": "pi-memory-secret"},
    )
    assert saved.status_code == 200
    assert saved.json()["scope"] == "user"

    found = await client.post(
        "/api/internal/context/search",
        json={"org_id": "org_demo", "team_id": "team_demo", "user_id": "user_demo",
              "question": "上次哪个地区销售额最高", "limit": 8},
        headers={"X-Pi-Service-Key": "pi-memory-secret"},
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
        headers={"X-Pi-Service-Key": "pi-memory-secret"},
    )
    assert deleted.json()["status"] == "deleted"
    missing = await client.post(
        "/api/internal/context/search",
        json={"org_id": "org_demo", "team_id": "team_demo", "user_id": "user_demo",
              "question": "上次哪个地区销售额最高", "limit": 8},
        headers={"X-Pi-Service-Key": "pi-memory-secret"},
    )
    assert all(item["source_type"] != "semantic_memory" for item in missing.json()["evidence"])


@pytest.mark.asyncio
async def test_context_marks_conflicting_memory_scopes_instead_of_silently_choosing(client, memory_env):
    from agent.memory.smp import SemanticMemoryPool

    pool = SemanticMemoryPool()
    pool.upsert("confirmed_fact", "revenue", "个人口径", user_id="user_demo", scope="user")
    pool.upsert_team("team_demo", "confirmed_fact", "revenue", "团队口径")
    response = await client.post(
        "/api/internal/context/search",
        json={"org_id": "org_demo", "team_id": "team_demo", "user_id": "user_demo",
              "question": "revenue", "limit": 12},
        headers={"X-Pi-Service-Key": "pi-memory-secret"},
    )
    matches = [item for item in response.json()["evidence"] if item["title"] == "confirmed_fact:revenue"]
    assert len(matches) == 2
    assert all(item["verification_level"] == "conflicted" for item in matches)


@pytest.mark.asyncio
async def test_memory_api_rejects_org_scope_escalation_and_missing_auth(client, memory_env):
    payload = {
        "org_id": "org_demo", "team_id": "team_demo", "user_id": "user_demo",
        "operation": "upsert", "category": "confirmed_fact", "key": "metric_demo",
        "value": "个人确认内容", "scope": "org",
    }
    unauthorized = await client.post("/api/internal/memory/entries", json=payload)
    assert unauthorized.status_code == 401
    # Scope in the untrusted payload cannot elevate the write; Pi endpoint fixes it to user.
    saved = await client.post(
        "/api/internal/memory/entries", json=payload,
        headers={"X-Pi-Service-Key": "pi-memory-secret"},
    )
    assert saved.json()["scope"] == "user"
