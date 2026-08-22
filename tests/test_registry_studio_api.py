from __future__ import annotations

import json

import pytest
from httpx import AsyncClient

from config import cfg
from web.routes.registry_studio import _store


@pytest.fixture
def studio_env(tmp_path, monkeypatch):
    registry = tmp_path / "schema.registry.json"
    registry.write_text(json.dumps({
        "tables": {"users": {"columns": {"id": {"type": "INTEGER", "pk": True}}}},
        "relationships": [{
            "id": "suggested_manager",
            "from": "users.id",
            "to": "users.id",
            "cardinality": "many_to_one",
            "status": "inferred",
            "source": "name_match",
        }],
    }))
    monkeypatch.setattr(cfg, "REGISTRY_PATH", registry)
    monkeypatch.setattr(cfg, "REGISTRY_STUDIO_DB_PATH", tmp_path / "studio.db")
    _store.cache_clear()
    yield registry
    _store.cache_clear()


@pytest.mark.asyncio
async def test_registry_studio_multiview_api_and_page(client: AsyncClient, studio_env):
    table = await client.get("/admin/api/registry-studio?view=table")
    ddl = await client.get("/admin/api/registry-studio?view=ddl")
    er = await client.get("/admin/api/registry-studio?view=er")
    page = await client.get("/admin/registry-studio")

    assert table.status_code == 200
    assert table.json()["projection"][0]["table"] == "users"
    assert 'CREATE TABLE "users"' in ddl.json()["projection"]
    assert er.json()["projection"]["nodes"][0]["id"] == "users"
    assert page.status_code == 200
    assert "不会连接数据库执行 DDL" in page.text
    assert 'id="ddl-editor"' in page.text
    assert "审核通过并发布" in page.text
    assert 'id="er-canvas"' in page.text


@pytest.mark.asyncio
async def test_registry_studio_draft_diff_and_cas_publish(client: AsyncClient, studio_env):
    active = (await client.get("/admin/api/registry-studio?view=json")).json()
    schema = active["projection"]
    schema["tables"]["users"]["description"] = "用户主数据"
    created = await client.post("/admin/api/registry-studio/drafts", json={
        "base_revision_id": active["revision_id"],
        "schema": schema,
        "reason": "补充注释",
    })

    assert created.status_code == 201
    draft = created.json()
    assert any(item["path"].endswith("users.description") for item in draft["diff"])

    stale = await client.post(
        f"/admin/api/registry-studio/drafts/{draft['draft_id']}/publish",
        json={"expected_version": 999},
    )
    assert stale.status_code == 409

    published = await client.post(
        f"/admin/api/registry-studio/drafts/{draft['draft_id']}/publish",
        json={"expected_version": active["version"]},
    )
    assert published.status_code == 200
    assert published.json()["schema"]["tables"]["users"]["description"] == "用户主数据"
    assert json.loads(studio_env.read_text())["registry_revision"] == published.json()["schema"]["registry_revision"]
    history = await client.get("/admin/api/registry-studio/revisions")
    assert len(history.json()["revisions"]) == 2
    rollback = await client.post("/admin/api/registry-studio/rollback", json={
        "revision_id": active["revision_id"],
        "expected_version": published.json()["version"],
        "reason": "API rollback test",
    })
    assert rollback.status_code == 200
    assert rollback.json()["schema"]["registry_revision"] == active["revision_id"]


@pytest.mark.asyncio
async def test_registry_studio_relationship_confirmation_creates_reviewable_draft(
    client: AsyncClient, studio_env
):
    response = await client.post(
        "/admin/api/registry-studio/relationships/suggested_manager/confirm",
        json={"reason": "人工核对关系"},
    )

    assert response.status_code == 201
    body = response.json()
    relationship = body["schema"]["relationships"][0]
    assert relationship["status"] == "confirmed"
    assert relationship["source"] == "manual_confirmation"
    assert json.loads(studio_env.read_text())["relationships"][0]["status"] == "inferred"


@pytest.mark.asyncio
async def test_registry_studio_ddl_import_only_creates_draft(client: AsyncClient, studio_env):
    active = (await client.get("/admin/api/registry-studio?view=json")).json()
    response = await client.post("/admin/api/registry-studio/drafts", json={
        "base_revision_id": active["revision_id"],
        "ddl": "CREATE TABLE events (id INTEGER PRIMARY KEY, happened_at TEXT NOT NULL);",
        "dialect": "sqlite",
        "reason": "DDL import review",
    })

    assert response.status_code == 201
    assert response.json()["status"] == "draft"
    # Draft creation must not overwrite the active Registry file.
    assert "events" not in json.loads(studio_env.read_text())["tables"]
