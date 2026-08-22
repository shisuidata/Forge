from __future__ import annotations

import json
from pathlib import Path

import pytest

from registry.studio import (
    RegistryStudioError,
    RegistryStudioStore,
    deterministic_diff,
    er_projection,
    migrate_legacy_registry,
    parse_ddl_draft,
    render_ddl,
    validate_canonical_schema,
)


def _legacy() -> dict:
    return {
        "tables": {
            "users": {
                "description": "用户",
                "columns": {
                    "id": {"type": "INTEGER", "nullable": False, "pk": True},
                    "name": {"type": "TEXT", "description": "用户名"},
                },
            },
            "orders": {
                "columns": {
                    "id": {"type": "INTEGER", "pk": True},
                    "user_id": {"type": "INTEGER"},
                    "amount": {"type": "REAL"},
                }
            },
        },
        "relationships": [{
            "id": "orders_user",
            "from": "orders.user_id",
            "to": "users.id",
            "cardinality": "many_to_one",
            "status": "confirmed",
            "source": "database_foreign_key",
        }],
    }


def test_legacy_migration_produces_valid_runtime_compatible_canonical_schema():
    canonical = migrate_legacy_registry(_legacy(), datasource_id="demo", dialect="sqlite")

    validate_canonical_schema(canonical)
    assert canonical["datasource"]["id"] == "demo"
    assert canonical["tables"]["users"]["columns"]["id"]["primary_key"] is True
    assert canonical["tables"]["orders"]["columns"]["amount"]["normalized_type"] == "number"
    assert canonical["registry_revision"].startswith("sha256:")


def test_deterministic_diff_marks_destructive_and_type_changes_for_review():
    before = migrate_legacy_registry(_legacy())
    after = json.loads(json.dumps(before))
    del after["tables"]["users"]["columns"]["name"]
    after["tables"]["orders"]["columns"]["amount"]["raw_type"] = "INTEGER"

    changes = deterministic_diff(before, after)

    assert [item["path"] for item in changes] == sorted(item["path"] for item in changes)
    assert all(item["risk"] == "review_required" for item in changes)


def test_ddl_and_er_are_deterministic_projections_and_inference_is_not_trusted():
    canonical = migrate_legacy_registry(_legacy())
    canonical["relationships"].append({
        "id": "inferred_order",
        "from": "orders.user_id",
        "to": "users.id",
        "cardinality": "many_to_one",
        "status": "inferred",
        "source": "name_match",
    })
    # Projection intentionally accepts an in-memory draft before revision refresh.
    ddl = render_ddl(canonical)
    er = er_projection(canonical)

    assert 'CREATE TABLE "users"' in ddl
    assert 'PRIMARY KEY ("id")' in ddl
    assert 'FOREIGN KEY ("user_id") REFERENCES "users" ("id")' in ddl
    assert [item["id"] for item in er["edges"]] == ["orders_user"]
    assert [item["id"] for item in er["proposals"]] == ["inferred_order"]


def test_bounded_ddl_parser_round_trips_supported_column_contract():
    schema = parse_ddl_draft(
        'CREATE TABLE "events" (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL);',
        datasource_id="ddl-demo",
        dialect="sqlite",
    )

    validate_canonical_schema(schema)
    assert schema["tables"]["events"]["columns"]["name"]["nullable"] is False
    assert 'CREATE TABLE "events"' in render_ddl(schema)


def test_draft_publish_uses_cas_and_preserves_revision_history(tmp_path: Path):
    registry_path = tmp_path / "schema.registry.json"
    registry_path.write_text(json.dumps(_legacy()), encoding="utf-8")
    store = RegistryStudioStore(tmp_path / "studio.db", registry_path)
    active = store.active()
    candidate = json.loads(json.dumps(active["schema"]))
    candidate["tables"]["users"]["description"] = "客户用户主数据"
    draft = store.create_draft(
        candidate,
        base_revision_id=active["schema"]["registry_revision"],
        actor="admin",
        reason="补充业务注释",
    )

    published = store.publish(draft["draft_id"], expected_version=active["version"], actor="admin")

    assert published["version"] == active["version"] + 1
    assert published["schema"]["tables"]["users"]["description"] == "客户用户主数据"
    assert json.loads(registry_path.read_text())["schema_version"] == "1.0.0"
    assert store.get_draft(draft["draft_id"])["status"] == "published"
    rolled_back = store.rollback(
        active["schema"]["registry_revision"],
        expected_version=published["version"],
        actor="admin",
        reason="rollback test",
    )
    assert rolled_back["version"] == published["version"] + 1
    assert rolled_back["schema"]["tables"]["users"]["description"] == "用户"
    assert len(store.history()) == 2

    with pytest.raises(RegistryStudioError, match="已经处理"):
        store.publish(draft["draft_id"], expected_version=published["version"], actor="admin")


def test_publish_rejects_stale_binding_without_overwriting_registry(tmp_path: Path):
    registry_path = tmp_path / "schema.registry.json"
    registry_path.write_text(json.dumps(_legacy()), encoding="utf-8")
    store = RegistryStudioStore(tmp_path / "studio.db", registry_path)
    active = store.active()
    draft = store.create_draft(
        active["schema"],
        base_revision_id=active["schema"]["registry_revision"],
        actor="admin",
        reason="no-op",
    )
    before = registry_path.read_bytes()

    with pytest.raises(RegistryStudioError, match="version 冲突"):
        store.publish(draft["draft_id"], expected_version=999, actor="admin")

    assert registry_path.read_bytes() == before
