from __future__ import annotations

import sqlite3

import pytest

from agent.model_control import (
    ModelBindingConflictError,
    ModelControlError,
    ModelControlStore,
)
from agent.model_config import get_model_config, reset_model_config_cache


LINEAGE = {
    "registry_revision": "registry-r1",
    "assurance_revision": "assurance-r1",
    "policy_revision": "policy-r1",
}


def _config(model: str = "model-a", secret_ref: str = "env:MODEL_TEST_KEY") -> dict:
    return {
        "provider": "openai",
        "protocol": "openai_chat",
        "base_url": "https://provider.example/v1",
        "model": model,
        "tool_choice": "required",
        "timeout_seconds": 30,
        "secret_ref": secret_ref,
        "capabilities": {"tool_calling": True},
    }


def _validated_revision(store: ModelControlStore, model: str) -> str:
    revision = store.create_revision(
        profile_id="query-model",
        name="Query Model",
        config=_config(model),
    )
    store.record_validation(
        revision,
        passed=True,
        report={
            "tool_calling": True,
            "structured_output": True,
            "quality_gate": {"passed": True, "lineage": LINEAGE},
        },
    )
    return revision


def test_revision_store_never_persists_secret_value(tmp_path):
    path = tmp_path / "models.db"
    store = ModelControlStore(path)
    revision = store.create_revision(
        profile_id="query-model",
        name="Query Model",
        config=_config(),
    )

    assert store.get_revision(revision)["config"]["secret_ref"] == "env:MODEL_TEST_KEY"
    assert path.stat().st_mode & 0o777 == 0o600
    assert "actual-secret" not in path.read_bytes().decode("utf-8", errors="ignore")
    with pytest.raises(ModelControlError, match="不支持的配置字段"):
        store.create_revision(
            profile_id="unsafe",
            name="Unsafe",
            config={**_config(), "api_key": "actual-secret"},
        )


def test_only_validated_revision_can_activate_with_cas(tmp_path):
    store = ModelControlStore(tmp_path / "models.db")
    pending = store.create_revision(
        profile_id="query-model",
        name="Query Model",
        config=_config(),
    )
    with pytest.raises(ModelControlError, match="尚未通过验证"):
        store.activate(
            pending, expected_version=0, actor="admin", current_lineage=LINEAGE
        )

    store.record_validation(
        pending,
        passed=True,
        report={
            "tool_calling": True,
            "quality_gate": {"passed": True, "lineage": LINEAGE},
        },
    )
    assert store.activate(
        pending, expected_version=0, actor="admin", current_lineage=LINEAGE
    ) == 1
    with pytest.raises(ModelBindingConflictError):
        store.activate(
            pending,
            expected_version=0,
            actor="stale-admin",
            current_lineage=LINEAGE,
        )
    with pytest.raises(ModelControlError, match="不允许重新验证"):
        store.record_validation(pending, passed=False, report={"error": "late failure"})


def test_smoke_only_revision_cannot_bypass_quality_gate(tmp_path):
    store = ModelControlStore(tmp_path / "models.db")
    revision = store.create_revision(
        profile_id="query-model",
        name="Query Model",
        config=_config(),
    )
    store.record_validation(
        revision,
        passed=True,
        report={
            "tool_calling": True,
            "structured_output": True,
            "quality_gate": {"passed": False, "status": "not_run"},
        },
    )
    with pytest.raises(ModelControlError, match="质量与性能门禁"):
        store.activate(
            revision, expected_version=0, actor="admin", current_lineage=LINEAGE
        )


def test_rollback_is_versioned_and_audited(tmp_path):
    store = ModelControlStore(tmp_path / "models.db")
    first = _validated_revision(store, "model-a")
    second = _validated_revision(store, "model-b")
    store.activate(first, expected_version=0, actor="admin", current_lineage=LINEAGE)
    store.activate(second, expected_version=1, actor="admin", current_lineage=LINEAGE)

    version = store.rollback(
        expected_version=2, actor="admin", current_lineage=LINEAGE
    )

    active = store.get_active()
    assert version == 3
    assert active.revision_id == first
    assert [item["action"] for item in store.list_audit()] == [
        "activate", "activate", "rollback",
    ]


def test_active_binding_hot_loads_secret_without_yaml_or_restart(tmp_path, monkeypatch):
    path = tmp_path / "models.db"
    store = ModelControlStore(path)
    revision = _validated_revision(store, "model-control-active")
    store.activate(
        revision, expected_version=0, actor="admin", current_lineage=LINEAGE
    )
    monkeypatch.setenv("MODEL_CONTROL_DB_PATH", str(path))
    monkeypatch.setenv("MODEL_TEST_KEY", "actual-secret")
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    monkeypatch.delenv("LLM_MODEL", raising=False)
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    reset_model_config_cache()

    snapshot = get_model_config()

    assert snapshot.model == "model-control-active"
    assert snapshot.api_key == "actual-secret"
    assert snapshot.revision == revision
    assert snapshot.source == "model-control:forge.query_planning:v1"


def test_new_binding_hot_loads_without_manual_cache_reset(tmp_path, monkeypatch):
    path = tmp_path / "models.db"
    store = ModelControlStore(path)
    first = _validated_revision(store, "model-a")
    second = _validated_revision(store, "model-b")
    store.activate(
        first, expected_version=0, actor="admin", current_lineage=LINEAGE
    )
    monkeypatch.setenv("MODEL_CONTROL_DB_PATH", str(path))
    monkeypatch.setenv("MODEL_TEST_KEY", "actual-secret")
    reset_model_config_cache()
    assert get_model_config().model == "model-a"

    store.activate(
        second, expected_version=1, actor="admin", current_lineage=LINEAGE
    )

    assert get_model_config().model == "model-b"
    assert get_model_config().source.endswith(":v2")


def test_secret_file_must_not_be_group_or_world_readable(tmp_path, monkeypatch):
    secret = tmp_path / "model.key"
    secret.write_text("actual-secret")
    secret.chmod(0o644)
    path = tmp_path / "models.db"
    store = ModelControlStore(path)
    revision = store.create_revision(
        profile_id="query-model",
        name="Query Model",
        config=_config(secret_ref=f"file:{secret}"),
    )
    store.record_validation(
        revision,
        passed=True,
        report={
            "tool_calling": True,
            "quality_gate": {"passed": True, "lineage": LINEAGE},
        },
    )
    store.activate(
        revision, expected_version=0, actor="admin", current_lineage=LINEAGE
    )
    monkeypatch.setenv("MODEL_CONTROL_DB_PATH", str(path))
    reset_model_config_cache()

    with pytest.raises(Exception, match="权限必须为 600"):
        get_model_config()
