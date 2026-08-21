from __future__ import annotations

import json

import pytest

from config import cfg
from forge.assurance import QueryAssuranceError, assure_query


@pytest.fixture
def assurance_registry(tmp_path, monkeypatch):
    path = tmp_path / "schema.registry.json"
    path.write_text(json.dumps({
        "tables": {
            "orders": {"columns": {
                "id": {}, "user_id": {}, "status": {}, "total_amount": {},
            }},
            "users": {"columns": {"id": {}, "name": {}}},
        }
    }), encoding="utf-8")
    monkeypatch.setattr(cfg, "REGISTRY_PATH", path)
    return path


def test_assurance_returns_versioned_hash_bound_report(assurance_registry):
    report = assure_query(
        {
            "scan": "orders",
            "joins": [{
                "type": "inner",
                "table": "users",
                "on": {"left": "orders.user_id", "right": "users.id"},
            }],
            "select": ["orders.id", "users.name"],
        },
        "查询订单用户",
        dialect="sqlite",
    )

    assert report.status == "passed"
    assert report.sql_hash and len(report.sql_hash) == 64
    assert report.registry_revision and len(report.registry_revision) == 64
    assert report.model_revision == "unknown"
    assert [gate.gate for gate in report.gates] == [
        "contract_registry_acl", "convention_policy", "scope_type_compile", "sql_safety"
    ]
    assert all(gate.status == "passed" for gate in report.gates)


def test_assurance_rejects_unknown_registry_field_without_leaking_enum(assurance_registry):
    with pytest.raises(QueryAssuranceError) as caught:
        assure_query(
            {"scan": "orders", "select": ["orders.secret_unknown"]},
            "查询未知字段",
            dialect="sqlite",
        )

    report = caught.value.report
    assert report.status == "failed"
    assert report.gates[-1].gate == "contract_registry_acl"
    assert "不存在" in str(caught.value)
    assert "secret_unknown" not in str(caught.value)


def test_assurance_fails_closed_when_registry_is_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "REGISTRY_PATH", tmp_path / "missing.json")

    with pytest.raises(QueryAssuranceError, match="Registry 不可用") as caught:
        assure_query(
            {"scan": "orders", "select": ["orders.id"]},
            "查询订单",
            dialect="sqlite",
            model_revision="model-r1",
        )

    assert caught.value.report.registry_revision == "unavailable"
    assert caught.value.report.model_revision == "model-r1"


def test_assurance_enforces_table_acl_server_side(assurance_registry):
    with pytest.raises(QueryAssuranceError, match="未授权或不存在"):
        assure_query(
            {"scan": "users", "select": ["users.id"]},
            "查询用户",
            dialect="sqlite",
            allowed_tables=["orders"],
        )
