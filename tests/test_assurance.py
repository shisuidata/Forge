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
            "order_items": {"columns": {"id": {}, "order_id": {}, "amount": {}}},
        },
        "relationships": [
            {
                "id": "orders_user",
                "from": "orders.user_id",
                "to": "users.id",
                "cardinality": "many_to_one",
                "status": "confirmed",
                "source": "manual",
            },
            {
                "id": "items_order",
                "from": "order_items.order_id",
                "to": "orders.id",
                "cardinality": "many_to_one",
                "status": "confirmed",
                "source": "manual",
            },
        ],
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
    assert report.sql_hash and report.sql_hash.startswith("sha256:")
    assert len(report.sql_hash) == 71
    assert report.registry_revision and len(report.registry_revision) == 64
    assert report.model_revision == "unknown"
    assert [gate.gate for gate in report.gates] == [
        "contract_scope_type",
        "registry_acl_alias",
        "relationship_grain",
        "convention_policy",
        "intent_fulfillment",
        "scope_type_compile",
        "sql_safety",
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
    assert report.gates[-1].gate == "registry_acl_alias"
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


def test_assurance_rejects_undefined_bare_select_alias(assurance_registry):
    with pytest.raises(QueryAssuranceError, match="未定义的字段或计算别名"):
        assure_query(
            {
                "scan": "orders",
                "joins": [{
                    "type": "inner",
                    "table": "users",
                    "on": {"left": "orders.user_id", "right": "users.id"},
                }],
                "group": ["users.name"],
                "select": ["users.name", "repurchase_rate"],
            },
            "各城市订单总额",
            dialect="sqlite",
        )


def test_assurance_accepts_defined_aggregate_alias(assurance_registry):
    report = assure_query(
        {
            "scan": "orders",
            "agg": [{"fn": "sum", "col": "orders.total_amount", "as": "order_total"}],
            "select": ["order_total"],
            "sort": [{"col": "order_total", "dir": "desc"}],
        },
        "订单总额",
        dialect="sqlite",
    )
    assert report.status == "passed"


def test_assurance_rejects_join_without_confirmed_relationship(assurance_registry):
    registry = json.loads(assurance_registry.read_text())
    registry["relationships"][0]["status"] = "inferred"
    assurance_registry.write_text(json.dumps(registry))

    with pytest.raises(QueryAssuranceError, match="人工确认") as caught:
        assure_query(
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
    assert caught.value.report.gates[-1].gate == "relationship_grain"


def test_assurance_rejects_aggregate_fanout_from_existing_side(assurance_registry):
    with pytest.raises(QueryAssuranceError, match="放大已有侧聚合度量"):
        assure_query(
            {
                "scan": "orders",
                "joins": [{
                    "type": "inner",
                    "table": "order_items",
                    "on": {"left": "orders.id", "right": "order_items.order_id"},
                }],
                "agg": [{"fn": "sum", "col": "orders.total_amount", "as": "revenue"}],
                "select": ["revenue"],
            },
            "订单总额",
            dialect="sqlite",
        )


def test_assurance_allows_many_to_one_aggregate_join(assurance_registry):
    report = assure_query(
        {
            "scan": "orders",
            "joins": [{
                "type": "inner",
                "table": "users",
                "on": {"left": "orders.user_id", "right": "users.id"},
            }],
            "group": ["users.name"],
            "agg": [{"fn": "sum", "col": "orders.total_amount", "as": "revenue"}],
            "select": ["users.name", "revenue"],
        },
        "各用户订单总额",
        dialect="sqlite",
    )
    assert report.status == "passed"


@pytest.mark.parametrize(
    ("question", "query", "message"),
    [
        (
            "显示每个用户上一笔订单金额",
            {"scan": "orders", "select": ["orders.user_id", "orders.total_amount"]},
            "LAG",
        ),
        (
            "订单按金额降序排列",
            {"scan": "orders", "select": ["orders.id", "orders.total_amount"]},
            "sort",
        ),
        (
            "计算每个用户订单金额占比",
            {"scan": "orders", "select": ["orders.user_id", "orders.total_amount"]},
            "比率",
        ),
        (
            "每笔订单金额排名并显示排名",
            {"scan": "orders", "select": ["orders.user_id", "orders.total_amount"]},
            "排名窗口",
        ),
    ],
)
def test_assurance_rejects_omitted_explicit_user_intent(
    assurance_registry, question, query, message
):
    with pytest.raises(QueryAssuranceError, match=message) as caught:
        assure_query(query, question, dialect="sqlite")

    assert caught.value.report.gates[-1].gate == "intent_fulfillment"


def test_assurance_enforces_table_acl_server_side(assurance_registry):
    with pytest.raises(QueryAssuranceError, match="未授权或不存在"):
        assure_query(
            {"scan": "users", "select": ["users.id"]},
            "查询用户",
            dialect="sqlite",
            allowed_tables=["orders"],
        )
