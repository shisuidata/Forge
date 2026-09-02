from __future__ import annotations

import json

import pytest

from config import cfg
from forge.assurance import QueryAssuranceError, assure_direct_sql, assure_query


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
    with pytest.raises(QueryAssuranceError, match="未定义的字段或计算别名") as caught:
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

    assert '"expr"' in str(caught.value)


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
    assert "共同的可信维表" in str(caught.value)


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


def test_assurance_allows_group_rank_used_only_for_filter(assurance_registry):
    report = assure_query(
        {
            "scan": "orders",
            "window": [
                {
                    "fn": "row_number",
                    "partition": ["orders.user_id"],
                    "order": [{"col": "orders.total_amount", "dir": "desc"}],
                    "as": "rn",
                }
            ],
            "qualify": [{"col": "rn", "op": "eq", "val": 1}],
            "select": ["orders.user_id", "orders.total_amount"],
        },
        "各用户内按金额排名第1，显示用户ID和金额",
        dialect="sqlite",
    )

    assert report.status == "passed"


def test_assurance_allows_aggregate_cumulative_spend_without_window(assurance_registry):
    report = assure_query(
        {
            "scan": "orders",
            "group": ["orders.user_id"],
            "agg": [{"fn": "sum", "col": "orders.total_amount", "as": "total_spent"}],
            "having": [{"col": "total_spent", "op": "gt", "val": 5000}],
            "select": ["orders.user_id", "total_spent"],
        },
        "找出累计消费超过5000元的用户，显示用户ID和消费总额",
        dialect="sqlite",
    )

    assert report.status == "passed"


def test_assurance_rejects_missing_explicit_display_field(assurance_registry):
    with pytest.raises(QueryAssuranceError, match="用户名") as caught:
        assure_query(
            {
                "scan": "orders",
                "group": ["orders.user_id"],
                "agg": [{"fn": "count", "col": "orders.id", "as": "order_count"}],
                "select": ["orders.user_id", "order_count"],
            },
            "显示用户名和订单数",
            dialect="sqlite",
        )

    assert caught.value.report.gates[-1].gate == "intent_fulfillment"


def test_assurance_enforces_table_acl_server_side(assurance_registry):
    with pytest.raises(QueryAssuranceError, match="未授权或不存在"):
        assure_query(
            {"scan": "users", "select": ["users.id"]},
            "查询用户",
            dialect="sqlite",
            allowed_tables=["orders"],
        )


def test_direct_sql_assurance_returns_same_hash_bound_report(assurance_registry):
    sql = "SELECT orders.id, orders.total_amount FROM orders"

    report = assure_direct_sql(
        sql,
        dialect="sqlite",
        allowed_tables=["orders"],
        producer_revision="external-agent-r1",
    )

    assert report.status == "passed"
    assert report.input_kind == "direct_sql"
    assert report.candidate_revision == "query-candidate-v1"
    assert report.sql == sql
    assert report.sql_hash.startswith("sha256:")
    assert [gate.gate for gate in report.gates] == [
        "sql_safety", "sql_parse", "registry_acl",
    ]


def test_direct_sql_and_forge_json_share_assurance_identity(assurance_registry):
    forge_report = assure_query(
        {"scan": "orders", "select": ["orders.id", "orders.total_amount"]},
        "查询订单 ID 和总额",
        dialect="sqlite",
        allowed_tables=["orders"],
    )

    direct_report = assure_direct_sql(
        forge_report.sql,
        dialect="sqlite",
        allowed_tables=["orders"],
    )

    assert forge_report.input_kind == "forge_json"
    assert direct_report.input_kind == "direct_sql"
    assert direct_report.sql_hash == forge_report.sql_hash
    assert direct_report.assurance_revision == forge_report.assurance_revision
    assert direct_report.policy_revision == forge_report.policy_revision
    assert direct_report.registry_revision == forge_report.registry_revision
    assert direct_report.candidate_revision == forge_report.candidate_revision


def test_direct_sql_assurance_accepts_explicit_registry_snapshot(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "REGISTRY_PATH", tmp_path / "missing.json")
    registry = {
        "tables": {
            "external_orders": {"columns": {"id": {}, "amount": {}}},
        },
    }

    report = assure_direct_sql(
        "SELECT external_orders.id FROM external_orders",
        dialect="sqlite",
        registry_snapshot=registry,
        registry_revision="snapshot-r1",
    )

    assert report.status == "passed"
    assert report.registry_revision == "snapshot-r1"


def test_direct_sql_assurance_rejects_unknown_field_in_explicit_snapshot(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "REGISTRY_PATH", tmp_path / "missing.json")
    registry = {"tables": {"external_orders": {"columns": {"id": {}}}}}

    with pytest.raises(QueryAssuranceError) as caught:
        assure_direct_sql(
            "SELECT external_orders.secret FROM external_orders",
            dialect="sqlite",
            registry_snapshot=registry,
            registry_revision="snapshot-r1",
        )

    assert caught.value.report.registry_revision == "snapshot-r1"


def test_direct_sql_assurance_rejects_mutation(assurance_registry):
    with pytest.raises(QueryAssuranceError, match="只允许执行只读") as caught:
        assure_direct_sql("DELETE FROM orders", dialect="sqlite")

    assert caught.value.report.input_kind == "direct_sql"
    assert caught.value.report.gates[-1].gate == "sql_safety"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT users.id FROM users",
        "SELECT orders.secret_unknown FROM orders",
    ],
)
def test_direct_sql_assurance_enforces_registry_scope(assurance_registry, sql):
    with pytest.raises(QueryAssuranceError, match="未授权或不存在") as caught:
        assure_direct_sql(sql, dialect="sqlite", allowed_tables=["orders"])

    assert caught.value.report.gates[-1].gate == "registry_acl"
