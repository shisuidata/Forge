from forge.benchmark_v2 import (
    build_context_snapshot,
    build_result_contract,
    semantic_result_compare,
)


def structure():
    return {
        "tables": [
            {"name": "users", "columns": [
                {"name": "id", "description": "user identifier", "values": ""},
                {"name": "name", "description": "user name", "values": ""},
            ]},
            {"name": "orders", "columns": [
                {"name": "user_id", "description": "ordering user", "values": ""},
                {"name": "amount", "description": "order revenue amount", "values": ""},
            ]},
            {"name": "products", "columns": [
                {"name": "id", "description": "product identifier", "values": ""},
            ]},
        ],
        "relationships": [{"from": "orders.user_id", "to": "users.id"}],
    }


def test_context_snapshot_is_bounded_frozen_and_relationship_complete():
    snapshot = build_context_snapshot(
        "List each user and total order revenue",
        "revenue refers to orders.amount",
        structure(),
        top_k_rounds=(1, 2, 3),
    )
    assert snapshot.sufficiency_status == "sufficient"
    assert "orders" in snapshot.tables
    assert "users" in snapshot.tables
    assert "orders.amount" in snapshot.fields
    assert snapshot.relationships == ("orders.user_id -> users.id",)
    assert len(snapshot.retrieval_rounds) <= 3
    assert snapshot.content_hash.startswith("sha256:")


def test_semantic_compare_allows_column_permutation_when_values_identify_mapping():
    contract = build_result_contract("List user name and total revenue")
    verdict = semantic_result_compare(
        [("Alice", 10.0), ("Bob", 20.0)],
        [(20.0, "Bob"), (10.0, "Alice")],
        contract,
    )
    assert verdict == {
        "correct": True,
        "verdict": "multiset_equal",
        "column_mapping": [1, 0],
    }


def test_semantic_compare_enforces_order_only_when_question_requires_it():
    ordered = build_result_contract("Show users sorted by revenue descending")
    unordered = build_result_contract("Show users and revenue")
    gold = [("Bob", 20), ("Alice", 10)]
    reverse = [("Alice", 10), ("Bob", 20)]
    assert semantic_result_compare(gold, reverse, ordered)["correct"] is False
    assert semantic_result_compare(gold, reverse, unordered)["correct"] is True


def test_semantic_compare_preserves_duplicate_multiplicity():
    contract = build_result_contract("List every order status")
    assert semantic_result_compare([("paid",), ("paid",)], [("paid",), ("open",)], contract)["correct"] is False


def test_semantic_compare_uses_explicit_rounding_policy_only():
    exact = build_result_contract("Show the ratio")
    rounded = build_result_contract("Show the ratio rounded to 2 decimal places")
    assert semantic_result_compare([(1.234,)], [(1.23,)], exact)["correct"] is False
    assert semantic_result_compare([(1.234,)], [(1.2344,)], rounded)["correct"] is True
