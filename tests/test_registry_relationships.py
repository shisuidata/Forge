import pytest

from registry.relationships import is_fanout_from_existing, load_relationships


REGISTRY = {
    "tables": {
        "orders": {"columns": {"id": {}, "user_id": {}}},
        "users": {"columns": {"id": {}}},
    },
    "relationships": [{
        "id": "orders_user",
        "from": "orders.user_id",
        "to": "users.id",
        "cardinality": "many_to_one",
        "status": "confirmed",
        "source": "manual",
    }],
}


def test_load_relationships_validates_canonical_contract():
    relationship = load_relationships(REGISTRY)[0]
    assert relationship.trusted
    assert not is_fanout_from_existing(
        relationship,
        existing_field="orders.user_id",
        joined_field="users.id",
    )
    assert is_fanout_from_existing(
        relationship,
        existing_field="users.id",
        joined_field="orders.user_id",
    )


def test_declared_relationship_requires_database_source():
    registry = {**REGISTRY, "relationships": [{
        **REGISTRY["relationships"][0],
        "status": "declared",
        "source": "manual",
    }]}
    assert not load_relationships(registry)[0].trusted


def test_inferred_relationship_is_not_trusted():
    registry = {**REGISTRY, "relationships": [{
        **REGISTRY["relationships"][0], "status": "inferred",
    }]}
    assert not load_relationships(registry)[0].trusted


@pytest.mark.parametrize("change", [
    {"from": "orders.missing"},
    {"cardinality": "unknown"},
    {"id": ""},
])
def test_invalid_relationship_contract_fails_closed(change):
    relationship = {**REGISTRY["relationships"][0], **change}
    with pytest.raises(ValueError):
        load_relationships({**REGISTRY, "relationships": [relationship]})
