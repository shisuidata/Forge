"""Canonical Registry relationship contracts and deterministic JOIN validation."""
from __future__ import annotations

from dataclasses import dataclass
TRUSTED_RELATIONSHIP_STATUSES = frozenset({"declared", "confirmed"})
VALID_CARDINALITIES = frozenset({"many_to_one", "one_to_many", "one_to_one"})


@dataclass(frozen=True)
class RegistryRelationship:
    relationship_id: str
    from_field: str
    to_field: str
    cardinality: str
    status: str
    source: str

    @property
    def trusted(self) -> bool:
        return self.status == "confirmed" or (
            self.status == "declared" and self.source == "database"
        )


def load_relationships(registry: dict) -> tuple[RegistryRelationship, ...]:
    """Validate relationship metadata without guessing missing relationships."""
    tables = registry.get("tables", registry)
    known_fields = {
        f"{table}.{column}"
        for table, info in tables.items()
        for column in (info.get("columns", info) if isinstance(info, dict) else info)
    }
    result: list[RegistryRelationship] = []
    seen_ids: set[str] = set()
    seen_edges: set[tuple[str, str]] = set()
    for raw in registry.get("relationships", []):
        if not isinstance(raw, dict):
            raise ValueError("Registry 关系元数据格式错误。")
        relationship_id = raw.get("id")
        from_field = raw.get("from")
        to_field = raw.get("to")
        cardinality = raw.get("cardinality")
        status = raw.get("status")
        source = raw.get("source")
        if not all(isinstance(value, str) and value for value in (
            relationship_id, from_field, to_field, cardinality, status, source
        )):
            raise ValueError("Registry 关系元数据缺少必填属性。")
        if relationship_id in seen_ids or tuple(sorted((from_field, to_field))) in seen_edges:
            raise ValueError("Registry 关系元数据包含重复定义。")
        if from_field not in known_fields or to_field not in known_fields:
            raise ValueError("Registry 关系引用了不存在的表或字段。")
        if from_field.split(".", 1)[0] == to_field.split(".", 1)[0]:
            raise ValueError("Registry 关系两端必须属于不同物理表。")
        if cardinality not in VALID_CARDINALITIES:
            raise ValueError("Registry 关系基数不受支持。")
        seen_ids.add(relationship_id)
        seen_edges.add(tuple(sorted((from_field, to_field))))
        result.append(RegistryRelationship(
            relationship_id=relationship_id,
            from_field=from_field,
            to_field=to_field,
            cardinality=cardinality,
            status=status,
            source=source,
        ))
    return tuple(result)


def relationship_to_dict(relationship: RegistryRelationship) -> dict[str, str]:
    return {
        "id": relationship.relationship_id,
        "from": relationship.from_field,
        "to": relationship.to_field,
        "cardinality": relationship.cardinality,
        "status": relationship.status,
        "source": relationship.source,
    }


def is_fanout_from_existing(
    relationship: RegistryRelationship,
    *,
    existing_field: str,
    joined_field: str,
) -> bool:
    """Return whether one existing-side row may match multiple joined-side rows."""
    if relationship.cardinality == "one_to_one":
        return False
    if relationship.cardinality == "many_to_one":
        return (
            existing_field == relationship.to_field
            and joined_field == relationship.from_field
        )
    return (
        existing_field == relationship.from_field
        and joined_field == relationship.to_field
    )
