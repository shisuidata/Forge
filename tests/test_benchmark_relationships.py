from __future__ import annotations

import json
import re
from pathlib import Path

from registry.relationships import load_relationships

DATASET = Path(__file__).parent / "datasets" / "large"
_SQL_KEYWORDS = {
    "on", "where", "inner", "left", "right", "full", "cross", "join",
    "group", "order", "having", "union", "limit",
}


def _physical_join_edges(sql: str, physical_tables: set[str]) -> set[frozenset[str]]:
    cte_names = set(re.findall(r"(?:\bWITH|,)\s*([A-Za-z_]\w*)\s+AS\s*\(", sql, re.I))
    aliases: dict[str, str] = {}
    for match in re.finditer(
        r"\b(?:FROM|JOIN)\s+([A-Za-z_]\w*)(?:\s+(?:AS\s+)?([A-Za-z_]\w*))?",
        sql,
        re.I,
    ):
        table, alias = match.group(1), match.group(2)
        if table in physical_tables:
            aliases[table] = table
            if alias and alias.lower() not in _SQL_KEYWORDS:
                aliases[alias] = table

    edges: set[frozenset[str]] = set()
    for _, _, left, right in re.findall(
        r"\bJOIN\s+([A-Za-z_]\w*)(?:\s+(?:AS\s+)?((?!ON\b)[A-Za-z_]\w*))?\s+ON\s+"
        r"([A-Za-z_]\w*\.[A-Za-z_]\w*)\s*=\s*([A-Za-z_]\w*\.[A-Za-z_]\w*)",
        sql,
        re.I,
    ):
        left_table, left_column = left.split(".", 1)
        right_table, right_column = right.split(".", 1)
        resolved_left = aliases.get(left_table, left_table)
        resolved_right = aliases.get(right_table, right_table)
        if (
            resolved_left in physical_tables
            and resolved_right in physical_tables
            and resolved_left != resolved_right
            and resolved_left not in cte_names
            and resolved_right not in cte_names
        ):
            edges.add(frozenset((
                f"{resolved_left}.{left_column}",
                f"{resolved_right}.{right_column}",
            )))
    return edges


def test_large_registry_relationships_cover_every_reference_sql_physical_join():
    registry = json.loads((DATASET / "schema.registry.json").read_text())
    cases = json.loads((DATASET / "cases.json").read_text())
    relationships = load_relationships(registry)
    trusted_edges = {
        frozenset((item.from_field, item.to_field))
        for item in relationships
        if item.trusted
    }
    reference_edges: set[frozenset[str]] = set()
    for case in cases:
        reference_edges.update(
            _physical_join_edges(case["reference_sql"], set(registry["tables"]))
        )

    assert reference_edges
    assert reference_edges <= trusted_edges, sorted(reference_edges - trusted_edges, key=sorted)


def test_large_registry_relationship_projection_has_one_canonical_source():
    registry = json.loads((DATASET / "schema.registry.json").read_text())
    reference = json.loads((DATASET / "relationships.reference.json").read_text())

    assert registry["relationships"] == reference
    assert all(item["status"] == "confirmed" for item in reference)
    assert all(item["source"] == "benchmark_reference" for item in reference)
