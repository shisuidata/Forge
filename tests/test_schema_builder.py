from __future__ import annotations

import json

from forge.schema_builder import build_tool_schema


def _registry() -> dict:
    return {
        "tables": {
            "orders": {
                "columns": {
                    "id": {"type": "integer"},
                    "user_id": {"type": "integer"},
                    "total_amount": {"type": "number"},
                }
            }
        }
    }


def test_tool_schema_keeps_physical_enums_and_allows_declared_cte_references():
    schema = build_tool_schema(_registry())

    scan = schema["properties"]["scan"]
    assert scan["anyOf"][0]["enum"] == ["orders"]
    assert "CTE" in scan["anyOf"][1]["description"]

    serialized = json.dumps(schema, ensure_ascii=False)
    assert "Alias or CTE output reference" in serialized
    assert "aggregate CTE followed by window + qualify" in serialized


def test_cte_schema_no_longer_contradicts_aggregate_per_group_topn():
    schema = build_tool_schema(_registry())
    description = schema["properties"]["cte"]["description"]

    assert "requires an aggregate CTE" in description
    assert "Do NOT use for ranking/TopN" not in description
