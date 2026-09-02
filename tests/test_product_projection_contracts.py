"""Cross-language checks for the Product Projection v1 boundary."""
from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from time import perf_counter
from typing import Any

import pytest
from jsonschema import ValidationError
from jsonschema.validators import validator_for

from agent.contracts import load_contract, validate_contract
from agent.contracts.product_projection_semantics import validate_product_projection

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = json.loads(
    (ROOT / "agent/contracts/product-projection-fixtures.v1.json").read_text(
        encoding="utf-8"
    )
)


def _mutate(value: dict[str, Any], mutation: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(value)
    target: Any = result
    for segment in mutation["path"][:-1]:
        target = target[segment]
    final = mutation["path"][-1]
    if mutation["op"] == "append":
        target[final].append(deepcopy(mutation["value"]))
    else:
        target[final] = deepcopy(mutation["value"])
    return result


def _valid_by_id() -> dict[str, dict[str, Any]]:
    return {
        case["case_id"]: case["value"]
        for cases in FIXTURES["valid"].values()
        for case in cases
    }


def test_product_projection_v1_is_a_valid_json_schema() -> None:
    schema = load_contract("product_projection_v1")
    validator_for(schema).check_schema(schema)
    assert schema["$id"].endswith("product-projection-v1.schema.json")


def test_python_accepts_all_shared_product_projection_fixtures() -> None:
    assert FIXTURES["schema_version"] == 1
    for contract, cases in FIXTURES["valid"].items():
        assert cases, contract
        for candidate in cases:
            validate_contract("product_projection_v1", candidate["value"])
            assert validate_product_projection(contract, candidate["value"]) == []


def test_python_rejects_shared_shape_mutations() -> None:
    valid = _valid_by_id()
    contract_cases = [
        case
        for case in FIXTURES["invalid"]
        if case["expected_code"] == "contract.invalid"
    ]
    assert len(contract_cases) >= 3
    for candidate in contract_cases:
        with pytest.raises(ValidationError):
            validate_contract(
                "product_projection_v1",
                _mutate(valid[candidate["base_case_id"]], candidate["mutation"]),
            )


def test_python_rejects_a_valid_projection_under_the_wrong_contract_name() -> None:
    valid = _valid_by_id()
    assert validate_product_projection(
        "report_summary_v1", valid["action_approve_query_enabled"]
    ) == ["contract.invalid"]


def test_python_semantics_match_all_shared_negative_reason_codes() -> None:
    valid = _valid_by_id()
    assert len(FIXTURES["invalid"]) >= 15
    for candidate in FIXTURES["invalid"]:
        errors = validate_product_projection(
            candidate["contract"],
            _mutate(valid[candidate["base_case_id"]], candidate["mutation"]),
        )
        assert candidate["expected_code"] in errors, (
            candidate["case_id"],
            candidate["expected_code"],
            errors,
        )


def test_bulk_product_projection_validation_stays_within_page_budget() -> None:
    valid = _valid_by_id()
    task = valid["task_summary_needs_input"]
    report = valid["report_published"]

    started = perf_counter()
    for _ in range(100):
        assert validate_product_projection("task_summary_v1", task) == []
    for _ in range(50):
        assert validate_product_projection("report_summary_v1", report) == []

    assert perf_counter() - started < 2.0
