"""M0 Governance Contract and Action Catalog tests."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import ValidationError

from agent.contracts import validate_contract

CONTRACTS_DIR = Path(__file__).resolve().parents[1] / "agent" / "contracts"
FIXTURES_PATH = CONTRACTS_DIR / "governance-contract-fixtures.v1.json"
CATALOG_PATH = CONTRACTS_DIR / "governance-action-catalog.v1.json"

GOVERNANCE_CONTRACTS = {
    "resource_ref_v1",
    "principal_context_v1",
    "agent_mandate_v1",
    "policy_decision_v1",
    "datasource_binding_v1",
    "registry_binding_v1",
}

REQUIRED_ACTIONS = {
    "query.prepare",
    "query.approve",
    "query.execute",
    "query.cancel",
    "registry.publish",
    "registry.rollback",
    "model.activate",
    "model.rollback",
    "skill_policy.update",
    "report.read",
    "report.share",
    "report.export",
    "memory_proposal.confirm",
    "memory_proposal.forget",
}


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_governance_fixture_corpus_matches_json_schema() -> None:
    fixtures = _load(FIXTURES_PATH)
    assert set(fixtures["valid"]) == GOVERNANCE_CONTRACTS
    assert set(fixtures["invalid"]) == GOVERNANCE_CONTRACTS

    for contract_name, instances in fixtures["valid"].items():
        assert instances, f"{contract_name} must have at least one valid fixture"
        for instance in instances:
            validate_contract(contract_name, instance)

    for contract_name, cases in fixtures["invalid"].items():
        assert cases, f"{contract_name} must have at least one invalid fixture"
        for case in cases:
            with pytest.raises(ValidationError):
                validate_contract(contract_name, case["value"])


def test_actor_and_final_accountability_are_distinct() -> None:
    fixtures = _load(FIXTURES_PATH)
    contexts = fixtures["valid"]["principal_context_v1"]
    agent_context = next(
        context for context in contexts
        if context["actor_principal"]["principal_type"] == "agent"
    )
    assert agent_context["accountable_principal"]["principal_type"] in {
        "human", "team", "organization"
    }
    assert agent_context["actor_principal"]["principal_id"] != agent_context["accountable_principal"]["principal_id"]


def test_action_catalog_is_complete_unique_and_fully_governed() -> None:
    catalog = _load(CATALOG_PATH)
    validate_contract("governance_action_catalog_v1", catalog)

    actions = catalog["actions"]
    action_names = [action["action"] for action in actions]
    assert len(action_names) == len(set(action_names))
    assert set(action_names) == REQUIRED_ACTIONS

    supported = [action for action in actions if action["support_status"] == "supported"]
    governed = [action for action in supported if action["governed"]]
    assert supported
    assert len(governed) / len(supported) == 1.0
    assert catalog["unsupported_high_risk_behavior"] == "fail_closed"

    for action in supported:
        assert action["required_context"]["principal"] is True
        if action["risk_level"] == "high":
            assert action["required_context"]["policy_decision"] == "required"
            assert action["required_context"]["human_decision"] == "required"
            assert action["failure_policy"] == "deny"


def test_governance_boundaries_do_not_define_secret_fields() -> None:
    forbidden_names = {
        "token", "access_token", "refresh_token", "api_key", "password",
        "database_password", "database_url", "secret", "secret_ref",
    }
    fixtures = _load(FIXTURES_PATH)
    documents = [fixtures["valid"], _load(CATALOG_PATH)]
    documents.extend(
        _load(CONTRACTS_DIR / filename)
        for filename in (
            "resource-ref-v1.schema.json",
            "principal-context-v1.schema.json",
            "agent-mandate-v1.schema.json",
            "policy-decision-v1.schema.json",
            "datasource-binding-v1.schema.json",
            "registry-binding-v1.schema.json",
            "governance-action-catalog-v1.schema.json",
        )
    )

    def visit(value: object) -> None:
        if isinstance(value, dict):
            assert forbidden_names.isdisjoint(value)
            for nested in value.values():
                visit(nested)
        elif isinstance(value, list):
            for nested in value:
                visit(nested)

    for document in documents:
        visit(document)
