"""Versioned JSON contracts shared by the Pi orchestrator and Forge.

The contracts live next to this module so they remain available when Forge is
installed as a Python package. Call :func:`validate_contract` at every service
or stage boundary before accepting an artifact.
"""
from __future__ import annotations

import json
from functools import lru_cache
from importlib.resources import files
from typing import Any

from jsonschema import FormatChecker
from jsonschema.validators import validator_for

_CONTRACT_FILES = {
    "task_run": "task-run.schema.json",
    "clarification_artifact": "clarification-artifact.schema.json",
    "execution_plan_artifact": "execution-plan-artifact.schema.json",
    "chart_artifact": "chart-artifact.schema.json",
    "chart_artifact_v2": "chart-artifact-v2.schema.json",
    "technical_report_artifact": "technical-report-artifact.schema.json",
    "report_bundle_artifact": "report-bundle-artifact.schema.json",
    "publication_artifact": "publication-artifact.schema.json",
    "metric_definition_artifact": "metric-definition-artifact.schema.json",
    "query_result_artifact": "query-result-artifact.schema.json",
    "analysis_artifact": "analysis-artifact.schema.json",
    "advisory_artifact": "advisory-artifact.schema.json",
    "rendered_output_artifact": "rendered-output-artifact.schema.json",
    "resource_ref_v1": "resource-ref-v1.schema.json",
    "principal_context_v1": "principal-context-v1.schema.json",
    "delegated_mandate_v1": "delegated-mandate-v1.schema.json",
    "policy_decision_v1": "policy-decision-v1.schema.json",
    "datasource_binding_v1": "datasource-binding-v1.schema.json",
    "registry_binding_v1": "registry-binding-v1.schema.json",
    "governance_action_catalog_v1": "governance-action-catalog-v1.schema.json",
    "benchmark_failure_v1": "benchmark-failure-v1.schema.json",
    "query_candidate_v1": "query-candidate-v1.schema.json",
    "evaluation_suite_v1": "evaluation-suite-v1.schema.json",
    "evaluation_run_manifest_v1": "evaluation-run-manifest-v1.schema.json",
    "enforce_query_request_v1": "enforce-query-request-v1.schema.json",
    "enforce_query_approval_v1": "enforce-query-approval-v1.schema.json",
    "enforce_query_response_v1": "enforce-query-response-v1.schema.json",
    "explain_query_response_v1": "explain-query-response-v1.schema.json",
    "product_projection_v1": "product-projection-v1.schema.json",
}


def contract_names() -> tuple[str, ...]:
    """Return the stable public names of all available contracts."""
    return tuple(_CONTRACT_FILES)


@lru_cache(maxsize=None)
def load_contract(name: str) -> dict[str, Any]:
    """Load a contract by public name.

    Raises:
        ValueError: if ``name`` is not a registered contract.
    """
    try:
        filename = _CONTRACT_FILES[name]
    except KeyError as exc:
        available = ", ".join(_CONTRACT_FILES)
        raise ValueError(f"Unknown contract {name!r}; available: {available}") from exc

    resource = files(__package__).joinpath(filename)
    return json.loads(resource.read_text(encoding="utf-8"))


@lru_cache(maxsize=None)
def _compiled_validator(name: str) -> Any:
    schema = load_contract(name)
    validator_class = validator_for(schema)
    validator_class.check_schema(schema)
    return validator_class(schema, format_checker=FormatChecker())


def validate_contract(name: str, instance: Any) -> None:
    """Validate an instance against a versioned contract.

    ``jsonschema.ValidationError`` is intentionally allowed to propagate so API
    and orchestration layers can translate it into their own bounded error type.
    """
    _compiled_validator(name).validate(instance)
