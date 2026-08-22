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
    "technical_report_artifact": "technical-report-artifact.schema.json",
    "report_bundle_artifact": "report-bundle-artifact.schema.json",
    "publication_artifact": "publication-artifact.schema.json",
    "metric_definition_artifact": "metric-definition-artifact.schema.json",
    "query_result_artifact": "query-result-artifact.schema.json",
    "analysis_artifact": "analysis-artifact.schema.json",
    "advisory_artifact": "advisory-artifact.schema.json",
    "rendered_output_artifact": "rendered-output-artifact.schema.json",
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


def validate_contract(name: str, instance: Any) -> None:
    """Validate an instance against a versioned contract.

    ``jsonschema.ValidationError`` is intentionally allowed to propagate so API
    and orchestration layers can translate it into their own bounded error type.
    """
    schema = load_contract(name)
    validator_class = validator_for(schema)
    validator_class.check_schema(schema)
    validator_class(schema, format_checker=FormatChecker()).validate(instance)
