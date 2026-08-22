"""Narrow deterministic Forge JSON completion for unambiguous model omissions."""
from __future__ import annotations

from copy import deepcopy
from typing import Any

_RATIO_ALIAS_TOKENS = ("pct", "ratio", "rate", "share", "占比", "比例")
_DIMENSION_SUFFIXES = ("_id", "_name", "month", "date", "_dt")


def complete_unambiguous_ratio_alias(query: dict[str, Any], question: str) -> dict[str, Any]:
    """Materialize one missing ratio expression when numerator/denominator are unique.

    This is intentionally narrow: it only runs for an explicit ratio request, one
    undefined ratio-like output alias, one SUM window denominator, and one visible
    non-dimension numerator. Ambiguous inputs are returned unchanged and continue
    through normal Assurance rejection/retry.
    """
    result = deepcopy(query)
    if not any(term in question for term in ("占比", "比例")):
        return result

    select = result.get("select", [])
    defined = {
        str(item.get("as"))
        for key in ("agg", "window")
        for item in result.get(key, [])
        if isinstance(item, dict) and item.get("as")
    }
    defined.update(
        str(item.get("as"))
        for item in select
        if isinstance(item, dict) and item.get("as")
    )
    missing_ratio_indexes = [
        index
        for index, item in enumerate(select)
        if isinstance(item, str)
        and "." not in item
        and item not in defined
        and any(token in item.lower() for token in _RATIO_ALIAS_TOKENS)
    ]
    denominators = [
        str(item.get("as"))
        for item in result.get("window", [])
        if isinstance(item, dict)
        and item.get("fn") == "sum"
        and item.get("as")
    ]
    if len(missing_ratio_indexes) != 1 or len(denominators) != 1:
        return result

    ratio_index = missing_ratio_indexes[0]
    ratio_alias = str(select[ratio_index])
    denominator = denominators[0]
    numerators: list[str] = []
    for item in select:
        if not isinstance(item, str) or item == ratio_alias:
            continue
        name = item.rsplit(".", 1)[-1]
        if name == denominator or name.endswith(_DIMENSION_SUFFIXES):
            continue
        numerators.append(item)
    if len(numerators) != 1:
        return result

    numerator = numerators[0]
    select[ratio_index] = {
        "expr": f"ROUND({numerator} * 1.0 / {denominator}, 4)",
        "as": ratio_alias,
    }
    return result
