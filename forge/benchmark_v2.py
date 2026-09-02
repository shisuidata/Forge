"""Pi-native benchmark support: bounded retrieval, immutable context, and result evaluation."""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
import hashlib
import itertools
import json
import math
import re
from typing import Any


_TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|[\u4e00-\u9fff]{1,8}")
_ORDER_PATTERNS = (
    re.compile(r"\b(?:order(?:ed)?|sort(?:ed)?)\s+by\b", re.IGNORECASE),
    re.compile(r"\bin\s+(?:ascending|descending)\s+order\b", re.IGNORECASE),
    re.compile(r"\b(?:ascending|descending)\b", re.IGNORECASE),
    re.compile(r"\b(?:top|bottom)\s+\d+\b", re.IGNORECASE),
    re.compile(r"(?:按.+(?:排序|升序|降序)|(?:前|后)\s*\d+|升序|降序)"),
)
_ROUND_PATTERNS = (
    re.compile(r"\bround(?:ed|ing)?\b", re.IGNORECASE),
    re.compile(r"\b(?:decimal places?|approximately)\b", re.IGNORECASE),
    re.compile(r"(?:四舍五入|\d+\s*位小数|近似值)"),
)


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _tokens(text: str) -> set[str]:
    return {match.group(0).lower() for match in _TOKEN_RE.finditer(text)}


@dataclass(frozen=True)
class ResultContract:
    required_output_semantics: tuple[str, ...]
    column_order_significant: bool
    row_order_significant: bool
    duplicate_policy: str
    numeric_mode: str
    numeric_scale: int | None
    null_policy: str
    expected_grain: str
    revision: str


@dataclass(frozen=True)
class RetrievalRound:
    round_index: int
    top_k: int
    selected_tables: tuple[str, ...]
    selected_fields: tuple[str, ...]
    relationship_paths: tuple[str, ...]
    concept_coverage: float
    join_connected: bool
    sufficient: bool


@dataclass(frozen=True)
class ContextSnapshot:
    question: str
    evidence: str
    question_concepts: tuple[str, ...]
    tables: tuple[str, ...]
    fields: tuple[str, ...]
    relationships: tuple[str, ...]
    retrieval_rounds: tuple[RetrievalRound, ...]
    sufficiency_status: str
    result_contract: ResultContract
    content_hash: str


def build_result_contract(question: str, evidence: str = "") -> ResultContract:
    lowered = question.lower()
    semantics = tuple(sorted(_tokens(question) | _tokens(evidence)))
    row_order = any(pattern.search(question) for pattern in _ORDER_PATTERNS)
    numeric_mode = "rounded" if any(pattern.search(question) for pattern in _ROUND_PATTERNS) else "exact"
    scale_match = re.search(r"(\d+)\s*(?:decimal places?|位小数)", lowered)
    scale = int(scale_match.group(1)) if scale_match else None
    grain = "grouped" if re.search(r"(?:\b(?:each|per)\b|\bgrouped?\s+by\b|每个|各(?:个|类|项)?|按.+(?:分组|统计))", lowered) else "scalar_or_detail"
    body = {
        "semantics": semantics,
        "column_order": False,
        "row_order": row_order,
        "duplicate_policy": "multiset",
        "numeric_mode": numeric_mode,
        "numeric_scale": scale,
        "null_policy": "exact",
        "expected_grain": grain,
    }
    revision = "sha256:" + hashlib.sha256(_canonical(body).encode()).hexdigest()
    return ResultContract(
        required_output_semantics=semantics,
        column_order_significant=False,
        row_order_significant=row_order,
        duplicate_policy="multiset",
        numeric_mode=numeric_mode,
        numeric_scale=scale,
        null_policy="exact",
        expected_grain=grain,
        revision=revision,
    )


def _normalize_value(value: Any, contract: ResultContract) -> Any:
    if isinstance(value, float) and contract.numeric_mode == "rounded" and contract.numeric_scale is not None:
        return round(value, contract.numeric_scale)
    return value


def _normalize_rows(rows: list[tuple[Any, ...]], contract: ResultContract) -> list[tuple[Any, ...]]:
    return [tuple(_normalize_value(value, contract) for value in row) for row in rows]


def _column_fingerprint(rows: list[tuple[Any, ...]], index: int, contract: ResultContract) -> tuple[tuple[str, int], ...]:
    values = Counter(_canonical(_normalize_value(row[index], contract)) for row in rows)
    return tuple(sorted(values.items()))


def _column_mapping(
    gold_rows: list[tuple[Any, ...]],
    predicted_rows: list[tuple[Any, ...]],
    contract: ResultContract,
) -> tuple[int, ...] | None:
    if not gold_rows and not predicted_rows:
        return ()
    width = len(gold_rows[0] if gold_rows else predicted_rows[0])
    if width > 8:
        return tuple(range(width))
    gold_fingerprints = [_column_fingerprint(gold_rows, index, contract) for index in range(width)]
    predicted_fingerprints = [_column_fingerprint(predicted_rows, index, contract) for index in range(width)]
    candidates = [
        [index for index, fingerprint in enumerate(predicted_fingerprints) if fingerprint == gold_fingerprint]
        for gold_fingerprint in gold_fingerprints
    ]
    if any(not items for items in candidates):
        return None
    mappings: list[tuple[int, ...]] = []
    for mapping in itertools.product(*candidates):
        if len(set(mapping)) == width:
            mappings.append(tuple(mapping))
            if len(mappings) > 1:
                return None
    return mappings[0] if mappings else None


def semantic_result_compare(
    gold_rows: list[tuple[Any, ...]],
    predicted_rows: list[tuple[Any, ...]],
    contract: ResultContract,
) -> dict[str, Any]:
    if len(gold_rows) != len(predicted_rows):
        return {
            "correct": False,
            "verdict": "row_count_mismatch",
            "column_mapping": None,
            "failure_code": "result_row_count_mismatch",
        }
    gold_width = len(gold_rows[0]) if gold_rows else 0
    predicted_width = len(predicted_rows[0]) if predicted_rows else 0
    if gold_width != predicted_width or any(len(row) != gold_width for row in gold_rows + predicted_rows):
        return {
            "correct": False,
            "verdict": "column_count_mismatch",
            "column_mapping": None,
            "failure_code": "result_column_count_mismatch",
        }
    mapping = tuple(range(gold_width))
    if not contract.column_order_significant:
        resolved = _column_mapping(gold_rows, predicted_rows, contract)
        if resolved is None:
            return {
                "correct": False,
                "verdict": "column_alignment_ambiguous",
                "column_mapping": None,
                "failure_code": "result_column_alignment_ambiguous",
            }
        mapping = resolved
    aligned = [tuple(row[index] for index in mapping) for row in predicted_rows]
    gold = _normalize_rows(gold_rows, contract)
    predicted = _normalize_rows(aligned, contract)
    if contract.row_order_significant:
        correct = gold == predicted
        verdict = "ordered_equal" if correct else "row_order_or_value_mismatch"
        failure_code = None if correct else "result_order_or_value_mismatch"
    elif contract.duplicate_policy == "multiset":
        correct = Counter(gold) == Counter(predicted)
        verdict = "multiset_equal" if correct else "multiset_mismatch"
        failure_code = None if correct else "result_value_mismatch"
    else:
        correct = set(gold) == set(predicted)
        verdict = "set_equal" if correct else "set_mismatch"
        failure_code = None if correct else "result_value_mismatch"
    return {
        "correct": correct,
        "verdict": verdict,
        "column_mapping": list(mapping),
        "failure_code": failure_code,
    }


def _field_records(structure: dict[str, Any]) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for table in structure.get("tables", []):
        table_name = str(table.get("name", ""))
        for column in table.get("columns", []):
            records.append({
                "table": table_name,
                "field": str(column.get("name", "")),
                "text": " ".join(
                    str(value) for value in (
                        table_name,
                        column.get("name", ""),
                        column.get("description", ""),
                        column.get("values", ""),
                    ) if value
                ),
            })
    return records


def _score(text_tokens: set[str], candidate: str) -> float:
    candidate_tokens = _tokens(candidate)
    if not candidate_tokens:
        return 0.0
    overlap = len(text_tokens & candidate_tokens)
    return overlap / math.sqrt(max(1, len(candidate_tokens)))


def _relationships_connect(
    relevant_tables: set[str],
    chosen_tables: set[str],
    relationships: list[tuple[str, str]],
) -> bool:
    if len(relevant_tables) <= 1:
        return True
    graph: dict[str, set[str]] = defaultdict(set)
    for left, right in relationships:
        left_table = left.split(".", 1)[0]
        right_table = right.split(".", 1)[0]
        if left_table in chosen_tables and right_table in chosen_tables:
            graph[left_table].add(right_table)
            graph[right_table].add(left_table)
    pending = [next(iter(relevant_tables))]
    visited: set[str] = set()
    while pending:
        table = pending.pop()
        if table in visited:
            continue
        visited.add(table)
        pending.extend(graph[table] - visited)
    return relevant_tables.issubset(visited)


def build_context_snapshot(
    question: str,
    evidence: str,
    structure: dict[str, Any],
    *,
    top_k_rounds: tuple[int, ...] = (5, 10, 20),
) -> ContextSnapshot:
    if (
        not top_k_rounds
        or any(top_k <= 0 for top_k in top_k_rounds)
        or tuple(sorted(set(top_k_rounds))) != top_k_rounds
    ):
        raise ValueError("top_k_rounds must contain unique, increasing positive budgets")
    query_tokens = _tokens(question + " " + evidence)
    concepts = tuple(sorted(query_tokens))
    fields = _field_records(structure)
    table_scores: dict[str, float] = defaultdict(float)
    field_scores: list[tuple[float, str, str]] = []
    for record in fields:
        score = _score(query_tokens, record["text"])
        table_scores[record["table"]] = max(table_scores[record["table"]], score)
        field_scores.append((score, record["table"], record["field"]))
    relationships = [
        (str(item.get("from", "")), str(item.get("to", "")))
        for item in structure.get("relationships", [])
    ]
    ranked_tables = sorted(table_scores, key=lambda table: (-table_scores[table], table))
    full_schema_fits = bool(ranked_tables) and len(ranked_tables) <= max(top_k_rounds)
    round_budgets = (len(ranked_tables),) if full_schema_fits else top_k_rounds
    rounds: list[RetrievalRound] = []
    selected_tables: tuple[str, ...] = ()
    selected_fields: tuple[str, ...] = ()
    selected_relationships: tuple[str, ...] = ()
    for round_index, top_k in enumerate(round_budgets, start=1):
        chosen = set(ranked_tables if full_schema_fits else ranked_tables[: min(top_k, len(ranked_tables))])
        chosen_fields = [
            f"{table}.{field}" for score, table, field in sorted(field_scores, reverse=True)
            if table in chosen and score > 0
        ]
        selected_tables = tuple(sorted(chosen))
        selected_fields = tuple(dict.fromkeys(chosen_fields[: max(top_k * 4, 12)]))
        selected_relationships = tuple(
            f"{left} -> {right}" for left, right in relationships
            if left.split(".", 1)[0] in chosen and right.split(".", 1)[0] in chosen
        )
        covered: set[str] = set()
        for record in fields:
            if record["table"] in chosen:
                covered |= query_tokens & _tokens(record["text"])
        concept_coverage = len(covered) / max(1, len(query_tokens))
        relevant_tables = {
            table for table in chosen if table_scores.get(table, 0.0) > 0
        }
        join_connected = _relationships_connect(relevant_tables, chosen, relationships)
        sufficient = full_schema_fits or (
            bool(selected_fields) and join_connected and concept_coverage >= 0.18
        )
        rounds.append(RetrievalRound(
            round_index=round_index,
            top_k=top_k,
            selected_tables=selected_tables,
            selected_fields=selected_fields,
            relationship_paths=selected_relationships,
            concept_coverage=round(concept_coverage, 4),
            join_connected=join_connected,
            sufficient=sufficient,
        ))
    status = "sufficient" if rounds[-1].sufficient else "retrieval_insufficient"
    contract = build_result_contract(question, evidence)
    body = {
        "question": question,
        "evidence": evidence,
        "concepts": concepts,
        "tables": selected_tables,
        "fields": selected_fields,
        "relationships": selected_relationships,
        "rounds": [asdict(item) for item in rounds],
        "status": status,
        "result_contract": asdict(contract),
    }
    content_hash = "sha256:" + hashlib.sha256(_canonical(body).encode()).hexdigest()
    return ContextSnapshot(
        question=question,
        evidence=evidence,
        question_concepts=concepts,
        tables=selected_tables,
        fields=selected_fields,
        relationships=selected_relationships,
        retrieval_rounds=tuple(rounds),
        sufficiency_status=status,
        result_contract=contract,
        content_hash=content_hash,
    )


def snapshot_dict(snapshot: ContextSnapshot) -> dict[str, Any]:
    return asdict(snapshot)
