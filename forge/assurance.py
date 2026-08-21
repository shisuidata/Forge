"""Unified Forge JSON assurance pipeline used before SQL review."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
import re
from typing import Any

from config import cfg
from forge.compiler import compile_query, validate_query_contract
from forge.executor import validate_readonly_sql
from forge.lint import lint_conventions
from registry.relationships import (
    RegistryRelationship,
    is_fanout_from_existing,
    load_relationships,
)

ASSURANCE_REVISION = "query-assurance-v2"
POLICY_REVISION = "convention-policy-v1"


@dataclass(frozen=True)
class GateResult:
    gate: str
    status: str
    revision: str
    diagnostics: tuple[str, ...] = ()


@dataclass(frozen=True)
class QueryAssuranceReport:
    status: str
    assurance_revision: str
    policy_revision: str
    registry_revision: str
    model_revision: str
    gates: tuple[GateResult, ...]
    sql: str | None = None
    sql_hash: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class QueryAssuranceError(ValueError):
    def __init__(self, report: QueryAssuranceReport):
        self.report = report
        diagnostics = [d for gate in report.gates for d in gate.diagnostics]
        super().__init__(diagnostics[0] if diagnostics else "查询保障校验未通过。")


def assure_query(
    forge_json: dict,
    question: str,
    *,
    dialect: str,
    allowed_tables: list[str] | None = None,
    model_revision: str = "unknown",
) -> QueryAssuranceReport:
    """Run all deterministic pre-review gates and return an immutable report."""
    gates: list[GateResult] = []
    try:
        registry, registry_revision = _load_registry()
    except (FileNotFoundError, OSError, json.JSONDecodeError, TypeError) as exc:
        gates.append(GateResult(
            "contract_registry_acl", "failed", "unavailable",
            ("Registry 不可用或格式错误。",),
        ))
        raise QueryAssuranceError(
            _failed_report(gates, "unavailable", model_revision)
        ) from exc
    scoped_registry = _scope_registry(registry, allowed_tables)

    try:
        normalized = validate_query_contract(forge_json)
        gates.append(GateResult("contract_scope_type", "passed", ASSURANCE_REVISION))
    except ValueError as exc:
        gates.append(GateResult("contract_scope_type", "failed", ASSURANCE_REVISION, (str(exc),)))
        raise QueryAssuranceError(
            _failed_report(gates, registry_revision, model_revision)
        ) from exc

    try:
        _validate_registry_tables(normalized, scoped_registry)
        _validate_registry_fields(normalized, scoped_registry)
        _validate_select_symbols(normalized, scoped_registry)
        gates.append(GateResult("registry_acl_alias", "passed", registry_revision))
    except ValueError as exc:
        gates.append(GateResult("registry_acl_alias", "failed", registry_revision, (str(exc),)))
        raise QueryAssuranceError(
            _failed_report(gates, registry_revision, model_revision)
        ) from exc

    try:
        relationships = load_relationships(scoped_registry)
        _validate_join_relationships(normalized, scoped_registry, relationships)
        gates.append(GateResult("relationship_grain", "passed", registry_revision))
    except ValueError as exc:
        gates.append(GateResult("relationship_grain", "failed", registry_revision, (str(exc),)))
        raise QueryAssuranceError(
            _failed_report(gates, registry_revision, model_revision)
        ) from exc

    warnings = lint_conventions(normalized, question)
    if warnings:
        gates.append(GateResult("convention_policy", "failed", POLICY_REVISION, tuple(warnings)))
        raise QueryAssuranceError(
            _failed_report(gates, registry_revision, model_revision)
        )
    gates.append(GateResult("convention_policy", "passed", POLICY_REVISION))

    try:
        sql = compile_query(normalized, dialect=dialect)
    except Exception as exc:
        gates.append(GateResult("scope_type_compile", "failed", ASSURANCE_REVISION, (str(exc),)))
        raise QueryAssuranceError(
            _failed_report(gates, registry_revision, model_revision)
        ) from exc
    gates.append(GateResult("scope_type_compile", "passed", ASSURANCE_REVISION))

    try:
        validate_readonly_sql(sql)
    except ValueError as exc:
        gates.append(GateResult("sql_safety", "failed", ASSURANCE_REVISION, (str(exc),)))
        raise QueryAssuranceError(
            _failed_report(gates, registry_revision, model_revision)
        ) from exc
    gates.append(GateResult("sql_safety", "passed", ASSURANCE_REVISION))

    return QueryAssuranceReport(
        status="passed",
        assurance_revision=ASSURANCE_REVISION,
        policy_revision=POLICY_REVISION,
        registry_revision=registry_revision,
        model_revision=model_revision,
        gates=tuple(gates),
        sql=sql,
        sql_hash="sha256:" + hashlib.sha256(sql.encode("utf-8")).hexdigest(),
    )


def _load_registry() -> tuple[dict, str]:
    path = Path(cfg.REGISTRY_PATH)
    raw = path.read_bytes()
    registry = json.loads(raw)
    return registry, hashlib.sha256(raw).hexdigest()


def _scope_registry(registry: dict, allowed_tables: list[str] | None) -> dict:
    if allowed_tables is None:
        return registry
    tables = registry.get("tables", registry)
    allowed = set(allowed_tables)
    scoped_tables = {name: value for name, value in tables.items() if name in allowed}
    relationships = [
        item for item in registry.get("relationships", [])
        if isinstance(item, dict)
        and isinstance(item.get("from"), str)
        and isinstance(item.get("to"), str)
        and item["from"].split(".", 1)[0] in allowed
        and item["to"].split(".", 1)[0] in allowed
    ]
    return {"tables": scoped_tables, "relationships": relationships}


def _validate_join_relationships(
    query: dict,
    registry: dict,
    relationships: tuple[RegistryRelationship, ...],
) -> None:
    tables = registry.get("tables", registry)
    physical_tables = set(tables)
    existing_tables = {query.get("scan")} if query.get("scan") in physical_tables else set()
    aggregate_sources = _aggregate_source_tables(query)

    for join in query.get("joins", []):
        joined_table = join.get("table")
        if joined_table not in physical_tables:
            continue
        if join.get("type") == "cross":
            raise ValueError("关系校验失败：物理表不允许未经关系约束的 CROSS JOIN。")
        pairs = _join_field_pairs(join.get("on"))
        matched = False
        fanout = False
        for left, right in pairs:
            left_table = left.split(".", 1)[0]
            right_table = right.split(".", 1)[0]
            if joined_table == left_table and right_table in existing_tables:
                joined_field, existing_field = left, right
            elif joined_table == right_table and left_table in existing_tables:
                joined_field, existing_field = right, left
            else:
                continue
            matched = True
            relationship = _find_relationship(relationships, existing_field, joined_field)
            if relationship is None or not relationship.trusted:
                raise ValueError("关系校验失败：JOIN 未使用数据库声明或人工确认的关系。")
            fanout = fanout or is_fanout_from_existing(
                relationship,
                existing_field=existing_field,
                joined_field=joined_field,
            )
        if not matched:
            raise ValueError("关系校验失败：JOIN 未连接当前查询作用域中的物理表。")
        if (
            fanout
            and join.get("type") not in {"semi", "anti"}
            and (aggregate_sources & existing_tables or "*" in aggregate_sources)
        ):
            raise ValueError("粒度校验失败：JOIN 会放大已有侧聚合度量。")
        existing_tables.add(joined_table)

    for cte in query.get("cte", []):
        _validate_join_relationships(cte["query"], registry, relationships)
        if cte.get("recursive_term"):
            _validate_join_relationships(cte["recursive_term"], registry, relationships)
    for operation in ("union", "intersect", "except"):
        for branch in query.get(operation, []):
            nested = branch.get("query", branch)
            if isinstance(nested, dict):
                _validate_join_relationships(nested, registry, relationships)


def _join_field_pairs(on: Any) -> list[tuple[str, str]]:
    conditions = on if isinstance(on, list) else [on]
    pairs: list[tuple[str, str]] = []
    for condition in conditions:
        if not isinstance(condition, dict):
            continue
        left, right = condition.get("left"), condition.get("right")
        if isinstance(left, str) and isinstance(right, str):
            pairs.append((left, right))
    return pairs


def _find_relationship(
    relationships: tuple[RegistryRelationship, ...],
    left: str,
    right: str,
) -> RegistryRelationship | None:
    edge = frozenset((left, right))
    return next(
        (item for item in relationships if frozenset((item.from_field, item.to_field)) == edge),
        None,
    )


def _aggregate_source_tables(query: dict) -> set[str]:
    result: set[str] = set()
    aggregate_expressions = list(query.get("agg", []))
    aggregate_expressions.extend(
        item for item in query.get("window", [])
        if isinstance(item, dict) and item.get("fn") in {"sum", "avg", "count", "min", "max"}
    )
    for aggregate in aggregate_expressions:
        if not isinstance(aggregate, dict):
            continue
        if aggregate.get("fn") == "count_distinct":
            continue
        col = aggregate.get("col")
        if col is None:
            result.add("*")
        elif isinstance(col, str):
            result.update(table for table, _ in _FIELD_REF_RE.findall(col))
    return result


def _validate_registry_tables(query: dict, registry: dict) -> None:
    tables = registry.get("tables", registry)
    cte_names = {item.get("name") for item in query.get("cte", [])}
    physical_refs = {query.get("scan")}
    physical_refs.update(join.get("table") for join in query.get("joins", []))
    unknown = sorted(
        str(table) for table in physical_refs
        if table not in tables and table not in cte_names
    )
    if unknown:
        raise ValueError("Registry/权限校验失败：查询使用了未授权或不存在的表。")
    for cte in query.get("cte", []):
        _validate_registry_tables(cte["query"], registry)
        if cte.get("recursive_term"):
            _validate_registry_tables(cte["recursive_term"], registry)
    for operation in ("union", "intersect", "except"):
        for branch in query.get(operation, []):
            nested = branch.get("query", branch)
            if isinstance(nested, dict):
                _validate_registry_tables(nested, registry)


_FIELD_REF_RE = re.compile(r"(?<![A-Za-z0-9_])([A-Za-z_]\w*)\.([A-Za-z_]\w*)")
_IGNORED_VALUE_KEYS = {"$date", "$preset", "as", "default", "explain", "hi", "lo", "val"}


def _validate_registry_fields(query: dict, registry: dict) -> None:
    tables = registry.get("tables", registry)
    known = {
        f"{table}.{column}"
        for table, info in tables.items()
        for column in (info.get("columns", info) if isinstance(info, dict) else info)
    }
    cte_names = {item.get("name") for item in query.get("cte", [])}
    references = _collect_field_refs(query)
    unknown = sorted(
        ref for ref in references
        if ref.split(".", 1)[0] not in cte_names and ref not in known
    )
    if unknown:
        raise ValueError("Registry/权限校验失败：查询使用了未授权或不存在的表/字段。")


def _validate_select_symbols(query: dict, registry: dict) -> None:
    """Reject bare SELECT symbols that have no physical or computed source."""
    tables = registry.get("tables", registry)
    table_columns = {
        table: set(info.get("columns", info) if isinstance(info, dict) else info)
        for table, info in tables.items()
    }
    for cte in query.get("cte", []):
        _validate_select_symbols(cte["query"], registry)
        if cte.get("recursive_term"):
            _validate_select_symbols(cte["recursive_term"], registry)
        table_columns[cte["name"]] = _query_output_names(cte["query"])
    for operation in ("union", "intersect", "except"):
        for branch in query.get(operation, []):
            nested = branch.get("query", branch)
            if isinstance(nested, dict):
                _validate_select_symbols(nested, registry)

    computed = {
        item.get("as") for item in query.get("agg", []) + query.get("window", [])
        if isinstance(item, dict) and isinstance(item.get("as"), str)
    }
    computed.update(
        item.get("as") for item in query.get("group", [])
        if isinstance(item, dict) and isinstance(item.get("as"), str)
    )
    scan_columns = table_columns.get(query.get("scan"), set())
    has_joins = bool(query.get("joins"))
    unknown: list[str] = []
    for item in query.get("select", []):
        if not isinstance(item, str) or not re.fullmatch(r"[A-Za-z_]\w*", item):
            continue
        if item in computed:
            continue
        if not has_joins and item in scan_columns:
            continue
        unknown.append(item)
    if unknown:
        raise ValueError(
            "Registry/类型校验失败：SELECT 使用了未定义的字段或计算别名。"
        )


def _query_output_names(query: dict) -> set[str]:
    names: set[str] = set()
    for item in query.get("select", []):
        if isinstance(item, str):
            names.add(item.rsplit(".", 1)[-1])
        elif isinstance(item, dict) and isinstance(item.get("as"), str):
            names.add(item["as"])
    return names


def _collect_field_refs(value: Any, key: str | None = None) -> set[str]:
    if isinstance(value, str):
        if key in _IGNORED_VALUE_KEYS:
            return set()
        return {f"{table}.{column}" for table, column in _FIELD_REF_RE.findall(value)}
    if isinstance(value, list):
        refs: set[str] = set()
        for child in value:
            refs.update(_collect_field_refs(child, key))
        return refs
    if isinstance(value, dict):
        refs = set()
        for child_key, child in value.items():
            refs.update(_collect_field_refs(child, child_key))
        return refs
    return set()


def _failed_report(
    gates: list[GateResult], registry_revision: str, model_revision: str
) -> QueryAssuranceReport:
    return QueryAssuranceReport(
        status="failed",
        assurance_revision=ASSURANCE_REVISION,
        policy_revision=POLICY_REVISION,
        registry_revision=registry_revision,
        model_revision=model_revision,
        gates=tuple(gates),
    )
