"""Versioned Canonical Schema drafts, deterministic projections, and CAS publish."""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import threading
from typing import Any, Iterator
from uuid import uuid4

from jsonschema import Draft202012Validator

_CONTRACT_PATH = Path(__file__).parent / "contracts" / "canonical-schema.schema.json"
_LOCK = threading.RLock()
_DANGEROUS_PARTS = {"raw_type", "normalized_type", "nullable", "primary_key", "relationships"}


class RegistryStudioError(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def revision_of(schema: dict[str, Any]) -> str:
    body = deepcopy(schema)
    body["registry_revision"] = "pending"
    return "sha256:" + hashlib.sha256(_canonical(body).encode()).hexdigest()


def migrate_legacy_registry(
    registry: dict[str, Any], *, datasource_id: str = "default", dialect: str = "sqlite"
) -> dict[str, Any]:
    """Enrich the existing Registry shape without breaking runtime readers."""
    tables = registry.get("tables", registry)
    canonical_tables: dict[str, Any] = {}
    for table_name, raw_table in tables.items():
        table = raw_table if isinstance(raw_table, dict) else {"columns": raw_table}
        raw_columns = table.get("columns", {})
        items = raw_columns.items() if isinstance(raw_columns, dict) else ((name, {}) for name in raw_columns)
        columns: dict[str, Any] = {}
        for ordinal, (column_name, raw_column) in enumerate(items):
            column = dict(raw_column) if isinstance(raw_column, dict) else {}
            raw_type = str(column.get("raw_type") or column.get("type") or "")
            normalized = str(column.get("normalized_type") or _normalize_type(raw_type))
            columns[str(column_name)] = {
                **column,
                "ordinal": int(column.get("ordinal", ordinal)),
                "raw_type": raw_type,
                "normalized_type": normalized,
                "nullable": bool(column.get("nullable", True)),
                "primary_key": bool(column.get("primary_key", column.get("pk", False))),
                "unique": bool(column.get("unique", False)),
                "description": str(column.get("description", "")),
            }
        canonical_tables[str(table_name)] = {
            **table,
            "kind": str(table.get("kind", "table")),
            "description": str(table.get("description", "")),
            "business_aliases": list(table.get("business_aliases", [])),
            "tags": list(table.get("tags", [])),
            "columns": columns,
            "constraints": list(table.get("constraints", [])),
            "indexes": list(table.get("indexes", [])),
        }
    fingerprint = hashlib.sha256(_canonical(registry).encode()).hexdigest()
    result = {
        "schema_version": "1.0.0",
        "registry_revision": "pending",
        "datasource": {
            "id": datasource_id,
            "catalog": None,
            "schema": None,
            "dialect": dialect,
        },
        "tables": canonical_tables,
        "relationships": list(registry.get("relationships", [])),
        "metadata": {
            "source_fingerprint": fingerprint,
            "synced_at": _now(),
            "editor": "migration",
            "change_reason": "legacy registry migration",
        },
    }
    result["registry_revision"] = revision_of(result)
    validate_canonical_schema(result)
    return result


def validate_canonical_schema(schema: dict[str, Any]) -> None:
    contract = json.loads(_CONTRACT_PATH.read_text(encoding="utf-8"))
    errors = sorted(Draft202012Validator(contract).iter_errors(schema), key=lambda item: list(item.path))
    if errors:
        path = ".".join(str(item) for item in errors[0].path) or "$"
        raise RegistryStudioError(f"Canonical Schema 校验失败：{path}: {errors[0].message}")
    expected = revision_of(schema)
    if schema.get("registry_revision") != expected:
        raise RegistryStudioError("Canonical Schema registry_revision 与内容不匹配。")


def deterministic_diff(before: Any, after: Any, path: str = "$") -> list[dict[str, Any]]:
    if isinstance(before, dict) and isinstance(after, dict):
        changes: list[dict[str, Any]] = []
        for key in sorted(set(before) | set(after)):
            child = f"{path}.{key}"
            if child in {
                "$.registry_revision",
                "$.metadata.synced_at",
                "$.metadata.editor",
                "$.metadata.change_reason",
            }:
                continue
            if key not in before:
                changes.append(_change("add", child, None, after[key]))
            elif key not in after:
                changes.append(_change("remove", child, before[key], None))
            else:
                changes.extend(deterministic_diff(before[key], after[key], child))
        return changes
    if before != after:
        return [_change("replace", path, before, after)]
    return []


def _change(operation: str, path: str, before: Any, after: Any) -> dict[str, Any]:
    dangerous = operation == "remove" or any(part in path for part in _DANGEROUS_PARTS)
    if path.endswith(".nullable") and before is True and after is False:
        dangerous = True
    return {"operation": operation, "path": path, "before": before, "after": after,
            "risk": "review_required" if dangerous else "safe"}


def render_ddl(schema: dict[str, Any], dialect: str | None = None) -> str:
    dialect = dialect or str(schema["datasource"]["dialect"])
    if dialect not in {"sqlite", "postgresql", "mysql"}:
        raise RegistryStudioError(f"不支持的 DDL 方言：{dialect}")
    statements: list[str] = []
    for table_name, table in schema["tables"].items():
        if table["kind"] == "view":
            statements.append(f"-- VIEW {_quote(table_name, dialect)}: definition unavailable in Canonical Schema")
            continue
        definitions: list[str] = []
        primary: list[str] = []
        for column_name, column in sorted(table["columns"].items(), key=lambda item: item[1]["ordinal"]):
            raw_type = column.get("raw_type") or _ddl_type(column["normalized_type"], dialect)
            definition = f"  {_quote(column_name, dialect)} {raw_type}"
            if not column["nullable"]:
                definition += " NOT NULL"
            if column.get("unique"):
                definition += " UNIQUE"
            if column.get("default") is not None:
                definition += f" DEFAULT {column['default']}"
            definitions.append(definition)
            if column.get("primary_key"):
                primary.append(column_name)
        if primary:
            definitions.append("  PRIMARY KEY (" + ", ".join(_quote(name, dialect) for name in primary) + ")")
        for relationship in schema.get("relationships", []):
            if relationship.get("status") not in {"confirmed", "declared"}:
                continue
            from_table, from_column = str(relationship.get("from", "")).split(".", 1)
            to_table, to_column = str(relationship.get("to", "")).split(".", 1)
            if from_table == table_name:
                definitions.append(
                    "  FOREIGN KEY (" + _quote(from_column, dialect) + ") REFERENCES "
                    + _quote(to_table, dialect) + " (" + _quote(to_column, dialect) + ")"
                )
        statements.append(
            f"CREATE TABLE {_quote(table_name, dialect)} (\n" + ",\n".join(definitions) + "\n);"
        )
    return "\n\n".join(statements)


def er_projection(schema: dict[str, Any]) -> dict[str, Any]:
    nodes = [
        {"id": name, "label": name, "kind": table["kind"], "columns": list(table["columns"])}
        for name, table in schema["tables"].items()
    ]
    edges = [
        item for item in schema["relationships"]
        if item.get("status") == "confirmed"
    ]
    proposals = [
        item for item in schema["relationships"]
        if item.get("status") != "confirmed"
    ]
    return {"nodes": nodes, "edges": edges, "proposals": proposals}


def parse_ddl_draft(ddl: str, *, datasource_id: str, dialect: str) -> dict[str, Any]:
    """Parse a deliberately bounded CREATE TABLE subset into a Canonical Draft."""
    tables: dict[str, Any] = {}
    pattern = re.compile(r"CREATE\s+TABLE\s+[`\"]?([\w]+)[`\"]?\s*\((.*?)\)\s*;", re.I | re.S)
    for match in pattern.finditer(ddl):
        columns: dict[str, Any] = {}
        for ordinal, fragment in enumerate(_split_definitions(match.group(2))):
            part = fragment.strip()
            if re.match(r"^(PRIMARY|FOREIGN|UNIQUE|CONSTRAINT|CHECK)\b", part, re.I):
                continue
            column_match = re.match(r"[`\"]?(\w+)[`\"]?\s+([\w()]+)(.*)$", part, re.I | re.S)
            if column_match is None:
                raise RegistryStudioError(f"不支持的 DDL 字段定义：{part[:120]}")
            name, raw_type, suffix = column_match.groups()
            columns[name] = {
                "ordinal": ordinal,
                "raw_type": raw_type,
                "normalized_type": _normalize_type(raw_type),
                "nullable": "NOT NULL" not in suffix.upper(),
                "primary_key": "PRIMARY KEY" in suffix.upper(),
                "unique": "UNIQUE" in suffix.upper(),
                "description": "",
            }
        tables[match.group(1)] = {
            "kind": "table", "description": "", "business_aliases": [], "tags": [],
            "columns": columns, "constraints": [], "indexes": [],
        }
    if not tables:
        raise RegistryStudioError("DDL 未包含受支持的 CREATE TABLE 语句。")
    result = {
        "schema_version": "1.0.0", "registry_revision": "pending",
        "datasource": {"id": datasource_id, "catalog": None, "schema": None, "dialect": dialect},
        "tables": tables, "relationships": [],
        "metadata": {"source_fingerprint": "ddl-import", "synced_at": _now(),
                     "editor": "ddl-import", "change_reason": "DDL import draft"},
    }
    result["registry_revision"] = revision_of(result)
    validate_canonical_schema(result)
    return result


class RegistryStudioStore:
    def __init__(self, path: str | Path, registry_path: str | Path):
        self.path = Path(path).expanduser().resolve()
        self.registry_path = Path(registry_path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.executescript("""
              CREATE TABLE IF NOT EXISTS registry_revisions(
                revision_id TEXT PRIMARY KEY, parent_revision_id TEXT, schema_json TEXT NOT NULL,
                actor TEXT NOT NULL, reason TEXT NOT NULL, created_at TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS registry_drafts(
                draft_id TEXT PRIMARY KEY, base_revision_id TEXT NOT NULL, schema_json TEXT NOT NULL,
                diff_json TEXT NOT NULL, status TEXT NOT NULL, actor TEXT NOT NULL,
                reason TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS registry_binding(
                singleton INTEGER PRIMARY KEY CHECK(singleton=1), revision_id TEXT NOT NULL, version INTEGER NOT NULL);
              CREATE TABLE IF NOT EXISTS registry_publish_audit(
                audit_id TEXT PRIMARY KEY, action TEXT NOT NULL, from_revision_id TEXT,
                to_revision_id TEXT NOT NULL, binding_version INTEGER NOT NULL,
                actor TEXT NOT NULL, reason TEXT NOT NULL, created_at TEXT NOT NULL);
            """)
        self.path.chmod(0o600)

    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        try:
            yield db
            db.commit()
        finally:
            db.close()

    def bootstrap(self, *, datasource_id: str = "default", dialect: str = "sqlite") -> dict[str, Any]:
        with _LOCK, self._db() as db:
            binding = db.execute("SELECT revision_id,version FROM registry_binding WHERE singleton=1").fetchone()
            if binding is not None:
                return self.get_revision(str(binding["revision_id"]))
            raw = json.loads(self.registry_path.read_text(encoding="utf-8"))
            schema = raw if raw.get("schema_version") == "1.0.0" else migrate_legacy_registry(
                raw, datasource_id=datasource_id, dialect=dialect
            )
            validate_canonical_schema(schema)
            self._insert_revision(db, schema, None, "bootstrap", "initial import")
            db.execute("INSERT INTO registry_binding(singleton,revision_id,version) VALUES(1,?,1)",
                       (schema["registry_revision"],))
            return schema

    def active(self) -> dict[str, Any]:
        with self._db() as db:
            row = db.execute("SELECT revision_id,version FROM registry_binding WHERE singleton=1").fetchone()
        if row is None:
            self.bootstrap()
            return self.active()
        return {"version": row["version"], "schema": self.get_revision(row["revision_id"])}

    def get_revision(self, revision_id: str) -> dict[str, Any]:
        with self._db() as db:
            row = db.execute("SELECT schema_json FROM registry_revisions WHERE revision_id=?", (revision_id,)).fetchone()
        if row is None:
            raise RegistryStudioError("Registry Revision 不存在。")
        return json.loads(row["schema_json"])

    def create_draft(self, schema: dict[str, Any], *, base_revision_id: str, actor: str, reason: str) -> dict[str, Any]:
        candidate = deepcopy(schema)
        candidate["metadata"] = {**candidate["metadata"], "editor": actor, "change_reason": reason}
        candidate["registry_revision"] = revision_of(candidate)
        validate_canonical_schema(candidate)
        before = self.get_revision(base_revision_id)
        diff = deterministic_diff(before, candidate)
        draft_id = "rd_" + uuid4().hex
        now = _now()
        with self._db() as db:
            db.execute("INSERT INTO registry_drafts VALUES(?,?,?,?,?,?,?,?,?)",
                       (draft_id, base_revision_id, _canonical(candidate), _canonical(diff), "draft",
                        actor, reason, now, now))
        return {"draft_id": draft_id, "base_revision_id": base_revision_id, "schema": candidate,
                "diff": diff, "status": "draft"}

    def get_draft(self, draft_id: str) -> dict[str, Any]:
        with self._db() as db:
            row = db.execute("SELECT * FROM registry_drafts WHERE draft_id=?", (draft_id,)).fetchone()
        if row is None:
            raise RegistryStudioError("Registry Draft 不存在。")
        return {"draft_id": row["draft_id"], "base_revision_id": row["base_revision_id"],
                "schema": json.loads(row["schema_json"]), "diff": json.loads(row["diff_json"]),
                "status": row["status"], "actor": row["actor"], "reason": row["reason"]}

    def publish(self, draft_id: str, *, expected_version: int, actor: str) -> dict[str, Any]:
        with _LOCK:
            draft = self.get_draft(draft_id)
            if draft["status"] != "draft":
                raise RegistryStudioError("Registry Draft 已经处理。")
            validate_canonical_schema(draft["schema"])
            with self._db() as db:
                binding = db.execute("SELECT revision_id,version FROM registry_binding WHERE singleton=1").fetchone()
                if binding is None or binding["version"] != expected_version:
                    raise RegistryStudioError("Registry binding version 冲突，请刷新后重试。")
                if binding["revision_id"] != draft["base_revision_id"]:
                    raise RegistryStudioError("Registry Draft base revision 已过期。")
                old_bytes = self.registry_path.read_bytes()
                tmp = self.registry_path.with_suffix(self.registry_path.suffix + ".studio.tmp")
                try:
                    tmp.write_text(json.dumps(draft["schema"], ensure_ascii=False, indent=2), encoding="utf-8")
                    tmp.replace(self.registry_path)
                    self._insert_revision(db, draft["schema"], draft["base_revision_id"], actor, draft["reason"])
                    changed = db.execute(
                        "UPDATE registry_binding SET revision_id=?,version=version+1 WHERE singleton=1 AND version=?",
                        (draft["schema"]["registry_revision"], expected_version),
                    ).rowcount
                    if changed != 1:
                        raise RegistryStudioError("Registry CAS publish 冲突。")
                    db.execute("UPDATE registry_drafts SET status='published',updated_at=? WHERE draft_id=?",
                               (_now(), draft_id))
                    db.execute("INSERT INTO registry_publish_audit VALUES(?,?,?,?,?,?,?,?)", (
                        "ra_" + uuid4().hex, "publish", draft["base_revision_id"],
                        draft["schema"]["registry_revision"], expected_version + 1,
                        actor, draft["reason"], _now(),
                    ))
                except Exception:
                    self.registry_path.write_bytes(old_bytes)
                    tmp.unlink(missing_ok=True)
                    raise
            return self.active()

    def rollback(self, revision_id: str, *, expected_version: int, actor: str, reason: str) -> dict[str, Any]:
        target = self.get_revision(revision_id)
        validate_canonical_schema(target)
        with _LOCK, self._db() as db:
            binding = db.execute("SELECT revision_id,version FROM registry_binding WHERE singleton=1").fetchone()
            if binding is None or binding["version"] != expected_version:
                raise RegistryStudioError("Registry binding version 冲突，请刷新后重试。")
            from_revision = str(binding["revision_id"])
            old_bytes = self.registry_path.read_bytes()
            tmp = self.registry_path.with_suffix(self.registry_path.suffix + ".studio.tmp")
            try:
                tmp.write_text(json.dumps(target, ensure_ascii=False, indent=2), encoding="utf-8")
                tmp.replace(self.registry_path)
                changed = db.execute(
                    "UPDATE registry_binding SET revision_id=?,version=version+1 WHERE singleton=1 AND version=?",
                    (revision_id, expected_version),
                ).rowcount
                if changed != 1:
                    raise RegistryStudioError("Registry CAS rollback 冲突。")
                db.execute("INSERT INTO registry_publish_audit VALUES(?,?,?,?,?,?,?,?)", (
                    "ra_" + uuid4().hex, "rollback", from_revision, revision_id,
                    expected_version + 1, actor, reason, _now(),
                ))
            except Exception:
                self.registry_path.write_bytes(old_bytes)
                tmp.unlink(missing_ok=True)
                raise
        return self.active()

    def history(self) -> list[dict[str, Any]]:
        with self._db() as db:
            rows = db.execute(
                "SELECT revision_id,parent_revision_id,actor,reason,created_at "
                "FROM registry_revisions ORDER BY created_at DESC"
            ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _insert_revision(db: sqlite3.Connection, schema: dict[str, Any], parent: str | None,
                         actor: str, reason: str) -> None:
        db.execute("INSERT OR IGNORE INTO registry_revisions VALUES(?,?,?,?,?,?)",
                   (schema["registry_revision"], parent, _canonical(schema), actor, reason, _now()))


def _normalize_type(raw: str) -> str:
    value = raw.lower()
    if "int" in value: return "integer"
    if any(item in value for item in ("real", "float", "double", "decimal", "numeric")): return "number"
    if "bool" in value: return "boolean"
    if "datetime" in value or "timestamp" in value: return "datetime"
    if value == "date": return "date"
    if any(item in value for item in ("blob", "binary")): return "binary"
    if "json" in value: return "json"
    if any(item in value for item in ("char", "text", "string")): return "string"
    return "unknown"


def _ddl_type(normalized: str, dialect: str) -> str:
    mapping = {"integer": "INTEGER", "number": "REAL" if dialect == "sqlite" else "DOUBLE" if dialect == "mysql" else "DOUBLE PRECISION",
               "boolean": "BOOLEAN", "date": "DATE", "datetime": "TIMESTAMP", "binary": "BLOB",
               "json": "JSON", "string": "TEXT", "unknown": "TEXT"}
    return mapping[normalized]


def _quote(name: str, dialect: str) -> str:
    marker = "`" if dialect == "mysql" else '"'
    return marker + name.replace(marker, marker * 2) + marker


def _split_definitions(body: str) -> list[str]:
    result: list[str] = []
    start = depth = 0
    for index, char in enumerate(body):
        if char == "(": depth += 1
        elif char == ")": depth -= 1
        elif char == "," and depth == 0:
            result.append(body[start:index]); start = index + 1
    result.append(body[start:])
    return result
