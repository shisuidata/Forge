"""Durable model profile revisions, validation state, and CAS activation."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
from typing import Any

MODEL_SCOPE_QUERY_PLANNING = "forge.query_planning"
_ALLOWED_PROVIDERS = {"openai", "anthropic"}
_ALLOWED_PROTOCOLS = {"openai_chat", "anthropic_messages"}
_ALLOWED_TOOL_CHOICES = {"auto", "required", "named"}


class ModelControlError(ValueError):
    pass


class ModelBindingConflictError(ModelControlError):
    pass


@dataclass(frozen=True)
class ActiveModelRevision:
    scope: str
    binding_version: int
    revision_id: str
    profile_id: str
    config: dict[str, Any]
    validation_report: dict[str, Any]


_SCHEMA_LOCK = threading.RLock()
_SCHEMA_READY: set[Path] = set()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _connect(path: Path) -> sqlite3.Connection:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with _SCHEMA_LOCK:
        if path not in _SCHEMA_READY:
            db = sqlite3.connect(path)
            try:
                db.executescript("""
                    PRAGMA journal_mode=WAL;
                    PRAGMA busy_timeout=5000;
                    CREATE TABLE IF NOT EXISTS model_profiles (
                        profile_id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS model_profile_revisions (
                        revision_id TEXT PRIMARY KEY,
                        profile_id TEXT NOT NULL,
                        config_json TEXT NOT NULL,
                        validation_status TEXT NOT NULL DEFAULT 'pending',
                        validation_report_json TEXT NOT NULL DEFAULT '{}',
                        created_at TEXT NOT NULL,
                        validated_at TEXT,
                        FOREIGN KEY(profile_id) REFERENCES model_profiles(profile_id)
                    );
                    CREATE TABLE IF NOT EXISTS active_model_bindings (
                        scope TEXT PRIMARY KEY,
                        revision_id TEXT NOT NULL,
                        previous_revision_id TEXT,
                        binding_version INTEGER NOT NULL,
                        updated_at TEXT NOT NULL,
                        FOREIGN KEY(revision_id) REFERENCES model_profile_revisions(revision_id)
                    );
                    CREATE TABLE IF NOT EXISTS model_switch_audit (
                        audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        scope TEXT NOT NULL,
                        action TEXT NOT NULL,
                        from_revision_id TEXT,
                        to_revision_id TEXT NOT NULL,
                        binding_version INTEGER NOT NULL,
                        actor TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );
                """)
                db.commit()
            finally:
                db.close()
            path.chmod(0o600)
            _SCHEMA_READY.add(path)
    db = sqlite3.connect(path, timeout=5)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")
    db.execute("PRAGMA foreign_keys=ON")
    return db


def _validate_config(config: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "provider", "protocol", "base_url", "model", "tool_choice",
        "timeout_seconds", "secret_ref", "capabilities",
    }
    if set(config) - allowed:
        raise ModelControlError("Model Profile 包含不支持的配置字段。")
    provider = str(config.get("provider", "")).strip().lower()
    protocol = str(config.get("protocol", "")).strip().lower()
    model = str(config.get("model", "")).strip()
    base_url = str(config.get("base_url", "")).strip().rstrip("/")
    tool_choice = str(config.get("tool_choice", "required")).strip().lower()
    secret_ref = str(config.get("secret_ref", "")).strip()
    if provider not in _ALLOWED_PROVIDERS:
        raise ModelControlError("Model Profile Provider 不受支持。")
    expected_protocol = "openai_chat" if provider == "openai" else "anthropic_messages"
    if protocol != expected_protocol:
        raise ModelControlError("Provider 与协议不兼容。")
    if not model:
        raise ModelControlError("Model Profile 缺少模型名称。")
    if tool_choice not in _ALLOWED_TOOL_CHOICES:
        raise ModelControlError("Model Profile tool_choice 不受支持。")
    if not (secret_ref.startswith("env:") or secret_ref.startswith("file:")):
        raise ModelControlError("Model Profile secret_ref 只支持 env: 或 file:。")
    try:
        timeout_seconds = max(1.0, float(config.get("timeout_seconds", 120)))
    except (TypeError, ValueError) as exc:
        raise ModelControlError("Model Profile timeout_seconds 必须是数字。") from exc
    capabilities = config.get("capabilities", {})
    if not isinstance(capabilities, dict):
        raise ModelControlError("Model Profile capabilities 必须是对象。")
    return {
        "provider": provider,
        "protocol": protocol,
        "base_url": base_url,
        "model": model,
        "tool_choice": tool_choice,
        "timeout_seconds": timeout_seconds,
        "secret_ref": secret_ref,
        "capabilities": capabilities,
    }


class ModelControlStore:
    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().resolve()

    def create_revision(
        self,
        *,
        profile_id: str,
        name: str,
        config: dict[str, Any],
    ) -> str:
        if not profile_id.strip() or not name.strip():
            raise ModelControlError("Model Profile ID 和名称不能为空。")
        normalized = _validate_config(config)
        revision_input = _canonical_json({"profile_id": profile_id, "config": normalized})
        revision_id = "sha256:" + hashlib.sha256(revision_input.encode()).hexdigest()
        now = _now()
        with _connect(self.path) as db:
            db.execute(
                "INSERT INTO model_profiles(profile_id,name,created_at,updated_at) VALUES(?,?,?,?) "
                "ON CONFLICT(profile_id) DO UPDATE SET name=excluded.name,updated_at=excluded.updated_at",
                (profile_id, name, now, now),
            )
            db.execute(
                "INSERT OR IGNORE INTO model_profile_revisions"
                "(revision_id,profile_id,config_json,created_at) VALUES(?,?,?,?)",
                (revision_id, profile_id, _canonical_json(normalized), now),
            )
        return revision_id

    def get_revision(self, revision_id: str) -> dict[str, Any] | None:
        with _connect(self.path) as db:
            row = db.execute(
                "SELECT * FROM model_profile_revisions WHERE revision_id=?",
                (revision_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "revision_id": row["revision_id"],
            "profile_id": row["profile_id"],
            "config": json.loads(row["config_json"]),
            "validation_status": row["validation_status"],
            "validation_report": json.loads(row["validation_report_json"]),
            "created_at": row["created_at"],
            "validated_at": row["validated_at"],
        }

    def record_validation(
        self,
        revision_id: str,
        *,
        passed: bool,
        report: dict[str, Any],
    ) -> None:
        safe_report = {
            key: value for key, value in report.items()
            if key not in {"api_key", "secret", "secret_value"}
        }
        with _connect(self.path) as db:
            updated = db.execute(
                "UPDATE model_profile_revisions SET validation_status=?,"
                "validation_report_json=?,validated_at=? WHERE revision_id=? "
                "AND NOT EXISTS(SELECT 1 FROM active_model_bindings WHERE revision_id=?)",
                (
                    "passed" if passed else "failed",
                    _canonical_json(safe_report),
                    _now(),
                    revision_id,
                    revision_id,
                ),
            ).rowcount
            exists = db.execute(
                "SELECT 1 FROM model_profile_revisions WHERE revision_id=?",
                (revision_id,),
            ).fetchone()
        if updated != 1:
            if exists is None:
                raise ModelControlError("Model Profile Revision 不存在。")
            raise ModelControlError("Active Model Profile Revision 不允许重新验证。")

    def get_active(self, scope: str = MODEL_SCOPE_QUERY_PLANNING) -> ActiveModelRevision | None:
        with _connect(self.path) as db:
            row = db.execute(
                "SELECT b.scope,b.binding_version,r.* FROM active_model_bindings b "
                "JOIN model_profile_revisions r ON r.revision_id=b.revision_id WHERE b.scope=?",
                (scope,),
            ).fetchone()
        if row is None:
            return None
        return ActiveModelRevision(
            scope=row["scope"],
            binding_version=row["binding_version"],
            revision_id=row["revision_id"],
            profile_id=row["profile_id"],
            config=json.loads(row["config_json"]),
            validation_report=json.loads(row["validation_report_json"]),
        )

    def activate(
        self,
        revision_id: str,
        *,
        expected_version: int,
        actor: str,
        scope: str = MODEL_SCOPE_QUERY_PLANNING,
        action: str = "activate",
    ) -> int:
        with _connect(self.path) as db:
            db.execute("BEGIN IMMEDIATE")
            revision = db.execute(
                "SELECT validation_status FROM model_profile_revisions WHERE revision_id=?",
                (revision_id,),
            ).fetchone()
            if revision is None:
                raise ModelControlError("Model Profile Revision 不存在。")
            if revision["validation_status"] != "passed":
                raise ModelControlError("Model Profile Revision 尚未通过验证。")
            validation_report = json.loads(db.execute(
                "SELECT validation_report_json FROM model_profile_revisions WHERE revision_id=?",
                (revision_id,),
            ).fetchone()["validation_report_json"])
            if validation_report.get("quality_gate", {}).get("passed") is not True:
                raise ModelControlError("Model Profile Revision 尚未通过质量与性能门禁。")
            current = db.execute(
                "SELECT revision_id,binding_version FROM active_model_bindings WHERE scope=?",
                (scope,),
            ).fetchone()
            current_version = current["binding_version"] if current else 0
            if current_version != expected_version:
                raise ModelBindingConflictError("Active Model Binding 已变化，请刷新后重试。")
            if current is not None and current["revision_id"] == revision_id:
                raise ModelControlError("该 Model Profile Revision 已经处于激活状态。")
            next_version = current_version + 1
            previous = current["revision_id"] if current else None
            db.execute(
                "INSERT INTO active_model_bindings(scope,revision_id,previous_revision_id,binding_version,updated_at) "
                "VALUES(?,?,?,?,?) ON CONFLICT(scope) DO UPDATE SET "
                "revision_id=excluded.revision_id,previous_revision_id=excluded.previous_revision_id,"
                "binding_version=excluded.binding_version,updated_at=excluded.updated_at",
                (scope, revision_id, previous, next_version, _now()),
            )
            db.execute(
                "INSERT INTO model_switch_audit(scope,action,from_revision_id,to_revision_id,"
                "binding_version,actor,created_at) VALUES(?,?,?,?,?,?,?)",
                (scope, action, previous, revision_id, next_version, actor, _now()),
            )
        return next_version

    def rollback(
        self,
        *,
        expected_version: int,
        actor: str,
        scope: str = MODEL_SCOPE_QUERY_PLANNING,
    ) -> int:
        with _connect(self.path) as db:
            current = db.execute(
                "SELECT previous_revision_id,binding_version FROM active_model_bindings WHERE scope=?",
                (scope,),
            ).fetchone()
        if current is None or not current["previous_revision_id"]:
            raise ModelControlError("当前 Model Binding 没有可回滚 Revision。")
        if current["binding_version"] != expected_version:
            raise ModelBindingConflictError("Active Model Binding 已变化，请刷新后重试。")
        return self.activate(
            current["previous_revision_id"],
            expected_version=expected_version,
            actor=actor,
            scope=scope,
            action="rollback",
        )

    def list_audit(self, scope: str = MODEL_SCOPE_QUERY_PLANNING) -> list[dict[str, Any]]:
        with _connect(self.path) as db:
            rows = db.execute(
                "SELECT * FROM model_switch_audit WHERE scope=? ORDER BY audit_id",
                (scope,),
            ).fetchall()
        return [dict(row) for row in rows]
