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
import uuid

MODEL_SCOPE_QUERY_PLANNING = "forge.query_planning"  # compatibility alias for query_generation
MODEL_STAGE_SCOPES = {
    "intent_router": "pi.intent_router",
    "clarification": "pi.clarification",
    "metric_definition": "pi.metric_definition",
    "query_generation": MODEL_SCOPE_QUERY_PLANNING,
    "query_repair": "forge.query_repair",
    "knowledge_answer": "pi.knowledge_answer",
    "analysis": "pi.analysis",
    "report": "pi.report",
    "memory_extraction": "pi.memory_extraction",
}
MODEL_SCOPE_TO_STAGE = {scope: stage for stage, scope in MODEL_STAGE_SCOPES.items()}
SQL_CRITICAL_MODEL_STAGES = {"metric_definition", "query_generation", "query_repair"}
SQL_CRITICAL_MODEL_SCOPES = {MODEL_STAGE_SCOPES[stage] for stage in SQL_CRITICAL_MODEL_STAGES}
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
                    CREATE TABLE IF NOT EXISTS model_quality_validation_runs (
                        run_id TEXT PRIMARY KEY,
                        revision_id TEXT NOT NULL,
                        status TEXT NOT NULL,
                        thresholds_json TEXT NOT NULL,
                        metrics_json TEXT NOT NULL DEFAULT '{}',
                        error_code TEXT,
                        created_at TEXT NOT NULL,
                        started_at TEXT,
                        completed_at TEXT,
                        FOREIGN KEY(revision_id) REFERENCES model_profile_revisions(revision_id)
                    );
                    CREATE TABLE IF NOT EXISTS model_quality_validation_cases (
                        run_id TEXT NOT NULL,
                        case_id TEXT NOT NULL,
                        result_json TEXT NOT NULL,
                        PRIMARY KEY(run_id, case_id),
                        FOREIGN KEY(run_id) REFERENCES model_quality_validation_runs(run_id)
                    );
                    CREATE TABLE IF NOT EXISTS model_control_settings (
                        setting_key TEXT PRIMARY KEY,
                        value_json TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        updated_by TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS model_control_settings_audit (
                        audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        setting_key TEXT NOT NULL,
                        from_value_json TEXT,
                        to_value_json TEXT NOT NULL,
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
        "timeout_seconds", "max_output_tokens", "temperature", "secret_ref", "capabilities",
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
        max_output_tokens = max(256, int(config.get("max_output_tokens", 8192)))
        temperature = float(config.get("temperature", 0.0))
        if not 0.0 <= temperature <= 2.0:
            raise ValueError("temperature out of range")
    except (TypeError, ValueError) as exc:
        raise ModelControlError("Model Profile timeout/max_output_tokens/temperature 必须是有效数字。") from exc
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
        "max_output_tokens": max_output_tokens,
        "temperature": temperature,
        "secret_ref": secret_ref,
        "capabilities": capabilities,
    }


def model_scope_for_stage(stage: str) -> str:
    try:
        return MODEL_STAGE_SCOPES[stage]
    except KeyError as exc:
        raise ModelControlError("Model Stage 不受支持。") from exc


def validate_model_scope(scope: str) -> str:
    if scope not in MODEL_SCOPE_TO_STAGE:
        raise ModelControlError("Model Binding scope 不受支持。")
    return scope


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
        validate_model_scope(scope)
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

    def list_active(self) -> dict[str, ActiveModelRevision]:
        result: dict[str, ActiveModelRevision] = {}
        for stage, scope in MODEL_STAGE_SCOPES.items():
            active = self.get_active(scope)
            if active is not None:
                result[stage] = active
        return result

    def sql_quality_gate_enabled(self) -> bool:
        """Return the durable SQL gate switch; existing installs default fail-closed."""
        with _connect(self.path) as db:
            row = db.execute(
                "SELECT value_json FROM model_control_settings WHERE setting_key=?",
                ("sql_quality_gate_enabled",),
            ).fetchone()
        if row is None:
            return True
        try:
            value = json.loads(row["value_json"])
        except json.JSONDecodeError as exc:
            raise ModelControlError("SQL Quality Gate 开关状态损坏。") from exc
        if not isinstance(value, bool):
            raise ModelControlError("SQL Quality Gate 开关必须是布尔值。")
        return value

    def set_sql_quality_gate_enabled(self, enabled: bool, *, actor: str) -> None:
        if not actor.strip():
            raise ModelControlError("SQL Quality Gate 开关缺少操作者。")
        now = _now()
        encoded = _canonical_json(enabled)
        with _connect(self.path) as db:
            db.execute("BEGIN IMMEDIATE")
            current = db.execute(
                "SELECT value_json FROM model_control_settings WHERE setting_key=?",
                ("sql_quality_gate_enabled",),
            ).fetchone()
            previous = current["value_json"] if current else None
            db.execute(
                "INSERT INTO model_control_settings(setting_key,value_json,updated_at,updated_by) "
                "VALUES(?,?,?,?) ON CONFLICT(setting_key) DO UPDATE SET "
                "value_json=excluded.value_json,updated_at=excluded.updated_at,updated_by=excluded.updated_by",
                ("sql_quality_gate_enabled", encoded, now, actor),
            )
            db.execute(
                "INSERT INTO model_control_settings_audit"
                "(setting_key,from_value_json,to_value_json,actor,created_at) VALUES(?,?,?,?,?)",
                ("sql_quality_gate_enabled", previous, encoded, actor, now),
            )

    def activate(
        self,
        revision_id: str,
        *,
        expected_version: int,
        actor: str,
        current_lineage: dict[str, str],
        scope: str = MODEL_SCOPE_QUERY_PLANNING,
        action: str = "activate",
    ) -> int:
        validate_model_scope(scope)
        with _connect(self.path) as db:
            db.execute("BEGIN IMMEDIATE")
            revision = db.execute(
                "SELECT validation_status,config_json FROM model_profile_revisions WHERE revision_id=?",
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
            quality_gate_required = scope in SQL_CRITICAL_MODEL_SCOPES and self.sql_quality_gate_enabled()
            if quality_gate_required:
                quality_gate = validation_report.get("quality_gate", {})
                if quality_gate.get("passed") is not True:
                    raise ModelControlError("SQL Critical Model Revision 尚未通过完整质量与性能门禁。")
                if quality_gate.get("lineage") != current_lineage:
                    raise ModelControlError("SQL Critical Model Revision 的 Registry 或 Assurance lineage 已过期。")
            else:
                capability_gate = validation_report.get("capability_gate", {})
                if capability_gate.get("passed") is not True:
                    raise ModelControlError("Stage Model Revision 尚未通过协议、Tool、Artifact 与内容安全门禁。")
            validation_running = db.execute(
                "SELECT 1 FROM model_quality_validation_runs "
                "WHERE revision_id=? AND status IN ('queued','running')",
                (revision_id,),
            ).fetchone()
            if validation_running:
                raise ModelControlError("Model Profile Revision 的质量验证尚未结束。")
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
            effective_action = (
                f"{action}_compatibility_only"
                if scope in SQL_CRITICAL_MODEL_SCOPES and not quality_gate_required
                else action
            )
            db.execute(
                "INSERT INTO model_switch_audit(scope,action,from_revision_id,to_revision_id,"
                "binding_version,actor,created_at) VALUES(?,?,?,?,?,?,?)",
                (scope, effective_action, previous, revision_id, next_version, actor, _now()),
            )
        return next_version

    def rollback(
        self,
        *,
        expected_version: int,
        actor: str,
        current_lineage: dict[str, str],
        scope: str = MODEL_SCOPE_QUERY_PLANNING,
    ) -> int:
        validate_model_scope(scope)
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
            current_lineage=current_lineage,
            scope=scope,
            action="rollback",
        )

    def create_quality_validation_run(
        self,
        revision_id: str,
        *,
        thresholds: dict[str, Any],
    ) -> str:
        if self.get_revision(revision_id) is None:
            raise ModelControlError("Model Profile Revision 不存在。")
        run_id = "mvr_" + uuid.uuid4().hex
        with _connect(self.path) as db:
            db.execute("BEGIN IMMEDIATE")
            bound = db.execute(
                "SELECT 1 FROM active_model_bindings WHERE revision_id=?",
                (revision_id,),
            ).fetchone()
            if bound:
                raise ModelControlError("Active Model Profile Revision 不允许重新验证。")
            active = db.execute(
                "SELECT 1 FROM model_quality_validation_runs "
                "WHERE revision_id=? AND status IN ('queued','running')",
                (revision_id,),
            ).fetchone()
            if active:
                raise ModelControlError("该 Revision 已有进行中的 Quality Validation Run。")
            db.execute(
                "INSERT INTO model_quality_validation_runs"
                "(run_id,revision_id,status,thresholds_json,created_at) VALUES(?,?,?,?,?)",
                (run_id, revision_id, "queued", _canonical_json(thresholds), _now()),
            )
        return run_id

    def mark_quality_validation_running(self, run_id: str) -> None:
        with _connect(self.path) as db:
            updated = db.execute(
                "UPDATE model_quality_validation_runs SET status='running',started_at=? "
                "WHERE run_id=? AND status='queued'",
                (_now(), run_id),
            ).rowcount
        if updated != 1:
            raise ModelControlError("Quality Validation Run 不可启动。")

    def record_quality_validation_case(
        self,
        run_id: str,
        case_id: str,
        result: dict[str, Any],
    ) -> None:
        with _connect(self.path) as db:
            db.execute(
                "INSERT OR REPLACE INTO model_quality_validation_cases(run_id,case_id,result_json) "
                "VALUES(?,?,?)",
                (run_id, str(case_id), _canonical_json(result)),
            )

    def complete_quality_validation_run(
        self,
        run_id: str,
        *,
        status: str,
        metrics: dict[str, Any],
        error_code: str | None = None,
    ) -> None:
        if status not in {"passed", "failed", "interrupted"}:
            raise ModelControlError("Quality Validation Run 终态不受支持。")
        with _connect(self.path) as db:
            updated = db.execute(
                "UPDATE model_quality_validation_runs SET status=?,metrics_json=?,error_code=?,"
                "completed_at=? WHERE run_id=? AND status='running'",
                (status, _canonical_json(metrics), error_code, _now(), run_id),
            ).rowcount
        if updated != 1:
            raise ModelControlError("Quality Validation Run 状态已变化。")

    def get_quality_validation_run(self, run_id: str) -> dict[str, Any] | None:
        with _connect(self.path) as db:
            row = db.execute(
                "SELECT * FROM model_quality_validation_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
            cases = db.execute(
                "SELECT case_id,result_json FROM model_quality_validation_cases "
                "WHERE run_id=? ORDER BY case_id",
                (run_id,),
            ).fetchall()
        if row is None:
            return None
        return {
            "run_id": row["run_id"],
            "revision_id": row["revision_id"],
            "status": row["status"],
            "thresholds": json.loads(row["thresholds_json"]),
            "metrics": json.loads(row["metrics_json"]),
            "error_code": row["error_code"],
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "completed_at": row["completed_at"],
            "cases": [
                {"case_id": item["case_id"], **json.loads(item["result_json"])}
                for item in cases
            ],
        }

    def reconcile_quality_validation_runs(self) -> int:
        """Mark abandoned work interrupted; never replay provider or SQL calls."""
        with _connect(self.path) as db:
            rows = db.execute(
                "SELECT run_id FROM model_quality_validation_runs WHERE status='running'"
            ).fetchall()
            for row in rows:
                db.execute(
                    "UPDATE model_quality_validation_runs SET status='interrupted',"
                    "error_code='process_restarted',completed_at=? WHERE run_id=?",
                    (_now(), row["run_id"]),
                )
        return len(rows)

    def list_audit(self, scope: str = MODEL_SCOPE_QUERY_PLANNING) -> list[dict[str, Any]]:
        with _connect(self.path) as db:
            rows = db.execute(
                "SELECT * FROM model_switch_audit WHERE scope=? ORDER BY audit_id",
                (scope,),
            ).fetchall()
        return [dict(row) for row in rows]
