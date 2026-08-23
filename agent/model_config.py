"""Hot-reloadable, immutable LLM configuration snapshots.

Only model settings are hot reloaded. Database and execution settings remain
process configuration because they own long-lived engines and safety state.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import threading
from typing import Any

import yaml

from agent.model_control import (
    MODEL_SCOPE_QUERY_PLANNING,
    ModelControlStore,
    SQL_CRITICAL_MODEL_SCOPES,
    model_scope_for_stage,
)


class LLMNotConfiguredError(RuntimeError):
    """No usable active model configuration exists."""


class LLMConfigurationError(RuntimeError):
    """The active model configuration is malformed or incompatible."""


@dataclass(frozen=True)
class ModelConfigSnapshot:
    provider: str
    model: str
    api_key: str
    base_url: str
    tool_choice: str
    timeout_seconds: float
    revision: str
    source: str
    max_output_tokens: int = 8192
    temperature: float = 0.0
    capabilities: dict[str, Any] = field(default_factory=dict)


_lock = threading.RLock()
_cached_signature: tuple[Any, ...] | None = None
_cached_snapshot: ModelConfigSnapshot | None = None


def model_control_db_path() -> Path:
    return Path(os.getenv("MODEL_CONTROL_DB_PATH", ".forge/model_control.db")).expanduser().resolve()


def model_config_path() -> Path:
    override = os.getenv("FORGE_CONFIG_PATH")
    if override:
        return Path(override).expanduser().resolve()
    return Path(__file__).resolve().parents[1] / "forge.yaml"


def reset_model_config_cache() -> None:
    global _cached_signature, _cached_snapshot
    with _lock:
        _cached_signature = None
        _cached_snapshot = None


def _yaml_llm(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        body = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise LLMConfigurationError("LLM 配置文件无法读取或格式错误") from exc
    llm = body.get("llm", {}) if isinstance(body, dict) else {}
    if not isinstance(llm, dict):
        raise LLMConfigurationError("LLM 配置必须是对象")
    return llm


def _resolve_secret(secret_ref: str) -> str:
    if secret_ref.startswith("env:"):
        value = os.getenv(secret_ref[4:], "").strip()
        if not value:
            raise LLMNotConfiguredError("Active Model Profile 的 Secret 环境变量不可用")
        return value
    if secret_ref.startswith("file:"):
        path = Path(secret_ref[5:]).expanduser().resolve()
        try:
            file_stat = path.stat()
            if stat.S_IMODE(file_stat.st_mode) != 0o600:
                raise LLMConfigurationError("Model Secret 文件权限必须为 600")
            value = path.read_text(encoding="utf-8").strip()
        except FileNotFoundError as exc:
            raise LLMNotConfiguredError("Active Model Profile 的 Secret 文件不存在") from exc
        except OSError as exc:
            raise LLMConfigurationError("Active Model Profile 的 Secret 文件不可读取") from exc
        if not value:
            raise LLMNotConfiguredError("Active Model Profile 的 Secret 文件为空")
        return value
    raise LLMConfigurationError("Active Model Profile 的 secret_ref 不受支持")


def _control_signature(path: Path) -> tuple[Any, ...]:
    signatures: list[Any] = [str(path)]
    for candidate in (path, Path(str(path) + "-wal")):
        try:
            stat = candidate.stat()
            signatures.extend((stat.st_mtime_ns, stat.st_size))
        except FileNotFoundError:
            signatures.extend((None, None))
    return tuple(signatures)


def _value(env_name: str, yaml_body: dict[str, Any], yaml_name: str, default: str = "") -> str:
    env_value = os.getenv(env_name)
    if env_value not in (None, ""):
        return str(env_value)
    value = yaml_body.get(yaml_name, default)
    return "" if value is None else str(value)


def get_revision_model_config(
    revision_id: str,
    *,
    db_path: str | Path | None = None,
) -> ModelConfigSnapshot:
    revision = ModelControlStore(db_path or model_control_db_path()).get_revision(revision_id)
    if revision is None:
        raise LLMConfigurationError("Model Profile Revision 不存在")
    config = revision["config"]
    return ModelConfigSnapshot(
        provider=str(config["provider"]),
        model=str(config["model"]),
        api_key=_resolve_secret(str(config["secret_ref"])),
        base_url=str(config.get("base_url", "")),
        tool_choice=str(config.get("tool_choice", "required")),
        timeout_seconds=float(config.get("timeout_seconds", 120)),
        revision=revision_id,
        max_output_tokens=int(config.get("max_output_tokens", 8192)),
        temperature=float(config.get("temperature", 0.0)),
        capabilities=dict(config.get("capabilities", {})),
        source="model-control-validation",
    )


def get_model_config(stage: str = "query_generation") -> ModelConfigSnapshot:
    """Return the current model snapshot, reloading only when config changes."""
    global _cached_signature, _cached_snapshot
    path = model_config_path()
    try:
        stat_signature: tuple[Any, ...] = (path.stat().st_mtime_ns, path.stat().st_size)
    except FileNotFoundError:
        stat_signature = (None, None)
    env_signature = tuple(
        os.getenv(name)
        for name in (
            "LLM_PROVIDER",
            "LLM_MODEL",
            "LLM_API_KEY",
            "LLM_BASE_URL",
            "LLM_TOOL_CHOICE",
            "LLM_TIMEOUT_SECONDS",
            "LLM_MAX_OUTPUT_TOKENS",
            "LLM_TEMPERATURE",
        )
    )
    control_path = model_control_db_path()
    control_signature = _control_signature(control_path)
    requested_scope = model_scope_for_stage(stage)
    signature = (requested_scope, str(path), *stat_signature, *env_signature, *control_signature)
    with _lock:
        if _cached_signature == signature and _cached_snapshot is not None:
            return _cached_snapshot

        try:
            store = ModelControlStore(control_path)
            active = store.get_active(requested_scope) if control_path.exists() else None
            if active is None and requested_scope != MODEL_SCOPE_QUERY_PLANNING and control_path.exists():
                active = store.get_active(MODEL_SCOPE_QUERY_PLANNING)
            if active is None and requested_scope in SQL_CRITICAL_MODEL_SCOPES and control_path.exists():
                raise LLMConfigurationError("SQL-critical model stage has no validated active binding")
        except (OSError, sqlite3.DatabaseError, json.JSONDecodeError, KeyError, TypeError) as exc:
            raise LLMConfigurationError("Model Control Plane 状态不可用") from exc
        if active is not None:
            gate_name = (
                "quality_gate"
                if requested_scope in SQL_CRITICAL_MODEL_SCOPES and store.sql_quality_gate_enabled()
                else "capability_gate"
            )
            gate = active.validation_report.get(gate_name, {})
            if not isinstance(gate, dict) or gate.get("passed") is not True:
                raise LLMConfigurationError(
                    f"Active Model Profile 不满足当前 {gate_name} 开关要求"
                )
            config = active.config
            api_key = _resolve_secret(str(config["secret_ref"]))
            snapshot = ModelConfigSnapshot(
                provider=str(config["provider"]),
                model=str(config["model"]),
                api_key=api_key,
                base_url=str(config.get("base_url", "")),
                tool_choice=str(config.get("tool_choice", "required")),
                timeout_seconds=float(config.get("timeout_seconds", 120)),
                revision=active.revision_id,
                max_output_tokens=int(config.get("max_output_tokens", 8192)),
                temperature=float(config.get("temperature", 0.0)),
                capabilities=dict(config.get("capabilities", {})),
                source=f"model-control:{active.scope}:v{active.binding_version}",
            )
            _cached_signature = signature
            _cached_snapshot = snapshot
            return snapshot

        body = _yaml_llm(path)
        provider = _value("LLM_PROVIDER", body, "provider").strip().lower()
        model = _value("LLM_MODEL", body, "model").strip()
        api_key = _value("LLM_API_KEY", body, "api_key").strip()
        base_url = _value("LLM_BASE_URL", body, "base_url").strip().rstrip("/")
        tool_choice = _value("LLM_TOOL_CHOICE", body, "tool_choice", "auto").strip().lower()
        timeout_raw = _value("LLM_TIMEOUT_SECONDS", body, "timeout_seconds", "120")
        max_output_raw = _value("LLM_MAX_OUTPUT_TOKENS", body, "max_output_tokens", "8192")
        temperature_raw = _value("LLM_TEMPERATURE", body, "temperature", "0")

        if not provider and not model and not api_key:
            raise LLMNotConfiguredError("尚未配置 LLM")
        missing = [name for name, value in (("provider", provider), ("model", model), ("api_key", api_key)) if not value]
        if missing:
            raise LLMNotConfiguredError("LLM 配置不完整：" + ", ".join(missing))
        if provider not in {"anthropic", "openai"}:
            raise LLMConfigurationError("LLM Provider 只支持 anthropic 或 openai")
        if tool_choice not in {"auto", "required", "named"}:
            raise LLMConfigurationError("LLM tool_choice 必须是 auto、required 或 named")
        try:
            timeout_seconds = max(1.0, float(timeout_raw))
            max_output_tokens = max(256, int(max_output_raw))
            temperature = float(temperature_raw)
            if not 0.0 <= temperature <= 2.0:
                raise ValueError("temperature out of range")
        except (TypeError, ValueError) as exc:
            raise LLMConfigurationError("LLM timeout/max_output_tokens/temperature 配置必须是有效数字") from exc

        source = "environment" if any(env_signature[:4]) else "forge.yaml"
        revision_input = json.dumps(
            {
                "provider": provider,
                "model": model,
                "base_url": base_url,
                "tool_choice": tool_choice,
                "timeout_seconds": timeout_seconds,
                "max_output_tokens": max_output_tokens,
                "temperature": temperature,
                "key_fingerprint": hashlib.sha256(api_key.encode()).hexdigest(),
            },
            sort_keys=True,
        )
        snapshot = ModelConfigSnapshot(
            provider=provider,
            model=model,
            api_key=api_key,
            base_url=base_url,
            tool_choice=tool_choice,
            timeout_seconds=timeout_seconds,
            revision="sha256:" + hashlib.sha256(revision_input.encode()).hexdigest(),
            source=source,
            max_output_tokens=max_output_tokens,
            temperature=temperature,
        )
        _cached_signature = signature
        _cached_snapshot = snapshot
        return snapshot
