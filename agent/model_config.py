"""Hot-reloadable, immutable LLM configuration snapshots.

Only model settings are hot reloaded. Database and execution settings remain
process configuration because they own long-lived engines and safety state.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import threading
from typing import Any

import yaml


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


_lock = threading.RLock()
_cached_signature: tuple[Any, ...] | None = None
_cached_snapshot: ModelConfigSnapshot | None = None


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


def _value(env_name: str, yaml_body: dict[str, Any], yaml_name: str, default: str = "") -> str:
    env_value = os.getenv(env_name)
    if env_value not in (None, ""):
        return str(env_value)
    value = yaml_body.get(yaml_name, default)
    return "" if value is None else str(value)


def get_model_config() -> ModelConfigSnapshot:
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
        )
    )
    signature = (str(path), *stat_signature, *env_signature)
    with _lock:
        if _cached_signature == signature and _cached_snapshot is not None:
            return _cached_snapshot

        body = _yaml_llm(path)
        provider = _value("LLM_PROVIDER", body, "provider").strip().lower()
        model = _value("LLM_MODEL", body, "model").strip()
        api_key = _value("LLM_API_KEY", body, "api_key").strip()
        base_url = _value("LLM_BASE_URL", body, "base_url").strip().rstrip("/")
        tool_choice = _value("LLM_TOOL_CHOICE", body, "tool_choice", "auto").strip().lower()
        timeout_raw = _value("LLM_TIMEOUT_SECONDS", body, "timeout_seconds", "120")

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
        except (TypeError, ValueError) as exc:
            raise LLMConfigurationError("LLM timeout_seconds 必须是数字") from exc

        source = "environment" if any(env_signature[:4]) else "forge.yaml"
        revision_input = json.dumps(
            {
                "provider": provider,
                "model": model,
                "base_url": base_url,
                "tool_choice": tool_choice,
                "timeout_seconds": timeout_seconds,
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
        )
        _cached_signature = signature
        _cached_snapshot = snapshot
        return snapshot
