from __future__ import annotations

import os

import pytest
import yaml

from agent.model_config import (
    LLMConfigurationError,
    LLMNotConfiguredError,
    get_model_config,
    reset_model_config_cache,
)


@pytest.fixture(autouse=True)
def isolated_model_environment(monkeypatch, tmp_path):
    config_path = tmp_path / "forge.yaml"
    monkeypatch.setenv("FORGE_CONFIG_PATH", str(config_path))
    for name in (
        "LLM_PROVIDER",
        "LLM_MODEL",
        "LLM_API_KEY",
        "LLM_BASE_URL",
        "LLM_TOOL_CHOICE",
        "LLM_TIMEOUT_SECONDS",
    ):
        monkeypatch.delenv(name, raising=False)
    reset_model_config_cache()
    yield config_path
    reset_model_config_cache()


def _write(path, llm):
    path.write_text(yaml.safe_dump({"llm": llm}, sort_keys=False), encoding="utf-8")
    os.utime(path, None)


def test_model_config_hot_reloads_without_process_restart(isolated_model_environment):
    path = isolated_model_environment
    _write(path, {
        "provider": "openai",
        "model": "model-a",
        "api_key": "secret-a",
        "base_url": "https://provider-a.example/v1",
    })
    first = get_model_config()

    _write(path, {
        "provider": "openai",
        "model": "model-b-longer",
        "api_key": "secret-b",
        "base_url": "https://provider-b.example/v1",
    })
    second = get_model_config()

    assert first.model == "model-a"
    assert second.model == "model-b-longer"
    assert second.base_url == "https://provider-b.example/v1"
    assert first.revision != second.revision


def test_model_config_fails_closed_when_missing(isolated_model_environment):
    with pytest.raises(LLMNotConfiguredError, match="尚未配置"):
        get_model_config()


def test_model_config_fails_closed_when_partial(isolated_model_environment):
    _write(isolated_model_environment, {"provider": "openai", "model": "model-a"})
    with pytest.raises(LLMNotConfiguredError, match="api_key"):
        get_model_config()


def test_model_config_rejects_unknown_provider(isolated_model_environment):
    _write(isolated_model_environment, {
        "provider": "unknown",
        "model": "model-a",
        "api_key": "secret-a",
    })
    with pytest.raises(LLMConfigurationError, match="Provider"):
        get_model_config()


def test_environment_override_is_explicit_and_cached(monkeypatch, isolated_model_environment):
    _write(isolated_model_environment, {
        "provider": "openai",
        "model": "yaml-model",
        "api_key": "yaml-key",
    })
    monkeypatch.setenv("LLM_MODEL", "environment-model")

    config = get_model_config()

    assert config.model == "environment-model"
    assert config.source == "environment"
