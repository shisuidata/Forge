"""Read/write helpers for the registry JSON/YAML stores backing admin pages.

Moved from web/router.py (REQ-2026-09-21-069). Bodies unchanged. Private names
are kept so historical documentation (docs/development.md) stays accurate.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path

import yaml

from config import cfg

logger = logging.getLogger(__name__)


def _load_schema() -> dict:
    """Load structural layer (schema.registry.json)."""
    try:
        return json.loads(cfg.REGISTRY_PATH.read_text())
    except (FileNotFoundError, OSError, json.JSONDecodeError) as exc:
        logger.warning("Failed to load schema registry: %s", exc)
        return {}


def _load_metrics() -> dict:
    """Load semantic layer (metrics.registry.yaml)."""
    try:
        return yaml.safe_load(cfg.METRICS_PATH.read_text()) or {}
    except (FileNotFoundError, OSError, yaml.YAMLError) as exc:
        logger.warning("Failed to load metrics registry: %s", exc)
        return {}


def _save_metrics(metrics: dict) -> None:
    path = cfg.METRICS_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    content = yaml.safe_dump(
        metrics,
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
    )
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as tmp:
            tmp.write(content)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_path = Path(tmp.name)
        yaml.safe_load(tmp_path.read_text(encoding="utf-8"))
        os.replace(tmp_path, path)
    finally:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()


def _load_disambiguations() -> dict:
    try:
        return yaml.safe_load(cfg.DISAMBIGUATIONS_PATH.read_text()) or {}
    except (FileNotFoundError, OSError, yaml.YAMLError):
        return {}


def _save_disambiguations(data: dict) -> None:
    cfg.DISAMBIGUATIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    cfg.DISAMBIGUATIONS_PATH.write_text(
        yaml.dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False)
    )


def _load_conventions() -> dict:
    try:
        return yaml.safe_load(cfg.CONVENTIONS_PATH.read_text()) or {}
    except (FileNotFoundError, OSError, yaml.YAMLError):
        return {}


def _save_conventions(data: dict) -> None:
    cfg.CONVENTIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    cfg.CONVENTIONS_PATH.write_text(
        yaml.dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False)
    )
