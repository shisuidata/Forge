"""
Tests for forge sync — uses SQLite in-memory databases.
"""
import json
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))

from registry.sync import run_sync


def _make_db_url(tmp_path: Path, ddl_statements: list[str]) -> str:
    db_file = tmp_path / "test.db"
    url = f"sqlite:///{db_file}"
    engine = create_engine(url)
    with engine.connect() as conn:
        for stmt in ddl_statements:
            conn.execute(text(stmt))
        conn.commit()
    engine.dispose()
    return url


def test_sync_creates_correct_registry_structure(tmp_path):
    url = _make_db_url(tmp_path, [
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, city TEXT)",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, status TEXT)",
    ])
    registry_path = tmp_path / "schema.registry.json"

    result = run_sync(url, registry_path)

    assert "tables" in result
    assert set(result["tables"].keys()) == {"users", "orders"}
    # columns 现在是 dict 格式，包含列元数据（低基数列自动附 enum）
    assert set(result["tables"]["users"]["columns"].keys()) == {"id", "name", "city"}
    assert set(result["tables"]["orders"]["columns"].keys()) == {"id", "user_id", "status"}
    assert "metrics" not in result

    on_disk = json.loads(registry_path.read_text())
    assert on_disk == result


def test_sync_only_writes_structural_layer(tmp_path):
    """sync must not touch metrics — that's metrics.registry.yaml's job."""
    url = _make_db_url(tmp_path, [
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)",
    ])
    registry_path = tmp_path / "schema.registry.json"

    result = run_sync(url, registry_path)

    assert "metrics" not in result
    assert set(result["tables"].keys()) == {"products"}


def test_sync_db_flag_overrides_config(tmp_path, monkeypatch):
    url = _make_db_url(tmp_path, [
        "CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT, ts TEXT)",
    ])
    registry_path = tmp_path / "schema.registry.json"

    import config
    monkeypatch.setattr(config.cfg, "DATABASE_URL", "sqlite:///should-not-be-used.db")
    monkeypatch.setattr(config.cfg, "REGISTRY_PATH", registry_path)

    result = run_sync(url, registry_path)

    assert "events" in result["tables"]
    assert set(result["tables"]["events"]["columns"].keys()) == {"id", "name", "ts"}
