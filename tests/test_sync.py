"""
Tests for forge sync — uses SQLite in-memory databases.
"""
import json
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))

from forge.sync import run_sync


SQLITE_URL = "sqlite:///:memory:"


def _build_db(engine, ddl_statements: list[str]):
    with engine.connect() as conn:
        for stmt in ddl_statements:
            conn.execute(text(stmt))
        conn.commit()


def _make_db_url(tmp_path: Path, ddl_statements: list[str]) -> str:
    db_file = tmp_path / "test.db"
    url = f"sqlite:///{db_file}"
    engine = create_engine(url)
    _build_db(engine, ddl_statements)
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
    assert result["tables"]["users"] == {"columns": ["id", "name", "city"]}
    assert result["tables"]["orders"] == {"columns": ["id", "user_id", "status"]}
    assert "metrics" not in result

    on_disk = json.loads(registry_path.read_text())
    assert on_disk == result


def test_sync_preserves_existing_metrics(tmp_path):
    url = _make_db_url(tmp_path, [
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)",
    ])
    registry_path = tmp_path / "schema.registry.json"
    existing = {
        "tables": {
            "old_table": {"columns": ["a", "b"]}
        },
        "metrics": {
            "revenue": {"description": "Total revenue"},
            "dau": {"description": "Daily active users"},
        },
    }
    registry_path.write_text(json.dumps(existing))

    result = run_sync(url, registry_path)

    assert "metrics" in result
    assert result["metrics"] == existing["metrics"]
    assert set(result["tables"].keys()) == {"products"}
    assert "old_table" not in result["tables"]


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
    assert result["tables"]["events"] == {"columns": ["id", "name", "ts"]}
