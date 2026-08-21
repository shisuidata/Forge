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


def test_sync_imports_declared_foreign_keys_as_trusted_relationships(tmp_path):
    url = _make_db_url(tmp_path, [
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, "
        "FOREIGN KEY(user_id) REFERENCES users(id))",
    ])

    result = run_sync(url, tmp_path / "schema.registry.json")

    assert result["relationships"] == [{
        "id": "db:fk_orders_users_0",
        "from": "orders.user_id",
        "to": "users.id",
        "cardinality": "many_to_one",
        "status": "declared",
        "source": "database",
    }]


def test_sync_does_not_trust_partial_composite_foreign_keys(tmp_path):
    url = _make_db_url(tmp_path, [
        "CREATE TABLE parents (a INTEGER, b INTEGER, PRIMARY KEY(a, b))",
        "CREATE TABLE children (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, "
        "FOREIGN KEY(a, b) REFERENCES parents(a, b))",
    ])

    result = run_sync(url, tmp_path / "schema.registry.json")

    assert result["relationships"] == []


def test_sync_preserves_confirmed_relationship_metadata(tmp_path):
    url = _make_db_url(tmp_path, [
        "CREATE TABLE users (id INTEGER PRIMARY KEY)",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)",
    ])
    path = tmp_path / "schema.registry.json"
    path.write_text(json.dumps({
        "tables": {},
        "relationships": [{
            "id": "manual:orders_user",
            "from": "orders.user_id",
            "to": "users.id",
            "cardinality": "many_to_one",
            "status": "confirmed",
            "source": "manual",
        }],
    }))

    result = run_sync(url, path)

    assert result["relationships"][0]["status"] == "confirmed"
    assert result["relationships"][0]["source"] == "manual"


def test_database_declaration_supersedes_inferred_edge(tmp_path):
    url = _make_db_url(tmp_path, [
        "CREATE TABLE users (id INTEGER PRIMARY KEY)",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, "
        "FOREIGN KEY(user_id) REFERENCES users(id))",
    ])
    path = tmp_path / "schema.registry.json"
    path.write_text(json.dumps({
        "tables": {},
        "relationships": [{
            "id": "guess:orders_user",
            "from": "orders.user_id",
            "to": "users.id",
            "cardinality": "many_to_one",
            "status": "inferred",
            "source": "naming",
        }],
    }))

    result = run_sync(url, path)

    assert result["relationships"][0]["status"] == "declared"
    assert result["relationships"][0]["source"] == "database"


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


def test_sync_cli_out_flag_writes_requested_registry_path(tmp_path, monkeypatch, capsys):
    url = _make_db_url(tmp_path, [
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)",
    ])
    out_path = tmp_path / "registry" / "schema.registry.json"

    from forge import cli

    monkeypatch.setattr(sys, "argv", [
        "forge",
        "sync",
        "--db",
        url,
        "--out",
        str(out_path),
    ])

    cli.main()

    output = capsys.readouterr().out
    assert out_path.exists()
    assert str(out_path) in output
    registry = json.loads(out_path.read_text())
    assert set(registry["tables"].keys()) == {"customers"}


def test_sync_quotes_reserved_and_mixed_case_identifiers(tmp_path):
    url = _make_db_url(tmp_path, [
        'CREATE TABLE "order" ("id" INTEGER PRIMARY KEY, "group" TEXT, "Status Code" TEXT)',
        'INSERT INTO "order" ("group", "Status Code") VALUES '
        "('retail', 'PAID'), ('retail', 'PENDING'), ('wholesale', 'PAID')",
    ])

    result = run_sync(url, tmp_path / "schema.registry.json")
    columns = result["tables"]["order"]["columns"]

    assert columns["group"]["enum"] == ["retail", "wholesale"]
    assert columns["Status Code"]["enum"] == ["PAID", "PENDING"]


def test_sync_enum_count_is_limited_to_sample_rows(tmp_path, monkeypatch):
    import registry.sync as sync_module

    monkeypatch.setattr(sync_module, "_ENUM_SAMPLE_ROWS", 5)
    high_cardinality_tail = ", ".join(f"('tail-{i:02d}')" for i in range(31))
    url = _make_db_url(tmp_path, [
        "CREATE TABLE events (id INTEGER PRIMARY KEY, status TEXT)",
        "INSERT INTO events (status) VALUES ('paid'), ('pending'), ('paid'), ('pending'), ('paid')",
        f"INSERT INTO events (status) VALUES {high_cardinality_tail}",
    ])

    result = run_sync(url, tmp_path / "schema.registry.json")

    assert result["tables"]["events"]["columns"]["status"]["enum"] == ["paid", "pending"]


def test_sync_ignores_null_only_enum_columns(tmp_path):
    url = _make_db_url(tmp_path, [
        "CREATE TABLE events (id INTEGER PRIMARY KEY, status TEXT)",
        "INSERT INTO events (status) VALUES (NULL), (NULL)",
    ])

    result = run_sync(url, tmp_path / "schema.registry.json")

    assert result["tables"]["events"]["columns"]["status"] == {}
