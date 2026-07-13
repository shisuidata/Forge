"""Cross-database smoke test for sync, compile, and guarded execution."""
from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text

import forge.executor as executor
from config import cfg
from forge.compiler import compile_query
from forge.executor import execute_with_data, validate_readonly_sql
from registry.sync import run_sync


def _database_url(tmp_path) -> str:
    return os.getenv("FORGE_SMOKE_DATABASE_URL") or f"sqlite:///{tmp_path / 'compat.db'}"


def test_database_compatibility_smoke(tmp_path, monkeypatch):
    database_url = _database_url(tmp_path)
    engine = create_engine(database_url)
    table_name = "forge_compat_smoke"

    try:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            conn.execute(text(
                f"CREATE TABLE {table_name} ("
                "id INTEGER PRIMARY KEY, status VARCHAR(32), amount INTEGER)"
            ))
            conn.execute(
                text(
                    f"INSERT INTO {table_name} (id, status, amount) VALUES "
                    "(1, 'paid', 100), (2, 'pending', 80), (3, 'paid', 60)"
                )
            )

        registry = run_sync(database_url, tmp_path / "schema.registry.json")
        columns = registry["tables"][table_name]["columns"]
        assert columns["status"]["enum"] == ["paid", "pending"]

        dialect = "mysql" if engine.dialect.name == "mariadb" else engine.dialect.name
        sql = compile_query(
            {
                "scan": table_name,
                "select": [f"{table_name}.id", f"{table_name}.status"],
                "sort": [{"col": f"{table_name}.id", "dir": "asc"}],
            },
            dialect=dialect,
        )

        monkeypatch.setattr(cfg, "DATABASE_URL", database_url)
        monkeypatch.setattr(cfg, "EXECUTION_ENABLED", True)
        monkeypatch.setattr(cfg, "EXECUTION_MAX_ROWS", 2)
        monkeypatch.setattr(cfg, "EXECUTION_DISPLAY_ROWS", 2)
        monkeypatch.setattr(cfg, "EXECUTION_TIMEOUT_SECONDS", 10)
        executor._engine = None

        rendered, result_columns, rows = execute_with_data(sql, max_rows=50)
        assert not rendered.startswith("⚠")
        assert result_columns == ["id", "status"]
        assert [tuple(row) for row in rows] == [(1, "paid"), (2, "pending")]

        with pytest.raises(ValueError, match="只允许执行只读"):
            validate_readonly_sql(f"DELETE FROM {table_name}")
    finally:
        if executor._engine is not None:
            executor._engine.dispose()
            executor._engine = None
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        engine.dispose()
