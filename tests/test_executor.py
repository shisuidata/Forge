from __future__ import annotations

from types import SimpleNamespace

import pytest

from forge.executor import _apply_statement_timeout, validate_readonly_sql


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "WITH recent AS (SELECT 1 AS n) SELECT n FROM recent;",
        "SELECT 'drop table users' AS harmless",
    ],
)
def test_validate_readonly_sql_accepts_read_queries(sql: str):
    validate_readonly_sql(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM orders",
        "UPDATE orders SET status = 'x'",
        "SELECT 1; DROP TABLE users",
        "PRAGMA table_info(users)",
        "WITH doomed AS (SELECT 1) DELETE FROM users",
    ],
)
def test_validate_readonly_sql_rejects_mutating_queries(sql: str):
    with pytest.raises(ValueError):
        validate_readonly_sql(sql)


def test_execute_with_data_respects_configured_row_cap(monkeypatch):
    import forge.executor as executor

    monkeypatch.setattr(executor.cfg, "DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setattr(executor.cfg, "EXECUTION_ENABLED", True)
    monkeypatch.setattr(executor.cfg, "EXECUTION_MAX_ROWS", 2)
    monkeypatch.setattr(executor.cfg, "EXECUTION_DISPLAY_ROWS", 2)
    monkeypatch.setattr(executor, "_engine", None)

    sql = "SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3"
    text, cols, rows = executor.execute_with_data(sql, max_rows=200)

    assert cols == ["n"]
    assert len(rows) == 2
    assert "仅显示" in text or "显示前" in text


def test_execute_with_data_can_be_disabled(monkeypatch):
    import forge.executor as executor

    monkeypatch.setattr(executor.cfg, "EXECUTION_ENABLED", False)

    text, cols, rows = executor.execute_with_data("SELECT 1")

    assert "禁用" in text
    assert cols == []
    assert rows == []


def test_bounded_timeout_seconds(monkeypatch):
    import forge.executor as executor

    monkeypatch.setattr(executor.cfg, "EXECUTION_TIMEOUT_SECONDS", 45)
    assert executor._bounded_timeout_seconds() == 45

    monkeypatch.setattr(executor.cfg, "EXECUTION_TIMEOUT_SECONDS", -1)
    assert executor._bounded_timeout_seconds() == 0


@pytest.mark.parametrize(
    ("dialect", "expected_sql", "expected_params"),
    [
        ("postgresql", "SET LOCAL statement_timeout = %s", (12000,)),
        ("mysql", "SET SESSION max_execution_time = 12000", None),
    ],
)
def test_apply_statement_timeout_uses_sqlalchemy_driver_api(
    dialect, expected_sql, expected_params
):
    calls = []

    class Connection:
        def __init__(self, dialect_name):
            self.dialect = SimpleNamespace(name=dialect_name)

        def exec_driver_sql(self, sql, params=None):
            calls.append((sql, params))

    _apply_statement_timeout(Connection(dialect), 12)

    assert calls == [(expected_sql, expected_params)]
