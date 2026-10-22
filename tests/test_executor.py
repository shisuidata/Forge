from __future__ import annotations

import sqlite3

import pytest
from sqlalchemy import create_engine

from sqlglot import exp

from forge import executor
from forge.executor import SqlParseError, validate_readonly_sql


@pytest.mark.parametrize("sql", [
    "SELECT 1", "WITH recent AS (SELECT 1 AS n) SELECT n FROM recent;",
    "SELECT 'drop table users' AS harmless",
    "SELECT '/*' AS a, '*/' AS b", "SELECT 1 /* ; DELETE */ -- ; DROP",
    'SELECT "delete" FROM orders', "SELECT n FROM orders UNION SELECT n FROM archive",
])
def test_validate_readonly_sql_accepts_read_queries(sql):
    validate_readonly_sql(sql)


@pytest.mark.parametrize("sql", [
    "DELETE FROM orders", "UPDATE orders SET status = 'x'",
    "SELECT 1; DROP TABLE users", "PRAGMA table_info(users)",
    "WITH doomed AS (SELECT 1) DELETE FROM users",
    # A string that looks like a comment opener must not hide the following statements.
    "SELECT '/*'; SELECT n FROM private_data; SELECT '*/' AS x",
    "SELECT '/*'; DELETE FROM numbers; SELECT '*/' AS x",
    "SELECT n INTO created_table FROM numbers",
    "SELECT nextval('synthetic_sequence')",
    "SELECT pg_catalog.pg_sleep(10)",
    "SELECT query_to_xml('DELETE FROM numbers', true, true, '')",
    "SELECT n FROM numbers FOR UPDATE",
])
def test_validate_readonly_sql_rejects_mutating_queries(sql):
    with pytest.raises(ValueError):
        validate_readonly_sql(sql, "postgres")


@pytest.mark.parametrize(("sql", "dialect"), [
    ("SELECT n FROM numbers FOR SHARE", "postgres"),
    ("WITH gone AS (DELETE FROM numbers RETURNING n) SELECT n FROM gone", "postgres"),
    ("SELECT 'a\\'; DELETE FROM numbers; SELECT 1'", "postgres"),
])
def test_validate_readonly_sql_uses_target_dialect_ast(sql, dialect):
    with pytest.raises(ValueError):
        validate_readonly_sql(sql, dialect)


def test_validate_readonly_sql_returns_checked_ast_and_separates_parse_errors():
    expression = validate_readonly_sql("SELECT n FROM numbers;", "sqlite")
    assert [table.name for table in expression.find_all(exp.Table)] == ["numbers"]
    with pytest.raises(SqlParseError):
        validate_readonly_sql("SELECT 'unterminated", "sqlite")
    with pytest.raises(SqlParseError):
        validate_readonly_sql("SELECT FROM", "sqlite")


@pytest.fixture
def sqlite_executor(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    monkeypatch.setattr(executor, "_engine", engine)
    monkeypatch.setattr(executor.cfg, "DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setattr(executor.cfg, "EXECUTION_ENABLED", True)
    monkeypatch.setattr(executor.cfg, "EXECUTION_MAX_ROWS", 2)
    monkeypatch.setattr(executor.cfg, "EXECUTION_DISPLAY_ROWS", 1)
    monkeypatch.setattr(executor.cfg, "EXECUTION_TIMEOUT_SECONDS", 1)
    yield engine
    engine.dispose()


def test_warning_named_column_and_empty_result_are_successful(sqlite_executor):
    result = executor.execute('SELECT 1 AS "\u26a0合法列名"')
    assert result.success and result.error_code is None
    assert result.columns == ["\u26a0合法列名"]
    assert [tuple(row) for row in result.rows] == [(1,)]
    empty = executor.execute('SELECT 1 AS "\u26a0合法列名" WHERE 0')
    assert empty.success and not empty.truncated
    assert empty.columns == result.columns and empty.rows == []


def test_row_cap_and_display_cap_have_distinct_semantics(sqlite_executor):
    result = executor.execute("SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3")
    assert result.success and result.truncated
    assert [tuple(row) for row in result.rows] == [(1,), (2,)]
    exact = executor.execute("SELECT 1 AS n UNION ALL SELECT 2")
    assert exact.success and not exact.truncated
    assert exact.rows == result.rows


@pytest.mark.parametrize(("sql", "code"), [
    ("DELETE FROM orders", "sql_not_readonly"),
    ("SELECT '/*'; DELETE FROM orders; SELECT '*/'", "sql_not_readonly"),
    ("SELECT FROM", "execution_syntax_invalid"),
    ("SELECT missing FROM missing_table", "execution_reference_invalid"),
])
def test_sql_failures_have_stable_codes(sqlite_executor, sql, code):
    result = executor.execute(sql)
    assert not result.success and result.error_code == code
    assert result.rows == []


def test_execution_disabled_cannot_reach_database(sqlite_executor, monkeypatch):
    monkeypatch.setattr(executor.cfg, "EXECUTION_ENABLED", False)
    result = executor.execute("SELECT 1")
    assert not result.success and result.error_code == "execution_disabled"


def test_sqlite_authorizer_denial_has_permission_code(sqlite_executor):
    with sqlite_executor.connect() as conn:
        conn.connection.driver_connection.set_authorizer(
            lambda action, *args: sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_SELECT else sqlite3.SQLITE_OK
        )
    result = executor.execute("SELECT 1")
    assert not result.success and result.error_code == "execution_permission_denied"


def test_sqlite_timeout_is_failure_not_empty_result(sqlite_executor):
    result = executor.execute(
        "WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM numbers) "
        "SELECT sum(n) FROM numbers"
    )
    assert not result.success and result.error_code == "execution_timeout"
    assert executor.execute("SELECT 1").success
