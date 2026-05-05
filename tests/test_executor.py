from __future__ import annotations

import pytest

from forge.executor import validate_readonly_sql


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
