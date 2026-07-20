"""
SQL 执行器 — 连接数据库执行 SQL 并返回格式化结果。

支持所有 SQLAlchemy 方言（SQLite / PostgreSQL / MySQL 等）。
数据库 URL 从 cfg.DATABASE_URL 读取。

返回值：纯文本表格，直接发送给用户。
"""
from __future__ import annotations

from contextlib import contextmanager
import logging
import re
import time
from typing import Any

from config import cfg

logger = logging.getLogger(__name__)

_engine = None

_FORBIDDEN_SQL_KEYWORDS = {
    "alter",
    "attach",
    "call",
    "copy",
    "create",
    "delete",
    "detach",
    "drop",
    "execute",
    "grant",
    "insert",
    "merge",
    "pragma",
    "replace",
    "revoke",
    "truncate",
    "update",
    "vacuum",
}


def _get_engine():
    """Return a process-wide SQLAlchemy engine for query execution."""
    global _engine
    if _engine is None:
        from sqlalchemy import create_engine

        _engine = create_engine(cfg.DATABASE_URL)
    return _engine


def _apply_statement_timeout(conn, timeout_seconds: int) -> None:
    """Apply a best-effort per-statement timeout for supported dialects."""
    if timeout_seconds <= 0:
        return
    dialect = conn.dialect.name
    if dialect == "postgresql":
        conn.exec_driver_sql("SET LOCAL statement_timeout = %s", (timeout_seconds * 1000,))
    elif dialect in {"mysql", "mariadb"}:
        conn.exec_driver_sql(f"SET SESSION max_execution_time = {timeout_seconds * 1000}")


@contextmanager
def _sqlite_timeout_guard(conn, timeout_seconds: int):
    """Interrupt SQLite VM work when a query exceeds the configured timeout."""
    if conn.dialect.name != "sqlite" or timeout_seconds <= 0:
        yield
        return
    raw = conn.connection.driver_connection
    deadline = time.monotonic() + timeout_seconds

    def abort_if_expired() -> int:
        return 1 if time.monotonic() > deadline else 0

    raw.set_progress_handler(abort_if_expired, 1000)
    try:
        yield
    finally:
        raw.set_progress_handler(None, 0)


def _strip_sql_literals_and_comments(sql: str) -> str:
    """Remove strings and comments so keyword checks do not scan user values."""
    without_block_comments = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    without_line_comments = re.sub(r"--[^\n\r]*", " ", without_block_comments)
    without_single_quotes = re.sub(r"'(?:''|[^'])*'", "''", without_line_comments)
    without_double_quotes = re.sub(r'"(?:""|[^"])*"', '""', without_single_quotes)
    return without_double_quotes


def validate_readonly_sql(sql: str) -> None:
    """
    Reject SQL that is not a single read-only SELECT/WITH query.

    The executor is intended for reviewed analytics queries. This guard is not a
    substitute for a database read-only role, but it prevents obvious mutating
    statements from the web API and Feishu approval path.
    """
    normalized = _strip_sql_literals_and_comments(sql).strip()
    if not normalized:
        raise ValueError("SQL 为空。")

    # Allow one trailing semicolon, reject stacked statements.
    body = normalized[:-1].strip() if normalized.endswith(";") else normalized
    if ";" in body:
        raise ValueError("只允许执行单条 SQL 查询。")

    first = re.match(r"([A-Za-z_]\w*)", body)
    first_token = first.group(1).lower() if first else ""
    if first_token not in {"select", "with"}:
        raise ValueError("只允许执行只读 SELECT/WITH 查询。")

    keywords = {m.group(1).lower() for m in re.finditer(r"\b([A-Za-z_]\w*)\b", body)}
    forbidden = sorted(keywords & _FORBIDDEN_SQL_KEYWORDS)
    if forbidden:
        raise ValueError(f"SQL 包含非只读关键字：{', '.join(forbidden)}。")


def execute(sql: str, max_rows: int = 50) -> str:
    """
    执行 SQL，返回格式化的纯文本结果。

    Args:
        sql:      要执行的 SQL 字符串
        max_rows: 最多返回的行数，超出时附加提示

    Returns:
        格式化后的结果字符串；执行失败时返回错误说明。
    """
    if not cfg.EXECUTION_ENABLED:
        return "⚠ SQL 执行已被配置禁用。"
    if not cfg.DATABASE_URL:
        return "⚠ 未配置数据库连接（DATABASE_URL），无法执行查询。"

    try:
        from sqlalchemy import text
    except ImportError:
        return "⚠ 缺少依赖：请运行 pip install sqlalchemy"

    try:
        validate_readonly_sql(sql)
        max_rows = _bounded_max_rows(max_rows)
        timeout_seconds = _bounded_timeout_seconds()
        engine = _get_engine()
        with engine.connect() as conn:
            _apply_statement_timeout(conn, timeout_seconds)
            with _sqlite_timeout_guard(conn, timeout_seconds):
                result = conn.execute(text(sql))
                rows   = result.fetchmany(max_rows + 1)
                cols   = list(result.keys())
    except Exception as exc:
        logger.error("SQL execution failed: %s", exc)
        return f"⚠ 执行失败：{exc}"

    if not rows:
        return "查询完成，结果为空。"

    truncated = len(rows) > max_rows
    if truncated:
        rows = rows[:max_rows]

    # 计算每列宽度
    col_widths = [len(str(c)) for c in cols]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val) if val is not None else "NULL"))

    def fmt_row(vals: list[Any]) -> str:
        parts = [str(v if v is not None else "NULL").ljust(col_widths[i]) for i, v in enumerate(vals)]
        return "  ".join(parts)

    sep   = "  ".join("-" * w for w in col_widths)
    lines = [fmt_row(cols), sep] + [fmt_row(list(r)) for r in rows]

    if truncated:
        lines.append(f"（仅显示前 {max_rows} 行）")

    return "\n".join(lines)


def execute_with_data(
    sql: str, max_rows: int = 200
) -> tuple[str, list[str], list[tuple]]:
    """
    执行 SQL，同时返回格式化文本和原始数据（供图表使用）。

    Returns:
        (text, cols, rows)  — text 同 execute()；cols/rows 用于图表生成
    """
    if not cfg.EXECUTION_ENABLED:
        return "⚠ SQL 执行已被配置禁用。", [], []
    if not cfg.DATABASE_URL:
        return "⚠ 未配置数据库连接（DATABASE_URL），无法执行查询。", [], []

    try:
        from sqlalchemy import text as sa_text
    except ImportError:
        return "⚠ 缺少依赖：请运行 pip install sqlalchemy", [], []

    try:
        validate_readonly_sql(sql)
        max_rows = _bounded_max_rows(max_rows)
        timeout_seconds = _bounded_timeout_seconds()
        engine = _get_engine()
        with engine.connect() as conn:
            _apply_statement_timeout(conn, timeout_seconds)
            with _sqlite_timeout_guard(conn, timeout_seconds):
                result = conn.execute(sa_text(sql))
                rows   = result.fetchmany(max_rows + 1)
                cols   = list(result.keys())
    except Exception as exc:
        logger.error("SQL execution failed: %s", exc)
        return f"⚠ 执行失败：{exc}", [], []

    if not rows:
        return "查询完成，结果为空。", cols, []

    truncated = len(rows) > max_rows
    if truncated:
        rows = rows[:max_rows]

    # 格式化文本时再限制展示行数，原始 rows 仍保留给图表/分析。
    display_limit = _bounded_display_rows()
    display_rows = rows[:display_limit]
    col_widths = [len(str(c)) for c in cols]
    for row in display_rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val) if val is not None else "NULL"))

    def fmt_row(vals):
        parts = [str(v if v is not None else "NULL").ljust(col_widths[i]) for i, v in enumerate(vals)]
        return "  ".join(parts)

    sep   = "  ".join("-" * w for w in col_widths)
    lines = [fmt_row(cols), sep] + [fmt_row(list(r)) for r in display_rows]
    if truncated or len(rows) > display_limit:
        lines.append(f"（显示前 {len(display_rows)} 行，共 {len(rows)} 行）")

    return "\n".join(lines), cols, list(rows)


def _bounded_max_rows(requested: int) -> int:
    """Clamp requested result size to the deployment-level hard cap."""
    hard_cap = max(1, int(getattr(cfg, "EXECUTION_MAX_ROWS", 200) or 200))
    return max(1, min(int(requested or hard_cap), hard_cap))


def _bounded_display_rows() -> int:
    """Return the configured text-display row cap."""
    display_cap = max(1, int(getattr(cfg, "EXECUTION_DISPLAY_ROWS", 50) or 50))
    max_rows = max(1, int(getattr(cfg, "EXECUTION_MAX_ROWS", 200) or 200))
    return min(display_cap, max_rows)


def _bounded_timeout_seconds() -> int:
    """Return the configured query timeout; 0 disables timeout."""
    timeout = int(getattr(cfg, "EXECUTION_TIMEOUT_SECONDS", 30) or 0)
    return max(0, timeout)
