"""
SQL 执行器 — 返回结构化成功/失败、稳定错误码、数据与独立展示文本。

支持所有 SQLAlchemy 方言（SQLite / PostgreSQL / MySQL 等）。
数据库 URL 从 cfg.DATABASE_URL 读取。

空结果与截断是成功语义，展示文字或列名不参与领域状态判定。
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import logging
import time

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.errors import SqlglotError
from sqlglot.tokens import TokenType

from config import cfg
from forge.dialects import DialectResolutionError, resolve_dialect, sqlglot_dialect

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
    "into",
}

# Read-only syntax can still mutate state, sleep, lock, touch files or run nested SQL
# through these functions. This list is defence in depth; a read-only database role
# remains the final boundary.
_SIDE_EFFECT_FUNCTIONS = frozenset({
    "nextval", "setval", "set_config", "pg_notify",
    "pg_sleep", "pg_sleep_for", "pg_sleep_until", "sleep", "benchmark",
    "pg_advisory_lock", "pg_advisory_lock_shared", "pg_advisory_xact_lock",
    "pg_advisory_xact_lock_shared", "pg_try_advisory_lock", "pg_try_advisory_lock_shared",
    "pg_try_advisory_xact_lock", "pg_try_advisory_xact_lock_shared",
    "get_lock", "release_lock", "release_all_locks",
    "lo_import", "lo_export", "lo_unlink", "lo_create", "lo_from_bytea", "lo_put",
    "pg_read_file", "pg_read_binary_file", "pg_ls_dir", "pg_stat_file", "load_file",
    "pg_terminate_backend", "pg_cancel_backend", "pg_reload_conf", "pg_rotate_logfile",
    "query_to_xml", "query_to_xml_and_xmlschema", "query_to_xmlschema",
    "cursor_to_xml", "cursor_to_xmlschema",
    "dblink", "dblink_exec", "dblink_connect", "dblink_send_query",
    "load_extension", "sys_exec", "sys_eval",
})

_NON_KEYWORD_TOKENS = frozenset({
    TokenType.STRING, TokenType.NATIONAL_STRING, TokenType.RAW_STRING,
    TokenType.BIT_STRING, TokenType.HEX_STRING, TokenType.BYTE_STRING,
    TokenType.HEREDOC_STRING, TokenType.UNICODE_STRING, TokenType.IDENTIFIER,
})

_MUTATING_NODES = tuple(getattr(exp, name) for name in (
    "Into", "Lock", "Insert", "Update", "Delete", "Merge", "Create", "Drop", "Alter",
    "Command", "TruncateTable", "Copy", "Pragma", "Grant", "Revoke", "Transaction",
    "Commit", "Rollback", "LoadData", "Attach", "Detach", "Use",
))


class SqlParseError(ValueError):
    """SQL cannot be tokenized or parsed for the target dialect."""


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


def validate_readonly_sql(sql: str, dialect: str | None = None) -> exp.Expression:
    """
    Reject SQL that is not a single read-only SELECT/WITH query; return its AST.

    Strings, comments and statement boundaries come from the dialect's own
    tokenizer, so the checked text is the text the database will run. Callers
    that inspect resources must use the returned AST. ``dialect`` is a SQLGlot
    dialect name; ``None`` uses the generic parser. This guard is not a
    substitute for a database read-only role.
    """
    if not sql.strip():
        raise ValueError("SQL 为空。")
    try:
        tokens = Dialect.get_or_raise(dialect).tokenize(sql)
    except SqlglotError as exc:
        raise SqlParseError("SQL 无法按所选 dialect 解析。") from exc

    while tokens and tokens[-1].token_type == TokenType.SEMICOLON:
        tokens = tokens[:-1]
    if not tokens:
        raise ValueError("SQL 为空。")
    if any(token.token_type == TokenType.SEMICOLON for token in tokens):
        raise ValueError("只允许执行单条 SQL 查询。")
    if tokens[0].text.lower() not in {"select", "with"}:
        raise ValueError("只允许执行只读 SELECT/WITH 查询。")
    keywords = {
        token.text.lower() for token in tokens if token.token_type not in _NON_KEYWORD_TOKENS
    }
    forbidden = sorted(keywords & _FORBIDDEN_SQL_KEYWORDS)
    if forbidden:
        raise ValueError(f"SQL 包含非只读关键字：{', '.join(forbidden)}。")

    try:
        statements = [s for s in sqlglot.parse(sql, read=dialect) if s is not None]
    except SqlglotError as exc:
        raise SqlParseError("SQL 无法按所选 dialect 解析。") from exc
    if len(statements) != 1:
        raise ValueError("只允许执行单条 SQL 查询。")
    expression = statements[0]
    if not isinstance(expression, exp.Query):
        raise ValueError("只允许执行只读 SELECT/WITH 查询。")
    if expression.find(*_MUTATING_NODES) is not None:
        raise ValueError("SQL 包含写入、锁定或非查询语句。")
    for function in expression.find_all(exp.Func):
        name = function.name if isinstance(function, exp.Anonymous) else function.sql_name()
        if name.lower() in _SIDE_EFFECT_FUNCTIONS:
            raise ValueError(f"SQL 调用了有副作用的函数：{name.lower()}。")
    return expression


@dataclass(frozen=True)
class ExecutionResult:
    """Execution outcome; presentation text never determines success."""

    success: bool
    text: str
    columns: list[str]
    rows: list[tuple]
    truncated: bool = False
    error_code: str | None = None


def _failure(code: str, message: str) -> ExecutionResult:
    return ExecutionResult(False, message, [], [], error_code=code)


def execute(sql: str, max_rows: int = 200) -> ExecutionResult:
    """Execute one reviewed read-only query with a structured, bounded outcome."""
    if not cfg.EXECUTION_ENABLED:
        return _failure("execution_disabled", "SQL 执行已被配置禁用。")
    if not cfg.DATABASE_URL:
        return _failure("datasource_unconfigured", "未配置数据库连接，无法执行查询。")

    try:
        from sqlalchemy import text as sa_text
    except ImportError:
        return _failure("execution_dependency_missing", "缺少数据库执行依赖。")

    try:
        target = resolve_dialect(configured_dialect=cfg.SQL_DIALECT,
                                 database_url=cfg.DATABASE_URL, allow_generic=True)
    except DialectResolutionError:
        return _failure("dialect_unsupported", "数据源方言不受支持，无法安全校验 SQL。")
    try:
        validate_readonly_sql(sql, sqlglot_dialect(target.resolved))
    except SqlParseError:
        return _failure("execution_syntax_invalid", "SQL 语法错误，请重新生成。")
    except ValueError as exc:
        return _failure("sql_not_readonly", str(exc))

    try:
        max_rows = _bounded_max_rows(max_rows)
        timeout_seconds = _bounded_timeout_seconds()
        engine = _get_engine()
        with engine.connect() as conn:
            _apply_statement_timeout(conn, timeout_seconds)
            with _sqlite_timeout_guard(conn, timeout_seconds):
                result = conn.execute(sa_text(sql))
                rows = result.fetchmany(max_rows + 1)
                cols = list(result.keys())
    except Exception as exc:
        # Never log SQL, bound values, credentials or raw driver messages.
        logger.error("SQL execution failed: %s", type(exc).__name__)
        return _execution_failure(exc)

    truncated = len(rows) > max_rows
    if truncated:
        rows = rows[:max_rows]
    return ExecutionResult(True, _render_result(cols, rows, truncated), cols, rows, truncated)


def _render_result(cols: list[str], rows: list[tuple], truncated: bool) -> str:
    if not rows:
        return "查询完成，结果为空。"
    display_limit = _bounded_display_rows()
    display_rows = rows[:display_limit]
    col_widths = [len(str(c)) for c in cols]
    for row in display_rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val) if val is not None else "NULL"))

    def fmt_row(vals):
        parts = [str(v if v is not None else "NULL").ljust(col_widths[i]) for i, v in enumerate(vals)]
        return "  ".join(parts)

    sep = "  ".join("-" * w for w in col_widths)
    lines = [fmt_row(cols), sep] + [fmt_row(r) for r in display_rows]
    if truncated or len(rows) > display_limit:
        lines.append(f"（显示前 {len(display_rows)} 行，共 {len(rows)} 行）")
    return "\n".join(lines)


def _execution_failure(exc: Exception) -> ExecutionResult:
    """Normalize driver errors to stable public codes without exposing driver details."""
    original = getattr(exc, "orig", exc)
    sqlstate = getattr(original, "sqlstate", None) or getattr(original, "pgcode", None)
    sqlite_code = getattr(original, "sqlite_errorcode", None)
    message = str(original).lower()
    if sqlstate == "57014" or sqlite_code == 9 or any(
        token in message for token in ("timeout", "timed out", "interrupted", "canceling statement")
    ):
        return _failure("execution_timeout", "查询超时，请缩小查询范围后重试。")
    if sqlstate == "42501" or sqlite_code in {8, 23} or any(
        token in message for token in ("permission denied", "readonly", "read-only", "not authorized", "prohibited")
    ):
        return _failure("execution_permission_denied", "数据库拒绝了该操作，请确认只读权限和查询范围。")
    if sqlstate in {"42703", "42P01"} or any(
        token in message for token in ("no such column", "no such table", "undefined column")
    ):
        return _failure("execution_reference_invalid", "SQL 引用了不存在或未绑定的表/字段，请重新生成。")
    if sqlstate == "42601" or "syntax error" in message or "incomplete input" in message:
        return _failure("execution_syntax_invalid", "SQL 语法错误，请重新生成。")
    return _failure("execution_failed", "数据库查询失败，请检查 SQL 或联系管理员。")


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
