"""
数据库抽象层 — SQLite / PostgreSQL 统一接口。

根据 cfg.MEMORY_DB_URL 自动选择后端：
    - 空 / sqlite:///  → SQLite（默认，零配置）
    - postgresql://    → PostgreSQL（生产环境，支持并发）

用法：
    from agent.db import get_engine, get_connection

    engine = get_engine()                    # SQLAlchemy Engine
    with get_connection() as conn:           # 自动提交的连接
        conn.execute(text("SELECT 1"))

所有记忆模块（EMS / SMP / knowledge / tenant）统一使用此模块获取数据库连接。
"""
from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_engine: Engine | None = None


def _build_url() -> str:
    """从 config 构建数据库 URL。"""
    try:
        from config import cfg
        if cfg.MEMORY_DB_URL:
            return cfg.MEMORY_DB_URL
        # 默认 SQLite
        db_path = Path(cfg.MEMORY_DB_PATH)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite:///{db_path.resolve()}"
    except (ImportError, AttributeError):
        Path(".forge").mkdir(parents=True, exist_ok=True)
        return "sqlite:///.forge/memory.db"


def get_engine() -> Engine:
    """获取全局 SQLAlchemy Engine（单例）。"""
    global _engine
    if _engine is None:
        url = _build_url()
        is_sqlite = url.startswith("sqlite")

        kwargs = {}
        if is_sqlite:
            kwargs["connect_args"] = {"check_same_thread": False}
        else:
            # PostgreSQL 连接池配置
            kwargs["pool_size"] = 10
            kwargs["max_overflow"] = 20
            kwargs["pool_pre_ping"] = True

        _engine = create_engine(url, **kwargs)

        # SQLite: 启用 WAL 模式（并发读写）
        if is_sqlite:
            @event.listens_for(_engine, "connect")
            def _set_sqlite_pragma(dbapi_conn, connection_record):
                cursor = dbapi_conn.cursor()
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA busy_timeout=5000")
                cursor.close()

        logger.info("Database engine created: %s", url.split("@")[-1] if "@" in url else url)

    return _engine


def get_connection():
    """
    获取 SQLAlchemy Connection（上下文管理器）。

    用法：
        with get_connection() as conn:
            conn.execute(text("INSERT INTO ..."))
    """
    return get_engine().connect()


class _UnifiedCursor:
    """统一游标接口：兼容 sqlite3 和 psycopg2 的 cursor。"""

    def __init__(self, cur, is_pg: bool, has_returning: bool = False):
        self._cur = cur
        self._is_pg = is_pg
        # psycopg2 INSERT + RETURNING id：立即取出 id，避免后续 fetch 混乱
        self._cached_lastrowid: int | None = None
        if has_returning:
            row = cur.fetchone()
            self._cached_lastrowid = row[0] if row else None

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    @property
    def lastrowid(self) -> int | None:
        if self._is_pg:
            return self._cached_lastrowid
        return self._cur.lastrowid

    @property
    def rowcount(self) -> int:
        return self._cur.rowcount


class _UnifiedConn:
    """
    统一连接接口：让 EMS/SMP/knowledge 的 sqlite3 风格代码同时兼容 PostgreSQL。

    - conn.execute(sql, params) → _UnifiedCursor（自动 ?→%s，自动 RETURNING id）
    - conn.commit()
    - conn.close()
    """

    def __init__(self, raw_conn, is_pg: bool):
        self._conn = raw_conn
        self._is_pg = is_pg

    def execute(self, sql: str, params=()) -> _UnifiedCursor:
        if self._is_pg:
            sql = sql.replace("?", "%s")
        cur = self._conn.cursor()

        # psycopg2 INSERT：自动追加 RETURNING id 以获取 lastrowid
        is_insert = sql.strip().upper().startswith("INSERT")
        has_returning = False
        if self._is_pg and is_insert and "RETURNING" not in sql.upper():
            sql = sql.rstrip().rstrip(";") + " RETURNING id"
            has_returning = True

        cur.execute(sql, params)
        return _UnifiedCursor(cur, self._is_pg, has_returning=has_returning)

    def commit(self):
        self._conn.commit()

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass


def get_connection_raw() -> _UnifiedConn:
    """
    获取统一封装的数据库连接。

    返回 _UnifiedConn，同时兼容 SQLite（sqlite3 风格）和 PostgreSQL（psycopg2）。
    EMS / SMP / knowledge 等模块直接调用 conn.execute() / conn.commit()，无需感知方言差异。
    """
    engine = get_engine()
    is_pg = not str(engine.url).startswith("sqlite")

    if not is_pg:
        import sqlite3
        url = str(engine.url)
        db_path = url.replace("sqlite:///", "")
        raw = sqlite3.connect(db_path, check_same_thread=False)
        raw.execute("PRAGMA journal_mode=WAL")
        raw.execute("PRAGMA busy_timeout=5000")
    else:
        raw = engine.raw_connection()

    return _UnifiedConn(raw, is_pg)


def execute_ddl(ddl: str) -> None:
    """执行 DDL 语句（建表等），自动处理方言差异。"""
    engine = get_engine()
    is_sqlite = str(engine.url).startswith("sqlite")

    # 方言适配
    if not is_sqlite:
        # SQLite → PostgreSQL 语法转换
        ddl = ddl.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
        ddl = ddl.replace("datetime('now','utc')", "NOW() AT TIME ZONE 'UTC'")
        ddl = ddl.replace("datetime('now','localtime')", "NOW()")
        ddl = ddl.replace("DEFAULT 1,", "DEFAULT TRUE,")   # BOOLEAN DEFAULT 1 → TRUE
        ddl = ddl.replace("DEFAULT 0,", "DEFAULT FALSE,")  # BOOLEAN DEFAULT 0 → FALSE

    # 每条语句独立事务，避免单条失败回滚其他建表语句
    for statement in ddl.split(";"):
        statement = statement.strip()
        if not statement or statement.startswith("--"):
            continue
        try:
            with engine.begin() as conn:
                conn.execute(text(statement))
        except Exception as exc:
            if "already exists" in str(exc).lower():
                continue
            logger.warning("DDL execution warning: %s", exc)


def is_postgres() -> bool:
    """当前是否使用 PostgreSQL。"""
    return not str(get_engine().url).startswith("sqlite")


def adapt_sql(sql: str) -> str:
    """
    将 SQLite 风格的 SQL 适配到当前方言。

    SQLite → PostgreSQL:
        ? → %s（占位符）
        datetime('now','utc') → NOW() AT TIME ZONE 'UTC'
        AUTOINCREMENT → 无（SERIAL 已在 DDL 处理）
    """
    if not is_postgres():
        return sql
    sql = sql.replace("?", "%s")
    sql = sql.replace("datetime('now','utc')", "NOW() AT TIME ZONE 'UTC'")
    sql = sql.replace("datetime('now','localtime')", "NOW()")
    return sql
