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


def get_connection_raw():
    """
    获取原始 DBAPI 连接（sqlite3.Connection 或 psycopg2.connection）。

    用于需要 cursor-level 操作的场景（EMS / SMP 等裸 SQL 模块）。
    SQLite 模式下直接返回 sqlite3 连接（单例，WAL 模式已在 Engine 层启用）。
    PostgreSQL 模式下返回 psycopg2 连接。
    """
    engine = get_engine()
    if str(engine.url).startswith("sqlite"):
        # SQLite: 使用固定文件连接（非池化），兼容旧代码的 check_same_thread=False
        import sqlite3
        url = str(engine.url)
        db_path = url.replace("sqlite:///", "")
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn
    else:
        return engine.raw_connection()


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
        ddl = ddl.replace("BOOLEAN", "BOOLEAN")  # 兼容

    with engine.begin() as conn:
        for statement in ddl.split(";"):
            statement = statement.strip()
            if statement and not statement.startswith("--"):
                try:
                    conn.execute(text(statement))
                except Exception as exc:
                    # IF NOT EXISTS 失败时忽略（表已存在）
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
