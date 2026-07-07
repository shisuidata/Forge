"""
用户 SQL 反馈队列。

当用户指出 SQL 错误、结果不对、口径不一致时，将反馈结构化保存，
后续由管理员转成 Registry 规则、lint 规则或回归测试。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import aiosqlite

from agent import audit

_DDL = """
CREATE TABLE IF NOT EXISTS feedback_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp      TEXT NOT NULL,
    user_id        TEXT NOT NULL,
    audit_id       INTEGER,
    question       TEXT,
    sql            TEXT,
    feedback_type  TEXT NOT NULL,
    message        TEXT NOT NULL,
    expected       TEXT,
    status         TEXT NOT NULL DEFAULT 'pending'
);
"""


async def _ensure_schema() -> None:
    await audit._ensure_schema()
    async with aiosqlite.connect(audit._db_path()) as db:
        await db.execute(_DDL)
        await db.commit()


async def submit(
    *,
    user_id: str,
    feedback_type: str,
    message: str,
    audit_id: int | None = None,
    question: str | None = None,
    sql: str | None = None,
    expected: str | None = None,
) -> int:
    if not message.strip():
        raise ValueError("反馈内容不能为空。")
    if feedback_type not in {"wrong_result", "wrong_sql", "metric_definition", "missing_context", "other"}:
        raise ValueError(f"未知反馈类型：{feedback_type}")

    await _ensure_schema()
    ts = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(audit._db_path()) as db:
        cursor = await db.execute(
            """
            INSERT INTO feedback_log
                (timestamp, user_id, audit_id, question, sql, feedback_type, message, expected, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')
            """,
            (ts, user_id, audit_id, question, sql, feedback_type, message, expected),
        )
        await db.commit()
        return int(cursor.lastrowid)


async def list_pending(limit: int = 100) -> list[dict[str, Any]]:
    await _ensure_schema()
    async with aiosqlite.connect(audit._db_path()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT * FROM feedback_log
            WHERE status = 'pending'
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def update_status(feedback_id: int, status: str) -> None:
    if status not in {"pending", "triaged", "converted", "ignored"}:
        raise ValueError(f"未知反馈状态：{status}")
    await _ensure_schema()
    async with aiosqlite.connect(audit._db_path()) as db:
        await db.execute(
            "UPDATE feedback_log SET status = ? WHERE id = ?",
            (status, feedback_id),
        )
        await db.commit()
