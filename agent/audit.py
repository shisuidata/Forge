"""
Forge audit log store — backed by SQLite via aiosqlite.

Every query attempt is recorded with:
- timestamp
- user_id       (Feishu open_id)
- user_message  (original natural language)
- forge_json    (generated Forge JSON, serialised as string)
- sql           (compiled SQL)
- status        pending | approved | cancelled | error
- error_message (populated when status == "error")
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import aiosqlite

DB_PATH = "forge_audit.db"

_DDL = """
CREATE TABLE IF NOT EXISTS audit_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp     TEXT    NOT NULL,
    user_id       TEXT    NOT NULL,
    user_message  TEXT    NOT NULL,
    forge_json    TEXT,
    sql           TEXT,
    status        TEXT    NOT NULL DEFAULT 'pending',
    error_message TEXT
);
"""


async def _ensure_schema() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(_DDL)
        await db.commit()


async def log(
    *,
    user_id: str,
    user_message: str,
    forge_json: dict | None = None,
    sql: str | None = None,
    status: str = "pending",
    error_message: str | None = None,
) -> int:
    """Write a new audit record. Returns the inserted row id."""
    await _ensure_schema()
    ts = datetime.now(timezone.utc).isoformat()
    forge_str = json.dumps(forge_json, ensure_ascii=False) if forge_json is not None else None
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            INSERT INTO audit_log
                (timestamp, user_id, user_message, forge_json, sql, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, user_id, user_message, forge_str, sql, status, error_message),
        )
        await db.commit()
        return cursor.lastrowid  # type: ignore[return-value]


async def recent(limit: int = 50) -> list[dict[str, Any]]:
    """Return the most recent *limit* audit records, newest first."""
    await _ensure_schema()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM audit_log ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def update_status(record_id: int, status: str) -> None:
    """Update the status of an existing audit record."""
    await _ensure_schema()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE audit_log SET status = ? WHERE id = ?",
            (status, record_id),
        )
        await db.commit()
