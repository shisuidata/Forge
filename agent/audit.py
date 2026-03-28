"""
Forge 审计日志模块 — 基于 aiosqlite 的异步 SQLite 存储。

每次用户查询（无论成功与否）都会写入一条审计记录，记录完整的操作链路：
    用户原始问题 → 生成的 Forge JSON → 编译后的 SQL → 执行状态

状态流转：
    pending（SQL 已生成，等待用户确认）
        ↓ 用户确认 → approved（SQL 已确认，由调用方负责执行）
        ↓ 用户取消 → cancelled
        ↓ 生成失败 → error（附 error_message）

文件位置：
    forge_audit.db（SQLite 文件，与服务进程同目录）
    生产环境建议通过环境变量将 DB_PATH 指向持久化存储目录。

线程安全：
    全部操作为 async，基于 aiosqlite，适用于 FastAPI 的 asyncio 事件循环。
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import aiosqlite

# SQLite 数据库文件路径
DB_PATH = "forge_audit.db"

# 建表 DDL：IF NOT EXISTS 保证幂等，服务每次启动时调用不会报错
_DDL = """
CREATE TABLE IF NOT EXISTS audit_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp     TEXT    NOT NULL,          -- ISO 8601 UTC 时间戳
    user_id       TEXT    NOT NULL,          -- 飞书 open_id
    user_message  TEXT    NOT NULL,          -- 用户原始自然语言
    forge_json    TEXT,                      -- 生成的 Forge JSON（JSON 字符串）
    sql           TEXT,                      -- 编译后的 SQL
    status        TEXT    NOT NULL DEFAULT 'pending',  -- pending | approved | cancelled | error
    error_message TEXT                       -- 仅 status=error 时填写
);
"""


async def _ensure_schema() -> None:
    """
    确保 audit_log 表存在。

    每个公开函数调用前都会调用此方法，保证数据库在首次使用时自动初始化，
    无需在服务启动时单独执行迁移脚本。
    """
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(_DDL)
        await db.commit()


async def log(
    *,
    user_id:       str,
    user_message:  str,
    forge_json:    dict | None = None,
    sql:           str | None  = None,
    status:        str         = "pending",
    error_message: str | None  = None,
) -> int:
    """
    写入一条新的审计记录。

    Args:
        user_id:       飞书用户 open_id。
        user_message:  用户发送的原始自然语言查询。
        forge_json:    LLM 生成的 Forge JSON 字典；None 表示生成失败。
        sql:           编译后的 SQL 字符串；None 表示编译未执行。
        status:        初始状态，通常为 "pending" 或 "error"。
        error_message: 错误详情，仅 status="error" 时填写。

    Returns:
        新插入记录的自增 ID，可用于后续 update_status() 调用。
    """
    await _ensure_schema()
    # 使用 UTC 时间戳，避免时区混乱
    ts = datetime.now(timezone.utc).isoformat()
    # forge_json 序列化为字符串存储，读取时由调用方反序列化
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
    """
    查询最近的审计记录，按 id 倒序返回（最新的在前）。

    Args:
        limit: 返回条数上限，默认 50；管理后台展示时传入 100。

    Returns:
        字典列表，每条字典对应 audit_log 的一行记录。
    """
    await _ensure_schema()
    async with aiosqlite.connect(DB_PATH) as db:
        # Row 模式允许用列名访问字段，再转为普通 dict 方便序列化
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM audit_log ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def search(
    *,
    status: str = "",
    keyword: str = "",
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    """
    带筛选的审计记录查询（支持分页）。

    Args:
        status:  按状态过滤（pending/approved/cancelled/error），空字符串不过滤。
        keyword: 搜索用户消息或 SQL 中包含的关键词，空字符串不过滤。
        limit:   返回条数上限。
        offset:  分页偏移。

    Returns:
        (records, total_filtered) — 记录列表和符合条件的总数。
    """
    await _ensure_schema()
    conditions: list[str] = []
    params: list[Any] = []

    if status:
        conditions.append("status = ?")
        params.append(status)
    if keyword:
        conditions.append("(user_message LIKE ? OR sql LIKE ?)")
        params.extend([f"%{keyword}%", f"%{keyword}%"])

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # 总数
        count_cursor = await db.execute(f"SELECT COUNT(*) FROM audit_log{where}", params)
        total = (await count_cursor.fetchone())[0]
        # 分页查询
        query = f"SELECT * FROM audit_log{where} ORDER BY id DESC LIMIT ? OFFSET ?"
        cursor = await db.execute(query, params + [limit, offset])
        rows = await cursor.fetchall()
        return [dict(row) for row in rows], total


async def stats() -> dict[str, int]:
    """返回各状态的记录计数和总数。"""
    await _ensure_schema()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT status, COUNT(*) as cnt FROM audit_log GROUP BY status"
        )
        rows = await cursor.fetchall()
        result: dict[str, int] = {"total": 0}
        for row in rows:
            result[row["status"]] = row["cnt"]
            result["total"] += row["cnt"]
        return result


async def update_status(record_id: int, status: str) -> None:
    """
    更新指定审计记录的状态。

    在用户对 pending SQL 执行 approve/cancel 操作时调用，
    将状态从 "pending" 更新为 "approved" 或 "cancelled"。

    Args:
        record_id: log() 返回的记录 ID。
        status:    新状态值，应为 approved | cancelled | error 之一。
    """
    await _ensure_schema()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE audit_log SET status = ? WHERE id = ?",
            (status, record_id),
        )
        await db.commit()
