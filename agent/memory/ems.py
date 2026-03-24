"""
EMS — Episodic Memory Store（情景记忆层）

全量保留每轮对话的完整记录，只追加不修改。
是 SMP 提炼和 WMB 构建的唯一事实来源。

存储内容：
    - 用户消息、助手回复、系统注入、工具调用
    - 可变状态（pending_sql 等）建模为事件，从事件流还原

Session 边界：
    - 超过 SESSION_TIMEOUT_MIN 无交互自动开新 session
    - 显式 reset 也开新 session

保留策略：
    - 默认无限保留
    - 可配置 retention_days，定期清理过期记录
"""
from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

logger = logging.getLogger(__name__)

# ── 配置 ──────────────────────────────────────────────────────────────────────

try:
    from config import cfg as _cfg
    SESSION_TIMEOUT_MIN = _cfg.MEMORY_SESSION_TIMEOUT
    DB_PATH = Path(_cfg.MEMORY_DB_PATH)
except (ImportError, AttributeError):
    SESSION_TIMEOUT_MIN = 30
    DB_PATH = Path(".forge/memory.db")

# ── 角色类型 ──────────────────────────────────────────────────────────────────

Role = Literal["user", "assistant", "system", "tool", "state"]


# ── DDL ───────────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS memory_ems (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id  TEXT    NOT NULL,
    user_id     TEXT    NOT NULL,
    seq         INTEGER NOT NULL,
    role        TEXT    NOT NULL,
    content     TEXT    NOT NULL DEFAULT '',
    tool_name   TEXT,
    tool_input  TEXT,
    tool_output TEXT,
    action      TEXT,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now','utc')),

    UNIQUE(session_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_ems_session ON memory_ems(session_id, seq);
CREATE INDEX IF NOT EXISTS idx_ems_user    ON memory_ems(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_ems_state   ON memory_ems(user_id, role, action)
    WHERE role = 'state';
"""


# ── EMS Store ─────────────────────────────────────────────────────────────────

class EpisodicMemoryStore:
    """情景记忆存储。线程安全（SQLite WAL 模式 + 连接级锁）。"""

    def __init__(self, db_path: Path | str | None = None):
        self._db_path = Path(db_path) if db_path else DB_PATH
        self._conn: sqlite3.Connection | None = None
        # 用户当前 session 缓存：{user_id: (session_id, last_seq, last_active)}
        self._sessions: dict[str, tuple[str, int, datetime]] = {}
        self._init_db()

    def _init_db(self) -> None:
        try:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(
                str(self._db_path),
                check_same_thread=False,
                isolation_level="DEFERRED",
            )
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(_DDL)
            self._conn.commit()
        except (sqlite3.Error, OSError) as exc:
            logger.warning("EMS DB init failed: %s", exc)
            self._conn = None

    def _ensure_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._init_db()
        if self._conn is None:
            raise RuntimeError("EMS database unavailable")
        return self._conn

    # ── Session 管理 ──────────────────────────────────────────────────────────

    def _get_or_create_session(self, user_id: str) -> tuple[str, int]:
        """
        获取用户当前 session_id 和下一个 seq。
        超过 SESSION_TIMEOUT_MIN 自动新建。
        """
        now = datetime.now(timezone.utc)

        if user_id in self._sessions:
            sid, last_seq, last_active = self._sessions[user_id]
            if (now - last_active) < timedelta(minutes=SESSION_TIMEOUT_MIN):
                next_seq = last_seq + 1
                self._sessions[user_id] = (sid, next_seq, now)
                return sid, next_seq

        # 尝试从 DB 恢复最近的 session
        conn = self._ensure_conn()
        row = conn.execute(
            "SELECT session_id, seq, created_at FROM memory_ems "
            "WHERE user_id = ? ORDER BY id DESC LIMIT 1",
            (user_id,),
        ).fetchone()

        if row:
            last_sid, last_seq, last_ts = row
            try:
                last_active = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
                if last_active.tzinfo is None:
                    last_active = last_active.replace(tzinfo=timezone.utc)
            except (ValueError, AttributeError):
                last_active = datetime.min.replace(tzinfo=timezone.utc)

            if (now - last_active) < timedelta(minutes=SESSION_TIMEOUT_MIN):
                next_seq = last_seq + 1
                self._sessions[user_id] = (last_sid, next_seq, now)
                return last_sid, next_seq

        # 新建 session
        new_sid = f"s_{uuid.uuid4().hex[:12]}"
        self._sessions[user_id] = (new_sid, 1, now)
        return new_sid, 1

    def current_session_id(self, user_id: str) -> str:
        """获取用户当前 session_id。"""
        sid, _ = self._get_or_create_session(user_id)
        return sid

    # ── 写入 ──────────────────────────────────────────────────────────────────

    def record(
        self,
        user_id: str,
        role: Role,
        content: str = "",
        *,
        tool_name: str | None = None,
        tool_input: str | None = None,
        tool_output: str | None = None,
        action: str | None = None,
    ) -> int:
        """
        追加一条情景记忆。返回记录 ID。

        Args:
            user_id:     用户标识
            role:        user / assistant / system / tool / state
            content:     消息内容
            tool_name:   工具名（generate_forge_query / define_metric 等）
            tool_input:  工具输入 JSON 字符串
            tool_output: 工具输出
            action:      动作类型（sql_review / approved / cancelled / error / state_set / state_cleared）
        """
        conn = self._ensure_conn()
        session_id, seq = self._get_or_create_session(user_id)

        cursor = conn.execute(
            "INSERT INTO memory_ems "
            "(session_id, user_id, seq, role, content, tool_name, tool_input, tool_output, action) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (session_id, user_id, seq, role, content,
             tool_name, tool_input, tool_output, action),
        )
        conn.commit()
        return cursor.lastrowid  # type: ignore[return-value]

    # ── 状态管理（事件溯源）────────────────────────────────────────────────────

    def set_state(self, user_id: str, key: str, value: Any) -> None:
        """设置可变状态（建模为 EMS 事件）。"""
        self.record(
            user_id,
            role="state",
            content=json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value,
            action="state_set",
            tool_name=key,
        )

    def clear_state(self, user_id: str, key: str) -> None:
        """清除可变状态。"""
        self.record(
            user_id,
            role="state",
            content="",
            action="state_cleared",
            tool_name=key,
        )

    def get_state(self, user_id: str, key: str) -> Any | None:
        """
        从事件流还原状态：取该 key 最后一条 state 事件。
        state_set → 返回 value；state_cleared → 返回 None。
        """
        conn = self._ensure_conn()
        row = conn.execute(
            "SELECT action, content FROM memory_ems "
            "WHERE user_id = ? AND role = 'state' AND tool_name = ? "
            "ORDER BY id DESC LIMIT 1",
            (user_id, key),
        ).fetchone()

        if not row:
            return None
        action, content = row
        if action == "state_cleared":
            return None
        # 尝试 JSON 反序列化
        try:
            return json.loads(content)
        except (json.JSONDecodeError, TypeError):
            return content

    # ── 读取 ──────────────────────────────────────────────────────────────────

    def get_session_messages(
        self,
        session_id: str,
        roles: tuple[str, ...] = ("user", "assistant"),
        limit: int | None = None,
    ) -> list[dict]:
        """
        获取指定 session 的消息列表（按 seq 正序）。
        只返回指定角色的消息（默认 user + assistant，排除 state/system/tool）。
        """
        conn = self._ensure_conn()
        placeholders = ",".join("?" for _ in roles)
        params: list[Any] = [session_id, *roles]

        if limit:
            # 取最后 N 条（先倒序取 N 条，再正序排列）
            query = (
                f"SELECT role, content, tool_name, action, created_at FROM ("
                f"  SELECT role, content, tool_name, action, created_at, seq "
                f"  FROM memory_ems "
                f"  WHERE session_id = ? AND role IN ({placeholders}) "
                f"  ORDER BY seq DESC LIMIT ?"
                f") sub ORDER BY seq ASC"
            )
            params.append(limit)
        else:
            query = (
                f"SELECT role, content, tool_name, action, created_at "
                f"FROM memory_ems "
                f"WHERE session_id = ? AND role IN ({placeholders}) "
                f"ORDER BY seq ASC"
            )

        rows = conn.execute(query, params).fetchall()
        return [
            {"role": r[0], "content": r[1], "tool_name": r[2], "action": r[3], "created_at": r[4]}
            for r in rows
        ]

    def get_recent_messages(
        self,
        user_id: str,
        limit: int = 4,
        roles: tuple[str, ...] = ("user", "assistant"),
    ) -> list[dict]:
        """获取用户当前 session 的最近 N 条消息。"""
        session_id = self.current_session_id(user_id)
        return self.get_session_messages(session_id, roles=roles, limit=limit)

    def get_recent_tables(self, user_id: str) -> list[str]:
        """
        从当前 session 的 tool_output（SQL）中提取用过的表名。
        用于追问时确保 retriever 不会遗漏上一轮查询涉及的表。
        """
        import re
        session_id = self.current_session_id(user_id)
        conn = self._ensure_conn()
        rows = conn.execute(
            "SELECT tool_output FROM memory_ems "
            "WHERE session_id = ? AND tool_name = 'generate_forge_query' AND tool_output IS NOT NULL "
            "ORDER BY seq DESC LIMIT 3",
            (session_id,),
        ).fetchall()

        tables: list[str] = []
        seen: set[str] = set()
        for row in rows:
            sql = row[0] or ""
            # 从 SQL 中提取 FROM/JOIN 后的表名
            for match in re.finditer(r'(?:FROM|JOIN)\s+(\w+)', sql, re.IGNORECASE):
                t = match.group(1).lower()
                if t not in seen and t not in ("select", "where", "on", "and", "or"):
                    tables.append(t)
                    seen.add(t)
        return tables

    def get_full_session(self, session_id: str) -> list[dict]:
        """获取完整 session（含所有角色，用于 SMP 提炼）。"""
        conn = self._ensure_conn()
        rows = conn.execute(
            "SELECT role, content, tool_name, tool_input, tool_output, action, created_at "
            "FROM memory_ems WHERE session_id = ? ORDER BY seq ASC",
            (session_id,),
        ).fetchall()
        return [
            {"role": r[0], "content": r[1], "tool_name": r[2],
             "tool_input": r[3], "tool_output": r[4], "action": r[5], "created_at": r[6]}
            for r in rows
        ]

    def get_user_sessions(self, user_id: str, limit: int = 20) -> list[dict]:
        """获取用户的 session 列表（最近 N 个）。"""
        conn = self._ensure_conn()
        rows = conn.execute(
            "SELECT session_id, MIN(created_at) as started, MAX(created_at) as ended, COUNT(*) as msg_count "
            "FROM memory_ems WHERE user_id = ? "
            "GROUP BY session_id ORDER BY ended DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
        return [
            {"session_id": r[0], "started": r[1], "ended": r[2], "msg_count": r[3]}
            for r in rows
        ]

    # ── 重置 ──────────────────────────────────────────────────────────────────

    def reset_session(self, user_id: str) -> str:
        """强制开启新 session，返回新的 session_id。"""
        new_sid = f"s_{uuid.uuid4().hex[:12]}"
        now = datetime.now(timezone.utc)
        self._sessions[user_id] = (new_sid, 0, now)
        # 清除所有活跃状态
        conn = self._ensure_conn()
        # 插入一个 reset 标记事件
        conn.execute(
            "INSERT INTO memory_ems "
            "(session_id, user_id, seq, role, content, action) "
            "VALUES (?, ?, 0, 'system', 'session_reset', 'reset')",
            (new_sid, user_id),
        )
        conn.commit()
        self._sessions[user_id] = (new_sid, 1, now)
        return new_sid

    # ── 清理 ──────────────────────────────────────────────────────────────────

    def cleanup(self, retention_days: int) -> int:
        """清理超过 retention_days 天的记录。返回删除行数。"""
        if not self._conn:
            return 0
        cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
        cursor = self._conn.execute(
            "DELETE FROM memory_ems WHERE created_at < ?", (cutoff,)
        )
        self._conn.commit()
        deleted = cursor.rowcount
        if deleted:
            logger.info("EMS cleanup: deleted %d records older than %d days", deleted, retention_days)
        return deleted
