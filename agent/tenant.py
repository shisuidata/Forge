"""
多租户模型 — 用户 / 团队 / 组织 三层隔离。

存储：SQLite（与 memory.db 共享）。

用法：
    from agent.tenant import tenants

    tenants.set_team("user_abc", "marketing")
    team = tenants.get_team("user_abc")           # "marketing"
    members = tenants.get_team_members("marketing") # ["user_abc", ...]
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

DEFAULT_TEAM = "default"

_DDL = """
CREATE TABLE IF NOT EXISTS tenant_users (
    user_id     TEXT PRIMARY KEY,
    team_id     TEXT NOT NULL DEFAULT 'default',
    display_name TEXT,
    role        TEXT NOT NULL DEFAULT 'member',   -- admin / member
    created_at  TEXT NOT NULL DEFAULT (datetime('now','utc')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now','utc'))
);
CREATE INDEX IF NOT EXISTS idx_tenant_team ON tenant_users(team_id);

CREATE TABLE IF NOT EXISTS tenant_teams (
    team_id     TEXT PRIMARY KEY,
    display_name TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now','utc'))
);
"""


class TenantStore:

    def __init__(self):
        self._conn = None
        self._cache: dict[str, str] = {}
        self._init_db()

    def _init_db(self) -> None:
        try:
            from agent.db import execute_ddl
            execute_ddl(_DDL)
        except Exception as exc:
            logger.warning("Tenant DB init failed: %s", exc)

    def _ensure_conn(self):
        if self._conn is None:
            from agent.db import get_connection_raw
            self._conn = get_connection_raw()
        return self._conn

    # ── 团队管理 ──────────────────────────────────────────────────────────────

    def create_team(self, team_id: str, display_name: str = "") -> None:
        conn = self._ensure_conn()
        conn.execute(
            "INSERT OR IGNORE INTO tenant_teams (team_id, display_name) VALUES (?, ?)",
            (team_id, display_name or team_id),
        )
        conn.commit()

    def list_teams(self) -> list[dict]:
        conn = self._ensure_conn()
        rows = conn.execute(
            "SELECT t.team_id, t.display_name, COUNT(u.user_id) as member_count "
            "FROM tenant_teams t LEFT JOIN tenant_users u ON t.team_id = u.team_id "
            "GROUP BY t.team_id ORDER BY t.team_id"
        ).fetchall()
        return [{"team_id": r[0], "display_name": r[1], "member_count": r[2]} for r in rows]

    # ── 用户-团队映射 ─────────────────────────────────────────────────────────

    def set_team(self, user_id: str, team_id: str, display_name: str = "", role: str = "member") -> None:
        conn = self._ensure_conn()
        # 确保团队存在
        conn.execute("INSERT OR IGNORE INTO tenant_teams (team_id) VALUES (?)", (team_id,))
        conn.execute(
            "INSERT INTO tenant_users (user_id, team_id, display_name, role) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET "
            "team_id = excluded.team_id, display_name = excluded.display_name, "
            "role = excluded.role, updated_at = datetime('now','utc')",
            (user_id, team_id, display_name, role),
        )
        conn.commit()
        self._cache[user_id] = team_id

    def get_team(self, user_id: str) -> str:
        if user_id in self._cache:
            return self._cache[user_id]
        conn = self._ensure_conn()
        row = conn.execute(
            "SELECT team_id FROM tenant_users WHERE user_id = ?", (user_id,)
        ).fetchone()
        team = row[0] if row else DEFAULT_TEAM
        self._cache[user_id] = team
        return team

    def get_team_members(self, team_id: str) -> list[dict]:
        conn = self._ensure_conn()
        rows = conn.execute(
            "SELECT user_id, display_name, role FROM tenant_users WHERE team_id = ?",
            (team_id,),
        ).fetchall()
        return [{"user_id": r[0], "display_name": r[1], "role": r[2]} for r in rows]

    def get_user_info(self, user_id: str) -> dict | None:
        conn = self._ensure_conn()
        row = conn.execute(
            "SELECT user_id, team_id, display_name, role FROM tenant_users WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if row:
            return {"user_id": row[0], "team_id": row[1], "display_name": row[2], "role": row[3]}
        return None


# 全局单例
tenants = TenantStore()
