"""
SMP — Semantic Memory Pool（语义记忆层）

从 EMS 提炼的结构化知识，分组织级（org）和个人级（user）。
所有场景可共享，由 WMB 按需读取。

知识类别：
    - user_profile:    用户画像（常用表、查询偏好）
    - correction:      纠错记录（错误→正确的映射）
    - confirmed_fact:  确认事实（已验证的业务规则）
    - session_summary: 会话摘要
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS memory_smp (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    scope           TEXT    NOT NULL DEFAULT 'user',   -- 'org' | 'user'
    user_id         TEXT    NOT NULL,                  -- org 级用 '__org__'
    category        TEXT    NOT NULL,
    key             TEXT    NOT NULL,
    value           TEXT    NOT NULL,
    source_sessions TEXT,
    confidence      REAL    DEFAULT 1.0,
    source_revision TEXT,
    status          TEXT    NOT NULL DEFAULT 'confirmed',
    expires_at      TEXT,
    deleted_at      TEXT,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now','utc')),
    updated_at      TEXT    NOT NULL DEFAULT (datetime('now','utc')),

    UNIQUE(scope, user_id, category, key)
);

CREATE INDEX IF NOT EXISTS idx_smp_user ON memory_smp(user_id, category);
CREATE INDEX IF NOT EXISTS idx_smp_org  ON memory_smp(scope, category) WHERE scope = 'org';
"""

ORG_USER_ID = "__org__"
TEAM_PREFIX = "__team__"    # team scope 的 user_id 格式: "__team__marketing"


class SemanticMemoryPool:
    """语义记忆池。"""

    def __init__(self):
        self._conn = None
        self._init_db()

    def _init_db(self) -> None:
        try:
            from agent.db import execute_ddl, get_connection_raw
            execute_ddl(_DDL)
            conn = get_connection_raw()
            columns = {row[1] for row in conn.execute("PRAGMA table_info(memory_smp)").fetchall()}
            migrations = {
                "source_revision": "ALTER TABLE memory_smp ADD COLUMN source_revision TEXT",
                "status": "ALTER TABLE memory_smp ADD COLUMN status TEXT NOT NULL DEFAULT 'confirmed'",
                "expires_at": "ALTER TABLE memory_smp ADD COLUMN expires_at TEXT",
                "deleted_at": "ALTER TABLE memory_smp ADD COLUMN deleted_at TEXT",
            }
            for column, statement in migrations.items():
                if column not in columns:
                    conn.execute(statement)
            conn.commit()
        except Exception as exc:
            logger.warning("SMP DB init failed: %s", exc)

    def _ensure_conn(self):
        from agent.db import get_connection_raw
        if self._conn is None:
            self._conn = get_connection_raw()
            return self._conn
        try:
            self._conn.execute("SELECT 1")
        except Exception:
            try:
                self._conn._conn.rollback()
            except Exception:
                pass
            self._conn = get_connection_raw()
        return self._conn

    # ── 写入 ──────────────────────────────────────────────────────────────────

    def upsert(
        self,
        category: str,
        key: str,
        value: Any,
        *,
        user_id: str = ORG_USER_ID,
        scope: str = "user",
        source_session: str = "",
        confidence: float = 1.0,
        source_revision: str = "",
        status: str = "confirmed",
        expires_at: str | None = None,
    ) -> None:
        """写入或更新一条语义记忆。"""
        conn = self._ensure_conn()
        value_str = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
        now = datetime.now(timezone.utc).isoformat()

        if scope not in {"user", "team", "org"}:
            raise ValueError("memory scope is invalid")
        if status not in {"confirmed", "superseded", "conflicted"}:
            raise ValueError("memory status is invalid")
        conn.execute(
            "INSERT INTO memory_smp (scope, user_id, category, key, value, source_sessions, confidence, source_revision, status, expires_at, deleted_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?) "
            "ON CONFLICT(scope, user_id, category, key) DO UPDATE SET "
            "  value = excluded.value, "
            "  source_sessions = CASE WHEN excluded.source_sessions != '' "
            "    THEN COALESCE(memory_smp.source_sessions, '') || ',' || excluded.source_sessions "
            "    ELSE memory_smp.source_sessions END, "
            "  confidence = excluded.confidence, source_revision = excluded.source_revision, "
            "  status = excluded.status, expires_at = excluded.expires_at, deleted_at = NULL, "
            "  updated_at = excluded.updated_at",
            (scope, user_id, category, key, value_str, source_session, confidence,
             source_revision, status, expires_at, now),
        )
        conn.commit()

    def upsert_org(self, category: str, key: str, value: Any, **kwargs) -> None:
        """写入组织级知识。"""
        self.upsert(category, key, value, user_id=ORG_USER_ID, scope="org", **kwargs)

    def upsert_team(self, team_id: str, category: str, key: str, value: Any, **kwargs) -> None:
        """写入团队级知识。"""
        self.upsert(
            category, key, value,
            user_id=f"{TEAM_PREFIX}{team_id}",
            scope="team",
            **kwargs,
        )

    # ── 读取 ──────────────────────────────────────────────────────────────────

    def query(
        self,
        user_id: str,
        category: str = "",
        limit: int = 10,
        team_id: str = "",
        include_shadowed: bool = False,
    ) -> list[dict]:
        """
        查询用户可见的知识，三层合并：user > team > org（就近覆盖）。
        """
        conn = self._ensure_conn()

        # 构建匹配条件：user 本人 + team + org
        user_conditions = ["(scope = 'user' AND user_id = ?)"]
        params: list[Any] = [user_id]
        if team_id:
            user_conditions.append("(scope = 'team' AND user_id = ?)")
            params.append(f"{TEAM_PREFIX}{team_id}")
        user_conditions.append("(scope = 'org' AND user_id = ?)")
        params.append(ORG_USER_ID)

        scope_filter = "(" + " OR ".join(user_conditions) + ")"
        conditions = [scope_filter, "deleted_at IS NULL", "status = 'confirmed'", "(expires_at IS NULL OR expires_at > ?)"]
        params.append(datetime.now(timezone.utc).isoformat())
        if category:
            conditions.append("category = ?")
            params.append(category)

        where = " AND ".join(conditions)
        # 优先级：user=0, team=1, org=2（同 key 就近覆盖）
        rows = conn.execute(
            f"SELECT id, scope, user_id, category, key, value, confidence, source_revision, status, expires_at, updated_at "
            f"FROM memory_smp WHERE {where} "
            f"ORDER BY CASE scope WHEN 'user' THEN 0 WHEN 'team' THEN 1 ELSE 2 END, "
            f"confidence DESC, updated_at DESC "
            f"LIMIT ?",
            (*params, limit),
        ).fetchall()

        results = []
        seen_keys: set[str] = set()
        for r in rows:
            k = f"{r[3]}:{r[4]}"
            if not include_shadowed and k in seen_keys:
                continue
            seen_keys.add(k)
            try:
                val = json.loads(r[5])
            except (json.JSONDecodeError, TypeError):
                val = r[5]
            results.append({
                "id": r[0], "scope": r[1], "user_id": r[2], "category": r[3],
                "key": r[4], "value": val, "confidence": r[6],
                "source_revision": r[7], "status": r[8], "expires_at": r[9],
                "updated_at": r[10],
            })
        return results

    def delete_user_entry(self, user_id: str, category: str, key: str) -> bool:
        """Soft-delete one user-scoped entry; team/org memory requires admin tooling."""
        conn = self._ensure_conn()
        now = datetime.now(timezone.utc).isoformat()
        result = conn.execute(
            "UPDATE memory_smp SET deleted_at=?, updated_at=? "
            "WHERE scope='user' AND user_id=? AND category=? AND key=? AND deleted_at IS NULL",
            (now, now, user_id, category, key),
        )
        conn.commit()
        return bool(getattr(result, "rowcount", 0))

    def get_knowledge_text(self, user_id: str, max_items: int = 5, team_id: str = "") -> str:
        """
        获取用户可见的知识摘要文本（用于注入 system prompt）。
        三层合并：user > team > org。
        """
        items = self.query(user_id, limit=max_items, team_id=team_id)
        if not items:
            return ""

        lines = ["## 历史知识（来自语义记忆）"]
        for item in items:
            cat = item["category"]
            key = item["key"]
            val = item["value"]
            if isinstance(val, dict):
                val_str = json.dumps(val, ensure_ascii=False)
            else:
                val_str = str(val)
            scope_tag = {"org": "[组织]", "team": "[团队]", "user": "[个人]"}.get(item["scope"], "[未知]")
            lines.append(f"- {scope_tag} [{cat}] {key}: {val_str}")

        return "\n".join(lines)
