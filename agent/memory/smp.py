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
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DB_PATH = Path(".forge/memory.db")   # 与 EMS 共享同一个 DB 文件

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
    created_at      TEXT    NOT NULL DEFAULT (datetime('now','utc')),
    updated_at      TEXT    NOT NULL DEFAULT (datetime('now','utc')),

    UNIQUE(scope, user_id, category, key)
);

CREATE INDEX IF NOT EXISTS idx_smp_user ON memory_smp(user_id, category);
CREATE INDEX IF NOT EXISTS idx_smp_org  ON memory_smp(scope, category) WHERE scope = 'org';
"""

ORG_USER_ID = "__org__"


class SemanticMemoryPool:
    """语义记忆池。"""

    def __init__(self, db_path: Path | str | None = None):
        self._db_path = Path(db_path) if db_path else DB_PATH
        self._conn: sqlite3.Connection | None = None
        self._init_db()

    def _init_db(self) -> None:
        try:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
            self._conn.executescript(_DDL)
            self._conn.commit()
        except (sqlite3.Error, OSError) as exc:
            logger.warning("SMP DB init failed: %s", exc)
            self._conn = None

    def _ensure_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._init_db()
        if self._conn is None:
            raise RuntimeError("SMP database unavailable")
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
    ) -> None:
        """写入或更新一条语义记忆。"""
        conn = self._ensure_conn()
        value_str = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
        now = datetime.now(timezone.utc).isoformat()

        conn.execute(
            "INSERT INTO memory_smp (scope, user_id, category, key, value, source_sessions, confidence, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(scope, user_id, category, key) DO UPDATE SET "
            "  value = excluded.value, "
            "  source_sessions = CASE WHEN excluded.source_sessions != '' "
            "    THEN memory_smp.source_sessions || ',' || excluded.source_sessions "
            "    ELSE memory_smp.source_sessions END, "
            "  confidence = MAX(memory_smp.confidence, excluded.confidence), "
            "  updated_at = excluded.updated_at",
            (scope, user_id, category, key, value_str, source_session, confidence, now),
        )
        conn.commit()

    def upsert_org(self, category: str, key: str, value: Any, **kwargs) -> None:
        """写入组织级知识。"""
        self.upsert(category, key, value, user_id=ORG_USER_ID, scope="org", **kwargs)

    # ── 读取 ──────────────────────────────────────────────────────────────────

    def query(
        self,
        user_id: str,
        category: str = "",
        limit: int = 10,
    ) -> list[dict]:
        """
        查询用户可见的知识（个人级 + 组织级合并，个人级优先）。
        """
        conn = self._ensure_conn()
        conditions = ["(user_id = ? OR (scope = 'org' AND user_id = ?))"]
        params: list[Any] = [user_id, ORG_USER_ID]

        if category:
            conditions.append("category = ?")
            params.append(category)

        where = " AND ".join(conditions)
        # 按 scope 排序：user 优先于 org（同 key 时个人覆盖组织）
        rows = conn.execute(
            f"SELECT scope, user_id, category, key, value, confidence, updated_at "
            f"FROM memory_smp WHERE {where} "
            f"ORDER BY CASE scope WHEN 'user' THEN 0 ELSE 1 END, confidence DESC, updated_at DESC "
            f"LIMIT ?",
            (*params, limit),
        ).fetchall()

        results = []
        seen_keys: set[str] = set()
        for r in rows:
            k = f"{r[2]}:{r[3]}"
            if k in seen_keys:
                continue  # 个人级已覆盖同 key 的组织级
            seen_keys.add(k)
            try:
                val = json.loads(r[4])
            except (json.JSONDecodeError, TypeError):
                val = r[4]
            results.append({
                "scope": r[0], "user_id": r[1], "category": r[2],
                "key": r[3], "value": val, "confidence": r[5], "updated_at": r[6],
            })
        return results

    def get_knowledge_text(self, user_id: str, max_items: int = 5) -> str:
        """
        获取用户可见的知识摘要文本（用于注入 system prompt）。
        """
        items = self.query(user_id, limit=max_items)
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
            scope_tag = "[组织]" if item["scope"] == "org" else "[个人]"
            lines.append(f"- {scope_tag} [{cat}] {key}: {val_str}")

        return "\n".join(lines)
