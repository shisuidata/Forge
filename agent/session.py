"""
对话会话管理 — SQLite 持久化 + 内存缓存。

设计原则：
    - 持久化：对话历史写入 SQLite，服务重启后自动恢复
    - 隔离：不同用户的上下文严格隔离，SessionStore 以 user_id 为键
    - 节流：每个 Session 最多保留最近 20 条消息，防止 token 过载
    - 状态：pending_* 字段为瞬态字段，不持久化（重启后需用户重新发起）

会话生命周期：
    get(user_id)  → 先查内存缓存，miss 时从 SQLite 恢复；均不存在则新建
    clear(user_id)→ 同时删除内存和 SQLite 中的记录
"""
from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

logger = logging.getLogger(__name__)

_DEFAULT_DB_PATH = Path(".forge/sessions.db")


@dataclass
class Message:
    """单条对话消息，记录角色（user/assistant）和文本内容。"""
    role:    Literal["user", "assistant"]
    content: str


@dataclass
class IntentSpec:
    """
    Agent 在向用户发出澄清问题后保存的中间状态。

    当 Agent 检测到问题存在歧义、需要用户补充信息时，
    将原始问题和已识别的歧义点记录在此，等待用户回复后继续推进。

    Attributes:
        original_question: 用户的原始问题（澄清前）
        clarification_prompt: Agent 向用户提出的澄清问题文本
        ambiguity_keys: 触发本次澄清的歧义规则 key 列表（来自 disambiguations.registry.yaml）
    """
    original_question:    str
    clarification_prompt: str
    ambiguity_keys:       list[str] = field(default_factory=list)


@dataclass
class Session:
    """
    单用户的对话状态容器。

    Attributes:
        user_id:                 飞书 open_id，作为全局唯一标识
        history:                 对话历史，最多保留最近 20 条（超出后从头部丢弃）
        pending_sql:             待用户确认的 SQL 字符串；None 表示当前无待确认项
        pending_forge:           生成 pending_sql 所对应的 Forge JSON；与 pending_sql 同步清空
        pending_metric_proposal: 待用户确认的指标定义提案（猜测→确认→入库流程）
    """
    user_id:                 str
    history:                 list[Message] = field(default_factory=list)
    pending_sql:             str | None       = None
    pending_forge:           dict | None      = None
    pending_metric_proposal: dict | None      = None
    pending_cache_id:        str | None       = None   # Stage 2 反馈：等待用户确认结果准确性
    pending_intent:          IntentSpec | None = None  # 澄清轮次：等待用户补充信息
    _on_change:              object           = None   # Callable[[], None] | None，持久化回调

    def add(self, role: Literal["user", "assistant"], content: str) -> None:
        """
        追加一条消息到历史记录。

        裁剪策略：
        - 超过 20 条时，优先丢弃 [系统] 注入消息（编译重试错误等）
        - 仍然超出则从头部丢弃最旧消息
        持久化通过 _on_change 回调自动触发。
        """
        self.history.append(Message(role=role, content=content))
        if len(self.history) > 20:
            # 优先清理系统注入消息
            self.history = [
                m for m in self.history
                if not (m.role == "user" and m.content.startswith("[系统]"))
            ]
        if len(self.history) > 20:
            self.history = self.history[-20:]
        if self._on_change:
            self._on_change()

    def recent(self, n: int = 10) -> list[Message]:
        """
        返回最近 n 条消息，用于构建 LLM 调用的 messages 列表。

        Args:
            n: 返回条数，默认 10（5 轮对话），不足时返回全部历史。
        """
        return self.history[-n:]


def _serialize_history(history: list[Message]) -> str:
    return json.dumps(
        [{"role": m.role, "content": m.content} for m in history],
        ensure_ascii=False,
    )


def _deserialize_history(raw: str) -> list[Message]:
    try:
        items = json.loads(raw)
        return [Message(role=m["role"], content=m["content"]) for m in items]
    except (json.JSONDecodeError, KeyError, TypeError):
        return []


class SessionStore:
    """
    全局会话存储，SQLite 持久化 + 内存缓存。

    对话历史在每次 add() 后写入 SQLite。
    pending_* 瞬态字段仅在内存中维护，不持久化。
    """

    def __init__(self, db_path: Path | str | None = None) -> None:
        self._sessions: dict[str, Session] = {}
        self._db_path = Path(db_path) if db_path else _DEFAULT_DB_PATH
        self._conn: sqlite3.Connection | None = None
        self._init_db()

    def _init_db(self) -> None:
        try:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS sessions ("
                "  user_id    TEXT PRIMARY KEY,"
                "  history    TEXT NOT NULL DEFAULT '[]',"
                "  updated_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))"
                ")"
            )
            self._conn.commit()
        except (sqlite3.Error, OSError) as exc:
            logger.warning("Session DB init failed, falling back to memory-only: %s", exc)
            self._conn = None

    def _persist(self, session: Session | None) -> None:
        if self._conn is None or session is None:
            return
        try:
            self._conn.execute(
                "INSERT INTO sessions (user_id, history, updated_at) "
                "VALUES (?, ?, datetime('now','localtime')) "
                "ON CONFLICT(user_id) DO UPDATE SET "
                "  history = excluded.history, updated_at = excluded.updated_at",
                (session.user_id, _serialize_history(session.history)),
            )
            self._conn.commit()
        except sqlite3.Error as exc:
            logger.warning("Session persist failed for %s: %s", session.user_id, exc)

    def _load_from_db(self, user_id: str) -> Session | None:
        if self._conn is None:
            return None
        try:
            row = self._conn.execute(
                "SELECT history FROM sessions WHERE user_id = ?", (user_id,)
            ).fetchone()
            if row:
                history = _deserialize_history(row[0])
                return Session(user_id=user_id, history=history)
        except sqlite3.Error as exc:
            logger.warning("Session load failed for %s: %s", user_id, exc)
        return None

    def get(self, user_id: str) -> Session:
        """
        获取指定用户的 Session。
        查找顺序：内存缓存 → SQLite → 新建。
        """
        if user_id not in self._sessions:
            restored = self._load_from_db(user_id)
            if restored:
                self._sessions[user_id] = restored
                logger.debug("Session restored from DB for %s (%d messages)",
                             user_id, len(restored.history))
            else:
                self._sessions[user_id] = Session(user_id=user_id)
            # 注入持久化回调：每次 add() 后自动写入 SQLite
            sess = self._sessions[user_id]
            sess._on_change = lambda uid=user_id: self._persist(self._sessions.get(uid))  # type: ignore[assignment]
        return self._sessions[user_id]

    def save(self, user_id: str) -> None:
        """显式持久化指定用户的 Session。供外部在 add() 后调用。"""
        if user_id in self._sessions:
            self._persist(self._sessions[user_id])

    def clear(self, user_id: str) -> None:
        """删除指定用户的 Session（内存 + SQLite）。"""
        self._sessions.pop(user_id, None)
        if self._conn:
            try:
                self._conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
                self._conn.commit()
            except sqlite3.Error as exc:
                logger.warning("Session clear failed for %s: %s", user_id, exc)


# 模块级全局单例，由 agent.py 和 web/router.py 共享引用
store = SessionStore()
