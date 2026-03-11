"""
In-memory session store — one session per Feishu user.
Keeps the last N conversation turns for context.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Literal


@dataclass
class Message:
    role:    Literal["user", "assistant"]
    content: str


@dataclass
class Session:
    user_id:  str
    history:  list[Message] = field(default_factory=list)
    # SQL pending user approval (set after compile, cleared after approve/cancel)
    pending_sql:   str | None = None
    pending_forge: dict | None = None

    def add(self, role: Literal["user", "assistant"], content: str) -> None:
        self.history.append(Message(role=role, content=content))
        # keep last 20 turns to limit token usage
        if len(self.history) > 20:
            self.history = self.history[-20:]

    def recent(self, n: int = 10) -> list[Message]:
        return self.history[-n:]


class SessionStore:
    def __init__(self) -> None:
        self._sessions: dict[str, Session] = {}

    def get(self, user_id: str) -> Session:
        if user_id not in self._sessions:
            self._sessions[user_id] = Session(user_id=user_id)
        return self._sessions[user_id]

    def clear(self, user_id: str) -> None:
        self._sessions.pop(user_id, None)


store = SessionStore()
