"""
Forge Agent 三层记忆系统 — 统一门面。

EMS（Episodic Memory Store）  → 情景记忆：完整对话轨迹
SMP（Semantic Memory Pool）   → 语义记忆：提炼后的结构化知识
WMB（Working Memory Buffer）  → 工作记忆：按场景实时裁剪

用法：
    from agent.memory import memory

    # 记录消息
    memory.record(user_id, "user", "各城市的订单数")

    # 构建 LLM 输入
    messages, knowledge = memory.build("query", user_id, "各城市的订单数")

    # 状态管理
    memory.set_state(user_id, "pending_sql", sql)
    sql = memory.get_state(user_id, "pending_sql")
    memory.clear_state(user_id, "pending_sql")

    # 重置对话
    memory.reset(user_id)
"""
from __future__ import annotations

import logging
from typing import Any

from agent.memory.ems import EpisodicMemoryStore
from agent.memory.smp import SemanticMemoryPool
from agent.memory.wmb import WorkingMemoryBuffer
from agent.memory.extractor import Extractor

logger = logging.getLogger(__name__)


class MemoryManager:
    """
    记忆系统统一入口。

    agent.py / feishu.py / router.py 只和 MemoryManager 交互，
    不直接操作 EMS / SMP / WMB / Extractor。
    """

    def __init__(self):
        self.ems = EpisodicMemoryStore()
        self.smp = SemanticMemoryPool()
        self.wmb = WorkingMemoryBuffer(self.ems, self.smp)
        self.extractor = Extractor(self.ems, self.smp)

    # ── EMS 代理：记录 ────────────────────────────────────────────────────────

    def record(
        self,
        user_id: str,
        role: str,
        content: str = "",
        **kwargs,
    ) -> int:
        """追加一条情景记忆。每条消息后自动重置异步提炼计时器。"""
        rid = self.ems.record(user_id, role, content, **kwargs)
        # 每条用户/助手消息都重置 debounce 计时器
        if role in ("user", "assistant"):
            self.extractor.schedule_async_extract(user_id)
        return rid

    # ── WMB 代理：构建 LLM 输入 ──────────────────────────────────────────────

    def build(
        self,
        scene: str,
        user_id: str,
        query: str = "",
        team_id: str = "",
    ) -> tuple[list[dict[str, str]], str, list[str]]:
        """
        按场景构建 LLM 输入。

        Returns:
            (messages, knowledge_context, extra_tables)
            extra_tables: 当前 session 中已用过的表名（确保追问时不遗漏）
        """
        if not team_id:
            try:
                from agent.tenant import tenants
                team_id = tenants.get_team(user_id)
            except Exception:
                pass
        messages, knowledge = self.wmb.build(scene, user_id, query, team_id=team_id)
        extra_tables = self.ems.get_recent_tables(user_id)
        return messages, knowledge, extra_tables

    # ── 状态管理 ──────────────────────────────────────────────────────────────

    def set_state(self, user_id: str, key: str, value: Any) -> None:
        """设置可变状态（pending_sql / pending_forge / pending_intent 等）。"""
        self.ems.set_state(user_id, key, value)

    def get_state(self, user_id: str, key: str) -> Any | None:
        """读取可变状态。"""
        return self.ems.get_state(user_id, key)

    def clear_state(self, user_id: str, key: str) -> None:
        """清除可变状态。"""
        self.ems.clear_state(user_id, key)

    # ── Session 管理 ──────────────────────────────────────────────────────────

    def reset(self, user_id: str) -> str:
        """重置用户对话（开新 session）。返回新 session_id。"""
        return self.ems.reset_session(user_id)

    def current_session_id(self, user_id: str) -> str:
        """获取用户当前 session_id。"""
        return self.ems.current_session_id(user_id)


# 全局单例
memory = MemoryManager()
