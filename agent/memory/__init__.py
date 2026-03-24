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

logger = logging.getLogger(__name__)


class MemoryManager:
    """
    记忆系统统一入口。

    agent.py / feishu.py / router.py 只和 MemoryManager 交互，
    不直接操作 EMS / SMP / WMB。
    """

    def __init__(self):
        self.ems = EpisodicMemoryStore()
        self.smp = SemanticMemoryPool()
        self.wmb = WorkingMemoryBuffer(self.ems, self.smp)

    # ── EMS 代理：记录 ────────────────────────────────────────────────────────

    def record(
        self,
        user_id: str,
        role: str,
        content: str = "",
        **kwargs,
    ) -> int:
        """追加一条情景记忆。"""
        return self.ems.record(user_id, role, content, **kwargs)

    # ── WMB 代理：构建 LLM 输入 ──────────────────────────────────────────────

    def build(
        self,
        scene: str,
        user_id: str,
        query: str = "",
    ) -> tuple[list[dict[str, str]], str]:
        """
        按场景构建 LLM 输入。

        Returns:
            (messages, knowledge_context)
            messages:          LLM messages 数组
            knowledge_context: 追加到 system prompt 的知识文本
        """
        return self.wmb.build(scene, user_id, query)

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
