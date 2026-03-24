"""
WMB — Working Memory Buffer（工作记忆层）

不存储。每次 LLM 调用前实时构建。
从 EMS 裁剪对话历史 + 从 SMP 提取相关知识，组合为 LLM 输入。

场景配置：
    query:  查询模式，最近 4 条 EMS 消息
    define: 定义模式，当前 session 全部消息
    admin:  管理助手，最近 6 条 EMS 消息
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


# ── 场景配置 ──────────────────────────────────────────────────────────────────

@dataclass
class SceneConfig:
    """场景的记忆裁剪配置。"""
    ems_limit: int | None       # EMS 消息条数上限，None=全 session
    smp_max_items: int          # SMP 知识条数上限
    ems_token_budget: int       # EMS 消息的大致 token 预算（按字符估算，1 token ≈ 2 中文字符）


SCENE_CONFIGS: dict[str, SceneConfig] = {
    "query": SceneConfig(
        ems_limit=4,
        smp_max_items=5,
        ems_token_budget=3000,
    ),
    "define": SceneConfig(
        ems_limit=None,          # 全 session
        smp_max_items=5,
        ems_token_budget=4000,
    ),
    "admin": SceneConfig(
        ems_limit=6,
        smp_max_items=5,
        ems_token_budget=3000,
    ),
}


# ── WMB Builder ───────────────────────────────────────────────────────────────

class WorkingMemoryBuffer:
    """
    工作记忆构建器。

    build() 返回 (messages, knowledge_context):
        messages:          给 LLM 的 messages 数组（从 EMS 裁剪）
        knowledge_context: 追加到 system prompt 的知识文本（从 SMP 提取）
    """

    def __init__(self, ems: Any, smp: Any):
        """
        Args:
            ems: EpisodicMemoryStore 实例
            smp: SemanticMemoryPool 实例
        """
        self._ems = ems
        self._smp = smp

    def build(
        self,
        scene: str,
        user_id: str,
        current_query: str = "",
    ) -> tuple[list[dict[str, str]], str]:
        """
        按场景构建 LLM 输入。

        Args:
            scene:         场景名（query / define / admin）
            user_id:       用户标识
            current_query: 当前用户输入（用于 SMP 相关性排序，暂未实现）

        Returns:
            (messages, knowledge_context)
            messages:          [{"role": "user"|"assistant", "content": "..."}]
            knowledge_context: 追加到 system prompt 尾部的文本
        """
        config = SCENE_CONFIGS.get(scene, SCENE_CONFIGS["query"])

        # ── 1. 从 EMS 裁剪对话历史 ─────────────────────────────────────────
        if config.ems_limit is not None:
            raw_messages = self._ems.get_recent_messages(
                user_id,
                limit=config.ems_limit,
                roles=("user", "assistant"),
            )
        else:
            # 全 session
            session_id = self._ems.current_session_id(user_id)
            raw_messages = self._ems.get_session_messages(
                session_id,
                roles=("user", "assistant"),
            )

        # Token 预算裁剪（粗估：1 字符 ≈ 0.5 token）
        messages = self._trim_by_budget(raw_messages, config.ems_token_budget)

        # 转换为 LLM messages 格式
        lm_messages = [
            {"role": m["role"], "content": m["content"]}
            for m in messages
            if m["content"]  # 跳过空消息
        ]

        # ── 2. 从 SMP 提取知识上下文 ──────────────────────────────────────
        knowledge_context = ""
        try:
            knowledge_context = self._smp.get_knowledge_text(
                user_id,
                max_items=config.smp_max_items,
            )
        except Exception as exc:
            logger.debug("SMP knowledge retrieval failed: %s", exc)

        return lm_messages, knowledge_context

    def _trim_by_budget(
        self,
        messages: list[dict],
        budget_tokens: int,
    ) -> list[dict]:
        """
        按 token 预算裁剪消息列表（保留最新的）。
        粗估：1 中文字符 ≈ 0.5 token，1 英文单词 ≈ 1 token。
        """
        # 估算每条消息的 token 数
        def est_tokens(msg: dict) -> int:
            content = msg.get("content", "")
            # 粗估：中文字符数 * 0.5 + 英文字符数 * 0.25
            return max(len(content) // 2, 1)

        total = sum(est_tokens(m) for m in messages)
        if total <= budget_tokens:
            return messages

        # 从最旧的开始丢弃
        result = list(messages)
        while result and total > budget_tokens:
            dropped = result.pop(0)
            total -= est_tokens(dropped)

        return result
