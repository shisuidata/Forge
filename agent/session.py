"""
对话会话管理 — 内存存储，每个飞书用户对应一个独立 Session。

设计原则：
    - 轻量：纯内存，无数据库依赖，服务重启后会话自动清空
    - 隔离：不同用户的上下文严格隔离，SessionStore 以 user_id 为键
    - 节流：每个 Session 最多保留最近 20 条消息，防止 token 过载
    - 状态：pending_sql / pending_forge 记录等待用户确认的 SQL，
            approve()/cancel() 在读取后立即清空，保证状态单向流转

会话生命周期：
    get(user_id)  → 不存在则创建新 Session，存在则返回已有 Session
    clear(user_id)→ 删除该用户的 Session（如用户发送"重置"命令时）
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Literal


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

    def add(self, role: Literal["user", "assistant"], content: str) -> None:
        """
        追加一条消息到历史记录。

        超过 20 条时自动裁剪头部，保留最近 20 条。
        20 条的上限对应约 10 轮对话，在保证上下文连贯性的同时控制 token 消耗。
        """
        self.history.append(Message(role=role, content=content))
        if len(self.history) > 20:
            self.history = self.history[-20:]

    def recent(self, n: int = 10) -> list[Message]:
        """
        返回最近 n 条消息，用于构建 LLM 调用的 messages 列表。

        Args:
            n: 返回条数，默认 10（5 轮对话），不足时返回全部历史。
        """
        return self.history[-n:]


class SessionStore:
    """
    全局会话存储，以 user_id 为键管理所有 Session 实例。

    线程安全说明：
        当前实现为简单 dict，适用于单进程 asyncio 服务。
        若需多进程部署，应替换为 Redis 等共享存储。
    """

    def __init__(self) -> None:
        # 内部字典：user_id → Session
        self._sessions: dict[str, Session] = {}

    def get(self, user_id: str) -> Session:
        """
        获取指定用户的 Session。若不存在则自动创建并注册。

        Args:
            user_id: 飞书 open_id 或其他唯一用户标识。

        Returns:
            该用户的 Session 实例（首次调用时为空历史）。
        """
        if user_id not in self._sessions:
            self._sessions[user_id] = Session(user_id=user_id)
        return self._sessions[user_id]

    def clear(self, user_id: str) -> None:
        """
        删除指定用户的 Session，释放内存并重置对话上下文。

        Args:
            user_id: 要清除的用户 ID。若不存在则静默忽略。
        """
        self._sessions.pop(user_id, None)


# 模块级全局单例，由 agent.py 和 web/router.py 共享引用
store = SessionStore()
