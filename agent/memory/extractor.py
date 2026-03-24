"""
SMP 提炼器 — 从 EMS 事件中提取结构化知识写入 SMP。

两种触发方式：
    实时提炼：approve / cancel / cache_verify 时立即调用
    异步提炼：对话结束后（30s 无新消息）由后台线程调用

提炼规则：
    approve + cache_verify → confirmed_fact（已验证的查询模式）
    cancel + 重新提问     → correction（纠错记录）
    对话结束              → session_summary（会话摘要）
    累积统计              → user_profile（用户画像）
"""
from __future__ import annotations

import json
import logging
import threading
from typing import Any

logger = logging.getLogger(__name__)

# 异步提炼定时器：{user_id: Timer}
_pending_timers: dict[str, threading.Timer] = {}
_timer_lock = threading.Lock()

ASYNC_DELAY_SEC = 30   # 对话结束后多久触发异步提炼


class Extractor:
    """SMP 提炼器。"""

    def __init__(self, ems: Any, smp: Any):
        self._ems = ems
        self._smp = smp

    # ── 实时提炼 ──────────────────────────────────────────────────────────────

    def on_approve(self, user_id: str, sql: str, forge_json: dict, question: str) -> None:
        """用户确认 SQL 执行 → 记录查询模式为 candidate fact。"""
        if not question:
            return
        try:
            key = f"query_{hash(question) & 0xFFFFFFFF:08x}"
            self._smp.upsert(
                category="confirmed_fact",
                key=key,
                value={
                    "question": question,
                    "sql_pattern": sql[:200] if sql else "",
                    "status": "candidate",   # 待 verify 后提升
                },
                user_id=user_id,
                scope="user",
                source_session=self._ems.current_session_id(user_id),
                confidence=0.5,
            )
        except Exception as exc:
            logger.debug("on_approve extract failed: %s", exc)

    def on_cache_verify(self, user_id: str) -> None:
        """用户确认结果准确 → 提升已有 candidate 的置信度，写入 org。"""
        try:
            # 找该用户最近的 candidate fact
            facts = self._smp.query(user_id, category="confirmed_fact", limit=1)
            for f in facts:
                if isinstance(f["value"], dict) and f["value"].get("status") == "candidate":
                    # 提升为 verified
                    val = f["value"]
                    val["status"] = "verified"
                    self._smp.upsert(
                        category="confirmed_fact",
                        key=f["key"],
                        value=val,
                        user_id=user_id,
                        scope="user",
                        confidence=1.0,
                    )
                    # 同时写入 org 级（candidate，等待其他用户确认后自动提升）
                    self._smp.upsert_org(
                        category="confirmed_fact",
                        key=f["key"],
                        value=val,
                        confidence=0.7,
                    )
                    break
        except Exception as exc:
            logger.debug("on_cache_verify extract failed: %s", exc)

    def on_cancel(self, user_id: str, cancelled_sql: str) -> None:
        """用户取消 SQL → 如果之后重新提问，记录纠错。"""
        if not cancelled_sql:
            return
        try:
            key = f"cancel_{hash(cancelled_sql) & 0xFFFFFFFF:08x}"
            self._smp.upsert(
                category="correction",
                key=key,
                value={
                    "wrong_sql_prefix": cancelled_sql[:200],
                    "reason": "user_cancelled",
                    "status": "pending_correction",  # 等后续查询来补全 correct 版本
                },
                user_id=user_id,
                scope="user",
                source_session=self._ems.current_session_id(user_id),
                confidence=0.3,
            )
        except Exception as exc:
            logger.debug("on_cancel extract failed: %s", exc)

    def on_cache_reject(self, user_id: str) -> None:
        """用户标记结果不准确 → 降低对应 fact 的置信度。"""
        try:
            facts = self._smp.query(user_id, category="confirmed_fact", limit=1)
            for f in facts:
                val = f["value"] if isinstance(f["value"], dict) else {}
                val["status"] = "rejected"
                self._smp.upsert(
                    category="confirmed_fact",
                    key=f["key"],
                    value=val,
                    user_id=user_id,
                    scope="user",
                    confidence=0.1,
                )
                break
        except Exception as exc:
            logger.debug("on_cache_reject extract failed: %s", exc)

    # ── 用户画像更新 ──────────────────────────────────────────────────────────

    def update_user_profile(self, user_id: str, question: str) -> None:
        """从用户查询中提取偏好信息。"""
        if not question:
            return
        try:
            # 简单统计：记录查询关键词
            self._smp.upsert(
                category="user_profile",
                key="last_query",
                value=question[:100],
                user_id=user_id,
                scope="user",
            )
        except Exception as exc:
            logger.debug("update_user_profile failed: %s", exc)

    # ── 异步提炼 ──────────────────────────────────────────────────────────────

    def schedule_async_extract(self, user_id: str) -> None:
        """
        安排异步提炼：对话结束后 ASYNC_DELAY_SEC 秒触发。
        每次新消息都重置计时器（debounce）。
        """
        with _timer_lock:
            old = _pending_timers.get(user_id)
            if old:
                old.cancel()
            timer = threading.Timer(
                ASYNC_DELAY_SEC,
                self._async_extract,
                args=(user_id,),
            )
            timer.daemon = True
            timer.start()
            _pending_timers[user_id] = timer

    def _async_extract(self, user_id: str) -> None:
        """对话结束后的异步提炼：生成会话摘要。"""
        with _timer_lock:
            _pending_timers.pop(user_id, None)

        try:
            session_id = self._ems.current_session_id(user_id)
            messages = self._ems.get_session_messages(
                session_id, roles=("user", "assistant"), limit=20
            )
            if len(messages) < 2:
                return

            # 用简单规则提取摘要（不调 LLM，避免额外消耗）
            user_msgs = [m["content"] for m in messages if m["role"] == "user" and m["content"]]
            topics = user_msgs[:3]  # 取前 3 条用户消息作为主题
            summary = " → ".join(t[:30] for t in topics)

            self._smp.upsert(
                category="session_summary",
                key=session_id,
                value={
                    "summary": summary,
                    "msg_count": len(messages),
                    "topics": topics,
                },
                user_id=user_id,
                scope="user",
                source_session=session_id,
            )
            logger.info("Async extract: session %s summary saved for %s", session_id, user_id)

        except Exception as exc:
            logger.warning("Async extract failed for %s: %s", user_id, exc)

    def extract_with_llm(self, user_id: str, session_id: str) -> None:
        """
        用 LLM 生成高质量会话摘要和纠错记录。
        比 _async_extract 更准确但消耗 token。
        """
        try:
            from agent import llm

            full_session = self._ems.get_full_session(session_id)
            if len(full_session) < 4:
                return

            # 构建对话文本
            conv_text = "\n".join(
                f"[{m['role']}] {m['content'][:200]}"
                for m in full_session
                if m["role"] in ("user", "assistant") and m["content"]
            )

            prompt = f"""分析以下对话，提取：
1. 一句话摘要（summary）
2. 主要话题列表（topics）
3. 纠错记录：用户纠正了哪些错误（corrections，数组，每条含 wrong 和 correct）

对话记录：
{conv_text[:2000]}

用 JSON 格式回复（不要代码块标记）：
{{"summary": "...", "topics": ["..."], "corrections": [{{"wrong": "...", "correct": "..."}}]}}"""

            result = llm.call(
                [{"role": "user", "content": prompt}],
                system_override="你是一个对话分析助手，从对话中提取结构化信息。只输出 JSON。",
            )
            text = result.get("text", "")
            import re
            m = re.search(r'\{[\s\S]+\}', text)
            if m:
                data = json.loads(m.group())
                # 保存摘要
                self._smp.upsert(
                    category="session_summary",
                    key=session_id,
                    value=data,
                    user_id=user_id,
                    scope="user",
                    source_session=session_id,
                )
                # 保存纠错
                for corr in data.get("corrections", []):
                    if corr.get("wrong") and corr.get("correct"):
                        ckey = f"corr_{hash(corr['wrong']) & 0xFFFFFFFF:08x}"
                        self._smp.upsert(
                            category="correction",
                            key=ckey,
                            value=corr,
                            user_id=user_id,
                            scope="user",
                            source_session=session_id,
                        )
                        # 纠错也写入 org
                        self._smp.upsert_org(
                            category="correction",
                            key=ckey,
                            value=corr,
                            source_session=session_id,
                            confidence=0.5,
                        )

        except Exception as exc:
            logger.warning("LLM extract failed for session %s: %s", session_id, exc)
