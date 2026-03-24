"""
知识收集框架 — 统一五通道的候选知识管理。

五个通道：
    1. web_ui:       管理员手动录入
    2. conversation:  对话中自动提取
    3. document:      文档导入（PDF/Markdown）
    4. web_search:    搜索引擎 + URL 抓取
    5. rss:           RSS 订阅

所有通道产出 KnowledgeCandidate → 用户确认 → 写入 business_context / SMP。

用法：
    from agent.knowledge import knowledge_store

    # 添加候选
    knowledge_store.add_candidate(
        source="conversation", category="threshold",
        key="refund_rate_warning", value={"threshold": 0.05},
        scope="team:marketing",
    )

    # 审核
    candidates = knowledge_store.list_candidates(status="pending")
    knowledge_store.confirm(candidate_id)
    knowledge_store.reject(candidate_id)
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS knowledge_candidates (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT NOT NULL,           -- web_ui / conversation / document / web_search / rss
    source_url  TEXT DEFAULT '',         -- 来源 URL、session_id、文件路径
    category    TEXT NOT NULL,           -- threshold / calendar / benchmark / rule / fact
    key         TEXT NOT NULL,
    value       TEXT NOT NULL,           -- JSON
    extracted_by TEXT DEFAULT 'human',   -- human / llm
    confidence  REAL DEFAULT 1.0,
    scope       TEXT DEFAULT 'org',      -- org / team:{id} / user:{id}
    status      TEXT DEFAULT 'pending',  -- pending / confirmed / rejected
    reviewed_by TEXT DEFAULT '',         -- 审核人 user_id
    created_at  TEXT NOT NULL DEFAULT (datetime('now','utc')),
    reviewed_at TEXT DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_kc_status ON knowledge_candidates(status, created_at);

CREATE TABLE IF NOT EXISTS knowledge_sources (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    type        TEXT NOT NULL,           -- rss / web_search / url_fetch
    name        TEXT NOT NULL,
    config      TEXT NOT NULL,           -- JSON: {url, keywords, schedule, ...}
    enabled     BOOLEAN DEFAULT 1,
    last_run    TEXT DEFAULT '',
    created_at  TEXT NOT NULL DEFAULT (datetime('now','utc'))
);
"""


class KnowledgeStore:
    """知识候选管理。"""

    def __init__(self):
        self._conn = None
        self._init_db()

    def _init_db(self) -> None:
        try:
            from agent.db import execute_ddl
            execute_ddl(_DDL)
        except Exception as exc:
            logger.warning("Knowledge DB init failed: %s", exc)

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

    # ── 候选管理 ──────────────────────────────────────────────────────────────

    def add_candidate(
        self,
        source: str,
        category: str,
        key: str,
        value: Any,
        *,
        scope: str = "org",
        source_url: str = "",
        extracted_by: str = "human",
        confidence: float = 1.0,
    ) -> int:
        """添加一条知识候选。返回 ID。"""
        conn = self._ensure_conn()
        value_str = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
        cursor = conn.execute(
            "INSERT INTO knowledge_candidates "
            "(source, source_url, category, key, value, extracted_by, confidence, scope) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (source, source_url, category, key, value_str, extracted_by, confidence, scope),
        )
        conn.commit()
        return cursor.lastrowid

    def list_candidates(
        self,
        status: str = "pending",
        limit: int = 50,
    ) -> list[dict]:
        """列出候选知识（默认待审核）。"""
        conn = self._ensure_conn()
        rows = conn.execute(
            "SELECT id, source, source_url, category, key, value, extracted_by, "
            "confidence, scope, status, created_at "
            "FROM knowledge_candidates WHERE status = ? "
            "ORDER BY created_at DESC LIMIT ?",
            (status, limit),
        ).fetchall()
        results = []
        for r in rows:
            try:
                val = json.loads(r[5])
            except (json.JSONDecodeError, TypeError):
                val = r[5]
            results.append({
                "id": r[0], "source": r[1], "source_url": r[2],
                "category": r[3], "key": r[4], "value": val,
                "extracted_by": r[6], "confidence": r[7], "scope": r[8],
                "status": r[9], "created_at": r[10],
            })
        return results

    def confirm(self, candidate_id: int, reviewed_by: str = "") -> bool:
        """确认候选 → 写入 business_context 或 SMP。"""
        conn = self._ensure_conn()
        row = conn.execute(
            "SELECT category, key, value, scope FROM knowledge_candidates WHERE id = ?",
            (candidate_id,),
        ).fetchone()
        if not row:
            return False

        conn.execute(
            "UPDATE knowledge_candidates SET status = 'confirmed', "
            "reviewed_by = ?, reviewed_at = datetime('now','utc') WHERE id = ?",
            (reviewed_by, candidate_id),
        )
        conn.commit()

        # 写入 SMP
        category, key, value_str, scope = row
        try:
            from agent.memory import memory
            try:
                value = json.loads(value_str)
            except (json.JSONDecodeError, TypeError):
                value = value_str

            if scope == "org":
                memory.smp.upsert_org(category, key, value)
            elif scope.startswith("team:"):
                team_id = scope.split(":", 1)[1]
                memory.smp.upsert_team(team_id, category, key, value)
            else:
                user_id = scope.split(":", 1)[1] if ":" in scope else scope
                memory.smp.upsert(category, key, value, user_id=user_id, scope="user")
        except Exception as exc:
            logger.warning("Failed to write confirmed knowledge to SMP: %s", exc)

        return True

    def reject(self, candidate_id: int, reviewed_by: str = "") -> bool:
        """拒绝候选。"""
        conn = self._ensure_conn()
        conn.execute(
            "UPDATE knowledge_candidates SET status = 'rejected', "
            "reviewed_by = ?, reviewed_at = datetime('now','utc') WHERE id = ?",
            (reviewed_by, candidate_id),
        )
        conn.commit()
        return True

    def pending_count(self) -> int:
        """待审核数量。"""
        conn = self._ensure_conn()
        row = conn.execute(
            "SELECT COUNT(*) FROM knowledge_candidates WHERE status = 'pending'"
        ).fetchone()
        return row[0] if row else 0

    # ── 知识源管理 ────────────────────────────────────────────────────────────

    def add_source(self, type: str, name: str, config: dict) -> int:
        """添加知识源（RSS / Web Search / URL Fetch）。"""
        conn = self._ensure_conn()
        cursor = conn.execute(
            "INSERT INTO knowledge_sources (type, name, config) VALUES (?, ?, ?)",
            (type, name, json.dumps(config, ensure_ascii=False)),
        )
        conn.commit()
        return cursor.lastrowid

    def list_sources(self, enabled_only: bool = True) -> list[dict]:
        """列出知识源。"""
        conn = self._ensure_conn()
        where = "WHERE enabled = TRUE" if enabled_only else ""
        rows = conn.execute(
            f"SELECT id, type, name, config, enabled, last_run, created_at "
            f"FROM knowledge_sources {where} ORDER BY created_at DESC"
        ).fetchall()
        results = []
        for r in rows:
            try:
                config = json.loads(r[3])
            except (json.JSONDecodeError, TypeError):
                config = {}
            results.append({
                "id": r[0], "type": r[1], "name": r[2], "config": config,
                "enabled": bool(r[4]), "last_run": r[5], "created_at": r[6],
            })
        return results

    def update_source_last_run(self, source_id: int) -> None:
        conn = self._ensure_conn()
        conn.execute(
            "UPDATE knowledge_sources SET last_run = datetime('now','utc') WHERE id = ?",
            (source_id,),
        )
        conn.commit()

    def delete_source(self, source_id: int) -> None:
        conn = self._ensure_conn()
        conn.execute("DELETE FROM knowledge_sources WHERE id = ?", (source_id,))
        conn.commit()


# 全局单例
knowledge_store = KnowledgeStore()


# ── 知识收集执行器 ─────────────────────────────────────────────────────────────

class KnowledgeCollector:
    """
    知识收集执行器。
    遍历 enabled=True 的 KnowledgeSource，拉取内容，提取候选知识。
    """

    def run_all(self) -> dict:
        """运行所有启用的知识源。返回统计 {"processed": N, "added": N, "errors": N}."""
        sources = knowledge_store.list_sources(enabled_only=True)
        processed = 0
        added = 0
        errors = 0
        for source in sources:
            try:
                n = self.run_source(source)
                added += n
                processed += 1
            except Exception as exc:
                logger.warning("Knowledge source %s (%s) failed: %s", source.get("id"), source.get("name"), exc)
                errors += 1
        return {"processed": processed, "added": added, "errors": errors}

    def run_source(self, source: dict) -> int:
        """运行单个知识源。返回新增候选数。"""
        src_type = source.get("type", "")
        if src_type == "rss":
            n = self._collect_rss(source)
        elif src_type == "url_fetch":
            n = self._collect_url_fetch(source)
        elif src_type == "web_search":
            n = self._collect_web_search(source)
        else:
            logger.warning("Unknown knowledge source type: %s", src_type)
            n = 0
        knowledge_store.update_source_last_run(source["id"])
        return n

    def _collect_rss(self, source: dict) -> int:
        """RSS 订阅：拉取 feed，每条 entry 作为一个候选。"""
        import httpx
        import xml.etree.ElementTree as ET

        config = source.get("config", {})
        url = config.get("url", "")
        if not url:
            logger.warning("RSS source %s has no URL", source.get("name"))
            return 0

        try:
            resp = httpx.get(url, timeout=15, follow_redirects=True)
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("RSS fetch failed for %s: %s", url, exc)
            return 0

        added = 0
        try:
            root = ET.fromstring(resp.text)
            # 支持 RSS 2.0 和 Atom 格式
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            # RSS 2.0: channel/item
            items = root.findall(".//item")
            # Atom: feed/entry
            if not items:
                items = root.findall(".//atom:entry", ns) or root.findall(".//entry")

            for item in items:
                # RSS 2.0 字段
                title_el = item.find("title")
                desc_el   = item.find("description") or item.find("summary")
                link_el   = item.find("link")
                # Atom 字段
                if title_el is None:
                    title_el = item.find("atom:title", ns)
                if desc_el is None:
                    desc_el = item.find("atom:summary", ns) or item.find("atom:content", ns)
                if link_el is None:
                    link_el = item.find("atom:link", ns)

                title   = (title_el.text or "").strip() if title_el is not None else ""
                summary = (desc_el.text   or "").strip() if desc_el   is not None else ""
                # Atom link 可能是属性 href
                if link_el is not None:
                    link = link_el.text or link_el.get("href", "")
                else:
                    link = ""

                if not title and not summary:
                    continue

                value = f"title: {title}\n{summary[:500]}" if title else summary[:500]
                try:
                    knowledge_store.add_candidate(
                        source="rss",
                        category="fact",
                        key=title[:80] if title else "rss_entry",
                        value=value,
                        source_url=link[:200],
                        extracted_by="auto",
                        confidence=0.8,
                    )
                    added += 1
                except Exception as exc:
                    logger.debug("Failed to add RSS candidate: %s", exc)
        except ET.ParseError as exc:
            logger.warning("RSS XML parse error for %s: %s", url, exc)

        return added

    def _collect_url_fetch(self, source: dict) -> int:
        """URL 抓取：fetch HTML，用 LLM 提取关键知识点。"""
        import httpx
        import re

        config = source.get("config", {})
        url = config.get("url", "")
        if not url:
            logger.warning("URL fetch source %s has no URL", source.get("name"))
            return 0

        try:
            resp = httpx.get(url, timeout=15, follow_redirects=True)
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("URL fetch failed for %s: %s", url, exc)
            return 0

        # 去掉 HTML 标签，取前 2000 字符
        html = resp.text
        text = re.sub(r"<[^>]+>", "", html)
        text = re.sub(r"\s+", " ", text).strip()
        text = text[:2000]

        if not text:
            return 0

        # 尝试用 LLM 提取知识点
        added = 0
        try:
            from agent import llm as llm_module
            system_prompt = (
                "你是知识提取助手。从以下网页内容中提取3-5条有价值的业务知识点，"
                "每条50字以内，JSON数组格式：[{\"key\":\"知识点标题\",\"value\":\"内容\"}]"
                "只输出 JSON 数组，不要有其他内容。"
            )
            messages = [{"role": "user", "content": f"URL: {url}\n\n{text}"}]
            result = llm_module.call(messages, system_override=system_prompt)
            raw = result.get("text", "") or ""
            # 尝试解析 JSON
            json_match = re.search(r"\[.*\]", raw, re.DOTALL)
            if json_match:
                items = json.loads(json_match.group())
                for item in items:
                    k = str(item.get("key", "url_fact"))[:80]
                    v = str(item.get("value", ""))[:500]
                    if not v:
                        continue
                    try:
                        knowledge_store.add_candidate(
                            source="web_search",
                            category="fact",
                            key=k,
                            value=v,
                            source_url=url[:200],
                            extracted_by="llm",
                            confidence=0.7,
                        )
                        added += 1
                    except Exception as exc:
                        logger.debug("Failed to add URL fetch candidate: %s", exc)
            else:
                raise ValueError("No JSON array in LLM response")
        except Exception as exc:
            logger.info("LLM extraction failed for %s (%s), storing raw summary", url, exc)
            # 降级：存整个摘要为一条
            try:
                knowledge_store.add_candidate(
                    source="web_search",
                    category="fact",
                    key=f"url_summary_{source.get('name', 'fetch')}",
                    value=text[:500],
                    source_url=url[:200],
                    extracted_by="auto",
                    confidence=0.5,
                )
                added = 1
            except Exception as exc2:
                logger.debug("Failed to store URL summary: %s", exc2)

        return added

    def _collect_web_search(self, source: dict) -> int:
        """WebSearch：暂时 stub，返回 0。"""
        # TODO: 需要外部搜索 API
        return 0


# 全局收集器单例
knowledge_collector = KnowledgeCollector()
