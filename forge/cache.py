"""
SQL 查询缓存 — 基于用户双重确认的查询复用机制。

生命周期：
    Stage 1: 用户确认 SQL 执行 → add_pending()  → status=pending
    Stage 2: 用户确认结果准确  → verify()        → status=verified
             用户标记结果不准  → reject()         → status=rejected（软删除）

查询：
    lookup_fuzzy()  基于 embedding 余弦相似度，跳过 LLM 直接复用（仅 verified）
    lookup_exact()  基于 Forge JSON hash，编译后精确匹配

schema 变更后旧缓存自动失效（通过 schema_hash 字段比对）。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from config import cfg

_DB_PATH = Path(cfg.REGISTRY_PATH).parent / ".forge" / "sql_cache.db"


@dataclass
class CacheEntry:
    cache_id:   str
    question:   str
    forge_json: dict
    sql:        str
    status:     str   # pending / verified / rejected
    hit_count:  int
    schema_hash: str


class SQLCache:
    def __init__(self, db_path: Path = _DB_PATH) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self) -> None:
        with self._lock:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS sql_cache (
                    cache_id     TEXT PRIMARY KEY,
                    question     TEXT NOT NULL,
                    question_emb BLOB,
                    forge_json   TEXT NOT NULL,
                    forge_hash   TEXT NOT NULL,
                    sql          TEXT NOT NULL,
                    schema_hash  TEXT NOT NULL,
                    status       TEXT NOT NULL DEFAULT 'pending',
                    hit_count    INTEGER NOT NULL DEFAULT 0,
                    created_at   TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                    last_used_at TEXT
                )
            """)
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_forge ON sql_cache(forge_hash, schema_hash)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_status ON sql_cache(status, schema_hash)"
            )
            self._conn.commit()

    # ── 哈希工具 ─────────────────────────────────────────────────────────────

    @staticmethod
    def forge_hash(forge_json: dict) -> str:
        """规范化 Forge JSON → MD5，作为精确匹配的 key。"""
        s = json.dumps(forge_json, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(s.encode()).hexdigest()

    @staticmethod
    def schema_hash() -> str:
        try:
            return hashlib.md5(cfg.REGISTRY_PATH.read_bytes()).hexdigest()[:16]
        except Exception:
            return "unknown"

    # ── 查询 ─────────────────────────────────────────────────────────────────

    def lookup_exact(self, forge_json: dict) -> CacheEntry | None:
        """Forge JSON hash 精确命中（verified 优先，其次 pending）。"""
        fh = self.forge_hash(forge_json)
        sh = self.schema_hash()
        with self._lock:
            cur = self._conn.execute(
                """SELECT cache_id, question, forge_json, sql, status, hit_count, schema_hash
                   FROM sql_cache
                   WHERE forge_hash=? AND schema_hash=? AND status != 'rejected'
                   ORDER BY CASE status WHEN 'verified' THEN 0 ELSE 1 END,
                            hit_count DESC
                   LIMIT 1""",
                (fh, sh),
            )
            row = cur.fetchone()
        if not row:
            return None
        return CacheEntry(
            cache_id=row[0], question=row[1],
            forge_json=json.loads(row[2]), sql=row[3],
            status=row[4], hit_count=row[5], schema_hash=row[6],
        )

    def lookup_fuzzy(
        self,
        question_emb: np.ndarray,
        threshold: float = 0.92,
    ) -> CacheEntry | None:
        """
        Embedding 余弦相似度模糊匹配，仅返回 verified 条目。
        命中时可直接跳过 LLM。
        """
        sh = self.schema_hash()
        with self._lock:
            cur = self._conn.execute(
                """SELECT cache_id, question, question_emb, forge_json, sql, hit_count
                   FROM sql_cache
                   WHERE status='verified' AND schema_hash=? AND question_emb IS NOT NULL""",
                (sh,),
            )
            rows = cur.fetchall()
        if not rows:
            return None

        q = question_emb.astype(np.float32)
        q = q / (np.linalg.norm(q) + 1e-9)
        best_score, best_row = -1.0, None
        for row in rows:
            emb = np.frombuffer(row[2], dtype=np.float32)
            emb = emb / (np.linalg.norm(emb) + 1e-9)
            score = float(np.dot(q, emb))
            if score > best_score:
                best_score, best_row = score, row

        if best_score < threshold or best_row is None:
            return None
        return CacheEntry(
            cache_id=best_row[0], question=best_row[1],
            forge_json=json.loads(best_row[3]), sql=best_row[4],
            status="verified", hit_count=best_row[5],
            schema_hash=sh,
        )

    # ── 写入 ─────────────────────────────────────────────────────────────────

    def add_pending(
        self,
        question: str,
        question_emb: np.ndarray | None,
        forge_json: dict,
        sql: str,
    ) -> str:
        """
        Stage 1：用户确认执行后写入 pending 条目。
        若已有相同 forge_hash 的 verified 条目则跳过，返回空字符串。
        """
        fh = self.forge_hash(forge_json)
        sh = self.schema_hash()
        emb_bytes = (
            question_emb.astype(np.float32).tobytes()
            if question_emb is not None else None
        )
        with self._lock:
            # 已有 verified，无需重复写入
            cur = self._conn.execute(
                "SELECT cache_id FROM sql_cache WHERE forge_hash=? AND schema_hash=? AND status='verified' LIMIT 1",
                (fh, sh),
            )
            if cur.fetchone():
                return ""
            cid = uuid.uuid4().hex
            self._conn.execute(
                """INSERT INTO sql_cache
                   (cache_id, question, question_emb, forge_json, forge_hash, sql, schema_hash)
                   VALUES (?,?,?,?,?,?,?)""",
                (cid, question, emb_bytes,
                 json.dumps(forge_json, ensure_ascii=False), fh, sql, sh),
            )
            self._conn.commit()
        return cid

    def verify(self, cache_id: str) -> None:
        """Stage 2 👍：pending → verified。"""
        with self._lock:
            self._conn.execute(
                "UPDATE sql_cache SET status='verified', last_used_at=datetime('now','localtime') WHERE cache_id=?",
                (cache_id,),
            )
            self._conn.commit()

    def reject(self, cache_id: str) -> None:
        """Stage 2 👎：pending → rejected（软删除）。"""
        with self._lock:
            self._conn.execute(
                "UPDATE sql_cache SET status='rejected' WHERE cache_id=?",
                (cache_id,),
            )
            self._conn.commit()

    def record_hit(self, cache_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE sql_cache SET hit_count=hit_count+1, last_used_at=datetime('now','localtime') WHERE cache_id=?",
                (cache_id,),
            )
            self._conn.commit()

    # ── 语义层洞察 ───────────────────────────────────────────────────────────

    def suggest_metrics(self, min_hits: int = 5) -> list[dict]:
        """
        返回高频已验证的查询，作为「建议注册为指标」的候选列表。
        用于语义层治理：发现数据飞轮中沉淀的业务知识。
        """
        sh = self.schema_hash()
        with self._lock:
            cur = self._conn.execute(
                """SELECT cache_id, question, sql, hit_count, last_used_at
                   FROM sql_cache
                   WHERE status='verified' AND schema_hash=? AND hit_count >= ?
                   ORDER BY hit_count DESC""",
                (sh, min_hits),
            )
            return [
                {"cache_id": r[0], "question": r[1], "sql": r[2],
                 "hit_count": r[3], "last_used_at": r[4]}
                for r in cur.fetchall()
            ]


# 全局单例
cache = SQLCache()
