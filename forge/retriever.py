"""
Schema 向量检索器 (Schema Retriever)

解决问题：
    当 Registry 包含几十/几百张表时，把完整 schema 放进 prompt 会超 context window。
    本模块通过向量相似度，只把与用户问题最相关的 top-k 张表注入 prompt。

工作流程：
    1. build_index(embed_fn)
           为每张表生成描述文本 → 调用 embedding API → L2 归一化 → 缓存为 .pkl 文件
    2. retrieve(question, embed_fn, top_k=3)
           嵌入问题 → cosine 相似度排序 → 返回 top-k 表名
           若 embedding API 不可用，自动降级到 BM25-lite 关键词匹配
    3. get_schema_ddl(table_names)
           为选中的表生成 DDL 风格的 schema 文本，直接注入 system prompt

典型用法：
    from forge.retriever import SchemaRetriever, make_embed_fn
    import json

    registry = json.load(open("schema.registry.json"))
    embed_fn = make_embed_fn(api_key=..., base_url="https://api.xxx.com/v1", model="embo-01")

    retriever = SchemaRetriever(registry, cache_path=".forge/schema_embeddings.pkl")
    if not retriever.load_index():
        retriever.build_index(embed_fn)   # 首次或 schema 变更后调用

    # 每次查询时
    tables = retriever.retrieve("统计各城市 VIP 用户的消费总额", embed_fn, top_k=3)
    schema_context = retriever.get_schema_ddl(tables)
    # → 只包含 users / orders 两张表的 schema（自动过滤不相关的 products/order_items）
"""
from __future__ import annotations

import math
import pickle
import re
from pathlib import Path
from typing import Callable

import numpy as np


class SchemaRetriever:
    """
    基于向量嵌入的 Schema 表检索器。

    Args:
        registry:    schema.registry.json 解析后的字典
        cache_path:  向量索引缓存路径（.pkl），None 则不持久化
    """

    def __init__(
        self,
        registry: dict,
        cache_path: Path | str | None = None,
    ) -> None:
        self.registry = registry
        self.tables: list[str] = list(registry.get("tables", {}).keys())
        self._fks: dict[str, set[str]] = self._detect_fks()   # 先检测 FK，再构建文本
        self.table_texts: dict[str, str] = self._build_table_texts()
        self._embeddings: np.ndarray | None = None   # shape: (n_tables, embed_dim)
        self._idf: dict[str, float] | None = None    # BM25-lite 降级缓存
        self.cache_path = Path(cache_path) if cache_path else None

    # ── FK 检测 ───────────────────────────────────────────────────────────────

    def _detect_fks(self) -> dict[str, set[str]]:
        """
        基于命名约定自动检测外键关系（规范 schema 假设）。

        规则：列名以 `_id` 结尾且去掉后缀后与已知表名匹配，则视为 FK。
        例：orders.user_id → users，order_items.order_id → orders

        Returns:
            {table: {直接引用的表集合}}
        """
        tables_info = self.registry.get("tables", self.registry)
        table_set = set(self.tables)
        fks: dict[str, set[str]] = {t: set() for t in self.tables}

        for table, info in tables_info.items():
            if not isinstance(info, dict):
                continue
            cols = info.get("columns", {})
            col_names = cols.keys() if isinstance(cols, dict) else cols
            for col in col_names:
                if col.endswith("_id"):
                    stem = col[:-3]   # 去掉 _id 后缀，如 user_id → user
                    # 候选引用：单数/复数 × 无前缀/常见前缀（dim_ / dwd_ / ods_）
                    candidates = [stem, stem + "s"]
                    for prefix in ("dim_", "dwd_", "ods_", "dws_", "ads_"):
                        candidates.append(prefix + stem)
                        candidates.append(prefix + stem + "s")
                    for ref in candidates:
                        if ref in table_set and ref != table:
                            fks[table].add(ref)
                            break

        return fks

    # ── 文本构建 ──────────────────────────────────────────────────────────────

    def _build_table_texts(self) -> dict[str, str]:
        """
        为每张表生成富文本描述，用于嵌入。

        格式：
            Table: {name}. Description: {desc}.
            Columns: {col1 (枚举值), col2, ...}.
            Related tables: {fk_table1, fk_table2}.   ← 新增，让向量感知 JOIN 关系

        包含枚举值：能让 "已完成订单" 命中 orders.status=completed。
        包含关联表：能让 "用户消费" 同时召回 orders（通过 users 的向量）。
        """
        texts: dict[str, str] = {}
        tables_info = self.registry.get("tables", self.registry)

        for table_name, info in tables_info.items():
            parts = [f"Table: {table_name}"]

            if isinstance(info, dict):
                desc = info.get("description", "")
                if desc:
                    parts.append(f"Description: {desc}")

                cols = info.get("columns", {})
                col_parts: list[str] = []
                items = cols.items() if isinstance(cols, dict) else [(c, {}) for c in cols]
                for col_name, meta in items:
                    if isinstance(meta, dict) and meta.get("enum"):
                        vals = ", ".join(str(v) for v in meta["enum"][:8])
                        col_parts.append(f"{col_name} ({vals})")
                    else:
                        col_parts.append(col_name)

                parts.append(f"Columns: {', '.join(col_parts)}")

            # 注入 FK 关系（已在 _detect_fks 中计算）
            related = self._fks.get(table_name, set())
            if related:
                parts.append(f"Related tables: {', '.join(sorted(related))}")

            texts[table_name] = ". ".join(parts)

        return texts

    # ── 索引构建与加载 ─────────────────────────────────────────────────────────

    def build_index(self, embed_fn: Callable[[list[str]], np.ndarray]) -> None:
        """
        批量嵌入所有表描述，L2 归一化后缓存到 cache_path。

        Args:
            embed_fn: 接受 list[str]，返回 np.ndarray shape (n, d)
        """
        texts = [self.table_texts[t] for t in self.tables]
        raw = embed_fn(texts)
        self._embeddings = self._normalize(np.array(raw, dtype=np.float32))

        if self.cache_path:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_path, "wb") as f:
                pickle.dump({
                    "tables": self.tables,
                    "embeddings": self._embeddings,
                    "table_texts": self.table_texts,
                }, f)

    def load_index(self) -> bool:
        """
        从 cache_path 加载已有向量索引。

        Returns:
            True：加载成功（表集合未变，可直接使用）
            False：缓存不存在或已过期（需重新 build_index）
        """
        if not (self.cache_path and self.cache_path.exists()):
            return False

        with open(self.cache_path, "rb") as f:
            data = pickle.load(f)

        # 表集合变化时缓存失效
        if data.get("tables") != self.tables:
            return False

        self._embeddings = data["embeddings"]
        return True

    # ── 检索 ─────────────────────────────────────────────────────────────────

    def retrieve(
        self,
        question: str,
        embed_fn: Callable[[list[str]], np.ndarray] | None = None,
        top_k: int = 3,
    ) -> list[str]:
        """
        三层检索策略，返回与问题相关的表名列表。

        Phase 1 — 表级语义检索（向量 or BM25）：
            抓住"订单金额"→ orders、"用户城市"→ users 这类语义关联。

        Phase 2 — 列名关键词补充：
            规范 schema 下列名本身即语义，直接匹配问题中出现的词。
            补充 Phase 1 可能遗漏的表（如问题里直接提到了 product_id）。

        Phase 3 — FK 自动扩展：
            基于 _detect_fks 的外键图，把已选表的直接依赖表拉进来。
            确保 JOIN 所需的父表不被遗漏（如 order_items → orders → users）。

        Args:
            question: 用户自然语言问题
            embed_fn: 嵌入函数（可 None，触发降级）
            top_k:    Phase 1+2 的初始召回数量（FK 扩展后可能更多）

        Returns:
            表名列表，按相关度降序排列（Phase 1 优先，FK 扩展附加在后）
        """
        if top_k >= len(self.tables):
            return list(self.tables)

        # Phase 1：表级语义检索
        if self._embeddings is not None and embed_fn is not None:
            candidates = self._retrieve_by_embedding(question, embed_fn, top_k)
        else:
            candidates = self._retrieve_by_keywords(question, top_k)

        # Phase 2：列名关键词补充（去重合并，candidates 优先）
        col_tables = self._retrieve_by_columns(question, top_k)
        seen: set[str] = set(candidates)
        merged = list(candidates)
        for t in col_tables:
            if t not in seen:
                seen.add(t)
                merged.append(t)

        # Phase 3：FK 扩展
        return self._expand_fks(merged)

    def _retrieve_by_embedding(
        self,
        question: str,
        embed_fn: Callable,
        top_k: int,
    ) -> list[str]:
        """向量检索：问题嵌入 → cosine 相似度排序。"""
        q_emb = self._normalize(np.array(embed_fn([question]), dtype=np.float32))  # (1, d)
        scores = (q_emb @ self._embeddings.T).flatten()   # (n_tables,)
        top_idx = np.argsort(scores)[::-1][:top_k]
        return [self.tables[i] for i in top_idx]

    def _retrieve_by_keywords(self, question: str, top_k: int) -> list[str]:
        """BM25-lite 降级：TF×IDF 关键词权重匹配。"""
        if self._idf is None:
            self._build_idf()

        q_terms = self._tokenize(question)
        scores: dict[str, float] = {}
        for table in self.tables:
            doc_terms = self._tokenize(self.table_texts[table])
            tf = {}
            for t in doc_terms:
                tf[t] = tf.get(t, 0) + 1
            score = sum(tf.get(t, 0) * self._idf.get(t, 0.0) for t in q_terms)
            scores[table] = score

        return sorted(self.tables, key=lambda t: scores[t], reverse=True)[:top_k]

    def _retrieve_by_columns(self, question: str, top_k: int) -> list[str]:
        """
        列名关键词匹配：在问题中查找与列名重叠的词，按命中数排序返回表。

        规范 schema 假设下列名有语义（total_amount、order_status），
        问题中可能直接出现列名词根（amount、status、category）。

        匹配策略：
          1. 问题 token 与列名完全匹配（最高权重）
          2. 问题 token 是列名的子串（如 "amount" in "total_amount"）
          3. 列名是问题 token 的子串（如 "city" in "city_name"）
          列的 description 字段若存在，同样参与 BM25 匹配。
        """
        q_tokens = set(self._tokenize(question.lower()))
        tables_info = self.registry.get("tables", self.registry)
        scores: dict[str, float] = {t: 0.0 for t in self.tables}

        for table, info in tables_info.items():
            if not isinstance(info, dict):
                continue
            cols = info.get("columns", {})
            items = cols.items() if isinstance(cols, dict) else [(c, {}) for c in cols]
            for col_name, meta in items:
                col_lower = col_name.lower()
                col_tokens = set(col_lower.split("_"))   # total_amount → {total, amount}

                # 完全匹配：问题 token 与列名词根完全吻合
                exact = q_tokens & col_tokens
                scores[table] += len(exact) * 2.0

                # 子串匹配：问题 token 包含在列名里，或列名包含在问题 token 里
                for qt in q_tokens:
                    if len(qt) >= 3 and (qt in col_lower or col_lower in qt):
                        scores[table] += 0.5

                # 列 description 里的 BM25 匹配
                if isinstance(meta, dict):
                    desc = meta.get("description", "")
                    if desc:
                        desc_tokens = set(self._tokenize(desc.lower()))
                        scores[table] += len(q_tokens & desc_tokens) * 1.0

        # 只返回有得分的表，按得分降序
        ranked = sorted(
            [t for t in self.tables if scores[t] > 0],
            key=lambda t: scores[t],
            reverse=True,
        )
        return ranked[:top_k]

    def _expand_fks(self, tables: list[str]) -> list[str]:
        """
        FK 图扩展：对已选表集合，加入它们直接引用的父表。

        只扩展一层（直接 FK），避免过度膨胀。
        已在列表中的表不重复添加，扩展的表附加在原列表末尾。

        例：[order_items] → [order_items, orders, products]
            [orders]      → [orders, users]
        """
        seen: set[str] = set(tables)
        expanded = list(tables)
        for table in list(tables):   # 遍历原始列表，不递归扩展新加入的表
            for ref in self._fks.get(table, set()):
                if ref not in seen:
                    seen.add(ref)
                    expanded.append(ref)
        return expanded

    def _build_idf(self) -> None:
        """预计算语料 IDF 权重（BM25-lite 使用）。"""
        N = len(self.tables)
        df: dict[str, int] = {}
        for table in self.tables:
            for term in set(self._tokenize(self.table_texts[table])):
                df[term] = df.get(term, 0) + 1
        self._idf = {t: math.log((N + 1) / (cnt + 1)) for t, cnt in df.items()}

    @staticmethod
    def _tokenize(text: str) -> list[str]:
        """
        分词策略：
        - 英文/数字：按非字母数字分隔，保留长度 >= 2 的词
        - 中文：字符级 bigram（滑动窗口长度 2），确保"品类"能从"商品品类分类"中匹配
          同时保留原始连续中文串（长度 >= 2），提升精确匹配权重
        """
        tokens: list[str] = []

        # 先按非字母数字/非汉字分割，得到「词块」
        for chunk in re.split(r'[^a-zA-Z0-9\u4e00-\u9fff]+', text.lower()):
            if not chunk:
                continue

            # 判断是否为纯中文块
            is_chinese = all('\u4e00' <= c <= '\u9fff' for c in chunk)
            if is_chinese:
                # 原串本身（精确匹配权重高）
                if len(chunk) >= 2:
                    tokens.append(chunk)
                # 字符级 bigram（模糊匹配）
                for i in range(len(chunk) - 1):
                    tokens.append(chunk[i:i + 2])
            else:
                # 英文/混合：整块保留
                if len(chunk) >= 2:
                    tokens.append(chunk)

        return tokens

    @staticmethod
    def _normalize(x: np.ndarray) -> np.ndarray:
        """L2 归一化（用于余弦相似度计算）。"""
        norms = np.linalg.norm(x, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1.0, norms)
        return x / norms

    # ── Schema DDL 生成 ───────────────────────────────────────────────────────

    def get_schema_ddl(self, table_names: list[str]) -> str:
        """
        为指定表集合生成 DDL 风格的 schema 文本，可直接注入 system prompt。

        示例输出：
            你可以查询以下数据库表（SQLite）：

            users (id, name, city, created_at, is_vip)
            orders (id, user_id, status, total_amount, created_at)

            字段枚举值：
            - orders.status: 'completed' | 'pending' | 'cancelled'
        """
        lines = ["你可以查询以下数据库表（SQLite）：", ""]
        tables_info = self.registry.get("tables", self.registry)
        enum_hints: list[str] = []

        for table_name in table_names:
            info = tables_info.get(table_name, {})
            if not isinstance(info, dict):
                lines.append(table_name)
                continue

            cols = info.get("columns", {})
            col_names: list[str] = []
            items = cols.items() if isinstance(cols, dict) else [(c, {}) for c in cols]
            for col_name, meta in items:
                col_names.append(col_name)
                if isinstance(meta, dict) and meta.get("enum"):
                    vals = " | ".join(
                        f"'{v}'" if isinstance(v, str) else str(v)
                        for v in meta["enum"]
                    )
                    enum_hints.append(f"- {table_name}.{col_name}: {vals}")

            lines.append(f"{table_name} ({', '.join(col_names)})")

        if enum_hints:
            lines += ["", "字段枚举值："] + enum_hints

        return "\n".join(lines)

    # ── 调试工具 ──────────────────────────────────────────────────────────────

    def explain(
        self,
        question: str,
        embed_fn: Callable[[list[str]], np.ndarray] | None = None,
        top_k: int = 3,
    ) -> str:
        """
        输出检索过程的可读说明（调试/测试用）。
        """
        mode = "向量" if (self._embeddings is not None and embed_fn) else "关键词(BM25)"
        tables = self.retrieve(question, embed_fn, top_k)

        lines = [
            f"问题: {question}",
            f"检索模式: {mode}",
            f"Top-{top_k} 相关表: {', '.join(tables)}",
            "",
            "检索到的 Schema:",
            self.get_schema_ddl(tables),
        ]
        return "\n".join(lines)


# ── 工厂函数 ───────────────────────────────────────────────────────────────────


def make_embed_fn(
    api_key: str,
    base_url: str,
    model: str,
    embed_type: str = "db",
) -> Callable[[list[str]], np.ndarray]:
    """
    工厂函数：返回调用 embedding API 的嵌入函数，自动适配多种响应格式。

    支持的响应格式（自动检测）：
    - 标准 OpenAI：{"data": [{"index": 0, "embedding": [...]}]}
    - MiniMax：{"vectors": [[...], [...]]}  （直接列表，无索引）
    - MiniMax（旧版）：{"vectors": [{"index": 0, "vector": [...]}]}

    MiniMax 请求格式与标准 OpenAI 不同：
    - 用 "texts" 而非 "input"
    - 需要 "type" 字段（"db" 表示被检索文档，"query" 表示查询）

    Args:
        api_key:    API 密钥
        base_url:   API 基础 URL（如 https://api.minimaxi.com/v1）
        model:      嵌入模型名（如 "embo-01" / "text-embedding-3-small"）
        embed_type: "db"（索引文档）或 "query"（查询文本），MiniMax 专用

    Returns:
        embed_fn(texts: list[str]) -> np.ndarray  shape (len(texts), embed_dim)
    """
    import requests as _requests

    _url = base_url.rstrip("/") + "/embeddings"
    _headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    def embed(texts: list[str], _type: str = embed_type) -> np.ndarray:
        # 先用 MiniMax 格式尝试（texts + type），失败则回落到 OpenAI 格式（input）
        for payload in [
            {"model": model, "texts": texts, "type": _type},
            {"model": model, "input": texts},
        ]:
            resp = _requests.post(_url, headers=_headers, json=payload, timeout=30)
            if resp.status_code != 200:
                continue
            body = resp.json()

            # 检查 API 层错误（MiniMax 用 base_resp.status_code 报错）
            base = body.get("base_resp", {})
            if base.get("status_code", 0) != 0:
                continue

            # 格式 1：MiniMax 直接列表 {"vectors": [[...], [...]]}
            if "vectors" in body and body["vectors"]:
                raw = body["vectors"]
                if isinstance(raw[0], list):        # 直接 float 列表
                    return np.array(raw, dtype=np.float32)
                if isinstance(raw[0], dict):        # 对象列表（旧版）
                    items = sorted(raw, key=lambda x: x.get("index", 0))
                    return np.array([x["vector"] for x in items], dtype=np.float32)

            # 格式 2：标准 OpenAI {"data": [{"index": 0, "embedding": [...]}]}
            if "data" in body and body["data"]:
                items = sorted(body["data"], key=lambda x: x.get("index", 0))
                return np.array([x["embedding"] for x in items], dtype=np.float32)

        raise ValueError(f"embedding API 调用失败，URL={_url}")

    return embed


def make_query_embed_fn(
    api_key: str,
    base_url: str,
    model: str,
) -> Callable[[list[str]], np.ndarray]:
    """
    与 make_embed_fn 相同，但 type="query"（用于查询文本的嵌入）。

    MiniMax 区分 db（文档）和 query（查询）两种嵌入类型，
    混用会降低检索质量。
    """
    return make_embed_fn(api_key, base_url, model, embed_type="query")
