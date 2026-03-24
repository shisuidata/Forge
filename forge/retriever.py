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
        metrics_registry: dict | None = None,
    ) -> None:
        self.registry = registry
        self.tables: list[str] = list(registry.get("tables", {}).keys())
        self._fks: dict[str, set[str]] = self._detect_fks()
        self._metrics_registry: dict = metrics_registry or {}
        # 指标→表 映射：从 metrics_registry 反推每张表承载哪些业务指标
        self._table_metrics: dict[str, list[str]] = self._build_table_metrics(self._metrics_registry)
        self.table_texts: dict[str, str] = self._build_table_texts()
        self._embeddings: np.ndarray | None = None
        self._idf: dict[str, float] | None = None
        self.cache_path = Path(cache_path) if cache_path else None

    # ── 指标→表 映射 ──────────────────────────────────────────────────────────

    def _build_table_metrics(self, metrics_registry: dict) -> dict[str, list[str]]:
        """
        从语义层（metrics.registry.yaml）反向推导每张表承载的业务指标。

        规范 schema 假设：metrics 的 measure / period_col / dimensions 字段均为
        table.column 格式，可以直接提取表名。

        Returns:
            {table_name: [metric_label, ...]}
            例：{"dwd_order_detail": ["GMV（成交总额）", "支付GMV", "订单量", "复购用户数", ...]}
        """
        table_to_labels: dict[str, list[str]] = {t: [] for t in self.tables}

        for metric_name, metric in metrics_registry.items():
            label = metric.get("label", metric_name)
            # 从 measure / period_col / dimensions 中提取表名
            ref_cols: list[str] = []
            if metric.get("measure"):
                ref_cols.append(metric["measure"])
            if metric.get("period_col"):
                ref_cols.append(metric["period_col"])
            for dim in metric.get("dimensions", []):
                if "." in dim:
                    ref_cols.append(dim)
            for numerator_or_denom in ("numerator", "denominator"):
                # 衍生指标：递归从原子指标里取 measure（简单处理：只取一层）
                ref_name = metric.get(numerator_or_denom, "")
                if ref_name and ref_name in metrics_registry:
                    ref_m = metrics_registry[ref_name].get("measure", "")
                    if ref_m:
                        ref_cols.append(ref_m)

            for col_ref in ref_cols:
                if "." in col_ref:
                    table = col_ref.split(".")[0]
                    if table in table_to_labels and label not in table_to_labels[table]:
                        table_to_labels[table].append(label)

        return table_to_labels

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
                    if isinstance(meta, dict):
                        hints = []
                        if meta.get("description"):
                            hints.append(meta["description"])
                        if meta.get("enum"):
                            hints.append(", ".join(str(v) for v in meta["enum"][:6]))
                        col_parts.append(f"{col_name} ({'; '.join(hints)})" if hints else col_name)
                    else:
                        col_parts.append(col_name)

                parts.append(f"Columns: {', '.join(col_parts)}")

            # 注入 FK 关系（已在 _detect_fks 中计算）
            related = self._fks.get(table_name, set())
            if related:
                parts.append(f"Related tables: {', '.join(sorted(related))}")

            # 注入业务指标（已在 _build_table_metrics 中计算）
            # 让 "dwd_order_detail" 的向量包含 "GMV / 客单价 / 复购率" 等业务词
            metrics = self._table_metrics.get(table_name, [])
            if metrics:
                parts.append(f"业务指标: {', '.join(metrics[:12])}")

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
        # 分批调用 embedding API（部分 API 对单次请求条数有限制）
        batch_size = 32
        parts = []
        for i in range(0, len(texts), batch_size):
            parts.append(embed_fn(texts[i:i + batch_size]))
        raw = np.concatenate(parts, axis=0)
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
        allowed_tables: list[str] | None = None,
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
            question:       用户自然语言问题
            embed_fn:       嵌入函数（可 None，触发降级）
            top_k:          Phase 1+2 的初始召回数量（FK 扩展后可能更多）
            allowed_tables: 数据权限白名单；非 None 时只在该列表内检索

        Returns:
            表名列表，按相关度降序排列（Phase 1 优先，FK 扩展附加在后）
        """
        # 数据权限：限制可见表范围
        effective_tables = self.tables
        if allowed_tables is not None:
            allowed_set = set(allowed_tables)
            effective_tables = [t for t in self.tables if t in allowed_set]
            if not effective_tables:
                return []

        if top_k >= len(effective_tables):
            return list(effective_tables)

        # 动态 top_k：根据问题中实体数量自动调整召回数
        # 每识别到一个独立实体词（"品牌" "退款" "渠道"）+1，上限为 top_k * 2
        top_k = self._dynamic_top_k(question, top_k)

        # Phase 0：指标名称直接匹配（规范语义层假设）
        # 若问题命中已注册的业务指标，直接从其定义中提取所需表，精度最高
        metric_tables = self._retrieve_by_metrics(question)

        # Phase 1：表级语义检索（embedding 不可用或含 NaN 时自动降级 BM25）
        if (self._embeddings is not None
                and embed_fn is not None
                and not np.isnan(self._embeddings).any()):
            candidates = self._retrieve_by_embedding(question, embed_fn, top_k)
        else:
            if self._embeddings is not None and np.isnan(self._embeddings).any():
                logger.warning("Embedding index contains NaN, using BM25 fallback")
            candidates = self._retrieve_by_keywords(question, top_k)

        # Phase 2：列名关键词补充（去重合并，candidates 优先）
        col_tables = self._retrieve_by_columns(question, top_k)

        # 合并优先级：metric_tables > vector/BM25 > column match
        allowed_set = set(effective_tables) if allowed_tables is not None else None
        seen: set[str] = set()
        merged: list[str] = []
        for t in metric_tables + candidates:
            if t not in seen and (allowed_set is None or t in allowed_set):
                seen.add(t)
                merged.append(t)
        for t in col_tables:
            if t not in seen and (allowed_set is None or t in allowed_set):
                seen.add(t)
                merged.append(t)

        # Phase 3：FK 扩展（扩展后也过滤权限）
        expanded = self._expand_fks(merged)
        if allowed_set is not None:
            expanded = [t for t in expanded if t in allowed_set]
        return expanded

    def _dynamic_top_k(self, question: str, base_k: int) -> int:
        """
        根据问题复杂度动态调整 top_k。

        策略：统计问题中出现的"域关键词"数量——
        每多一个不同域（用户/订单/商品/评价/退款/购物车/渠道/品牌/品类），
        top_k 加 1，最终值在 [base_k, base_k * 2] 区间内。

        目的：简单查询（单表）不引入噪声表；复杂多维查询不漏召。
        """
        domain_keywords = [
            ('用户', '会员', 'vip', 'user'),
            ('订单', '成交', '下单', 'order'),
            ('商品', '产品', 'product', '货品'),
            ('评价', '评分', '好评', '差评', 'comment', 'rating'),
            ('退款', '售后', 'refund'),
            ('购物车', '加购', 'cart'),
            ('渠道', '来源', 'channel'),
            ('品牌', 'brand'),
            ('品类', '分类', 'category'),
            ('支付', '付款', 'payment'),
        ]
        q = question.lower()
        hit_domains = sum(
            1 for domain in domain_keywords
            if any(kw in q for kw in domain)
        )
        adjusted = base_k + max(0, hit_domains - 1)
        return min(adjusted, base_k * 2)

    def _retrieve_by_embedding(
        self,
        question: str,
        embed_fn: Callable,
        top_k: int,
    ) -> list[str]:
        """向量检索：问题嵌入 → cosine 相似度排序。失败时降级到 BM25。"""
        try:
            q_emb = self._normalize(np.array(embed_fn([question]), dtype=np.float32))
            scores = (q_emb @ self._embeddings.T).flatten()
            # 检查 scores 是否有效
            if np.isnan(scores).all() or np.all(scores == 0):
                logger.warning("Embedding scores all NaN/zero, falling back to BM25")
                return self._retrieve_by_keywords(question, top_k)
            top_idx = np.argsort(scores)[::-1][:top_k]
            return [self.tables[i] for i in top_idx]
        except Exception as exc:
            logger.warning("Embedding retrieval failed (%s), falling back to BM25", exc)
            return self._retrieve_by_keywords(question, top_k)

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

    def _retrieve_by_metrics(self, question: str) -> list[str]:
        """
        Phase 0：指标名称直接匹配。

        将问题与已注册业务指标的 label 和 name 进行关键词匹配。
        命中后从指标定义中提取 measure / period_col / dimensions 所在的表，
        直接返回为高置信度候选表。

        优势：对"复购率""客单价""广告 ROI"等明确指标名的查询精度接近 100%。
        依赖 metrics_registry 传入（构造时注入）。
        """
        if not hasattr(self, '_metrics_registry') or not self._metrics_registry:
            return []

        q_lower = question.lower()
        matched_tables: list[str] = []
        seen: set[str] = set()

        for metric_name, metric in self._metrics_registry.items():
            label = metric.get("label", "")
            # 匹配 metric name 或 label 的任意词
            match_targets = [metric_name.lower(), label.lower()]
            hit = any(
                (t in q_lower or q_lower in t)
                for t in match_targets
                if len(t) >= 2
            )
            # 也尝试 label 中的关键词（"客单价" → "客单" "单价"）
            if not hit:
                for chunk in re.split(r'[（）\s/（）、,，()]', label):
                    chunk = chunk.strip().lower()
                    if len(chunk) >= 2 and chunk in q_lower:
                        hit = True
                        break

            # 滑动 n-gram（长度 ≥ 3）：覆盖复合词，如"首购转化"在"首购转化率"中
            if not hit:
                label_clean = re.sub(r'\s', '', label.lower())
                for n in range(3, min(len(label_clean) + 1, 6)):
                    for i in range(len(label_clean) - n + 1):
                        sub = label_clean[i : i + n]
                        if sub in q_lower:
                            hit = True
                            break
                    if hit:
                        break

            # 步长-2 双字节 bigram 覆盖：label 每隔 2 个字符取一对，全部命中则视为匹配
            # 解决 label 中各词散落在问题里的情况，如"门店营收"→["门店","营收"]均在"各门店近7天营收"中
            if not hit:
                cjk_label = re.sub(r'[^\u4e00-\u9fff]', '', label.lower())
                bigrams = [cjk_label[i : i + 2] for i in range(0, len(cjk_label) - 1, 2)
                           if len(cjk_label[i : i + 2]) == 2]
                if bigrams and all(b in q_lower for b in bigrams):
                    hit = True

            if not hit:
                continue

            # 提取该指标所需的表
            ref_cols: list[str] = []
            if metric.get("measure"):
                ref_cols.append(metric["measure"])
            if metric.get("period_col"):
                ref_cols.append(metric["period_col"])
            for dim in metric.get("dimensions", []):
                if "." in dim:
                    ref_cols.append(dim)
            # 衍生指标：从分子/分母原子指标里取 measure
            for key in ("numerator", "denominator"):
                ref_name = metric.get(key, "")
                if ref_name and ref_name in self._metrics_registry:
                    ref_m = self._metrics_registry[ref_name].get("measure", "")
                    if ref_m:
                        ref_cols.append(ref_m)

            for col_ref in ref_cols:
                if "." in col_ref:
                    table = col_ref.split(".")[0]
                    if table in set(self.tables) and table not in seen:
                        seen.add(table)
                        matched_tables.append(table)

        return matched_tables

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

    def _expand_fks(self, tables: list[str], depth: int = 2) -> list[str]:
        """
        FK 图扩展：BFS 扩展 `depth` 层直接引用的父表。

        depth=2 覆盖典型星型模型的多跳链路：
            [dwd_order_item_detail]
              → 第 1 层：dwd_order_detail, dim_product
              → 第 2 层：dim_user, dim_channel（来自 dwd_order_detail）
                         dim_category, dim_brand（来自 dim_product）

        已在列表中的表不重复添加，扩展的表按层次附加在原列表末尾。
        """
        seen: set[str] = set(tables)
        frontier = list(tables)

        for _ in range(depth):
            next_frontier: list[str] = []
            for table in frontier:
                for ref in self._fks.get(table, set()):
                    if ref not in seen:
                        seen.add(ref)
                        next_frontier.append(ref)
            frontier = next_frontier

        # 保持原始顺序，扩展表追加在后
        return list(tables) + [t for t in seen if t not in set(tables)]

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
        """L2 归一化（用于余弦相似度计算）。处理 NaN 和零向量。"""
        # 先把 NaN/Inf 替换为 0
        x = np.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0)
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
        enum_hints: list[str] = []    # 有枚举值的字段
        desc_hints: list[str] = []    # 仅有描述、无枚举的字段

        for table_name in table_names:
            info = tables_info.get(table_name, {})
            if not isinstance(info, dict):
                lines.append(table_name)
                continue

            # 表描述（内联在表名后）
            table_desc = info.get("description", "")
            cols = info.get("columns", {})
            col_names: list[str] = []
            items = cols.items() if isinstance(cols, dict) else [(c, {}) for c in cols]
            for col_name, meta in items:
                col_names.append(col_name)
                if isinstance(meta, dict):
                    if meta.get("enum"):
                        vals = " | ".join(
                            f"'{v}'" if isinstance(v, str) else str(v)
                            for v in meta["enum"]
                        )
                        hint = f"- {table_name}.{col_name}: {vals}"
                        if meta.get("description"):
                            hint += f"  # {meta['description']}"
                        enum_hints.append(hint)
                    elif meta.get("description"):
                        desc_hints.append(f"- {table_name}.{col_name}: {meta['description']}")

            table_line = f"{table_name} ({', '.join(col_names)})"
            if table_desc:
                table_line += f"  -- {table_desc}"
            lines.append(table_line)

        if enum_hints:
            lines += ["", "字段枚举值："] + enum_hints
        if desc_hints:
            lines += ["", "字段说明："] + desc_hints

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
