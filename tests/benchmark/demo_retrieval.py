#!/usr/bin/env python3
"""
Schema 向量检索演示脚本。

用法：
    # 使用向量检索（调用 MiniMax embedding API）
    python tests/benchmark/demo_retrieval.py

    # 强制降级到关键词检索（不调用 API）
    python tests/benchmark/demo_retrieval.py --keywords-only

    # 重新构建向量索引
    python tests/benchmark/demo_retrieval.py --rebuild
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))

from forge.retriever import SchemaRetriever, make_embed_fn, make_query_embed_fn

# ── 配置 ──────────────────────────────────────────────────────────────────────

REGISTRY_PATH    = ROOT / "schema.registry.json"
EMBED_CACHE_PATH = ROOT / ".forge" / "schema_embeddings.pkl"

# MiniMax OpenAI-compatible endpoint（用于 embedding）
EMBED_BASE_URL = os.environ.get("EMBED_BASE_URL", "https://api.minimaxi.com/v1")
EMBED_API_KEY  = os.environ.get("EMBED_API_KEY",
                 os.environ.get("MINIMAX_API_KEY", ""))
EMBED_MODEL    = os.environ.get("EMBED_MODEL", "embo-01")

# ── 测试问题集（覆盖不同表组合） ──────────────────────────────────────────────

TEST_QUESTIONS = [
    # 期望命中: users
    ("查询北京的 VIP 用户名单",
     {"users"}),

    # 期望命中: orders
    ("统计已完成订单的总金额",
     {"orders"}),

    # 期望命中: products
    ("各商品品类的成本价均值",
     {"products"}),

    # 期望命中: order_items + products
    ("每个品类的销售数量汇总",
     {"order_items", "products"}),

    # 期望命中: users + orders
    ("VIP 用户的复购率是多少",
     {"users", "orders"}),

    # 期望命中: users + orders + order_items + products（四表）
    ("广州用户购买的电子产品订单明细",
     {"users", "orders", "order_items", "products"}),

    # 期望命中: orders（时间相关）
    ("2024 年每月的订单量趋势",
     {"orders"}),

    # 期望命中: orders + order_items
    ("平均订单金额与商品单价的关系",
     {"orders", "order_items"}),
]


def build_or_load_index(retriever: SchemaRetriever, embed_fn, rebuild: bool) -> str:
    """构建或加载向量索引，返回状态描述。"""
    if not rebuild and retriever.load_index():
        return "✅ 从缓存加载向量索引"

    if embed_fn is None:
        return "⚠️  无 embedding 函数，跳过索引构建（将使用关键词检索）"

    print("⏳ 正在构建向量索引（调用 embedding API）...")
    retriever.build_index(embed_fn)
    return f"✅ 向量索引已构建并缓存至 {retriever.cache_path}"


def eval_retrieval(
    retriever: SchemaRetriever,
    embed_fn,
    top_k: int = 3,
    keywords_only: bool = False,
) -> None:
    """运行测试问题集，输出检索结果并计算召回率。"""

    print(f"\n{'='*70}")
    mode_label = "关键词 BM25-lite" if keywords_only else "向量（cosine similarity）"
    print(f"  检索模式: {mode_label}   top_k={top_k}")
    print(f"{'='*70}\n")

    total_expected = 0
    total_recalled = 0

    for question, expected_tables in TEST_QUESTIONS:
        fn = None if keywords_only else embed_fn
        retrieved = retriever.retrieve(question, fn, top_k=top_k)
        retrieved_set = set(retrieved)

        hit = expected_tables & retrieved_set
        miss = expected_tables - retrieved_set
        recall = len(hit) / len(expected_tables) if expected_tables else 1.0

        total_expected += len(expected_tables)
        total_recalled += len(hit)

        status = "✅" if recall == 1.0 else ("⚠️ " if recall > 0 else "❌")
        print(f"{status} 「{question}」")
        print(f"   期望: {sorted(expected_tables)}")
        print(f"   命中: {sorted(retrieved_set)}   召回 {recall*100:.0f}%"
              + (f"  缺失: {sorted(miss)}" if miss else ""))
        print()

    overall_recall = total_recalled / total_expected if total_expected else 0
    print(f"{'─'*70}")
    print(f"  整体召回率: {total_recalled}/{total_expected} = {overall_recall*100:.1f}%")
    print()


def show_schema_context_demo(retriever: SchemaRetriever, embed_fn) -> None:
    """演示：对比全量 schema 与检索后精简 schema 的 token 估算。"""
    full_schema = retriever.get_schema_ddl(retriever.tables)
    full_tokens = len(full_schema) // 3  # 粗估：3 字符 ≈ 1 token

    print(f"\n{'='*70}")
    print("  Schema 注入对比（全量 vs 检索精简）")
    print(f"{'='*70}\n")

    sample_questions = [
        "统计各城市 VIP 用户数量",
        "计算每个商品品类的销售额",
    ]

    for q in sample_questions:
        retrieved = retriever.retrieve(q, embed_fn, top_k=2)
        trimmed_schema = retriever.get_schema_ddl(retrieved)
        trimmed_tokens = len(trimmed_schema) // 3

        reduction = (1 - trimmed_tokens / full_tokens) * 100
        print(f"问题: 「{q}」")
        print(f"  全量 schema: ~{full_tokens} tokens  ({len(retriever.tables)} 张表)")
        print(f"  检索精简:    ~{trimmed_tokens} tokens  ({len(retrieved)} 张表: {', '.join(retrieved)})")
        print(f"  减少: {reduction:.0f}%")
        print()
        print("  ── 精简后的 schema ──")
        for line in trimmed_schema.splitlines():
            print(f"  {line}")
        print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Schema 向量检索演示")
    parser.add_argument("--keywords-only", action="store_true",
                        help="强制使用关键词检索，不调用 embedding API")
    parser.add_argument("--rebuild", action="store_true",
                        help="强制重新构建向量索引")
    parser.add_argument("--top-k", type=int, default=3,
                        help="检索表数量（默认 3）")
    args = parser.parse_args()

    # 加载 registry
    registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    retriever = SchemaRetriever(registry, cache_path=EMBED_CACHE_PATH)

    # 初始化 embedding 函数
    embed_fn = None
    query_embed_fn = None
    if not args.keywords_only:
        if not EMBED_API_KEY:
            print("⚠️  未配置 EMBED_API_KEY / MINIMAX_API_KEY，降级到关键词检索")
        else:
            try:
                # 文档嵌入（type=db）用于构建索引
                db_embed_fn = make_embed_fn(EMBED_API_KEY, EMBED_BASE_URL, EMBED_MODEL, "db")
                # 查询嵌入（type=query）用于检索
                query_embed_fn = make_query_embed_fn(EMBED_API_KEY, EMBED_BASE_URL, EMBED_MODEL)
                status = build_or_load_index(retriever, db_embed_fn, args.rebuild)
                print(status)
                embed_fn = query_embed_fn  # 检索时用 query 类型
            except Exception as e:
                print(f"⚠️  向量索引构建失败（{e}），降级到关键词检索")
                embed_fn = None

    if embed_fn is None:
        args.keywords_only = True

    # 对比两种模式（若 embed 可用）
    if not args.keywords_only:
        eval_retrieval(retriever, embed_fn, top_k=args.top_k, keywords_only=False)

    eval_retrieval(retriever, None, top_k=args.top_k, keywords_only=True)

    # Schema 精简演示
    show_schema_context_demo(retriever, embed_fn if not args.keywords_only else None)


if __name__ == "__main__":
    main()
