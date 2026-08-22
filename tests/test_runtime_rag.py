from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

from agent import llm
from config import cfg
from forge.retriever import SchemaRetriever

DATASET = Path(__file__).parent / "datasets" / "large"


def _retriever() -> SchemaRetriever:
    registry = json.loads((DATASET / "schema.registry.json").read_text())
    metrics = yaml.safe_load((DATASET / "metrics.registry.yaml").read_text()) or {}
    return SchemaRetriever(registry, metrics_registry=metrics)


def test_bm25_rag_covers_reference_tables_for_all_40_cases():
    retriever = _retriever()
    cases = json.loads((DATASET / "cases.json").read_text())
    physical_tables = set(retriever.tables)
    counts: list[int] = []

    for case in cases:
        selected = retriever.retrieve(case["question"], None, top_k=5)
        required = {
            table for table in physical_tables
            if re.search(rf"\b{re.escape(table)}\b", case["reference_sql"])
        }
        assert required <= set(selected), (case["id"], sorted(required - set(selected)))
        assert selected == retriever.retrieve(case["question"], None, top_k=5)
        counts.append(len(selected))

    assert max(counts) < len(physical_tables) / 2


def test_runtime_context_injects_structure_semantics_relationships_and_conventions(monkeypatch):
    retriever = _retriever()
    selected = retriever.retrieve(
        "统计各品牌已完成订单的总销售额和订单数",
        None,
        top_k=5,
    )
    monkeypatch.setattr(llm, "_get_retriever", lambda: (retriever, None))
    monkeypatch.setattr(cfg, "REGISTRY_PATH", DATASET / "schema.registry.json")
    monkeypatch.setattr(cfg, "METRICS_PATH", DATASET / "metrics.registry.yaml")
    monkeypatch.setattr(cfg, "DISAMBIGUATIONS_PATH", DATASET / "disambiguations.registry.yaml")
    monkeypatch.setattr(cfg, "CONVENTIONS_PATH", DATASET / "field_conventions.registry.yaml")

    context = llm._registry_context(
        "统计各品牌已完成订单的总销售额和订单数",
        selected_tables=selected,
    )

    assert "订单明细事实表" in context
    assert "订单原始总金额" in context
    assert "dwd_order_item_detail.product_id -> dim_product.product_id" in context
    assert "原子指标（直接可查）" in context
    assert "订单状态过滤原则" in context
    assert "品牌钻石会员均价审核字段" in context
