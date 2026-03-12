"""
Tests for agent.llm._registry_context() — verifies that both structural and
semantic layers are correctly formatted for LLM consumption.
"""
import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

import agent.llm as llm_mod


# ── helpers ───────────────────────────────────────────────────────────────────

STRUCTURAL = {
    "tables": {
        "orders": {"columns": ["id", "user_id", "status", "total_amount", "created_at"]},
        "users":  {"columns": ["id", "name", "city"]},
    }
}

ATOMIC_METRIC = {
    "order_amount": {
        "metric_class": "atomic",
        "label":        "订单金额",
        "description":  "已完成订单的成交金额",
        "measure":      "orders.total_amount",
        "aggregation":  "sum",
        "qualifiers":   ["orders.status = 'completed'"],
        "period_col":   "orders.created_at",
        "dimensions":   ["users.city", "users.is_vip"],
    }
}

DERIVATIVE_METRIC = {
    "repurchase_rate": {
        "metric_class": "derivative",
        "label":        "复购率",
        "description":  "有重复购买行为的用户占比",
        "numerator":    "repurchase_users",
        "denominator":  "ordered_users",
        "period_col":   "orders.created_at",
        "notes":        "分子分母均不限定 status",
    }
}


def _setup(structural: dict, metrics: dict):
    """Write temp registry files and return (schema_path, metrics_path)."""
    tmp = tempfile.mkdtemp()
    schema_path  = Path(tmp) / "schema.registry.json"
    metrics_path = Path(tmp) / "metrics.registry.yaml"
    schema_path.write_text(json.dumps(structural))
    metrics_path.write_text(yaml.dump(metrics, allow_unicode=True))
    return schema_path, metrics_path


# ── table structure ───────────────────────────────────────────────────────────

def test_context_includes_table_names():
    schema_path, metrics_path = _setup(STRUCTURAL, {})
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "orders" in ctx
    assert "users"  in ctx


def test_context_includes_column_names():
    schema_path, metrics_path = _setup(STRUCTURAL, {})
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "total_amount" in ctx
    assert "city"         in ctx


def test_context_table_section_label():
    schema_path, metrics_path = _setup(STRUCTURAL, {})
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "表结构" in ctx


def test_context_missing_schema_file_shows_fallback():
    _, metrics_path = _setup(STRUCTURAL, {})
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", Path("/nonexistent/schema.json")), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "forge sync" in ctx


# ── atomic metrics ────────────────────────────────────────────────────────────

def test_context_includes_atomic_section_label():
    schema_path, metrics_path = _setup(STRUCTURAL, ATOMIC_METRIC)
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "原子指标" in ctx


def test_context_atomic_name_and_label():
    schema_path, metrics_path = _setup(STRUCTURAL, ATOMIC_METRIC)
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "order_amount" in ctx
    assert "订单金额" in ctx


def test_context_atomic_aggregation_and_measure():
    schema_path, metrics_path = _setup(STRUCTURAL, ATOMIC_METRIC)
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "sum" in ctx
    assert "orders.total_amount" in ctx


def test_context_atomic_qualifiers():
    schema_path, metrics_path = _setup(STRUCTURAL, ATOMIC_METRIC)
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "orders.status = 'completed'" in ctx
    assert "必须过滤" in ctx


def test_context_atomic_period_col():
    schema_path, metrics_path = _setup(STRUCTURAL, ATOMIC_METRIC)
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "orders.created_at" in ctx
    assert "时间字段" in ctx


def test_context_atomic_dimensions():
    schema_path, metrics_path = _setup(STRUCTURAL, ATOMIC_METRIC)
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "users.city"   in ctx
    assert "可用维度" in ctx


def test_context_atomic_description():
    schema_path, metrics_path = _setup(STRUCTURAL, ATOMIC_METRIC)
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "已完成订单的成交金额" in ctx


# ── derivative metrics ────────────────────────────────────────────────────────

def test_context_includes_derivative_section_label():
    schema_path, metrics_path = _setup(STRUCTURAL, DERIVATIVE_METRIC)
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "衍生指标" in ctx


def test_context_derivative_name_and_label():
    schema_path, metrics_path = _setup(STRUCTURAL, DERIVATIVE_METRIC)
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "repurchase_rate" in ctx
    assert "复购率" in ctx


def test_context_derivative_formula_format():
    """Derivative shown as 'numerator / denominator'."""
    schema_path, metrics_path = _setup(STRUCTURAL, DERIVATIVE_METRIC)
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "repurchase_users" in ctx
    assert "ordered_users"    in ctx
    assert "/" in ctx


def test_context_derivative_period_col_note():
    schema_path, metrics_path = _setup(STRUCTURAL, DERIVATIVE_METRIC)
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "统一应用于分子和分母" in ctx


def test_context_derivative_notes_shown():
    schema_path, metrics_path = _setup(STRUCTURAL, DERIVATIVE_METRIC)
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "分子分母均不限定 status" in ctx
    assert "注意" in ctx


# ── mixed registry ─────────────────────────────────────────────────────────────

def test_context_both_atomic_and_derivative_present():
    mixed = {**ATOMIC_METRIC, **DERIVATIVE_METRIC}
    schema_path, metrics_path = _setup(STRUCTURAL, mixed)
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "原子指标" in ctx
    assert "衍生指标" in ctx


def test_context_section_order_tables_before_metrics():
    """表结构 must appear before metrics in the context string."""
    mixed = {**ATOMIC_METRIC, **DERIVATIVE_METRIC}
    schema_path, metrics_path = _setup(STRUCTURAL, mixed)
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert ctx.index("表结构") < ctx.index("原子指标")
    assert ctx.index("原子指标") < ctx.index("衍生指标")


# ── empty / edge cases ────────────────────────────────────────────────────────

def test_context_no_metrics_yaml_no_crash():
    schema_path, _ = _setup(STRUCTURAL, {})
    # point METRICS_PATH at a missing file
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  Path("/nonexistent/metrics.yaml")):
        ctx = llm_mod._registry_context()
    # Should still return table structure without crashing
    assert "orders" in ctx
    assert "原子指标" not in ctx
    assert "衍生指标" not in ctx


def test_context_empty_metrics_registry():
    schema_path, metrics_path = _setup(STRUCTURAL, {})
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "原子指标" not in ctx
    assert "衍生指标" not in ctx


def test_context_atomic_without_qualifiers_no_filter_line():
    metrics = {
        "order_count": {
            "metric_class": "atomic",
            "label":        "订单数",
            "description":  "订单总数",
            "measure":      "orders.id",
            "aggregation":  "count",
        }
    }
    schema_path, metrics_path = _setup(STRUCTURAL, metrics)
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert "必须过滤" not in ctx
    assert "order_count" in ctx


def test_context_returns_string():
    schema_path, metrics_path = _setup(STRUCTURAL, ATOMIC_METRIC)
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        ctx = llm_mod._registry_context()
    assert isinstance(ctx, str)
    assert len(ctx) > 0
