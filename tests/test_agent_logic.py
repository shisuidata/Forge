"""
Tests for agent core logic — approve, cancel, _validate_and_save, process().
LLM calls are mocked; no real API key required.

NOTE: 这些测试依赖已删除的 agent/session.py。
      迁移到新记忆系统（agent/memory）后需要重写。暂时全部 skip。
"""
import pytest
pytestmark = pytest.mark.skip(reason="依赖已删除的 agent/session.py，待用 memory 系统重写")

# 以下 import 被 skip marker 跳过，但 pytest 在 collection 阶段仍会执行 module-level 代码。
# 用延迟 import 避免 ModuleNotFoundError。
import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import agent.agent as agent_mod
    from agent.session import SessionStore
except ModuleNotFoundError:
    agent_mod = None  # type: ignore
    SessionStore = None  # type: ignore


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_store() -> SessionStore:
    """Return a fresh, isolated SessionStore."""
    return SessionStore()


def _patch_store(store: SessionStore):
    """Patch agent_mod.store with a custom store."""
    return patch.object(agent_mod, "store", store)


# ── approve ───────────────────────────────────────────────────────────────────

def test_approve_returns_sql_when_pending():
    store = _make_store()
    with _patch_store(store):
        s = store.get("u1")
        s.pending_sql   = "SELECT 1"
        s.pending_forge = {"scan": "t", "select": ["t.id"]}

        resp = agent_mod.approve("u1")

    assert resp.action == "approved"
    assert resp.sql == "SELECT 1"
    assert s.pending_sql is None
    assert s.pending_forge is None


def test_approve_no_pending_returns_error():
    store = _make_store()
    with _patch_store(store):
        resp = agent_mod.approve("u_nobody")

    assert resp.action == "error"
    assert resp.sql is None


# ── cancel ────────────────────────────────────────────────────────────────────

def test_cancel_clears_pending():
    store = _make_store()
    with _patch_store(store):
        s = store.get("u2")
        s.pending_sql   = "SELECT 99"
        s.pending_forge = {}

        resp = agent_mod.cancel("u2")

    assert resp.action == "cancelled"
    assert s.pending_sql is None
    assert s.pending_forge is None


def test_cancel_with_no_pending_is_safe():
    store = _make_store()
    with _patch_store(store):
        resp = agent_mod.cancel("u_empty")

    assert resp.action == "cancelled"


# ── _validate_and_save ────────────────────────────────────────────────────────

STRUCTURAL = {
    "tables": {
        "orders": {"columns": ["id", "user_id", "status", "total_amount", "created_at"]},
        "users":  {"columns": ["id", "name", "city", "is_vip"]},
    }
}

VALID_ATOMIC = {
    "name":         "order_amount",
    "metric_class": "atomic",
    "label":        "订单金额",
    "description":  "完成订单的成交金额",
    "measure":      "orders.total_amount",
    "aggregation":  "sum",
    "qualifiers":   ["orders.status = 'completed'"],
    "period_col":   "orders.created_at",
    "dimensions":   ["users.city"],
}


def _tmp_registry(structural: dict, metrics: dict):
    """Write temp files and patch cfg paths. Returns (tmp_dir, patcher_ctx)."""
    tmp = tempfile.mkdtemp()
    schema_path  = Path(tmp) / "schema.registry.json"
    metrics_path = Path(tmp) / "metrics.registry.yaml"
    schema_path.write_text(json.dumps(structural))
    metrics_path.write_text(yaml.dump(metrics, allow_unicode=True))
    return schema_path, metrics_path


def test_validate_and_save_valid_metric_writes_yaml():
    schema_path, metrics_path = _tmp_registry(STRUCTURAL, {})
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        errors, warnings = agent_mod._validate_and_save(dict(VALID_ATOMIC), "order_amount")

    assert errors == []
    saved = yaml.safe_load(metrics_path.read_text())
    assert "order_amount" in saved
    assert saved["order_amount"]["aggregation"] == "sum"
    assert "name" not in saved["order_amount"]   # 'name' key removed from stored entry
    assert "updated_at" in saved["order_amount"]  # timestamp injected


def test_validate_and_save_invalid_metric_returns_errors():
    schema_path, metrics_path = _tmp_registry(STRUCTURAL, {})
    import config
    bad = dict(VALID_ATOMIC)
    bad["measure"] = "orders.ghost_col"   # non-existent column
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        errors, _ = agent_mod._validate_and_save(bad, "bad_metric")

    assert errors
    # file should NOT have been written
    saved = yaml.safe_load(metrics_path.read_text()) or {}
    assert "bad_metric" not in saved


def test_validate_and_save_overwrites_existing_metric():
    schema_path, metrics_path = _tmp_registry(STRUCTURAL, {
        "order_amount": {"metric_class": "atomic", "label": "old label",
                         "measure": "orders.total_amount", "aggregation": "sum"}
    })
    import config
    updated = dict(VALID_ATOMIC)
    updated["label"] = "新订单金额"
    with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        errors, _ = agent_mod._validate_and_save(updated, "order_amount")

    assert errors == []
    saved = yaml.safe_load(metrics_path.read_text())
    assert saved["order_amount"]["label"] == "新订单金额"


def test_validate_and_save_missing_registry_skips_col_check():
    """If schema file is missing, column checks are skipped and save proceeds."""
    _, metrics_path = _tmp_registry(STRUCTURAL, {})
    import config
    with patch.object(config.cfg, "REGISTRY_PATH", Path("/nonexistent/path.json")), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path):
        errors, _ = agent_mod._validate_and_save(dict(VALID_ATOMIC), "order_amount")

    assert errors == []


# ── process() — query flow ────────────────────────────────────────────────────

SIMPLE_FORGE = {
    "scan":   "orders",
    "select": ["orders.id"],
}


def test_process_query_sets_pending_sql():
    store = _make_store()
    with _patch_store(store), \
         patch.object(agent_mod.llm, "call",
                      return_value={"tool": "generate_forge_query", "input": SIMPLE_FORGE}):
        resp = agent_mod.process("u_q", "查询所有订单 ID")

    assert resp.action == "sql_review"
    assert resp.sql is not None
    assert "SELECT" in resp.sql
    assert store.get("u_q").pending_sql is not None


def test_process_plain_text_reply():
    store = _make_store()
    with _patch_store(store), \
         patch.object(agent_mod.llm, "call",
                      return_value={"tool": None, "text": "你好，请问需要查询什么？"}):
        resp = agent_mod.process("u_t", "你好")

    assert resp.action == "message"
    assert "你好" in resp.text


def test_process_compile_error_returns_error_after_retries():
    """All retries fail → action=error."""
    store = _make_store()
    bad_forge = {"scan": "orders"}  # missing 'select' → compile error

    with _patch_store(store), \
         patch.object(agent_mod.llm, "call",
                      return_value={"tool": "generate_forge_query", "input": bad_forge}):
        resp = agent_mod.process("u_err", "查询订单")

    assert resp.action == "error"
    assert store.get("u_err").pending_sql is None


def test_process_define_metric_valid():
    store  = _make_store()
    schema_path, metrics_path = _tmp_registry(STRUCTURAL, {})
    import config
    metric_input = dict(VALID_ATOMIC)

    with _patch_store(store), \
         patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path), \
         patch.object(agent_mod.llm, "call",
                      return_value={"tool": "define_metric", "input": metric_input}):
        resp = agent_mod.process("u_def", "定义订单金额指标")

    assert resp.action == "metric_saved"
    assert "订单金额" in resp.text


def test_process_define_metric_invalid():
    store  = _make_store()
    schema_path, metrics_path = _tmp_registry(STRUCTURAL, {})
    import config
    bad_metric = dict(VALID_ATOMIC)
    bad_metric["measure"] = "orders.ghost_col"

    with _patch_store(store), \
         patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
         patch.object(config.cfg, "METRICS_PATH",  metrics_path), \
         patch.object(agent_mod.llm, "call",
                      return_value={"tool": "define_metric", "input": bad_metric}):
        resp = agent_mod.process("u_inv", "定义错误指标")

    assert resp.action == "error"
    assert "未保存" in resp.text


# ── process() — retry flow ────────────────────────────────────────────────────

def test_process_retries_on_compile_error_then_succeeds():
    """First call returns bad JSON; second call returns valid JSON — should succeed."""
    store = _make_store()
    call_count = {"n": 0}

    def _side_effect(history):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return {"tool": "generate_forge_query", "input": {"scan": "orders"}}  # bad
        return {"tool": "generate_forge_query", "input": SIMPLE_FORGE}  # good

    with _patch_store(store), \
         patch.object(agent_mod.llm, "call", side_effect=_side_effect):
        resp = agent_mod.process("u_retry", "查询订单")

    assert resp.action == "sql_review"
    assert resp.sql is not None
    assert call_count["n"] == 2
