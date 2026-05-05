from __future__ import annotations

from types import SimpleNamespace

import pytest


class FakeMemory:
    def __init__(self):
        self.state = {}
        self.records = []
        self.extractor = SimpleNamespace(
            on_approve=lambda *args, **kwargs: None,
            update_user_profile=lambda *args, **kwargs: None,
            on_cancel=lambda *args, **kwargs: None,
        )
        self.ems = SimpleNamespace(
            get_recent_messages=lambda *args, **kwargs: [
                {"content": "查询订单", "role": "user"}
            ]
        )

    def get_state(self, user_id, key):
        return self.state.get((user_id, key))

    def set_state(self, user_id, key, value):
        self.state[(user_id, key)] = value

    def clear_state(self, user_id, key):
        self.state.pop((user_id, key), None)

    def record(self, user_id, role, content, **kwargs):
        self.records.append({"user_id": user_id, "role": role, "content": content, **kwargs})

    def build(self, mode, user_id, text):
        return [{"role": "user", "content": text}], "", []


@pytest.fixture
def isolated_agent(monkeypatch):
    import agent.agent as agent_mod
    import agent.tenant as tenant_mod

    fake_memory = FakeMemory()
    monkeypatch.setattr(agent_mod, "memory", fake_memory)
    monkeypatch.setattr(
        tenant_mod.tenants,
        "get_allowed_tables_for_user",
        lambda user_id: None,
    )
    monkeypatch.setattr(agent_mod.cfg, "FEEDBACK_ENABLED", False)
    monkeypatch.setattr(agent_mod.cfg, "SQL_DIALECT", "sqlite")
    monkeypatch.setattr(agent_mod.cfg, "DATABASE_URL", "")
    return agent_mod, fake_memory


def test_process_retries_once_when_convention_lint_fails(isolated_agent, monkeypatch):
    agent_mod, fake_memory = isolated_agent
    calls = []

    bad = {
        "scan": "dwd_order_detail",
        "select": ["dwd_order_detail.user_id"],
    }
    fixed = {
        "scan": "dwd_order_detail",
        "select": ["dwd_order_detail.user_id"],
        "filter": [
            {"col": "dwd_order_detail.order_status", "op": "eq", "val": "已完成"}
        ],
    }

    def fake_call(*args, **kwargs):
        calls.append((args, kwargs))
        return {
            "tool": "generate_forge_query",
            "input": bad if len(calls) == 1 else fixed,
        }

    monkeypatch.setattr(agent_mod.llm, "call", fake_call)

    resp = agent_mod.process("u1", "查询复购用户")

    assert resp.action == "sql_review"
    assert resp.retry_count == 1
    assert len(calls) == 2
    assert "order_status" in resp.sql
    assert fake_memory.get_state("u1", "pending_sql") == resp.sql


def test_process_uses_configured_postgresql_dialect(isolated_agent, monkeypatch):
    agent_mod, _ = isolated_agent
    monkeypatch.setattr(agent_mod.cfg, "SQL_DIALECT", "auto")
    monkeypatch.setattr(agent_mod.cfg, "DATABASE_URL", "postgresql://user:pass@localhost/db")
    monkeypatch.setattr(
        agent_mod.llm,
        "call",
        lambda *args, **kwargs: {
            "tool": "generate_forge_query",
            "input": {
                "scan": "orders",
                "select": ["orders.id"],
                "filter": [
                    {"col": "orders.created_at", "op": "gte", "val": {"$preset": "today"}}
                ],
            },
        },
    )

    resp = agent_mod.process("u2", "查询今天订单")

    assert resp.action == "sql_review"
    assert "CURRENT_DATE" in resp.sql
    assert "DATE('now')" not in resp.sql


def test_process_returns_error_when_llm_call_fails(isolated_agent, monkeypatch):
    agent_mod, _ = isolated_agent

    def fail_call(*args, **kwargs):
        raise RuntimeError("upstream unavailable")

    monkeypatch.setattr(agent_mod.llm, "call", fail_call)

    resp = agent_mod.process("u3", "查询订单")

    assert resp.action == "error"
    assert "LLM 调用失败" in resp.text
    assert "upstream unavailable" in resp.text


def test_approve_clears_pending_sql_and_returns_sql(isolated_agent, monkeypatch):
    agent_mod, fake_memory = isolated_agent
    monkeypatch.setattr(agent_mod.cache, "add_pending", lambda **kwargs: None)
    fake_memory.set_state("u4", "pending_sql", "SELECT 1")
    fake_memory.set_state("u4", "pending_forge", {"scan": "orders", "select": ["orders.id"]})

    resp = agent_mod.approve("u4")

    assert resp.action == "approved"
    assert resp.sql == "SELECT 1"
    assert fake_memory.get_state("u4", "pending_sql") is None
    assert fake_memory.get_state("u4", "pending_forge") is None


def test_cancel_clears_pending_sql(isolated_agent):
    agent_mod, fake_memory = isolated_agent
    fake_memory.set_state("u5", "pending_sql", "SELECT 1")
    fake_memory.set_state("u5", "pending_forge", {"scan": "orders", "select": ["orders.id"]})

    resp = agent_mod.cancel("u5")

    assert resp.action == "cancelled"
    assert fake_memory.get_state("u5", "pending_sql") is None
    assert fake_memory.get_state("u5", "pending_forge") is None
