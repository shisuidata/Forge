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
def isolated_agent(monkeypatch, tmp_path):
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
    monkeypatch.setattr(
        agent_mod.llm,
        "get_model_config",
        lambda stage="query_generation": SimpleNamespace(
            provider="openai",
            model="fixture-model",
            api_key="fixture-key",
            base_url="https://provider.example/v1",
            tool_choice="required",
            timeout_seconds=120.0,
            revision="fixture-revision",
            source="test",
        ),
    )
    registry_path = tmp_path / "schema.registry.json"
    registry_path.write_text(
        __import__("json").dumps({
            "tables": {
                "orders": {"columns": {"id": {}, "created_at": {}}},
                "dwd_order_detail": {"columns": {
                    "order_id": {}, "user_id": {}, "city_id": {},
                    "total_amount": {}, "order_status": {}, "order_dt": {},
                }},
                "dim_city": {"columns": {"city_id": {}, "city_name": {}}},
                "dim_user": {"columns": {
                    "user_id": {}, "region_id": {}, "register_time": {},
                }},
                "dim_region": {"columns": {
                    "region_id": {}, "region_name": {}, "level": {},
                }},
            },
            "relationships": [
                {
                    "id": "order_user",
                    "from": "dwd_order_detail.user_id",
                    "to": "dim_user.user_id",
                    "cardinality": "many_to_one",
                    "status": "confirmed",
                    "source": "fixture",
                },
                {
                    "id": "user_region",
                    "from": "dim_user.region_id",
                    "to": "dim_region.region_id",
                    "cardinality": "many_to_one",
                    "status": "confirmed",
                    "source": "fixture",
                },
            ],
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(agent_mod.cfg, "REGISTRY_PATH", registry_path)
    return agent_mod, fake_memory


def test_prepare_query_returns_bounded_timeout_without_retrying_provider(
    isolated_agent, monkeypatch
):
    agent_mod, _ = isolated_agent
    calls = []

    def timeout_call(*args, **kwargs):
        calls.append(kwargs)
        raise agent_mod.llm.LLMRequestTimeoutError("provider detail")

    monkeypatch.setattr(agent_mod.llm, "call", timeout_call)

    result = agent_mod.prepare_query("u-timeout", "查询订单")

    assert result["status"] == "timed_out"
    assert result["error"] == "查询准备超时，请稍后重试或缩小问题范围。"
    assert result["retry_count"] == 0
    assert len(calls) == 1
    assert calls[0]["timeout_seconds"] <= agent_mod.cfg.QUERY_PREPARE_TIMEOUT_SECONDS


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


def test_process_retries_unbound_table_and_self_join_before_review(isolated_agent, monkeypatch):
    agent_mod, fake_memory = isolated_agent
    calls = []
    invalid = {
        "scan": "dwd_order_detail",
        "joins": [{
            "type": "inner",
            "table": "dwd_order_detail",
            "on": {
                "left": "dwd_order_detail.order_id",
                "right": "dwd_order_detail.order_id",
            },
        }],
        "select": ["dim_city.city_name", "dwd_order_detail.total_amount"],
    }
    corrected = {
        "scan": "dwd_order_detail",
        "joins": [
            {
                "type": "inner",
                "table": "dim_user",
                "on": {
                    "left": "dwd_order_detail.user_id",
                    "right": "dim_user.user_id",
                },
            },
            {
                "type": "inner",
                "table": "dim_region",
                "on": {
                    "left": "dim_user.region_id",
                    "right": "dim_region.region_id",
                },
            },
        ],
        "filter": [{"col": "dim_region.level", "op": "eq", "val": "city"}],
        "group": ["dim_region.region_name"],
        "agg": [{"fn": "sum", "col": "dwd_order_detail.total_amount", "as": "order_total"}],
        "select": ["dim_region.region_name", "order_total"],
    }

    def fake_call(*args, **kwargs):
        calls.append(kwargs)
        return {
            "tool": "generate_forge_query",
            "input": invalid if len(calls) == 1 else corrected,
        }

    monkeypatch.setattr(agent_mod.llm, "call", fake_call)

    resp = agent_mod.process("u-integrity", "各城市的订单总额是多少？")

    assert resp.action == "sql_review"
    assert resp.retry_count == 1
    assert len(calls) == 2
    assert {call["config_snapshot"].revision for call in calls} == {"fixture-revision"}
    assert "INNER JOIN dim_region" in resp.sql
    assert "SUM(dwd_order_detail.total_amount)" in resp.sql
    assert fake_memory.get_state("u-integrity", "pending_sql") == resp.sql


def test_process_lints_every_retry_before_sql_review(isolated_agent, monkeypatch):
    agent_mod, _ = isolated_agent
    calls = []
    missing_status = {
        "scan": "dwd_order_detail",
        "select": ["dwd_order_detail.user_id"],
    }
    wrong_direction = {
        "scan": "dwd_order_detail",
        "filter": [
            {"col": "dwd_order_detail.order_status", "op": "eq", "val": "已完成"}
        ],
        "window": [
            {
                "fn": "lag",
                "col": "dwd_order_detail.order_dt",
                "partition": ["dwd_order_detail.user_id"],
                "order": [{"col": "dwd_order_detail.order_dt", "dir": "desc"}],
                "as": "prev_order_dt",
            }
        ],
        "select": ["dwd_order_detail.user_id", "prev_order_dt"],
    }
    fixed = {
        **wrong_direction,
        "window": [
            {
                **wrong_direction["window"][0],
                "order": [{"col": "dwd_order_detail.order_dt", "dir": "asc"}],
            }
        ],
    }

    def fake_call(*args, **kwargs):
        outputs = [missing_status, wrong_direction, fixed]
        result = outputs[len(calls)]
        calls.append(result)
        return {"tool": "generate_forge_query", "input": result}

    monkeypatch.setattr(agent_mod.llm, "call", fake_call)

    resp = agent_mod.process("u-retry", "每个用户相邻两次下单之间的时间间隔")

    assert resp.action == "sql_review"
    assert resp.retry_count == 2
    assert len(calls) == 3
    assert "ASC" in resp.sql


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


@pytest.mark.parametrize("greeting", ["hello", "Hello!", "你好", "您好。", "在吗？"])
def test_prepare_query_greeting_requires_input_without_calling_model(
    isolated_agent, monkeypatch, greeting
):
    agent_mod, fake_memory = isolated_agent

    def unexpected_model_call(*args, **kwargs):
        raise AssertionError("pure greeting must not call the model")

    monkeypatch.setattr(agent_mod.llm, "call", unexpected_model_call)

    result = agent_mod.prepare_query("external-agent", greeting)

    assert result["status"] == "needs_clarification"
    assert result["sql"] is None
    assert result["forge_json"] is None
    assert result["can_execute"] is False
    assert "指标" in result["text"]
    assert fake_memory.get_state("external-agent", "pending_sql") is None


def test_prepare_query_reports_missing_llm_without_sql(isolated_agent, monkeypatch):
    from agent.model_config import LLMNotConfiguredError

    agent_mod, _ = isolated_agent
    monkeypatch.setattr(
        agent_mod.llm,
        "call",
        lambda *args, **kwargs: (_ for _ in ()).throw(LLMNotConfiguredError()),
    )

    result = agent_mod.prepare_query("external-agent", "查询订单")

    assert result["status"] == "error"
    assert result["sql"] is None
    assert result["forge_json"] is None
    assert result["error"] == "尚未配置 LLM，请管理员先在模型设置中完成配置。"


def test_prepare_query_reports_bounded_llm_configuration_error(isolated_agent, monkeypatch):
    from agent.model_config import LLMConfigurationError

    agent_mod, _ = isolated_agent
    monkeypatch.setattr(
        agent_mod.llm,
        "call",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            LLMConfigurationError("secret provider response")
        ),
    )

    result = agent_mod.prepare_query("external-agent", "查询订单")

    assert result["status"] == "error"
    assert "LLM 配置错误" in result["error"]
    assert "secret provider response" not in result["error"]


def test_prepare_query_fails_fast_with_actionable_quota_message(isolated_agent, monkeypatch):
    agent_mod, _ = isolated_agent
    calls = 0

    def call(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise agent_mod.llm.LLMQuotaExceededError("provider account detail")

    monkeypatch.setattr(agent_mod.llm, "call", call)
    result = agent_mod.prepare_query("external-agent", "查询订单 ID")

    assert result["status"] == "error"
    assert result["error"] == "模型服务额度已用完，请在额度恢复后重新发起。"
    assert result["retry_count"] == 0
    assert calls == 1
    assert "provider account detail" not in result["error"]


def test_prepare_query_retries_bounded_tool_contract_violations(isolated_agent, monkeypatch):
    agent_mod, _ = isolated_agent
    calls = 0

    def call(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise agent_mod.llm.LLMCompatibilityError("invalid tool payload")
        return {
            "tool": "generate_forge_query",
            "input": {"scan": "orders", "select": ["orders.id"]},
        }

    monkeypatch.setattr(agent_mod.llm, "call", call)

    result = agent_mod.prepare_query("external-agent", "查询订单 ID")

    assert result["status"] == "needs_review"
    assert result["retry_count"] == 2
    assert calls == 3


def test_prepare_query_allows_one_final_recovery_within_retry_budget(isolated_agent, monkeypatch):
    agent_mod, _ = isolated_agent
    calls = 0

    def call(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls < 4:
            raise agent_mod.llm.LLMCompatibilityError("invalid tool payload")
        return {
            "tool": "generate_forge_query",
            "input": {"scan": "orders", "select": ["orders.id"]},
        }

    monkeypatch.setattr(agent_mod.llm, "call", call)

    result = agent_mod.prepare_query("external-agent", "查询订单 ID")

    assert result["status"] == "needs_review"
    assert result["retry_count"] == 3
    assert calls == 4


def test_prepare_query_keeps_short_data_question_valid(isolated_agent, monkeypatch):
    agent_mod, _ = isolated_agent
    monkeypatch.setattr(
        agent_mod.llm,
        "call",
        lambda *args, **kwargs: {
            "tool": "generate_forge_query",
            "input": {"scan": "dim_user", "agg": [{"fn": "count", "col": "dim_user.user_id", "as": "user_count"}], "select": ["user_count"]},
        },
    )

    result = agent_mod.prepare_query("external-agent", "用户数")

    assert result["status"] == "needs_review"
    assert result["sql"] is not None


def test_prepare_query_does_not_create_pending_execution_state(isolated_agent, monkeypatch):
    agent_mod, fake_memory = isolated_agent
    monkeypatch.setattr(
        fake_memory,
        "build",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("prepare_query must not read legacy conversation memory")
        ),
    )
    monkeypatch.setattr(
        agent_mod.llm,
        "call",
        lambda *args, **kwargs: {
            "tool": "generate_forge_query",
            "input": {
                "scan": "orders",
                "select": ["orders.id"],
            },
        },
    )

    result = agent_mod.prepare_query("external-agent", "查询订单 ID")

    assert result["status"] == "needs_review"
    assert result["sql"] == "SELECT orders.id\nFROM orders"
    assert result["review_required"] is True
    assert result["can_execute"] is False
    assert fake_memory.get_state("external-agent", "pending_sql") is None
    assert fake_memory.get_state("external-agent", "pending_forge") is None


def test_prepare_query_reuses_retry_and_dialect_logic(isolated_agent, monkeypatch):
    agent_mod, fake_memory = isolated_agent
    calls = []
    missing_status = {
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
            "input": missing_status if len(calls) == 1 else fixed,
        }

    monkeypatch.setattr(agent_mod.llm, "call", fake_call)

    result = agent_mod.prepare_query("u-prepare", "查询复购用户", dialect="postgresql")

    assert result["status"] == "needs_review"
    assert result["dialect"] == "postgresql"
    assert result["retry_count"] == 1
    assert len(calls) == 2
    assert "order_status" in result["sql"]
    assert fake_memory.get_state("u-prepare", "pending_sql") is None


def test_prepare_query_retry_includes_all_failed_assurance_diagnostics(isolated_agent, monkeypatch):
    from forge.assurance import GateResult, QueryAssuranceError, QueryAssuranceReport

    agent_mod, _ = isolated_agent
    calls = []

    def call(messages, **kwargs):
        calls.append(messages)
        return {
            "tool": "generate_forge_query",
            "input": {"scan": "orders", "select": ["orders.id"]},
        }

    assurance_calls = 0

    def assure(*args, **kwargs):
        nonlocal assurance_calls
        assurance_calls += 1
        if assurance_calls == 1:
            raise QueryAssuranceError(QueryAssuranceReport(
                status="failed",
                assurance_revision="test",
                policy_revision="test",
                registry_revision="test",
                model_revision="test",
                gates=(GateResult(
                    gate="convention_policy",
                    status="failed",
                    revision="test",
                    diagnostics=("first contract", "second contract"),
                ),),
            ))
        return type("Report", (), {
            "sql": "SELECT orders.id FROM orders",
            "to_dict": lambda self: {"status": "passed"},
        })()

    monkeypatch.setattr(agent_mod.llm, "call", call)
    monkeypatch.setattr(agent_mod, "assure_query", assure)

    result = agent_mod.prepare_query("u-all-diagnostics", "查询订单 ID")

    assert result["status"] == "needs_review"
    retry_text = calls[1][-1]["content"]
    assert "first contract" in retry_text
    assert "second contract" in retry_text
    assert "同时修复" in retry_text


def test_prepare_query_reports_llm_error_without_secret_leak(isolated_agent, monkeypatch):
    agent_mod, _ = isolated_agent

    def fail_call(*args, **kwargs):
        raise RuntimeError("upstream unavailable")

    monkeypatch.setattr(agent_mod.llm, "call", fail_call)

    result = agent_mod.prepare_query("u-error", "查询订单")

    assert result["status"] == "error"
    assert "LLM 调用失败" in result["error"]
    assert "upstream unavailable" not in result["error"]


def test_prepare_query_rejects_unknown_dialect(isolated_agent):
    agent_mod, _ = isolated_agent

    result = agent_mod.prepare_query("u-dialect", "查询订单", dialect="oracle")

    assert result["status"] == "error"
    assert "dialect must be one of" in result["error"]


def test_process_returns_error_when_llm_call_fails(isolated_agent, monkeypatch):
    agent_mod, _ = isolated_agent

    def fail_call(*args, **kwargs):
        raise RuntimeError("upstream unavailable")

    monkeypatch.setattr(agent_mod.llm, "call", fail_call)

    resp = agent_mod.process("u3", "查询订单")

    assert resp.action == "error"
    assert "LLM 调用失败" in resp.text
    assert "upstream unavailable" not in resp.text


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
