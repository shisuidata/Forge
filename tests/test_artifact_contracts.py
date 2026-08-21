"""Contract tests for Pi ↔ Forge task and artifact boundaries."""
from __future__ import annotations

from copy import deepcopy

import pytest
from jsonschema import ValidationError
from jsonschema.validators import validator_for

from agent.contracts import contract_names, load_contract, validate_contract

NOW = "2026-08-21T08:00:00Z"
TASK_ID = "tr_demo_001"
SQL_HASH = "sha256:" + "a" * 64


def _envelope(artifact_type: str, producer: str, payload: dict) -> dict:
    return {
        "artifact_id": f"ar_{artifact_type}_001",
        "artifact_type": artifact_type,
        "schema_version": 1,
        "task_run_id": TASK_ID,
        "producer": producer,
        "created_at": NOW,
        "payload": payload,
    }


@pytest.fixture
def valid_instances() -> dict[str, dict]:
    return {
        "task_run": {
            "task_run_id": TASK_ID,
            "org_id": "org_demo",
            "team_id": "team_growth",
            "user_id": "user_123",
            "channel": "web",
            "channel_conversation_id": "conversation_1",
            "intent": "business_root_cause_analysis",
            "status": "clarifying",
            "current_stage": "requirement_clarification",
            "correlation_id": "corr_001",
            "parent_task_run_id": None,
            "created_at": NOW,
            "updated_at": NOW,
            "metadata": {"locale": "zh-CN"},
        },
        "clarification_artifact": _envelope(
            "clarification",
            "data-requirement-clarifier",
            {
                "status": "needs_confirmation",
                "goal": "定位最近两周新用户首购转化下降的主要贡献因素",
                "known_facts": ["需要按渠道和终端拆分"],
                "assumptions": ["新用户按首次注册时间识别"],
                "open_questions": ["首购观察窗口是多少天？"],
                "dimensions": ["channel", "device"],
                "time_range": {
                    "description": "最近两个完整自然周",
                    "start": "2026-08-03T00:00:00+08:00",
                    "end": "2026-08-17T00:00:00+08:00",
                    "timezone": "Asia/Shanghai",
                    "granularity": "day",
                },
                "acceptance_criteria": ["识别转化下降贡献最大的渠道和终端"],
            },
        ),
        "metric_definition_artifact": _envelope(
            "metric_definition",
            "metric-definition-reviewer",
            {
                "status": "confirmed",
                "metric_name": "新用户首购转化率",
                "business_definition": "注册后七天内完成首笔支付的新用户占比",
                "numerator": "注册后七天内完成首笔支付的新用户数",
                "denominator": "同期有效注册新用户数",
                "grain": "registration_date/channel/device",
                "window": "registration_at + 7 days",
                "filters": [
                    {"field": "order_status", "operator": "eq", "value": "paid"}
                ],
                "boundary_conditions": ["同一用户只计入首次注册 Cohort"],
                "open_questions": [],
            },
        ),
        "query_result_artifact": _envelope(
            "query_result",
            "forge",
            {
                "query_run_id": "qr_demo_001",
                "sql_hash": SQL_HASH,
                "columns": ["channel", "device", "conversion_rate"],
                "rows": [["organic", "mobile", 0.069]],
                "row_count": 1,
                "truncated": False,
                "dialect": "postgresql",
                "registry_version": "registry-demo-v1",
                "execution_ms": 42,
                "executed_at": NOW,
                "result_contract": {"conversion_rate": "ratio"},
            },
        ),
        "analysis_artifact": _envelope(
            "analysis",
            "business-root-cause-analysis",
            {
                "status": "complete",
                "summary": "移动端贡献了本轮首购转化率下降的主要部分。",
                "findings": [
                    {
                        "statement": "移动端首购转化率低于桌面端。",
                        "evidence_refs": ["qr_demo_001#row:1"],
                        "confidence": "high",
                    }
                ],
                "hypotheses": [
                    {
                        "statement": "移动支付页性能变化可能影响转化。",
                        "evidence_refs": [],
                        "status": "unverified",
                    }
                ],
                "recommendations": [
                    {
                        "action": "补查移动支付页加载时间与失败率",
                        "rationale": "当前结果只能定位异常集中点，不能确认产品原因",
                        "priority": "high",
                    }
                ],
                "limitations": ["当前查询结果不包含支付页性能数据"],
                "suggested_queries": [],
            },
        ),
        "rendered_output_artifact": _envelope(
            "rendered_output",
            "data-analysis-report-writer",
            {
                "status": "complete",
                "title": "新用户首购转化率分析",
                "audience": "业务负责人和产品经理",
                "executive_summary": "移动端是当前结果中最明确的异常集中点。",
                "key_findings": [
                    {
                        "statement": "移动端首购转化率低于桌面端。",
                        "interpretation": "应优先补查移动支付链路。",
                        "evidence_refs": ["qr_demo_001#row:1"],
                        "confidence": "high",
                    }
                ],
                "recommendations": [
                    {
                        "action": "补查移动支付页加载时间与失败率",
                        "rationale": "现有结果不能确认产品原因",
                        "priority": "high",
                    }
                ],
                "limitations": ["当前缺少支付页性能数据"],
                "next_steps": ["完成性能数据补查"],
                "source_artifact_ids": ["ar_analysis_001"],
                "markdown": "# 新用户首购转化率分析\n\n移动端是异常集中点。",
            },
        ),
    }


def test_all_registered_contracts_are_valid_json_schemas() -> None:
    assert contract_names() == (
        "task_run",
        "clarification_artifact",
        "metric_definition_artifact",
        "query_result_artifact",
        "analysis_artifact",
        "rendered_output_artifact",
    )
    for name in contract_names():
        schema = load_contract(name)
        validator_for(schema).check_schema(schema)


def test_valid_instances_satisfy_contracts(valid_instances: dict[str, dict]) -> None:
    for name, instance in valid_instances.items():
        validate_contract(name, instance)


@pytest.mark.parametrize(
    ("contract_name", "mutation"),
    [
        ("task_run", lambda value: value.update(channel="slack")),
        ("clarification_artifact", lambda value: value["payload"].pop("goal")),
        (
            "metric_definition_artifact",
            lambda value: value["payload"].update(status="silently_approved"),
        ),
        (
            "query_result_artifact",
            lambda value: value["payload"].update(sql_hash="not-a-hash"),
        ),
        (
            "analysis_artifact",
            lambda value: value["payload"]["findings"][0].update(evidence_refs=[]),
        ),
        (
            "rendered_output_artifact",
            lambda value: value["payload"]["key_findings"][0].update(evidence_refs=[]),
        ),
    ],
)
def test_contracts_reject_invalid_boundary_data(
    valid_instances: dict[str, dict], contract_name: str, mutation
) -> None:
    instance = deepcopy(valid_instances[contract_name])
    mutation(instance)
    with pytest.raises(ValidationError):
        validate_contract(contract_name, instance)


def test_incomplete_analysis_requires_a_suggested_query(
    valid_instances: dict[str, dict],
) -> None:
    instance = deepcopy(valid_instances["analysis_artifact"])
    instance["payload"]["status"] = "incomplete"
    instance["payload"]["suggested_queries"] = []

    with pytest.raises(ValidationError):
        validate_contract("analysis_artifact", instance)

    instance["payload"]["suggested_queries"] = [
        {
            "question": "按终端查询支付页加载时间和失败率",
            "reason": "验证移动端性能是否与转化下降同步",
            "priority": "high",
        }
    ]
    validate_contract("analysis_artifact", instance)


def test_contract_rejects_unknown_fields(valid_instances: dict[str, dict]) -> None:
    instance = deepcopy(valid_instances["query_result_artifact"])
    instance["database_password"] = "must-never-cross-the-boundary"
    with pytest.raises(ValidationError):
        validate_contract("query_result_artifact", instance)


def test_unknown_contract_name_is_bounded() -> None:
    with pytest.raises(ValueError, match="Unknown contract"):
        load_contract("missing")
