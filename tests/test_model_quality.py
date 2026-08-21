from __future__ import annotations

import json

import pytest

from agent.model_control import ModelControlError, ModelControlStore
from agent.model_quality import DEFAULT_THRESHOLDS, evaluate_metrics, run_quality_validation


def _revision(store: ModelControlStore) -> str:
    revision = store.create_revision(
        profile_id="candidate",
        name="Candidate",
        config={
            "provider": "openai",
            "protocol": "openai_chat",
            "base_url": "https://provider.example/v1",
            "model": "candidate-model",
            "tool_choice": "required",
            "timeout_seconds": 30,
            "secret_ref": "env:MODEL_TEST_KEY",
            "capabilities": {},
        },
    )
    store.record_validation(
        revision,
        passed=True,
        report={
            "tool_calling": True,
            "structured_output": True,
            "quality_gate": {"passed": False, "status": "not_run"},
        },
    )
    return revision


def test_evaluate_metrics_applies_every_activation_threshold():
    results = [
        {"correct": True, "assurance_passed": True, "retry_count": 0, "latency_ms": 10, "timed_out": False},
        {"correct": False, "assurance_passed": True, "retry_count": 2, "latency_ms": 40, "timed_out": False},
    ]
    metrics = evaluate_metrics(
        results,
        thresholds={
            **DEFAULT_THRESHOLDS,
            "accuracy_min": 0.75,
            "assurance_pass_rate_min": 1.0,
            "average_retry_max": 1.0,
            "p95_latency_ms_max": 50,
            "timeout_rate_max": 0,
        },
        smoke_report={"tool_calling": True, "structured_output": True},
    )
    assert metrics["passed"] is False
    assert metrics["checks"]["accuracy"] is False
    assert metrics["checks"]["assurance_pass_rate"] is True


def test_quality_run_persists_cases_and_marks_revision_eligible(tmp_path, monkeypatch):
    store = ModelControlStore(tmp_path / "models.db")
    revision = _revision(store)
    monkeypatch.setenv("MODEL_TEST_KEY", "secret")
    cases_path = tmp_path / "cases.json"
    cases_path.write_text(json.dumps([
        {"id": 1, "question": "q1", "reference_sql": "SELECT 1"},
        {"id": 2, "question": "q2", "reference_sql": "SELECT 2"},
    ]))
    thresholds = {
        **DEFAULT_THRESHOLDS,
        "accuracy_min": 1.0,
        "assurance_pass_rate_min": 1.0,
        "average_retry_max": 0,
        "p95_latency_ms_max": 10_000,
        "timeout_rate_max": 0,
    }
    run_id = store.create_quality_validation_run(revision, thresholds=thresholds)

    def prepare(_user, question, **_kwargs):
        return {
            "status": "needs_review",
            "sql": "SELECT 1" if question == "q1" else "SELECT 2",
            "retry_count": 0,
        }

    metrics = run_quality_validation(
        store,
        run_id,
        cases_path=cases_path,
        database_url="sqlite:///:memory:",
        prepare_fn=prepare,
        execute_fn=lambda _url, sql: [(int(sql.split()[-1]),)],
    )

    run = store.get_quality_validation_run(run_id)
    assert metrics["passed"] is True
    assert run["status"] == "passed"
    assert len(run["cases"]) == 2
    assert store.get_revision(revision)["validation_report"]["quality_gate"]["passed"] is True
    lineage = store.get_revision(revision)["validation_report"]["quality_gate"]["lineage"]
    assert store.activate(
        revision,
        expected_version=0,
        actor="test",
        current_lineage=lineage,
    ) == 1
    with pytest.raises(ModelControlError, match="不允许重新验证"):
        store.create_quality_validation_run(revision, thresholds=thresholds)


def test_reconciliation_interrupts_without_replaying_work(tmp_path):
    store = ModelControlStore(tmp_path / "models.db")
    revision = _revision(store)
    run_id = store.create_quality_validation_run(revision, thresholds=DEFAULT_THRESHOLDS)
    store.mark_quality_validation_running(run_id)

    assert store.reconcile_quality_validation_runs() == 1
    assert store.get_quality_validation_run(run_id)["status"] == "interrupted"
    assert store.reconcile_quality_validation_runs() == 0
