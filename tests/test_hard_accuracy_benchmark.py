from __future__ import annotations

import json
from pathlib import Path
import pytest

from agent.model_config import ModelConfigSnapshot
from forge import hard_accuracy_benchmark as hard
from forge.benchmark_methods import bird_execution_accuracy


def test_bird_execution_accuracy_uses_exact_result_sets():
    assert bird_execution_accuracy(
        [(1, "A"), (2, "B")], [(2, "B"), (1, "A"), (1, "A")]
    )
    assert not bird_execution_accuracy([(44.26229508196721,)], [(44.26,)])
    assert not bird_execution_accuracy([(1, "A")], [(1, "a")])
    assert not bird_execution_accuracy([(1, 2)], [(2, 1)])


def test_full_mini_dev_suite_has_official_coverage_without_model_calls():
    if not all((hard._DB_ROOT / db_id / f"{db_id}.sqlite").exists() for db_id in ("california_schools", "financial", "formula_1")):
        pytest.skip("full Mini-Dev runtime assets are not installed")
    suite = hard.load_suite(hard._FULL_SUITE_ID)
    assert len(suite["cases"]) == 500
    assert len(suite["tables"]) == 11
    assert suite["manifest"]["expected_model_calls"] == 1000
    assert suite["manifest"]["evaluation"]["sql_text_scored"] is False
    assert len({_case["case_id"] for _case in suite["cases"]}) == 500
    cached = hard._cached_gold_answers(suite)
    assert len(cached) == 500
    assert cached["md-340"]["rows"] == [(0.6644518272425249,)]
    assert len(cached["md-393"]["rows"]) == 546


def test_full_gold_cache_avoids_runtime_gold_queries(monkeypatch):
    if not all((hard._DB_ROOT / db_id / f"{db_id}.sqlite").exists() for db_id in ("california_schools", "financial", "formula_1")):
        pytest.skip("full Mini-Dev runtime assets are not installed")
    suite = hard.load_suite(hard._FULL_SUITE_ID)
    monkeypatch.setattr(
        hard,
        "execute_result",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("cache miss")),
    )
    assert len(hard.validate_gold_cases(suite)) == 500


def test_create_run_persists_before_gold_validation(tmp_path: Path, monkeypatch):
    cases_path = tmp_path / "cases.json"
    tables_path = tmp_path / "tables.json"
    cases_path.write_text("[]")
    tables_path.write_text("[]")
    unloaded_suite = {
        **suite(),
        "_case_path": cases_path,
        "_tables_path": tables_path,
    }
    monkeypatch.setattr(hard, "load_suite", lambda suite_id: unloaded_suite)
    monkeypatch.setattr(
        hard,
        "hydrate_suite",
        lambda suite_id: (_ for _ in ()).throw(AssertionError("Gold validation blocked create")),
    )
    monkeypatch.setattr(hard, "_method_ai_snapshot", lambda method: snapshot())
    monkeypatch.setattr(hard, "ark_coding_plan_method", lambda root: object())
    monkeypatch.setattr(hard, "_code_revision", lambda: "test")
    monkeypatch.setattr(hard, "_file_sha256", lambda path: "sha256:test")
    monkeypatch.setattr(hard, "_column_descriptions", lambda db_id: {})
    store = hard.HardBenchmarkStore(tmp_path / "create.db")

    run_id, _, loaded = hard.create_hard_run(
        store,
        hard.HardBenchmarkConfig(
            suite_id=hard._FULL_SUITE_ID, runs_per_case=1, workers=1
        ),
    )

    assert loaded is unloaded_suite
    assert store.run_suite_id(run_id) == hard._FULL_SUITE_ID
    assert store.snapshot(run_id, unloaded_suite)["status"] == "queued"


def test_execute_result_interrupts_overlong_sql(tmp_path: Path):
    database = tmp_path / "timeout.sqlite"
    with hard.sqlite3.connect(database) as db:
        db.execute("CREATE TABLE seed(value INTEGER)")
        db.execute("INSERT INTO seed VALUES(1)")

    with pytest.raises(hard.HardBenchmarkError, match="timeout"):
        hard.execute_result(
            database,
            "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq) SELECT * FROM seq",
            timeout_seconds=0.01,
        )


def snapshot() -> ModelConfigSnapshot:
    return ModelConfigSnapshot(
        provider="openai",
        model="ark-code-latest",
        api_key="never-projected",
        base_url="https://ark.cn-beijing.volces.com/api/coding/v3",
        tool_choice="auto",
        timeout_seconds=120,
        revision="rev-hard-test",
        source="test",
    )


def suite() -> dict:
    return {
        "manifest": {
            "suite": hard._LEGACY_SUITE_ID,
            "source": {"project": "BIRD-SQL"},
        },
        "tables": {
            "demo": {
                "db_id": "demo",
                "table_names_original": ["facts"],
                "table_names": ["facts"],
                "column_names_original": [[-1, "*"], [0, "id"], [0, "value"]],
                "column_names": [[-1, "*"], [0, "id"], [0, "value"]],
                "column_types": ["text", "number", "number"],
                "primary_keys": [1],
                "foreign_keys": [],
            }
        },
        "cases": [
            {
                "question_id": 1,
                "db_id": "demo",
                "difficulty": "challenging",
                "question": "Official hard question",
                "evidence": "Official evidence",
                "SQL": "SELECT value FROM facts",
                "source": {"dataset": "BIRD-SQL Mini-Dev"},
                "gold_preview": {
                    "columns": ["value"],
                    "rows": [[1]],
                    "row_count": 1,
                    "truncated": False,
                },
                "_gold_rows": [(1,)],
            }
        ],
    }


def observation(method: str, *, correct: bool) -> dict:
    return {
        "method_id": method,
        "case_id": "1",
        "run_index": 1,
        "db_id": "demo",
        "correct": correct,
        "scoring_standard": hard._SCORING_STANDARD,
        "executable": True,
        "attempts": 1,
        "latency_ms": 120.0 if method == "forge" else 80.0,
        "error_code": None if correct else "incorrect_result",
        "error_message": None if correct else "SQL 可以执行，但结果不一致。",
        "generated_sql": (
            "SELECT value FROM facts" if correct else "SELECT id FROM facts"
        ),
        "forge_json": (
            {"scan": "facts", "select": ["facts.value"]}
            if method == "forge"
            else None
        ),
        "generated_preview": {
            "columns": ["value"],
            "rows": [[1]],
            "row_count": 1,
            "truncated": False,
        },
        "gold_preview": {
            "columns": ["value"],
            "rows": [[1]],
            "row_count": 1,
            "truncated": False,
        },
        "sql_hash": "sha256:test",
    }


def test_hard_benchmark_projects_analysis_history_logs_and_sql_details(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setattr(hard, "_column_descriptions", lambda db_id: {})
    store = hard.HardBenchmarkStore(tmp_path / "hard.db")
    run_id = store.create_run(
        hard.HardBenchmarkConfig(
            suite_id=hard._LEGACY_SUITE_ID, runs_per_case=1, workers=1
        ),
        snapshot(),
        total_cases=1,
        lineage={"code_revision": "test"},
    )
    store.mark_running(run_id)
    store.log(
        run_id,
        method_id="forge",
        case_id="1",
        run_index=1,
        stage="model_call",
        level="info",
        message="forge model call",
    )
    store.record_observation(run_id, observation("forge", correct=True))
    store.record_observation(run_id, observation("direct", correct=False))
    store.complete(run_id, status="completed")

    projected = store.snapshot(run_id, suite())
    assert projected is not None
    assert projected["status"] == "completed"
    assert projected["schema_version"] == 3
    assert projected["config"]["suite_id"] == hard._LEGACY_SUITE_ID
    assert projected["progress"]["completed_calls"] == 2
    assert projected["metrics"]["forge"]["execution_accuracy"] == 1.0
    assert projected["metrics"]["direct"]["execution_accuracy"] == 0.0
    assert projected["delta"]["execution_accuracy"] == 1.0
    case = projected["cases"][0]
    assert case["winner"] == "forge"
    assert case["evidence"] == "Official evidence"
    assert case["gold_sql"] == "SELECT value FROM facts"
    assert case["results"]["forge"][0]["completed_at"]
    assert case["results"]["direct"][0]["error_code"] == "incorrect_result"

    history = store.history(limit=5)
    assert history[0]["run_id"] == run_id
    assert history[0]["metrics"]["forge"]["execution_accuracy"] == 1.0
    logs = store.logs(run_id, method_id="forge", stage="evaluated")
    assert logs is not None and logs["total"] == 1
    assert logs["items"][0]["payload"]["correct"] is True
    assert "never-projected" not in json.dumps(projected)


def test_hard_benchmark_reconciles_without_replaying(tmp_path: Path):
    store = hard.HardBenchmarkStore(tmp_path / "hard.db")
    run_id = store.create_run(
        hard.HardBenchmarkConfig(
            suite_id=hard._LEGACY_SUITE_ID, runs_per_case=1, workers=1
        ),
        snapshot(),
        total_cases=1,
        lineage={"code_revision": "test"},
    )
    store.mark_running(run_id)
    assert store.reconcile_interrupted() == 1
    with store._connect() as db:
        run = dict(
            db.execute(
                "SELECT * FROM hard_benchmark_runs WHERE run_id=?", (run_id,)
            ).fetchone()
        )
        observations = db.execute(
            "SELECT COUNT(*) FROM hard_benchmark_observations WHERE run_id=?",
            (run_id,),
        ).fetchone()[0]
    assert run["status"] == "interrupted"
    assert run["error_code"] == "process_restarted"
    assert observations == 0
