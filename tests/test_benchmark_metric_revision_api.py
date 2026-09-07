"""Version-bound benchmark API behavior without candidate generation."""
from __future__ import annotations

import json
import sqlite3

import pytest

from forge.benchmark_v2 import RESULT_COMPARATOR_REVISION


@pytest.fixture
def benchmark_api(tmp_path, monkeypatch):
    from config import cfg
    from forge import bird_benchmark as bird
    from web.routes import benchmark_v2 as routes
    from forge import benchmark_service as service

    root = tmp_path / "dataset"
    root.mkdir()
    database = root / "benchmark.sqlite"
    with sqlite3.connect(database) as db:
        db.execute("CREATE TABLE orders(id INTEGER)")
        db.execute("INSERT INTO orders VALUES(1)")
    structure = {
        "db_id": "test",
        "tables": [{"name": "orders", "columns": [{"name": "id", "type": "integer", "description": "order identifier", "values": ""}]}],
        "relationships": [],
    }
    suite = {
        "manifest": {"suite": "fixed-test-suite"},
        "tables": {"test": structure},
        "cases": [{"case_id": "one", "question_id": 1, "db_id": "test", "difficulty": "simple", "question": "List order identifiers", "evidence": "orders.id", "SQL": "SELECT id FROM orders"}],
    }
    suite["cases"].extend({**suite["cases"][0], "case_id": f"other-{i}", "question_id": i + 2} for i in range(499))
    (root / "mini_dev_sqlite.json").write_text(json.dumps(suite["cases"]))
    (root / "dev_tables.json").write_text(json.dumps(structure))
    standard = bird._fingerprints(root, [root / "mini_dev_sqlite.json", root / "dev_tables.json", database])
    monkeypatch.setattr(bird, "standard_files", lambda: standard)
    monkeypatch.setattr(cfg, "PI_SERVICE_API_KEYS", ["test-pi-key"])
    monkeypatch.setattr(routes, "_suite", lambda: suite)
    monkeypatch.setattr(bird.hard, "load_suite", lambda _: suite)
    monkeypatch.setattr(bird.hard, "_BIRD_RUNTIME", root)
    monkeypatch.setattr(bird.hard, "_database_path", lambda _: database)
    monkeypatch.setattr(bird.hard, "_description_dir", lambda _: root)
    monkeypatch.setattr(bird, "PROTOCOL_DIR", tmp_path / "protocols")
    monkeypatch.setattr(service, "structure_projection", lambda value: value)
    monkeypatch.setattr(service, "_database_path", lambda _: database)
    return {"headers": {"X-Pi-Service-Key": "test-pi-key"}, "suite": suite, "database": database, "standard": standard}


async def preflight(client, fixture):
    response = await client.post("/api/internal/benchmark-v2/protocol", headers=fixture["headers"], json={
        "provider": "offline", "model": "fixture", "case_ids": ["one"], "confirm_model_calls": 2,
    })
    assert response.status_code == 200, response.text
    return response.json()


@pytest.mark.asyncio
async def test_evaluate_rejects_missing_or_stale_revision_before_execution(client, benchmark_api, monkeypatch):
    from web.routes import benchmark_v2 as routes

    def forbidden_suite():
        pytest.fail("Version rejection must precede context lookup and SQL execution")

    monkeypatch.setattr(routes, "_suite", forbidden_suite)
    payload = {"case_id": "one", "arm": "direct", "output": "SELECT 1", "context_snapshot": {}, "protocol_revision": "sha256:" + "0" * 64}
    missing = await client.post("/api/internal/benchmark-v2/evaluate", headers=benchmark_api["headers"], json=payload)
    assert missing.status_code == 422
    stale = await client.post("/api/internal/benchmark-v2/evaluate", headers=benchmark_api["headers"], json={
        **payload, "metric_revision": RESULT_COMPARATOR_REVISION + "-stale",
    })
    assert stale.status_code == 409


@pytest.mark.asyncio
async def test_frozen_context_evaluation_and_missing_generation(client, benchmark_api):
    frozen = await preflight(client, benchmark_api)
    context = frozen["contexts"]["one"]
    payload = {
        "case_id": "one", "arm": "direct", "output": "SELECT orders.id FROM orders",
        "context_snapshot": context["context_snapshot"], "metric_revision": frozen["metric_revision"],
        "protocol_revision": frozen["protocol_revision"],
    }
    success = await client.post("/api/internal/benchmark-v2/evaluate", headers=benchmark_api["headers"], json=payload)
    assert success.status_code == 200
    assert success.json()["official_ea"] is True
    assert success.json()["contract_accuracy"] is True
    assert success.json()["scored"] is True
    failed = await client.post("/api/internal/benchmark-v2/evaluate", headers=benchmark_api["headers"], json={**payload, "output": ""})
    assert failed.status_code == 200
    assert failed.json()["failure"]["code"] == "generation_empty"
    assert failed.json()["scored"] is True
    assert failed.json()["official_ea"] is False
    assert failed.json()["contract_accuracy"] is False


@pytest.mark.asyncio
async def test_ambiguous_execution_is_unknown_not_wrong(client, benchmark_api):
    benchmark_api["suite"]["cases"][0]["SQL"] = "SELECT 1, 1 UNION ALL SELECT 2, 2"
    frozen = await preflight(client, benchmark_api)
    response = await client.post("/api/internal/benchmark-v2/evaluate", headers=benchmark_api["headers"], json={
        "case_id": "one", "arm": "direct", "output": "SELECT 1, 2 UNION ALL SELECT 2, 1",
        "context_snapshot": frozen["contexts"]["one"]["context_snapshot"],
        "metric_revision": frozen["metric_revision"], "protocol_revision": frozen["protocol_revision"],
    })
    assert response.status_code == 200
    assert response.json()["contract_accuracy"] is None
    assert response.json()["column_mapping"] is None
    assert response.json()["failure"]["code"] == "result_column_alignment_ambiguous"


@pytest.mark.asyncio
async def test_preflight_enforces_exact_budget_and_rejects_arbitrary_paths(client, benchmark_api):
    payload = {"provider": "offline", "model": "fixture", "case_ids": ["one"], "confirm_model_calls": 3}
    response = await client.post("/api/internal/benchmark-v2/protocol", headers=benchmark_api["headers"], json=payload)
    assert response.status_code == 409
    frozen = await preflight(client, benchmark_api)
    frozen["manifest"]["dataset_files"] = {"/etc/passwd": "sha256:" + "0" * 64}
    response = await client.post("/api/internal/benchmark-v2/protocol", headers=benchmark_api["headers"], json={
        **payload, "confirm_model_calls": 2, "protocol_manifest": frozen["manifest"],
    })
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_post_preflight_database_drift_blocks_context_and_evaluation(client, benchmark_api):
    frozen = await preflight(client, benchmark_api)
    with sqlite3.connect(benchmark_api["database"]) as db:
        db.execute("INSERT INTO orders VALUES(2)")
    request = {"case_id": "one", "protocol_revision": frozen["protocol_revision"]}
    context = await client.post("/api/internal/benchmark-v2/context", headers=benchmark_api["headers"], json=request)
    assert context.status_code == 409
    result = await client.post("/api/internal/benchmark-v2/evaluate", headers=benchmark_api["headers"], json={
        **request, "metric_revision": frozen["metric_revision"], "arm": "direct", "output": "SELECT id FROM orders",
        "context_snapshot": frozen["contexts"]["one"]["context_snapshot"],
    })
    assert result.status_code == 409


@pytest.mark.asyncio
async def test_tampered_result_contract_cannot_reuse_snapshot_hash(client, benchmark_api):
    frozen = await preflight(client, benchmark_api)
    context = frozen["contexts"]["one"]["context_snapshot"]
    context["result_contract"]["order_sensitive"] = True
    result = await client.post("/api/internal/benchmark-v2/evaluate", headers=benchmark_api["headers"], json={
        "case_id": "one", "protocol_revision": frozen["protocol_revision"], "metric_revision": frozen["metric_revision"],
        "arm": "direct", "output": "SELECT id FROM orders", "context_snapshot": context,
    })
    assert result.status_code == 409


def test_gold_readiness_checks_only_selection_without_exposing_or_mutating_answers(benchmark_api):
    from forge import benchmark_service as routes

    suite = benchmark_api["suite"]
    suite["cases"][1]["SQL"] = "SELECT missing_secret FROM orders"
    before = json.dumps(suite, sort_keys=True)
    database_before = benchmark_api["database"].read_bytes()
    assert routes.check_gold_readiness(suite, ["one"]) == []
    assert json.dumps(suite, sort_keys=True) == before
    assert benchmark_api["database"].read_bytes() == database_before
    failures = routes.check_gold_readiness(suite, ["one", "other-0"])
    assert failures == [{"case_id": "other-0", "db_id": "test", "code": "unknown_column"}]
    assert "missing_secret" not in json.dumps(failures)


def test_gold_readiness_reports_every_failed_selection_without_sql_or_errors(benchmark_api):
    from forge import benchmark_service as routes

    suite = benchmark_api["suite"]
    suite["cases"][0]["SQL"] = "SELECT missing_secret FROM orders"
    suite["cases"][1]["SQL"] = "SELECT * FROM private_secret"
    failures = routes.check_gold_readiness(suite, ["one", "other-0"])
    assert failures == [
        {"case_id": "one", "db_id": "test", "code": "unknown_column"},
        {"case_id": "other-0", "db_id": "test", "code": "unknown_table"},
    ]
    assert "secret" not in json.dumps(failures)


@pytest.mark.asyncio
async def test_default_preflight_rejects_unscorable_gold_without_exposing_answers(client, benchmark_api):
    benchmark_api["suite"]["cases"][1]["SQL"] = "SELECT missing_secret FROM orders"
    response = await client.post("/api/internal/benchmark-v2/protocol", headers=benchmark_api["headers"], json={
        "provider": "offline", "model": "fixture", "case_ids": ["one", "other-0"],
        "confirm_model_calls": 4,
    })
    assert response.status_code == 409
    assert "other-0" in response.text
    assert "unknown_column" in response.text
    assert "missing_secret" not in response.text


@pytest.mark.asyncio
async def test_explicit_gold_skip_keeps_denominator_and_requires_reduced_budget(client, benchmark_api):
    from forge import bird_benchmark as bird

    benchmark_api["suite"]["cases"][1]["SQL"] = "SELECT missing_secret FROM orders"
    selected = ["one", "other-0"]
    frozen = bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=selected,
                         gold_policy="skip_unscorable")
    payload = {
        "provider": "offline", "model": "fixture", "case_ids": selected,
        "confirm_model_calls": 4, "protocol_manifest": frozen["manifest"],
    }
    rejected = await client.post("/api/internal/benchmark-v2/protocol", headers=benchmark_api["headers"], json=payload)
    assert rejected.status_code == 409
    response = await client.post("/api/internal/benchmark-v2/protocol", headers=benchmark_api["headers"], json={
        **payload, "confirm_model_calls": 2,
    })
    assert response.status_code == 200, response.text
    protocol = response.json()
    assert protocol["case_ids"] == selected
    assert protocol["manifest"]["denominator"] == 2
    assert protocol["generation"]["max_model_calls"] == 2
    assert protocol["gold_readiness"] == {
        "policy": "skip_unscorable",
        "blocked_cases": [{"case_id": "other-0", "db_id": "test", "code": "unknown_column"}],
    }
    assert protocol["manifest"]["gold_readiness"] == protocol["gold_readiness"]
    assert set(protocol["contexts"]) == set(selected)
    assert "missing_secret" not in response.text


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_case, sql", [
    (1, "SELECT id FROM orders"),
    (0, "SELECT missing_secret FROM orders"),
    (1, "SELECT * FROM private_secret"),
])
async def test_explicit_gold_skip_rejects_live_report_drift(client, benchmark_api, changed_case, sql):
    from forge import bird_benchmark as bird

    benchmark_api["suite"]["cases"][1]["SQL"] = "SELECT missing_secret FROM orders"
    selected = ["one", "other-0"]
    frozen = bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=selected,
                         gold_policy="skip_unscorable")
    # In-memory fixture change leaves dataset/context hashes intact, isolating readiness drift.
    benchmark_api["suite"]["cases"][changed_case]["SQL"] = sql
    response = await client.post("/api/internal/benchmark-v2/protocol", headers=benchmark_api["headers"], json={
        "provider": "offline", "model": "fixture", "case_ids": selected,
        "confirm_model_calls": 2, "protocol_manifest": frozen["manifest"],
    })
    assert response.status_code == 409
    assert "secret" not in response.text



@pytest.mark.asyncio
@pytest.mark.parametrize("arm, output", [
    ("direct", "SELECT orders.id FROM orders"),
    ("forge", {"scan": "orders", "select": ["id"]}),
])
async def test_gold_failure_preserves_candidate_success_as_unscored(client, benchmark_api, arm, output):
    frozen = await preflight(client, benchmark_api)
    benchmark_api["suite"]["cases"][0]["SQL"] = "SELECT missing_secret FROM orders"
    response = await client.post("/api/internal/benchmark-v2/evaluate", headers=benchmark_api["headers"], json={
        "case_id": "one", "arm": arm, "output": output,
        "context_snapshot": frozen["contexts"]["one"]["context_snapshot"],
        "metric_revision": frozen["metric_revision"], "protocol_revision": frozen["protocol_revision"],
    })
    assert response.status_code == 200
    result = response.json()
    assert result["execution_status"] == "passed"
    assert result["scored"] is False
    assert result["official_ea"] is None
    assert result["contract_accuracy"] is None
    assert result["result"]["rows"] == [[1]]
    assert result["failure"] == {"stage": "gold", "code": "gold_execution_failed", "retryable": False}
    assert "missing_secret" not in response.text
