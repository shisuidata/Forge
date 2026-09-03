"""Persistent evaluation suite, replay, manifest, and regression gate tests."""
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import sys

import httpx
import pytest

from agent.contracts import validate_contract
from forge.cli import main as cli_main
from forge.evaluation_runs import recompute_aggregate

_ROOT = Path(__file__).resolve().parents[1]
_PUBLIC_SUITE = _ROOT / "examples" / "evaluation-suite-v1.json"


@pytest.fixture
def evaluation_run_env(tmp_path, monkeypatch):
    from config import cfg

    registry_path = tmp_path / "schema.registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "tables": {
                    "dim_user": {
                        "columns": {
                            "user_id": {},
                            "name": {},
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(cfg, "REGISTRY_PATH", registry_path)
    monkeypatch.setenv("EVALUATION_RUN_DB_PATH", str(tmp_path / "evaluation-runs.db"))
    return json.loads(_PUBLIC_SUITE.read_text(encoding="utf-8"))


def _run_request(suite: dict, **overrides) -> dict:
    request = {
        "schema_version": 1,
        "suite": suite,
        "regression_gate": {
            "max_new_failures": 0,
            "max_pass_rate_drop": 0.0,
        },
    }
    request.update(overrides)
    return request


@pytest.mark.asyncio
async def test_public_suite_persists_replays_and_exports_recomputable_manifest(
    client, evaluation_run_env
):
    response = await client.post(
        "/api/v1/evaluation-runs",
        json=_run_request(evaluation_run_env),
    )

    assert response.status_code == 200
    manifest = response.json()
    validate_contract("evaluation_run_manifest_v1", manifest)
    assert manifest["status"] == "completed"
    assert manifest["aggregate"] == {
        "total_cases": 2,
        "passed_cases": 2,
        "failed_cases": 0,
        "pass_rate": 1.0,
        "evaluation_status_counts": {"failed": 1, "passed": 1},
        "failure_code_counts": {"readonly_violation": 1},
    }
    assert recompute_aggregate(manifest["outcomes"]) == manifest["aggregate"]
    assert manifest["regression"]["status"] == "not_requested"
    assert manifest["configuration"]["evaluator_revision"] == "evaluate-v1"
    assert manifest["configuration"]["metric_revision"] == "semantic-result-compare-v1"
    assert manifest["configuration"]["registry_revisions"]

    exported = await client.get(f"/api/v1/evaluation-runs/{manifest['run_id']}")
    suite = await client.get(
        f"/api/v1/evaluation-suites/{manifest['suite_revision']}"
    )
    replay = await client.post(
        "/api/v1/evaluation-runs",
        json={
            "schema_version": 1,
            "suite_revision": manifest["suite_revision"],
            "regression_gate": {
                "max_new_failures": 0,
                "max_pass_rate_drop": 0.0,
            },
        },
    )

    assert exported.status_code == 200
    assert exported.json() == manifest
    assert suite.status_code == 200
    assert suite.json() == evaluation_run_env
    assert replay.status_code == 200
    replay_manifest = replay.json()
    assert replay_manifest["aggregate"] == manifest["aggregate"]
    assert [item["evaluation"] for item in replay_manifest["outcomes"]] == [
        item["evaluation"] for item in manifest["outcomes"]
    ]


@pytest.mark.asyncio
async def test_regression_gate_detects_new_failure_across_producer_revisions(
    client, evaluation_run_env
):
    baseline = (
        await client.post(
            "/api/v1/evaluation-runs",
            json=_run_request(evaluation_run_env),
        )
    ).json()
    changed = deepcopy(evaluation_run_env)
    changed["producer"]["revision"] = "example-agent-v2"
    for case in changed["cases"]:
        case["candidate"]["producer_revision"] = "example-agent-v2"
    changed["cases"][0]["actual_result"]["rows"] = [[2]]

    response = await client.post(
        "/api/v1/evaluation-runs",
        json=_run_request(changed, baseline_run_id=baseline["run_id"]),
    )

    assert response.status_code == 200
    manifest = response.json()
    assert manifest["aggregate"]["failed_cases"] == 1
    assert manifest["regression"] == {
        "status": "failed",
        "release_gate": "failed",
        "baseline_run_id": baseline["run_id"],
        "comparable": True,
        "incompatible_dimensions": [],
        "new_failures": ["exact-result-pass"],
        "recovered_cases": [],
        "pass_rate_delta": -0.5,
        "gate": {"max_new_failures": 0, "max_pass_rate_drop": 0.0},
    }


@pytest.mark.asyncio
async def test_regression_gate_marks_changed_evaluation_basis_not_comparable(
    client, evaluation_run_env
):
    baseline = (
        await client.post(
            "/api/v1/evaluation-runs",
            json=_run_request(evaluation_run_env),
        )
    ).json()
    incompatible = deepcopy(evaluation_run_env)
    incompatible["dataset"]["revision"] = "inline-public-fixture-v2"

    response = await client.post(
        "/api/v1/evaluation-runs",
        json=_run_request(incompatible, baseline_run_id=baseline["run_id"]),
    )

    assert response.status_code == 200
    regression = response.json()["regression"]
    assert regression["status"] == "not_comparable"
    assert regression["release_gate"] == "failed"
    assert regression["comparable"] is False
    assert regression["incompatible_dimensions"] == ["dataset"]


@pytest.mark.asyncio
async def test_evaluation_run_api_rejects_ambiguous_or_missing_inputs(
    client, evaluation_run_env
):
    ambiguous = await client.post(
        "/api/v1/evaluation-runs",
        json={
            "schema_version": 1,
            "suite": evaluation_run_env,
            "suite_revision": "sha256:" + "a" * 64,
        },
    )
    missing_suite = await client.post(
        "/api/v1/evaluation-runs",
        json={
            "schema_version": 1,
            "suite_revision": "sha256:" + "a" * 64,
        },
    )
    missing_run = await client.get("/api/v1/evaluation-runs/evr_" + "a" * 32)

    assert ambiguous.status_code == 422
    assert missing_suite.status_code == 404
    assert missing_run.status_code == 404


def test_public_fixture_satisfies_versioned_suite_contract(evaluation_run_env):
    validate_contract("evaluation_suite_v1", evaluation_run_env)


def test_evaluate_cli_runs_and_persists_suite(tmp_path: Path, monkeypatch, capsys):
    suite = json.loads(_PUBLIC_SUITE.read_text(encoding="utf-8"))
    suite_path = tmp_path / "suite.json"
    suite_path.write_text(json.dumps(suite), encoding="utf-8")
    response_payload = {
        "schema_version": 1,
        "run_id": "evr_" + "a" * 32,
        "status": "completed",
        "aggregate": {"failed_cases": 0},
        "regression": {"release_gate": "not_evaluated"},
    }
    captured = {}

    def fake_post(url, *, json, headers, timeout):
        captured.update({"url": url, "json": json, "timeout": timeout})
        return httpx.Response(200, json=response_payload)

    monkeypatch.setattr(httpx, "post", fake_post)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "forge",
            "evaluate",
            str(suite_path),
            "--suite",
            "--baseline-run",
            "evr_" + "b" * 32,
            "--url",
            "https://forge.example/",
        ],
    )

    cli_main()

    assert json.loads(capsys.readouterr().out) == response_payload
    assert captured["url"] == "https://forge.example/api/v1/evaluation-runs"
    assert captured["json"]["suite"] == suite
    assert captured["json"]["baseline_run_id"] == "evr_" + "b" * 32
    assert captured["json"]["regression_gate"] == {
        "max_new_failures": 0,
        "max_pass_rate_drop": 0.0,
    }


def test_evaluate_cli_fails_closed_on_regression_gate(monkeypatch, capsys):
    response_payload = {
        "schema_version": 1,
        "run_id": "evr_" + "a" * 32,
        "status": "completed",
        "aggregate": {"failed_cases": 0},
        "regression": {"release_gate": "failed"},
    }
    monkeypatch.setattr(
        httpx,
        "post",
        lambda *args, **kwargs: httpx.Response(200, json=response_payload),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "forge",
            "evaluate",
            "--suite-revision",
            "sha256:" + "a" * 64,
        ],
    )

    with pytest.raises(SystemExit) as caught:
        cli_main()

    assert caught.value.code == 1
    assert json.loads(capsys.readouterr().out) == response_payload
