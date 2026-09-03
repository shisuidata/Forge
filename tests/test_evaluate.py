"""Public Evaluate API and CLI contract tests."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx
import pytest

from forge.cli import main as cli_main


@pytest.fixture
def evaluate_registry(tmp_path, monkeypatch):
    from config import cfg

    path = tmp_path / "schema.registry.json"
    path.write_text(
        json.dumps(
            {
                "tables": {
                    "orders": {
                        "columns": {
                            "id": {},
                            "user_id": {},
                            "status": {},
                            "total_amount": {},
                        }
                    },
                    "users": {"columns": {"id": {}, "name": {}}},
                },
                "relationships": [
                    {
                        "id": "orders_user",
                        "from": "orders.user_id",
                        "to": "users.id",
                        "cardinality": "many_to_one",
                        "status": "confirmed",
                        "source": "manual",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(cfg, "REGISTRY_PATH", path)
    return path


def _request(candidate: dict, **overrides) -> dict:
    payload = {
        "schema_version": 1,
        "question": "List order identifiers",
        "dialect": "sqlite",
        "candidate": candidate,
    }
    payload.update(overrides)
    return payload


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "candidate,expected_sql_fragment",
    [
        (
            {
                "kind": "direct_sql",
                "sql": "SELECT orders.id FROM orders",
                "producer_revision": "external-agent-r1",
            },
            "SELECT orders.id FROM orders",
        ),
        (
            {
                "kind": "forge_json",
                "forge_json": {"scan": "orders", "select": ["orders.id"]},
                "producer_revision": "external-agent-r1",
            },
            "SELECT orders.id",
        ),
    ],
)
async def test_public_evaluate_uses_one_envelope_for_both_input_kinds(
    client, evaluate_registry, candidate, expected_sql_fragment
):
    response = await client.post("/api/v1/evaluate", json=_request(candidate))

    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == 1
    assert body["status"] == "passed"
    assert body["candidate"]["input_kind"] == candidate["kind"]
    assert body["candidate"]["candidate_revision"] == "query-candidate-v1"
    assert body["policy"] == {
        "verdict": "allow_review",
        "review_required": True,
        "execution_authorized": False,
    }
    assert body["result_comparison"]["status"] == "not_requested"
    assert body["failure"] is None
    assert expected_sql_fragment in body["compiled_sql"]
    assert body["lineage"]["sql_hash"].startswith("sha256:")
    assert body["evidence_refs"][0].endswith("#candidate")
    assert "rows" not in body


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "candidate,allowed_tables,code",
    [
        (
            {"kind": "direct_sql", "sql": "SELECT 1", "unexpected": True},
            None,
            "candidate_contract_invalid",
        ),
        (
            {"kind": "unsupported", "sql": "SELECT 1"},
            None,
            "candidate_contract_invalid",
        ),
        (
            {"kind": "direct_sql", "sql": "UPDATE orders SET status = 'paid'"},
            None,
            "readonly_violation",
        ),
        (
            {"kind": "direct_sql", "sql": "SELECT orders.id FROM orders"},
            ["users"],
            "unknown_schema_reference",
        ),
    ],
)
async def test_public_evaluate_returns_stable_candidate_failure_codes(
    client, evaluate_registry, candidate, allowed_tables, code
):
    response = await client.post(
        "/api/v1/evaluate",
        json=_request(candidate, allowed_tables=allowed_tables),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "failed"
    assert body["policy"]["verdict"] == "deny"
    assert body["policy"]["execution_authorized"] is False
    assert body["failure"]["code"] == code
    assert body["result_comparison"]["status"] == "not_run"
    if candidate.get("kind") == "unsupported":
        assert body["candidate"]["input_kind"] == "unknown"


@pytest.mark.asyncio
async def test_public_evaluate_compares_external_results_without_executing(
    client, evaluate_registry
):
    payload = _request(
        {"kind": "direct_sql", "sql": "SELECT orders.id FROM orders"},
        expected_result={"columns": ["id"], "rows": [[1], [2]]},
        actual_result={"columns": ["id"], "rows": [[1], [3]]},
    )

    first = (await client.post("/api/v1/evaluate", json=payload)).json()
    second = (await client.post("/api/v1/evaluate", json=payload)).json()

    assert first["status"] == "failed"
    assert first["policy"]["verdict"] == "allow_review"
    assert first["result_comparison"]["status"] == "failed"
    assert first["failure"] == {
        "stage": "result_contract",
        "code": "result_value_mismatch",
        "retryable": True,
    }
    assert first["evaluation_id"] == second["evaluation_id"]
    assert first["lineage"]["request_hash"] == second["lineage"]["request_hash"]
    assert first["evidence_refs"][-1].endswith("#result-comparison")


@pytest.mark.asyncio
async def test_public_evaluate_requires_expected_and_actual_results_together(
    client, evaluate_registry
):
    response = await client.post(
        "/api/v1/evaluate",
        json=_request(
            {"kind": "direct_sql", "sql": "SELECT orders.id FROM orders"},
            expected_result={"columns": ["id"], "rows": [[1]]},
        ),
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_public_evaluate_honors_api_auth(client, evaluate_registry, monkeypatch):
    from config import cfg

    monkeypatch.setattr(cfg, "AUTH_ENABLED", True)
    monkeypatch.setattr(cfg, "AUTH_API_KEYS", ["valid-key"])
    payload = _request({"kind": "direct_sql", "sql": "SELECT 1"})

    unauthorized = await client.post("/api/v1/evaluate", json=payload)
    authorized = await client.post(
        "/api/v1/evaluate",
        json=payload,
        headers={"X-API-Key": "valid-key"},
    )

    assert unauthorized.status_code == 401
    assert authorized.status_code == 200
    assert authorized.json()["status"] == "passed"


def test_evaluate_cli_posts_versioned_request_and_prints_response(
    tmp_path: Path, monkeypatch, capsys
):
    request_path = tmp_path / "evaluate.json"
    request_payload = _request({"kind": "direct_sql", "sql": "SELECT 1 AS value"})
    request_path.write_text(json.dumps(request_payload), encoding="utf-8")
    response_payload = {
        "schema_version": 1,
        "status": "passed",
        "evaluation_id": "ev_example",
    }
    captured_request = {}

    def fake_post(url, *, json, headers, timeout):
        captured_request.update(
            {"url": url, "json": json, "headers": headers, "timeout": timeout}
        )
        return httpx.Response(200, json=response_payload)

    monkeypatch.setattr(httpx, "post", fake_post)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "forge",
            "evaluate",
            str(request_path),
            "--url",
            "https://forge.example/",
            "--api-key",
            "secret",
            "--timeout",
            "12",
        ],
    )

    cli_main()

    output = json.loads(capsys.readouterr().out)
    assert output == response_payload
    assert captured_request == {
        "url": "https://forge.example/api/v1/evaluate",
        "json": request_payload,
        "headers": {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-API-Key": "secret",
        },
        "timeout": 12.0,
    }


def test_evaluate_cli_exits_nonzero_when_gate_fails(tmp_path: Path, monkeypatch, capsys):
    request_path = tmp_path / "evaluate.json"
    request_path.write_text(
        json.dumps(_request({"kind": "direct_sql", "sql": "DELETE FROM orders"})),
        encoding="utf-8",
    )
    response_payload = {
        "schema_version": 1,
        "status": "failed",
        "failure": {"stage": "assurance", "code": "readonly_violation", "retryable": False},
    }

    monkeypatch.setattr(
        httpx,
        "post",
        lambda *args, **kwargs: httpx.Response(200, json=response_payload),
    )
    monkeypatch.setattr(sys, "argv", ["forge", "evaluate", str(request_path)])

    with pytest.raises(SystemExit) as caught:
        cli_main()

    assert caught.value.code == 1
    assert json.loads(capsys.readouterr().out) == response_payload


def test_evaluate_cli_rejects_non_object_response(tmp_path: Path, monkeypatch, capsys):
    request_path = tmp_path / "evaluate.json"
    request_path.write_text(
        json.dumps(_request({"kind": "direct_sql", "sql": "SELECT 1"})),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        httpx,
        "post",
        lambda *args, **kwargs: httpx.Response(200, json=[]),
    )
    monkeypatch.setattr(sys, "argv", ["forge", "evaluate", str(request_path)])

    with pytest.raises(SystemExit) as caught:
        cli_main()

    assert caught.value.code == 2
    assert "Evaluate response must be a JSON object" in capsys.readouterr().err
