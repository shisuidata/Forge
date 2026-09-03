"""Public Explain API and CLI evidence contract tests."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
import sys

import httpx
import pytest

from agent.contracts import validate_contract
from forge.cli import main as cli_main


@pytest.fixture
def explain_env(tmp_path, monkeypatch):
    from config import cfg
    import forge.executor as executor

    registry = tmp_path / "schema.registry.json"
    registry.write_text(
        json.dumps(
            {
                "tables": {
                    "numbers": {
                        "description": "Synthetic numbers",
                        "columns": {
                            "n": {"description": "Synthetic integer"}
                        },
                    },
                    "secrets": {
                        "description": "Restricted values",
                        "columns": {"value": {"description": "Restricted value"}},
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    database = tmp_path / "data.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE numbers (n INTEGER)")
        connection.executemany("INSERT INTO numbers VALUES (?)", [(1,), (2,), (3,)])
        connection.execute("CREATE TABLE secrets (value TEXT)")
        connection.execute("INSERT INTO secrets VALUES ('hidden')")

    monkeypatch.setattr(cfg, "QUERY_RUN_DB_PATH", str(tmp_path / "query_runs.db"))
    monkeypatch.setattr(cfg, "REGISTRY_PATH", registry)
    monkeypatch.setattr(cfg, "DATABASE_URL", f"sqlite:///{database}")
    monkeypatch.setattr(cfg, "DATABASE_READONLY_CONFIRMED", True)
    monkeypatch.setattr(cfg, "EXECUTION_ENABLED", True)
    monkeypatch.setattr(cfg, "EXECUTION_MAX_ROWS", 2)
    monkeypatch.setattr(cfg, "EXECUTION_TIMEOUT_SECONDS", 5)
    monkeypatch.setattr(cfg, "QUERY_RUN_REVIEW_TTL_SECONDS", 900)
    monkeypatch.setattr(cfg, "DATASOURCE_ID", "demo")
    monkeypatch.setattr(cfg, "AUTH_ENABLED", False)
    monkeypatch.setattr(cfg, "AUTH_API_KEYS", [])
    monkeypatch.setattr(cfg, "ENFORCE_REVIEWER_API_KEYS", [])
    monkeypatch.setattr(executor, "_engine", None)
    return tmp_path


def _resource() -> dict:
    return {
        "schema_version": 1,
        "resource_type": "datasource",
        "resource_id": "demo",
        "organization_id": "org_demo",
        "workspace_id": "ws_demo",
        "parent_resource_id": "ws_demo",
        "resource_revision": None,
    }


def _principal() -> dict:
    now = datetime.now(timezone.utc)
    return {
        "schema_version": 1,
        "principal_context_id": "pc_explain_001",
        "actor_principal": {
            "principal_id": "human_1",
            "principal_type": "human",
        },
        "accountable_principal": {
            "principal_id": "human_1",
            "principal_type": "human",
        },
        "organization_id": "org_demo",
        "workspace_id": "ws_demo",
        "authentication_context": {
            "method": "local",
            "assurance_level": "single_factor",
            "authenticated_at": (now - timedelta(minutes=2)).isoformat(),
            "session_id_hash": None,
        },
        "delegation_chain": [],
        "issued_at": (now - timedelta(minutes=1)).isoformat(),
        "expires_at": (now + timedelta(minutes=15)).isoformat(),
    }


def _request(*, task_run_id: str, sql: str) -> dict:
    return {
        "schema_version": 1,
        "task_run_id": task_run_id,
        "purpose": "Read synthetic numbers for Explain verification",
        "question": "List synthetic numbers",
        "principal_context": _principal(),
        "delegated_mandate": None,
        "resource_scope": [_resource()],
        "candidate": {
            "kind": "direct_sql",
            "sql": sql,
            "producer_revision": "external-agent-r1",
        },
        "dialect": "sqlite",
    }


def _approval(review: dict) -> dict:
    return {
        "schema_version": 1,
        "approver_principal": {
            "principal_id": "human_1",
            "principal_type": "human",
        },
        "sql_hash": review["review"]["sql_hash"],
        "assurance_report_hash": review["review"]["assurance_report_hash"],
        "enforcement_context_hash": review["review"]["enforcement_context_hash"],
    }


async def _create(client, suffix: str, *, approve: bool = False) -> dict:
    review_response = await client.post(
        "/api/v1/enforce/query-runs",
        headers={"Idempotency-Key": f"create-explain-{suffix}"},
        json=_request(
            task_run_id=f"tr_explain_{suffix}",
            sql="SELECT numbers.n FROM numbers ORDER BY numbers.n",
        ),
    )
    assert review_response.status_code == 200
    review = review_response.json()
    if not approve:
        return review
    approved = await client.post(
        f"/api/v1/enforce/query-runs/{review['query_run_id']}/approve",
        headers={"Idempotency-Key": f"approve-explain-{suffix}"},
        json=_approval(review),
    )
    assert approved.status_code == 200
    return approved.json()


@pytest.mark.asyncio
async def test_explain_completed_run_projects_evidence_lineage_and_limits(
    client, explain_env
):
    completed = await _create(client, "complete", approve=True)

    response = await client.get(
        f"/api/v1/explain/query-runs/{completed['query_run_id']}"
    )

    assert response.status_code == 200
    body = response.json()
    validate_contract("explain_query_response_v1", body)
    assert body["status"] == "completed"
    assert body["statement"]["actual_sql"] == (
        "SELECT numbers.n FROM numbers ORDER BY numbers.n"
    )
    assert body["statement"]["result"]["rows"] == [[1], [2]]
    assert body["statement"]["result"]["truncated"] is True
    assert body["sources"]["physical_tables"] == ["numbers"]
    assert body["semantics"]["bindings"] == [
        {
            "binding_type": "table",
            "identifier": "numbers",
            "description": "Synthetic numbers",
            "registry_revision": body["sources"]["registry_revision"],
        },
        {
            "binding_type": "column",
            "identifier": "numbers.n",
            "description": "Synthetic integer",
            "registry_revision": body["sources"]["registry_revision"],
        },
    ]
    assert body["integrity"]["status"] == "verified"
    assert set(body["integrity"]["verified_hashes"]) == {
        "candidate",
        "sql",
        "assurance",
        "policy",
        "enforcement_context",
        "source_context",
        "approval",
        "result",
    }
    assert {item["evidence_type"] for item in body["evidence"]} == {
        "candidate",
        "query",
        "assurance",
        "policy",
        "source",
        "approval",
        "result",
    }
    assert {item["code"] for item in body["limitations"]} == {
        "semantic_correctness_unproven",
        "live_data_no_snapshot",
        "result_truncated",
    }


@pytest.mark.asyncio
async def test_explain_review_required_is_verified_and_explicitly_incomplete(
    client, explain_env
):
    review = await _create(client, "review")

    response = await client.get(f"/api/v1/explain/query-runs/{review['query_run_id']}")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "review_required"
    assert body["statement"]["result"] is None
    assert body["governance"]["approval"] is None
    assert body["integrity"] == {
        "status": "verified",
        "explanation_hash": body["integrity"]["explanation_hash"],
        "verified_hashes": [
            "candidate",
            "policy",
            "enforcement_context",
            "sql",
            "assurance",
            "source_context",
        ],
        "unverified_components": [],
    }
    assert "execution_not_completed" in {
        item["code"] for item in body["limitations"]
    }


@pytest.mark.asyncio
async def test_explain_denied_run_preserves_bounded_failure_and_candidate(
    client, explain_env
):
    denied_response = await client.post(
        "/api/v1/enforce/query-runs",
        headers={"Idempotency-Key": "create-explain-denied"},
        json=_request(
            task_run_id="tr_explain_denied",
            sql="DELETE FROM numbers",
        ),
    )
    assert denied_response.status_code == 200
    denied = denied_response.json()

    response = await client.get(
        f"/api/v1/explain/query-runs/{denied['query_run_id']}"
    )

    assert response.status_code == 200
    body = response.json()
    validate_contract("explain_query_response_v1", body)
    assert body["status"] == "denied"
    assert body["statement"]["actual_sql"] is None
    assert body["statement"]["result"] is None
    assert body["statement"]["failure"]["code"] == "readonly_violation"
    assert body["semantics"]["candidate"]["sql"] == "DELETE FROM numbers"
    assert body["integrity"]["status"] == "verified"
    assert {item["code"] for item in body["limitations"]} >= {
        "semantic_correctness_unproven",
        "live_data_no_snapshot",
        "semantic_binding_partial",
        "execution_not_completed",
    }
@pytest.mark.asyncio
async def test_explain_uses_persisted_source_context_after_registry_changes(
    client, explain_env
):
    completed = await _create(client, "registry-history", approve=True)
    registry = explain_env / "schema.registry.json"
    registry.write_text(
        json.dumps(
            {
                "tables": {
                    "numbers": {
                        "description": "Changed current description",
                        "columns": {"n": {"description": "Changed current column"}},
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    response = await client.get(
        f"/api/v1/explain/query-runs/{completed['query_run_id']}"
    )

    assert response.status_code == 200
    descriptions = {
        item["identifier"]: item["description"]
        for item in response.json()["semantics"]["bindings"]
    }
    assert descriptions == {
        "numbers": "Synthetic numbers",
        "numbers.n": "Synthetic integer",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("assignment", "expected_code"),
    [
        ("explain_context = '{}'", "source_context_drift"),
        ("result_rows = '[[99]]'", "result_evidence_drift"),
        ("approver_user_id = 'tampered'", "approval_evidence_drift"),
    ],
)
async def test_explain_fails_closed_on_persisted_evidence_drift(
    client, explain_env, assignment, expected_code
):
    completed = await _create(client, expected_code, approve=True)
    with sqlite3.connect(explain_env / "query_runs.db") as connection:
        connection.execute(
            f"UPDATE query_runs SET {assignment} WHERE query_run_id = ?",
            (completed["query_run_id"],),
        )

    response = await client.get(
        f"/api/v1/explain/query-runs/{completed['query_run_id']}"
    )

    assert response.status_code == 409
    assert response.json() == {
        "schema_version": 1,
        "status": "error",
        "failure": {
            "stage": "explain",
            "code": expected_code,
            "retryable": False,
        },
    }


@pytest.mark.asyncio
async def test_explain_marks_legacy_unanchored_components_partial(client, explain_env):
    completed = await _create(client, "legacy", approve=True)
    with sqlite3.connect(explain_env / "query_runs.db") as connection:
        connection.execute(
            """
            UPDATE query_runs
            SET explain_context = NULL, explain_context_hash = NULL,
                approval_hash = NULL, result_hash = NULL
            WHERE query_run_id = ?
            """,
            (completed["query_run_id"],),
        )

    response = await client.get(
        f"/api/v1/explain/query-runs/{completed['query_run_id']}"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["integrity"]["status"] == "partial"
    assert body["integrity"]["unverified_components"] == [
        "source_context",
        "approval",
        "result",
    ]
    assert {
        "source_context_unanchored",
        "approval_unanchored",
        "result_unanchored",
    } <= {item["code"] for item in body["limitations"]}


@pytest.mark.asyncio
async def test_explain_is_bound_to_creator_credential(client, explain_env, monkeypatch):
    from config import cfg

    monkeypatch.setattr(cfg, "AUTH_ENABLED", True)
    monkeypatch.setattr(cfg, "AUTH_API_KEYS", ["creator-key"])
    monkeypatch.setattr(cfg, "ENFORCE_REVIEWER_API_KEYS", ["reviewer-key"])
    review_response = await client.post(
        "/api/v1/enforce/query-runs",
        headers={
            "X-API-Key": "creator-key",
            "Idempotency-Key": "create-explain-credential",
        },
        json=_request(
            task_run_id="tr_explain_credential",
            sql="SELECT numbers.n FROM numbers",
        ),
    )
    review = review_response.json()

    creator = await client.get(
        f"/api/v1/explain/query-runs/{review['query_run_id']}",
        headers={"X-API-Key": "creator-key"},
    )
    reviewer = await client.get(
        f"/api/v1/explain/query-runs/{review['query_run_id']}",
        headers={"X-API-Key": "reviewer-key"},
    )

    assert creator.status_code == 200
    assert reviewer.status_code == 403
    assert reviewer.json()["failure"]["code"] == "query_run_access_denied"


def test_explain_cli_reads_public_projection(monkeypatch, capsys):
    response = httpx.Response(
        200,
        json={"schema_version": 1, "status": "review_required"},
        request=httpx.Request("GET", "http://forge.test"),
    )
    captured: dict = {}

    def fake_get(url, *, headers, timeout):
        captured.update({"url": url, "headers": headers, "timeout": timeout})
        return response

    monkeypatch.setattr(httpx, "get", fake_get)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "forge",
            "explain",
            "qr_" + "a" * 32,
            "--url",
            "http://forge.test/",
            "--api-key",
            "creator-key",
        ],
    )

    cli_main()

    assert captured == {
        "url": "http://forge.test/api/v1/explain/query-runs/qr_" + "a" * 32,
        "headers": {"Accept": "application/json", "X-API-Key": "creator-key"},
        "timeout": 30.0,
    }
    assert json.loads(capsys.readouterr().out)["status"] == "review_required"
@pytest.mark.asyncio
async def test_dashboard_projects_recent_queryrun_without_copying_state(
    client, explain_env
):
    completed = await _create(client, "dashboard", approve=True)

    response = await client.get("/admin/dashboard")

    assert response.status_code == 200
    html = response.text
    assert "data-trust-runtime-path" in html
    assert "Evaluate → Enforce → Explain" in html
    assert "data-governed-run-list" in html
    assert completed["query_run_id"] in html
    assert "verified · 7 refs" in html
