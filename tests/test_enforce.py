"""Public Enforce API and CLI security contract tests."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import sys

import aiosqlite
import httpx
import pytest

from forge.cli import main as cli_main


@pytest.fixture
def enforce_env(tmp_path, monkeypatch):
    from config import cfg
    import forge.executor as executor

    registry = tmp_path / "schema.registry.json"
    registry.write_text(
        json.dumps(
            {
                "tables": {
                    "numbers": {"columns": {"n": {}}},
                    "secrets": {"columns": {"value": {}}},
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
    monkeypatch.setattr(cfg, "PI_SERVICE_API_KEYS", ["pi-service-secret"])
    monkeypatch.setattr(executor, "_engine", None)
    return tmp_path


def _resource(resource_type: str, resource_id: str, *, parent: str | None) -> dict:
    return {
        "schema_version": 1,
        "resource_type": resource_type,
        "resource_id": resource_id,
        "organization_id": "org_demo",
        "workspace_id": "ws_demo",
        "parent_resource_id": parent,
        "resource_revision": None,
    }


def _principal(*, actor_type: str = "human", actor_id: str = "human_1") -> dict:
    now = datetime.now(timezone.utc)
    accountable_id = actor_id if actor_type == "human" else "human_1"
    return {
        "schema_version": 1,
        "principal_context_id": "pc_enforce_001",
        "actor_principal": {
            "principal_id": actor_id,
            "principal_type": actor_type,
        },
        "accountable_principal": {
            "principal_id": accountable_id,
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


def _request(candidate: dict, **overrides) -> dict:
    payload = {
        "schema_version": 1,
        "task_run_id": "tr_enforce_001",
        "purpose": "Read synthetic numbers for a regression check",
        "question": "List synthetic numbers",
        "principal_context": _principal(),
        "delegated_mandate": None,
        "resource_scope": [_resource("datasource", "demo", parent="ws_demo")],
        "candidate": candidate,
        "dialect": "sqlite",
    }
    payload.update(overrides)
    return payload


def _approval(review: dict, principal_id: str = "human_1") -> dict:
    return {
        "schema_version": 1,
        "approver_principal": {
            "principal_id": principal_id,
            "principal_type": "human",
        },
        "sql_hash": review["review"]["sql_hash"],
        "assurance_report_hash": review["review"]["assurance_report_hash"],
        "enforcement_context_hash": review["review"]["enforcement_context_hash"],
    }


@pytest.mark.asyncio
async def test_public_enforce_prepares_review_without_execution(client, enforce_env):
    response = await client.post(
        "/api/v1/enforce/query-runs",
        headers={"Idempotency-Key": "create-enforce-001"},
        json=_request(
            {
                "kind": "direct_sql",
                "sql": "SELECT n FROM numbers ORDER BY n",
                "producer_revision": "external-agent-r1",
            }
        ),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "review_required"
    assert body["decision"] == {
        "verdict": "conditional",
        "review_required": True,
        "execution_authorized": False,
    }
    assert body["candidate"]["candidate_hash"].startswith("sha256:")
    assert body["policy_decision"]["effect"] == "conditional"
    assert {item["obligation_type"] for item in body["policy_decision"]["obligations"]} == {
        "approval",
        "read_only",
        "audit",
    }
    assert body["result"] is None
    assert body["failure"] is None
    assert body["review"]["enforcement_context_hash"].startswith("sha256:")


@pytest.mark.asyncio
async def test_public_enforce_denies_mutation_before_review(client, enforce_env):
    response = await client.post(
        "/api/v1/enforce/query-runs",
        headers={"Idempotency-Key": "create-enforce-mutation"},
        json=_request({"kind": "direct_sql", "sql": "DELETE FROM numbers"}),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "denied"
    assert body["decision"]["verdict"] == "deny"
    assert body["decision"]["execution_authorized"] is False
    assert body["policy_decision"]["effect"] == "deny"
    assert body["failure"]["code"] == "readonly_violation"
    assert body["result"] is None


@pytest.mark.asyncio
async def test_public_enforce_approval_executes_once_with_bounded_rows(client, enforce_env):
    review = (
        await client.post(
            "/api/v1/enforce/query-runs",
            headers={"Idempotency-Key": "create-enforce-execute"},
            json=_request({"kind": "direct_sql", "sql": "SELECT n FROM numbers ORDER BY n"}),
        )
    ).json()
    headers = {"Idempotency-Key": "approve-enforce-execute"}
    endpoint = f"/api/v1/enforce/query-runs/{review['query_run_id']}/approve"

    first = await client.post(endpoint, headers=headers, json=_approval(review))
    replay = await client.post(endpoint, headers=headers, json=_approval(review))

    assert first.status_code == 200
    assert replay.status_code == 200
    body = first.json()
    assert replay.json() == body
    assert body["status"] == "completed"
    assert body["decision"]["execution_authorized"] is True
    assert body["result"]["columns"] == ["n"]
    assert body["result"]["rows"] == [[1], [2]]
    assert body["result"]["row_count"] == 2
    assert body["result"]["truncated"] is True


@pytest.mark.asyncio
async def test_public_enforce_rejects_wrong_context_hash(client, enforce_env):
    review = (
        await client.post(
            "/api/v1/enforce/query-runs",
            headers={"Idempotency-Key": "create-enforce-bad-hash"},
            json=_request({"kind": "direct_sql", "sql": "SELECT n FROM numbers"}),
        )
    ).json()
    approval = _approval(review)
    approval["enforcement_context_hash"] = "sha256:" + "0" * 64

    response = await client.post(
        f"/api/v1/enforce/query-runs/{review['query_run_id']}/approve",
        headers={"Idempotency-Key": "approve-enforce-bad-hash"},
        json=approval,
    )

    assert response.status_code == 409
    assert response.json()["failure"]["code"] == "enforcement_context_hash_mismatch"


@pytest.mark.asyncio
async def test_public_enforce_rejects_registry_drift(client, enforce_env, monkeypatch):
    review = (
        await client.post(
            "/api/v1/enforce/query-runs",
            headers={"Idempotency-Key": "create-enforce-registry-drift"},
            json=_request({"kind": "direct_sql", "sql": "SELECT n FROM numbers"}),
        )
    ).json()
    import forge.query_runs as query_runs

    monkeypatch.setattr(query_runs, "current_registry_version", lambda: "sha256:changed")
    response = await client.post(
        f"/api/v1/enforce/query-runs/{review['query_run_id']}/approve",
        headers={"Idempotency-Key": "approve-enforce-registry-drift"},
        json=_approval(review),
    )

    assert response.status_code == 409
    assert response.json()["failure"]["code"] == "registry_drift"

@pytest.mark.asyncio
async def test_public_enforce_rejects_persisted_policy_drift(client, enforce_env):
    from config import cfg

    review = (
        await client.post(
            "/api/v1/enforce/query-runs",
            headers={"Idempotency-Key": "create-enforce-policy-drift"},
            json=_request({"kind": "direct_sql", "sql": "SELECT n FROM numbers"}),
        )
    ).json()
    async with aiosqlite.connect(cfg.QUERY_RUN_DB_PATH) as database:
        cursor = await database.execute(
            "SELECT policy_decision FROM query_runs WHERE query_run_id = ?",
            (review["query_run_id"],),
        )
        policy = json.loads((await cursor.fetchone())[0])
        policy["reason"] = "Tampered after review"
        await database.execute(
            "UPDATE query_runs SET policy_decision = ? WHERE query_run_id = ?",
            (json.dumps(policy), review["query_run_id"]),
        )
        await database.commit()

    response = await client.post(
        f"/api/v1/enforce/query-runs/{review['query_run_id']}/approve",
        headers={"Idempotency-Key": "approve-enforce-policy-drift"},
        json=_approval(review),
    )

    assert response.status_code == 400
    assert response.json()["failure"]["code"] == "policy_context_drift"


@pytest.mark.asyncio
async def test_public_enforce_requires_agent_delegation(client, enforce_env):
    principal = _principal(actor_type="agent", actor_id="agent_1")
    response = await client.post(
        "/api/v1/enforce/query-runs",
        headers={"Idempotency-Key": "create-enforce-no-mandate"},
        json=_request(
            {"kind": "direct_sql", "sql": "SELECT n FROM numbers"},
            principal_context=principal,
        ),
    )

    assert response.status_code == 400
    assert response.json()["failure"]["code"] == "delegation_required"


@pytest.mark.asyncio
async def test_public_enforce_accepts_active_agent_mandate(client, enforce_env):
    scope = [_resource("datasource", "demo", parent="ws_demo")]
    principal = _principal(actor_type="agent", actor_id="agent_1")
    principal["delegation_chain"] = [
        {
            "delegation_id": "dlg_enforce_001",
            "delegator_principal_id": "human_1",
            "delegate_principal_id": "agent_1",
            "mandate_id": "md_enforce_001",
            "issued_at": principal["issued_at"],
            "expires_at": principal["expires_at"],
        }
    ]
    mandate = {
        "schema_version": 1,
        "mandate_id": "md_enforce_001",
        "revision": 1,
        "delegate_principal": {"principal_id": "agent_1", "principal_type": "agent"},
        "delegator_principal": {"principal_id": "human_1", "principal_type": "human"},
        "accountable_principal": {"principal_id": "human_1", "principal_type": "human"},
        "organization_id": "org_demo",
        "workspace_id": "ws_demo",
        "purpose": "Read synthetic numbers for a regression check",
        "task_run_id": "tr_enforce_001",
        "audience": "forge",
        "capabilities": ["query.prepare", "query.execute"],
        "resource_scope": scope,
        "budget_ref": None,
        "approval_policy_ref": "policy_query_review_v1",
        "can_delegate": False,
        "status": "active",
        "issued_at": principal["issued_at"],
        "expires_at": principal["expires_at"],
    }
    response = await client.post(
        "/api/v1/enforce/query-runs",
        headers={"Idempotency-Key": "create-enforce-agent"},
        json=_request(
            {"kind": "direct_sql", "sql": "SELECT n FROM numbers"},
            principal_context=principal,
            delegated_mandate=mandate,
            resource_scope=scope,
        ),
    )

    assert response.status_code == 200
    assert response.json()["status"] == "review_required"
    assert response.json()["policy_decision"]["mandate_id"] == "md_enforce_001"

@pytest.mark.asyncio
async def test_public_enforce_applies_table_resource_scope(client, enforce_env):
    scoped = [
        _resource("datasource", "demo", parent="ws_demo"),
        _resource("table", "numbers", parent="demo"),
    ]
    response = await client.post(
        "/api/v1/enforce/query-runs",
        headers={"Idempotency-Key": "create-enforce-table-scope"},
        json=_request(
            {"kind": "direct_sql", "sql": "SELECT value FROM secrets"},
            resource_scope=scoped,
        ),
    )

    assert response.status_code == 200
    assert response.json()["status"] == "denied"
    assert response.json()["failure"]["code"] == "unknown_schema_reference"



@pytest.mark.asyncio
async def test_public_enforce_idempotency_rejects_context_change(client, enforce_env):
    headers = {"Idempotency-Key": "create-enforce-idempotency"}
    first = await client.post(
        "/api/v1/enforce/query-runs",
        headers=headers,
        json=_request({"kind": "direct_sql", "sql": "SELECT n FROM numbers"}),
    )
    second = await client.post(
        "/api/v1/enforce/query-runs",
        headers=headers,
        json=_request({"kind": "direct_sql", "sql": "SELECT value FROM secrets"}),
    )

    assert first.status_code == 200
    assert second.status_code == 409
    assert second.json()["failure"]["code"] == "idempotency_key_conflict"

@pytest.mark.asyncio
async def test_public_enforce_binds_authentication_and_separates_reviewer_key(
    client, enforce_env, monkeypatch
):
    from config import cfg

    monkeypatch.setattr(cfg, "AUTH_ENABLED", True)
    monkeypatch.setattr(cfg, "AUTH_API_KEYS", ["agent-key"])
    monkeypatch.setattr(cfg, "ENFORCE_REVIEWER_API_KEYS", ["reviewer-key"])
    review_response = await client.post(
        "/api/v1/enforce/query-runs",
        headers={"X-API-Key": "agent-key", "Idempotency-Key": "create-auth-bound"},
        json=_request({"kind": "direct_sql", "sql": "SELECT n FROM numbers"}),
    )
    review = review_response.json()

    assert review_response.status_code == 200
    authentication = review["context"]["principal_context"]["authentication_context"]
    assert authentication["method"] == "service_key"
    assert authentication["assurance_level"] == "service_asserted"
    assert authentication["session_id_hash"].startswith("sha256:")

    run_endpoint = f"/api/v1/enforce/query-runs/{review['query_run_id']}"
    owned = await client.get(run_endpoint, headers={"X-API-Key": "agent-key"})
    foreign = await client.get(run_endpoint, headers={"X-API-Key": "reviewer-key"})
    assert owned.status_code == 200
    assert foreign.status_code == 403
    assert foreign.json()["failure"]["code"] == "query_run_access_denied"

    endpoint = f"/api/v1/enforce/query-runs/{review['query_run_id']}/approve"
    denied = await client.post(
        endpoint,
        headers={"X-API-Key": "agent-key", "Idempotency-Key": "approve-agent-key"},
        json=_approval(review),
    )
    approved = await client.post(
        endpoint,
        headers={"X-API-Key": "reviewer-key", "Idempotency-Key": "approve-reviewer-key"},
        json=_approval(review),
    )

    assert denied.status_code == 403
    assert approved.status_code == 200
    assert approved.json()["status"] == "completed"


@pytest.mark.asyncio
async def test_public_enforce_get_rejects_internal_query_run(client, enforce_env):
    internal = await client.post(
        "/api/internal/query-runs",
        headers={
            "X-Pi-Service-Key": "pi-service-secret",
            "Idempotency-Key": "create-internal-only",
        },
        json={
            "task_run_id": "tr_internal_only",
            "org_id": "org_demo",
            "team_id": "team_demo",
            "user_id": "human_1",
            "question": "Internal only",
            "dialect": "sqlite",
            "candidate": {"kind": "direct_sql", "sql": "SELECT n FROM numbers"},
        },
    )

    response = await client.get(
        f"/api/v1/enforce/query-runs/{internal.json()['query_run_id']}"
    )
    assert response.status_code == 404
    assert response.json()["failure"]["code"] == "enforcement_context_missing"

def test_enforce_cli_routes_create_get_and_approve(tmp_path: Path, monkeypatch, capsys):
    request_path = tmp_path / "request.json"
    approval_path = tmp_path / "approval.json"
    request_path.write_text(json.dumps({"schema_version": 1}), encoding="utf-8")
    approval_path.write_text(json.dumps({"schema_version": 1}), encoding="utf-8")
    calls: list[dict] = []

    def fake_post(url, *, json, headers, timeout):
        calls.append({"method": "POST", "url": url, "json": json, "headers": headers})
        return httpx.Response(200, json={"status": "review_required"})

    def fake_get(url, *, headers, timeout):
        calls.append({"method": "GET", "url": url, "headers": headers})
        return httpx.Response(200, json={"status": "review_required"})

    monkeypatch.setattr(httpx, "post", fake_post)
    monkeypatch.setattr(httpx, "get", fake_get)

    monkeypatch.setattr(
        sys,
        "argv",
        ["forge", "enforce", str(request_path), "--idempotency-key", "create-1"],
    )
    cli_main()
    capsys.readouterr()

    monkeypatch.setattr(sys, "argv", ["forge", "enforce", "--run-id", "qr_demo"])
    cli_main()
    capsys.readouterr()

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "forge",
            "enforce",
            str(approval_path),
            "--approve",
            "qr_demo",
            "--idempotency-key",
            "approve-1",
        ],
    )
    cli_main()

    assert [call["method"] for call in calls] == ["POST", "GET", "POST"]
    assert calls[0]["url"].endswith("/api/v1/enforce/query-runs")
    assert calls[0]["headers"]["Idempotency-Key"] == "create-1"
    assert calls[1]["url"].endswith("/api/v1/enforce/query-runs/qr_demo")
    assert calls[2]["url"].endswith("/api/v1/enforce/query-runs/qr_demo/approve")
    assert calls[2]["headers"]["Idempotency-Key"] == "approve-1"


def test_enforce_cli_fails_on_denied_candidate(tmp_path: Path, monkeypatch, capsys):
    request_path = tmp_path / "request.json"
    request_path.write_text(json.dumps({"schema_version": 1}), encoding="utf-8")
    monkeypatch.setattr(
        httpx,
        "post",
        lambda *args, **kwargs: httpx.Response(200, json={"status": "denied"}),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["forge", "enforce", str(request_path), "--idempotency-key", "denied-1"],
    )

    with pytest.raises(SystemExit) as caught:
        cli_main()

    assert caught.value.code == 1
    assert json.loads(capsys.readouterr().out)["status"] == "denied"
