"""Evidence projection and authenticated Explain access for governed QueryRuns."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import sqlite3
import sys

import httpx
import pytest

from agent.contracts import validate_contract
from agent.tenant import TenantStore
from forge.cli import main as cli_main
from forge.enforce import build_enforcement_context_hash
from forge.governance_credentials import authenticate_credential, create_credential
from forge.governance_mandates import issue_mandate, revoke_mandate
from forge.query_runs import create_governed_query_run, execute_authorized_query_run, get_query_run


@pytest.fixture
def explain_env(tmp_path, monkeypatch):
    from config import cfg
    import forge.executor as executor

    registry = tmp_path / "registry.json"
    registry.write_text(json.dumps({"tables": {
        "numbers": {"columns": {"n": {"type": "integer"}}},
        "secrets": {"columns": {"value": {"type": "text"}}},
    }}), encoding="utf-8")
    database = tmp_path / "data.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE numbers (n INTEGER)")
        connection.executemany("INSERT INTO numbers VALUES (?)", [(1,), (2,), (3,)])
        connection.execute("CREATE TABLE secrets (value TEXT)")
        connection.execute("INSERT INTO secrets VALUES ('private')")
    monkeypatch.setattr(cfg, "QUERY_RUN_DB_PATH", str(tmp_path / "runs.db"))
    monkeypatch.setattr(cfg, "REGISTRY_PATH", registry)
    monkeypatch.setattr(cfg, "DATABASE_URL", f"sqlite:///{database}")
    monkeypatch.setattr(cfg, "DATABASE_READONLY_CONFIRMED", True)
    monkeypatch.setattr(cfg, "EXECUTION_ENABLED", True)
    monkeypatch.setattr(cfg, "EXECUTION_MAX_ROWS", 2)
    monkeypatch.setattr(cfg, "EXECUTION_TIMEOUT_SECONDS", 5)
    monkeypatch.setattr(cfg, "DATASOURCE_ID", "demo")
    monkeypatch.setattr(cfg, "AUTH_ENABLED", True)
    monkeypatch.setattr(executor, "_engine", None)
    store = TenantStore()
    store.create_team("workspace", organization_id="org")
    store.set_allowed_tables("workspace", ["numbers", "secrets"])
    for name, kind, role in (("admin", "human", "admin"), ("reviewer", "human", "reviewer"),
                             ("agent", "agent", "member"), ("other", "agent", "member")):
        store.set_team(name, "workspace", role=role, principal_type=kind, status="active")
    tokens = {name: create_credential(name)[1] for name in ("admin", "reviewer", "agent", "other")}
    actors = {name: authenticate_credential(token) for name, token in tokens.items()}
    mandate = issue_mandate(actor=actors["admin"], request={
        "schema_version": 1, "task_run_id": "tr_evidence", "delegate_principal_id": "agent",
        "accountable_principal_id": "admin", "reviewer_principal_id": "reviewer",
        "purpose": "Read synthetic data", "datasource_id": "demo",
        "allowed_tables": ["numbers", "secrets"], "autonomous_tables": ["numbers"],
        "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
    }, idempotency_key="issue-evidence")
    return {"tokens": tokens, "actors": actors, "mandate": mandate, "path": tmp_path}


async def _prepare(env, sql, key):
    return await create_governed_query_run(
        actor=env["actors"]["agent"], mandate_id=env["mandate"]["mandate_id"],
        task_run_id="tr_evidence", purpose="Read synthetic data", question="List values",
        dialect="sqlite", candidate={"kind": "direct_sql", "sql": sql,
                                      "producer_revision": "external-agent-r1"},
        idempotency_key=key,
    )


def _hashes(run):
    return {field: run[field] for field in (
        "sql_hash", "assurance_report_hash", "enforcement_context_hash",
    )}


async def _finished(run_id):
    for _ in range(100):
        run = await get_query_run(run_id)
        if run["status"] != "executing":
            return run
        await asyncio.sleep(.01)
    pytest.fail("governed SQL did not reach a terminal state")


@pytest.mark.asyncio
async def test_autonomous_explain_records_mandate_and_actual_claim(client, explain_env):
    env = explain_env
    prepared = await _prepare(env, "SELECT n FROM numbers ORDER BY n", "autonomous")
    assert prepared["status"] == "ready"
    response = await client.get(f"/api/v2/explain/query-runs/{prepared['query_run_id']}",
                                headers={"X-API-Key": env["tokens"]["agent"]})
    assert response.status_code == 200, response.text
    assert response.json()["governance"]["authorization"] is None
    await execute_authorized_query_run(query_run_id=prepared["query_run_id"],
                                       actor=env["actors"]["agent"], action="execute",
                                       hashes=_hashes(prepared), idempotency_key="execute-evidence")
    finished = await _finished(prepared["query_run_id"])
    assert finished["status"] == "completed", finished.get("error")
    response = await client.get(f"/api/v2/explain/query-runs/{prepared['query_run_id']}",
                                headers={"X-API-Key": env["tokens"]["agent"]})
    assert response.status_code == 200, response.text
    body = response.json()
    validate_contract("explain_query_response_v2", body)
    assert body["integrity"]["status"] == "verified"
    assert {"mandate", "authorization"} <= set(body["integrity"]["verified_hashes"])
    assert body["governance"]["approval"] is None
    assert body["governance"]["issuer_principal"]["principal_id"] == "admin"
    assert body["governance"]["authorization"]["authorizing_principal_id"] == "agent"
    assert body["lineage"]["authorization_hash"] == finished["authorization_hash"]
    assert {"mandate", "authorization"} <= {e["evidence_type"] for e in body["evidence"]}
    assert {"authorization_is_not_semantic_correctness", "semantic_correctness_unproven",
            "live_data_no_snapshot"} <= {limit["code"] for limit in body["limitations"]}


@pytest.mark.asyncio
async def test_human_review_explain_names_the_designated_reviewer(client, explain_env):
    env = explain_env
    prepared = await _prepare(env, "SELECT value FROM secrets", "human-review")
    assert prepared["status"] == "review_required"
    await execute_authorized_query_run(query_run_id=prepared["query_run_id"],
                                       actor=env["actors"]["reviewer"], action="approve",
                                       hashes=_hashes(prepared), idempotency_key="approve-evidence")
    finished = await _finished(prepared["query_run_id"])
    assert finished["status"] == "completed", finished.get("error")
    response = await client.get(f"/api/v2/explain/query-runs/{prepared['query_run_id']}",
                                headers={"X-API-Key": env["tokens"]["reviewer"]})
    assert response.status_code == 200, response.text
    evidence = response.json()
    assert evidence["integrity"]["status"] == "verified"
    assert evidence["governance"]["approval"]["approver_principal"]["principal_id"] == "reviewer"
    assert evidence["governance"]["authorization"]["actor_principal_id"] == "agent"
    assert evidence["governance"]["authorization"]["authorizing_principal_id"] == "reviewer"
    assert {"approval", "mandate", "authorization"} <= {item["evidence_type"] for item in evidence["evidence"]}

@pytest.mark.asyncio
async def test_historical_hash_stable_after_revocation_and_access_scoped(client, explain_env):
    env = explain_env
    prepared = await _prepare(env, "SELECT n FROM numbers", "revocable")
    path = f"/api/v2/explain/query-runs/{prepared['query_run_id']}"
    headers = {"X-API-Key": env["tokens"]["admin"]}
    before = await client.get(path, headers=headers)
    assert before.status_code == 200, before.text
    revoked = revoke_mandate(actor=env["actors"]["admin"],
                             mandate_id=env["mandate"]["mandate_id"],
                             expected_state_revision=1, reason="synthetic test")
    assert revoked["status"] == "revoked"
    after = await client.get(path, headers=headers)
    assert after.status_code == 200, after.text
    assert after.json()["integrity"]["explanation_hash"] == before.json()["integrity"]["explanation_hash"]
    assert after.json()["governance"]["current_authorization"]["status"] == "revoked"
    assert (await client.get(path, headers={"X-API-Key": env["tokens"]["agent"]})).status_code == 404
    assert (await client.get(path, headers={"X-API-Key": env["tokens"]["other"]})).status_code == 404
    assert (await client.get("/api/v1/explain/query-runs/" + prepared["query_run_id"], headers=headers)).status_code == 404
    assert (await client.get(path, headers={"X-API-Key": "unbound-old-key"})).status_code == 401
    denied = await client.get("/api/v1/explain/query-runs/" + prepared["query_run_id"],
                              headers={"X-API-Key": env["tokens"]["agent"]})
    assert denied.status_code == 403


@pytest.mark.asyncio
async def test_explain_store_unavailable_is_not_reported_as_missing(client, explain_env, monkeypatch):
    import web.routes.explain as route

    async def unavailable(_query_run_id):
        raise sqlite3.OperationalError("synthetic database outage")

    monkeypatch.setattr(route, "get_query_run", unavailable)
    response = await client.get("/api/v2/explain/query-runs/qr_" + "0" * 32,
                                headers={"X-API-Key": explain_env["tokens"]["admin"]})
    assert response.status_code == 503
    assert response.json()["failure"]["code"] == "governance_unavailable"


@pytest.mark.asyncio
async def test_historical_v1_is_admin_only_and_still_checks_original_hashes(client, explain_env):
    from forge.governance import connect_governance

    env = explain_env
    prepared = await _prepare(env, "SELECT value FROM secrets", "historical")
    historical = {**prepared, "enforce_schema_version": 1, "status": "needs_review"}
    historical_hash = build_enforcement_context_hash(historical)
    with connect_governance() as connection:
        connection.execute("UPDATE query_runs SET enforce_schema_version=1,status='needs_review',"
                           "enforcement_context_hash=? WHERE query_run_id=?",
                           (historical_hash, prepared["query_run_id"]))
        connection.commit()
    path = "/api/v1/explain/query-runs/" + prepared["query_run_id"]
    response = await client.get(path, headers={"X-API-Key": env["tokens"]["admin"]})
    assert response.status_code == 200, response.text
    assert response.json()["schema_version"] == 1
    assert response.json()["integrity"]["status"] == "verified"
    assert (await client.get(path, headers={"X-API-Key": env["tokens"]["agent"]})).status_code == 403
    with connect_governance() as connection:
        connection.execute("UPDATE query_runs SET candidate_hash=? WHERE query_run_id=?",
                           ("sha256:" + "0" * 64, prepared["query_run_id"]))
        connection.commit()
    tampered = await client.get(path, headers={"X-API-Key": env["tokens"]["admin"]})
    assert tampered.status_code == 409
    assert tampered.json()["failure"]["code"] == "candidate_evidence_drift"

@pytest.mark.asyncio
@pytest.mark.parametrize("field,expected", [
    ("authorization_record", "authorization_evidence_drift"),
    ("mandate_issuance_hash", "enforcement_context_drift"),
    ("result_hash", "result_evidence_drift"),
])
async def test_explain_fails_closed_on_tampered_anchors(client, explain_env, field, expected):
    from forge.governance import connect_governance
    env = explain_env
    prepared = await _prepare(env, "SELECT n FROM numbers", field)
    await execute_authorized_query_run(query_run_id=prepared["query_run_id"],
                                       actor=env["actors"]["agent"], action="execute",
                                       hashes=_hashes(prepared), idempotency_key="execute-" + field)
    await _finished(prepared["query_run_id"])
    with connect_governance() as connection:
        if field == "authorization_record":
            connection.execute("UPDATE query_runs SET authorization_record=? WHERE query_run_id=?",
                               ('{}', prepared["query_run_id"]))
        else:
            connection.execute(f"UPDATE query_runs SET {field}=? WHERE query_run_id=?",
                               ("sha256:" + "0" * 64, prepared["query_run_id"]))
        connection.commit()
    response = await client.get(f"/api/v2/explain/query-runs/{prepared['query_run_id']}",
                                headers={"X-API-Key": env["tokens"]["admin"]})
    assert response.status_code == 409, response.text
    assert response.json()["failure"]["code"] == expected


def test_explain_cli_reads_v2_projection_without_literal_key(monkeypatch, capsys):
    response = httpx.Response(200, json={"schema_version": 2, "status": "review_required"},
                              request=httpx.Request("GET", "http://forge.test"))
    captured = {}

    def fake_get(url, *, headers, timeout):
        captured.update(url=url, headers=headers, timeout=timeout)
        return response

    monkeypatch.setattr(httpx, "get", fake_get)
    monkeypatch.setenv("EXPLAIN_TEST_KEY", "synthetic-key")
    monkeypatch.setattr(sys, "argv", ["forge", "explain", "qr_" + "a" * 32,
                                  "--url", "http://forge.test/", "--api-key-env", "EXPLAIN_TEST_KEY"])
    cli_main()
    assert captured["url"] == "http://forge.test/api/v2/explain/query-runs/qr_" + "a" * 32
    assert captured["headers"] == {"Accept": "application/json", "X-API-Key": "synthetic-key"}
    assert json.loads(capsys.readouterr().out)["status"] == "review_required"
