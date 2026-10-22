"""HTTP contract and authorization boundaries for governed Enforce."""
from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture
def governed_api(tmp_path, monkeypatch):
    from config import cfg
    from agent.tenant import tenants
    from forge.governance_credentials import authenticate_credential, create_credential
    from forge.governance_mandates import issue_mandate
    import forge.executor as executor

    registry = tmp_path / "schema.registry.json"
    registry.write_text(json.dumps({"tables": {
        "numbers": {"columns": {"n": {}}}, "secrets": {"columns": {"value": {}}},
    }}))
    database = tmp_path / "data.db"
    with sqlite3.connect(database) as conn:
        conn.execute("CREATE TABLE numbers (n INTEGER)")
        conn.executemany("INSERT INTO numbers VALUES (?)", [(1,), (2,), (3,)])
        conn.execute("CREATE TABLE secrets (value TEXT)")
        conn.execute("INSERT INTO secrets VALUES ('hidden')")
    for key, value in {
        "QUERY_RUN_DB_PATH": str(tmp_path / "query_runs.db"), "REGISTRY_PATH": registry,
        "DATABASE_URL": f"sqlite:///{database}", "DATABASE_READONLY_CONFIRMED": True,
        "EXECUTION_ENABLED": True, "EXECUTION_MAX_ROWS": 2, "EXECUTION_TIMEOUT_SECONDS": 5,
        "QUERY_RUN_REVIEW_TTL_SECONDS": 900, "DATASOURCE_ID": "demo", "AUTH_ENABLED": True,
    }.items():
        monkeypatch.setattr(cfg, key, value)
    monkeypatch.setattr(executor, "_engine", None)
    tenants.create_team("ws_api", organization_id="org_api")
    tenants.set_allowed_tables("ws_api", ["numbers", "secrets"])
    for principal_id, principal_type, role in (
        ("issuer_api", "human", "admin"), ("reviewer_api", "human", "reviewer"),
        ("other_api", "human", "member"), ("worker_api", "agent", "member"),
    ):
        tenants.set_team(principal_id, "ws_api", principal_type=principal_type,
                         role=role, status="active")
    keys = {name: create_credential(name)[1] for name in
            ("issuer_api", "reviewer_api", "other_api", "worker_api")}
    issuer = authenticate_credential(keys["issuer_api"])
    mandate = issue_mandate(actor=issuer, idempotency_key="api-issue", request={
        "schema_version": 1, "task_run_id": "tr_api", "delegate_principal_id": "worker_api",
        "accountable_principal_id": "issuer_api", "reviewer_principal_id": "reviewer_api",
        "purpose": "Synthetic reporting", "datasource_id": "demo",
        "allowed_tables": ["numbers", "secrets"], "autonomous_tables": ["numbers"],
        "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
    })
    return keys, mandate["mandate_id"]


def _headers(keys, name, key):
    return {"X-API-Key": keys[name], "Idempotency-Key": key}


def _request(mandate_id, sql):
    return {"schema_version": 2, "mandate_id": mandate_id, "task_run_id": "tr_api",
            "purpose": "Synthetic reporting", "question": "Synthetic reporting",
            "dialect": "sqlite", "candidate": {"kind": "direct_sql", "sql": sql}}


def _action(review):
    return {"schema_version": 2, **{name: review[name] for name in
            ("sql_hash", "assurance_report_hash", "enforcement_context_hash")}}


@pytest.mark.asyncio
async def test_governed_http_autonomous_claim_and_scoped_replay(client, governed_api):
    keys, mandate_id = governed_api
    root = "/api/v2/enforce/query-runs"
    prepared = await client.post(root, headers=_headers(keys, "worker_api", "create-number"),
                                 json=_request(mandate_id, "SELECT n FROM numbers ORDER BY n"))
    assert prepared.status_code == 200, prepared.text
    body = prepared.json()
    assert body["status"] == "ready"
    assert body["authorization"] is None
    assert body["decision"]["execution_authorized"] is False
    run_id = body["query_run_id"]
    assert (await client.get(f"{root}/{run_id}", headers={"X-API-Key": keys["other_api"]})).status_code == 404
    action = _action(body["review"])
    assert (await client.post(f"{root}/{run_id}/execute", json={**action, "approver_principal": "issuer_api"},
                              headers=_headers(keys, "worker_api", "execute-number"))).status_code == 422
    started = await client.post(f"{root}/{run_id}/execute", json=action,
                                headers=_headers(keys, "worker_api", "execute-number"))
    assert started.status_code in {200, 202}, started.text
    assert started.json()["authorization"]["kind"] == "mandate"
    for _ in range(100):
        finished = await client.get(f"{root}/{run_id}", headers={"X-API-Key": keys["worker_api"]})
        if finished.json()["status"] == "completed":
            break
        await asyncio.sleep(.02)
    assert finished.json()["status"] == "completed", finished.text
    assert finished.json()["result"]["rows"] == [[1], [2]]
    assert finished.json()["result"]["truncated"] is True
    replay = await client.post(f"{root}/{run_id}/execute", json=action,
                               headers=_headers(keys, "worker_api", "execute-number"))
    assert replay.status_code == 200
    assert replay.json()["authorization"] == finished.json()["authorization"]
    assert (await client.post(f"{root}/{run_id}/cancel",
                              headers={"X-API-Key": keys["worker_api"]})).status_code == 409


@pytest.mark.asyncio
async def test_governed_http_reviewer_and_legacy_closure(client, governed_api):
    keys, mandate_id = governed_api
    root = "/api/v2/enforce/query-runs"
    prepared = await client.post(root, headers=_headers(keys, "worker_api", "create-secret"),
                                 json=_request(mandate_id, "SELECT value FROM secrets"))
    assert prepared.status_code == 200, prepared.text
    body = prepared.json()
    assert body["status"] == "review_required"
    run_id = body["query_run_id"]
    action = _action(body["review"])
    for name in ("issuer_api", "other_api", "worker_api"):
        denied = await client.post(f"{root}/{run_id}/approve", json=action,
                                   headers=_headers(keys, name, "approval-" + name))
        assert denied.status_code == 403, denied.text
    approved = await client.post(f"{root}/{run_id}/approve", json=action,
                                 headers=_headers(keys, "reviewer_api", "approval-real"))
    assert approved.status_code in {200, 202}, approved.text
    assert approved.json()["authorization"]["reviewer_principal_id"] == "reviewer_api"
    assert (await client.post("/api/v1/enforce/query-runs", json=_request(mandate_id, "SELECT 1"),
                              headers=_headers(keys, "worker_api", "old"))).status_code == 410
    assert (await client.get(f"/api/v1/enforce/query-runs/{run_id}",
                             headers={"X-API-Key": keys["issuer_api"]})).status_code == 404
    assert (await client.post(root, json={**_request(mandate_id, "SELECT 1"), "principal_context": {}},
                              headers=_headers(keys, "worker_api", "self-report"))).status_code == 422


@pytest.mark.asyncio
async def test_governed_http_cancel_denial_and_auth_disabled(client, governed_api, monkeypatch):
    from config import cfg
    keys, mandate_id = governed_api
    root = "/api/v2/enforce/query-runs"
    denied = await client.post(root, headers=_headers(keys, "worker_api", "denied-write"),
                               json=_request(mandate_id, "DELETE FROM numbers"))
    assert denied.status_code == 200, denied.text
    assert denied.json()["status"] == "denied"
    assert denied.json()["decision"]["execution_mode"] is None
    prepared = await client.post(root, headers=_headers(keys, "worker_api", "cancel-secret"),
                                 json=_request(mandate_id, "SELECT value FROM secrets"))
    assert prepared.status_code == 200
    run_id = prepared.json()["query_run_id"]
    wrong = await client.post(f"{root}/{run_id}/cancel", headers={"X-API-Key": keys["other_api"]})
    assert wrong.status_code == 404
    cancelled = await client.post(f"{root}/{run_id}/cancel", headers={"X-API-Key": keys["reviewer_api"]})
    assert cancelled.status_code == 200, cancelled.text
    assert cancelled.json()["status"] == "cancelled"
    assert cancelled.json()["authorization"] is None
    monkeypatch.setattr(cfg, "AUTH_ENABLED", False)
    blocked = await client.post(root, headers=_headers(keys, "worker_api", "auth-off"),
                                json=_request(mandate_id, "SELECT n FROM numbers"))
    assert blocked.status_code == 401
    assert blocked.json()["detail"] == "authentication_required"

@pytest.mark.asyncio
async def test_reviewer_inbox_exposes_only_designated_pending_sql(client, governed_api):
    keys, mandate_id = governed_api
    root = "/api/v2/enforce/query-runs"
    prepared = await client.post(root, headers=_headers(keys, "worker_api", "pending-secret"),
                                 json=_request(mandate_id, "SELECT value FROM secrets"))
    assert prepared.status_code == 200 and prepared.json()["status"] == "review_required"
    inbox = await client.get("/api/v2/governance/reviews", headers={"X-API-Key": keys["reviewer_api"]})
    assert inbox.status_code == 200
    assert [item["query_run_id"] for item in inbox.json()["reviews"]] == [prepared.json()["query_run_id"]]
    assert inbox.json()["reviews"][0]["sql"] == "SELECT value FROM secrets"
    assert (await client.get("/api/v2/governance/reviews", headers={"X-API-Key": keys["issuer_api"]})).json() == {"reviews": []}
    assert (await client.get("/api/v2/governance/reviews", headers={"X-API-Key": keys["other_api"]})).status_code == 403
    assert (await client.get("/api/v2/governance/reviews", headers={"X-API-Key": keys["worker_api"]})).status_code == 403
