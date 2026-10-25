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


@pytest.mark.asyncio
@pytest.mark.parametrize("sql", [
    "SELECT '/*'; SELECT value FROM secrets; SELECT '*/' AS x",
    "SELECT n INTO copied_numbers FROM numbers",
])
async def test_governed_http_never_classifies_hidden_statements_as_autonomous(
        client, governed_api, sql):
    keys, mandate_id = governed_api
    prepared = await client.post("/api/v2/enforce/query-runs",
                                 headers=_headers(keys, "worker_api", "create-hidden"),
                                 json=_request(mandate_id, sql))
    assert prepared.status_code == 200, prepared.text
    body = prepared.json()
    assert body["status"] == "denied"
    assert body["decision"]["execution_mode"] is None
    assert body["failure"] == {"stage": "assurance", "code": "assurance_denied", "retryable": False}
    assert body["review"]["sql_hash"] is None


def _age_query_run(column: str, query_run_id: str) -> None:
    from config import cfg
    past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    with sqlite3.connect(cfg.QUERY_RUN_DB_PATH) as conn:
        conn.execute(f"UPDATE query_runs SET {column}=? WHERE query_run_id=?", (past, query_run_id))


@pytest.mark.asyncio
async def test_expired_review_is_persisted_and_leaves_the_reviewer_inbox(client, governed_api):
    keys, mandate_id = governed_api
    root = "/api/v2/enforce/query-runs"
    prepared = await client.post(root, headers=_headers(keys, "worker_api", "expiring-secret"),
                                 json=_request(mandate_id, "SELECT value FROM secrets"))
    assert prepared.json()["status"] == "review_required"
    run_id = prepared.json()["query_run_id"]
    _age_query_run("expires_at", run_id)
    inbox = await client.get("/api/v2/governance/reviews", headers={"X-API-Key": keys["reviewer_api"]})
    assert inbox.json()["reviews"] == []
    current = await client.get(f"{root}/{run_id}", headers={"X-API-Key": keys["worker_api"]})
    assert current.json()["status"] == "expired"
    assert current.json()["failure"] == {"stage": "approval", "code": "review_expired", "retryable": True}
    approved = await client.post(f"{root}/{run_id}/approve", json=_action(prepared.json()["review"]),
                                 headers=_headers(keys, "reviewer_api", "late-approval"))
    assert approved.status_code == 409
    assert approved.json()["failure"]["code"] == "review_expired"


@pytest.mark.asyncio
async def test_lost_worker_with_expired_lease_is_failed_on_read_without_replay(client, governed_api, monkeypatch):
    import forge.query_runs as query_runs

    async def lost_worker(query_run_id, claim_id):
        return None

    monkeypatch.setattr(query_runs, "_run_governed_worker", lost_worker)
    keys, mandate_id = governed_api
    root = "/api/v2/enforce/query-runs"
    prepared = await client.post(root, headers=_headers(keys, "worker_api", "lost-number"),
                                 json=_request(mandate_id, "SELECT n FROM numbers"))
    run_id = prepared.json()["query_run_id"]
    started = await client.post(f"{root}/{run_id}/execute", json=_action(prepared.json()["review"]),
                                headers=_headers(keys, "worker_api", "lost-execute"))
    assert started.json()["status"] == "executing"
    _age_query_run("execution_lease_expires_at", run_id)
    current = await client.get(f"{root}/{run_id}", headers={"X-API-Key": keys["worker_api"]})
    assert current.json()["status"] == "failed"
    assert current.json()["failure"] == {"stage": "execution", "code": "execution_failed", "retryable": True}
    assert current.json()["result"] is None


@pytest.mark.asyncio
async def test_reading_a_live_query_run_takes_no_write_lock(client, governed_api):
    from config import cfg
    from forge.query_runs import get_query_run, list_reviewer_query_runs
    from forge.governance_credentials import authenticate_credential

    keys, mandate_id = governed_api
    prepared = await client.post("/api/v2/enforce/query-runs", headers=_headers(keys, "worker_api", "locked-read"),
                                 json=_request(mandate_id, "SELECT value FROM secrets"))
    run_id = prepared.json()["query_run_id"]
    reviewer = authenticate_credential(keys["reviewer_api"])
    writer = sqlite3.connect(cfg.QUERY_RUN_DB_PATH, timeout=0)
    try:
        writer.execute("BEGIN IMMEDIATE")  # A governance transaction holding the write lock.
        run = await asyncio.wait_for(get_query_run(run_id), timeout=2)
        inbox = await asyncio.wait_for(list_reviewer_query_runs(reviewer), timeout=2)
    finally:
        writer.rollback()
        writer.close()
    assert run["status"] == "review_required"
    assert [item["query_run_id"] for item in inbox] == [run_id]


@pytest.mark.asyncio
async def test_creator_can_recover_a_run_by_its_create_key_and_no_one_else_can(client, governed_api):
    keys, mandate_id = governed_api
    root = "/api/v2/enforce/query-runs"
    prepared = await client.post(root, headers=_headers(keys, "worker_api", "lost-create-response"),
                                 json=_request(mandate_id, "SELECT value FROM secrets"))
    found = await client.get(root, params={"create_idempotency_key": "lost-create-response"},
                             headers={"X-API-Key": keys["worker_api"]})
    assert found.status_code == 200, found.text
    assert found.json()["query_run_id"] == prepared.json()["query_run_id"]
    for name, key in (("reviewer_api", "lost-create-response"), ("issuer_api", "lost-create-response"),
                      ("worker_api", "never-created")):
        hidden = await client.get(root, params={"create_idempotency_key": key}, headers={"X-API-Key": keys[name]})
        assert hidden.status_code == 404, (name, key)


@pytest.mark.asyncio
async def test_query_run_storage_holds_no_sqlite_lock_between_awaits(client, governed_api, monkeypatch):
    """CI deadlock: a lock held across an await blocks a synchronous write on the event-loop thread."""
    import aiosqlite
    import forge.query_runs as query_runs
    from config import cfg
    from forge.governance_credentials import authenticate_credential

    async def lost_worker(query_run_id, claim_id):
        return None

    monkeypatch.setattr(query_runs, "_run_governed_worker", lost_worker)
    keys, mandate_id = governed_api
    root = "/api/v2/enforce/query-runs"
    pending = await client.post(root, headers=_headers(keys, "worker_api", "lock-window"),
                                json=_request(mandate_id, "SELECT value FROM secrets"))
    ready = await client.post(root, headers=_headers(keys, "worker_api", "lock-window-ready"),
                              json=_request(mandate_id, "SELECT n FROM numbers"))
    claimed = await client.post(f"{root}/{ready.json()['query_run_id']}/execute", json=_action(ready.json()["review"]),
                                headers=_headers(keys, "worker_api", "lock-window-execute"))
    pending_id, executing_id = pending.json()["query_run_id"], claimed.json()["query_run_id"]
    claim_id = (await query_runs.get_query_run(executing_id))["execution_claim_id"]
    blocked = []

    def write_from_loop_thread():
        conn = sqlite3.connect(cfg.QUERY_RUN_DB_PATH, timeout=0)
        try:
            conn.execute("BEGIN EXCLUSIVE")
            conn.rollback()
        except sqlite3.OperationalError as exc:
            blocked.append(str(exc))
        finally:
            conn.close()

    original = aiosqlite.Connection.execute

    async def execute_then_yield_to_writer(self, *args, **kwargs):
        cursor = await original(self, *args, **kwargs)
        write_from_loop_thread()
        return cursor

    monkeypatch.setattr(aiosqlite.Connection, "execute", execute_then_yield_to_writer)
    reviewer = authenticate_credential(keys["reviewer_api"])
    assert (await query_runs.get_query_run(pending_id))["status"] == "review_required"
    assert (await query_runs._get_by_create_key("lock-window"))["query_run_id"] == pending_id
    assert [run["query_run_id"] for run in await query_runs.list_reviewer_query_runs(reviewer)] == [pending_id]
    completed = await query_runs.complete_query_run(executing_id, columns=["n"], rows=[[1]], row_count=1,
                                                    truncated=False, execution_ms=1, claim_id=claim_id)
    assert completed["status"] == "completed"
    _age_query_run("expires_at", pending_id)
    assert (await query_runs.get_query_run(pending_id))["status"] == "expired"
    assert blocked == []
