"""Internal Pi requests must remain tied to a registered service and live Task scope."""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from agent.tenant import TenantStore
from forge.governance import issue_control_token
from forge.governance_credentials import create_credential, revoke_credential
from web.routes.query_runs import router, candidate_router, internal_governance_router
from web.routes.context import router as context_router
from web.routes.memory import router as memory_router
from web.routes.reports import router as reports_router


@pytest.fixture
def internal(tmp_path, monkeypatch):
    from config import cfg
    monkeypatch.setattr(cfg, "QUERY_RUN_DB_PATH", str(tmp_path / "governance.db"))
    monkeypatch.setattr(cfg, "AUTH_ENABLED", True)
    monkeypatch.setattr(cfg, "PI_EXECUTION_PRINCIPAL_ID", "pi")
    store = TenantStore()
    store.create_team("w", organization_id="org")
    store.set_team("pi", "w", principal_type="service", status="active")
    store.set_team("a", "w", role="admin", principal_type="human", status="active")
    store.set_allowed_tables("w", ["numbers"])
    service_id, service_key = create_credential("pi", "pi_service")
    _, human_key = create_credential("a", "api_key")
    app = FastAPI()
    for route in (router, candidate_router, internal_governance_router, context_router, memory_router, reports_router):
        app.include_router(route)
    return app, service_id, service_key, human_key


@pytest.mark.asyncio
async def test_control_ticket_is_exact_request_bound_and_service_only(internal):
    app, service_id, service_key, human_key = internal
    from forge.governance_credentials import authenticate_credential
    principal = authenticate_credential(human_key)
    path = "/v1/tasks/tr_one?limit=1"
    token = issue_control_token(principal, "GET", path, b"", "task.read", task_run_id="tr_one")
    request = {"token": token, "method": "GET", "path": path,
               "body_hash": "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
               "idempotency_key": None}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        url = "/api/internal/governance/control-tokens/resolve"
        assert (await client.post(url, json=request)).status_code == 401
        assert (await client.post(url, json=request, headers={"X-API-Key": human_key})).status_code == 401
        headers = {"X-API-Key": service_key}
        good = await client.post(url, json=request, headers=headers)
        assert good.status_code == 200, good.text
        assert good.json()["principal"]["principal_id"] == "a"
        assert good.json()["action"] == "task.read"
        assert "token" not in good.json()
        for changed in ({"method": "POST"}, {"path": "/v1/tasks/tr_other?limit=1"},
                        {"idempotency_key": "new"}):
            wrong = await client.post(url, json={**request, **changed}, headers=headers)
            assert wrong.status_code == 403
        revoke_credential(service_id)
        assert (await client.post(url, json=request, headers=headers)).status_code == 401


@pytest.mark.asyncio
async def test_old_query_runs_closed_even_with_valid_service(internal):
    app, _, service_key, _ = internal
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        headers = {"X-API-Key": service_key}
        for method, path in (("POST", "/api/internal/query-runs"),
                             ("POST", "/api/internal/query-runs/qr_x/approve"),
                             ("GET", "/api/internal/query-runs/qr_x/result")):
            response = await client.request(method, path, headers=headers)
            assert response.status_code == 410
            assert response.json()["error"] == "governed_api_v2_required"


@pytest.mark.asyncio
async def test_internal_context_rejects_claimed_scope_before_retrieval(internal, monkeypatch):
    app, _, key, _ = internal
    async def scope(task_id):
        return {"task_id": task_id, "org_id": "org", "team_id": "w", "user_id": "a",
                "execution_actor_id": "pi", "mandate_id": None, "status": "created"}
    monkeypatch.setattr("web.routes.context.task_scope", scope)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        headers = {"X-API-Key": key}
        response = await client.post("/api/internal/context/search", headers=headers,
                                     json={"task_run_id": "tr_one", "question": "numbers", "user_id": "other"})
        assert response.status_code == 422
        memory = await client.post("/api/internal/memory/entries", headers=headers,
                                   json={"task_run_id": "tr_one", "operation": "delete",
                                         "category": "confirmed_fact", "key": "k", "org_id": "other"})
        assert memory.status_code == 422


@pytest.mark.asyncio
async def test_task_scope_rechecks_pi_metadata_and_live_identity(internal, monkeypatch):
    import httpx
    from agent.tenant import tenants
    from web import pi_task_scope
    from config import cfg

    app, _, key, _ = internal
    monkeypatch.setattr(cfg, "PI_CHANNEL_SERVICE_KEY", "synthetic-control-key")
    reply = {"task_id": "tr_one", "org_id": "org", "team_id": "w", "user_id": "a",
             "execution_actor_id": "pi", "mandate_id": None, "status": "created"}
    real_client = httpx.AsyncClient
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=reply)
                                     if request.headers.get("X-Channel-Service-Key") == "synthetic-control-key"
                                     else httpx.Response(401))
    monkeypatch.setattr(pi_task_scope.httpx, "AsyncClient", lambda **kwargs: real_client(transport=transport))
    scope = await pi_task_scope.task_scope("tr_one")
    assert scope["user_id"] == "a"
    reply["user_id"] = "other"
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as denied:
        await pi_task_scope.task_scope("tr_one")
    assert denied.value.status_code == 404
    reply["user_id"] = "a"
    tenants.set_team("a", "w", role="admin", principal_type="human", status="disabled")
    with pytest.raises(HTTPException) as disabled:
        await pi_task_scope.task_scope("tr_one")
    assert disabled.value.status_code == 404


@pytest.mark.asyncio
async def test_report_rejects_self_reported_identity(internal):
    app, _, service_key, _ = internal
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/internal/reports", headers={"X-API-Key": service_key},
                                     json={"task_run_id": "tr_one", "org_id": "org", "team_id": "w",
                                           "user_id": "a"})
        assert response.status_code == 422
        assert "fixed contract" in response.json()["detail"]


def test_report_query_data_must_equal_completed_run(internal):
    import json
    from fastapi import HTTPException
    from forge.governance import connect_governance
    from web.routes.reports import _verify_query_evidence

    with connect_governance() as conn:
        conn.execute("CREATE TABLE query_runs (query_run_id TEXT, task_run_id TEXT, status TEXT, sql_hash TEXT, "
                     "result_columns TEXT, result_rows TEXT, row_count INTEGER, truncated INTEGER)")
        conn.execute("INSERT INTO query_runs VALUES (?,?,?,?,?,?,?,?)",
                     ("qr_one", "tr_one", "completed", "sha256:sql", json.dumps(["n"]),
                      json.dumps([[7]]), 1, 0))
    scope = {"task_id": "tr_one"}
    query = {"query_run_id": "qr_one", "sql_hash": "sha256:sql", "columns": ["n"],
             "rows": [[7]], "row_count": 1, "truncated": False}
    _verify_query_evidence(scope, {"query_result": query})
    for changed in ({"rows": [[99]]}, {"query_run_id": "qr_other"}, {"sql_hash": "sha256:other"}):
        with pytest.raises(HTTPException) as invalid:
            _verify_query_evidence(scope, {"query_result": {**query, **changed}})
        assert invalid.value.status_code == 422


@pytest.mark.asyncio
async def test_candidate_requires_attached_mandate_and_only_returns_untrusted_candidate(internal, monkeypatch):
    app, _, key, _ = internal
    async def scope(task_id):
        return {"task_id": task_id, "org_id": "org", "team_id": "w", "user_id": "a",
                "execution_actor_id": "pi", "mandate_id": "md_right", "status": "needs_input"}
    monkeypatch.setattr("web.routes.query_runs.task_scope", scope)
    monkeypatch.setattr("web.routes.query_runs.task_tables", lambda scope, **kwargs: ["numbers"])
    observed = []
    def prepare(actor, question, dialect, *, allowed_tables):
        observed.append((actor, allowed_tables))
        return {"status": "needs_review", "forge_json": {"select": ["n"]}}
    monkeypatch.setattr("web.routes.query_runs.prepare_query", prepare)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        data = {"task_run_id": "tr_one", "mandate_id": "md_wrong", "question": "numbers"}
        headers = {"X-API-Key": key}
        assert (await client.post("/api/internal/query-candidates", json=data, headers=headers)).status_code == 403
        assert observed == []
        response = await client.post("/api/internal/query-candidates", json={**data, "mandate_id": "md_right"}, headers=headers)
        assert response.status_code == 200, response.text
        assert response.json() == {"candidate": {"kind": "forge_json", "forge_json": {"select": ["n"]}}}
        assert observed == [("pi", ["numbers"])]


def test_context_tables_cannot_exceed_active_mandate_or_current_acl(internal):
    import json
    from datetime import datetime, timedelta, timezone
    from fastapi import HTTPException
    from forge.governance import canonical_hash, connect_governance
    from web.pi_task_scope import task_tables

    issuance = {"policy": {"allowed_tables": ["numbers", "secrets"]}}
    with connect_governance() as conn:
        conn.execute("INSERT INTO issued_mandates "
                     "(mandate_id,issuer_id,delegate_id,accountable_id,reviewer_id,org_id,workspace_id,"
                     "task_run_id,issuance_json,issuance_hash,issuance_revision,status,state_revision,issued_at,"
                     "expires_at,create_key,request_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                     ("md_one", "a", "pi", "a", "a", "org", "w", "tr_one", json.dumps(issuance),
                      canonical_hash(issuance), 1, "active", 1, datetime.now(timezone.utc).isoformat(),
                      (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(), "key", "hash"))
    scope = {"task_id": "tr_one", "org_id": "org", "team_id": "w", "user_id": "a",
             "execution_actor_id": "pi", "mandate_id": "md_one"}
    assert task_tables(scope, require_mandate=True) == ["numbers"]
    with connect_governance() as conn:
        conn.execute("UPDATE issued_mandates SET status='revoked' WHERE mandate_id='md_one'")
    with pytest.raises(HTTPException) as revoked:
        task_tables(scope, require_mandate=True)
    assert revoked.value.status_code == 403


@pytest.mark.asyncio
async def test_memory_upsert_is_attributed_to_task_requester(internal, monkeypatch):
    app, _, key, _ = internal
    async def scope(task_id):
        return {"task_id": task_id, "org_id": "org", "team_id": "w", "user_id": "a",
                "execution_actor_id": "pi", "mandate_id": None, "status": "created"}
    monkeypatch.setattr("web.routes.memory.task_scope", scope)
    saved = []
    class Pool:
        def upsert(self, **entry):
            saved.append(entry)
    monkeypatch.setattr("web.routes.memory._pool", Pool)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/internal/memory/entries", headers={"X-API-Key": key},
                                     json={"task_run_id": "tr_one", "operation": "upsert",
                                           "category": "confirmed_fact", "key": "unit", "value": "USD"})
        assert response.status_code == 200, response.text
        assert saved[0]["user_id"] == "a" and saved[0]["scope"] == "user"
