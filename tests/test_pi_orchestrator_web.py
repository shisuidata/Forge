"""Authenticated Pi BFF boundary behavior with synthetic principals."""
from __future__ import annotations

import pytest

@pytest.mark.asyncio
async def test_web_task_creation_and_reviewer_action_use_authenticated_principals(monkeypatch):
    from forge.governance import AuthenticatedPrincipal
    from config import cfg
    import web.routes.pi as routes
    admin = AuthenticatedPrincipal("admin_a", "human", "org_one", "team_one",
                                   "admin", 1, "key_a", 1, "api_key", "now")
    reviewer = AuthenticatedPrincipal("reviewer_b", "human", "org_one", "team_one",
                                      "reviewer", 1, "key_b", 1, "api_key", "now")
    calls = []
    async def upstream(method, path, payload=None, **kwargs):
        calls.append((method, path, payload, kwargs))
        return (202 if path.endswith("/approve-query") else 201), {"status": "executing"}
    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(routes, "_pi_request", upstream)
    await routes.api_pi_create_task(routes.PiTaskCreateRequest(message="synthetic"), principal=admin)
    assert calls[-1][2]["user_id"] == "admin_a"
    assert calls[-1][2]["org_id"] == "org_one" and calls[-1][2]["team_id"] == "team_one"
    assert calls[-1][3]["action"] == "task.write" and calls[-1][3]["principal"] is admin
    body = routes.PiApproveQueryRequest(query_run_id="qr_a", sql_hash="sql",
        assurance_report_hash="assurance", enforcement_context_hash="context",
        idempotency_key="approval-once")
    response = await routes.api_pi_approve_query("tr_a", body, principal=reviewer)
    assert response.status_code == 202
    scope = calls[-1][3]
    assert scope["principal"] is reviewer and scope["action"] == "query.approve"
    assert scope["task_run_id"] == "tr_a" and scope["query_run_id"] == "qr_a"
    assert scope["idempotency_key"] == "approval-once"
    assert scope["approval_hashes"] == {"sql_hash": "sql",
        "assurance_report_hash": "assurance", "enforcement_context_hash": "context"}


@pytest.mark.asyncio
async def test_web_task_scope_rejects_other_team_and_other_owner(monkeypatch):
    from forge.governance import AuthenticatedPrincipal
    import web.routes.pi as routes
    principal = AuthenticatedPrincipal("owner_a", "human", "org", "team",
                                       "member", 1, "key", 1, "api_key", "now")
    async def other_team(method, path, payload=None, **scope):
        return 200, {"task": {"task_run_id": "tr_other", "org_id": "org",
                              "team_id": "other", "user_id": "owner_a"}}
    monkeypatch.setattr(routes, "_pi_request", other_team)
    status, _ = await routes._pi_scoped_task_get("tr_other", principal)
    assert status == 404
    async def other_owner(method, path, payload=None, **scope):
        return 200, {"task": {"task_run_id": "tr_other", "org_id": "org",
                              "team_id": "team", "user_id": "owner_b"}}
    monkeypatch.setattr(routes, "_pi_request", other_owner)
    status, _ = await routes._pi_scoped_task_get("tr_other", principal)
    assert status == 404


@pytest.mark.asyncio
async def test_chat_action_cannot_use_old_approval_bypass(monkeypatch):
    from forge.governance import AuthenticatedPrincipal
    from config import cfg
    import web.routes.pi as routes
    principal = AuthenticatedPrincipal("owner_a", "human", "org", "team",
                                       "admin", 1, "key", 1, "api_key", "now")
    async def unexpected(*args, **kwargs):
        raise AssertionError("legacy chat approval must not contact Pi")
    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(routes, "_pi_request", unexpected)
    request = routes.PiWebChatActionRequest(action="approve_query",
        conversation_id="web_conv_12345678", message_id="web_msg_12345678",
        payload={"query_run_id": "qr_a"})
    result = await routes.api_pi_web_chat_action("tr_a", request, principal=principal)
    assert result.status_code == 410


@pytest.mark.asyncio
async def test_pi_client_sends_only_the_signed_bytes_and_header_scope(monkeypatch):
    import json
    from forge.governance import AuthenticatedPrincipal
    from config import cfg
    import web.pi_client as client
    principal = AuthenticatedPrincipal("admin_a", "human", "org", "team",
                                       "admin", 1, "credential_a", 1, "api_key", "now")
    observed = {}
    class Response:
        status_code = 202
        def json(self):
            return {"status": "executing"}
    class Transport:
        def __init__(self, **kwargs):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *_):
            pass
        async def request(self, method, url, **kwargs):
            observed["wire"] = (method, url, kwargs)
            return Response()
    def mint(actor, method, path, body, action, **scope):
        observed["ticket"] = (actor, method, path, body, action, scope)
        return "synthetic-ticket"
    monkeypatch.setattr(cfg, "PI_CHANNEL_SERVICE_KEY", "synthetic-service-key")
    monkeypatch.setattr(client, "issue_control_token", mint)
    monkeypatch.setattr(client.httpx, "AsyncClient", Transport)
    code, _ = await client.pi_request(
        "POST", "/v1/tasks/tr_a/mandate?z=2&a=1",
        {"mandate_id": "mandate_a", "expected_mandate_id": None},
        principal=principal, action="task.attach_mandate",
        task_run_id="tr_a", idempotency_key="attach-once",
    )
    actor, method, path, body, action, scope = observed["ticket"]
    wire = observed["wire"][2]
    assert code == 202 and actor is principal and method == "POST"
    assert path == "/v1/tasks/tr_a/mandate?a=1&z=2" and action == "task.attach_mandate"
    assert json.loads(body) == {"mandate_id": "mandate_a", "expected_mandate_id": None}
    assert wire["content"] == body and scope["task_run_id"] == "tr_a"
    assert wire["headers"]["X-Forge-Control-Token"] == "synthetic-ticket"
    assert wire["headers"]["Idempotency-Key"] == "attach-once"


def test_product_scope_ignores_configured_admin_scopes_and_rejects_cross_tenant():
    from fastapi import HTTPException
    from forge.governance import AuthenticatedPrincipal
    from web.routes.product import _scope
    principal = AuthenticatedPrincipal("human_a", "human", "org_a", "team_a",
                                       "member", 1, "key", 1, "api_key", "now")
    assert _scope(None, None, principal) == ("org_a", "team_a", "human_a")
    with pytest.raises(HTTPException) as denied:
        _scope("org_b", "team_a", principal)
    assert denied.value.status_code == 404


@pytest.mark.asyncio
async def test_http_mandate_attach_requires_admin_and_binds_exact_task(monkeypatch):
    from fastapi import FastAPI, HTTPException
    from httpx import ASGITransport, AsyncClient
    from forge.governance import AuthenticatedPrincipal
    from config import cfg
    from web.auth import require_admin_principal
    import web.routes.pi as routes
    app = FastAPI()
    app.include_router(routes.router)
    actor = AuthenticatedPrincipal("admin_a", "human", "org", "team",
                                   "admin", 1, "key", 1, "api_key", "now")
    async def admin():
        return actor
    app.dependency_overrides[require_admin_principal] = admin
    calls = []
    async def upstream(method, path, payload=None, **scope):
        calls.append((method, path, payload, scope))
        return 200, {"task": {"task_run_id": "tr_a"}}
    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(routes, "_pi_request", upstream)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://synthetic") as web:
        missing = await web.post("/api/pi/tasks/tr_a/mandate",
            json={"mandate_id": "mandate_a", "expected_mandate_id": None})
        assert missing.status_code == 400 and not calls
        spoofed = await web.post("/api/pi/tasks/tr_a/mandate",
            headers={"Idempotency-Key": "attach-spoof"},
            json={"mandate_id": "mandate_a", "expected_mandate_id": None,
                  "actor_principal_id": "different_admin"})
        assert spoofed.status_code == 422 and not calls
        attached = await web.post("/api/pi/tasks/tr_a/mandate",
            headers={"Idempotency-Key": "attach-once"},
            json={"mandate_id": "mandate_a", "expected_mandate_id": None})
        assert attached.status_code == 200
        assert calls[-1][3]["action"] == "task.attach_mandate"
        assert calls[-1][3]["task_run_id"] == "tr_a"
        assert calls[-1][3]["idempotency_key"] == "attach-once"
        assert calls[-1][2] == {"mandate_id": "mandate_a", "expected_mandate_id": None}


@pytest.mark.asyncio
async def test_product_admin_reads_same_team_task_with_actual_owner(monkeypatch):
    from forge.governance import AuthenticatedPrincipal
    import web.routes.product as product
    principal = AuthenticatedPrincipal("admin_a", "human", "org", "team",
                                       "admin", 1, "key", 1, "api_key", "now")
    task = {"task_run_id": "tr_b", "status": "needs_input",
            "created_at": "2026-09-23T00:00:00Z", "updated_at": "2026-09-23T00:00:01Z",
            "org_id": "org", "team_id": "team", "user_id": "human_b",
            "channel": "web", "intent": "query_prepare",
            "metadata": {"original_message": "Synthetic task"}}
    paths = []
    async def upstream(path, actor):
        paths.append(path)
        assert actor is principal
        return 200, {"tasks": [task]}
    monkeypatch.setattr(product, "_pi_product_get", upstream)
    available, tasks = await product._tasks("org", "team", "admin_a", principal)
    assert available and tasks[0]["scope"]["user_id"] == "human_b"
    assert "user_id=" not in paths[0]  # Pi applies scope from the signed admin principal.


@pytest.mark.asyncio
async def test_reviewer_projection_does_not_unlock_full_task(monkeypatch):
    from forge.governance import AuthenticatedPrincipal
    from config import cfg
    import web.routes.pi as routes
    reviewer = AuthenticatedPrincipal("reviewer_b", "human", "org", "team",
                                      "reviewer", 1, "key", 1, "api_key", "now")
    paths = []
    async def upstream(method, path, payload=None, **scope):
        paths.append((path, scope))
        if path.endswith("/query-review"):
            return 200, {"review": {"sql": "SELECT 1", "query_run_id": "qr_a"}}
        return 200, {"task": {"task_run_id": "tr_a", "org_id": "org",
                              "team_id": "team", "user_id": "owner_a"}}
    monkeypatch.setattr(cfg, "PI_ORCHESTRATOR_ENABLED", True)
    monkeypatch.setattr(routes, "_pi_request", upstream)
    limited = await routes.api_pi_query_review("tr_a", principal=reviewer)
    assert limited.status_code == 200 and paths[-1][0].endswith("/query-review")
    assert paths[-1][1]["action"] == "task.read" and paths[-1][1]["task_run_id"] == "tr_a"
    full = await routes.api_pi_task("tr_a", principal=reviewer)
    assert full.status_code == 404
