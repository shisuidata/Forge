"""Internal QueryRun lifecycle and approval security tests."""
from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.fixture
def query_run_env(tmp_path, monkeypatch):
    from config import cfg
    from forge.assurance import ASSURANCE_REVISION, POLICY_REVISION
    import forge.executor as executor

    monkeypatch.setattr(cfg, "QUERY_RUN_DB_PATH", str(tmp_path / "query_runs.db"))
    monkeypatch.setattr(cfg, "QUERY_RUN_REVIEW_TTL_SECONDS", 900)
    monkeypatch.setattr(cfg, "PI_SERVICE_API_KEYS", ["pi-service-secret"])
    monkeypatch.setattr(cfg, "DATASOURCE_ID", "demo")
    monkeypatch.setattr(cfg, "DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setattr(cfg, "DATABASE_READONLY_CONFIRMED", True)
    monkeypatch.setattr(cfg, "EXECUTION_ENABLED", True)
    monkeypatch.setattr(cfg, "EXECUTION_MAX_ROWS", 2)
    monkeypatch.setattr(cfg, "EXECUTION_DISPLAY_ROWS", 2)
    monkeypatch.setattr(executor, "_engine", None)

    def fake_prepare(user_id, question, dialect=None):
        sql = "SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3"
        return {
            "status": "needs_review",
            "question": question,
            "user_id": user_id,
            "input_kind": "forge_json",
            "forge_json": {"scan": "synthetic", "select": ["n"]},
            "sql": sql,
            "dialect": dialect or "sqlite",
            "assurance_report": {
                "status": "passed",
                "assurance_revision": ASSURANCE_REVISION,
                "policy_revision": POLICY_REVISION,
                "registry_revision": "sha256:assurance-registry",
                "model_revision": "sha256:model",
                "gates": [],
                "sql": sql,
                "sql_hash": "sha256:" + __import__("hashlib").sha256(sql.encode()).hexdigest(),
                "input_kind": "forge_json",
                "candidate_revision": "query-candidate-v1",
            },
            "review_required": True,
            "can_execute": False,
            "retry_count": 0,
            "text": "",
            "error": "",
        }

    import agent.agent as agent_mod
    monkeypatch.setattr(agent_mod, "prepare_query", fake_prepare)
    return {"X-Pi-Service-Key": "pi-service-secret", "Idempotency-Key": "create-001"}


async def _create(client: AsyncClient, headers: dict[str, str]):
    return await client.post(
        "/api/internal/query-runs",
        headers=headers,
        json={
            "task_run_id": "tr_demo_001",
            "org_id": "org_demo",
            "team_id": "team_growth",
            "user_id": "user_123",
            "question": "查询前三个数字",
            "dialect": "sqlite",
        },
    )


@pytest.mark.asyncio
async def test_execution_reconciliation_fails_only_expired_leases_without_replay(
    client: AsyncClient, query_run_env
):
    import aiosqlite
    from config import cfg
    from forge.query_runs import reconcile_expired_query_run_executions

    created = (await _create(client, query_run_env)).json()
    async with aiosqlite.connect(cfg.QUERY_RUN_DB_PATH) as db:
        await db.execute(
            """UPDATE query_runs SET status = 'executing', execution_owner = 'dead-worker',
               execution_lease_expires_at = '2000-01-01T00:00:00+00:00'
               WHERE query_run_id = ?""",
            (created["query_run_id"],),
        )
        await db.commit()
    assert await reconcile_expired_query_run_executions() == 1
    recovered = await client.get(
        f"/api/internal/query-runs/{created['query_run_id']}", headers=query_run_env
    )
    assert recovered.status_code == 200
    assert recovered.json()["status"] == "failed"
    assert recovered.json()["error"] == "execution_interrupted_or_lease_expired"
    assert await reconcile_expired_query_run_executions() == 0


@pytest.mark.asyncio
async def test_execution_reconciliation_preserves_unexpired_other_worker_lease(
    client: AsyncClient, query_run_env
):
    import aiosqlite
    from config import cfg
    from forge.query_runs import reconcile_expired_query_run_executions

    created = (await _create(client, query_run_env)).json()
    async with aiosqlite.connect(cfg.QUERY_RUN_DB_PATH) as db:
        await db.execute(
            """UPDATE query_runs SET status = 'executing', execution_owner = 'live-worker',
               execution_lease_expires_at = '2999-01-01T00:00:00+00:00'
               WHERE query_run_id = ?""",
            (created["query_run_id"],),
        )
        await db.commit()
    assert await reconcile_expired_query_run_executions() == 0


@pytest.mark.asyncio
async def test_internal_query_run_requires_dedicated_pi_service_key(
    client: AsyncClient, query_run_env
):
    response = await client.post(
        "/api/internal/query-runs",
        headers={"Idempotency-Key": "create-unauthorized"},
        json={
            "task_run_id": "tr_demo",
            "org_id": "org_demo",
            "team_id": "team_demo",
            "user_id": "user_demo",
            "question": "查询数据",
        },
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_create_query_run_is_review_only_and_idempotent(
    client: AsyncClient, query_run_env
):
    first = await _create(client, query_run_env)
    second = await _create(client, query_run_env)

    assert first.status_code == 200
    assert second.status_code == 200
    first_data = first.json()
    assert second.json()["query_run_id"] == first_data["query_run_id"]
    assert first_data["status"] == "needs_review"
    assert first_data["review_required"] is True
    assert first_data["can_execute"] is False
    assert first_data["sql_hash"].startswith("sha256:")
    assert first_data["assurance_report_hash"].startswith("sha256:")
    assert first_data["assurance_report"]["status"] == "passed"
    from forge.assurance import ASSURANCE_REVISION, POLICY_REVISION
    assert first_data["assurance_revision"] == ASSURANCE_REVISION
    assert first_data["policy_revision"] == POLICY_REVISION
    assert first_data["model_revision"] == "sha256:model"
    assert "rows" not in first_data


@pytest.mark.asyncio
async def test_create_fails_closed_when_registry_changes_during_prepare(
    client: AsyncClient, query_run_env, monkeypatch
):
    import forge.query_runs as query_runs

    versions = iter(["sha256:before", "sha256:after"])
    monkeypatch.setattr(query_runs, "current_registry_version", lambda: next(versions))
    response = await _create(
        client,
        {**query_run_env, "Idempotency-Key": "create-registry-race"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "failed"
    assert "Registry changed while preparing" in response.json()["error"]
    assert response.json()["sql"] is None


@pytest.mark.asyncio
async def test_create_persists_bounded_prepare_timeout(
    client: AsyncClient, query_run_env, monkeypatch
):
    import agent.agent as agent_mod

    monkeypatch.setattr(
        agent_mod,
        "prepare_query",
        lambda user_id, question, dialect=None: {
            "status": "timed_out",
            "question": question,
            "user_id": user_id,
            "forge_json": None,
            "sql": None,
            "dialect": dialect or "sqlite",
            "assurance_report": None,
            "text": "",
            "error": "查询准备超时，请稍后重试或缩小问题范围。",
        },
    )
    response = await _create(
        client,
        {**query_run_env, "Idempotency-Key": "create-timeout"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "timed_out"
    assert "查询准备超时" in response.json()["error"]
    assert response.json()["review_required"] is False


@pytest.mark.asyncio
async def test_create_query_run_requires_idempotency_key(client: AsyncClient, query_run_env):
    headers = {"X-Pi-Service-Key": "pi-service-secret"}
    response = await _create(client, headers)
    assert response.status_code == 400
    assert response.json()["code"] == "idempotency_key_required"


@pytest.mark.asyncio
async def test_approval_rejects_changed_sql_hash(client: AsyncClient, query_run_env):
    created = (await _create(client, query_run_env)).json()
    response = await client.post(
        f"/api/internal/query-runs/{created['query_run_id']}/approve",
        headers={
            "X-Pi-Service-Key": "pi-service-secret",
            "Idempotency-Key": "approve-wrong-hash",
        },
        json={"approver_user_id": "user_123", "sql_hash": "sha256:" + "0" * 64,
              "assurance_report_hash": created["assurance_report_hash"]},
    )
    assert response.status_code == 409
    assert response.json()["code"] == "sql_hash_mismatch"


@pytest.mark.asyncio
async def test_approval_rejects_changed_assurance_report_hash(
    client: AsyncClient, query_run_env
):
    created = (await _create(client, query_run_env)).json()
    response = await client.post(
        f"/api/internal/query-runs/{created['query_run_id']}/approve",
        headers={
            "X-Pi-Service-Key": "pi-service-secret",
            "Idempotency-Key": "approve-wrong-assurance-hash",
        },
        json={
            "approver_user_id": "user_123",
            "sql_hash": created["sql_hash"],
            "assurance_report_hash": "sha256:" + "0" * 64,
        },
    )
    assert response.status_code == 409
    assert response.json()["code"] == "assurance_hash_mismatch"


@pytest.mark.asyncio
async def test_approval_rejects_a_different_user(client: AsyncClient, query_run_env):
    created = (await _create(client, query_run_env)).json()
    response = await client.post(
        f"/api/internal/query-runs/{created['query_run_id']}/approve",
        headers={
            "X-Pi-Service-Key": "pi-service-secret",
            "Idempotency-Key": "approve-wrong-user",
        },
        json={"approver_user_id": "other_user", "sql_hash": created["sql_hash"],
              "assurance_report_hash": created["assurance_report_hash"]},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_approval_rejects_registry_drift(client: AsyncClient, query_run_env, monkeypatch):
    created = (await _create(client, query_run_env)).json()
    import forge.query_runs as query_runs

    monkeypatch.setattr(query_runs, "current_registry_version", lambda: "sha256:changed")
    response = await client.post(
        f"/api/internal/query-runs/{created['query_run_id']}/approve",
        headers={
            "X-Pi-Service-Key": "pi-service-secret",
            "Idempotency-Key": "approve-registry-drift",
        },
        json={"approver_user_id": "user_123", "sql_hash": created["sql_hash"],
              "assurance_report_hash": created["assurance_report_hash"]},
    )
    assert response.status_code == 409
    assert response.json()["code"] == "registry_drift"


@pytest.mark.asyncio
async def test_approval_rejects_assurance_policy_drift(
    client: AsyncClient, query_run_env, monkeypatch
):
    created = (await _create(client, query_run_env)).json()
    import forge.assurance as assurance

    monkeypatch.setattr(assurance, "POLICY_REVISION", "convention-policy-v-next")
    response = await client.post(
        f"/api/internal/query-runs/{created['query_run_id']}/approve",
        headers={
            "X-Pi-Service-Key": "pi-service-secret",
            "Idempotency-Key": "approve-policy-drift",
        },
        json={
            "approver_user_id": "user_123",
            "sql_hash": created["sql_hash"],
            "assurance_report_hash": created["assurance_report_hash"],
        },
    )
    assert response.status_code == 409
    assert response.json()["code"] == "policy_revision_drift"


@pytest.mark.asyncio
async def test_approval_rejects_expired_review(client: AsyncClient, query_run_env):
    import aiosqlite
    from config import cfg

    created = (await _create(client, query_run_env)).json()
    async with aiosqlite.connect(cfg.QUERY_RUN_DB_PATH) as db:
        await db.execute(
            "UPDATE query_runs SET expires_at = ? WHERE query_run_id = ?",
            ("2000-01-01T00:00:00+00:00", created["query_run_id"]),
        )
        await db.commit()

    response = await client.post(
        f"/api/internal/query-runs/{created['query_run_id']}/approve",
        headers={
            "X-Pi-Service-Key": "pi-service-secret",
            "Idempotency-Key": "approve-expired",
        },
        json={"approver_user_id": "user_123", "sql_hash": created["sql_hash"],
              "assurance_report_hash": created["assurance_report_hash"]},
    )
    assert response.status_code == 409
    assert response.json()["code"] == "review_expired"


@pytest.mark.asyncio
async def test_approval_requires_confirmed_database_readonly_account(
    client: AsyncClient, query_run_env, monkeypatch
):
    from config import cfg

    created = (await _create(client, query_run_env)).json()
    monkeypatch.setattr(cfg, "DATABASE_READONLY_CONFIRMED", False)
    response = await client.post(
        f"/api/internal/query-runs/{created['query_run_id']}/approve",
        headers={
            "X-Pi-Service-Key": "pi-service-secret",
            "Idempotency-Key": "approve-no-readonly",
        },
        json={"approver_user_id": "user_123", "sql_hash": created["sql_hash"],
              "assurance_report_hash": created["assurance_report_hash"]},
    )
    assert response.status_code == 503
    assert response.json()["code"] == "readonly_identity_unconfirmed"


@pytest.mark.asyncio
async def test_approved_query_executes_once_and_returns_bounded_result(
    client: AsyncClient, query_run_env
):
    created = (await _create(client, query_run_env)).json()
    approval_headers = {
        "X-Pi-Service-Key": "pi-service-secret",
        "Idempotency-Key": "approve-001",
    }
    approval_body = {
        "approver_user_id": "user_123",
        "sql_hash": created["sql_hash"],
        "assurance_report_hash": created["assurance_report_hash"],
    }

    first = await client.post(
        f"/api/internal/query-runs/{created['query_run_id']}/approve",
        headers=approval_headers,
        json=approval_body,
    )
    replay = await client.post(
        f"/api/internal/query-runs/{created['query_run_id']}/approve",
        headers=approval_headers,
        json=approval_body,
    )

    assert first.status_code == 200
    assert replay.status_code == 200
    data = first.json()
    assert replay.json() == data
    assert data["status"] == "completed"
    assert data["columns"] == ["n"]
    assert data["rows"] == [[1], [2]]
    assert data["row_count"] == 2
    assert data["truncated"] is True

    result = await client.get(
        f"/api/internal/query-runs/{created['query_run_id']}/result",
        headers={"X-Pi-Service-Key": "pi-service-secret"},
    )
    assert result.status_code == 200
    assert result.json()["sql_hash"] == created["sql_hash"]


@pytest.mark.asyncio
async def test_cancelled_query_run_cannot_execute(client: AsyncClient, query_run_env):
    created = (await _create(client, query_run_env)).json()
    cancelled = await client.post(
        f"/api/internal/query-runs/{created['query_run_id']}/cancel",
        headers={"X-Pi-Service-Key": "pi-service-secret"},
        json={"user_id": "user_123"},
    )
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "cancelled"

    approval = await client.post(
        f"/api/internal/query-runs/{created['query_run_id']}/approve",
        headers={
            "X-Pi-Service-Key": "pi-service-secret",
            "Idempotency-Key": "approve-cancelled",
        },
        json={"approver_user_id": "user_123", "sql_hash": created["sql_hash"],
              "assurance_report_hash": created["assurance_report_hash"]},
    )
    assert approval.status_code == 409


@pytest.mark.asyncio
async def test_direct_sql_candidate_uses_same_review_and_approval_chain(
    client: AsyncClient, query_run_env
):
    created_response = await client.post(
        "/api/internal/query-runs",
        headers={
            **query_run_env,
            "Idempotency-Key": "create-direct-sql",
        },
        json={
            "task_run_id": "tr_direct_001",
            "org_id": "org_demo",
            "team_id": "team_growth",
            "user_id": "user_123",
            "question": "返回两个数字",
            "dialect": "sqlite",
            "candidate": {
                "kind": "direct_sql",
                "sql": "SELECT 7 AS n UNION ALL SELECT 8",
                "producer_revision": "external-agent-r1",
            },
        },
    )

    assert created_response.status_code == 200
    created = created_response.json()
    assert created["status"] == "needs_review"
    assert created["input_kind"] == "direct_sql"
    assert created["candidate_revision"] == "query-candidate-v1"
    assert created["forge_json"] is None
    assert created["review_required"] is True
    assert created["can_execute"] is False
    assert created["assurance_report"]["input_kind"] == "direct_sql"
    assert "rows" not in created

    approved = await client.post(
        f"/api/internal/query-runs/{created['query_run_id']}/approve",
        headers={
            "X-Pi-Service-Key": "pi-service-secret",
            "Idempotency-Key": "approve-direct-sql",
        },
        json={
            "approver_user_id": "user_123",
            "sql_hash": created["sql_hash"],
            "assurance_report_hash": created["assurance_report_hash"],
        },
    )

    assert approved.status_code == 200
    result = approved.json()
    assert result["status"] == "completed"
    assert result["input_kind"] == "direct_sql"
    assert result["candidate_revision"] == "query-candidate-v1"
    assert result["sql_hash"] == created["sql_hash"]
    assert result["rows"] == [[7], [8]]


@pytest.mark.asyncio
async def test_direct_sql_mutation_fails_before_review(client: AsyncClient, query_run_env):
    response = await client.post(
        "/api/internal/query-runs",
        headers={
            **query_run_env,
            "Idempotency-Key": "create-direct-mutation",
        },
        json={
            "task_run_id": "tr_direct_mutation",
            "org_id": "org_demo",
            "team_id": "team_growth",
            "user_id": "user_123",
            "question": "删除数据",
            "dialect": "sqlite",
            "candidate": {"kind": "direct_sql", "sql": "DELETE FROM orders"},
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "failed"
    assert data["input_kind"] == "direct_sql"
    assert data["sql"] is None
    assert data["sql_hash"] is None
    assert data["review_required"] is False
    assert data["assurance_report"]["gates"][-1]["gate"] == "sql_safety"


@pytest.mark.asyncio
async def test_invalid_query_candidate_contract_is_rejected(
    client: AsyncClient, query_run_env
):
    response = await client.post(
        "/api/internal/query-runs",
        headers={
            **query_run_env,
            "Idempotency-Key": "create-invalid-candidate",
        },
        json={
            "task_run_id": "tr_invalid_candidate",
            "org_id": "org_demo",
            "team_id": "team_growth",
            "user_id": "user_123",
            "question": "查询数据",
            "dialect": "sqlite",
            "candidate": {"kind": "direct_sql"},
        },
    )

    assert response.status_code == 400
    assert response.json()["code"] == "candidate_contract_invalid"


@pytest.fixture
def isolated_approval_store(tmp_path, monkeypatch):
    import json
    import sqlite3
    from config import cfg
    import forge.executor as executor

    registry = tmp_path / "registry.json"
    registry.write_text(json.dumps({"tables": {
        "numbers": {"columns": {"n": {}}},
        "missing_table": {"columns": {"n": {}}},
    }}))
    for name in ("REGISTRY_PATH", "METRICS_PATH", "DISAMBIGUATIONS_PATH",
                 "CONVENTIONS_PATH", "BUSINESS_CONTEXT_PATH"):
        monkeypatch.setattr(cfg, name, registry if name == "REGISTRY_PATH" else tmp_path / name)
    data = tmp_path / "data.sqlite"
    with sqlite3.connect(data) as conn:
        conn.executescript("CREATE TABLE numbers(n INTEGER); INSERT INTO numbers VALUES (1),(2),(3);")
    monkeypatch.setattr(cfg, "DATABASE_URL", f"sqlite:///{data}")
    monkeypatch.setattr(cfg, "QUERY_RUN_DB_PATH", str(tmp_path / "runs.sqlite"))
    monkeypatch.setattr(cfg, "EXECUTION_ENABLED", True)
    monkeypatch.setattr(cfg, "DATABASE_READONLY_CONFIRMED", True)
    monkeypatch.setattr(cfg, "EXECUTION_MAX_ROWS", 2)
    monkeypatch.setattr(executor, "_engine", None)
    yield registry
    if executor._engine is not None:
        executor._engine.dispose()


async def _review_local_sql(sql, *, revision_override=None):
    from forge.assurance import assure_direct_sql
    from forge.query_runs import create_query_run

    def prepare(user_id, question, dialect):
        report = assure_direct_sql(sql, dialect="sqlite").to_dict()
        if revision_override:
            report.update(revision_override)
        return {"status": "needs_review", "input_kind": "direct_sql", "sql": sql,
                "dialect": "sqlite", "assurance_report": report}

    return await create_query_run(task_run_id="test", org_id="test", team_id="test",
                                  user_id="reviewer", question="local", dialect="sqlite",
                                  idempotency_key="create", prepare_fn=prepare)


async def _approve_local_run(run, *, user="reviewer"):
    from forge.query_runs import approve_and_execute_query_run
    return await approve_and_execute_query_run(
        query_run_id=run["query_run_id"], approver_user_id=user,
        sql_hash=run["sql_hash"], assurance_report_hash=run["assurance_report_hash"],
        idempotency_key="approve",
    )


@pytest.mark.parametrize(("sql", "rows", "truncated"), [
    ('SELECT n AS "\u26a0合法列名" FROM numbers WHERE n = 1', [[1]], False),
    ('SELECT n AS "\u26a0合法列名" FROM numbers WHERE 0', [], False),
    ('SELECT n AS "\u26a0合法列名" FROM numbers ORDER BY n', [[1], [2]], True),
])
async def test_structured_sqlite_approval_success(isolated_approval_store, sql, rows, truncated):
    run = await _review_local_sql(sql)
    assert run["status"] == "needs_review"
    completed = await _approve_local_run(run)
    assert completed["status"] == "completed"
    assert completed["result_rows"] == rows
    assert completed["result_columns"] == ["\u26a0合法列名"]
    assert bool(completed["truncated"]) is truncated
    replay = await _approve_local_run(run)
    assert replay["result_hash"] == completed["result_hash"]


async def test_structured_sqlite_approval_failure_and_acl(isolated_approval_store):
    from forge.query_runs import QueryRunError, get_query_run
    run = await _review_local_sql("SELECT n FROM missing_table")
    with pytest.raises(QueryRunError) as denied:
        await _approve_local_run(run, user="not-reviewer")
    assert denied.value.code == "approver_not_authorized"
    assert (await get_query_run(run["query_run_id"]))["status"] == "needs_review"
    with pytest.raises(QueryRunError) as failed:
        await _approve_local_run(run)
    assert failed.value.code == "execution_reference_invalid"
    persisted = await get_query_run(run["query_run_id"])
    assert persisted["status"] == "failed" and persisted["error"] == failed.value.code


@pytest.mark.parametrize(("field", "revision", "code"), [
    ("assurance_revision", "query-assurance-v10", "assurance_revision_drift"),
    ("policy_revision", "convention-policy-v9", "policy_revision_drift"),
])
async def test_old_review_revision_fails_closed(isolated_approval_store, field, revision, code):
    from forge.query_runs import QueryRunError
    run = await _review_local_sql("SELECT n FROM numbers", revision_override={field: revision})
    with pytest.raises(QueryRunError) as caught:
        await _approve_local_run(run)
    assert caught.value.code == code


async def test_profile_change_invalidates_review(isolated_approval_store):
    import json
    from forge.query_runs import QueryRunError
    run = await _review_local_sql("SELECT n FROM numbers")
    registry = json.loads(isolated_approval_store.read_text())
    registry["assurance_profile"] = {"id": "large-benchmark", "revision": "large-benchmark-v1"}
    isolated_approval_store.write_text(json.dumps(registry))
    with pytest.raises(QueryRunError) as caught:
        await _approve_local_run(run)
    assert caught.value.code == "registry_drift"
