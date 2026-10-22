"""Bounded agent policy and QueryRun authorization behavior."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
import pytest



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


@pytest.fixture
def governed_env(enforce_env):
    from agent.tenant import tenants
    from forge.governance_credentials import authenticate_credential, create_credential
    from forge.governance_mandates import issue_mandate

    tenants.create_team("ws_demo", organization_id="org_demo")
    tenants.set_allowed_tables("ws_demo", ["numbers", "secrets"])
    for name, kind, role in (("issuer", "human", "admin"), ("reviewer", "human", "reviewer"),
                             ("accountable", "human", "member"), ("worker", "agent", "member"),
                             ("other", "agent", "member")):
        tenants.set_team(name, "ws_demo", principal_type=kind, role=role, status="active")

    issuer = authenticate_credential(create_credential("issuer")[1])
    worker = authenticate_credential(create_credential("worker")[1])
    mandate = issue_mandate(actor=issuer, idempotency_key="governed-policy-issue", request={
        "schema_version": 1, "task_run_id": "tr_governed", "delegate_principal_id": "worker",
        "accountable_principal_id": "accountable", "reviewer_principal_id": "reviewer",
        "purpose": "Synthetic reporting", "datasource_id": "demo",
        "allowed_tables": ["numbers", "secrets"], "autonomous_tables": ["numbers"],
        "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
    })
    return worker, issuer, mandate, tenants


def _governed_prepare(governed_env, sql, **overrides):
    from forge.enforce import prepare_governed_query

    actor, _, mandate, _ = governed_env
    args = dict(actor=actor, mandate_id=mandate["mandate_id"], task_run_id="tr_governed",
                purpose="Synthetic reporting", question="Synthetic reporting", dialect="sqlite",
                candidate={"kind": "direct_sql", "sql": sql})
    args.update(overrides)
    return prepare_governed_query(**args)


def test_governed_policy_modes_and_denial(governed_env):
    from forge.enforce import AUTHORIZATION_POLICY_REVISION

    numbers = _governed_prepare(governed_env, "SELECT n FROM numbers ORDER BY n")
    assert numbers["status"] == "ready"
    assert numbers["assurance_report"]["referenced_tables"] == ("numbers",)
    assert numbers["policy_decision"]["effect"] == "allow"
    assert numbers["policy_decision"]["decision_authority"]["principal_id"] == "issuer"
    assert {x["obligation_type"] for x in numbers["policy_decision"]["obligations"]} == {
        "read_only", "audit", "expiry"}
    assert numbers["policy_decision"]["policy_revision"] == AUTHORIZATION_POLICY_REVISION
    assert numbers["assurance_report"]["policy_revision"] != AUTHORIZATION_POLICY_REVISION

    secret = _governed_prepare(governed_env, "SELECT value FROM secrets")
    assert secret["status"] == "review_required"
    assert secret["policy_decision"]["effect"] == "conditional"
    assert secret["policy_decision"]["decision_authority"]["principal_id"] == "reviewer"
    assert "approval" in {x["obligation_type"] for x in secret["policy_decision"]["obligations"]}
    mixed = _governed_prepare(governed_env, "SELECT n FROM numbers UNION ALL SELECT value FROM secrets")
    assert mixed["status"] == "review_required"
    for sql in ("DELETE FROM numbers", "SELECT bogus FROM numbers", "SELECT name FROM sqlite_master"):
        denied = _governed_prepare(governed_env, sql)
        assert denied["status"] == "denied"
        assert denied["policy_decision"]["effect"] == "deny"
        assert denied["sql"] is None


def test_governed_policy_constant_and_hash_binding(governed_env):
    from forge.enforce import build_enforcement_context_hash

    constant = _governed_prepare(governed_env, "SELECT 1")
    assert constant["status"] == "ready"
    assert constant["assurance_report"]["referenced_tables"] == ()
    original = constant["enforcement_context_hash"]
    for field, value in (("mandate_issuance_hash", "sha256:" + "0" * 64),
                         ("execution_mode", "human_review"),
                         ("reviewer_principal_id", "other"),
                         ("referenced_tables", ["secrets"]),
                         ("governance_revision_snapshot", {"identities": {}, "acls": {}})):
        changed = dict(constant["hash_record"], **{field: value})
        assert build_enforcement_context_hash(changed) != original


def test_governed_policy_rejects_wrong_scope_revocation_and_acl(governed_env):
    from dataclasses import replace
    from forge.enforce import EnforceContextError
    from forge.governance_credentials import authenticate_credential, create_credential
    from forge.governance_mandates import revoke_mandate

    actor, issuer, mandate, tenants = governed_env
    other = authenticate_credential(create_credential("other")[1])
    for changes, code in (({"actor": other}, "mandate_not_found"),
                          ({"actor": replace(actor, principal_id="other")}, "credential_revoked"),
                          ({"task_run_id": "other_task"}, "mandate_task_mismatch"),
                          ({"purpose": "Other purpose"}, "mandate_purpose_mismatch")):
        with pytest.raises(EnforceContextError) as exc:
            _governed_prepare(governed_env, "SELECT n FROM numbers", **changes)
        assert exc.value.code == code
    tenants.set_allowed_tables("ws_demo", [])
    with pytest.raises(EnforceContextError, match="Table permission was narrowed"):
        _governed_prepare(governed_env, "SELECT 1")
    tenants.set_allowed_tables("ws_demo", ["numbers", "secrets"])
    revoke_mandate(actor=issuer, mandate_id=mandate["mandate_id"],
                   expected_state_revision=1, reason="synthetic revoke")
    with pytest.raises(EnforceContextError) as exc:
        _governed_prepare(governed_env, "SELECT 1")
    assert exc.value.code in {"mandate_not_found", "mandate_inactive"}


def test_governed_policy_compiled_candidate_and_nested_scope(governed_env):
    compiled = _governed_prepare(
        governed_env, "", candidate={"kind": "forge_json", "forge_json": {
            "scan": "numbers", "select": ["numbers.n"],
        }})
    assert compiled["status"] == "ready"
    assert compiled["assurance_report"]["input_kind"] == "forge_json"
    assert compiled["assurance_report"]["referenced_tables"] == ("numbers",)

    nested = _governed_prepare(governed_env, "WITH secrets AS (SELECT n FROM numbers) SELECT n FROM secrets")
    assert nested["status"] == "ready"
    assert nested["assurance_report"]["referenced_tables"] == ("numbers",)
    hidden = _governed_prepare(governed_env, "WITH masked AS (SELECT value FROM secrets) SELECT value FROM masked")
    assert hidden["status"] == "review_required"
    assert hidden["assurance_report"]["referenced_tables"] == ("secrets",)

@pytest.mark.asyncio
async def test_governed_query_run_autonomous_and_review_execution(governed_env):
    import asyncio
    from forge.governance_credentials import authenticate_credential, create_credential
    from forge.query_runs import create_governed_query_run, execute_authorized_query_run, get_query_run

    _, _, mandate, _ = governed_env
    _, worker_token = create_credential("worker")
    _, reviewer_token = create_credential("reviewer")
    worker = authenticate_credential(worker_token)
    reviewer = authenticate_credential(reviewer_token)
    arguments = dict(actor=worker, mandate_id=mandate["mandate_id"], task_run_id="tr_governed",
                     purpose="Synthetic reporting", question="Synthetic reporting", dialect="sqlite")
    ready = await create_governed_query_run(**arguments,
        candidate={"kind": "direct_sql", "sql": "SELECT n FROM numbers ORDER BY n"}, idempotency_key="run-autonomous")
    assert ready["status"] == "ready" and ready["result_rows"] is None
    hashes = {name: ready[name] for name in ("sql_hash", "assurance_report_hash", "enforcement_context_hash")}
    claimed = await execute_authorized_query_run(query_run_id=ready["query_run_id"], actor=worker,
                                                  action="execute", hashes=hashes, idempotency_key="execute-autonomous")
    assert claimed["status"] in {"executing", "completed"}
    for _ in range(100):
        result = await get_query_run(ready["query_run_id"])
        if result["status"] != "executing":
            break
        await asyncio.sleep(.01)
    assert result["status"] == "completed" and result["result_rows"] == [[1], [2]]
    assert result["approver_user_id"] is None and result["authorization_record"]["kind"] == "mandate"

    review = await create_governed_query_run(**arguments,
        candidate={"kind": "direct_sql", "sql": "SELECT value FROM secrets"}, idempotency_key="run-review")
    assert review["status"] == "review_required" and review["result_rows"] is None
    hashes = {name: review[name] for name in ("sql_hash", "assurance_report_hash", "enforcement_context_hash")}
    with pytest.raises(Exception):
        await execute_authorized_query_run(query_run_id=review["query_run_id"], actor=worker,
                                           action="approve", hashes=hashes, idempotency_key="review-by-worker")
    await execute_authorized_query_run(query_run_id=review["query_run_id"], actor=reviewer,
                                       action="approve", hashes=hashes, idempotency_key="review-by-reviewer")
    for _ in range(100):
        result = await get_query_run(review["query_run_id"])
        if result["status"] != "executing":
            break
        await asyncio.sleep(.01)
    assert result["status"] == "completed" and result["approver_user_id"] == "reviewer"
    assert result["authorization_record"]["actor_principal_id"] == "worker"


@pytest.mark.asyncio
async def test_governed_claim_revocation_and_idempotency(governed_env):
    from forge.governance_credentials import authenticate_credential, create_credential, revoke_credential
    from forge.governance_mandates import revoke_mandate
    from forge.query_runs import create_governed_query_run, execute_authorized_query_run, QueryRunError

    _, issuer, mandate, _ = governed_env
    credential_id, token = create_credential("worker")
    actor = authenticate_credential(token)
    args = dict(actor=actor, mandate_id=mandate["mandate_id"], task_run_id="tr_governed",
                purpose="Synthetic reporting", question="Synthetic reporting", dialect="sqlite")
    candidate = {"kind": "direct_sql", "sql": "SELECT n FROM numbers"}
    run = await create_governed_query_run(**args, candidate=candidate, idempotency_key="same-key")
    assert (await create_governed_query_run(**args, candidate=candidate,
                                            idempotency_key="same-key"))["query_run_id"] == run["query_run_id"]
    with pytest.raises(QueryRunError, match="another request"):
        await create_governed_query_run(**args, candidate={"kind": "direct_sql", "sql": "SELECT 1"},
                                        idempotency_key="same-key")
    hashes = {name: run[name] for name in ("sql_hash", "assurance_report_hash", "enforcement_context_hash")}
    revoke_credential(credential_id)
    with pytest.raises(QueryRunError) as denied:
        await execute_authorized_query_run(query_run_id=run["query_run_id"], actor=actor,
                                           action="execute", hashes=hashes, idempotency_key="revoked-key")
    assert denied.value.code == "credential_revoked"
    _, replacement = create_credential("worker")
    actor = authenticate_credential(replacement)
    revoke_mandate(actor=issuer, mandate_id=mandate["mandate_id"],
                   expected_state_revision=mandate["state_revision"], reason="synthetic revoke")
    with pytest.raises(QueryRunError) as denied:
        await execute_authorized_query_run(query_run_id=run["query_run_id"], actor=actor,
                                           action="execute", hashes=hashes, idempotency_key="revoked-mandate")
    assert denied.value.code == "mandate_inactive"


@pytest.mark.asyncio
async def test_pi_approval_ticket_binds_actual_reviewer_and_execution(governed_env, monkeypatch):
    from config import cfg
    from forge.governance import issue_control_token, governance_transaction
    from forge.governance_credentials import authenticate_credential, create_credential
    from forge.query_runs import create_governed_query_run, execute_authorized_query_run, QueryRunError

    _, _, mandate, tenants = governed_env
    tenants.set_team("pi", "ws_demo", principal_type="service", role="member", status="active")
    monkeypatch.setattr(cfg, "PI_EXECUTION_PRINCIPAL_ID", "pi")
    _, worker_key = create_credential("worker")
    _, reviewer_key = create_credential("reviewer")
    _, pi_key = create_credential("pi", "pi_service")
    worker, reviewer, pi = map(authenticate_credential, (worker_key, reviewer_key, pi_key))
    run = await create_governed_query_run(actor=worker, mandate_id=mandate["mandate_id"],
        task_run_id="tr_governed", purpose="Synthetic reporting", question="Synthetic reporting",
        dialect="sqlite", candidate={"kind": "direct_sql", "sql": "SELECT value FROM secrets"},
        idempotency_key="pi-ticket-create")
    hashes = {key: run[key] for key in ("sql_hash", "assurance_report_hash", "enforcement_context_hash")}
    ticket = issue_control_token(reviewer, "POST", "/v1/tasks/tr_governed/approve-query", b'{}',
                                 "query.approve", task_run_id="tr_governed", query_run_id=run["query_run_id"],
                                 approval_hashes=hashes, idempotency_key="pi-approval")
    with pytest.raises(QueryRunError) as mismatch:
        await execute_authorized_query_run(query_run_id=run["query_run_id"], actor=pi,
            action="approve", hashes=hashes, idempotency_key="different-key", control_token=ticket)
    assert mismatch.value.code == "control_token_invalid"
    claimed = await execute_authorized_query_run(query_run_id=run["query_run_id"], actor=pi,
        action="approve", hashes=hashes, idempotency_key="pi-approval", control_token=ticket)
    assert claimed["approver_user_id"] == "reviewer"
    assert claimed["authorization_record"]["actor_principal_id"] == "worker"
    assert claimed["authorization_record"]["credential_id"] == reviewer.credential_id
    with governance_transaction() as conn:
        row = conn.execute("SELECT consumed_execution_key FROM governance_control_tokens WHERE query_run_id=?",
                           (run["query_run_id"],)).fetchone()
        assert row["consumed_execution_key"] == "pi-approval"


@pytest.mark.asyncio
async def test_claim_before_revoke_keeps_only_committed_sql_license(governed_env, monkeypatch):
    import asyncio
    import threading
    from forge.governance_credentials import authenticate_credential, create_credential
    from forge.governance_mandates import revoke_mandate
    from forge.query_runs import create_governed_query_run, execute_authorized_query_run, get_query_run, QueryRunError
    import forge.executor as executor

    _, issuer, mandate, _ = governed_env
    _, token = create_credential("worker")
    actor = authenticate_credential(token)
    entry = threading.Event()
    release = threading.Event()
    calls = []
    real_execute = executor.execute
    def gated_execute(sql, limit):
        calls.append(sql)
        entry.set()
        if not release.wait(5):
            raise RuntimeError("worker gate timed out")
        return real_execute(sql, limit)
    monkeypatch.setattr(executor, "execute", gated_execute)
    args = dict(actor=actor, mandate_id=mandate["mandate_id"], task_run_id="tr_governed",
                purpose="Synthetic reporting", question="Synthetic reporting", dialect="sqlite")
    first = await create_governed_query_run(**args,
        candidate={"kind": "direct_sql", "sql": "SELECT n FROM numbers ORDER BY n"}, idempotency_key="first")
    second = await create_governed_query_run(**args,
        candidate={"kind": "direct_sql", "sql": "SELECT n FROM numbers"}, idempotency_key="second")
    hashes = lambda run: {key: run[key] for key in ("sql_hash", "assurance_report_hash", "enforcement_context_hash")}
    try:
        claimed = await execute_authorized_query_run(query_run_id=first["query_run_id"], actor=actor,
            action="execute", hashes=hashes(first), idempotency_key="first-execution")
        assert claimed["status"] == "executing"
        assert await asyncio.to_thread(entry.wait, 3)
        revoke_mandate(actor=issuer, mandate_id=mandate["mandate_id"],
                       expected_state_revision=mandate["state_revision"], reason="claim race")
        with pytest.raises(QueryRunError) as denied:
            await execute_authorized_query_run(query_run_id=second["query_run_id"], actor=actor,
                action="execute", hashes=hashes(second), idempotency_key="second-execution")
        assert denied.value.code == "mandate_inactive"
    finally:
        release.set()
    for _ in range(100):
        result = await get_query_run(first["query_run_id"])
        if result["status"] != "executing":
            break
        await asyncio.sleep(.01)
    assert result["status"] == "completed" and len(calls) == 1

