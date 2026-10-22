"""Isolated authenticated Evaluate → Enforce → Explain demonstration."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
from importlib.metadata import PackageNotFoundError, version
from datetime import datetime, timedelta, timezone
import json
import os
import platform
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from typing import Any, Callable, Iterator
import uuid

import httpx


class QuickstartError(RuntimeError):
    """Bounded quickstart failure safe to print to a terminal."""


def _available_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        return int(server.getsockname()[1])


def _write_demo_inputs(workdir: Path) -> None:
    registry = {
        "tables": {
            "numbers": {
                "description": "Synthetic numbers used by the public quickstart",
                "columns": {
                    "n": {"description": "A synthetic integer"},
                },
            },
            "secrets": {"description": "Synthetic review-only data", "columns": {"label": {"description": "Synthetic label"}}},
        }
    }
    (workdir / "registry.json").write_text(
        json.dumps(registry, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    with sqlite3.connect(workdir / "data.db") as connection:
        connection.execute("CREATE TABLE numbers (n INTEGER NOT NULL)")
        connection.executemany("INSERT INTO numbers VALUES (?)", [(1,), (2,), (3,)])
        connection.execute("CREATE TABLE secrets (label TEXT NOT NULL)")
        connection.execute("INSERT INTO secrets VALUES ('synthetic')")
    for name in ("metrics.yaml", "disambiguations.yaml", "conventions.yaml"):
        (workdir / name).write_text("{}\n", encoding="utf-8")


def _server_environment(workdir: Path) -> dict[str, str]:
    # Never inherit application credentials, dotenv paths or a caller's database.
    inherited = ("PATH", "HOME", "TMPDIR", "SYSTEMROOT", "VIRTUAL_ENV", "DYLD_LIBRARY_PATH")
    environment = {key: os.environ[key] for key in inherited if key in os.environ}
    environment.update({
        "PYTHONPATH": str(Path(__file__).resolve().parent.parent),
        "FORGE_DISABLE_DOTENV": "true", "FORGE_DISABLE_YAML_CONFIG": "true",
        "AUTH_ENABLED": "true", "AUTH_ADMIN_PRINCIPAL_ID": "quickstart-admin",
        "AUTH_PASSWORD": "", "AUTH_API_KEYS": "", "AUTH_COOKIE_SECURE": "false",
        "DATABASE_DIALECT": "sqlite", "DATABASE_READONLY_CONFIRMED": "true",
        "DATABASE_URL": f"sqlite:///{workdir / 'data.db'}", "DATASOURCE_ID": "quickstart",
        "EXECUTION_ENABLED": "true", "RAW_SQL_ENABLED": "false",
        "EXECUTION_MAX_ROWS": "2", "EXECUTION_TIMEOUT_SECONDS": "5",
        "QUERY_RUN_DB_PATH": str(workdir / "query_runs.db"),
        "AUDIT_DB_PATH": str(workdir / "audit.db"),
        "REPORT_DB_PATH": str(workdir / "reports.db"),
        "REPORT_ARTIFACT_DIR": str(workdir / "report-artifacts"),
        "QUERY_RUN_REVIEW_TTL_SECONDS": "900",
        "REGISTRY_PATH": str(workdir / "registry.json"),
        "METRICS_PATH": str(workdir / "metrics.yaml"),
        "DISAMBIGUATIONS_PATH": str(workdir / "disambiguations.yaml"),
        "CONVENTIONS_PATH": str(workdir / "conventions.yaml"),
        "PI_ORCHESTRATOR_ENABLED": "false", "PI_CHANNEL_SERVICE_KEY": "",
        "PI_EXECUTION_PRINCIPAL_ID": "", "LEGACY_AGENT_API_ENABLED": "false",
        "LLM_API_KEY": "", "EMBED_API_KEY": "", "FEISHU_APP_ID": "",
        "DINGTALK_APP_KEY": "",
    })
    return environment


def _bootstrap_demo(workdir: Path) -> None:
    manifest = {
        "schema_version": 1,
        "teams": [{"team_id": "quickstart-workspace", "organization_id": "quickstart-org",
                   "display_name": "Synthetic quickstart", "allowed_tables": ["numbers", "secrets"]}],
        "principals": [
            {"principal_id": principal, "team_id": "quickstart-workspace",
             "display_name": principal, "principal_type": kind, "role": role, "status": "active"}
            for principal, kind, role in (
                ("quickstart-admin", "human", "admin"),
                ("quickstart-reviewer", "human", "reviewer"),
                ("quickstart-agent", "agent", "member"),
            )
        ],
        "channel_bindings": [],
    }
    (workdir / "bootstrap.json").write_text(json.dumps(manifest), encoding="utf-8")
    bootstrap_code = (
        "from pathlib import Path\n"
        "import json, os\n"
        "from forge.governance import bootstrap\n"
        "from forge.governance_credentials import create_credential\n"
        "bootstrap(json.loads(Path('bootstrap.json').read_text()))\n"
        "for name in ('admin', 'reviewer', 'agent'):\n"
        "    _, token = create_credential('quickstart-' + name)\n"
        "    fd = os.open(name + '.key', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)\n"
        "    with os.fdopen(fd, 'w') as file: file.write(token)\n"
    )
    completed = subprocess.run(
        [sys.executable, "-c", bootstrap_code], cwd=workdir,
        env=_server_environment(workdir), stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, check=False,
    )
    if completed.returncode:
        raise QuickstartError("Isolated governance bootstrap failed")


def _server_failure(log_path: Path) -> QuickstartError:
    try:
        lines = log_path.read_text(encoding="utf-8").splitlines()[-12:]
    except OSError:
        lines = []
    detail = "\n".join(lines)
    message = "Local Forge server did not become ready"
    return QuickstartError(f"{message}\n{detail}" if detail else message)


@contextmanager
def _local_server(workdir: Path) -> Iterator[str]:
    port = _available_port()
    base_url = f"http://127.0.0.1:{port}"
    log_path = workdir / "server.log"
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
            ],
            cwd=workdir,
            env=_server_environment(workdir),
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
    try:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise _server_failure(log_path)
            try:
                response = httpx.get(f"{base_url}/health", timeout=0.5)
                if response.status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
        else:
            raise _server_failure(log_path)
        yield base_url
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def _response_json(response: httpx.Response, operation: str) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise QuickstartError(f"{operation} returned a non-JSON response") from exc
    if not isinstance(payload, dict):
        raise QuickstartError(f"{operation} returned a non-object response")
    if response.is_error or payload.get("status") == "error":
        failure = payload.get("failure")
        code = failure.get("code") if isinstance(failure, dict) else "request_failed"
        raise QuickstartError(f"{operation} failed: {code}")
    return payload


def _forge_version() -> str:
    try:
        return version("forge")
    except PackageNotFoundError:
        return "uninstalled"


def _run_receipt(
    summary: dict[str, Any],
    *,
    started_at: datetime,
    started_monotonic: float,
) -> dict[str, Any]:
    completed_at = datetime.now(timezone.utc)
    receipt = {
        "schema_version": 1,
        "forge_version": _forge_version(),
        "environment": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "python_implementation": platform.python_implementation(),
            "python_version": platform.python_version(),
        },
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "runtime_duration_ms": round((time.monotonic() - started_monotonic) * 1000),
        "outcome": {
            "query_run_id": summary["query_run_id"],
            "mandate_id": summary["mandate_id"],
            "review": summary["review"],
            "revocation": summary["revocation"],
            "fail_closed": summary["fail_closed"],
            "evaluate": summary["evaluate"],
            "enforce": {
                "status": summary["enforce"]["status"],
                "truncated": summary["enforce"]["truncated"],
            },
            "explain": summary["explain"],
            "dashboard": summary["dashboard"],
        },
    }
    canonical = json.dumps(
        receipt,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    receipt["receipt_hash"] = "sha256:" + hashlib.sha256(canonical).hexdigest()
    return receipt


def _request_payloads() -> tuple[dict[str, Any], dict[str, Any]]:
    candidate = {"kind": "direct_sql", "sql": "SELECT numbers.n FROM numbers ORDER BY numbers.n",
                 "producer_revision": "quickstart-v2"}
    rejection = {"schema_version": 1, "question": "Delete synthetic numbers", "dialect": "sqlite",
                 "candidate": {"kind": "direct_sql", "sql": "DELETE FROM numbers",
                               "producer_revision": "quickstart-v2"}, "allowed_tables": ["numbers"]}
    evaluation = {"schema_version": 1, "question": "List synthetic numbers", "dialect": "sqlite",
                  "candidate": candidate, "allowed_tables": ["numbers"],
                  "expected_result": {"columns": ["n"], "rows": [[1], [2], [3]]},
                  "actual_result": {"columns": ["n"], "rows": [[1], [2], [3]]}}
    return rejection, evaluation


def _hash_action(run: dict[str, Any]) -> dict[str, Any]:
    review = run.get("review")
    if not isinstance(review, dict):
        raise QuickstartError("QueryRun omitted review hashes")
    return {"schema_version": 2, **{field: review[field] for field in (
        "sql_hash", "assurance_report_hash", "enforcement_context_hash")}}


def _wait_for_query_run(client: httpx.Client, run_id: str, headers: dict[str, str]) -> dict[str, Any]:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        result = _response_json(client.get(f"/api/v2/enforce/query-runs/{run_id}", headers=headers), "QueryRun read")
        if result["status"] == "completed":
            return result
        if result["status"] in {"failed", "denied", "cancelled"}:
            raise QuickstartError(f"QueryRun ended in {result['status']}")
        time.sleep(0.1)
    raise QuickstartError(f"QueryRun {run_id} result unknown; read the existing run before retrying")


def _run_public_path(
    base_url: str, workdir: Path, *, agent_mode: bool,
    approve: Callable[[str], bool], progress: Callable[[str], None],
) -> dict[str, Any]:
    rejection_request, evaluate_request = _request_payloads()
    admin = {"X-API-Key": (workdir / "admin.key").read_text(encoding="utf-8")}
    reviewer = {"X-API-Key": (workdir / "reviewer.key").read_text(encoding="utf-8")}
    agent = {"X-API-Key": (workdir / "agent.key").read_text(encoding="utf-8")}
    task_id = "tr_quickstart_" + uuid.uuid4().hex[:12]
    purpose = "Demonstrate synthetic governed execution"
    with httpx.Client(base_url=base_url, timeout=10) as client:
        rejection = _response_json(client.post("/api/v1/evaluate", json=rejection_request, headers=agent), "Fail-closed Evaluate")
        failure = rejection.get("failure")
        if not (rejection.get("status") == "failed" and isinstance(failure, dict)
                and failure.get("stage") == "assurance" and failure.get("code") == "readonly_violation"):
            raise QuickstartError("Evaluate did not reject write SQL")
        progress("Evaluate rejected write SQL: assurance/readonly_violation")
        evaluation = _response_json(client.post("/api/v1/evaluate", json=evaluate_request, headers=agent), "Evaluate")
        if evaluation.get("status") != "passed":
            raise QuickstartError("Evaluate did not pass the read-only candidate")
        progress("Evaluate passed the synthetic direct SQL candidate")

        issued = _response_json(client.post("/api/v2/governance/mandates", json={
            "schema_version": 1, "task_run_id": task_id,
            "delegate_principal_id": "quickstart-agent",
            "accountable_principal_id": "quickstart-admin",
            "reviewer_principal_id": "quickstart-reviewer",
            "purpose": purpose, "datasource_id": "quickstart",
            "allowed_tables": ["numbers", "secrets"],
            "autonomous_tables": ["numbers"] if agent_mode else [],
            "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        }, headers={**admin, "Idempotency-Key": "quickstart-mandate-" + task_id}), "Issue mandate")
        mandate_id = issued["mandate_id"]
        progress(f"Admin issued bounded mandate {mandate_id}")

        def prepare(sql: str, question: str) -> dict[str, Any]:
            response = client.post("/api/v2/enforce/query-runs", json={
                "schema_version": 2, "mandate_id": mandate_id, "task_run_id": task_id,
                "purpose": purpose, "question": question, "dialect": "sqlite",
                "candidate": {"kind": "direct_sql", "sql": sql,
                              "producer_revision": "quickstart-v2"},
            }, headers={**agent, "Idempotency-Key": "quickstart-create-" + uuid.uuid4().hex})
            return _response_json(response, "Enforce prepare")

        numbers = prepare("SELECT numbers.n FROM numbers ORDER BY numbers.n", "List synthetic numbers")
        expected = "ready" if agent_mode else "review_required"
        if numbers["status"] != expected:
            raise QuickstartError(f"Numbers query expected {expected}; received {numbers['status']}")
        sql = numbers["review"]["sql"]
        progress(f"SQL to execute ({expected}):\n{sql}")
        if not agent_mode and not approve(sql):
            raise QuickstartError("Approval declined; no SQL was executed")
        action = "execute" if agent_mode else "approve"
        authorizer = agent if agent_mode else reviewer
        started = _response_json(client.post(
            f"/api/v2/enforce/query-runs/{numbers['query_run_id']}/{action}",
            json=_hash_action(numbers),
            headers={**authorizer, "Idempotency-Key": "quickstart-action-" + numbers["query_run_id"]},
        ), "Enforce authorization")
        if started["status"] not in {"executing", "completed"}:
            raise QuickstartError("Enforce did not claim the authorized QueryRun")
        completed = _wait_for_query_run(client, numbers["query_run_id"], agent)
        explanation = _response_json(client.get(
            f"/api/v2/explain/query-runs/{numbers['query_run_id']}", headers=admin,
        ), "Explain")
        if explanation.get("integrity", {}).get("status") != "verified":
            raise QuickstartError("Explain did not verify governed Evidence")
        progress("Numbers completed with verified mandate and authorization Evidence")

        review_proof = None
        revoked_proof = None
        if agent_mode:
            secrets_run = prepare("SELECT COUNT(*) AS n FROM secrets", "Count synthetic review-only records")
            if secrets_run["status"] != "review_required":
                raise QuickstartError("Non-autonomous table did not require human review")
            progress(f"SQL requiring reviewer approval:\n{secrets_run['review']['sql']}")
            if not approve(secrets_run["review"]["sql"]):
                raise QuickstartError("Approval declined; no review-only SQL was executed")
            started = _response_json(client.post(
                f"/api/v2/enforce/query-runs/{secrets_run['query_run_id']}/approve",
                json=_hash_action(secrets_run),
                headers={**reviewer, "Idempotency-Key": "quickstart-approve-" + secrets_run["query_run_id"]},
            ), "Reviewer approval")
            if started["status"] not in {"executing", "completed"}:
                raise QuickstartError("Reviewer approval did not claim the QueryRun")
            _wait_for_query_run(client, secrets_run["query_run_id"], agent)
            review_explanation = _response_json(client.get(
                f"/api/v2/explain/query-runs/{secrets_run['query_run_id']}", headers=reviewer,
            ), "Reviewer Evidence")
            if review_explanation.get("integrity", {}).get("status") != "verified":
                raise QuickstartError("Human review Evidence did not verify")
            review_proof = {
                "query_run_id": secrets_run["query_run_id"], "status": "completed",
                "approval_principal_id": review_explanation["governance"]["approval"]["approver_principal"]["principal_id"],
                "evidence_types": [item["evidence_type"] for item in review_explanation["evidence"]],
            }
            progress("Reviewer completed non-autonomous secrets query")
            revoked = _response_json(client.post(
                f"/api/v2/governance/mandates/{mandate_id}/revoke",
                json={"expected_state_revision": issued["state_revision"], "reason": "Synthetic demo completed"},
                headers=admin,
            ), "Revoke mandate")
            denied = client.post("/api/v2/enforce/query-runs", json={
                "schema_version": 2, "mandate_id": mandate_id, "task_run_id": task_id,
                "purpose": purpose, "question": "List synthetic numbers again", "dialect": "sqlite",
                "candidate": evaluate_request["candidate"],
            }, headers={**agent, "Idempotency-Key": "quickstart-after-revoke-" + task_id})
            denied_body = denied.json()
            if (denied.status_code < 400 or denied_body.get("status") != "error"
                    or denied_body.get("failure", {}).get("code") in {"enforce_request_invalid", "query_run_invalid"}):
                raise QuickstartError("Revoked mandate allowed a new QueryRun")
            revoked_proof = {"status": revoked["status"], "new_prepare_status": denied.status_code,
                             "failure_code": denied_body["failure"]["code"]}
            progress("Revocation prevented a new execution claim")

        login = client.post("/login", data={"api_key": admin["X-API-Key"], "next": "/admin/dashboard"},
                            follow_redirects=True)
        if login.status_code != 200 or "/admin/dashboard" not in str(login.url):
            raise QuickstartError("Admin login failed for the browsable dashboard")
        dashboard = client.get("/admin/dashboard")
        if dashboard.status_code != 200 or numbers["query_run_id"] not in dashboard.text:
            raise QuickstartError("Dashboard did not project the same governed QueryRun")
        progress("Authenticated dashboard projected the same QueryRun")

    result = explanation["statement"]["result"]
    return {
        "status": "passed", "query_run_id": numbers["query_run_id"],
        "mandate_id": mandate_id,
        "fail_closed": {"status": rejection["status"], "failure": {
            "stage": failure["stage"], "code": failure["code"], "retryable": failure["retryable"]}},
        "evaluate": {"status": evaluation["status"],
                     "policy_verdict": evaluation["policy"]["verdict"],
                     "result_comparison": evaluation["result_comparison"]["status"]},
        "enforce": {"status": completed["status"], "rows": result["rows"],
                    "truncated": result["truncated"],
                    "execution_mode": numbers["decision"]["execution_mode"]},
        "explain": {"integrity": explanation["integrity"]["status"],
                    "evidence_types": [item["evidence_type"] for item in explanation["evidence"]],
                    "limitations": [item["code"] for item in explanation["limitations"]],
                    "approval": explanation["governance"]["approval"]},
        "review": review_proof, "revocation": revoked_proof,
        "dashboard": {"status": "passed", "path": "/admin/dashboard"},
    }


def run_quickstart(
    *,
    workdir: Path | None,
    auto_approve: bool,
    agent_mode: bool = False,
    progress: Callable[[str], None] = print,
    input_fn: Callable[[str], str] = input,
    hold_open: Callable[[dict[str, Any], str], None] | None = None,
) -> dict[str, Any]:
    """Run the public Golden Path against an isolated local Forge server."""
    started_at = datetime.now(timezone.utc)
    started_monotonic = time.monotonic()
    temporary: tempfile.TemporaryDirectory[str] | None = None
    if workdir is None:
        temporary = tempfile.TemporaryDirectory(prefix="forge-quickstart-")
        root = Path(temporary.name)
    else:
        root = workdir.expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        occupied = [
            name
            for name in ("data.db", "query_runs.db", "registry.json", "admin.key", "reviewer.key", "agent.key")
            if (root / name).exists()
        ]
        if occupied:
            raise QuickstartError(
                f"Quickstart workdir must not contain: {', '.join(occupied)}"
            )
    try:
        _write_demo_inputs(root)
        _bootstrap_demo(root)

        def approve(_sql: str) -> bool:
            if auto_approve:
                progress("Demo approval accepted by --yes (local synthetic data only)")
                return True
            try:
                answer = input_fn("Approve this read-only demo SQL? [y/N] ")
            except EOFError:
                return False
            return answer.strip().lower() in {"y", "yes"}

        with _local_server(root) as base_url:
            summary = _run_public_path(base_url, root, agent_mode=agent_mode,
                                       approve=approve, progress=progress)
            summary["run_receipt"] = _run_receipt(
                summary,
                started_at=started_at,
                started_monotonic=started_monotonic,
            )
            if workdir is not None:
                summary["workdir"] = str(root)
                (root / "summary.json").write_text(
                    json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8",
                )
            if hold_open is not None:
                summary["credential_files"] = {name: str(root / f"{name}.key")
                                               for name in ("admin", "reviewer")}
                hold_open(summary, base_url)
        return summary
    finally:
        if temporary is not None:
            temporary.cleanup()
