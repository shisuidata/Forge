"""Self-contained public Evaluate → Enforce → Explain quickstart."""
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
            }
        }
    }
    (workdir / "registry.json").write_text(
        json.dumps(registry, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    with sqlite3.connect(workdir / "data.db") as connection:
        connection.execute("CREATE TABLE numbers (n INTEGER NOT NULL)")
        connection.executemany("INSERT INTO numbers VALUES (?)", [(1,), (2,), (3,)])


def _server_environment(workdir: Path) -> dict[str, str]:
    environment = os.environ.copy()
    project_root = str(Path(__file__).resolve().parent.parent)
    existing_pythonpath = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        project_root
        if not existing_pythonpath
        else os.pathsep.join((project_root, existing_pythonpath))
    )
    environment.update(
        {
            "AUTH_ENABLED": "false",
            "DATABASE_DIALECT": "sqlite",
            "DATABASE_READONLY_CONFIRMED": "true",
            "DATABASE_URL": f"sqlite:///{workdir / 'data.db'}",
            "DATASOURCE_ID": "quickstart",
            "EXECUTION_ENABLED": "true",
            "EXECUTION_MAX_ROWS": "2",
            "EXECUTION_TIMEOUT_SECONDS": "5",
            "QUERY_RUN_DB_PATH": str(workdir / "query_runs.db"),
            "QUERY_RUN_REVIEW_TTL_SECONDS": "900",
            "REGISTRY_PATH": str(workdir / "registry.json"),
        }
    )
    return environment


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


def _principal(now: datetime) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "principal_context_id": "pc_quickstart",
        "actor_principal": {
            "principal_id": "quickstart-human",
            "principal_type": "human",
        },
        "accountable_principal": {
            "principal_id": "quickstart-human",
            "principal_type": "human",
        },
        "organization_id": "quickstart-org",
        "workspace_id": "quickstart-workspace",
        "authentication_context": {
            "method": "local",
            "assurance_level": "single_factor",
            "authenticated_at": now.isoformat(),
            "session_id_hash": None,
        },
        "delegation_chain": [],
        "issued_at": now.isoformat(),
        "expires_at": (now + timedelta(minutes=15)).isoformat(),
    }


def _resource_scope() -> list[dict[str, Any]]:
    return [
        {
            "schema_version": 1,
            "resource_type": "datasource",
            "resource_id": "quickstart",
            "organization_id": "quickstart-org",
            "workspace_id": "quickstart-workspace",
            "parent_resource_id": "quickstart-workspace",
            "resource_revision": None,
        }
    ]


def _request_payloads() -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    sql = "SELECT numbers.n FROM numbers ORDER BY numbers.n"
    candidate = {
        "kind": "direct_sql",
        "sql": sql,
        "producer_revision": "quickstart-v1",
    }
    rejected_evaluate = {
        "schema_version": 1,
        "question": "Delete the synthetic numbers",
        "dialect": "sqlite",
        "candidate": {
            "kind": "direct_sql",
            "sql": "DELETE FROM numbers",
            "producer_revision": "quickstart-v1",
        },
        "allowed_tables": ["numbers"],
    }
    evaluate = {
        "schema_version": 1,
        "question": "List the synthetic numbers in ascending order",
        "dialect": "sqlite",
        "candidate": candidate,
        "allowed_tables": ["numbers"],
        "expected_result": {
            "columns": ["n"],
            "rows": [[1], [2], [3]],
        },
        "actual_result": {
            "columns": ["n"],
            "rows": [[1], [2], [3]],
        },
    }
    now = datetime.now(timezone.utc)
    enforce = {
        "schema_version": 1,
        "task_run_id": "tr_quickstart_" + uuid.uuid4().hex[:12],
        "purpose": "Demonstrate the public Trust Runtime Golden Path",
        "question": evaluate["question"],
        "principal_context": _principal(now),
        "delegated_mandate": None,
        "resource_scope": _resource_scope(),
        "candidate": candidate,
        "dialect": "sqlite",
    }
    return rejected_evaluate, evaluate, enforce


def _approval_payload(review: dict[str, Any]) -> dict[str, Any]:
    review_hashes = review.get("review")
    if not isinstance(review_hashes, dict):
        raise QuickstartError("Enforce prepare response omitted review hashes")
    return {
        "schema_version": 1,
        "approver_principal": {
            "principal_id": "quickstart-human",
            "principal_type": "human",
        },
        "sql_hash": review_hashes["sql_hash"],
        "assurance_report_hash": review_hashes["assurance_report_hash"],
        "enforcement_context_hash": review_hashes["enforcement_context_hash"],
    }


def _run_public_path(
    base_url: str,
    *,
    approve: Callable[[str], bool],
    progress: Callable[[str], None],
) -> dict[str, Any]:
    rejected_request, evaluate_request, enforce_request = _request_payloads()
    with httpx.Client(base_url=base_url, timeout=10) as client:
        rejection = _response_json(
            client.post("/api/v1/evaluate", json=rejected_request),
            "Fail-closed Evaluate",
        )
        failure = rejection.get("failure")
        if (
            rejection.get("status") != "failed"
            or not isinstance(failure, dict)
            or failure.get("stage") != "assurance"
            or failure.get("code") != "readonly_violation"
        ):
            raise QuickstartError("Evaluate did not fail closed on write SQL")
        progress("[1/6] Evaluate failed closed: assurance/readonly_violation")

        evaluation = _response_json(
            client.post("/api/v1/evaluate", json=evaluate_request),
            "Evaluate",
        )
        if evaluation.get("status") != "passed":
            raise QuickstartError("Evaluate did not pass the read-only candidate")
        progress("[2/6] Evaluate passed: candidate and exact result are reproducible")

        run_key = uuid.uuid4().hex
        review = _response_json(
            client.post(
                "/api/v1/enforce/query-runs",
                json=enforce_request,
                headers={"Idempotency-Key": f"quickstart-create-{run_key}"},
            ),
            "Enforce prepare",
        )
        if review.get("status") != "review_required":
            raise QuickstartError("Enforce did not stop for human review")
        sql = review.get("review", {}).get("sql")
        if not isinstance(sql, str) or not sql:
            raise QuickstartError("Enforce prepare response omitted the reviewed SQL")
        progress(f"[3/6] Enforce stopped for review\n\nSQL to execute:\n{sql}\n")
        if not approve(sql):
            raise QuickstartError("Approval declined; no SQL was executed")

        completed = _response_json(
            client.post(
                f"/api/v1/enforce/query-runs/{review['query_run_id']}/approve",
                json=_approval_payload(review),
                headers={"Idempotency-Key": f"quickstart-approve-{run_key}"},
            ),
            "Enforce approval",
        )
        if completed.get("status") != "completed":
            raise QuickstartError("Approved QueryRun did not complete")
        progress("[4/6] Enforce completed once with read-only and row-limit gates")

        explanation = _response_json(
            client.get(f"/api/v1/explain/query-runs/{review['query_run_id']}"),
            "Explain",
        )
        integrity = explanation.get("integrity", {}).get("status")
        if integrity != "verified":
            raise QuickstartError(f"Explain integrity is {integrity or 'missing'}")
        progress("[5/6] Explain verified: Evidence, lineage, and limitations are bound")

        dashboard = client.get("/admin/dashboard")
        if dashboard.status_code != 200 or review["query_run_id"] not in dashboard.text:
            raise QuickstartError("Dashboard did not project the completed QueryRun")
        progress("[6/6] Dashboard projected the same governed QueryRun")

    result = explanation["statement"]["result"]
    evidence_types = [item["evidence_type"] for item in explanation["evidence"]]
    limitation_codes = [item["code"] for item in explanation["limitations"]]
    return {
        "status": "passed",
        "query_run_id": review["query_run_id"],
        "fail_closed": {
            "status": rejection["status"],
            "failure": {
                "stage": failure["stage"],
                "code": failure["code"],
                "retryable": failure["retryable"],
            },
        },
        "evaluate": {
            "status": evaluation["status"],
            "policy_verdict": evaluation["policy"]["verdict"],
            "result_comparison": evaluation["result_comparison"]["status"],
        },
        "enforce": {
            "status": completed["status"],
            "rows": result["rows"],
            "truncated": result["truncated"],
        },
        "explain": {
            "integrity": integrity,
            "evidence_types": evidence_types,
            "limitations": limitation_codes,
        },
        "dashboard": {
            "status": "passed",
            "path": "/admin/dashboard",
        },
    }


def run_quickstart(
    *,
    workdir: Path | None,
    auto_approve: bool,
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
            for name in ("data.db", "query_runs.db", "registry.json")
            if (root / name).exists()
        ]
        if occupied:
            raise QuickstartError(
                f"Quickstart workdir must not contain: {', '.join(occupied)}"
            )
    try:
        _write_demo_inputs(root)

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
            summary = _run_public_path(base_url, approve=approve, progress=progress)
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
                hold_open(summary, base_url)
        return summary
    finally:
        if temporary is not None:
            temporary.cleanup()
