"""MCP entry for existing agents: the agent's own credential against the governed v2 API.

The server holds no QueryRun state and makes no authorization decision. Forge decides
what the agent may prepare and execute; anything outside the Mandate's autonomous
tables waits for the designated human reviewer, and no tool can approve.
"""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import shutil
import sys
import time
from typing import Any, Iterator
import uuid

import httpx

_TERMINAL = {"completed", "failed", "cancelled", "expired", "denied"}
_DEMO_MARKER = ".forge-mcp-demo"
_HINTS = {
    "mandate_inactive": "The Mandate is revoked or expired; ask an admin to issue a new one.",
    "mandate_not_found": "FORGE_MANDATE_ID is unknown or not issued to this agent.",
    "invalid_credential": "FORGE_API_KEY is not a valid credential for this Forge server.",
    "credential_revoked": "This agent's credential has been revoked.",
    "actor_not_authorized": "Only an autonomous QueryRun created by this agent can be executed by it.",
    "reviewer_not_authorized": "This QueryRun needs the designated human reviewer.",
    "query_run_status_conflict": "The QueryRun is not in a state that allows this action; read it first.",
    "review_expired": "The review window expired; prepare the query again.",
    "execution_unavailable": "The Forge server has execution disabled or an unconfirmed read-only datasource.",
}


class ForgeRefusal(RuntimeError):
    """A bounded Forge answer the agent can act on; never carries credentials."""

    def __init__(self, code: str, detail: Any = None):
        self.code = code
        self.detail = detail
        message = f"Forge refused: {code}"
        if code in _HINTS:
            message += f". {_HINTS[code]}"
        if detail:
            message += f" Details: {json.dumps(detail, ensure_ascii=False)}"
        super().__init__(message)


def _failure_code(response: httpx.Response, payload: Any) -> str:
    if isinstance(payload, dict):
        failure = payload.get("failure")
        if isinstance(failure, dict) and failure.get("code"):
            return str(failure["code"])
        for key in ("code", "detail", "error"):
            if isinstance(payload.get(key), str):
                return payload[key]
    return f"http_{response.status_code}"


def _failed_gates(run: dict[str, Any]) -> list[dict[str, Any]]:
    gates = (run.get("assurance") or {}).get("gates") or []
    return [{"gate": gate.get("gate"), "diagnostics": gate.get("diagnostics") or []}
            for gate in gates if gate.get("status") == "failed"]


class ForgeGateway:
    """Thin async client for the endpoints an agent principal may call."""

    def __init__(self, base_url: str, api_key: str, *, timeout: float = 30.0,
                 transport: httpx.AsyncBaseTransport | None = None):
        self._client = httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=timeout,
                                         headers={"X-API-Key": api_key}, transport=transport)
        self.timeout = timeout

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _call(self, method: str, path: str, *, body: Any = None,
                    idempotency_key: str | None = None) -> dict[str, Any]:
        headers = {"Idempotency-Key": idempotency_key} if idempotency_key else None
        response = await self._client.request(method, path, json=body, headers=headers)
        try:
            payload = response.json()
        except ValueError:
            payload = None
        if response.is_error or not isinstance(payload, dict) or payload.get("status") == "error":
            raise ForgeRefusal(_failure_code(response, payload))
        return payload

    async def mandate(self, mandate_id: str) -> dict[str, Any]:
        return await self._call("GET", f"/api/v2/governance/mandates/{mandate_id}")

    async def prepare(self, request: dict[str, Any]) -> dict[str, Any]:
        key = "mcp-create-" + uuid.uuid4().hex
        try:
            return await self._call("POST", "/api/v2/enforce/query-runs", body=request, idempotency_key=key)
        except httpx.TransportError:
            # The create may have landed; recover it instead of creating a second run.
            try:
                return await self._call("GET", f"/api/v2/enforce/query-runs?create_idempotency_key={key}")
            except (ForgeRefusal, httpx.HTTPError):
                raise ForgeRefusal("result_unknown", {"create_idempotency_key": key}) from None

    async def get(self, query_run_id: str) -> dict[str, Any]:
        return await self._call("GET", f"/api/v2/enforce/query-runs/{query_run_id}")

    async def execute(self, run: dict[str, Any]) -> dict[str, Any]:
        review = run["review"]
        action = {"schema_version": 2, **{field: review[field] for field in (
            "sql_hash", "assurance_report_hash", "enforcement_context_hash")}}
        # One key per QueryRun: a retried call replays the original claim, never a second execution.
        claimed = await self._call("POST", f"/api/v2/enforce/query-runs/{run['query_run_id']}/execute",
                                   body=action, idempotency_key="mcp-execute-" + run["query_run_id"])
        return await self.wait(claimed)

    async def wait(self, run: dict[str, Any]) -> dict[str, Any]:
        deadline = time.monotonic() + self.timeout
        while run["status"] not in _TERMINAL:
            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.2)
            run = await self.get(run["query_run_id"])
        return run

    async def cancel(self, query_run_id: str) -> dict[str, Any]:
        return await self._call("POST", f"/api/v2/enforce/query-runs/{query_run_id}/cancel", body={})

    async def explain(self, query_run_id: str) -> dict[str, Any]:
        return await self._call("GET", f"/api/v2/explain/query-runs/{query_run_id}")


def _scope(mandate: dict[str, Any]) -> dict[str, Any]:
    delegated, policy = mandate["delegated_mandate"], mandate["policy"]
    datasource = next((item["resource_id"] for item in delegated["resource_scope"]
                       if item["resource_type"] == "datasource"), None)
    return {
        "mandate_id": mandate["mandate_id"], "status": mandate["status"],
        "task_run_id": delegated["task_run_id"], "purpose": delegated["purpose"],
        "datasource_id": datasource, "expires_at": delegated["expires_at"],
        "allowed_tables": policy["allowed_tables"], "autonomous_tables": policy["autonomous_tables"],
        "reviewer_principal_id": policy["reviewer_principal_id"],
        "accountable_principal_id": delegated["accountable_principal"]["principal_id"],
    }


def _run_view(run: dict[str, Any], review_command: str) -> dict[str, Any]:
    view: dict[str, Any] = {
        "query_run_id": run["query_run_id"], "status": run["status"],
        "execution_mode": run["decision"]["execution_mode"], "sql": run["review"]["sql"],
    }
    status = run["status"]
    if status == "ready":
        view["review_expires_at"] = run["review"]["expires_at"]
        view["next_step"] = "Within the autonomous scope: call forge_execute_query to run it once."
    elif status == "review_required":
        view["review_expires_at"] = run["review"]["expires_at"]
        view["next_step"] = ("Needs the designated human reviewer; you cannot approve it. Ask the user to run "
                             f"`{review_command} {run['query_run_id']}` in their own terminal, then call "
                             "forge_get_query_run to read the result.")
    elif status == "executing":
        view["next_step"] = "Still executing; call forge_get_query_run again."
    if run.get("result"):
        view["result"] = {key: run["result"][key] for key in ("columns", "rows", "row_count", "truncated")}
    if run.get("failure") and status in {"failed", "cancelled", "expired", "denied"}:
        view["failure"] = run["failure"]
    if status == "denied":
        view["failed_gates"] = _failed_gates(run)
    return view


def build_server(gateway: ForgeGateway, mandate_id: str, *, review_command: str = "forge review",
                 instructions_suffix: str = ""):
    from mcp.server import MCPServer
    from mcp.server.mcpserver.exceptions import ToolError
    from mcp.types import ToolAnnotations

    server = MCPServer(
        "forge",
        instructions=(
            "Forge governs this agent's read-only SQL. Call forge_scope first to see which tables you may "
            "query and which run without review. Prepare SQL with forge_prepare_query; execute only runs "
            "whose status is ready. Runs needing review must be approved by a human outside this session. "
            "Denied runs list failed gates: fix the SQL and prepare again." + instructions_suffix
        ),
    )
    reading = ToolAnnotations(read_only_hint=True, open_world_hint=False)

    async def call(coroutine):
        try:
            return await coroutine
        except ForgeRefusal as exc:
            raise ToolError(str(exc)) from None
        except httpx.HTTPError as exc:
            raise ToolError(f"Forge unreachable ({type(exc).__name__}); the outcome of a write is unknown, "
                            "read the QueryRun before retrying.") from None

    async def scope() -> dict[str, Any]:
        return _scope(await gateway.mandate(mandate_id))

    @server.tool(annotations=reading)
    async def forge_scope() -> dict[str, Any]:
        """Show this agent's Mandate: task, purpose, datasource, allowed tables, the autonomous subset, reviewer and expiry."""
        return await call(scope())

    @server.tool(annotations=ToolAnnotations(read_only_hint=False, destructive_hint=False,
                                             idempotent_hint=False, open_world_hint=False))
    async def forge_prepare_query(sql: str, question: str) -> dict[str, Any]:
        """Submit one read-only SQL query for Forge to check under this agent's Mandate. Nothing executes.

        Returns status ready (you may execute), review_required (a human must approve), or denied with the
        failed gates. Qualify columns with their table names.
        """
        async def run() -> dict[str, Any]:
            bound = await scope()
            prepared = await gateway.prepare({
                "schema_version": 2, "mandate_id": mandate_id, "task_run_id": bound["task_run_id"],
                "purpose": bound["purpose"], "question": question, "dialect": "auto",
                "candidate": {"kind": "direct_sql", "sql": sql, "producer_revision": "forge-mcp"},
            })
            return _run_view(prepared, review_command)
        return await call(run())

    @server.tool(annotations=ToolAnnotations(read_only_hint=False, destructive_hint=False,
                                             idempotent_hint=True, open_world_hint=False))
    async def forge_execute_query(query_run_id: str) -> dict[str, Any]:
        """Execute a QueryRun whose status is ready, once, and return its bounded result."""
        async def run() -> dict[str, Any]:
            return _run_view(await gateway.execute(await gateway.get(query_run_id)), review_command)
        return await call(run())

    @server.tool(annotations=reading)
    async def forge_get_query_run(query_run_id: str) -> dict[str, Any]:
        """Read a QueryRun's status, SQL and result (for example after a human approved it)."""
        async def run() -> dict[str, Any]:
            return _run_view(await gateway.get(query_run_id), review_command)
        return await call(run())

    @server.tool(annotations=reading)
    async def forge_explain_query(query_run_id: str) -> dict[str, Any]:
        """Read the Evidence for a QueryRun: integrity, executed SQL, who authorized it, and stated limitations."""
        async def run() -> dict[str, Any]:
            explanation = await gateway.explain(query_run_id)
            governance = explanation["governance"]
            return {
                "query_run_id": explanation["query_run_id"], "status": explanation["status"],
                "integrity": explanation["integrity"]["status"],
                "actual_sql": explanation["statement"]["actual_sql"],
                "authorization": governance.get("approval") or governance.get("authorization"),
                "evidence_types": [item["evidence_type"] for item in explanation["evidence"]],
                "limitations": [{"code": item["code"], "description": item["description"]}
                                for item in explanation["limitations"]],
            }
        return await call(run())

    @server.tool(annotations=ToolAnnotations(read_only_hint=False, destructive_hint=False,
                                             idempotent_hint=True, open_world_hint=False))
    async def forge_cancel_query(query_run_id: str) -> dict[str, Any]:
        """Cancel a QueryRun that has not started executing."""
        async def run() -> dict[str, Any]:
            return _run_view(await gateway.cancel(query_run_id), review_command)
        return await call(run())

    return server


def default_demo_dir() -> Path:
    return Path.home() / ".forge" / "mcp-demo"


def _reset_demo_dir(root: Path) -> None:
    if root.exists():
        if not (root / _DEMO_MARKER).exists():
            raise RuntimeError(f"{root} exists and was not created by `forge mcp --demo`; choose another --workdir")
        shutil.rmtree(root)
    root.mkdir(parents=True)
    (root / _DEMO_MARKER).write_text("Synthetic Forge MCP demo; safe to delete.\n", encoding="utf-8")


@contextmanager
def demo_backend(root: Path) -> Iterator[dict[str, str]]:
    """Fresh synthetic Forge server plus a Mandate: `numbers` runs autonomously, `secrets` needs review."""
    from forge.quickstart import _response_json, isolated_runtime

    _reset_demo_dir(root)
    with isolated_runtime(root) as base_url:
        admin = {"X-API-Key": (root / "admin.key").read_text(encoding="utf-8")}
        task_run_id = "tr_mcp_demo_" + uuid.uuid4().hex[:12]
        issued = _response_json(httpx.post(f"{base_url}/api/v2/governance/mandates", json={
            "schema_version": 1, "task_run_id": task_run_id,
            "delegate_principal_id": "quickstart-agent",
            "accountable_principal_id": "quickstart-admin",
            "reviewer_principal_id": "quickstart-reviewer",
            "purpose": "Try Forge from an MCP client on synthetic data",
            "datasource_id": "quickstart", "allowed_tables": ["numbers", "secrets"],
            "autonomous_tables": ["numbers"],
            "expires_at": (datetime.now(timezone.utc) + timedelta(hours=12)).isoformat(),
        }, headers={**admin, "Idempotency-Key": "mcp-demo-" + task_run_id}, timeout=10), "Issue demo mandate")
        state = {"base_url": base_url, "mandate_id": issued["mandate_id"],
                 "reviewer_key_file": str(root / "reviewer.key")}
        (root / "demo.json").write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
        yield {**state, "agent_key": (root / "agent.key").read_text(encoding="utf-8")}


def demo_state(root: Path) -> dict[str, str]:
    try:
        return json.loads((root / "demo.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"No running `forge mcp --demo` found in {root}") from exc


def serve(*, demo: bool, workdir: Path | None) -> None:
    """Run over stdio. stdout belongs to the MCP protocol, so all human guidance goes to stderr."""
    try:
        import mcp  # noqa: F401
    except ImportError:
        print("forge mcp needs the optional MCP SDK. Install Forge with its [mcp] extra from your checkout "
              "or distribution, e.g. pip install -e '.[mcp]' (the PyPI package named forge is unrelated).",
              file=sys.stderr)
        raise SystemExit(2)
    if demo:
        root = workdir or default_demo_dir()
        try:
            with demo_backend(root) as backend:
                review = "forge review --demo" + ("" if workdir is None else f" --workdir {root}")
                print(f"Forge MCP demo: synthetic server {backend['base_url']} (Dashboard: "
                      f"{backend['base_url']}/admin/dashboard). Approve review-only runs with "
                      f"`{review} <query_run_id>`.", file=sys.stderr)
                _run_stdio(backend["base_url"], backend["agent_key"], backend["mandate_id"], review,
                           " This is a local synthetic demo: table numbers runs autonomously, "
                           "table secrets needs review.")
        except SystemExit as exc:
            # Terminated by the client: the demo server is already stopped, but the stdio reader
            # thread can still be blocked on stdin and would hold interpreter shutdown open.
            sys.stderr.flush()
            os._exit(exc.code if isinstance(exc.code, int) else 1)
        return
    base_url = os.environ.get("FORGE_BASE_URL", "http://127.0.0.1:8000")
    api_key, mandate_id = os.environ.get("FORGE_API_KEY", ""), os.environ.get("FORGE_MANDATE_ID", "")
    if not api_key or not mandate_id:
        print("forge mcp needs FORGE_API_KEY (the agent's credential) and FORGE_MANDATE_ID; "
              "use `forge mcp --demo` to try it on synthetic data.", file=sys.stderr)
        raise SystemExit(2)
    _run_stdio(base_url, api_key, mandate_id, "forge review", "")


def _run_stdio(base_url: str, api_key: str, mandate_id: str, review_command: str, suffix: str) -> None:
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per request would flood client logs
    gateway = ForgeGateway(base_url, api_key)
    build_server(gateway, mandate_id, review_command=review_command, instructions_suffix=suffix).run("stdio")
