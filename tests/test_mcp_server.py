"""Forge MCP server against a real isolated Forge server, through the official MCP client."""
from __future__ import annotations

import json
from pathlib import Path
import sys

import httpx
import pytest

pytest.importorskip("mcp")
from mcp import Client, StdioServerParameters  # noqa: E402

from forge import cli  # noqa: E402
from forge.mcp_server import ForgeGateway, build_server, demo_backend  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
TOOLS = {"forge_scope", "forge_prepare_query", "forge_execute_query",
         "forge_get_query_run", "forge_explain_query", "forge_cancel_query"}


@pytest.fixture(scope="module")
def backend(tmp_path_factory):
    with demo_backend(tmp_path_factory.mktemp("mcp") / "demo") as state:
        yield state


@pytest.fixture
def server(backend):
    return build_server(ForgeGateway(backend["base_url"], backend["agent_key"]), backend["mandate_id"])


def _key(backend, role: str) -> dict[str, str]:
    root = Path(backend["reviewer_key_file"]).parent
    return {"X-API-Key": (root / f"{role}.key").read_text(encoding="utf-8")}


async def _call(client, name: str, arguments: dict | None = None):
    result = await client.call_tool(name, arguments or {})
    text = "".join(getattr(block, "text", "") for block in result.content)
    return result, (result.structured_content if not result.is_error else None), text


def _approve(backend, run_id: str) -> None:
    """The human reviewer acting outside the agent session."""
    headers = _key(backend, "reviewer")
    run = httpx.get(f"{backend['base_url']}/api/v2/enforce/query-runs/{run_id}", headers=headers).json()
    hashes = {k: run["review"][k] for k in ("sql_hash", "assurance_report_hash", "enforcement_context_hash")}
    response = httpx.post(f"{backend['base_url']}/api/v2/enforce/query-runs/{run_id}/approve",
                          json={"schema_version": 2, **hashes},
                          headers={**headers, "Idempotency-Key": "test-approve-" + run_id})
    assert response.status_code in {200, 202}, response.text


async def test_tools_expose_no_approval_and_no_credential(server, backend):
    async with Client(server) as client:
        tools = (await client.list_tools()).tools
    assert {tool.name for tool in tools} == TOOLS
    listing = json.dumps([tool.model_dump(mode="json") for tool in tools])
    assert "approve" not in {tool.name for tool in tools}
    assert backend["agent_key"] not in listing and "api_key" not in listing.lower()
    assert {tool.name for tool in tools if tool.annotations and tool.annotations.read_only_hint} == {
        "forge_scope", "forge_get_query_run", "forge_explain_query"}


async def test_autonomous_table_prepares_executes_and_explains(server, backend):
    async with Client(server) as client:
        _, scope, _ = await _call(client, "forge_scope")
        assert scope["autonomous_tables"] == ["numbers"] and scope["allowed_tables"] == ["numbers", "secrets"]
        _, prepared, _ = await _call(client, "forge_prepare_query", {
            "sql": "SELECT numbers.n FROM numbers ORDER BY numbers.n", "question": "List numbers"})
        assert prepared["status"] == "ready" and prepared["execution_mode"] == "autonomous"
        _, executed, text = await _call(client, "forge_execute_query", {"query_run_id": prepared["query_run_id"]})
        assert executed["status"] == "completed" and executed["result"]["rows"] == [[1], [2]]
        # A retried execute replays the original claim instead of running the SQL again.
        _, replay, _ = await _call(client, "forge_execute_query", {"query_run_id": prepared["query_run_id"]})
        assert replay["status"] == "completed"
        _, explained, _ = await _call(client, "forge_explain_query", {"query_run_id": prepared["query_run_id"]})
    assert explained["integrity"] == "verified"
    assert {"mandate", "authorization", "result"} <= set(explained["evidence_types"])
    assert explained["limitations"] and backend["agent_key"] not in text


async def test_review_table_waits_for_the_human_reviewer(server, backend):
    async with Client(server) as client:
        _, prepared, _ = await _call(client, "forge_prepare_query", {
            "sql": "SELECT COUNT(*) AS n FROM secrets", "question": "Count secrets"})
        assert prepared["status"] == "review_required"
        assert f"forge review {prepared['query_run_id']}" in prepared["next_step"]
        refused, _, text = await _call(client, "forge_execute_query", {"query_run_id": prepared["query_run_id"]})
        assert refused.is_error and "query_run_status_conflict" in text
        _approve(backend, prepared["query_run_id"])
        _, after, _ = await _call(client, "forge_get_query_run", {"query_run_id": prepared["query_run_id"]})
        for _ in range(20):
            if after["status"] == "completed":
                break
            _, after, _ = await _call(client, "forge_get_query_run", {"query_run_id": prepared["query_run_id"]})
        _, explained, _ = await _call(client, "forge_explain_query", {"query_run_id": prepared["query_run_id"]})
    assert after["status"] == "completed" and after["result"]["row_count"] == 1
    assert explained["authorization"]["approver_principal"]["principal_id"] == "quickstart-reviewer"


async def test_denied_sql_returns_failed_gates_and_cancel_is_bounded(server):
    async with Client(server) as client:
        _, denied, _ = await _call(client, "forge_prepare_query", {"sql": "DELETE FROM numbers", "question": "x"})
        _, outside, _ = await _call(client, "forge_prepare_query", {"sql": "SELECT * FROM other", "question": "x"})
        _, pending, _ = await _call(client, "forge_prepare_query", {
            "sql": "SELECT COUNT(*) AS n FROM secrets", "question": "Count secrets"})
        _, cancelled, _ = await _call(client, "forge_cancel_query", {"query_run_id": pending["query_run_id"]})
        again, _, text = await _call(client, "forge_execute_query", {"query_run_id": pending["query_run_id"]})
    assert denied["status"] == "denied" and [g["gate"] for g in denied["failed_gates"]] == ["sql_safety"]
    assert outside["status"] == "denied" and outside["failed_gates"]
    assert cancelled["status"] == "cancelled" and again.is_error and "query_run_status_conflict" in text


async def test_revoked_mandate_stops_new_prepares(tmp_path):
    with demo_backend(tmp_path / "demo") as backend:
        server = build_server(ForgeGateway(backend["base_url"], backend["agent_key"]), backend["mandate_id"])
        admin = _key(backend, "admin")
        issued = httpx.get(f"{backend['base_url']}/api/v2/governance/mandates/{backend['mandate_id']}", headers=admin).json()
        httpx.post(f"{backend['base_url']}/api/v2/governance/mandates/{backend['mandate_id']}/revoke",
                   json={"expected_state_revision": issued["state_revision"], "reason": "test"},
                   headers=admin).raise_for_status()
        async with Client(server) as client:
            refused, _, text = await _call(client, "forge_prepare_query", {
                "sql": "SELECT numbers.n FROM numbers", "question": "x"})
    assert refused.is_error and ("mandate_inactive" in text or "mandate_not_found" in text)


async def test_lost_create_response_recovers_the_same_run():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.headers.get("Idempotency-Key"), str(request.url)))
        if request.method == "POST":
            raise httpx.ReadTimeout("lost", request=request)
        return httpx.Response(200, json={"query_run_id": "qr_1", "status": "ready"})

    gateway = ForgeGateway("http://forge.test", "k", transport=httpx.MockTransport(handler))
    recovered = await gateway.prepare({"schema_version": 2})
    await gateway.aclose()
    assert recovered["query_run_id"] == "qr_1"
    create_key = seen[0][1]
    assert seen[1][0] == "GET" and f"create_idempotency_key={create_key}" in seen[1][2]


def test_demo_refuses_to_reuse_a_foreign_directory(tmp_path):
    (tmp_path / "mine").mkdir()
    (tmp_path / "mine" / "notes.txt").write_text("keep", encoding="utf-8")
    with pytest.raises(RuntimeError, match="not created by"):
        with demo_backend(tmp_path / "mine"):
            pass
    assert (tmp_path / "mine" / "notes.txt").exists()


def test_review_cli_requires_an_interactive_human(backend, monkeypatch, capsys):
    async def prepare():
        server = build_server(ForgeGateway(backend["base_url"], backend["agent_key"]), backend["mandate_id"])
        async with Client(server) as client:
            return (await _call(client, "forge_prepare_query", {
                "sql": "SELECT COUNT(*) AS n FROM secrets", "question": "Count secrets"}))[1]

    import asyncio
    run_id = asyncio.run(prepare())["query_run_id"]
    workdir = str(Path(backend["reviewer_key_file"]).parent)
    argv = ["forge", "review", run_id, "--demo", "--workdir", workdir]
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: False, raising=False)
    with pytest.raises(SystemExit) as refused:
        cli.main()
    assert refused.value.code == 2 and "interactive terminal" in capsys.readouterr().err

    monkeypatch.setattr(sys.stdin, "isatty", lambda: True, raising=False)
    monkeypatch.setattr("builtins.input", lambda _prompt: "y")
    cli.main()
    out = capsys.readouterr().out
    assert "SELECT COUNT(*) AS n FROM secrets" in out and f"{run_id}: completed, 1 row(s)" in out


async def test_stdio_demo_end_to_end(tmp_path):
    params = StdioServerParameters(command=sys.executable, args=[
        "-m", "forge.cli", "mcp", "--demo", "--workdir", str(tmp_path / "demo")], cwd=str(ROOT))
    async with Client(params, read_timeout_seconds=60) as client:
        assert {tool.name for tool in (await client.list_tools()).tools} == TOOLS
        _, prepared, _ = await _call(client, "forge_prepare_query", {
            "sql": "SELECT numbers.n FROM numbers ORDER BY numbers.n", "question": "List numbers"})
        _, executed, _ = await _call(client, "forge_execute_query", {"query_run_id": prepared["query_run_id"]})
    assert executed["status"] == "completed" and executed["result"]["row_count"] == 2


@pytest.mark.parametrize("stop", ["close_stdin", "sigterm"])
def test_demo_server_stops_with_the_mcp_process(tmp_path, stop):
    import signal
    import subprocess
    import time

    workdir = tmp_path / "demo"
    process = subprocess.Popen([sys.executable, "-m", "forge.cli", "mcp", "--demo", "--workdir", str(workdir)],
                               cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + 30
        while not (workdir / "demo.json").exists() and time.monotonic() < deadline:
            time.sleep(0.1)
        base_url = json.loads((workdir / "demo.json").read_text(encoding="utf-8"))["base_url"]
        assert httpx.get(f"{base_url}/health", timeout=2).status_code == 200
        if stop == "close_stdin":
            process.stdin.close()
        else:
            process.send_signal(signal.SIGTERM)
        process.wait(timeout=20)
    finally:
        if process.poll() is None:
            process.kill()
    with pytest.raises(httpx.HTTPError):
        httpx.get(f"{base_url}/health", timeout=2)
