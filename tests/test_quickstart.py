"""End-to-end governed quickstart against the isolated HTTP service."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import sys

import httpx
import pytest

from forge.cli import main as cli_main
from forge.quickstart import _server_environment, run_quickstart


def test_quickstart_human_review_and_private_credentials(tmp_path, monkeypatch):
    monkeypatch.setenv("AUTH_API_KEYS", "inherited-private-key")
    monkeypatch.setenv("DATABASE_URL", "sqlite:////private/customer.db")
    monkeypatch.setenv("LLM_API_KEY", "inherited-model-key")
    env = _server_environment(tmp_path)
    assert all(secret not in json.dumps(env) for secret in (
        "inherited-private-key", "/private/customer.db", "inherited-model-key"))
    assert env["FORGE_DISABLE_DOTENV"] == env["FORGE_DISABLE_YAML_CONFIG"] == "true"
    assert env["PI_ORCHESTRATOR_ENABLED"] == "false"

    messages = []

    def observe(summary, base_url):
        # A saved path is useful to a browser operator, but never discloses a key.
        admin_path = summary["credential_files"]["admin"]
        assert stat.S_IMODE(os.stat(admin_path).st_mode) == 0o600
        with httpx.Client(base_url=base_url, timeout=5) as client:
            assert client.get(summary["dashboard"]["path"]).status_code == 401
            login = client.post("/login", data={"api_key": Path(admin_path).read_text(),
                                                "next": summary["dashboard"]["path"]},
                                follow_redirects=True)
            assert login.status_code == 200
            assert summary["query_run_id"] in client.get(summary["dashboard"]["path"]).text

    summary = run_quickstart(workdir=tmp_path, auto_approve=True,
                             progress=messages.append, hold_open=observe)
    assert summary["status"] == "passed"
    assert summary["fail_closed"]["failure"]["code"] == "readonly_violation"
    assert summary["evaluate"]["status"] == "passed"
    assert summary["enforce"] == {"status": "completed", "rows": [[1], [2]],
                                  "truncated": True, "execution_mode": "human_review"}
    assert summary["explain"]["integrity"] == "verified"
    assert summary["explain"]["approval"]["approver_principal"]["principal_id"] == "quickstart-reviewer"
    assert {"mandate", "authorization", "approval"} <= set(summary["explain"]["evidence_types"])
    assert "result_truncated" in summary["explain"]["limitations"]
    receipt = summary["run_receipt"]
    body = {key: value for key, value in receipt.items() if key != "receipt_hash"}
    assert receipt["receipt_hash"] == "sha256:" + hashlib.sha256(json.dumps(
        body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    assert (tmp_path / "query_runs.db").is_file()
    assert json.loads((tmp_path / "summary.json").read_text())["query_run_id"] == summary["query_run_id"]
    outputs = json.dumps(summary) + (tmp_path / "server.log").read_text() + json.dumps(receipt)
    assert "inherited-private-key" not in outputs
    for name in ("admin", "reviewer", "agent"):
        key = (tmp_path / f"{name}.key").read_text()
        assert len(key) >= 40
        assert key not in outputs
        assert stat.S_IMODE((tmp_path / f"{name}.key").stat().st_mode) == 0o600
    assert not any("inherited-model-key" in message for message in messages)


def test_quickstart_agent_executes_autonomous_and_review_then_revoke(tmp_path):
    summary = run_quickstart(workdir=tmp_path, auto_approve=True,
                             agent_mode=True, progress=lambda _: None)
    assert summary["status"] == "passed"
    assert summary["enforce"]["execution_mode"] == "autonomous"
    assert summary["enforce"]["rows"] == [[1], [2]]
    assert summary["explain"]["approval"] is None
    assert {"mandate", "authorization"} <= set(summary["explain"]["evidence_types"])
    assert "approval" not in summary["explain"]["evidence_types"]
    assert summary["review"]["status"] == "completed"
    assert summary["review"]["approval_principal_id"] == "quickstart-reviewer"
    assert "approval" in summary["review"]["evidence_types"]
    assert summary["revocation"]["status"] == "revoked"
    assert summary["revocation"]["failure_code"] == "mandate_inactive"
    assert summary["revocation"]["new_prepare_status"] == 409


def test_quickstart_json_requires_noninteractive_approval(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["forge", "quickstart", "--json"])
    with pytest.raises(SystemExit) as caught:
        cli_main()
    assert caught.value.code == 2
    assert json.loads(capsys.readouterr().out) == {
        "status": "failed", "error": "--json requires --yes because approval is interactive"}
