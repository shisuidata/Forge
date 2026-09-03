"""End-to-end proof for the public Trust Runtime quickstart."""
from __future__ import annotations

import hashlib
import json
import sys

import httpx
import pytest

from forge.cli import main as cli_main
from forge.quickstart import run_quickstart


def test_quickstart_completes_public_trust_runtime_path(tmp_path):
    messages: list[str] = []
    live_dashboard: list[str] = []

    def observe_dashboard(summary: dict, base_url: str) -> None:
        response = httpx.get(base_url + summary["dashboard"]["path"], timeout=5)
        response.raise_for_status()
        assert summary["query_run_id"] in response.text
        live_dashboard.append(base_url)

    summary = run_quickstart(
        workdir=tmp_path,
        auto_approve=True,
        progress=messages.append,
        hold_open=observe_dashboard,
    )

    assert summary["status"] == "passed"
    assert summary["fail_closed"] == {
        "status": "failed",
        "failure": {
            "stage": "assurance",
            "code": "readonly_violation",
            "retryable": False,
        },
    }
    assert summary["evaluate"] == {
        "status": "passed",
        "policy_verdict": "allow_review",
        "result_comparison": "passed",
    }
    assert summary["enforce"] == {
        "status": "completed",
        "rows": [[1], [2]],
        "truncated": True,
    }
    assert summary["explain"]["integrity"] == "verified"
    assert summary["explain"]["evidence_types"] == [
        "candidate",
        "policy",
        "query",
        "assurance",
        "source",
        "approval",
        "result",
    ]
    assert "result_truncated" in summary["explain"]["limitations"]
    assert summary["dashboard"] == {
        "status": "passed",
        "path": "/admin/dashboard",
    }
    assert len(live_dashboard) == 1
    assert messages[0] == "[1/6] Evaluate failed closed: assurance/readonly_violation"
    assert messages[2].startswith("[3/6] Enforce stopped for review")
    assert "SQL to execute:" in messages[2]
    assert (tmp_path / "query_runs.db").is_file()
    receipt = summary["run_receipt"]
    assert receipt["schema_version"] == 1
    assert receipt["forge_version"]
    assert receipt["runtime_duration_ms"] > 0
    assert receipt["outcome"]["fail_closed"] == summary["fail_closed"]
    assert set(receipt["environment"]) == {
        "system",
        "release",
        "machine",
        "python_implementation",
        "python_version",
    }
    receipt_body = {key: value for key, value in receipt.items() if key != "receipt_hash"}
    canonical = json.dumps(
        receipt_body,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    assert receipt["receipt_hash"] == "sha256:" + hashlib.sha256(canonical).hexdigest()
    assert (tmp_path / "server.log").is_file()
    persisted = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert persisted["query_run_id"] == summary["query_run_id"]
    assert persisted["run_receipt"]["receipt_hash"] == receipt["receipt_hash"]


def test_quickstart_json_requires_noninteractive_approval(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["forge", "quickstart", "--json"])

    with pytest.raises(SystemExit) as caught:
        cli_main()

    assert caught.value.code == 2
    assert json.loads(capsys.readouterr().out) == {
        "status": "failed",
        "error": "--json requires --yes because approval is interactive",
    }
