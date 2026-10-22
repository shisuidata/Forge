"""Consumer-level checks for the governed HTTP CLI contract."""

import json
import sys

import httpx
import pytest

from forge.cli import main


HASHES = {
    "schema_version": 2,
    "sql_hash": "a" * 64,
    "assurance_report_hash": "b" * 64,
    "enforcement_context_hash": "c" * 64,
}


def _invoke(monkeypatch, *args):
    monkeypatch.setattr(sys, "argv", ["forge", *args])
    main()


def test_execute_polls_same_run_after_claim_and_does_not_repost(tmp_path, monkeypatch, capsys):
    path = tmp_path / "action.json"
    path.write_text(json.dumps(HASHES), encoding="utf-8")
    monkeypatch.setenv("SYNTHETIC_FORGE_KEY", "synthetic-agent-key")
    posted = []
    reads = []

    def post(url, *, json, headers, timeout):
        posted.append((url, json, headers))
        return httpx.Response(202, json={"query_run_id": "qr_1", "status": "executing"})

    def get(url, *, headers, timeout):
        reads.append((url, headers))
        return httpx.Response(200, json={"query_run_id": "qr_1", "status": "completed", "result": {"rows": []}})

    monkeypatch.setattr(httpx, "post", post)
    monkeypatch.setattr(httpx, "get", get)
    _invoke(monkeypatch, "enforce", "--input", str(path), "--execute", "qr_1", "--idempotency-key", "execute-1", "--api-key-env", "SYNTHETIC_FORGE_KEY")
    assert len(posted) == len(reads) == 1
    assert posted[0][0] == reads[0][0] + "/execute"
    assert posted[0][1] == HASHES
    assert posted[0][2]["Idempotency-Key"] == "execute-1"
    assert posted[0][2]["X-API-Key"] == reads[0][1]["X-API-Key"] == "synthetic-agent-key"
    assert json.loads(capsys.readouterr().out)["status"] == "completed"


def test_approval_timeout_reports_unknown_without_replaying(tmp_path, monkeypatch, capsys):
    path = tmp_path / "action.json"
    path.write_text(json.dumps(HASHES), encoding="utf-8")
    posts = []
    reads = []

    def post(url, *, json, headers, timeout):
        posts.append(url)
        return httpx.Response(202, json={"query_run_id": "qr_2", "status": "executing"})

    def get(url, *, headers, timeout):
        reads.append(url)
        return httpx.Response(200, json={"query_run_id": "qr_2", "status": "executing"})

    monkeypatch.setattr(httpx, "post", post)
    monkeypatch.setattr(httpx, "get", get)
    with pytest.raises(SystemExit) as error:
        _invoke(monkeypatch, "enforce", str(path), "--approve", "qr_2", "--idempotency-key", "approve-1", "--timeout", "0.01")
    assert error.value.code != 0
    assert posts == ["http://127.0.0.1:8000/api/v2/enforce/query-runs/qr_2/approve"]
    assert all(url.endswith("/qr_2") for url in reads)
    error_output = capsys.readouterr().err
    assert "qr_2" in error_output
    assert "result unknown" in error_output


def test_mandate_issue_get_revoke_use_bound_identity(tmp_path, monkeypatch, capsys):
    payload = {"schema_version": 1, "task_run_id": "task_1"}
    path = tmp_path / "mandate.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv("SYNTHETIC_FORGE_KEY", "synthetic-admin-key")
    requests = []

    def post(url, *, json, headers, timeout):
        requests.append(("POST", url, json, headers))
        return httpx.Response(200, json={"mandate_id": "m_1", "status": "active"})

    def get(url, *, headers, timeout):
        requests.append(("GET", url, None, headers))
        return httpx.Response(200, json={"mandate_id": "m_1", "status": "active"})

    monkeypatch.setattr(httpx, "post", post)
    monkeypatch.setattr(httpx, "get", get)
    common = ("--api-key-env", "SYNTHETIC_FORGE_KEY")
    _invoke(monkeypatch, "governance", "mandate", "issue", "--input", str(path), "--idempotency-key", "issue-1", *common)
    _invoke(monkeypatch, "governance", "mandate", "get", "--id", "m_1", *common)
    _invoke(monkeypatch, "governance", "mandate", "revoke", "--id", "m_1", "--revision", "1", "--reason", "scope ended", *common)
    assert [item[0] for item in requests] == ["POST", "GET", "POST"]
    assert requests[0][1].endswith("/api/v2/governance/mandates")
    assert requests[0][2] == payload
    assert requests[1][1].endswith("/api/v2/governance/mandates/m_1")
    assert requests[2][1].endswith("/api/v2/governance/mandates/m_1/revoke")
    assert requests[2][2] == {"expected_state_revision": 1, "reason": "scope ended"}
    assert all(item[3]["X-API-Key"] == "synthetic-admin-key" for item in requests)
    assert "synthetic-admin-key" not in capsys.readouterr().out


def test_governed_commands_reject_literal_api_key(monkeypatch):
    for args in [("enforce", "--run-id", "qr_1"), ("explain", "qr_1"), ("governance", "mandate", "get", "--id", "m_1")]:
        with pytest.raises(SystemExit) as error:
            _invoke(monkeypatch, *args, "--api-key", "secret")
        assert error.value.code == 2
