from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest


def _registry_files(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    schema = tmp_path / "schema.registry.json"
    metrics = tmp_path / "metrics.registry.yaml"
    disambiguations = tmp_path / "disambiguations.registry.yaml"
    conventions = tmp_path / "field_conventions.registry.yaml"
    schema.write_text('{"tables":{"orders":{"columns":{"id":{}}}}}', encoding="utf-8")
    for path in (metrics, disambiguations, conventions):
        path.write_text("{}", encoding="utf-8")
    return schema, metrics, disambiguations, conventions


def _configure_minimal(monkeypatch, tmp_path: Path) -> None:
    from config import cfg

    schema, metrics, disambiguations, conventions = _registry_files(tmp_path)
    monkeypatch.setattr(cfg, "REGISTRY_PATH", schema)
    monkeypatch.setattr(cfg, "METRICS_PATH", metrics)
    monkeypatch.setattr(cfg, "DISAMBIGUATIONS_PATH", disambiguations)
    monkeypatch.setattr(cfg, "CONVENTIONS_PATH", conventions)
    monkeypatch.setattr(cfg, "AUDIT_DB_PATH", str(tmp_path / "audit.db"))
    monkeypatch.setattr(cfg, "EXECUTION_MAX_ROWS", 200)
    monkeypatch.setattr(cfg, "EXECUTION_TIMEOUT_SECONDS", 30)


def test_readiness_profiles_have_distinct_gates(monkeypatch, tmp_path):
    from config import cfg
    from forge.readiness import readiness_payload

    _configure_minimal(monkeypatch, tmp_path)
    monkeypatch.setattr(cfg, "AUTH_ENABLED", False)
    monkeypatch.setattr(cfg, "AUTH_ADMIN_PASSWORD", "")
    monkeypatch.setattr(cfg, "AUTH_COOKIE_SECURE", False)
    monkeypatch.setattr(cfg, "LLM_API_KEY", "")
    monkeypatch.setattr(cfg, "EXECUTION_ENABLED", True)
    monkeypatch.setattr(cfg, "DATABASE_URL", "")
    monkeypatch.setattr(cfg, "DATABASE_READONLY_CONFIRMED", False)
    monkeypatch.setattr(cfg, "RAW_SQL_ENABLED", True)

    assert readiness_payload("dev")["status"] == "warn"
    assert readiness_payload("poc")["status"] == "fail"
    assert readiness_payload("prod")["status"] == "fail"

    monkeypatch.setattr(cfg, "AUTH_ENABLED", True)
    monkeypatch.setattr(cfg, "AUTH_ADMIN_PASSWORD", "strong-password")
    monkeypatch.setattr(cfg, "AUTH_COOKIE_SECURE", True)
    monkeypatch.setattr(cfg, "LLM_API_KEY", "sk-test")
    monkeypatch.setattr(cfg, "DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setattr(cfg, "DATABASE_READONLY_CONFIRMED", True)
    monkeypatch.setattr(cfg, "RAW_SQL_ENABLED", False)

    assert readiness_payload("prod")["status"] == "ok"


def test_poc_and_prod_reject_bundled_test_registry(monkeypatch, tmp_path):
    from config import cfg
    from forge.readiness import readiness_payload

    test_registry_dir = tmp_path / "tests" / "datasets" / "large"
    test_registry_dir.mkdir(parents=True)
    schema = test_registry_dir / "schema.registry.json"
    metrics = test_registry_dir / "metrics.registry.yaml"
    disambiguations = test_registry_dir / "disambiguations.registry.yaml"
    conventions = test_registry_dir / "field_conventions.registry.yaml"
    schema.write_text('{"tables":{"orders":{"columns":{"id":{}}}}}', encoding="utf-8")
    for path in (metrics, disambiguations, conventions):
        path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(cfg, "REGISTRY_PATH", schema)
    monkeypatch.setattr(cfg, "METRICS_PATH", metrics)
    monkeypatch.setattr(cfg, "DISAMBIGUATIONS_PATH", disambiguations)
    monkeypatch.setattr(cfg, "CONVENTIONS_PATH", conventions)
    monkeypatch.setattr(cfg, "AUDIT_DB_PATH", str(tmp_path / "audit.db"))
    monkeypatch.setattr(cfg, "AUTH_ENABLED", True)
    monkeypatch.setattr(cfg, "AUTH_ADMIN_PASSWORD", "strong-password")
    monkeypatch.setattr(cfg, "AUTH_COOKIE_SECURE", True)
    monkeypatch.setattr(cfg, "LLM_API_KEY", "sk-test")
    monkeypatch.setattr(cfg, "EXECUTION_ENABLED", True)
    monkeypatch.setattr(cfg, "DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setattr(cfg, "DATABASE_READONLY_CONFIRMED", True)
    monkeypatch.setattr(cfg, "RAW_SQL_ENABLED", False)
    monkeypatch.setattr(cfg, "EXECUTION_MAX_ROWS", 200)
    monkeypatch.setattr(cfg, "EXECUTION_TIMEOUT_SECONDS", 30)

    poc_payload = readiness_payload("poc")
    prod_payload = readiness_payload("prod")
    dev_payload = readiness_payload("dev")

    assert poc_payload["status"] == "fail"
    assert prod_payload["status"] == "fail"
    assert dev_payload["status"] == "warn"
    assert next(c for c in poc_payload["checks"] if c["name"] == "registry")["status"] == "fail"


def test_doctor_json_cli_does_not_emit_secrets(monkeypatch, tmp_path, capsys):
    from config import cfg
    from forge import cli

    _configure_minimal(monkeypatch, tmp_path)
    monkeypatch.setattr(cfg, "AUTH_ENABLED", False)
    monkeypatch.setattr(cfg, "LLM_API_KEY", "sk-secret-value")
    monkeypatch.setattr(cfg, "EXECUTION_ENABLED", False)
    monkeypatch.setattr(cfg, "DATABASE_URL", "")
    monkeypatch.setattr(cfg, "RAW_SQL_ENABLED", False)
    monkeypatch.setattr(sys, "argv", ["forge", "doctor", "--profile", "dev", "--json"])

    cli.main()
    payload = json.loads(capsys.readouterr().out)

    assert payload["profile"] == "dev"
    assert payload["status"] in {"ok", "warn"}
    assert "sk-secret-value" not in json.dumps(payload, ensure_ascii=False)


def test_poc_init_validate_and_report(tmp_path):
    from forge import poc

    workspace = tmp_path / "customer-poc"
    init_result = poc.init_workspace(workspace)

    assert init_result["ok"] is True
    assert (workspace / "cases.json").exists()
    assert (workspace / "delivery_report.md").exists()
    assert (workspace / "failure_triage.md").exists()
    assert (workspace / "results").is_dir()
    assert poc.validate_workspace(workspace)["status"] == "ok"

    (workspace / "results" / "doctor.json").write_text('{"status":"ok"}', encoding="utf-8")
    (workspace / "results" / "provider-smoke.json").write_text('{"status":"ok"}', encoding="utf-8")
    (workspace / "results" / "database-smoke.json").write_text('{"status":"ok"}', encoding="utf-8")
    (workspace / "results" / "production-smoke.json").write_text('{"status":"ok"}', encoding="utf-8")
    (workspace / "results" / "ea.json").write_text(
        '{"status":"ok","case_ea_any":"100%","case_ea_all":"90%","run_acc":"95%"}',
        encoding="utf-8",
    )

    report = poc.write_report(workspace)

    assert report["status"] == "pass"
    assert "Recommendation: pass" in (workspace / "delivery_report.md").read_text(encoding="utf-8")


def test_poc_validate_reports_missing_reference_sql(tmp_path):
    from forge import poc

    workspace = tmp_path / "customer-poc"
    poc.init_workspace(workspace)
    cases = json.loads((workspace / "cases.json").read_text(encoding="utf-8"))
    cases[0]["reference_sql"] = ""
    (workspace / "cases.json").write_text(json.dumps(cases), encoding="utf-8")

    result = poc.validate_workspace(workspace)

    assert result["status"] == "fail"
    assert any("reference_sql" in issue["message"] for issue in result["issues"])


def test_provider_smoke_writes_fixed_json_shape(monkeypatch, tmp_path, capsys):
    import scripts.provider_smoke as provider_smoke
    from config import cfg

    monkeypatch.setattr(cfg, "LLM_API_KEY", "")
    monkeypatch.setattr(cfg, "LLM_PROVIDER", "openai")
    monkeypatch.setattr(cfg, "LLM_MODEL", "test-model")
    out = tmp_path / "provider-smoke.json"

    code = provider_smoke.main(["--json", "--out", str(out)])
    payload = json.loads(out.read_text(encoding="utf-8"))

    assert code == 2
    assert set(payload) == {
        "provider",
        "model",
        "tool_call",
        "schema",
        "compile",
        "dialect",
        "status",
        "error",
        "sql_preview",
    }
    assert payload["status"] == "skipped"
    assert "LLM_API_KEY" in payload["error"]
    assert capsys.readouterr().out
