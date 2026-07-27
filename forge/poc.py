"""Customer PoC workspace helpers."""
from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_DIR = ROOT / "customer-poc-template"
REGISTRY_FILES = (
    "schema.registry.json",
    "metrics.registry.yaml",
    "disambiguations.registry.yaml",
    "field_conventions.registry.yaml",
)


def init_workspace(target: Path) -> dict:
    """Create a customer PoC workspace from the bundled template."""
    target = target.expanduser()
    if target.exists() and any(target.iterdir()):
        raise ValueError(f"PoC 目录已存在且非空：{target}")
    shutil.copytree(TEMPLATE_DIR, target, dirs_exist_ok=True)
    (target / "results").mkdir(parents=True, exist_ok=True)
    _copy_if_missing(target / "cases.example.json", target / "cases.json")
    _copy_if_missing(target / "delivery_report.template.md", target / "delivery_report.md")
    _copy_if_missing(target / "failure_triage.template.md", target / "failure_triage.md")
    return {"ok": True, "path": str(target), "message": "PoC workspace initialized"}


def validate_workspace(target: Path) -> dict:
    """Validate the minimum customer PoC workspace contract."""
    target = target.expanduser()
    issues: list[dict] = []

    if not target.exists():
        return _validation_payload(target, [{"level": "error", "path": str(target), "message": "PoC 目录不存在"}])

    registry_dir = target / "registry"
    for name in REGISTRY_FILES:
        _require_file(registry_dir / name, issues)

    cases_path = target / "cases.json"
    _require_file(cases_path, issues)
    if cases_path.exists():
        _validate_cases(cases_path, issues)

    _require_file(target / "delivery_report.md", issues)
    _require_file(target / "failure_triage.md", issues)
    if not (target / "results").is_dir():
        issues.append({"level": "error", "path": str(target / "results"), "message": "缺少 results 目录"})

    return _validation_payload(target, issues)


def write_report(target: Path, out_path: Path | None = None) -> dict:
    """Write a delivery report from existing PoC evidence files."""
    target = target.expanduser()
    validation = validate_workspace(target)
    out_path = out_path.expanduser() if out_path else target / "delivery_report.md"
    results_dir = target / "results"

    doctor = _read_json(results_dir / "doctor.json")
    provider = _read_json(results_dir / "provider-smoke.json")
    database = _read_json(results_dir / "database-smoke.json")
    smoke = _read_json(results_dir / "production-smoke.json")
    ea = _read_json(results_dir / "ea.json")

    gates = [
        ("forge doctor", _gate_status(doctor), str(results_dir / "doctor.json")),
        ("Provider smoke", _gate_status(provider), str(results_dir / "provider-smoke.json")),
        ("Database SELECT 1", _gate_status(database), str(results_dir / "database-smoke.json")),
        ("Production smoke", _gate_status(smoke), str(results_dir / "production-smoke.json")),
        ("Customer golden questions", _gate_status(ea), str(results_dir / "ea.json")),
        ("PoC workspace validation", "fail" if validation["status"] == "fail" else "ok", "forge poc validate"),
    ]
    recommendation = _recommendation(gates)
    report = _render_report(target, validation, gates, recommendation, ea)
    out_path.write_text(report, encoding="utf-8")
    return {
        "ok": recommendation != "blocked",
        "status": recommendation,
        "path": str(out_path),
        "issues": validation["issues"],
    }


def _copy_if_missing(src: Path, dst: Path) -> None:
    if src.exists() and not dst.exists():
        shutil.copyfile(src, dst)


def _require_file(path: Path, issues: list[dict]) -> None:
    if not path.exists():
        issues.append({"level": "error", "path": str(path), "message": "缺少文件"})
    elif path.is_file() and path.stat().st_size == 0:
        issues.append({"level": "warn", "path": str(path), "message": "文件为空"})


def _validate_cases(path: Path, issues: list[dict]) -> None:
    try:
        cases = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        issues.append({"level": "error", "path": str(path), "message": f"cases.json 不是合法 JSON：{exc}"})
        return
    if not isinstance(cases, list) or not cases:
        issues.append({"level": "error", "path": str(path), "message": "cases.json 必须是非空数组"})
        return
    for index, case in enumerate(cases, start=1):
        if not isinstance(case, dict):
            issues.append({"level": "error", "path": f"{path}#{index}", "message": "case 必须是对象"})
            continue
        for field in ("id", "question", "reference_sql"):
            if not str(case.get(field, "")).strip():
                issues.append({"level": "error", "path": f"{path}#{case.get('id', index)}", "message": f"缺少 {field}"})


def _validation_payload(target: Path, issues: list[dict]) -> dict:
    errors = [issue for issue in issues if issue["level"] == "error"]
    warnings = [issue for issue in issues if issue["level"] == "warn"]
    return {
        "path": str(target),
        "status": "fail" if errors else ("warn" if warnings else "ok"),
        "issues": issues,
    }


def _read_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"status": "fail", "error": f"无法读取 {path.name}: {exc}"}
    return data if isinstance(data, dict) else {"status": "fail", "error": f"{path.name} must contain a JSON object"}


def _gate_status(data: dict | None) -> str:
    if data is None:
        return "missing"
    status = str(data.get("status", "")).lower()
    if status in {"ok", "pass", "passed"}:
        return "ok"
    if status in {"warn", "conditional", "conditional pass"}:
        return "warn"
    if status in {"skipped", "skip"}:
        return "skipped"
    return "fail"


def _recommendation(gates: list[tuple[str, str, str]]) -> str:
    statuses = {status for _, status, _ in gates}
    if "fail" in statuses:
        return "blocked"
    if statuses & {"warn", "missing", "skipped"}:
        return "conditional pass"
    return "pass"


def _render_report(
    target: Path,
    validation: dict,
    gates: list[tuple[str, str, str]],
    recommendation: str,
    ea: dict | None,
) -> str:
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    gate_rows = "\n".join(f"| {name} | {status} | `{evidence}` |" for name, status, evidence in gates)
    issue_rows = "\n".join(
        f"| {issue['level']} | `{issue['path']}` | {issue['message']} |"
        for issue in validation["issues"]
    ) or "| ok | - | No workspace validation issues |"
    accuracy_rows = _accuracy_rows(ea)
    return f"""# Forge PoC Delivery Report

## Executive Summary

- Customer:
- Business domain:
- Database platform:
- Provider / model:
- Recommendation: {recommendation}
- Generated at: {generated_at}
- Workspace: `{target}`

## Acceptance Results

| Gate | Result | Evidence |
|---|---|---|
{gate_rows}

## Accuracy

| Segment | Case EA(any) | Case EA(all) | Run ACC |
|---|---:|---:|---:|
{accuracy_rows}

## Workspace Validation

| Level | Path | Message |
|---|---|---|
{issue_rows}

## Decisions

- Registry rules added:
- Product boundaries:
- Follow-up engineering work:

## Known Limits

- Unsupported database features:
- Provider/tool-calling limitations:
- Questions requiring manual clarification:
"""


def _accuracy_rows(ea: dict | None) -> str:
    if not ea:
        return "| All questions | missing | missing | missing |"
    case_any = ea.get("case_ea_any", ea.get("case_ea_any_pct", ""))
    case_all = ea.get("case_ea_all", ea.get("case_ea_all_pct", ""))
    run_acc = ea.get("run_acc", ea.get("run_acc_pct", ""))
    return f"| All questions | {case_any or 'unknown'} | {case_all or 'unknown'} | {run_acc or 'unknown'} |"
