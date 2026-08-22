"""Persistent, non-replaying quality validation for candidate model revisions."""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import time
from typing import Any, Callable

from sqlalchemy import create_engine, text

from agent.agent import _prepare_query
from agent.llm import RUNTIME_CONTEXT_REVISION
from config import cfg
from agent.model_config import get_revision_model_config
from agent.model_control import ModelControlError, ModelControlStore
from forge.assurance import ASSURANCE_REVISION, POLICY_REVISION
from forge.executor import validate_readonly_sql

DEFAULT_THRESHOLDS: dict[str, float] = {
    "accuracy_min": 0.80,
    "assurance_pass_rate_min": 0.90,
    "average_retry_max": 1.0,
    "p95_latency_ms_max": 180_000.0,
    "timeout_rate_max": 0.05,
}


class ModelQualityConfigurationError(ModelControlError):
    pass


def current_quality_lineage() -> dict[str, str]:
    try:
        registry_revision = hashlib.sha256(Path(cfg.REGISTRY_PATH).read_bytes()).hexdigest()
    except OSError as exc:
        raise ModelQualityConfigurationError("Model Validation Registry 不可读取。") from exc
    return {
        "registry_revision": registry_revision,
        "assurance_revision": ASSURANCE_REVISION,
        "policy_revision": POLICY_REVISION,
        "runtime_context_revision": RUNTIME_CONTEXT_REVISION,
    }


def validation_cases_path() -> Path:
    value = os.getenv("MODEL_VALIDATION_CASES_PATH", "").strip()
    if not value:
        raise ModelQualityConfigurationError("未配置 Model Validation cases_path。")
    return Path(value).expanduser().resolve()


def validation_database_url() -> str:
    value = os.getenv("MODEL_VALIDATION_DATABASE_URL", "").strip()
    if not value:
        raise ModelQualityConfigurationError("未配置 Model Validation database_url。")
    readonly = "mode=ro" in value or os.getenv(
        "MODEL_VALIDATION_DATABASE_READONLY_CONFIRMED", "false"
    ).lower() == "true"
    if not readonly:
        raise ModelQualityConfigurationError("Model Validation 数据库必须显式确认为只读。")
    return value


def load_validation_cases(path: Path) -> list[dict[str, Any]]:
    try:
        cases = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ModelQualityConfigurationError("Model Validation cases 不可读取或格式错误。") from exc
    if not isinstance(cases, list) or not cases:
        raise ModelQualityConfigurationError("Model Validation cases 必须是非空数组。")
    normalized: list[dict[str, Any]] = []
    for case in cases:
        if not isinstance(case, dict) or not all(
            case.get(key) for key in ("id", "question", "reference_sql")
        ):
            raise ModelQualityConfigurationError("Model Validation case 缺少必填字段。")
        normalized.append(case)
    return normalized


def _normalize_value(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    return value


def execute_result_set(database_url: str, sql: str) -> list[tuple[Any, ...]]:
    validate_readonly_sql(sql)
    engine = create_engine(database_url)
    try:
        with engine.connect() as connection:
            rows = connection.execute(text(sql)).fetchmany(10_001)
        if len(rows) > 10_000:
            raise ModelControlError("Model Validation 查询结果超过安全上限。")
        return [tuple(_normalize_value(value) for value in row) for row in rows]
    finally:
        engine.dispose()


def result_sets_equal(
    generated: list[tuple[Any, ...]],
    reference: list[tuple[Any, ...]],
    *,
    order_sensitive: bool,
) -> bool:
    if order_sensitive:
        return generated == reference
    key = lambda row: tuple(repr(value) for value in row)
    return sorted(generated, key=key) == sorted(reference, key=key)


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)]


def evaluate_metrics(
    case_results: list[dict[str, Any]],
    *,
    thresholds: dict[str, float],
    smoke_report: dict[str, Any],
) -> dict[str, Any]:
    total = len(case_results)
    accuracy = sum(item["correct"] for item in case_results) / total
    assurance_rate = sum(item["assurance_passed"] for item in case_results) / total
    average_retry = sum(item["retry_count"] for item in case_results) / total
    timeout_rate = sum(item["timed_out"] for item in case_results) / total
    p95_latency = _p95([float(item["latency_ms"]) for item in case_results])
    checks = {
        "tool_calling": smoke_report.get("tool_calling") is True,
        "structured_output": smoke_report.get("structured_output") is True,
        "accuracy": accuracy >= thresholds["accuracy_min"],
        "assurance_pass_rate": assurance_rate >= thresholds["assurance_pass_rate_min"],
        "average_retry": average_retry <= thresholds["average_retry_max"],
        "p95_latency_ms": p95_latency <= thresholds["p95_latency_ms_max"],
        "timeout_rate": timeout_rate <= thresholds["timeout_rate_max"],
    }
    return {
        "passed": all(checks.values()),
        "status": "passed" if all(checks.values()) else "failed",
        "total_cases": total,
        "correct_cases": sum(item["correct"] for item in case_results),
        "accuracy": accuracy,
        "assurance_pass_rate": assurance_rate,
        "average_retry": average_retry,
        "p95_latency_ms": p95_latency,
        "timeout_rate": timeout_rate,
        "checks": checks,
        "thresholds": thresholds,
    }


def run_quality_validation(
    store: ModelControlStore,
    run_id: str,
    *,
    cases_path: Path | None = None,
    database_url: str | None = None,
    prepare_fn: Callable[..., dict[str, Any]] = _prepare_query,
    execute_fn: Callable[[str, str], list[tuple[Any, ...]]] = execute_result_set,
) -> dict[str, Any]:
    """Execute one persisted run. Callers must schedule it off the HTTP event loop."""
    run = store.get_quality_validation_run(run_id)
    if run is None:
        raise ModelControlError("Quality Validation Run 不存在。")
    revision = store.get_revision(run["revision_id"])
    if revision is None:
        raise ModelControlError("Model Profile Revision 不存在。")
    smoke_report = revision["validation_report"]
    if not (
        revision["validation_status"] == "passed"
        and smoke_report.get("tool_calling") is True
        and smoke_report.get("structured_output") is True
    ):
        raise ModelControlError("候选 Revision 尚未通过 Provider smoke。")

    store.mark_quality_validation_running(run_id)
    results: list[dict[str, Any]] = []
    try:
        validation_lineage = current_quality_lineage()
        snapshot = get_revision_model_config(run["revision_id"], db_path=store.path)
        cases = load_validation_cases(cases_path or validation_cases_path())
        db_url = database_url or validation_database_url()
        for case in cases:
            started = time.monotonic()
            prepared = prepare_fn(
                f"model-validation:{run_id}",
                str(case["question"]),
                model_snapshot=snapshot,
            )
            latency_ms = round((time.monotonic() - started) * 1000, 1)
            assurance_passed = prepared.get("status") == "needs_review"
            timed_out = prepared.get("status") == "timed_out"
            correct = False
            error_code: str | None = None
            if assurance_passed:
                try:
                    generated_rows = execute_fn(db_url, str(prepared["sql"]))
                    reference_rows = execute_fn(db_url, str(case["reference_sql"]))
                    correct = result_sets_equal(
                        generated_rows,
                        reference_rows,
                        order_sensitive="ORDER BY" in str(case["reference_sql"]).upper(),
                    )
                except Exception:
                    error_code = "execution_comparison_failed"
            else:
                error_code = "timed_out" if timed_out else "assurance_or_generation_failed"
            result = {
                "correct": correct,
                "assurance_passed": assurance_passed,
                "retry_count": int(prepared.get("retry_count", 0)),
                "latency_ms": latency_ms,
                "timed_out": timed_out,
                "error_code": error_code,
                "retrieval_trace": prepared.get("retrieval_trace"),
            }
            results.append(result)
            store.record_quality_validation_case(run_id, str(case["id"]), result)

        if current_quality_lineage() != validation_lineage:
            raise ModelControlError("Model Validation 期间 Registry 或 Assurance Policy 已变化。")
        metrics = evaluate_metrics(
            results,
            thresholds=run["thresholds"],
            smoke_report=smoke_report,
        )
        metrics["lineage"] = validation_lineage
        final_report = {
            **smoke_report,
            "quality_gate": metrics,
            "quality_validation_run_id": run_id,
        }
        store.record_validation(
            run["revision_id"],
            passed=True,
            report=final_report,
        )
        store.complete_quality_validation_run(
            run_id,
            status="passed" if metrics["passed"] else "failed",
            metrics=metrics,
        )
        return metrics
    except Exception:
        try:
            store.complete_quality_validation_run(
                run_id,
                status="failed",
                metrics={"completed_cases": len(results)},
                error_code="validation_runtime_failed",
            )
        except ModelControlError:
            pass
        raise
