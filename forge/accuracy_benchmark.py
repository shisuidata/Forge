"""Persistent Ark Coding Plan SQL accuracy benchmark runtime.

The benchmark store is the single source of truth for run progress. Web surfaces
only project snapshots from this store; browser state never owns benchmark truth.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from agent.model_config import ModelConfigSnapshot, get_model_config
from forge.benchmark_methods import ark_coding_plan_method, bird_execution_accuracy, run_forge_oai

_ROOT = Path(__file__).resolve().parents[1]
_ACCURACY_DIR = _ROOT / "tests" / "accuracy"
_LARGE_DATASET_DIR = _ROOT / "tests" / "datasets" / "large"
_CASES_PATH = _LARGE_DATASET_DIR / "cases.json"
_DATABASE_PATH = _LARGE_DATASET_DIR / "database.db"
_TERMINAL_STATUSES = {"completed", "failed", "interrupted"}
_SAFE_ERROR_CODES = {
    "generation_failed": "模型未生成可编译的 Forge JSON。",
    "evaluation_failed": "生成 SQL 或参考 SQL 无法在固定数据集上完成比较。",
    "incorrect_result": "SQL 可以执行，但结果与参考结果不一致。",
    "runtime_failed": "Benchmark Runtime 异常终止；已完成结果仍被保留。",
    "process_restarted": "服务进程重启，未完成调用没有自动重放。",
}


class AccuracyBenchmarkError(RuntimeError):
    """Bounded benchmark domain error safe to surface through the API."""


@dataclass(frozen=True)
class BenchmarkConfig:
    method_id: str = "ai"
    dataset: str = "large"
    runs_per_case: int = 3
    compile_retries: int = 2
    workers: int = 4


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _code_revision() -> str:
    try:
        revision = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        dirty = subprocess.call(
            ["git", "diff", "--quiet"],
            cwd=_ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ) != 0
        return revision + ("+dirty" if dirty else "")
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def _benchmark_lineage() -> dict[str, str]:
    files = {
        "cases": _CASES_PATH,
        "schema": _LARGE_DATASET_DIR / "schema.registry.json",
        "metrics": _LARGE_DATASET_DIR / "metrics.registry.yaml",
        "relationships": _LARGE_DATASET_DIR / "relationships.reference.json",
        "conventions": _LARGE_DATASET_DIR / "field_conventions.registry.yaml",
    }
    return {
        "code_revision": _code_revision(),
        **{f"{name}_revision": _file_sha256(path) for name, path in files.items()},
    }


def _safe_endpoint(base_url: str) -> str:
    parsed = urlparse(base_url)
    return parsed.netloc or "configured-provider"


def _db_path() -> Path:
    return Path(
        os.getenv("ACCURACY_BENCHMARK_DB_PATH", ".forge/accuracy_benchmark.db")
    ).expanduser().resolve()


class AccuracyBenchmarkStore:
    def __init__(self, path: str | Path | None = None):
        self.path = Path(path).expanduser().resolve() if path else _db_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._schema_lock = threading.Lock()
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA foreign_keys=ON")
        return db

    def _ensure_schema(self) -> None:
        with self._schema_lock:
            with self._connect() as db:
                db.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS accuracy_benchmark_runs (
                        run_id TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        method_id TEXT NOT NULL,
                        method_label TEXT NOT NULL,
                        dataset TEXT NOT NULL,
                        model_provider TEXT NOT NULL,
                        model_name TEXT NOT NULL,
                        model_revision TEXT NOT NULL,
                        model_source TEXT NOT NULL,
                        endpoint_host TEXT NOT NULL,
                        runs_per_case INTEGER NOT NULL,
                        compile_retries INTEGER NOT NULL,
                        workers INTEGER NOT NULL,
                        total_cases INTEGER NOT NULL,
                        total_calls INTEGER NOT NULL,
                        lineage_json TEXT NOT NULL,
                        sequence INTEGER NOT NULL DEFAULT 0,
                        error_code TEXT,
                        created_at TEXT NOT NULL,
                        started_at TEXT,
                        completed_at TEXT
                    );
                    CREATE TABLE IF NOT EXISTS accuracy_benchmark_calls (
                        run_id TEXT NOT NULL,
                        case_id TEXT NOT NULL,
                        run_index INTEGER NOT NULL,
                        category TEXT NOT NULL,
                        difficulty INTEGER NOT NULL,
                        question TEXT NOT NULL,
                        compiled INTEGER NOT NULL,
                        correct INTEGER NOT NULL,
                        attempts INTEGER NOT NULL,
                        latency_ms REAL NOT NULL,
                        error_code TEXT,
                        sql_hash TEXT,
                        completed_at TEXT NOT NULL,
                        PRIMARY KEY (run_id, case_id, run_index),
                        FOREIGN KEY(run_id) REFERENCES accuracy_benchmark_runs(run_id)
                    );
                    CREATE INDEX IF NOT EXISTS idx_accuracy_benchmark_runs_created
                    ON accuracy_benchmark_runs(created_at DESC);
                    """
                )

    def reconcile_interrupted(self) -> int:
        with self._connect() as db:
            rows = db.execute(
                "SELECT run_id FROM accuracy_benchmark_runs "
                "WHERE status IN ('queued','running')"
            ).fetchall()
            if rows:
                db.execute(
                    "UPDATE accuracy_benchmark_runs "
                    "SET status='interrupted', error_code='process_restarted', "
                    "completed_at=?, sequence=sequence+1 "
                    "WHERE status IN ('queued','running')",
                    (_now(),),
                )
        return len(rows)

    def create_run(
        self,
        config: BenchmarkConfig,
        snapshot: ModelConfigSnapshot,
        *,
        method_label: str,
        total_cases: int,
        lineage: dict[str, str],
    ) -> str:
        if config.runs_per_case < 1 or config.runs_per_case > 5:
            raise AccuracyBenchmarkError("每题运行次数必须在 1 到 5 之间。")
        if config.workers < 1 or config.workers > 10:
            raise AccuracyBenchmarkError("并发数必须在 1 到 10 之间。")
        run_id = "abr_" + uuid.uuid4().hex
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            active = db.execute(
                "SELECT run_id FROM accuracy_benchmark_runs "
                "WHERE status IN ('queued','running') ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            if active:
                raise AccuracyBenchmarkError(
                    f"已有运行中的 Benchmark：{active['run_id']}"
                )
            db.execute(
                """
                INSERT INTO accuracy_benchmark_runs (
                    run_id,status,method_id,method_label,dataset,model_provider,
                    model_name,model_revision,model_source,endpoint_host,
                    runs_per_case,compile_retries,workers,total_cases,total_calls,
                    lineage_json,created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    "queued",
                    config.method_id,
                    method_label,
                    config.dataset,
                    snapshot.provider,
                    snapshot.model,
                    snapshot.revision,
                    snapshot.source,
                    _safe_endpoint(snapshot.base_url),
                    config.runs_per_case,
                    config.compile_retries,
                    config.workers,
                    total_cases,
                    total_cases * config.runs_per_case,
                    _canonical_json(lineage),
                    _now(),
                ),
            )
        return run_id

    def mark_running(self, run_id: str) -> None:
        with self._connect() as db:
            changed = db.execute(
                "UPDATE accuracy_benchmark_runs "
                "SET status='running', started_at=?, sequence=sequence+1 "
                "WHERE run_id=? AND status='queued'",
                (_now(), run_id),
            ).rowcount
        if changed != 1:
            raise AccuracyBenchmarkError("Benchmark Run 不可启动。")

    def record_call(self, run_id: str, result: dict[str, Any]) -> None:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            status = db.execute(
                "SELECT status FROM accuracy_benchmark_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
            if status is None or status["status"] != "running":
                raise AccuracyBenchmarkError("Benchmark Run 已不接受新结果。")
            db.execute(
                """
                INSERT INTO accuracy_benchmark_calls (
                    run_id,case_id,run_index,category,difficulty,question,
                    compiled,correct,attempts,latency_ms,error_code,sql_hash,completed_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    str(result["case_id"]),
                    int(result["run_index"]),
                    str(result["category"]),
                    int(result["difficulty"]),
                    str(result["question"]),
                    int(bool(result["compiled"])),
                    int(bool(result["correct"])),
                    int(result["attempts"]),
                    float(result["latency_ms"]),
                    result.get("error_code"),
                    result.get("sql_hash"),
                    _now(),
                ),
            )
            db.execute(
                "UPDATE accuracy_benchmark_runs SET sequence=sequence+1 WHERE run_id=?",
                (run_id,),
            )

    def complete(self, run_id: str, *, status: str, error_code: str | None = None) -> None:
        if status not in _TERMINAL_STATUSES:
            raise AccuracyBenchmarkError("Benchmark 终态不受支持。")
        with self._connect() as db:
            changed = db.execute(
                "UPDATE accuracy_benchmark_runs "
                "SET status=?, error_code=?, completed_at=?, sequence=sequence+1 "
                "WHERE run_id=? AND status='running'",
                (status, error_code, _now(), run_id),
            ).rowcount
        if changed != 1:
            raise AccuracyBenchmarkError("Benchmark Run 状态已变化。")

    def latest_run_id(self) -> str | None:
        with self._connect() as db:
            row = db.execute(
                "SELECT run_id FROM accuracy_benchmark_runs ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
        return str(row["run_id"]) if row else None

    def snapshot(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as db:
            run = db.execute(
                "SELECT * FROM accuracy_benchmark_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            rows = db.execute(
                "SELECT * FROM accuracy_benchmark_calls WHERE run_id=? "
                "ORDER BY completed_at, case_id, run_index",
                (run_id,),
            ).fetchall()
        if run is None:
            return None
        return _project_snapshot(dict(run), [dict(row) for row in rows])


def _project_snapshot(run: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    runs_per_case = int(run["runs_per_case"])
    total_calls = int(run["total_calls"])
    completed_calls = len(rows)
    compiled_calls = sum(int(row["compiled"]) for row in rows)
    correct_runs = sum(int(row["correct"]) for row in rows)
    latencies = [float(row["latency_ms"]) for row in rows]

    cases: dict[str, dict[str, Any]] = {}
    categories: dict[str, dict[str, Any]] = {}
    for row in rows:
        case = cases.setdefault(
            str(row["case_id"]),
            {
                "case_id": str(row["case_id"]),
                "question": row["question"],
                "category": row["category"],
                "difficulty": row["difficulty"],
                "completed_runs": 0,
                "correct_runs": 0,
                "compiled_runs": 0,
                "last_error_code": None,
                "last_latency_ms": 0.0,
            },
        )
        case["completed_runs"] += 1
        case["correct_runs"] += int(row["correct"])
        case["compiled_runs"] += int(row["compiled"])
        if row["error_code"]:
            case["last_error_code"] = row["error_code"]
        case["last_latency_ms"] = float(row["latency_ms"])

    completed_cases = 0
    correct_cases = 0
    all_correct_cases = 0
    for case in cases.values():
        case["status"] = (
            "correct"
            if case["correct_runs"] == case["completed_runs"] == runs_per_case
            else "mixed"
            if case["correct_runs"] > 0
            else "failed"
            if case["completed_runs"] == runs_per_case
            else "running"
        )
        case["error_message"] = _SAFE_ERROR_CODES.get(case["last_error_code"] or "")
        if case["completed_runs"] == runs_per_case:
            completed_cases += 1
            if case["correct_runs"] > 0:
                correct_cases += 1
            if case["correct_runs"] == runs_per_case:
                all_correct_cases += 1
            category = categories.setdefault(
                case["category"],
                {
                    "category": case["category"],
                    "completed_cases": 0,
                    "correct_cases": 0,
                    "correct_runs": 0,
                    "total_runs": 0,
                },
            )
            category["completed_cases"] += 1
            category["correct_cases"] += int(case["correct_runs"] > 0)
            category["correct_runs"] += case["correct_runs"]
            category["total_runs"] += case["completed_runs"]

    category_metrics = []
    for category in sorted(categories.values(), key=lambda item: item["category"]):
        category_metrics.append(
            {
                **category,
                "ea": category["correct_cases"] / category["completed_cases"],
                "run_accuracy": category["correct_runs"] / category["total_runs"],
            }
        )

    recent = []
    for row in reversed(rows[-12:]):
        recent.append(
            {
                "case_id": str(row["case_id"]),
                "run_index": int(row["run_index"]),
                "category": row["category"],
                "correct": bool(row["correct"]),
                "compiled": bool(row["compiled"]),
                "attempts": int(row["attempts"]),
                "latency_ms": float(row["latency_ms"]),
                "error_code": row["error_code"],
                "error_message": _SAFE_ERROR_CODES.get(row["error_code"] or ""),
                "completed_at": row["completed_at"],
            }
        )

    status = str(run["status"])
    phase = "final" if status in _TERMINAL_STATUSES else "partial"
    return {
        "schema_version": 1,
        "projection_type": "accuracy_benchmark_run_v1",
        "run_id": run["run_id"],
        "status": status,
        "score_phase": phase,
        "sequence": int(run["sequence"]),
        "method": {
            "id": run["method_id"],
            "label": run["method_label"],
            "dataset": run["dataset"],
            "runs_per_case": runs_per_case,
            "compile_retries": int(run["compile_retries"]),
            "workers": int(run["workers"]),
        },
        "model": {
            "provider": run["model_provider"],
            "name": run["model_name"],
            "revision": run["model_revision"],
            "source": run["model_source"],
            "endpoint_host": run["endpoint_host"],
        },
        "lineage": json.loads(run["lineage_json"]),
        "progress": {
            "total_cases": int(run["total_cases"]),
            "completed_cases": completed_cases,
            "total_calls": total_calls,
            "completed_calls": completed_calls,
            "percent": completed_calls / total_calls if total_calls else 0.0,
        },
        "metrics": {
            "case_ea": correct_cases / completed_cases if completed_cases else None,
            "all_runs_case_ea": (
                all_correct_cases / completed_cases if completed_cases else None
            ),
            "run_accuracy": correct_runs / completed_calls if completed_calls else None,
            "compile_success_rate": (
                compiled_calls / completed_calls if completed_calls else None
            ),
            "correct_cases": correct_cases,
            "all_correct_cases": all_correct_cases,
            "correct_runs": correct_runs,
            "compiled_calls": compiled_calls,
            "p95_latency_ms": _p95(latencies),
        },
        "categories": category_metrics,
        "cases": sorted(cases.values(), key=lambda item: int(item["case_id"])),
        "recent": recent,
        "error_code": run["error_code"],
        "error_message": _SAFE_ERROR_CODES.get(run["error_code"] or ""),
        "created_at": run["created_at"],
        "started_at": run["started_at"],
        "completed_at": run["completed_at"],
        "disclaimer": (
            "固定 Enterprise Reference、固定 Registry、固定模型与当前代码 revision 下的有界结果；"
            "不代表开放世界或真实客户 SQL 100% 准确。"
        ),
    }


def _p95(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int((len(ordered) * 0.95 + 0.9999)) - 1))
    return ordered[index]


def _load_method_ai():
    return ark_coding_plan_method(_ROOT)


def _load_cases() -> list[dict[str, Any]]:
    try:
        cases = json.loads(_CASES_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AccuracyBenchmarkError("large Benchmark cases 不可读取。") from exc
    if not isinstance(cases, list) or not cases:
        raise AccuracyBenchmarkError("large Benchmark cases 为空。")
    return cases

def _evaluate_sql(sql: str | None, reference_sql: str) -> tuple[bool, str | None]:
    if not sql:
        return False, "generation_failed"
    try:
        with sqlite3.connect(_DATABASE_PATH) as db:
            reference_rows = db.execute(reference_sql).fetchall()
            generated_rows = db.execute(sql).fetchall()
    except sqlite3.Error:
        return False, "evaluation_failed"
    return bird_execution_accuracy(reference_rows, generated_rows), None

def _method_ai_snapshot(method) -> ModelConfigSnapshot:
    """Resolve the already configured Ark Coding Plan without exposing its secret."""
    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env")
    api_key = os.getenv("ARK_API_KEY", "").strip()
    if api_key:
        revision_body = _canonical_json({
            "provider": "openai",
            "model": method.model,
            "base_url": method.base_url,
            "key_fingerprint": hashlib.sha256(api_key.encode()).hexdigest(),
        })
        return ModelConfigSnapshot(
            provider="openai",
            model=str(method.model),
            api_key=api_key,
            base_url=str(method.base_url or ""),
            tool_choice="auto",
            timeout_seconds=120,
            revision="sha256:" + hashlib.sha256(revision_body.encode()).hexdigest(),
            source="method_ai:ARK_API_KEY",
            max_output_tokens=8192,
            temperature=0.0,
        )

    try:
        snapshot = get_model_config("query_generation")
    except Exception as exc:
        raise AccuracyBenchmarkError("已配置的 Ark Coding Plan 凭证当前不可用。") from exc
    if snapshot.provider != "openai":
        raise AccuracyBenchmarkError("当前模型不是 OpenAI-compatible Coding Plan。")
    return snapshot



def create_benchmark_run(
    store: AccuracyBenchmarkStore,
    config: BenchmarkConfig | None = None,
) -> tuple[str, ModelConfigSnapshot]:
    config = config or BenchmarkConfig()
    method = _load_method_ai()
    cases = _load_cases()
    snapshot = _method_ai_snapshot(method)
    run_id = store.create_run(
        config,
        snapshot,
        method_label=method.label,
        total_cases=len(cases),
        lineage=_benchmark_lineage(),
    )
    return run_id, snapshot


def run_benchmark(
    store: AccuracyBenchmarkStore,
    run_id: str,
    snapshot: ModelConfigSnapshot,
) -> None:
    from agent.prompts import build_system

    run = store.snapshot(run_id)
    if run is None:
        raise AccuracyBenchmarkError("Benchmark Run 不存在。")
    method = _load_method_ai()
    cases = _load_cases()
    runs_per_case = int(run["method"]["runs_per_case"])
    compile_retries = int(run["method"]["compile_retries"])
    workers = int(run["method"]["workers"])
    tasks = [
        (case, run_index)
        for case in cases
        for run_index in range(1, runs_per_case + 1)
    ]

    store.mark_running(run_id)

    def dispatch(task: tuple[dict[str, Any], int]) -> dict[str, Any]:
        case, run_index = task
        question = str(case["question"])
        system = build_system(
            str(method.registry_context), question=question, mode="benchmark"
        )
        started = time.monotonic()
        generated = run_forge_oai(
            snapshot.api_key,
            snapshot.base_url,
            question,
            system,
            snapshot.model,
            max_compile_retries=compile_retries,
        )
        latency_ms = round((time.monotonic() - started) * 1000, 1)
        compiled = generated.get("error_code") is None and bool(generated.get("sql"))
        correct, evaluation_error = _evaluate_sql(
            generated.get("sql"), str(case["reference_sql"])
        )
        error_code = evaluation_error
        if not compiled:
            error_code = "generation_failed"
        elif not correct and error_code is None:
            error_code = "incorrect_result"
        sql = str(generated.get("sql") or "")
        return {
            "case_id": str(case["id"]),
            "run_index": run_index,
            "category": str(case["category"]),
            "difficulty": int(case["difficulty"]),
            "question": str(case["question"]),
            "compiled": compiled,
            "correct": correct,
            "attempts": int(generated.get("attempts", 1)),
            "latency_ms": latency_ms,
            "error_code": error_code,
            "sql_hash": "sha256:" + hashlib.sha256(sql.encode()).hexdigest() if sql else None,
        }

    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_map = {pool.submit(dispatch, task): task for task in tasks}
            for future in as_completed(future_map):
                case, run_index = future_map[future]
                try:
                    result = future.result()
                except Exception:
                    # A failed provider call is still a completed benchmark observation.
                    result = {
                        "case_id": str(case["id"]),
                        "run_index": run_index,
                        "category": str(case["category"]),
                        "difficulty": int(case["difficulty"]),
                        "question": str(case["question"]),
                        "compiled": False,
                        "correct": False,
                        "attempts": 0,
                        "latency_ms": 0.0,
                        "error_code": "generation_failed",
                        "sql_hash": None,
                    }
                store.record_call(run_id, result)
        store.complete(run_id, status="completed")
    except Exception:
        try:
            store.complete(run_id, status="failed", error_code="runtime_failed")
        except AccuracyBenchmarkError:
            pass
        raise


class AccuracyBenchmarkService:
    def __init__(self, store: AccuracyBenchmarkStore | None = None):
        self.store = store or AccuracyBenchmarkStore()
        self.store.reconcile_interrupted()

    def model_summary(self) -> dict[str, str]:
        snapshot = _method_ai_snapshot(_load_method_ai())
        return {
            "provider": snapshot.provider,
            "name": snapshot.model,
            "revision": snapshot.revision,
            "source": snapshot.source,
            "endpoint_host": _safe_endpoint(snapshot.base_url),
        }

    def create(self, config: BenchmarkConfig | None = None) -> tuple[str, ModelConfigSnapshot]:
        return create_benchmark_run(self.store, config)

    def run(self, run_id: str, snapshot: ModelConfigSnapshot) -> None:
        run_benchmark(self.store, run_id, snapshot)

    def latest(self) -> dict[str, Any] | None:
        run_id = self.store.latest_run_id()
        return self.store.snapshot(run_id) if run_id else None

    def snapshot(self, run_id: str) -> dict[str, Any] | None:
        return self.store.snapshot(run_id)


_service: AccuracyBenchmarkService | None = None
_service_lock = threading.Lock()


def get_accuracy_benchmark_service() -> AccuracyBenchmarkService:
    global _service
    with _service_lock:
        if _service is None:
            _service = AccuracyBenchmarkService()
        return _service
