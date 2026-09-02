"""BIRD Mini-Dev hard benchmark: Forge DSL versus direct SQL.

Questions, evidence, schemas, databases, and gold SQL are sourced from the
public BIRD-SQL Mini-Dev dataset. The runtime never generates benchmark truth.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import random
import re
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from agent.model_config import ModelConfigSnapshot
from forge.accuracy_benchmark import (
    AccuracyBenchmarkError,
    _canonical_json,
    _code_revision,
    _file_sha256,
    _method_ai_snapshot,
    _p95,
    _safe_endpoint,
)
from forge.benchmark_methods import (
    ark_coding_plan_method,
    bird_execution_accuracy,
    run_forge_oai,
    run_sql_oai,
)
from forge.executor import validate_readonly_sql

_ROOT = Path(__file__).resolve().parents[1]
_SUITE_DIR = _ROOT / "tests" / "datasets" / "bird_mini_dev_hard"
_CASES_PATH = _SUITE_DIR / "cases.json"
_TABLES_PATH = _SUITE_DIR / "tables.json"
_MANIFEST_PATH = _SUITE_DIR / "dataset.json"
_GOLD_RESULTS_PATH = _SUITE_DIR / "gold_results.json"
_BIRD_RUNTIME = (
    _ROOT
    / ".forge"
    / "benchmarks"
    / "bird-mini-dev"
    / "minidev"
    / "MINIDEV"
)
_OFFICIAL_CASES_PATH = _BIRD_RUNTIME / "mini_dev_sqlite.json"
_OFFICIAL_TABLES_PATH = _BIRD_RUNTIME / "dev_tables.json"
_DB_ROOT = _BIRD_RUNTIME / "dev_databases"
_LEGACY_SUITE_ID = "bird-mini-dev-hard-v1"
_FULL_SUITE_ID = "bird-mini-dev-full-v1"
_TERMINAL = {"completed", "failed", "interrupted"}
_METHODS = ("forge", "direct")
_SCORING_STANDARD = "bird_execution_accuracy_exact_set_v1"
_SAFE_ERRORS = {
    "generation_failed": "模型未生成可执行的 SQL。",
    "compile_failed": "Forge JSON 未通过确定性编译。",
    "execution_failed": "生成 SQL 未通过只读校验、超时或无法执行。",
    "incorrect_result": "SQL 可以执行，但结果与 BIRD Gold SQL 不一致。",
    "runtime_failed": "Hard Benchmark Runtime 异常终止；已完成证据仍保留。",
    "process_restarted": "服务重启，未完成模型调用未自动重放。",
    "missing_official_assets": "BIRD 官方数据库或描述文件不可用。",
    "gold_validation_failed": "BIRD Gold SQL 预检失败，未发起模型调用。",
}


class HardBenchmarkError(RuntimeError):
    pass


@dataclass(frozen=True)
class HardBenchmarkConfig:
    suite_id: str = _FULL_SUITE_ID
    runs_per_case: int = 1
    workers: int = 4
    compile_retries: int = 2


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _db_path() -> Path:
    return Path(
        os.getenv("ACCURACY_BENCHMARK_DB_PATH", ".forge/accuracy_benchmark.db")
    ).expanduser().resolve()


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HardBenchmarkError(f"Benchmark asset unavailable: {path.name}") from exc


def _database_path(db_id: str) -> Path:
    path = _DB_ROOT / db_id / f"{db_id}.sqlite"
    if not path.exists():
        raise HardBenchmarkError("BIRD 官方数据库未下载。")
    return path


def _description_dir(db_id: str) -> Path:
    path = _DB_ROOT / db_id / "database_description"
    if not path.exists():
        raise HardBenchmarkError("BIRD 官方 database_description 不可用。")
    return path


def load_suite(suite_id: str = _FULL_SUITE_ID) -> dict[str, Any]:
    if suite_id == _LEGACY_SUITE_ID:
        cases = [
            {**case, "case_id": str(case["question_id"])}
            for case in _load_json(_CASES_PATH)
        ]
        tables = _load_json(_TABLES_PATH)
        manifest = _load_json(_MANIFEST_PATH)
        case_path = _CASES_PATH
        tables_path = _TABLES_PATH
    elif suite_id == _FULL_SUITE_ID:
        official_cases = _load_json(_OFFICIAL_CASES_PATH)
        cases = [
            {
                **case,
                "case_id": f"md-{index:03d}",
                "source": {
                    "dataset": "BIRD-SQL Mini-Dev",
                    "question_id": case["question_id"],
                    "instance_index": index,
                    "repository": "https://github.com/bird-bench/mini_dev",
                    "license": "CC BY-SA 4.0",
                },
            }
            for index, case in enumerate(official_cases)
        ]
        tables = _load_json(_OFFICIAL_TABLES_PATH)
        manifest = {
            "suite": _FULL_SUITE_ID,
            "title": "BIRD-SQL Mini-Dev Full",
            "source": {
                "project": "BIRD-SQL",
                "repository": "https://github.com/bird-bench/mini_dev",
                "license": "CC BY-SA 4.0",
            },
            "selection": {
                "cases": len(cases),
                "databases": len({case["db_id"] for case in cases}),
                "difficulty": {
                    difficulty: sum(case["difficulty"] == difficulty for case in cases)
                    for difficulty in ("simple", "moderate", "challenging")
                },
            },
            "evaluation": {
                "primary": "BIRD Execution Accuracy",
                "verdict": "set(gold_result_tuples) == set(predicted_result_tuples)",
                "sql_text_scored": False,
            },
            "default_runs_per_case": 1,
            "expected_model_calls": len(cases) * len(_METHODS),
            "result_boundary": "Complete official 500-case Mini-Dev across 11 databases with Oracle Evidence.",
        }
        case_path = _OFFICIAL_CASES_PATH
        tables_path = _OFFICIAL_TABLES_PATH
    else:
        raise HardBenchmarkError(f"Unknown BIRD suite: {suite_id}")

    table_index = {item["db_id"]: item for item in tables}
    for db_id in sorted({str(case["db_id"]) for case in cases}):
        _database_path(db_id)
        _description_dir(db_id)
        if db_id not in table_index:
            raise HardBenchmarkError(f"BIRD structure layer missing: {db_id}")
    return {
        "cases": cases,
        "tables": table_index,
        "manifest": manifest,
        "_case_path": case_path,
        "_tables_path": tables_path,
    }


def _case_id(case: dict[str, Any]) -> str:
    return str(case.get("case_id", case["question_id"]))


def _column_descriptions(db_id: str) -> dict[str, dict[str, dict[str, str]]]:
    result: dict[str, dict[str, dict[str, str]]] = {}
    for path in sorted(_description_dir(db_id).glob("*.csv")):
        table = path.stem
        columns: dict[str, dict[str, str]] = {}
        rows = None
        for encoding in ("utf-8-sig", "cp1252"):
            try:
                text = path.read_text(encoding=encoding)
                rows = csv.DictReader(text.splitlines())
                break
            except (OSError, UnicodeDecodeError):
                continue
        if rows is None:
            continue
        try:
            for row in rows:
                name = str(row.get("original_column_name") or "").strip()
                if not name:
                    continue
                columns[name] = {
                    "label": str(row.get("column_name") or "").strip(),
                    "description": str(row.get("column_description") or "").strip(),
                    "format": str(row.get("data_format") or "").strip(),
                    "values": str(row.get("value_description") or "").strip(),
                }
        except csv.Error:
            continue


        result[table] = columns
    return result

def structure_projection(table_entry: dict[str, Any]) -> dict[str, Any]:
    db_id = str(table_entry["db_id"])
    table_names = list(table_entry["table_names_original"])
    descriptions = _column_descriptions(db_id)
    columns: dict[str, list[dict[str, Any]]] = {name: [] for name in table_names}
    primary_keys: set[int] = set()
    for key in table_entry.get("primary_keys", []):
        if isinstance(key, list):
            primary_keys.update(int(index) for index in key)
        else:
            primary_keys.add(int(key))
    foreign_pairs = {
        int(source): int(target)
        for source, target in table_entry.get("foreign_keys", [])
    }
    column_names = table_entry["column_names_original"]
    column_types = table_entry["column_types"]
    for index, ((table_index, column_name), column_type) in enumerate(
        zip(column_names, column_types)
    ):
        if int(table_index) < 0:
            continue
        table_name = table_names[int(table_index)]
        description = descriptions.get(table_name, {}).get(str(column_name), {})
        columns[table_name].append(
            {
                "name": column_name,
                "type": column_type,
                "primary_key": index in primary_keys,
                "foreign_key": index in foreign_pairs,
                **description,
            }
        )
    relationships = []
    for source, target in foreign_pairs.items():
        source_table_index, source_column = column_names[source]
        target_table_index, target_column = column_names[target]
        relationships.append(
            {
                "from": f"{table_names[int(source_table_index)]}.{source_column}",
                "to": f"{table_names[int(target_table_index)]}.{target_column}",
            }
        )
    return {
        "db_id": db_id,
        "tables": [
            {"name": name, "columns": columns.get(name, [])} for name in table_names
        ],
        "relationships": relationships,
    }


def structure_prompt(structure: dict[str, Any]) -> str:
    lines = [f"# Official BIRD structure layer: {structure['db_id']}"]
    for table in structure["tables"]:
        rendered_columns = []
        for column in table["columns"]:
            detail = f"{column['name']} {column['type']}"
            if column.get("description"):
                detail += f" — {column['description']}"
            if column.get("values"):
                detail += f"; values: {column['values']}"
            rendered_columns.append(detail)
        lines.append(f"\n## {table['name']}\n" + "\n".join(f"- {item}" for item in rendered_columns))
    if structure["relationships"]:
        lines.append("\n# Foreign-key relationships")
        lines.extend(
            f"- {item['from']} -> {item['to']}" for item in structure["relationships"]
        )
    return "\n".join(lines)


def _clean_sql(raw: str) -> str:
    sql = raw.strip()
    if sql.startswith("```"):
        lines = sql.splitlines()
        lines = lines[1:] if lines else lines
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        sql = "\n".join(lines).strip()
    if sql.lower().startswith("sql\n"):
        sql = sql[4:].strip()
    return sql.rstrip(";").strip()


def _direct_system(context: str, evidence: str) -> str:
    return (
        "You are evaluating text-to-SQL on the official BIRD Mini-Dev benchmark.\n"
        "Use the supplied structure layer and official evidence. Generate one read-only SQLite SELECT query.\n"
        "Return SQL only, without Markdown or explanation.\n\n"
        f"{context}\n\n# Official BIRD evidence\n{evidence}"
    )


def _forge_context(context: str, evidence: str) -> str:
    return f"{context}\n\n# Official BIRD evidence\n{evidence}"


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def execute_result(
    db_path: Path,
    sql: str,
    *,
    limit: int = 20,
    timeout_seconds: float = 30.0,
) -> tuple[list[tuple[Any, ...]], dict[str, Any]]:
    validate_readonly_sql(sql)
    deadline = time.monotonic() + timeout_seconds
    interrupted = False
    db = sqlite3.connect(db_path)

    def abort_overlong_query() -> int:
        nonlocal interrupted
        if time.monotonic() >= deadline:
            interrupted = True
            return 1
        return 0

    db.set_progress_handler(abort_overlong_query, 10_000)
    try:
        cursor = db.execute(sql)
        columns = [item[0] for item in cursor.description or []]
        rows = cursor.fetchall()
    except sqlite3.OperationalError as exc:
        if interrupted:
            raise HardBenchmarkError(
                f"BIRD SQL execution timeout after {timeout_seconds:g}s"
            ) from exc
        raise
    finally:
        db.close()
    preview = {
        "columns": columns,
        "rows": [[_json_safe(value) for value in row] for row in rows[:limit]],
        "row_count": len(rows),
        "truncated": len(rows) > limit,
    }
    return rows, preview


def _compare_results(generated_rows: list[tuple[Any, ...]], gold_rows: list[tuple[Any, ...]]) -> bool:
    return bird_execution_accuracy(gold_rows, generated_rows)

def _decode_gold_value(value: Any) -> Any:
    if isinstance(value, dict) and set(value) == {"__bytes__"}:
        return bytes.fromhex(str(value["__bytes__"]))
    return value


def _cached_gold_answers(suite: dict[str, Any]) -> dict[str, dict[str, Any]]:
    if not _GOLD_RESULTS_PATH.exists():
        return {}
    payload = _load_json(_GOLD_RESULTS_PATH)
    if payload.get("suite_id") != _FULL_SUITE_ID:
        return {}
    entries = payload.get("cases", {})
    answers: dict[str, dict[str, Any]] = {}
    for case in suite["cases"]:
        case_id = _case_id(case)
        entry = entries.get(case_id)
        expected_hash = hashlib.sha256(str(case["SQL"]).encode()).hexdigest()
        if (
            not isinstance(entry, dict)
            or entry.get("db_id") != case["db_id"]
            or entry.get("sql_sha256") != expected_hash
        ):
            continue
        rows = [
            tuple(_decode_gold_value(value) for value in row)
            for row in entry.get("rows", [])
        ]
        answers[case_id] = {
            "rows": rows,
            "preview": {
                "columns": entry.get("columns", []),
                "rows": [[_json_safe(value) for value in row] for row in rows[:20]],
                "row_count": len(rows),
                "truncated": len(rows) > 20,
            },
        }
    return answers


def validate_gold_cases(
    suite: dict[str, Any],
    *,
    workers: int = 4,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> dict[str, dict[str, Any]]:
    answers = _cached_gold_answers(suite)
    cases = [
        case for case in suite["cases"]
        if _case_id(case) not in answers
    ]
    total = len(suite["cases"])
    if not cases:
        if on_progress:
            on_progress(total, total, "cache")
        return answers

    def validate(case: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        case_id = _case_id(case)
        rows, preview = execute_result(
            _database_path(str(case["db_id"])), str(case["SQL"])
        )
        return case_id, {"rows": rows, "preview": preview}

    pool = ThreadPoolExecutor(max_workers=max(1, workers))
    futures = {pool.submit(validate, case): case for case in cases}
    try:
        for completed, future in enumerate(as_completed(futures), start=len(answers) + 1):
            case_id, answer = future.result()
            answers[case_id] = answer
            if on_progress:
                on_progress(completed, total, case_id)
    except Exception:
        for future in futures:
            future.cancel()
        pool.shutdown(wait=True, cancel_futures=True)
        raise
    else:
        pool.shutdown(wait=True)
    return answers


def hydrate_suite(
    suite_id: str,
    *,
    workers: int = 4,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> dict[str, Any]:
    suite = load_suite(suite_id)
    answers = validate_gold_cases(suite, workers=workers, on_progress=on_progress)
    return {
        **suite,
        "cases": [
            {
                **case,
                "gold_preview": answers[_case_id(case)]["preview"],
                "_gold_rows": answers[_case_id(case)]["rows"],
            }
            for case in suite["cases"]
        ],
    }


class HardBenchmarkStore:
    def __init__(self, path: str | Path | None = None):
        self.path = Path(path).expanduser().resolve() if path else _db_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        return db

    def _ensure_schema(self) -> None:
        with self._lock, self._connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS hard_benchmark_runs (
                    run_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    suite_id TEXT NOT NULL,
                    model_json TEXT NOT NULL,
                    lineage_json TEXT NOT NULL,
                    runs_per_case INTEGER NOT NULL,
                    workers INTEGER NOT NULL,
                    compile_retries INTEGER NOT NULL,
                    total_cases INTEGER NOT NULL,
                    total_calls INTEGER NOT NULL,
                    sequence INTEGER NOT NULL DEFAULT 0,
                    error_code TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT
                );
                CREATE TABLE IF NOT EXISTS hard_benchmark_observations (
                    run_id TEXT NOT NULL,
                    method_id TEXT NOT NULL,
                    case_id TEXT NOT NULL,
                    run_index INTEGER NOT NULL,
                    result_json TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    PRIMARY KEY(run_id, method_id, case_id, run_index)
                );
                CREATE TABLE IF NOT EXISTS hard_benchmark_logs (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    method_id TEXT,
                    case_id TEXT,
                    run_index INTEGER,
                    stage TEXT NOT NULL,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                """
            )

    def reconcile_interrupted(self) -> int:
        with self._connect() as db:
            rows = db.execute(
                "SELECT run_id FROM hard_benchmark_runs WHERE status IN ('queued','running')"
            ).fetchall()
            for row in rows:
                db.execute(
                    "UPDATE hard_benchmark_runs SET status='interrupted',error_code='process_restarted',"
                    "completed_at=?,sequence=sequence+1 WHERE run_id=?",
                    (_now(), row["run_id"]),
                )
                self._log_tx(
                    db,
                    str(row["run_id"]),
                    stage="interrupted",
                    level="warning",
                    message=_SAFE_ERRORS["process_restarted"],
                )
        return len(rows)

    def create_run(
        self,
        config: HardBenchmarkConfig,
        snapshot: ModelConfigSnapshot,
        *,
        total_cases: int,
        lineage: dict[str, str],
    ) -> str:
        run_id = "hbr_" + uuid.uuid4().hex
        total_calls = total_cases * config.runs_per_case * len(_METHODS)
        model = {
            "provider": snapshot.provider,
            "name": snapshot.model,
            "revision": snapshot.revision,
            "source": snapshot.source,
            "endpoint_host": _safe_endpoint(snapshot.base_url),
        }
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            active = db.execute(
                "SELECT run_id FROM hard_benchmark_runs WHERE status IN ('queued','running') LIMIT 1"
            ).fetchone()
            if active:
                raise HardBenchmarkError(f"已有运行中的 Hard Benchmark：{active['run_id']}")
            db.execute(
                "INSERT INTO hard_benchmark_runs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    run_id,
                    "queued",
                    config.suite_id,
                    _canonical_json(model),
                    _canonical_json(lineage),
                    config.runs_per_case,
                    config.workers,
                    config.compile_retries,
                    total_cases,
                    total_calls,
                    0,
                    None,
                    _now(),
                    None,
                    None,
                ),
            )
            self._log_tx(
                db,
                run_id,
                stage="queued",
                level="info",
                message=f"已创建 BIRD Hard 双臂运行：{total_cases} 题，{total_calls} 次调用。",
            )
        return run_id

    def _log_tx(
        self,
        db: sqlite3.Connection,
        run_id: str,
        *,
        stage: str,
        level: str,
        message: str,
        method_id: str | None = None,
        case_id: str | None = None,
        run_index: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        db.execute(
            "INSERT INTO hard_benchmark_logs(run_id,method_id,case_id,run_index,stage,level,message,payload_json,created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (
                run_id,
                method_id,
                case_id,
                run_index,
                stage,
                level,
                message,
                _canonical_json(payload or {}),
                _now(),
            ),
        )
        db.execute(
            "UPDATE hard_benchmark_runs SET sequence=sequence+1 WHERE run_id=?", (run_id,)
        )

    def log(self, run_id: str, **entry: Any) -> None:
        with self._connect() as db:
            self._log_tx(db, run_id, **entry)

    def mark_running(self, run_id: str) -> None:
        with self._connect() as db:
            changed = db.execute(
                "UPDATE hard_benchmark_runs SET status='running',started_at=?,sequence=sequence+1 "
                "WHERE run_id=? AND status='queued'",
                (_now(), run_id),
            ).rowcount
            if changed == 1:
                self._log_tx(
                    db,
                    run_id,
                    stage="running",
                    level="info",
                    message="Forge 与 Direct SQL 公平对照已开始。",
                )
        if changed != 1:
            raise HardBenchmarkError("Hard Benchmark Run 不可启动。")

    def record_observation(self, run_id: str, result: dict[str, Any]) -> None:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            status = db.execute(
                "SELECT status FROM hard_benchmark_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if status is None or status["status"] != "running":
                raise HardBenchmarkError("Hard Benchmark Run 已不接受新结果。")
            db.execute(
                "INSERT INTO hard_benchmark_observations VALUES(?,?,?,?,?,?)",
                (
                    run_id,
                    result["method_id"],
                    result["case_id"],
                    result["run_index"],
                    _canonical_json(result),
                    _now(),
                ),
            )
            verdict = "正确" if result["correct"] else _SAFE_ERRORS.get(
                result.get("error_code") or "incorrect_result"
            )
            self._log_tx(
                db,
                run_id,
                method_id=result["method_id"],
                case_id=result["case_id"],
                run_index=result["run_index"],
                stage="evaluated",
                level="success" if result["correct"] else "warning",
                message=f"{result['method_id']} · Case {result['case_id']} · run {result['run_index']}：{verdict}",
                payload={
                    "correct": result["correct"],
                    "latency_ms": result["latency_ms"],
                    "error_code": result.get("error_code"),
                },
            )

    def complete(self, run_id: str, *, status: str, error_code: str | None = None) -> None:
        if status not in _TERMINAL:
            raise HardBenchmarkError("Hard Benchmark 终态不支持。")
        with self._connect() as db:
            changed = db.execute(
                "UPDATE hard_benchmark_runs SET status=?,error_code=?,completed_at=?,sequence=sequence+1 "
                "WHERE run_id=? AND status='running'",
                (status, error_code, _now(), run_id),
            ).rowcount
            if changed == 1:
                self._log_tx(
                    db,
                    run_id,
                    stage=status,
                    level="success" if status == "completed" else "error",
                    message=(
                        "BIRD Hard 双臂运行已完成。"
                        if status == "completed"
                        else _SAFE_ERRORS.get(error_code or "runtime_failed")
                    ),
                )
        if changed != 1:
            raise HardBenchmarkError("Hard Benchmark Run 状态已变化。")

    def latest_run_id(self) -> str | None:
        with self._connect() as db:
            row = db.execute(
                "SELECT run_id FROM hard_benchmark_runs ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
        return str(row["run_id"]) if row else None

    def run_suite_id(self, run_id: str) -> str | None:
        with self._connect() as db:
            row = db.execute(
                "SELECT suite_id FROM hard_benchmark_runs WHERE run_id=?", (run_id,)
            ).fetchone()
        return str(row["suite_id"]) if row else None

    def history(self, *, limit: int = 20) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 100))
        with self._connect() as db:
            runs = db.execute(
                "SELECT * FROM hard_benchmark_runs ORDER BY created_at DESC LIMIT ?",
                (safe_limit,),
            ).fetchall()
            result = []
            for run in runs:
                observations = [
                    json.loads(row["result_json"])
                    for row in db.execute(
                        "SELECT result_json FROM hard_benchmark_observations WHERE run_id=?",
                        (run["run_id"],),
                    ).fetchall()
                ]
                by_method = {
                    method: [item for item in observations if item["method_id"] == method]
                    for method in _METHODS
                }
                metrics = {
                    method: _method_metrics(
                        items, int(run["total_cases"]), int(run["runs_per_case"])
                    )
                    for method, items in by_method.items()
                }
                result.append(
                    {
                        "run_id": run["run_id"],
                        "suite_id": run["suite_id"],
                        "status": run["status"],
                        "model": json.loads(run["model_json"]),
                        "metrics": metrics,
                        "delta": {
                            "execution_accuracy": (
                                metrics["forge"]["execution_accuracy"]
                                - metrics["direct"]["execution_accuracy"]
                                if metrics["forge"]["execution_accuracy"] is not None
                                and metrics["direct"]["execution_accuracy"] is not None
                                else None
                            )
                        },
                        "total_cases": int(run["total_cases"]),
                        "total_calls": int(run["total_calls"]),
                        "completed_calls": len(observations),
                        "created_at": run["created_at"],
                        "completed_at": run["completed_at"],
                    }
                )
        return result

    def logs(
        self,
        run_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
        method_id: str | None = None,
        stage: str | None = None,
        level: str | None = None,
        case_id: str | None = None,
        search: str | None = None,
    ) -> dict[str, Any] | None:
        clauses = ["run_id=?"]
        params: list[Any] = [run_id]
        for column, value in (
            ("method_id", method_id),
            ("stage", stage),
            ("level", level),
            ("case_id", case_id),
        ):
            if value:
                clauses.append(f"{column}=?")
                params.append(value)
        if search:
            clauses.append("message LIKE ?")
            params.append(f"%{search}%")
        where = " AND ".join(clauses)
        safe_limit = max(1, min(int(limit), 500))
        safe_offset = max(0, int(offset))
        with self._connect() as db:
            if db.execute(
                "SELECT 1 FROM hard_benchmark_runs WHERE run_id=?", (run_id,)
            ).fetchone() is None:
                return None
            total = db.execute(
                f"SELECT COUNT(*) FROM hard_benchmark_logs WHERE {where}", params
            ).fetchone()[0]
            rows = db.execute(
                f"SELECT * FROM hard_benchmark_logs WHERE {where} "
                "ORDER BY log_id DESC LIMIT ? OFFSET ?",
                [*params, safe_limit, safe_offset],
            ).fetchall()
        return {
            "run_id": run_id,
            "total": int(total),
            "limit": safe_limit,
            "offset": safe_offset,
            "items": [
                {
                    **dict(row),
                    "payload": json.loads(row["payload_json"]),
                }
                for row in rows
            ],
        }

    def snapshot(self, run_id: str, suite: dict[str, Any]) -> dict[str, Any] | None:
        with self._connect() as db:
            run = db.execute(
                "SELECT * FROM hard_benchmark_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            observations = db.execute(
                "SELECT result_json,completed_at FROM hard_benchmark_observations WHERE run_id=? "
                "ORDER BY completed_at,method_id,case_id,run_index",
                (run_id,),
            ).fetchall()
            logs = db.execute(
                "SELECT * FROM hard_benchmark_logs WHERE run_id=? ORDER BY log_id DESC LIMIT 160",
                (run_id,),
            ).fetchall()
        if run is None:
            return None
        return project_snapshot(
            dict(run),
            [
                {**json.loads(row["result_json"]), "completed_at": row["completed_at"]}
                for row in observations
            ],
            [dict(row) for row in reversed(logs)],
            suite,
        )

    def rescore_legacy(self, suite: dict[str, Any]) -> int:
        """Replace legacy approximate verdicts with official BIRD EA verdicts."""
        cases = {_case_id(case): case for case in suite["cases"]}
        changed = 0
        changed_runs: set[str] = set()
        with self._lock, self._connect() as db:
            rows = db.execute(
                "SELECT o.rowid AS observation_id,o.run_id,o.method_id,o.case_id,o.run_index,o.result_json "
                "FROM hard_benchmark_observations AS o "
                "JOIN hard_benchmark_runs AS r ON r.run_id=o.run_id "
                "WHERE r.status IN ('completed','failed','interrupted')"
            ).fetchall()
            for row in rows:
                result = json.loads(row["result_json"])
                if result.get("scoring_standard") == _SCORING_STANDARD:
                    continue
                case = cases.get(str(row["case_id"]))
                if case is None:
                    continue
                generated_sql = result.get("generated_sql")
                if generated_sql:
                    try:
                        generated_rows, _ = execute_result(
                            _database_path(str(case["db_id"])), str(generated_sql)
                        )
                        result["executable"] = True
                        result["correct"] = _compare_results(
                            generated_rows, case["_gold_rows"]
                        )
                        result["error_code"] = (
                            None if result["correct"] else "incorrect_result"
                        )
                    except Exception:
                        result["executable"] = False
                        result["correct"] = False
                        result["error_code"] = "execution_failed"
                else:
                    result["correct"] = False
                result["error_message"] = _SAFE_ERRORS.get(result.get("error_code") or "")
                result["scoring_standard"] = _SCORING_STANDARD
                db.execute(
                    "UPDATE hard_benchmark_observations SET result_json=? WHERE rowid=?",
                    (_canonical_json(result), row["observation_id"]),
                )
                verdict = "正确" if result["correct"] else _SAFE_ERRORS.get(
                    result.get("error_code") or "incorrect_result"
                )
                db.execute(
                    "UPDATE hard_benchmark_logs SET level=?,message=?,payload_json=? "
                    "WHERE run_id=? AND method_id=? AND case_id=? AND run_index=? AND stage='evaluated'",
                    (
                        "success" if result["correct"] else "warning",
                        f"{row['method_id']} · Case {row['case_id']} · run {row['run_index']}：{verdict}",
                        _canonical_json(
                            {
                                "correct": result["correct"],
                                "latency_ms": result["latency_ms"],
                                "error_code": result.get("error_code"),
                            }
                        ),
                        row["run_id"],
                        row["method_id"],
                        row["case_id"],
                        row["run_index"],
                    ),
                )
                changed += 1
                changed_runs.add(str(row["run_id"]))
            for run_id in changed_runs:
                self._log_tx(
                    db,
                    run_id,
                    stage="rescored",
                    level="info",
                    message="历史观测已按 BIRD 官方 Execution Accuracy 精确集合标准重评分。",
                    payload={"scoring_standard": _SCORING_STANDARD},
                )
        return changed


def _method_metrics(
    observations: list[dict[str, Any]], total_cases: int, runs_per_case: int
) -> dict[str, Any]:
    completed = len(observations)
    correct_runs = sum(bool(item["correct"]) for item in observations)
    executable = sum(bool(item["executable"]) for item in observations)
    latencies = [float(item["latency_ms"]) for item in observations]
    by_case: dict[str, list[dict[str, Any]]] = {}
    for item in observations:
        by_case.setdefault(item["case_id"], []).append(item)
    completed_cases = [items for items in by_case.values() if len(items) == runs_per_case]
    passed_cases = sum(any(item["correct"] for item in items) for items in completed_cases)
    all_correct = sum(all(item["correct"] for item in items) for items in completed_cases)
    first_runs = [item for item in observations if int(item["run_index"]) == 1]
    return {
        "completed_calls": completed,
        "total_calls": total_cases * runs_per_case,
        "completed_cases": len(completed_cases),
        "total_cases": total_cases,
        "execution_accuracy": correct_runs / completed if completed else None,
        "first_run_ea": (
            sum(bool(item["correct"]) for item in first_runs) / len(first_runs)
            if first_runs else None
        ),
        "pass_at_k": passed_cases / len(completed_cases) if completed_cases else None,
        "consistent_at_k": all_correct / len(completed_cases) if completed_cases else None,
        "execution_success_rate": executable / completed if completed else None,
        "p95_latency_ms": _p95(latencies),
        "correct_runs": correct_runs,
    }


def project_snapshot(
    run: dict[str, Any],
    observations: list[dict[str, Any]],
    logs: list[dict[str, Any]],
    suite: dict[str, Any],
) -> dict[str, Any]:
    config_runs = int(run["runs_per_case"])
    case_source = {_case_id(case): case for case in suite["cases"]}
    by_method = {
        method: [item for item in observations if item["method_id"] == method]
        for method in _METHODS
    }
    metrics = {
        method: _method_metrics(items, int(run["total_cases"]), config_runs)
        for method, items in by_method.items()
    }
    case_items = []
    for case_id, case in case_source.items():
        method_results = {
            method: [item for item in by_method[method] if item["case_id"] == case_id]
            for method in _METHODS
        }
        scores = {
            method: sum(bool(item["correct"]) for item in items)
            for method, items in method_results.items()
        }
        completed = {method: len(items) for method, items in method_results.items()}
        if all(completed[method] == config_runs for method in _METHODS):
            winner = (
                "forge" if scores["forge"] > scores["direct"] else
                "direct" if scores["direct"] > scores["forge"] else "tie"
            )
            status = "complete"
        elif any(completed.values()):
            winner = None
            status = "running"
        else:
            winner = None
            status = "pending"
        gold_preview = case.get("gold_preview")
        if gold_preview is None:
            gold_preview = next(
                (
                    result.get("gold_preview")
                    for items in method_results.values()
                    for result in items
                    if result.get("gold_preview") is not None
                ),
                None,
            )
        case_items.append(
            {
                "case_id": case_id,
                "question_id": case["question_id"],
                "db_id": case["db_id"],
                "difficulty": case["difficulty"],
                "question": case["question"],
                "evidence": case["evidence"],
                "gold_sql": case["SQL"],
                "gold_preview": gold_preview,
                "source": case["source"],
                "status": status,
                "winner": winner,
                "scores": scores,
                "completed": completed,
                "results": method_results,
            }
        )
    completed_calls = len(observations)
    total_calls = int(run["total_calls"])
    projected_logs = [
        {
            "log_id": row["log_id"],
            "method_id": row["method_id"],
            "case_id": row["case_id"],
            "run_index": row["run_index"],
            "stage": row["stage"],
            "level": row["level"],
            "message": row["message"],
            "payload": json.loads(row["payload_json"]),
            "created_at": row["created_at"],
        }
        for row in logs
    ]
    is_full_suite = run["suite_id"] == _FULL_SUITE_ID
    return {
        "schema_version": 3,
        "projection_type": "hard_accuracy_comparison_v3",
        "run_id": run["run_id"],
        "status": run["status"],
        "score_phase": "final" if run["status"] in _TERMINAL else "partial",
        "scoring_standard": _SCORING_STANDARD,
        "sequence": int(run["sequence"]),
        "suite": suite["manifest"],
        "model": json.loads(run["model_json"]),
        "lineage": json.loads(run["lineage_json"]),
        "config": {
            "suite_id": run["suite_id"],
            "runs_per_case": config_runs,
            "workers": int(run["workers"]),
            "compile_retries": int(run["compile_retries"]),
            "methods": list(_METHODS),
        },
        "progress": {
            "completed_calls": completed_calls,
            "total_calls": total_calls,
            "percent": completed_calls / total_calls if total_calls else 0.0,
            "completed_cases": sum(item["status"] == "complete" for item in case_items),
            "total_cases": int(run["total_cases"]),
        },
        "metrics": metrics,
        "delta": {
            "execution_accuracy": (
                metrics["forge"]["execution_accuracy"] - metrics["direct"]["execution_accuracy"]
                if metrics["forge"]["execution_accuracy"] is not None
                and metrics["direct"]["execution_accuracy"] is not None
                else None
            )
        },
        "cases": case_items,
        "structures": {
            db_id: structure_projection(entry)
            for db_id, entry in suite["tables"].items()
        },
        "logs": projected_logs,
        "error_code": run["error_code"],
        "error_message": _SAFE_ERRORS.get(run["error_code"] or ""),
        "created_at": run["created_at"],
        "started_at": run["started_at"],
        "completed_at": run["completed_at"],
        "disclaimer": (
            "按 BIRD 官方 Execution Accuracy（SQLite 结果 tuple 集合精确相等）评分。"
            + (
                "完整 Mini-Dev 覆盖 500 题与 11 个数据库；Oracle Evidence 随题提供。"
                if is_full_suite
                else "当前运行来自 12/102 challenging 题、2/11 数据库的历史诊断子集。"
            )
            + "该结果不代表无 Evidence 或真实客户环境准确率。"
        ),
    }


def create_hard_run(
    store: HardBenchmarkStore,
    config: HardBenchmarkConfig | None = None,
) -> tuple[str, ModelConfigSnapshot, dict[str, Any]]:
    config = config or HardBenchmarkConfig()
    suite = load_suite(config.suite_id)
    snapshot = _method_ai_snapshot(ark_coding_plan_method(_ROOT))
    lineage = {
        "code_revision": _code_revision(),
        "bird_cases_revision": _file_sha256(suite["_case_path"]),
        "bird_tables_revision": _file_sha256(suite["_tables_path"]),
        "database_count": str(len(suite["tables"])),
        "scoring_standard": _SCORING_STANDARD,
    }
    run_id = store.create_run(
        config,
        snapshot,
        total_cases=len(suite["cases"]),
        lineage=lineage,
    )
    return run_id, snapshot, suite


def run_hard_benchmark(
    store: HardBenchmarkStore,
    run_id: str,
    snapshot: ModelConfigSnapshot,
    suite: dict[str, Any],
) -> None:
    from agent.prompts import build_system

    current = store.snapshot(run_id, suite)
    if current is None:
        raise HardBenchmarkError("Hard Benchmark Run 不存在。")
    runs_per_case = int(current["config"]["runs_per_case"])
    workers = int(current["config"]["workers"])
    compile_retries = int(current["config"]["compile_retries"])
    store.mark_running(run_id)
    store.log(
        run_id,
        stage="gold_validation",
        level="info",
        message=f"开始后台校验 {len(suite['cases'])} 条 BIRD Gold SQL；校验完成前不会调用模型。",
    )

    def report_gold_progress(completed: int, total: int, case_id: str) -> None:
        if completed == total or completed % 10 == 0:
            store.log(
                run_id,
                case_id=case_id,
                stage="gold_validation",
                level="info",
                message=f"Gold SQL 预检：{completed}/{total}。",
                payload={"completed": completed, "total": total},
            )

    try:
        suite = hydrate_suite(
            current["config"]["suite_id"],
            workers=workers,
            on_progress=report_gold_progress,
        )
    except Exception:
        store.complete(run_id, status="failed", error_code="gold_validation_failed")
        raise
    store.log(
        run_id,
        stage="gold_validation",
        level="success",
        message="BIRD Gold SQL 预检完成，开始双臂模型调用。",
    )
    structures = {
        db_id: structure_projection(entry) for db_id, entry in suite["tables"].items()
    }
    tasks = [
        (case, run_index, method)
        for case in suite["cases"]
        for run_index in range(1, runs_per_case + 1)
        for method in _METHODS
    ]
    random.Random(20260826).shuffle(tasks)

    def dispatch(task: tuple[dict[str, Any], int, str]) -> dict[str, Any]:
        case, run_index, method = task
        case_id = _case_id(case)
        context = structure_prompt(structures[str(case["db_id"])])
        evidence = str(case["evidence"])
        store.log(
            run_id,
            method_id=method,
            case_id=case_id,
            run_index=run_index,
            stage="model_call",
            level="info",
            message=f"{method} · Case {case_id} · run {run_index}：请求 Coding Plan。",
        )
        started = time.monotonic()
        generated_sql: str | None = None
        forge_json: dict[str, Any] | None = None
        attempts = 1
        error_code: str | None = None
        try:
            if method == "forge":
                system = build_system(
                    _forge_context(context, evidence),
                    question=str(case["question"]),
                    mode="benchmark",
                )
                generated = run_forge_oai(
                    snapshot.api_key,
                    snapshot.base_url,
                    str(case["question"]),
                    system,
                    snapshot.model,
                    max_compile_retries=compile_retries,
                )
                generated_sql = generated.get("sql")
                forge_json = generated.get("forge_json")
                attempts = int(generated.get("attempts", 1))
                if generated.get("error_code") or not generated_sql:
                    error_code = "compile_failed"
            else:
                raw_sql = run_sql_oai(
                    snapshot.api_key,
                    snapshot.base_url,
                    str(case["question"]),
                    _direct_system(context, evidence),
                    snapshot.model,
                ).get("sql")
                generated_sql = _clean_sql(str(raw_sql or ""))
                if not generated_sql:
                    error_code = "generation_failed"
        except Exception:
            error_code = "generation_failed"

        latency_ms = round((time.monotonic() - started) * 1000, 1)
        executable = False
        correct = False
        generated_preview: dict[str, Any] | None = None
        if generated_sql and error_code is None:
            try:
                generated_rows, generated_preview = execute_result(
                    _database_path(str(case["db_id"])), generated_sql
                )
                executable = True
                correct = _compare_results(generated_rows, case["_gold_rows"])
                if not correct:
                    error_code = "incorrect_result"
            except Exception:
                error_code = "execution_failed"
        return {
            "method_id": method,
            "case_id": case_id,
            "run_index": run_index,
            "db_id": case["db_id"],
            "correct": correct,
            "scoring_standard": _SCORING_STANDARD,
            "executable": executable,
            "attempts": attempts,
            "latency_ms": latency_ms,
            "error_code": error_code,
            "error_message": _SAFE_ERRORS.get(error_code or ""),
            "generated_sql": generated_sql,
            "forge_json": forge_json,
            "generated_preview": generated_preview,
            "gold_preview": case["gold_preview"],
            "sql_hash": (
                "sha256:" + hashlib.sha256(generated_sql.encode()).hexdigest()
                if generated_sql else None
            ),
        }

    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_map = {pool.submit(dispatch, task): task for task in tasks}
            for future in as_completed(future_map):
                case, run_index, method = future_map[future]
                try:
                    result = future.result()
                except Exception:
                    result = {
                        "method_id": method,
                        "case_id": _case_id(case),
                        "run_index": run_index,
                        "db_id": case["db_id"],
                        "correct": False,
                        "executable": False,
                        "attempts": 0,
                        "latency_ms": 0.0,
                        "error_code": "generation_failed",
                        "error_message": _SAFE_ERRORS["generation_failed"],
                        "generated_sql": None,
                        "forge_json": None,
                        "generated_preview": None,
                        "gold_preview": case["gold_preview"],
                        "sql_hash": None,
                    }
                store.record_observation(run_id, result)
        store.complete(run_id, status="completed")
    except Exception:
        try:
            store.complete(run_id, status="failed", error_code="runtime_failed")
        except HardBenchmarkError:
            pass
        raise


class HardBenchmarkService:
    def __init__(
        self,
        store: HardBenchmarkStore | None = None,
        *,
        reconcile_on_start: bool = False,
    ):
        self.store = store or HardBenchmarkStore()
        if reconcile_on_start:
            self.store.reconcile_interrupted()
        legacy_suite = hydrate_suite(_LEGACY_SUITE_ID)
        self.store.rescore_legacy(legacy_suite)
        self._suite_cache: dict[str, dict[str, Any]] = {
            _LEGACY_SUITE_ID: legacy_suite,
        }
        self._active_suites: dict[str, dict[str, Any]] = {}
        self._preview_suite = load_suite(_FULL_SUITE_ID)

    def _suite(self, suite_id: str) -> dict[str, Any]:
        suite = self._suite_cache.get(suite_id)
        if suite is None:
            suite = load_suite(suite_id)
            self._suite_cache[suite_id] = suite
        return suite

    def _suite_for_run(self, run_id: str) -> dict[str, Any] | None:
        suite_id = self.store.run_suite_id(run_id)
        return self._suite(suite_id) if suite_id else None

    def create(self, config: HardBenchmarkConfig | None = None):
        run_id, snapshot, suite = create_hard_run(self.store, config)
        self._suite_cache[config.suite_id if config else _FULL_SUITE_ID] = suite
        self._active_suites[run_id] = suite
        return run_id, snapshot

    def run(self, run_id: str, snapshot: ModelConfigSnapshot) -> None:
        suite = self._active_suites.get(run_id) or self._suite_for_run(run_id)
        if suite is None:
            raise HardBenchmarkError("Hard Benchmark Run 不存在。")
        try:
            run_hard_benchmark(self.store, run_id, snapshot, suite)
        finally:
            self._active_suites.pop(run_id, None)

    def latest(self) -> dict[str, Any] | None:
        run_id = self.store.latest_run_id()
        return self.snapshot(run_id) if run_id else None

    def snapshot(self, run_id: str) -> dict[str, Any] | None:
        suite = self._suite_for_run(run_id)
        return self.store.snapshot(run_id, suite) if suite else None

    def history(self, *, limit: int = 20) -> list[dict[str, Any]]:
        return self.store.history(limit=limit)

    def logs(self, run_id: str, **filters: Any) -> dict[str, Any] | None:
        return self.store.logs(run_id, **filters)

    def suite_preview(self) -> dict[str, Any]:
        return {
            "manifest": self._preview_suite["manifest"],
            "cases": [
                {
                    "case_id": _case_id(case),
                    "question_id": case["question_id"],
                    "db_id": case["db_id"],
                    "difficulty": case["difficulty"],
                    "question": case["question"],
                }
                for case in self._preview_suite["cases"]
            ],
            "structures": {
                db_id: structure_projection(entry)
                for db_id, entry in self._preview_suite["tables"].items()
            },
        }


_service: HardBenchmarkService | None = None
_service_lock = threading.Lock()


def get_hard_benchmark_service() -> HardBenchmarkService:
    global _service
    with _service_lock:
        if _service is None:
            _service = HardBenchmarkService(reconcile_on_start=True)
        return _service
