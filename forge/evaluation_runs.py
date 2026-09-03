"""Persistent, reproducible evaluation suites and regression release gates."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import threading
from typing import Any
import uuid

from agent.contracts import validate_contract
from forge.assurance import ASSURANCE_REVISION, POLICY_REVISION, QUERY_CANDIDATE_REVISION
from forge.evaluate import (
    EVALUATOR_REVISION,
    RESULT_COMPARATOR_REVISION,
    canonical_hash,
    evaluate_query_candidate,
)

EVALUATION_RUN_SCHEMA_VERSION = 1
_DEFAULT_GATE = {"max_new_failures": 0, "max_pass_rate_drop": 0.0}


class EvaluationRunError(RuntimeError):
    """Bounded public evaluation-run error."""


class EvaluationRunNotFound(EvaluationRunError):
    """Requested suite or run does not exist."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _db_path() -> Path:
    return Path(
        os.getenv("EVALUATION_RUN_DB_PATH", ".forge/evaluation_runs.db")
    ).expanduser().resolve()


class EvaluationRunStore:
    """SQLite truth source for immutable evaluation suites and run manifests."""

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
        with self._schema_lock, self._connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS evaluation_suites (
                    suite_revision TEXT PRIMARY KEY,
                    suite_id TEXT NOT NULL,
                    manifest_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS evaluation_runs (
                    run_id TEXT PRIMARY KEY,
                    suite_revision TEXT NOT NULL,
                    manifest_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(suite_revision) REFERENCES evaluation_suites(suite_revision)
                );
                CREATE INDEX IF NOT EXISTS idx_evaluation_runs_created
                ON evaluation_runs(created_at DESC);
                """
            )

    def save_suite(self, suite: dict[str, Any]) -> str:
        validate_contract("evaluation_suite_v1", suite)
        suite_revision = canonical_hash(suite)
        encoded = _canonical_json(suite)
        with self._connect() as db:
            db.execute(
                "INSERT OR IGNORE INTO evaluation_suites "
                "(suite_revision,suite_id,manifest_json,created_at) VALUES(?,?,?,?)",
                (suite_revision, suite["suite_id"], encoded, _now()),
            )
            stored = db.execute(
                "SELECT manifest_json FROM evaluation_suites WHERE suite_revision=?",
                (suite_revision,),
            ).fetchone()
        if stored is None or stored["manifest_json"] != encoded:
            raise EvaluationRunError("Evaluation suite revision collision")
        return suite_revision

    def get_suite(self, suite_revision: str) -> dict[str, Any]:
        with self._connect() as db:
            row = db.execute(
                "SELECT manifest_json FROM evaluation_suites WHERE suite_revision=?",
                (suite_revision,),
            ).fetchone()
        if row is None:
            raise EvaluationRunNotFound("Evaluation suite not found")
        return json.loads(row["manifest_json"])

    def save_run(self, manifest: dict[str, Any]) -> None:
        validate_contract("evaluation_run_manifest_v1", manifest)
        with self._connect() as db:
            db.execute(
                "INSERT INTO evaluation_runs "
                "(run_id,suite_revision,manifest_json,created_at) VALUES(?,?,?,?)",
                (
                    manifest["run_id"],
                    manifest["suite_revision"],
                    _canonical_json(manifest),
                    manifest["created_at"],
                ),
            )

    def get_run(self, run_id: str) -> dict[str, Any]:
        with self._connect() as db:
            row = db.execute(
                "SELECT manifest_json FROM evaluation_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
        if row is None:
            raise EvaluationRunNotFound("Evaluation run not found")
        return json.loads(row["manifest_json"])


def _evaluation_basis(suite: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "case_id": case["case_id"],
            "question": case["question"],
            "dialect": case.get("dialect", "auto"),
            "allowed_tables": case.get("allowed_tables"),
            "expected_result": case.get("expected_result"),
            "expected_outcome": case["expected_outcome"],
        }
        for case in suite["cases"]
    ]


def _case_outcome(case: dict[str, Any]) -> dict[str, Any]:
    request = {
        key: value
        for key, value in case.items()
        if key not in {"case_id", "expected_outcome"}
    }
    evaluation = evaluate_query_candidate(request)
    expected = case["expected_outcome"]
    failure = evaluation.get("failure")
    observed = {
        "status": evaluation["status"],
        "failure_code": failure["code"] if failure else None,
    }
    matches = observed["status"] == expected["status"]
    if expected.get("failure_code") is not None:
        matches = matches and observed["failure_code"] == expected["failure_code"]
    return {
        "case_id": case["case_id"],
        "status": "passed" if matches else "failed",
        "expected": expected,
        "observed": observed,
        "evaluation": evaluation,
    }


def recompute_aggregate(outcomes: list[dict[str, Any]]) -> dict[str, Any]:
    """Recompute the published aggregate from raw case outcomes."""
    status_counts = Counter(outcome["status"] for outcome in outcomes)
    evaluation_status_counts = Counter(
        outcome["evaluation"]["status"] for outcome in outcomes
    )
    failure_code_counts = Counter(
        outcome["observed"]["failure_code"]
        for outcome in outcomes
        if outcome["observed"]["failure_code"] is not None
    )
    total = len(outcomes)
    passed = status_counts["passed"]
    return {
        "total_cases": total,
        "passed_cases": passed,
        "failed_cases": status_counts["failed"],
        "pass_rate": passed / total if total else 0.0,
        "evaluation_status_counts": dict(sorted(evaluation_status_counts.items())),
        "failure_code_counts": dict(sorted(failure_code_counts.items())),
    }


def _configuration(
    suite: dict[str, Any], outcomes: list[dict[str, Any]]
) -> dict[str, Any]:
    registry_revisions = sorted(
        {
            revision
            for outcome in outcomes
            if (revision := outcome["evaluation"]["lineage"]["registry_revision"])
            is not None
        }
    )
    return {
        "dataset": suite["dataset"],
        "case_selection_revision": canonical_hash(
            [case["case_id"] for case in suite["cases"]]
        ),
        "evaluation_basis_revision": canonical_hash(_evaluation_basis(suite)),
        "producer": suite["producer"],
        "retry_policy_revision": suite["retry_policy_revision"],
        "timeout_policy_revision": suite["timeout_policy_revision"],
        "evaluator_revision": EVALUATOR_REVISION,
        "metric_revision": RESULT_COMPARATOR_REVISION,
        "candidate_contract_revision": QUERY_CANDIDATE_REVISION,
        "assurance_revision": ASSURANCE_REVISION,
        "policy_revision": POLICY_REVISION,
        "registry_revisions": registry_revisions,
        "dialects": sorted({case.get("dialect", "auto") for case in suite["cases"]}),
    }


_COMPARABILITY_FIELDS = (
    "dataset",
    "case_selection_revision",
    "evaluation_basis_revision",
    "retry_policy_revision",
    "timeout_policy_revision",
    "evaluator_revision",
    "metric_revision",
    "candidate_contract_revision",
    "assurance_revision",
    "policy_revision",
    "registry_revisions",
    "dialects",
)


def _regression_result(
    current: dict[str, Any],
    baseline: dict[str, Any] | None,
    gate: dict[str, Any],
) -> dict[str, Any]:
    if baseline is None:
        return {
            "status": "not_requested",
            "release_gate": "not_evaluated",
            "baseline_run_id": None,
            "comparable": None,
            "incompatible_dimensions": [],
            "new_failures": [],
            "recovered_cases": [],
            "pass_rate_delta": None,
            "gate": gate,
        }

    current_configuration = current["configuration"]
    baseline_configuration = baseline["configuration"]
    incompatible = [
        field
        for field in _COMPARABILITY_FIELDS
        if current_configuration.get(field) != baseline_configuration.get(field)
    ]
    if incompatible:
        return {
            "status": "not_comparable",
            "release_gate": "failed",
            "baseline_run_id": baseline["run_id"],
            "comparable": False,
            "incompatible_dimensions": incompatible,
            "new_failures": [],
            "recovered_cases": [],
            "pass_rate_delta": None,
            "gate": gate,
        }

    baseline_cases = {outcome["case_id"]: outcome for outcome in baseline["outcomes"]}
    current_cases = {outcome["case_id"]: outcome for outcome in current["outcomes"]}
    new_failures = sorted(
        case_id
        for case_id, outcome in current_cases.items()
        if baseline_cases[case_id]["status"] == "passed" and outcome["status"] == "failed"
    )
    recovered = sorted(
        case_id
        for case_id, outcome in current_cases.items()
        if baseline_cases[case_id]["status"] == "failed" and outcome["status"] == "passed"
    )
    pass_rate_delta = (
        current["aggregate"]["pass_rate"] - baseline["aggregate"]["pass_rate"]
    )
    passed = (
        len(new_failures) <= gate["max_new_failures"]
        and pass_rate_delta >= -gate["max_pass_rate_drop"]
    )
    return {
        "status": "passed" if passed else "failed",
        "release_gate": "passed" if passed else "failed",
        "baseline_run_id": baseline["run_id"],
        "comparable": True,
        "incompatible_dimensions": [],
        "new_failures": new_failures,
        "recovered_cases": recovered,
        "pass_rate_delta": pass_rate_delta,
        "gate": gate,
    }


def create_evaluation_run(
    store: EvaluationRunStore,
    suite: dict[str, Any],
    *,
    baseline_run_id: str | None = None,
    regression_gate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate and atomically persist a complete, reproducible run manifest."""
    validate_contract("evaluation_suite_v1", suite)
    baseline = store.get_run(baseline_run_id) if baseline_run_id else None
    suite_revision = store.save_suite(suite)
    gate = {**_DEFAULT_GATE, **(regression_gate or {})}
    outcomes = [_case_outcome(case) for case in suite["cases"]]
    aggregate = recompute_aggregate(outcomes)
    manifest = {
        "schema_version": EVALUATION_RUN_SCHEMA_VERSION,
        "run_id": "evr_" + uuid.uuid4().hex,
        "status": "completed",
        "suite_revision": suite_revision,
        "suite": suite,
        "configuration": _configuration(suite, outcomes),
        "aggregate": aggregate,
        "regression": {
            "status": "not_requested",
            "release_gate": "not_evaluated",
            "baseline_run_id": None,
            "comparable": None,
            "incompatible_dimensions": [],
            "new_failures": [],
            "recovered_cases": [],
            "pass_rate_delta": None,
            "gate": gate,
        },
        "outcomes": outcomes,
        "created_at": _now(),
    }
    manifest["regression"] = _regression_result(manifest, baseline, gate)
    validate_contract("evaluation_run_manifest_v1", manifest)
    store.save_run(manifest)
    return manifest
