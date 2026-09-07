"""Immutable BIRD inputs and offline candidate replay; never dispatches a model.

Minimal ledger: {"schema_version":"bird-candidates-v1", "protocol_revision":
"sha256:…", "candidates":[{"case_id":"md-000", "arm":"direct", "output":
"SELECT …"}, {"case_id":"md-000", "arm":"forge", "output":{…}}]}.
Exactly one record per selected case/arm is required. A failed generation uses
output:null and failure:{…}; it remains in the denominator. Pi run exports use
protocol_manifest/protocol_revision and cases[].{forge,direct}.output instead.
Unversioned Pi exports require --diagnostic and cannot establish a new gain.
"""
from __future__ import annotations

import copy
import hashlib
import json
import random
import re
from collections import defaultdict
from functools import lru_cache
from importlib.metadata import version as package_version
import platform
import sqlite3
from pathlib import Path
from typing import Any

from forge import hard_accuracy_benchmark as hard
from forge.benchmark_v2 import RESULT_COMPARATOR_REVISION
from agent.prompts import STRUCTURED_BENCHMARK_PROMPT_REVISION, STRUCTURED_BENCHMARK_PROMPT_REVISIONS

ROOT = Path(__file__).resolve().parents[1]
PROTOCOL_DIR = ROOT / ".forge" / "benchmarks" / "protocols"
STANDARD_CATALOG = ROOT / "tests/datasets/bird_mini_dev_standard.json"
VERSION = "bird-protocol-v4"
FACTORS = ("model", "prompt", "compiler", "schema", "date_context", "grain_context", "value_context")
_GRAIN_PATTERN = re.compile(r"(?:\b(?:each|per)\b|\bgrouped?\s+by\b|每个|各(?:个|类|项)?|按.+(?:分组|统计))")


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical(value)).hexdigest()


@lru_cache(maxsize=4096)
def _file_hash(path: str, signature: tuple[int, ...]) -> str:
    with open(path, "rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def file_hash(path: Path) -> str:
    stat = path.stat()
    signature = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
    result = _file_hash(str(path), signature)
    after = path.stat()
    if signature != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise ValueError("Input changed while fingerprinting")
    return result


def _fingerprints(root: Path, paths: list[Path]) -> dict[str, str]:
    root = root.resolve(strict=True)
    result = {}
    for path in sorted(paths):
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(root) or path.is_symlink():
            raise ValueError("Dataset/source paths must remain inside the configured root")
        result[path.relative_to(root).as_posix()] = file_hash(resolved)
    return result


@lru_cache(maxsize=4)
def _load_standard(path: str, revision: str) -> dict[str, str]:
    return json.loads(Path(path).read_text(encoding="utf-8"))["files"]


def standard_files() -> dict[str, str]:
    return _load_standard(str(STANDARD_CATALOG), file_hash(STANDARD_CATALOG))


def dataset_fingerprints() -> dict[str, str]:
    # Never use a path supplied by a client manifest. Include every official DB,
    # metadata file and dataset file, not just the sampled databases.
    root = hard.bird_runtime_root().resolve(strict=True)
    entries = list(root.rglob("*"))
    if any(path.is_symlink() for path in entries):
        raise ValueError("Official dataset must not contain symlinks")
    if any(p.is_file() and p.name.endswith(("-wal", "-journal")) and p.stat().st_size for p in entries):
        raise ValueError("Nonempty SQLite WAL/journal: official snapshot has uncheckpointed state")
    # SHM contains volatile read locks, not data. Empty WAL/journal files likewise
    # carry no data. Never modify or delete the caller's SQLite sidecars.
    paths = [root / "mini_dev_sqlite.json", root / "dev_tables.json",
             *root.rglob("*.sqlite"), *root.rglob("*.csv")]
    actual = _fingerprints(root, paths)
    if actual != standard_files():
        raise ValueError("Dataset differs from the repository-pinned official public snapshot")
    return actual


def source_fingerprints() -> dict[str, dict[str, str]]:
    groups = {
        "prompt": [ROOT / "agent/prompts.py"],
        "schema": [ROOT / "forge/schema.json", ROOT / "agent/contracts/query-candidate-v1.schema.json"],
        "compiler": [ROOT / "forge/compiler.py"],
        "context": [ROOT / "forge/benchmark_v2.py", ROOT / "forge/benchmark_metadata.py"],
        "safety": [ROOT / "forge/executor.py", ROOT / "forge/lint.py", ROOT / "registry/relationships.py"],
        "official_snapshot": [STANDARD_CATALOG],
        "evaluator": [ROOT / "forge/hard_accuracy_benchmark.py", ROOT / "forge/benchmark_methods.py",
                      ROOT / "forge/assurance.py", ROOT / "forge/benchmark_service.py",
                      ROOT / "forge/bird_benchmark.py", ROOT / "agent/contracts/__init__.py",
                      ROOT / "agent/contracts/benchmark-failure-v1.schema.json", ROOT / "pyproject.toml"],
    }
    return {name: _fingerprints(ROOT, paths) for name, paths in groups.items()}


@lru_cache(maxsize=1)
def runtime_versions() -> dict[str, str]:
    return {"python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
            **{name: package_version(name) for name in ("sqlglot", "jsonschema", "numpy", "pydantic")}}


def generation(count: int) -> dict[str, Any]:
    return {"max_model_calls": 2 * count, "provider_retries": 0,
            "max_agent_turns_per_arm": 1, "timeout_seconds": 120,
            "sampling": "provider_default", "max_output_tokens": None}


def select_cases(suite: dict, cohort: str, case_ids: list[str] | None,
                 seed: int | None, size: int | None) -> tuple[list[str], dict]:
    available = [c["case_id"] for c in suite["cases"]]
    if cohort not in ("D", "R"):
        raise ValueError("Exposed Mini-Dev permits D or full-500 R, never H")
    if cohort == "R":
        if case_ids is not None or seed is not None or size is not None or len(available) != 500:
            raise ValueError("R requires all official 500 cases, with no selection flags")
        return available, {"method": "full_500"}
    if case_ids is not None:
        if seed is not None or size is not None:
            raise ValueError("Explicit cases and seeded sampling are mutually exclusive")
        if not case_ids or len(set(case_ids)) != len(case_ids) or not set(case_ids) <= set(available):
            raise ValueError("case_ids must be unique, nonempty official IDs")
        return case_ids, {"method": "explicit"}
    if seed is None or size is None or not 1 <= size <= len(available):
        raise ValueError("D requires --case-ids or --seed and --size")
    strata: dict[tuple[str, str], list[str]] = defaultdict(list)
    for case in suite["cases"]:
        strata[(case["db_id"], case["difficulty"])].append(case["case_id"])
    rng = random.Random(seed)
    groups = list(sorted(strata.items()))
    for _, values in groups:
        values.sort()
        rng.shuffle(values)
    rng.shuffle(groups)
    chosen = []
    while len(chosen) < size:
        for _, values in groups:
            if values and len(chosen) < size:
                chosen.append(values.pop())
    return chosen, {"method": "seeded_db_difficulty_round_robin_v1", "seed": seed, "size": size}


def repeat_history(previous: dict, history: list[dict], *, provider: str, model: str) -> dict:
    """Derive exposure from saved Pi runs/candidate ledgers, never a task-state mirror."""
    expected = {"provider": provider, "model": model}
    if previous.get("status") not in ("completed", "failed", "stopped", "interrupted"):
        raise ValueError("Previous run must be terminal")
    cases = previous.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("Previous Pi run must contain cases")
    previous_ids = [c["case_id"] for c in cases]
    if len(set(previous_ids)) != len(previous_ids):
        raise ValueError("Duplicate previous case")
    failed = sorted(c["case_id"] for c in cases if any(
        c[arm].get("scored") is False or c[arm].get("official_ea") is not True or
        c[arm].get("contract_accuracy") is not True
        for arm in ("forge", "direct")))
    sources = []
    seen: set[str] = set()
    for item in [previous, *history]:
        snapshot = item.get("model", {})
        if {key: snapshot.get(key) for key in expected} != expected:
            raise ValueError("All history must match the selected provider/model")
        exposed = set()
        if isinstance(item.get("cases"), list):
            for case in item["cases"]:
                if any(case[arm].get("output") is not None or
                       (case[arm].get("evidence") or {}).get("dispatches", 0) > 0
                       for arm in ("forge", "direct")):
                    exposed.add(case["case_id"])
        elif item.get("schema_version") == "bird-candidates-v1":
            for candidate in item["candidates"]:
                if candidate.get("output") is not None or (candidate.get("evidence") or {}).get("dispatches", 0) > 0:
                    exposed.add(candidate["case_id"])
        else:
            raise ValueError("History requires saved Pi runs or bird-candidates-v1 ledgers")
        seen.update(exposed)
        sources.append({"content_hash": digest(item), "case_ids": sorted(exposed)})
    return {"model": expected, "previous": {"content_hash": digest(previous),
            "run_id": previous.get("run_id"), "case_ids": previous_ids, "failed_case_ids": failed},
            "sources": sources, "seen_case_ids": sorted(seen)}


def _repeat_selection(suite: dict, history: dict, seed: int, size: int) -> tuple[list[str], dict]:
    available = {c["case_id"] for c in suite["cases"]}
    if type(seed) is not int or type(size) is not int or not 1 <= size <= len(available):
        raise ValueError("Repeated D selection requires integer seed and valid size")
    previous = history["previous"]
    required = previous["failed_case_ids"]
    seen = set(history["seen_case_ids"])
    source_seen = set().union(*(set(s["case_ids"]) for s in history["sources"]))
    if (seen != source_seen or not seen <= available or
            not set(previous["case_ids"]) <= available or not set(required) <= set(previous["case_ids"]) or
            len(set(required)) != len(required)):
        raise ValueError("Invalid previous failures or exposure history")
    if len(required) > size:
        raise ValueError("Previous failures exceed requested size; do not drop failures or expand budget")
    remaining = size - len(required)
    new_count = (remaining * 7 + 5) // 10  # nearest integer; exact halves round up
    old_count = remaining - new_count
    new_pool = sorted(available - seen - set(required))
    old_pool = sorted(seen - set(required))
    if len(new_pool) < new_count or len(old_pool) < old_count:
        raise ValueError(f"Insufficient exposure pools: need {new_count} new/{old_count} seen, "
                         f"have {len(new_pool)}/{len(old_pool)}; no silent substitution")
    rng = random.Random(seed)
    new = rng.sample(new_pool, new_count)
    old = rng.sample(old_pool, old_count)
    ids = [*required, *new, *old]
    return ids, {"method": "failure_carryover_70_30_v1", "seed": seed, "size": size,
                 "history": history, "carried_case_ids": required, "new_case_ids": new,
                 "reviewed_case_ids": old}



@lru_cache(maxsize=8)
def _date_audit(root: str, dataset_revision: str, audit_revision: str, max_rows: int) -> dict:
    """Cache only the same immutable data, audit implementation and sample bound."""
    from forge.benchmark_metadata import audit_metadata
    return audit_metadata(Path(root), max_rows=max_rows)


@lru_cache(maxsize=8)
def _value_audit(root: str, dataset_revision: str, audit_revision: str,
                 field: tuple[str, str, str], max_values: int) -> dict:
    """Cache identical data, source, field and cap; projection isolates caller mutations."""
    from forge.benchmark_metadata import audit_value_domain
    return audit_value_domain(Path(root), field, max_values=max_values)


def _contexts(suite: dict, case_ids: list[str], date_context: dict, grain_context: dict,
              value_config: dict, forge_prompt_revision: str = STRUCTURED_BENCHMARK_PROMPT_REVISION) -> dict[str, dict]:
    from forge.benchmark_service import build_context_response
    from forge.benchmark_metadata import date_context_evidence, render_date_context
    index = {c["case_id"]: c for c in suite["cases"]}
    audit = None
    if date_context["mode"] == "observed":
        audit = _date_audit(str(hard.bird_runtime_root().resolve()), digest(dataset_fingerprints()),
                            digest(source_fingerprints()), date_context["max_rows"])
    contexts = {}
    value_audit = None
    for cid in case_ids:
        context = build_context_response(suite, index[cid], forge_prompt_revision=forge_prompt_revision)
        evidence = date_context_evidence(audit, index[cid]["db_id"], context["context_snapshot"]["fields"]) if audit else None
        context["date_evidence"] = evidence
        suffix = render_date_context(evidence)
        if suffix:
            context["forge_instructions"] += "\n\n" + suffix
            context["direct_instructions"] += "\n\n" + suffix
        context["grain_evidence"] = None
        if grain_context["mode"] == "question_heuristic":
            # Explicit ablation control, never part of the business ResultContract.
            question = index[cid]["question"]
            context["grain_evidence"] = {
                "method": "question_heuristic_v1", "question_hash": digest(question),
                "business_confirmed": False,
                "expected_grain": "grouped" if _GRAIN_PATTERN.search(question.lower()) else "scalar_or_detail",
            }
            suffix = ("Experimental grain suggestion from question wording; not business-confirmed:\n"
                      + json.dumps(context["grain_evidence"], ensure_ascii=False, sort_keys=True))
            context["forge_instructions"] += "\n\n" + suffix
            context["direct_instructions"] += "\n\n" + suffix
        context["value_evidence"] = None
        field = value_config["field"]
        if (value_config["mode"] == "observed" and field is not None
                and field[0] == index[cid]["db_id"]
                and f"{field[1]}.{field[2]}" in context["context_snapshot"]["fields"]):
            from forge.benchmark_metadata import value_context_evidence, render_value_context
            # Visibility is a collection boundary, not merely a projection filter.
            if value_audit is None:
                value_audit = _value_audit(
                    str(hard.bird_runtime_root().resolve()), digest(dataset_fingerprints()),
                    digest(source_fingerprints()), tuple(field), value_config["max_values"])
            evidence = value_context_evidence(value_audit, index[cid]["db_id"], context["context_snapshot"]["fields"])
            context["value_evidence"] = evidence
            suffix = render_value_context(evidence)
            if suffix:
                context["forge_instructions"] += "\n\n" + suffix
                context["direct_instructions"] += "\n\n" + suffix
        contexts[cid] = context
    return contexts


def context_hashes(contexts: dict) -> dict:
    return {cid: {"snapshot": digest(ctx["context_snapshot"]),
                  "schema": digest(ctx["schema_context"]),
                  "forge": digest(ctx["forge_instructions"]),
                  "direct": digest(ctx["direct_instructions"]),
                  "date_evidence": digest(ctx["date_evidence"]),
                  "grain_evidence": digest(ctx["grain_evidence"]),
                  "value_evidence": digest(ctx["value_evidence"])} for cid, ctx in contexts.items()}


def freeze(*, cohort: str, provider: str, model: str, case_ids: list[str] | None = None,
           seed: int | None = None, size: int | None = None, variable: str | None = None,
           repeat: dict | None = None, gold_policy: str = "require_all",
           date_context: str = "off", date_max_rows: int = 100, grain_context: str = "off",
           value_context: str = "off", value_field: list[str] | None = None, value_max_values: int = 16,
           forge_prompt_revision: str = STRUCTURED_BENCHMARK_PROMPT_REVISION,
           _gold_readiness: dict | None = None) -> dict:
    if not provider.strip() or not model.strip() or variable not in (None, *FACTORS):
        raise ValueError("Nonempty provider/model and a supported single variable are required")
    if forge_prompt_revision not in STRUCTURED_BENCHMARK_PROMPT_REVISIONS:
        raise ValueError("Unknown Forge prompt revision")
    if forge_prompt_revision != STRUCTURED_BENCHMARK_PROMPT_REVISION and variable != "prompt":
        raise ValueError("Nondefault Forge prompts require the explicit prompt factor")
    if (date_context not in ("off", "observed") or isinstance(date_max_rows, bool)
            or not isinstance(date_max_rows, int) or not 1 <= date_max_rows <= 10_000):
        raise ValueError("Date context requires off/observed and an integer sample bound in 1..10000")
    date_config = {"mode": date_context, "max_rows": date_max_rows}
    if grain_context not in ("off", "question_heuristic") or (grain_context != "off" and variable != "grain_context"):
        raise ValueError("Grain hints require the explicit grain_context factor and question_heuristic/off mode")
    grain_config = {"mode": grain_context}
    if (value_context not in ("off", "observed") or isinstance(value_max_values, bool)
            or not isinstance(value_max_values, int) or not 1 <= value_max_values <= 100):
        raise ValueError("Value context requires off/observed and an integer max_values in 1..100")
    if value_field is not None and (not isinstance(value_field, list) or len(value_field) != 3
            or any(not isinstance(part, str) or not part.strip() for part in value_field)):
        raise ValueError("Value field requires exactly three nonempty strings: DB TABLE COLUMN")
    if ((value_context == "observed" and value_field is None)
            or ((value_context != "off" or value_field is not None) and variable != "value_context")):
        raise ValueError("Value observations and qualified controls require the explicit value_context factor and field")
    value_config = {"mode": value_context, "field": list(value_field) if value_field is not None else None,
                    "max_values": value_max_values}
    before = dataset_fingerprints()
    sources = source_fingerprints()
    suite = hard.load_suite(hard._FULL_SUITE_ID)
    if len(suite["cases"]) != 500:
        raise ValueError("Freezing requires the complete official 500-case input, including for D")
    root = hard.bird_runtime_root().resolve(strict=True)
    for db_id in {c["db_id"] for c in suite["cases"]}:
        if not isinstance(db_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", db_id):
            raise ValueError("Invalid official database identifier")
        for path in (hard._database_path(db_id), hard._description_dir(db_id)):
            if not path.resolve(strict=True).is_relative_to(root):
                raise ValueError("Official database/metadata path escaped the configured root")
    if repeat is not None:
        if cohort != "D" or case_ids is not None or repeat["model"] != {"provider": provider, "model": model}:
            raise ValueError("Repeated selection requires D, matching history model, and no explicit cases")
        ids, selection = _repeat_selection(suite, repeat, seed, size)
    else:
        ids, selection = select_cases(suite, cohort, case_ids, seed, size)
    if gold_policy not in ("require_all", "skip_unscorable"):
        raise ValueError("Unknown Gold readiness policy")
    if _gold_readiness is None:
        from forge.benchmark_service import check_gold_readiness
        blocked = check_gold_readiness(suite, ids) if gold_policy == "skip_unscorable" else []
        gold_readiness = {"policy": gold_policy, "blocked_cases": blocked}
    else:
        gold_readiness = copy.deepcopy(_gold_readiness)
        blocked = gold_readiness["blocked_cases"]
    index = {c["case_id"]: c for c in suite["cases"]}
    if (set(gold_readiness) != {"policy", "blocked_cases"} or gold_readiness["policy"] != gold_policy or
            not isinstance(blocked, list) or (gold_policy == "require_all" and blocked)):
        raise ValueError("Invalid frozen Gold readiness")
    blocked_ids = []
    for item in blocked:
        cid = item.get("case_id")
        if (set(item) != {"case_id", "db_id", "code"} or cid not in ids or
                item["db_id"] != index[cid]["db_id"] or not isinstance(item["code"], str) or not item["code"]):
            raise ValueError("Invalid blocked Gold case")
        blocked_ids.append(cid)
    if len(set(blocked_ids)) != len(blocked_ids):
        raise ValueError("Duplicate blocked Gold case")

    contexts = _contexts(suite, ids, date_config, grain_config, value_config, forge_prompt_revision)
    if before != dataset_fingerprints() or sources != source_fingerprints():
        raise ValueError("Dataset/source changed during freeze")
    manifest = {
        "schema_version": VERSION,
        "source": {"dataset": "BIRD-SQL Mini-Dev", "repository": "https://github.com/bird-bench/mini_dev",
                   "license": "CC BY-SA 4.0", "exposed": True, "gold_use": "scoring_only"},
        "cohort": cohort, "purpose": "development" if cohort == "D" else "regression_not_holdout",
        "case_ids": ids, "selection": selection, "denominator": len(ids), "official_denominator": 500,
        "dataset_files": before, "sources": sources,
        "model": {"provider": provider, "model": model}, "generation": generation(len(ids) - len(blocked_ids)),
        "gold_readiness": gold_readiness,
        "variables": {"allowed_changes": [variable] if variable else []},
        "date_context": date_config, "grain_context": grain_config, "value_context": value_config,
        "metric_revision": RESULT_COMPARATOR_REVISION,
        "official_ex_revision": hard._SCORING_STANDARD,
        "forge_prompt_revision": forge_prompt_revision,
        "forge_schema_revision": sources["schema"]["forge/schema.json"],
        "context_hashes": context_hashes(contexts),
        "runtime": runtime_versions(),
    }
    return {"manifest": manifest, "protocol_revision": digest(manifest), "case_ids": ids,
            "contexts": contexts, "generation": manifest["generation"], "gold_readiness": gold_readiness,
            "metric_revision": manifest["metric_revision"],
            "forge_prompt_revision": manifest["forge_prompt_revision"],
            "forge_schema_revision": manifest["forge_schema_revision"]}


def unwrap(value: dict) -> dict:
    manifest = value.get("manifest", value.get("protocol_manifest", value))
    if not isinstance(manifest, dict) or manifest.get("schema_version") != VERSION:
        raise ValueError("Unversioned/unsupported protocol: diagnostic only, not comparable")
    if "protocol_revision" in value and value["protocol_revision"] != digest(manifest):
        raise ValueError("Protocol content address mismatch")
    return manifest


def validate(value: dict, *, allow_declared_drift: bool = False) -> dict:
    manifest = unwrap(value)
    try:
        selection = manifest["selection"]
        cohort = manifest["cohort"]
        sampled = selection["method"] == "seeded_db_difficulty_round_robin_v1"
        repeated = selection["method"] == "failure_carryover_70_30_v1"
        response = freeze(cohort=cohort, provider=manifest["model"]["provider"], model=manifest["model"]["model"],
                          case_ids=None if cohort == "R" or sampled or repeated else manifest["case_ids"],
                          seed=selection.get("seed") if sampled or repeated else None,
                          size=selection.get("size") if sampled or repeated else None,
                          variable=next(iter(manifest["variables"]["allowed_changes"]), None),
                          repeat=selection.get("history") if repeated else None,
                          gold_policy=manifest["gold_readiness"]["policy"], _gold_readiness=manifest["gold_readiness"],
                          date_context=manifest["date_context"]["mode"], date_max_rows=manifest["date_context"]["max_rows"],
                          grain_context=manifest["grain_context"]["mode"],
                          value_context=manifest["value_context"]["mode"], value_field=manifest["value_context"]["field"],
                          value_max_values=manifest["value_context"]["max_values"],
                          forge_prompt_revision=manifest["forge_prompt_revision"])
    except (KeyError, TypeError, StopIteration, AttributeError) as exc:
        raise ValueError("Malformed protocol manifest") from exc
    # Parameter-only treatments never authorize source drift. Recompute all hashes
    # even in compare so a caller cannot substitute arbitrary prompt facts.
    source_drift = allow_declared_drift and manifest["variables"]["allowed_changes"] not in (
        ["date_context"], ["grain_context"], ["value_context"])
    actual = _comparison_manifest(manifest) if source_drift else manifest
    expected = _comparison_manifest(response["manifest"]) if source_drift else response["manifest"]
    if canonical(actual) != canonical(expected):
        raise ValueError("Protocol drift: dataset, sample, context, source, budget or undeclared fields differ")
    return response


def persist(manifest: dict) -> str:
    revision = digest(manifest)
    PROTOCOL_DIR.mkdir(parents=True, exist_ok=True)
    path = PROTOCOL_DIR / (revision.removeprefix("sha256:") + ".json")
    content = canonical(manifest)
    try:
        with path.open("xb") as stream:
            stream.write(content)
    except FileExistsError:
        if path.is_symlink() or path.read_bytes() != content:
            raise ValueError("Immutable protocol artifact conflict")
    return revision


def load_revision(revision: str) -> dict:
    if not isinstance(revision, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", revision):
        raise ValueError("Invalid protocol revision")
    path = PROTOCOL_DIR / (revision[7:] + ".json")
    if path.is_symlink():
        raise ValueError("Protocol artifact must not be a symlink")
    try:
        manifest = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("Frozen protocol artifact not found or invalid") from exc
    if digest(manifest) != revision:
        raise ValueError("Frozen protocol artifact was modified")
    return manifest


def verify_case(revision: str, case_id: str) -> dict:
    manifest = load_revision(revision)
    if case_id not in manifest["case_ids"]:
        raise ValueError("Case is outside the frozen denominator")
    # Stat-keyed hashing avoids repeated DB reads, but catches post-preflight drift.
    if manifest["dataset_files"] != dataset_fingerprints() or manifest["sources"] != source_fingerprints():
        raise ValueError("Frozen dataset/source drift")
    return manifest


def preflight(*, provider: str, model: str, case_ids: list[str], confirm_model_calls: int,
              protocol_manifest: dict | None = None) -> dict:
    response = validate(protocol_manifest) if protocol_manifest is not None else freeze(
        cohort="D", provider=provider, model=model, case_ids=case_ids)
    manifest = response["manifest"]
    if manifest["case_ids"] != case_ids or manifest["model"] != {"provider": provider, "model": model}:
        raise ValueError("Request does not match frozen cases/provider/model")
    if type(confirm_model_calls) is not int or confirm_model_calls != manifest["generation"]["max_model_calls"]:
        raise ValueError("Confirm exactly the frozen model-call budget after explicit Gold skips")
    from forge.benchmark_service import check_gold_readiness
    failures = check_gold_readiness(hard.load_suite(hard._FULL_SUITE_ID), case_ids)
    readiness = manifest["gold_readiness"]
    if readiness["policy"] == "require_all" and failures:
        raise ValueError("Gold readiness failed: " + json.dumps(failures, sort_keys=True))
    if canonical(failures) != canonical(readiness["blocked_cases"]):
        raise ValueError("Gold readiness changed; freeze a new report and confirm its budget")
    if manifest["dataset_files"] != dataset_fingerprints() or manifest["sources"] != source_fingerprints():
        raise ValueError("Frozen dataset/source changed during Gold preflight")
    persist(manifest)
    return response



def candidate_records(value: dict) -> list[dict]:
    if value.get("schema_version") == "bird-candidates-v1":
        records = value.get("candidates")
        if not isinstance(records, list):
            raise ValueError("Ledger candidates must be an array")
    elif isinstance(value.get("cases"), list):
        records = [{**(case.get(arm) or {}), "case_id": case["case_id"], "arm": arm,
                    "output": (case.get(arm) or {}).get("output")}
                   for case in value["cases"] for arm in ("forge", "direct")]
    else:
        raise ValueError("Expected bird-candidates-v1 ledger or Pi cases export")
    for record in records:
        if not isinstance(record, dict) or not {"case_id", "arm", "output"} <= record.keys():
            raise ValueError("Every candidate needs case_id, arm and output (null preserves missing output)")
        if record["arm"] not in ("forge", "direct"):
            raise ValueError("Candidate arm must be forge or direct")
    return records


def replay(value: dict, protocol: dict | None = None, *, diagnostic: bool = False) -> dict:
    from forge.benchmark_service import CandidateEvaluation, evaluate_candidate, _failed_evaluation
    records = candidate_records(value)
    versioned = isinstance(value.get("protocol_revision"), str)
    if not versioned and not diagnostic:
        raise ValueError("Old unversioned candidates require --diagnostic; not comparable")
    origin = value.get("protocol_manifest")
    if versioned and origin is None:
        if protocol is not None and digest(unwrap(protocol)) == value["protocol_revision"]:
            origin = unwrap(protocol)
        else:
            origin = load_revision(value["protocol_revision"])
    if versioned and digest(unwrap(origin)) != value["protocol_revision"]:
        raise ValueError("Candidate origin protocol content address mismatch")
    if protocol is None:
        protocol = origin
    if protocol is None:
        ids = list(dict.fromkeys(record["case_id"] for record in records))
        response = freeze(cohort="D", provider="offline", model="unversioned", case_ids=ids)
    else:
        response = validate(protocol)
    manifest, revision = response["manifest"], response["protocol_revision"]
    if versioned and canonical(_comparison_manifest(unwrap(origin))) != canonical(_comparison_manifest(manifest)):
        raise ValueError("Replay target differs outside the predeclared factor")
    expected = {(cid, arm) for cid in manifest["case_ids"] for arm in ("forge", "direct")}
    keys = [(r["case_id"], r["arm"]) for r in records]
    if len(keys) != len(set(keys)) or set(keys) != expected:
        raise ValueError("Candidate ledger must preserve the complete case × arm denominator exactly once")
    suite = hard.load_suite(hard._FULL_SUITE_ID)
    results = []
    blocked_ids = {item["case_id"] for item in manifest["gold_readiness"]["blocked_cases"]}
    for record in records:
        try:
            if manifest["dataset_files"] != dataset_fingerprints() or manifest["sources"] != source_fingerprints():
                raise ValueError("Inputs drifted during replay")
            req = CandidateEvaluation(case_id=record["case_id"], arm=record["arm"], output=record["output"],
                                  protocol_revision=revision, metric_revision=manifest["metric_revision"],
                                  context_snapshot=response["contexts"][record["case_id"]]["context_snapshot"])
            if record["output"] is None and record["case_id"] in blocked_ids:
                evaluation = _failed_evaluation(req, compile_status="pending" if req.arm == "forge" else "not_applicable",
                    execution_status="skipped", stage="gold", code="gold_execution_failed", retryable=False, scored=False)
            elif record["output"] is None and record.get("scored") is False:
                # No candidate and no recorded verdict: replay must not invent a failed attempt.
                evaluation = {
                    "arm": req.arm, "metric_revision": manifest["metric_revision"],
                    "compile_status": record.get("compile_status") or "pending",
                    "execution_status": record.get("execution_status") or "pending",
                    "scored": False, "official_ea": None, "contract_accuracy": None,
                    "failure": copy.deepcopy(record.get("failure")), "error_code": record.get("error_code"),
                    "sql": None, "forge_json": None, "assurance": None,
                }
            else:
                evaluation = evaluate_candidate(req, suite)
            if manifest["dataset_files"] != dataset_fingerprints() or manifest["sources"] != source_fingerprints():
                raise ValueError("Inputs drifted during candidate execution")
        except Exception as exc:
            evaluation = {"scored": False, "execution_status": "failed", "official_ea": None, "contract_accuracy": None,
                          "failure": {"stage": "replay", "code": type(exc).__name__, "message": str(exc)}}
        results.append({**record, "output_hash": digest(record["output"]), "evaluation": evaluation})
    return {"schema_version": "bird-replay-v1", "protocol_manifest": manifest, "protocol_revision": revision,
            "candidate_protocol_revision": value.get("protocol_revision"),
            "diagnostic": diagnostic or not versioned, "model_calls": 0, "denominator": manifest["denominator"],
            "model": copy.deepcopy(value.get("model")),
            "generation_contract": copy.deepcopy(value.get("generation_contract")),
            "candidates": results, "complete": all(r["evaluation"].get("scored") is True for r in results)}


def _comparison_manifest(manifest: dict) -> dict:
    result = copy.deepcopy(manifest)
    allowed = result["variables"]["allowed_changes"]
    if len(allowed) > 1 or any(f not in FACTORS for f in allowed):
        raise ValueError("Only one predeclared factor may change")
    for factor in allowed:
        if factor == "model":
            result.pop("model")
        elif factor not in ("date_context", "grain_context", "value_context"):
            result["sources"].pop(factor)
        if factor == "prompt":
            result.pop("forge_prompt_revision")
            for context in result["context_hashes"].values():
                context.pop("forge")
        elif factor == "schema":
            result.pop("forge_schema_revision")
        elif factor in ("date_context", "grain_context", "value_context"):
            result[factor].pop("mode")
            for context in result["context_hashes"].values():
                for key in ("forge", "direct", factor.replace("_context", "_evidence")):
                    context.pop(key)
    return result


def compare(left: dict, right: dict) -> dict:
    reasons = []
    try:
        manifests = [unwrap(item) for item in (left, right)]
        for item, manifest in zip((left, right), manifests):
            if item.get("schema_version") != "bird-replay-v1" or item.get("diagnostic") or not item.get("complete"):
                reasons.append("Unscored, diagnostic or old unversioned record")
            if item.get("protocol_revision") != digest(manifest):
                reasons.append("Protocol content address mismatch")
            keys = [(r["case_id"], r["arm"]) for r in item.get("candidates", [])]
            expected = {(cid, arm) for cid in manifest["case_ids"] for arm in ("forge", "direct")}
            if len(keys) != len(set(keys)) or set(keys) != expected:
                reasons.append("Incomplete denominator")
            validate(manifest, allow_declared_drift=True)
            if any(r.get("evaluation", {}).get("scored") is not True for r in item.get("candidates", [])):
                reasons.append("Unscored candidate evaluation")
            if any(r.get("output_hash") != digest(r["output"]) for r in item.get("candidates", [])):
                reasons.append("Candidate output hash mismatch")
            for record in item.get("candidates", []):
                evaluation = record.get("evaluation", {})
                if any(metric not in evaluation or evaluation[metric] is not None and type(evaluation[metric]) is not bool
                       for metric in ("official_ea", "contract_accuracy")):
                    reasons.append("Missing or invalid scored verdict")
        if canonical(_comparison_manifest(manifests[0])) != canonical(_comparison_manifest(manifests[1])):
            reasons.append("Fields outside the predeclared factor differ")
        allowed = manifests[0]["variables"]["allowed_changes"]
        if not allowed or allowed[0] in ("compiler", "schema"):
            outputs = [{(r["case_id"], r["arm"]): r["output_hash"] for r in item["candidates"]} for item in (left, right)]
            if outputs[0] != outputs[1]:
                reasons.append("Saved candidates changed during deterministic replay comparison")
        contracts = [copy.deepcopy(item.get("generation_contract")) for item in (left, right)]
        if "prompt" in allowed:
            for contract, manifest in zip(contracts, manifests):
                if isinstance(contract, dict):
                    if contract.get("forge_prompt_revision") != manifest["forge_prompt_revision"]:
                        reasons.append("Actual Forge prompt revision differs from frozen input")
                    contract.pop("forge_prompt_revision", None)
        if canonical(contracts[0]) != canonical(contracts[1]):
            reasons.append("Actual generation contract or Pi runtime/SDK revision changed")
        if "model" not in allowed and canonical(left.get("model")) != canonical(right.get("model")):
            reasons.append("Actual provider/model revision changed")
    except (ValueError, KeyError, TypeError) as exc:
        reasons.append(str(exc))
    if reasons:
        return {"comparable": False, "reasons": reasons}
    scores = []
    transitions = {}
    for item in (left, right):
        scores.append({arm: {metric: {
            "passed": sum(r["evaluation"][metric] is True for r in item["candidates"] if r["arm"] == arm),
            "failed": sum(r["evaluation"][metric] is False for r in item["candidates"] if r["arm"] == arm),
            "unknown": sum(r["evaluation"][metric] is None for r in item["candidates"] if r["arm"] == arm),
            "denominator": manifests[0]["denominator"],
        } for metric in ("official_ea", "contract_accuracy")} for arm in ("forge", "direct")})
    indexes = [{(r["case_id"], r["arm"]): r["evaluation"] for r in item["candidates"]} for item in (left, right)]
    for arm in ("forge", "direct"):
        transitions[arm] = {}
        for metric in ("official_ea", "contract_accuracy"):
            pairs = [(cid, indexes[0][cid, arm][metric], indexes[1][cid, arm][metric]) for cid in manifests[0]["case_ids"]]
            transitions[arm][metric] = {
                "newly_passed": [cid for cid, old, new in pairs if old is False and new is True],
                "regressed": [cid for cid, old, new in pairs if old is True and new is False],
                "baseline_unknown": [cid for cid, old, _ in pairs if old is None],
                "candidate_unknown": [cid for cid, _, new in pairs if new is None],
                "resolved_unknown": [cid for cid, old, new in pairs if old is None and new is not None],
                "became_unknown": [cid for cid, old, new in pairs if old is not None and new is None],
            }
    provenance_known = all(
        isinstance(item.get("model"), dict) and bool(item["model"].get("revision"))
        and isinstance(item.get("generation_contract"), dict)
        and all(item["generation_contract"].get(key) for key in ("pi_runtime_revision", "pi_sdk_lock_revision"))
        for item in (left, right)
    )
    return {"comparable": True, "denominator": manifests[0]["denominator"], "scores": scores,
            "transitions": transitions, "generation_provenance_known": provenance_known,
            "comparison_scope": "saved_candidates_offline", "generative_gain_claim": False,
            "interpretation": "Saved-candidate offline comparison only; unknown generation provenance is diagnostic, not generative evidence"}
