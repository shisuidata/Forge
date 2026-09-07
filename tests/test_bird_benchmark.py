"""Offline protocol invariants: no provider and no task-state store."""
from __future__ import annotations

import copy
import json
import sqlite3

import pytest

from forge import bird_benchmark as bird
from test_benchmark_metric_revision_api import benchmark_api  # noqa: F401
from test_benchmark_metadata import date_dataset  # noqa: F401


def frozen():
    return bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=["one"], variable="model")


def ledger(response):
    return {"schema_version": "bird-candidates-v1", "protocol_revision": response["protocol_revision"],
            "protocol_manifest": response["manifest"], "candidates": [
                {"case_id": "one", "arm": "direct", "output": "SELECT id FROM orders"},
                {"case_id": "one", "arm": "forge", "output": {"scan": "orders", "select": ["id"]}},
            ]}


def test_cohort_selection_preserves_full_denominator_and_seeded_strata():
    suite = {"cases": [{"case_id": f"md-{i:03d}", "db_id": f"db-{i % 4}", "difficulty": str(i % 3)} for i in range(500)]}
    ids, selection = bird.select_cases(suite, "R", None, None, None)
    assert ids == [c["case_id"] for c in suite["cases"]]
    with pytest.raises(ValueError):
        bird.select_cases(suite, "R", ids[:20], None, None)
    with pytest.raises(ValueError):
        bird.select_cases(suite, "H", None, None, None)
    one = bird.select_cases(suite, "D", None, 42, 12)[0]
    assert one == bird.select_cases(suite, "D", None, 42, 12)[0]
    index = {c["case_id"]: c for c in suite["cases"]}
    assert len({(index[c]["db_id"], index[c]["difficulty"]) for c in one}) == 12


def test_manifest_gold_free_and_metadata_change_invalidates(benchmark_api):
    response = frozen()
    assert "SELECT id FROM orders" not in bird.canonical(response).decode()
    bird.persist(response["manifest"])
    assert bird.verify_case(response["protocol_revision"], "one")["case_ids"] == ["one"]
    metadata = benchmark_api["database"].parent / "dev_tables.json"
    metadata.write_text(metadata.read_text() + " ")
    with pytest.raises(ValueError):
        bird.validate(response)
    with pytest.raises(ValueError):
        bird.verify_case(response["protocol_revision"], "one")


def test_replay_real_chain_preserves_empty_gold_and_readonly_database(benchmark_api):
    with sqlite3.connect(benchmark_api["database"]) as db:
        db.execute("DELETE FROM orders")
    benchmark_api["standard"]["benchmark.sqlite"] = bird.file_hash(benchmark_api["database"])
    response = frozen()
    before = bird.file_hash(benchmark_api["database"])
    result = bird.replay(ledger(response))
    assert result["complete"] is True
    assert result["model_calls"] == 0
    assert all(r["evaluation"]["official_ea"] is True for r in result["candidates"])
    assert all(r["evaluation"]["result"]["row_count"] == 0 for r in result["candidates"])
    assert bird.file_hash(benchmark_api["database"]) == before


def test_replay_retains_failure_and_rejects_missing_or_duplicate_candidates(benchmark_api):
    response = frozen()
    candidates = ledger(response)
    candidates["candidates"][1]["output"] = {"scan": "orders", "select": [{"unsupported": True}]}
    result = bird.replay(candidates)
    assert result["denominator"] == 1
    assert result["candidates"][1]["evaluation"]["failure"]["code"] == "compile_failed"
    assert result["complete"] is True
    assert bird.compare(result, result)["comparable"] is True
    candidates["candidates"].pop()
    with pytest.raises(ValueError):
        bird.replay(candidates)
    candidates["candidates"].append(candidates["candidates"][0])
    with pytest.raises(ValueError):
        bird.replay(candidates)


def test_declared_cross_version_replay_and_undeclared_drift(benchmark_api):
    baseline = frozen()
    next_protocol = bird.freeze(cohort="D", provider="offline", model="other", case_ids=["one"], variable="model")
    old = bird.replay(ledger(baseline))
    new = bird.replay(ledger(baseline), next_protocol)
    assert new["candidate_protocol_revision"] == baseline["protocol_revision"]
    assert new["protocol_revision"] == next_protocol["protocol_revision"]
    assert bird.compare(old, new)["comparable"] is True
    drifted = copy.deepcopy(new)
    drifted["protocol_manifest"]["generation"]["provider_retries"] = 1
    drifted["protocol_revision"] = bird.digest(drifted["protocol_manifest"])
    assert bird.compare(old, drifted)["comparable"] is False
    with pytest.raises(ValueError):
        bird.replay(ledger(baseline), drifted["protocol_manifest"])


def test_unversioned_pi_export_is_diagnostic_only(benchmark_api):
    old = {"cases": [{"case_id": "one", "forge": {"output": {"scan": "orders", "select": ["id"]}},
                       "direct": {"output": "SELECT id FROM orders"}}]}
    with pytest.raises(ValueError):
        bird.replay(old)
    result = bird.replay(old, diagnostic=True)
    assert result["complete"] is True
    assert result["diagnostic"] is True
    assert bird.compare(result, result)["comparable"] is False


def test_revision_traversal_and_symlink_escape_rejected(benchmark_api, tmp_path):
    with pytest.raises(ValueError):
        bird.load_revision("../../private-file")
    outside = tmp_path / "private"
    outside.write_text("not benchmark data")
    (benchmark_api["database"].parent / "escape.json").symlink_to(outside)
    with pytest.raises(ValueError):
        frozen()


def test_compiler_fix_compares_scored_failure_without_changing_candidates(benchmark_api, monkeypatch):
    from forge import benchmark_service as routes
    response = bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=["one"], variable="compiler")
    candidates = ledger(response)
    real_compile = routes.compile_query
    def old_compiler(_):
        raise ValueError("Previously unsupported valid candidate")
    monkeypatch.setattr(routes, "compile_query", old_compiler)
    baseline = bird.replay(candidates)
    monkeypatch.setattr(routes, "compile_query", real_compile)
    corrected = bird.replay(candidates)
    compared = bird.compare(baseline, corrected)
    assert compared["comparable"] is True
    assert compared["transitions"]["forge"]["official_ea"]["newly_passed"] == ["one"]
    assert bird.compare(corrected, baseline)["transitions"]["forge"]["official_ea"]["regressed"] == ["one"]
    assert compared["scores"][0]["forge"]["official_ea"] == {"passed": 0, "failed": 1, "unknown": 0, "denominator": 1}
    assert compared["generation_provenance_known"] is False
    assert compared["generative_gain_claim"] is False
    altered = ledger(response)
    altered["candidates"][1]["output"]["limit"] = 1
    assert bird.compare(corrected, bird.replay(altered))["comparable"] is False


def test_generation_failure_is_scored_but_gold_failure_is_not(benchmark_api):
    response = frozen()
    candidates = ledger(response)
    candidates["candidates"][1]["output"] = None
    failure = bird.replay(candidates)
    assert failure["complete"] is True
    assert bird.compare(failure, failure)["scores"][0]["forge"]["official_ea"]["failed"] == 1
    benchmark_api["suite"]["cases"][0]["SQL"] = "SELECT missing FROM orders"
    unscored = bird.replay(ledger(response))
    assert unscored["complete"] is False
    assert bird.compare(unscored, unscored)["comparable"] is False

def test_replay_preserves_unstarted_pi_arm_without_hiding_generation_failure(benchmark_api):
    response = frozen()
    pending = {"output": None, "scored": False, "official_ea": None, "contract_accuracy": None,
               "compile_status": "pending", "execution_status": "pending", "failure": None,
               "error_code": None, "evidence": {"dispatches": 0}}
    failed = {**pending, "scored": True, "official_ea": False, "contract_accuracy": False,
              "execution_status": "skipped", "evidence": {"dispatches": 1}}
    exported = {"protocol_revision": response["protocol_revision"], "protocol_manifest": response["manifest"],
                "cases": [{"case_id": "one", "forge": pending, "direct": failed}]}
    result = bird.replay(exported)
    unstarted, dispatched = [record["evaluation"] for record in result["candidates"]]
    assert unstarted["scored"] is False
    assert unstarted["official_ea"] is None
    assert unstarted["contract_accuracy"] is None
    assert unstarted["execution_status"] == "pending"
    assert unstarted["failure"] is None
    assert dispatched["scored"] is True
    assert dispatched["official_ea"] is False
    assert dispatched["contract_accuracy"] is False
    assert dispatched["execution_status"] == "skipped"
    assert result["denominator"] == 1
    assert result["complete"] is False
    assert bird.compare(result, result)["comparable"] is False



def test_unknown_contract_is_separate_from_wrong_and_improvement(benchmark_api):
    benchmark_api["suite"]["cases"][0]["SQL"] = "SELECT 1, 1 UNION ALL SELECT 2, 2"
    response = frozen()
    candidates = ledger(response)
    candidates["candidates"][0]["output"] = "SELECT 1, 2 UNION ALL SELECT 2, 1"
    unknown = bird.replay(candidates)
    candidates["candidates"][0]["output"] = "SELECT 1, 1 UNION ALL SELECT 2, 2"
    resolved = bird.replay(candidates)
    comparison = bird.compare(unknown, resolved)
    assert comparison["comparable"] is True
    assert comparison["scores"][0]["direct"]["contract_accuracy"]["unknown"] == 1
    transition = comparison["transitions"]["direct"]["contract_accuracy"]
    assert transition["newly_passed"] == []
    assert transition["baseline_unknown"] == ["one"]
    assert transition["resolved_unknown"] == ["one"]


def test_pi_generation_provenance_is_preserved_and_bound(benchmark_api):
    response = bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=["one"], variable="compiler")
    candidates = ledger(response)
    candidates["model"] = {"provider": "offline", "model": "fixture", "revision": "model-rev-1"}
    candidates["generation_contract"] = {**response["generation"], "pi_runtime_revision": "runtime-1", "pi_sdk_lock_revision": "sdk-1"}
    baseline = bird.replay(candidates)
    assert baseline["model"] == candidates["model"]
    assert baseline["generation_contract"] == candidates["generation_contract"]
    assert bird.compare(baseline, baseline)["generation_provenance_known"] is True
    candidates["generation_contract"]["pi_sdk_lock_revision"] = "sdk-2"
    assert bird.compare(baseline, bird.replay(candidates))["comparable"] is False
    candidates["generation_contract"]["pi_sdk_lock_revision"] = "sdk-1"
    candidates["model"]["revision"] = "model-rev-2"
    assert bird.compare(baseline, bird.replay(candidates))["comparable"] is False


def test_volatile_shm_and_empty_wal_do_not_invalidate_snapshot(benchmark_api):
    database = benchmark_api["database"]
    shm = database.with_name(database.name + "-shm")
    wal = database.with_name(database.name + "-wal")
    shm.write_bytes(b"volatile lock state")
    wal.write_bytes(b"")
    response = frozen()
    bird.persist(response["manifest"])
    shm.write_bytes(b"different reader lock state")
    assert bird.verify_case(response["protocol_revision"], "one")["case_ids"] == ["one"]
    assert bird.validate(response)["protocol_revision"] == response["protocol_revision"]
    assert shm.read_bytes() == b"different reader lock state"
    assert wal.exists()


@pytest.mark.parametrize("suffix", ["-wal", "-journal"])
def test_nonempty_sqlite_sidecars_fail_closed_without_deleting(benchmark_api, suffix):
    database = benchmark_api["database"]
    sidecar = database.with_name(database.name + suffix)
    sidecar.write_bytes(b"uncheckpointed data")
    with pytest.raises(ValueError, match="WAL/journal"):
        frozen()
    assert sidecar.read_bytes() == b"uncheckpointed data"


def test_changed_gold_cannot_be_refrozen_as_official(benchmark_api):
    cases = benchmark_api["database"].parent / "mini_dev_sqlite.json"
    cases.write_text(cases.read_text().replace("SELECT id FROM orders", "SELECT 9 FROM orders"))
    with pytest.raises(ValueError, match="official public snapshot"):
        frozen()


def repeat_fixture():
    model = {"provider": "offline", "model": "fixture"}
    def outcome(correct=True, *, dispatched=True):
        return {"official_ea": correct, "contract_accuracy": correct, "scored": correct is not None,
                "output": "SELECT 1" if dispatched else None, "evidence": {"dispatches": int(dispatched)}}
    previous = {"run_id": "previous", "status": "failed", "model": model, "cases": [
        {"case_id": str(i), "forge": outcome(False if i == 0 else True),
         "direct": outcome(None if i == 1 else True)} for i in range(6)]}
    older = {"schema_version": "bird-candidates-v1", "model": model,
             "candidates": [{"case_id": "6", "arm": "forge", "output": "SELECT 1"}]}
    suite = {"cases": [{"case_id": str(i)} for i in range(40)]}
    return suite, previous, older


def test_repeat_preserves_both_arm_failures_and_disjoint_exposure_groups():
    suite, previous, older = repeat_fixture()
    history = bird.repeat_history(previous, [older], provider="offline", model="fixture")
    ids, selection = bird._repeat_selection(suite, history, 43, 12)
    assert selection["carried_case_ids"] == ["0", "1"]
    assert len(selection["new_case_ids"]) == 7
    assert len(selection["reviewed_case_ids"]) == 3
    assert set(selection["new_case_ids"]).isdisjoint(history["seen_case_ids"])
    assert set(selection["reviewed_case_ids"]) <= set(history["seen_case_ids"]) - {"0", "1"}
    assert len(set(ids)) == 12
    assert bird._repeat_selection(suite, history, 43, 12) == (ids, selection)
    previous["cases"][2]["forge"]["contract_accuracy"] = False
    assert "2" in bird.repeat_history(previous, [older], provider="offline", model="fixture")["previous"]["failed_case_ids"]


def test_repeat_exposure_requires_generation_and_matching_model():
    suite, previous, older = repeat_fixture()
    previous["cases"][2]["forge"].update(output=None, evidence={"dispatches": 0})
    previous["cases"][2]["direct"].update(output=None, evidence={"dispatches": 0})
    previous["cases"][1]["direct"].update(output=None, evidence={"dispatches": 1})
    history = bird.repeat_history(previous, [older], provider="offline", model="fixture")
    assert "2" not in history["seen_case_ids"]
    assert "1" in history["seen_case_ids"]  # dispatched timeout is still exposed
    older["model"] = {"provider": "other", "model": "fixture"}
    with pytest.raises(ValueError, match="provider/model"):
        bird.repeat_history(previous, [older], provider="offline", model="fixture")


def test_repeat_exhaustion_never_drops_failures_or_substitutes_pools():
    suite, previous, older = repeat_fixture()
    history = bird.repeat_history(previous, [older], provider="offline", model="fixture")
    with pytest.raises(ValueError, match="failures exceed"):
        bird._repeat_selection(suite, history, 43, 1)
    with pytest.raises(ValueError, match="Insufficient exposure pools"):
        bird._repeat_selection({"cases": suite["cases"][:8]}, history, 43, 8)
    with pytest.raises(ValueError, match="Insufficient exposure pools"):
        bird._repeat_selection(suite, history, 43, 30)


def test_repeat_manifest_rejects_relabelled_exposure_groups(benchmark_api):
    previous = {"status": "completed", "model": {"provider": "offline", "model": "fixture"}, "cases": [
        {"case_id": "one", "forge": {"official_ea": False, "contract_accuracy": False, "output": "bad"},
         "direct": {"official_ea": True, "contract_accuracy": True, "output": "SELECT id FROM orders"}}]}
    history = bird.repeat_history(previous, [], provider="offline", model="fixture")
    response = bird.freeze(cohort="D", provider="offline", model="fixture", seed=43, size=1, repeat=history)
    assert bird.validate(response)["case_ids"] == ["one"]
    altered = copy.deepcopy(response["manifest"])
    altered["selection"]["new_case_ids"] = ["one"]
    with pytest.raises(ValueError, match="Protocol drift"):
        bird.validate(altered)




@pytest.mark.asyncio
async def test_repeat_protocol_accepts_json_integer_number_roundtrip(client, benchmark_api):
    previous = {"status": "failed", "model": {"provider": "offline", "model": "fixture"}, "cases": [
        {"case_id": "one", "forge": {"official_ea": False, "contract_accuracy": False, "output": "bad"},
         "direct": {"official_ea": True, "contract_accuracy": True, "output": "SELECT id FROM orders"}}]}
    history = bird.repeat_history(previous, [], provider="offline", model="fixture")
    response = bird.freeze(cohort="D", provider="offline", model="fixture", seed=48, size=2, repeat=history)

    def json_number(value):
        number = float(value)
        return int(number) if number.is_integer() else number

    # JSON clients such as JavaScript re-encode 1.0 as 1 without changing its value.
    forwarded = json.loads(json.dumps(response["manifest"]), parse_float=json_number)
    checked = await client.post("/api/internal/benchmark-v2/protocol", headers=benchmark_api["headers"], json={
        "provider": "offline", "model": "fixture", "case_ids": response["case_ids"],
        "confirm_model_calls": 4, "protocol_manifest": forwarded,
    })
    assert checked.status_code == 200, checked.text
    assert checked.json()["protocol_revision"] == response["protocol_revision"]


def test_explicit_gold_skip_replay_remains_unknown_not_generation_failure(benchmark_api):
    benchmark_api["suite"]["cases"][0]["SQL"] = "SELECT missing FROM orders"
    response = bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=["one"],
                           gold_policy="skip_unscorable")
    candidates = ledger(response)
    for candidate in candidates["candidates"]:
        candidate["output"] = None
    replay = bird.replay(candidates)
    assert replay["denominator"] == 1
    assert response["generation"]["max_model_calls"] == 0
    for record in replay["candidates"]:
        assert record["evaluation"]["scored"] is False
        assert record["evaluation"]["official_ea"] is None
        assert record["evaluation"]["contract_accuracy"] is None
        assert record["evaluation"]["failure"]["stage"] == "gold"
    assert bird.compare(replay, replay)["comparable"] is False


@pytest.fixture
def date_protocol(date_dataset, monkeypatch, tmp_path):
    from config import cfg
    root = date_dataset
    cases = [{"question_id": i, "db_id": "sample", "difficulty": "simple",
              "question": "List loan dates", "evidence": "records.loan_date",
              "SQL": "SELECT loan_date FROM records"} for i in range(500)]
    (root / "mini_dev_sqlite.json").write_text(json.dumps(cases))
    monkeypatch.setattr(bird.hard, "_BIRD_RUNTIME", root)
    monkeypatch.setattr(bird, "PROTOCOL_DIR", tmp_path / "protocols")
    monkeypatch.setattr(cfg, "PI_SERVICE_API_KEYS", ["test-pi-key"])
    paths = [root / "mini_dev_sqlite.json", root / "dev_tables.json", *root.rglob("*.sqlite"), *root.rglob("*.csv")]
    standard = bird._fingerprints(root, paths)
    monkeypatch.setattr(bird, "standard_files", lambda: standard)
    return {"root": root, "paths": paths, "standard": standard}


def date_frozen(mode, max_rows=2):
    return bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=["md-000"],
                       variable="date_context", date_context=mode, date_max_rows=max_rows)


def date_ledger(response):
    return {"schema_version": "bird-candidates-v1", "protocol_revision": response["protocol_revision"],
            "protocol_manifest": response["manifest"], "candidates": [
                {"case_id": "md-000", "arm": "direct", "output": "SELECT loan_date FROM records"},
                {"case_id": "md-000", "arm": "forge", "output": {"scan": "records", "select": ["loan_date"]}},
            ]}


@pytest.mark.asyncio
async def test_date_treatment_shared_facts_preserve_context_and_execution(client, date_protocol):
    baseline, treatment = date_frozen("off"), date_frozen("observed")
    original, enhanced = baseline["contexts"]["md-000"], treatment["contexts"]["md-000"]
    assert original["date_evidence"] is None
    assert original["context_snapshot"] == enhanced["context_snapshot"]
    assert original["schema_context"] == enhanced["schema_context"]
    suffixes = [enhanced[key].removeprefix(original[key]) for key in ("forge_instructions", "direct_instructions")]
    assert suffixes[0] == suffixes[1]
    loan = next(field for field in enhanced["date_evidence"]["fields"] if field["column"] == "loan_date")
    assert loan["observed_layouts"] == {"YYYY-MM-DD": 2}
    assert loan["declared_layouts"] == []
    payload = {"provider": "offline", "model": "fixture", "case_ids": ["md-000"],
               "confirm_model_calls": 2, "protocol_manifest": treatment["manifest"]}
    headers = {"X-Pi-Service-Key": "test-pi-key"}
    ready = await client.post("/api/internal/benchmark-v2/protocol", json=payload, headers=headers)
    assert ready.status_code == 200
    context = await client.post("/api/internal/benchmark-v2/context", headers=headers, json={
        "case_id": "md-000", "protocol_revision": treatment["protocol_revision"],
    })
    assert context.status_code == 200
    assert context.json()["date_evidence"] == enhanced["date_evidence"]
    for key in ("forge_instructions", "direct_instructions"):
        assert context.json()[key] == enhanced[key]
    left = bird.replay(date_ledger(baseline))
    right = bird.replay(date_ledger(treatment))
    assert bird.compare(left, right)["comparable"] is True
    assert all(record["evaluation"]["contract_accuracy"] is True for record in right["candidates"])


def test_date_factor_cannot_authorize_forged_generation_context(date_protocol):
    frozen = date_frozen("observed")
    manifest = copy.deepcopy(frozen["manifest"])
    manifest["context_hashes"]["md-000"]["forge"] = bird.digest("forged evidence")
    with pytest.raises(ValueError):
        bird.validate(manifest, allow_declared_drift=True)
    replay = bird.replay(date_ledger(frozen))
    forged = copy.deepcopy(replay)
    forged["protocol_manifest"] = manifest
    forged["protocol_revision"] = bird.digest(manifest)
    assert bird.compare(replay, forged)["comparable"] is False


def test_date_sample_limit_and_evaluator_remain_frozen(date_protocol):
    first = bird.replay(date_ledger(date_frozen("off", 1)))
    wider = bird.replay(date_ledger(date_frozen("observed", 2)))
    assert bird.compare(first, wider)["comparable"] is False
    manifest = copy.deepcopy(wider["protocol_manifest"])
    manifest["date_context"]["path"] = "/not-an-authorized-dataset"
    with pytest.raises(ValueError):
        bird.validate(manifest, allow_declared_drift=True)
    manifest = copy.deepcopy(wider["protocol_manifest"])
    manifest["sources"]["evaluator"]["forge/benchmark_service.py"] = bird.digest("changed scoring")
    with pytest.raises(ValueError):
        bird.validate(manifest, allow_declared_drift=True)


def test_date_cache_cannot_reuse_observations_after_snapshot_change(date_protocol):
    first = date_frozen("observed", 3)
    database = date_protocol["root"] / "dev_databases/sample/sample.sqlite"
    with sqlite3.connect(database) as db:
        db.execute("INSERT INTO records(loan_date) VALUES ('930103')")
    with pytest.raises(ValueError):
        bird.validate(first)
    # Authorize a new synthetic snapshot, not a change to the real official data.
    date_protocol["standard"].update(bird._fingerprints(date_protocol["root"], date_protocol["paths"]))
    second = date_frozen("observed", 3)
    loan = next(field for field in second["contexts"]["md-000"]["date_evidence"]["fields"] if field["column"] == "loan_date")
    assert loan["observed_layouts"] == {"YYYY-MM-DD": 2, "YYMMDD": 1}
    assert second["protocol_revision"] != first["protocol_revision"]

@pytest.mark.asyncio
async def test_grain_ablation_keeps_business_context_and_scoring_unchanged(client, benchmark_api):
    control = bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=["one"],
                          variable="grain_context", grain_context="question_heuristic")
    treatment = bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=["one"],
                            variable="grain_context", grain_context="off")
    original, omitted = control["contexts"]["one"], treatment["contexts"]["one"]
    assert original["context_snapshot"] == omitted["context_snapshot"]
    assert "expected_grain" not in original["context_snapshot"]["result_contract"]
    assert original["grain_evidence"]["business_confirmed"] is False
    suffixes = [original[key].removeprefix(omitted[key]) for key in ("forge_instructions", "direct_instructions")]
    assert suffixes[0] == suffixes[1]
    headers = {"X-Pi-Service-Key": "test-pi-key"}
    response = await client.post("/api/internal/benchmark-v2/protocol", headers=headers, json={
        "provider": "offline", "model": "fixture", "case_ids": ["one"], "confirm_model_calls": 2,
        "protocol_manifest": control["manifest"],
    })
    assert response.status_code == 200
    context = await client.post("/api/internal/benchmark-v2/context", headers=headers, json={
        "case_id": "one", "protocol_revision": control["protocol_revision"],
    })
    assert context.status_code == 200
    evaluated = await client.post("/api/internal/benchmark-v2/evaluate", headers=headers, json={
        "case_id": "one", "arm": "direct", "output": "SELECT id FROM orders",
        "context_snapshot": context.json()["context_snapshot"], "metric_revision": control["metric_revision"],
        "protocol_revision": control["protocol_revision"],
    })
    assert evaluated.status_code == 200
    assert evaluated.json()["official_ea"] is True
    assert evaluated.json()["contract_accuracy"] is True
    left, right = bird.replay(ledger(control)), bird.replay(ledger(treatment))
    assert bird.compare(left, right)["comparable"] is True
    assert all(record["evaluation"]["contract_accuracy"] is True for record in left["candidates"] + right["candidates"])


def test_grain_factor_cannot_authorize_forged_facts_or_scoring_drift(benchmark_api):
    with pytest.raises(ValueError):
        bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=["one"],
                    grain_context="question_heuristic")
    control = bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=["one"],
                          variable="grain_context", grain_context="question_heuristic")
    forged = copy.deepcopy(control["manifest"])
    forged["context_hashes"]["one"]["grain_evidence"] = bird.digest("pretend business confirmation")
    with pytest.raises(ValueError):
        bird.validate(forged, allow_declared_drift=True)
    drifted = copy.deepcopy(control["manifest"])
    drifted["sources"]["context"]["forge/benchmark_v2.py"] = bird.digest("changed contract or comparison")
    with pytest.raises(ValueError):
        bird.validate(drifted, allow_declared_drift=True)
    changed_date = bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=["one"],
                              variable="grain_context", date_max_rows=101)
    assert bird.compare(bird.replay(ledger(control)), bird.replay(ledger(changed_date)))["comparable"] is False



@pytest.fixture
def value_protocol(date_protocol):
    root = date_protocol["root"]
    metadata = root / "dev_tables.json"
    tables = json.loads(metadata.read_text())
    tables[0]["column_types"] = ["text"] * len(tables[0]["column_types"])
    tables[0]["table_names_original"].append("secrets")
    tables[0]["column_names_original"].append([1, "category"])
    tables[0]["column_types"].append("text")
    metadata.write_text(json.dumps(tables))
    with sqlite3.connect(root / "dev_databases/sample/sample.sqlite") as db:
        db.execute("CREATE TABLE secrets(category TEXT)")
        db.execute("INSERT INTO secrets VALUES ('not visible')")
        db.execute("UPDATE records SET loan_date = 'A' WHERE rowid = 1")
        db.execute("UPDATE records SET loan_date = 'a ' WHERE rowid = 2")
    date_protocol["standard"].update(bird._fingerprints(root, date_protocol["paths"]))
    clear_cache = bird._value_audit.cache_clear
    clear_cache()
    yield date_protocol
    clear_cache()


def value_frozen(mode="observed", **kwargs):
    return bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=["md-000"],
                       **{"variable": "value_context", "value_context": mode,
                          "value_field": ["sample", "records", "loan_date"], **kwargs})


@pytest.mark.parametrize("options", [
    {"value_context": "observed"},
    {"value_field": ["sample", "records", "loan_date"]},
    {"variable": "model", "value_context": "observed", "value_field": ["sample", "records", "loan_date"]},
    {"variable": "value_context", "value_context": "automatic"},
    *[{"variable": "value_context", "value_field": field} for field in
      ("sample.records.loan_date", ["sample", "records"], ["sample", "records", "loan_date", "extra"],
       ["sample", "records", " "], ["sample", "records", 1])],
    *[{"value_max_values": cap} for cap in (0, 101, True, 1.5)],
])
def test_value_invalid_scope_or_bound_rejected_before_dataset_access(monkeypatch, options):
    monkeypatch.setattr(bird, "dataset_fingerprints", lambda: pytest.fail("Invalid configuration reached dataset"))
    with pytest.raises(ValueError):
        bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=["md-000"], **options)


def test_value_off_and_invisible_fields_never_collect(value_protocol, monkeypatch):
    from forge.benchmark_service import build_context_response
    suite = bird.hard.load_suite(bird.hard._FULL_SUITE_ID)
    original = build_context_response(suite, suite["cases"][0])
    assert "secrets.category" not in original["context_snapshot"]["fields"]
    monkeypatch.setattr(bird, "_value_audit", lambda *args: pytest.fail("Invisible/off value collection"))
    responses = [bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=["md-000"]),
                 value_frozen("off")]
    responses.extend(value_frozen(value_field=field) for field in (
        ["other", "records", "loan_date"], ["sample", "secrets", "category"],
        ["sample", "records", "LOAN_DATE"], ["sample", "missing", "loan_date"],
    ))
    for response in responses:
        context = response["contexts"]["md-000"]
        assert context["value_evidence"] is None
        for key in ("forge_instructions", "direct_instructions", "context_snapshot", "schema_context"):
            assert context[key] == original[key]


@pytest.mark.asyncio
async def test_value_treatment_symmetric_exact_facts_without_scoring_repair(client, value_protocol):
    baseline, treatment = value_frozen("off"), value_frozen()
    original, enhanced = baseline["contexts"]["md-000"], treatment["contexts"]["md-000"]
    assert original["context_snapshot"] == enhanced["context_snapshot"]
    assert original["schema_context"] == enhanced["schema_context"]
    suffixes = [enhanced[key].removeprefix(original[key]) for key in ("forge_instructions", "direct_instructions")]
    assert suffixes[0] == suffixes[1]
    facts = json.loads(suffixes[0].strip().split("\n", 1)[1])
    assert facts["values"] == ["A", "a "]
    assert facts["coverage"]["business_authoritative"] is False
    assert facts["coverage"]["scan_complete"] is True
    bird.persist(treatment["manifest"])
    context = await client.post("/api/internal/benchmark-v2/context", headers={"X-Pi-Service-Key": "test-pi-key"},
                                json={"case_id": "md-000", "protocol_revision": treatment["protocol_revision"]})
    assert context.status_code == 200
    for key in ("forge_instructions", "direct_instructions"):
        assert context.json()[key] == enhanced[key]
    left, right = bird.replay(date_ledger(baseline)), bird.replay(date_ledger(treatment))
    assert bird.compare(left, right)["comparable"] is True
    candidates = date_ledger(treatment)
    candidates["candidates"][0]["output"] = "SELECT loan_date FROM records WHERE loan_date = 'a'"
    result = bird.replay(candidates)["candidates"][0]
    assert result["evaluation"]["official_ea"] is False
    assert result["evaluation"]["contract_accuracy"] is False


@pytest.mark.parametrize("key", ["forge", "direct", "value_evidence", "snapshot", "schema"])
def test_value_factor_rejects_context_tampering_even_with_declared_drift(value_protocol, key):
    response = value_frozen()
    manifest = copy.deepcopy(response["manifest"])
    manifest["context_hashes"]["md-000"][key] = bird.digest("forged context")
    with pytest.raises(ValueError):
        bird.validate(manifest, allow_declared_drift=True)


def test_value_factor_binds_field_cap_sources_and_other_settings(value_protocol, monkeypatch):
    baseline = bird.replay(date_ledger(value_frozen("off")))
    for options in ({"value_max_values": 1}, {"value_max_values": 100},
                    {"value_field": ["sample", "records", "account_date"]}, {"date_max_rows": 101}):
        changed = bird.replay(date_ledger(value_frozen(**options)))
        assert bird.compare(baseline, changed)["comparable"] is False
    source = bird.source_fingerprints()
    source["prompt"]["agent/prompts.py"] = bird.digest("changed source")
    monkeypatch.setattr(bird, "source_fingerprints", lambda: source)
    with pytest.raises(ValueError):
        bird.validate(baseline["protocol_manifest"], allow_declared_drift=True)
    changed = bird.replay(date_ledger(value_frozen()))
    assert bird.compare(baseline, changed)["comparable"] is False


def test_value_cache_isolation_and_snapshot_invalidation(value_protocol):
    first = value_frozen()
    first["contexts"]["md-000"]["value_evidence"]["values"].append("forged")
    assert value_frozen()["contexts"]["md-000"]["value_evidence"]["values"] == ["A", "a "]
    assert value_frozen(value_max_values=1)["contexts"]["md-000"]["value_evidence"]["values"] is None
    database = value_protocol["root"] / "dev_databases/sample/sample.sqlite"
    with sqlite3.connect(database) as db:
        db.execute("INSERT INTO records(loan_date) VALUES ('B')")
    with pytest.raises(ValueError):
        bird.validate(first, allow_declared_drift=True)
    value_protocol["standard"].update(bird._fingerprints(value_protocol["root"], value_protocol["paths"]))
    second = value_frozen()
    assert second["contexts"]["md-000"]["value_evidence"]["values"] == ["A", "B", "a "]
    assert second["protocol_revision"] != first["protocol_revision"]



def test_registered_prompt_requires_its_declared_factor(benchmark_api):
    from agent.prompts import DENOMINATOR_SCOPE_PROMPT_REVISION

    with pytest.raises(ValueError):
        bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=["one"],
                    forge_prompt_revision=DENOMINATOR_SCOPE_PROMPT_REVISION)
    with pytest.raises(ValueError):
        bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=["one"],
                    variable="prompt", forge_prompt_revision="unregistered-prompt")


@pytest.mark.asyncio
async def test_registered_prompt_roundtrip_rejects_forged_instructions(client, benchmark_api):
    from agent.prompts import DENOMINATOR_SCOPE_PROMPT_REVISION

    treatment = bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=["one"],
                            variable="prompt", forge_prompt_revision=DENOMINATOR_SCOPE_PROMPT_REVISION)
    bird.persist(treatment["manifest"])
    response = await client.post("/api/internal/benchmark-v2/context", headers=benchmark_api["headers"], json={
        "case_id": "one", "protocol_revision": treatment["protocol_revision"],
    })
    assert response.status_code == 200
    context = response.json()
    assert bird.context_hashes({"one": context}) == treatment["manifest"]["context_hashes"]
    forged = copy.deepcopy(treatment["manifest"])
    forged["context_hashes"]["one"]["forge"] = bird.digest("arbitrary instructions")
    rejected = await client.post("/api/internal/benchmark-v2/protocol", headers=benchmark_api["headers"], json={
        "provider": "offline", "model": "fixture", "case_ids": ["one"], "confirm_model_calls": 2,
        "protocol_manifest": forged,
    })
    assert rejected.status_code == 409


def test_prompt_compare_binds_each_revision_and_keeps_runtime_fixed(benchmark_api):
    from agent.prompts import DENOMINATOR_SCOPE_PROMPT_REVISION

    control = bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=["one"], variable="prompt")
    treatment = bird.freeze(cohort="D", provider="offline", model="fixture", case_ids=["one"],
                            variable="prompt", forge_prompt_revision=DENOMINATOR_SCOPE_PROMPT_REVISION)
    results = []
    for response in (control, treatment):
        candidates = ledger(response)
        candidates["generation_contract"] = {
            **response["generation"], "forge_prompt_revision": response["forge_prompt_revision"],
            "pi_runtime_revision": "same-runtime", "pi_sdk_lock_revision": "same-sdk",
        }
        results.append(bird.replay(candidates))
    assert bird.compare(*results)["comparable"] is True
    unbound = copy.deepcopy(results[1])
    unbound["generation_contract"]["forge_prompt_revision"] = control["forge_prompt_revision"]
    assert bird.compare(results[0], unbound)["comparable"] is False
    changed_runtime = copy.deepcopy(results[1])
    changed_runtime["generation_contract"]["pi_runtime_revision"] = "different-runtime"
    assert bird.compare(results[0], changed_runtime)["comparable"] is False
    shared_drift = copy.deepcopy(results[1])
    shared_drift["protocol_manifest"]["context_hashes"]["one"]["direct"] = bird.digest("changed Direct prompt")
    shared_drift["protocol_revision"] = bird.digest(shared_drift["protocol_manifest"])
    assert bird.compare(results[0], shared_drift)["comparable"] is False
