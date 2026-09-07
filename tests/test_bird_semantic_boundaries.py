"""EX snapshot matches must not be promoted to SQL/business equivalence claims."""

import sqlite3

from forge.benchmark_methods import bird_execution_accuracy
from forge.benchmark_v2 import build_result_contract, semantic_result_compare


def test_top_five_snapshot_coincidence_breaks_on_a_sixth_eligible_school():
    gold_sql = "SELECT name FROM school WHERE county = 'A' ORDER BY score DESC LIMIT 5"
    missing_top_five = "SELECT name FROM school WHERE county = 'A' ORDER BY score DESC"
    db = sqlite3.connect(":memory:")
    try:
        db.execute("CREATE TABLE school (name TEXT, county TEXT, score INTEGER)")
        db.executemany("INSERT INTO school VALUES (?, 'A', ?)", [(f"school-{i}", 100 - i) for i in range(5)])
        gold = db.execute(gold_sql).fetchall()
        predicted = db.execute(missing_top_five).fetchall()
        assert bird_execution_accuracy(gold, predicted) is True
        # Even the stronger row-order/multiset contract cannot detect this missing predicate on this snapshot.
        contract = build_result_contract("List the top 5 schools by score")
        assert semantic_result_compare(gold, predicted, contract)["correct"] is True
        db.execute("INSERT INTO school VALUES ('school-5', 'A', 95)")
        gold = db.execute(gold_sql).fetchall()
        predicted = db.execute(missing_top_five).fetchall()
        assert gold == [(f"school-{i}",) for i in range(5)]
        assert predicted == gold + [("school-5",)]
        assert bird_execution_accuracy(gold, predicted) is False
        assert semantic_result_compare(gold, predicted, contract)["correct"] is False
    finally:
        db.close()


def test_contract_and_official_ex_are_separate_verdicts_not_interchangeable():
    gold = [("first", 10), ("second", 5)]
    reverse = list(reversed(gold))
    ordered = build_result_contract("List results sorted by score descending")
    assert bird_execution_accuracy(gold, reverse) is True
    assert semantic_result_compare(gold, reverse, ordered)["correct"] is False
    swapped_columns = [(score, name) for name, score in gold]
    assert bird_execution_accuracy(gold, swapped_columns) is False
    assert semantic_result_compare(gold, swapped_columns, ordered)["correct"] is True
