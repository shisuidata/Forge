"""
Tests for forge.metric_validator — atomic and derivative metrics.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from registry.validator import validate_metric

REGISTRY = {
    "tables": {
        "users":  {"columns": ["id", "name", "city", "is_vip"]},
        "orders": {"columns": ["id", "user_id", "status", "total_amount", "created_at"]},
    }
}

# ── fixture helpers ───────────────────────────────────────────────────────────

def atomic(overrides: dict = {}) -> dict:
    m = {
        "metric_class": "atomic",
        "label":        "订单金额",
        "description":  "已完成订单的成交金额",
        "measure":      "orders.total_amount",
        "aggregation":  "sum",
        "qualifiers":   ["orders.status = 'completed'"],
        "period_col":   "orders.created_at",
        "dimensions":   ["users.city", "users.is_vip"],
    }
    m.update(overrides)
    return m


def derivative(overrides: dict = {}) -> dict:
    m = {
        "metric_class": "derivative",
        "label":        "复购率",
        "description":  "有重复购买行为的用户占比",
        "numerator":    "repurchase_users",
        "denominator":  "ordered_users",
        "period_col":   "orders.created_at",
    }
    m.update(overrides)
    return m


# existing metrics used for derivative validation
EXISTING = {
    "order_amount": {
        "metric_class": "atomic",
        "measure":      "orders.total_amount",
        "aggregation":  "sum",
        "qualifiers":   ["orders.status = 'completed'"],
        "period_col":   "orders.created_at",
    },
    "order_count": {
        "metric_class": "atomic",
        "measure":      "orders.id",
        "aggregation":  "count_distinct",
        "qualifiers":   ["orders.status = 'completed'"],
        "period_col":   "orders.created_at",
    },
    "repurchase_users": {
        "metric_class": "atomic",
        "measure":      "orders.user_id",
        "aggregation":  "count_distinct",
        "period_col":   "orders.created_at",
    },
    "ordered_users": {
        "metric_class": "atomic",
        "measure":      "orders.user_id",
        "aggregation":  "count_distinct",
        "period_col":   "orders.created_at",
    },
    "user_count": {
        "metric_class": "atomic",
        "measure":      "users.id",       # different table from orders
        "aggregation":  "count_distinct",
    },
}


# ══════════════════════════════════════════════════════════════════════════════
# Atomic metrics
# ══════════════════════════════════════════════════════════════════════════════

def test_valid_atomic_passes():
    r = validate_metric(atomic(), REGISTRY)
    assert r.valid
    assert r.errors == []


def test_atomic_missing_label():
    r = validate_metric(atomic({"label": ""}), REGISTRY)
    assert not r.valid
    assert any("label" in e for e in r.errors)


def test_atomic_missing_description():
    r = validate_metric(atomic({"description": ""}), REGISTRY)
    assert not r.valid
    assert any("description" in e for e in r.errors)


def test_atomic_missing_measure():
    r = validate_metric(atomic({"measure": ""}), REGISTRY)
    assert not r.valid
    assert any("measure" in e for e in r.errors)


def test_atomic_measure_wrong_format():
    r = validate_metric(atomic({"measure": "just_column"}), REGISTRY)
    assert not r.valid
    assert any("table.column" in e for e in r.errors)


def test_atomic_measure_nonexistent_column():
    r = validate_metric(atomic({"measure": "orders.ghost_col"}), REGISTRY)
    assert not r.valid
    assert any("orders.ghost_col" in e for e in r.errors)


def test_atomic_missing_aggregation():
    r = validate_metric(atomic({"aggregation": ""}), REGISTRY)
    assert not r.valid
    assert any("aggregation" in e for e in r.errors)


def test_atomic_invalid_aggregation():
    r = validate_metric(atomic({"aggregation": "median"}), REGISTRY)
    assert not r.valid
    assert any("median" in e for e in r.errors)


def test_atomic_bad_qualifier_column():
    r = validate_metric(atomic({"qualifiers": ["orders.ghost = 'x'"]}), REGISTRY)
    assert not r.valid
    assert any("orders.ghost" in e for e in r.errors)


def test_atomic_bad_dimension():
    r = validate_metric(atomic({"dimensions": ["users.city", "users.ghost"]}), REGISTRY)
    assert not r.valid
    assert any("users.ghost" in e for e in r.errors)


def test_atomic_bad_period_col():
    r = validate_metric(atomic({"period_col": "orders.ghost_ts"}), REGISTRY)
    assert not r.valid
    assert any("orders.ghost_ts" in e for e in r.errors)


def test_atomic_dimension_without_dot_skipped():
    r = validate_metric(atomic({"dimensions": ["users.city", "bare_col"]}), REGISTRY)
    assert r.valid


def test_atomic_empty_registry_skips_col_check():
    m = atomic({"measure": "ghost.col", "qualifiers": ["ghost.x = 1"]})
    r = validate_metric(m, {})
    assert r.valid


def test_atomic_sum_without_qualifiers_warns():
    r = validate_metric(atomic({"qualifiers": []}), REGISTRY)
    assert r.valid
    assert any("qualifiers" in w for w in r.warnings)


def test_atomic_missing_dimensions_warns():
    r = validate_metric(atomic({"dimensions": []}), REGISTRY)
    assert r.valid
    assert any("dimensions" in w for w in r.warnings)


def test_atomic_count_without_qualifiers_no_warn():
    """count without qualifiers is normal (e.g. total user count)."""
    m = atomic({"aggregation": "count", "qualifiers": []})
    r = validate_metric(m, REGISTRY)
    assert r.valid
    assert not any("qualifiers" in w for w in r.warnings)


def test_invalid_metric_class():
    r = validate_metric(atomic({"metric_class": "derived"}), REGISTRY)
    assert not r.valid
    assert any("atomic" in e or "derivative" in e for e in r.errors)


# ══════════════════════════════════════════════════════════════════════════════
# Derivative metrics
# ══════════════════════════════════════════════════════════════════════════════

def test_valid_derivative_passes():
    r = validate_metric(derivative(), REGISTRY, metric_name="repurchase_rate", all_metrics=EXISTING)
    assert r.valid
    assert r.errors == []


def test_derivative_missing_numerator():
    r = validate_metric(derivative({"numerator": ""}), REGISTRY, all_metrics=EXISTING)
    assert not r.valid
    assert any("numerator" in e for e in r.errors)


def test_derivative_missing_denominator():
    r = validate_metric(derivative({"denominator": ""}), REGISTRY, all_metrics=EXISTING)
    assert not r.valid
    assert any("denominator" in e for e in r.errors)


def test_derivative_nonexistent_numerator():
    r = validate_metric(derivative({"numerator": "ghost_metric"}), REGISTRY, all_metrics=EXISTING)
    assert not r.valid
    assert any("ghost_metric" in e for e in r.errors)


def test_derivative_nonexistent_denominator():
    r = validate_metric(derivative({"denominator": "ghost_metric"}), REGISTRY, all_metrics=EXISTING)
    assert not r.valid
    assert any("ghost_metric" in e for e in r.errors)


def test_derivative_cannot_reference_another_derivative():
    existing = dict(EXISTING)
    existing["some_rate"] = {"metric_class": "derivative", "numerator": "order_count", "denominator": "ordered_users"}
    m = derivative({"numerator": "some_rate"})
    r = validate_metric(m, REGISTRY, all_metrics=existing)
    assert not r.valid
    assert any("衍生指标" in e for e in r.errors)


def test_derivative_self_reference():
    r = validate_metric(derivative(), REGISTRY, metric_name="repurchase_users", all_metrics=EXISTING)
    assert not r.valid
    assert any("自身" in e for e in r.errors)


def test_derivative_cross_table_grain_warns():
    """numerator from users table, denominator from orders table → warning."""
    m = derivative({"numerator": "user_count", "denominator": "order_count"})
    r = validate_metric(m, REGISTRY, all_metrics=EXISTING)
    assert r.valid  # warning only, not error
    assert any("不同的表" in w for w in r.warnings)


def test_derivative_qualifier_mismatch_warns_without_notes():
    """order_amount has qualifier, ordered_users does not → warn if no notes."""
    m = derivative({"numerator": "order_amount", "denominator": "ordered_users"})
    r = validate_metric(m, REGISTRY, all_metrics=EXISTING)
    assert r.valid
    assert any("业务限定不一致" in w for w in r.warnings)


def test_derivative_qualifier_mismatch_no_warn_with_notes():
    """Same mismatch but notes provided → no warning."""
    m = derivative({
        "numerator":   "order_amount",
        "denominator": "ordered_users",
        "notes":       "分母故意不限定 completed，用于计算转化率",
    })
    r = validate_metric(m, REGISTRY, all_metrics=EXISTING)
    assert r.valid
    assert not any("业务限定不一致" in w for w in r.warnings)


def test_derivative_same_qualifiers_no_warn():
    """Both components have same qualifiers → no qualifier warning."""
    m = derivative({"numerator": "order_amount", "denominator": "order_count"})
    r = validate_metric(m, REGISTRY, all_metrics=EXISTING)
    assert r.valid
    assert not any("业务限定不一致" in w for w in r.warnings)


def test_derivative_period_col_mismatch_warns():
    existing = dict(EXISTING)
    existing["metric_a"] = {"metric_class": "atomic", "measure": "orders.id",
                            "aggregation": "count", "period_col": "orders.created_at"}
    existing["metric_b"] = {"metric_class": "atomic", "measure": "orders.id",
                            "aggregation": "count", "period_col": "users.created_at"}
    m = {"metric_class": "derivative", "label": "X", "description": "X",
         "numerator": "metric_a", "denominator": "metric_b"}
    r = validate_metric(m, REGISTRY, all_metrics=existing)
    assert r.valid
    assert any("period_col" in w for w in r.warnings)


def test_derivative_period_col_mismatch_suppressed_by_explicit():
    existing = dict(EXISTING)
    existing["metric_a"] = {"metric_class": "atomic", "measure": "orders.id",
                            "aggregation": "count", "period_col": "orders.created_at"}
    existing["metric_b"] = {"metric_class": "atomic", "measure": "orders.id",
                            "aggregation": "count", "period_col": "users.created_at"}
    m = {"metric_class": "derivative", "label": "X", "description": "X",
         "numerator": "metric_a", "denominator": "metric_b",
         "period_col": "orders.created_at"}  # explicit override
    r = validate_metric(m, REGISTRY, all_metrics=existing)
    assert not any("period_col" in w for w in r.warnings)
