"""
指标定义校验器 — 支持原子指标（atomic）和衍生指标（derivative）。

校验分两层：
    硬错误（errors）  → 阻止保存，调用方必须修正后重试
    软警告（warnings）→ 保存继续，但在 UI 和 Agent 回复中展示提示

原子指标必填字段：
    metric_class, label, description, measure（table.column 格式）, aggregation

原子指标可选字段：
    qualifiers（业务限定，永远应用）, period_col（时间字段）, dimensions（可分析维度）

衍生指标必填字段：
    metric_class, label, description, numerator（原子指标名）, denominator（原子指标名）

衍生指标约束规则：
    ① numerator/denominator 必须是已注册的原子指标（不允许引用另一个衍生指标）
    ② 不允许自引用（numerator 或 denominator 等于自身名称）
    ③ 跨表粒度差异 → 警告（不同 measure 来自不同表）
    ④ qualifiers 不一致且无 notes → 警告（要求业务说明）
    ⑤ period_col 不一致且衍生指标未显式声明 period_col → 警告

字段引用校验：
    measure、qualifiers 中的列引用、dimensions（含 . 的项）、period_col
    都会在 schema.registry.json 中验证是否存在。
    若 structural_registry 为空字典，跳过所有字段引用检查（测试或首次使用场景）。
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

# 合法聚合函数集合（与 Forge DSL schema.json 中 AggWithCol.fn 枚举对齐）
VALID_AGGREGATIONS   = {"sum", "count", "count_distinct", "avg", "min", "max"}
# 合法指标类型集合
VALID_METRIC_CLASSES = {"atomic", "derivative"}

# 匹配 table.column 格式的正则，用于从 qualifiers 等字符串中提取所有列引用
_COL_REF = re.compile(r'\b([a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*)\b')


@dataclass
class ValidationResult:
    """
    校验结果容器。

    Attributes:
        valid:    是否通过校验（errors 为空时为 True）
        errors:   硬错误列表，任意一条都会阻止保存
        warnings: 软警告列表，不阻止保存但需要关注
    """
    valid:    bool
    errors:   list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        """格式化输出，用于日志或终端展示。"""
        lines = [f"❌ {e}" for e in self.errors] + [f"⚠ {w}" for w in self.warnings]
        return "\n".join(lines)


# ── 辅助函数 ──────────────────────────────────────────────────────────────────

def _valid_columns(structural_registry: dict) -> set[str]:
    """
    从结构层注册表中提取所有合法的 table.column 引用集合。

    支持两种格式：
        {"tables": {"orders": {"columns": ["id", ...]}, ...}}  （标准格式）
        {"orders": {"columns": ["id", ...]}, ...}              （兼容旧格式）

    Args:
        structural_registry: schema.registry.json 的内容字典。

    Returns:
        所有合法列引用的集合，如 {"orders.id", "orders.user_id", "users.name", ...}
        若 structural_registry 为空，返回空集合（跳过字段引用校验）。
    """
    valid: set[str] = set()
    tables = structural_registry.get("tables", structural_registry)
    for table, info in tables.items():
        cols = info.get("columns", info) if isinstance(info, dict) else info
        for col in cols:
            valid.add(f"{table}.{col}")
    return valid


def _check_col_refs(exprs: list[str], valid_cols: set[str], field_name: str) -> list[str]:
    """
    从表达式列表中提取所有 table.column 引用，校验每个引用是否在合法列集合中。

    Args:
        exprs:      待检查的表达式列表（如 qualifiers 条件字符串、dimension 列表）。
        valid_cols: 合法列引用集合，由 _valid_columns() 生成。
        field_name: 字段名称，用于生成友好的错误信息。

    Returns:
        错误信息列表；为空表示所有引用都合法。
    """
    errors = []
    for expr in exprs:
        for ref in _COL_REF.findall(expr):
            if ref not in valid_cols:
                errors.append(f"{field_name} 引用了不存在的字段：{ref}")
    return errors


# ── 原子指标校验 ──────────────────────────────────────────────────────────────

def _validate_atomic(metric: dict, valid_cols: set[str]) -> tuple[list[str], list[str]]:
    """
    校验原子指标的字段完整性和字段引用合法性。

    校验项：
        measure     必填，格式必须为 table.column，字段必须在注册表中存在
        aggregation 必填，必须是 VALID_AGGREGATIONS 中的值
        qualifiers  可选，每个条件中的列引用必须存在（valid_cols 非空时检查）
        dimensions  可选，含 . 的项视为列引用并检查存在性；不含 . 的项（如别名）跳过
        period_col  可选，字段必须存在（valid_cols 非空时检查）

    软警告：
        sum/avg 无 qualifiers → 提示可能统计到非预期状态的数据
        无 dimensions → 提示 LLM 生成查询时缺少维度限制

    Args:
        metric:     指标定义字典。
        valid_cols: 合法列引用集合，为空时跳过字段引用检查。

    Returns:
        (errors, warnings) 元组。
    """
    errors:   list[str] = []
    warnings: list[str] = []

    # ── measure 校验 ─────────────────────────────────────────────────────────
    measure = metric.get("measure", "")
    if not measure:
        errors.append("原子指标必须声明度量字段（measure），格式为 table.column")
    else:
        if "." not in measure or not _COL_REF.fullmatch(measure):
            errors.append(f"measure 格式错误，应为 table.column，当前：{measure!r}")
        elif valid_cols and measure not in valid_cols:
            errors.append(f"measure 引用了不存在的字段：{measure}")

    # ── aggregation 校验 ─────────────────────────────────────────────────────
    agg = metric.get("aggregation", "")
    if not agg:
        errors.append("原子指标必须声明聚合方式（aggregation）")
    elif agg not in VALID_AGGREGATIONS:
        errors.append(
            f"无效的聚合方式：{agg!r}，"
            f"必须是 {' | '.join(sorted(VALID_AGGREGATIONS))} 之一"
        )

    # ── 字段引用校验（仅在 valid_cols 非空时执行）────────────────────────────
    if valid_cols:
        # qualifiers 是 SQL 条件字符串列表，需要从字符串中解析列引用
        errors += _check_col_refs(metric.get("qualifiers", []), valid_cols, "qualifiers")
        # dimensions 中只检查含 . 的项（bare_col 这类非 table.col 格式的维度跳过）
        errors += _check_col_refs(
            [d for d in metric.get("dimensions", []) if "." in d],
            valid_cols,
            "dimensions",
        )
        pc = metric.get("period_col", "")
        if pc and pc not in valid_cols:
            errors.append(f"period_col 引用了不存在的字段：{pc}")

    # ── 软警告 ───────────────────────────────────────────────────────────────
    if metric.get("aggregation") in ("sum", "avg") and not metric.get("qualifiers"):
        warnings.append("sum / avg 类型建议设置 qualifiers，否则统计所有状态的数据")
    if not metric.get("dimensions"):
        warnings.append("未设置 dimensions，LLM 生成查询时无维度限制")

    return errors, warnings


# ── 衍生指标校验 ──────────────────────────────────────────────────────────────

def _validate_derivative(
    metric:      dict,
    metric_name: str,
    all_metrics: dict,
) -> tuple[list[str], list[str]]:
    """
    校验衍生指标的引用关系和一致性约束。

    校验项（硬错误）：
        numerator/denominator 不为空
        numerator/denominator 不等于自身名称（防止自引用死循环）
        numerator/denominator 在 all_metrics 中存在
        numerator/denominator 的 metric_class 必须是 atomic（禁止衍生指标链式引用）

    校验项（软警告）：
        跨表粒度：分子分母 measure 来自不同表
        qualifier 不一致：分子分母的业务限定不同且未提供 notes 说明
        period_col 不一致：分子分母时间字段不同且衍生指标未显式声明 period_col

    Args:
        metric:      指标定义字典。
        metric_name: 当前指标的名称（用于自引用检测）。
        all_metrics: 已注册的所有指标字典（metrics.registry.yaml 的内容）。

    Returns:
        (errors, warnings) 元组。
    """
    errors:   list[str] = []
    warnings: list[str] = []

    num_name = metric.get("numerator", "")
    den_name = metric.get("denominator", "")

    # ── 必填检查 ──────────────────────────────────────────────────────────────
    if not num_name:
        errors.append("衍生指标必须声明分子（numerator），填写原子指标名称")
    if not den_name:
        errors.append("衍生指标必须声明分母（denominator），填写原子指标名称")
    if errors:
        return errors, warnings   # 必填缺失时后续检查无意义，提前返回

    # ── 自引用检查 ────────────────────────────────────────────────────────────
    if num_name == metric_name or den_name == metric_name:
        errors.append("衍生指标不能引用自身")
        return errors, warnings

    # ── 引用存在性检查 ────────────────────────────────────────────────────────
    num_def = all_metrics.get(num_name)
    den_def = all_metrics.get(den_name)

    if num_def is None:
        errors.append(f"numerator 引用的指标不存在：{num_name!r}")
    elif num_def.get("metric_class") == "derivative":
        # 不允许衍生指标引用另一个衍生指标（防止链式组合引发语义混乱）
        errors.append(f"numerator 只能引用原子指标，{num_name!r} 是衍生指标")

    if den_def is None:
        errors.append(f"denominator 引用的指标不存在：{den_name!r}")
    elif den_def.get("metric_class") == "derivative":
        errors.append(f"denominator 只能引用原子指标，{den_name!r} 是衍生指标")

    if errors:
        return errors, warnings   # 引用不合法时，一致性检查无意义

    # ── 粒度一致性检查（警告）────────────────────────────────────────────────
    num_measure = num_def.get("measure", "")
    den_measure = den_def.get("measure", "")
    if num_measure and den_measure:
        num_table = num_measure.split(".")[0]
        den_table = den_measure.split(".")[0]
        if num_table != den_table:
            # 跨表比率在业务上有时合理（如用户留存率），但需要人工确认
            warnings.append(
                f"分子（{num_name}）和分母（{den_name}）的度量字段来自不同的表"
                f"（{num_table} vs {den_table}），请确认口径是否合理"
            )

    # ── qualifier 一致性检查（警告，notes 可抑制）────────────────────────────
    num_quals = frozenset(num_def.get("qualifiers", []))
    den_quals = frozenset(den_def.get("qualifiers", []))
    if num_quals != den_quals and not metric.get("notes"):
        # qualifier 不一致时通常意味着业务口径存在差异（如转化率的分母不限定状态）
        # 要求用户在 notes 中说明原因，将隐式的业务决策变为显式文档
        warnings.append(
            f"分子（{num_name}）与分母（{den_name}）的业务限定不一致：\n"
            f"  分子 qualifiers：{sorted(num_quals) or '无'}\n"
            f"  分母 qualifiers：{sorted(den_quals) or '无'}\n"
            "  如果这是有意为之（如转化率），请在 notes 字段中说明原因"
        )

    # ── period_col 一致性检查（警告，显式声明可抑制）─────────────────────────
    num_pc = num_def.get("period_col", "")
    den_pc = den_def.get("period_col", "")
    if num_pc and den_pc and num_pc != den_pc and not metric.get("period_col"):
        # 分子分母时间字段不同且衍生指标未显式指定 period_col
        # → 查询时不知道用哪个时间字段做时间窗口过滤
        warnings.append(
            f"分子和分母使用不同的时间字段（{num_pc} vs {den_pc}），"
            "建议在衍生指标上显式声明 period_col 以统一时间窗口"
        )

    return errors, warnings


# ── 公开 API ──────────────────────────────────────────────────────────────────

def validate_metric(
    metric:              dict,
    structural_registry: dict,
    metric_name:         str        = "",
    all_metrics:         dict | None = None,
) -> ValidationResult:
    """
    校验一个指标定义，返回 ValidationResult。

    调用流程：
        1. 校验通用字段（label、description、metric_class）
        2. 根据 metric_class 分发到 _validate_atomic 或 _validate_derivative
        3. 汇总 errors 和 warnings，构造 ValidationResult 返回

    Args:
        metric:              指标字段字典（不含 name key）。
        structural_registry: schema.registry.json 的内容，用于字段引用校验。
                             传入空字典 {} 时跳过所有字段引用检查（适用于无 schema 场景）。
        metric_name:         当前指标的 key，用于衍生指标的自引用检测。可为空字符串。
        all_metrics:         所有已注册指标的字典，衍生指标校验时必须提供。
                             原子指标校验时可传 None。

    Returns:
        ValidationResult，valid=True 表示可以保存，valid=False 表示必须修正。
    """
    errors:   list[str] = []
    warnings: list[str] = []

    # ── 通用字段校验 ──────────────────────────────────────────────────────────
    if not metric.get("label"):
        errors.append("缺少显示名称（label）")
    if not metric.get("description"):
        errors.append("缺少指标定义（description）")

    metric_class = metric.get("metric_class", "")
    if metric_class not in VALID_METRIC_CLASSES:
        errors.append(f"无效的指标类型：{metric_class!r}，必须是 atomic 或 derivative")
        # metric_class 无效时无法分发，提前返回
        return ValidationResult(valid=False, errors=errors)

    # 从结构层注册表提取合法列集合（空注册表时返回空集合，跳过字段引用检查）
    valid_cols = _valid_columns(structural_registry)

    # ── 类型专属校验 ──────────────────────────────────────────────────────────
    if metric_class == "atomic":
        e, w = _validate_atomic(metric, valid_cols)
    else:
        e, w = _validate_derivative(metric, metric_name, all_metrics or {})

    errors   += e
    warnings += w
    return ValidationResult(valid=len(errors) == 0, errors=errors, warnings=warnings)
