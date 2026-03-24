"""
Forge JSON 约定检查器（Convention Lint）

在编译前对 Forge JSON 做语义级检查，捕获模型"编译能过但业务逻辑错"的情况。
程序化验证的覆盖率是 100%，模型注意力不是。

用法：
    from forge.lint import lint_conventions
    warnings = lint_conventions(forge_json, question)
    # warnings 为空 → 通过；非空 → 反馈给模型修正
"""
from __future__ import annotations


def lint_conventions(forge_json: dict, question: str) -> list[str]:
    """检查 Forge JSON 是否违反字段使用约定，返回修复建议列表。"""
    warnings: list[str] = []
    q = question

    # ── 规则 1：用户行为查询应过滤 order_status='已完成' ──────────────────────
    _check_order_status(forge_json, q, warnings)

    # ── 规则 2：SELECT 含 category_id 但缺 category_name ─────────────────────
    _check_category_name(forge_json, warnings)

    # ── 规则 3：客单价应是 WHERE 过滤，不是 AVG+HAVING ────────────────────────
    _check_unit_price(forge_json, q, warnings)

    return warnings


# ── 内部检查函数 ──────────────────────────────────────────────────────────────

_BEHAVIOR_KEYWORDS = [
    "消费排名", "消费总额排名", "消费金额排名",
    "复购", "消费轨迹", "下单时间间隔",
    "消费排名第", "总消费金额排名",
]


def _check_order_status(forge_json: dict, question: str, warnings: list[str]) -> None:
    """用户行为分析类查询应包含 order_status='已完成' 过滤。"""
    if not any(kw in question for kw in _BEHAVIOR_KEYWORDS):
        return
    if _has_order_status_filter(forge_json):
        return
    warnings.append(
        "此查询涉及用户消费行为分析（消费排名/复购/消费轨迹），根据字段约定，"
        "应添加 dwd_order_detail.order_status = '已完成' 过滤条件。"
        '请在相关查询的 filter 中添加：'
        '{"col": "dwd_order_detail.order_status", "op": "eq", "val": "已完成"}'
    )


def _check_category_name(forge_json: dict, warnings: list[str]) -> None:
    """SELECT 中包含 category_id 但缺少 category_name 时提醒。"""
    fields = _collect_select_fields(forge_json)
    has_cat_id = any("category_id" in f for f in fields)
    has_cat_name = any("category_name" in f for f in fields)
    if has_cat_id and not has_cat_name:
        warnings.append(
            "SELECT 中包含 category_id 但缺少 category_name。"
            "展示品类时应使用 dim_category.category_name 而非 category_id。"
            "请将 SELECT 中的 category_id 替换为 dim_category.category_name，"
            "并确保 JOIN 了 dim_category 表。"
        )


def _check_unit_price(forge_json: dict, question: str, warnings: list[str]) -> None:
    """客单价在X-Y之间应是 WHERE 过滤，不是 AVG+HAVING。"""
    if "客单价" not in question:
        return
    range_words = ["之间", "到", "以上", "以下", "超过", "低于", "大于", "小于"]
    if not any(w in question for w in range_words):
        return
    if _has_avg_amount_agg(forge_json):
        warnings.append(
            '"客单价在X到Y之间"指单笔订单金额的 WHERE 过滤（filter），'
            "不是用户平均消费的 AVG+HAVING。"
            "请将 total_amount 或 pay_amount 的范围条件放在 filter 中，"
            "而非使用 agg avg + having。"
        )


# ── 辅助函数 ──────────────────────────────────────────────────────────────────

def _has_order_status_filter(fj: dict) -> bool:
    """递归检查是否存在 order_status 过滤（含 CTE）。"""
    for f in fj.get("filter", []):
        col = str(f.get("col", ""))
        if "order_status" in col:
            return True
    for cte in fj.get("cte", []):
        if _has_order_status_filter(cte.get("query", {})):
            return True
    return False


def _collect_select_fields(fj: dict) -> list[str]:
    """收集所有 SELECT 字段名（含 CTE 和 expr）。"""
    fields: list[str] = []
    for item in fj.get("select", []):
        if isinstance(item, str):
            fields.append(item)
        elif isinstance(item, dict):
            fields.append(item.get("as", ""))
            fields.append(str(item.get("expr", "")))
    for cte in fj.get("cte", []):
        fields.extend(_collect_select_fields(cte.get("query", {})))
    return fields


def _has_avg_amount_agg(fj: dict) -> bool:
    """检查是否使用了 AVG(total_amount/pay_amount)。"""
    for agg in fj.get("agg", []):
        if agg.get("fn") == "avg" and "amount" in str(agg.get("col", "")).lower():
            return True
    for cte in fj.get("cte", []):
        if _has_avg_amount_agg(cte.get("query", {})):
            return True
    return False
