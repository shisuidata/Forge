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

    # ── 规则 2：品类输出/分组应同时满足可读性与去重键要求 ─────────────────────
    _check_category_fields(forge_json, q, warnings)

    # ── 规则 3：客单价应是 WHERE 过滤，不是 AVG+HAVING ────────────────────────
    _check_unit_price(forge_json, q, warnings)

    # ── 规则 4：LAG/LEAD 应按时间 ASC 排序 ────────────────────────────────────
    _check_lag_lead_order(forge_json, q, warnings)

    # ── 规则 5：退款率必须使用退款事实表 ───────────────────────────────────────
    _check_refund_rate(forge_json, q, warnings)

    # ── 规则 6：分组内 TopN 必须用 window + qualify ───────────────────────────
    _check_per_group_topn(forge_json, q, warnings)

    return warnings


# ── 内部检查函数 ──────────────────────────────────────────────────────────────

_BEHAVIOR_KEYWORDS = [
    "消费排名", "消费总额排名", "消费金额排名",
    "复购", "消费轨迹", "下单时间间隔",
    "消费排名第", "总消费金额排名",
]

_CATEGORY_WORDS = ["品类", "类别", "类目", "品类名"]
_CATEGORY_ID_WORDS = ["品类ID", "品类 ID", "类别ID", "类别 ID", "category_id"]
_TEMPORAL_NAV_WORDS = [
    "上一笔", "下一笔", "上一次", "下一次", "上一条", "下一条",
    "相邻", "间隔", "历史订单", "消费轨迹", "下单时间间隔",
]
_RANKING_FNS = {"row_number", "rank", "dense_rank"}
_PER_GROUP_WORDS = ["各", "每个", "每组", "每类", "内"]
_TOPN_WORDS = ["前", "top", "TOP", "排名第", "排名前", "最高", "最多", "最低", "最少"]


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


def _check_category_fields(forge_json: dict, question: str, warnings: list[str]) -> None:
    """检查品类输出和 GROUP/PARTITION 键是否符合字段约定。"""
    fields = _collect_select_fields(forge_json)
    has_cat_id = any("category_id" in f for f in fields)
    has_cat_name = any("category_name" in f for f in fields)
    question_mentions_category = any(w in question for w in _CATEGORY_WORDS)
    question_explicitly_wants_id = any(w in question for w in _CATEGORY_ID_WORDS)

    if has_cat_id and not has_cat_name and not question_explicitly_wants_id:
        warnings.append(
            "SELECT 中包含 category_id 但缺少 category_name。"
            "展示品类时应使用 dim_category.category_name 而非 category_id。"
            "请将 SELECT 中的 category_id 替换为 dim_category.category_name，"
            "并确保 JOIN 了 dim_category 表。"
        )

    all_refs = _collect_field_refs(forge_json)
    if (
        question_mentions_category
        and any("category_id" in f for f in all_refs)
        and not has_cat_name
        and not question_explicitly_wants_id
    ):
        warnings.append(
            "用户问题询问品类/类别，但输出中缺少 dim_category.category_name。"
            "请在 select 中加入 dim_category.category_name，避免只返回内部 category_id。"
        )

    grouping_fields = _collect_group_fields(forge_json)
    groups_by_name = any("category_name" in f for f in grouping_fields)
    groups_by_id = any("category_id" in f for f in grouping_fields)
    if groups_by_name and not groups_by_id:
        warnings.append(
            "按 dim_category.category_name 分组时，应同时加入 dim_category.category_id。"
            "同名但不同 ID 的品类可能被错误合并。"
            '请使用类似：group: ["dim_category.category_id", "dim_category.category_name"]。'
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


def _check_lag_lead_order(forge_json: dict, question: str, warnings: list[str]) -> None:
    """LAG/LEAD 处理时序问题时必须按时间升序取上一条/下一条。"""
    is_temporal_question = any(w in question for w in _TEMPORAL_NAV_WORDS)
    for window in _collect_windows(forge_json):
        if str(window.get("fn", "")).lower() not in {"lag", "lead"}:
            continue
        order = window.get("order") or []
        if not order and is_temporal_question:
            warnings.append(
                "LAG/LEAD 用于上一笔/下一笔/相邻记录时必须声明时间排序。"
                "请添加 order: [{\"col\": \"<时间字段>\", \"dir\": \"asc\"}]。"
            )
            continue
        for sort_key in order:
            col = str(sort_key.get("col", "")).lower()
            direction = str(sort_key.get("dir", "")).lower()
            if direction != "desc":
                continue
            if is_temporal_question or _looks_like_time_field(col):
                warnings.append(
                    "LAG/LEAD 的时间排序方向不应使用 DESC。"
                    "按时间序列取上一条/下一条记录时，应使用时间字段 ASC 排序，"
                    "否则 LAG 会取到更晚的记录，LEAD 会取到更早的记录。"
                )


def _check_refund_rate(forge_json: dict, question: str, warnings: list[str]) -> None:
    """退款率应从订单事实表 LEFT JOIN 退款事实表计算。"""
    if "退款率" not in question:
        return
    refs = _collect_field_refs(forge_json)
    joined_or_scanned_refund = any("dwd_refund_detail" in f for f in refs)
    uses_order_refund_id = any("dwd_order_detail.refund_id" in f for f in refs)
    if uses_order_refund_id or not joined_or_scanned_refund:
        warnings.append(
            "退款率应使用 dwd_refund_detail 退款事实表计算。"
            "从 dwd_order_detail 出发作为总订单数分母，"
            "LEFT JOIN dwd_refund_detail ON dwd_refund_detail.order_id = dwd_order_detail.order_id，"
            "分子使用 COUNT(DISTINCT dwd_refund_detail.order_id) 或 "
            "COUNT(DISTINCT dwd_refund_detail.refund_id)。"
            "不要引用 dwd_order_detail.refund_id。"
        )


def _check_per_group_topn(forge_json: dict, question: str, warnings: list[str]) -> None:
    """组内 TopN 必须用排名窗口函数，并用 qualify 过滤排名。"""
    if not _question_asks_per_group_topn(question):
        return

    windows = _collect_windows(forge_json)
    ranking_aliases = {
        str(w.get("as", ""))
        for w in windows
        if str(w.get("fn", "")).lower() in _RANKING_FNS
    }
    qualifies = _collect_qualify_conditions(forge_json)
    qualify_cols = {str(cond.get("col", "")) for cond in qualifies if isinstance(cond, dict)}
    has_rank_qualify = bool(ranking_aliases & qualify_cols)

    if ranking_aliases and not has_rank_qualify:
        warnings.append(
            "这是分组内 TopN 查询。已经生成了排名窗口函数，但缺少 qualify 过滤排名，"
            "会返回每组全部行而不是每组前 N。"
            "请添加类似：qualify: [{\"col\": \"排名别名\", \"op\": \"lte\", \"val\": N}]。"
        )
        return

    if not ranking_aliases:
        warnings.append(
            "这是分组内 TopN 查询，不能只用全局 sort/limit。"
            "请先用 row_number/rank/dense_rank 窗口函数按分组 partition 排名，"
            "再用 qualify 过滤每组 rank <= N。"
        )


# ── 辅助函数 ──────────────────────────────────────────────────────────────────

def _has_order_status_filter(fj: dict) -> bool:
    """递归检查是否存在 order_status 过滤（含 CTE）。"""
    for query in _iter_queries(fj):
        for f in query.get("filter", []):
            if _condition_has_col(f, "order_status"):
                return True
    return False


def _collect_select_fields(fj: dict) -> list[str]:
    """收集所有 SELECT 字段名（含 CTE 和 expr）。"""
    fields: list[str] = []
    for query in _iter_queries(fj):
        for item in query.get("select", []):
            if isinstance(item, str):
                fields.append(item)
            elif isinstance(item, dict):
                fields.append(str(item.get("as", "")))
                fields.append(str(item.get("expr", "")))
    return fields


def _has_avg_amount_agg(fj: dict) -> bool:
    """检查是否使用了 AVG(total_amount/pay_amount)。"""
    for query in _iter_queries(fj):
        for agg in query.get("agg", []):
            if agg.get("fn") == "avg" and "amount" in str(agg.get("col", "")).lower():
                return True
    return False


def _iter_queries(fj: dict):
    """遍历主查询、CTE、递归项以及集合运算分支。"""
    if not isinstance(fj, dict):
        return
    yield fj
    for cte in fj.get("cte", []):
        yield from _iter_queries(cte.get("query", {}))
        yield from _iter_queries(cte.get("recursive_term", {}))
    for key in ("union", "intersect", "except"):
        for branch in fj.get(key, []):
            yield from _iter_queries(branch.get("query", {}))


def _collect_group_fields(fj: dict) -> list[str]:
    """收集 GROUP BY 字段。"""
    fields: list[str] = []
    for query in _iter_queries(fj):
        for item in query.get("group", []):
            if isinstance(item, str):
                fields.append(item)
            elif isinstance(item, dict):
                fields.append(str(item.get("expr", "")))
                fields.append(str(item.get("as", "")))
    return fields


def _collect_windows(fj: dict) -> list[dict]:
    """收集所有窗口表达式。"""
    windows: list[dict] = []
    for query in _iter_queries(fj):
        windows.extend(w for w in query.get("window", []) if isinstance(w, dict))
    return windows


def _collect_qualify_conditions(fj: dict) -> list[dict]:
    """收集所有 qualify 条件。"""
    conditions: list[dict] = []
    for query in _iter_queries(fj):
        conditions.extend(c for c in query.get("qualify", []) if isinstance(c, dict))
    return conditions


def _collect_field_refs(fj: dict) -> list[str]:
    """宽松收集 Forge JSON 中可能的字段引用，供约定检查使用。"""
    refs: list[str] = []
    for query in _iter_queries(fj):
        for key in ("scan",):
            refs.append(str(query.get(key, "")))
        for item in query.get("select", []):
            if isinstance(item, str):
                refs.append(item)
            elif isinstance(item, dict):
                refs.append(str(item.get("expr", "")))
                refs.append(str(item.get("as", "")))
        for join in query.get("joins", []):
            refs.append(str(join.get("table", "")))
            refs.append(str(join.get("on", "")))
            refs.extend(_collect_refs_from_obj(join.get("on", [])))
        for key in ("filter", "having", "qualify", "agg", "group", "sort", "window"):
            refs.extend(_collect_refs_from_obj(query.get(key, [])))
    return [r for r in refs if r]


def _collect_refs_from_obj(obj) -> list[str]:
    refs: list[str] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in {"col", "col2", "expr", "as", "left", "right", "table"}:
                refs.append(str(value))
            refs.extend(_collect_refs_from_obj(value))
    elif isinstance(obj, list):
        for item in obj:
            refs.extend(_collect_refs_from_obj(item))
    elif isinstance(obj, str):
        if "." in obj or "_" in obj:
            refs.append(obj)
    return refs


def _condition_has_col(condition, needle: str) -> bool:
    if isinstance(condition, dict):
        col = str(condition.get("col", ""))
        if needle in col:
            return True
        return any(_condition_has_col(value, needle) for value in condition.values())
    if isinstance(condition, list):
        return any(_condition_has_col(item, needle) for item in condition)
    return False


def _looks_like_time_field(col: str) -> bool:
    return any(token in col for token in ("_dt", "_date", "_time", "created_at", "updated_at"))


def _question_asks_per_group_topn(question: str) -> bool:
    has_group_scope = any(w in question for w in _PER_GROUP_WORDS)
    has_topn = any(w in question for w in _TOPN_WORDS)
    # 避免把"各品类销售额，按销售额排序"误判成组内 TopN；需要明确排名/前N/最高等 TopN 信号。
    return has_group_scope and has_topn
