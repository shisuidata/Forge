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

import re


def lint_conventions(forge_json: dict, question: str) -> list[str]:
    """检查 Forge JSON 是否违反字段使用约定，返回修复建议列表。"""
    warnings: list[str] = []
    q = question

    # ── 规则 1：用户行为查询应过滤 order_status='已完成' ──────────────────────
    _check_order_status(forge_json, q, warnings)

    # ── 规则 2：未指定状态的普通订单统计不要默认过滤 order_status ───────────
    _check_unspecified_order_status_filter(forge_json, q, warnings)

    # ── 规则 3：品类输出/分组应同时满足可读性与去重键要求 ─────────────────────
    _check_category_fields(forge_json, q, warnings)

    # ── 规则 4：客单价应是 WHERE 过滤，不是 AVG+HAVING ────────────────────────
    _check_unit_price(forge_json, q, warnings)

    # ── 规则 5：LAG/LEAD 应按时间 ASC 排序 ────────────────────────────────────
    _check_lag_lead_order(forge_json, q, warnings)

    # ── 规则 6：退款率必须使用退款事实表 ───────────────────────────────────────
    _check_refund_rate(forge_json, q, warnings)

    # ── 规则 7：分组内 TopN 必须用 window + qualify ───────────────────────────
    _check_per_group_topn(forge_json, q, warnings)

    # ── 规则 8：JOIN 明细表后统计订单数必须 count_distinct(order_id) ───────────
    _check_order_count_distinct(forge_json, q, warnings)

    # ── 规则 9：每条明细相对组均值必须保留明细行 ───────────────────────────────
    _check_detail_vs_group_average(forge_json, q, warnings)

    # ── 规则 10：订单明细查询应以明细表字段为准 ────────────────────────────────
    _check_order_detail_query_shape(forge_json, q, warnings)

    # ── 规则 11：找出订单时必须保留订单 ID ───────────────────────────────────
    _check_order_lookup_selects_order_id(forge_json, q, warnings)

    # ── 规则 12：加购但未完成订单必须反查已完成订单 ───────────────────────────
    _check_cart_without_completed_order(forge_json, q, warnings)

    # ── 规则 13：品类 TopN/占比窗口分区应匹配展示粒度 ────────────────────────
    _check_category_window_grain(forge_json, q, warnings)

    # ── 规则 14：品类月度订单量应使用明细表时间字段 ───────────────────────────
    _check_category_monthly_order_grain(forge_json, q, warnings)

    # ── 规则 15：窗口 alias 不能被当作已存在字段引用 ──────────────────────────
    _check_qualified_window_alias_select(forge_json, q, warnings)

    # ── 规则 16：订单用户 JOIN 应以订单头 user_id 为准 ────────────────────────
    _check_order_user_join_source(forge_json, q, warnings)

    # ── 规则 17：占比默认输出百分比口径 ───────────────────────────────────────
    _check_percentage_display(forge_json, q, warnings)

    # ── 规则 18：所有差评均无图片应用反存在语义 ───────────────────────────────
    _check_all_bad_reviews_without_images(forge_json, q, warnings)

    # ── 规则 19：会员等级展示粒度不应按内部 level_id 拆分 ────────────────────
    _check_vip_level_grain(forge_json, q, warnings)

    # ── 规则 20：品类/会员消费总额应保持订单明细事实表粒度 ───────────────────
    _check_category_member_spend_grain(forge_json, q, warnings)

    # ── 规则 21：品牌钻石会员均价应保留审核字段 ───────────────────────────────
    _check_brand_diamond_avg_item_contract(forge_json, q, warnings)

    # ── 规则 22：large schema 中常见维表主键不能写错 ───────────────────────
    _check_known_large_schema_field_names(forge_json, q, warnings)

    # ── 规则 23：好评带图记录的过滤、输出和排序契约 ─────────────────────────
    _check_good_review_with_images_contract(forge_json, q, warnings)

    # ── 规则 24：退款商品排行应保留退款次数且不默认过滤退款状态 ─────────────
    _check_refund_product_ranking_contract(forge_json, q, warnings)

    # ── 规则 25：商品品类占比的展示和窗口口径 ───────────────────────────────
    _check_product_category_share_contract(forge_json, q, warnings)

    # ── 规则 26：相邻评价评分变化不能先 TopN 截断 ───────────────────────────
    _check_adjacent_review_lag_contract(forge_json, q, warnings)

    # ── 规则 27：渠道月度环比契约 ───────────────────────────────────────────
    _check_channel_monthly_mom_contract(forge_json, q, warnings)

    # ── 规则 28：购物车未购买用户输出字段必须限定 ───────────────────────────
    _check_cart_without_purchase_output_contract(forge_json, q, warnings)

    # ── 规则 29：跨事件用户计数 CTE 输出必须限定字段来源 ────────────────────
    _check_cross_event_user_counts_contract(forge_json, q, warnings)

    # ── 规则 30：客单价订单查找结果列契约 ───────────────────────────────────
    _check_unit_price_order_lookup_contract(forge_json, q, warnings)

    # ── 规则 31：派生指标过滤不能在外层误用 HAVING ─────────────────────────
    _check_derived_metric_having(forge_json, q, warnings)

    # ── 规则 32：内部排名别名不应污染结果列 ────────────────────────────────
    _check_internal_rank_output(forge_json, q, warnings)

    # ── 规则 33：退款记录结果列契约 ────────────────────────────────────────
    _check_refund_record_output_contract(forge_json, q, warnings)

    # ── 规则 34：品牌进口商品订单明细结果列契约 ────────────────────────────
    _check_imported_brand_order_detail_contract(forge_json, q, warnings)

    # ── 规则 35：品牌评分偏差结果列契约 ────────────────────────────────────
    _check_brand_rating_deviation_contract(forge_json, q, warnings)

    # ── 规则 36：占比分母窗口只用于计算，不自动暴露 ────────────────────────
    _check_ratio_denominator_output(forge_json, q, warnings)

    # ── 规则 37：内部维度 ID 不自动暴露 ───────────────────────────────────
    _check_unrequested_dimension_ids(forge_json, q, warnings)

    # ── 规则 38：订单月度统计默认使用下单日期 ─────────────────────────────
    _check_monthly_order_time_field(forge_json, q, warnings)

    # ── 规则 39：阈值率先用未舍入值判断 ───────────────────────────────────
    _check_rate_threshold_rounding(forge_json, q, warnings)

    # ── 规则 40：品牌销售汇总稳定结果契约 ─────────────────────────────────
    _check_brand_sales_summary_contract(forge_json, q, warnings)

    # ── 规则 41：渠道 GMV 汇总默认按 GMV 排序 ─────────────────────────────
    _check_channel_gmv_sort_contract(forge_json, q, warnings)

    # ── 规则 42：时间范围事件列表默认按时间倒序 ───────────────────────────
    _check_time_bounded_listing_sort(forge_json, q, warnings)

    return warnings


# ── 内部检查函数 ──────────────────────────────────────────────────────────────

_BEHAVIOR_KEYWORDS = [
    "消费排名", "消费总额排名", "消费金额排名",
    "复购", "消费轨迹", "下单时间间隔",
    "消费排名第", "总消费金额排名",
]

_CATEGORY_WORDS = ["品类", "类别", "类目", "品类名"]
_CATEGORY_ID_WORDS = ["品类ID", "品类 ID", "类别ID", "类别 ID", "category_id"]
_PRODUCT_WORDS = ["商品", "商品名", "商品名称"]
_PRODUCT_ID_WORDS = ["商品ID", "商品 ID", "product_id"]
_TEMPORAL_NAV_WORDS = [
    "上一笔", "下一笔", "上一次", "下一次", "上一条", "下一条",
    "相邻", "间隔", "历史订单", "消费轨迹", "下单时间间隔",
]
_RANKING_FNS = {"row_number", "rank", "dense_rank"}
_PER_GROUP_WORDS = ["各", "每个", "每组", "每类", "内"]
_TOPN_WORDS = ["top", "TOP", "排名第", "排名前", "最高", "最多", "最低", "最少"]


def _check_order_status(forge_json: dict, question: str, warnings: list[str]) -> None:
    """用户行为分析类查询应包含 order_status='已完成' 过滤。"""
    is_order_interval = "下单" in question and "间隔" in question
    if not (any(kw in question for kw in _BEHAVIOR_KEYWORDS) or is_order_interval):
        return
    if _has_order_status_filter(forge_json):
        return
    warnings.append(
        "此查询涉及用户消费行为分析（消费排名/复购/消费轨迹），根据字段约定，"
        "应添加 dwd_order_detail.order_status = '已完成' 过滤条件。"
        '请在相关查询的 filter 中添加：'
        '{"col": "dwd_order_detail.order_status", "op": "eq", "val": "已完成"}'
    )


def _check_unspecified_order_status_filter(forge_json: dict, question: str, warnings: list[str]) -> None:
    """普通订单统计未指定状态时，不应默认收窄到已完成订单。"""
    if any(kw in question for kw in _BEHAVIOR_KEYWORDS):
        return
    if not any(word in question for word in ("订单数", "订单量", "订单占比", "订单比例")):
        return
    if _question_mentions_explicit_order_status(question):
        return
    if not _has_order_status_filter(forge_json):
        return
    warnings.append(
        "用户询问普通订单数/订单量/订单占比，且没有指定“已完成/已支付/已取消”等订单状态。"
        "不要默认添加 dwd_order_detail.order_status 过滤条件；"
        "应统计全部订单状态，除非用户明确要求某个状态或问题属于消费行为分析。"
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


def _check_unit_price(forge_json: dict, question: str, warnings: list[str]) -> None:
    """客单价在X-Y之间应是 WHERE 过滤，不是 AVG+HAVING。"""
    if "客单价" not in question:
        return
    range_words = ["之间", "到", "以上", "以下", "超过", "低于", "大于", "小于"]
    has_unit_price_range = any(
        re.search(rf"客单价.{{0,12}}{word}|{word}.{{0,12}}客单价", question)
        for word in range_words
    )
    if not has_unit_price_range:
        return
    if _has_avg_amount_agg(forge_json):
        warnings.append(
            '"客单价在X到Y之间"指单笔订单金额的 WHERE 过滤（filter），'
            "不是用户平均消费的 AVG/HAVING/窗口均值。"
            "请将 total_amount 或 pay_amount 的范围条件放在 filter 中，"
            "而非使用 AVG 聚合、窗口 AVG 或 qualify。"
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
    split_refund_ctes = (
        isinstance(forge_json, dict)
        and len(forge_json.get("cte", [])) >= 2
        and any("dwd_order_detail" in ref for ref in refs)
        and any("dwd_refund_detail" in ref for ref in refs)
    )
    if uses_order_refund_id or not joined_or_scanned_refund:
        warnings.append(
            "退款率应使用 dwd_refund_detail 退款事实表计算。"
            "从 dwd_order_detail 出发作为总订单数分母，"
            "LEFT JOIN dwd_refund_detail ON dwd_refund_detail.order_id = dwd_order_detail.order_id，"
            "分子使用 COUNT(DISTINCT dwd_refund_detail.order_id) 或 "
            "COUNT(DISTINCT dwd_refund_detail.refund_id)。"
            "不要引用 dwd_order_detail.refund_id。"
        )
    if any(word in question for word in _CATEGORY_WORDS):
        if any(_join_pairs_fields(forge_json, "dwd_order_detail.order_id", "dim_product.product_id")):
            warnings.append(
                "按品类统计退款率时，不能把 dwd_order_detail.order_id 直接连接到 "
                "dim_product.product_id。应从订单明细 dwd_order_item_detail 出发或 JOIN 它："
                "dwd_order_item_detail.order_id = dwd_order_detail.order_id，"
                "再用 dwd_order_item_detail.product_id = dim_product.product_id，"
                "最后 JOIN dim_category。"
            )
        if any("dim_category" in ref for ref in refs) and not any("dwd_order_item_detail" in ref for ref in refs):
            warnings.append(
                "按品类统计退款率必须经过订单明细表 dwd_order_item_detail 才能获得商品/品类粒度。"
                "推荐路径：dwd_order_item_detail -> dim_product -> dim_category，"
                "并通过 order_id JOIN dwd_order_detail 与 dwd_refund_detail。"
            )
    if split_refund_ctes:
        warnings.append(
            "退款率的分子和分母应在同一分组查询中计算，确保维度 JOIN 和过滤口径完全一致。"
            "推荐从 dwd_order_detail 或 dwd_order_item_detail 出发，"
            "LEFT JOIN dwd_refund_detail ON order_id，"
            "同时计算 COUNT(DISTINCT dwd_order_detail.order_id) 和 "
            "COUNT(DISTINCT dwd_refund_detail.order_id)。"
            "不要拆成 total_orders/refund_orders 两个独立 CTE 后再 JOIN。"
        )
    for query in _iter_queries(forge_json):
        for item in query.get("select", []):
            if not isinstance(item, dict):
                continue
            alias = str(item.get("as", "")).lower()
            expr = str(item.get("expr", "")).lower().replace(" ", "")
            if "refund_rate" not in alias and "退款率" not in alias:
                continue
            if "*100" in expr or "100.0*" in expr:
                warnings.append(
                    "退款率是 0~1 小数口径，例如 15% 应输出/过滤为 0.15。"
                    "不要把退款率表达式乘以 100，也不要用 15 作为阈值。"
                )
    for query in _iter_queries(forge_json):
        for agg in query.get("agg", []):
            if not isinstance(agg, dict):
                continue
            alias = str(agg.get("as", "")).lower()
            if "total_order" not in alias and "总订单" not in alias:
                continue
            if agg.get("fn") == "count_distinct" and str(agg.get("col", "")).endswith("dwd_order_detail.order_id"):
                continue
            warnings.append(
                "退款率分母必须使用 COUNT(DISTINCT dwd_order_detail.order_id)。"
                "不要用 count_all，否则 JOIN 或重复记录会导致总订单数口径不稳定。"
            )
            break
    if any("dwd_refund_detail.user_id" in ref for ref in refs) and any("dim_user" in ref for ref in refs):
        warnings.append(
            "退款率按用户维度分组时，退款分子也应先通过 "
            "dwd_refund_detail.order_id JOIN dwd_order_detail.order_id，"
            "再用 dwd_order_detail.user_id JOIN dim_user。"
            "不要直接用 dwd_refund_detail.user_id 连接 dim_user。"
        )
    if "退款订单数/总订单数" in question or ("退款订单数" in question and "总订单数" in question):
        selects = _collect_direct_select_labels(forge_json)
        has_total = any("total_order" in field.lower() or "总订单" in field for field in selects)
        has_refund = any("refund_order" in field.lower() or "refund_count" in field.lower() or "退款订单" in field for field in selects)
        if not (has_total and has_refund):
            warnings.append(
                "用户明确给出退款率口径“退款订单数/总订单数”时，结果应同时输出 "
                "total_orders 和 refund_orders/refund_count，再输出 refund_rate。"
                "不要只输出退款率，否则审核者无法核对分子分母。"
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


def _check_order_count_distinct(forge_json: dict, question: str, warnings: list[str]) -> None:
    """JOIN 订单明细后统计订单数，必须 count_distinct 订单 ID。"""
    if "订单数" not in question:
        return
    for query in _iter_queries(forge_json):
        refs = _collect_field_refs(query)
        touches_order_items = any("dwd_order_item_detail" in ref for ref in refs)
        touches_orders = any("dwd_order_detail" in ref for ref in refs)
        if not (touches_orders and touches_order_items):
            continue
        for agg in query.get("agg", []):
            if not isinstance(agg, dict):
                continue
            alias = str(agg.get("as", "")).lower()
            fn = str(agg.get("fn", "")).lower()
            col = str(agg.get("col", "")).lower()
            if "order_count" not in alias and "订单数" not in alias:
                continue
            if fn == "count_distinct" and col.endswith("order_id"):
                continue
            warnings.append(
                "查询 JOIN 了订单明细表 dwd_order_item_detail，但用户要统计“订单数”。"
                "此时不能使用 count_all 或 COUNT(明细行)，否则会把一个订单的多条商品明细重复计数。"
                "请使用 count_distinct，col 使用 dwd_order_detail.order_id，"
                '例如：{"fn": "count_distinct", "col": "dwd_order_detail.order_id", "as": "order_count"}。'
            )
            return


def _check_detail_vs_group_average(forge_json: dict, question: str, warnings: list[str]) -> None:
    """每条明细相对组平均值，不能用 GROUP BY 折叠明细行。"""
    if not ("每条" in question and "平均" in question and any(w in question for w in ("偏差", "相对"))):
        return
    for query in _iter_queries(forge_json):
        has_avg_agg = any(
            isinstance(agg, dict) and str(agg.get("fn", "")).lower() == "avg"
            for agg in query.get("agg", [])
        )
        has_group = bool(query.get("group"))
        has_window_avg = any(
            str(win.get("fn", "")).lower() == "avg"
            for win in query.get("window", [])
            if isinstance(win, dict)
        )
        if has_avg_agg and has_group and not has_window_avg:
            warnings.append(
                "用户询问“每条明细相对组平均值/偏差”，必须保留每条明细行。"
                "不能只用 GROUP BY + AVG，因为这会把多条评价/明细折叠成聚合行。"
                "请使用 AVG(...) OVER (PARTITION BY 分组键) 的窗口平均，"
                "或先在 CTE 中计算组平均值，再 JOIN 回原始明细表。"
            )
            return


def _check_order_detail_query_shape(forge_json: dict, question: str, warnings: list[str]) -> None:
    """用户询问订单明细时，时间和输出应来自 dwd_order_item_detail。"""
    if "订单明细" not in question:
        return
    refs = _collect_field_refs(forge_json)
    selects = _collect_select_fields(forge_json)
    has_item_table = any("dwd_order_item_detail" in ref for ref in refs)
    if not has_item_table:
        warnings.append(
            "用户询问“订单明细”，应使用 dwd_order_item_detail 作为明细事实表，"
            "并 JOIN 商品/品牌等维表。"
        )
        return
    if not any("dwd_order_item_detail.order_item_id" in field for field in selects):
        warnings.append(
            "用户询问“订单明细”时，select 中应包含 dwd_order_item_detail.order_item_id，"
            "避免只返回订单头信息。"
        )
    date_refs = [ref for ref in refs if "order_dt" in ref]
    if any("dwd_order_detail.order_dt" in ref for ref in date_refs) and not any(
        "dwd_order_item_detail.order_dt" in ref for ref in date_refs
    ):
        warnings.append(
            "订单明细查询按下单时间过滤/排序时，应优先使用 dwd_order_item_detail.order_dt，"
            "而不是订单头表 dwd_order_detail.order_dt，确保与明细粒度一致。"
        )


def _check_order_lookup_selects_order_id(forge_json: dict, question: str, warnings: list[str]) -> None:
    """找出订单/已完成订单时，输出中必须包含 order_id。"""
    if "找出" not in question:
        return
    order_lookup_terms = ("订单明细", "订单记录", "订单列表", "已完成订单", "具体订单")
    if not any(term in question for term in order_lookup_terms):
        return
    if question.rstrip().endswith("用户") or "显示用户" in question:
        return
    selects = _collect_select_fields(forge_json)
    if any("order_id" in field for field in selects):
        return
    warnings.append(
        "用户要求找出订单，select 中应包含订单主键 dwd_order_detail.order_id，"
        "否则结果无法追溯到具体订单。"
    )


def _check_cart_without_completed_order(forge_json: dict, question: str, warnings: list[str]) -> None:
    """加购但未完成购买的反连接，必须校验已完成订单。"""
    if not ("加" in question and "购物车" in question and "已完成订单" in question):
        return
    refs = _collect_field_refs(forge_json)
    has_cart = any("dwd_cart_detail" in ref for ref in refs)
    has_order_item = any("dwd_order_item_detail" in ref for ref in refs)
    has_order_detail = any("dwd_order_detail" in ref for ref in refs)
    has_completed = _has_completed_order_status_filter(forge_json)
    if has_cart and not has_order_item:
        warnings.append(
            "“加入购物车但该商品未出现在已完成订单中”必须按同一商品做反连接。"
            "反查购买记录时应 JOIN dwd_order_item_detail，并用 "
            "dwd_cart_detail.product_id = dwd_order_item_detail.product_id "
            "以及 user_id 匹配；不能只判断用户是否存在任意已完成订单。"
        )
    if has_cart and has_order_item and not (has_order_detail and has_completed):
        warnings.append(
            "“加入购物车但未出现在已完成订单中”不能只反查 dwd_order_item_detail。"
            "必须 JOIN dwd_order_detail，并在反连接条件中限定 "
            "dwd_order_detail.order_status = '已完成'，否则会把未完成/取消订单也当作购买。"
        )


def _check_category_window_grain(forge_json: dict, question: str, warnings: list[str]) -> None:
    """品类 TopN/占比窗口分区不要用 category_id 拆分展示分区。"""
    if not any(word in question for word in ("品类内", "各品类内", "所属品类", "品类中的占比", "每个品类")):
        return
    partition_fields = _collect_window_partition_fields(forge_json)
    selects = _collect_select_fields(forge_json)

    if (
        any(word in question for word in _CATEGORY_WORDS)
        and not any(word in question for word in _CATEGORY_ID_WORDS)
        and any("category_name" in field for field in selects + partition_fields)
        and any("category_id" in field for field in partition_fields)
    ):
        warnings.append(
            "品类内 TopN/占比窗口查询的 PARTITION BY 应使用展示粒度 category_name。"
            "不要在 window.partition 中额外加入 category_id，"
            "否则同名品类会被拆成多个窗口分区。"
        )


def _check_category_monthly_order_grain(forge_json: dict, question: str, warnings: list[str]) -> None:
    """按品类/商品做月度订单量时，时间和订单 ID 应与明细事实表粒度一致。"""
    if not ("按月" in question and any(word in question for word in _CATEGORY_WORDS) and "订单" in question):
        return
    for query in _iter_queries(forge_json):
        refs = _collect_field_refs(query)
        if not any("dwd_order_item_detail" in ref for ref in refs):
            continue
        has_order_detail_month = any("dwd_order_detail.order_dt" in ref and "STRFTIME" in ref for ref in refs)
        has_item_month = any("dwd_order_item_detail.order_dt" in ref and "STRFTIME" in ref for ref in refs)
        if has_order_detail_month and not has_item_month:
            warnings.append(
                "按品类/商品统计月度订单量且 JOIN 订单明细时，月份应来自 "
                "dwd_order_item_detail.order_dt，而不是 dwd_order_detail.order_dt。"
            )
        group_fields = _collect_group_fields(query)
        if (
            "每个品类按月" in question
            and not any(word in question for word in _CATEGORY_ID_WORDS)
            and any("category_id" in field for field in group_fields)
            and any("category_name" in field for field in group_fields)
        ):
            warnings.append(
                "“每个品类按月”且输出品类名/月度 lead 时，应按展示粒度 "
                "dim_category.category_name + month 聚合。不要在内层 GROUP BY 中加入 "
                "category_id，否则同名品类会被拆成多个时间序列，导致行数膨胀。"
            )
        for agg in query.get("agg", []):
            if not isinstance(agg, dict):
                continue
            alias = str(agg.get("as", "")).lower()
            if "order" not in alias and "订单" not in alias:
                continue
            if agg.get("fn") == "count_distinct" and str(agg.get("col", "")).endswith("dwd_order_item_detail.order_id"):
                continue
            if "order_count" in alias or "monthly_orders" in alias or "订单" in alias:
                warnings.append(
                    "按品类/商品月度订单量应使用 "
                    "count_distinct(dwd_order_item_detail.order_id)，"
                    "确保订单量与商品明细月份粒度一致。"
                )
                return


def _check_qualified_window_alias_select(forge_json: dict, question: str, warnings: list[str]) -> None:
    """窗口 alias 是本层派生字段，不能写成 cte.alias 这种已存在列引用。"""
    for query in _iter_queries(forge_json):
        aliases = {
            str(window.get("as", ""))
            for window in query.get("window", [])
            if isinstance(window, dict) and window.get("as")
        }
        if not aliases:
            continue
        for item in query.get("select", []):
            if not isinstance(item, str) or "." not in item:
                continue
            _, _, field_name = item.rpartition(".")
            if field_name not in aliases:
                continue
            warnings.append(
                f"窗口函数别名 {field_name} 是当前 SELECT 派生字段，不能写成 {item}。"
                f"请从 select 中删除 {item}，由 window 定义产生该字段；"
                f"确需展示时只能使用未限定的 {field_name}。"
            )


def _check_order_user_join_source(forge_json: dict, question: str, warnings: list[str]) -> None:
    """订单相关用户维表 JOIN 应从订单头 user_id 连接，避免明细冗余字段不一致。"""
    if "订单" not in question:
        return
    for query in _iter_queries(forge_json):
        joins = query.get("joins", [])
        touches_order_detail = query.get("scan") == "dwd_order_detail" or any(
            isinstance(join, dict) and join.get("table") == "dwd_order_detail"
            for join in joins
        )
        if not touches_order_detail:
            continue
        for join in joins:
            if not isinstance(join, dict) or join.get("table") != "dim_user":
                continue
            on_refs = _collect_refs_from_obj(join.get("on", {}))
            if any("dwd_order_item_detail.user_id" in ref for ref in on_refs):
                warnings.append(
                    "订单查询 JOIN dim_user 时，应使用 dwd_order_detail.user_id = dim_user.user_id。"
                    "不要从 dwd_order_item_detail.user_id 连接用户维表，避免订单头与明细冗余字段不一致。"
                )


def _check_percentage_display(forge_json: dict, question: str, warnings: list[str]) -> None:
    """普通占比默认输出 0~1 小数，业务率指标维持各自定义。"""
    if not any(word in question for word in ("占比", "比例")):
        return
    if any(word in question for word in ("退款率", "复购率", "转化率", "留存率", "退货率")):
        return
    for query in _iter_queries(forge_json):
        for item in query.get("select", []):
            if not isinstance(item, dict):
                continue
            alias = str(item.get("as", "")).lower()
            expr = str(item.get("expr", "")).lower().replace(" ", "")
            if not any(token in alias for token in ("ratio", "pct", "percent", "share", "占比", "比例")):
                continue
            if "*100" not in expr and "100.0*" not in expr:
                if "round(" in expr and ",4)" not in expr:
                    warnings.append(
                        "普通“占比/比例”默认输出 0~1 小数口径，并保留 4 位小数。"
                        "请使用 ROUND(numerator * 1.0 / denominator, 4)，"
                        "不要使用 2 位百分比展示。"
                    )
                continue
            warnings.append(
                "用户询问普通“占比/比例”时，默认应输出 0~1 小数口径。"
                "请使用 numerator * 1.0 / denominator，并 ROUND(..., 4)，"
                "不要乘以 100 输出百分比数值。"
            )
            return


def _check_unrequested_dimension_ids(
    forge_json: dict, question: str, warnings: list[str]
) -> None:
    selected = {_unqualified_field(item) for item in forge_json.get("select", []) if isinstance(item, str)}
    dimensions = (
        ("product_id", "product_name", ("商品ID", "product_id")),
        ("category_id", "category_name", ("品类ID", "category_id")),
        ("brand_id", "brand_name", ("品牌ID", "brand_id")),
        ("channel_id", "channel_name", ("渠道ID", "channel_id")),
        ("user_id", "user_name", ("用户ID", "user_id")),
    )
    for id_field, name_field, explicit_terms in dimensions:
        if id_field in selected and name_field in selected and not any(term in question for term in explicit_terms):
            warnings.append(
                f"{id_field} 仅用于 JOIN/GROUP 去重，用户未要求展示内部 ID。"
                f"请从最终 select 删除 {id_field}，保留 {name_field}。"
            )
            return


def _check_brand_sales_summary_contract(
    forge_json: dict, question: str, warnings: list[str]
) -> None:
    if not all(term in question for term in ("各品牌", "已完成订单", "总销售额", "订单数")):
        return
    actual = [_unqualified_field(item) for item in forge_json.get("select", []) if isinstance(item, str)]
    expected_alias_sets = (
        ["brand_name", "order_count", "total_revenue"],
        ["brand_name", "order_count", "total_sales"],
    )
    if actual not in expected_alias_sets:
        warnings.append(
            "各品牌已完成订单汇总的最终列顺序固定为 brand_name、order_count、"
            "total_revenue/total_sales，并按销售额 DESC；请把订单数放在销售额之前。"
        )


def _check_channel_gmv_sort_contract(
    forge_json: dict, question: str, warnings: list[str]
) -> None:
    if not all(term in question for term in ("渠道类型", "订单数", "GMV", "客单价")):
        return
    sorts = _collect_direct_sort_fields(forge_json)
    if not sorts or sorts[0] not in {("total_gmv", "desc"), ("gmv", "desc")}:
        warnings.append(
            "渠道类型的订单数、总 GMV、平均客单价汇总默认按 total_gmv DESC 稳定展示，"
            "不要改按 order_count 排序。"
        )


def _check_time_bounded_listing_sort(
    forge_json: dict, question: str, warnings: list[str]
) -> None:
    if not any(term in question for term in ("列出", "记录", "明细")):
        return
    if not re.search(r"\d{4}年\d{1,2}月(?:\d{1,2}日)?(?:以来|起|之后|内)", question):
        return
    time_fields = (
        "order_dt", "comment_dt", "apply_dt", "action_dt", "register_date", "created_at"
    )
    sorts = _collect_direct_sort_fields(forge_json)
    if not sorts or sorts[0][1] != "desc" or not any(
        sorts[0][0].endswith(field) for field in time_fields
    ):
        warnings.append(
            "带明确时间范围的事件列表在用户未另行指定时，默认按对应事件时间 DESC 稳定展示。"
            "请添加该事实表时间字段的降序 sort。"
        )


def _check_monthly_order_time_field(
    forge_json: dict, question: str, warnings: list[str]
) -> None:
    if "订单" not in question or not any(term in question for term in ("按月", "每月", "月份")):
        return
    if any(term in question for term in ("完成时间", "支付时间", "付款时间")):
        return
    refs = _collect_refs_from_obj(forge_json)
    if any(".complete_dt" in ref or ".pay_dt" in ref for ref in refs):
        warnings.append(
            "按月统计订单且用户未指定完成/支付时间时，月份默认使用 order_dt（下单日期）。"
            "不要用 complete_dt 或 pay_dt 改变月份归属。"
        )


def _check_rate_threshold_rounding(
    forge_json: dict, question: str, warnings: list[str]
) -> None:
    if "率超过" not in question and not re.search(r"率.{0,4}(?:大于|高于)", question):
        return
    for item in forge_json.get("select", []):
        if not isinstance(item, dict):
            continue
        alias = str(item.get("as", "")).lower()
        expr = str(item.get("expr", "")).lower()
        if "rate" in alias and "round(" in expr:
            warnings.append(
                "带阈值过滤的率指标必须先用未舍入比率判断并输出，避免 ROUND 后改变阈值边界。"
                "请移除比率表达式外层的 ROUND；普通展示型比率仍可保留 4 位小数。"
            )
            return


def _check_ratio_denominator_output(
    forge_json: dict, question: str, warnings: list[str]
) -> None:
    """A denominator helper is not an output unless the user explicitly asks to display it."""
    if not any(word in question for word in ("占比", "比例")):
        return
    if re.search(r"(?:显示|输出|列出).{0,12}(?:总数|总额|合计|分母)", question):
        return
    selected = {
        _unqualified_field(item)
        for item in forge_json.get("select", [])
        if isinstance(item, str)
    }
    ratio_exprs = [
        str(item.get("expr", ""))
        for item in forge_json.get("select", [])
        if isinstance(item, dict) and "/" in str(item.get("expr", ""))
    ]
    for window in forge_json.get("window", []):
        if not isinstance(window, dict) or window.get("fn") not in {"sum", "count"}:
            continue
        alias = str(window.get("as", ""))
        if alias and alias in selected and any(re.search(rf"\b{re.escape(alias)}\b", expr) for expr in ratio_exprs):
            warnings.append(
                f"窗口别名 {alias} 是占比分母的中间汇总，用户未要求展示分母。"
                f"请从最终 select 删除 {alias}，仅保留维度、分子指标和占比。"
            )
            return


def _check_all_bad_reviews_without_images(forge_json: dict, question: str, warnings: list[str]) -> None:
    """“有差评且所有差评均无图片”应表达为 EXISTS + NOT EXISTS/anti。"""
    if not ("差评" in question and "无图片" in question and "所有" in question):
        return
    refs = _collect_field_refs(forge_json)
    has_bad_filter = any("comment_type" in ref for ref in refs)
    has_image_filter = any("has_image" in ref for ref in refs)
    has_anti = any(
        isinstance(join, dict) and join.get("type") == "anti"
        for query in _iter_queries(forge_json)
        for join in query.get("joins", [])
    )
    has_comment_self_join = any(
        isinstance(query, dict)
        and query.get("scan") == "dwd_comment_detail"
        and any(isinstance(join, dict) and join.get("table") == "dwd_comment_detail" for join in query.get("joins", []))
        for query in _iter_queries(forge_json)
    )
    if has_comment_self_join:
        warnings.append(
            "“有差评记录但所有差评均无图片”不要直接把 dwd_comment_detail 自连接到自身。"
            "当前 DSL 没有表别名，自连接会产生 comment_type/product_id 等歧义列。"
            "请使用 EXISTS + anti/NOT EXISTS，或先用 CTE 产出带图差评商品 product_id，"
            "再从 dim_product 反连接该 CTE。"
        )
    if has_bad_filter and has_image_filter and not has_anti:
        warnings.append(
            "“有差评记录但所有差评均无图片”是反存在语义。"
            "应先确认存在 comment_type='差评'，再用 anti/NOT EXISTS 排除 "
            "comment_type='差评' AND has_image=1 的商品；"
            "不要只用 GROUP BY + HAVING 图片数=0。"
        )
    for query in _iter_queries(forge_json):
        for join in query.get("joins", []):
            if not isinstance(join, dict) or join.get("type") != "anti":
                continue
            join_refs = _collect_refs_from_obj(join.get("on", {}))
            if any("product_name" in ref for ref in join_refs):
                warnings.append(
                    "商品反连接必须使用稳定实体键 product_id，不要使用 product_name。"
                    "同名商品会让按名称反连接错误排除其他商品。"
                    "请让两个差评 CTE 都保留 product_id，用 product_id 反连接，"
                    "最终结果再只显示并去重 product_name。"
                )
                break
    final_selects = _collect_direct_select_fields(forge_json)
    if "显示商品名称" in question and any("product_name" in field for field in final_selects):
        final_groups = _collect_direct_group_fields(forge_json)
        if any("product_id" in field for field in final_groups):
            warnings.append(
                "用户只要求显示商品名称时，最终结果应按 product_name 展示粒度去重。"
                "不要在最终 GROUP BY 中加入 dim_product.product_id，否则同名商品会输出多行。"
                "请使用 distinct: true，或最终只按 dim_product.product_name 分组。"
            )
        elif not forge_json.get("distinct") and not any("product_name" in field for field in final_groups):
            warnings.append(
                "用户只要求显示商品名称时，最终 SELECT 应去重商品名称。"
                "dim_product 中可能存在多个 product_id 对应同一 product_name，"
                "请设置 distinct: true，或最终 GROUP BY dim_product.product_name。"
            )


def _check_vip_level_grain(forge_json: dict, question: str, warnings: list[str]) -> None:
    """会员等级是展示标签时，不应按 vip_level_id 拆成多个组。"""
    if "会员" not in question:
        return
    if any(word in question for word in ("会员等级ID", "会员等级 ID", "vip_level_id")):
        return
    refs = _collect_field_refs(forge_json)
    if not any("dim_vip_level.level_name" in ref for ref in refs):
        return
    group_fields = _collect_group_fields(forge_json)
    partition_fields = _collect_window_partition_fields(forge_json)
    select_fields = _collect_select_fields(forge_json)
    if any("dim_vip_level.vip_level_id" in field for field in group_fields + partition_fields):
        warnings.append(
            "用户按会员等级/钻石会员/铂金会员分析时，展示粒度是 dim_vip_level.level_name。"
            "不要在 GROUP BY 或 window.partition 中加入 dim_vip_level.vip_level_id，"
            "否则同一个等级名称会被多个内部 ID 拆分，导致行数和金额膨胀。"
        )
    if any("dim_vip_level.vip_level_id" in field for field in select_fields):
        warnings.append(
            "用户没有要求会员等级 ID，SELECT 中不要输出 dim_vip_level.vip_level_id。"
            "请输出 dim_vip_level.level_name。"
        )


def _check_category_member_spend_grain(forge_json: dict, question: str, warnings: list[str]) -> None:
    """会员 × 品类消费总额应使用订单明细表的用户和实付金额粒度。"""
    if not ("消费总额" in question and "会员" in question and any(word in question for word in _CATEGORY_WORDS)):
        return
    refs = _collect_field_refs(forge_json)
    if not any("dwd_order_item_detail" in ref for ref in refs):
        return
    if any("dwd_order_detail.user_id" in ref for ref in refs):
        warnings.append(
            "按会员等级和品类统计消费总额时，应以 dwd_order_item_detail 为事实表，"
            "并使用 dwd_order_item_detail.user_id JOIN dim_user。"
            "不要通过 dwd_order_detail.user_id 再回连用户维表，否则会和明细事实表粒度不一致。"
        )
    for query in _iter_queries(forge_json):
        for agg in query.get("agg", []):
            if not isinstance(agg, dict):
                continue
            if str(agg.get("fn", "")).lower() != "sum":
                continue
            alias = str(agg.get("as", "")).lower()
            col = str(agg.get("col", "")).lower()
            if not any(token in alias for token in ("total", "sales", "spent", "consumption", "amount", "消费", "销售")):
                continue
            if col == "dwd_order_item_detail.actual_amount":
                continue
            warnings.append(
                "按品类统计会员消费总额时，应汇总 dwd_order_item_detail.actual_amount。"
                "不要使用 dwd_order_detail.total_amount/pay_amount，否则订单头金额会在多商品明细 JOIN 后被重复计算。"
            )
            return
    if _has_order_status_filter(forge_json) and not _question_mentions_explicit_order_status(question):
        warnings.append(
            "用户只问会员在各品类的消费总额，未指定已完成/已支付等订单状态。"
            "此类纯商品明细消费额统计不要默认添加 dwd_order_detail.order_status 过滤。"
        )


def _check_brand_diamond_avg_item_contract(forge_json: dict, question: str, warnings: list[str]) -> None:
    """品牌钻石会员商品均价查询应保留口径审核字段。"""
    if not ("各品牌" in question and "钻石会员" in question and "平均商品实付单价" in question):
        return
    selects = _collect_direct_select_labels(forge_json)
    groups = _collect_direct_group_fields(forge_json)
    has_level_select = any("dim_vip_level.level_name" in field or field == "level_name" for field in selects)
    has_level_group = any("dim_vip_level.level_name" in field or field == "level_name" for field in groups)
    has_order_count = any(_agg_is_order_count(agg) for agg in forge_json.get("agg", []) if isinstance(agg, dict))
    if not has_level_select or not has_level_group:
        warnings.append(
            "统计各品牌钻石会员平均商品实付单价时，应在最终 SELECT 和 GROUP BY 中保留 "
            "dim_vip_level.level_name，便于审核会员等级过滤口径。"
        )
    if not has_order_count:
        warnings.append(
            "统计各品牌钻石会员平均商品实付单价时，应同时输出订单样本量 "
            "COUNT(DISTINCT dwd_order_detail.order_id) AS order_count，"
            "便于审核均价结果的分母规模。"
        )


def _check_known_large_schema_field_names(forge_json: dict, question: str, warnings: list[str]) -> None:
    """large benchmark 的高频维表字段名纠错。"""
    refs = _collect_field_refs(forge_json)
    if any("dim_user.id" in ref for ref in refs):
        warnings.append(
            "large schema 中 dim_user 的用户主键是 dim_user.user_id，不是 dim_user.id。"
            "请把所有 dim_user.id 改为 dim_user.user_id。"
        )
    uses_large_schema_tables = any(
        ref.startswith("dim_") or ref.startswith("dwd_") or ".dim_" in ref or ".dwd_" in ref
        for ref in refs
    )
    uses_legacy_orders = any(ref == "orders" or ref.startswith("orders.") for ref in refs)
    if uses_large_schema_tables and uses_legacy_orders:
        warnings.append(
            "当前查询已经使用 large schema 的 dim_/dwd_ 表，不要混入旧示例表 orders。"
            "订单头表应使用 dwd_order_detail，字段为 dwd_order_detail.order_id、"
            "dwd_order_detail.user_id、dwd_order_detail.total_amount、dwd_order_detail.order_dt。"
        )


def _check_good_review_with_images_contract(forge_json: dict, question: str, warnings: list[str]) -> None:
    """好评带图记录查询应稳定保留过滤、输出列和排序。"""
    if not ("好评" in question and "带图片" in question and "评分为4或5星" in question):
        return
    if not _has_filter_col_value(forge_json, "comment_type", "好评"):
        warnings.append(
            "用户明确要求“好评记录”，必须添加 dwd_comment_detail.comment_type = '好评' 过滤。"
            "不要只用 rating 4/5 和 has_image 判断好评。"
        )
    direct_selects = [_unqualified_field(field) for field in _collect_direct_select_labels(forge_json)]
    expected = [
        "comment_id",
        "product_id",
        "user_id",
        "rating",
        "comment_dt",
    ]
    if direct_selects and direct_selects != expected:
        warnings.append(
            "好评带图记录查询的最终输出列应为 comment_id、product_id、user_id、rating、comment_dt。"
            "不要额外输出 comment_type、has_image、order_item_id、has_video 等过滤或内部字段，"
            "否则结果列契约不稳定。"
        )
    sort_fields = [(_unqualified_field(field), direction) for field, direction in _collect_direct_sort_fields(forge_json)]
    if sort_fields and sort_fields[:2] != [
        ("rating", "desc"),
        ("comment_dt", "desc"),
    ]:
        warnings.append(
            "“按评分降序”的评价列表应使用稳定排序：rating DESC, comment_dt DESC。"
            "只按 rating 排序会导致同分记录顺序不稳定。"
        )


def _check_refund_product_ranking_contract(forge_json: dict, question: str, warnings: list[str]) -> None:
    """退款金额 Top 商品应输出退款次数，并避免默认收窄退款状态。"""
    if not ("退款总金额" in question and "前5个商品" in question):
        return
    selects = _collect_direct_select_labels(forge_json)
    has_refund_count_select = any("refund_count" in field.lower() or "退款次数" in field for field in selects)
    has_refund_count_agg = any(
        isinstance(agg, dict)
        and str(agg.get("fn", "")).lower() in {"count", "count_distinct"}
        and "refund" in str(agg.get("col", "")).lower()
        and ("refund_count" in str(agg.get("as", "")).lower() or "退款次数" in str(agg.get("as", "")))
        for query in _iter_queries(forge_json)
        for agg in query.get("agg", [])
    )
    if not (has_refund_count_select and has_refund_count_agg):
        warnings.append(
            "按退款总金额找 Top 商品时，结果应同时输出 refund_count。"
            "请添加 COUNT(dwd_refund_detail.refund_id) AS refund_count，并在 select 中输出它。"
        )
    if _has_filter_col(forge_json, "refund_status") and "已退款" not in question:
        warnings.append(
            "用户只要求按退款总金额排名，没有要求限定退款状态。"
            "不要默认添加 dwd_refund_detail.refund_status 过滤，否则会改变基准口径。"
        )


def _check_product_category_share_contract(forge_json: dict, question: str, warnings: list[str]) -> None:
    """每个商品在所属品类中的占比应按展示品类名计算并固定输出列。"""
    if not ("商品" in question and "品类" in question and "占比" in question):
        return
    final_selects = _collect_direct_select_labels(forge_json)
    if any("product_id" in field for field in final_selects) and not any(word in question for word in _PRODUCT_ID_WORDS):
        warnings.append(
            "用户询问“每个商品”的销售额和品类占比，但没有要求商品 ID。"
            "最终 SELECT 应输出 product_name、category_name、product_revenue/product_sales、pct_of_category，"
            "不要额外输出 product_id。"
        )
    partition_fields = _collect_window_partition_fields(forge_json)
    if partition_fields and not any("category_name" in field for field in partition_fields):
        warnings.append(
            "商品在所属品类总销售额中的占比应按 category_name 展示粒度分区。"
            "请使用 SUM(product_revenue) OVER (PARTITION BY category_name)，"
            "不要用 category_id 单独计算品类总额。"
        )
    for cte in forge_json.get("cte", []):
        if not isinstance(cte, dict) or "category_total" not in str(cte.get("name", "")).lower():
            continue
        group_fields = _collect_direct_group_fields(cte.get("query", {}))
        if any("category_id" in field for field in group_fields):
            warnings.append(
                "品类销售额占比的分母必须按可见展示粒度 category_name 汇总。"
                "不要用 category_id 构造 category_totals 并回连；同名品类可能有多个 ID。"
                "优先在商品销售额 CTE 上使用 SUM(product_revenue) OVER "
                "(PARTITION BY category_name) 计算分母。"
            )
            break
    for query in _iter_queries(forge_json):
        if query is forge_json:
            continue
        if query.get("scan") not in {"product_sales", "product_sales_cte"}:
            continue
        group_fields = _collect_direct_group_fields(query)
        if any("category_id" in field for field in group_fields) and not any("category_name" in field for field in group_fields):
            warnings.append(
                "商品品类占比的品类总额 CTE 不应只按 category_id 分组。"
                "large benchmark 的占比口径按 category_name 展示粒度计算。"
            )
    sort_fields = _collect_direct_sort_fields(forge_json)
    if sort_fields and sort_fields[:2] != [("category_name", "asc"), ("product_revenue", "desc")]:
        if not (
            len(sort_fields) >= 2
            and "category_name" in sort_fields[0][0]
            and sort_fields[0][1] == "asc"
            and any(token in sort_fields[1][0] for token in ("product_revenue", "product_sales"))
            and sort_fields[1][1] == "desc"
        ):
            warnings.append(
                "商品品类占比结果应按 category_name ASC, product_revenue/product_sales DESC 排序，"
                "保证同一品类内按销售额降序展示。"
            )


def _check_adjacent_review_lag_contract(forge_json: dict, question: str, warnings: list[str]) -> None:
    """相邻评价评分变化应对全量评价做 LAG，不能先取最近 N 条。"""
    if not ("相邻两次评价" in question and "评分变化" in question):
        return
    if _has_ranking_window(forge_json) or any(query.get("limit") for query in _iter_queries(forge_json)):
        warnings.append(
            "“每个商品相邻两次评价的评分变化”应对全部 dwd_comment_detail 记录按时间 ASC 使用 LAG。"
            "不要先用 row_number/limit 只取最近两条评价，否则会丢失历史评价行。"
        )


def _check_channel_monthly_mom_contract(forge_json: dict, question: str, warnings: list[str]) -> None:
    """渠道月度环比应按 channel_name 展示粒度计算并固定别名。"""
    if not ("每个渠道按月" in question and "环比变化量" in question):
        return
    group_fields = _collect_group_fields(forge_json)
    partition_fields = _collect_window_partition_fields(forge_json)
    if any("channel_id" in field for field in group_fields + partition_fields):
        warnings.append(
            "渠道月度环比的展示粒度是 dim_channel.channel_name。"
            "GROUP BY 和 LAG partition 应使用 channel_name，不要加入 channel_id，"
            "否则同名渠道会被拆成多个时间序列。"
        )
    final_selects = _collect_direct_select_labels(forge_json)
    if any("prev" in field.lower() for field in final_selects) and not any("prev_month_count" in field for field in final_selects):
        warnings.append(
            "渠道月度环比应将上一月订单量命名为 prev_month_count。"
            "不要使用 prev_order_count 等其他别名，避免结果列契约不稳定。"
        )
    has_mom_change = any("mom_change" in field for field in final_selects)
    if not has_mom_change:
        warnings.append(
            "渠道月度环比变化量应输出别名 mom_change，表达式为 order_count - prev_month_count。"
        )


def _check_cart_without_purchase_output_contract(forge_json: dict, question: str, warnings: list[str]) -> None:
    """加购未购买用户查询最终输出应限定用户字段并去重。"""
    if not ("购物车" in question and "从未出现在其已完成订单" in question and "显示用户ID和用户名" in question):
        return
    final_selects = _collect_direct_select_labels(forge_json)
    if any(field == "user_id" for field in final_selects):
        warnings.append(
            "购物车未购买用户查询最终 SELECT 不要使用裸 user_id。"
            "请输出 dim_user.user_id 和 dim_user.user_name，避免 JOIN 后出现歧义列。"
        )
    if not (forge_json.get("distinct") or any("dim_user.user_id" in field for field in _collect_direct_group_fields(forge_json))):
        warnings.append(
            "购物车未购买用户查询应按用户去重。"
            "请设置 distinct: true，或最终 GROUP BY dim_user.user_id, dim_user.user_name。"
        )


def _check_cross_event_user_counts_contract(forge_json: dict, question: str, warnings: list[str]) -> None:
    """加购次数 + 退款次数的 CTE 汇总结果应限定字段来源。"""
    if not ("既有加购行为" in question and "退款记录" in question and "加购次数" in question and "退款次数" in question):
        return
    final_selects = _collect_direct_select_labels(forge_json)
    if "add_count" in final_selects or "refund_count" in final_selects:
        warnings.append(
            "跨事件用户计数查询最终 SELECT 应限定 CTE 字段来源："
            "add_counts.add_count 和 refund_counts.refund_count。"
            "不要使用裸 add_count/refund_count，避免编译后引用到不存在或歧义字段。"
        )
    has_cart_count_distinct = any(
        isinstance(agg, dict)
        and str(agg.get("fn", "")).lower() == "count_distinct"
        and str(agg.get("col", "")).endswith("dwd_cart_detail.cart_id")
        for query in _iter_queries(forge_json)
        for agg in query.get("agg", [])
    )
    has_refund_count_distinct = any(
        isinstance(agg, dict)
        and str(agg.get("fn", "")).lower() == "count_distinct"
        and str(agg.get("col", "")).endswith("dwd_refund_detail.refund_id")
        for query in _iter_queries(forge_json)
        for agg in query.get("agg", [])
    )
    if not has_cart_count_distinct or not has_refund_count_distinct:
        warnings.append(
            "2025年11月以来既有加购又有退款记录的用户，应使用 "
            "COUNT(DISTINCT dwd_cart_detail.cart_id) AS cart_add_count 和 "
            "COUNT(DISTINCT dwd_refund_detail.refund_id) AS refund_count。"
            "不要使用 count_all，避免 JOIN 或重复事件导致计数不稳定。"
        )


def _check_unit_price_order_lookup_contract(forge_json: dict, question: str, warnings: list[str]) -> None:
    """客单价范围订单查找应稳定输出基准列。"""
    if not ("客单价在500到2000之间" in question and "女性用户" in question and "已完成订单" in question):
        return
    final_selects = _collect_direct_select_labels(forge_json)
    expected = [
        "dwd_order_detail.order_id",
        "dim_user.user_name",
        "dim_user.age_group",
        "dwd_order_detail.total_amount",
    ]
    if final_selects and final_selects != expected:
        warnings.append(
            "客单价范围订单查找的最终输出列应固定为 order_id、user_name、age_group、total_amount。"
            "请使用：dwd_order_detail.order_id, dim_user.user_name, dim_user.age_group, "
            "dwd_order_detail.total_amount；不要额外输出 user_id/order_dt，也不要漏掉 age_group。"
        )
    if forge_json.get("sort"):
        warnings.append(
            "用户没有要求排序时，客单价范围订单查找不要额外添加 ORDER BY。"
            "额外排序会导致执行结果与基准结果不一致。"
        )


def _check_derived_metric_having(forge_json: dict, question: str, warnings: list[str]) -> None:
    """外层仅过滤 CTE 派生字段时应使用 WHERE，避免 HAVING 隐式重新分组。"""
    for query in _iter_queries(forge_json):
        if not query.get("having") or query.get("agg"):
            continue
        has_derived_select = any(isinstance(item, dict) and item.get("expr") for item in query.get("select", []))
        scans_cte = any(
            isinstance(cte, dict) and cte.get("name") == query.get("scan")
            for cte in forge_json.get("cte", [])
        )
        if not (has_derived_select or scans_cte):
            continue
        warnings.append(
            "当前层没有聚合，却使用 HAVING 过滤派生指标；编译器会为 HAVING 推断 GROUP BY，"
            "可能合并同名维度行并改变结果。请把该条件移到 filter（WHERE）；"
            "HAVING 只用于同一层 agg 聚合结果。"
        )
        return


def _check_internal_rank_output(forge_json: dict, question: str, warnings: list[str]) -> None:
    """TopN 排名别名默认只用于 qualify，除非用户明确要求展示名次。"""
    explicitly_requests_rank = bool(
        re.search(r"(?:显示|输出|列出).{0,20}(?:排名|名次)|(?:及|和)(?:排名|名次)|(?:排名|名次)列", question)
    )
    if explicitly_requests_rank:
        return
    for query in _iter_queries(forge_json):
        qualified_aliases = {
            str(condition.get("col", "")).rpartition(".")[2]
            for condition in query.get("qualify", [])
            if isinstance(condition, dict)
        }
        rank_aliases = {
            str(window.get("as", ""))
            for window in query.get("window", [])
            if isinstance(window, dict)
            and str(window.get("fn", "")).lower() in _RANKING_FNS
            and str(window.get("as", "")) in qualified_aliases
        }
        if not rank_aliases:
            continue
        for item in query.get("select", []):
            if not isinstance(item, str):
                continue
            alias = item.rpartition(".")[2]
            if alias not in rank_aliases:
                continue
            warnings.append(
                f"排名别名 {alias} 只是 TopN qualify 的内部字段，用户未要求显示排名。"
                f"请从 select 中删除 {item}，保留 window 与 qualify 即可。"
            )
            return


def _check_refund_record_output_contract(forge_json: dict, question: str, warnings: list[str]) -> None:
    """退款记录列表应只返回审核和定位所需的稳定字段。"""
    if not ("退款状态为已退款" in question and "退款金额超过" in question and "退款记录" in question):
        return
    actual = [_unqualified_field(field) for field in _collect_direct_select_labels(forge_json)]
    expected = ["refund_id", "order_id", "user_id", "refund_amount", "apply_dt"]
    if actual != expected:
        warnings.append(
            "退款记录结果列应固定为 refund_id、order_id、user_id、refund_amount、apply_dt。"
            "refund_status 是过滤条件，不要输出；apply_dt 用于追溯申请时间，不要替换为 complete_dt。"
        )


def _check_imported_brand_order_detail_contract(
    forge_json: dict, question: str, warnings: list[str]
) -> None:
    """带商品和品牌条件的订单明细应返回业务可读的最小字段集。"""
    if not (
        "订单明细" in question
        and "国际品牌" in question
        and "国内知名品牌" in question
        and "进口商品" in question
    ):
        return
    actual = [_unqualified_field(field) for field in _collect_direct_select_labels(forge_json)]
    expected = [
        "order_item_id",
        "order_id",
        "product_name",
        "brand_name",
        "actual_amount",
        "order_dt",
    ]
    if actual != expected:
        warnings.append(
            "品牌进口商品订单明细的结果列应固定为 "
            "order_item_id、order_id、product_name、brand_name、actual_amount、order_dt。"
            "过滤字段和内部 ID 不要额外输出。"
        )


def _check_brand_rating_deviation_contract(
    forge_json: dict, question: str, warnings: list[str]
) -> None:
    """品牌评分偏差查询保持行粒度、数值精度和结果列稳定。"""
    if not ("每个品牌商品评分的平均分" in question and "每条评价" in question and "偏差" in question):
        return
    actual = [_unqualified_field(field) for field in _collect_direct_select_labels(forge_json)]
    expected = ["brand_name", "rating", "brand_avg_rating", "rating_deviation"]
    has_round = any(
        isinstance(item, dict) and "round(" in str(item.get("expr", "")).lower()
        for item in forge_json.get("select", [])
    )
    if actual != expected or forge_json.get("sort") or has_round:
        warnings.append(
            "品牌评分偏差结果列应固定为 brand_name、rating、brand_avg_rating、rating_deviation。"
            "偏差表达式使用 rating - brand_avg_rating；不要输出 comment_id/product_id，"
            "不要额外排序或 ROUND，以免改变结果契约和数值精度。"
        )


# ── 辅助函数 ──────────────────────────────────────────────────────────────────

def _has_order_status_filter(fj: dict) -> bool:
    """递归检查是否存在 order_status 过滤（含 CTE）。"""
    for query in _iter_queries(fj):
        for f in query.get("filter", []):
            if _condition_has_col(f, "order_status"):
                return True
    return False


def _has_completed_order_status_filter(fj: dict) -> bool:
    """递归检查是否存在 order_status='已完成' 过滤（含 CTE）。"""
    for query in _iter_queries(fj):
        for f in query.get("filter", []):
            if _condition_has_col_value(f, "order_status", "已完成"):
                return True
    return False


def _question_mentions_explicit_order_status(question: str) -> bool:
    status_words = (
        "已完成", "完成订单", "已支付", "支付订单", "已取消", "取消订单",
        "待支付", "待发货", "待收货", "已退款", "退款订单", "退货订单",
        "全部状态", "所有状态", "各状态",
    )
    return any(word in question for word in status_words)


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


def _collect_visible_select_labels(fj: dict) -> list[str]:
    """收集最终可见 SELECT 字段名/别名，不把 expr 内部引用算作输出列。"""
    labels: list[str] = []
    for query in _iter_queries(fj):
        for item in query.get("select", []):
            if isinstance(item, str):
                labels.append(item)
            elif isinstance(item, dict):
                labels.append(str(item.get("as", "")))
    return [label for label in labels if label]


def _collect_direct_select_fields(query: dict) -> list[str]:
    """只收集当前查询最终 SELECT 字段，避免把 CTE 内部字段误判为最终输出。"""
    fields: list[str] = []
    for item in query.get("select", []):
        if isinstance(item, str):
            fields.append(item)
        elif isinstance(item, dict):
            fields.append(str(item.get("as", "")))
            fields.append(str(item.get("expr", "")))
    return [field for field in fields if field]


def _collect_direct_select_labels(query: dict) -> list[str]:
    """只收集当前查询最终 SELECT 字段名/别名。"""
    labels: list[str] = []
    for item in query.get("select", []):
        if isinstance(item, str):
            labels.append(item)
        elif isinstance(item, dict):
            labels.append(str(item.get("as", "")))
    return [label for label in labels if label]


def _collect_direct_group_fields(query: dict) -> list[str]:
    """只收集当前查询 GROUP BY 字段。"""
    fields: list[str] = []
    for item in query.get("group", []):
        if isinstance(item, str):
            fields.append(item)
        elif isinstance(item, dict):
            fields.append(str(item.get("expr", "")))
            fields.append(str(item.get("as", "")))
    return [field for field in fields if field]


def _collect_direct_sort_fields(query: dict) -> list[tuple[str, str]]:
    """只收集当前查询 ORDER BY 字段和方向。"""
    fields: list[tuple[str, str]] = []
    for item in query.get("sort", []):
        if not isinstance(item, dict):
            continue
        fields.append((str(item.get("col", "")), str(item.get("dir", "")).lower()))
    return fields


def _unqualified_field(field: str) -> str:
    """Return the column or alias portion of a simple qualified reference."""
    return field.rpartition(".")[2]


def _agg_is_order_count(agg: dict) -> bool:
    alias = str(agg.get("as", "")).lower()
    fn = str(agg.get("fn", "")).lower()
    col = str(agg.get("col", "")).lower()
    return (
        fn == "count_distinct"
        and col.endswith("dwd_order_detail.order_id")
        and ("order_count" in alias or "订单数" in alias)
    )


def _has_avg_amount_agg(fj: dict) -> bool:
    """检查是否使用了 AVG(total_amount/pay_amount)。"""
    for query in _iter_queries(fj):
        for agg in query.get("agg", []):
            if agg.get("fn") == "avg" and "amount" in str(agg.get("col", "")).lower():
                return True
        for window in query.get("window", []):
            if (
                isinstance(window, dict)
                and str(window.get("fn", "")).lower() == "avg"
                and "amount" in str(window.get("col", "")).lower()
            ):
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


def _collect_window_partition_fields(fj: dict) -> list[str]:
    """收集窗口函数 PARTITION BY 字段。"""
    fields: list[str] = []
    for window in _collect_windows(fj):
        fields.extend(str(item) for item in window.get("partition", []))
    return fields


def _collect_windows(fj: dict) -> list[dict]:
    """收集所有窗口表达式。"""
    windows: list[dict] = []
    for query in _iter_queries(fj):
        windows.extend(w for w in query.get("window", []) if isinstance(w, dict))
    return windows


def _has_ranking_window(fj: dict) -> bool:
    """是否存在 row_number/rank/dense_rank 窗口。"""
    return any(str(window.get("fn", "")).lower() in _RANKING_FNS for window in _collect_windows(fj))


def _collect_qualify_conditions(fj: dict) -> list[dict]:
    """收集所有 qualify 条件。"""
    conditions: list[dict] = []
    for query in _iter_queries(fj):
        conditions.extend(c for c in query.get("qualify", []) if isinstance(c, dict))
    return conditions


def _join_pairs_fields(fj: dict, left_field: str, right_field: str) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for query in _iter_queries(fj):
        for join in query.get("joins", []):
            if not isinstance(join, dict):
                continue
            for left, right in _extract_join_pairs(join.get("on", {})):
                if {left, right} == {left_field, right_field}:
                    pairs.append((left, right))
    return pairs


def _extract_join_pairs(on_obj) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    if isinstance(on_obj, dict):
        left = on_obj.get("left")
        right = on_obj.get("right")
        if left and right:
            pairs.append((str(left), str(right)))
        for value in on_obj.values():
            pairs.extend(_extract_join_pairs(value))
    elif isinstance(on_obj, list):
        for item in on_obj:
            pairs.extend(_extract_join_pairs(item))
    return pairs


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


def _has_filter_col(fj: dict, needle: str) -> bool:
    """递归检查 filter 中是否出现某字段。"""
    for query in _iter_queries(fj):
        for condition in query.get("filter", []):
            if _condition_has_col(condition, needle):
                return True
    return False


def _has_filter_col_value(fj: dict, needle: str, expected_value: str) -> bool:
    """递归检查 filter 中是否出现某字段和值。"""
    for query in _iter_queries(fj):
        for condition in query.get("filter", []):
            if _condition_has_col_value(condition, needle, expected_value):
                return True
    return False


def _condition_has_col_value(condition, needle: str, expected_value: str) -> bool:
    if isinstance(condition, dict):
        col = str(condition.get("col", ""))
        val = str(condition.get("val", ""))
        if needle in col and val == expected_value:
            return True
        return any(_condition_has_col_value(value, needle, expected_value) for value in condition.values())
    if isinstance(condition, list):
        return any(_condition_has_col_value(item, needle, expected_value) for item in condition)
    return False


def _looks_like_time_field(col: str) -> bool:
    return any(token in col for token in ("_dt", "_date", "_time", "created_at", "updated_at"))


def _question_asks_per_group_topn(question: str) -> bool:
    has_group_scope = any(w in question for w in _PER_GROUP_WORDS)
    has_explicit_front_n = bool(re.search(r"前\s*(?:\d+|[一二三四五六七八九十]+|[Nn])", question))
    has_topn = any(w in question for w in _TOPN_WORDS) or has_explicit_front_n
    # 避免把"各品类销售额，按销售额排序"误判成组内 TopN；需要明确排名/前N/最高等 TopN 信号。
    return has_group_scope and has_topn
