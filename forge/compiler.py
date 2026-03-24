"""
Forge DSL → SQL 编译器。

职责：接收经过 JSON Schema 校验的 Forge JSON 字典，确定性地输出 SQL 字符串。
编译过程无副作用、无随机性，相同输入永远产生相同输出。

编译流水线（与 SQL 执行语义一致）：
    scan → joins → filter(WHERE) → group(GROUP BY) → agg(SELECT 聚合)
    → having(HAVING) → select(SELECT) → sort(ORDER BY) → limit(LIMIT) → offset(OFFSET)

特殊 join 处理：
    anti join  → 无 filter：LEFT JOIN … WHERE right_key IS NULL
                 有 filter：WHERE NOT EXISTS (SELECT 1 FROM … WHERE … AND filter_conds)
    semi join  → WHERE EXISTS (SELECT 1 FROM … WHERE …) （不产生 JOIN 关键字）

窗口函数（window 字段）：
    在 SELECT 阶段展开为 fn() OVER (PARTITION BY … ORDER BY …) AS alias
    支持三类：排名类（row_number/rank/dense_rank）、聚合类（sum/avg/count/min/max）、
    导航类（lag/lead，支持 offset 和 default 参数）
"""

from __future__ import annotations
import json
import pathlib
import re
from typing import Any

import jsonschema

# 在模块加载时一次性读取并解析 JSON Schema，避免重复 I/O
_SCHEMA_PATH = pathlib.Path(__file__).parent / "schema.json"
_SCHEMA = json.loads(_SCHEMA_PATH.read_text())

# 标准 JOIN 类型到 SQL 关键字的映射；anti/semi/cross 由 _join() 单独处理
_JOIN_KEYWORDS = {
    "inner": "INNER JOIN",
    "left":  "LEFT JOIN",
    "right": "RIGHT JOIN",
    "full":  "FULL OUTER JOIN",
}

# 简单二元比较运算符映射
_OP_SYMBOLS = {
    "eq":  "=",
    "neq": "!=",
    "gt":  ">",
    "gte": ">=",
    "lt":  "<",
    "lte": "<=",
}


# ── 公开入口 ──────────────────────────────────────────────────────────────────

_SUPPORTED_DIALECTS = ("sqlite", "mysql", "postgresql", "bigquery", "snowflake")


def _col_is_nullable(col: str, nullable_cols: frozenset[str]) -> bool:
    """检查列引用是否被标记为可空（支持 table.col 和 col 两种格式）。"""
    return col in nullable_cols or col.split(".", 1)[-1] in nullable_cols


def compile_query(forge: dict, dialect: str = "sqlite",
                  nullable_cols: frozenset[str] | None = None) -> str:
    """
    校验 Forge JSON 并编译为 SQL 字符串。

    Args:
        forge:        符合 Forge DSL 规范的查询字典。
        dialect:      目标 SQL 方言，可选 "sqlite"（默认）、"mysql"、"postgresql"、
                      "bigquery"、"snowflake"。
                      控制日期函数、字符串聚合、JOIN 等方言差异的编译输出。
        nullable_cols: 可空列名集合（支持 "table.col" 或 "col" 格式）。
                      当某列被标记为可空且使用 neq 运算符时，自动展开为
                      (col != val OR col IS NULL)，避免 NULL 被静默排除。

    Returns:
        多行 SQL 字符串，子句间以换行分隔。

    Raises:
        ValueError: Forge JSON 违反 schema 约束，或方言不支持某特性时，抛出人类可读的错误描述。
    """
    if dialect not in _SUPPORTED_DIALECTS:
        raise ValueError(
            f"不支持的方言：'{dialect}'。合法值：{_SUPPORTED_DIALECTS}"
        )
    forge = _coerce(forge)
    try:
        jsonschema.validate(forge, _SCHEMA)
    except jsonschema.ValidationError as exc:
        raise ValueError(_friendly_error(exc)) from exc
    return _compile(forge, dialect, nullable_cols)


def _coerce(q: dict) -> dict:
    """
    在 schema 校验前对常见模型输出偏差做容错修复（不改变查询语义）。

    修复项（仅限格式规范化，不猜测语义意图）：
    1. filter/having 为 dict 时自动包装为列表
    2. between 条件中 val 为 [lo, hi] 数组时拆分为 lo/hi 字段
    3. window default=None 转为 null（保持 JSON 语义）
    4. agg 中 count_all + col 字段共存时，删除多余的 col
    5. group 存在时，select 里的非聚合/非窗口/非 group_expr 别名字段自动补齐到 group
    6. having 中出现 fn 字段（内联聚合表达式）→ 替换为对应 agg alias
    7. qualify 引用的 window 别名未在 select 中时，自动补齐
    8. 顶层 "query" 为 JSON 字符串时解包
    9. select 中字符串含内联别名（"expr AS alias"）→ 转为 expr 对象
    10. 有 agg/group 但缺少 select 时，自动从 group + agg 别名生成 select
    11. 顶层缺少 scan 但有 cte 时，自动推断 scan 为最后一个 CTE 名
    12. 清理 agg items 中的非法字段
    13. select expr 字符串中出现 count_all() → 替换为 COUNT(*)
    14. 当 scan 为 CTE 名时，剥离 table.col 中的 table. 前缀
    15. joins 元素中的 filter 属性 → 提取到顶层 filter
    16. filter/having 中的 expr 条件 → col（由 _coerce_condition 处理）
    17. HAVING 存在但缺少 GROUP BY → 从 select 非聚合列推断
    18. window lag/lead col 为 agg alias → 展开为原始聚合表达式
    19. 顶层 filter 引用 semi/anti join 专属表 → 移至对应 join 的 filter
    20. filter/having 中 {"$preset": "YYYY-MM-DD"} → {"$date": "YYYY-MM-DD"}
    21. filter/having 条件中缺少 op 字段 → 删除该条件
    22. filter/having 条件 val: {"col": "x"} → col2: "x"（由 _coerce_condition 处理）
    23. filter/having 条件 col_right → col2（由 _coerce_condition 处理）
    24. agg items 中 "expr" 字段 → "col"
    """
    q = dict(q)  # 浅拷贝，避免修改调用方的原始对象
    q = _coerce_toplevel(q)
    q = _coerce_filters(q)
    q = _coerce_agg(q)
    q = _coerce_having_agg_refs(q)
    q = _coerce_select(q)
    q = _coerce_group(q)
    q = _coerce_cte_refs(q)
    q = _coerce_joins(q)
    q = _coerce_filter_vals(q)
    q = _coerce_window_qualify(q)
    return q


def _coerce_toplevel(q: dict) -> dict:
    """顶层结构修复。处理修复 8（query 字符串解包）、修复 11（scan 从 CTE 推断）。"""
    # 修复 8：顶层 "query" 为 JSON 字符串时解包
    # 场景：模型把完整 Forge JSON 嵌套在 "query" 字段里，且顶层缺少必填的 scan/select。
    # 此修复仅在顶层无 scan 且 query 是字符串时生效，不影响正常 union/CTE 中的 query dict。
    if (isinstance(q.get("query"), str)
            and "scan" not in q
            and "select" not in q):
        try:
            inner = json.loads(q["query"])
            if isinstance(inner, dict):
                q = inner
        except (json.JSONDecodeError, TypeError):
            pass

    # 修复 11：缺少 scan 但有 cte 时，推断 scan 为最后一个 CTE 名
    # 场景：模型生成了 CTE 列表和 group/agg/select，但忘了写主查询的 scan 字段
    if "scan" not in q and q.get("cte"):
        q["scan"] = q["cte"][-1]["name"]

    return q


def _coerce_filters(q: dict) -> dict:
    """过滤条件基础规范化。处理修复 1（dict→list）、修复 2（between val 拆分）。"""
    # 修复 1：filter/having 为 dict 时包装为列表
    for field in ("filter", "having"):
        if isinstance(q.get(field), dict):
            q[field] = [q[field]]

    # 修复 2：between val 数组 → lo/hi
    for field in ("filter", "having"):
        fixed = []
        for cond in q.get(field, []):
            fixed.append(_coerce_condition(cond))
        if fixed:
            q[field] = fixed

    return q


def _coerce_agg(q: dict) -> dict:
    """聚合项清理。处理修复 12（非法字段清理）、修复 24（expr→col）、修复 4（count_all col 删除）。"""
    # 修复 12：清理 agg items 中的非法字段
    # 场景：模型在 agg 项中混入 having、where 等非法字段，导致 schema 校验失败
    _VALID_AGG_FIELDS = {"fn", "col", "as", "separator", "filter"}
    if q.get("agg"):
        cleaned_agg = []
        for item in q["agg"]:
            # 修复 24：agg 中 "expr" 字段 → "col"（CASE WHEN 表达式）
            if "expr" in item and "col" not in item:
                item = dict(item)
                item["col"] = item.pop("expr")
            cleaned_agg.append({k: v for k, v in item.items() if k in _VALID_AGG_FIELDS})
        q["agg"] = cleaned_agg

    # 修复 4：count_all 不允许有 col 字段
    if q.get("agg"):
        fixed_agg = []
        for agg_item in q["agg"]:
            if agg_item.get("fn") == "count_all" and "col" in agg_item:
                agg_item = {k: v for k, v in agg_item.items() if k != "col"}
            fixed_agg.append(agg_item)
        q["agg"] = fixed_agg

    return q


def _coerce_having_agg_refs(q: dict) -> dict:
    """HAVING 内联聚合函数替换。处理修复 6（having fn→agg alias）。"""
    # 修复 6：having 中出现内联聚合函数 → 替换为 agg alias
    if q.get("having") and q.get("agg"):
        # 构建 (fn, col) → alias 查找表；count_all 的 col 为空字符串
        agg_lookup: dict[tuple, str] = {}
        for agg_item in q["agg"]:
            if "as" in agg_item:
                key = (agg_item.get("fn"), agg_item.get("col", ""))
                agg_lookup[key] = agg_item["as"]
        q["having"] = [_coerce_having_fn(c, agg_lookup) for c in q["having"]]

    return q


def _coerce_select(q: dict) -> dict:
    """SELECT 规范化与自动生成。处理修复 9（内联别名）、修复 10（自动生成 select）、修复 13（count_all→COUNT(*)）。"""
    # 修复 9：select 中的字符串含内联别名（"expr AS alias"）→ 转为 expr 对象
    # 场景：模型把 SELECT 的 AS 子句写进字符串，如 "STRFTIME('%Y-%m', t.col) as month"
    # 若不处理，fix 5 会把含 "." 的整个字符串（含 " as month"）加入 GROUP BY，产生语法错误
    if q.get("select"):
        new_select = []
        for item in q["select"]:
            if isinstance(item, str):
                # 贪婪匹配末尾的 " AS alias"（alias 为合法标识符，忽略大小写）
                m = re.match(r'^(.+)\s+[Aa][Ss]\s+([A-Za-z_]\w*)\s*$', item)
                if m:
                    item = {"expr": m.group(1).strip(), "as": m.group(2)}
            new_select.append(item)
        q["select"] = new_select

    # 修复 10：有 agg/group 但缺少 select 时，自动生成 select
    # 场景：CTE 子查询常见，模型写了 scan/filter/group/agg 但忘了写 select
    # 自动生成顺序：group 维度列在前，agg 别名在后（与 SQL 语义一致）
    if "select" not in q and (q.get("agg") or q.get("group")):
        auto_select: list = []
        agg_aliases = [a["as"] for a in q.get("agg", []) if "as" in a]
        for g in q.get("group", []):
            if isinstance(g, str):
                auto_select.append(g)
        auto_select.extend(agg_aliases)
        if auto_select:
            q["select"] = auto_select

    # 修复 13：select expr 字符串中出现 count_all() → 替换为 COUNT(*)
    # 场景：模型在 expr 中内联 count_all()，SQLite 不认识该函数名
    if q.get("select"):
        fixed_sel = []
        for item in q["select"]:
            if isinstance(item, dict) and "expr" in item:
                item = dict(item)
                item["expr"] = re.sub(
                    r'\bcount_all\s*\(\s*\)', 'COUNT(*)', item["expr"], flags=re.IGNORECASE
                )
            fixed_sel.append(item)
        q["select"] = fixed_sel

    # 修复 21：多 CTE JOIN 时外层 SELECT 的裸列名歧义
    # 场景：scan=CTE_A，JOIN CTE_B，两个 CTE 都有 "month" 列，
    #       外层 SELECT ["month", ...] → SQLite: ambiguous column name: month
    # 策略：对外层 SELECT 中不含 "." 的裸列字符串，加上主扫描 CTE 的前缀
    if q.get("cte") and q.get("select") and q.get("joins"):
        _cte_names = {c["name"] for c in q["cte"] if isinstance(c, dict) and "name" in c}
        _scan = q.get("scan", "")
        if _scan in _cte_names:
            _joined_ctes = {j["table"] for j in q.get("joins", []) if j.get("table") in _cte_names}
            if _joined_ctes:
                new_sel = []
                for item in q["select"]:
                    if isinstance(item, str) and "." not in item:
                        item = f"{_scan}.{item}"
                    new_sel.append(item)
                q["select"] = new_sel

    return q


def _coerce_group(q: dict) -> dict:
    """GROUP BY 补全与推断。处理修复 5（select 维度列补入 group）、修复 17（从 having 推断 group）。"""
    # 修复 5：group 存在时，补全 select 中缺失的维度列到 group
    if q.get("group") is not None and q.get("agg"):
        agg_aliases    = {a["as"] for a in q.get("agg", [])    if "as" in a}
        window_aliases = {w["as"] for w in q.get("window", []) if "as" in w}
        # group expr 别名（如 "month"）已有对应 group 条目，不应再追加为普通列
        group_expr_aliases = {
            g["as"] for g in q.get("group", [])
            if isinstance(g, dict) and "as" in g
        }
        known_aliases  = agg_aliases | window_aliases | group_expr_aliases
        current_group  = list(q["group"])
        # group_set 只存字符串形式（用于重复检测）
        group_set      = {g if isinstance(g, str) else g.get("as", "") for g in current_group}
        for sel_item in q.get("select", []):
            # select 可以是字符串或 expr 对象 — 只处理字符串形式
            if not isinstance(sel_item, str):
                continue
            if sel_item in known_aliases:
                continue          # agg/window/group_expr 别名，不需要进 GROUP BY
            if sel_item in group_set:
                continue          # 已经在 GROUP BY 里
            if "." in sel_item:   # 看起来像 table.col 格式
                current_group.append(sel_item)
                group_set.add(sel_item)
        q["group"] = current_group

    return q


def _coerce_cte_refs(q: dict) -> dict:
    """CTE 列引用前缀剥离。处理修复 14（scan 为 CTE 名时，剥离 table.col 中非 CTE 的 table. 前缀）。"""
    # 修复 14：当 scan 为 CTE 名时，外层列引用中的 table.col 去掉 table. 前缀
    # 场景：模型在 CTE 内 GROUP BY products.category，然后外层 SELECT products.category
    #       CTE 输出列名不带表前缀，SQLite 报 "no such column: products.category"
    # 策略：收集 CTE 名集合，若 scan 是 CTE 名，则对 select 字符串、agg.col、group 中
    #       出现的 "other_table.col" 格式（other_table 不是 CTE 名）剥离表前缀
    if q.get("cte") and q.get("scan"):
        cte_names = {c["name"] for c in q["cte"] if isinstance(c, dict) and "name" in c}
        if q["scan"] in cte_names:
            def _strip_prefix(s: str) -> str:
                """若 s 形如 table.col 且 table 是主扫描 CTE，剥离 table. 前缀。
                JOIN 表的前缀必须保留，防止多表场景中出现 ambiguous column name。
                """
                if isinstance(s, str) and "." in s:
                    parts = s.split(".", 1)
                    if parts[0] == q["scan"]:   # 只剥主扫描 CTE 自己的前缀
                        return parts[1]
                return s

            # select 字符串项 及 expr 对象中纯列引用（"table.col"）
            # 修复 21 冲突：当有 joined CTE 时，SELECT 的前缀由 fix 21（_coerce_select）负责，
            # 这里跳过剥离，避免把 fix 21 加的消歧前缀又剥掉。
            _has_joined_ctes = any(
                j.get("table") in cte_names for j in q.get("joins", [])
            )
            if q.get("select") and not _has_joined_ctes:
                new_sel = []
                for item in q["select"]:
                    if isinstance(item, str):
                        item = _strip_prefix(item)
                    elif isinstance(item, dict) and "expr" in item:
                        expr_val = item["expr"]
                        # 仅当 expr 是纯 table.col 形式（无空格/括号）时才剥离
                        if (isinstance(expr_val, str)
                                and re.match(r'^[A-Za-z_]\w*\.[A-Za-z_]\w*$', expr_val)):
                            stripped = _strip_prefix(expr_val)
                            if stripped != expr_val:
                                item = dict(item)
                                item["expr"] = stripped
                    new_sel.append(item)
                q["select"] = new_sel
            # agg.col
            if q.get("agg"):
                new_agg = []
                for agg_item in q["agg"]:
                    agg_item = dict(agg_item)
                    if "col" in agg_item:
                        agg_item["col"] = _strip_prefix(agg_item["col"])
                    new_agg.append(agg_item)
                q["agg"] = new_agg
            # group
            if q.get("group"):
                q["group"] = [_strip_prefix(g) for g in q["group"]]
            # window partition
            if q.get("window"):
                new_win = []
                for w in q["window"]:
                    if w.get("partition"):
                        w = dict(w)
                        w["partition"] = [_strip_prefix(p) for p in w["partition"]]
                    new_win.append(w)
                q["window"] = new_win

    return q


def _coerce_joins(q: dict) -> dict:
    """JOIN 过滤条件修复。处理修复 15（join filter→顶层）、修复 17（having 推断 group）、修复 19（semi/anti filter 迁移）。"""
    # 修复 15：inner/left/right/full join 中的 filter 属性 → 提取到顶层 filter
    # 场景：模型在普通 join 中附加 filter 字段，该表在 FROM 作用域内，条件可直接移至 WHERE
    # 注意：semi/anti join 的 filter 留在 join 内（由 _join() 并入 EXISTS/NOT EXISTS 子查询）
    if q.get("joins"):
        new_joins = []
        top_filter: list = list(q.get("filter", []))
        for join_item in q["joins"]:
            if (isinstance(join_item, dict)
                    and "filter" in join_item
                    and join_item.get("type") not in ("semi", "anti")):
                join_item = dict(join_item)
                extra = join_item.pop("filter")
                if isinstance(extra, list):
                    top_filter.extend(extra)
                elif isinstance(extra, dict):
                    top_filter.append(extra)
            new_joins.append(join_item)
        q["joins"] = new_joins
        if top_filter:
            q["filter"] = top_filter

    # 修复 17：HAVING 存在但缺少 GROUP BY → 从 select 的非聚合/非窗口列推断 GROUP BY
    # 场景：模型在 CTE 最终查询中使用 HAVING 但忘写 GROUP BY（如 JOIN 两个 CTE 后 HAVING ratio > 0.2）
    if q.get("having") and not q.get("group"):
        agg_aliases_h    = {a["as"] for a in q.get("agg", [])    if "as" in a}
        window_aliases_h = {w["as"] for w in q.get("window", []) if "as" in w}
        known_aliases_h  = agg_aliases_h | window_aliases_h
        inferred_group: list = []
        seen_group: set = set()
        for item in q.get("select", []):
            if isinstance(item, str) and item not in known_aliases_h and item not in seen_group:
                inferred_group.append(item)
                seen_group.add(item)
        if inferred_group:
            q["group"] = inferred_group

    # 修复 19：顶层 filter 中引用 semi/anti join 专属表的条件 → 移至对应 join 的 filter
    # 场景：模型把 semi/anti join 内部的过滤条件（如 dwd_cart_detail.action_type='add'）
    #       错误地放在顶层 filter，而该表不在主 FROM/JOIN 作用域内，导致 SQL 引用不存在的列
    if q.get("filter") and q.get("joins"):
        _main_tables: set = {q.get("scan", "")}
        for _j in q.get("joins", []):
            if _j.get("type") not in ("semi", "anti"):
                _main_tables.add(_j.get("table", ""))
        # 找出仅出现在 semi/anti join 中、不在主作用域的表
        _semi_anti_map: dict[str, int] = {}  # table_name → join index (first occurrence)
        for _i, _j in enumerate(q.get("joins", [])):
            if _j.get("type") in ("semi", "anti"):
                _tbl = _j.get("table", "")
                if _tbl not in _main_tables and _tbl not in _semi_anti_map:
                    _semi_anti_map[_tbl] = _i
        if _semi_anti_map:
            _keep_filter: list = []
            _move_to_join: dict[int, list] = {}
            for _cond in q.get("filter", []):
                _moved = False
                if isinstance(_cond, dict) and "col" in _cond:
                    _col_ref = _cond["col"]
                    if "." in _col_ref:
                        _tbl_ref = _col_ref.split(".")[0]
                        if _tbl_ref in _semi_anti_map:
                            _ji = _semi_anti_map[_tbl_ref]
                            _move_to_join.setdefault(_ji, []).append(_cond)
                            _moved = True
                if not _moved:
                    _keep_filter.append(_cond)
            if _move_to_join:
                q["filter"] = _keep_filter
                _new_joins = list(q.get("joins", []))
                for _ji, _extra_conds in _move_to_join.items():
                    _jj = dict(_new_joins[_ji])
                    _jj["filter"] = list(_jj.get("filter", [])) + _extra_conds
                    _new_joins[_ji] = _jj
                q["joins"] = _new_joins

    return q


# 修复 20 使用的常量和辅助函数（模块级别，避免每次调用重建）
_ISO_DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
_VALID_PRESETS = {
    "today", "yesterday", "last_7_days", "last_30_days",
    "this_month", "last_month", "this_quarter", "this_year",
}


def _coerce_val(v):
    """将值中的非法 $preset 日期字符串转为 $date。"""
    if isinstance(v, dict) and "$preset" in v:
        p = v["$preset"]
        if p not in _VALID_PRESETS and _ISO_DATE_RE.match(str(p)):
            return {"$date": p}
    return v


def _coerce_cond_val(cond: dict) -> dict:
    """递归修复条件中的 $preset → $date 值。"""
    if "or" in cond:
        return {"or": [_coerce_cond_val(c) for c in cond["or"]]}
    if "and" in cond:
        return {"and": [_coerce_cond_val(c) for c in cond["and"]]}
    if "val" in cond:
        cond = dict(cond)
        cond["val"] = _coerce_val(cond["val"])
    for bound in ("lo", "hi"):
        if bound in cond:
            cond = dict(cond)
            cond[bound] = _coerce_val(cond[bound])
    return cond


def _has_valid_op(cond: dict) -> bool:
    """判断条件是否含有效的 op，或为 or/and 组。"""
    if "or" in cond or "and" in cond:
        return True
    return "op" in cond


def _coerce_filter_vals(q: dict) -> dict:
    """过滤条件值修复与清理。处理修复 20（$preset→$date）、修复 21（缺少 op 的条件删除）。"""
    # 修复 20：filter/having 中 {"$preset": "YYYY-MM-DD"} → 转为 {"$date": "..."}
    # 场景：模型用 {"$preset": "2025-11-01"} 而 "2025-11-01" 不是合法 preset 名
    for field in ("filter", "having"):
        if q.get(field):
            q[field] = [_coerce_cond_val(c) for c in q[field]]

    # 修复 21：filter/having 条件中缺少 op 字段 → 删除该条件
    # 场景：模型生成 {"col": "x"} 没有 op，导致 schema 校验失败
    for field in ("filter", "having"):
        if q.get(field):
            q[field] = [c for c in q[field] if _has_valid_op(c)]

    return q


def _coerce_window_qualify(q: dict) -> dict:
    """窗口函数与 qualify 修复。处理修复 7（qualify 引用的 window 别名补入 select）。"""
    # 修复 7：qualify 引用的 window 别名未在 select 中时，自动补齐
    # 解决：模型使用 window + qualify 时遗漏 rank alias，导致外层 WHERE 引用未定义列。
    # 机制：qualify 编译为 SELECT * FROM (inner_sql) WHERE alias = val，
    #       alias 必须出现在 inner_sql 的 SELECT 中才能被外层引用。
    if q.get("qualify") and q.get("window"):
        win_aliases = {w["as"] for w in q.get("window", []) if "as" in w}
        current_sel = list(q.get("select", []))
        sel_strs = {s for s in current_sel if isinstance(s, str)}
        for qcond in q.get("qualify", []):
            if isinstance(qcond, dict):
                col = qcond.get("col")
                if col and col in win_aliases and col not in sel_strs:
                    current_sel.append(col)
                    sel_strs.add(col)
        q["select"] = current_sel

    return q


def _coerce_condition(cond: dict) -> dict:
    """递归修复条件中的 between val 数组格式；透传 expr 条件（修复 16）。"""
    if "or" in cond:
        return {"or": [_coerce_condition(c) for c in cond["or"]]}
    if "and" in cond:
        return {"and": [_coerce_condition(c) for c in cond["and"]]}
    # 修复 16：expr 条件 → 将 expr 值折叠为 col，通过 schema 校验
    # {"expr":"CAST(x AS FLOAT)/...","op":"gt","val":0.15}
    # → {"col":"CAST(x AS FLOAT)/...","op":"gt","val":0.15}
    # schema 要求 col 字段（字符串），而 _condition() 将 col 直接作为 SQL 左值输出，语义一致
    if "expr" in cond and "col" not in cond:
        cond = dict(cond)
        cond["col"] = cond.pop("expr")
        return cond
    # 修复 22：val: {"col": "other_col"} → col2: "other_col"（列与列比较）
    # 模型有时把右侧列引用放在 val 字段，如 {"col":"good_count","op":"gt","val":{"col":"bad_count"}}
    # → {"col":"good_count","op":"gt","col2":"bad_count"}
    if (isinstance(cond.get("val"), dict)
            and "col" in cond.get("val", {})
            and len(cond.get("val", {})) == 1):
        cond = dict(cond)
        cond["col2"] = cond.pop("val")["col"]
        return cond
    # 修复 23：col_right → col2（模型有时用 col_right 代替 col2）
    if "col_right" in cond and "col2" not in cond:
        cond = dict(cond)
        cond["col2"] = cond.pop("col_right")
        return cond
    if (cond.get("op") == "between"
            and "val" in cond
            and isinstance(cond["val"], list)
            and len(cond["val"]) == 2):
        cond = dict(cond)
        lo, hi = cond.pop("val")
        cond["lo"] = lo
        cond["hi"] = hi
    return cond


def _coerce_having_fn(cond: dict, agg_lookup: dict) -> dict:
    """
    将 having 中出现的内联聚合条件替换为 agg alias 引用。

    如 {"col": "orders.total_amount", "fn": "avg", "op": "gt", "val": 800}
    → {"col": "avg_amount", "op": "gt", "val": 800}  （若 agg 中有对应别名）
    """
    if "or" in cond:
        return {"or": [_coerce_having_fn(c, agg_lookup) for c in cond["or"]]}
    if "and" in cond:
        return {"and": [_coerce_having_fn(c, agg_lookup) for c in cond["and"]]}
    if "fn" in cond:
        fn  = cond.get("fn")
        col = cond.get("col", "")
        alias = agg_lookup.get((fn, col))
        if alias:
            new_cond: dict = {"col": alias, "op": cond["op"]}
            if "val" in cond:
                new_cond["val"] = cond["val"]
            if "lo" in cond:
                new_cond["lo"] = cond["lo"]
            if "hi" in cond:
                new_cond["hi"] = cond["hi"]
            return new_cond
    return cond


def _friendly_error(exc: jsonschema.ValidationError) -> str:
    """
    将 jsonschema 的原始错误转换为对 LLM 友好的中文提示。

    针对测试中出现的高频错误做定向翻译，其余情况降级为原始消息。
    """
    msg  = exc.message
    path = list(exc.absolute_path)

    # ── 最高频：缺少 select 字段 ──────────────────────────────────────────────
    if "'select' is a required property" in msg:
        return (
            "Forge JSON 缺少必填字段 'select'。"
            "请添加 select 数组，列出所有要输出的列名或聚合别名，"
            "例如：\"select\": [\"users.city\", \"total_gmv\"]"
        )

    # ── 缺少 scan 字段 ────────────────────────────────────────────────────────
    if "'scan' is a required property" in msg:
        return (
            "Forge JSON 缺少必填字段 'scan'。"
            "请指定要查询的主表名，例如：\"scan\": \"orders\""
        )

    # ── 排名窗口函数带了 col 字段 ─────────────────────────────────────────────
    if path and path[0] == "window" and (
        "Additional properties are not allowed" in msg and "'col'" in msg
    ):
        idx = path[1] if len(path) > 1 else "?"
        return (
            f"window[{idx}] 中的排名函数（row_number / rank / dense_rank）不需要 'col' 字段。"
            "请删除 col，只保留 fn、partition、order、as。"
        )

    # ── filter/having 条件不合法 ──────────────────────────────────────────────
    if path and path[0] in ("filter", "having") and "is not valid" in msg:
        field = path[0]
        return (
            f"{field} 中的条件格式有误：{msg}。"
            "简单条件格式：{\"col\":\"t.col\",\"op\":\"eq\",\"val\":...}；"
            "OR 组格式：{\"or\":[...]}；"
            "OR 内嵌 AND：{\"or\":[...,{\"and\":[...]}]}"
        )

    # ── join 缺少 type ────────────────────────────────────────────────────────
    if "'type' is a required property" in msg and path and path[0] == "joins":
        return (
            "join 定义缺少必填字段 'type'。"
            "必须明确指定 JOIN 类型：inner / left / right / full / anti / semi"
        )

    # ── 无效的聚合函数名 ──────────────────────────────────────────────────────
    if "is not one of" in msg and path and "fn" in path:
        return f"无效的函数名：{msg}。请检查 fn 字段的拼写。"

    # ── 降级：返回简洁的原始信息，去掉冗长的 schema 路径 ─────────────────────
    return f"Forge JSON 格式错误：{msg}（路径：{' > '.join(str(p) for p in path) or '根节点'}）"


# ── 主编译逻辑 ────────────────────────────────────────────────────────────────

def _compile(q: dict, dialect: str = "sqlite",
             nullable_cols: frozenset[str] | None = None) -> str:
    """将已校验的 Forge 字典逐子句编译为 SQL，按标准子句顺序追加到列表后合并。

    若存在 qualify 字段，将内层查询包裹为子查询，qualify 条件作为外层 WHERE，
    实现窗口函数结果过滤（等价于 QUALIFY 语法，但兼容所有 SQL 方言）。

    若存在 union/intersect/except 字段，将各分支编译后以相应关键字拼接。
    sort/limit/offset 在集合运算存在时被提升到整体结果之后（标准 SQL 语义）。

    若存在 cte 字段，最终输出以 WITH [RECURSIVE] 前置。
    若任意 CTE 含 recursive_term，整个 WITH 子句使用 WITH RECURSIVE。

    dialect:      "sqlite" | "mysql" | "postgresql" | "bigquery" | "snowflake"，控制方言相关的 SQL 输出。
    nullable_cols: 可空列名集合，用于 neq 条件的 NULL 安全展开。
    """
    clauses: list[str] = []

    # SELECT [DISTINCT]：必须最先构建，以便解析 agg/window 别名
    select_prefix = "SELECT DISTINCT" if q.get("distinct") else "SELECT"
    clauses.append(select_prefix + " " + ", ".join(_select_exprs(q, dialect)))

    # FROM：主扫描表
    clauses.append("FROM " + q["scan"])

    # JOIN：anti/semi 会注入额外的 WHERE 条件，收集到 extra_where
    extra_where: list[str] = []
    for join in q.get("joins", []):
        join_sql, injected = _join(join, dialect, nullable_cols)
        if join_sql:
            clauses.append(join_sql)
        extra_where.extend(injected)

    # WHERE：用户显式过滤条件 + anti/semi join 注入的隐式条件，全部 AND 连接
    where_parts = [_condition(c, dialect, nullable_cols) for c in q.get("filter", [])] + extra_where
    if where_parts:
        clauses.append("WHERE " + " AND ".join(where_parts))

    # GROUP BY — 支持字符串引用和 {"expr":"...","as":"alias"} 两种形式
    if "group" in q:
        def _group_item(g) -> str:
            if isinstance(g, dict) and "expr" in g:
                return g["expr"]
            return str(g)
        clauses.append("GROUP BY " + ", ".join(_group_item(g) for g in q["group"]))

    # HAVING：聚合后的行级过滤，条件间 AND 连接
    # 注意：SQLite 中 HAVING alias 若与原表字段同名，会被解析为原始列而非聚合结果
    # 解决方案：将 HAVING 条件中引用的 agg 别名替换为完整聚合表达式
    _having_agg_map: dict[str, str] = {
        agg["as"]: _agg_expr(agg, dialect) for agg in q.get("agg", [])
    }
    def _having_condition(c: dict) -> str:
        """编译 HAVING 条件，将 agg 别名替换为完整聚合表达式。"""
        if isinstance(c, dict) and "col" in c and c.get("col") in _having_agg_map:
            c = dict(c, col=_having_agg_map[c["col"]])
        return _condition(c, dialect, nullable_cols)
    having_parts = [_having_condition(c) for c in q.get("having", [])]
    if having_parts:
        clauses.append("HAVING " + " AND ".join(having_parts))

    # UNION 存在时，sort/limit/offset 需提升到整个 UNION 之后（标准 SQL 语义）。
    # 将尾部子句延迟处理：先收集，UNION 组装完再追加。
    tail_clauses: list[str] = []
    if "sort" in q:
        sort_exprs = [f"{s['col']} {s['dir'].upper()}" for s in q["sort"]]
        tail_clauses.append("ORDER BY " + ", ".join(sort_exprs))
    if "limit" in q:
        tail_clauses.append(f"LIMIT {q['limit']}")
    if "offset" in q:
        tail_clauses.append(f"OFFSET {q['offset']}")

    # 若无集合运算，直接合并尾部子句到主体
    has_set_ops = q.get("union") or q.get("intersect") or q.get("except")
    if not has_set_ops:
        clauses.extend(tail_clauses)
        tail_clauses = []

    inner_sql = "\n".join(clauses)

    # QUALIFY：窗口函数结果过滤，将内层查询包裹为子查询
    # 用途：实现 per-group TopN，如"每个品类成本排名前3的商品"
    if "qualify" in q:
        qualify_parts = [_condition(c, dialect, nullable_cols) for c in q["qualify"]]
        inner_sql = (
            f"SELECT * FROM (\n"
            + "\n".join(f"  {line}" for line in inner_sql.splitlines())
            + f"\n) AS _q\nWHERE {' AND '.join(qualify_parts)}"
        )

    # 集合运算（UNION / INTERSECT / EXCEPT）：拼接各分支，尾部子句最后追加
    has_set_ops = q.get("union") or q.get("intersect") or q.get("except")
    if has_set_ops:
        parts = [inner_sql]
        for branch in q.get("union", []):
            keyword = "UNION ALL" if branch["mode"] == "union_all" else "UNION"
            branch_sql = _compile(_coerce(branch["query"]), dialect, nullable_cols)
            parts.append(f"{keyword}\n{branch_sql}")
        for branch in q.get("intersect", []):
            branch_sql = _compile(_coerce(branch["query"]), dialect, nullable_cols)
            parts.append(f"INTERSECT\n{branch_sql}")
        for branch in q.get("except", []):
            branch_sql = _compile(_coerce(branch["query"]), dialect, nullable_cols)
            parts.append(f"EXCEPT\n{branch_sql}")
        inner_sql = "\n".join(parts)
        if tail_clauses:
            inner_sql += "\n" + "\n".join(tail_clauses)

    # CTE（WITH 子句）：多步查询，每条 CTE 递归编译其 query 字段
    if q.get("cte"):
        cte_parts: list[str] = []
        is_recursive = False
        for cte_item in q["cte"]:
            cte_name = cte_item["name"]
            try:
                anchor_sql = _compile(_coerce(cte_item["query"]), dialect, nullable_cols)
            except (KeyError, TypeError) as exc:
                raise ValueError(
                    f"CTE '{cte_name}' 内部查询格式错误：{exc}"
                ) from exc

            if cte_item.get("recursive") and cte_item.get("recursive_term"):
                is_recursive = True
                try:
                    rec_sql = _compile(_coerce(cte_item["recursive_term"]), dialect, nullable_cols)
                except (KeyError, TypeError) as exc:
                    raise ValueError(
                        f"CTE '{cte_name}' 递归部分格式错误：{exc}"
                    ) from exc
                union_kw = (
                    "UNION ALL"
                    if cte_item.get("recursive_union", "union_all") == "union_all"
                    else "UNION"
                )
                cte_body = anchor_sql + f"\n{union_kw}\n" + rec_sql
            else:
                cte_body = anchor_sql

            indented = "\n".join(f"  {line}" for line in cte_body.splitlines())
            cte_parts.append(f"{cte_name} AS (\n{indented}\n)")

        prefix = "WITH RECURSIVE" if is_recursive else "WITH"
        return prefix + " " + ",\n".join(cte_parts) + "\n" + inner_sql

    return inner_sql


# ── SELECT 表达式构建 ─────────────────────────────────────────────────────────

def _expand_aliases(expr_str: str, alias_map: dict[str, str]) -> str:
    """
    将 expr 字符串中出现的别名（整词匹配）替换为对应的 SQL 表达式。

    用途：select 中的 {"expr":"...","as":"..."} 可能引用同一查询的 agg/window 别名，
    而 SQL 不允许在同一 SELECT 层级引用别名（SQLite 会报 "no such column"）。
    展开后的 expr 直接嵌入 SQL 函数调用，避免运行时错误。

    示例：
        alias_map = {"total_users": "COUNT(*)", "repeat_users": "COUNT(CASE WHEN ...)"}
        expr = "repeat_users * 1.0 / total_users"
        → "COUNT(CASE WHEN ...) * 1.0 / COUNT(*)"

    策略：按别名长度降序替换，避免短别名误匹配长别名的子串。
    跳过 FROM/JOIN 后的词（CTE/表名引用），避免将表名展开为聚合表达式。
    """
    for alias in sorted(alias_map.keys(), key=len, reverse=True):
        replacement = alias_map[alias]
        pattern = re.compile(r'\b' + re.escape(alias) + r'\b')

        def _sub(m, _repl=replacement, _expr=expr_str):
            before = _expr[:m.start()].rstrip()
            if re.search(r'\b(FROM|JOIN)\s*$', before, re.IGNORECASE):
                return m.group(0)
            return _repl

        expr_str = pattern.sub(_sub, expr_str)
    return expr_str


def _select_exprs(q: dict, dialect: str = "sqlite") -> list[str]:
    """
    将 select 列表中的每个项展开为完整的 SELECT 表达式。

    查找顺序：
    1. 若项为 dict（expr 对象）→ 展开其中的 agg/window 别名，编译为 expr AS alias
    2. 若项名是 agg 的别名 → 展开为聚合表达式（如 SUM(col) AS alias）
    3. 若项名是 window 的别名 → 展开为窗口函数表达式（如 ROW_NUMBER() OVER (...) AS alias）
    4. 若项名是 group expr 的别名 → 展开为计算表达式（如 STRFTIME(...) AS month）
    5. 否则直接透传（普通列引用）

    对 expr 对象的别名展开：
        agg/window 别名不能在同一 SELECT 中被 expr 字符串直接引用（SQL 不允许）。
        编译器在此自动将别名替换为完整表达式，使生成的 SQL 在所有方言下可执行。
        window 别名中的 agg 别名也已在 _window_expr 中展开（支持嵌套聚合窗口模式）。
    """
    # 预先构建别名 → SQL 表达式的查找字典，避免 O(n²) 线性扫描
    agg_map: dict[str, str] = {
        agg["as"]: _agg_expr(agg, dialect) for agg in q.get("agg", [])
    }
    win_map: dict[str, str] = {
        w["as"]: _window_expr(w, agg_map) for w in q.get("window", [])
    }
    # group expr 别名：{"expr": "STRFTIME(...)", "as": "month"} → "month": "STRFTIME(...)"
    group_expr_map: dict[str, str] = {
        g["as"]: g["expr"]
        for g in q.get("group", [])
        if isinstance(g, dict) and "expr" in g and "as" in g
    }
    # 合并 map：agg 优先（agg 别名可能被 window col 引用，已在 win_map 构建时展开）
    expand_map = {**agg_map, **win_map}

    # expr 对象中 table.alias 形式 → 先剥离 table. 前缀再展开
    # 场景：模型在 expr 中用 cte_name.window_alias 引用同级窗口别名，
    # 或用 other_table.agg_alias 引用本层聚合别名（两者均需展开为完整表达式）
    def _pre_strip_table_prefix(s: str, known_aliases: set) -> str:
        for alias in sorted(known_aliases, key=len, reverse=True):
            # 将 word.alias 替换为 alias（word = 任意标识符，不含操作符/括号）
            s = re.sub(
                r'[A-Za-z_]\w*\.' + re.escape(alias) + r'\b',
                alias,
                s
            )
        return s

    exprs = []
    for col in q["select"]:
        if isinstance(col, dict):
            # expr 对象：先剥离 table.alias 前缀，再展开 agg/window 别名
            pre = _pre_strip_table_prefix(col["expr"], set(expand_map.keys()))
            expanded = _expand_aliases(pre, expand_map)
            exprs.append(f"{expanded} AS {col['as']}")
        elif col in agg_map:
            exprs.append(f"{agg_map[col]} AS {col}")
        elif col in win_map:
            exprs.append(f"{win_map[col]} AS {col}")
        elif col in group_expr_map:
            exprs.append(f"{group_expr_map[col]} AS {col}")
        else:
            exprs.append(col)
    return exprs


def _agg_expr(agg: dict, dialect: str = "sqlite") -> str:
    """
    将单条聚合定义编译为 SQL 聚合表达式（不含 AS 子句）。

    count_all        → COUNT(*)
    count_distinct   → COUNT(DISTINCT col)
    group_concat     → 方言差异：
        sqlite/mysql → GROUP_CONCAT(col)  /  GROUP_CONCAT(col, sep)
        mysql+sep    → GROUP_CONCAT(col SEPARATOR sep)
        postgresql   → STRING_AGG(col, ',')  /  STRING_AGG(col, sep)
    其余             → FN(col)，函数名大写（跨方言兼容）

    FILTER 子句（filter 字段）：
        SUM(col) FILTER (WHERE cond1 AND cond2)
        SQLite（≥3.30）和 PostgreSQL 原生支持。MySQL 不支持，需手动改写。
    """
    fn = agg["fn"]
    if fn == "count_all":
        base = "COUNT(*)"
    elif fn == "count_distinct":
        base = f"COUNT(DISTINCT {agg['col']})"
    elif fn == "group_concat":
        col = agg["col"]
        sep = agg.get("separator")
        if dialect in ("postgresql", "bigquery"):
            sep_sql = _val(sep) if sep is not None else "','"
            base = f"STRING_AGG({col}, {sep_sql})"
        elif dialect == "snowflake":
            sep_sql = f"'{sep}'" if sep is not None else "','"
            base = f"LISTAGG({col}, {sep_sql})"
        elif dialect == "mysql":
            if sep is not None:
                base = f"GROUP_CONCAT({col} SEPARATOR {_val(sep)})"
            else:
                base = f"GROUP_CONCAT({col})"
        else:
            # sqlite（默认）
            if sep is not None:
                base = f"GROUP_CONCAT({col}, {_val(sep)})"
            else:
                base = f"GROUP_CONCAT({col})"
    else:
        base = f"{fn.upper()}({agg['col']})"

    # FILTER (WHERE ...) 子句
    filter_conds = agg.get("filter", [])
    if filter_conds:
        if dialect == "mysql":
            raise ValueError(
                f"MySQL 不支持 FILTER (WHERE ...) 子句（agg alias: {agg.get('as','?')}）。"
                "请改写为 SUM(CASE WHEN cond THEN col END) 形式，或切换到 SQLite / PostgreSQL。"
            )
        if dialect == "bigquery":
            raise ValueError(
                f"BigQuery 不支持 FILTER (WHERE ...) 子句（agg alias: {agg.get('as','?')}）。"
                "请改写为 COUNTIF / SUM(CASE WHEN cond THEN col END) 形式。"
            )
        if dialect == "snowflake":
            raise ValueError(
                f"Snowflake 不支持 FILTER (WHERE ...) 子句（agg alias: {agg.get('as','?')}）。"
                "请改写为 SUM(CASE WHEN cond THEN col END) 形式，或切换到 SQLite / PostgreSQL。"
            )
        filter_sql = " AND ".join(_condition(c, dialect) for c in filter_conds)
        return f"{base} FILTER (WHERE {filter_sql})"

    return base


def _frame_bound(s: str) -> str:
    """
    将窗口帧边界字符串转换为 SQL 关键字。

    支持格式：
        "unbounded_preceding" → "UNBOUNDED PRECEDING"
        "current_row"         → "CURRENT ROW"
        "unbounded_following" → "UNBOUNDED FOLLOWING"
        "6 preceding"         → "6 PRECEDING"
        "1 following"         → "1 FOLLOWING"
    """
    s_norm = s.strip().lower()
    _MAP = {
        "unbounded_preceding": "UNBOUNDED PRECEDING",
        "current_row":         "CURRENT ROW",
        "unbounded_following": "UNBOUNDED FOLLOWING",
    }
    if s_norm in _MAP:
        return _MAP[s_norm]
    # "N preceding" / "N following" 形式
    m = re.match(r'^(\d+)\s+(preceding|following)$', s_norm)
    if m:
        return f"{m.group(1)} {m.group(2).upper()}"
    # 透传（用户自定义或未知格式）
    return s.upper()


def _window_expr(w: dict, agg_map: dict[str, str] | None = None) -> str:
    """
    将单条窗口函数定义编译为 SQL 窗口表达式（不含 AS 子句）。

    函数调用部分：
        - 排名类（row_number/rank/dense_rank）：fn() 无参数
        - 分布类（percent_rank/cume_dist）：fn() 无参数
        - 分桶类（ntile）：NTILE(n)，n 由 DSL 的 n 字段指定
        - 导航类（lag/lead）：fn(col[, offset[, default]])
        - 值类（first_value/last_value）：fn(col)，可配合 frame 控制窗口范围
        - 聚合类（sum/avg/count/min/max）：fn(col)

    OVER 子句：
        - 若有 partition → PARTITION BY col1, col2, ...
        - 若有 order    → ORDER BY col dir, ...
        - 若有 frame    → ROWS/RANGE BETWEEN start AND end
        - 两者均缺时输出 OVER ()，适用于全局排名场景

    agg_map: 可选的聚合别名 → 原始表达式映射。ORDER BY 中若出现 agg alias，
             自动展开为原始聚合表达式（如 total_gmv → SUM(orders.total_amount)），
             避免 SQLite 等方言在 OVER 子句内不支持 alias 引用的问题。
    """
    fn = w["fn"]

    # ── 函数调用部分 ───────────────────────────────────────────────────────────
    if fn in ("row_number", "rank", "dense_rank", "percent_rank", "cume_dist"):
        # 排名/分布函数不接受输入列
        call = f"{fn.upper()}()"
    elif fn == "ntile":
        # 分桶：NTILE(n)，n 必须由用户指定
        n = w.get("n", 4)
        call = f"NTILE({n})"
    elif fn in ("lag", "lead"):
        # 导航函数：col 必填，offset/default 可选，按位置顺序追加
        # 若 col 是 agg 别名，展开为原始聚合表达式（如 order_count → COUNT(*)）
        _lag_col = w["col"]
        if agg_map and _lag_col in agg_map:
            _lag_col = agg_map[_lag_col]
        args: list[str] = [_lag_col]
        if "offset" in w:
            args.append(str(w["offset"]))
            if "default" in w:
                # default 可以是任意标量类型，统一通过 _val() 格式化
                args.append(_val(w["default"]))
        call = f"{fn.upper()}({', '.join(args)})"
    elif fn in ("first_value", "last_value"):
        # 值函数：直接取列，无额外参数
        call = f"{fn.upper()}({w['col']})"
    else:
        # 聚合窗口函数：sum/avg/count/min/max
        # 若 col 是 agg 别名，展开为原始聚合表达式（支持 SUM(SUM(expr)) OVER () 模式）
        col = w["col"]
        if agg_map and col in agg_map:
            col = agg_map[col]
        call = f"{fn.upper()}({col})"

    # ── OVER 子句 ─────────────────────────────────────────────────────────────
    over_parts: list[str] = []
    if w.get("partition"):
        over_parts.append("PARTITION BY " + ", ".join(w["partition"]))
    if w.get("order"):
        sort_exprs = [
            f"{agg_map[s['col']] if agg_map and s['col'] in agg_map else s['col']} {s['dir'].upper()}"
            for s in w["order"]
        ]
        over_parts.append("ORDER BY " + ", ".join(sort_exprs))

    # 窗口帧（frame）：ROWS/RANGE BETWEEN start AND end
    if w.get("frame"):
        frame = w["frame"]
        unit  = frame["unit"].upper()   # ROWS or RANGE
        start = _frame_bound(frame["start"])
        if "end" in frame:
            end = _frame_bound(frame["end"])
            over_parts.append(f"{unit} BETWEEN {start} AND {end}")
        else:
            over_parts.append(f"{unit} {start}")

    return f"{call} OVER ({' '.join(over_parts)})"


# ── JOIN 处理 ─────────────────────────────────────────────────────────────────

def _join(join: dict, dialect: str = "sqlite",
          nullable_cols: frozenset[str] | None = None) -> tuple[str | None, list[str]]:
    """
    将单条 join 定义编译为（JOIN 子句, 额外 WHERE 条件列表）。

    on 支持两种形态：
        单等值条件（dict）：{"left": "t1.col", "right": "t2.col"}
        多条件数组（list）：[SimpleCondition, ...] — inner/left/right/full 专用

    普通 join（inner/left/right/full）：
        返回 JOIN 子句字符串，无额外 WHERE 条件。

    anti join（NOT IN 的安全替代）：
        返回 LEFT JOIN 子句 + WHERE right_key IS NULL 条件。
        通过 IS NULL 过滤实现"不存在于右表"的语义，自动处理 NULL 值陷阱。
        仅支持单等值 on（需要明确的 right_key 做 IS NULL 检测）。

    semi join（EXISTS 模式）：
        不返回 JOIN 子句（避免行数膨胀），仅注入 WHERE EXISTS 子查询。
        仅支持单等值 on。
    """
    jtype = join["type"]
    table = join["table"]
    on    = join.get("on")

    # ── CROSS JOIN（无 ON 条件，用于标量 CTE 如平均值）─────────────────────
    if jtype == "cross":
        return f"CROSS JOIN {table}", []

    # ── 多条件 join（array）─────────────────────────────────────────────────
    # MySQL 不支持 FULL OUTER JOIN，提前报错
    if jtype == "full" and dialect == "mysql":
        raise ValueError(
            "MySQL 不支持 FULL OUTER JOIN。"
            "请改用 LEFT JOIN + UNION + RIGHT JOIN 模拟，或切换到 SQLite / PostgreSQL。"
        )

    # BigQuery 不支持 RIGHT JOIN
    if jtype == "right" and dialect == "bigquery":
        raise ValueError(
            "BigQuery 不支持 RIGHT JOIN。请改用 LEFT JOIN（交换表顺序）。"
        )

    if isinstance(on, list):
        if jtype == "anti":
            # 多条件 anti join
            eq_parts = [f"{c['left']} = {c['right']}" for c in on
                        if isinstance(c, dict) and "left" in c and "right" in c]
            filter_conds = join.get("filter", [])
            if filter_conds:
                # 有 filter：用 NOT EXISTS，精确排除右表中满足条件的行
                not_exists_conds = eq_parts + [
                    _condition(fc, dialect, nullable_cols)
                    for fc in filter_conds
                ]
                return (
                    None,
                    [f"NOT EXISTS (SELECT 1 FROM {table} WHERE {' AND '.join(not_exists_conds)})"],
                )
            else:
                # 无 filter：LEFT JOIN IS NULL（等同于 NOT EXISTS 无条件版本）
                null_check = on[0]["right"]
                on_clause = " AND ".join(eq_parts)
                return (
                    f"LEFT JOIN {table} ON {on_clause}",
                    [f"{null_check} IS NULL"],
                )
        if jtype == "semi":
            # 多条件 semi join：EXISTS 子查询内包含所有等值条件 + filter
            eq_parts = [f"{c['left']} = {c['right']}" for c in on
                        if isinstance(c, dict) and "left" in c and "right" in c]
            semi_conds = eq_parts + [
                _condition(fc, dialect, nullable_cols)
                for fc in join.get("filter", [])
            ]
            return (
                None,
                [f"EXISTS (SELECT 1 FROM {table} WHERE {' AND '.join(semi_conds)})"],
            )
        on_clause = " AND ".join(_condition(c, dialect, nullable_cols) for c in on)
        keyword = _JOIN_KEYWORDS[jtype]
        return f"{keyword} {table} ON {on_clause}", []

    # ── 单等值条件（dict）───────────────────────────────────────────────────
    left  = on["left"]
    right = on["right"]

    if jtype == "anti":
        filter_conds = join.get("filter", [])
        if filter_conds:
            # 有 filter：用 NOT EXISTS，精确排除右表中满足条件的行
            # 例：从未写过差评 → NOT EXISTS (SELECT 1 FROM t WHERE t.user_id = u.user_id AND t.type = '差评')
            not_exists_conds = [f"{left} = {right}"] + [
                _condition(fc, dialect, nullable_cols)
                for fc in filter_conds
            ]
            return (
                None,
                [f"NOT EXISTS (SELECT 1 FROM {table} WHERE {' AND '.join(not_exists_conds)})"],
            )
        else:
            # 无 filter：LEFT JOIN IS NULL（保留原有语义，排除右表中任何匹配行）
            return (
                f"LEFT JOIN {table} ON {left} = {right}",
                [f"{right} IS NULL"],
            )

    if jtype == "semi":
        # EXISTS 子查询：只检查关联关系是否存在，不拉取右表列
        # 若 join 带 filter，将过滤条件并入 EXISTS 的 WHERE 子句
        semi_conds = [f"{left} = {right}"]
        for fc in join.get("filter", []):
            semi_conds.append(_condition(fc, dialect, nullable_cols))
        return (
            None,
            [f"EXISTS (SELECT 1 FROM {table} WHERE {' AND '.join(semi_conds)})"],
        )

    keyword = _JOIN_KEYWORDS[jtype]
    return f"{keyword} {table} ON {left} = {right}", []


# ── 条件表达式 ────────────────────────────────────────────────────────────────

def _condition(cond: dict, dialect: str = "sqlite",
               nullable_cols: frozenset[str] | None = None) -> str:
    """
    将单条条件定义编译为 SQL 谓词字符串。

    支持的结构：
        OrCondition：{"or": [...]} → (cond1 OR cond2 OR ...)
        AndCondition：{"and": [...]} → (cond1 AND cond2 AND ...)
        SimpleCondition：{"col": ..., "op": ..., ...}

    SimpleCondition 运算符说明：
        is_null / is_not_null → 无 val，直接生成 IS NULL / IS NOT NULL
        between → 需要 lo/hi，生成 BETWEEN lo AND hi
        in      → val 为列表，生成 IN (v1, v2, ...)
        like    → val 为字符串模式
        eq/neq/gt/gte/lt/lte → 标准二元比较符
        neq + nullable_cols → (col != val OR col IS NULL)，避免 NULL 被静默排除
    """
    # OR 组：递归处理每个子条件，外层加括号保证优先级正确
    if "or" in cond:
        return "(" + " OR ".join(_condition(c, dialect, nullable_cols) for c in cond["or"]) + ")"

    # AND 组：用于 OR 内嵌 AND，如 (A AND B) OR C 中的 (A AND B) 部分
    if "and" in cond:
        return "(" + " AND ".join(_condition(c, dialect, nullable_cols) for c in cond["and"]) + ")"

    # ExprCondition（修复 16）：{"expr": "...", "op": "...", "val": ...}
    # 模型生成原始 SQL 表达式作为比较左侧（如 CAST(a.x AS FLOAT) / CAST(b.y AS FLOAT)）
    if "expr" in cond and "col" not in cond:
        expr_str = cond["expr"]
        op = cond["op"]
        symbol = _OP_SYMBOLS.get(op, op)
        return f"{expr_str} {symbol} {_val(cond['val'], dialect)}"

    col = cond["col"]
    op  = cond["op"]

    # col2：列与列比较，如 good_count > bad_count
    if "col2" in cond:
        symbol = _OP_SYMBOLS.get(op, op)
        return f"{col} {symbol} {cond['col2']}"

    if op == "is_null":
        return f"{col} IS NULL"
    if op == "is_not_null":
        return f"{col} IS NOT NULL"
    if op == "between":
        return f"{col} BETWEEN {_val(cond['lo'], dialect)} AND {_val(cond['hi'], dialect)}"
    if op == "in":
        val = cond["val"]
        if isinstance(val, dict) and "subquery" in val:
            # IN (SELECT ...) 子查询
            sub_sql = _compile(_coerce(val["subquery"]), dialect, nullable_cols)
            indented = "\n".join(f"  {line}" for line in sub_sql.splitlines())
            return f"{col} IN (\n{indented}\n)"
        items = ", ".join(_val(v, dialect) for v in val)
        return f"{col} IN ({items})"
    if op == "like":
        return f"{col} LIKE {_val(cond['val'], dialect)}"

    # neq + nullable 列：展开为 (col != val OR col IS NULL)，避免 NULL 被静默排除
    if op == "neq" and nullable_cols and _col_is_nullable(col, nullable_cols):
        return f"({col} != {_val(cond['val'], dialect)} OR {col} IS NULL)"

    # 标准比较运算符，从映射表取符号
    symbol = _OP_SYMBOLS[op]
    if "val" not in cond:
        # val 缺失（模型生成残缺条件），回退为 IS NOT NULL（保守处理）
        return f"{col} IS NOT NULL"
    return f"{col} {symbol} {_val(cond['val'], dialect)}"


# ── 值格式化 ──────────────────────────────────────────────────────────────────

def _val(v: Any, dialect: str = "sqlite") -> str:
    """
    将 Python 值格式化为 SQL 字面量字符串。

    类型处理规则：
        None              → NULL
        {"$date": "..."}  → 'YYYY-MM-DD'（跨方言统一）
        {"$preset": "..."}→ 方言相关的日期表达式（见 _preset_val）
        bool              → TRUE / FALSE（须优先于 int 检测，因为 bool 是 int 子类）
        str               → '...' 并对内部单引号做转义（'' 替换）
        list              → 不应出现在此函数中（in 操作符由 _condition 处理）
        其余              → str() 直接转换（int、float 等数字类型）
    """
    if v is None:
        return "NULL"
    if isinstance(v, dict):
        if "$date" in v:
            return f"'{v['$date']}'"
        if "$preset" in v:
            return _preset_val(v["$preset"], dialect)
    if isinstance(v, bool):           # 必须在 int 检测之前，因为 bool 是 int 子类
        return "TRUE" if v else "FALSE"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


def _preset_val(preset: str, dialect: str) -> str:
    """
    将 $preset 相对日期转换为目标方言的 SQL 日期表达式。

    dialect = "sqlite"     → DATE('now', ...) / strftime(...)
    dialect = "mysql"      → CURDATE() / DATE_SUB() / DATE_FORMAT() / DATE_ADD()
    dialect = "postgresql" → CURRENT_DATE / CURRENT_DATE - INTERVAL '...' / DATE_TRUNC()
    """
    _VALID_PRESETS = (
        "today", "yesterday", "last_7_days", "last_30_days",
        "this_month", "last_month", "this_year", "this_quarter",
    )
    if preset not in _VALID_PRESETS:
        raise ValueError(
            f"未知的 $preset 值：'{preset}'。合法值：{_VALID_PRESETS}"
        )

    if dialect == "bigquery":
        _MAP = {
            "today":        "CURRENT_DATE()",
            "yesterday":    "DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)",
            "last_7_days":  "DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)",
            "last_30_days": "DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)",
            "this_month":   "DATE_TRUNC(CURRENT_DATE(), MONTH)",
            "last_month":   "DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)",
            "this_year":    "DATE_TRUNC(CURRENT_DATE(), YEAR)",
            "this_quarter": "DATE_TRUNC(CURRENT_DATE(), QUARTER)",
        }
        return _MAP[preset]

    if dialect == "snowflake":
        _MAP = {
            "today":        "CURRENT_DATE()",
            "yesterday":    "DATEADD(day, -1, CURRENT_DATE())",
            "last_7_days":  "DATEADD(day, -7, CURRENT_DATE())",
            "last_30_days": "DATEADD(day, -30, CURRENT_DATE())",
            "this_month":   "DATE_TRUNC('month', CURRENT_DATE())",
            "last_month":   "DATE_TRUNC('month', DATEADD(month, -1, CURRENT_DATE()))",
            "this_year":    "DATE_TRUNC('year', CURRENT_DATE())",
            "this_quarter": "DATE_TRUNC('quarter', CURRENT_DATE())",
        }
        return _MAP[preset]

    if dialect == "mysql":
        _MAP = {
            "today":        "CURDATE()",
            "yesterday":    "DATE_SUB(CURDATE(), INTERVAL 1 DAY)",
            "last_7_days":  "DATE_SUB(CURDATE(), INTERVAL 7 DAY)",
            "last_30_days": "DATE_SUB(CURDATE(), INTERVAL 30 DAY)",
            "this_month":   "DATE_FORMAT(CURDATE(), '%Y-%m-01')",
            "last_month":   "DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 MONTH), '%Y-%m-01')",
            "this_year":    "DATE_FORMAT(CURDATE(), '%Y-01-01')",
            "this_quarter": (
                "DATE_ADD(DATE_FORMAT(CURDATE(), '%Y-01-01'), "
                "INTERVAL (QUARTER(CURDATE())-1)*3 MONTH)"
            ),
        }
        return _MAP[preset]

    if dialect == "postgresql":
        _MAP = {
            "today":        "CURRENT_DATE",
            "yesterday":    "CURRENT_DATE - INTERVAL '1 day'",
            "last_7_days":  "CURRENT_DATE - INTERVAL '7 days'",
            "last_30_days": "CURRENT_DATE - INTERVAL '30 days'",
            "this_month":   "DATE_TRUNC('month', CURRENT_DATE)",
            "last_month":   "DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')",
            "this_year":    "DATE_TRUNC('year', CURRENT_DATE)",
            "this_quarter": "DATE_TRUNC('quarter', CURRENT_DATE)",
        }
        return _MAP[preset]

    # sqlite（默认）
    _MAP = {
        "today":        "DATE('now')",
        "yesterday":    "DATE('now','-1 day')",
        "last_7_days":  "DATE('now','-7 days')",
        "last_30_days": "DATE('now','-30 days')",
        "this_month":   "DATE('now','start of month')",
        "last_month":   "DATE('now','start of month','-1 month')",
        "this_year":    "DATE('now','start of year')",
        "this_quarter": (
            "DATE('now','start of month',"
            "'-' || ((CAST(strftime('%m','now') AS INTEGER)-1)%3) || ' months')"
        ),
    }
    return _MAP[preset]
