"""
Forge DSL → SQL 编译器。

职责：接收经过 JSON Schema 校验的 Forge JSON 字典，确定性地输出 SQL 字符串。
编译过程无副作用、无随机性，相同输入永远产生相同输出。

编译流水线（与 SQL 执行语义一致）：
    scan → joins → filter(WHERE) → group(GROUP BY) → agg(SELECT 聚合)
    → having(HAVING) → select(SELECT) → sort(ORDER BY) → limit(LIMIT) → offset(OFFSET)

特殊 join 处理：
    anti join  → LEFT JOIN … WHERE right_key IS NULL  （避免 NOT IN 的 NULL 陷阱）
    semi join  → WHERE EXISTS (SELECT 1 FROM … WHERE …) （不产生 JOIN 关键字）

窗口函数（window 字段）：
    在 SELECT 阶段展开为 fn() OVER (PARTITION BY … ORDER BY …) AS alias
    支持三类：排名类（row_number/rank/dense_rank）、聚合类（sum/avg/count/min/max）、
    导航类（lag/lead，支持 offset 和 default 参数）
"""

from __future__ import annotations
import json
import pathlib
from typing import Any

import jsonschema

# 在模块加载时一次性读取并解析 JSON Schema，避免重复 I/O
_SCHEMA_PATH = pathlib.Path(__file__).parent / "schema.json"
_SCHEMA = json.loads(_SCHEMA_PATH.read_text())

# 标准 JOIN 类型到 SQL 关键字的映射；anti/semi 由 _join() 单独处理
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

def compile_query(forge: dict) -> str:
    """
    校验 Forge JSON 并编译为 SQL 字符串。

    Args:
        forge: 符合 Forge DSL 规范的查询字典。

    Returns:
        多行 SQL 字符串，子句间以换行分隔。

    Raises:
        ValueError: Forge JSON 违反 schema 约束时，抛出人类可读的错误描述。
    """
    forge = _coerce(forge)
    try:
        jsonschema.validate(forge, _SCHEMA)
    except jsonschema.ValidationError as exc:
        raise ValueError(_friendly_error(exc)) from exc
    return _compile(forge)


def _coerce(q: dict) -> dict:
    """
    在 schema 校验前对常见模型输出偏差做容错修复（不改变查询语义）。

    修复项：
    1. filter/having 为 dict 时自动包装为列表
       模型有时生成 "filter": {"or":[...]} 而非 "filter": [{"or":[...]}]
    2. between 条件中 val 为 [lo, hi] 数组时拆分为 lo/hi 字段
       模型有时生成 {"op":"between","val":[500,2000]} 而非 {"lo":500,"hi":2000}
    3. window default=None 转为 null（保持 JSON 语义，_val() 后续转 NULL）
    4. agg 中 count_all + col 字段共存时，删除多余的 col
       模型有时生成 {"fn":"count_all","col":"t.id","as":"..."} 而非 {"fn":"count_all","as":"..."}
    5. group 存在时，select 里的非聚合/非窗口字段自动补齐到 group
       模型有时 GROUP BY user_id 但 SELECT 里包含 name、city 等未 GROUP 的列
    6. having 中出现 fn 字段（内联聚合表达式）→ 替换为对应 agg alias
       模型有时生成 {"col":"orders.total_amount","fn":"avg","op":"gt","val":800}
       而非 {"col":"avg_amount","op":"gt","val":800}
    7. qualify 引用的 window 别名未在 select 中时，自动补齐
       模型在 window + qualify 组合中有时遗漏 rank alias，导致外层 WHERE 引用未定义列
    """
    q = dict(q)  # 浅拷贝，避免修改调用方的原始对象

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

    # 修复 4：count_all 不允许有 col 字段
    if q.get("agg"):
        fixed_agg = []
        for agg_item in q["agg"]:
            if agg_item.get("fn") == "count_all" and "col" in agg_item:
                agg_item = {k: v for k, v in agg_item.items() if k != "col"}
            fixed_agg.append(agg_item)
        q["agg"] = fixed_agg

    # 修复 6：having 中出现内联聚合函数 → 替换为 agg alias
    if q.get("having") and q.get("agg"):
        # 构建 (fn, col) → alias 查找表；count_all 的 col 为空字符串
        agg_lookup: dict[tuple, str] = {}
        for agg_item in q["agg"]:
            if "as" in agg_item:
                key = (agg_item.get("fn"), agg_item.get("col", ""))
                agg_lookup[key] = agg_item["as"]
        q["having"] = [_coerce_having_fn(c, agg_lookup) for c in q["having"]]

    # 修复 5：group 存在时，补全 select 中缺失的维度列到 group
    if q.get("group") is not None and q.get("agg"):
        agg_aliases    = {a["as"] for a in q.get("agg", [])    if "as" in a}
        window_aliases = {w["as"] for w in q.get("window", []) if "as" in w}
        known_aliases  = agg_aliases | window_aliases
        current_group  = list(q["group"])
        # group_set 只存字符串形式（用于重复检测）
        group_set      = {g if isinstance(g, str) else g.get("as", "") for g in current_group}
        for sel_item in q.get("select", []):
            # select 可以是字符串或 expr 对象 — 只处理字符串形式
            if not isinstance(sel_item, str):
                continue
            if sel_item in known_aliases:
                continue          # agg/window 别名，不需要进 GROUP BY
            if sel_item in group_set:
                continue          # 已经在 GROUP BY 里
            if "." in sel_item:   # 看起来像 table.col 格式
                current_group.append(sel_item)
                group_set.add(sel_item)
        q["group"] = current_group

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
    """递归修复条件中的 between val 数组格式。"""
    if "or" in cond:
        return {"or": [_coerce_condition(c) for c in cond["or"]]}
    if "and" in cond:
        return {"and": [_coerce_condition(c) for c in cond["and"]]}
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

def _compile(q: dict) -> str:
    """将已校验的 Forge 字典逐子句编译为 SQL，按标准子句顺序追加到列表后合并。

    若存在 qualify 字段，将内层查询包裹为子查询，qualify 条件作为外层 WHERE，
    实现窗口函数结果过滤（等价于 QUALIFY 语法，但兼容所有 SQL 方言）。

    explain 和 cte 字段不参与编译（explain 为调试元数据，cte 暂未实现）。
    """
    clauses: list[str] = []

    # SELECT：必须最先构建，以便解析 agg/window 别名
    clauses.append("SELECT " + ", ".join(_select_exprs(q)))

    # FROM：主扫描表
    clauses.append("FROM " + q["scan"])

    # JOIN：anti/semi 会注入额外的 WHERE 条件，收集到 extra_where
    extra_where: list[str] = []
    for join in q.get("joins", []):
        sql, injected = _join(join)
        if sql:
            clauses.append(sql)
        extra_where.extend(injected)

    # WHERE：用户显式过滤条件 + anti/semi join 注入的隐式条件，全部 AND 连接
    where_parts = [_condition(c) for c in q.get("filter", [])] + extra_where
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
    having_parts = [_condition(c) for c in q.get("having", [])]
    if having_parts:
        clauses.append("HAVING " + " AND ".join(having_parts))

    # ORDER BY：dir 字段必填（asc/desc），已由 schema 保证
    if "sort" in q:
        sort_exprs = [f"{s['col']} {s['dir'].upper()}" for s in q["sort"]]
        clauses.append("ORDER BY " + ", ".join(sort_exprs))

    # LIMIT
    if "limit" in q:
        clauses.append(f"LIMIT {q['limit']}")

    # OFFSET（分页）
    if "offset" in q:
        clauses.append(f"OFFSET {q['offset']}")

    inner_sql = "\n".join(clauses)

    # QUALIFY：窗口函数结果过滤，将内层查询包裹为子查询
    # 用途：实现 per-group TopN，如"每个品类成本排名前3的商品"
    if "qualify" in q:
        qualify_parts = [_condition(c) for c in q["qualify"]]
        inner_sql = (
            f"SELECT * FROM (\n"
            + "\n".join(f"  {line}" for line in inner_sql.splitlines())
            + f"\n) AS _q\nWHERE {' AND '.join(qualify_parts)}"
        )

    # CTE（WITH 子句）：多步查询，每条 CTE 递归编译其 query 字段
    if q.get("cte"):
        cte_parts: list[str] = []
        for cte_item in q["cte"]:
            cte_name = cte_item["name"]
            try:
                cte_sql = _compile(_coerce(cte_item["query"]))
            except (KeyError, TypeError) as exc:
                raise ValueError(
                    f"CTE '{cte_name}' 内部查询格式错误：{exc}"
                ) from exc
            indented = "\n".join(f"  {line}" for line in cte_sql.splitlines())
            cte_parts.append(f"{cte_name} AS (\n{indented}\n)")
        return "WITH " + ",\n".join(cte_parts) + "\n" + inner_sql

    return inner_sql


# ── SELECT 表达式构建 ─────────────────────────────────────────────────────────

def _select_exprs(q: dict) -> list[str]:
    """
    将 select 列表中的每个项展开为完整的 SELECT 表达式。

    查找顺序：
    1. 若项为 dict（expr 对象）→ 直接编译为 expr AS alias
    2. 若项名是 agg 的别名 → 展开为聚合表达式（如 SUM(col) AS alias）
    3. 若项名是 window 的别名 → 展开为窗口函数表达式（如 ROW_NUMBER() OVER (...) AS alias）
    4. 否则直接透传（普通列引用）
    """
    # 预先构建别名 → SQL 表达式的查找字典，避免 O(n²) 线性扫描
    agg_map: dict[str, str] = {
        agg["as"]: _agg_expr(agg) for agg in q.get("agg", [])
    }
    win_map: dict[str, str] = {
        w["as"]: _window_expr(w, agg_map) for w in q.get("window", [])
    }
    exprs = []
    for col in q["select"]:
        if isinstance(col, dict):
            # expr 对象：{"expr": "quantity * unit_price", "as": "revenue"}
            exprs.append(f"{col['expr']} AS {col['as']}")
        elif col in agg_map:
            exprs.append(f"{agg_map[col]} AS {col}")
        elif col in win_map:
            exprs.append(f"{win_map[col]} AS {col}")
        else:
            exprs.append(col)
    return exprs


def _agg_expr(agg: dict) -> str:
    """
    将单条聚合定义编译为 SQL 聚合表达式（不含 AS 子句）。

    count_all → COUNT(*)
    count_distinct → COUNT(DISTINCT col)
    其余 → FN(col)，函数名大写
    """
    fn = agg["fn"]
    if fn == "count_all":
        return "COUNT(*)"
    if fn == "count_distinct":
        return f"COUNT(DISTINCT {agg['col']})"
    return f"{fn.upper()}({agg['col']})"


def _window_expr(w: dict, agg_map: dict[str, str] | None = None) -> str:
    """
    将单条窗口函数定义编译为 SQL 窗口表达式（不含 AS 子句）。

    函数调用部分：
        - 排名类（row_number/rank/dense_rank）：fn() 无参数
        - 导航类（lag/lead）：fn(col[, offset[, default]])，offset/default 按需追加
        - 聚合类（sum/avg/count/min/max）：fn(col)

    OVER 子句：
        - 若有 partition → PARTITION BY col1, col2, ...
        - 若有 order    → ORDER BY col dir, ...
        - 两者均缺时输出 OVER ()，适用于全局排名场景

    agg_map: 可选的聚合别名 → 原始表达式映射。ORDER BY 中若出现 agg alias，
             自动展开为原始聚合表达式（如 total_gmv → SUM(orders.total_amount)），
             避免 SQLite 等方言在 OVER 子句内不支持 alias 引用的问题。
    """
    fn = w["fn"]

    # ── 函数调用部分 ───────────────────────────────────────────────────────────
    if fn in ("row_number", "rank", "dense_rank"):
        # 排名函数不接受输入列
        call = f"{fn.upper()}()"
    elif fn in ("lag", "lead"):
        # 导航函数：col 必填，offset/default 可选，按位置顺序追加
        args: list[str] = [w["col"]]
        if "offset" in w:
            args.append(str(w["offset"]))
            if "default" in w:
                # default 可以是任意标量类型，统一通过 _val() 格式化
                args.append(_val(w["default"]))
        call = f"{fn.upper()}({', '.join(args)})"
    else:
        # 聚合窗口函数：sum/avg/count/min/max
        call = f"{fn.upper()}({w['col']})"

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

    return f"{call} OVER ({' '.join(over_parts)})"


# ── JOIN 处理 ─────────────────────────────────────────────────────────────────

def _join(join: dict) -> tuple[str | None, list[str]]:
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
    on    = join["on"]

    # ── 多条件 join（array）─────────────────────────────────────────────────
    if isinstance(on, list):
        if jtype in ("anti", "semi"):
            raise ValueError(
                f"{jtype} join 不支持多条件 on。"
                "anti/semi join 需要明确的等值键，请使用单条件 on 格式。"
            )
        on_clause = " AND ".join(_condition(c) for c in on)
        keyword = _JOIN_KEYWORDS[jtype]
        return f"{keyword} {table} ON {on_clause}", []

    # ── 单等值条件（dict）───────────────────────────────────────────────────
    left  = on["left"]
    right = on["right"]

    if jtype == "anti":
        # LEFT JOIN 保留主表所有行，IS NULL 筛选出在右表中无匹配的行
        return (
            f"LEFT JOIN {table} ON {left} = {right}",
            [f"{right} IS NULL"],
        )

    if jtype == "semi":
        # EXISTS 子查询：只检查关联关系是否存在，不拉取右表列
        return (
            None,
            [f"EXISTS (SELECT 1 FROM {table} WHERE {left} = {right})"],
        )

    keyword = _JOIN_KEYWORDS[jtype]
    return f"{keyword} {table} ON {left} = {right}", []


# ── 条件表达式 ────────────────────────────────────────────────────────────────

def _condition(cond: dict) -> str:
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
    """
    # OR 组：递归处理每个子条件，外层加括号保证优先级正确
    if "or" in cond:
        return "(" + " OR ".join(_condition(c) for c in cond["or"]) + ")"

    # AND 组：用于 OR 内嵌 AND，如 (A AND B) OR C 中的 (A AND B) 部分
    if "and" in cond:
        return "(" + " AND ".join(_condition(c) for c in cond["and"]) + ")"

    col = cond["col"]
    op  = cond["op"]

    if op == "is_null":
        return f"{col} IS NULL"
    if op == "is_not_null":
        return f"{col} IS NOT NULL"
    if op == "between":
        return f"{col} BETWEEN {_val(cond['lo'])} AND {_val(cond['hi'])}"
    if op == "in":
        items = ", ".join(_val(v) for v in cond["val"])
        return f"{col} IN ({items})"
    if op == "like":
        return f"{col} LIKE {_val(cond['val'])}"

    # 标准比较运算符，从映射表取符号
    symbol = _OP_SYMBOLS[op]
    return f"{col} {symbol} {_val(cond['val'])}"


# ── 值格式化 ──────────────────────────────────────────────────────────────────

def _val(v: Any) -> str:
    """
    将 Python 值格式化为 SQL 字面量字符串。

    类型处理规则：
        None              → NULL
        {"$date": "..."}  → 'YYYY-MM-DD'
        {"$preset": "..."} → 抛出 NotImplementedError（需运行时解析）
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
            _PRESET_MAP = {
                "today":        "DATE('now')",
                "yesterday":    "DATE('now','-1 day')",
                "last_7_days":  "DATE('now','-7 days')",
                "last_30_days": "DATE('now','-30 days')",
                "this_month":   "DATE('now','start of month')",
                "last_month":   "DATE('now','start of month','-1 month')",
                "this_year":    "DATE('now','start of year')",
            }
            preset = v["$preset"]
            if preset in _PRESET_MAP:
                return _PRESET_MAP[preset]
            if preset == "this_quarter":
                # SQLite 无原生季度函数：((month-1)%3) 计算本月距季度起始的月数
                return (
                    "DATE('now','start of month',"
                    "'-' || ((CAST(strftime('%m','now') AS INTEGER)-1)%3) || ' months')"
                )
            raise ValueError(
                f"未知的 $preset 值：'{preset}'。"
                f"合法值：{sorted(_PRESET_MAP) + ['this_quarter']}"
            )
    if isinstance(v, bool):           # 必须在 int 检测之前，因为 bool 是 int 子类
        return "TRUE" if v else "FALSE"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)
