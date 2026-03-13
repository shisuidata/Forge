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
import re
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

_SUPPORTED_DIALECTS = ("sqlite", "mysql", "postgresql")


def compile_query(forge: dict, dialect: str = "sqlite") -> str:
    """
    校验 Forge JSON 并编译为 SQL 字符串。

    Args:
        forge:   符合 Forge DSL 规范的查询字典。
        dialect: 目标 SQL 方言，可选 "sqlite"（默认）、"mysql"、"postgresql"。
                 控制日期函数、字符串聚合、JOIN 等方言差异的编译输出。

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
    return _compile(forge, dialect)


def _coerce(q: dict) -> dict:
    """
    在 schema 校验前对常见模型输出偏差做容错修复（不改变查询语义）。

    修复项（仅限格式规范化，不猜测语义意图）：
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

    注意：缺少必填字段（如 scan、select）不在此修复，由 schema 校验报错后交给模型重试。
    8. 顶层 "query" 为 JSON 字符串时解包（模型有时把整个 Forge JSON 放进 "query" 字段）
       模型有时生成 {"explain":"...", "query": "{\"scan\":\"orders\",...}"}
       → 将 "query" 字符串解析后作为实际查询
    """
    q = dict(q)  # 浅拷贝，避免修改调用方的原始对象

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

def _compile(q: dict, dialect: str = "sqlite") -> str:
    """将已校验的 Forge 字典逐子句编译为 SQL，按标准子句顺序追加到列表后合并。

    若存在 qualify 字段，将内层查询包裹为子查询，qualify 条件作为外层 WHERE，
    实现窗口函数结果过滤（等价于 QUALIFY 语法，但兼容所有 SQL 方言）。

    若存在 union 字段，将各分支编译后以 UNION / UNION ALL 拼接。
    sort/limit/offset 在 UNION 存在时被提升到整个 UNION 结果之后（标准 SQL 语义）。

    若存在 cte 字段，最终输出以 WITH [RECURSIVE] 前置。
    若任意 CTE 含 recursive_term，整个 WITH 子句使用 WITH RECURSIVE。

    dialect: "sqlite" | "mysql" | "postgresql"，控制方言相关的 SQL 输出。
    """
    clauses: list[str] = []

    # SELECT：必须最先构建，以便解析 agg/window 别名
    clauses.append("SELECT " + ", ".join(_select_exprs(q, dialect)))

    # FROM：主扫描表
    clauses.append("FROM " + q["scan"])

    # JOIN：anti/semi 会注入额外的 WHERE 条件，收集到 extra_where
    extra_where: list[str] = []
    for join in q.get("joins", []):
        join_sql, injected = _join(join, dialect)
        if join_sql:
            clauses.append(join_sql)
        extra_where.extend(injected)

    # WHERE：用户显式过滤条件 + anti/semi join 注入的隐式条件，全部 AND 连接
    where_parts = [_condition(c, dialect) for c in q.get("filter", [])] + extra_where
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
    having_parts = [_condition(c, dialect) for c in q.get("having", [])]
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

    # 若无 UNION，直接合并到主体
    if not q.get("union"):
        clauses.extend(tail_clauses)
        tail_clauses = []

    inner_sql = "\n".join(clauses)

    # QUALIFY：窗口函数结果过滤，将内层查询包裹为子查询
    # 用途：实现 per-group TopN，如"每个品类成本排名前3的商品"
    if "qualify" in q:
        qualify_parts = [_condition(c, dialect) for c in q["qualify"]]
        inner_sql = (
            f"SELECT * FROM (\n"
            + "\n".join(f"  {line}" for line in inner_sql.splitlines())
            + f"\n) AS _q\nWHERE {' AND '.join(qualify_parts)}"
        )

    # UNION / UNION ALL：拼接各分支，尾部子句（ORDER BY/LIMIT/OFFSET）最后追加
    if q.get("union"):
        parts = [inner_sql]
        for branch in q["union"]:
            keyword = "UNION ALL" if branch["mode"] == "union_all" else "UNION"
            branch_sql = _compile(_coerce(branch["query"]), dialect)
            parts.append(f"{keyword}\n{branch_sql}")
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
                anchor_sql = _compile(_coerce(cte_item["query"]), dialect)
            except (KeyError, TypeError) as exc:
                raise ValueError(
                    f"CTE '{cte_name}' 内部查询格式错误：{exc}"
                ) from exc

            if cte_item.get("recursive") and cte_item.get("recursive_term"):
                is_recursive = True
                try:
                    rec_sql = _compile(_coerce(cte_item["recursive_term"]), dialect)
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
    """
    for alias in sorted(alias_map.keys(), key=len, reverse=True):
        expr_str = re.sub(r'\b' + re.escape(alias) + r'\b', alias_map[alias], expr_str)
    return expr_str


def _select_exprs(q: dict, dialect: str = "sqlite") -> list[str]:
    """
    将 select 列表中的每个项展开为完整的 SELECT 表达式。

    查找顺序：
    1. 若项为 dict（expr 对象）→ 展开其中的 agg/window 别名，编译为 expr AS alias
    2. 若项名是 agg 的别名 → 展开为聚合表达式（如 SUM(col) AS alias）
    3. 若项名是 window 的别名 → 展开为窗口函数表达式（如 ROW_NUMBER() OVER (...) AS alias）
    4. 否则直接透传（普通列引用）

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
    # 合并 map：agg 优先（agg 别名可能被 window col 引用，已在 win_map 构建时展开）
    expand_map = {**agg_map, **win_map}

    exprs = []
    for col in q["select"]:
        if isinstance(col, dict):
            # expr 对象：展开其中引用的 agg/window 别名后再输出
            expanded = _expand_aliases(col["expr"], expand_map)
            exprs.append(f"{expanded} AS {col['as']}")
        elif col in agg_map:
            exprs.append(f"{agg_map[col]} AS {col}")
        elif col in win_map:
            exprs.append(f"{win_map[col]} AS {col}")
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
    """
    fn = agg["fn"]
    if fn == "count_all":
        return "COUNT(*)"
    if fn == "count_distinct":
        return f"COUNT(DISTINCT {agg['col']})"
    if fn == "group_concat":
        col = agg["col"]
        sep = agg.get("separator")
        if dialect == "postgresql":
            sep_sql = _val(sep) if sep is not None else "','"
            return f"STRING_AGG({col}, {sep_sql})"
        if dialect == "mysql":
            if sep is not None:
                return f"GROUP_CONCAT({col} SEPARATOR {_val(sep)})"
            return f"GROUP_CONCAT({col})"
        # sqlite（默认）
        if sep is not None:
            return f"GROUP_CONCAT({col}, {_val(sep)})"
        return f"GROUP_CONCAT({col})"
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

    return f"{call} OVER ({' '.join(over_parts)})"


# ── JOIN 处理 ─────────────────────────────────────────────────────────────────

def _join(join: dict, dialect: str = "sqlite") -> tuple[str | None, list[str]]:
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
    # MySQL 不支持 FULL OUTER JOIN，提前报错
    if jtype == "full" and dialect == "mysql":
        raise ValueError(
            "MySQL 不支持 FULL OUTER JOIN。"
            "请改用 LEFT JOIN + UNION + RIGHT JOIN 模拟，或切换到 SQLite / PostgreSQL。"
        )

    if isinstance(on, list):
        if jtype in ("anti", "semi"):
            raise ValueError(
                f"{jtype} join 不支持多条件 on。"
                "anti/semi join 需要明确的等值键，请使用单条件 on 格式。"
            )
        on_clause = " AND ".join(_condition(c, dialect) for c in on)
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

def _condition(cond: dict, dialect: str = "sqlite") -> str:
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
        return "(" + " OR ".join(_condition(c, dialect) for c in cond["or"]) + ")"

    # AND 组：用于 OR 内嵌 AND，如 (A AND B) OR C 中的 (A AND B) 部分
    if "and" in cond:
        return "(" + " AND ".join(_condition(c, dialect) for c in cond["and"]) + ")"

    col = cond["col"]
    op  = cond["op"]

    if op == "is_null":
        return f"{col} IS NULL"
    if op == "is_not_null":
        return f"{col} IS NOT NULL"
    if op == "between":
        return f"{col} BETWEEN {_val(cond['lo'], dialect)} AND {_val(cond['hi'], dialect)}"
    if op == "in":
        items = ", ".join(_val(v, dialect) for v in cond["val"])
        return f"{col} IN ({items})"
    if op == "like":
        return f"{col} LIKE {_val(cond['val'], dialect)}"

    # 标准比较运算符，从映射表取符号
    symbol = _OP_SYMBOLS[op]
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
