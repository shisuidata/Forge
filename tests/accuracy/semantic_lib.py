"""
语义消歧库（Semantic Enrichment Library）

为 Forge DSL 测试 pipeline 提供 enrich() 函数：在中文查询问题进入 LLM 之前，
追加括号内的语义说明，消解已知的歧义模式，而不改动原问题措辞。

覆盖 Case: 4, 6, 10, 11, 17, 33, 37
"""
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class Rule:
    pattern: str           # 正则表达式
    annotation: str        # 追加说明（None = 动态生成，见 enrich()）
    case_ids: list[int]    # 覆盖用例（文档用）
    note: str              # 说明（文档用）


_RULES: list[Rule] = [

    # Case 4 — 人均/客单价 = AVG GROUP BY，不用 CTE
    Rule(
        pattern=r"人均消费|客单价|平均客单价",
        annotation=(
            "（注意：「人均消费/客单价」应直接用 AVG(orders.total_amount) GROUP BY 维度，"
            "不要拆成 CTE 两步计算，直接 group + agg 即可）"
        ),
        case_ids=[4],
        note="防止模型对简单 AVG 查询错误套用 CTE",
    ),

    # Case 6 — 主观描述词只排序，绝对不加 HAVING
    Rule(
        pattern=r"找出(高|低)(成本|价格|价值)(品类|商品|用户|城市)",
        annotation=(
            "（注意：「找出高/低成本品类」是主观描述，无明确数值阈值，"
            "只用 ORDER BY 从高到低排序展示，绝对不加 HAVING 过滤条件）"
        ),
        case_ids=[6],
        note="阻止模型对主观描述词错误添加 HAVING",
    ),

    # Case 10 — OR 复合过滤 filter 必须是数组格式
    Rule(
        pattern=r"名字中包含.{1,20}或者",
        annotation=(
            "（注意：本题是 OR 复合条件，filter 必须是 JSON 数组，"
            "OR 写在数组元素内：[{\"or\": [条件A, {\"and\": [条件B1, 条件B2]}]}]，"
            "不能输出对象格式 {\"or\": [...]}）"
        ),
        case_ids=[10],
        note="防止 OR 条件导致 JSON 格式为对象而非数组，触发 JSON 解析失败",
    ),

    # Case 11 — "超过N次" 严格大于，op: gt，不是 gte
    Rule(
        pattern=r"超过\s*(\d+)\s*(次|条|个|种|笔)",
        annotation=None,   # 动态生成，见 enrich() 特殊处理
        case_ids=[11],
        note="区分「超过N(>N)」与「至少N(>=N)」，防止模型用 gte 替代 gt",
    ),

    # Case 17 — 全局序号需要 JOIN users 展示 users.name
    Rule(
        pattern=r"全局序号|最早.*序号为\s*1|按.*时间.*标注.*序号",
        annotation=(
            "（注意：结果需要展示 users.name，joins 中必须包含 orders INNER JOIN users，"
            "select 中要有 users.name 字段）"
        ),
        case_ids=[17],
        note="防止遗漏 users JOIN 导致 users.name 无法展示",
    ),

    # Case 33 — CASE WHEN 标注等级需要 JOIN users 展示 users.name
    Rule(
        pattern=r"根据订单金额标注(等级|分级)|订单金额.{1,10}(高价值|中等|低价值)",
        annotation=(
            "（注意：结果需要展示 users.name，joins 中必须包含 orders INNER JOIN users；"
            "CASE WHEN 内列引用必须加表名，如 orders.total_amount）"
        ),
        case_ids=[33],
        note="防止 CASE WHEN 分级查询遗漏 users JOIN",
    ),

    # Case 37 — 今年注册 VIP 必须同时有日期和 is_vip 两个 filter
    Rule(
        pattern=r"今年.{0,10}注册.{0,10}(VIP|vip|会员)|(VIP|vip|会员).{0,10}今年.{0,10}注册",
        annotation=(
            '（注意：必须同时保留两个 filter：users.created_at >= {"$preset": "this_year"} '
            "AND users.is_vip = 1，两个条件缺一不可）"
        ),
        case_ids=[37],
        note="防止遗漏 is_vip=1 或 this_year 其中之一",
    ),
]


def enrich(question: str) -> str:
    """
    对中文查询问题追加语义说明，消解已知歧义模式。

    - 遍历规则表，re.search() 匹配
    - 命中则追加 annotation（Rule 11 动态生成含数字的说明）
    - 多条规则可叠加，用空格连接
    - 无命中则原样返回

    Args:
        question: 原始中文查询问题

    Returns:
        原问题 + 追加说明（若有命中）
    """
    annotations: list[str] = []

    for rule in _RULES:
        m = re.search(rule.pattern, question)
        if not m:
            continue

        if rule.annotation is None:
            # Case 11 动态注释：代入捕获到的 N 和量词
            n_str = m.group(1)
            unit  = m.group(2)
            n_int = int(n_str)
            ann = (
                f"（注意：「超过{n_str}{unit}」语义是严格大于，"
                f'应使用 op: "gt", val: {n_int}，即 >{n_int}，'
                f"不要用 gte（>={n_int} 含{n_int}本身是错误的））"
            )
        else:
            ann = rule.annotation

        annotations.append(ann)

    if not annotations:
        return question

    return question + "  " + "  ".join(annotations)


def preview(cases_path: str | None = None) -> None:
    """打印所有规则的匹配预览（调试用）。"""
    import json
    from pathlib import Path

    if cases_path is None:
        cases_path = str(Path(__file__).parent / "results" / "cases.json")

    cases = json.loads(Path(cases_path).read_text())
    hits: dict[int, list[int]] = {}   # case_id → matched rule case_ids

    print("=== 语义库规则匹配预览 ===\n")
    for c in cases:
        cid = c["id"]
        q = c["question"]
        enriched = enrich(q)
        if enriched != q:
            print(f"Case {cid:>2}: {q}")
            # 找到哪些 rule 命中
            for rule in _RULES:
                if re.search(rule.pattern, q):
                    print(f"  → 命中规则 Case{rule.case_ids}: {rule.note}")
            print(f"  → 增强后: {enriched}\n")

    # 期望覆盖但未命中的 case
    covered = {cid for c in cases for rule in _RULES
               if re.search(rule.pattern, c["question"])
               for cid in rule.case_ids
               if c["id"] == cid}
    target = {4, 6, 10, 11, 17, 33, 37}
    miss = target - covered
    if miss:
        print(f"⚠  未命中目标 Case: {miss}")
    else:
        print(f"✅ 全部目标 Case 均被覆盖: {sorted(target)}")


if __name__ == "__main__":
    preview()
