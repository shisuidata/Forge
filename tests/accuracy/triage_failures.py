#!/usr/bin/env python3
"""
EA 失败归因工具。

输入 evaluate_ea.py 生成的 ea.json、runner.py 生成的 runs.json，以及测试 cases；
输出机器可读 JSON 和 Markdown 报告，用于把准确率失败转成工程 backlog。

用法：
  python tests/accuracy/triage_failures.py --method u
  python tests/accuracy/triage_failures.py --method u --out docs/accuracy-triage-u.md
"""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ACCURACY_DIR = Path(__file__).parent
RESULTS_DIR = ACCURACY_DIR / "results"


ROOT_CAUSES = {
    "compile_error": "生成合法性/编译失败",
    "exec_error": "执行失败",
    "topn_filter": "组内 TopN 结构缺失",
    "metric_semantics": "指标口径/业务语义缺失",
    "filter_semantics": "复杂过滤/字段约定缺失",
    "temporal_window": "时序/窗口语义错误",
    "join_semantics": "JOIN/ANTI/SEMI 语义错误",
    "aggregation_shape": "聚合粒度/输出结构错误",
    "complex_reasoning": "综合复杂推理",
    "unknown": "待人工归因",
}

NEXT_ACTIONS = {
    "compile_error": "补 schema/compiler guard 或 retry 错误反馈。",
    "exec_error": "检查方言兼容和编译器输出，优先补执行回归测试。",
    "topn_filter": "补 lint：组内排名必须有 qualify；补 TopN 示例和回归测试。",
    "metric_semantics": "沉淀到 metrics/disambiguations Registry，并补业务口径测试。",
    "filter_semantics": "把字段约定转成 lint 或 field_conventions 规则。",
    "temporal_window": "补窗口函数示例、排序方向 lint、NULL 尾行过滤规则。",
    "join_semantics": "补 JOIN 类型/主从表方向 lint，优先覆盖 ANTI/SEMI。",
    "aggregation_shape": "检查 group/select/agg 粒度，补 compiler/lint 回归测试。",
    "complex_reasoning": "拆成子模式，判断是否需要澄清、Registry 或多步计划。",
    "unknown": "人工查看 reference SQL 与 generated SQL 后再归因。",
}


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _text_blob(*parts: str) -> str:
    return "\n".join(p or "" for p in parts).lower()


def classify_failure(
    *,
    category: str,
    question: str,
    reason: str,
    reference_sql: str,
    generated_sqls: list[str],
) -> str:
    blob = _text_blob(category, question, reason, reference_sql, "\n".join(generated_sqls))
    reason_l = reason.lower()

    if "json解析失败" in reason or "编译失败" in reason or "重试耗尽" in reason:
        return "compile_error"
    if "exec error" in reason_l:
        return "exec_error"

    asks_topn = bool(
        re.search(r"(各|每个|每组|每类|组内)", question)
        and re.search(r"(前\s*\d+|top\s*\d+|排名|最高|最多|最低|最少)", question, re.I)
    )
    has_rank = any(re.search(r"\b(row_number|rank|dense_rank)\s*\(", sql, re.I) for sql in generated_sqls)
    has_qualify = any(re.search(r"\bqualify\b", sql, re.I) for sql in generated_sqls)
    has_rank_filter = any(
        re.search(r"\b(where|having|qualify)\b[\s\S]{0,300}\b(rn|rank|sales_rank|row_num|row_number)\b\s*(<=|<|=)", sql, re.I)
        for sql in generated_sqls
    )
    if asks_topn and (not has_rank or not (has_qualify or has_rank_filter)):
        return "topn_filter"

    if any(word in blob for word in ("退款率", "退货率", "复购率", "客单价", "转化率", "留存率")):
        return "metric_semantics"

    if any(word in blob for word in ("已完成", "进口", "评分", "图片", "客单价在", "之间", "过滤")):
        return "filter_semantics"

    if any(word in blob for word in ("lag", "lead", "上一", "下一", "环比", "同比", "最近", "间隔", "窗口")):
        return "temporal_window"

    if any(word in blob for word in ("anti", "semi", "没有", "未购买", "未下单", "join", "left join")):
        return "join_semantics"

    if any(word in blob for word in ("group by", "having", "聚合", "分组", "平均", "总", "count", "sum", "avg")):
        return "aggregation_shape"

    if "综合" in category or "复杂" in category:
        return "complex_reasoning"

    return "unknown"


def build_triage(cases: list[dict], runs_data: dict, ea_report: dict) -> dict:
    cases_by_id = {str(case["id"]): case for case in cases}
    failures = []
    counts: Counter[str] = Counter()
    category_counts: dict[str, Counter[str]] = defaultdict(Counter)

    for cid, result in sorted(ea_report.get("case_results", {}).items(), key=lambda x: int(x[0])):
        if result.get("any_correct"):
            continue
        case = cases_by_id.get(str(cid), {})
        run_entry = runs_data.get(str(cid), {})
        runs = run_entry.get("runs", [])
        generated_sqls = [r.get("sql") or "" for r in runs if isinstance(r, dict)]
        reasons = [
            r.get("reason", "")
            for r in result.get("runs", [])
            if r.get("correct") is not True
        ]
        reason = reasons[0] if reasons else ""
        root_cause = classify_failure(
            category=result.get("category") or case.get("category", "unknown"),
            question=result.get("question") or case.get("question", ""),
            reason=reason,
            reference_sql=case.get("reference_sql", ""),
            generated_sqls=generated_sqls,
        )
        counts[root_cause] += 1
        category_counts[result.get("category", "unknown")][root_cause] += 1
        failures.append(
            {
                "case_id": str(cid),
                "category": result.get("category", case.get("category", "unknown")),
                "question": result.get("question", case.get("question", "")),
                "root_cause": root_cause,
                "root_cause_label": ROOT_CAUSES[root_cause],
                "reason": reason,
                "next_action": NEXT_ACTIONS[root_cause],
                "reference_sql": case.get("reference_sql", ""),
                "generated_sql": generated_sqls[0] if generated_sqls else "",
            }
        )

    return {
        "method": ea_report.get("method", ""),
        "ea": ea_report.get("ea", 0),
        "run_accuracy": ea_report.get("run_accuracy", 0),
        "total_failures": len(failures),
        "root_cause_counts": dict(counts.most_common()),
        "category_root_cause_counts": {
            category: dict(counter.most_common())
            for category, counter in sorted(category_counts.items())
        },
        "failures": failures,
    }


def render_markdown(triage: dict) -> str:
    lines = [
        f"# Method {triage['method']} EA 失败归因",
        "",
        "## 摘要",
        "",
        f"- EA：{triage['ea']:.1%}",
        f"- Run ACC：{triage['run_accuracy']:.1%}",
        f"- 失败案例：{triage['total_failures']}",
        "",
        "## 根因分布",
        "",
        "| 根因 | 数量 | 下一步 |",
        "|---|---:|---|",
    ]
    for root, count in triage["root_cause_counts"].items():
        lines.append(f"| {ROOT_CAUSES[root]} | {count} | {NEXT_ACTIONS[root]} |")

    lines += [
        "",
        "## 分类 × 根因",
        "",
        "| 类别 | 根因 | 数量 |",
        "|---|---|---:|",
    ]
    for category, counter in triage["category_root_cause_counts"].items():
        for root, count in counter.items():
            lines.append(f"| {category} | {ROOT_CAUSES[root]} | {count} |")

    lines += ["", "## 失败明细", ""]
    for item in triage["failures"]:
        lines += [
            f"### Case {item['case_id']} · {item['category']}",
            "",
            f"- 问题：{item['question']}",
            f"- 根因：{item['root_cause_label']}",
            f"- 评估原因：{item['reason'] or '(empty)'}",
            f"- 下一步：{item['next_action']}",
            "",
            "```sql",
            item["generated_sql"].strip() or "-- empty",
            "```",
            "",
        ]
    return "\n".join(lines).rstrip() + "\n"


def default_paths(method: str) -> tuple[Path, Path, Path]:
    method_dir = RESULTS_DIR / f"method_{method}"
    return (
        RESULTS_DIR / "cases_large.json",
        method_dir / "runs.json",
        method_dir / "ea.json",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="EA 失败归因工具")
    parser.add_argument("--method", required=True, help="method id，如 u/r/v")
    parser.add_argument("--cases", type=Path, default=None)
    parser.add_argument("--runs", type=Path, default=None)
    parser.add_argument("--ea", type=Path, default=None)
    parser.add_argument("--out", type=Path, default=None, help="Markdown 输出路径")
    parser.add_argument("--json-out", type=Path, default=None, help="JSON 输出路径")
    args = parser.parse_args()

    cases_path, runs_path, ea_path = default_paths(args.method)
    cases_path = args.cases or cases_path
    runs_path = args.runs or runs_path
    ea_path = args.ea or ea_path

    cases = load_json(cases_path)
    runs_data = load_json(runs_path)
    ea_report = load_json(ea_path)
    triage = build_triage(cases, runs_data, ea_report)

    out_md = args.out or (runs_path.parent / "failure_triage.md")
    out_json = args.json_out or (runs_path.parent / "failure_triage.json")
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(render_markdown(triage), encoding="utf-8")
    out_json.write_text(json.dumps(triage, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"已生成：{out_md}")
    print(f"已生成：{out_json}")


if __name__ == "__main__":
    main()
