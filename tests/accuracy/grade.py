#!/usr/bin/env python3
"""
Step 3 of 3 — 生成三方法对比测试报告

读取 results/cases.json 和 results/results.json，
直接由 Claude 在对话中评分并生成 results/report_v*.md。

三种方法：
  - Method A：旧 Forge DSL 提示词（SQL 术语风格）→ Forge JSON → 编译为 SQL
  - Method B：直接生成 SQL
  - Method C：新 Forge DSL 提示词（声明式风格，去除 SQL 思维）→ Forge JSON → 编译为 SQL

运行：
    python tests/accuracy/grade.py [scores_file]
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

RESULTS_DIR  = Path(__file__).parent / "results"
CASES_FILE   = RESULTS_DIR / "cases.json"
RESULTS_FILE = RESULTS_DIR / "results.json"


def build_grading_prompt(cases: list[dict], results: dict) -> str:
    """构建批量评分 prompt。"""
    has_c = any("runs_c" in v for v in results.values())
    has_d = any("runs_d" in v for v in results.values())
    has_e = any("runs_e" in v for v in results.values())

    lines = [
        "你是一位资深 SQL 评审专家。请对以下测试用例的每一个答案进行打分。",
        "",
        "## 评分标准（满分 10 分）",
        "- 语义正确性（6分）：与参考 SQL 语义等价，能正确回答业务问题",
        "- 结构完整性（2分）：包含业务需要的 JOIN / GROUP BY / HAVING / ORDER BY / 窗口函数等",
        "- 边界处理（2分）：NULL 处理、去重、过滤条件的合理性",
        "",
        "## 说明",
        "- Method A：旧 Forge DSL 提示词（SQL 术语风格）编译得到的 SQL，编译失败则 sql 为 null",
        "- Method B：模型直接生成的 SQL",
    ]
    if has_c:
        lines.append("- Method C：新 Forge DSL 提示词（声明式风格）编译得到的 SQL，编译失败则 sql 为 null")
    if has_d:
        lines.append("- Method D：新 Forge DSL 提示词 + 精准枚举 schema 编译得到的 SQL")
    if has_e:
        lines.append("- Method E：新 Forge DSL + 枚举 schema + 升级版提示词（having alias 规则、LIMIT 精确取值、排名函数对比表、LAG/LEAD default 规则等）编译得到的 SQL")
    lines += ["- 每个方法各运行 5 次，每次独立上下文", "", "## 测试用例及答案", ""]

    cases_by_id = {str(c["id"]): c for c in cases}

    for case_id, result in sorted(results.items(), key=lambda x: int(x[0])):
        case = cases_by_id.get(case_id, {})
        lines.append("---")
        lines.append(f"### 用例 {case_id}（{result['category']}，难度{result['difficulty']}）")
        lines.append(f"**问题**：{result['question']}")
        lines.append("**参考 SQL**：")
        lines.append("```sql")
        lines.append(case.get("reference_sql", "（无）"))
        lines.append("```")
        lines.append("")

        for method, key in (("A", "runs_a"), ("B", "runs_b"), ("C", "runs_c"), ("D", "runs_d"), ("E", "runs_e")):
            runs = result.get(key, [])
            if not runs:
                continue
            for i, run in enumerate(runs, 1):
                if run and run.get("error"):
                    lines.append(f"**{method}-{i}**：❌ 失败（{run['error'][:100]}）")
                else:
                    lines.append(f"**{method}-{i}**：")
                    lines.append("```sql")
                    lines.append((run or {}).get("sql", "null") or "null")
                    lines.append("```")
        lines.append("")

    lines += [
        "---", "",
        "## 输出格式要求", "",
        "输出一个 JSON 对象，key 为用例 ID（字符串），value 包含各方法评分：", "",
        "```json",
        "{",
        '  "1": {',
        '    "method_a": {"scores": [8,7,8,6,8], "avg": 7.4, "comment": "评语..."},',
        '    "method_b": {"scores": [6,7,5,7,6], "avg": 6.2, "comment": "评语..."},',
        '    "method_c": {"scores": [9,8,9,9,8], "avg": 8.6, "comment": "评语..."}',
        "  }",
        "}",
        "```",
        "",
        "scores 按运行顺序（-1 到 -5），失败的运行打 0 分。只输出 JSON，不要任何其他文字。",
    ]
    return "\n".join(lines)


def build_report(cases: list[dict], results: dict, scores: dict) -> str:
    """根据评分结果生成 Markdown 报告。"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cases_by_id = {str(c["id"]): c for c in cases}

    methods = ["A", "SQL", "C", "D", "E"]
    method_keys = {"A": "method_a", "SQL": "method_b", "C": "method_c", "D": "method_d", "E": "method_e"}
    method_labels = {
        "A":   "Method A（旧 Forge DSL）",
        "SQL": "Method SQL（直接生成 SQL）",
        "C":   "Method C（新 Forge DSL）",
        "D":   "Method D（新 Forge DSL + 枚举 schema）",
        "E":   "Method E（枚举 schema + 升级提示词）",
    }
    # scores 文件里 B 对应 method_b，兼容旧文件
    for sc in scores.values():
        if "method_b" in sc and "method_sql" not in sc:
            sc["method_sql"] = sc["method_b"]  # 别名映射

    # 过滤掉没有数据的方法
    active_methods = [m for m in methods
                      if any(method_keys[m] in sc for sc in scores.values())]

    avgs: dict[str, list[float]] = {m: [] for m in active_methods}
    compile_errors: dict[str, int] = {m: 0 for m in ("A", "C", "D", "E")}
    compile_total:  dict[str, int] = {m: 0 for m in ("A", "C", "D", "E")}

    for case_id, sc in scores.items():
        result = results.get(case_id, {})
        for m in active_methods:
            mk = method_keys[m]
            if sc.get(mk, {}).get("avg") is not None:
                avgs[m].append(sc[mk]["avg"])
            if m in ("A", "C", "D", "E"):
                run_key = f"runs_{m.lower()}"
                for run in result.get(run_key, []):
                    compile_total[m] += 1
                    if run and run.get("error"):
                        compile_errors[m] += 1

    overall = {m: (sum(avgs[m]) / len(avgs[m]) if avgs[m] else 0) for m in active_methods}

    # 分类统计
    cat_avgs: dict[str, dict[str, list]] = defaultdict(lambda: {m: [] for m in active_methods})
    for case_id, sc in scores.items():
        cat = cases_by_id.get(case_id, {}).get("category", "?")
        for m in active_methods:
            mk = method_keys[m]
            if sc.get(mk, {}).get("avg") is not None:
                cat_avgs[cat][m].append(sc[mk]["avg"])

    lines = [
        "# Forge DSL 准确性测试报告 — 三方法对比",
        "",
        f"> 生成时间：{now}",
        f"> 测试用例：{len(cases)} 个  ·  每方法运行 {5} 次",
        "",
        "## 总体结果", "",
    ]

    # 总分表头
    header_cols = " | ".join(f"**{method_labels[m]}**" for m in active_methods)
    lines.append(f"| 指标 | {header_cols} |")
    lines.append("|---|" + "---|" * len(active_methods))

    score_row = " | ".join(f"**{overall[m]:.2f}**" for m in active_methods)
    lines.append(f"| 总均分 | {score_row} |")

    # 编译失败率行
    fail_cols = []
    for m in active_methods:
        if m in ("A", "C", "D", "E") and compile_total.get(m, 0) > 0:
            fail_cols.append(f"{compile_errors[m]/compile_total[m]*100:.1f}%")
        else:
            fail_cols.append("N/A")
    lines.append(f"| 编译失败率 | {' | '.join(fail_cols)} |")

    # 胜负统计
    wins = {m: 0 for m in active_methods}
    ties = 0
    for sc in scores.values():
        case_scores = {m: (sc.get(method_keys[m], {}).get("avg") or 0) for m in active_methods}
        best = max(case_scores.values())
        winners = [m for m, v in case_scores.items() if v == best]
        if len(winners) == 1:
            wins[winners[0]] += 1
        else:
            ties += 1
    win_cols = " | ".join(f"{wins[m]}" for m in active_methods)
    lines.append(f"| 胜出用例数（平局{ties}） | {win_cols} |")
    lines.append("")

    # 最优方法结论
    best_m = max(active_methods, key=lambda m: overall[m])
    lines.append(f"**结论**：{method_labels[best_m]} 总均分最高（{overall[best_m]:.2f}）。")
    if "A" in active_methods and "E" in active_methods:
        diff = overall["E"] - overall["A"]
        sign = f"+{diff:.2f}" if diff >= 0 else f"{diff:.2f}"
        lines.append(f"新提示词 E 相对旧提示词 A：{sign} 分。")
    elif "A" in active_methods and "D" in active_methods:
        diff = overall["D"] - overall["A"]
        sign = f"+{diff:.2f}" if diff >= 0 else f"{diff:.2f}"
        lines.append(f"新提示词 D 相对旧提示词 A：{sign} 分。")
    if "SQL" in active_methods:
        diff = overall[best_m] - overall["SQL"]
        sign = f"+{diff:.2f}" if diff >= 0 else f"{diff:.2f}"
        lines.append(f"最优 Forge 方法相对直接 SQL：{sign} 分。")
    lines += ["", "---", "", "## 分类得分", ""]

    cat_header = " | ".join(f"A均分 | B均分 | C均分" if "C" in active_methods else "A均分 | B均分"
                            for _ in [""])
    lines.append(f"| 类别 | 用例数 | " + " | ".join(f"{method_labels[m]}" for m in active_methods) + " |")
    lines.append("|---|---|" + "---|" * len(active_methods))

    categories = ['多表JOIN+聚合', '复杂过滤', '分组+HAVING', '排名与TopN',
                  '窗口聚合', '时序导航', 'ANTI/SEMI JOIN', '综合复杂查询']
    for cat in categories:
        if cat not in cat_avgs:
            continue
        n = len(cat_avgs[cat][active_methods[0]])
        score_cells = " | ".join(
            f"{sum(cat_avgs[cat][m])/len(cat_avgs[cat][m]):.2f}" if cat_avgs[cat][m] else "—"
            for m in active_methods
        )
        lines.append(f"| {cat} | {n} | {score_cells} |")

    lines += ["", "---", "", "## 逐用例得分", ""]
    col_heads = " | ".join(f"{m}均分" for m in active_methods)
    lines.append(f"| Case | 类别 | 难度 | {col_heads} | 优胜 | A评语 |")
    lines.append("|---|---|---|" + "---|" * len(active_methods) + "---|---|")

    for case_id in sorted(scores.keys(), key=int):
        sc   = scores[case_id]
        case = cases_by_id.get(case_id, {})
        case_scores = {m: (sc.get(method_keys[m], {}).get("avg") or 0) for m in active_methods}
        best_val = max(case_scores.values())
        winners = [m for m, v in case_scores.items() if v == best_val]
        winner_str = "/".join(winners)
        score_cells = " | ".join(f"{case_scores[m]:.1f}" for m in active_methods)
        comment = sc.get("method_a", {}).get("comment", "")[:45]
        lines.append(f"| {case_id} | {case.get('category','?')} | {case.get('difficulty','?')} "
                     f"| {score_cells} | {winner_str} | {comment} |")

    lines += ["", "---", "", "## 残余问题 & 改进建议", ""]
    lines += [
        "根据本轮测试，主要改进方向：",
        "",
        "1. **复杂过滤**：OR+AND 嵌套对模型仍有挑战，filter 数组格式需持续强化",
        "2. **时序导航**：LAG/LEAD partition 语义，需确保模型正确理解分组范围",
        "3. **多窗口叠加**（综合复杂查询）：两个或更多 window 函数同时使用时，结果稳定性有待提升",
        "4. **join 类型选择**：LEFT JOIN 泛滥问题在新提示词中是否有改善，见分类得分对比",
    ]

    return "\n".join(lines)


def main() -> None:
    for f in (CASES_FILE, RESULTS_FILE):
        if not f.exists():
            print(f"❌ 找不到 {f}", file=sys.stderr)
            sys.exit(1)

    cases   = json.loads(CASES_FILE.read_text())
    results = json.loads(RESULTS_FILE.read_text())
    print(f"📋 加载 {len(cases)} 个用例，{len(results)} 份结果")

    # 支持命令行传入 scores 文件
    if len(sys.argv) > 1:
        scores_file = Path(sys.argv[1])
    else:
        scores_file = RESULTS_DIR / "scores.json"

    if scores_file.exists():
        scores = json.loads(scores_file.read_text())
        print(f"✅ 加载评分文件 → {scores_file}")
    else:
        prompt = build_grading_prompt(cases, results)
        prompt_file = RESULTS_DIR / "grading_prompt.txt"
        prompt_file.write_text(prompt)
        print(f"📝 评分 prompt 已写入 → {prompt_file}")
        print("⚠  请完成评分后重新运行 grade.py 生成报告")
        sys.exit(0)

    report_file = RESULTS_DIR / f"report_v3_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.md"
    report_md = build_report(cases, results, scores)
    report_file.write_text(report_md)
    print(f"📊 报告已生成 → {report_file}")

    # 终端摘要
    method_keys = {"A": "method_a", "B": "method_b", "C": "method_c"}
    method_labels = {"A": "旧 Forge DSL", "B": "直接 SQL", "C": "新 Forge DSL"}
    print("\n=== 结果摘要 ===")
    for m, mk in method_keys.items():
        avgs = [sc[mk]["avg"] for sc in scores.values() if sc.get(mk, {}).get("avg") is not None]
        if avgs:
            print(f"Method {m}（{method_labels[m]}）均分：{sum(avgs)/len(avgs):.2f}")


if __name__ == "__main__":
    main()
