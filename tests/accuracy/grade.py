#!/usr/bin/env python3
"""
Step 3 of 3 — 评分并生成单方法报告

用法：
    python tests/accuracy/grade.py --method f
        → 读 results/method_f/runs.json
        → 若无 scores.json：生成评分 prompt，退出
        → 若有 scores.json：生成 results/method_f/report_{ts}.md

    python tests/accuracy/grade.py --method f --scores /path/to/scores.json
        → 使用指定评分文件生成报告

兼容旧格式（legacy/）：
    python tests/accuracy/grade.py --legacy
        → 读 results/legacy/ 中的 results.json + scores_v3.json，生成多方法对比报告
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ACCURACY_DIR = Path(__file__).parent
RESULTS_DIR  = ACCURACY_DIR / "results"
CASES_FILE   = RESULTS_DIR / "cases.json"


# ── 数据加载 ─────────────────────────────────────────────────────────────────

def load_cases() -> list[dict]:
    if not CASES_FILE.exists():
        print(f"❌ 找不到 {CASES_FILE}", file=sys.stderr)
        sys.exit(1)
    return json.loads(CASES_FILE.read_text())


def load_runs_new(method_id: str) -> dict:
    """加载新格式：results/method_{id}/runs.json"""
    path = RESULTS_DIR / f"method_{method_id}" / "runs.json"
    if not path.exists():
        print(f"❌ 找不到 {path}，请先运行 runner.py --method {method_id}", file=sys.stderr)
        sys.exit(1)
    return json.loads(path.read_text())


# ── 评分 Prompt 生成 ──────────────────────────────────────────────────────────

def build_grading_prompt(method_id: str, label: str,
                          cases: list[dict], runs: dict) -> str:
    cases_by_id = {str(c["id"]): c for c in cases}
    lines = [
        "你是一位资深 SQL 评审专家。请对以下测试用例的每一次运行结果进行打分。",
        "",
        "## 评分标准（满分 10 分）",
        "- 语义正确性（6分）：与参考 SQL 语义等价，能正确回答业务问题",
        "- 结构完整性（2分）：包含业务需要的 JOIN / GROUP BY / HAVING / ORDER BY / 窗口函数等",
        "- 边界处理（2分）：NULL 处理、去重、过滤条件的合理性",
        "",
        f"## 测试方法：{label}",
        "每个用例运行 5 次，每次独立上下文。",
        "",
        "## 测试用例及答案",
        "",
    ]

    for case_id in sorted(runs.keys(), key=int):
        case_runs = runs[case_id]
        ref_sql = cases_by_id.get(case_id, {}).get("reference_sql", "（无）")
        lines.append("---")
        lines.append(f"### 用例 {case_id}（{case_runs['category']}，难度{case_runs['difficulty']}）")
        lines.append(f"**问题**：{case_runs['question']}")
        lines.append("**参考 SQL**：")
        lines.append("```sql")
        lines.append(ref_sql)
        lines.append("```")
        lines.append("")

        for i, run in enumerate(case_runs.get("runs", []), 1):
            if run and run.get("error"):
                lines.append(f"**run-{i}**：❌ 失败（{run['error'][:100]}）")
            else:
                lines.append(f"**run-{i}**：")
                lines.append("```sql")
                lines.append((run or {}).get("sql", "null") or "null")
                lines.append("```")
        lines.append("")

    lines += [
        "---", "",
        "## 输出格式要求", "",
        "输出一个 JSON 对象，key 为用例 ID（字符串），value 包含评分：", "",
        "```json",
        "{",
        '  "1": {"scores": [8,7,8,6,8], "avg": 7.4, "comment": "评语..."},',
        '  "2": {"scores": [9,9,9,9,9], "avg": 9.0, "comment": "评语..."}',
        "}",
        "```",
        "",
        "scores 按运行顺序（run-1 到 run-5），失败的运行打 0 分。只输出 JSON，不要任何其他文字。",
    ]
    return "\n".join(lines)


# ── 单方法报告生成 ─────────────────────────────────────────────────────────────

def build_single_report(method_id: str, label: str,
                         cases: list[dict], runs: dict, scores: dict) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cases_by_id = {str(c["id"]): c for c in cases}

    avgs = [sc["avg"] for sc in scores.values() if sc.get("avg") is not None]
    overall = sum(avgs) / len(avgs) if avgs else 0

    # 编译失败率（仅 forge 方法有意义）
    all_runs = [r for v in runs.values() for r in v.get("runs", [])]
    ok  = sum(1 for r in all_runs if r and not r.get("error"))
    err = sum(1 for r in all_runs if r and r.get("error"))
    total_runs = ok + err
    fail_rate = f"{err/total_runs*100:.1f}%" if total_runs > 0 else "N/A"

    # 分类统计
    cat_avgs: dict[str, list] = defaultdict(list)
    for case_id, sc in scores.items():
        cat = cases_by_id.get(case_id, {}).get("category", "?")
        if sc.get("avg") is not None:
            cat_avgs[cat].append(sc["avg"])

    lines = [
        f"# Method {method_id.upper()} 准确性测试报告",
        f"**{label}**",
        "",
        f"> 生成时间：{now}",
        f"> 测试用例：{len(cases)} 个  ·  每用例运行 5 次",
        "",
        "## 总体结果",
        "",
        f"| 指标 | 值 |",
        "|---|---|",
        f"| **总均分** | **{overall:.2f}** |",
        f"| 编译失败率 | {fail_rate} |",
        f"| 有效评分用例 | {len(avgs)} / {len(cases)} |",
        "",
        "## 分类得分",
        "",
        "| 类别 | 用例数 | 均分 |",
        "|---|---|---|",
    ]

    categories = ['多表JOIN+聚合', '复杂过滤', '分组+HAVING', '排名与TopN',
                  '窗口聚合', '时序导航', 'ANTI/SEMI JOIN', '综合复杂查询']
    for cat in categories:
        if cat not in cat_avgs:
            continue
        vals = cat_avgs[cat]
        lines.append(f"| {cat} | {len(vals)} | {sum(vals)/len(vals):.2f} |")

    lines += ["", "## 逐用例得分", "",
              "| Case | 类别 | 难度 | 均分 | 评语 |",
              "|---|---|---|---|---|"]

    for case_id in sorted(scores.keys(), key=int):
        sc   = scores[case_id]
        case = cases_by_id.get(case_id, {})
        avg  = sc.get("avg", "—")
        comment = sc.get("comment", "")[:60]
        avg_str = f"{avg:.1f}" if isinstance(avg, float) else str(avg)
        lines.append(f"| {case_id} | {case.get('category','?')} | {case.get('difficulty','?')} "
                     f"| {avg_str} | {comment} |")

    return "\n".join(lines)


# ── 主流程 ────────────────────────────────────────────────────────────────────

def run_new(method_id: str, scores_path: str | None) -> None:
    from methods import load  # noqa: E402
    try:
        cfg = load(method_id)
        label = cfg.label
    except Exception:
        label = f"Method {method_id.upper()}"

    cases = load_cases()
    runs  = load_runs_new(method_id)
    print(f"📋 加载 {len(cases)} 个用例，{len(runs)} 份结果 → method_{method_id}")

    # 确定 scores 文件
    method_result_dir = RESULTS_DIR / f"method_{method_id}"
    if scores_path:
        sf = Path(scores_path)
    else:
        sf = method_result_dir / "scores.json"

    if sf.exists():
        scores = json.loads(sf.read_text())
        print(f"✅ 加载评分文件 → {sf}")
    else:
        # 生成评分 prompt
        prompt = build_grading_prompt(method_id, label, cases, runs)
        pf = method_result_dir / "grading_prompt.txt"
        pf.write_text(prompt)
        print(f"📝 评分 prompt 已写入 → {pf}")
        print("⚠  请完成评分后将 scores.json 放入同目录，然后重新运行")
        sys.exit(0)

    report_md = build_single_report(method_id, label, cases, runs, scores)
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    rf = method_result_dir / f"report_{ts}.md"
    rf.write_text(report_md)
    print(f"📊 报告已生成 → {rf}")

    # 摘要
    avgs = [sc["avg"] for sc in scores.values() if sc.get("avg") is not None]
    if avgs:
        print(f"\n=== 结果摘要 ===")
        print(f"  {label} 总均分：{sum(avgs)/len(avgs):.2f}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Forge DSL 准确性测试评分器")
    parser.add_argument("--method", "-m", default="",
                        help="方法 id（如 'f'）")
    parser.add_argument("--scores", default=None,
                        help="指定评分 JSON 文件路径（可选）")
    parser.add_argument("--cases", default=None,
                        help="测试用例文件（默认 results/cases.json），如 results/cases_large.json")
    # 兼容旧的位置参数（直接传 scores 文件）
    parser.add_argument("scores_file", nargs="?", default=None,
                        help="[旧格式兼容] 直接传入 scores 文件路径")
    args = parser.parse_args()

    global CASES_FILE
    if args.cases:
        p = Path(args.cases)
        CASES_FILE = p if p.is_absolute() else ACCURACY_DIR / args.cases

    if args.method:
        run_new(args.method, args.scores or args.scores_file)
    elif args.scores_file:
        # 旧格式兼容：grade.py scores_v3.json → 使用 legacy 处理
        _run_legacy(args.scores_file)
    else:
        parser.print_help()
        sys.exit(1)


def _run_legacy(scores_file_path: str) -> None:
    """兼容旧版多方法 grade.py 调用（读 legacy/ 格式）。"""
    legacy_dir = RESULTS_DIR / "legacy"
    cases_file = legacy_dir / "cases.json"
    results_file = legacy_dir / "results.json"

    # cases 可能在 legacy 或 results 目录
    if not cases_file.exists():
        cases_file = CASES_FILE
    if not cases_file.exists() or not results_file.exists():
        print("❌ legacy 模式需要 results/legacy/results.json 和 cases.json", file=sys.stderr)
        sys.exit(1)

    cases   = json.loads(cases_file.read_text())
    results = json.loads(results_file.read_text())
    scores  = json.loads(Path(scores_file_path).read_text())

    from _grade_legacy import build_report  # type: ignore
    report = build_report(cases, results, scores)
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    rf = legacy_dir / f"report_v3_{ts}.md"
    rf.write_text(report)
    print(f"📊 Legacy 报告已生成 → {rf}")


if __name__ == "__main__":
    main()
