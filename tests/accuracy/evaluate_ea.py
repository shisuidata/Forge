"""
Execution Accuracy (EA) 评估器

对比生成 SQL 与参考 SQL 在 large_demo.db 上的执行结果，
自动计算每个 method 的准确率，不需要 LLM 打分。

用法：
  python evaluate_ea.py --methods k b_large b_large_sem
  python evaluate_ea.py --methods k --cases results/cases_large.json
"""

import argparse
import json
import sqlite3
from itertools import combinations, permutations
from pathlib import Path

ACCURACY_DIR = Path(__file__).parent
RESULTS_DIR  = ACCURACY_DIR / "results"
DB_PATH      = ACCURACY_DIR.parent.parent / "demo" / "large_demo.db"


# ── SQL 执行 ──────────────────────────────────────────────────────────────────

def _exec(conn: sqlite3.Connection, sql: str):
    """执行 SQL，返回 (rows, error)。rows 为排序后的元组列表。"""
    if not sql or not sql.strip():
        return [], "empty SQL"
    try:
        cur = conn.execute(sql)
        rows = cur.fetchall()
        # 规范化：转为字符串元组，忽略列名差异，按行内容排序
        normalized = sorted(
            tuple(str(v) if v is not None else "NULL" for v in row)
            for row in rows
        )
        return normalized, None
    except Exception as e:
        return [], str(e)


# ── 结果比对 ──────────────────────────────────────────────────────────────────

def _numeric_approx(a: str, b: str) -> bool:
    """判断两个字符串是否表示近似相等的数值。容差：绝对 0.005 或相对 0.001（0.1%）。"""
    try:
        fa, fb = float(a), float(b)
        if fa == fb:
            return True
        diff = abs(fa - fb)
        if diff < 0.005:
            return True
        max_val = max(abs(fa), abs(fb))
        return max_val > 0 and diff / max_val < 0.001
    except (ValueError, OverflowError):
        return a == b  # 非数字则精确比较


def _compare(ref_rows: list, gen_rows: list) -> bool:
    """
    判断两个结果集是否等价。
    - 忽略列名（按位置比较）
    - 忽略行顺序（已排序）
    - 允许空集 == 空集
    - 宽松列匹配 Layer 1：gen 列数 > ref 列数时，截取 gen 的前 N 列再比较
    - 宽松列匹配 Layer 2：列序无关匹配
        若行数相同且 gen_width >= ref_width（≤6），枚举 gen 列的所有组合+排列，
        检查是否存在某种列投影使得结果完全吻合。
        用于捕获"值正确但列序不同"的情况，如 (brand, count, revenue) vs (brand, revenue, count)。
    - 宽松数值匹配 Layer 3：对 Layer 1 的截取列结果，逐行逐格进行数值近似比较
        容差 0.005 绝对值 / 0.1% 相对，用于捕获 ROUND 精度差异（如 '0.5652' vs '0.565217...'）。
    """
    if ref_rows == gen_rows:
        return True
    if not ref_rows or not gen_rows:
        return ref_rows == gen_rows

    ref_width = len(ref_rows[0]) if ref_rows else 0
    gen_width = len(gen_rows[0]) if gen_rows else 0

    if len(ref_rows) != len(gen_rows):
        return False

    # Layer 1：前 N 列截取
    if ref_width != gen_width and ref_width > 0 and gen_width > 0:
        n = min(ref_width, gen_width)
        trimmed_ref = sorted(row[:n] for row in ref_rows)
        trimmed_gen = sorted(row[:n] for row in gen_rows)
        if trimmed_ref == trimmed_gen:
            return True
        # Layer 3：数值近似比较（处理 ROUND 精度差异，如 '0.5652' vs '0.565217...'）
        if all(
            all(_numeric_approx(a, b) for a, b in zip(rr, gr))
            for rr, gr in zip(trimmed_ref, trimmed_gen)
        ):
            return True

    # Layer 3b：等宽但精度不同（如 '500.0' vs '500'）
    if ref_width == gen_width and ref_width > 0:
        ref_sorted = sorted(ref_rows)
        gen_sorted = sorted(gen_rows)
        if all(
            all(_numeric_approx(a, b) for a, b in zip(rr, gr))
            for rr, gr in zip(ref_sorted, gen_sorted)
        ):
            return True

    # Layer 2：列投影匹配
    # 先用列值指纹缩小候选集，再尝试排列，避免全量 C(gen,ref)×ref! 爆炸
    if ref_width >= 1 and gen_width >= ref_width:
        ref_sorted = sorted(ref_rows)
        # 为每列建「排序值签名」，快速过滤候选
        gen_sigs = [tuple(sorted(str(row[g]) for row in gen_rows)) for g in range(gen_width)]
        ref_sigs = [tuple(sorted(str(row[r]) for row in ref_rows)) for r in range(ref_width)]
        # ref 的每一列，找 gen 中值签名匹配（或数值近似匹配）的候选列
        candidates = []
        for r in range(ref_width):
            cands = []
            for g in range(gen_width):
                if gen_sigs[g] == ref_sigs[r]:
                    cands.append(g)
                # 数值近似签名：当精度不同时（如 0.1893 vs 18.93）签名不同，另行检查
                elif all(
                    _numeric_approx(str(row_r[r]), str(row_g[g]))
                    for row_r, row_g in zip(ref_sorted, sorted(gen_rows))
                ):
                    cands.append(g)
            if not cands:
                candidates = None
                break
            candidates.append(cands)

        if candidates is not None:
            from itertools import product as iproduct
            for col_mapping in iproduct(*candidates):
                if len(set(col_mapping)) != ref_width:   # 要求单射（不重复引用同一列）
                    continue
                projected = sorted(tuple(row[g] for g in col_mapping) for row in gen_rows)
                if projected == ref_sorted:
                    return True
                if all(
                    all(_numeric_approx(a, b) for a, b in zip(rr, gr))
                    for rr, gr in zip(projected, ref_sorted)
                ):
                    return True

    return False


# ── 评估单个 method ────────────────────────────────────────────────────────────

def evaluate_method(method_id: str, cases_file: Path, conn: sqlite3.Connection) -> dict:
    """对单个 method 的所有 runs 做 EA 评估。"""
    method_dir = RESULTS_DIR / f"method_{method_id}"
    runs_path  = method_dir / "runs.json"
    if not runs_path.exists():
        print(f"  ⚠ {runs_path} 不存在，跳过")
        return {}

    with open(runs_path) as f:
        runs_data = json.load(f)
    with open(cases_file) as f:
        cases = {str(c["id"]): c for c in json.load(f)}

    results: dict[str, dict] = {}
    cat_stats = {}

    def _sid(x):
        try: return (0, int(x[0]))
        except ValueError: return (1, x[0])

    for cid, entry in sorted(runs_data.items(), key=_sid):
        case      = cases.get(cid, {})
        ref_sql   = case.get("reference_sql", "")
        category  = case.get("category", "unknown")
        question  = case.get("question", "")

        ref_rows, ref_err = _exec(conn, ref_sql)

        run_results = []
        for run in entry.get("runs", []):
            gen_sql = run.get("sql", "") or ""
            if run.get("error"):
                run_results.append({"correct": False, "reason": run["error"]})
                continue
            gen_rows, gen_err = _exec(conn, gen_sql)
            if gen_err:
                run_results.append({"correct": False, "reason": f"exec error: {gen_err}"})
            elif ref_err:
                # 参考 SQL 执行失败（参考 SQL 本身有问题）
                run_results.append({"correct": None, "reason": f"ref error: {ref_err}"})
            else:
                correct = _compare(ref_rows, gen_rows)
                run_results.append({
                    "correct": correct,
                    "ref_rows": len(ref_rows),
                    "gen_rows": len(gen_rows),
                    "reason": "" if correct else f"ref={len(ref_rows)}行, gen={len(gen_rows)}行"
                })

        # 案例是否正确：任意一次 run 正确即视为正确（same as EA convention）
        any_correct  = any(r["correct"] is True for r in run_results)
        all_correct  = all(r["correct"] is True for r in run_results)
        valid_runs   = [r for r in run_results if r["correct"] is not None]
        correct_runs = sum(1 for r in valid_runs if r["correct"])

        results[cid] = {
            "question": question,
            "category": category,
            "any_correct": any_correct,
            "all_correct": all_correct,
            "correct_runs": correct_runs,
            "total_runs": len(valid_runs),
            "runs": run_results,
        }

        # 分类统计
        if category not in cat_stats:
            cat_stats[category] = {"correct": 0, "total": 0, "correct_runs": 0, "total_runs": 0}
        cat_stats[category]["total"] += 1
        cat_stats[category]["total_runs"] += len(valid_runs)
        cat_stats[category]["correct_runs"] += correct_runs
        if any_correct:
            cat_stats[category]["correct"] += 1

    # 整体统计
    total_cases   = len(results)
    correct_cases = sum(1 for r in results.values() if r["any_correct"])
    all_cases     = sum(1 for r in results.values() if r["all_correct"])
    total_runs    = sum(r["total_runs"] for r in results.values())
    correct_runs_total = sum(r["correct_runs"] for r in results.values())
    ea            = correct_cases / total_cases if total_cases else 0.0
    run_acc       = correct_runs_total / total_runs if total_runs else 0.0

    return {
        "method": method_id,
        "ea": ea,
        "run_accuracy": run_acc,
        "correct_cases": correct_cases,
        "all_correct_cases": all_cases,
        "total_cases": total_cases,
        "correct_runs": correct_runs_total,
        "total_runs": total_runs,
        "category_ea": {
            cat: {
                "ea": s["correct"] / s["total"],
                "correct": s["correct"],
                "total": s["total"],
                "run_accuracy": s["correct_runs"] / s["total_runs"] if s["total_runs"] else 0.0,
                "correct_runs": s["correct_runs"],
                "total_runs": s["total_runs"],
            }
            for cat, s in cat_stats.items()
        },
        "case_results": results,
    }


# ── 打印摘要 ──────────────────────────────────────────────────────────────────

def print_summary(report: dict) -> None:
    method    = report["method"]
    ea        = report["ea"]
    run_acc   = report["run_accuracy"]
    n_ok      = report["correct_cases"]
    n_all     = report["all_correct_cases"]
    n_tot     = report["total_cases"]
    n_cruns   = report["correct_runs"]
    n_truns   = report["total_runs"]
    print(f"\n{'='*68}")
    print(f"Method: {method}")
    print(f"  Case EA  (任一run正确): {ea:.1%}  ({n_ok}/{n_tot})")
    print(f"  Case EA  (全部run正确): {n_all/n_tot:.1%}  ({n_all}/{n_tot})")
    print(f"  Run  ACC (run级正确率): {run_acc:.1%}  ({n_cruns}/{n_truns})")
    print(f"{'='*68}")
    print(f"{'类别':<20} {'CaseEA':>8} {'RunACC':>8} {'case正确/总':>12} {'run正确/总':>12}")
    print("-" * 64)
    for cat, stats in report["category_ea"].items():
        print(f"{cat:<20} {stats['ea']:>7.1%} {stats['run_accuracy']:>8.1%}"
              f" {stats['correct']:>5}/{stats['total']:<6}"
              f" {stats['correct_runs']:>5}/{stats['total_runs']:<5}")

    # 打印失败的案例
    failures = [(cid, r) for cid, r in report["case_results"].items() if not r["any_correct"]]
    if failures:
        print(f"\n失败案例 ({len(failures)}):")
        for cid, r in failures:
            reasons = [run.get("reason", "") for run in r["runs"] if not run.get("correct")]
            print(f"  Case {cid:>2} [{r['category']}] {r['question'][:50]}")
            for reason in reasons[:1]:
                if reason:
                    print(f"           → {reason[:100]}")


# ── 主流程 ────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="EA 评估器")
    parser.add_argument("--methods", nargs="+", required=True,
                        help="要评估的 method ID，如 k b_large b_large_sem")
    parser.add_argument("--cases", type=str, default="results/cases_large.json",
                        help="测试用例文件（默认 results/cases_large.json）")
    parser.add_argument("--db", type=str, default=str(DB_PATH),
                        help=f"数据库路径（默认 {DB_PATH}）")
    parser.add_argument("--save", action="store_true",
                        help="将结果保存到 results/method_<id>/ea.json")
    args = parser.parse_args()

    cases_path = Path(args.cases)
    if not cases_path.is_absolute():
        cases_path = ACCURACY_DIR / args.cases

    conn = sqlite3.connect(args.db)
    conn.row_factory = None

    all_reports = []
    for method_id in args.methods:
        print(f"\n评估 method_{method_id} ...")
        report = evaluate_method(method_id, cases_path, conn)
        if report:
            print_summary(report)
            all_reports.append(report)
            if args.save:
                out_path = RESULTS_DIR / f"method_{method_id}" / "ea.json"
                with open(out_path, "w") as f:
                    json.dump(report, f, ensure_ascii=False, indent=2)
                print(f"  → 已保存到 {out_path}")

    # 三方对比表
    if len(all_reports) > 1:
        print(f"\n{'='*60}")
        print("三方对比（EA）")
        print(f"{'='*60}")
        cats = list(all_reports[0]["category_ea"].keys())
        header = f"{'类别':<20}" + "".join(f"{r['method']:>12}" for r in all_reports)
        print(header)
        print("-" * (20 + 12 * len(all_reports)))
        for cat in cats:
            row = f"{cat:<20}"
            for r in all_reports:
                ea = r["category_ea"].get(cat, {}).get("ea", 0)
                row += f"{ea:>11.1%} "
            print(row)
        print("-" * (20 + 12 * len(all_reports)))
        overall = f"{'Case EA (any)':20}"
        for r in all_reports:
            overall += f"{r['ea']:>11.1%} "
        print(overall)
        overall2 = f"{'Case EA (all)':20}"
        for r in all_reports:
            v = r["all_correct_cases"] / r["total_cases"] if r["total_cases"] else 0.0
            overall2 += f"{v:>11.1%} "
        print(overall2)
        overall3 = f"{'Run ACC':20}"
        for r in all_reports:
            overall3 += f"{r['run_accuracy']:>11.1%} "
        print(overall3)

    conn.close()


if __name__ == "__main__":
    main()
