#!/usr/bin/env python3
"""
多方法横向对比报告生成器。

用法：
    python tests/accuracy/compare.py --methods e,f
    python tests/accuracy/compare.py --methods a,b,d,e,f
    python tests/accuracy/compare.py --methods e,f --out results/compare/my_report.md

每个方法需要已有 results/method_{id}/scores.json。
也支持从 legacy/scores_v3.json 读取旧方法数据（用 --legacy-scores 指定）。

报告输出到 results/compare/report_{ts}_{methods}.md
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
COMPARE_DIR  = RESULTS_DIR / "compare"


def load_cases() -> list[dict]:
    if not CASES_FILE.exists():
        print(f"❌ 找不到 {CASES_FILE}", file=sys.stderr)
        sys.exit(1)
    return json.loads(CASES_FILE.read_text())


def load_method_scores(method_id: str, legacy_scores: dict | None = None) -> dict | None:
    """按方法 id 加载 scores.json。先找新路径，找不到再回退到 legacy。"""
    new_path = RESULTS_DIR / f"method_{method_id}" / "scores.json"
    if new_path.exists():
        return json.loads(new_path.read_text())

    # 从 legacy scores 提取（旧格式 key = method_{a,b,c,d,e}）
    if legacy_scores:
        key = f"method_{method_id}"
        alt_key = "method_b" if method_id == "sql" else None
        # 检查是否有该方法的数据
        sample = next(iter(legacy_scores.values()), {})
        if key in sample or (alt_key and alt_key in sample):
            use_key = key if key in sample else alt_key
            extracted: dict = {}
            for case_id, sc in legacy_scores.items():
                if use_key in sc:
                    extracted[case_id] = sc[use_key]
            return extracted if extracted else None

    return None


def load_method_runs_stats(method_id: str) -> tuple[int, int]:
    """返回 (ok, err) 编译统计。只适用于 forge 方法。"""
    path = RESULTS_DIR / f"method_{method_id}" / "runs.json"
    if not path.exists():
        # 尝试从 legacy results.json
        legacy_path = RESULTS_DIR / "legacy" / "results.json"
        if legacy_path.exists():
            results = json.loads(legacy_path.read_text())
            key = f"runs_{method_id.lower()}"
            all_runs = [r for v in results.values() for r in v.get(key, [])]
            ok  = sum(1 for r in all_runs if r and not r.get("error"))
            err = sum(1 for r in all_runs if r and r.get("error"))
            return ok, err
        return 0, 0

    data = json.loads(path.read_text())
    all_runs = [r for v in data.values() for r in v.get("runs", [])]
    ok  = sum(1 for r in all_runs if r and not r.get("error"))
    err = sum(1 for r in all_runs if r and r.get("error"))
    return ok, err


def method_label(method_id: str) -> str:
    """加载方法标签，找不到 config 则生成默认值。"""
    sys.path.insert(0, str(ACCURACY_DIR))
    try:
        from methods import load
        return load(method_id).label
    except Exception:
        labels = {
            "a": "Method A（旧 Forge DSL）",
            "b": "Method SQL（直接生成 SQL）",
            "c": "Method C（声明式风格，已淘汰）",
            "d": "Method D（枚举 schema）",
            "e": "Method E（枚举 schema + 升级提示词）",
        }
        return labels.get(method_id, f"Method {method_id.upper()}")


def build_compare_report(method_ids: list[str],
                          cases: list[dict],
                          all_scores: dict[str, dict]) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cases_by_id = {str(c["id"]): c for c in cases}

    labels = {mid: method_label(mid) for mid in method_ids}

    # 总均分
    overall: dict[str, float] = {}
    for mid in method_ids:
        scores = all_scores.get(mid, {})
        avgs = [sc["avg"] for sc in scores.values() if sc.get("avg") is not None]
        overall[mid] = sum(avgs) / len(avgs) if avgs else 0.0

    # 编译失败率
    fail_rates: dict[str, str] = {}
    for mid in method_ids:
        ok, err = load_method_runs_stats(mid)
        total = ok + err
        if total > 0:
            fail_rates[mid] = f"{err/total*100:.1f}%"
        else:
            fail_rates[mid] = "N/A"

    # 胜负统计
    wins: dict[str, int] = {mid: 0 for mid in method_ids}
    ties = 0
    for sc_dict in all_scores.values():
        case_scores = {}
        for mid in method_ids:
            sc = sc_dict.get(mid, {})
            case_scores[mid] = sc.get("avg") or 0.0
        best = max(case_scores.values())
        winners = [m for m, v in case_scores.items() if v == best]
        if len(winners) == 1:
            wins[winners[0]] += 1
        else:
            ties += 1

    # 分类统计
    cat_avgs: dict[str, dict[str, list]] = defaultdict(lambda: {m: [] for m in method_ids})
    for case_id, sc_dict in all_scores.items():
        cat = cases_by_id.get(case_id, {}).get("category", "?")
        for mid in method_ids:
            sc = sc_dict.get(mid, {})
            if sc.get("avg") is not None:
                cat_avgs[cat][mid].append(sc["avg"])

    # ── 报告正文 ──────────────────────────────────────────────────────────────
    lines = [
        "# Forge DSL 准确性测试报告 — 多方法对比",
        "",
        f"> 生成时间：{now}",
        f"> 测试用例：{len(cases)} 个  ·  每方法运行 5 次",
        "",
        "## 总体结果",
        "",
    ]

    header = " | ".join(f"**{labels[m]}**" for m in method_ids)
    lines.append(f"| 指标 | {header} |")
    lines.append("|---|" + "---|" * len(method_ids))

    score_row = " | ".join(f"**{overall[m]:.2f}**" for m in method_ids)
    lines.append(f"| 总均分 | {score_row} |")

    fail_row = " | ".join(fail_rates[m] for m in method_ids)
    lines.append(f"| 编译失败率 | {fail_row} |")

    win_row = " | ".join(str(wins[m]) for m in method_ids)
    lines.append(f"| 胜出用例数（平局{ties}） | {win_row} |")
    lines.append("")

    # 结论
    best_m = max(method_ids, key=lambda m: overall[m])
    lines.append(f"**结论**：{labels[best_m]} 总均分最高（{overall[best_m]:.2f}）。")
    # 显示各方法间的分差
    for mid in method_ids:
        if mid != best_m:
            diff = overall[best_m] - overall[mid]
            sign = f"+{diff:.2f}" if diff >= 0 else f"{diff:.2f}"
            lines.append(f"相对 {labels[mid]}：{sign} 分。")
    lines += ["", "---", "", "## 分类得分", ""]

    cat_header = " | ".join(labels[m] for m in method_ids)
    lines.append(f"| 类别 | 用例数 | {cat_header} |")
    lines.append("|---|---|" + "---|" * len(method_ids))

    categories = ['多表JOIN+聚合', '复杂过滤', '分组+HAVING', '排名与TopN',
                  '窗口聚合', '时序导航', 'ANTI/SEMI JOIN', '综合复杂查询']
    for cat in categories:
        if cat not in cat_avgs:
            continue
        n = max(len(cat_avgs[cat][method_ids[0]]), 1)
        cells = " | ".join(
            f"{sum(cat_avgs[cat][m])/len(cat_avgs[cat][m]):.2f}" if cat_avgs[cat][m] else "—"
            for m in method_ids
        )
        lines.append(f"| {cat} | {n} | {cells} |")

    lines += ["", "---", "", "## 逐用例得分", ""]
    col_heads = " | ".join(f"{m.upper()}均分" for m in method_ids)
    lines.append(f"| Case | 类别 | 难度 | {col_heads} | 优胜 |")
    lines.append("|---|---|---|" + "---|" * len(method_ids) + "---|")

    # 合并所有方法的分数到按 case_id 索引的 dict
    all_case_ids = sorted(all_scores.keys(), key=int)
    for case_id in all_case_ids:
        sc_dict = all_scores[case_id]
        case    = cases_by_id.get(case_id, {})
        case_scores = {m: (sc_dict.get(m, {}).get("avg") or 0.0) for m in method_ids}
        best_val = max(case_scores.values())
        winners  = [m for m, v in case_scores.items() if v == best_val]
        winner_str = "/".join(m.upper() for m in winners)
        score_cells = " | ".join(f"{case_scores[m]:.1f}" for m in method_ids)
        lines.append(f"| {case_id} | {case.get('category','?')} | {case.get('difficulty','?')} "
                     f"| {score_cells} | {winner_str} |")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="多方法横向对比报告")
    parser.add_argument("--methods", "-m", required=True,
                        help="方法 id 列表，逗号分隔（如 'e,f' 或 'a,b,d,e,f'）")
    parser.add_argument("--legacy-scores", default=None,
                        help="旧格式 scores JSON 文件（如 legacy/scores_v3.json），用于加载 A/B/C/D/E 历史数据")
    parser.add_argument("--out", default=None,
                        help="输出报告路径（默认自动命名到 results/compare/）")
    args = parser.parse_args()

    method_ids = [m.strip() for m in args.methods.split(",") if m.strip()]

    # 加载 legacy scores（如果提供）
    legacy_scores: dict | None = None
    if args.legacy_scores:
        lp = Path(args.legacy_scores)
        if lp.exists():
            legacy_scores = json.loads(lp.read_text())
            print(f"📂 加载 legacy scores → {lp}")

    cases = load_cases()

    # 加载每个方法的 scores，构建 {case_id: {method_id: {scores/avg/comment}}} 结构
    per_method: dict[str, dict] = {}
    for mid in method_ids:
        scores = load_method_scores(mid, legacy_scores)
        if scores is None:
            print(f"❌ 找不到 method_{mid} 的 scores.json，请先完成评分", file=sys.stderr)
            sys.exit(1)
        per_method[mid] = scores
        print(f"✅ method_{mid}：{len(scores)} 个用例评分")

    # 合并为 {case_id: {method_id: sc_dict}}
    all_case_ids = sorted(
        set(k for sc in per_method.values() for k in sc.keys()),
        key=int
    )
    merged: dict[str, dict] = {cid: {} for cid in all_case_ids}
    for mid, scores in per_method.items():
        for cid, sc in scores.items():
            merged[cid][mid] = sc

    report = build_compare_report(method_ids, cases, merged)

    # 输出路径
    if args.out:
        out_path = Path(args.out)
    else:
        COMPARE_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        methods_str = "_vs_".join(method_ids)
        out_path = COMPARE_DIR / f"report_{ts}_{methods_str}.md"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report)
    print(f"\n📊 对比报告已生成 → {out_path}")


if __name__ == "__main__":
    main()
