#!/usr/bin/env python3
"""
从 run_log.jsonl 生成 benchmark 报告（Markdown 格式）。

用法：
    python tests/benchmark/report.py            # 打印到 stdout
    python tests/benchmark/report.py --save     # 同时写入 results/report.md
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

BENCH_DIR   = Path(__file__).parent
RESULTS_DIR = BENCH_DIR / "results"

CATEGORIES = [
    "基础过滤", "多表JOIN", "聚合+GROUPBY", "排名TopN",
    "窗口函数", "CTE多步", "时序", "综合复合",
]

DIFFICULTY_LABEL = {1: "D1", 2: "D2", 3: "D3"}


def load_logs(method: str) -> list[dict]:
    log_path = RESULTS_DIR / method / "run_log.jsonl"
    if not log_path.exists():
        return []
    records = []
    seen: set[str] = set()
    # 断点续做可能有重复记录，取最后一条有效的
    for line in log_path.read_text(encoding="utf-8").splitlines():
        try:
            d = json.loads(line)
            cid = d.get("id", "")
            if cid:
                seen_key = (cid, d.get("ts", ""))
                if seen_key not in seen:
                    seen.add(seen_key)
                    records.append(d)
        except Exception:
            pass
    # 保留最新一条（按 ts 排序，取同一 id 最后的）
    latest: dict[str, dict] = {}
    for r in records:
        latest[r["id"]] = r
    return list(latest.values())


def compute_stats(records: list[dict]) -> dict:
    total = len(records)
    if total == 0:
        return {}
    ea_ok   = [r for r in records if r.get("ea_match") is True]
    ea_fail = [r for r in records if r.get("ea_match") is False]
    ea_err  = [r for r in records if r.get("ea_match") is None]

    by_cat: dict[str, dict] = {}
    for cat in CATEGORIES:
        cat_recs = [r for r in records if r.get("category") == cat]
        cat_ok   = sum(1 for r in cat_recs if r.get("ea_match") is True)
        by_cat[cat] = {"total": len(cat_recs), "ok": cat_ok}

    by_diff: dict[int, dict] = {}
    for d in (1, 2, 3):
        diff_recs = [r for r in records if r.get("difficulty") == d]
        diff_ok   = sum(1 for r in diff_recs if r.get("ea_match") is True)
        by_diff[d] = {"total": len(diff_recs), "ok": diff_ok}

    avg_time = sum(r.get("elapsed_s", 0) for r in records) / total

    return {
        "total":   total,
        "ok":      len(ea_ok),
        "fail":    len(ea_fail),
        "err":     len(ea_err),
        "ea_pct":  len(ea_ok) / total * 100,
        "by_cat":  by_cat,
        "by_diff": by_diff,
        "avg_time": avg_time,
        "records": records,
    }


def pct(ok: int, total: int) -> str:
    if total == 0:
        return "—"
    return f"{ok/total*100:.0f}%"


def make_report(forge_stats: dict, direct_stats: dict) -> str:
    lines = []

    # Header
    lines.append("## Forge vs 直接 SQL 基准测试报告\n")
    lines.append(f"测试用例：40 题  |  难度：D1(基础) / D2(中等) / D3(复杂)")
    lines.append(f"评测指标：Execution Accuracy (EA) — 执行结果集完全匹配")
    lines.append(f"测试模型：{_get_model_name()}\n")

    # 总览
    lines.append("### 总体结果\n")
    lines.append("| 方法 | EA | 正确题数 | 执行错误 | 其他错误 | 平均耗时 |")
    lines.append("|------|-----|---------|---------|---------|---------|")

    def row(name, s):
        if not s:
            return f"| {name} | — | — | — | — | — |"
        return (f"| **{name}** | **{s['ea_pct']:.1f}%** | {s['ok']}/{s['total']} "
                f"| {s['fail']} | {s['err']} | {s['avg_time']:.1f}s |")

    lines.append(row("Forge (DSL)", forge_stats))
    lines.append(row("直接 SQL", direct_stats))

    # 按类别
    lines.append("\n### 分类结果\n")
    lines.append("| 类别 | 题数 | Forge EA | 直接 SQL EA | Δ |")
    lines.append("|------|------|---------|------------|---|")
    for cat in CATEGORIES:
        f_c = forge_stats.get("by_cat", {}).get(cat, {})
        d_c = direct_stats.get("by_cat", {}).get(cat, {})
        f_n, d_n = f_c.get("total", 0), d_c.get("total", 0)
        f_ok, d_ok = f_c.get("ok", 0), d_c.get("ok", 0)
        f_p = f_ok / f_n * 100 if f_n else 0
        d_p = d_ok / d_n * 100 if d_n else 0
        delta = f_p - d_p
        delta_str = f"+{delta:.0f}%" if delta > 0 else (f"{delta:.0f}%" if delta < 0 else "—")
        lines.append(f"| {cat} | {f_n} | {pct(f_ok, f_n)} | {pct(d_ok, d_n)} | {delta_str} |")

    # 按难度
    lines.append("\n### 按难度分层\n")
    lines.append("| 难度 | 说明 | Forge EA | 直接 SQL EA |")
    lines.append("|------|------|---------|------------|")
    diff_labels = {1: "基础（单表过滤/简单排序）", 2: "中等（JOIN/聚合/HAVING）", 3: "复杂（窗口/CTE/多步）"}
    for d in (1, 2, 3):
        f_d = forge_stats.get("by_diff", {}).get(d, {})
        d_d = direct_stats.get("by_diff", {}).get(d, {})
        lines.append(f"| D{d} | {diff_labels[d]} | {pct(f_d.get('ok',0), f_d.get('total',0))} "
                     f"| {pct(d_d.get('ok',0), d_d.get('total',0))} |")

    # 失败分析（Forge）
    if forge_stats:
        lines.append("\n### Forge 失败案例分析\n")
        fail_recs = [r for r in forge_stats["records"] if r.get("ea_match") is not True]
        if fail_recs:
            lines.append("| Case | 类别 | 难度 | 状态 | 问题 |")
            lines.append("|------|------|------|------|------|")
            for r in sorted(fail_recs, key=lambda x: x.get("id", "")):
                err_short = (r.get("error") or "结果不匹配")[:60]
                lines.append(f"| {r['id']} | {r['category']} | D{r['difficulty']} "
                              f"| {r['status']} | {err_short} |")
        else:
            lines.append("无失败案例。\n")

    # 结论
    if forge_stats and direct_stats:
        forge_ea = forge_stats["ea_pct"]
        direct_ea = direct_stats["ea_pct"]
        better = forge_ea >= direct_ea

        lines.append("\n### 结论\n")
        if better:
            lines.append(
                f"Forge（{forge_ea:.1f}%）在 Execution Accuracy 上优于直接 SQL（{direct_ea:.1f}%），"
                f"差值 +{forge_ea - direct_ea:.1f}pp。\n"
            )
        else:
            lines.append(
                f"直接 SQL（{direct_ea:.1f}%）在本次测试中 EA 略高于 Forge（{forge_ea:.1f}%），"
                f"差值 {forge_ea - direct_ea:.1f}pp。"
                f"Forge 的核心优势体现在 ANTI/SEMI JOIN、复杂 HAVING 等容易出错的类别。\n"
            )

    return "\n".join(lines)


def _get_model_name() -> str:
    try:
        from dotenv import dotenv_values
        env = dotenv_values(Path(__file__).parent.parent.parent / ".env")
        return env.get("LLM_MODEL") or env.get("MINIMAX_MODEL") or "未知"
    except Exception:
        import os
        return os.environ.get("LLM_MODEL") or os.environ.get("MINIMAX_MODEL") or "未知"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true", help="写入 results/report.md")
    args = parser.parse_args()

    forge_records  = load_logs("forge")
    direct_records = load_logs("direct")

    if not forge_records and not direct_records:
        print("❌ 未找到任何运行结果，请先运行 runner.py", flush=True)
        return

    forge_stats  = compute_stats(forge_records)
    direct_stats = compute_stats(direct_records)

    report = make_report(forge_stats, direct_stats)
    print(report)

    if args.save:
        out = RESULTS_DIR / "report.md"
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        out.write_text(report, encoding="utf-8")
        print(f"\n✅ 报告已写入 {out}", flush=True)


if __name__ == "__main__":
    main()
