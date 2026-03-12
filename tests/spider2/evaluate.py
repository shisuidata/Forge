#!/usr/bin/env python3
"""
Spider2-Lite SQLite 子集评估脚本（Execution Match）。

评估逻辑：
  1. 对每个 predicted SQL，在对应的 SQLite 数据库上执行
  2. 对同一 instance_id 的 gold SQL 在同一数据库上执行
  3. 比较执行结果（排序不敏感，结果集完全一致 = 正确）

如果没有 gold SQL，尝试使用 gold execution_results CSV（Spider2 官方格式）。

用法：
    python tests/spider2/evaluate.py                      # 评估 forge_j 方法
    python tests/spider2/evaluate.py --method forge_j     # 指定方法
    python tests/spider2/evaluate.py --verbose            # 显示失败用例详情

输出：
    tests/spider2/results/{method}/eval_report.json
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).parent))

from tests.spider2.setup import _find_db_path

DATA_DIR     = Path(__file__).parent / "data" / "spider2-lite"
SQLITE_DIR   = DATA_DIR / "resource" / "databases" / "sqlite"
GOLD_SQL_DIR = DATA_DIR / "evaluation_suite" / "gold" / "sql_queries"
GOLD_CSV_DIR = DATA_DIR / "evaluation_suite" / "gold" / "execution_results"
RESULTS_DIR  = Path(__file__).parent / "results"


# ── 执行工具 ─────────────────────────────────────────────────────────────────

def execute_sql(db_path: str, sql: str) -> tuple[list, str | None]:
    """
    在 SQLite 数据库上执行 SQL，返回 (rows, error)。
    rows 是二维列表（每行是 list），error 是 None 或错误字符串。
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(sql)
        rows = [list(row) for row in cursor.fetchall()]
        conn.close()
        return rows, None
    except Exception as e:
        return [], str(e)


def normalize_result(rows: list) -> frozenset:
    """将结果集标准化为 frozenset，用于无序比较。每行转为 tuple 后加入集合。"""
    normalized = []
    for row in rows:
        normalized.append(tuple(
            str(v).strip() if v is not None else "NULL"
            for v in row
        ))
    return frozenset(normalized)


def load_gold_csv(csv_path: Path) -> list:
    """从 Spider2 官方 gold CSV 文件加载期望结果。"""
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
    # 第一行是 header，跳过
    return rows[1:] if rows else []


# ── 用例数据 ─────────────────────────────────────────────────────────────────

def load_sqlite_cases() -> dict[str, dict]:
    """返回 {instance_id: case_dict}，只包含 SQLite 用例。"""
    jsonl_path = DATA_DIR / "spider2-lite.jsonl"
    if not jsonl_path.exists():
        print(f"❌ 找不到 {jsonl_path}，请先运行 setup.py", file=sys.stderr)
        sys.exit(1)

    cases = {}
    for line in jsonl_path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        c = json.loads(line)
        db_path = _find_db_path(c.get("db", ""), SQLITE_DIR)
        if db_path:
            c["_db_path"] = str(db_path)
            cases[c["instance_id"]] = c
    return cases


# ── 单用例评估 ────────────────────────────────────────────────────────────────

def evaluate_one(instance_id: str, predicted_sql: str, case: dict) -> dict:
    """评估单个用例，返回评估结果 dict。"""
    db_path = case["_db_path"]
    result  = {
        "instance_id":   instance_id,
        "db":            case.get("db"),
        "question":      case.get("question", ""),
        "predicted_sql": predicted_sql,
        "gold_sql":      None,
        "match":         False,
        "error":         None,
        "pred_rows":     0,
        "gold_rows":     0,
    }

    # 执行预测 SQL
    pred_rows, pred_err = execute_sql(db_path, predicted_sql)
    if pred_err:
        result["error"] = f"predicted SQL error: {pred_err}"
        return result
    result["pred_rows"] = len(pred_rows)

    # 获取 gold 结果（优先 SQL，回退 CSV）
    gold_sql_path = GOLD_SQL_DIR / f"{instance_id}.sql"
    gold_csv_path = GOLD_CSV_DIR / f"{instance_id}.csv"

    if gold_sql_path.exists():
        gold_sql = gold_sql_path.read_text().strip()
        result["gold_sql"] = gold_sql
        gold_rows, gold_err = execute_sql(db_path, gold_sql)
        if gold_err:
            result["error"] = f"gold SQL error: {gold_err}"
            return result
    elif gold_csv_path.exists():
        gold_rows = load_gold_csv(gold_csv_path)
        result["gold_sql"] = "(from CSV)"
    else:
        result["error"] = "no gold SQL or CSV found"
        return result

    result["gold_rows"] = len(gold_rows)
    result["match"] = normalize_result(pred_rows) == normalize_result(gold_rows)
    return result


# ── 主流程 ───────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Spider2-Lite SQLite 评估（Execution Match）")
    parser.add_argument("--method",  default="forge_j", help="方法 ID（对应 results/ 子目录）")
    parser.add_argument("--verbose", action="store_true", help="显示失败用例详情")
    args = parser.parse_args()

    method_dir = RESULTS_DIR / args.method
    if not method_dir.exists():
        print(f"❌ 找不到 {method_dir}，请先运行 runner.py", file=sys.stderr)
        sys.exit(1)

    sql_files = sorted(method_dir.glob("*.sql"))
    if not sql_files:
        print(f"❌ {method_dir} 中没有 .sql 文件", file=sys.stderr)
        sys.exit(1)

    cases = load_sqlite_cases()
    print(f"\n📊 评估 Method {args.method}  |  {len(sql_files)} 个预测 SQL\n")

    results   = []
    no_gold   = []
    exec_err  = []

    for sql_file in sql_files:
        instance_id = sql_file.stem
        if instance_id not in cases:
            continue   # 不是 SQLite 用例

        predicted_sql = sql_file.read_text().strip()
        res = evaluate_one(instance_id, predicted_sql, cases[instance_id])
        results.append(res)

        if res["error"] and "no gold" in str(res["error"]):
            no_gold.append(instance_id)
        elif res["error"]:
            exec_err.append(instance_id)

    # 统计
    evaluated = [r for r in results if not r["error"]]
    matched   = [r for r in evaluated if r["match"]]

    print(f"  总预测 SQL：{len(sql_files)}")
    print(f"  可评估（有 gold）：{len(evaluated)}")
    print(f"  无 gold 跳过：{len(no_gold)}")
    print(f"  执行错误：{len(exec_err)}")
    print()
    if evaluated:
        acc = len(matched) / len(evaluated) * 100
        print(f"  ✅ Execution Match：{len(matched)} / {len(evaluated)}  =  {acc:.1f}%")
    else:
        print("  ⚠ 无可评估用例")

    # 失败用例明细
    if args.verbose:
        failed = [r for r in evaluated if not r["match"]]
        if failed:
            print(f"\n  失败用例（{len(failed)} 个）：")
            for r in failed[:20]:
                print(f"\n  [{r['instance_id']}] {r['question'][:80]}")
                print(f"    pred rows={r['pred_rows']}  gold rows={r['gold_rows']}")
                print(f"    predicted: {r['predicted_sql'][:120]}")
                if r['gold_sql'] != "(from CSV)":
                    print(f"    gold:      {r['gold_sql'][:120]}")

    # 保存报告
    report = {
        "method":          args.method,
        "total_sql_files": len(sql_files),
        "evaluated":       len(evaluated),
        "matched":         len(matched),
        "accuracy":        round(len(matched) / len(evaluated) * 100, 2) if evaluated else 0,
        "no_gold_skipped": len(no_gold),
        "exec_errors":     len(exec_err),
        "details":         results,
    }
    report_path = method_dir / "eval_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"\n  报告 → {report_path}")


if __name__ == "__main__":
    main()
