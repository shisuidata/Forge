#!/usr/bin/env python3
"""
本地测试题 Execution Accuracy (EA) 评测。

流程：
  NL question → MiniMax (Forge tool_use) → Forge JSON → compile → SQL → SQLite 执行 → 比对 gold

用法：
    python tests/text-to-sql-failures/run_ea.py
    python tests/text-to-sql-failures/run_ea.py --case A1   # 只跑指定题目
    python tests/text-to-sql-failures/run_ea.py --model MiniMax-M2.5-highspeed
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))

from forge.compiler import compile_query
from forge.schema_builder import build_tool_schema
from registry.sync import run_sync
from tests.spider2.runner import registry_to_context, _call_anthropic
from tests.spider2.prompts import build_system

DB_PATH = Path(__file__).parent / "test.db"

# ── 测试用例定义 ───────────────────────────────────────────────────────────────
#
# gold_sql 已适配 SQLite 方言（无 DATE_TRUNC / generate_series / EXTRACT）
# C1/C2 不可在 SQLite 上执行（需要日期序列生成），标记为 skip=True
# D1 歧义题：选"分母=有下单记录的用户"版本
# E1 使用窗口函数方案（跨方言通用，SQLite 3.25+ 支持）

CASES = [
    {
        "id": "A1",
        "question": "统计所有用户的订单数量，没有下单的用户也要显示，订单数为 0",
        # gold 只保留关键列：user_id + order_count，用 key_cols=[0,-1] 比对
        "gold_sql": """
            SELECT u.id, COUNT(o.id) AS order_count
            FROM users u
            LEFT JOIN orders o ON o.user_id = u.id
            GROUP BY u.id
        """,
        # 比较时只取第 0 列（user_id）和最后一列（order_count），忽略额外的 user 字段
        "key_cols": [0, -1],
    },
    {
        "id": "A2",
        "question": "找出所有没有对应订单明细的异常订单",
        "gold_sql": """
            SELECT o.*
            FROM orders o
            LEFT JOIN order_items oi ON oi.order_id = o.id
            WHERE oi.id IS NULL
        """,
    },
    {
        "id": "B1",
        "question": "统计每个城市中，VIP 用户的平均客单价，只统计状态为'已完成'的订单",
        "gold_sql": """
            SELECT u.city, AVG(o.total_amount) AS avg_order_value
            FROM users u
            JOIN orders o ON o.user_id = u.id
            WHERE u.is_vip = 1
              AND o.status = 'completed'
            GROUP BY u.city
        """,
    },
    {
        "id": "B2",
        "question": "找出每个用户中，订单金额高于该用户历史平均订单金额的订单",
        "gold_sql": None,
        "skip": True,
        "skip_reason": "Forge DSL 限制：filter/having/qualify 的 val 只支持标量，无法做列对列比较（total_amount > user_avg）",
    },
    {
        "id": "C1",
        "question": "统计最近 30 天每天的新增用户数，没有新用户注册的日期也要显示，值为 0",
        "gold_sql": None,  # 需要 generate_series，SQLite 不支持，跳过
        "skip": True,
        "skip_reason": "需要日期序列生成（generate_series / recursive CTE），超出当前 Forge DSL 能力边界",
    },
    {
        "id": "C2",
        "question": "统计今年每个月的销售额，以及与去年同月相比的增长率",
        "gold_sql": None,  # 需要 DATE_TRUNC / strftime 月份截断 + 同比 JOIN，复杂方言
        "skip": True,
        "skip_reason": "需要月份截断函数 + 同比 JOIN，属于算法逻辑错误（Forge 能力边界外）",
    },
    {
        "id": "D1",
        "question": "统计复购率，复购用户定义为下过 2 次及以上订单的用户（分母：有过下单记录的用户）",
        "gold_sql": """
            SELECT
              COUNT(CASE WHEN order_count >= 2 THEN 1 END) * 1.0 / COUNT(*) AS repurchase_rate
            FROM (
              SELECT user_id, COUNT(*) AS order_count
              FROM orders
              GROUP BY user_id
            ) t
        """,
    },
    {
        "id": "D2",
        "question": "计算每个商品的实际毛利率，公式为（售价 - 成本）/ 售价，售价为该商品的平均成交单价",
        "gold_sql": """
            SELECT
              p.id, p.name, p.cost_price,
              AVG(oi.unit_price) AS avg_sell_price,
              (AVG(oi.unit_price) - p.cost_price) / NULLIF(AVG(oi.unit_price), 0) * 100
                AS gross_margin_pct
            FROM products p
            JOIN order_items oi ON oi.product_id = p.id
            GROUP BY p.id, p.name, p.cost_price
        """,
    },
    {
        "id": "E1",
        "question": "查询每个用户最近一次订单的详情",
        "gold_sql": """
            SELECT * FROM (
              SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) AS rn
              FROM orders
            ) t WHERE rn = 1
        """,
    },
    {
        "id": "E2",
        "question": "统计各商品品类的销售额，并计算每个品类占总销售额的百分比",
        "gold_sql": """
            SELECT
              p.category,
              SUM(oi.quantity * oi.unit_price) AS category_revenue,
              SUM(oi.quantity * oi.unit_price) * 100.0
                / SUM(SUM(oi.quantity * oi.unit_price)) OVER () AS pct_of_total
            FROM order_items oi
            JOIN products p ON p.id = oi.product_id
            GROUP BY p.category
            ORDER BY category_revenue DESC
        """,
    },
]


# ── 结果比较 ──────────────────────────────────────────────────────────────────

def _rows_to_set(rows: list[tuple], key_cols: list[int] | None = None) -> frozenset:
    """
    将结果集转为可比较的 frozenset（忽略行顺序，值规范化为字符串）。
    key_cols: 若指定，只提取这些列索引（支持负索引）进行比较，忽略其他列。
    """
    def norm(v):
        if v is None:
            return "NULL"
        if isinstance(v, float):
            return f"{v:.4f}"
        return str(v)
    if key_cols is not None:
        return frozenset(tuple(norm(row[i]) for i in key_cols) for row in rows)
    return frozenset(tuple(norm(v) for v in row) for row in rows)


def run_gold(sql: str) -> list[tuple] | None:
    """在 test.db 上执行 gold SQL，返回结果行列表，失败返回 None。"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        rows = conn.execute(sql).fetchall()
        conn.close()
        return rows
    except Exception as e:
        print(f"    ⚠ Gold SQL 执行失败: {e}")
        return None


def run_generated(sql: str) -> list[tuple] | None:
    """在 test.db 上执行生成的 SQL，返回结果行列表，失败返回 None。"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        rows = conn.execute(sql).fetchall()
        conn.close()
        return rows
    except Exception as e:
        print(f"    ⚠ 生成 SQL 执行失败: {e}")
        return None


# ── Forge 管道 ────────────────────────────────────────────────────────────────

def build_registry() -> dict:
    """从 test.db 构建 registry。"""
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        tmp_path = Path(f.name)
    try:
        registry = run_sync(f"sqlite:///{DB_PATH}", tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)
    return registry


def call_minimax(client, system: str, question: str, tools: list) -> dict:
    """调用 MiniMax，返回 Forge JSON dict 或错误信息。"""
    result = _call_anthropic(client, system, [{"role": "user", "content": question}], tools)
    tool_input, text = result
    if tool_input is not None:
        return {"ok": True, "input": tool_input}
    return {"ok": False, "error": f"模型未调用工具: {text[:200]}"}


def run_case(case: dict, client, system: str, tool_schema: dict) -> dict:
    """运行单个测试用例，返回结果 dict。"""
    cid = case["id"]
    question = case["question"]

    print(f"\n[{cid}] {question[:60]}")

    # 1. 调用 LLM
    tools = [{"name": "generate_forge_query",
              "description": "Generate a Forge JSON query for the given question.",
              "input_schema": tool_schema}]
    try:
        llm_result = call_minimax(client, system, question, tools)
    except Exception as e:
        print(f"  ✗ LLM 调用失败: {e}")
        return {"id": cid, "status": "llm_error", "error": str(e)}

    if not llm_result["ok"]:
        print(f"  ✗ LLM 未调用工具: {llm_result['error']}")
        return {"id": cid, "status": "no_tool_call", "error": llm_result["error"]}

    forge_json = llm_result["input"]
    print(f"  Forge JSON: {json.dumps(forge_json, ensure_ascii=False)[:120]}...")

    # 2. 编译
    try:
        sql = compile_query(forge_json)
    except Exception as e:
        print(f"  ✗ 编译失败: {e}")
        return {"id": cid, "status": "compile_error", "error": str(e), "forge_json": forge_json}

    print(f"  SQL: {sql[:120].replace(chr(10), ' ')}...")

    # 3. 执行 + 比对
    gold_rows = run_gold(case["gold_sql"])
    if gold_rows is None:
        return {"id": cid, "status": "gold_error", "sql": sql}

    gen_rows = run_generated(sql)
    if gen_rows is None:
        return {"id": cid, "status": "exec_error", "sql": sql, "forge_json": forge_json}

    key_cols = case.get("key_cols")
    gold_set = _rows_to_set(gold_rows, key_cols)
    gen_set  = _rows_to_set(gen_rows, key_cols)
    match = (gold_set == gen_set)

    if match:
        print(f"  ✅ PASS  ({len(gold_rows)} rows)")
    else:
        print(f"  ✗ FAIL  gold={len(gold_rows)} rows, gen={len(gen_rows)} rows")
        if len(gold_rows) <= 10 and len(gen_rows) <= 10:
            print(f"     gold: {gold_rows}")
            print(f"     gen:  {gen_rows}")

    return {
        "id": cid,
        "status": "pass" if match else "fail",
        "sql": sql,
        "forge_json": forge_json,
        "gold_rows": len(gold_rows),
        "gen_rows": len(gen_rows),
    }


# ── 主程序 ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", help="只跑指定题目 ID (e.g. A1)")
    parser.add_argument("--model", default=os.environ.get("LLM_MODEL", "MiniMax-M2.5-highspeed"))
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"❌ 找不到 {DB_PATH}，请先运行: python tests/text-to-sql-failures/create_db.py")
        sys.exit(1)

    # 选取要运行的用例
    cases = [c for c in CASES if not c.get("skip")]
    if args.case:
        cases = [c for c in cases if c["id"] == args.case]
        if not cases:
            skipped = next((c for c in CASES if c["id"] == args.case), None)
            if skipped:
                print(f"⚠ {args.case} 已标记跳过: {skipped['skip_reason']}")
            else:
                print(f"❌ 找不到用例 {args.case}")
            sys.exit(1)

    print(f"=== Forge 本地 EA 评测  ({len(cases)} 题, model={args.model}) ===")

    # 构建 Registry
    print("\n构建 Registry (forge sync test.db)...")
    registry = build_registry()
    registry_ctx = registry_to_context(registry)
    tool_schema = build_tool_schema(registry)
    system = build_system(registry_ctx)
    print(f"Registry: {len(registry.get('tables', {}))} 张表")

    # 初始化 MiniMax 客户端
    import anthropic
    api_key  = os.environ.get("LLM_API_KEY") or os.environ.get("MINIMAX_API_KEY", "")
    base_url = os.environ.get("LLM_BASE_URL") or os.environ.get("MINIMAX_BASE_URL",
               "https://api.minimaxi.com/anthropic")
    client = anthropic.Anthropic(api_key=api_key, base_url=base_url)

    # 暂时覆盖全局 LLM_MODEL（_call_anthropic 从 runner 的模块级变量读取）
    import tests.spider2.runner as runner_mod
    runner_mod.LLM_MODEL = args.model

    # 打印跳过的用例
    skipped = [c for c in CASES if c.get("skip")]
    if skipped:
        print(f"\n跳过 {len(skipped)} 个（超出 Forge DSL 能力边界）:")
        for c in skipped:
            print(f"  [{c['id']}] {c['skip_reason']}")

    # 运行
    results = []
    for case in cases:
        r = run_case(case, client, system, tool_schema)
        results.append(r)

    # 汇总
    total   = len(results)
    passed  = sum(1 for r in results if r["status"] == "pass")
    failed  = sum(1 for r in results if r["status"] == "fail")
    errors  = sum(1 for r in results if r["status"] not in ("pass", "fail"))

    print(f"\n{'='*50}")
    print(f"Execution Accuracy: {passed}/{total} = {passed/total*100:.1f}%")
    print(f"  ✅ Pass:  {passed}")
    print(f"  ✗ Fail:  {failed}")
    print(f"  ⚠ Error: {errors} (llm/compile/exec error)")

    print(f"\n详细结果:")
    for r in results:
        icon = "✅" if r["status"] == "pass" else ("✗" if r["status"] == "fail" else "⚠")
        print(f"  {icon} [{r['id']}] {r['status']}")


if __name__ == "__main__":
    main()
