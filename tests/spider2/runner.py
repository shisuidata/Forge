#!/usr/bin/env python3
"""
Spider2-Lite SQLite 子集测试运行器。

完整复现 Forge 生产管道（tool_use 模式）：
  1. 读取 spider2-lite.jsonl，过滤 SQLite 用例
  2. 对每个唯一数据库跑 forge sync，生成 Registry
  3. 用 Registry 动态构建带列名枚举约束的 tool schema
  4. 调用 LLM（tool_use），获取 Forge JSON
  5. 编译 Forge JSON → SQL
  6. 保存 {instance_id}.sql 到 results/{method}/

用法：
    python tests/spider2/runner.py                  # 默认跑所有 SQLite 用例
    python tests/spider2/runner.py --limit 20       # 只跑前 20 个
    python tests/spider2/runner.py --fresh          # 清空旧结果重跑
    python tests/spider2/runner.py --workers 5      # 并发数（默认 5）
    python tests/spider2/runner.py --estimate       # 只估算 token 消耗，不实际运行

结果输出：
    tests/spider2/results/{method}/{instance_id}.sql
    tests/spider2/results/{method}/run_log.jsonl     — 每条的详细记录（含 error）
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).parent))

from forge.compiler        import compile_query
from forge.schema_builder  import build_tool_schema
from registry.sync         import run_sync
from tests.spider2.prompts import build_system
from tests.spider2.setup   import _find_db_path, _find_ddl_dir


# ── 常量 ─────────────────────────────────────────────────────────────────────

DATA_DIR    = Path(__file__).parent / "data" / "spider2-lite"
SQLITE_DIR  = DATA_DIR / "resource" / "databases" / "sqlite"   # DDL + sample JSON
LOCALDB_DIR = DATA_DIR / "resource" / "databases" / "spider2-localdb"  # 实际 .sqlite
JSONL_PATH  = DATA_DIR / "spider2-lite.jsonl"
RESULTS_DIR = Path(__file__).parent / "results"

MINIMAX_API_KEY  = os.environ.get("MINIMAX_API_KEY", "")
MINIMAX_BASE_URL = os.environ.get("MINIMAX_BASE_URL", "https://api.minimaxi.com/anthropic")
MINIMAX_MODEL    = os.environ.get("MINIMAX_MODEL", "MiniMax-M2.5-highspeed")

METHOD = "forge_j"   # 方法标识，和 accuracy runner 保持命名一致


# ── 数据加载 ─────────────────────────────────────────────────────────────────

def load_sqlite_cases() -> list[dict]:
    """
    读取 JSONL，返回有 DDL schema 的本地 SQLite 用例。
    _db_path：实际 .sqlite 文件路径（若 Google Drive 数据已下载），否则为 None。
    _ddl_dir：DDL.csv 所在目录（始终存在，用于 registry 构建和 token 估算）。
    """
    if not JSONL_PATH.exists():
        print(f"❌ 找不到 {JSONL_PATH}，请先运行 setup.py", file=sys.stderr)
        sys.exit(1)

    cases, skipped = [], 0
    for line in JSONL_PATH.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        c = json.loads(line)
        ddl_dir = _find_ddl_dir(c.get("db", ""), SQLITE_DIR)
        if ddl_dir:
            c["_ddl_dir"] = str(ddl_dir)
            c["_db_path"] = str(_find_db_path(c.get("db", ""), SQLITE_DIR) or "")
            cases.append(c)
        else:
            skipped += 1

    has_db = sum(1 for c in cases if c["_db_path"])
    print(f"   SQLite 用例：{len(cases)} 个  |  有 .sqlite 文件：{has_db} 个  |  跳过：{skipped} 个")
    if not has_db:
        print(f"   ⚠  未找到 .sqlite 文件，执行评测需先下载：")
        print(f"      https://drive.usercontent.google.com/download?id=1coEVsCZq-Xvj9p2TnhBFoFTsY-UoYGmG")
        print(f"      解压后放入 tests/spider2/data/spider2-lite/resource/databases/spider2-localdb/")
    return cases


# ── Registry 缓存 ────────────────────────────────────────────────────────────

_registry_cache: dict[str, dict] = {}
_registry_lock  = threading.Lock()


def get_registry(case: dict) -> dict:
    """
    优先用实际 .sqlite 文件做 run_sync（枚举采样更准确）；
    若 .sqlite 不存在，退而从 DDL.csv 解析表结构。
    对同一数据库只处理一次，后续用缓存。
    """
    db_path  = case.get("_db_path", "")
    ddl_dir  = case.get("_ddl_dir", "")
    cache_key = db_path or ddl_dir

    if cache_key in _registry_cache:
        return _registry_cache[cache_key]

    with _registry_lock:
        if cache_key in _registry_cache:
            return _registry_cache[cache_key]

        if db_path and Path(db_path).exists():
            # 有真实数据库：走 forge sync（自动枚举低基数列）
            with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
                tmp_path = Path(f.name)
            registry = run_sync(f"sqlite:///{db_path}", tmp_path)
            tmp_path.unlink(missing_ok=True)
        elif ddl_dir:
            # 无真实数据库：从 DDL.csv 解析表结构
            registry = _registry_from_ddl(Path(ddl_dir))
        else:
            registry = {"tables": {}}

        _registry_cache[cache_key] = registry
        return registry


def _registry_from_ddl(ddl_dir: Path) -> dict:
    """从 Spider2 DDL.csv 构建 Forge registry（无 .sqlite 文件时的回退）。"""
    import csv, re

    ddl_path = ddl_dir / "DDL.csv"
    if not ddl_path.exists():
        return {"tables": {}}

    tables: dict[str, dict] = {}
    with ddl_path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            table_name = row.get("table_name", "").strip()
            ddl        = row.get("DDL", "")
            if not table_name:
                continue
            # 从 DDL 中提取列名（匹配行首缩进 + 标识符 + 类型）
            cols = {}
            for line in ddl.splitlines():
                m = re.match(r"^\s+([A-Za-z_]\w*)\s+\w", line)
                if m:
                    col_name = m.group(1)
                    # 跳过 SQLite 关键字行（PRIMARY、FOREIGN、UNIQUE、CHECK）
                    if col_name.upper() not in ("PRIMARY", "FOREIGN", "UNIQUE", "CHECK", "CONSTRAINT"):
                        cols[col_name] = {}
            tables[table_name] = {"columns": cols}

    return {"tables": tables}


def registry_to_context(registry: dict) -> str:
    """将 registry dict 转为 LLM 可读的表结构文本。"""
    lines = ["Database schema:"]
    tables = registry.get("tables", registry)
    for table, info in tables.items():
        cols = info.get("columns", info) if isinstance(info, dict) else info
        if isinstance(cols, dict):
            parts = []
            for col_name, meta in cols.items():
                if isinstance(meta, dict) and meta.get("enum"):
                    parts.append(f"{col_name}[{'/'.join(str(v) for v in meta['enum'])}]")
                else:
                    parts.append(col_name)
            lines.append(f"  {table}: {', '.join(parts)}")
        else:
            lines.append(f"  {table}: {', '.join(cols)}")
    return "\n".join(lines)


# ── LLM 调用（tool_use 模式）────────────────────────────────────────────────

def _call_tool_use(client, system: str, question: str, tools: list[dict],
                   _retries: int = 5, _backoff: float = 10.0) -> dict:
    """
    调用 LLM，要求使用 generate_forge_query 工具，返回工具调用的 input dict。
    """
    import anthropic

    for attempt in range(_retries):
        try:
            msg = client.messages.create(
                model=MINIMAX_MODEL,
                max_tokens=2048,
                system=system,
                tools=tools,
                tool_choice={"type": "any"},    # 强制必须调用工具
                messages=[{"role": "user", "content": question}],
            )
            for block in msg.content:
                if getattr(block, "type", None) == "tool_use":
                    return {"ok": True, "input": block.input, "tool": block.name}
            # 模型没有调用工具（直接文字回复）
            text = next((b.text for b in msg.content if hasattr(b, "text")), "")
            return {"ok": False, "error": f"模型未调用工具，文字回复：{text[:200]}"}

        except anthropic.InternalServerError:
            if attempt < _retries - 1:
                wait = _backoff * (2 ** attempt)
                tqdm.write(f"  ⚠ API 500，等待 {wait:.0f}s 后重试 (attempt {attempt+1}/{_retries})")
                time.sleep(wait)
            else:
                raise


def run_case(client, case: dict) -> dict:
    """对单个用例完整走 Forge 管道，返回结构化结果。"""
    db_path  = case["_db_path"]
    question = case["question"]

    try:
        registry = get_registry(case)
    except Exception as e:
        return {"instance_id": case["instance_id"], "sql": None,
                "forge_json": None, "error": f"registry 构建失败: {e}"}

    tools  = [{"name": "generate_forge_query",
               "description": "Generate a Forge JSON query from natural language.",
               "input_schema": build_tool_schema(registry)}]
    system = build_system(registry_to_context(registry))

    try:
        resp = _call_tool_use(client, system, question, tools)
    except Exception as e:
        return {"instance_id": case["instance_id"], "sql": None,
                "forge_json": None, "error": f"LLM 调用失败: {e}"}

    if not resp["ok"]:
        return {"instance_id": case["instance_id"], "sql": None,
                "forge_json": None, "error": resp["error"]}

    forge_json = resp["input"]
    try:
        sql = compile_query(forge_json)
        return {"instance_id": case["instance_id"], "sql": sql,
                "forge_json": forge_json, "error": None}
    except Exception as e:
        return {"instance_id": case["instance_id"], "sql": None,
                "forge_json": forge_json, "error": f"编译失败: {e}"}


# ── Token 估算 ───────────────────────────────────────────────────────────────

def estimate_tokens(cases: list[dict]) -> None:
    """粗估每个用例的 token 消耗（不调用 API）。"""
    print("\n📊 Token 消耗估算（粗估，实际会有偏差）\n")

    sample_sizes = []
    for case in cases[:20]:   # 采样前 20 个
        try:
            registry = get_registry(case)
        except Exception:
            continue

        context   = registry_to_context(registry)
        system    = build_system(context)
        question  = case["question"]

        # 粗估：每 4 个字符 ≈ 1 token（英文）
        sys_tokens = len(system) // 4
        q_tokens   = len(question) // 4
        # tool schema 约 800 token（固定开销）
        tool_tokens = 800
        # 输出约 400 token（Forge JSON）
        out_tokens  = 400

        total = sys_tokens + q_tokens + tool_tokens + out_tokens
        sample_sizes.append({
            "id":       case["instance_id"],
            "db":       case["db"],
            "sys":      sys_tokens,
            "question": q_tokens,
            "tools":    tool_tokens,
            "output":   out_tokens,
            "total":    total,
        })

    if not sample_sizes:
        print("  无法估算（没有可访问的 SQLite 数据库）")
        return

    avg_total  = sum(s["total"] for s in sample_sizes) / len(sample_sizes)
    avg_sys    = sum(s["sys"]   for s in sample_sizes) / len(sample_sizes)
    total_cases = len(cases)

    print(f"  采样用例数：{len(sample_sizes)}")
    print(f"  平均 system prompt tokens：{avg_sys:.0f}")
    print(f"  平均每次 API 调用 tokens： {avg_total:.0f}")
    print(f"\n  总用例数：{total_cases}")
    print(f"  预计总 token 消耗（×1次）：{avg_total * total_cases:,.0f}")
    print(f"  预计总 token 消耗（×3次）：{avg_total * total_cases * 3:,.0f}")
    print()

    # 逐样本明细
    print(f"  {'instance_id':<20} {'db':<20} {'system':>7} {'question':>8} {'total':>7}")
    print(f"  {'-'*20} {'-'*20} {'-'*7} {'-'*8} {'-'*7}")
    for s in sample_sizes[:10]:
        print(f"  {s['id']:<20} {s['db']:<20} {s['sys']:>7} {s['question']:>8} {s['total']:>7}")
    if len(sample_sizes) > 10:
        print(f"  ... (only showing first 10 of {len(sample_sizes)} samples)")


# ── 主流程 ───────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Spider2-Lite SQLite 测试运行器")
    parser.add_argument("--limit",    type=int, default=None, help="只跑前 N 个用例")
    parser.add_argument("--fresh",    action="store_true",    help="清空旧结果重跑")
    parser.add_argument("--workers",  type=int, default=5,    help="并发 worker 数（默认 5）")
    parser.add_argument("--estimate", action="store_true",    help="只估算 token 消耗，不实际运行")
    args = parser.parse_args()

    print(f"\n🔬 Spider2-Lite SQLite 测试（Forge pipeline, tool_use 模式）")
    print(f"   模型：{MINIMAX_MODEL}  |  并发：{args.workers}")

    cases = load_sqlite_cases()
    if args.limit:
        cases = cases[:args.limit]
        print(f"   限制：只跑前 {args.limit} 个用例")

    if args.estimate:
        estimate_tokens(cases)
        return

    if not MINIMAX_API_KEY:
        print("❌ 未设置 MINIMAX_API_KEY 环境变量", file=sys.stderr)
        sys.exit(1)

    # 输出目录
    out_dir = RESULTS_DIR / METHOD
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "run_log.jsonl"

    if args.fresh and log_path.exists():
        log_path.unlink()
        print(f"🗑  --fresh：清空旧日志")

    # 加载已完成的 instance_id（断点续跑）
    done: set[str] = set()
    if log_path.exists():
        for line in log_path.read_text().splitlines():
            try:
                done.add(json.loads(line)["instance_id"])
            except Exception:
                pass

    pending = [c for c in cases if c["instance_id"] not in done]
    no_db = [c for c in cases if not c.get("_db_path")]
    if no_db and not args.estimate:
        print(f"\n   ⚠  {len(no_db)} 个用例缺少 .sqlite 文件，将用 DDL.csv 构建 registry（仅 schema 约束，无枚举采样）")

    if not pending:
        print(f"✅ 所有 {len(cases)} 个用例已完成，无需重跑。")
        _print_stats(out_dir)
        return

    print(f"   待执行：{len(pending)} 个  |  已完成：{len(done)} 个\n")

    import anthropic
    client = anthropic.Anthropic(api_key=MINIMAX_API_KEY, base_url=MINIMAX_BASE_URL)

    log_lock = threading.Lock()
    ok_count = err_count = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        future_map = {pool.submit(run_case, client, c): c for c in pending}
        bar = tqdm(total=len(pending), unit="case", dynamic_ncols=True)

        for future in as_completed(future_map):
            result = future.result()
            iid    = result["instance_id"]

            # 保存 SQL 文件
            if result["sql"]:
                (out_dir / f"{iid}.sql").write_text(result["sql"])
                ok_count += 1
                status = "✓"
            else:
                err_count += 1
                status = f"✗ {str(result['error'])[:60]}"

            # 追加日志
            with log_lock:
                with log_path.open("a") as f:
                    f.write(json.dumps(result, ensure_ascii=False) + "\n")

            bar.write(f"  [{iid}] {status}")
            bar.update(1)

        bar.close()

    _print_stats(out_dir)


def _print_stats(out_dir: Path) -> None:
    log_path = out_dir / "run_log.jsonl"
    if not log_path.exists():
        return

    records = [json.loads(l) for l in log_path.read_text().splitlines() if l.strip()]
    ok  = sum(1 for r in records if not r.get("error"))
    err = sum(1 for r in records if r.get("error"))
    print(f"\n✅ 完成：{ok} 成功  |  {err} 失败  |  编译失败率 {err/(ok+err)*100:.1f}%")
    print(f"   SQL 文件 → {out_dir}/")


if __name__ == "__main__":
    main()
