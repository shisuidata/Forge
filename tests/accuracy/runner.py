#!/usr/bin/env python3
"""
通用 Forge DSL 测试运行器。

用法：
    python tests/accuracy/runner.py --method f
    python tests/accuracy/runner.py --method e,f        # 同时跑多个
    python tests/accuracy/runner.py --method all        # 跑所有已定义方法
    python tests/accuracy/runner.py --method f --fresh  # 清空旧结果重跑
    python tests/accuracy/runner.py --list              # 列出所有可用方法

结果保存到：tests/accuracy/results/method_{id}/runs.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import anthropic
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).parent))  # for methods/ import

from forge.compiler import compile_query  # noqa: E402

ACCURACY_DIR = Path(__file__).parent
RESULTS_DIR  = ACCURACY_DIR / "results"
CASES_FILE   = RESULTS_DIR / "cases.json"

MINIMAX_API_KEY  = os.environ.get("MINIMAX_API_KEY", "")
MINIMAX_BASE_URL = os.environ.get("MINIMAX_BASE_URL", "https://api.minimaxi.com/anthropic")
MINIMAX_MODEL    = os.environ.get("MINIMAX_MODEL", "MiniMax-M2.5-highspeed")


# ── API 调用 ──────────────────────────────────────────────────────────────────

def _extract_text(content) -> str:
    if isinstance(content, list):
        return "".join(
            b.text for b in content if getattr(b, "type", None) == "text"
        ).strip()
    return str(content).strip()


def _clean_json(raw: str) -> str:
    s = raw.strip()
    if s.startswith("```"):
        lines = s.split("\n")
        inner = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        s = "\n".join(inner).strip()
    return s


def _call_model(client: anthropic.Anthropic, system: str, question: str,
                model: str, max_tokens: int = 2048,
                _retries: int = 5, _backoff: float = 10.0) -> str:
    for attempt in range(_retries):
        try:
            msg = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=system,
                messages=[{"role": "user", "content": question}],
            )
            return _extract_text(msg.content)
        except anthropic.InternalServerError as e:
            if attempt < _retries - 1:
                wait = _backoff * (2 ** attempt)
                tqdm.write(f"  ⚠ API 500，等待 {wait:.0f}s 后重试 (attempt {attempt+1}/{_retries})")
                time.sleep(wait)
            else:
                raise


def run_forge(client: anthropic.Anthropic, question: str,
              system: str, model: str) -> dict:
    """Call model → parse Forge JSON → compile to SQL."""
    raw = _clean_json(_call_model(client, system, question, model))
    try:
        forge_json = json.loads(raw)
    except json.JSONDecodeError as e:
        return {"forge_json": None, "sql": None,
                "error": f"JSON解析失败: {e}\n原始输出: {raw[:500]}"}
    try:
        sql = compile_query(forge_json)
        return {"forge_json": forge_json, "sql": sql, "error": None}
    except Exception as e:
        return {"forge_json": forge_json, "sql": None,
                "error": f"编译失败: {e}"}


def run_sql(client: anthropic.Anthropic, question: str,
            system: str, model: str) -> dict:
    """Call model → return raw SQL."""
    sql = _call_model(client, system, question, model)
    return {"forge_json": None, "sql": sql, "error": None}


# ── Result I/O ────────────────────────────────────────────────────────────────

def method_dir(method_id: str) -> Path:
    return RESULTS_DIR / f"method_{method_id}"


def load_runs(method_id: str) -> dict:
    path = method_dir(method_id) / "runs.json"
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return {}


def save_runs(method_id: str, data: dict) -> None:
    d = method_dir(method_id)
    d.mkdir(parents=True, exist_ok=True)
    (d / "runs.json").write_text(json.dumps(data, ensure_ascii=False, indent=2))


# ── Main runner ───────────────────────────────────────────────────────────────

def run_method(method_id: str, fresh: bool = False,
               runs_override: int | None = None,
               workers_override: int | None = None) -> None:
    from methods import load  # local import after sys.path setup

    cfg = load(method_id)
    runs_per_method = runs_override or cfg.runs
    max_workers     = workers_override or 10
    model           = MINIMAX_MODEL  # env var takes precedence

    if not CASES_FILE.exists():
        print(f"❌ 找不到 {CASES_FILE}，请先运行 generate_cases.py", file=sys.stderr)
        sys.exit(1)

    cases = json.loads(CASES_FILE.read_text())

    if fresh:
        existing: dict = {}
        print(f"🗑  --fresh：清空 method_{method_id} 旧结果")
    else:
        existing = load_runs(method_id)

    dispatch_fn = run_forge if cfg.mode == "forge" else run_sql

    # 构建待执行任务
    tasks: list[tuple] = []
    for case in cases:
        cid  = str(case["id"])
        done = len(existing.get(cid, {}).get("runs", []))
        for i in range(done, runs_per_method):
            tasks.append((case, i))

    if not tasks:
        print(f"✅ method_{method_id} 所有 {len(cases)} 个用例已完成，无需重跑。")
        _print_stats(method_id, existing)
        return

    print(f"\n🔬 {cfg.label}")
    print(f"   模型：{model}  |  并发：{max_workers}  |  每用例：{runs_per_method} 次")
    print(f"   待执行：{len(tasks)} 次 API 调用\n")

    client = anthropic.Anthropic(api_key=MINIMAX_API_KEY, base_url=MINIMAX_BASE_URL)

    # 初始化结果 buf（合并已有结果）
    buf: dict[str, dict] = {}
    for case in cases:
        cid = str(case["id"])
        buf[cid] = {
            "question":  case["question"],
            "category":  case["category"],
            "difficulty": case["difficulty"],
            "runs": list(existing.get(cid, {}).get("runs", [])),
        }

    save_lock = threading.Lock()
    done_cases: set[str] = set()

    pending_cases = len({t[0]["id"] for t in tasks})
    case_bar = tqdm(total=len(cases), desc="用例完成",
                    initial=len(cases) - pending_cases,
                    unit="case", position=0, dynamic_ncols=True)
    call_bar = tqdm(total=len(tasks), desc="API 调用",
                    unit="call", position=1, dynamic_ncols=True)

    if cfg.use_semantic_lib:
        from semantic_lib import enrich as _enrich
    else:
        _enrich = lambda q: q

    def dispatch(task):
        case, run_idx = task
        question = _enrich(case["question"])
        result = dispatch_fn(client, question, cfg.system_prompt, model)
        return case, run_idx, result

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {pool.submit(dispatch, t): t for t in tasks}

        for future in as_completed(future_map):
            case, run_idx, result = future.result()
            cid  = str(case["id"])
            slot = buf[cid]

            runs = slot["runs"]
            while len(runs) <= run_idx:
                runs.append(None)
            runs[run_idx] = result

            status = "✓" if result.get("error") is None else f"✗ {str(result['error'])[:40]}"
            call_bar.write(f"  [{cid:>2}] run-{run_idx+1}: {status}")
            call_bar.update(1)

            # 检查该用例是否全部完成
            if (cid not in done_cases
                    and len(slot["runs"]) == runs_per_method
                    and None not in slot["runs"]):
                done_cases.add(cid)
                with save_lock:
                    existing[cid] = slot
                    save_runs(method_id, existing)
                case_bar.update(1)
                call_bar.write(f"💾 [{cid:>2}] {case['category']} 完成")

    call_bar.close()
    case_bar.close()
    _print_stats(method_id, existing)


def _print_stats(method_id: str, data: dict) -> None:
    total = len(data)
    all_runs = [r for v in data.values() for r in v.get("runs", [])]
    if not all_runs:
        print(f"\n   method_{method_id}：无结果数据")
        return
    ok  = sum(1 for r in all_runs if r and r.get("error") is None)
    err = sum(1 for r in all_runs if r and r.get("error") is not None)
    rate = f"{err/(ok+err)*100:.1f}%" if (ok + err) > 0 else "N/A"
    print(f"\n✅ method_{method_id}：{total} 用例 | 成功 {ok} / 失败 {err} | 编译失败率 {rate}")
    path = method_dir(method_id) / "runs.json"
    print(f"   结果 → {path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Forge DSL 准确性测试运行器",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--method", "-m", default="",
                        help="方法 id，逗号分隔（如 'f' 或 'e,f'），或 'all'")
    parser.add_argument("--list", "-l", action="store_true",
                        help="列出所有可用方法")
    parser.add_argument("--fresh", action="store_true",
                        help="清空旧结果，从头重跑")
    parser.add_argument("--runs", type=int, default=None,
                        help="覆盖每方法运行次数")
    parser.add_argument("--workers", type=int, default=None,
                        help="覆盖并发 worker 数")
    args = parser.parse_args()

    from methods import list_methods  # noqa: E402

    if args.list:
        print("可用方法：")
        for mid in list_methods():
            from methods import load
            try:
                cfg = load(mid)
                print(f"  {mid}  {cfg.label}")
                if cfg.notes:
                    print(f"       {cfg.notes}")
            except Exception as e:
                print(f"  {mid}  ❌ 加载失败: {e}")
        return

    if not args.method:
        parser.print_help()
        sys.exit(1)

    if args.method == "all":
        method_ids = list_methods()
    else:
        method_ids = [m.strip() for m in args.method.split(",") if m.strip()]

    if not MINIMAX_API_KEY:
        print("❌ 未设置 MINIMAX_API_KEY 环境变量", file=sys.stderr)
        sys.exit(1)

    for mid in method_ids:
        run_method(mid, fresh=args.fresh,
                   runs_override=args.runs,
                   workers_override=args.workers)


if __name__ == "__main__":
    main()
