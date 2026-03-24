#!/usr/bin/env python3
"""
Claude 直连 benchmark runner。

通过 `claude --print` 非交互模式调用当前 Claude Code 实例，
无需独立 API Key。

用法：
    python tests/accuracy/run_claude_direct.py
    python tests/accuracy/run_claude_direct.py --fresh
    python tests/accuracy/run_claude_direct.py --runs 1  # 快速验证
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))

from forge.compiler import compile_query  # noqa: E402
from agent.prompts import build_system    # noqa: E402

METHOD_ID   = "t"
LABEL       = "Method T（large 数据集，Claude Sonnet 4.6 直连）"
RUNS        = 3
MAX_WORKERS = 3   # claude --print 串行，不宜太高

DATASETS_DIR = ROOT / "tests" / "datasets" / "large"
CASES_FILE   = DATASETS_DIR / "cases.json"
RESULTS_DIR  = Path(__file__).parent / "results" / f"method_{METHOD_ID}"

REGISTRY_CONTEXT = (DATASETS_DIR / "schema_context.md").read_text(encoding="utf-8").strip()


def _call_claude(system: str, question: str) -> str:
    """通过 claude --print 调用当前 Claude 实例，返回原始文本输出。"""
    prompt = f"<system>\n{system}\n</system>\n\n{question}"
    result = subprocess.run(
        ["claude", "--print", "--no-session-persistence"],
        input=prompt,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return result.stdout.strip()


def _clean_json(raw: str) -> str:
    s = raw.strip()
    if s.startswith("```"):
        lines = s.split("\n")
        inner = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        s = "\n".join(inner).strip()
    return s


def run_one(question: str) -> dict:
    system = build_system(REGISTRY_CONTEXT, question=question, mode="benchmark")
    try:
        raw = _call_claude(system, question)
    except subprocess.TimeoutExpired:
        return {"forge_json": None, "sql": None, "error": "timeout"}
    except Exception as e:
        return {"forge_json": None, "sql": None, "error": str(e)}

    raw = _clean_json(raw)
    try:
        forge_json = json.loads(raw)
    except json.JSONDecodeError as e:
        return {"forge_json": None, "sql": None,
                "error": f"JSON解析失败: {e}\n原始输出: {raw[:300]}"}
    try:
        sql = compile_query(forge_json)
        return {"forge_json": forge_json, "sql": sql, "error": None}
    except Exception as e:
        return {"forge_json": forge_json, "sql": None, "error": f"编译失败: {e}"}


def load_runs() -> dict:
    p = RESULTS_DIR / "runs.json"
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            pass
    return {}


def save_runs(data: dict) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    (RESULTS_DIR / "runs.json").write_text(
        json.dumps(data, ensure_ascii=False, indent=2)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Claude 直连 benchmark")
    parser.add_argument("--fresh", action="store_true", help="清空旧结果重跑")
    parser.add_argument("--runs", type=int, default=RUNS, help="每题运行次数")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    args = parser.parse_args()

    cases = json.loads(CASES_FILE.read_text())
    existing = {} if args.fresh else load_runs()

    tasks = []
    for case in cases:
        cid = str(case["id"])
        done = len(existing.get(cid, {}).get("runs", []))
        for i in range(done, args.runs):
            tasks.append((case, i))

    if not tasks:
        print(f"✅ {LABEL} 所有用例已完成，无需重跑。")
        return

    print(f"\n🔬 {LABEL}")
    print(f"   模型：claude-sonnet-4-6（claude --print）")
    print(f"   并发：{args.workers}  每用例：{args.runs} 次")
    print(f"   待执行：{len(tasks)} 次调用\n")

    buf: dict[str, dict] = {}
    for case in cases:
        cid = str(case["id"])
        buf[cid] = {
            "question": case["question"],
            "category": case["category"],
            "difficulty": case["difficulty"],
            "runs": list(existing.get(cid, {}).get("runs", [])),
        }

    save_lock = threading.Lock()
    done_cases: set[str] = set()
    pending = len({t[0]["id"] for t in tasks})

    case_bar = tqdm(total=len(cases), desc="用例完成",
                    initial=len(cases) - pending, unit="case", position=0)
    call_bar = tqdm(total=len(tasks), desc="调用进度",
                    unit="call", position=1)

    def dispatch(task):
        case, run_idx = task
        result = run_one(case["question"])
        return case, run_idx, result

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        future_map = {pool.submit(dispatch, t): t for t in tasks}
        for future in as_completed(future_map):
            case, run_idx, result = future.result()
            cid = str(case["id"])
            slot = buf[cid]
            runs = slot["runs"]
            while len(runs) <= run_idx:
                runs.append(None)
            runs[run_idx] = result

            status = "✓" if result.get("error") is None else f"✗ {str(result['error'])[:50]}"
            call_bar.write(f"  [{cid:>2}] run-{run_idx+1}: {status}")
            call_bar.update(1)

            if (cid not in done_cases
                    and len(slot["runs"]) == args.runs
                    and None not in slot["runs"]):
                done_cases.add(cid)
                with save_lock:
                    existing[cid] = slot
                    save_runs(existing)
                case_bar.update(1)
                call_bar.write(f"💾 [{cid:>2}] {case['category']} 完成")

    call_bar.close()
    case_bar.close()

    all_runs = [r for v in existing.values() for r in v.get("runs", [])]
    ok  = sum(1 for r in all_runs if r and r.get("error") is None)
    err = sum(1 for r in all_runs if r and r.get("error") is not None)
    print(f"\n✅ method_{METHOD_ID}：{len(existing)} 用例 | 成功 {ok} / 失败 {err}")
    print(f"   结果 → {RESULTS_DIR / 'runs.json'}")


if __name__ == "__main__":
    main()
