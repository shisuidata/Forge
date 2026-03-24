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
from forge.lint import lint_conventions  # noqa: E402

ACCURACY_DIR = Path(__file__).parent
RESULTS_DIR  = ACCURACY_DIR / "results"
CASES_FILE   = RESULTS_DIR / "cases.json"   # default; override with --cases

MINIMAX_API_KEY  = os.environ.get("MINIMAX_API_KEY", "")
MINIMAX_BASE_URL = os.environ.get("MINIMAX_BASE_URL", "https://api.minimaxi.com/anthropic")
MINIMAX_MODEL    = os.environ.get("MINIMAX_MODEL", "MiniMax-M2.5-highspeed")

# Fallback：限速时自动切换（key 和 model 可各自独立覆盖）
MINIMAX_FALLBACK_KEY   = os.environ.get("MINIMAX_FALLBACK_KEY", "sk-api-FFEJeUuX8L5NY4P5XuiHj8bcyslSTWxTDiuH7j9AoRs5SKo66dfpjcDNW0GBEZav28JhFYi72tM2Fi2EaYrhoQybMr4LpFB0CIC0sEEQcHrYpWbzR3VlfYw")
MINIMAX_FALLBACK_MODEL = os.environ.get("MINIMAX_FALLBACK_MODEL", "MiniMax-M2.5")

# OpenAI-compatible provider（SiliconFlow / DeepSeek 官方）
DEEPSEEK_API_KEY  = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1")
DEEPSEEK_MODEL    = os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")

# 原生 Anthropic Claude API
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")


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


def _call_model(client: anthropic.Anthropic, system: str,
                model: str, max_tokens: int = 3000,
                question: str | None = None,
                messages: list[dict] | None = None,
                _retries: int = 5, _backoff: float = 10.0,
                _fallback_client: anthropic.Anthropic | None = None,
                _fallback_model: str | None = None) -> str:
    if messages is None:
        messages = [{"role": "user", "content": question}]
    cur_client, cur_model = client, model
    for attempt in range(_retries):
        try:
            msg = cur_client.messages.create(
                model=cur_model,
                max_tokens=max_tokens,
                system=system,
                messages=messages,
            )
            return _extract_text(msg.content)
        except anthropic.RateLimitError:
            if _fallback_client is not None and cur_client is client:
                tqdm.write(f"  ⚡ 主账号限速，切换到 fallback（{_fallback_model}）")
                cur_client = _fallback_client
                cur_model  = _fallback_model or model
                continue
            wait = _backoff * (2 ** attempt)
            tqdm.write(f"  ⚠ 限速，等待 {wait:.0f}s 后重试 (attempt {attempt+1}/{_retries})")
            time.sleep(wait)
        except anthropic.InternalServerError:
            if attempt < _retries - 1:
                wait = _backoff * (2 ** attempt)
                tqdm.write(f"  ⚠ API 500，等待 {wait:.0f}s 后重试 (attempt {attempt+1}/{_retries})")
                time.sleep(wait)
            else:
                raise


def run_forge(client: anthropic.Anthropic, question: str,
              system: str, model: str,
              fallback_client: anthropic.Anthropic | None = None,
              fallback_model: str | None = None,
              max_compile_retries: int = 0) -> dict:
    """Call model → parse Forge JSON → compile to SQL.

    max_compile_retries > 0 时，编译/解析失败后将错误反馈给模型重试，
    模拟 agent.py 的 MAX_RETRIES 行为。
    """
    messages = [{"role": "user", "content": question}]

    for attempt in range(1 + max_compile_retries):
        raw = _clean_json(_call_model(client, system, model=model,
                                      messages=messages,
                                      _fallback_client=fallback_client,
                                      _fallback_model=fallback_model))
        # 解析 JSON
        try:
            forge_json = json.loads(raw)
        except json.JSONDecodeError as e:
            if attempt < max_compile_retries:
                tqdm.write(f"    🔄 JSON 解析失败（第 {attempt+1} 次），重试...")
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content":
                    f"JSON 解析失败：{e}\n请修正后重新输出完整的 Forge JSON，不要包含任何解释文字。"})
                continue
            return {"forge_json": None, "sql": None,
                    "error": f"JSON解析失败: {e}\n原始输出: {raw[:500]}",
                    "attempts": attempt + 1}

        # 约定 lint（仅在首次尝试时检查，避免浪费所有重试次数）
        if attempt == 0 and max_compile_retries > 0:
            warnings = lint_conventions(forge_json, question)
            if warnings:
                warning_text = "\n".join(f"- {w}" for w in warnings)
                tqdm.write(f"    🔍 约定检查发现 {len(warnings)} 个问题，重试...")
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content":
                    f"约定检查发现以下问题：\n{warning_text}\n请修正后重新输出完整的 Forge JSON。"})
                continue

        # 编译
        try:
            sql = compile_query(forge_json)
            return {"forge_json": forge_json, "sql": sql, "error": None,
                    "attempts": attempt + 1}
        except Exception as e:
            if attempt < max_compile_retries:
                tqdm.write(f"    🔄 编译失败（第 {attempt+1} 次）：{str(e)[:60]}，重试...")
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content":
                    f"Forge JSON 编译失败：{e}\n请根据错误信息修正后重新输出完整的 Forge JSON。"})
                continue
            return {"forge_json": forge_json, "sql": None,
                    "error": f"编译失败: {e}",
                    "attempts": attempt + 1}

    return {"forge_json": None, "sql": None, "error": "重试耗尽",
            "attempts": 1 + max_compile_retries}


def run_sql(client: anthropic.Anthropic, question: str,
            system: str, model: str,
            fallback_client: anthropic.Anthropic | None = None,
            fallback_model: str | None = None) -> dict:
    """Call model → return raw SQL."""
    sql = _call_model(client, system, model=model, question=question,
                      _fallback_client=fallback_client,
                      _fallback_model=fallback_model)
    return {"forge_json": None, "sql": sql, "error": None}


# ── OpenAI-compatible API 调用（SiliconFlow / DeepSeek 官方等）────────────────

def _call_model_oai(api_key: str, base_url: str, system: str,
                    model: str, max_tokens: int = 4096,
                    question: str | None = None,
                    messages: list[dict] | None = None,
                    _retries: int = 5, _backoff: float = 10.0) -> str:
    """直接 HTTP 调用 OpenAI-compatible /chat/completions。"""
    import requests as _req
    url = base_url.rstrip("/") + "/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    if messages is None:
        messages = [{"role": "user", "content": question}]
    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "system", "content": system}] + messages,
    }
    for attempt in range(_retries):
        try:
            resp = _req.post(url, headers=headers, json=payload, timeout=120)
            if resp.status_code == 429:
                wait = _backoff * (2 ** attempt)
                tqdm.write(f"  ⚠ 限速，等待 {wait:.0f}s 后重试 (attempt {attempt+1}/{_retries})")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            body = resp.json()
            return body["choices"][0]["message"]["content"].strip()
        except Exception as e:
            if attempt < _retries - 1:
                wait = _backoff * (2 ** attempt)
                tqdm.write(f"  ⚠ OAI 调用失败 {e}，等待 {wait:.0f}s 后重试")
                time.sleep(wait)
            else:
                raise


def run_forge_oai(api_key: str, base_url: str, question: str,
                  system: str, model: str,
                  max_compile_retries: int = 0) -> dict:
    """OpenAI-compatible: Call model → parse Forge JSON → compile to SQL."""
    messages = [{"role": "user", "content": question}]

    for attempt in range(1 + max_compile_retries):
        raw = _clean_json(_call_model_oai(api_key, base_url, system,
                                          model=model, messages=messages))
        try:
            forge_json = json.loads(raw)
        except json.JSONDecodeError as e:
            if attempt < max_compile_retries:
                tqdm.write(f"    🔄 JSON 解析失败（第 {attempt+1} 次），重试...")
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content":
                    f"JSON 解析失败：{e}\n请修正后重新输出完整的 Forge JSON，不要包含任何解释文字。"})
                continue
            return {"forge_json": None, "sql": None,
                    "error": f"JSON解析失败: {e}\n原始输出: {raw[:500]}",
                    "attempts": attempt + 1}

        # 约定 lint
        if attempt == 0 and max_compile_retries > 0:
            warnings = lint_conventions(forge_json, question)
            if warnings:
                warning_text = "\n".join(f"- {w}" for w in warnings)
                tqdm.write(f"    🔍 约定检查发现 {len(warnings)} 个问题，重试...")
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content":
                    f"约定检查发现以下问题：\n{warning_text}\n请修正后重新输出完整的 Forge JSON。"})
                continue

        try:
            sql = compile_query(forge_json)
            return {"forge_json": forge_json, "sql": sql, "error": None,
                    "attempts": attempt + 1}
        except Exception as e:
            if attempt < max_compile_retries:
                tqdm.write(f"    🔄 编译失败（第 {attempt+1} 次）：{str(e)[:60]}，重试...")
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content":
                    f"Forge JSON 编译失败：{e}\n请根据错误信息修正后重新输出完整的 Forge JSON。"})
                continue
            return {"forge_json": forge_json, "sql": None,
                    "error": f"编译失败: {e}",
                    "attempts": attempt + 1}

    return {"forge_json": None, "sql": None, "error": "重试耗尽",
            "attempts": 1 + max_compile_retries}


def run_sql_oai(api_key: str, base_url: str, question: str,
                system: str, model: str) -> dict:
    """OpenAI-compatible: Call model → return raw SQL."""
    sql = _call_model_oai(api_key, base_url, system, model=model, question=question)
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
               workers_override: int | None = None,
               compile_retries: int = 0) -> None:
    from methods import load  # local import after sys.path setup

    cfg = load(method_id)
    runs_per_method = runs_override or cfg.runs
    max_workers     = workers_override or 10

    # 用例文件优先级：method 文件声明 > --cases 参数 > 默认 cases.json
    cases_path = Path(cfg.cases_file) if cfg.cases_file else CASES_FILE
    if not cases_path.exists():
        print(f"❌ 找不到测试用例 {cases_path}", file=sys.stderr)
        sys.exit(1)

    cases = json.loads(cases_path.read_text())

    if fresh:
        existing: dict = {}
        print(f"🗑  --fresh：清空 method_{method_id} 旧结果")
    else:
        existing = load_runs(method_id)

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

    # ── provider 路由 ──────────────────────────────────────────────────────────
    is_oai = cfg.llm_provider == "openai"
    if is_oai:
        # OpenAI-compatible（DeepSeek / SiliconFlow 等）
        oai_api_key  = cfg.api_key  or DEEPSEEK_API_KEY
        oai_base_url = cfg.base_url or DEEPSEEK_BASE_URL
        oai_model    = cfg.model
        if not oai_api_key:
            print(f"❌ OpenAI provider 需要 API Key（方法文件 API_KEY 或环境变量 DEEPSEEK_API_KEY）", file=sys.stderr)
            sys.exit(1)
        print(f"\n🔬 {cfg.label}")
        print(f"   Provider：openai-compat  模型：{oai_model}  并发：{max_workers}  每用例：{runs_per_method} 次")
        print(f"   Endpoint：{oai_base_url}")
        if compile_retries > 0:
            print(f"   编译重试：最多 {compile_retries} 次（模拟 agent 行为）")
        print(f"   待执行：{len(tasks)} 次 API 调用\n")
    else:
        # Anthropic SDK（MiniMax / 原生 Claude 等）
        model = cfg.model if cfg.model != "MiniMax-M2.5-highspeed" else MINIMAX_MODEL
        # 原生 Claude：base_url 为空或显式为 "anthropic"，走 ANTHROPIC_API_KEY
        is_native_claude = not cfg.base_url or cfg.base_url == "anthropic"
        if is_native_claude:
            client = anthropic.Anthropic(api_key=cfg.api_key or ANTHROPIC_API_KEY)
            fallback_client = None
            fallback_model = None
        else:
            client = anthropic.Anthropic(
                api_key=cfg.api_key or MINIMAX_API_KEY,
                base_url=cfg.base_url or MINIMAX_BASE_URL,
            )
            fallback_client = (
                anthropic.Anthropic(api_key=MINIMAX_FALLBACK_KEY, base_url=MINIMAX_BASE_URL)
                if MINIMAX_FALLBACK_KEY else None
            )
            fallback_model = MINIMAX_FALLBACK_MODEL if MINIMAX_FALLBACK_KEY else None
        print(f"\n🔬 {cfg.label}")
        print(f"   模型：{model}  |  并发：{max_workers}  |  每用例：{runs_per_method} 次")
        if compile_retries > 0:
            print(f"   编译重试：最多 {compile_retries} 次（模拟 agent 行为）")
        print(f"   待执行：{len(tasks)} 次 API 调用\n")
        if fallback_client:
            print(f"   Fallback：{fallback_model}（主账号限速时自动切换）")

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

    if cfg.registry_context is not None:
        # 新式：每次调用动态生成 prompt，始终反映 agent/prompts.py 当前版本
        # mode="benchmark"：直接输出 JSON，不使用工具调用
        from agent.prompts import build_system as _build_system
        def _get_system(question: str) -> str:
            return _build_system(cfg.registry_context, question=question, mode="benchmark")
    else:
        # 旧式：使用方法文件中冻结的 SYSTEM_PROMPT
        def _get_system(question: str) -> str:
            return cfg.system_prompt

    if is_oai:
        def dispatch(task):
            case, run_idx = task
            question = _enrich(case["question"])
            if cfg.mode == "forge":
                result = run_forge_oai(oai_api_key, oai_base_url, question,
                                       _get_system(question), oai_model,
                                       max_compile_retries=compile_retries)
            else:
                result = run_sql_oai(oai_api_key, oai_base_url, question,
                                     _get_system(question), oai_model)
            return case, run_idx, result
    else:
        def dispatch(task):
            case, run_idx = task
            question = _enrich(case["question"])
            if cfg.mode == "forge":
                result = run_forge(client, question, _get_system(question), model,
                                   fallback_client=fallback_client,
                                   fallback_model=fallback_model,
                                   max_compile_retries=compile_retries)
            else:
                result = run_sql(client, question, _get_system(question), model,
                                 fallback_client=fallback_client,
                                 fallback_model=fallback_model)
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
    parser.add_argument("--cases", type=str, default=None,
                        help="测试用例文件路径（默认 results/cases.json），如 results/cases_large.json")
    parser.add_argument("--retry", type=int, default=0,
                        help="编译失败时的最大重试次数（默认 0 = 不重试）。"
                             "设为 2 可模拟 agent.py 的 MAX_RETRIES 行为。")
    args = parser.parse_args()

    global CASES_FILE
    if args.cases:
        p = Path(args.cases)
        CASES_FILE = p if p.is_absolute() else ACCURACY_DIR / args.cases

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
                   workers_override=args.workers,
                   compile_retries=args.retry)


if __name__ == "__main__":
    main()
