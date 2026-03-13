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
import csv as _csv_mod
import sqlite3
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
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
RESULTS_DIR  = Path(__file__).parent / "results"
GOLD_SQL_DIR = DATA_DIR / "evaluation_suite" / "gold" / "sql"
GOLD_CSV_DIR = DATA_DIR / "evaluation_suite" / "gold" / "exec_result"

MINIMAX_API_KEY  = os.environ.get("MINIMAX_API_KEY", "")
MINIMAX_BASE_URL = os.environ.get("MINIMAX_BASE_URL", "https://api.minimaxi.com/anthropic")
MINIMAX_MODEL    = os.environ.get("MINIMAX_MODEL", "MiniMax-M2.5-highspeed")

# 通用 LLM 配置（优先于 MINIMAX_* 旧配置）
# LLM_FORMAT: "anthropic"（Anthropic SDK）或 "openai"（OpenAI 兼容，如硅基流动）
LLM_API_KEY  = os.environ.get("LLM_API_KEY",  MINIMAX_API_KEY)
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", MINIMAX_BASE_URL)
LLM_MODEL    = os.environ.get("LLM_MODEL",    MINIMAX_MODEL)
LLM_FORMAT   = os.environ.get("LLM_FORMAT",   "anthropic")   # "anthropic" | "openai"

METHOD = os.environ.get("LLM_METHOD", "forge_j")   # 方法标识 / 输出目录名

# schema token 超过此阈值时启用向量化表过滤
SCHEMA_TOKEN_LIMIT = 4000


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

REGISTRY_DISK_CACHE = Path(__file__).parent / "results" / "_registry_cache"


def get_registry(case: dict) -> dict:
    """
    优先用实际 .sqlite 文件做 run_sync（枚举采样更准确）；
    若 .sqlite 不存在，退而从 DDL.csv 解析表结构。
    对同一数据库只处理一次，后续用缓存。
    磁盘缓存：results/_registry_cache/{db_name}.json，跨进程复用。
    """
    db_path  = case.get("_db_path", "")
    ddl_dir  = case.get("_ddl_dir", "")
    cache_key = db_path or ddl_dir

    if cache_key in _registry_cache:
        return _registry_cache[cache_key]

    with _registry_lock:
        if cache_key in _registry_cache:
            return _registry_cache[cache_key]

        # 磁盘缓存 key：用数据库名（避免路径含特殊字符）
        db_name = Path(ddl_dir).name if ddl_dir else Path(db_path).stem
        disk_cache_path = REGISTRY_DISK_CACHE / f"{db_name}.json"

        if disk_cache_path.exists():
            registry = json.loads(disk_cache_path.read_text(encoding="utf-8"))
        elif db_path and Path(db_path).exists():
            # 有真实数据库：走 forge sync（自动枚举低基数列），最多等 60s
            from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
            print(f"  {ts}  sync  {db_name} ...", flush=True)
            with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
                tmp_path = Path(f.name)
            t0 = time.monotonic()
            try:
                ex  = ThreadPoolExecutor(max_workers=1)
                fut = ex.submit(run_sync, f"sqlite:///{db_path}", tmp_path)
                try:
                    registry = fut.result(timeout=60)
                    elapsed  = time.monotonic() - t0
                    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
                    print(f"  {ts}  sync  {db_name} done ({elapsed:.1f}s, cached)", flush=True)
                except FuturesTimeout:
                    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
                    print(f"  {ts}  sync  {db_name} TIMEOUT — fallback to DDL.csv", flush=True)
                    registry = _registry_from_ddl(Path(ddl_dir)) if ddl_dir else {"tables": {}}
                finally:
                    ex.shutdown(wait=False)   # 不阻塞等待超时线程
            finally:
                tmp_path.unlink(missing_ok=True)
            REGISTRY_DISK_CACHE.mkdir(parents=True, exist_ok=True)
            disk_cache_path.write_text(json.dumps(registry, ensure_ascii=False, indent=2), encoding="utf-8")
        elif ddl_dir:
            # 无真实数据库：从 DDL.csv 解析表结构
            registry = _registry_from_ddl(Path(ddl_dir))
            REGISTRY_DISK_CACHE.mkdir(parents=True, exist_ok=True)
            disk_cache_path.write_text(json.dumps(registry, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            registry = {"tables": {}}

        _registry_cache[cache_key] = registry
        return registry


def _registry_from_ddl(ddl_dir: Path) -> dict:
    """
    从 Spider2 DDL.csv + {table}.json 构建 Forge registry。
    - DDL.csv  → 表名、列名
    - {table}.json → sample_rows，推断低基数列的枚举值（≤20个唯一值视为枚举）
    """
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
            cols = {}
            for line in ddl.splitlines():
                m = re.match(r"^\s+([A-Za-z_]\w*)\s+\w", line)
                if m:
                    col_name = m.group(1)
                    if col_name.upper() not in ("PRIMARY", "FOREIGN", "UNIQUE", "CHECK", "CONSTRAINT"):
                        cols[col_name] = {}
            tables[table_name] = {"columns": cols}

    # 用样本 JSON 推断枚举值
    _enrich_from_samples(ddl_dir, tables)

    return {"tables": tables}


def _enrich_from_samples(ddl_dir: Path, tables: dict) -> None:
    """
    读取 ddl_dir/{table}.json 里的 sample_rows，
    对唯一值 ≤ 20 的字符串/整数列注入枚举提示。
    """
    ENUM_THRESHOLD = 20

    for table_name, table_info in tables.items():
        json_path = ddl_dir / f"{table_name}.json"
        if not json_path.exists():
            # 尝试小写文件名
            json_path = ddl_dir / f"{table_name.lower()}.json"
        if not json_path.exists():
            continue
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception:
            continue

        rows = data.get("sample_rows", [])
        if not rows:
            continue

        # 按列收集所有样本值
        col_values: dict[str, set] = {}
        for row in rows:
            for col, val in row.items():
                if val is None or val == "":
                    continue
                col_values.setdefault(col, set()).add(val)

        cols = table_info.get("columns", {})
        for col_name, vals in col_values.items():
            if col_name not in cols:
                continue
            # 只对字符串或小整数且唯一值少的列注入枚举
            if len(vals) <= ENUM_THRESHOLD and all(isinstance(v, (str, int, float)) for v in vals):
                sorted_vals = sorted(vals, key=str)
                cols[col_name] = {"enum": sorted_vals}


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


# ── 向量化表过滤 ──────────────────────────────────────────────────────────────

_embed_model = None
_embed_lock  = threading.Lock()


def _get_embed_model():
    global _embed_model
    if _embed_model is None:
        with _embed_lock:
            if _embed_model is None:
                from sentence_transformers import SentenceTransformer
                _embed_model = SentenceTransformer("all-MiniLM-L6-v2")
    return _embed_model


def filter_registry_by_question(registry: dict, question: str) -> dict:
    """
    当 schema token 数超过 SCHEMA_TOKEN_LIMIT 时，用向量相似度选取最相关的表。
    策略：按预算贪心选取，保证 context 不超限，同时尽量保留高相关表。

    相关度计算：embed(question) vs embed("{table}: {col1} {col2} ...")
    """
    tables = registry.get("tables", registry)
    context = registry_to_context(registry)
    if len(context) // 4 <= SCHEMA_TOKEN_LIMIT:
        return registry   # 不超限，直接返回

    model = _get_embed_model()

    # 构建每张表的文本表示：表名 + 所有列名
    table_texts = {}
    for tname, tinfo in tables.items():
        cols = tinfo.get("columns", tinfo) if isinstance(tinfo, dict) else tinfo
        col_names = list(cols.keys()) if isinstance(cols, dict) else list(cols)
        table_texts[tname] = f"{tname}: {' '.join(col_names)}"

    # 向量化
    tnames = list(table_texts.keys())
    corpus  = [table_texts[t] for t in tnames]
    t_vecs  = model.encode(corpus, convert_to_numpy=True, show_progress_bar=False)
    q_vec   = model.encode([question], convert_to_numpy=True, show_progress_bar=False)[0]

    # 余弦相似度
    from numpy.linalg import norm
    import numpy as np
    scores = [(tnames[i], float(np.dot(q_vec, t_vecs[i]) / (norm(q_vec) * norm(t_vecs[i]) + 1e-9)))
              for i in range(len(tnames))]
    scores.sort(key=lambda x: x[1], reverse=True)

    # 动态 TopN：按预算贪心选取
    selected: dict = {}
    token_budget = SCHEMA_TOKEN_LIMIT
    for tname, score in scores:
        tinfo = tables[tname]
        tctx  = registry_to_context({"tables": {tname: tinfo}})
        tokens = len(tctx) // 4
        if token_budget - tokens >= 0:
            selected[tname] = tinfo
            token_budget -= tokens
        if token_budget <= 0:
            break

    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"  {ts}  vec   {len(tables)}表 → {len(selected)}表 "
          f"(budget {SCHEMA_TOKEN_LIMIT}, top: {', '.join(list(selected)[:3])}...)", flush=True)

    return {"tables": selected}


# ── LLM 调用（tool_use 模式）────────────────────────────────────────────────

def _call_tool_use(client, system: str, question: str, tools: list[dict],
                   _retries: int = 5, _backoff: float = 10.0,
                   _messages_override: list | None = None) -> dict:
    """
    调用 LLM，要求使用 generate_forge_query 工具，返回工具调用的 input dict。
    支持 Anthropic SDK 格式和 OpenAI 兼容格式（由 LLM_FORMAT 控制）。
    _messages_override: 直接使用指定的 messages（用于编译错误回传重试）。
    """
    messages = _messages_override or [{"role": "user", "content": question}]

    for attempt in range(_retries):
        try:
            if LLM_FORMAT == "openai":
                tool_input, text = _call_openai(client, system, messages, tools)
            else:
                tool_input, text = _call_anthropic(client, system, messages, tools)

            if tool_input is not None:
                return {"ok": True, "input": tool_input, "tool": "generate_forge_query"}

            # 模型返回了文字而非工具调用 → 追加催促消息，下一轮重试
            if attempt < _retries - 1 and not _messages_override:
                messages = [
                    {"role": "user", "content": question},
                    {"role": "assistant", "content": text or "(no response)"},
                    {"role": "user",
                     "content": "Please call the generate_forge_query tool to answer the question."},
                ]
                continue
            return {"ok": False, "error": f"模型未调用工具，文字回复：{text[:200]}"}

        except Exception as e:
            err_str = str(e)
            if attempt < _retries - 1 and ("500" in err_str or "rate" in err_str.lower() or "timeout" in err_str.lower()):
                wait = _backoff * (2 ** attempt)
                tqdm.write(f"  ⚠ API 错误({err_str[:60]})，等待 {wait:.0f}s 后重试 (attempt {attempt+1}/{_retries})")
                time.sleep(wait)
            else:
                raise


def _call_anthropic(client, system: str, messages: list, tools: list) -> tuple[dict | None, str]:
    """Anthropic SDK 格式调用，返回 (tool_input_dict | None, text)。"""
    import anthropic
    msg = client.messages.create(
        model=LLM_MODEL,
        max_tokens=2048,
        system=system,
        tools=tools,
        tool_choice={"type": "tool", "name": "generate_forge_query"},
        messages=messages,
    )
    for block in msg.content:
        if getattr(block, "type", None) == "tool_use":
            return block.input, ""
    text = next((b.text for b in msg.content if hasattr(b, "text")), "")
    return None, text


def _call_openai(client, system: str, messages: list, tools: list) -> tuple[dict | None, str]:
    """OpenAI 兼容格式调用（硅基流动等），返回 (tool_input_dict | None, text)。"""
    # 将 Anthropic 格式 tool schema 转为 OpenAI function calling 格式
    oai_tools = [{
        "type": "function",
        "function": {
            "name": t["name"],
            "description": t.get("description", ""),
            "parameters": t.get("input_schema", t.get("parameters", {})),
        }
    } for t in tools]

    # 将 Anthropic 格式 messages 转为 OpenAI 格式
    # tool_result 消息需要转换
    oai_messages = [{"role": "system", "content": system}]
    for m in messages:
        role = m["role"]
        content = m["content"]
        if isinstance(content, list):
            # Anthropic tool_result 格式 → OpenAI tool 消息
            for block in content:
                if isinstance(block, dict) and block.get("type") == "tool_result":
                    oai_messages.append({
                        "role": "tool",
                        "tool_call_id": block["tool_use_id"],
                        "content": block["content"],
                    })
                elif isinstance(block, dict) and block.get("type") == "tool_use":
                    # assistant tool_use block → 已在下面处理
                    pass
            # assistant content with tool_use blocks
            tool_uses = [b for b in content if isinstance(b, dict) and b.get("type") == "tool_use"]
            if tool_uses and role == "assistant":
                oai_messages.append({
                    "role": "assistant",
                    "tool_calls": [{
                        "id": tu["id"],
                        "type": "function",
                        "function": {"name": tu["name"], "arguments": json.dumps(tu["input"])},
                    } for tu in tool_uses],
                })
        else:
            oai_messages.append({"role": role, "content": content})

    # 优先用 required（更广泛兼容），不指定具体函数名（Kimi 等不支持函数级强制）
    tool_choice = os.environ.get("LLM_TOOL_CHOICE", "required")
    if tool_choice == "named":
        tool_choice = {"type": "function", "function": {"name": "generate_forge_query"}}
    resp = client.chat.completions.create(
        model=LLM_MODEL,
        messages=oai_messages,
        tools=oai_tools,
        tool_choice=tool_choice,
        max_tokens=2048,
    )
    choice = resp.choices[0]
    if choice.message.tool_calls:
        tc = choice.message.tool_calls[0]
        try:
            return json.loads(tc.function.arguments), ""
        except json.JSONDecodeError as e:
            return None, f"JSON decode error: {e}"
    text = choice.message.content or ""
    return None, text


def _unwrap_forge_json(fj: dict) -> dict:
    """
    处理模型常见的包装错误：
    {"explain": "...", "query": "<JSON string>"}  → 递归解包 query 字段
    支持 query 字符串里含 CTE、多层嵌套等情况。
    """
    if not isinstance(fj, dict):
        return fj
    # 顶层有 "query" 且无 "scan" → 尝试解包
    if "query" in fj and "scan" not in fj:
        raw = fj["query"]
        if isinstance(raw, str):
            try:
                inner = json.loads(raw)
                if isinstance(inner, dict):
                    return _unwrap_forge_json(inner)   # 递归，防止多层包装
            except (json.JSONDecodeError, ValueError):
                pass
        elif isinstance(raw, dict):
            return _unwrap_forge_json(raw)
    return fj


# ── EA 执行评估辅助 ───────────────────────────────────────────────────────────

def _exec_sql(db_path: str, sql: str) -> tuple[list, str | None]:
    try:
        conn = sqlite3.connect(db_path)
        rows = [tuple(row) for row in conn.execute(sql).fetchall()]
        conn.close()
        return rows, None
    except Exception as e:
        return [], str(e)


def _norm_rows(rows: list) -> frozenset:
    return frozenset(
        tuple("NULL" if v is None else str(v).strip() for v in row)
        for row in rows
    )


_eval_meta_cache: dict[str, dict] = {}

def _get_eval_meta(instance_id: str) -> dict:
    """Load Spider2 eval metadata (condition_cols, ignore_order) for an instance."""
    global _eval_meta_cache
    if not _eval_meta_cache:
        eval_path = DATA_DIR / "evaluation_suite" / "gold" / "spider2lite_eval.jsonl"
        if eval_path.exists():
            for line in eval_path.read_text().splitlines():
                if line.strip():
                    try:
                        d = json.loads(line)
                        _eval_meta_cache[d["instance_id"]] = d
                    except Exception:
                        pass
    return _eval_meta_cache.get(instance_id, {})


def _norm_val(v: str) -> str:
    """Normalize a cell value: try rounding floats to 4 decimal places."""
    s = v.strip() if v is not None else "NULL"
    try:
        f = float(s)
        return f"{f:.4f}"
    except (ValueError, TypeError):
        return s


def _norm_rows_fuzzy(rows: list) -> frozenset:
    """Normalize rows with fuzzy float comparison."""
    return frozenset(
        tuple(_norm_val(str(v)) for v in row)
        for row in rows
    )


def _parse_condition_cols(condition_cols: list) -> tuple[str, list]:
    """
    Spider2 condition_cols 有两种格式：
    - per_subfile: [[1], [0], [0]]  → 每个子文件各自的列索引（local 用例）
    - flat:        [0] 或 [1, 2]   → 整数列表，过滤预测结果列（bq/sf 用例）
    返回 ("per_subfile", [[…],…]) 或 ("flat", [int,…]) 或 ("none", [])
    """
    if not condition_cols:
        return "none", []
    if all(isinstance(c, list) for c in condition_cols):
        return "per_subfile", condition_cols
    # flat list of ints
    return "flat", [int(c) for c in condition_cols if isinstance(c, int)]


def _load_gold_csv_sets(instance_id: str) -> tuple[list[list], str | None]:
    """
    Load all gold CSV sub-files for an instance_id.
    Returns list of raw data rows per sub-file (no column filtering here).
    Spider2 gold files are named {instance_id}_a.csv, _b.csv, etc.
    """
    # Find all sub-CSVs: local002_a.csv, local002_b.csv, ...
    sub_files = sorted(GOLD_CSV_DIR.glob(f"{instance_id}_*.csv"))
    if not sub_files:
        plain = GOLD_CSV_DIR / f"{instance_id}.csv"
        if plain.exists():
            sub_files = [plain]
    if not sub_files:
        return [], "no_gold"

    result = []
    for csv_path in sub_files:
        with csv_path.open(newline="", encoding="utf-8") as f:
            all_rows = list(_csv_mod.reader(f))
        data_rows = all_rows[1:] if len(all_rows) > 1 else []
        result.append(data_rows)
    return result, None


def _pred_matches_gold(pred_rows: list, gold_sub_rows: list[list],
                       condition_cols: list) -> bool:
    """
    Check if predicted rows match ANY gold sub-file.
    condition_cols 支持两种格式（由 _parse_condition_cols 解析）。
    逻辑：对每个子文件，用相同的列过滤同时作用于 pred 和 gold，再比较。
    """
    fmt, cc = _parse_condition_cols(condition_cols)

    for i, gold_rows in enumerate(gold_sub_rows):
        # 确定本子文件要过滤的列
        if fmt == "per_subfile" and i < len(cc):
            cols = cc[i]   # list[int]
        elif fmt == "flat":
            cols = cc      # list[int]
        else:
            cols = []

        if cols:
            g_filtered = [[row[c] for c in cols if c < len(row)] for row in gold_rows]
            p_filtered = [[row[c] for c in cols if c < len(row)] for row in pred_rows]
            if _norm_rows_fuzzy(p_filtered) == _norm_rows_fuzzy(g_filtered):
                return True
        else:
            if _norm_rows_fuzzy(pred_rows) == _norm_rows_fuzzy(gold_rows):
                return True
    return False


def _ea_evaluate(instance_id: str, db_path: str, sql: str) -> dict:
    """Execute sql on db_path and compare to gold. Returns ea_* fields."""
    if not db_path or not Path(db_path).exists():
        return {"ea_match": None, "ea_pred_rows": None, "ea_error": "no_db"}

    pred_rows, pred_err = _exec_sql(db_path, sql)
    if pred_err:
        return {"ea_match": False, "ea_pred_rows": 0, "ea_error": f"exec: {pred_err[:120]}"}

    # Try gold SQL first
    gold_sql_path = GOLD_SQL_DIR / f"{instance_id}.sql"
    if gold_sql_path.exists():
        gold_rows, gold_err = _exec_sql(db_path, gold_sql_path.read_text().strip())
        if gold_err:
            return {"ea_match": None, "ea_pred_rows": len(pred_rows), "ea_error": f"gold_sql_err: {gold_err[:80]}"}
        match = _norm_rows_fuzzy(pred_rows) == _norm_rows_fuzzy(gold_rows)
        return {"ea_match": match, "ea_pred_rows": len(pred_rows), "ea_error": None}

    # Use gold CSVs (Spider2 multi-sub-file format)
    gold_sets, err = _load_gold_csv_sets(instance_id)
    if err == "no_gold":
        return {"ea_match": None, "ea_pred_rows": len(pred_rows), "ea_error": "no_gold"}
    if err:
        return {"ea_match": None, "ea_pred_rows": len(pred_rows), "ea_error": err}

    meta = _get_eval_meta(instance_id)
    condition_cols = meta.get("condition_cols", [])
    match = _pred_matches_gold(pred_rows, gold_sets, condition_cols)
    return {"ea_match": match, "ea_pred_rows": len(pred_rows), "ea_error": None}


# ── 直接 SQL 兜底（超出 Forge DSL 能力时）────────────────────────────────────

_DIRECT_SQL_TOOL = {
    "name": "generate_sql_direct",
    "description": (
        "Generate a raw SQLite SQL query directly. "
        "Use this when Forge DSL cannot express the required logic."
    ),
    "input_schema": {
        "type": "object",
        "required": ["sql"],
        "additionalProperties": False,
        "properties": {
            "sql":     {"type": "string", "description": "A complete, valid SQLite SQL query."},
            "explain": {"type": "string", "description": "Brief explanation of query logic."},
        },
    },
}


def _fallback_to_raw_sql(client, system: str, question: str, compile_error: str) -> dict:
    """
    超出 Forge DSL 表达能力时，让 LLM 直接生成 SQL（SQLite 方言）。
    Returns {"ok": True, "sql": "..."} or {"ok": False, "error": "..."}.
    """
    fallback_msg = (
        f"Forge DSL compilation failed: {compile_error[:200]}\n"
        "This query may require SQL features beyond Forge DSL. "
        "Please use the generate_sql_direct tool to write the equivalent SQLite SQL directly."
    )
    messages = [
        {"role": "user", "content": question},
        {"role": "assistant", "content": [
            {"type": "tool_use", "id": "fb_0", "name": "generate_forge_query", "input": {}}
        ]},
        {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "fb_0", "content": fallback_msg}
        ]},
    ]
    tools = [_DIRECT_SQL_TOOL]
    try:
        if LLM_FORMAT == "openai":
            tool_input, _ = _call_openai(client, system, messages, tools)
        else:
            import anthropic as _anthro_mod
            msg = client.messages.create(
                model=LLM_MODEL,
                max_tokens=2048,
                system=system,
                tools=tools,
                tool_choice={"type": "tool", "name": "generate_sql_direct"},
                messages=messages,
            )
            tool_input = next(
                (b.input for b in msg.content if getattr(b, "type", None) == "tool_use"),
                None,
            )
    except Exception as e:
        return {"ok": False, "error": f"fallback LLM error: {e}"}
    if tool_input and tool_input.get("sql"):
        return {"ok": True, "sql": tool_input["sql"]}
    return {"ok": False, "error": "fallback LLM returned no SQL"}


def run_case(client, case: dict) -> dict:
    """对单个用例完整走 Forge 管道，返回结构化结果（含 EA 评估）。"""
    started_at = datetime.now(timezone.utc).isoformat()

    question    = case["question"]
    instance_id = case["instance_id"]
    db_path     = case.get("_db_path") or ""

    def _finish(**kw):
        return {"instance_id": instance_id,
                "started_at": started_at,
                "finished_at": datetime.now(timezone.utc).isoformat(),
                **kw}

    def _success(sql, forge_json, fallback=False):
        ea = _ea_evaluate(instance_id, db_path, sql)
        return _finish(sql=sql, forge_json=forge_json, error=None,
                       fallback=fallback, **ea)

    def _compile_failed(forge_json, compile_err):
        """尝试 raw SQL 兜底；失败则返回 compile error。"""
        fb = _fallback_to_raw_sql(client, system, question, str(compile_err))
        if fb["ok"]:
            ea = _ea_evaluate(instance_id, db_path, fb["sql"])
            return _finish(sql=fb["sql"], forge_json=forge_json, error=None,
                           fallback="raw_sql", **ea)
        return _finish(sql=None, forge_json=forge_json,
                       error=str(compile_err), fallback=False,
                       ea_match=None, ea_pred_rows=None, ea_error="compile_failed")

    try:
        registry = get_registry(case)
    except Exception as e:
        return _finish(sql=None, forge_json=None, error=f"registry 构建失败: {e}",
                       fallback=False, ea_match=None, ea_pred_rows=None, ea_error="registry_error")

    filtered = filter_registry_by_question(registry, question)
    tools  = [{"name": "generate_forge_query",
               "description": "Generate a Forge JSON query from natural language.",
               "input_schema": build_tool_schema(filtered)}]
    system = build_system(registry_to_context(filtered))

    try:
        resp = _call_tool_use(client, system, question, tools)
    except Exception as e:
        return _finish(sql=None, forge_json=None, error=f"LLM 调用失败: {e}",
                       fallback=False, ea_match=None, ea_pred_rows=None, ea_error="llm_error")

    if not resp["ok"]:
        return _finish(sql=None, forge_json=None, error=resp["error"],
                       fallback=False, ea_match=None, ea_pred_rows=None, ea_error="no_tool_call")

    forge_json = _unwrap_forge_json(resp["input"])
    try:
        sql = compile_query(forge_json)
        return _success(sql, forge_json)
    except Exception as compile_err:
        # 编译失败 → 把错误回传 LLM，让其修正（最多重试 1 次）
        retry_messages = [
            {"role": "user", "content": question},
            {"role": "assistant", "content": [
                {"type": "tool_use", "id": "retry_0",
                 "name": "generate_forge_query", "input": forge_json}
            ]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "retry_0",
                 "content": f"Compilation error: {compile_err}\nPlease fix the Forge JSON and call generate_forge_query again."}
            ]},
        ]
        try:
            retry_resp = _call_tool_use(client, system, question, tools,
                                        _messages_override=retry_messages)
        except Exception:
            return _compile_failed(forge_json, compile_err)

        if not retry_resp["ok"]:
            return _compile_failed(forge_json, compile_err)

        forge_json2 = _unwrap_forge_json(retry_resp["input"])
        try:
            sql = compile_query(forge_json2)
            return _success(sql, forge_json2)
        except Exception as e2:
            return _compile_failed(forge_json2, e2)


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
    print(f"   模型：{LLM_MODEL}  |  格式：{LLM_FORMAT}  |  并发：{args.workers}")

    cases = load_sqlite_cases()
    if args.limit:
        cases = cases[:args.limit]
        print(f"   限制：只跑前 {args.limit} 个用例")

    if args.estimate:
        estimate_tokens(cases)
        return

    if not LLM_API_KEY:
        print("❌ 未设置 LLM_API_KEY（或 MINIMAX_API_KEY）环境变量", file=sys.stderr)
        sys.exit(1)

    run_ts   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # 输出目录
    out_dir = RESULTS_DIR / METHOD
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path     = out_dir / "run_log.jsonl"                   # 追加式主日志（断点续跑）
    session_path = out_dir / f"session_{run_ts}.jsonl"         # 本次运行独立日志（带时间戳）

    print(f"   日志：{session_path.name}")

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

    if LLM_FORMAT == "openai":
        from openai import OpenAI
        client = OpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)
    else:
        import anthropic
        client = anthropic.Anthropic(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)

    log_lock   = threading.Lock()
    ok_count   = 0
    err_count  = 0
    start_time = time.monotonic()

    # ── 表头 ─────────────────────────────────────────────────────────────────
    total = len(pending)
    print(f"\n  {'TIME':>8}  {'#':>4}/{total:<4}  {'OK':>4}  {'ERR':>4}  {'ETA':>7}  INSTANCE")
    print(f"  {'─'*8}  {'─'*9}  {'─'*4}  {'─'*4}  {'─'*7}  {'─'*24}")

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        future_map = {pool.submit(run_case, client, c): c for c in pending}

        done_count = 0
        for future in as_completed(future_map):
            result    = future.result()
            iid       = result["instance_id"]
            done_count += 1

            # 保存 SQL 文件
            if result["sql"]:
                (out_dir / f"{iid}.sql").write_text(result["sql"])
                ok_count  += 1
                status_sym = "✓"
            else:
                err_count  += 1
                status_sym = "✗"

            # EA 标记
            ea_match = result.get("ea_match")
            if ea_match is True:
                ea_tag = " ✅EA"
            elif ea_match is False:
                ea_tag = " ✗EA"
            elif result.get("fallback") == "raw_sql":
                ea_tag = " ~FB"
            else:
                ea_tag = ""

            # 追加日志（主日志 + 本次 session 日志）
            with log_lock:
                line = json.dumps(result, ensure_ascii=False) + "\n"
                with log_path.open("a") as f:
                    f.write(line)
                with session_path.open("a") as f:
                    f.write(line)

            # ETA
            elapsed  = time.monotonic() - start_time
            avg_secs = elapsed / done_count
            remaining = avg_secs * (total - done_count)
            if remaining >= 3600:
                eta_str = f"{remaining/3600:.1f}h"
            elif remaining >= 60:
                eta_str = f"{remaining/60:.0f}m"
            else:
                eta_str = f"{remaining:.0f}s"

            ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
            print(f"  {ts}  {done_count:>4}/{total:<4}  {ok_count:>4}  {err_count:>4}  {eta_str:>7}  {status_sym}{ea_tag} {iid}", flush=True)

    _print_stats(out_dir)


def _print_stats(out_dir: Path) -> None:
    log_path = out_dir / "run_log.jsonl"
    if not log_path.exists():
        return

    records = [json.loads(l) for l in log_path.read_text().splitlines() if l.strip()]
    total   = len(records)
    ok      = sum(1 for r in records if not r.get("error"))
    err     = total - ok
    fallback_cnt = sum(1 for r in records if r.get("fallback") == "raw_sql")

    # EA 统计
    ea_pass    = sum(1 for r in records if r.get("ea_match") is True)
    ea_fail    = sum(1 for r in records if r.get("ea_match") is False)
    ea_no_gold = sum(1 for r in records if r.get("ea_error") == "no_gold")
    ea_eval    = ea_pass + ea_fail   # 有 gold 且有 SQL 的用例

    print(f"\n{'='*55}")
    print(f"  总用例：{total}  |  编译成功：{ok}  |  失败：{err}"
          + (f"  |  raw_sql兜底：{fallback_cnt}" if fallback_cnt else ""))
    if ea_eval:
        print(f"  EA (Execution Accuracy)：{ea_pass}/{ea_eval} = {ea_pass/ea_eval*100:.1f}%")
    print(f"  无 gold 参考答案：{ea_no_gold}  |  编译/执行错误：{err + ea_fail - ea_fail}")
    print(f"  SQL 文件 → {out_dir}/")


if __name__ == "__main__":
    main()
