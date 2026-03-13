#!/usr/bin/env python3
"""
Forge Benchmark Runner — 对比 Forge vs 直接 SQL 在自有 40 题上的 Execution Accuracy。

用法：
    python tests/benchmark/runner.py                   # 跑两种 method
    python tests/benchmark/runner.py --method forge    # 只跑 Forge
    python tests/benchmark/runner.py --method direct   # 只跑直接 SQL
    python tests/benchmark/runner.py --fresh           # 清除旧结果重跑
    python tests/benchmark/runner.py --workers 4       # 并发数（默认 4）

输出：
    tests/benchmark/results/forge/run_log.jsonl
    tests/benchmark/results/direct/run_log.jsonl
"""
from __future__ import annotations

import argparse
import csv as _csv_mod
import json
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ROOT      = Path(__file__).parent.parent.parent
BENCH_DIR = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from forge.compiler import compile_query
from forge.retriever import SchemaRetriever, make_embed_fn, make_query_embed_fn


# ── 配置 ──────────────────────────────────────────────────────────────────────

DB_PATH    = BENCH_DIR / "benchmark.db"
GOLD_DIR   = BENCH_DIR / "gold"
CASES_PATH = BENCH_DIR / "cases.json"
RESULTS_DIR = BENCH_DIR / "results"
REGISTRY_PATH = ROOT / "schema.registry.json"
EMBED_CACHE_PATH = ROOT / ".forge" / "schema_embeddings.pkl"

MINIMAX_API_KEY  = os.environ.get("MINIMAX_API_KEY", "")
MINIMAX_BASE_URL = os.environ.get("MINIMAX_BASE_URL", "https://api.minimaxi.com/anthropic")
MINIMAX_MODEL    = os.environ.get("MINIMAX_MODEL", "MiniMax-M2.5-highspeed")

LLM_API_KEY  = os.environ.get("LLM_API_KEY",  MINIMAX_API_KEY)
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", MINIMAX_BASE_URL)
LLM_MODEL    = os.environ.get("LLM_MODEL",    MINIMAX_MODEL)
LLM_FORMAT   = os.environ.get("LLM_FORMAT",   "anthropic")  # "anthropic" | "openai"

# Embedding 配置（用于 schema 检索；默认复用 MiniMax key，切换到 OpenAI-compatible /v1 端点）
EMBED_API_KEY  = os.environ.get("EMBED_API_KEY",  MINIMAX_API_KEY)
EMBED_BASE_URL = os.environ.get("EMBED_BASE_URL", "https://api.minimaxi.com/v1")
EMBED_MODEL    = os.environ.get("EMBED_MODEL",    "embo-01")
RETRIEVAL_TOP_K = int(os.environ.get("RETRIEVAL_TOP_K", "5"))


# ── Schema 检索器初始化 ────────────────────────────────────────────────────────

def _init_retriever() -> tuple[SchemaRetriever | None, object | None]:
    """
    初始化 SchemaRetriever 和查询嵌入函数。

    返回：
        (retriever, query_embed_fn)
        若 registry 文件不存在或 embedding API 不可用，query_embed_fn 为 None
        （retriever 仍可使用 BM25-lite 降级模式）
    """
    if not REGISTRY_PATH.exists():
        return None, None

    registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    retriever = SchemaRetriever(registry, cache_path=EMBED_CACHE_PATH)

    query_embed_fn = None
    if EMBED_API_KEY:
        try:
            # 尝试加载已有索引，避免每次启动都重新调用 embedding API
            if not retriever.load_index():
                db_embed_fn = make_embed_fn(EMBED_API_KEY, EMBED_BASE_URL, EMBED_MODEL, "db")
                retriever.build_index(db_embed_fn)
                print("  [retriever] 向量索引已构建", flush=True)
            else:
                print("  [retriever] 向量索引从缓存加载", flush=True)
            query_embed_fn = make_query_embed_fn(EMBED_API_KEY, EMBED_BASE_URL, EMBED_MODEL)
        except Exception as e:
            print(f"  [retriever] embedding API 不可用（{e}），使用 BM25-lite", flush=True)

    return retriever, query_embed_fn


# ── System Prompt 构建函数（替换硬编码的字符串常量）────────────────────────────

# 全量 schema（fallback：检索器不可用时使用）
_SCHEMA_FULL = """
你可以查询以下数据库表（SQLite）：

users       (id, name, city, created_at, is_vip)
orders      (id, user_id, status, total_amount, created_at)
order_items (id, order_id, product_id, quantity, unit_price)
products    (id, name, category, cost_price)

字段枚举值：
- orders.status:      'completed' | 'pending' | 'cancelled'
- users.is_vip:       0 | 1
- users.city:         '北京' | '上海' | '广州' | '成都' | '杭州'
- products.category:  '电子产品' | '服装' | '家居' | '食品'
"""

# Forge method — 完整 DSL 规范（来自 method_j.py）
_FORGE_SPEC = """
## Forge 查询格式

用以下 JSON 描述"你要什么数据"：

```json
{
  "cte":    [{"name":"中间表名","query":{嵌套Forge查询}}],
  "scan":    "主数据集（表名或 CTE 名）",
  "joins":   [{"type":"inner|left|right|full|anti|semi","table":"关联表","on":{"left":"主表.字段","right":"关联表.字段"}}],
  "filter":  [筛选条件数组],
  "group":   ["分组维度"],
  "agg":     [{"fn":"统计函数","col":"统计字段或表达式","as":"结果名"}],
  "having":  [分组后的二次筛选],
  "select":  ["输出字段列表或 expr 对象"],
  "window":  [窗口计算表达式],
  "qualify": [窗口结果筛选],
  "sort":    [{"col":"排序字段","dir":"asc|desc"}],
  "limit":   最多返回行数
}
```

## 各字段含义

| 字段 | 作用 |
|------|------|
| cte | 公共表表达式（WITH 子句），仅用于两步聚合（见 CTE 章节）|
| scan | 主数据集，可以是表名或 CTE 名 |
| joins | 引入其他数据集。inner=两侧都有记录才保留；left=主表记录全保留，关联表无匹配则为空；anti=只保留在关联表中**找不到**的记录（编译为 LEFT JOIN + IS NULL）；semi=只保留在关联表中**能找到**的记录（编译为 EXISTS，**天然去重**，不要用 inner join 代替）|
| filter | **数组**，筛选哪些行参与后续计算，多个条件之间是 AND |
| group | 按哪些维度分组统计 |
| agg | 每组的统计指标。fn：count_all（行数，**无 col 字段**）、count（非空数，需 col）、count_distinct（去重数，需 col）、sum、avg、min、max |
| having | 对分组统计结果的进一步筛选（见 HAVING 规则）|
| select | 最终输出哪些字段。可以是列名字符串，也可以是 expr 对象（见 CASE WHEN 章节）|
| window | 保留所有行的同时，计算排名或滑动统计 |
| qualify | 对窗口结果筛选（如"只保留每组排名前1"）|
| sort | 结果排序，dir 必填（asc/desc）|
| limit | 最多返回多少行。值必须来自问题中**明确的数量**（"前10名"→10，"前5"→5，绝不默认填1）|

## 筛选条件格式

简单条件：`{"col": "表.字段", "op": "操作符", "val": 值}`

操作符：eq、neq、gt/gte/lt/lte、in、like、is_null、is_not_null、between

between 必须用 lo/hi：
```json
{"col": "orders.total_amount", "op": "between", "lo": 500, "hi": 2000}
```

OR 条件（filter 是数组，OR 条件是数组里的一个元素）：
```json
"filter": [
  {"or": [
    {"col": "users.name", "op": "like", "val": "%明%"},
    {"and": [
      {"col": "users.created_at", "op": "gte", "val": "2024-01-01"},
      {"col": "users.is_vip", "op": "eq", "val": 1}
    ]}
  ]}
]
```
❌ 错误：`"filter": {"or": [...]}` — filter 必须是数组，不能是对象

## 中文数量词语义（必须严格区分）

| 中文表述 | 操作符 | 说明 |
|---------|--------|------|
| 超过N / 多于N / 大于N | **gt（严格大于 >N，不含N本身）** | "超过5次" → op: "gt", val: 5，即 6次及以上 |
| 至少N / 不少于N / ≥N | gte（大于等于 >=N，含N本身）| "至少3次" → op: "gte", val: 3 |
| 不超过N / 最多N | lte | "最多10条" → op: "lte", val: 10 |
| 不足N / 少于N | lt | "少于2次" → op: "lt", val: 2 |

## HAVING 规则

HAVING 用于对聚合结果的二次筛选。**只有当问题中包含明确的数值阈值时才添加 HAVING**。

having 中的 col 必须是 agg 定义的别名（as 字段），不能是原始列名。

## 平均值/人均的正确模式

「人均消费」「每用户平均」「客单价」等指标 = **直接 AVG() GROUP BY 维度**，不需要 CTE。

## CASE WHEN 表达式（select 中的 expr 对象）

```json
{"expr": "CASE WHEN orders.total_amount > 1000 THEN '高价值' ELSE '低价值' END", "as": "order_tier"}
```

## 函数表达式作为列引用

```json
"group": ["STRFTIME('%Y-%m', orders.created_at)"]
```

## CTE（公共表表达式）：两步聚合专用

✅ 用 CTE 的场景：「先按某个维度统计中间结果 → 再基于中间结果进行二次过滤或聚合」

❌ 不用 CTE 的场景：简单 AVG/SUM GROUP BY（直接 group + agg）、有 HAVING 的分组筛选（group + agg + having）、每组 TopN（window + qualify）

## 窗口计算（window）

| 需求场景 | 写法 |
|----------|------|
| 全局排名 | `{"fn":"row_number|rank|dense_rank","order":[...],"as":"别名"}` |
| 分组内排名 | 加 `"partition":["分组字段"]` |
| 分组内滑动统计 | `{"fn":"sum|avg|count|min|max","col":"字段","partition":[...],"order":[...],"as":"别名"}` |
| 相邻行对比 | `{"fn":"lag|lead","col":"字段","offset":1,"partition":["分组字段"],"order":[...],"as":"别名"}` |

排名函数（row_number/rank/dense_rank）**没有 col 字段**。

## 示例：每组 TopN（品类内销量最高的商品，不用 CTE）

```json
{
  "scan": "order_items",
  "joins": [{"type": "inner", "table": "products", "on": {"left": "order_items.product_id", "right": "products.id"}}],
  "group": ["products.id", "products.name", "products.category"],
  "agg": [{"fn": "sum", "col": "order_items.quantity", "as": "total_qty"}],
  "window": [{"fn": "row_number", "partition": ["products.category"], "order": [{"col": "total_qty", "dir": "desc"}], "as": "rn"}],
  "qualify": [{"col": "rn", "op": "eq", "val": 1}],
  "select": ["products.name", "products.category", "total_qty", "rn"]
}
```

## 示例：从未下单的用户（anti join）

```json
{
  "scan": "users",
  "joins": [{"type": "anti", "table": "orders", "on": {"left": "users.id", "right": "orders.user_id"}}],
  "select": ["users.name", "users.city"]
}
```

## 数据关联规则

- 有关联表时，所有字段引用必须加表名：`orders.total_amount`
- **JOIN 完整性**：select 中每个字段所属的表必须出现在 scan 或 joins 中
- anti join 用于"不存在于右表"（编译为 LEFT JOIN + IS NULL），不要用 NOT IN
- semi join 用于"存在于右表"（编译为 EXISTS，天然去重），不要用 inner join 代替

## 输出约束

- **select 必填**，至少一个字段
- "前N名"要设 limit，N 来自问题原文
- "每组前N名"用 window + qualify，window 别名**必须加到 select 中**
- 输出合法 JSON：无注释，无尾逗号，所有字符串用双引号

只输出 JSON 对象，不要任何解释，不要 markdown 代码块。
"""

def _build_forge_system(schema_ctx: str) -> str:
    return f"""你是一个专业的数据查询助手，帮助用户用 Forge 格式描述数据查询需求。

{schema_ctx}

{_FORGE_SPEC}

用户会描述一个数据查询需求，你需要输出符合 Forge 格式的 JSON。
只输出 JSON 对象，不要任何其他内容。"""


def _build_direct_system(schema_ctx: str) -> str:
    return f"""你是一个专业的 SQL 数据分析师。

{schema_ctx}

根据用户的问题，生成正确的 SQLite SQL 查询。
- 使用 SQLite 语法（日期函数用 STRFTIME，随机用 RANDOM()）
- 只输出 SQL，不要解释，不要 markdown 代码块
"""

# ── Tool Schema（Forge 模式用）────────────────────────────────────────────────

FORGE_TOOL = {
    "name": "generate_forge_query",
    "description": "Generate a Forge JSON query to answer the user's data question.",
    "input_schema": {
        "type": "object",
        "required": ["scan", "select"],
        "additionalProperties": True,
        "properties": {
            "scan":    {"type": "string"},
            "select":  {"type": "array"},
            "cte":     {"type": "array"},
            "joins":   {"type": "array"},
            "filter":  {"type": "array"},
            "group":   {"type": "array"},
            "agg":     {"type": "array"},
            "having":  {"type": "array"},
            "window":  {"type": "array"},
            "qualify": {"type": "array"},
            "sort":    {"type": "array"},
            "limit":   {"type": "integer"},
        },
    },
}

DIRECT_TOOL = {
    "name": "generate_sql_direct",
    "description": "Generate a raw SQLite SQL query to answer the user's data question.",
    "input_schema": {
        "type": "object",
        "required": ["sql"],
        "additionalProperties": False,
        "properties": {
            "sql": {"type": "string", "description": "A complete, valid SQLite SQL query."},
        },
    },
}

def _build_direct_system_tool(schema_ctx: str) -> str:
    return f"""你是一个专业的 SQL 数据分析师。

{schema_ctx}

根据用户的问题，调用 generate_sql_direct 工具，传入完整的 SQLite SQL 查询。
- 使用 SQLite 语法
- SQL 必须能直接执行，不要任何注释或 markdown
"""


# ── LLM 调用 ──────────────────────────────────────────────────────────────────

def _make_client():
    if LLM_FORMAT == "openai":
        from openai import OpenAI
        return OpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)
    else:
        import anthropic
        return anthropic.Anthropic(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)


def _call_anthropic(client, system: str, messages: list, tools: list,
                    tool_name: str) -> tuple[dict | None, str]:
    import anthropic
    msg = client.messages.create(
        model=LLM_MODEL,
        max_tokens=2048,
        system=system,
        tools=tools,
        tool_choice={"type": "tool", "name": tool_name},
        messages=messages,
    )
    for block in msg.content:
        if getattr(block, "type", None) == "tool_use":
            return block.input, ""
    text = next((b.text for b in msg.content if hasattr(b, "text")), "")
    return None, text


def _call_openai(client, system: str, messages: list, tools: list) -> tuple[dict | None, str]:
    oai_tools = [{
        "type": "function",
        "function": {
            "name": t["name"],
            "description": t.get("description", ""),
            "parameters": t.get("input_schema", {}),
        }
    } for t in tools]
    oai_messages = [{"role": "system", "content": system}] + messages
    resp = client.chat.completions.create(
        model=LLM_MODEL,
        messages=oai_messages,
        tools=oai_tools,
        tool_choice="required",
        max_tokens=2048,
        temperature=0,
    )
    choice = resp.choices[0]
    if choice.message.tool_calls:
        tc = choice.message.tool_calls[0]
        try:
            return json.loads(tc.function.arguments), ""
        except json.JSONDecodeError as e:
            return None, f"JSON decode error: {e}"
    return None, choice.message.content or ""


def _call_tool_use(client, system: str, question: str, tools: list, tool_name: str,
                   _retries: int = 4, _backoff: float = 8.0) -> dict:
    messages = [{"role": "user", "content": question}]
    for attempt in range(_retries):
        try:
            if LLM_FORMAT == "openai":
                tool_input, text = _call_openai(client, system, messages, tools)
            else:
                tool_input, text = _call_anthropic(client, system, messages, tools, tool_name)

            if tool_input is not None:
                return {"ok": True, "input": tool_input}

            if attempt < _retries - 1:
                messages = [
                    {"role": "user", "content": question},
                    {"role": "assistant", "content": text or "(no response)"},
                    {"role": "user", "content": f"Please call the {tool_name} tool."},
                ]
                continue
            return {"ok": False, "error": f"模型未调用工具：{text[:200]}"}

        except Exception as e:
            err_str = str(e)
            if attempt < _retries - 1 and any(k in err_str.lower() for k in ("500", "rate", "timeout")):
                wait = _backoff * (2 ** attempt)
                print(f"  ⚠ API 错误({err_str[:60]})，{wait:.0f}s 后重试", flush=True)
                time.sleep(wait)
            else:
                return {"ok": False, "error": err_str[:300]}
    return {"ok": False, "error": "超过最大重试次数"}


def _unwrap_forge_json(fj: dict) -> dict:
    if not isinstance(fj, dict):
        return fj
    if "query" in fj and "scan" not in fj:
        raw = fj["query"]
        if isinstance(raw, str):
            try:
                inner = json.loads(raw)
                if isinstance(inner, dict):
                    return _unwrap_forge_json(inner)
            except (json.JSONDecodeError, ValueError):
                pass
        elif isinstance(raw, dict):
            return _unwrap_forge_json(raw)
    return fj


# ── EA 评估 ───────────────────────────────────────────────────────────────────

def _exec_sql(db_path: str, sql: str) -> tuple[list, str | None]:
    try:
        conn = sqlite3.connect(db_path)
        rows = [tuple(row) for row in conn.execute(sql).fetchall()]
        conn.close()
        return rows, None
    except Exception as e:
        return [], str(e)


def _norm_val(v) -> str:
    s = str(v).strip() if v is not None else "NULL"
    try:
        return f"{float(s):.4f}"
    except (ValueError, TypeError):
        return s


def _norm_rows(rows: list) -> frozenset:
    return frozenset(tuple(_norm_val(v) for v in row) for row in rows)


def _load_gold(case_id: str) -> tuple[list | None, str | None]:
    path = GOLD_DIR / f"{case_id}.csv"
    if not path.exists():
        return None, "gold_not_found"
    with path.open(newline="", encoding="utf-8") as f:
        rows = list(_csv_mod.reader(f))
    data = rows[1:] if len(rows) > 1 else []
    return data, None


def _ea_match(pred_rows: list, gold_rows: list) -> bool:
    return _norm_rows(pred_rows) == _norm_rows(gold_rows)


# ── Case 运行器 ───────────────────────────────────────────────────────────────

def _get_schema_ctx(
    question: str,
    retriever: SchemaRetriever | None,
    query_embed_fn,
    top_k: int,
) -> str:
    """检索相关表并生成 schema 上下文文本；检索器不可用时返回全量 schema。"""
    if retriever is None:
        return _SCHEMA_FULL
    tables = retriever.retrieve(question, query_embed_fn, top_k=top_k)
    return retriever.get_schema_ddl(tables)


def run_forge_case(
    client,
    case: dict,
    retriever: SchemaRetriever | None = None,
    query_embed_fn=None,
    top_k: int = RETRIEVAL_TOP_K,
) -> dict:
    """Forge 模式：tool_use → compile_query → exec → EA"""
    cid      = case["id"]
    question = case["question"]
    t0       = time.monotonic()

    # 动态构建 system prompt（只含检索到的相关表）
    schema_ctx   = _get_schema_ctx(question, retriever, query_embed_fn, top_k)
    forge_system = _build_forge_system(schema_ctx)

    # Step 1: LLM → Forge JSON
    result = _call_tool_use(
        client, forge_system, question, [FORGE_TOOL], "generate_forge_query")
    if not result["ok"]:
        return _make_record(case, "llm_error", None, None, result["error"], t0)

    forge_json = _unwrap_forge_json(result["input"])

    # Step 2: compile → SQL（最多 2 次，第二次追加错误回传）
    sql, compile_err = None, None
    for attempt in range(2):
        try:
            sql = compile_query(forge_json, dialect="sqlite")
            compile_err = None
            break
        except Exception as e:
            compile_err = str(e)
            if attempt == 0:
                # 回传编译错误，让模型修正
                retry_q = (
                    f"{question}\n\n"
                    f"上次生成的 Forge JSON 编译失败：{compile_err[:200]}\n"
                    "请修正并重新生成正确的 Forge JSON。"
                )
                r2 = _call_tool_use(
                    client, forge_system, retry_q, [FORGE_TOOL], "generate_forge_query")
                if r2["ok"]:
                    forge_json = _unwrap_forge_json(r2["input"])
                else:
                    break

    if compile_err:
        return _make_record(case, "compile_error", forge_json, None, compile_err, t0)

    # Step 3: exec → EA
    pred_rows, exec_err = _exec_sql(str(DB_PATH), sql)
    if exec_err:
        return _make_record(case, "exec_error", forge_json, sql, exec_err, t0)

    gold_rows, gold_err = _load_gold(cid)
    if gold_err:
        return _make_record(case, "no_gold", forge_json, sql, gold_err, t0)

    match = _ea_match(pred_rows, gold_rows)
    return _make_record(case, "ok", forge_json, sql, None, t0,
                        ea_match=match, pred_rows=len(pred_rows), gold_rows=len(gold_rows))


def run_direct_case(
    client,
    case: dict,
    retriever: SchemaRetriever | None = None,
    query_embed_fn=None,
    top_k: int = RETRIEVAL_TOP_K,
) -> dict:
    """直接 SQL 模式：tool_use → exec → EA"""
    cid      = case["id"]
    question = case["question"]
    t0       = time.monotonic()

    schema_ctx    = _get_schema_ctx(question, retriever, query_embed_fn, top_k)
    direct_system = _build_direct_system_tool(schema_ctx)

    result = _call_tool_use(
        client, direct_system, question, [DIRECT_TOOL], "generate_sql_direct")
    if not result["ok"]:
        return _make_record(case, "llm_error", None, None, result["error"], t0)

    sql = result["input"].get("sql", "")
    if not sql:
        return _make_record(case, "llm_error", None, None, "空 SQL", t0)

    pred_rows, exec_err = _exec_sql(str(DB_PATH), sql)
    if exec_err:
        return _make_record(case, "exec_error", None, sql, exec_err, t0)

    gold_rows, gold_err = _load_gold(cid)
    if gold_err:
        return _make_record(case, "no_gold", None, sql, gold_err, t0)

    match = _ea_match(pred_rows, gold_rows)
    return _make_record(case, "ok", None, sql, None, t0,
                        ea_match=match, pred_rows=len(pred_rows), gold_rows=len(gold_rows))


def _make_record(case: dict, status: str, forge_json, sql, error, t0,
                 ea_match=None, pred_rows=None, gold_rows=None) -> dict:
    return {
        "id":        case["id"],
        "category":  case["category"],
        "difficulty": case["difficulty"],
        "question":  case["question"],
        "status":    status,
        "forge_json": forge_json,
        "sql":       sql,
        "error":     error,
        "ea_match":  ea_match,
        "pred_rows": pred_rows,
        "gold_rows": gold_rows,
        "elapsed_s": round(time.monotonic() - t0, 2),
        "ts":        datetime.now(timezone.utc).isoformat(),
    }


# ── 主流程 ────────────────────────────────────────────────────────────────────

def run_method(
    method: str,
    cases: list[dict],
    workers: int,
    fresh: bool,
    use_retrieval: bool = True,
    top_k: int = RETRIEVAL_TOP_K,
) -> None:
    if not DB_PATH.exists():
        print(f"❌ 找不到 {DB_PATH}，请先运行 create_db.py", file=sys.stderr)
        sys.exit(1)

    out_dir = RESULTS_DIR / method
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "run_log.jsonl"

    # 断点续做：读取已完成的 case id
    done: set[str] = set()
    if not fresh and log_path.exists():
        for line in log_path.read_text(encoding="utf-8").splitlines():
            try:
                d = json.loads(line)
                if d.get("status") == "ok" and d.get("ea_match") is not None:
                    done.add(d["id"])
            except Exception:
                pass
        if done:
            print(f"  [续跑] 已完成 {len(done)} 个，跳过", flush=True)

    todo = [c for c in cases if c["id"] not in done]
    if not todo:
        print(f"  [{method}] 全部已完成，跳过", flush=True)
        return

    # 初始化 schema 检索器（一次性，所有 worker 线程共享只读访问）
    retriever, query_embed_fn = (None, None)
    if use_retrieval:
        retriever, query_embed_fn = _init_retriever()
        mode_label = "向量" if query_embed_fn else "BM25-lite"
        print(f"  [retriever] 检索模式={mode_label}  top_k={top_k}", flush=True)
    else:
        print(f"  [retriever] 已禁用（全量 schema）", flush=True)

    client = _make_client()
    run_fn = run_forge_case if method == "forge" else run_direct_case

    print(f"\n🚀 [{method}] 运行 {len(todo)} 个 case  (workers={workers})", flush=True)
    ts_start = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"   开始时间：{ts_start}", flush=True)

    results: list[dict] = []
    with log_path.open("a", encoding="utf-8") as f_log:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(run_fn, client, c, retriever, query_embed_fn, top_k): c
                for c in todo
            }
            for i, fut in enumerate(as_completed(futures), 1):
                rec = fut.result()
                results.append(rec)
                f_log.write(json.dumps(rec, ensure_ascii=False) + "\n")
                f_log.flush()
                status_icon = "✅" if rec["ea_match"] else ("❌" if rec["ea_match"] is False else "⚠")
                print(f"  {i:3d}/{len(todo)} {status_icon} {rec['id']}  "
                      f"[{rec['category']}]  {rec['status']}  "
                      f"{'EA:OK' if rec['ea_match'] else rec['error'][:50] if rec['error'] else 'EA:FAIL'}"
                      f"  {rec['elapsed_s']:.1f}s", flush=True)

    ok   = sum(1 for r in results if r["ea_match"] is True)
    fail = sum(1 for r in results if r["ea_match"] is False)
    err  = sum(1 for r in results if r["ea_match"] is None)
    total = len(results)
    print(f"\n📊 [{method}] EA = {ok}/{total} = {ok/total*100:.1f}%  "
          f"(失败:{fail} 错误:{err})", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--method",  choices=["forge", "direct", "both"], default="both")
    parser.add_argument("--fresh",   action="store_true", help="清除旧结果重跑")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--no-retrieval", action="store_true",
                        help="禁用 schema 检索，使用全量 schema（对照组）")
    parser.add_argument("--top-k",   type=int, default=RETRIEVAL_TOP_K,
                        help=f"每次检索返回的表数量（默认 {RETRIEVAL_TOP_K}）")
    args = parser.parse_args()

    if not CASES_PATH.exists():
        print(f"❌ 找不到 {CASES_PATH}", file=sys.stderr)
        sys.exit(1)

    cases = json.loads(CASES_PATH.read_text(encoding="utf-8"))

    methods = ["forge", "direct"] if args.method == "both" else [args.method]
    for m in methods:
        run_method(m, cases, args.workers, args.fresh,
                   use_retrieval=not args.no_retrieval,
                   top_k=args.top_k)

    print("\n✅ 完成，运行 report.py 生成报告", flush=True)


if __name__ == "__main__":
    main()
