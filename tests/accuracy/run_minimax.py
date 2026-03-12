#!/usr/bin/env python3
"""
Step 2 of 3 — 用 MiniMax 跑所有测试用例

每个用例独立运行 15 次：
  - 5 次 Method A：旧 Forge DSL 提示词（SQL 术语风格），生成 Forge JSON → 本地编译
  - 5 次 Method B：直接生成 SQL（对照组）
  - 5 次 Method D：新 Forge DSL 提示词（声明式风格 + 枚举 schema）

Method C（新 DSL 无枚举）已因评分垫底（6.90）被淘汰，不再运行。

支持断点续跑：已完成的 method 不重跑，只补缺失的 method。
并行：MAX_WORKERS 控制最大并发 API 调用数。

运行：
    python tests/accuracy/run_minimax.py
"""
from __future__ import annotations

import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import anthropic
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))

from forge.compiler import compile_query

RESULTS_DIR  = Path(__file__).parent / "results"
CASES_FILE   = RESULTS_DIR / "cases.json"
RESULTS_FILE = RESULTS_DIR / "results.json"

MINIMAX_API_KEY  = os.environ["MINIMAX_API_KEY"]
MINIMAX_BASE_URL = os.environ.get("MINIMAX_BASE_URL", "https://api.minimaxi.com/anthropic")
MINIMAX_MODEL    = os.environ.get("MINIMAX_MODEL", "MiniMax-M2.5-highspeed")

MAX_WORKERS     = 10
RUNS_PER_METHOD = 5

SCHEMA_TEXT = """
你可以查询以下数据库表（SQLite）：

users       (id, name, city, created_at, is_vip)
orders      (id, user_id, status, total_amount, created_at)
order_items (id, order_id, product_id, quantity, unit_price)
products    (id, name, category, cost_price)

典型值：
- orders.status: 'completed' | 'cancelled' | 'pending'
- users.is_vip: 1 / 0
- products.category: 'electronics' | 'clothing' | 'food' | 'books'
"""

# Method D 使用更精准的 schema，包含实际枚举值
SCHEMA_TEXT_V2 = """
你可以查询以下数据库表（SQLite）：

users       (id, name, city, created_at, is_vip)
orders      (id, user_id, status, total_amount, created_at)
order_items (id, order_id, product_id, quantity, unit_price)
products    (id, name, category, cost_price)

字段枚举值：
- orders.status:      'completed' | 'pending' | 'cancelled'
- users.is_vip:       0 | 1
- users.city:         '北京' | '上海' | '广州' | '成都' | '杭州' | '武汉' | '深圳' | '西安'
- products.category:  '电子产品' | '服装' | '家居' | '食品'
"""

# ── Method A：旧提示词（SQL 术语风格）────────────────────────────────────────

FORGE_DSL_SPEC_V1 = """
## Forge DSL 规范

生成一个合法的 Forge JSON 对象，字段如下（执行顺序：scan→joins→filter→group→agg→having→select→window→qualify→sort→limit）：

```json
{
  "scan":    "table_name",
  "joins":   [{"type":"inner|left|right|full|anti|semi","table":"t","on":{"left":"t1.col","right":"t2.col"}}],
  "filter":  [
    {"col":"t.col","op":"eq|neq|gt|gte|lt|lte|in|like|is_null|is_not_null|between","val":...},
    {"or":[{"col":"...","op":"...","val":...}, {"and":[{"col":"...","op":"...","val":...},...]}]}
  ],
  "group":   ["t.col"],
  "agg":     [{"fn":"count|count_all|count_distinct|sum|avg|min|max","col":"t.col_or_expr","as":"alias"}],
  "having":  [{"col":"alias","op":"...","val":...}],
  "select":  ["t.col_or_alias"],
  "window":  [
    {"fn":"row_number|rank|dense_rank","partition":["t.col"],"order":[{"col":"t.col","dir":"asc|desc"}],"as":"alias"},
    {"fn":"sum|avg|count|min|max","col":"t.col","partition":["t.col"],"order":[...],"as":"alias"},
    {"fn":"lag|lead","col":"t.col","offset":1,"default":null,"partition":["t.col"],"order":[...],"as":"alias"}
  ],
  "qualify": [{"col":"window_alias","op":"lte","val":3}],
  "sort":    [{"col":"alias","dir":"asc|desc"}],
  "limit":   N
}
```

## 关键规则（违反会导致编译失败）

| 规则 | 说明 |
|------|------|
| **select 必填** | 每个 Forge JSON 必须有 select 字段，否则编译失败 |
| **排名函数无 col** | row_number / rank / dense_rank 只需 fn、partition、order、as，绝对不能有 col 字段 |
| **TopN 用 limit** | "前N名"必须设置 limit；per-group TopN 用 window + qualify |
| **filter 是数组** | filter 必须是数组：`[{...}]`，绝不能是对象 `{...}`；OR 条件也要放在数组元素里：`[{"or":[...]}]` |
| **between 用 lo/hi** | between 范围用 `"lo":下界,"hi":上界`，不能用 `"val":[下界,上界]` |
| **join 类型选择** | inner=两侧都有记录（默认用这个）；left=允许右侧为空；只有明确需要保留空值时才用 left |
| **select 只引用真实列** | select 中只能出现 scan/joins 表的字段、agg 别名或 window 别名，不能虚构字段名 |
| **group by 与 select 一致** | 有 group 时，select 中非聚合字段必须出现在 group 列表里，不能用 MIN/MAX 包裹 group-by 列 |
| join.type 必填 | inner/left/right/full/anti/semi，无默认值 |
| count_all 无 col | 其他聚合必须有 col（col 可以是表达式，如 "t.a * t.b"） |
| 有 JOIN 时列引用加表名 | 使用 table.col 格式避免歧义 |
| sort.dir 必填 | asc 或 desc，无默认值 |
| 反向过滤用 anti join | 禁止 NOT IN，改用 anti join 避免 NULL 陷阱 |
| OR 内嵌 AND | 用 {"or":[..., {"and":[...]}]} 表达 (A AND B) OR C |
| qualify 过滤窗口结果 | 用 qualify 字段过滤 window 别名，实现每组 TopN |
| lag/lead default 为 null | default 值若为空用 JSON null，如 `"default": null` |

## 示例 1：复杂过滤（filter 必须是数组，OR 条件包在数组元素里）

需求：找出名字含"明"的用户，或者 2024 年后注册的 VIP 用户

```json
{
  "scan": "users",
  "filter": [
    {
      "or": [
        {"col": "users.name", "op": "like", "val": "%明%"},
        {"and": [
          {"col": "users.created_at", "op": "gte", "val": "2024-01-01"},
          {"col": "users.is_vip",     "op": "eq",  "val": 1}
        ]}
      ]
    }
  ],
  "select": ["users.name", "users.city", "users.is_vip"]
}
```

注意：filter 是 **数组**，`{"or":[...]}` 是数组的**一个元素**，不能把 `{"or":[...]}` 直接作为 filter 的值。

## 示例 2：时序导航（LAG/LEAD 必须用 partition 按用户分区）

需求：对每个用户的已完成订单，展示当前金额和上一笔金额

```json
{
  "scan": "orders",
  "joins": [{"type": "inner", "table": "users", "on": {"left": "orders.user_id", "right": "users.id"}}],
  "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
  "window": [{
    "fn": "lag",
    "col": "orders.total_amount",
    "offset": 1,
    "default": null,
    "partition": ["orders.user_id"],
    "order": [{"col": "orders.created_at", "dir": "asc"}],
    "as": "prev_amount"
  }],
  "select": ["users.name", "orders.created_at", "orders.total_amount", "prev_amount"]
}
```

注意：**必须有 partition**，否则 LAG 会跨用户取上一行，语义完全错误。default 用 JSON null（不是字符串 "null"，不是 Python None）。

只输出 JSON 对象，不要任何解释文字，不要 markdown 代码块。
"""

# ── Method C：新提示词（声明式风格，去除 SQL 思维）──────────────────────────

FORGE_DSL_SPEC_V2 = """
## Forge 查询格式

你的任务是描述"要什么数据"，而非"如何获取"。用以下 JSON 格式声明你的数据需求：

```json
{
  "scan":    "主数据集（表名）",
  "joins":   [{"type":"inner|left|right|full|anti|semi","table":"关联表","on":{"left":"主表.字段","right":"关联表.字段"}}],
  "filter":  [筛选条件数组],
  "group":   ["分组维度"],
  "agg":     [{"fn":"统计函数","col":"统计字段或表达式","as":"结果名"}],
  "having":  [分组后的二次筛选],
  "select":  ["输出字段列表"],
  "window":  [窗口计算表达式],
  "qualify": [窗口结果筛选],
  "sort":    [{"col":"排序字段","dir":"asc|desc"}],
  "limit":   最多返回行数
}
```

## 各字段含义

| 字段 | 作用 |
|------|------|
| scan | 主数据集，查询的起点 |
| joins | 引入其他数据集。inner=两侧都有记录才保留；left=主表记录全保留，关联表无匹配则为空；anti=只保留在关联表中**找不到**的记录；semi=只保留在关联表中**能找到**的记录 |
| filter | **数组**，筛选哪些行参与后续计算，多个条件之间是 AND |
| group | 按哪些维度分组统计 |
| agg | 每组的统计指标。fn：count_all（行数，**无 col 字段**）、count（非空数，需 col）、count_distinct（去重数，需 col）、sum、avg、min、max |
| having | 对分组统计结果的进一步筛选。**col 必须是 agg 中定义的别名（as 字段），不能是原始列名或聚合表达式** |
| select | 最终输出哪些字段。有 group 时，select 里的非统计字段必须出现在 group 里 |
| window | 保留所有行的同时，计算排名或滑动统计 |
| qualify | 对窗口结果筛选（如"只保留每组排名前3"）|
| sort | 结果排序，dir 必填（asc/desc）|
| limit | 最多返回多少行。值必须来自问题中明确的数量（"前10名"→10，"前5"→5，绝不默认填1）|

## 筛选条件格式

简单条件：
```json
{"col": "表.字段", "op": "操作符", "val": 值}
```

操作符：eq（等于）、neq（不等于）、gt/gte/lt/lte（大/小于）、in（在列表中）、like（模糊匹配，用 % 通配）、is_null、is_not_null、between

between 必须用 lo/hi，不能用 val 数组：
```json
{"col": "orders.total_amount", "op": "between", "lo": 500, "hi": 2000}
```

val 支持的类型：字符串 "text"、数字 42、布尔 true/false、null、数组 ["a","b"]（用于 in）、日期对象 {"$date":"2024-01-01"}

OR 条件（filter 是数组，OR 条件是数组里的**一个元素**）：
```json
"filter": [
  {
    "or": [
      {"col": "users.name", "op": "like", "val": "%明%"},
      {"and": [
        {"col": "users.created_at", "op": "gte", "val": "2024-01-01"},
        {"col": "users.is_vip", "op": "eq", "val": 1}
      ]}
    ]
  }
]
```
❌ 错误：`"filter": {"or": [...]}` — filter 必须是数组，不能是对象

## 分组统计规则

- 有 group 时，select 中的非统计字段必须也出现在 group 列表里
- 不能用 min/max 来"绕过" group by 约束
- having 里只能引用 agg 的别名，例如：
  agg: [{"fn":"avg","col":"orders.total_amount","as":"avg_amount"}]
  having: [{"col":"avg_amount","op":"gt","val":800}]
  ❌ 错误：{"col":"orders.total_amount","fn":"avg","op":"gt","val":800}

## 窗口计算（window）

| 需求场景 | 写法 |
|----------|------|
| 全局排名 | `{"fn":"row_number\|rank\|dense_rank","order":[...],"as":"别名"}` |
| 分组内排名 | 加 `"partition":["分组字段"]` |
| 分组内滑动统计 | `{"fn":"sum\|avg\|count\|min\|max","col":"字段","partition":[...],"order":[...],"as":"别名"}` |
| 相邻行对比（上一行/下一行）| `{"fn":"lag\|lead","col":"字段","offset":1,"partition":["分组字段"],"order":[...],"as":"别名"}` |

**三种排名函数区别（必须按需选用）：**

| fn | 并列处理 | 下一名跳号 | 示例 |
|----|---------|-----------|------|
| row_number | 强制唯一，随机打破平局 | — | 1,2,3,4 |
| rank | 并列同号 | 是 | 1,1,3,4 |
| dense_rank | 并列同号 | 否 | 1,1,2,3 |

问题说"并列排名"→ rank；"不留空隙"→ dense_rank；只需序号→ row_number。

**partition 决定"在哪个范围内计算"**：按用户分析需填用户ID字段，全局分析则不填。
排名函数（row_number/rank/dense_rank）**没有 col 字段**。

**lag/lead 的 default 设置规则**：
- 问题要求首单/末单显示特定值（如"首单显示 first_order"）→ `"default": "first_order"`
- 没有明确要求，无前/后行时显示空 → 省略 default 字段（等同于 NULL）

## 数据关联规则

- 有关联表时，所有字段引用必须加表名：`orders.total_amount` 而非 `total_amount`
- 问题要求展示的字段所在表**必须出现在 joins 中**
- 需要"排除某类数据"时用 anti join，不要用 NOT IN（NOT IN 在关联表有空值时结果错误）
- 需要"确认某数据存在"时用 semi join 或 inner join

## 示例：每个用户历次订单金额 vs 上一笔金额对比

```json
{
  "scan": "orders",
  "joins": [{"type": "inner", "table": "users", "on": {"left": "orders.user_id", "right": "users.id"}}],
  "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
  "window": [{
    "fn": "lag",
    "col": "orders.total_amount",
    "offset": 1,
    "partition": ["orders.user_id"],
    "order": [{"col": "orders.created_at", "dir": "asc"}],
    "as": "prev_amount"
  }],
  "select": ["users.name", "orders.created_at", "orders.total_amount", "prev_amount"]
}
```

## 示例：标注上一笔订单状态，首单显示 first_order

```json
{
  "scan": "orders",
  "joins": [{"type": "inner", "table": "users", "on": {"left": "orders.user_id", "right": "users.id"}}],
  "window": [{
    "fn": "lag", "col": "orders.status", "offset": 1, "default": "first_order",
    "partition": ["orders.user_id"],
    "order": [{"col": "orders.created_at", "dir": "asc"}],
    "as": "prev_status"
  }],
  "select": ["users.name", "orders.created_at", "orders.status", "prev_status"]
}
```

## 输出约束

- **select 必填**，至少一个字段
- select 只能引用真实存在的字段或统计结果别名，不能虚构列名
- "前N名"要设 limit，N 来自问题（"前10"→10，"前5"→5，绝不默认填1）
- "每组前N名"用 window + qualify

只输出 JSON 对象，不要任何解释，不要 markdown 代码块。
"""

METHOD_A_SYSTEM = f"""你是一个专业的数据查询助手，擅长用 Forge DSL 表达数据查询需求。

{SCHEMA_TEXT}

{FORGE_DSL_SPEC_V1}

用户会描述一个数据查询需求，你需要输出符合 Forge DSL 规范的 JSON。
只输出 JSON 对象，不要任何其他内容。"""

METHOD_B_SYSTEM = f"""你是一个专业的数据查询助手，擅长编写 SQLite SQL 查询。

{SCHEMA_TEXT}

用户会描述一个数据查询需求，你需要输出可以在 SQLite 上执行的正确 SQL。
只输出 SQL 语句，不要任何解释，不要 markdown 代码块。"""

METHOD_C_SYSTEM = f"""你是一个专业的数据查询助手，帮助用户用 Forge 格式描述数据查询需求。

{SCHEMA_TEXT}

{FORGE_DSL_SPEC_V2}

用户会描述一个数据查询需求，你需要输出符合 Forge 格式的 JSON。
只输出 JSON 对象，不要任何其他内容。"""

# Method D：新声明式提示词 + 精准枚举 schema（修复 count_all 规则）
METHOD_D_SYSTEM = f"""你是一个专业的数据查询助手，帮助用户用 Forge 格式描述数据查询需求。

{SCHEMA_TEXT_V2}

{FORGE_DSL_SPEC_V2}

用户会描述一个数据查询需求，你需要输出符合 Forge 格式的 JSON。
只输出 JSON 对象，不要任何其他内容。"""

# Method E：枚举 schema + 升级版提示词
# 在 D 的基础上新增：having 必须用 alias、LIMIT 精确取值、
# 三种排名函数对比表、LAG/LEAD default 规则、JOIN 字段完整性、新示例
METHOD_E_SYSTEM = METHOD_D_SYSTEM


def extract_text(content) -> str:
    if isinstance(content, list):
        return "".join(
            b.text for b in content if getattr(b, "type", None) == "text"
        ).strip()
    return str(content).strip()


def clean_json(raw: str) -> str:
    s = raw.strip()
    if s.startswith("```"):
        lines = s.split("\n")
        inner = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        s = "\n".join(inner).strip()
    return s


def _run_forge(client: anthropic.Anthropic, question: str, system: str, method: str) -> dict:
    msg = client.messages.create(
        model=MINIMAX_MODEL,
        max_tokens=2048,
        system=system,
        messages=[{"role": "user", "content": question}],
    )
    raw = clean_json(extract_text(msg.content))
    try:
        forge_json = json.loads(raw)
    except json.JSONDecodeError as e:
        return {"method": method, "forge_json": None, "sql": None,
                "error": f"JSON解析失败: {e}\n原始输出: {raw[:500]}"}
    try:
        sql = compile_query(forge_json)
        return {"method": method, "forge_json": forge_json, "sql": sql, "error": None}
    except Exception as e:
        return {"method": method, "forge_json": forge_json, "sql": None,
                "error": f"编译失败: {e}"}


def run_method_a(client: anthropic.Anthropic, question: str) -> dict:
    return _run_forge(client, question, METHOD_A_SYSTEM, "A")


def run_method_b(client: anthropic.Anthropic, question: str) -> dict:
    msg = client.messages.create(
        model=MINIMAX_MODEL,
        max_tokens=2048,
        system=METHOD_B_SYSTEM,
        messages=[{"role": "user", "content": question}],
    )
    return {"method": "B", "sql": extract_text(msg.content), "error": None}


def run_method_c(client: anthropic.Anthropic, question: str) -> dict:
    return _run_forge(client, question, METHOD_C_SYSTEM, "C")


def run_method_d(client: anthropic.Anthropic, question: str) -> dict:
    return _run_forge(client, question, METHOD_D_SYSTEM, "D")


def run_method_e(client: anthropic.Anthropic, question: str) -> dict:
    return _run_forge(client, question, METHOD_E_SYSTEM, "E")


_METHOD_FN = {"A": run_method_a, "B": run_method_b, "D": run_method_d, "E": run_method_e}


def load_existing() -> dict:
    if RESULTS_FILE.exists():
        try:
            return json.loads(RESULTS_FILE.read_text())
        except Exception:
            pass
    return {}


def save_results(data: dict) -> None:
    RESULTS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def main() -> None:
    if not CASES_FILE.exists():
        print(f"❌ 找不到 {CASES_FILE}，请先运行 generate_cases.py", file=sys.stderr)
        sys.exit(1)

    cases     = json.loads(CASES_FILE.read_text())
    client    = anthropic.Anthropic(api_key=MINIMAX_API_KEY, base_url=MINIMAX_BASE_URL)
    all_results = load_existing()

    all_methods = ("A", "B", "D", "E")
    print(f"📋 {len(cases)} 个测试用例，每用例 {'+'.join(f'{m}×{RUNS_PER_METHOD}' for m in all_methods)} = {RUNS_PER_METHOD*len(all_methods)} 次调用")

    # 构建待执行任务：只补缺失的 method/run_idx
    tasks: list[tuple] = []
    for case in cases:
        cid = str(case["id"])
        existing = all_results.get(cid, {})
        for method in all_methods:
            key = f"runs_{method.lower()}"
            done = len(existing.get(key, []))
            for i in range(done, RUNS_PER_METHOD):
                tasks.append((case, method, i))

    if not tasks:
        print("✅ 所有方法均已完成，无需重跑。")
        _print_stats(all_results)
        return

    print(f"📡 待执行调用：{len(tasks)}（并发 {MAX_WORKERS} workers）\n")

    # 为每个用例初始化结果槽
    buf: dict[str, dict] = {}
    for case in cases:
        cid = str(case["id"])
        existing = all_results.get(cid, {})
        buf[cid] = {
            "case":   case,
            "runs_a": list(existing.get("runs_a", [])),
            "runs_b": list(existing.get("runs_b", [])),
            "runs_c": list(existing.get("runs_c", [])),
            "runs_d": list(existing.get("runs_d", [])),
            "runs_e": list(existing.get("runs_e", [])),
        }

    save_lock     = threading.Lock()
    case_done_set: set[str] = set()

    total_cases_pending = len({t[0]["id"] for t in tasks})
    case_bar = tqdm(total=len(cases), desc="用例完成",
                    initial=len(cases) - total_cases_pending,
                    unit="case", position=0, dynamic_ncols=True)
    call_bar = tqdm(total=len(tasks), desc="API 调用",
                    unit="call", position=1, dynamic_ncols=True)

    def dispatch(task):
        case, method, run_idx = task
        result = _METHOD_FN[method](client, case["question"])
        return case, method, run_idx, result

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_map = {pool.submit(dispatch, t): t for t in tasks}

        for future in as_completed(future_map):
            case, method, run_idx, result = future.result()
            cid = str(case["id"])
            slot = buf[cid]

            key = f"runs_{method.lower()}"
            # 插入到正确位置（并发时可能乱序到达）
            runs = slot[key]
            while len(runs) <= run_idx:
                runs.append(None)
            runs[run_idx] = result

            status = "✓" if result.get("error") is None else f"✗ {str(result['error'])[:30]}"
            call_bar.write(f"  [{cid:>2}] {method}-{run_idx+1}: {status}")
            call_bar.update(1)

            # 检查该用例是否全部完成
            if (cid not in case_done_set
                    and all(
                        len(slot[f"runs_{m}"]) == RUNS_PER_METHOD
                        and None not in slot[f"runs_{m}"]
                        for m in ("a", "b", "d", "e")
                    )):
                case_done_set.add(cid)
                c = slot["case"]
                all_results[cid] = {
                    "id":         c["id"],
                    "category":   c["category"],
                    "difficulty": c["difficulty"],
                    "question":   c["question"],
                    "runs_a":     slot["runs_a"],
                    "runs_b":     slot["runs_b"],
                    "runs_c":     slot["runs_c"],
                    "runs_d":     slot["runs_d"],
                    "runs_e":     slot["runs_e"],
                }
                with save_lock:
                    save_results(all_results)
                case_bar.update(1)
                call_bar.write(f"💾 [{cid:>2}] {c['category']} 完成")

    call_bar.close()
    case_bar.close()
    _print_stats(all_results)


def _print_stats(all_results: dict) -> None:
    total = len(all_results)
    for method, key in (("A", "runs_a"), ("B", "runs_b"), ("D", "runs_d"), ("E", "runs_e")):
        runs = [r for v in all_results.values() for r in v.get(key, [])]
        if not runs:
            continue
        ok  = sum(1 for r in runs if r and r.get("error") is None)
        err = sum(1 for r in runs if r and r.get("error") is not None)
        if method in ("A", "D", "E"):
            print(f"   Method {method}：成功 {ok} / 失败 {err}（编译失败率 {err/(ok+err)*100:.1f}%）")
    print(f"\n✅ 共 {total} 个用例，结果已保存 → {RESULTS_FILE}")


if __name__ == "__main__":
    main()
