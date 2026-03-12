#!/usr/bin/env python3
"""
Step 1 of 3 — 生成测试用例和参考答案

调用 Claude API 生成 32 个真实业务场景的 SQL 查询题目及参考 SQL。
题目覆盖 Forge DSL 全部特性，难度偏高，贴近真实分析师提问习惯。

输出：tests/accuracy/results/cases.json

运行：
    python tests/accuracy/generate_cases.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import anthropic
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))

RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)
OUTPUT_FILE = RESULTS_DIR / "cases.json"

SCHEMA_TEXT = """
表结构（SQLite，测试数据库）：

users       (id, name, city, created_at, is_vip)
orders      (id, user_id, status, total_amount, created_at)
order_items (id, order_id, product_id, quantity, unit_price)
products    (id, name, category, cost_price)

典型值说明：
- orders.status: 'completed' | 'cancelled' | 'pending'
- users.is_vip: 1 / 0
- products.category: 'electronics' | 'clothing' | 'food' | 'books'
- 所有金额字段为数值型（REAL）
"""

FORGE_DSL_SPEC = """
## Forge DSL 完整规范

Forge JSON 字段（execution order: scan→joins→filter→group→agg→having→select→window→sort→limit）：

```
{
  "scan":   "table_name",                          // 必填，主表
  "joins":  [{
    "type": "inner|left|right|full|anti|semi",     // 必填，无默认值
    "table": "table_name",
    "on": {"left": "t1.col", "right": "t2.col"}
  }],
  "filter": [                                      // WHERE 条件
    {"col": "t.col", "op": "eq|neq|gt|gte|lt|lte|in|like|is_null|is_not_null|between", "val": ...},
    {"or": [{...}, {...}]}                         // OR 组
  ],
  "group":  ["t.col"],                             // GROUP BY
  "agg":    [{"fn": "count|count_all|count_distinct|sum|avg|min|max", "col": "t.col", "as": "alias"}],
  "having": [{"col": "alias", "op": "...", "val": ...}],
  "select": ["t.col_or_alias"],
  "window": [{
    "fn": "row_number|rank|dense_rank|sum|avg|count|min|max|lag|lead",
    "col": "t.col",        // 排名类(row_number/rank/dense_rank)不填
    "partition": ["t.col"],
    "order": [{"col": "t.col", "dir": "asc|desc"}],
    "offset": 1,           // lag/lead 专用
    "default": null,       // lag/lead 专用
    "as": "alias"
  }],
  "sort":   [{"col": "col_or_alias", "dir": "asc|desc"}],  // dir 必填
  "limit":  N
}
```

特殊规则：
- anti join → 编译为 LEFT JOIN + WHERE right_key IS NULL（NOT IN 的安全替代）
- semi join → 编译为 WHERE EXISTS (SELECT 1 FROM ...)
- count_all 无 col 字段；其他聚合函数必须有 col
- between 用 "lo" / "hi" 而非 "val"
- 有 JOIN 时所有列引用必须带表名前缀（table.col）
"""

PROMPT = f"""你是一位资深 SQL 工程师，同时熟悉 Forge DSL。

{SCHEMA_TEXT}

{FORGE_DSL_SPEC}

你的任务：生成 32 个高质量的业务分析测试用例，用于评估 LLM 使用 Forge DSL 生成 SQL 的准确性。

## 用例设计要求

1. **真实业务场景**：模拟真实的数据分析师提问，使用自然的中文表达（如"统计一下 VIP 用户上个月的复购率"），不要出现技术术语。
2. **难度偏高**：尽量测试复杂场景，简单的单表 count 不要超过 2 个。
3. **Forge 能力范围内**：不要生成 Forge 无法表达的查询（如嵌套子查询、CTE、UNION、递归）。
4. **覆盖所有特性**：

   | 类别 | 用例数 | 特性 |
   |------|--------|------|
   | 多表 JOIN + 聚合 | 6 | 2-3 表 inner/left join |
   | 复杂过滤 | 4 | OR、in、between、like、is_null 组合 |
   | 分组 + HAVING | 5 | 多维分组，聚合过滤 |
   | 排名与 TopN | 5 | window row_number/rank + limit |
   | 窗口聚合 | 4 | SUM/AVG OVER PARTITION BY |
   | 时序导航 | 3 | LAG/LEAD 环比/同比 |
   | ANTI/SEMI JOIN | 3 | 找未下单用户、有订单的商品等 |
   | 综合复杂查询 | 2 | 3 表 + 窗口 + 过滤 + 排序 |

5. **参考 SQL**：每个用例给出在 SQLite 上正确执行的标准 SQL（不要用 Forge JSON，直接写 SQL）。

## 输出格式

严格输出以下 JSON 数组（不要有任何其他文字）：

```json
[
  {{
    "id": 1,
    "category": "多表JOIN+聚合",
    "difficulty": 2,
    "question": "统计每个城市的有效订单金额和订单数，只看已完成的订单，按金额降序排列",
    "reference_sql": "SELECT users.city, COUNT(orders.id) AS order_count, SUM(orders.total_amount) AS total\\nFROM orders\\nINNER JOIN users ON orders.user_id = users.id\\nWHERE orders.status = 'completed'\\nGROUP BY users.city\\nORDER BY total DESC"
  }},
  ...
]
```

difficulty: 1=简单, 2=中等, 3=困难

直接输出 JSON 数组，不要 markdown 代码块，不要任何解释。
"""


def main() -> None:
    if OUTPUT_FILE.exists():
        cases = json.loads(OUTPUT_FILE.read_text())
        print(f"✅ cases.json 已存在，共 {len(cases)} 个用例，跳过生成")
        return

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("❌ 未设置 ANTHROPIC_API_KEY（也可手动将 cases.json 放到 results/ 目录）", file=sys.stderr)
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    print("⏳ 正在调用 Claude API 生成测试用例...")
    msg = client.messages.create(
        model="claude-opus-4-6",
        max_tokens=8192,
        messages=[{"role": "user", "content": PROMPT}],
    )

    raw = msg.content[0].text.strip()

    # 兼容模型有时在 JSON 前后加 markdown fences
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    try:
        cases = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"❌ JSON 解析失败: {e}", file=sys.stderr)
        print("原始输出：", file=sys.stderr)
        print(raw[:2000], file=sys.stderr)
        sys.exit(1)

    if not isinstance(cases, list) or len(cases) == 0:
        print("❌ 返回的不是非空数组", file=sys.stderr)
        sys.exit(1)

    OUTPUT_FILE.write_text(json.dumps(cases, ensure_ascii=False, indent=2))
    print(f"✅ 已生成 {len(cases)} 个测试用例 → {OUTPUT_FILE}")

    # 统计分类分布
    from collections import Counter
    cats = Counter(c.get("category", "?") for c in cases)
    for cat, n in sorted(cats.items()):
        print(f"   {cat}: {n}")


if __name__ == "__main__":
    main()
