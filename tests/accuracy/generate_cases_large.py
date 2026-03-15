#!/usr/bin/env python3
"""
Method K 测试用例生成器

基于 large_demo.db（200 表电商数仓）生成 40 个测试用例，
覆盖 8 类业务场景 × 5 题，使用 14 张核心表。

输出：tests/accuracy/results/cases_large.json

运行：
    python tests/accuracy/generate_cases_large.py
    python tests/accuracy/generate_cases_large.py --force  # 强制重新生成
"""
from __future__ import annotations

import argparse
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
OUTPUT_FILE = RESULTS_DIR / "cases_large.json"

SCHEMA_TEXT = """
表结构（SQLite，large_demo.db 核心 14 张表）：

# 用户域
dim_user(user_id, user_name, gender, age_group, vip_level_id, region_id, channel_id, register_date, is_active)
  gender: 'male'|'female'|'unknown'
  age_group: '18-24'|'25-34'|'35-44'|'45-54'|'55+'
  is_active: 0|1

dim_vip_level(vip_level_id, level_name, min_points, discount_rate, free_shipping)
  level_name: '普通'|'银卡'|'金卡'|'铂金'|'钻石'

# 订单域
dwd_order_detail(order_id, user_id, merchant_id, channel_id, platform_id, promotion_id, order_status, total_amount, discount_amount, coupon_amount, freight_amount, pay_amount, order_dt)
  order_status: '待付款'|'待发货'|'待收货'|'已完成'|'已取消'

dwd_order_item_detail(order_item_id, order_id, product_id, user_id, quantity, unit_price, discount_rate, actual_amount, is_gift, order_dt)
  is_gift: 0|1

dwd_payment_detail(payment_id, order_id, user_id, payment_method_id, pay_amount, pay_status, pay_dt)
  pay_status: '成功'|'失败'|'超时'|'撤销'

dwd_refund_detail(refund_id, order_id, user_id, after_sale_type_id, refund_amount, refund_status, apply_dt, complete_dt, reason_id)
  refund_status: '申请中'|'审核通过'|'退款中'|'已退款'|'拒绝'

dwd_cart_detail(cart_id, user_id, product_id, action_type, quantity, action_dt, platform_id)
  action_type: 'add'|'remove'|'update_qty'|'checkout'

# 商品域
dim_product(product_id, product_name, category_id, brand_id, supplier_id, unit_price, cost_price, status, is_imported)
  status: 'on_sale'|'off_shelf'|'pre_sale'|'discontinued'
  is_imported: 0|1

dim_category(category_id, category_name, parent_id, level, is_leaf)
  level: 1|2|3
  is_leaf: 0|1

dim_brand(brand_id, brand_name, country, is_authorized, brand_level)
  brand_level: '国际'|'国内知名'|'新兴'|'白牌'
  is_authorized: 0|1

# 评价与售后
dwd_comment_detail(comment_id, order_item_id, user_id, product_id, rating, comment_type, has_image, has_video, comment_dt)
  rating: 1|2|3|4|5
  comment_type: '好评'|'中评'|'差评'

# 地域与渠道
dim_region(region_id, region_name, parent_id, level, tier)
  tier: '一线'|'新一线'|'二线'|'三线'|'其他'

dim_channel(channel_id, channel_name, channel_type, platform_id, cost_per_click)
  channel_type: 'organic'|'paid_search'|'social'|'email'|'affiliate'

关键 JOIN 路径：
- dwd_order_detail.user_id → dim_user.user_id
- dwd_order_detail.channel_id → dim_channel.channel_id
- dim_user.vip_level_id → dim_vip_level.vip_level_id
- dim_user.region_id → dim_region.region_id
- dwd_order_item_detail.order_id → dwd_order_detail.order_id
- dwd_order_item_detail.product_id → dim_product.product_id
- dwd_order_item_detail.user_id → dim_user.user_id
- dim_product.category_id → dim_category.category_id
- dim_product.brand_id → dim_brand.brand_id
- dwd_refund_detail.order_id → dwd_order_detail.order_id
- dwd_cart_detail.user_id → dim_user.user_id
- dwd_cart_detail.product_id → dim_product.product_id
- dwd_comment_detail.product_id → dim_product.product_id
"""

PROMPT = f"""你是一位资深电商数据分析师，同时熟悉 Forge DSL（一种 SQL 的结构化中间表示）。

{SCHEMA_TEXT}

你的任务：生成 40 个高质量的业务分析测试用例，用于评估 LLM 使用 Forge DSL 生成 SQL 的准确性。

## 用例设计要求

1. **真实业务场景**：模拟真实数据分析师的中文提问（如"统计各品牌钻石会员上个月的平均客单价"），自然口语化。
2. **难度分布**：简单题(difficulty=1)约8题，中等题(difficulty=2)约20题，困难题(difficulty=3)约12题。
3. **Forge DSL 能力范围**：不要生成需要 UNION、递归 CTE、嵌套子查询（CTE 内可以有嵌套一层 Forge 查询）的题目。
4. **覆盖 8 大类别，每类 5 题**：

| 类别 | 题数 | 重点技术特性 |
|------|------|------------|
| 多表JOIN+聚合 | 5 | 2-3 表 inner/left join，group by，sum/count/avg |
| 复杂过滤 | 5 | OR/IN/between/like/is_null 组合，$preset 相对日期 |
| 分组+HAVING | 5 | 多维 group by，having 数值阈值筛选 |
| 排名与TopN | 5 | window row_number/rank/dense_rank + qualify/limit |
| 窗口聚合 | 5 | SUM/AVG OVER PARTITION BY，累计值，组内占比 |
| 时序导航 | 5 | LAG/LEAD 环比对比，相邻行差值 |
| ANTI/SEMI JOIN | 5 | 找"没有"的记录用 anti，找"有"的记录用 semi |
| 综合复杂查询 | 5 | 3 表以上 + window + filter + 排序，含 CTE 两步聚合 |

5. **参考 SQL**：每个用例给出在 SQLite 上正确执行的标准 SQL（直接写 SQL，不要写 Forge JSON）。
6. **字段名精确**：SQL 中的表名、字段名必须与上面 schema 完全一致，不能使用不存在的字段。

## 输出格式

严格输出以下 JSON 数组（不要有任何其他文字，不要 markdown 代码块）：

[
  {{
    "id": 1,
    "category": "多表JOIN+聚合",
    "difficulty": 2,
    "question": "统计各品牌已完成订单的总销售额和订单数，按销售额降序排列",
    "reference_sql": "SELECT dim_brand.brand_name, COUNT(DISTINCT dwd_order_detail.order_id) AS order_count, SUM(dwd_order_item_detail.actual_amount) AS total_revenue\\nFROM dwd_order_item_detail\\nINNER JOIN dim_product ON dwd_order_item_detail.product_id = dim_product.product_id\\nINNER JOIN dim_brand ON dim_product.brand_id = dim_brand.brand_id\\nINNER JOIN dwd_order_detail ON dwd_order_item_detail.order_id = dwd_order_detail.order_id\\nWHERE dwd_order_detail.order_status = '已完成'\\nGROUP BY dim_brand.brand_id, dim_brand.brand_name\\nORDER BY total_revenue DESC"
  }},
  ...
]

difficulty: 1=简单, 2=中等, 3=困难

直接输出 JSON 数组，不要 markdown 代码块，不要任何解释。"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="强制重新生成（覆盖已有文件）")
    args = parser.parse_args()

    if OUTPUT_FILE.exists() and not args.force:
        cases = json.loads(OUTPUT_FILE.read_text())
        print(f"✅ cases_large.json 已存在，共 {len(cases)} 个用例，跳过生成（用 --force 强制重新生成）")
        return

    minimax_key  = os.environ.get("MINIMAX_API_KEY", "")
    minimax_url  = os.environ.get("MINIMAX_BASE_URL", "https://api.minimaxi.com/anthropic")
    minimax_model = os.environ.get("MINIMAX_MODEL", "MiniMax-M2.5")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")

    if minimax_key:
        client = anthropic.Anthropic(api_key=minimax_key, base_url=minimax_url)
        model  = minimax_model
        print(f"⏳ 正在调用 MiniMax API ({model}) 生成 40 个测试用例（大 Schema 版）...")
    elif anthropic_key:
        client = anthropic.Anthropic(api_key=anthropic_key)
        model  = "claude-opus-4-6"
        print("⏳ 正在调用 Claude API 生成 40 个测试用例（大 Schema 版）...")
    else:
        print("❌ 未设置 MINIMAX_API_KEY 或 ANTHROPIC_API_KEY", file=sys.stderr)
        sys.exit(1)

    msg = client.messages.create(
        model=model,
        max_tokens=16000,
        messages=[{"role": "user", "content": PROMPT}],
    )

    raw = msg.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    try:
        cases = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"❌ JSON 解析失败: {e}", file=sys.stderr)
        print("原始输出前 2000 字符：", file=sys.stderr)
        print(raw[:2000], file=sys.stderr)
        sys.exit(1)

    if not isinstance(cases, list) or len(cases) == 0:
        print("❌ 返回的不是非空数组", file=sys.stderr)
        sys.exit(1)

    OUTPUT_FILE.write_text(json.dumps(cases, ensure_ascii=False, indent=2))
    print(f"✅ 已生成 {len(cases)} 个测试用例 → {OUTPUT_FILE}")

    from collections import Counter
    cats = Counter(c.get("category", "?") for c in cases)
    diffs = Counter(c.get("difficulty", "?") for c in cases)
    print("\n分类分布：")
    for cat, n in sorted(cats.items()):
        print(f"  {cat}: {n}")
    print("\n难度分布：")
    for d in sorted(diffs.keys()):
        print(f"  difficulty={d}: {diffs[d]}")


if __name__ == "__main__":
    main()
