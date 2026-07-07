"""
Method W — large 数据集 × DeepSeek V3（商业化前语义约定强化）

基线：Method U = 62.5% Case EA / 58.3% Run ACC。
本方法用于验证 2026-05-06 新增的失败样本驱动优化：
  1. 品类/商品展示粒度以 name 为准，避免 ID 拆分同名实体
  2. 普通"占比"按百分比输出，业务"率"指标继续遵守各自定义
  3. 退款率分子通过订单头关联用户维度，分母使用 DISTINCT order_id
  4. 品类月度订单量使用 dwd_order_item_detail.order_dt / order_id
  5. 订单查询 JOIN dim_user 时以 dwd_order_detail.user_id 为准
"""
from pathlib import Path

METHOD_ID = "w"
LABEL = "Method W（large 数据集，DeepSeek V3，语义约定强化 retry=2）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
COMPILE_RETRIES = 2
DATASET = "large"
NOTES = "2026-05-06 失败样本驱动的 Registry/lint 语义约定强化"

LLM_PROVIDER = "openai"
MODEL = "deepseek-chat"
BASE_URL = "https://api.deepseek.com/v1"
API_KEY = ""  # 读 env DEEPSEEK_API_KEY

_DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets"

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from build_context import build_registry_context

REGISTRY_CONTEXT = build_registry_context(_DATASETS_DIR / "large")
CASES_FILE = str(_DATASETS_DIR / "large" / "cases.json")
