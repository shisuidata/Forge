"""
Method X — large 数据集 × DeepSeek V3（收窄版语义约定强化）

基线：
  - Method U：62.5% Case EA / 58.3% Run ACC
  - Method W：65.0% Case EA / 49.2% Run ACC，但普通 GROUP BY 退化

本方法验证 W 之后的收窄修正：
  1. 常规 GROUP BY 继续允许 category_id/product_id + name
  2. 仅在品类 TopN/占比窗口分区中约束 category_name 粒度
  3. 保留百分比占比、退款率同层计算、订单用户 JOIN、月度明细时间字段等规则
"""
from pathlib import Path

METHOD_ID = "x"
LABEL = "Method X（large 数据集，DeepSeek V3，收窄语义约定 retry=2）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
COMPILE_RETRIES = 2
DATASET = "large"
NOTES = "2026-05-06 Method W 后收窄 GROUP/PARTITION 语义约定"

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
