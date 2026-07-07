"""
Method AC — large 数据集 × DeepSeek V4 Pro（占比/退款率口径收敛）

基线：
  - Method AB（deepseek-v4-pro）：77.5% Case EA / 70.8% Run ACC

本方法验证三处补强：
  1. 普通"占比/比例"输出 0~1 小数，ROUND(..., 4)，不乘以 100
  2. 退款率按"退款订单数/总订单数"时输出分子、分母、退款率
  3. 品类退款率必须通过订单明细表连接商品/品类，禁止 order_id -> product_id 错误 JOIN
"""
from pathlib import Path

METHOD_ID = "ac"
LABEL = "Method AC（large 数据集，DeepSeek V4 Pro，占比/退款率口径 retry=2）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
COMPILE_RETRIES = 2
DATASET = "large"
NOTES = "2026-05-06 Method AB 后收敛普通占比和退款率 JOIN/输出口径"

LLM_PROVIDER = "openai"
MODEL = "deepseek-v4-pro"
BASE_URL = "https://api.deepseek.com/v1"
API_KEY = ""  # 读 env DEEPSEEK_API_KEY

_DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets"

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from build_context import build_registry_context

REGISTRY_CONTEXT = build_registry_context(_DATASETS_DIR / "large")
CASES_FILE = str(_DATASETS_DIR / "large" / "cases.json")
