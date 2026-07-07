"""
Method AF — large 数据集 × DeepSeek V4 Pro（AE + Case 7 契约修正）

基线：
  - Method AD：100.0% Case EA / 89.2% Run ACC / 75.0% Case EA(all)
  - Method AE：97.5% Case EA / 89.2% Run ACC / 77.5% Case EA(all)

本方法在 AE 的稳定性规则上补强 Case 7：
  - 客单价范围订单查找固定输出 order_id、user_name、age_group、total_amount
  - 禁止混用旧示例表 orders 与 large schema 的 dim_/dwd_ 表
"""
from pathlib import Path

METHOD_ID = "af"
LABEL = "Method AF（large 数据集，DeepSeek V4 Pro，Case 7 契约修正 retry=2）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
COMPILE_RETRIES = 2
DATASET = "large"
NOTES = "2026-05-06 在 Method AE 基础上修复 Case 7 输出列/表名契约"

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
