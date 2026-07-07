"""
Method Y — large 数据集 × DeepSeek V3（Method X 后继续收窄）

基线：
  - Method X：70.0% Case EA / 57.5% Run ACC

本方法验证 X 后的两处修正：
  1. "消费总额"不再默认强制已完成订单，避免把普通消费汇总误收窄
  2. 退款率强制 0~1 小数口径，避免被普通"占比"百分比规则污染
  3. "每个品类"月度/窗口查询也触发品类窗口分区约定
"""
from pathlib import Path

METHOD_ID = "y"
LABEL = "Method Y（large 数据集，DeepSeek V3，退款率/消费口径收窄 retry=2）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
COMPILE_RETRIES = 2
DATASET = "large"
NOTES = "2026-05-06 Method X 后继续收窄消费总额和退款率口径"

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
