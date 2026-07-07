"""
Method AD — large 数据集 × DeepSeek V4 Pro（剩余失败样本定向收敛）

基线：
  - Method AC（deepseek-v4-pro）：92.5% Case EA / 80.8% Run ACC

本方法验证三处补强：
  1. 会员等级聚合按 level_name 展示粒度，不按 vip_level_id 拆分
  2. 仅显示商品名称的反存在查询必须对 product_name 去重
  3. 品牌钻石会员均价保留 level_name 与 order_count 审核字段
"""
from pathlib import Path

METHOD_ID = "ad"
LABEL = "Method AD（large 数据集，DeepSeek V4 Pro，剩余失败样本定向 retry=2）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
COMPILE_RETRIES = 2
DATASET = "large"
NOTES = "2026-05-06 在 Method AC 基础上定向收敛 Case 5/33/36"

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
