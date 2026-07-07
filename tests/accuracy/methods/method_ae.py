"""
Method AE — large 数据集 × DeepSeek V4 Pro（多次生成一致性收敛）

基线：
  - Method AD（deepseek-v4-pro）：100.0% Case EA / 89.2% Run ACC / 75.0% Case EA(all)

本方法验证一组稳定性规则：
  1. 客单价范围、好评带图、退款商品排行的输出契约
  2. 商品品类占比、相邻评价、渠道月度环比的展示/排序契约
  3. 购物车反连接、跨事件计数的限定字段契约
"""
from pathlib import Path

METHOD_ID = "ae"
LABEL = "Method AE（large 数据集，DeepSeek V4 Pro，多次生成一致性 retry=2）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
COMPILE_RETRIES = 2
DATASET = "large"
NOTES = "2026-05-06 在 Method AD 基础上提升 all-correct 与 Run ACC 稳定性"

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
