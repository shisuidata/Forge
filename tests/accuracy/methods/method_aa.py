"""
Method AA — large 数据集 × DeepSeek V3（Method Z 后执行失败收敛）

基线：
  - Method Z：82.5% Case EA / 61.7% Run ACC

本方法验证两处补强：
  1. "每个品类按月"的 LEAD/LAG 查询按展示粒度 category_name + month 聚合
  2. 拦截把窗口 alias 写成 cte.alias 的无效字段引用，避免运行期 no such column
"""
from pathlib import Path

METHOD_ID = "aa"
LABEL = "Method AA（large 数据集，DeepSeek V3，月度品类粒度/窗口alias收敛 retry=2）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
COMPILE_RETRIES = 2
DATASET = "large"
NOTES = "2026-05-06 Method Z 后收敛品类月度 lead 粒度和窗口 alias 执行失败"

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
