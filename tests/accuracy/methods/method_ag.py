"""
Method AG - Method AF + every-retry lint and semantic stability guards.

Targets the remaining unstable patterns from the 2026-07-13 AF rerun:
  - derived metric filters must not regroup CTE output through HAVING
  - adjacent-record navigation must retain the full event history
  - order interval analysis uses completed orders
  - anti joins use stable entity IDs
  - internal ranking aliases do not leak into result columns
"""
from pathlib import Path

METHOD_ID = "ag"
LABEL = "Method AG（large 数据集，DeepSeek V4 Pro，全轮 lint + 稳定性约束 retry=2）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
COMPILE_RETRIES = 2
DATASET = "large"
NOTES = "2026-07-13 在 Method AF 基础上补强重试门禁与剩余不稳定查询契约"

LLM_PROVIDER = "openai"
MODEL = "deepseek-v4-pro"
BASE_URL = "https://api.deepseek.com/v1"
API_KEY = ""  # read from DEEPSEEK_API_KEY

_DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets"

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from build_context import build_registry_context

REGISTRY_CONTEXT = build_registry_context(_DATASETS_DIR / "large")
CASES_FILE = str(_DATASETS_DIR / "large" / "cases.json")
