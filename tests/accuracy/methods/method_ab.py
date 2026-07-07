"""
Method AB — large 数据集 × DeepSeek V4 Pro

基线：
  - Method AA（deepseek-chat）：82.5% Case EA / 68.3% Run ACC

本方法只切换模型，不改变 Forge 规则：
  - 模型从 deepseek-chat 切换为 deepseek-v4-pro
  - 继续使用 Method AA 之后的 registry/lint 规则
"""
from pathlib import Path

METHOD_ID = "ab"
LABEL = "Method AB（large 数据集，DeepSeek V4 Pro，AA 规则 retry=2）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
COMPILE_RETRIES = 2
DATASET = "large"
NOTES = "2026-05-06 切换 DeepSeek V4 Pro，对比 Method AA"

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
