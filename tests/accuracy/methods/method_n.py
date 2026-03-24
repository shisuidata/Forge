"""
Method N — large 数据集 × DeepSeek V3（官方 API）

与 Method L 唯一的区别是 LLM 换成 DeepSeek V3，用于横向对比模型能力。
数据集：tests/datasets/large/（40 个用例，200 张表电商数仓）
"""
from pathlib import Path

METHOD_ID = "n"
LABEL = "Method N（large 数据集，DeepSeek V3 官方 API）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
DATASET = "large"
NOTES = "2026-03-16 large 数据集 × DeepSeek-V3 基准"

LLM_PROVIDER = "openai"
MODEL    = "deepseek-chat"          # DeepSeek-V3（官方 API 最新别名）
BASE_URL = "https://api.deepseek.com/v1"
API_KEY  = ""                       # 优先读 env DEEPSEEK_API_KEY

_DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets"

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from build_context import build_registry_context
REGISTRY_CONTEXT = build_registry_context(_DATASETS_DIR / "large")
CASES_FILE = str(_DATASETS_DIR / "large" / "cases.json")
