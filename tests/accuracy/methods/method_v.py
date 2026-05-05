"""
Method V — large 数据集 × Z.AI GLM-5.1

使用 Z.AI 官方 OpenAI-compatible API。
数据集：tests/datasets/large/（40 个用例，200 张表电商数仓）
"""
from pathlib import Path
import os
import sys

METHOD_ID = "v"
LABEL = "Method V（large 数据集，Z.AI GLM-5.1）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
COMPILE_RETRIES = 2
DATASET = "large"
NOTES = "Z.AI GLM-5.1 × large 数据集，OpenAI-compatible API，retry=2"

LLM_PROVIDER = "openai"
MODEL = os.environ.get("ZAI_MODEL", "glm-5.1")
BASE_URL = os.environ.get("ZAI_BASE_URL", "https://api.z.ai/api/paas/v4")
API_KEY = (
    os.environ.get("ZAI_API_KEY")
    or os.environ.get("GLM_API_KEY")
    or os.environ.get("ZHIPU_API_KEY")
    or ""
)

_DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets"

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from build_context import build_registry_context

REGISTRY_CONTEXT = build_registry_context(_DATASETS_DIR / "large")
CASES_FILE = str(_DATASETS_DIR / "large" / "cases.json")
