"""Method AH - Method AG evaluated through Volcengine Ark Coding Plan."""
from pathlib import Path

METHOD_ID = "ah"
LABEL = "Method AH（large 数据集，Ark Coding Plan，全轮 lint + 稳定性约束 retry=2）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
COMPILE_RETRIES = 2
DATASET = "large"
NOTES = "2026-07-13 使用 ark-code-latest 验证 OpenAI-compatible 跨 Provider 准确率"

LLM_PROVIDER = "openai"
MODEL = "ark-code-latest"
BASE_URL = "https://ark.cn-beijing.volces.com/api/coding/v3"

import os
import sys

API_KEY = os.getenv("ARK_API_KEY", "")
_DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets"

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from build_context import build_registry_context

REGISTRY_CONTEXT = build_registry_context(_DATASETS_DIR / "large")
CASES_FILE = str(_DATASETS_DIR / "large" / "cases.json")
