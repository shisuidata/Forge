"""Method AI - Ark Coding Plan after result-contract and QUALIFY fixes."""
from pathlib import Path

METHOD_ID = "ai"
LABEL = "Method AI（large 数据集，Ark Coding Plan，结果契约 + QUALIFY 修复）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
COMPILE_RETRIES = 2
DATASET = "large"
NOTES = "2026-07-13 验证弱模型结果列契约、品类展示粒度和内部排名列隔离"

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
