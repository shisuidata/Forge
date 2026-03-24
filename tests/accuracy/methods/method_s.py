"""
Method S — large 数据集 × Claude Sonnet 4.6（原生 Anthropic API）

用于对比 Forge DSL + Claude 在大 Schema（200 张表）场景下的 EA。
与 Method R（M2.7）和 Method N（DeepSeek）横向对比。
数据集：tests/datasets/large/（40 个用例，200 张表电商数仓）
"""
from pathlib import Path

METHOD_ID = "s"
LABEL = "Method S（large 数据集，Claude Sonnet 4.6）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
DATASET = "large"
NOTES = "2026-03-18 Claude Sonnet 4.6 × large 数据集基准"

MODEL    = "claude-sonnet-4-6"
BASE_URL = "anthropic"          # 触发原生 Anthropic API 路径（非 MiniMax）

_DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets"

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from build_context import build_registry_context
REGISTRY_CONTEXT = build_registry_context(_DATASETS_DIR / "large")
CASES_FILE = str(_DATASETS_DIR / "large" / "cases.json")
