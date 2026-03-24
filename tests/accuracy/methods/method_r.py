"""
Method R — large 数据集 × MiniMax M2.7-highspeed

与 Method L 唯一的区别是模型从 M2.5-highspeed 换成 M2.7-highspeed，
用于对比同代际新模型在大 Schema（200 张表电商数仓）上的 EA 变化。
数据集：tests/datasets/large/（40 个用例，200 张表电商数仓）
"""
from pathlib import Path

METHOD_ID = "r"
LABEL = "Method R（large 数据集，MiniMax M2.7-highspeed）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
DATASET = "large"
NOTES = "2026-03-18 MiniMax M2.7-highspeed × large 数据集基准"

MODEL = "MiniMax-M2.7-highspeed"
BASE_URL = "https://api.minimaxi.com/anthropic"

_DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets"

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from build_context import build_registry_context
REGISTRY_CONTEXT = build_registry_context(_DATASETS_DIR / "large")
CASES_FILE = str(_DATASETS_DIR / "large" / "cases.json")
