"""
Method S — large 数据集 × MiniMax M2.7-highspeed，P0 修复后基准

与 Method R 完全一致，用于验证以下 P0 修复的实际 EA 提升：
  1. antijoin.md — 新增 scan 必须是主表的警告和反例
  2. field_conventions.registry.yaml — 新增 category_output_name 约定
     （品类查询输出 category_name 而非 category_id）

数据集：tests/datasets/large/（40 个用例，200 张表电商数仓）
对比基准：Method R = 67.5%
"""
from pathlib import Path

METHOD_ID = "s"
LABEL = "Method S（large 数据集，MiniMax M2.7-highspeed，P0修复后）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
DATASET = "large"
NOTES = "2026-03-25 P0修复后（antijoin scan警告 + category_output_name约定）"

MODEL = "MiniMax-M2.7-highspeed"
BASE_URL = "https://api.minimaxi.com/anthropic"

_DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets"

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from build_context import build_registry_context
REGISTRY_CONTEXT = build_registry_context(_DATASETS_DIR / "large")
CASES_FILE = str(_DATASETS_DIR / "large" / "cases.json")
