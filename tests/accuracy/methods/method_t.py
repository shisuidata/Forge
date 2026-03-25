"""
Method T — large 数据集 × MiniMax M2.7-highspeed，全量 P0+P1 修复后

与 Method R 相同配置（retry=2, USE_SEMANTIC_LIB=True），
但包含 2026-03-25 新增的两项 P0 修复：
  1. antijoin.md — scan 必须是主表的警告 + ❌/✅ 反例
  2. field_conventions.registry.yaml — category_output_name 约定

数据集：tests/datasets/large/（40 个用例，200 张表电商数仓）
对比基准：Method R = 72.5%（2026-03-19）
"""
from pathlib import Path

METHOD_ID = "t"
LABEL = "Method T（large 数据集，MiniMax M2.7-highspeed，全量修复后 retry=2）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
COMPILE_RETRIES = 2
DATASET = "large"
NOTES = "2026-03-25 全量 P0+P1 修复后（antijoin scan + category约定 + retry=2）"

MODEL = "MiniMax-M2.7-highspeed"
BASE_URL = "https://api.minimaxi.com/anthropic"

_DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets"

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from build_context import build_registry_context
REGISTRY_CONTEXT = build_registry_context(_DATASETS_DIR / "large")
CASES_FILE = str(_DATASETS_DIR / "large" / "cases.json")
