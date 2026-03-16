"""
Method M — small 数据集 × 当前生产版本 prompt

用于验证 agent/prompts.py 在 small 数据集（4 张表，简单电商）上的准确率。
与 Method L 唯一的区别是数据集从 large 换成 small。
"""
from pathlib import Path

METHOD_ID = "m"
LABEL = "Method M（small 数据集，prompt 由 agent/prompts.py 动态生成）"
MODE = "forge"
USE_SEMANTIC_LIB = False
RUNS = 3
DATASET = "small"
NOTES = "2026-03-16 small 数据集基准"

_DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets"

REGISTRY_CONTEXT = (_DATASETS_DIR / "small" / "schema_context.md").read_text(encoding="utf-8").strip()
CASES_FILE = str(_DATASETS_DIR / "small" / "cases.json")
