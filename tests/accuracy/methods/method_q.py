"""
Method Q — small 数据集 × MiniMax M2.7-highspeed

与 Method M 唯一的区别是模型从 M2.5-highspeed 换成 M2.7-highspeed，
用于对比同代际新模型在小数据集上的 EA 变化。
数据集：tests/datasets/small/（40 个用例，4 张表简单电商）
"""
from pathlib import Path

METHOD_ID = "q"
LABEL = "Method Q（small 数据集，MiniMax M2.7-highspeed）"
MODE = "forge"
USE_SEMANTIC_LIB = False
RUNS = 3
DATASET = "small"
NOTES = "2026-03-18 MiniMax M2.7-highspeed × small 数据集基准"

MODEL = "MiniMax-M2.7-highspeed"

_DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets"

REGISTRY_CONTEXT = (_DATASETS_DIR / "small" / "schema_context.md").read_text(encoding="utf-8").strip()
CASES_FILE = str(_DATASETS_DIR / "small" / "cases.json")
