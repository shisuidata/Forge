"""
Method O — small 数据集 × DeepSeek V3（官方 API）

与 Method M 唯一的区别是 LLM 换成 DeepSeek V3，用于横向对比模型能力。
数据集：tests/datasets/small/（40 个用例，4 张表简单电商）
"""
from pathlib import Path

METHOD_ID = "o"
LABEL = "Method O（small 数据集，DeepSeek V3 官方 API）"
MODE = "forge"
USE_SEMANTIC_LIB = False
RUNS = 3
DATASET = "small"
NOTES = "2026-03-16 small 数据集 × DeepSeek-V3 基准"

LLM_PROVIDER = "openai"
MODEL    = "deepseek-chat"
BASE_URL = "https://api.deepseek.com/v1"
API_KEY  = ""                       # 优先读 env DEEPSEEK_API_KEY

_DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets"

REGISTRY_CONTEXT = (_DATASETS_DIR / "small" / "schema_context.md").read_text(encoding="utf-8").strip()
CASES_FILE = str(_DATASETS_DIR / "small" / "cases.json")
