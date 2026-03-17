"""
Method B-Qwen — small 数据集 × Qwen2.5-7B 直出 SQL（对照组）

与 Method P 对比，验证 Forge DSL 约束在弱模型上的增益。
    - P:      Forge DSL × Qwen-7B → EA ???
    - B-Qwen: 直出 SQL × Qwen-7B → EA ???

数据集：tests/datasets/small/（40 个用例，4 张表简单电商）
"""
from pathlib import Path

METHOD_ID = "b_qwen"
LABEL = "Method B-Qwen（small 数据集，Qwen2.5-7B 直出 SQL 对照组）"
MODE = "direct_sql"
USE_SEMANTIC_LIB = False
RUNS = 3
DATASET = "small"
NOTES = "2026-03-17 small 数据集 × Qwen2.5-7B 直出 SQL（弱模型对照组）"

LLM_PROVIDER = "openai"
MODEL    = "Qwen/Qwen2.5-7B-Instruct"
BASE_URL = "https://api.siliconflow.cn/v1"
API_KEY  = ""                       # 优先读 env SILICONFLOW_API_KEY

_DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets"

REGISTRY_CONTEXT = (_DATASETS_DIR / "small" / "schema_context.md").read_text(encoding="utf-8").strip()
CASES_FILE = str(_DATASETS_DIR / "small" / "cases.json")
