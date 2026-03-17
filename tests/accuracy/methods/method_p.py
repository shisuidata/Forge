"""
Method P — small 数据集 × Qwen2.5-7B（硅基流动 API）

验证 Forge 在弱模型上的增益。与 Method M 对比：
    - M: Claude (强模型) × small → EA 95%
    - P: Qwen-7B (弱模型) × small → EA ???

预期：Forge DSL 约束在弱模型上增益显著（> 10pp vs 直出 SQL）。
数据集：tests/datasets/small/（40 个用例，4 张表简单电商）
"""
from pathlib import Path

METHOD_ID = "p"
LABEL = "Method P（small 数据集，Qwen2.5-7B 硅基流动 API）"
MODE = "forge"
USE_SEMANTIC_LIB = False
RUNS = 3
DATASET = "small"
NOTES = "2026-03-17 small 数据集 × Qwen2.5-7B（弱模型增强验证）"

LLM_PROVIDER = "openai"
MODEL    = "Qwen/Qwen2.5-7B-Instruct"
BASE_URL = "https://api.siliconflow.cn/v1"
API_KEY  = ""                       # 优先读 env SILICONFLOW_API_KEY

_DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets"

REGISTRY_CONTEXT = (_DATASETS_DIR / "small" / "schema_context.md").read_text(encoding="utf-8").strip()
CASES_FILE = str(_DATASETS_DIR / "small" / "cases.json")
