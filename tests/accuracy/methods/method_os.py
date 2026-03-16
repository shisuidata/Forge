"""
Method OS — small 数据集 × DeepSeek V3 Strict Tool Calling（Beta）

与 Method O 的唯一区别：
  - BASE_URL 指向 DeepSeek beta 端点
  - STRICT_TOOLS = True，使用 tool calling + strict JSON Schema
  - schema_builder 输出全字段 required + anyOf[T, null] 可选字段

目的：验证 strict mode 是否消除剩余的编译失败和格式错误。
数据集：tests/datasets/small/（40 个用例，4 张表简单电商）
"""
from pathlib import Path

METHOD_ID = "os"
LABEL = "Method OS（small 数据集，DeepSeek V3 Strict Tool Calling Beta）"
MODE = "forge"
USE_SEMANTIC_LIB = False
STRICT_TOOLS = True
RUNS = 3
DATASET = "small"
NOTES = "2026-03-16 DeepSeek strict tool calling beta 对比实验"

LLM_PROVIDER = "openai"
MODEL    = "deepseek-chat"
BASE_URL = "https://api.deepseek.com/beta/v1"
API_KEY  = ""                       # 优先读 env DEEPSEEK_API_KEY

_DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets"

REGISTRY_CONTEXT = (_DATASETS_DIR / "small" / "schema_context.md").read_text(encoding="utf-8").strip()
CASES_FILE = str(_DATASETS_DIR / "small" / "cases.json")
