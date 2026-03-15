"""
Method L — 当前生产版本基准（接线到 agent/prompts.py）

prompt 不再硬编码于此文件。
REGISTRY_CONTEXT 从 tests/datasets/large/schema_context.md 读取，
在运行时调用 agent.prompts.build_system() 动态生成 system prompt。

数据集：tests/datasets/large/
  database.db             ← 200 张表电商数仓（SQLite）
  schema.registry.json    ← 结构层（forge sync 生成）
  metrics.registry.yaml   ← 语义层
  cases.json              ← 40 个测试用例

prompt 版本由 git 管理，每次 benchmark 反映当前 HEAD 的生产代码。
"""
from pathlib import Path

METHOD_ID = "l"
LABEL = "Method L（当前生产版本，prompt 由 agent/prompts.py 动态生成）"
MODE = "forge"
USE_SEMANTIC_LIB = True
RUNS = 3
DATASET = "large"
NOTES = "2026-03-16 数据集迁移到 tests/datasets/large/，从文件读取 REGISTRY_CONTEXT"

_DATASETS_DIR = Path(__file__).parent.parent.parent / "datasets"

REGISTRY_CONTEXT = (_DATASETS_DIR / "large" / "schema_context.md").read_text(encoding="utf-8").strip()
CASES_FILE = str(_DATASETS_DIR / "large" / "cases.json")
