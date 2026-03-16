"""Method configuration modules for Forge DSL accuracy testing."""
from __future__ import annotations

import importlib
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass
class MethodConfig:
    id: str                                   # "l"
    label: str                                # used in reports
    mode: Literal["forge", "sql"]             # forge = compile via Forge DSL; sql = direct SQL
    system_prompt: str | None = None          # 旧式：硬编码完整 prompt（已冻结的历史版本）
    registry_context: str | None = None      # 新式：只提供 schema，prompt 由 build_system() 动态生成
    cases_file: str | None = None            # 测试用例路径（None = 使用 runner 默认）
    dataset: str | None = None               # 数据集名称（"small" | "large" | "spider"）
    model: str = "MiniMax-M2.5-highspeed"
    llm_provider: str = "anthropic"          # "anthropic" | "openai" (OpenAI-compatible)
    api_key: str | None = None               # 覆盖 env var；None = 使用环境变量
    base_url: str | None = None              # 覆盖 env var；None = 使用环境变量
    runs: int = 5
    notes: str = ""
    use_semantic_lib: bool = False
    strict_tools: bool = False   # True = DeepSeek beta strict tool calling

    def __post_init__(self):
        if self.system_prompt is None and self.registry_context is None:
            raise ValueError(f"Method '{self.id}': 必须提供 SYSTEM_PROMPT（旧式）或 REGISTRY_CONTEXT（新式）之一")


def load(method_id: str) -> MethodConfig:
    """Load a method config by its single-char id (e.g. 'l')."""
    try:
        mod = importlib.import_module(f"methods.method_{method_id}")
    except ModuleNotFoundError:
        import sys
        methods_dir = Path(__file__).parent
        if str(methods_dir.parent) not in sys.path:
            sys.path.insert(0, str(methods_dir.parent))
        mod = importlib.import_module(f"methods.method_{method_id}")
    return MethodConfig(
        id=mod.METHOD_ID,
        label=mod.LABEL,
        mode=mod.MODE,
        system_prompt=getattr(mod, "SYSTEM_PROMPT", None),
        registry_context=getattr(mod, "REGISTRY_CONTEXT", None),
        cases_file=getattr(mod, "CASES_FILE", None),
        dataset=getattr(mod, "DATASET", None),
        model=getattr(mod, "MODEL", "MiniMax-M2.5-highspeed"),
        llm_provider=getattr(mod, "LLM_PROVIDER", "anthropic"),
        api_key=getattr(mod, "API_KEY", None) or None,
        base_url=getattr(mod, "BASE_URL", None) or None,
        runs=getattr(mod, "RUNS", 5),
        notes=getattr(mod, "NOTES", ""),
        use_semantic_lib=getattr(mod, "USE_SEMANTIC_LIB", False),
        strict_tools=getattr(mod, "STRICT_TOOLS", False),
    )


def list_methods() -> list[str]:
    """Return sorted list of available method ids."""
    methods_dir = Path(__file__).parent
    ids = []
    for p in sorted(methods_dir.glob("method_*.py")):
        mid = p.stem.replace("method_", "")
        ids.append(mid)
    return ids
