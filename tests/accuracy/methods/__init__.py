"""Method configuration modules for Forge DSL accuracy testing."""
from __future__ import annotations

import importlib
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass
class MethodConfig:
    id: str                                   # "f"
    label: str                                # used in reports
    mode: Literal["forge", "sql"]             # forge = compile via Forge DSL; sql = direct SQL
    system_prompt: str                        # full system prompt
    model: str = "MiniMax-M2.5-highspeed"
    runs: int = 5
    notes: str = ""
    use_semantic_lib: bool = False            # 是否启用语义消歧库对问题进行预处理


def load(method_id: str) -> MethodConfig:
    """Load a method config by its single-char id (e.g. 'f')."""
    try:
        mod = importlib.import_module(f"methods.method_{method_id}")
    except ModuleNotFoundError:
        # Try relative import when running from tests/accuracy/
        import sys
        methods_dir = Path(__file__).parent
        if str(methods_dir.parent) not in sys.path:
            sys.path.insert(0, str(methods_dir.parent))
        mod = importlib.import_module(f"methods.method_{method_id}")
    return MethodConfig(
        id=mod.METHOD_ID,
        label=mod.LABEL,
        mode=mod.MODE,
        system_prompt=mod.SYSTEM_PROMPT,
        model=getattr(mod, "MODEL", "MiniMax-M2.5-highspeed"),
        runs=getattr(mod, "RUNS", 5),
        notes=getattr(mod, "NOTES", ""),
        use_semantic_lib=getattr(mod, "USE_SEMANTIC_LIB", False),
    )


def list_methods() -> list[str]:
    """Return sorted list of available method ids."""
    methods_dir = Path(__file__).parent
    ids = []
    for p in sorted(methods_dir.glob("method_*.py")):
        mid = p.stem.replace("method_", "")
        ids.append(mid)
    return ids
