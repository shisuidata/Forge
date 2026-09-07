"""
Method J_sem — Forge DSL + 语义消歧库

Method J 加强版：在 Method J 的基础上，通过语义消歧库对问题进行预处理，
追加针对已知歧义点的括号说明，再走 Forge JSON → SQL 编译流程。

与 Method J 的唯一区别：use_semantic_lib = True
"""
from methods.method_j import (  # 复用 Method J 的完整 system prompt
    SYSTEM_PROMPT,
    _SCHEMA,
)

METHOD_ID = "j_sem"
LABEL = "Method J+Sem（Forge DSL + 语义消歧库）"
MODE = "forge"
USE_SEMANTIC_LIB = True
NOTES = "2026-03-12 Method J + 语义消歧库预处理，测量语义库对 Forge DSL 方案的额外提升"
