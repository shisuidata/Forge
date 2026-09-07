"""Method b_large_sem — 直接生成 SQL + 语义消歧库（大 Schema 对照组）

与 Method K 使用同一套 14 张电商数仓表和 40 道测试题，
直接让模型输出 SQLite SQL（不经过 Forge DSL），但启用语义消歧库预处理。

三方对比：
  K          = Forge DSL + 大 Schema + 语义库
  b_large    = 直出 SQL  + 大 Schema（无语义库）
  b_large_sem = 直出 SQL + 大 Schema + 语义库  ← 本方法
"""
from methods.method_b_large import SYSTEM_PROMPT  # 复用相同的 schema 和 system prompt

METHOD_ID = "b_large_sem"
LABEL = "Method SQL-Large+Sem（大Schema直接SQL + 语义消歧库）"
MODE = "sql"
RUNS = 3
USE_SEMANTIC_LIB = True
NOTES = "2026-03-15 大Schema直出SQL对照组，加语义消歧库预处理，与K公平对比"
