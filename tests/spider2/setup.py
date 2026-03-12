#!/usr/bin/env python3
"""
Spider2-Lite 数据集下载脚本（仅 SQLite 子集）。

使用 git sparse-checkout 只克隆需要的目录，跳过 BigQuery / Snowflake 数据。

运行：
    python tests/spider2/setup.py

下载内容（保存到 tests/spider2/data/）：
    spider2-lite.jsonl                          — 全部 547 个任务描述
    resource/databases/sqlite/                  — SQLite 数据库文件
    evaluation_suite/gold/sql_queries/          — Gold SQL
    evaluation_suite/gold/execution_results/    — Gold 执行结果
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SPIDER2_REPO = "https://github.com/xlang-ai/Spider2.git"
DATA_DIR     = Path(__file__).parent / "data"

# spider2-lite/resource/databases/sqlite/ 目录包含的是 DDL.csv + sample JSON，
# 实际 .sqlite 文件需要从 Google Drive 单独下载（见下方说明）。
SPARSE_PATHS = [
    "spider2-lite/spider2-lite.jsonl",
    "spider2-lite/resource/databases/sqlite",
    "spider2-lite/evaluation_suite/gold",
]

# 实际 .sqlite 文件（执行查询必需）需从 Google Drive 下载：
#   https://drive.usercontent.google.com/download?id=1coEVsCZq-Xvj9p2TnhBFoFTsY-UoYGmG
# 下载后解压，将所有 .sqlite 文件放到：
#   tests/spider2/data/spider2-lite/resource/databases/spider2-localdb/
LOCALDB_DIR = DATA_DIR / "spider2-lite" / "resource" / "databases" / "spider2-localdb"


def run(cmd: list[str], cwd: Path | None = None) -> None:
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, check=True)


def setup() -> None:
    if DATA_DIR.exists() and (DATA_DIR / "spider2-lite" / "spider2-lite.jsonl").exists():
        print(f"✅ 数据已存在：{DATA_DIR}")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"📦 下载 Spider2-Lite（SQLite 子集） → {DATA_DIR}\n")

    run(["git", "init"], cwd=DATA_DIR)
    run(["git", "remote", "add", "origin", SPIDER2_REPO], cwd=DATA_DIR)
    run(["git", "config", "core.sparseCheckout", "true"], cwd=DATA_DIR)
    run(["git", "sparse-checkout", "init", "--cone"], cwd=DATA_DIR)
    run(["git", "sparse-checkout", "set"] + SPARSE_PATHS, cwd=DATA_DIR)
    run(["git", "pull", "--depth=1", "origin", "main"], cwd=DATA_DIR)

    # 验证
    jsonl = DATA_DIR / "spider2-lite" / "spider2-lite.jsonl"
    if not jsonl.exists():
        print("❌ 下载失败：spider2-lite.jsonl 不存在", file=sys.stderr)
        sys.exit(1)

    import json
    cases = [json.loads(l) for l in jsonl.read_text().splitlines() if l.strip()]
    sqlite_cases = [c for c in cases if _is_sqlite(c, DATA_DIR)]

    print(f"\n✅ 下载完成")
    print(f"   总用例：{len(cases)}")
    print(f"   SQLite 用例：{len(sqlite_cases)}")
    print(f"   数据目录：{DATA_DIR}")


def _is_sqlite(case: dict, data_dir: Path) -> bool:
    """判断是否为本地 SQLite 用例（有 DDL schema 文件即可）。"""
    db_name = case.get("db", "")
    schema_dir = data_dir / "spider2-lite" / "resource" / "databases" / "sqlite"
    return _find_ddl_dir(db_name, schema_dir) is not None


def _find_ddl_dir(db_name: str, schema_dir: Path) -> Path | None:
    """
    在 schema_dir 下寻找 db_name 对应的 DDL.csv 所在目录（大小写不敏感）。
    Spider2 的 sqlite/ 子目录名与 JSONL 中的 db 字段可能大小写不一致。
    """
    if not schema_dir.exists():
        return None
    db_lower = db_name.lower()
    for d in schema_dir.iterdir():
        if d.is_dir() and d.name.lower() == db_lower:
            if (d / "DDL.csv").exists():
                return d
    return None


def _find_db_path(db_name: str, schema_dir: Path) -> Path | None:
    """
    寻找实际可执行的 .sqlite 文件。
    先找 spider2-localdb/（官方下载目录），再尝试 schema_dir 下的同名文件。
    """
    localdb = schema_dir.parent / "spider2-localdb"
    db_lower = db_name.lower()

    # spider2-localdb/ 下按大小写不敏感搜索
    for search_dir in (localdb, schema_dir):
        if not search_dir.exists():
            continue
        for ext in (".sqlite", ".db", ".sqlite3"):
            # 尝试精确匹配和小写匹配
            for name in (db_name, db_lower):
                p = search_dir / f"{name}{ext}"
                if p.exists():
                    return p
            # 子目录
            for d in search_dir.iterdir():
                if d.is_dir() and d.name.lower() == db_lower:
                    for ext2 in (".sqlite", ".db", ".sqlite3"):
                        for name in (db_name, db_lower, d.name):
                            p = d / f"{name}{ext2}"
                            if p.exists():
                                return p
    return None


if __name__ == "__main__":
    setup()
