"""
Forge 命令行入口。

子命令：
    forge compile <file.json>   将 Forge JSON 文件编译为 SQL 并打印到 stdout
    forge compile -             从 stdin 读取 Forge JSON 并编译
    forge sync                  连接数据库，将表结构同步到 schema.registry.json
    forge sync --db <url>       用指定的数据库 URL 覆盖 config 中的 DATABASE_URL

典型用法：
    # 编译 JSON 文件
    forge compile query.json

    # 管道调用（配合 LLM 生成的 JSON）
    echo '{"scan":"orders","select":["orders.id"]}' | forge compile -

    # 同步数据库 schema（使用 .env 中的 DATABASE_URL）
    forge sync

    # 指定数据库 URL
    forge sync --db postgresql://user:pass@localhost:5432/mydb
"""

import argparse
import json
import sys

from .compiler import compile_query


def main() -> None:
    """CLI 入口函数，由 pyproject.toml 的 [project.scripts] 注册为 forge 命令。"""
    parser = argparse.ArgumentParser(prog="forge", description="Forge DSL compiler")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ── compile 子命令 ────────────────────────────────────────────────────────
    compile_parser = subparsers.add_parser("compile", help="Compile Forge JSON to SQL")
    compile_parser.add_argument(
        "input",
        help="Forge JSON 文件路径，或 - 表示从 stdin 读取",
    )

    # ── sync 子命令 ───────────────────────────────────────────────────────────
    sync_parser = subparsers.add_parser("sync", help="Sync schema from database")
    sync_parser.add_argument(
        "--db",
        metavar="URL",
        default=None,
        help="数据库 URL，优先级高于 config.DATABASE_URL",
    )

    args = parser.parse_args()

    # ── compile 处理 ─────────────────────────────────────────────────────────
    if args.command == "compile":
        if args.input == "-":
            # 从 stdin 读取，支持管道场景
            forge = json.load(sys.stdin)
        else:
            with open(args.input) as f:
                forge = json.load(f)
        # compile_query 会做 schema 校验，校验失败时抛出 jsonschema.ValidationError
        print(compile_query(forge))

    # ── sync 处理 ────────────────────────────────────────────────────────────
    elif args.command == "sync":
        # 延迟导入，避免在仅使用 compile 子命令时引入数据库依赖
        from config import cfg
        from registry.sync import run_sync

        # --db 参数优先；未提供则回退到 config.DATABASE_URL
        database_url = args.db or cfg.DATABASE_URL
        if not database_url:
            print(
                "错误：未提供数据库 URL。请设置 DATABASE_URL 环境变量或使用 --db 参数。",
                file=sys.stderr,
            )
            sys.exit(1)

        registry = run_sync(database_url, cfg.REGISTRY_PATH)
        table_count = len(registry.get("tables", {}))
        print(f"已同步 {table_count} 张表 → {cfg.REGISTRY_PATH}")


if __name__ == "__main__":
    main()
