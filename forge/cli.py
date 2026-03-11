"""
forge compile <file.json>   — compile Forge JSON to SQL
forge compile -             — read from stdin
forge sync                  — sync schema from DB
forge sync --db <url>       — override DATABASE_URL
"""

import argparse
import json
import sys

from .compiler import compile_query


def main() -> None:
    parser = argparse.ArgumentParser(prog="forge", description="Forge DSL compiler")
    subparsers = parser.add_subparsers(dest="command", required=True)

    compile_parser = subparsers.add_parser("compile", help="Compile Forge JSON to SQL")
    compile_parser.add_argument("input", help="Forge JSON file, or - for stdin")

    sync_parser = subparsers.add_parser("sync", help="Sync schema from database")
    sync_parser.add_argument(
        "--db",
        metavar="URL",
        default=None,
        help="Database URL (overrides DATABASE_URL from config)",
    )

    args = parser.parse_args()

    if args.command == "compile":
        if args.input == "-":
            forge = json.load(sys.stdin)
        else:
            with open(args.input) as f:
                forge = json.load(f)
        print(compile_query(forge))

    elif args.command == "sync":
        from config import cfg
        from .sync import run_sync

        database_url = args.db or cfg.DATABASE_URL
        if not database_url:
            print("Error: no database URL provided. Set DATABASE_URL or use --db.", file=sys.stderr)
            sys.exit(1)

        registry = run_sync(database_url, cfg.REGISTRY_PATH)
        table_count = len(registry.get("tables", {}))
        print(f"Synced {table_count} table(s) to {cfg.REGISTRY_PATH}")


if __name__ == "__main__":
    main()
