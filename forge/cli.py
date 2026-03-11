"""
forge compile <file.json>   — compile Forge JSON to SQL
forge compile -             — read from stdin
"""

import argparse
import json
import sys

from .compiler import compile_query


def main() -> None:
    parser = argparse.ArgumentParser(prog="forge", description="Forge DSL compiler")
    parser.add_argument("input", help="Forge JSON file, or - for stdin")
    args = parser.parse_args()

    if args.input == "-":
        forge = json.load(sys.stdin)
    else:
        with open(args.input) as f:
            forge = json.load(f)

    print(compile_query(forge))


if __name__ == "__main__":
    main()
