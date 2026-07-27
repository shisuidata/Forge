#!/usr/bin/env python3
"""Validate one configured LLM tool call through Forge compilation, without SQL execution."""
from __future__ import annotations

import json
import argparse
import sys
from pathlib import Path

from jsonschema import validate
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from agent import llm  # noqa: E402
from config import cfg  # noqa: E402
from forge.compiler import compile_query  # noqa: E402
from forge.schema_builder import build_tool_schema  # noqa: E402


def _dialect() -> str:
    configured = cfg.SQL_DIALECT.lower()
    if configured != "auto":
        return configured
    if not cfg.DATABASE_URL:
        return "sqlite"
    try:
        backend = make_url(cfg.DATABASE_URL).get_backend_name()
        return "mysql" if backend == "mariadb" else backend
    except Exception:
        return "unknown"


def _result(status: str, **values) -> dict:
    payload = {
        "provider": cfg.LLM_PROVIDER,
        "model": cfg.LLM_MODEL,
        "tool_call": values.get("tool_call", "unknown"),
        "schema": values.get("schema", "unknown"),
        "compile": values.get("compile", "unknown"),
        "dialect": values.get("dialect", _dialect()),
        "status": status,
        "error": values.get("error", ""),
        "sql_preview": values.get("sql_preview", ""),
    }
    return payload


def _emit(payload: dict, out: str | None, pretty: bool = True) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2 if pretty else None)
    print(text)
    if out:
        path = Path(out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", dest="json_output", help="输出结构化 JSON")
    parser.add_argument("--out", default=None, help="将结构化结果写入指定文件")
    args = parser.parse_args(argv)

    if not cfg.LLM_API_KEY:
        _emit(_result("skipped", tool_call="skipped", schema="skipped", compile="skipped",
                      error="LLM_API_KEY 未配置"), args.out)
        return 2

    try:
        registry = json.loads(cfg.REGISTRY_PATH.read_text(encoding="utf-8"))
        tables = registry.get("tables", registry)
        if not tables:
            raise ValueError("Registry 没有可用于 smoke 的表")
        first_table = next(iter(tables))
        info = tables[first_table]
        columns = info.get("columns", info) if isinstance(info, dict) else info
        first_column = next(iter(columns))
        qualified_column = f"{first_table}.{first_column}"

        tool = {
            "name": "generate_forge_query",
            "description": "Generate a minimal Forge query using the registered table and column.",
            "input_schema": build_tool_schema(registry),
        }
        messages = [{
            "role": "user",
            "content": (
                "Call generate_forge_query for a minimal query that selects "
                f"{qualified_column} from {first_table}."
            ),
        }]
        system = "Return a valid Forge query by calling the provided tool."
        if cfg.LLM_PROVIDER == "anthropic":
            result = llm._call_anthropic(messages, system, [tool])
        elif cfg.LLM_PROVIDER == "openai":
            result = llm._call_openai(messages, system, [tool])
        else:
            raise ValueError(f"不支持的 LLM_PROVIDER：{cfg.LLM_PROVIDER}")

        if result.get("tool") != "generate_forge_query":
            raise llm.LLMCompatibilityError("Provider 未调用 generate_forge_query。")
        forge_json = result.get("input")
        validate(instance=forge_json, schema=tool["input_schema"])
        dialect = _dialect()
        sql = compile_query(forge_json, dialect=dialect)
    except Exception as exc:
        _emit(_result("failed", tool_call="failed", schema="failed", compile="failed",
                      error=str(exc)), args.out)
        return 1

    _emit(_result("ok", tool_call="ok", schema="ok", compile="ok", dialect=dialect,
                  sql_preview=sql[:240]), args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
