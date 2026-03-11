"""
LLM client — supports Anthropic and any OpenAI-compatible endpoint.
Generates Forge JSON via tool_use / structured output.
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Any

from config import cfg

# ── load Forge JSON Schema once ───────────────────────────────────────────────
_SCHEMA = json.loads((Path(__file__).parent.parent / "forge" / "schema.json").read_text())

# ── system prompt ─────────────────────────────────────────────────────────────
_SYSTEM = """You are Forge, an AI data assistant. You translate natural language into Forge JSON queries.

Rules:
- Only reference tables and columns that exist in the provided schema registry
- JOIN type is always required (inner/left/right/full/anti/semi), never omit it
- Use "anti" join instead of NOT IN
- Row filters go in "filter" (→ WHERE), aggregate filters go in "having" (→ HAVING)
- count_all has no "col"; all other agg functions require "col"
- Always use table.column format when joins are present
- Sort direction is required (asc/desc)
- Dates use {"$date": "YYYY-MM-DD"} format

When the user asks a data question, call the generate_forge_query tool.
When the user defines a business metric, call the define_metric tool.
For anything else (greetings, clarifications), reply in plain text without calling tools."""


def _registry_context() -> str:
    """Load schema registry and format as context string."""
    try:
        registry = json.loads(cfg.REGISTRY_PATH.read_text())
    except Exception:
        return "No schema registry found."

    lines = ["Available tables and columns:"]
    tables = registry.get("tables", registry)  # support both formats
    for table, cols in tables.items():
        if isinstance(cols, list):
            lines.append(f"  {table}: {', '.join(cols)}")
        elif isinstance(cols, dict):
            col_names = cols.get("columns", [])
            lines.append(f"  {table}: {', '.join(col_names)}")

    if "metrics" in registry:
        lines.append("\nPre-defined metrics (reference by name):")
        for name, meta in registry["metrics"].items():
            desc = meta.get("description", "")
            lines.append(f"  {name}: {desc}")

    return "\n".join(lines)


# ── tools definition ──────────────────────────────────────────────────────────
_TOOLS = [
    {
        "name": "generate_forge_query",
        "description": "Generate a Forge JSON query from natural language.",
        "input_schema": _SCHEMA,
    },
    {
        "name": "define_metric",
        "description": "Extract a business metric definition from natural language for saving to registry.",
        "input_schema": {
            "type": "object",
            "required": ["name", "description"],
            "properties": {
                "name":        {"type": "string", "description": "Metric identifier (snake_case)"},
                "description": {"type": "string", "description": "Plain-language definition"},
            },
        },
    },
]


# ── Anthropic ─────────────────────────────────────────────────────────────────

def _call_anthropic(messages: list[dict]) -> dict:
    import anthropic
    client = anthropic.Anthropic(api_key=cfg.LLM_API_KEY)
    response = client.messages.create(
        model=cfg.LLM_MODEL,
        max_tokens=2048,
        system=f"{_SYSTEM}\n\n{_registry_context()}",
        tools=_TOOLS,
        messages=messages,
    )
    for block in response.content:
        if block.type == "tool_use":
            return {"tool": block.name, "input": block.input}
    # plain text response
    text = next((b.text for b in response.content if hasattr(b, "text")), "")
    return {"tool": None, "text": text}


# ── OpenAI-compatible ─────────────────────────────────────────────────────────

def _call_openai(messages: list[dict]) -> dict:
    import httpx, json as _json

    base_url = cfg.LLM_BASE_URL or "https://api.openai.com/v1"
    headers = {
        "Authorization": f"Bearer {cfg.LLM_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": cfg.LLM_MODEL,
        "messages": [{"role": "system", "content": f"{_SYSTEM}\n\n{_registry_context()}"}] + messages,
        "tools": [{"type": "function", "function": {
            "name": t["name"],
            "description": t["description"],
            "parameters": t["input_schema"],
        }} for t in _TOOLS],
        "tool_choice": "auto",
    }
    r = httpx.post(f"{base_url}/chat/completions", headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    choice = r.json()["choices"][0]["message"]
    if choice.get("tool_calls"):
        tc = choice["tool_calls"][0]
        return {"tool": tc["function"]["name"], "input": _json.loads(tc["function"]["arguments"])}
    return {"tool": None, "text": choice.get("content", "")}


# ── public API ────────────────────────────────────────────────────────────────

def call(history: list[Any]) -> dict:
    """
    Call LLM with conversation history.
    Returns {"tool": str, "input": dict} or {"tool": None, "text": str}
    """
    messages = [{"role": m.role, "content": m.content} for m in history]
    if cfg.LLM_PROVIDER == "anthropic":
        return _call_anthropic(messages)
    else:
        return _call_openai(messages)
