"""Bounded, read-only Registry and organizational knowledge retrieval for Pi."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

import yaml

from config import cfg

_MAX_CONTENT = 1200
_MAX_DOCUMENTS = 2000


def _load(path: Path) -> Any:
    try:
        text = path.read_text(encoding="utf-8")
        if path.suffix.lower() == ".json":
            return json.loads(text)
        return yaml.safe_load(text) or {}
    except (OSError, ValueError, yaml.YAMLError):
        return {}


def _bounded_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)[:_MAX_CONTENT]


def _registry_documents() -> list[dict[str, str]]:
    documents: list[dict[str, str]] = []
    schema = _load(cfg.REGISTRY_PATH)
    tables = schema.get("tables", {}) if isinstance(schema, dict) else {}
    if isinstance(tables, dict):
        for table_name, table in tables.items():
            if not isinstance(table_name, str) or not isinstance(table, dict):
                continue
            columns = table.get("columns", {})
            column_text = []
            if isinstance(columns, dict):
                for name, definition in columns.items():
                    description = definition.get("description", "") if isinstance(definition, dict) else ""
                    column_text.append(f"{name}: {description}")
            content = (
                f"表 {table_name}\n描述: {table.get('description', '')}\n字段:\n"
                + "\n".join(column_text)
            )[:_MAX_CONTENT]
            documents.append({"source_type": "schema", "title": table_name, "content": content})

    for source_type, path in (
        ("metric", cfg.METRICS_PATH),
        ("disambiguation", cfg.DISAMBIGUATIONS_PATH),
        ("convention", cfg.CONVENTIONS_PATH),
        ("business_context", cfg.BUSINESS_CONTEXT_PATH),
    ):
        data = _load(path)
        if not isinstance(data, dict):
            continue
        for key, value in data.items():
            if not isinstance(key, str):
                continue
            label = value.get("label") if isinstance(value, dict) else None
            title = f"{key} · {label}" if isinstance(label, str) and label else key
            documents.append({
                "source_type": source_type,
                "title": title,
                "content": _bounded_json(value),
            })
    return documents[:_MAX_DOCUMENTS]


def _memory_documents(user_id: str, team_id: str) -> list[dict[str, str]]:
    try:
        from agent.memory import memory
        items = memory.smp.query(user_id=user_id, team_id=team_id, limit=50)
    except Exception:
        return []
    documents = []
    for item in items:
        documents.append({
            "source_type": "semantic_memory",
            "title": f"{item.get('category', '')}:{item.get('key', '')}",
            "content": _bounded_json(item.get("value")),
        })
    return documents


def _tokens(text: str) -> set[str]:
    normalized = text.lower()
    tokens = set(re.findall(r"[a-z0-9_]{2,}|[\u4e00-\u9fff]{2,}", normalized))
    chinese = "".join(re.findall(r"[\u4e00-\u9fff]", normalized))
    tokens.update(chinese[index:index + 2] for index in range(max(0, len(chinese) - 1)))
    return {token for token in tokens if token}


def _score(question: str, document: dict[str, str]) -> int:
    query_tokens = _tokens(question)
    if not query_tokens:
        return 0
    title = document["title"].lower()
    content = document["content"].lower()
    score = sum(4 for token in query_tokens if token in title)
    score += sum(1 for token in query_tokens if token in content)
    compact_question = re.sub(r"\s+", "", question.lower())
    if len(compact_question) >= 2 and compact_question in (title + content).replace(" ", ""):
        score += 8
    return score


def search_context(
    *,
    question: str,
    user_id: str,
    team_id: str,
    limit: int = 8,
) -> dict[str, Any]:
    bounded_limit = max(1, min(limit, 12))
    documents = _registry_documents() + _memory_documents(user_id, team_id)
    ranked = sorted(
        ((score, index, document) for index, document in enumerate(documents)
         if (score := _score(question, document)) > 0),
        key=lambda item: (-item[0], item[1]),
    )[:bounded_limit]
    evidence = []
    for score, _, document in ranked:
        digest = hashlib.sha256(
            f"{document['source_type']}\0{document['title']}\0{document['content']}".encode()
        ).hexdigest()
        evidence.append({
            "evidence_ref": f"ctx_{digest[:24]}",
            "source_type": document["source_type"],
            "title": document["title"][:200],
            "content": document["content"][:_MAX_CONTENT],
            "score": score,
        })
    revision = hashlib.sha256(
        "\n".join(item["evidence_ref"] for item in evidence).encode()
    ).hexdigest()
    return {
        "status": "ok",
        "question": question,
        "evidence": evidence,
        "evidence_count": len(evidence),
        "context_revision": f"sha256:{revision}",
        "bounded": True,
    }
