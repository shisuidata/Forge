"""
forge sync — introspect a database and write the structural layer of schema.registry.json.
"""
from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import create_engine, inspect


def _introspect(database_url: str) -> dict[str, list[str]]:
    engine = create_engine(database_url)
    inspector = inspect(engine)
    result: dict[str, list[str]] = {}
    for table_name in inspector.get_table_names():
        columns = [col["name"] for col in inspector.get_columns(table_name)]
        result[table_name] = columns
    engine.dispose()
    return result


def run_sync(database_url: str, registry_path: Path) -> dict:
    existing: dict = {}
    if registry_path.exists():
        try:
            existing = json.loads(registry_path.read_text())
        except (json.JSONDecodeError, OSError):
            existing = {}

    tables_data = _introspect(database_url)

    tables_section = {
        name: {"columns": cols} for name, cols in tables_data.items()
    }

    registry: dict = {"tables": tables_section}
    if "metrics" in existing:
        registry["metrics"] = existing["metrics"]

    registry_path.write_text(json.dumps(registry, indent=2))
    return registry
