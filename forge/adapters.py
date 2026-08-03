"""Adapter contracts for database and LLM/provider compatibility work."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class DatabaseCapabilities:
    """Database feature flags that gate commercial support claims."""
    dialect: str
    schema_sync: bool = False
    dry_run: bool = False
    execution: bool = False
    statement_timeout: bool = False
    read_only_role_required: bool = True
    notes: tuple[str, ...] = field(default_factory=tuple)


class DatabaseAdapter(Protocol):
    """Contract for a database or data warehouse integration."""
    name: str
    capabilities: DatabaseCapabilities

    def introspect(self) -> dict:
        """Return a Forge schema registry fragment for the connected database."""

    def compile_dialect(self) -> str:
        """Return the compiler dialect to use for this database."""

    def dry_run(self, sql: str) -> dict:
        """Validate SQL without returning customer data where the platform supports it."""

    def execute(self, sql: str, max_rows: int) -> tuple[str, list[str], list[tuple]]:
        """Execute reviewed read-only SQL with row and timeout guardrails."""


@dataclass(frozen=True)
class LLMProviderCapabilities:
    """Provider feature flags used by smoke tests and compatibility docs."""
    tools: bool = False
    named_tool_choice: bool = False
    json_schema_strict: bool = False
    json_mode: bool = False
    plain_json_fallback: bool = False
    timeout_seconds: float = 120.0
    notes: tuple[str, ...] = field(default_factory=tuple)


class LLMProviderAdapter(Protocol):
    """Contract for model providers used by the Forge agent."""
    name: str
    capabilities: LLMProviderCapabilities

    def call(self, messages: list[dict], system: str, tools: list[dict]) -> dict[str, Any]:
        """Return Forge's normalized LLM response shape."""
