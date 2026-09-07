"""Pure SQL target selection; callers explicitly supply trusted datasource bindings."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from urllib.parse import urlsplit

SUPPORTED_DIALECTS = frozenset({"sqlite", "postgresql", "mysql", "bigquery", "snowflake"})


@dataclass(frozen=True)
class DialectResolution:
    requested: str
    resolved: str
    provenance: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


class DialectResolutionError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def resolve_dialect(
    requested: str | None = None,
    *,
    configured_dialect: str | None = None,
    database_url: str | None = None,
    allow_generic: bool = False,
) -> DialectResolution:
    """Resolve an explicit target or a trusted binding, never inspect configuration.

    Unbound Direct SQL may use SQLGlot's generic parser (``auto``). Compilation
    and execution require a concrete target; an absent binding is not SQLite.
    The returned provenance contains no URL, credentials or datasource details.
    """
    requested = (requested or "auto").lower()
    if requested != "auto":
        if requested not in SUPPORTED_DIALECTS:
            raise DialectResolutionError("dialect_unsupported")
        return DialectResolution(requested, requested, "explicit_request")
    configured = (configured_dialect or "auto").lower()
    if configured != "auto":
        if configured not in SUPPORTED_DIALECTS:
            raise DialectResolutionError("dialect_unsupported")
        return DialectResolution(requested, configured, "configured_dialect")
    if database_url:
        scheme = urlsplit(database_url).scheme.lower().split("+", 1)[0]
        resolved = "postgresql" if scheme == "postgres" else scheme
        if resolved not in SUPPORTED_DIALECTS:
            raise DialectResolutionError("dialect_unsupported")
        return DialectResolution(requested, resolved, "datasource_scheme")
    if allow_generic:
        return DialectResolution(requested, "auto", "generic_sql_parser")
    raise DialectResolutionError("dialect_required")
