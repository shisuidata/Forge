"""Deployment readiness checks for Forge installations."""
from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Literal

from config import cfg

ReadinessProfile = Literal["dev", "poc", "prod"]


def _status_for_profile(name: str, base_status: str, profile: ReadinessProfile) -> str:
    """Adjust a base readiness status for the selected delivery profile."""
    if base_status == "ok":
        return "ok"
    if profile == "prod":
        prod_fail = {
            "auth",
            "database",
            "database_readonly",
            "llm",
            "raw_sql",
            "query_timeout",
            "secure_cookie",
            "registry",
            "audit",
        }
        return "fail" if name in prod_fail else base_status
    if profile == "poc":
        poc_fail = {"database", "database_readonly", "llm", "query_timeout", "registry", "audit"}
        return "fail" if name in poc_fail else "warn"
    if profile == "dev":
        return "fail" if name == "audit" and base_status == "fail" else "warn"
    raise ValueError(f"unknown readiness profile: {profile}")


def _check(name: str, base_status: str, message: str, profile: ReadinessProfile) -> dict:
    return {
        "name": name,
        "status": _status_for_profile(name, base_status, profile),
        "message": message,
    }


def _uses_test_registry(path: Path) -> bool:
    """Return True when a Registry path points at bundled benchmark data."""
    normalized = path.expanduser().as_posix()
    return normalized.startswith("tests/datasets/") or "/tests/datasets/" in normalized


def readiness_checks(profile: ReadinessProfile = "prod") -> list[dict]:
    """Return readiness checks adjusted for dev, poc, or prod profile."""
    if profile not in {"dev", "poc", "prod"}:
        raise ValueError("profile must be one of: dev, poc, prod")

    checks: list[dict] = []

    def add(name: str, base_status: str, message: str) -> None:
        checks.append(_check(name, base_status, message, profile))

    if not cfg.AUTH_ENABLED:
        add("auth", "warn", "认证未开启；生产环境应开启 Web/API 认证。")
    elif not cfg.AUTH_ADMIN_PASSWORD or cfg.AUTH_ADMIN_PASSWORD == "123456":
        add("auth", "fail", "认证已开启，但管理员密码为空或仍为默认值。")
    else:
        add("auth", "ok", "认证已开启。")

    if cfg.EXECUTION_ENABLED and not cfg.DATABASE_URL:
        add("database", "fail", "SQL 执行已开启，但未配置 DATABASE_URL。")
    elif cfg.DATABASE_URL:
        add("database", "ok", "已配置 DATABASE_URL。")
    else:
        add("database", "warn", "未配置数据库连接，仅可生成 SQL。")

    if cfg.EXECUTION_ENABLED and cfg.DATABASE_URL and not cfg.DATABASE_READONLY_CONFIRMED:
        add("database_readonly", "fail", "SQL 执行已开启，但尚未确认 DATABASE_URL 使用数据库只读账号。")
    elif cfg.EXECUTION_ENABLED and cfg.DATABASE_URL:
        add("database_readonly", "ok", "已确认数据库连接使用只读账号。")
    else:
        add("database_readonly", "warn", "SQL 执行未启用，暂不检查数据库只读账号。")

    if not cfg.LLM_API_KEY:
        add("llm", "fail", "未配置 LLM_API_KEY。")
    else:
        add("llm", "ok", f"已配置 {cfg.LLM_PROVIDER}/{cfg.LLM_MODEL or '(default)'}。")

    if cfg.RAW_SQL_ENABLED:
        add("raw_sql", "warn", "手动 SQL 执行入口已开启；生产环境建议只给受信任管理员使用。")
    else:
        add("raw_sql", "ok", "手动 SQL 执行入口已关闭。")

    if cfg.EXECUTION_MAX_ROWS > 1000:
        add("row_cap", "warn", f"最大返回行数为 {cfg.EXECUTION_MAX_ROWS}，生产环境建议控制在 1000 行以内。")
    else:
        add("row_cap", "ok", f"最大返回行数为 {cfg.EXECUTION_MAX_ROWS}。")

    if cfg.EXECUTION_ENABLED and cfg.EXECUTION_TIMEOUT_SECONDS <= 0:
        add("query_timeout", "fail", "SQL 执行已开启，但未配置查询超时。")
    elif cfg.EXECUTION_TIMEOUT_SECONDS > 120:
        add("query_timeout", "warn", f"查询超时为 {cfg.EXECUTION_TIMEOUT_SECONDS}s，生产环境建议不超过 120s。")
    else:
        add("query_timeout", "ok", f"查询超时为 {cfg.EXECUTION_TIMEOUT_SECONDS}s。")

    if cfg.AUTH_ENABLED and not cfg.AUTH_COOKIE_SECURE:
        add("secure_cookie", "warn", "认证已开启，但 Secure Cookie 未开启；HTTPS 部署建议开启。")
    elif cfg.AUTH_ENABLED:
        add("secure_cookie", "ok", "Secure Cookie 已开启。")
    else:
        add("secure_cookie", "warn", "认证未开启，暂不检查 Secure Cookie。")

    registry_missing = [
        str(path)
        for path in (
            cfg.REGISTRY_PATH,
            cfg.METRICS_PATH,
            cfg.DISAMBIGUATIONS_PATH,
            cfg.CONVENTIONS_PATH,
        )
        if not Path(path).exists()
    ]
    registry_paths = (
        cfg.REGISTRY_PATH,
        cfg.METRICS_PATH,
        cfg.DISAMBIGUATIONS_PATH,
        cfg.CONVENTIONS_PATH,
    )
    if registry_missing:
        add("registry", "fail", "部分 Registry 文件不存在：" + ", ".join(registry_missing))
    elif any(_uses_test_registry(Path(path)) for path in registry_paths):
        add(
            "registry",
            "fail" if profile in {"poc", "prod"} else "warn",
            "当前 Registry 指向 tests/datasets benchmark 数据；PoC/生产交付必须显式配置客户 Registry 或 registry/data。",
        )
    else:
        add("registry", "ok", "Registry 文件齐备。")

    audit_path = Path(cfg.AUDIT_DB_PATH).expanduser()
    audit_dir = audit_path.parent if str(audit_path.parent) else Path(".")
    try:
        audit_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=audit_dir, prefix=".forge-audit-check-", delete=True):
            pass
        add("audit", "ok", f"审计目录可写：{audit_dir}")
    except OSError as exc:
        add("audit", "fail", f"审计目录不可写：{exc}")

    return checks


def readiness_payload(profile: ReadinessProfile = "prod") -> dict:
    """Return a stable JSON payload for readiness consumers."""
    checks = readiness_checks(profile)
    failed = [c for c in checks if c["status"] == "fail"]
    warned = [c for c in checks if c["status"] == "warn"]
    overall = "fail" if failed else ("warn" if warned else "ok")
    return {"profile": profile, "status": overall, "checks": checks}
