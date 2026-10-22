"""Deployment readiness checks for Forge installations."""
from __future__ import annotations

import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path
from typing import Literal

from config import cfg

ReadinessProfile = Literal["dev", "poc", "prod"]


def _status_for_profile(name: str, base_status: str, profile: ReadinessProfile) -> str:
    """Adjust a base readiness status for the selected delivery profile."""
    if base_status == "ok":
        return "ok"
    if name in {"auth", "governance_store", "identity_bindings", "pi_control_auth", "legacy_execution_closed"}:
        return "fail"
    if profile == "prod":
        prod_fail = {
            "auth",
            "database",
            "database_readonly",
            "llm",
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


def _governance_status() -> tuple[bool, bool, bool]:
    """Probe the authority read-only; absent or unmigrated stores are not ready."""
    path = Path(cfg.QUERY_RUN_DB_PATH).expanduser().resolve()
    if not path.is_file():
        return False, False, False
    try:
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
            for table in ("tenant_teams", "tenant_users", "team_table_acl",
                          "governance_credentials", "governance_channel_bindings",
                          "governance_events", "issued_mandates"):
                if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
                    return False, False, False
            active_admin = conn.execute(
                "SELECT 1 FROM tenant_users u JOIN tenant_teams t ON t.team_id=u.team_id "
                "WHERE u.principal_type='human' AND u.role='admin' AND u.status='active' "
                "AND u.user_id=? LIMIT 1", (cfg.AUTH_ADMIN_PRINCIPAL_ID,),
            ).fetchone()
            active_agent_key = conn.execute(
                "SELECT 1 FROM governance_credentials c JOIN tenant_users u ON u.user_id=c.principal_id "
                "WHERE c.kind='api_key' AND c.status='active' AND u.status='active' "
                "AND u.principal_type IN ('agent','service') AND (c.expires_at IS NULL OR "
                "c.expires_at > strftime('%Y-%m-%dT%H:%M:%f+00:00','now')) LIMIT 1"
            ).fetchone()
            active_key = conn.execute(
                "SELECT 1 FROM governance_credentials c JOIN tenant_users u ON u.user_id=c.principal_id "
                "WHERE c.kind='api_key' AND c.status='active' AND u.status='active' "
                "AND u.user_id=? AND (c.expires_at IS NULL OR "
                "c.expires_at > strftime('%Y-%m-%dT%H:%M:%f+00:00','now')) LIMIT 1",
                (cfg.AUTH_ADMIN_PRINCIPAL_ID,),
            ).fetchone()
            pi_key = conn.execute(
                "SELECT 1 FROM governance_credentials c JOIN tenant_users u ON u.user_id=c.principal_id "
                "WHERE c.kind='pi_service' AND c.status='active' AND u.status='active' "
                "AND u.principal_type='service' AND u.user_id=? "
                "AND (c.expires_at IS NULL OR c.expires_at > strftime('%Y-%m-%dT%H:%M:%f+00:00','now')) LIMIT 1",
                (cfg.PI_EXECUTION_PRINCIPAL_ID,),
            ).fetchone()
            bootstrapped = conn.execute(
                "SELECT 1 FROM governance_events WHERE event_type='bootstrap' LIMIT 1"
            ).fetchone()
            broken_bindings = conn.execute(
                "SELECT 1 FROM governance_channel_bindings b LEFT JOIN tenant_users u "
                "ON u.user_id=b.principal_id WHERE b.status='active' "
                "AND (u.user_id IS NULL OR u.status!='active') LIMIT 1"
            ).fetchone()
            return bool(bootstrapped), bool(active_admin and active_agent_key and not broken_bindings
                                           and (active_key or cfg.AUTH_ADMIN_PASSWORD and cfg.AUTH_ADMIN_PASSWORD != "123456")), bool(pi_key)
    except (OSError, sqlite3.Error):
        return False, False, False


def readiness_checks(profile: ReadinessProfile = "prod") -> list[dict]:
    """Return readiness checks adjusted for dev, poc, or prod profile."""
    if profile not in {"dev", "poc", "prod"}:
        raise ValueError("profile must be one of: dev, poc, prod")

    checks: list[dict] = []

    def add(name: str, base_status: str, message: str) -> None:
        checks.append(_check(name, base_status, message, profile))

    if not cfg.AUTH_ENABLED:
        add("auth", "fail", "认证未开启；治理执行必须验证主体身份。")
    elif cfg.AUTH_ADMIN_PASSWORD == "123456":
        add("auth", "fail", "默认管理员密码仍可用；必须禁用或替换。")
    else:
        add("auth", "ok", "认证已开启；每次执行仍需核验活跃身份和凭证。")

    store_ok, identities_ok, pi_key_ok = _governance_status()
    add("governance_store", "ok" if store_ok else "fail",
        "治理库结构可读。" if store_ok else "治理库不存在、不可读或尚未迁移；不可授予执行权。")
    add("identity_bindings", "ok" if identities_ok else "fail",
        "管理员及执行主体已绑定活跃身份与凭证。" if identities_ok else "管理员/执行主体身份或凭证缺失，或存在失效渠道绑定。")
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        add("pi_control_auth", "ok", "Pi 未启用；外部 Agent 无需 Pi 控制凭证。")
    else:
        pi_ready = bool(cfg.PI_CHANNEL_SERVICE_KEY and cfg.PI_EXECUTION_PRINCIPAL_ID and pi_key_ok)
        add("pi_control_auth", "ok" if pi_ready else "fail",
            "Pi service 凭证及控制通道密钥已配置；仍需运行时双向认证。" if pi_ready else
            "Pi 已启用，但缺少控制密钥或活跃的 Pi service 主体凭证。")
    legacy_closed = not cfg.LEGACY_AGENT_API_ENABLED and not cfg.RAW_SQL_ENABLED
    add("legacy_execution_closed", "ok" if legacy_closed else "fail",
        "旧执行开关已关闭。" if legacy_closed else "旧 Agent/裸 SQL 执行开关仍开启；不得作为治理部署入口。")

    if cfg.EXECUTION_ENABLED and not cfg.DATABASE_URL:
        add("database", "fail", "SQL 执行已开启，但未配置 DATABASE_URL。")
    elif cfg.DATABASE_URL:
        add("database", "ok", "已配置 DATABASE_URL。")
    else:
        add("database", "warn", "未配置数据库连接，仅可生成 SQL。")

    if cfg.EXECUTION_ENABLED and cfg.DATABASE_URL and not cfg.DATABASE_READONLY_CONFIRMED:
        add("database_readonly", "fail", "SQL 执行已开启，但尚未声明数据库只读凭证；该声明不构成独立权限验证。")
    elif cfg.EXECUTION_ENABLED and cfg.DATABASE_URL:
        add("database_readonly", "ok", "已声明数据库只读凭证；尚未独立验证数据库权限。")
    else:
        add("database_readonly", "warn", "SQL 执行未启用，暂不检查数据库只读账号。")

    if not cfg.PI_ORCHESTRATOR_ENABLED:
        add("llm", "ok", "Pi 生成未启用；外部 Agent 的直接候选无需 LLM。")
    elif not cfg.LLM_API_KEY:
        add("llm", "fail", "Pi 生成已启用，但未配置 LLM_API_KEY。")
    else:
        add("llm", "ok", f"已配置 {cfg.LLM_PROVIDER}/{cfg.LLM_MODEL or '(default)'}。")

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
