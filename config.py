"""
Forge 全局配置模块。

优先级（高 → 低）：
    1. 环境变量 / .env 文件      ← 适合生产/CI，覆盖敏感值
    2. forge.yaml               ← 推荐日常开发配置入口
    3. 硬编码默认值

使用方式：
    from config import cfg
    cfg.LLM_MODEL       # 读取模型 ID
    cfg.FEISHU_APP_ID   # 读取飞书 App ID
    cfg.REGISTRY_PATH   # 注册表路径（Path 对象）
"""
from __future__ import annotations
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# 优先加载 .env（环境变量级别最高）；受限 Channel Runtime 明确禁用整份 .env 注入。
if os.getenv("FORGE_DISABLE_DOTENV") != "true":
    load_dotenv()


def _load_yaml_cfg() -> dict:
    """读取 forge.yaml，返回嵌套 dict；文件不存在或解析失败时返回空 dict。"""
    try:
        import yaml
        p = Path(__file__).parent / "forge.yaml"
        if p.exists():
            return yaml.safe_load(p.read_text()) or {}
    except (ImportError, yaml.YAMLError, OSError) as exc:
        logger.warning("Failed to load forge.yaml: %s", exc)
    return {}


_yaml = _load_yaml_cfg()


def _y(*keys: str, default: str = "") -> str:
    """从 forge.yaml 按路径取值，任一键不存在则返回 default。"""
    node = _yaml
    for k in keys:
        if not isinstance(node, dict):
            return default
        node = node.get(k, {})
    return str(node) if node not in ({}, None, "") else default


def _env(var: str, *yaml_keys: str, default: str = "") -> str:
    """env var → forge.yaml → default，三级降级。"""
    return os.getenv(var) or _y(*yaml_keys, default=default)


class Config:
    # ── 飞书机器人 ─────────────────────────────────────────────────────────────
    FEISHU_APP_ID:             str = _env("FEISHU_APP_ID",             "feishu", "app_id")
    FEISHU_APP_SECRET:         str = _env("FEISHU_APP_SECRET",         "feishu", "app_secret")
    FEISHU_VERIFICATION_TOKEN: str = _env("FEISHU_VERIFICATION_TOKEN", "feishu", "verification_token")
    FEISHU_ENCRYPT_KEY:        str = _env("FEISHU_ENCRYPT_KEY",        "feishu", "encrypt_key")
    FEISHU_PI_ENABLED: bool = _env(
        "FEISHU_PI_ENABLED", "feishu", "pi_enabled", default="false"
    ).lower() == "true"
    PI_CHANNEL_SERVICE_KEY: str = _env(
        "PI_CHANNEL_SERVICE_KEY", "pi_orchestrator", "channel_service_key"
    )

    # ── LLM ───────────────────────────────────────────────────────────────────
    # 优先读 LLM_* 通用变量，若未设置则自动 fallback 到 MINIMAX_* 变量
    LLM_PROVIDER:  str = _env("LLM_PROVIDER",  "llm", "provider",  default="anthropic")
    LLM_API_KEY:   str = (
        _env("LLM_API_KEY",  "llm", "api_key")
        or os.getenv("MINIMAX_API_KEY", "")
    )
    LLM_MODEL:     str = (
        _env("LLM_MODEL",    "llm", "model",    default="")
        or os.getenv("MINIMAX_MODEL", "claude-sonnet-4-6")
    )
    LLM_BASE_URL:  str = (
        _env("LLM_BASE_URL", "llm", "base_url")
        or os.getenv("MINIMAX_BASE_URL", "")
    )
    LLM_TOOL_CHOICE: str = _env(
        "LLM_TOOL_CHOICE", "llm", "tool_choice", default="auto"
    ).lower()
    LLM_TIMEOUT_SECONDS: float = float(
        _env("LLM_TIMEOUT_SECONDS", "llm", "timeout_seconds", default="120")
    )
    LLM_MAX_OUTPUT_TOKENS: int = int(
        _env("LLM_MAX_OUTPUT_TOKENS", "llm", "max_output_tokens", default="8192")
    )
    LLM_TEMPERATURE: float = float(
        _env("LLM_TEMPERATURE", "llm", "temperature", default="0")
    )
    QUERY_PREPARE_TIMEOUT_SECONDS: float = float(
        _env("QUERY_PREPARE_TIMEOUT_SECONDS", "llm", "prepare_timeout_seconds", default="210")
    )

    # ── Embedding ─────────────────────────────────────────────────────────────
    # Embedding credentials must be explicit. Reusing an unrelated LLM key with
    # the default embedding endpoint makes retriever initialization fail and
    # silently expands the prompt to the full Registry.
    EMBED_API_KEY:   str = _env("EMBED_API_KEY", "embedding", "api_key")
    EMBED_BASE_URL:  str = _env("EMBED_BASE_URL",  "embedding", "base_url",  default="https://api.siliconflow.cn/v1")
    EMBED_MODEL:     str = _env("EMBED_MODEL",     "embedding", "model",     default="BAAI/bge-m3")
    RETRIEVAL_TOP_K: int = int(_env("RETRIEVAL_TOP_K", "embedding", "top_k", default="5"))

    # ── 数据库 ─────────────────────────────────────────────────────────────────
    DATABASE_URL: str = _env("DATABASE_URL", "database", "url")
    SQL_DIALECT:  str = _env("SQL_DIALECT",  "database", "dialect", default="auto")
    # auto = 从 DATABASE_URL 自动推断；可手动指定 sqlite/mysql/postgresql/bigquery/snowflake

    # ── SQL 执行安全 ───────────────────────────────────────────────────────────
    EXECUTION_ENABLED: bool = _env("EXECUTION_ENABLED", "execution", "enabled", default="true").lower() == "true"
    RAW_SQL_ENABLED: bool = _env("RAW_SQL_ENABLED", "execution", "raw_sql_enabled", default="true").lower() == "true"
    EXECUTION_MAX_ROWS: int = int(_env("EXECUTION_MAX_ROWS", "execution", "max_rows", default="200"))
    EXECUTION_DISPLAY_ROWS: int = int(_env("EXECUTION_DISPLAY_ROWS", "execution", "display_rows", default="50"))
    EXECUTION_TIMEOUT_SECONDS: int = int(_env("EXECUTION_TIMEOUT_SECONDS", "execution", "timeout_seconds", default="30"))
    DATABASE_READONLY_CONFIRMED: bool = _env(
        "DATABASE_READONLY_CONFIRMED",
        "execution",
        "database_readonly_confirmed",
        default="false",
    ).lower() == "true"

    # ── 反馈机制（语义库自动维护）──────────────────────────────────────────────
    FEEDBACK_ENABLED: bool = _env("FEEDBACK_ENABLED", "feedback", "enabled", default="true").lower() == "true"
    # true = 开启查询缓存 + 歧义澄清 → staging → 自动合并语义库
    # false = 纯查询模式，不记录反馈，不维护语义库

    # ── Registry 路径 ──────────────────────────────────────────────────────────
    # 默认指向 large 测试数据集；可通过 forge.yaml 的 registry.* 或环境变量覆盖
    REGISTRY_PATH:         Path = Path(_env("REGISTRY_PATH",         "registry", "schema_path",          default="tests/datasets/large/schema.registry.json"))
    REGISTRY_STUDIO_DB_PATH: Path = Path(
        _env("REGISTRY_STUDIO_DB_PATH", "registry", "studio_db_path", default=".forge/registry_studio.db")
    )
    METRICS_PATH:          Path = Path(_env("METRICS_PATH",          "registry", "metrics_path",         default="tests/datasets/large/metrics.registry.yaml"))
    DISAMBIGUATIONS_PATH:  Path = Path(_env("DISAMBIGUATIONS_PATH",  "registry", "disambiguations_path", default="tests/datasets/large/disambiguations.registry.yaml"))
    CONVENTIONS_PATH:      Path = Path(_env("CONVENTIONS_PATH",      "registry", "conventions_path",     default="tests/datasets/large/field_conventions.registry.yaml"))

    # ── Staging 目录（用户确认后的歧义消除暂存区）──────────────────────────────
    STAGING_DIR: Path = Path(_env("STAGING_DIR", "staging", "dir", default=".forge/staging"))

    # ── 业务上下文 ─────────────────────────────────────────────────────────────
    BUSINESS_CONTEXT_PATH: Path = Path(_env("BUSINESS_CONTEXT_PATH", "registry", "business_context_path", default="registry/business_context.yaml"))

    # ── 记忆系统 ─────────────────────────────────────────────────────────────────
    # db_url 支持 SQLAlchemy 格式：sqlite / postgresql / mysql
    # 默认 SQLite；生产环境建议 PostgreSQL
    MEMORY_DB_URL:        str = _env("MEMORY_DB_URL",        "memory", "db_url",          default="")
    MEMORY_DB_PATH:       str = _env("MEMORY_DB_PATH",       "memory", "db_path",         default=".forge/memory.db")  # 仅 SQLite 模式
    MEMORY_RETENTION_DAYS: int = int(_env("MEMORY_RETENTION_DAYS", "memory", "ems_retention_days", default="0"))
    MEMORY_SESSION_TIMEOUT: int = int(_env("MEMORY_SESSION_TIMEOUT", "memory", "session_timeout_min", default="30"))
    MEMORY_EXTRACT_MODEL: str = _env("MEMORY_EXTRACT_MODEL", "memory", "extract_model", default="")

    # ── 日志 ─────────────────────────────────────────────────────────────────────
    LOG_LEVEL: str = _env("LOG_LEVEL", "logging", "level", default="INFO")
    LOG_FILE:  str = _env("LOG_FILE",  "logging", "file",  default="")  # 空=只输出到 stderr

    # ── 审计 ─────────────────────────────────────────────────────────────────────
    AUDIT_DB_PATH: str = _env("AUDIT_DB_PATH", "audit", "db_path", default="forge_audit.db")

    # ── Web 服务器 ─────────────────────────────────────────────────────────────
    HOST: str = _env("HOST", "server", "host", default="0.0.0.0")
    PORT: int = int(_env("PORT", "server", "port", default="8000"))

    # ── Pi Orchestrator（任务控制面）────────────────────────────────────────────
    PI_ORCHESTRATOR_ENABLED: bool = _env(
        "PI_ORCHESTRATOR_ENABLED",
        "pi_orchestrator",
        "enabled",
        default="false",
    ).lower() == "true"
    LEGACY_AGENT_API_ENABLED: bool = _env(
        "LEGACY_AGENT_API_ENABLED",
        "pi_orchestrator",
        "legacy_agent_api_enabled",
        default="false",
    ).lower() == "true"
    PI_ORCHESTRATOR_URL: str = _env(
        "PI_ORCHESTRATOR_URL",
        "pi_orchestrator",
        "url",
        default="http://127.0.0.1:4310",
    ).rstrip("/")
    PI_ORCHESTRATOR_TIMEOUT_SECONDS: int = int(_env(
        "PI_ORCHESTRATOR_TIMEOUT_SECONDS",
        "pi_orchestrator",
        "timeout_seconds",
        default="300",
    ))
    PI_WEB_ADMIN_TASK_SCOPES: str = _env(
        "PI_WEB_ADMIN_TASK_SCOPES",
        "pi_orchestrator",
        "web_admin_task_scopes",
        default="org_default:team_default",
    )
    PI_SERVICE_API_KEYS: list[str] = [
        key for key in _env(
            "PI_SERVICE_API_KEYS",
            "pi_orchestrator",
            "service_api_keys",
            default="",
        ).split(",")
        if key
    ]

    # ── QueryRun ──────────────────────────────────────────────────────────────
    QUERY_RUN_DB_PATH: str = _env(
        "QUERY_RUN_DB_PATH",
        "query_runs",
        "db_path",
        default=".forge/query_runs.db",
    )
    REPORT_DB_PATH: str = _env(
        "REPORT_DB_PATH", "reports", "db_path", default=".forge/reports.db"
    )
    REPORT_ARTIFACT_DIR: str = _env(
        "REPORT_ARTIFACT_DIR", "reports", "artifact_dir", default=".forge/report-artifacts"
    )
    REPORT_PUBLIC_BASE_URL: str = _env(
        "REPORT_PUBLIC_BASE_URL", "reports", "public_base_url", default=""
    )
    PI_MODEL_CATALOG_PATH: str = _env(
        "PI_MODEL_CATALOG_PATH", "pi_orchestrator", "model_catalog_path", default=""
    )

    QUERY_RUN_REVIEW_TTL_SECONDS: int = int(_env(
        "QUERY_RUN_REVIEW_TTL_SECONDS",
        "query_runs",
        "review_ttl_seconds",
        default="900",
    ))
    DATASOURCE_ID: str = _env(
        "DATASOURCE_ID",
        "database",
        "datasource_id",
        default="default",
    )

    # ── 认证鉴权 ───────────────────────────────────────────────────────────────
    AUTH_ENABLED:        bool      = _env("AUTH_ENABLED", "server", "auth", "enabled", default="false").lower() == "true"
    AUTH_ADMIN_PASSWORD: str       = (
        os.getenv("AUTH_PASSWORD")
        or _y("server", "auth", "admin_password", default="")
    )
    AUTH_API_KEYS:       list[str] = [
        k for k in (
            os.getenv("AUTH_API_KEYS", "").split(",")
            + (_yaml.get("server", {}).get("auth", {}).get("api_keys") or [])
        )
        if k
    ]
    AUTH_COOKIE_SECURE: bool = (
        os.getenv("AUTH_COOKIE_SECURE")
        or _y("server", "auth", "cookie_secure", default="false")
    ).lower() == "true"


# 全局单例
cfg = Config()
