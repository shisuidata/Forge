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
import os
from pathlib import Path

from dotenv import load_dotenv

# 优先加载 .env（环境变量级别最高）
load_dotenv()


def _load_yaml_cfg() -> dict:
    """读取 forge.yaml，返回嵌套 dict；文件不存在或解析失败时返回空 dict。"""
    try:
        import yaml
        p = Path(__file__).parent / "forge.yaml"
        if p.exists():
            return yaml.safe_load(p.read_text()) or {}
    except Exception:
        pass
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

    # ── Embedding ─────────────────────────────────────────────────────────────
    EMBED_API_KEY:   str = _env("EMBED_API_KEY",   "embedding", "api_key")   or _env("LLM_API_KEY", "llm", "api_key")
    EMBED_BASE_URL:  str = _env("EMBED_BASE_URL",  "embedding", "base_url",  default="https://api.minimaxi.com/v1")
    EMBED_MODEL:     str = _env("EMBED_MODEL",     "embedding", "model",     default="embo-01")
    RETRIEVAL_TOP_K: int = int(_env("RETRIEVAL_TOP_K", "embedding", "top_k", default="5"))

    # ── 数据库 ─────────────────────────────────────────────────────────────────
    DATABASE_URL: str = _env("DATABASE_URL", "database", "url")

    # ── Registry 路径 ──────────────────────────────────────────────────────────
    REGISTRY_PATH:         Path = Path(_env("REGISTRY_PATH",         "registry", "schema_path",          default="registry/data/schema.registry.json"))
    METRICS_PATH:          Path = Path(_env("METRICS_PATH",          "registry", "metrics_path",         default="registry/data/metrics.registry.yaml"))
    DISAMBIGUATIONS_PATH:  Path = Path(_env("DISAMBIGUATIONS_PATH",  "registry", "disambiguations_path", default="registry/data/disambiguations.registry.yaml"))
    CONVENTIONS_PATH:      Path = Path(_env("CONVENTIONS_PATH",      "registry", "conventions_path",     default="registry/data/field_conventions.registry.yaml"))

    # ── Staging 目录（用户确认后的歧义消除暂存区）──────────────────────────────
    STAGING_DIR: Path = Path(_env("STAGING_DIR", "staging", "dir", default=".forge/staging"))

    # ── Web 服务器 ─────────────────────────────────────────────────────────────
    HOST: str = _env("HOST", "server", "host", default="0.0.0.0")
    PORT: int = int(_env("PORT", "server", "port", default="8000"))


# 全局单例
cfg = Config()
