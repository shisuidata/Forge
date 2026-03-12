"""
Forge 全局配置模块。

所有运行时配置从环境变量读取，支持通过 .env 文件覆盖。
配置项按职责分为五组：Feishu 集成、LLM、数据库、注册表路径、服务器。

使用方式：
    from config import cfg
    cfg.DATABASE_URL   # 读取数据库 URL
    cfg.REGISTRY_PATH  # 读取结构层注册表路径（Path 对象）
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# 优先加载项目根目录下的 .env 文件；若文件不存在则静默跳过
load_dotenv()


class Config:
    # ── Feishu 机器人集成 ─────────────────────────────────────────────────────
    # App ID / App Secret：飞书开放平台应用凭证，用于调用飞书 API
    FEISHU_APP_ID:             str = os.getenv("FEISHU_APP_ID", "")
    FEISHU_APP_SECRET:         str = os.getenv("FEISHU_APP_SECRET", "")
    # 事件订阅验证 Token：飞书推送事件时验证请求合法性
    FEISHU_VERIFICATION_TOKEN: str = os.getenv("FEISHU_VERIFICATION_TOKEN", "")
    # 消息加密密钥：启用加密推送时使用，不加密可留空
    FEISHU_ENCRYPT_KEY:        str = os.getenv("FEISHU_ENCRYPT_KEY", "")

    # ── LLM 配置 ──────────────────────────────────────────────────────────────
    # LLM_PROVIDER：选择后端驱动，"anthropic" 使用官方 SDK，其余值走 OpenAI 兼容接口
    LLM_PROVIDER:  str = os.getenv("LLM_PROVIDER", "anthropic")   # anthropic | openai
    LLM_API_KEY:   str = os.getenv("LLM_API_KEY", "")
    LLM_MODEL:     str = os.getenv("LLM_MODEL", "claude-sonnet-4-6")
    # LLM_BASE_URL：仅 OpenAI 兼容模式下使用，留空则默认 api.openai.com
    LLM_BASE_URL:  str = os.getenv("LLM_BASE_URL", "")

    # ── 数据库连接 ────────────────────────────────────────────────────────────
    # 支持 SQLAlchemy 所有方言，例如：
    #   sqlite:///./forge_demo.db
    #   postgresql://user:pass@host:5432/dbname
    #   mysql+pymysql://user:pass@host:3306/dbname
    DATABASE_URL:  str = os.getenv("DATABASE_URL", "")

    # ── 注册表文件路径 ─────────────────────────────────────────────────────────
    # REGISTRY_PATH：结构层，由 forge sync 自动生成，记录表名和字段名
    # METRICS_PATH ：语义层，人工维护或由 LLM 辅助编写，记录业务指标定义
    REGISTRY_PATH: Path = Path(os.getenv("REGISTRY_PATH", "schema.registry.json"))
    METRICS_PATH:  Path = Path(os.getenv("METRICS_PATH",  "metrics.registry.yaml"))

    # ── Web 服务器 ────────────────────────────────────────────────────────────
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))


# 全局单例，整个应用中唯一的配置对象
cfg = Config()
