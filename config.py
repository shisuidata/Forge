"""
Forge configuration — loaded from environment variables or .env file.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


class Config:
    # ── Feishu ────────────────────────────────────────────────────────────────
    FEISHU_APP_ID:             str = os.getenv("FEISHU_APP_ID", "")
    FEISHU_APP_SECRET:         str = os.getenv("FEISHU_APP_SECRET", "")
    FEISHU_VERIFICATION_TOKEN: str = os.getenv("FEISHU_VERIFICATION_TOKEN", "")
    FEISHU_ENCRYPT_KEY:        str = os.getenv("FEISHU_ENCRYPT_KEY", "")

    # ── LLM ──────────────────────────────────────────────────────────────────
    LLM_PROVIDER:  str = os.getenv("LLM_PROVIDER", "anthropic")   # anthropic | openai
    LLM_API_KEY:   str = os.getenv("LLM_API_KEY", "")
    LLM_MODEL:     str = os.getenv("LLM_MODEL", "claude-sonnet-4-6")
    LLM_BASE_URL:  str = os.getenv("LLM_BASE_URL", "")            # for OpenAI-compatible

    # ── Database ──────────────────────────────────────────────────────────────
    DATABASE_URL:  str = os.getenv("DATABASE_URL", "")             # e.g. postgresql://...

    # ── Registry ──────────────────────────────────────────────────────────────
    REGISTRY_PATH: Path = Path(os.getenv("REGISTRY_PATH", "schema.registry.json"))

    # ── Server ───────────────────────────────────────────────────────────────
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))


cfg = Config()
