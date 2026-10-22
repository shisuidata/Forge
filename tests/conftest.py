"""
Forge 测试共享 fixtures。

提供：
    - app:          isolated FastAPI app with no legacy authentication bypass
    - client:       unauthenticated httpx.AsyncClient for rejection checks
    - governed_client: isolated registered admin API key client
"""
from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient


@pytest.fixture(scope="session")
def app():
    """Create the shared FastAPI app; governance endpoints still require credentials."""
    from config import cfg
    # 直接覆盖已加载的 config 属性
    cfg.AUTH_ENABLED = False
    cfg.LLM_API_KEY = ""
    cfg.EMBED_API_KEY = ""
    cfg.DATABASE_URL = "sqlite:///:memory:"
    cfg.LEGACY_AGENT_API_ENABLED = False

    from main import app as _app
    return _app


@pytest_asyncio.fixture
async def client(app):
    """Unauthenticated httpx.AsyncClient."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c



@pytest_asyncio.fixture
async def governed_client(app, monkeypatch, tmp_path):
    """Registered synthetic principal for public API tests that require authentication."""
    from config import cfg
    from agent.tenant import tenants
    from forge.governance_credentials import create_credential

    monkeypatch.setattr(cfg, "QUERY_RUN_DB_PATH", str(tmp_path / "governance.db"))
    monkeypatch.setattr(cfg, "AUTH_ENABLED", True)
    tenants.create_team("fixture_team", organization_id="fixture_org")
    tenants.set_team("fixture_admin", "fixture_team", role="admin", principal_type="human", status="active")
    tenants.set_allowed_tables("fixture_team", ["orders", "users", "dim_user", "numbers", "secrets"])
    _, token = create_credential("fixture_admin")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test",
                           headers={"X-API-Key": token}) as scoped:
        yield scoped
