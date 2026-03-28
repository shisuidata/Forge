"""
Forge 测试共享 fixtures。

提供：
    - app:          禁用认证的 FastAPI 应用实例
    - client:       httpx.AsyncClient，连接到测试应用
    - auth_client:  附带有效 session cookie 的 httpx.AsyncClient
"""
from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient


@pytest.fixture(scope="session")
def app():
    """创建 FastAPI 应用实例，禁用认证。"""
    from config import cfg
    # 直接覆盖已加载的 config 属性
    cfg.AUTH_ENABLED = False
    cfg.LLM_API_KEY = ""
    cfg.EMBED_API_KEY = ""

    from main import app as _app
    return _app


@pytest_asyncio.fixture
async def client(app):
    """无认证的 httpx.AsyncClient。"""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture
async def auth_client(app):
    """附带有效 session cookie 的 httpx.AsyncClient（用于认证启用的测试场景）。"""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        resp = await c.post(
            "/login",
            data={"password": "test", "next": "/chat"},
            follow_redirects=False,
        )
        if "set-cookie" in resp.headers:
            cookie_str = resp.headers["set-cookie"]
            name, _, rest = cookie_str.partition("=")
            value = rest.split(";")[0]
            c.cookies.set(name.strip(), value.strip())
        yield c
