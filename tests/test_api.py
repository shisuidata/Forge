"""
Forge Web API 自动化测试。

覆盖范围：
    - 健康检查端点
    - 认证流程（登录 / 登出 / 未认证拦截）
    - 聊天 API（/api/chat, /api/approve, /api/cancel）
    - SQL 直接执行（/api/execute-raw）
    - Admin 页面路由可达性（dashboard, schema, metrics, audit 等）
    - Audit 分页
"""
from __future__ import annotations

import pytest
from httpx import AsyncClient


# ── 基础端点 ──────────────────────────────────────────────────────────────────

class TestHealthCheck:
    async def test_health_returns_ok(self, client: AsyncClient):
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    async def test_root_redirects_to_chat(self, client: AsyncClient):
        resp = await client.get("/", follow_redirects=False)
        assert resp.status_code == 302
        assert "/chat" in resp.headers["location"]


# ── 认证流程 ──────────────────────────────────────────────────────────────────

class TestAuth:
    async def test_login_page_loads(self, client: AsyncClient):
        resp = await client.get("/login")
        assert resp.status_code == 200
        assert "Forge" in resp.text

    async def test_login_success_redirects(self, client: AsyncClient):
        resp = await client.post(
            "/login",
            data={"password": "test", "next": "/chat"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "forge_session" in resp.headers.get("set-cookie", "")

    async def test_logout_clears_session(self, client: AsyncClient):
        resp = await client.get("/logout", follow_redirects=False)
        assert resp.status_code == 302


# ── Chat API ─────────────────────────────────────────────────────────────────

class TestChatAPI:
    @pytest.mark.skipif(True, reason="需要 LLM API Key 才能测试完整 chat 流程")
    async def test_chat_with_llm(self, client: AsyncClient):
        """完整 chat 流程（需要 LLM API Key）。"""
        resp = await client.post(
            "/api/chat",
            json={"message": "test query", "user_id": "test_user"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("action") in ("error", "message", "sql_review")

    async def test_cancel_without_pending(self, client: AsyncClient):
        """没有 pending SQL 时取消应正常返回。"""
        resp = await client.post(
            "/api/cancel",
            json={"message": "", "user_id": "test_user_cancel"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "action" in data


# ── Execute Raw API ──────────────────────────────────────────────────────────

class TestExecuteRaw:
    async def test_execute_simple_sql(self, client: AsyncClient):
        """直接执行简单 SQL 应返回结果。"""
        resp = await client.post(
            "/api/execute-raw",
            json={"sql": "SELECT 1 AS num", "user_id": "test_user"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["columns"] is not None
        assert data["rows"] is not None
        assert data["exec_error"] is None

    async def test_execute_invalid_sql(self, client: AsyncClient):
        """执行非法 SQL 应返回错误信息。"""
        resp = await client.post(
            "/api/execute-raw",
            json={"sql": "SELECT FROM WHERE INVALID", "user_id": "test_user"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["exec_error"] is not None

    async def test_execute_select_from_sqlite_master(self, client: AsyncClient):
        """对数据库执行 SQLite 系统查询。"""
        resp = await client.post(
            "/api/execute-raw",
            json={"sql": "SELECT name FROM sqlite_master WHERE type='table' LIMIT 5", "user_id": "test_user"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["columns"] is not None
        assert data["exec_error"] is None


# ── Admin 页面可达性 ─────────────────────────────────────────────────────────

class TestAdminPages:
    @pytest.mark.parametrize("path", [
        "/admin/dashboard",
        "/admin/schema",
        "/admin/metrics",
        "/admin/semantic",
        "/admin/staging",
        "/admin/audit",
        "/admin/settings",
    ])
    async def test_admin_page_loads(self, client: AsyncClient, path: str):
        resp = await client.get(path)
        assert resp.status_code == 200
        assert "Forge" in resp.text

    async def test_admin_root_redirects_to_dashboard(self, client: AsyncClient):
        resp = await client.get("/admin/", follow_redirects=False)
        assert resp.status_code in (302, 307)
        assert "dashboard" in resp.headers["location"]

    async def test_chat_page_loads(self, client: AsyncClient):
        resp = await client.get("/chat")
        assert resp.status_code == 200
        assert "Forge" in resp.text


# ── Dashboard 数据 ───────────────────────────────────────────────────────────

class TestDashboard:
    async def test_dashboard_has_overview_cards(self, client: AsyncClient):
        resp = await client.get("/admin/dashboard")
        assert resp.status_code == 200
        text = resp.text
        assert "数据表" in text
        assert "业务指标" in text
        assert "语义规则" in text
        assert "今日查询" in text

    async def test_dashboard_has_health_status(self, client: AsyncClient):
        resp = await client.get("/admin/dashboard")
        assert resp.status_code == 200
        assert "系统状态" in resp.text

    async def test_dashboard_has_quick_actions(self, client: AsyncClient):
        resp = await client.get("/admin/dashboard")
        assert resp.status_code == 200
        assert "开始查询" in resp.text
        assert "管理指标" in resp.text


# ── Audit 分页 ───────────────────────────────────────────────────────────────

class TestAuditPagination:
    async def test_audit_default_page(self, client: AsyncClient):
        resp = await client.get("/admin/audit")
        assert resp.status_code == 200
        assert "页" in resp.text  # 分页信息

    async def test_audit_page_param(self, client: AsyncClient):
        resp = await client.get("/admin/audit?page=1")
        assert resp.status_code == 200

    async def test_audit_with_status_filter(self, client: AsyncClient):
        resp = await client.get("/admin/audit?status=approved")
        assert resp.status_code == 200

    async def test_audit_with_search(self, client: AsyncClient):
        resp = await client.get("/admin/audit?q=test")
        assert resp.status_code == 200


# ── Metrics API ──────────────────────────────────────────────────────────────

class TestMetrics:
    async def test_metrics_page_has_search(self, client: AsyncClient):
        resp = await client.get("/admin/metrics")
        assert resp.status_code == 200
        assert "metrics-search" in resp.text  # 搜索框 id
