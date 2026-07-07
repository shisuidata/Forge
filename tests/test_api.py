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
from pathlib import Path


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

    async def test_readiness_reports_config_issues(self, client: AsyncClient, monkeypatch, tmp_path):
        from config import cfg

        monkeypatch.setattr(cfg, "AUTH_ENABLED", False)
        monkeypatch.setattr(cfg, "LLM_API_KEY", "")
        monkeypatch.setattr(cfg, "EXECUTION_ENABLED", True)
        monkeypatch.setattr(cfg, "DATABASE_URL", "")
        monkeypatch.setattr(cfg, "DATABASE_READONLY_CONFIRMED", False)
        monkeypatch.setattr(cfg, "EXECUTION_TIMEOUT_SECONDS", 0)
        monkeypatch.setattr(cfg, "AUDIT_DB_PATH", str(tmp_path / "audit.db"))

        resp = await client.get("/health/readiness")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "fail"
        names = {check["name"] for check in data["checks"]}
        assert {"auth", "database", "llm", "audit", "database_readonly", "query_timeout"} <= names

    async def test_readiness_requires_readonly_confirmation(self, client: AsyncClient, monkeypatch, tmp_path):
        from config import cfg

        schema = tmp_path / "schema.registry.json"
        metrics = tmp_path / "metrics.registry.yaml"
        disambiguations = tmp_path / "disambiguations.registry.yaml"
        conventions = tmp_path / "field_conventions.registry.yaml"
        for path in (schema, metrics, disambiguations, conventions):
            path.write_text("{}", encoding="utf-8")

        monkeypatch.setattr(cfg, "AUTH_ENABLED", True)
        monkeypatch.setattr(cfg, "AUTH_ADMIN_PASSWORD", "not-default")
        monkeypatch.setattr(cfg, "AUTH_COOKIE_SECURE", True)
        monkeypatch.setattr(cfg, "LLM_API_KEY", "sk-test")
        monkeypatch.setattr(cfg, "LLM_PROVIDER", "openai")
        monkeypatch.setattr(cfg, "LLM_MODEL", "deepseek-v4-pro")
        monkeypatch.setattr(cfg, "EXECUTION_ENABLED", True)
        monkeypatch.setattr(cfg, "DATABASE_URL", "sqlite:///:memory:")
        monkeypatch.setattr(cfg, "DATABASE_READONLY_CONFIRMED", False)
        monkeypatch.setattr(cfg, "RAW_SQL_ENABLED", False)
        monkeypatch.setattr(cfg, "EXECUTION_MAX_ROWS", 200)
        monkeypatch.setattr(cfg, "EXECUTION_TIMEOUT_SECONDS", 30)
        monkeypatch.setattr(cfg, "REGISTRY_PATH", schema)
        monkeypatch.setattr(cfg, "METRICS_PATH", metrics)
        monkeypatch.setattr(cfg, "DISAMBIGUATIONS_PATH", disambiguations)
        monkeypatch.setattr(cfg, "CONVENTIONS_PATH", conventions)
        monkeypatch.setattr(cfg, "AUDIT_DB_PATH", str(tmp_path / "audit.db"))

        resp = await client.get("/health/readiness")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "fail"
        readonly = next(check for check in data["checks"] if check["name"] == "database_readonly")
        assert readonly["status"] == "fail"

    def test_auth_enabled_reads_environment_variable(self, monkeypatch):
        import os
        import subprocess
        import sys

        env = os.environ.copy()
        env["AUTH_ENABLED"] = "true"
        result = subprocess.run(
            [sys.executable, "-c", "from config import cfg; print(cfg.AUTH_ENABLED)"],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )

        assert result.stdout.strip() == "True"


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

    async def test_login_rejects_external_next_redirect(self, client: AsyncClient):
        resp = await client.post(
            "/login",
            data={"password": "test", "next": "https://evil.example/path"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert resp.headers["location"] == "/chat"

    async def test_login_requires_password_when_auth_enabled(self, client: AsyncClient, monkeypatch):
        from config import cfg

        monkeypatch.setattr(cfg, "AUTH_ENABLED", True)
        monkeypatch.setattr(cfg, "AUTH_ADMIN_PASSWORD", "")

        resp = await client.post(
            "/login",
            data={"password": "anything", "next": "/chat"},
            follow_redirects=False,
        )
        assert resp.status_code == 401

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

    async def test_execute_raw_rejects_mutating_sql(self, client: AsyncClient):
        """手动 SQL 执行接口只允许只读查询。"""
        resp = await client.post(
            "/api/execute-raw",
            json={"sql": "DROP TABLE users", "user_id": "test_user"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["exec_error"] is not None
        assert "只允许" in data["exec_error"] or "非只读" in data["exec_error"]

    async def test_execute_raw_can_be_disabled(self, client: AsyncClient, monkeypatch):
        from config import cfg

        monkeypatch.setattr(cfg, "RAW_SQL_ENABLED", False)

        resp = await client.post(
            "/api/execute-raw",
            json={"sql": "SELECT 1", "user_id": "test_user"},
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["exec_error"] is not None
        assert "禁用" in data["exec_error"]


class TestFeedbackAPI:
    async def test_submit_feedback(self, client: AsyncClient, monkeypatch, tmp_path):
        from agent import feedback

        monkeypatch.setattr(feedback.audit.cfg, "AUDIT_DB_PATH", str(tmp_path / "audit.db"))

        resp = await client.post(
            "/api/feedback",
            json={
                "user_id": "u1",
                "feedback_type": "wrong_result",
                "message": "这个 SQL 少了已完成订单过滤",
                "question": "统计销售额",
                "sql": "SELECT 1",
            },
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["status"] == "pending"

    async def test_submit_feedback_rejects_empty_message(self, client: AsyncClient, monkeypatch, tmp_path):
        from agent import feedback

        monkeypatch.setattr(feedback.audit.cfg, "AUDIT_DB_PATH", str(tmp_path / "audit.db"))

        resp = await client.post(
            "/api/feedback",
            json={
                "user_id": "u1",
                "feedback_type": "wrong_result",
                "message": " ",
            },
        )

        assert resp.status_code == 400
        assert resp.json()["ok"] is False


class TestAdminAIAuth:
    async def test_admin_ai_requires_auth_when_enabled(self, client: AsyncClient, monkeypatch):
        from config import cfg

        monkeypatch.setattr(cfg, "AUTH_ENABLED", True)
        monkeypatch.setattr(cfg, "AUTH_API_KEYS", ["secret"])

        resp = await client.post(
            "/api/admin-apply",
            json={"type": "delete_metric", "proposal": {"name": "x"}},
        )
        assert resp.status_code == 401

    async def test_admin_apply_rejects_invalid_metric(self, client: AsyncClient, monkeypatch, tmp_path):
        from config import cfg

        schema_path = tmp_path / "schema.registry.json"
        metrics_path = tmp_path / "metrics.registry.yaml"
        schema_path.write_text(
            '{"tables":{"orders":{"columns":["id","total_amount"]}}}',
            encoding="utf-8",
        )
        metrics_path.write_text("", encoding="utf-8")
        monkeypatch.setattr(cfg, "REGISTRY_PATH", schema_path)
        monkeypatch.setattr(cfg, "METRICS_PATH", metrics_path)

        resp = await client.post(
            "/api/admin-apply",
            json={
                "type": "add_metric",
                "proposal": {
                    "name": "bad_metric",
                    "metric_class": "atomic",
                    "label": "坏指标",
                    "description": "不存在字段",
                    "aggregation": "sum",
                    "measure": "orders.missing_col",
                },
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is False
        assert "missing_col" in data["error"]


class TestSettingsRoutes:
    async def test_save_auth_settings_sets_cookie_secure(self, client: AsyncClient, monkeypatch):
        import web.routes.settings as settings_routes

        saved = {}
        monkeypatch.setattr(settings_routes, "_load_forge_yaml", lambda: {})
        monkeypatch.setattr(settings_routes, "_save_forge_yaml", lambda data: saved.update(data))

        resp = await client.post(
            "/admin/settings/auth",
            data={
                "enabled": "on",
                "cookie_secure": "on",
                "admin_password": "new-password",
                "api_keys": "k1\nk2",
            },
            follow_redirects=False,
        )

        assert resp.status_code == 303
        auth_cfg = saved["server"]["auth"]
        assert auth_cfg["enabled"] is True
        assert auth_cfg["cookie_secure"] is True
        assert auth_cfg["admin_password"] == "new-password"
        assert auth_cfg["api_keys"] == ["k1", "k2"]

    async def test_save_execution_settings(self, client: AsyncClient, monkeypatch):
        import web.routes.settings as settings_routes

        saved = {}
        monkeypatch.setattr(settings_routes, "_load_forge_yaml", lambda: {})
        monkeypatch.setattr(settings_routes, "_save_forge_yaml", lambda data: saved.update(data))

        resp = await client.post(
            "/admin/settings/execution",
            data={
                "enabled": "on",
                "database_readonly_confirmed": "on",
                "max_rows": "500",
                "display_rows": "40",
                "timeout_seconds": "45",
            },
            follow_redirects=False,
        )

        assert resp.status_code == 303
        execution_cfg = saved["execution"]
        assert execution_cfg["enabled"] is True
        assert execution_cfg["raw_sql_enabled"] is False
        assert execution_cfg["database_readonly_confirmed"] is True
        assert execution_cfg["max_rows"] == 500
        assert execution_cfg["display_rows"] == 40
        assert execution_cfg["timeout_seconds"] == 45


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
