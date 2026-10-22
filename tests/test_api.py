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
import pytest_asyncio
import yaml
from httpx import ASGITransport, AsyncClient
from pathlib import Path


@pytest_asyncio.fixture
async def client(monkeypatch, tmp_path):
    from config import cfg
    from main import app
    from agent.tenant import tenants
    from forge.governance_credentials import create_credential

    monkeypatch.setattr(cfg, "QUERY_RUN_DB_PATH", str(tmp_path / "governance.db"))
    monkeypatch.setattr(cfg, "AUTH_ENABLED", True)
    monkeypatch.setattr(cfg, "AUTH_ADMIN_PASSWORD", "test")
    monkeypatch.setattr(cfg, "AUTH_ADMIN_PRINCIPAL_ID", "test_admin")
    tenants.create_team("test_team", organization_id="test_org")
    tenants.set_team("test_admin", "test_team", role="admin", principal_type="human", status="active")
    tenants.set_allowed_tables("test_team", ["numbers", "orders"])
    _, token = create_credential("test_admin")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test",
                           headers={"X-API-Key": token}) as scoped_client:
        yield scoped_client

# ── 基础端点 ──────────────────────────────────────────────────────────────────

class TestHealthCheck:
    async def test_health_returns_ok(self, client: AsyncClient):
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    async def test_root_redirects_to_web_chat(self, client: AsyncClient):
        resp = await client.get("/", follow_redirects=False)
        assert resp.status_code == 302
        assert resp.headers["location"] == "/chat"

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

    async def test_logout_revokes_session(self, client: AsyncClient):
        login = await client.post("/login", data={"password": "test"}, follow_redirects=False)
        assert login.status_code == 303
        client.headers.pop("X-API-Key")
        token = client.cookies["forge_session"]
        csrf = (await client.get("/api/auth/csrf")).json()["csrf_token"]
        resp = await client.post("/logout", headers={"Origin": "http://test", "X-CSRF-Token": csrf}, follow_redirects=False)
        assert resp.status_code == 303
        assert (await client.get("/api/auth/csrf")).status_code == 401
        # A copied session token must stop working, not only the browser cookie.
        client.cookies.set("forge_session", token)
        assert (await client.get("/api/auth/csrf")).status_code == 401


# ── Chat API ─────────────────────────────────────────────────────────────────

class TestClosedExecutionPaths:
    async def test_legacy_agent_and_raw_sql_never_execute(self, client: AsyncClient, monkeypatch):
        from config import cfg
        monkeypatch.setattr(cfg, "LEGACY_AGENT_API_ENABLED", True)
        monkeypatch.setattr(cfg, "RAW_SQL_ENABLED", True)
        for path in ("/api/chat", "/api/approve", "/api/cancel", "/api/execute-raw"):
            response = await client.post(path, json={"message": "查询订单", "sql": "SELECT 1"})
            assert response.status_code == 410

    async def test_prepare_requires_registered_credential(self, client: AsyncClient):
        response = await client.post("/api/prepare-query", json={"question": "查询订单"}, headers={"X-API-Key": "invalid"})
        assert response.status_code == 401

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

        client.headers["X-API-Key"] = "invalid"
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


class TestAdminMetricRoutes:
    @staticmethod
    def _configure_registry(monkeypatch, tmp_path, metrics: dict | None = None):
        from config import cfg

        schema_path = tmp_path / "schema.registry.json"
        metrics_path = tmp_path / "metrics.registry.yaml"
        schema_path.write_text(
            '{"tables":{"orders":{"columns":["id","status","total_amount","created_at"]}}}',
            encoding="utf-8",
        )
        metrics_path.write_text(
            yaml.safe_dump(metrics or {}, allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
        monkeypatch.setattr(cfg, "REGISTRY_PATH", schema_path)
        monkeypatch.setattr(cfg, "METRICS_PATH", metrics_path)
        return metrics_path

    async def test_create_atomic_metric_uses_current_registry_contract(
        self, client: AsyncClient, monkeypatch, tmp_path
    ):
        metrics_path = self._configure_registry(monkeypatch, tmp_path)

        resp = await client.post(
            "/admin/metrics/metric",
            data={
                "name": "paid_gmv",
                "label": "支付 GMV",
                "description": "已完成订单的成交金额",
                "metric_class": "atomic",
                "measure": "orders.total_amount",
                "aggregation": "sum",
                "qualifiers": "orders.status = 'completed'\n\n",
                "period_col": "orders.created_at",
                "dimensions": "orders.status\n",
                "notes": "财务确认口径",
            },
            follow_redirects=False,
        )

        assert resp.status_code == 303
        saved = yaml.safe_load(metrics_path.read_text(encoding="utf-8"))
        assert saved["paid_gmv"] == {
            "label": "支付 GMV",
            "description": "已完成订单的成交金额",
            "metric_class": "atomic",
            "measure": "orders.total_amount",
            "aggregation": "sum",
            "qualifiers": ["orders.status = 'completed'"],
            "period_col": "orders.created_at",
            "dimensions": ["orders.status"],
            "notes": "财务确认口径",
            "updated_at": saved["paid_gmv"]["updated_at"],
        }
        assert "type" not in saved["paid_gmv"]
        assert "filters" not in saved["paid_gmv"]

    async def test_create_derivative_metric_only_saves_derivative_fields(
        self, client: AsyncClient, monkeypatch, tmp_path
    ):
        atomics = {
            "paid_orders": {
                "metric_class": "atomic",
                "label": "支付订单",
                "description": "支付订单数",
                "measure": "orders.id",
                "aggregation": "count_distinct",
            },
            "all_orders": {
                "metric_class": "atomic",
                "label": "全部订单",
                "description": "全部订单数",
                "measure": "orders.id",
                "aggregation": "count_distinct",
            },
        }
        metrics_path = self._configure_registry(monkeypatch, tmp_path, atomics)

        resp = await client.post(
            "/admin/metrics/metric",
            data={
                "name": "payment_rate",
                "label": "支付率",
                "description": "支付订单占全部订单的比例",
                "metric_class": "derivative",
                "numerator": "paid_orders",
                "denominator": "all_orders",
                "period_col": "orders.created_at",
                "measure": "orders.total_amount",
                "aggregation": "sum",
            },
            follow_redirects=False,
        )

        assert resp.status_code == 303
        saved = yaml.safe_load(metrics_path.read_text(encoding="utf-8"))["payment_rate"]
        assert saved["metric_class"] == "derivative"
        assert saved["numerator"] == "paid_orders"
        assert saved["denominator"] == "all_orders"
        assert "measure" not in saved
        assert "aggregation" not in saved

    async def test_invalid_metric_renders_errors_and_preserves_form(
        self, client: AsyncClient, monkeypatch, tmp_path
    ):
        self._configure_registry(monkeypatch, tmp_path)

        resp = await client.post(
            "/admin/metrics/metric",
            data={
                "name": "bad_metric",
                "label": "错误指标",
                "description": "引用不存在字段",
                "metric_class": "atomic",
                "measure": "orders.missing_col",
                "aggregation": "sum",
            },
        )

        assert resp.status_code == 422
        assert "orders.missing_col" in resp.text
        assert "bad_metric" in resp.text
        assert 'value="atomic"' in resp.text

    async def test_metric_warnings_are_shown_after_save(
        self, client: AsyncClient, monkeypatch, tmp_path
    ):
        metrics_path = self._configure_registry(monkeypatch, tmp_path)

        resp = await client.post(
            "/admin/metrics/metric",
            data={
                "name": "gmv",
                "label": "GMV",
                "description": "订单总金额",
                "metric_class": "atomic",
                "measure": "orders.total_amount",
                "aggregation": "sum",
            },
        )

        assert resp.status_code == 200
        assert "指标已保存" in resp.text
        assert "qualifiers" in resp.text
        assert "dimensions" in resp.text
        assert "gmv" in yaml.safe_load(metrics_path.read_text(encoding="utf-8"))

    async def test_delete_referenced_atomic_metric_returns_conflict(
        self, client: AsyncClient, monkeypatch, tmp_path
    ):
        metrics = {
            "paid_orders": {
                "metric_class": "atomic",
                "label": "支付订单",
                "description": "支付订单数",
                "measure": "orders.id",
                "aggregation": "count_distinct",
            },
            "payment_rate": {
                "metric_class": "derivative",
                "label": "支付率",
                "description": "支付订单占比",
                "numerator": "paid_orders",
                "denominator": "paid_orders",
            },
        }
        metrics_path = self._configure_registry(monkeypatch, tmp_path, metrics)

        resp = await client.delete("/admin/metrics/metric/paid_orders")

        assert resp.status_code == 409
        assert resp.json()["dependents"] == ["payment_rate"]
        saved = yaml.safe_load(metrics_path.read_text(encoding="utf-8"))
        assert "paid_orders" in saved


class TestSettingsRoutes:
    async def test_model_profile_validate_and_cas_activate(self, client: AsyncClient, monkeypatch, tmp_path):
        import web.routes.settings as settings_routes

        db_path = tmp_path / "model-control.db"
        monkeypatch.setattr(settings_routes, "model_control_db_path", lambda: db_path)
        monkeypatch.setenv("MODEL_TEST_KEY", "secret-not-returned")
        monkeypatch.setattr(
            settings_routes,
            "validate_model_snapshot",
            lambda snapshot: {
                "tool_calling": True,
                "structured_output": True,
                "provider": snapshot.provider,
                "model": snapshot.model,
                "quality_gate": {
                    "passed": True,
                    "status": "passed",
                    "lineage": settings_routes.current_quality_lineage(),
                },
            },
        )

        created = await client.post("/admin/settings/model-profiles", json={
            "profile_id": "query-model",
            "name": "Query Model",
            "provider": "openai",
            "protocol": "openai_chat",
            "model": "candidate-model",
            "base_url": "https://provider.example/v1",
            "tool_choice": "required",
            "secret_ref": "env:MODEL_TEST_KEY",
        })
        assert created.status_code == 201
        revision = created.json()["revision_id"]

        validated = await client.post(f"/admin/settings/model-profiles/{revision}/validate")
        assert validated.status_code == 200
        assert validated.json()["report"]["tool_calling"] is True

        def fake_quality(store, run_id):
            store.mark_quality_validation_running(run_id)
            store.complete_quality_validation_run(
                run_id,
                status="passed",
                metrics={"passed": True},
            )

        monkeypatch.setattr(settings_routes, "run_quality_validation", fake_quality)
        queued = await client.post(
            f"/admin/settings/model-profiles/{revision}/quality-validations",
            json={"thresholds": settings_routes.DEFAULT_THRESHOLDS},
        )
        assert queued.status_code == 202
        import asyncio
        await asyncio.sleep(0.05)
        polled = await client.get(
            f"/admin/settings/model-quality-validations/{queued.json()['run_id']}"
        )
        assert polled.status_code == 200
        assert polled.json()["status"] == "passed"

        activated = await client.post("/admin/settings/model-bindings/activate", json={
            "revision_id": revision,
            "expected_version": 0,
        })
        assert activated.status_code == 200
        assert activated.json()["binding_version"] == 1
        assert "secret-not-returned" not in activated.text + validated.text + created.text

        conflict = await client.post("/admin/settings/model-bindings/activate", json={
            "revision_id": revision,
            "expected_version": 0,
        })
        assert conflict.status_code == 409

    async def test_quality_gate_switch_allows_compatibility_only_activation(self, client: AsyncClient, monkeypatch, tmp_path):
        import web.routes.settings as settings_routes

        db_path = tmp_path / "model-control.db"
        monkeypatch.setattr(settings_routes, "model_control_db_path", lambda: db_path)
        monkeypatch.setenv("MODEL_TEST_KEY", "secret-not-returned")
        monkeypatch.setattr(
            settings_routes,
            "validate_model_snapshot",
            lambda snapshot: {
                "tool_calling": True,
                "structured_output": True,
                "capability_gate": {"passed": True},
                "quality_gate": {"passed": False, "status": "not_run"},
            },
        )
        created = await client.post("/admin/settings/model-profiles", json={
            "profile_id": "compat-model", "name": "Compat Model", "provider": "openai",
            "protocol": "openai_chat", "model": "candidate", "tool_choice": "required",
            "secret_ref": "env:MODEL_TEST_KEY",
        })
        revision = created.json()["revision_id"]
        assert (await client.post(f"/admin/settings/model-profiles/{revision}/validate")).status_code == 200
        switched = await client.post("/admin/settings/model-quality-gate", json={"enabled": False})
        assert switched.status_code == 200
        assert switched.json() == {"enabled": False, "mode": "compatibility_only"}
        activated = await client.post("/admin/settings/model-bindings/activate", json={
            "revision_id": revision, "expected_version": 0,
        })
        assert activated.status_code == 200
        assert activated.json()["gate_class"] == "compatibility_only"

    async def test_model_profile_failed_validation_cannot_activate(self, client: AsyncClient, monkeypatch, tmp_path):
        import web.routes.settings as settings_routes

        db_path = tmp_path / "model-control.db"
        monkeypatch.setattr(settings_routes, "model_control_db_path", lambda: db_path)
        monkeypatch.setenv("MODEL_TEST_KEY", "secret-not-returned")
        monkeypatch.setattr(
            settings_routes,
            "validate_model_snapshot",
            lambda snapshot: (_ for _ in ()).throw(
                settings_routes.LLMCompatibilityError("provider raw detail")
            ),
        )
        created = await client.post("/admin/settings/model-profiles", json={
            "profile_id": "bad-model",
            "name": "Bad Model",
            "provider": "openai",
            "protocol": "openai_chat",
            "model": "bad-model",
            "secret_ref": "env:MODEL_TEST_KEY",
        })
        revision = created.json()["revision_id"]

        validated = await client.post(f"/admin/settings/model-profiles/{revision}/validate")
        assert validated.status_code == 422
        assert "provider raw detail" not in validated.text
        activated = await client.post("/admin/settings/model-bindings/activate", json={
            "revision_id": revision,
            "expected_version": 0,
        })
        assert activated.status_code == 400

    async def test_save_llm_settings_reload_without_restart(self, client: AsyncClient, monkeypatch):
        import web.routes.settings as settings_routes

        saved = {}
        resets = []
        monkeypatch.setattr(settings_routes, "_load_forge_yaml", lambda: {})
        monkeypatch.setattr(settings_routes, "_save_forge_yaml", lambda data: saved.update(data))
        monkeypatch.setattr(settings_routes, "reset_model_config_cache", lambda: resets.append(True))
        monkeypatch.setattr(
            settings_routes,
            "get_model_config",
            lambda: object(),
        )

        resp = await client.post(
            "/admin/settings/llm",
            data={
                "provider": "openai",
                "model": "new-model",
                "api_key": "new-secret",
                "base_url": "https://provider.example/v1",
            },
            follow_redirects=False,
        )

        assert resp.status_code == 303
        assert resp.headers["location"].endswith("saved=llm")
        assert resets == [True]
        assert saved["llm"]["model"] == "new-model"

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
        "/admin/architecture",
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

    async def test_web_chat_page_is_a_first_class_pi_channel(self, client: AsyncClient):
        resp = await client.get("/chat", follow_redirects=False)
        assert resp.status_code == 200
        assert 'data-product-page="chat"' in resp.text
        assert 'data-conversation-list' in resp.text
        assert 'data-conversation-feed' in resp.text
        assert 'data-conversation-form' in resp.text
        assert "<script>" not in resp.text
        assert "/api/chat" not in resp.text

    async def test_chat_and_task_monitor_have_separate_navigation(self, client: AsyncClient):
        resp = await client.get("/tasks")
        assert resp.status_code == 200
        assert "数据任务" in resp.text
        assert 'href="/chat"' in resp.text
        assert "在对话中发起" in resp.text
        assert 'data-task-list' in resp.text


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
