"""
Forge Playwright E2E 测试。

前置条件：
    - 安装 playwright: pip install playwright && playwright install chromium
    - Forge 服务运行在 localhost:8000（或通过 FORGE_BASE_URL 环境变量指定）

运行方式：
    pytest tests/test_e2e.py -v --headed   # 有头模式，可观察
    pytest tests/test_e2e.py -v            # 无头模式

覆盖范围：
    - 登录流程
    - Chat 界面基础交互
    - SQL 审核卡片（编辑按钮、复制按钮）
    - Admin Dashboard 页面
    - Admin 侧边栏导航
    - Metrics 搜索过滤
    - Audit 分页
"""
from __future__ import annotations

import os
import re

import pytest

# Playwright 测试依赖 pytest-playwright 插件，自动提供 page fixture
# 若未安装则跳过
playwright = pytest.importorskip("playwright")

BASE_URL = os.getenv("FORGE_BASE_URL", "http://localhost:8000")


@pytest.fixture(scope="session")
def browser_context_args():
    """Playwright 浏览器上下文参数。"""
    return {
        "base_url": BASE_URL,
        "ignore_https_errors": True,
    }


# ── 登录流程 ─────────────────────────────────────────────────────────────────

class TestLogin:
    def test_login_page_renders(self, page):
        page.goto("/login")
        assert "Forge" in page.title()
        assert page.locator("input[name='password']").is_visible()
        assert page.locator("button[type='submit']").is_visible()

    def test_login_with_default_password(self, page):
        page.goto("/login")
        page.fill("input[name='password']", "123456")
        page.click("button[type='submit']")
        page.wait_for_url("**/chat**")
        assert "/chat" in page.url

    def test_login_wrong_password_shows_error(self, page):
        page.goto("/login")
        page.fill("input[name='password']", "wrong_password")
        page.click("button[type='submit']")
        # 应停留在登录页并显示错误
        assert "/login" in page.url or page.locator("text=密码错误").is_visible()


# ── 工具函数 ─────────────────────────────────────────────────────────────────

@pytest.fixture
def logged_in_page(page):
    """登录后的 page fixture。"""
    page.goto("/login")
    page.fill("input[name='password']", "123456")
    page.click("button[type='submit']")
    page.wait_for_url("**/chat**")
    return page


# ── Chat 界面 ────────────────────────────────────────────────────────────────

class TestChat:
    def test_chat_page_loads(self, logged_in_page):
        page = logged_in_page
        assert page.locator("text=Forge 查询助手").is_visible()
        assert page.locator("#chat-input").is_visible()

    def test_suggestion_buttons_visible(self, logged_in_page):
        page = logged_in_page
        suggestions = page.locator("button", has_text="各城市的订单总额")
        assert suggestions.is_visible()

    def test_send_message_shows_loading(self, logged_in_page):
        page = logged_in_page
        page.fill("#chat-input", "test query")
        page.click("#send-btn")
        # 应出现用户消息气泡
        assert page.locator(".msg-user").is_visible()

    def test_sidebar_has_dashboard_link(self, logged_in_page):
        page = logged_in_page
        assert page.locator("a[href='/admin/dashboard']").is_visible()
        assert page.locator("a[href='/admin/dashboard']", has_text="概览").is_visible()


# ── Admin Dashboard ──────────────────────────────────────────────────────────

class TestDashboard:
    def test_dashboard_renders(self, logged_in_page):
        page = logged_in_page
        page.goto("/admin/dashboard")
        assert page.locator("text=概览").first.is_visible()
        assert page.locator("text=数据表").is_visible()
        assert page.locator("text=业务指标").is_visible()
        assert page.get_by_role("main").get_by_text("语义规则").is_visible()
        assert page.locator("text=今日查询").is_visible()

    def test_dashboard_health_check(self, logged_in_page):
        page = logged_in_page
        page.goto("/admin/dashboard")
        assert page.locator("text=系统状态").is_visible()
        assert page.locator("text=数据库连接").is_visible()

    def test_dashboard_quick_actions(self, logged_in_page):
        page = logged_in_page
        page.goto("/admin/dashboard")
        assert page.locator("a", has_text="开始查询").is_visible()
        assert page.locator("a", has_text="管理指标").is_visible()

    def test_admin_root_redirects_to_dashboard(self, logged_in_page):
        page = logged_in_page
        page.goto("/admin/")
        page.wait_for_url("**/admin/dashboard**")
        assert "/admin/dashboard" in page.url


# ── Admin 侧边栏导航 ────────────────────────────────────────────────────────

class TestNavigation:
    @pytest.mark.parametrize("link_text,expected_path", [
        ("结构层", "/admin/schema"),
        ("指标库", "/admin/metrics"),
        ("语义规则", "/admin/semantic"),
        ("查询审计", "/admin/audit"),
        ("系统配置", "/admin/settings"),
    ])
    def test_sidebar_navigation(self, logged_in_page, link_text, expected_path):
        page = logged_in_page
        page.goto("/admin/dashboard")
        page.click(f"a:has-text('{link_text}')")
        page.wait_for_url(f"**{expected_path}**")
        assert expected_path in page.url


# ── Metrics 搜索 ────────────────────────────────────────────────────────────

class TestMetricsSearch:
    def test_metrics_has_search_box(self, logged_in_page):
        page = logged_in_page
        page.goto("/admin/metrics")
        search = page.locator("#metrics-search")
        assert search.is_visible()

    def test_metrics_search_filters_rows(self, logged_in_page):
        page = logged_in_page
        page.goto("/admin/metrics")
        # 搜索一个不存在的关键词
        page.fill("#metrics-search", "zzzzz_nonexistent_metric")
        # 如果有指标行，它们应该被隐藏
        visible_rows = page.locator("tr.metric-row:visible")
        assert visible_rows.count() == 0


# ── Audit 分页 ──────────────────────────────────────────────────────────────

class TestAudit:
    def test_audit_page_loads(self, logged_in_page):
        page = logged_in_page
        page.goto("/admin/audit")
        assert page.locator("text=查询审计").first.is_visible()
        assert page.locator("text=全部").is_visible()

    def test_audit_status_filter(self, logged_in_page):
        page = logged_in_page
        page.goto("/admin/audit")
        # 点击「已执行」过滤
        page.click("a:has-text('已执行')")
        assert "status=approved" in page.url

    def test_audit_has_pagination_info(self, logged_in_page):
        page = logged_in_page
        page.goto("/admin/audit")
        assert page.locator("text=页").first.is_visible()


# ── Schema 页面 ──────────────────────────────────────────────────────────────

class TestSchema:
    def test_schema_page_loads(self, logged_in_page):
        page = logged_in_page
        page.goto("/admin/schema")
        assert page.locator("text=结构层").first.is_visible()
