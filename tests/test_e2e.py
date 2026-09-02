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
import urllib.error
import urllib.request

import pytest

# Playwright 测试依赖 pytest-playwright 插件，自动提供 page fixture
# 若未安装则跳过
playwright = pytest.importorskip("playwright")

BASE_URL = os.getenv("FORGE_BASE_URL", "http://localhost:8000")


def _server_available() -> bool:
    try:
        with urllib.request.urlopen(f"{BASE_URL}/health", timeout=0.5) as resp:
            return 200 <= resp.status < 500
    except (OSError, urllib.error.URLError):
        return False


pytestmark = pytest.mark.skipif(
    not _server_available(),
    reason=f"Forge 服务未运行，跳过浏览器 E2E：{BASE_URL}",
)


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
        page.wait_for_url("**/tasks**")
        assert "/tasks" in page.url

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
    page.wait_for_url("**/tasks**")
    return page


# ── Chat 界面 ────────────────────────────────────────────────────────────────

@pytest.mark.skip(reason="旧 Chat UI 已退役；生产查询入口由 /tasks 的 Pi TaskRun UI 覆盖")
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


@pytest.mark.skipif(
    os.getenv("FORGE_E2E_ALLOW_MUTATION", "").lower() != "true",
    reason="指标 CRUD E2E 仅允许在隔离 Registry 上运行",
)
class TestMetricsCrud:
    def test_metric_create_edit_dependency_guard_and_delete(self, logged_in_page):
        page = logged_in_page
        page.goto("/admin/metrics")

        page.get_by_role("button", name="新增原子指标").click()
        page.fill("#f-name", "e2e_paid_gmv")
        page.fill("#f-label", "E2E 支付 GMV")
        page.fill("#f-description", "E2E 已完成订单金额")
        page.fill("#f-measure", "orders.total_amount")
        page.select_option("#f-aggregation", "sum")
        page.fill("#f-qualifiers", "orders.status = 'completed'")
        page.fill("#f-period-col", "orders.created_at")
        page.fill("#f-dimensions", "orders.status")
        page.locator("#metric-modal button[type='submit']").click()
        page.wait_for_url("**/admin/metrics?**")
        assert page.locator("text=指标已保存").is_visible()

        atomic_row = page.locator("#section-atomic tr.metric-row", has_text="e2e_paid_gmv")
        atomic_row.get_by_role("button", name="编辑").click()
        assert page.locator("#f-name").get_attribute("readonly") is not None
        page.fill("#f-label", "E2E 支付 GMV（已编辑）")
        page.locator("#metric-modal button[type='submit']").click()
        page.wait_for_url("**/admin/metrics?**")
        assert page.get_by_text("E2E 支付 GMV（已编辑）", exact=True).first.is_visible()

        page.get_by_role("button", name="新增衍生指标").click()
        page.fill("#f-name", "e2e_payment_rate")
        page.fill("#f-label", "E2E 支付率")
        page.fill("#f-description", "E2E 支付订单指标比率")
        page.select_option("#f-numerator", "e2e_paid_gmv")
        page.select_option("#f-denominator", "e2e_paid_gmv")
        page.fill("#f-period-col-d", "orders.created_at")
        page.locator("#metric-modal button[type='submit']").click()
        page.wait_for_url("**/admin/metrics?**")

        dialogs = []

        def handle_dialog(dialog):
            dialogs.append(dialog.message)
            dialog.accept()

        page.on("dialog", handle_dialog)
        page.locator("#section-atomic tr.metric-row", has_text="e2e_paid_gmv").get_by_role(
            "button", name="删除"
        ).click()
        page.wait_for_timeout(300)
        assert any("e2e_payment_rate" in message for message in dialogs)
        assert page.locator("#section-atomic tr.metric-row", has_text="e2e_paid_gmv").count() == 1

        page.locator("#section-derivative tr.metric-row", has_text="e2e_payment_rate").get_by_role(
            "button", name="删除"
        ).click()
        page.wait_for_timeout(300)
        page.locator("#section-atomic tr.metric-row", has_text="e2e_paid_gmv").get_by_role(
            "button", name="删除"
        ).click()
        page.wait_for_timeout(300)
        assert page.locator("#section-atomic tr.metric-row", has_text="e2e_paid_gmv").count() == 0

    def test_invalid_metric_reopens_form_with_error(self, logged_in_page):
        page = logged_in_page
        page.goto("/admin/metrics")
        page.get_by_role("button", name="新增原子指标").click()
        page.fill("#f-name", "e2e_invalid_metric")
        page.fill("#f-label", "E2E 错误指标")
        page.fill("#f-description", "引用不存在字段")
        page.fill("#f-measure", "orders.missing_col")
        page.select_option("#f-aggregation", "sum")
        page.locator("#metric-modal button[type='submit']").click()
        assert page.locator("text=校验未通过").is_visible()
        assert page.locator("text=orders.missing_col").is_visible()
        assert page.locator("#metric-modal").is_visible()


# ── Audit 分页 ──────────────────────────────────────────────────────────────

class TestAudit:
    def test_audit_page_loads(self, logged_in_page):
        page = logged_in_page
        page.goto("/admin/audit")
        assert page.locator("text=查询审计").first.is_visible()
        assert page.get_by_role("link", name="全部").is_visible()

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


# ── Product Conversation ─────────────────────────────────────────────────────

class TestProductConversation:
    def test_query_result_renders_bounded_table(self, page):
        conversation_id = "web_conv_table_test"
        summary = {
            "conversation_id": conversation_id,
            "title": "看下各个品类的销售额数据",
            "latest_message_preview": "查询结果",
            "updated_at": "2026-08-25T08:00:00Z",
            "href": f"/chat?conversation={conversation_id}",
            "display_state": "ready",
        }
        detail = {
            "summary": summary,
            "entries": [{
                "task": {
                    "task_run_id": "tr_table_test",
                    "conversation_id": conversation_id,
                    "status": "ready_for_analysis",
                    "display_state": "ready",
                },
                "user_message": {
                    "text": "看下各个品类的销售额数据",
                    "created_at": "2026-08-25T08:00:00Z",
                },
                "presentation": {
                    "title": "查询结果",
                    "markdown": "共 107 行。",
                    "fields": [],
                    "table": {
                        "columns": ["品类", "销售额"],
                        "rows": [["电子产品", 1200], ["家居用品", 800]],
                        "truncated": True,
                    },
                    "source_artifact_ids": ["ar_query_result"],
                },
                "actions": [],
            }],
        }

        def product_conversations(route):
            if route.request.url.split("?", 1)[0].endswith(f"/{conversation_id}"):
                route.fulfill(json={"conversation": detail})
            else:
                route.fulfill(json={"conversations": [summary]})

        page.route("**/api/product/conversations**", product_conversations)
        page.goto(f"/chat?conversation={conversation_id}")

        table = page.locator(".conversation-entry table.data-table")
        assert table.is_visible()
        assert table.locator("thead th").all_text_contents() == ["品类", "销售额"]
        assert table.locator("tbody tr").count() == 2
        assert table.get_by_text("电子产品").is_visible()
        assert page.get_by_text("结果已截断").is_visible()

    def test_latest_task_status_sidebar_is_visible_and_mobile_drawer_works(self, page):
        conversation_id = "web_conv_task_status"
        task_id = "tr_task_status"
        summary = {
            "conversation_id": conversation_id,
            "title": "分析渠道销售额",
            "latest_message_preview": "正在执行查询",
            "updated_at": "2026-08-25T09:00:00Z",
            "href": f"/chat?conversation={conversation_id}",
            "display_state": "running",
        }
        conversation = {
            "summary": summary,
            "entries": [{
                "task": {
                    "task_run_id": task_id,
                    "conversation_id": conversation_id,
                    "status": "querying",
                    "display_state": "running",
                },
                "user_message": {
                    "text": "分析渠道销售额",
                    "created_at": "2026-08-25T09:00:00Z",
                },
                "presentation": {
                    "title": "Forge 正在处理",
                    "markdown": "正在安全执行已审批的只读查询。",
                    "fields": [],
                    "table": None,
                    "source_artifact_ids": ["ar_plan"],
                },
                "actions": [],
            }],
        }
        task_detail = {
            "task": {
                "task_run_id": task_id,
                "conversation_id": conversation_id,
                "title": "分析渠道销售额",
                "status": "querying",
                "display_state": "running",
            },
            "plan": {
                "steps": [
                    {"step_id": "step_query", "title": "执行查询", "capability": "query", "required": True, "status": "running"},
                    {"step_id": "step_analyze", "title": "分析结果", "capability": "analysis", "required": True, "status": "pending"},
                ],
            },
            "actions": [],
            "artifacts": [{
                "artifact_id": "ar_plan",
                "artifact_type": "execution_plan",
                "title": "执行计划",
                "state": "ready",
                "evidence_refs": [],
            }],
            "activity": [{
                "sequence": 3,
                "title": "查询执行中",
                "state": "running",
                "created_at": "2026-08-25T09:00:03Z",
            }],
            "review_request": None,
        }
        for index in range(8):
            task_detail["artifacts"].append({
                "artifact_id": f"ar_extra_{index}",
                "artifact_type": "analysis",
                "title": f"分析证据 {index + 1}",
                "state": "ready",
                "evidence_refs": [f"ev_{index}"],
            })
            task_detail["activity"].append({
                "sequence": 10 + index,
                "title": f"任务活动 {index + 1}",
                "state": "ready",
                "created_at": f"2026-08-25T09:00:{10 + index:02d}Z",
            })

        def product_conversations(route):
            if route.request.url.split("?", 1)[0].endswith(f"/{conversation_id}"):
                route.fulfill(json={"conversation": conversation})
            else:
                route.fulfill(json={"conversations": [summary]})

        task_detail_requests = 0

        def product_task(route):
            nonlocal task_detail_requests
            task_detail_requests += 1
            route.fulfill(json={"detail": task_detail})

        page.route("**/api/product/conversations**", product_conversations)
        page.route(f"**/api/product/tasks/{task_id}", product_task)
        page.goto(f"/chat?conversation={conversation_id}")

        sidebar = page.locator("[data-conversation-task-panel]")
        assert sidebar.is_visible()
        sidebar.get_by_text("分析渠道销售额").wait_for(state="visible")
        assert sidebar.get_by_text("执行查询").is_visible()
        assert sidebar.get_by_text("任务活动 8").is_visible()
        assert not page.locator("[data-task-panel-toggle]").is_visible()
        panel_summary = sidebar.locator(".task-panel-summary")
        panel_summary.evaluate("(element) => { element.dataset.stabilityProbe = 'preserve'; }")
        page.wait_for_timeout(1300)
        assert task_detail_requests >= 2
        assert panel_summary.get_attribute("data-stability-probe") == "preserve"
        task_panel_scroll = sidebar.locator("[data-task-panel-body]")
        scroll_before = task_panel_scroll.evaluate(
            "(element) => { element.scrollTop = 120; return element.scrollTop; }"
        )
        assert scroll_before > 0
        task_detail["activity"].append({
            "sequence": 99,
            "title": "分析阶段已开始",
            "state": "running",
            "created_at": "2026-08-25T09:01:00Z",
        })
        page.wait_for_timeout(2400)
        assert task_detail_requests >= 3
        sidebar.get_by_text("分析阶段已开始").wait_for(state="visible")
        assert abs(task_panel_scroll.evaluate("(element) => element.scrollTop") - scroll_before) <= 1

        page.set_viewport_size({"width": 390, "height": 844})
        page.reload()
        toggle = page.locator("[data-task-panel-toggle]")
        assert toggle.is_visible()
        toggle.click()
        assert sidebar.get_attribute("data-open") == "true"
        assert toggle.get_attribute("aria-expanded") == "true"
        backdrop = page.locator("[data-task-panel-backdrop]")
        assert not backdrop.is_hidden()
        page.keyboard.press("Escape")
        assert sidebar.get_attribute("data-open") == "false"
        assert backdrop.is_hidden()
