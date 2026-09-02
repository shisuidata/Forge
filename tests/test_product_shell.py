from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "web/templates/product_base.html"
CSS = ROOT / "web/static/product/product.css"
JS = ROOT / "web/static/product/product-shell.js"
PAGES_JS = ROOT / "web/static/product/product-pages.js"


def test_product_shell_uses_only_local_versioned_assets() -> None:
    source = TEMPLATE.read_text(encoding="utf-8")
    assert '/static/product/product.css?v=5' in source
    assert '/static/product/product-shell.js?v=2' in source
    assert "cdn." not in source.lower()
    assert "https://" not in source.lower()
    assert "<style" not in source.lower()
    assert "<script>" not in source.lower()


def test_product_shell_navigation_matches_approved_future_scope() -> None:
    source = TEMPLATE.read_text(encoding="utf-8")
    for group in ("工作", "信任", "接入", "系统"):
        assert group in source
    for label in (
        "工作台", "对话", "任务", "交付", "数据资产",
        "治理与审计", "Agents &amp; Apps", "管理",
    ):
        assert label in source
    for href in (
        "/workspace", "/chat", "/tasks", "/deliverables",
        "/data", "/governance", "/runtime", "/manage",
    ):
        assert f'href="{href}"' in source
    for internal in ("Economics", "Pipeline", "Memory", "Architecture"):
        assert internal not in source
    assert "新建任务" not in source
    assert 'aria-current="page"' in source
    assert 'href="#product-main"' in source


def test_product_shell_renders_active_navigation_without_runtime_data() -> None:
    from web.router import templates

    template = templates.env.from_string(
        '{% extends "product_base.html" %}{% block content %}<p id="probe">真实页面内容</p>{% endblock %}'
    )
    rendered = template.render(active="tasks", page_title="任务")
    assert '<p id="probe">真实页面内容</p>' in rendered
    assert 'data-active-route="tasks"' in rendered
    assert 'href="/tasks" aria-current="page"' in rendered
    assert "演示数据" not in rendered


def test_product_shell_css_contains_shared_states_and_accessibility_guards() -> None:
    source = CSS.read_text(encoding="utf-8")
    for token in (
        "--paper:", "--surface:", "--ink:", "--moss:", "--lime:",
        '.status[data-state="running"]',
        '.status[data-state="waiting_decision"]',
        '.status[data-state="partial"]',
        '.status[data-state="failed"]',
        '.status[data-state="offline"]',
        '.status[data-state="planned"]',
        '.status[data-state="blocked"]',
        ".capability-grid",
        ".topbar-context",
        ":focus-visible",
        "prefers-reduced-motion",
        ".layout-grid > * { min-width: 0; }",
    ):
        assert token in source
    assert "overflow-x: hidden" not in source
    assert "purple" not in source.lower()


def test_product_shell_javascript_is_navigation_only_and_has_no_data_side_effects() -> None:
    source = JS.read_text(encoding="utf-8")
    assert "fetch(" not in source
    assert "XMLHttpRequest" not in source
    assert "WebSocket" not in source
    assert "localStorage" not in source
    assert "setNavigation" in source
    assert "Escape" in source
    assert "productStateLabel" in source


def test_product_pages_use_safe_dom_and_real_product_endpoints() -> None:
    source = PAGES_JS.read_text(encoding="utf-8")
    for endpoint in (
        "/api/product/workspace", "/api/product/conversations", "/api/product/tasks",
        "/api/product/reports", "/api/product/data-summary", "/api/pi/chat/messages",
    ):
        assert endpoint in source
    assert "innerHTML" not in source
    assert "eval(" not in source
    assert "localStorage" not in source
    assert "演示数据" not in source
    assert "renderMarkdown" in source
    assert "approve_query" in source
    assert "assurance_report_hash" in source
    assert "crypto.getRandomValues" in source
    assert "crypto.randomUUID" not in source
    assert "Math.random" not in source
    assert "parsed.origin === location.origin" in source
    assert "parsed.protocol === 'https:'" in source
    assert source.count("conversationNeedsPolling(conversation)") >= 4
    assert "display_state === 'ready' && !hasEnabledAction" in source
    assert "审核已结束" in source
    assert "当前没有可执行的 SQL 审核操作" in source


async def test_product_pages_are_routable_and_share_the_local_shell(client) -> None:
    data_pages = (
        "/workspace", "/chat", "/tasks", "/tasks/tr_demo_001",
        "/reports", "/deliverables", "/data",
    )
    for route in data_pages:
        response = await client.get(route)
        assert response.status_code == 200, route
        assert '/static/product/product.css?v=5' in response.text
        assert '/static/product/product-pages.js?v=6' in response.text
        assert "cdn." not in response.text.lower()

    capability_pages = (
        "/deliverables/reusable", "/deliverables/outcomes",
        "/data/quality", "/data/conflicts",
        "/governance", "/governance/decisions", "/governance/evidence",
        "/governance/audit", "/governance/policies",
        "/runtime", "/runtime/clients", "/runtime/tools", "/runtime/activity",
        "/manage", "/search", "/inbox",
    )
    for route in capability_pages:
        response = await client.get(route)
        assert response.status_code == 200, route
        assert '/static/product/product.css?v=5' in response.text
        assert "data-capability-status=" in response.text
        assert "演示数据" not in response.text
        assert "built-in method" not in response.text
        assert "cdn." not in response.text.lower()

    for route, state in (("/forbidden", "forbidden"), ("/offline", "offline")):
        response = await client.get(route)
        assert response.status_code == 200
        assert f'data-product-state="{state}"' in response.text

    missing_task = await client.get("/tasks/not-a-task")
    assert missing_task.status_code == 404
    assert 'data-product-state="empty"' in missing_task.text
    missing_surface = await client.get("/governance/not-real")
    assert missing_surface.status_code == 404
    assert 'data-product-state="empty"' in missing_surface.text


async def test_product_static_assets_are_served_locally(client) -> None:
    css = await client.get("/static/product/product.css?v=5")
    js = await client.get("/static/product/product-shell.js?v=2")
    pages = await client.get("/static/product/product-pages.js?v=6")
    assert css.status_code == 200
    assert css.headers["content-type"].startswith("text/css")
    assert js.status_code == 200
    assert pages.status_code == 200
    assert "javascript" in js.headers["content-type"]
