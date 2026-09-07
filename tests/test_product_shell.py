from __future__ import annotations



def test_product_shell_renders_active_navigation_without_runtime_data() -> None:
    from web.templates import templates

    template = templates.env.from_string(
        '{% extends "product_base.html" %}{% block content %}<p id="probe">真实页面内容</p>{% endblock %}'
    )
    rendered = template.render(active="tasks", page_title="任务")
    assert '<p id="probe">真实页面内容</p>' in rendered
    assert 'data-active-route="tasks"' in rendered
    assert 'href="/tasks" aria-current="page"' in rendered
    assert "演示数据" not in rendered



async def test_product_pages_are_routable_and_share_the_local_shell(client) -> None:
    data_pages = (
        "/workspace", "/chat", "/tasks", "/tasks/tr_demo_001",
        "/reports", "/deliverables", "/data",
    )
    for route in data_pages:
        response = await client.get(route)
        assert response.status_code == 200, route

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
    css = await client.get("/static/product/product.css")
    js = await client.get("/static/product/product-shell.js")
    pages = await client.get("/static/product/product-pages.js")
    assert css.status_code == 200
    assert css.headers["content-type"].startswith("text/css")
    assert js.status_code == 200
    assert pages.status_code == 200
    assert "javascript" in js.headers["content-type"]
