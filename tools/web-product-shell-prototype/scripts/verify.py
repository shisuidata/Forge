#!/usr/bin/env python3
"""Exercise the W3A Product Shell desktop prototype."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from playwright.sync_api import Page, sync_playwright


def no_horizontal_overflow(page: Page) -> None:
    metrics = page.evaluate("() => ({viewport: innerWidth, document: document.documentElement.scrollWidth})")
    assert metrics["document"] <= metrics["viewport"], metrics


def capture(page: Page, output_dir: Path, name: str, full_page: bool = False) -> None:
    page.screenshot(path=str(output_dir / f"{name}.png"), full_page=full_page)


def verify(base_url: str, output_dir: Path, width: int = 1440, height: int = 900) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    errors: list[str] = []
    routes: list[str] = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": width, "height": height}, device_scale_factor=1)
        page.on("console", lambda message: errors.append(message.text) if message.type == "error" else None)
        page.on("pageerror", lambda error: errors.append(str(error)))

        def visit(fragment: str) -> None:
            page.goto(f"{base_url.rstrip('/')}/#{fragment}", wait_until="networkidle")
            page.wait_for_selector('body[data-ready="true"]')
            assert page.get_by_text("交互原型", exact=True).is_visible()
            no_horizontal_overflow(page)
            routes.append(fragment)

        visit("/workspace")
        assert page.get_by_role("heading", name="待处理事项 3").is_visible()
        assert page.get_by_label("产品导航").get_by_role("link", name="工作台", exact=True).get_attribute("aria-current") == "page"
        assert page.get_by_text("等待审批", exact=True).count() >= 1
        capture(page, output_dir, "01-workspace")

        page.get_by_role("link", name="新建任务", exact=True).first.click()
        page.wait_for_url("**#/new")
        page.get_by_label("问题与使用场景").fill("比较本周各渠道销售额并生成报告")
        page.get_by_role("button", name="创建演示任务").click()
        page.wait_for_url("**#/tasks/tr_repurchase_definition")
        assert page.get_by_text("等待补充", exact=True).count() >= 1
        page.get_by_text("演示任务已创建，未写入生产 Store", exact=True).wait_for()
        assert "演示任务已创建" in page.locator("#toast").text_content()
        no_horizontal_overflow(page)

        visit("/tasks")
        search = page.get_by_label("搜索任务")
        search.fill("库存")
        visible_rows = page.locator("#task-table-body tr:visible")
        assert visible_rows.count() == 1
        assert "库存周转异常排查" in visible_rows.first.inner_text()

        visit("/tasks/tr_sales_channel?tab=sql")
        assert page.get_by_role("heading", name="确认数据库将要执行的内容").is_visible()
        assert page.get_by_text("SQL 尚未执行", exact=False).count() == 0
        page.get_by_role("button", name="批准只读执行").click()
        dialog = page.get_by_role("dialog")
        assert dialog.is_visible()
        assert dialog.get_by_text("待执行 SQL", exact=True).is_visible()
        assert "SELECT" in dialog.locator("pre").inner_text()
        assert dialog.get_by_text("4 项执行检查已通过").is_visible()
        assert dialog.get_by_text("不会操作真实数据", exact=False).is_visible()
        confirm = dialog.get_by_role("button", name="批准查询（演示）")
        assert confirm.is_disabled()
        dialog.get_by_label("我理解这是演示操作").check()
        assert confirm.is_enabled()
        capture(page, output_dir, "02-sql-approval")
        confirm.click()
        assert page.get_by_text("查询中", exact=True).count() >= 1
        assert "未连接数据库" in page.locator("#toast").text_content()

        page.get_by_role("button", name="查看状态样本").click()
        panel = page.get_by_role("complementary", name="原型状态样本")
        assert panel.is_visible()
        panel.get_by_role("button", name="失败").click()
        assert page.get_by_text("任务在查询阶段停止").is_visible()
        page.go_back()
        page.go_forward()
        page.reload(wait_until="networkidle")
        page.wait_for_selector('body[data-ready="true"]')
        assert page.get_by_role("heading", name="确认数据库将要执行的内容").is_visible()
        no_horizontal_overflow(page)

        visit("/reports")
        assert page.get_by_role("heading", name="3 份报告").is_visible()
        page.get_by_role("link", name="品类组合与增长诊断", exact=False).click()
        page.wait_for_url("**#/reports/rp_category_h1")
        summary_heading = page.get_by_role("heading", name="执行摘要")
        summary_heading.wait_for()
        assert summary_heading.count() == 1, (page.url, page.locator("h1,h2").all_inner_texts())
        summary_heading.scroll_into_view_if_needed()
        assert summary_heading.is_visible()
        assert page.get_by_text("当前限制", exact=True).is_visible()
        capture(page, output_dir, "03-report-detail")

        visit("/data")
        assert page.get_by_role("heading", name="数据资产", exact=True).count() >= 1
        page.get_by_role("link", name="指标与语义").click()
        page.get_by_text("支付销售额", exact=True).wait_for()
        assert page.get_by_text("支付销售额", exact=True).is_visible()
        page.get_by_role("link", name="草案与发布").click()
        page.get_by_text("新增渠道字段约定", exact=True).wait_for()
        assert page.get_by_text("新增渠道字段约定", exact=True).is_visible()

        visit("/admin")
        assert page.get_by_role("heading", name="管理", exact=True).count() >= 1
        assert page.get_by_text("企业多用户授权尚未开放", exact=True).is_visible()
        capture(page, output_dir, "04-admin")

        page.keyboard.press("Tab")
        focused = page.evaluate("() => document.activeElement?.tagName")
        assert focused in {"A", "BUTTON"}, focused
        browser.close()

    assert errors == [], errors
    result = {
        "routes": routes,
        "screenshots": sorted(str(path) for path in output_dir.glob("*.png")),
        "console_errors": errors,
        "viewport": f"{width}x{height}",
        "demo_only": True,
        "production_requests": 0,
    }
    (output_dir / "browser-gate.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:4176/")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--width", type=int, default=1440)
    parser.add_argument("--height", type=int, default=900)
    args = parser.parse_args()
    print(json.dumps(verify(args.url, args.output_dir, args.width, args.height), ensure_ascii=False))


if __name__ == "__main__":
    main()
