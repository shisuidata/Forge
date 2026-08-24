#!/usr/bin/env python3
"""Desktop browser gate for the isolated ECharts focused candidate."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from playwright.sync_api import sync_playwright


def verify(base_url: str, output_dir: Path) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    errors: list[str] = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1600, "height": 1000}, device_scale_factor=1)
        page.on("console", lambda message: errors.append(message.text) if message.type == "error" else None)
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.goto(base_url, wait_until="networkidle")
        page.wait_for_selector('body[data-ready="true"]')

        metrics = page.evaluate("window.__FORGE_ECHARTS_CANDIDATE__")
        assert metrics["svgCount"] == 4, metrics
        assert metrics["canvasCount"] == 0, metrics
        assert page.locator(".chart-host svg").count() == 4
        assert page.locator(".chart-host canvas").count() == 0

        first_chart = page.locator("#chart-ranking").bounding_box()
        assert first_chart is not None and first_chart["y"] < 1000, first_chart

        candidates = page.locator("#chart-ranking svg path").evaluate_all(
            "nodes => nodes.map((node, index) => { const box=node.getBoundingClientRect(); return {index,x:box.x,y:box.y,width:box.width,height:box.height}; }).filter(box => box.width > 180 && box.height > 8 && box.height < 60)"
        )
        assert candidates, "ranking bar geometry was not rendered by ECharts"
        bar = candidates[0]
        page.mouse.move(bar["x"] + bar["width"] * 0.7, bar["y"] + bar["height"] / 2)
        page.wait_for_timeout(120)
        assert page.locator("text=点击柱形查看数据来源").count() >= 1
        page.mouse.click(bar["x"] + bar["width"] * 0.7, bar["y"] + bar["height"] / 2)
        page.wait_for_timeout(80)
        assert page.locator("#evidence-drawer").get_attribute("data-open") == "true"
        assert "qr_category_story#row:1" in page.locator("#evidence-refs").inner_text()
        page.locator(".drawer-close").click()

        target_toggle = page.get_by_role("button", name="目标")
        target_toggle.click()
        assert target_toggle.get_attribute("aria-pressed") == "false"
        target_toggle.click()
        assert target_toggle.get_attribute("aria-pressed") == "true"

        page.locator('[data-table="contribution"]').click()
        assert page.locator("#data-dialog").is_visible()
        assert page.locator("#data-dialog tbody tr").count() == 3
        dialog_text = page.locator("#data-dialog").inner_text()
        assert all(value in dialog_text for value in ("直营", "+¥8.7万", "50.0%", "qr_monthly_story#row:4"))
        page.locator('#data-dialog form button').click()

        page.locator('[data-view="contribution"]').click()
        assert "qr_monthly_story#row:4" in (page.locator("#evidence-refs").text_content() or "")
        assert "qr_monthly_story#row:6" in (page.locator("#evidence-refs").text_content() or "")

        page.screenshot(path=str(output_dir / "echarts-focused-full.png"), full_page=True)
        page.locator("#decision-contribution").screenshot(path=str(output_dir / "echarts-contribution.png"))
        assert errors == [], errors

        no_script = browser.new_context(java_script_enabled=False, viewport={"width": 1600, "height": 1000})
        fallback = no_script.new_page()
        fallback.goto(base_url, wait_until="domcontentloaded")
        assert fallback.get_by_role("heading", name="品类组合与增长诊断").is_visible()
        assert fallback.locator(".no-script-note").is_visible()
        assert "图表交互当前不可用" in fallback.locator(".no-script-note").inner_text()
        assert fallback.get_by_text("三项渠道增量 87K + 53K + 34K = 174K", exact=False).is_visible()
        no_script.close()
        browser.close()

    result = {
        "metrics": metrics,
        "first_chart_y": first_chart["y"],
        "tooltip": True,
        "legend_toggle": True,
        "evidence_bridge": True,
        "table_fallback_rows": 3,
        "no_js_core_conclusions": True,
        "errors": errors,
    }
    (output_dir / "browser-gate.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:4175/")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.url, args.output_dir), ensure_ascii=False))


if __name__ == "__main__":
    main()
