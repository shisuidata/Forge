"""Model replies rendered in the console AI panel must never become live HTML."""
from __future__ import annotations

from pathlib import Path

import pytest

playwright = pytest.importorskip("playwright.async_api")

ROOT = Path(__file__).parents[1]
STATIC = ROOT / "web" / "static"
PAYLOADS = (
    '<img src=x onerror="window.__xss=1">',
    "<script>window.__xss=1</script>",
    "[点击](javascript:window.__xss=1)",
    '<a href="javascript:window.__xss=1">x</a>',
    '<svg onload="window.__xss=1"></svg>',
    '<iframe srcdoc="<script>parent.__xss=1</script>"></iframe>',
    '<a href="//evil.example/x">协议相对地址</a>',
)


@pytest.fixture
async def page():
    async with playwright.async_playwright() as runner:
        try:
            browser = await runner.chromium.launch()
        except Exception as exc:  # Browser binaries are optional outside the e2e environment.
            pytest.skip(f"Chromium unavailable: {exc}")
        page = await browser.new_page()
        await page.set_content('<div id="out"></div>')
        await page.add_script_tag(path=str(STATIC / "vendor" / "marked.umd.js"))
        await page.add_script_tag(path=str(STATIC / "safe-markdown.js"))
        yield page
        await browser.close()


async def _render(page, markdown: str) -> dict:
    return await page.evaluate("""async (markdown) => {
        const out = document.getElementById('out');
        out.replaceChildren(renderSafeMarkdown(markdown));
        await new Promise(resolve => setTimeout(resolve, 50));
        return {
            html: out.innerHTML,
            xss: window.__xss === 1,
            handlers: [...out.querySelectorAll('*')].flatMap(el => [...el.attributes].map(a => a.name))
                .filter(name => name.startsWith('on')),
            active: out.querySelectorAll('script,img,svg,iframe,style,object,embed').length,
            hrefs: [...out.querySelectorAll('a')].map(a => a.getAttribute('href')),
        };
    }""", markdown)


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", PAYLOADS)
async def test_untrusted_markdown_cannot_run_script_or_keep_active_markup(page, payload):
    rendered = await _render(page, f"结论如下：\n\n{payload}\n")
    assert rendered["xss"] is False
    assert rendered["handlers"] == []
    assert rendered["active"] == 0
    assert all(href is None or href.startswith(("https:", "http:", "mailto:", "/", "#")) and not href.startswith("//")
               for href in rendered["hrefs"])


@pytest.mark.asyncio
async def test_safe_markdown_keeps_the_formatting_the_panel_styles(page):
    rendered = await _render(page, "## 结论\n\n**订单**增长，见[报告](https://forge.test/r)。\n\n"
                                   "| 地区 | 销售额 |\n|:--|--:|\n| 华东 | 120 |\n\n- `SELECT 1`\n")
    html = rendered["html"]
    for fragment in ("<h2>结论</h2>", "<strong>订单</strong>", "<table>", '<td align="right">120</td>', "<code>SELECT 1</code>"):
        assert fragment in html
    assert rendered["hrefs"] == ["https://forge.test/r"]


def test_console_panel_renders_model_replies_only_through_safe_markdown():
    base = (ROOT / "web" / "templates" / "base.html").read_text()
    assert "marked.parse" not in base
    assert '<script src="/static/safe-markdown.js' in base
    assert "renderSafeMarkdown(content)" in base
