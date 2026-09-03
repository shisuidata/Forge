from __future__ import annotations

import re
from pathlib import Path


MARKDOWN_LINK = re.compile(r"\[[^\]]+\]\(([^)]+)\)")


def test_public_markdown_local_links_resolve():
    """Keep public docs shippable by catching broken relative Markdown links."""
    roots = [Path("README.md"), Path("README.zh-CN.md"), *Path("docs").rglob("*.md")]
    docs = [path for path in roots if path.exists() and "conversation-logs" not in path.parts]
    missing: list[tuple[str, str]] = []

    for path in docs:
        text = path.read_text(encoding="utf-8")
        for match in MARKDOWN_LINK.finditer(text):
            target = match.group(1).strip()
            if target.startswith(("http://", "https://", "mailto:", "#")) or "://" in target:
                continue
            target = target.split("#", 1)[0]
            if not target or target.startswith("/"):
                continue
            if not (path.parent / target).exists():
                missing.append((str(path), target))

    assert missing == []
