from __future__ import annotations

import re
from pathlib import Path


COURSE_DIR = Path("docs/architecture-course")
WEBSITE_DIR = Path("website/src/content/docs/course")
IMAGE_LINK = re.compile(r"!\[[^\]]*\]\(([^)]+)\)")


def _body(text: str) -> str:
    """Return Markdown body while allowing website-only frontmatter changes."""
    if not text.startswith("---\n"):
        return text.strip()
    _, _, rest = text.partition("---\n")
    _, separator, body = rest.partition("\n---\n")
    return body.strip() if separator else text.strip()


def test_course_and_website_bodies_match():
    source_files = sorted(COURSE_DIR.glob("*.md"))
    assert source_files, "architecture course is missing"

    assert {path.name for path in source_files} == {
        path.name for path in WEBSITE_DIR.glob("*.md")
    }
    for source in source_files:
        mirror = WEBSITE_DIR / source.name
        assert _body(source.read_text(encoding="utf-8")) == _body(
            mirror.read_text(encoding="utf-8")
        ), f"course mirror drifted: {source.name}"


def test_course_local_images_and_sources_exist():
    for source in COURSE_DIR.glob("*.md"):
        text = source.read_text(encoding="utf-8")
        for target in IMAGE_LINK.findall(text):
            if target.startswith(("http://", "https://")):
                continue
            assert (source.parent / target).exists(), f"missing image: {source}:{target}"

    source_assets = {
        path.name: path.read_bytes()
        for path in (COURSE_DIR / "assets").iterdir()
        if path.is_file()
    }
    website_assets = {
        path.name: path.read_bytes()
        for path in (WEBSITE_DIR / "assets").iterdir()
        if path.is_file()
    }
    assert source_assets == website_assets

    svg_names = {Path(name).stem for name in source_assets if name.endswith(".svg")}
    source_names = {Path(name).stem for name in source_assets if name.endswith(".mmd")}
    png_names = {Path(name).stem for name in source_assets if name.endswith(".png")}
    assert svg_names == source_names == png_names
