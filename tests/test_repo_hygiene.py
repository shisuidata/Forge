"""Repository hygiene gates for the REQ-2026-09-21-069 governance round.

These tests assert observable repository contracts: legacy bulk assets stay out
of Git, tracked files respect a size ceiling, the monitored paths collect no new
untracked noise, and web/router.py remains a bounded aggregator.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(
    not (REPO_ROOT / ".git").is_dir(), reason="repository metadata not available"
)

# Paths intentionally removed from Git tracking in REQ-2026-09-21-069.
LEGACY_TRACKED_ASSET_PATTERNS = [
    "tests/accuracy/results/method_k",
    "tests/text-to-sql-failures/test.db",
    "tests/datasets/large/database.db",
    "web/static/charts",
]

# Maximum size for any tracked file. The largest legitimate file today is a
# ~5.3 MB benchmark JSON; the ceiling leaves headroom without readmitting the
# previously tracked 25.4 MB SQLite database.
TRACKED_FILE_SIZE_CEILING = 8 * 1024 * 1024
SIZE_ALLOWLIST: dict[str, int] = {}

# Paths that must show no untracked noise in `git status`.
NO_UNTRACKED_NOISE_PATHS = [
    "demo",
    "registry/data",
    "tests/datasets",
    "web/static/charts",
]

# web/router.py is an aggregator; route implementations live in web/routes/.
ROUTER_AGGREGATOR_LINE_CEILING = 450


def _run_git(*args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return completed.stdout


def test_legacy_tracked_assets_removed():
    for pattern in LEGACY_TRACKED_ASSET_PATTERNS:
        tracked = [line for line in _run_git("ls-files", "--", pattern).splitlines() if line]
        assert tracked == [], f"legacy asset still tracked under {pattern}: {tracked}"


def test_tracked_file_size_ceiling():
    """Measure index blob sizes so broken worktree symlinks cannot mask bloat."""
    listing = _run_git("ls-files", "-s", "-z")
    entries: list[tuple[str, str]] = []
    for record in listing.split("\0"):
        if not record:
            continue
        meta, path = record.split("\t", 1)
        mode, sha, _stage = meta.split()
        if mode in {"120000", "160000"}:
            # symlink blobs hold only the link text; gitlinks have no blob here
            continue
        entries.append((sha, path))
    sizes = _blob_sizes({sha for sha, _ in entries})
    oversized = [
        f"{path} ({sizes.get(sha, 0)} bytes > {SIZE_ALLOWLIST.get(path, TRACKED_FILE_SIZE_CEILING)})"
        for sha, path in entries
        if sizes.get(sha, 0) > SIZE_ALLOWLIST.get(path, TRACKED_FILE_SIZE_CEILING)
    ]
    assert oversized == [], f"tracked files exceed size ceiling: {oversized}"


def _blob_sizes(shas: set[str]) -> dict[str, int]:
    if not shas:
        return {}
    completed = subprocess.run(
        ["git", "cat-file", "--batch-check=%(objectname) %(objectsize)"],
        cwd=REPO_ROOT,
        input="\n".join(sorted(shas)),
        capture_output=True,
        text=True,
        check=True,
    )
    sizes: dict[str, int] = {}
    for line in completed.stdout.splitlines():
        sha, _, size = line.rpartition(" ")
        sizes[sha] = int(size)
    return sizes


def test_no_untracked_noise():
    untracked = _run_git(
        "ls-files", "--others", "--exclude-standard", "--", *NO_UNTRACKED_NOISE_PATHS
    )
    assert untracked.strip() == "", f"untracked noise appeared: {untracked!r}"


def test_router_aggregator_size_ceiling():
    router_lines = len((REPO_ROOT / "web" / "router.py").read_text(encoding="utf-8").splitlines())
    assert router_lines <= ROUTER_AGGREGATOR_LINE_CEILING, (
        f"web/router.py grew to {router_lines} lines "
        f"(ceiling {ROUTER_AGGREGATOR_LINE_CEILING}); move new routes into web/routes/"
    )
