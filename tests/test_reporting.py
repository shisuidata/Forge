from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from forge.reporting import ReportStore


def _source() -> dict:
    return {
        "report_id": "rp_demo001",
        "task_run_id": "tr_demo001",
        "org_id": "org_demo",
        "team_id": "team_demo",
        "user_id": "user_demo",
        "revision": 1,
        "bundle_hash": "sha256:" + "a" * 64,
        "title": "地区销售额分析",
        "business_report": {
            "title": "地区销售额分析",
            "executive_summary": "华东销售额最高。",
            "recommendations": [{"action": "继续关注华东", "rationale": "贡献最高"}],
        },
        "analysis": {
            "method_summary": {
                "objective": "比较各地区销售额",
                "dimensions": ["region"],
                "comparison_baseline": "地区横向对比",
                "approach_steps": ["按地区汇总", "比较销售额"],
            },
            "summary": "华东销售额最高。",
            "findings": [{"statement": "华东销售额最高。"}],
            "limitations": ["仅覆盖当前查询时间范围"],
        },
        "query_result": {
            "columns": ["region", "sales"],
            "rows": [["华东", 120], ["华南", 80]],
        },
        "charts": [{
            "chart_type": "bar", "title": "各地区销售额", "dimension": "region",
            "measures": ["sales"], "alt_text": "各地区销售额柱状图",
        }],
        "technical_report": {
            "title": "地区销售额分析 · 技术报告",
            "sql": "SELECT region, SUM(amount) AS sales FROM orders GROUP BY region",
            "approval": {"approved": True},
            "execution": {"row_count": 2},
            "lineage": {"registry_version": "v1"},
            "decision_log": [{"stage": "analysis", "decision": "按地区比较", "rationale": "用户问题要求地区维度"}],
        },
    }


def test_report_store_builds_immutable_html_and_pptx(tmp_path: Path):
    store = ReportStore(str(tmp_path / "reports.db"), str(tmp_path / "artifacts"))
    created = store.create(_source())
    assert created["status"] == "publishing"

    published = store.build("rp_demo001")

    assert published["status"] == "published"
    assert published["pptx_status"] == "ready"
    html = store.file("rp_demo001", "index.html").read_text()
    technical = store.file("rp_demo001", "technical.html").read_text()
    assert "华东销售额最高" in html
    assert "<svg" in html
    assert "SELECT region" in technical
    assert "hidden chain-of-thought" in technical
    assert (tmp_path / "artifacts").stat().st_mode & 0o777 == 0o700
    assert (tmp_path / "reports.db").stat().st_mode & 0o077 == 0
    import sqlite3
    with sqlite3.connect(tmp_path / "reports.db") as conn:
        attempts = conn.execute("SELECT stage, status FROM report_attempts ORDER BY started_at").fetchall()
    assert ("html", "succeeded") in attempts
    assert ("pptx", "succeeded") in attempts


def test_report_store_is_idempotent_and_rejects_bundle_drift(tmp_path: Path):
    store = ReportStore(str(tmp_path / "reports.db"), str(tmp_path / "artifacts"))
    first = store.create(_source())
    assert store.create(_source())["report_id"] == first["report_id"]
    changed = _source()
    changed["bundle_hash"] = "sha256:" + "b" * 64
    with pytest.raises(ValueError, match="different bundle"):
        store.create(changed)


def test_report_store_restart_marks_running_attempt_interrupted_without_replay(tmp_path: Path):
    import sqlite3
    db = tmp_path / "reports.db"
    store = ReportStore(str(db), str(tmp_path / "artifacts"))
    store.create(_source())
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO report_attempts VALUES ('rpa_running', 'rp_demo001', 'pdf', 'running', ?, NULL, NULL)",
            (datetime.now(timezone.utc).isoformat(),),
        )
    ReportStore(str(db), str(tmp_path / "artifacts"))
    with sqlite3.connect(db) as conn:
        attempt = conn.execute("SELECT status FROM report_attempts WHERE attempt_id='rpa_running'").fetchone()
        report = conn.execute("SELECT status FROM reports WHERE report_id='rp_demo001'").fetchone()
    assert attempt == ("interrupted",)
    assert report == ("failed",)


def test_report_store_rejects_hidden_reasoning(tmp_path: Path):
    store = ReportStore(str(tmp_path / "reports.db"), str(tmp_path / "artifacts"))
    source = _source()
    source["analysis"]["summary"] = "<think>private reasoning</think>"
    with pytest.raises(ValueError, match="forbidden reasoning"):
        store.create(source)


@pytest.mark.asyncio
async def test_business_share_is_expiring_revocable_and_cannot_open_technical_report(client, monkeypatch, tmp_path):
    import web.routes.reports as reports
    from config import cfg

    store = ReportStore(str(tmp_path / "reports.db"), str(tmp_path / "artifacts"))
    store.create(_source())
    store.build("rp_demo001")
    monkeypatch.setattr(reports, "get_report_store", lambda: store)
    share = store.create_share(
        "rp_demo001", (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    )
    old_auth = cfg.AUTH_ENABLED
    cfg.AUTH_ENABLED = True
    try:
        landing = await client.get(f"/reports/share/{share['share_id']}")
        assert landing.status_code == 200
        assert share["token"] not in landing.text
        exchange = await client.post(
            f"/reports/share/{share['share_id']}/exchange", json={"token": share["token"]}
        )
        assert exchange.status_code == 200
        page = await client.get("/reports/rp_demo001")
        assert page.status_code == 200
        technical = await client.get("/reports/rp_demo001/technical")
        assert technical.status_code == 401
        store.revoke_share("rp_demo001", share["share_id"])
        revoked = await client.get("/reports/rp_demo001")
        assert revoked.status_code == 401
    finally:
        cfg.AUTH_ENABLED = old_auth


@pytest.mark.asyncio
async def test_report_api_requires_pi_auth_and_serves_authenticated_outputs(client, monkeypatch, tmp_path):
    import web.routes.reports as reports

    store = ReportStore(str(tmp_path / "reports.db"), str(tmp_path / "artifacts"))
    reports.get_report_store.cache_clear()
    monkeypatch.setattr(reports, "get_report_store", lambda: store)

    unauthorized = await client.post("/api/internal/reports", json=_source())
    assert unauthorized.status_code == 401

    from config import cfg
    old_keys = cfg.PI_SERVICE_API_KEYS
    cfg.PI_SERVICE_API_KEYS = ["pi-secret"]
    try:
        created = await client.post(
            "/api/internal/reports",
            json=_source(),
            headers={"X-Pi-Service-Key": "pi-secret"},
        )
        assert created.status_code == 202
        status = await client.get(
            "/api/internal/reports/rp_demo001",
            headers={"X-Pi-Service-Key": "pi-secret"},
        )
        assert status.json()["report"]["status"] == "published"
        page = await client.get("/reports/rp_demo001")
        assert page.status_code == 200
        assert "地区销售额分析" in page.text
        pptx = await client.get("/reports/rp_demo001/download/pptx")
        assert pptx.status_code == 200
    finally:
        cfg.PI_SERVICE_API_KEYS = old_keys
