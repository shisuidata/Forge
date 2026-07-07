from __future__ import annotations

import pytest

from agent import audit


@pytest.mark.asyncio
async def test_audit_uses_configured_db_path_and_updates_latest_pending(tmp_path, monkeypatch):
    db_path = tmp_path / "audit" / "forge_audit.db"
    monkeypatch.setattr(audit.cfg, "AUDIT_DB_PATH", str(db_path))

    record_id = await audit.log(
        user_id="u1",
        user_message="查一下销售额",
        forge_json={"scan": "orders"},
        sql="SELECT 1",
        status="pending",
    )

    updated = await audit.update_latest_pending(
        "u1",
        "approved",
        row_count=3,
        execution_ms=12,
    )
    records, total = await audit.search(status="approved", keyword="销售额")

    assert db_path.exists()
    assert updated == record_id
    assert total == 1
    assert records[0]["row_count"] == 3
    assert records[0]["execution_ms"] == 12


@pytest.mark.asyncio
async def test_audit_update_latest_pending_returns_none_without_pending(tmp_path, monkeypatch):
    monkeypatch.setattr(audit.cfg, "AUDIT_DB_PATH", str(tmp_path / "audit.db"))

    updated = await audit.update_latest_pending("missing-user", "cancelled")

    assert updated is None
