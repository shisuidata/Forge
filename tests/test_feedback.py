from __future__ import annotations

import pytest

from agent import feedback


@pytest.mark.asyncio
async def test_feedback_submit_and_list_pending(tmp_path, monkeypatch):
    monkeypatch.setattr(feedback.audit.cfg, "AUDIT_DB_PATH", str(tmp_path / "audit.db"))

    feedback_id = await feedback.submit(
        user_id="u1",
        feedback_type="wrong_result",
        message="结果少算了取消订单过滤",
        question="统计销售额",
        sql="SELECT 1",
        expected="应该只统计已完成订单",
    )
    pending = await feedback.list_pending()

    assert feedback_id > 0
    assert len(pending) == 1
    assert pending[0]["message"] == "结果少算了取消订单过滤"
    assert pending[0]["status"] == "pending"


@pytest.mark.asyncio
async def test_feedback_rejects_empty_message(tmp_path, monkeypatch):
    monkeypatch.setattr(feedback.audit.cfg, "AUDIT_DB_PATH", str(tmp_path / "audit.db"))

    with pytest.raises(ValueError):
        await feedback.submit(
            user_id="u1",
            feedback_type="wrong_result",
            message=" ",
        )
