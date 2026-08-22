from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

from web.dingtalk_pi import DingTalkPiAdapter, presentation_to_dingtalk_card
from web.pi_channel import PiChannelClient


def test_dingtalk_adapter_is_thin_and_uses_shared_channel_contract(monkeypatch):
    source = Path("web/dingtalk_pi.py").read_text(encoding="utf-8")
    assert "from agent" not in source
    assert "from forge" not in source
    assert "DATABASE_URL" not in source

    response = Mock()
    response.status_code = 202
    response.json.return_value = {"task": {"task_run_id": "tr_ding"}}
    request = Mock(return_value=response)
    monkeypatch.setattr("web.pi_channel.httpx.request", request)
    adapter = DingTalkPiAdapter(PiChannelClient(
        base_url="http://pi.test",
        service_key="channel-secret",
        channel="dingtalk",
    ))

    adapter.submit_message(
        event_id="evt_ding",
        user_id="ding_user",
        conversation_id="cid_ding",
        message_id="msg_ding",
        text="查询订单",
    )

    assert request.call_args.kwargs["json"]["channel"] == "dingtalk"
    assert request.call_args.kwargs["headers"] == {"X-Channel-Service-Key": "channel-secret"}


def test_dingtalk_card_actions_only_return_pi_callback_contract():
    card = presentation_to_dingtalk_card({
        "title": "Forge SQL 审核",
        "markdown": "SELECT 1",
        "actions": [{
            "type": "cancel_task",
            "label": "取消任务",
            "task_run_id": "tr_ding",
            "payload": {},
        }],
    })

    button = card["actionCard"]["btns"][0]
    assert "actionURL" not in button
    assert button["callback"] == {
        "pi_action": True,
        "action_type": "cancel_task",
        "task_run_id": "tr_ding",
        "payload": {},
    }
