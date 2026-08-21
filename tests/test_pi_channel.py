from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pytest

from web.pi_channel import (
    PiChannelClient,
    PiChannelError,
    presentation_to_feishu_card,
    task_run_id_from_response,
)


def test_thin_feishu_pi_adapter_has_no_execution_layer_imports():
    source = Path("web/feishu_pi.py").read_text(encoding="utf-8")
    assert "from agent" not in source
    assert "from forge" not in source
    assert "DATABASE_URL" not in source
    assert "_execute_sql" not in source


def test_feishu_card_preserves_only_channel_action_contract():
    card = presentation_to_feishu_card(
        {
            "kind": "query_review",
            "title": "Forge SQL 审核",
            "markdown": "```sql\nSELECT 1\n```",
            "fields": [],
            "table": None,
            "actions": [
                {
                    "type": "approve_query",
                    "label": "确认执行",
                    "task_run_id": "tr_demo",
                    "payload": {
                        "query_run_id": "qr_demo",
                        "sql_hash": "sha256:" + "a" * 64,
                    },
                    "style": "primary",
                }
            ],
        },
        external_user_id="ou_demo",
        conversation_id="oc_demo",
    )
    button = card["body"]["elements"][-1]["columns"][0]["elements"][0]
    assert button["value"] == {
        "pi_action": True,
        "action_type": "approve_query",
        "task_run_id": "tr_demo",
        "payload": {
            "query_run_id": "qr_demo",
            "sql_hash": "sha256:" + "a" * 64,
        },
        "user_id": "ou_demo",
        "conversation_id": "oc_demo",
    }


def test_pi_channel_client_sends_dedicated_service_key(monkeypatch):
    response = Mock()
    response.status_code = 202
    response.json.return_value = {
        "status": "accepted",
        "task": {"task_run_id": "tr_demo"},
    }
    request = Mock(return_value=response)
    monkeypatch.setattr("web.pi_channel.httpx.request", request)
    client = PiChannelClient(
        base_url="http://pi.test",
        service_key="channel-secret",
    )
    result = client.submit_message(
        event_id="evt_demo",
        external_user_id="ou_demo",
        conversation_id="oc_demo",
        message_id="om_demo",
        text="查询订单",
    )
    assert task_run_id_from_response(result) == "tr_demo"
    assert request.call_args.kwargs["headers"] == {
        "X-Channel-Service-Key": "channel-secret"
    }
    assert request.call_args.kwargs["json"]["channel"] == "feishu"


def test_pi_channel_client_requires_channel_service_key():
    with pytest.raises(PiChannelError, match="PI_CHANNEL_SERVICE_KEY"):
        PiChannelClient(base_url="http://pi.test", service_key="")
