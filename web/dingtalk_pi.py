"""Thin DingTalk adapter for the shared Pi ChannelEvent contract.

The DingTalk SDK/Stream callback layer calls this module with already verified
operator and conversation identifiers. Business state remains in Pi.
"""
from __future__ import annotations

from typing import Any

from web.pi_channel import PiChannelClient, task_run_id_from_response


class DingTalkPiAdapter:
    def __init__(self, client: PiChannelClient | None = None) -> None:
        self.client = client or PiChannelClient(channel="dingtalk")

    def submit_message(
        self,
        *,
        event_id: str,
        user_id: str,
        conversation_id: str,
        message_id: str,
        text: str,
    ) -> dict[str, Any]:
        return self.client.submit_message(
            event_id=event_id,
            external_user_id=user_id,
            conversation_id=conversation_id,
            message_id=message_id,
            text=text,
        )

    def submit_action(
        self,
        *,
        event_id: str,
        user_id: str,
        conversation_id: str,
        message_id: str,
        task_run_id: str,
        action: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return self.client.submit_action(
            event_id=event_id,
            external_user_id=user_id,
            conversation_id=conversation_id,
            message_id=message_id,
            task_run_id=task_run_id,
            action=action,
            payload=payload,
        )

    def wait_for_presentation(self, response: dict[str, Any]) -> dict[str, Any]:
        return self.client.wait_for_presentation(task_run_id_from_response(response))


def presentation_to_dingtalk_card(presentation: dict[str, Any]) -> dict[str, Any]:
    """Render a channel-neutral presentation into DingTalk ActionCard data.

    `callback` values are consumed by the DingTalk SDK adapter and sent back as
    ChannelEvent payloads; URLs are deliberately absent so buttons cannot bypass Pi.
    """
    actions: list[dict[str, Any]] = []
    for item in presentation.get("actions") or []:
        if not isinstance(item, dict):
            continue
        actions.append({
            "title": str(item.get("label") or "继续"),
            "callback": {
                "pi_action": True,
                "action_type": item.get("type"),
                "task_run_id": item.get("task_run_id"),
                "payload": item.get("payload") or {},
            },
        })
    return {
        "msgtype": "actionCard",
        "actionCard": {
            "title": str(presentation.get("title") or "Forge"),
            "text": str(presentation.get("markdown") or ""),
            "btnOrientation": "0",
            "btns": actions,
        },
    }
