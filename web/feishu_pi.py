"""Thin Feishu adapter for Pi ChannelEvent API.

No Forge Agent, Executor, Registry, database, or Memory imports are allowed here.
"""
from __future__ import annotations

import json
import logging
import threading

import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    CreateMessageRequest,
    CreateMessageRequestBody,
    PatchMessageRequest,
    PatchMessageRequestBody,
)
from lark_oapi.event.callback.model.p2_card_action_trigger import (
    CallBackToast,
    P2CardActionTrigger,
    P2CardActionTriggerResponse,
)

from config import cfg
from web.pi_channel import (
    PiChannelClient,
    presentation_to_feishu_card,
    task_run_id_from_response,
)

logger = logging.getLogger(__name__)
_client: lark.Client | None = None
_pi_client: PiChannelClient | None = None


def _get_client() -> lark.Client:
    global _client
    if _client is None:
        _client = (
            lark.Client.builder()
            .app_id(cfg.FEISHU_APP_ID)
            .app_secret(cfg.FEISHU_APP_SECRET)
            .log_level(lark.LogLevel.WARNING)
            .build()
        )
    return _client


def _get_pi_client() -> PiChannelClient:
    global _pi_client
    if _pi_client is None:
        _pi_client = PiChannelClient()
    return _pi_client


def _send_card(open_id: str, card: dict) -> None:
    request = (
        CreateMessageRequest.builder()
        .receive_id_type("open_id")
        .request_body(
            CreateMessageRequestBody.builder()
            .receive_id(open_id)
            .msg_type("interactive")
            .content(json.dumps(card, ensure_ascii=False))
            .build()
        )
        .build()
    )
    response = _get_client().im.v1.message.create(request)
    if not response.success():
        logger.error("Pi Feishu send card failed: %s %s", response.code, response.msg)


def _update_card(message_id: str, card: dict) -> None:
    request = (
        PatchMessageRequest.builder()
        .message_id(message_id)
        .request_body(
            PatchMessageRequestBody.builder()
            .content(json.dumps(card, ensure_ascii=False))
            .build()
        )
        .build()
    )
    response = _get_client().im.v1.message.patch(request)
    if not response.success():
        logger.error("Pi Feishu update card failed: %s %s", response.code, response.msg)


def _error_card(message: str) -> dict:
    return {
        "schema": "2.0",
        "header": {
            "title": {"tag": "plain_text", "content": "Forge"},
            "template": "red",
        },
        "body": {"elements": [{"tag": "markdown", "content": message}]},
    }


def _process_message(
    open_id: str,
    conversation_id: str,
    message_id: str,
    text: str,
    chat_type: str,
) -> None:
    try:
        accepted = _get_pi_client().submit_message(
            event_id=message_id,
            external_user_id=open_id,
            conversation_id=conversation_id,
            message_id=message_id,
            text=text,
            chat_type=chat_type,
        )
        task_run_id = task_run_id_from_response(accepted)
        presentation = _get_pi_client().wait_for_presentation(task_run_id)
        _send_card(
            open_id,
            presentation_to_feishu_card(
                presentation,
                external_user_id=open_id,
                conversation_id=conversation_id,
            ),
        )
    except Exception as exc:
        logger.exception("Pi Feishu message failed for %s: %s", open_id, exc)
        _send_card(open_id, _error_card("Forge 任务暂时无法处理，请稍后重试。"))


def _process_action(
    open_id: str,
    conversation_id: str,
    message_id: str,
    callback_event_id: str,
    action_type: str,
    task_run_id: str,
    payload: dict,
) -> None:
    try:
        accepted = _get_pi_client().submit_action(
            event_id=callback_event_id,
            external_user_id=open_id,
            conversation_id=conversation_id,
            message_id=message_id,
            task_run_id=task_run_id,
            action=action_type,
            payload=payload,
        )
        resolved_task_run_id = task_run_id_from_response(accepted)
        presentation = _get_pi_client().wait_for_presentation(resolved_task_run_id)
        _update_card(
            message_id,
            presentation_to_feishu_card(
                presentation,
                external_user_id=open_id,
                conversation_id=conversation_id,
            ),
        )
    except Exception as exc:
        logger.exception("Pi Feishu action failed for %s: %s", open_id, exc)
        _update_card(message_id, _error_card("操作失败，请重新发起或稍后重试。"))


def _on_message(data: lark.im.v1.P2ImMessageReceiveV1) -> None:
    try:
        message = data.event.message
        if message.message_type != "text":
            return
        content = json.loads(message.content or "{}")
        text = str(content.get("text") or "").strip()
        open_id = data.event.sender.sender_id.open_id
        if not text or not open_id:
            return
        conversation_id = getattr(message, "chat_id", None) or open_id
        chat_type = str(getattr(message, "chat_type", None) or "")
        threading.Thread(
            target=_process_message,
            args=(open_id, conversation_id, message.message_id, text, chat_type),
            daemon=True,
        ).start()
    except Exception as exc:
        logger.exception("Pi Feishu event parsing failed: %s", exc)


def _on_card_action(data: P2CardActionTrigger) -> P2CardActionTriggerResponse:
    response = P2CardActionTriggerResponse()
    try:
        value = data.event.action.value or {}
        if value.get("pi_action") is not True:
            response.toast = CallBackToast()
            response.toast.type = "warning"
            response.toast.content = "旧卡片已失效，请重新发起任务"
            return response
        open_id = data.event.operator.open_id
        message_id = data.event.context.open_message_id
        action_type = str(value.get("action_type") or "")
        task_run_id = str(value.get("task_run_id") or "")
        conversation_id = str(value.get("conversation_id") or open_id)
        payload = value.get("payload")
        if action_type == "provide_input" and isinstance(payload, dict):
            form_value = getattr(data.event.action, "form_value", None) or {}
            if isinstance(form_value, dict):
                payload = {**payload, "text": str(form_value.get("text") or "").strip()}
        callback_event_id = str(
            getattr(getattr(data, "header", None), "event_id", "")
            or f"{message_id}:{action_type}:{open_id}"
        )
        if not action_type or not task_run_id or not isinstance(payload, dict):
            raise ValueError("无效的 Pi 渠道操作")
        if action_type == "provide_input" and not payload.get("text"):
            raise ValueError("补充信息不能为空")
        response.toast = CallBackToast()
        response.toast.type = "info"
        response.toast.content = "Forge 正在处理..."
        threading.Thread(
            target=_process_action,
            args=(
                open_id,
                conversation_id,
                message_id,
                callback_event_id,
                action_type,
                task_run_id,
                payload,
            ),
            daemon=True,
        ).start()
    except Exception as exc:
        logger.exception("Pi Feishu card callback failed: %s", exc)
        response.toast = CallBackToast()
        response.toast.type = "error"
        response.toast.content = "操作失败"
    return response


def build_event_handler():
    return (
        lark.EventDispatcherHandler.builder(
            cfg.FEISHU_VERIFICATION_TOKEN,
            cfg.FEISHU_ENCRYPT_KEY,
        )
        .register_p2_im_message_receive_v1(_on_message)
        .register_p2_card_action_trigger(_on_card_action)
        .build()
    )


def start_bot() -> None:
    import time

    if not cfg.FEISHU_APP_ID or not cfg.FEISHU_APP_SECRET:
        logger.error("FEISHU_APP_ID / FEISHU_APP_SECRET 未配置，飞书 Bot 不会启动。")
        return
    handler = build_event_handler()
    while True:
        try:
            client = lark.ws.Client(
                cfg.FEISHU_APP_ID,
                cfg.FEISHU_APP_SECRET,
                event_handler=handler,
                log_level=lark.LogLevel.INFO,
            )
            logger.info("Forge Pi 飞书 Bot 已启动")
            client.start()
        except Exception as exc:
            logger.error("Pi Feishu WebSocket exited: %s", exc)
        time.sleep(3)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    start_bot()
