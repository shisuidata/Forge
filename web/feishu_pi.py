"""Thin Feishu adapter for Pi ChannelEvent API.

No Forge Agent, Executor, Registry, database, or Memory imports are allowed here.
"""
from __future__ import annotations

import json
import logging
import threading
import hashlib

from diagnostics import record, request_id
from web.channel_delivery import DeliveryBusy, get_delivery_store
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
    action_progress_presentation,
    presentation_to_feishu_card,
    stable_channel_action_event_id,
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


def _send_card(open_id: str, card: dict, *, delivery_uuid: str | None = None) -> str | None:
    request = (
        CreateMessageRequest.builder()
        .receive_id_type("open_id")
        .request_body(
            CreateMessageRequestBody.builder()
            .receive_id(open_id)
            .msg_type("interactive")
            .uuid(delivery_uuid)
            .content(json.dumps(card, ensure_ascii=False))
            .build()
        )
        .build()
    )
    response = _get_client().im.v1.message.create(request)
    if not response.success():
        raise DeliveryRejected("Feishu rejected the presentation")
    return getattr(response.data, "message_id", None)


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
        raise DeliveryRejected("Feishu rejected the presentation")



class DeliveryRejected(RuntimeError):
    pass


def deliver_receipt(delivery_id: str, *, wait: bool = False, presentation: dict | None = None, progress_action: str | None = None) -> dict:
    """Fetch/re-send presentation only. Never submit a ChannelEvent or rerun a Task."""
    store = get_delivery_store()
    receipt = store.claim(delivery_id)
    delivery_started = False
    try:
        client = _get_pi_client()
        if presentation is None and progress_action and receipt["message_id"]:
            try:
                _update_card(receipt["message_id"], presentation_to_feishu_card(action_progress_presentation(progress_action), external_user_id=receipt["target_id"], conversation_id=receipt["conversation_id"]))
            except Exception:
                record("delivery_progress", delivery_id=delivery_id, task_run_id=receipt["task_run_id"], error_code="progress_delivery_unknown")
        if presentation is None:
            presentation = client.wait_for_presentation(receipt["task_run_id"]) if wait else client.get_presentation(receipt["task_run_id"])
        card = presentation_to_feishu_card(presentation, external_user_id=receipt["target_id"], conversation_id=receipt["conversation_id"])
        delivery_started = True
        if receipt["message_id"]:
            _update_card(receipt["message_id"], card)
            remote_id = receipt["message_id"]
        else:
            remote_id = _send_card(receipt["target_id"], card, delivery_uuid=receipt["delivery_id"])
        return store.public(store.finish(receipt, "delivered", message_id=remote_id))
    except DeliveryRejected:
        return store.public(store.finish(receipt, "failed", error_code="delivery_rejected"))
    except Exception:
        return store.public(store.finish(receipt, "unknown" if delivery_started else "failed", error_code="delivery_outcome_unknown" if delivery_started else "presentation_unavailable"))


def _process_message(open_id: str, conversation_id: str, message_id: str, text: str, chat_type: str) -> None:
    token = request_id.set("feishu_" + hashlib.sha256(message_id.encode()).hexdigest()[:40])
    try:
        accepted = _get_pi_client().submit_message(event_id=message_id, external_user_id=open_id,
            conversation_id=conversation_id, message_id=message_id, text=text, chat_type=chat_type)
        task_run_id = task_run_id_from_response(accepted)
        receipt = get_delivery_store().create(task_run_id=task_run_id, command_id=message_id,
            target_id=open_id, conversation_id=conversation_id)
        if receipt["status"] != "delivered":
            deliver_receipt(receipt["delivery_id"], wait=True)
    except DeliveryBusy:
        pass
    except Exception:
        record("channel_ingress", command_id=message_id, error_code="channel_result_unknown")
    finally:
        request_id.reset(token)


def _process_action(open_id: str, conversation_id: str, message_id: str, callback_event_id: str,
                    action_type: str, task_run_id: str, payload: dict) -> None:
    token = request_id.set("feishu_" + hashlib.sha256(callback_event_id.encode()).hexdigest()[:40])
    try:
        accepted = _get_pi_client().submit_action(event_id=callback_event_id, external_user_id=open_id,
            conversation_id=conversation_id, message_id=message_id, task_run_id=task_run_id, action=action_type, payload=payload)
        resolved_id = task_run_id_from_response(accepted)
        receipt = get_delivery_store().create(task_run_id=resolved_id, command_id=callback_event_id,
            target_id=open_id, conversation_id=conversation_id, message_id=message_id)
        accepted_presentation = accepted.get("presentation")
        presentation = accepted_presentation if isinstance(accepted_presentation, dict) and accepted_presentation.get("kind") != "progress" else None
        if receipt["status"] != "delivered":
            deliver_receipt(receipt["delivery_id"], wait=True, presentation=presentation, progress_action=action_type)
    except DeliveryBusy:
        pass
    except Exception:
        record("channel_ingress", task_run_id=task_run_id, command_id=callback_event_id, error_code="channel_result_unknown")
    finally:
        request_id.reset(token)



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
    except Exception:
        record("channel_event", error_code="event_invalid")


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
        callback_event_id = stable_channel_action_event_id(
            message_id,
            task_run_id,
            action_type,
            payload,
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
    except Exception:
        record("channel_event", error_code="action_invalid")
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
        except Exception:
            record("channel_connection", error_code="channel_disconnected")
        time.sleep(3)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    start_bot()
