"""
Feishu Bot handler.
Receives im.message.receive_v1 events and card action callbacks.
Sends interactive cards for SQL review.
"""
from __future__ import annotations
import json
import logging

import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    CreateMessageRequest,
    CreateMessageRequestBody,
    ReplyMessageRequest,
    ReplyMessageRequestBody,
)

from config import cfg
from agent import agent as forge_agent

logger = logging.getLogger(__name__)


# ── Feishu client ─────────────────────────────────────────────────────────────

_client = lark.Client.builder() \
    .app_id(cfg.FEISHU_APP_ID) \
    .app_secret(cfg.FEISHU_APP_SECRET) \
    .log_level(lark.LogLevel.ERROR) \
    .build()


# ── event dispatcher ──────────────────────────────────────────────────────────

def _on_message(data: lark.im.v1.P2ImMessageReceiveV1) -> None:
    msg     = data.event.message
    sender  = data.event.sender
    user_id = sender.sender_id.open_id

    # only handle text messages
    if msg.message_type != "text":
        _send_text(msg.chat_id, "目前只支持文字消息。")
        return

    try:
        content = json.loads(msg.content)
        text = content.get("text", "").strip()
    except Exception:
        return

    if not text:
        return

    logger.info("message from %s: %s", user_id, text)
    response = forge_agent.process(user_id, text)

    if response.action == "sql_review":
        _send_sql_review_card(msg.chat_id, response.sql, user_id)
    else:
        _send_text(msg.chat_id, response.text or "（无响应）")


def _on_card_action(data: lark.card.v1.P2CardActionTrigger) -> lark.card.v1.P2CardActionTriggerResponse:
    action  = data.action.value or {}
    user_id = data.operator.open_id
    op      = action.get("op")
    chat_id = action.get("chat_id")

    if op == "approve":
        response = forge_agent.approve(user_id)
        _send_text(chat_id, response.text)
        # TODO: execute SQL against DB and show results
    elif op == "cancel":
        response = forge_agent.cancel(user_id)
        _send_text(chat_id, response.text)

    return lark.card.v1.P2CardActionTriggerResponse.builder().build()


dispatcher = (
    lark.EventDispatcherHandler.builder(
        cfg.FEISHU_ENCRYPT_KEY,
        cfg.FEISHU_VERIFICATION_TOKEN,
        lark.LogLevel.ERROR,
    )
    .register_p2_im_message_receive_v1(_on_message)
    .register_p2_card_action_trigger(_on_card_action)
    .build()
)


# ── card builders ─────────────────────────────────────────────────────────────

def _sql_review_card(sql: str, user_id: str, chat_id: str) -> dict:
    return {
        "config": {"wide_screen_mode": True},
        "elements": [
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": "**📋 生成的 SQL，请确认后执行：**"},
            },
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"```sql\n{sql}\n```"},
            },
            {
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "✅ 执行"},
                        "type": "primary",
                        "value": {"op": "approve", "user_id": user_id, "chat_id": chat_id},
                    },
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "❌ 取消"},
                        "type": "danger",
                        "value": {"op": "cancel", "user_id": user_id, "chat_id": chat_id},
                    },
                ],
            },
        ],
    }


# ── send helpers ──────────────────────────────────────────────────────────────

def _send_text(chat_id: str, text: str) -> None:
    req = (
        CreateMessageRequest.builder()
        .receive_id_type("chat_id")
        .request_body(
            CreateMessageRequestBody.builder()
            .receive_id(chat_id)
            .msg_type("text")
            .content(json.dumps({"text": text}))
            .build()
        )
        .build()
    )
    resp = _client.im.v1.message.create(req)
    if not resp.success():
        logger.error("send_text failed: %s", resp.msg)


def _send_sql_review_card(chat_id: str, sql: str, user_id: str) -> None:
    card = _sql_review_card(sql, user_id, chat_id)
    req = (
        CreateMessageRequest.builder()
        .receive_id_type("chat_id")
        .request_body(
            CreateMessageRequestBody.builder()
            .receive_id(chat_id)
            .msg_type("interactive")
            .content(json.dumps(card))
            .build()
        )
        .build()
    )
    resp = _client.im.v1.message.create(req)
    if not resp.success():
        logger.error("send_card failed: %s", resp.msg)
