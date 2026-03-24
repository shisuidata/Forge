"""
飞书 WebSocket Bot — Forge Agent 的飞书接入层。

特性：
    - WebSocket 长连接，无需公网 IP，内网直接可用
    - 文字消息 → agent.process()
    - sql_review → 发送带「确认执行」「取消」按钮的交互卡片
    - metric_clarification → 发送带「确认入库」「取消」按钮的交互卡片
    - 卡片按钮点击 → P2CardActionTrigger 回调，直接路由到 approve/cancel/confirm/reject
    - approve 后自动执行 SQL 并返回结果
    - message_id 去重，防止重试投递导致重复响应
    - 收到消息立即用👀表情回应，处理完换成✅

用法：
    python web/feishu.py              # 独立运行（阻塞）
    from web.feishu import start_bot  # 在其他模块中启动
"""
from __future__ import annotations

import json
import logging
import threading

import io
import queue

import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    CreateImageRequest,
    CreateImageRequestBody,
    CreateMessageRequest,
    CreateMessageRequestBody,
    CreateMessageReactionRequest,
    CreateMessageReactionRequestBody,
    Emoji,
    PatchMessageRequest,
    PatchMessageRequestBody,
)
from lark_oapi.api.cardkit.v1 import (
    CreateCardRequest,
    CreateCardRequestBody,
    ContentCardElementRequest,
    ContentCardElementRequestBody,
    SettingsCardRequest,
    SettingsCardRequestBody,
    UpdateCardRequest,
    UpdateCardRequestBody,
)
from lark_oapi.event.callback.model.p2_card_action_trigger import (
    P2CardActionTrigger,
    P2CardActionTriggerResponse,
    CallBackToast,
    CallBackCard,
)

from config import cfg
from agent import agent
from agent.session import store
from forge.executor import execute_with_data as _execute_sql
from forge.chart import generate as _generate_chart, generate_image as _generate_chart_image

logger = logging.getLogger(__name__)


# ── 每用户串行队列：确保同一用户的消息按序处理，避免并发导致回复错乱 ──────────────

_user_queues:  dict[str, queue.Queue] = {}
_user_workers: dict[str, threading.Thread] = {}
_user_q_lock = threading.Lock()


def _get_or_create_queue(open_id: str) -> queue.Queue:
    with _user_q_lock:
        if open_id not in _user_queues:
            _user_queues[open_id] = queue.Queue()
        return _user_queues[open_id]


def _user_worker(open_id: str, q: queue.Queue) -> None:
    """每用户一个 worker，顺序处理队列中的消息，30s 无新消息则退出。"""
    while True:
        try:
            text = q.get(timeout=30)
        except queue.Empty:
            with _user_q_lock:
                _user_queues.pop(open_id, None)
                _user_workers.pop(open_id, None)
            break
        try:
            _dispatch(open_id, text)
        except Exception as exc:
            logger.exception("dispatch error for %s: %s", open_id, exc)
            try:
                _send_info_card(open_id, f"❌ 处理出错：{exc}", template="red")
            except Exception as send_exc:
                logger.warning("Failed to send error card to %s: %s", open_id, send_exc)
        finally:
            q.task_done()


def _enqueue(open_id: str, text: str) -> None:
    """将消息加入用户队列，按需启动 worker 线程。"""
    q = _get_or_create_queue(open_id)
    q.put(text)
    with _user_q_lock:
        worker = _user_workers.get(open_id)
        if worker is None or not worker.is_alive():
            t = threading.Thread(target=_user_worker, args=(open_id, q), daemon=True)
            t.start()
            _user_workers[open_id] = t


# ── 去重：记录已处理的 message_id，防止重试投递导致重复响应 ────────────────────

_seen_messages: set[str] = set()
_seen_lock = threading.Lock()
_MAX_SEEN = 500


def _is_duplicate(message_id: str) -> bool:
    global _seen_messages
    with _seen_lock:
        if message_id in _seen_messages:
            return True
        _seen_messages.add(message_id)
        if len(_seen_messages) > _MAX_SEEN:
            half = list(_seen_messages)[:_MAX_SEEN // 2]
            _seen_messages -= set(half)
        return False


# ── 关键词检测（文字确认作为兜底）────────────────────────────────────────────

_CONFIRM_WORDS = {"确认", "是", "对", "yes", "approve", "确定", "好的", "ok", "✅"}
_CANCEL_WORDS  = {"取消", "否", "不", "no", "cancel", "算了", "退出", "❌"}


def _is_confirm(text: str) -> bool:
    t = text.strip().lower()
    return t in _CONFIRM_WORDS or any(w in t for w in _CONFIRM_WORDS)


def _is_cancel(text: str) -> bool:
    t = text.strip().lower()
    return t in _CANCEL_WORDS or any(w in t for w in _CANCEL_WORDS)


# ── 飞书 Client 单例 ──────────────────────────────────────────────────────────

_client: lark.Client | None = None
_client_lock = threading.Lock()


def _get_client() -> lark.Client:
    global _client
    with _client_lock:
        if _client is None:
            _client = (
                lark.Client.builder()
                .app_id(cfg.FEISHU_APP_ID)
                .app_secret(cfg.FEISHU_APP_SECRET)
                .log_level(lark.LogLevel.WARNING)
                .build()
            )
    return _client


# ── 消息发送 ──────────────────────────────────────────────────────────────────

def _send_text(open_id: str, text: str) -> None:
    client = _get_client()
    req = (
        CreateMessageRequest.builder()
        .receive_id_type("open_id")
        .request_body(
            CreateMessageRequestBody.builder()
            .receive_id(open_id)
            .msg_type("text")
            .content(json.dumps({"text": text}, ensure_ascii=False))
            .build()
        )
        .build()
    )
    resp = client.im.v1.message.create(req)
    if not resp.success():
        logger.error("send_text failed: %s %s", resp.code, resp.msg)


def _send_card(open_id: str, card: dict) -> None:
    client = _get_client()
    req = (
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
    resp = client.im.v1.message.create(req)
    if not resp.success():
        logger.error("send_card failed: %s %s", resp.code, resp.msg)


# ── 流式卡片 ──────────────────────────────────────────────────────────────────

_STREAMING_ELEMENT_ID = "stream_md"


def _create_streaming_card() -> str | None:
    """创建流式卡片实体，返回 card_id。"""
    client = _get_client()
    card_json = json.dumps({
        "schema": "2.0",
        "config": {
            "streaming_mode": True,
            "streaming_config": {
                "print_frequency_ms": {"default": 50},
                "print_step": {"default": 2},
                "print_strategy": "fast",
            },
        },
        "header": {
            "title": {"tag": "plain_text", "content": "Forge"},
            "template": "blue",
        },
        "body": {
            "elements": [
                {
                    "tag": "markdown",
                    "content": "",
                    "element_id": _STREAMING_ELEMENT_ID,
                },
            ],
        },
    }, ensure_ascii=False)
    try:
        req = (
            CreateCardRequest.builder()
            .request_body(
                CreateCardRequestBody.builder()
                .type("card_json")
                .data(card_json)
                .build()
            )
            .build()
        )
        resp = client.cardkit.v1.card.create(req)
        if resp.success() and resp.data:
            return resp.data.card_id
        logger.warning("create streaming card failed: %s %s", resp.code, resp.msg)
    except Exception as exc:
        logger.warning("create streaming card error: %s", exc)
    return None


def _send_card_by_id(open_id: str, card_id: str) -> None:
    """使用 card_id 发送卡片消息。"""
    client = _get_client()
    content = json.dumps({"type": "card", "data": {"card_id": card_id}})
    req = (
        CreateMessageRequest.builder()
        .receive_id_type("open_id")
        .request_body(
            CreateMessageRequestBody.builder()
            .receive_id(open_id)
            .msg_type("interactive")
            .content(content)
            .build()
        )
        .build()
    )
    resp = client.im.v1.message.create(req)
    if not resp.success():
        logger.error("send_card_by_id failed: %s %s", resp.code, resp.msg)


def _stream_update_text(card_id: str, text: str) -> None:
    """推送流式文本（完整内容，平台自动 diff 出增量部分做打字机效果）。"""
    client = _get_client()
    try:
        req = (
            ContentCardElementRequest.builder()
            .card_id(card_id)
            .element_id(_STREAMING_ELEMENT_ID)
            .request_body(
                ContentCardElementRequestBody.builder()
                .content(text)
                .sequence(_next_seq(card_id))
                .build()
            )
            .build()
        )
        resp = client.cardkit.v1.card_element.content(req)
        if not resp.success():
            logger.debug("stream_update failed: %s %s", resp.code, resp.msg)
    except Exception as exc:
        logger.debug("stream_update error: %s", exc)


def _close_streaming(card_id: str) -> None:
    """关闭流式模式，恢复交互能力。"""
    client = _get_client()
    try:
        req = (
            SettingsCardRequest.builder()
            .card_id(card_id)
            .request_body(
                SettingsCardRequestBody.builder()
                .settings(json.dumps({"config": {"streaming_mode": False}}))
                .sequence(_next_seq(card_id))
                .build()
            )
            .build()
        )
        resp = client.cardkit.v1.card.settings(req)
        if not resp.success():
            logger.warning("close_streaming failed: %s %s", resp.code, resp.msg)
    except Exception as exc:
        logger.warning("close_streaming error: %s", exc)


# 全局递增序列号，CardKit API 要求 sequence 严格单调递增
_card_seq_lock = threading.Lock()
_card_sequences: dict[str, int] = {}


def _next_seq(card_id: str) -> int:
    """获取卡片的下一个 sequence 值。"""
    with _card_seq_lock:
        seq = _card_sequences.get(card_id, 0) + 1
        _card_sequences[card_id] = seq
        return seq


def _update_card_by_id(card_id: str, card_json: dict) -> None:
    """更新卡片实体的完整内容（关闭流式后用，加入按钮等交互组件）。"""
    from lark_oapi.api.cardkit.v1.model.card import Card
    client = _get_client()
    try:
        card_obj = (
            Card.builder()
            .type("card_json")
            .data(json.dumps(card_json, ensure_ascii=False))
            .build()
        )
        req = (
            UpdateCardRequest.builder()
            .card_id(card_id)
            .request_body(
                UpdateCardRequestBody.builder()
                .card(card_obj)
                .sequence(_next_seq(card_id))
                .build()
            )
            .build()
        )
        resp = client.cardkit.v1.card.update(req)
        if not resp.success():
            logger.warning("update_card_by_id failed: %s %s", resp.code, resp.msg)
    except Exception as exc:
        logger.warning("update_card_by_id error: %s", exc)


# ── 业务卡片 ──────────────────────────────────────────────────────────────────

def _send_sql_review_card(open_id: str, sql: str) -> None:
    """发送 SQL 审核卡片，SQL 以 Markdown 代码块展示 + 交互按钮。"""
    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": "Forge SQL 审核"},
            "template": "blue",
        },
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"```\n{sql}\n```",
                },
            },
            {"tag": "hr"},
            {
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "确认执行"},
                        "type": "primary",
                        "value": {"action": "approve", "user_id": open_id},
                    },
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "取消"},
                        "type": "default",
                        "value": {"action": "cancel", "user_id": open_id},
                    },
                ],
            },
        ],
    }
    _send_card(open_id, card)


def _send_metric_card(open_id: str, proposal_text: str) -> None:
    """发送指标提案卡片，带确认/取消按钮。"""
    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": "Forge 指标提案"},
            "template": "orange",
        },
        "elements": [
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": proposal_text},
            },
            {"tag": "hr"},
            {
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "确认入库"},
                        "type": "primary",
                        "value": {"action": "confirm_metric", "user_id": open_id},
                    },
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "取消"},
                        "type": "default",
                        "value": {"action": "reject_metric", "user_id": open_id},
                    },
                ],
            },
        ],
    }
    _send_card(open_id, card)


def _send_chat_card(open_id: str, text: str) -> None:
    """发送普通对话卡片，内容以 lark_md 渲染。"""
    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": "Forge"},
            "template": "indigo",
        },
        "elements": [
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": text},
            },
        ],
    }
    _send_card(open_id, card)


def _send_result_card(
    open_id: str,
    sql: str,
    cols: list[str],
    rows: list[tuple],
    img_key: str | None = None,
    chart_url: str | None = None,
) -> None:
    """发送 SQL 执行结果卡片：Markdown SQL + 原生 table + 图表。"""
    elements: list = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**已执行 SQL**\n```\n{sql}\n```",
            },
        },
        {"tag": "hr"},
    ]

    if cols and rows:
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": "**查询结果**"},
        })
        elements.append(_rows_to_table_element(cols, rows))
        if len(rows) > 50:
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": f"_仅显示前 50 行，共 {len(rows)} 行_"},
            })
    else:
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": "查询执行完毕，结果为空。"},
        })

    if img_key:
        elements.append({"tag": "hr"})
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": "**推荐图表**"},
        })
        elements.append({
            "tag": "img",
            "img_key": img_key,
            "alt": {"tag": "plain_text", "content": "查询结果图表"},
            "mode": "fit_horizontal",
        })

    if chart_url:
        elements.append({
            "tag": "action",
            "actions": [
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "📊 查看交互图表"},
                    "type": "default",
                    "url": chart_url,
                }
            ],
        })

    # 结果反馈按钮（Stage 2）
    if cols and rows:
        elements.append({"tag": "hr"})
        elements.append({
            "tag": "action",
            "actions": [
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "👍 结果准确"},
                    "type": "default",
                    "value": {"action": "cache_verify", "user_id": open_id},
                },
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "👎 结果不准确"},
                    "type": "default",
                    "value": {"action": "cache_reject", "user_id": open_id},
                },
            ],
        })

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": "查询结果"},
            "template": "green",
        },
        "elements": elements,
    }
    _send_card(open_id, card)


def _send_info_card(open_id: str, text: str, template: str = "yellow") -> None:
    """发送提示/警告卡片。"""
    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": "⚠️ Forge 提示"},
            "template": template,
        },
        "elements": [
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": text},
            },
        ],
    }
    _send_card(open_id, card)


def _update_card(message_id: str, card: dict) -> None:
    """通过 PATCH API 更新已发送的卡片内容。"""
    try:
        client = _get_client()
        req = (
            PatchMessageRequest.builder()
            .message_id(message_id)
            .request_body(
                PatchMessageRequestBody.builder()
                .content(json.dumps(card, ensure_ascii=False))
                .build()
            )
            .build()
        )
        resp = client.im.v1.message.patch(req)
        if not resp.success():
            logger.warning("update_card failed: %s %s", resp.code, resp.msg)
    except Exception as exc:
        logger.warning("update_card error: %s", exc)


def _upload_image(image_bytes: bytes) -> str | None:
    """上传图片到飞书，返回 img_key；失败返回 None。"""
    try:
        client = _get_client()
        req = (
            CreateImageRequest.builder()
            .request_body(
                CreateImageRequestBody.builder()
                .image_type("message")
                .image(io.BytesIO(image_bytes))
                .build()
            )
            .build()
        )
        resp = client.im.v1.image.create(req)
        if resp.success():
            return resp.data.image_key
        logger.warning("upload image failed: %s %s", resp.code, resp.msg)
    except Exception as exc:
        logger.warning("upload image error: %s", exc)
    return None


def _rows_to_table_element(cols: list[str], rows: list[tuple], max_rows: int = 50) -> dict:
    """将查询结果转换为飞书卡片原生 table 元素。"""
    display_rows = rows[:max_rows]
    columns = [
        {
            "name":         col,
            "display_name": col,
            "data_type":    "text",
            "width":        "auto",
            "horizontal_align": "left",
        }
        for col in cols
    ]
    table_rows = []
    for row in display_rows:
        table_rows.append({
            col: str(v) if v is not None else ""
            for col, v in zip(cols, row)
        })
    elem: dict = {
        "tag":          "table",
        "page_size":    min(len(display_rows), 10),
        "row_height":   "low",
        "header_style": {"background_style": "grey", "bold": True},
        "columns":      columns,
        "rows":         table_rows,
    }
    if len(rows) > max_rows:
        elem["_note"] = f"仅显示前 {max_rows} 行"
    return elem


def _react(message_id: str, emoji: str = "DONE") -> None:
    try:
        client = _get_client()
        emoji_obj = Emoji.builder().emoji_type(emoji).build()
        body = (
            CreateMessageReactionRequestBody.builder()
            .reaction_type(emoji_obj)
            .build()
        )
        req = (
            CreateMessageReactionRequest.builder()
            .message_id(message_id)
            .request_body(body)
            .build()
        )
        resp = client.im.v1.message_reaction.create(req)
        if not resp.success():
            logger.warning("react failed: %s %s", resp.code, resp.msg)
    except Exception as exc:
        logger.warning("react error: %s", exc)


# ── 消息路由 ──────────────────────────────────────────────────────────────────

def _handle_approve(open_id: str, query_hint: str = "") -> None:
    resp = agent.approve(open_id)
    sql  = resp.sql or ""
    _result_text, cols, rows = _execute_sql(sql) if sql else ("", [], [])

    # 生成图表图片并上传到飞书
    img_key:   str | None = None
    chart_url: str | None = None
    if cols and rows:
        png = _generate_chart_image(cols, rows, query_hint)
        if png:
            img_key = _upload_image(png)
        # 同时保留交互版链接
        filename = _generate_chart(cols, rows, query_hint)
        if filename:
            chart_url = f"http://192.168.8.4:{cfg.PORT}/charts/{filename}"

    _send_result_card(open_id, sql, cols, rows, img_key, chart_url)


_ACCURATE_WORDS   = {"准确", "正确", "对的", "没错", "是的"}
_INACCURATE_WORDS = {"不准确", "不对", "不正确", "错了", "有问题", "错误"}


def _is_accurate(text: str) -> bool:
    t = text.strip()
    return t in _ACCURATE_WORDS or any(w in t for w in _ACCURATE_WORDS)


def _is_inaccurate(text: str) -> bool:
    t = text.strip()
    return t in _INACCURATE_WORDS or any(w in t for w in _INACCURATE_WORDS)


_RESET_WORDS = {"重置", "清空", "reset", "clear", "新对话", "重新开始"}


def _dispatch(open_id: str, text: str) -> None:
    # 重置对话指令
    if text.strip().lower() in _RESET_WORDS:
        store.clear(open_id)
        _send_chat_card(open_id, "对话已重置，请开始新的查询。")
        return

    session = store.get(open_id)

    if session.pending_sql:
        if _is_confirm(text):
            _handle_approve(open_id, query_hint=session.pending_sql or "")
        elif _is_cancel(text):
            resp = agent.cancel(open_id)
            _send_chat_card(open_id, resp.text)
        else:
            _send_info_card(open_id, "当前有待确认的 SQL，请回复 **确认** 执行或 **取消** 放弃。")
        return

    if session.pending_metric_proposal:
        if _is_confirm(text):
            resp = agent.confirm_metric_definition(open_id)
            _send_chat_card(open_id, resp.text)
        elif _is_cancel(text):
            resp = agent.reject_metric_proposal(open_id)
            _send_chat_card(open_id, resp.text)
        else:
            _send_info_card(open_id, "当前有待确认的指标提案，请回复 **确认** 入库或 **取消** 放弃。")
        return

    # Stage 2：结果准确性反馈
    if session.pending_cache_id:
        if _is_accurate(text):
            resp = agent.cache_verify(open_id)
            _send_chat_card(open_id, resp.text)
            return
        if _is_inaccurate(text):
            resp = agent.cache_reject(open_id)
            _send_chat_card(open_id, resp.text)
            return
        # 用户直接发了新问题，自动放弃本次反馈，继续处理新消息
        session.pending_cache_id = None

    _dispatch_query(open_id, text)


def _v2_button(label: str, btn_type: str, action: str, user_id: str,
               card_id: str = "") -> dict:
    """构造 Card JSON v2 按钮。用 value 字段触发 P2CardActionTrigger 回调（WebSocket 模式）。"""
    val = {"action": action, "user_id": user_id}
    if card_id:
        val["card_id"] = card_id
    return {
        "tag": "button",
        "text": {"tag": "plain_text", "content": label},
        "type": btn_type,
        "value": val,
    }


def _v2_sql_review_card(sql: str, open_id: str, card_id: str = "") -> dict:
    """Card JSON v2 格式的 SQL 审核卡片。"""
    return {
        "schema": "2.0",
        "header": {
            "title": {"tag": "plain_text", "content": "Forge SQL 审核"},
            "template": "blue",
        },
        "body": {
            "elements": [
                {"tag": "markdown", "content": f"```\n{sql}\n```"},
                {"tag": "hr"},
                {
                    "tag": "column_set",
                    "flex_mode": "none",
                    "background_style": "default",
                    "horizontal_spacing": "default",
                    "columns": [
                        {
                            "tag": "column",
                            "width": "auto",
                            "elements": [
                                _v2_button("确认执行", "primary", "approve", open_id, card_id),
                            ],
                        },
                        {
                            "tag": "column",
                            "width": "auto",
                            "elements": [
                                _v2_button("取消", "default", "cancel", open_id, card_id),
                            ],
                        },
                    ],
                },
            ],
        },
    }


def _v2_metric_card(proposal_text: str, open_id: str, card_id: str = "") -> dict:
    """Card JSON v2 格式的指标提案卡片。"""
    return {
        "schema": "2.0",
        "header": {
            "title": {"tag": "plain_text", "content": "Forge 指标提案"},
            "template": "orange",
        },
        "body": {
            "elements": [
                {"tag": "markdown", "content": proposal_text},
                {"tag": "hr"},
                {
                    "tag": "column_set",
                    "flex_mode": "none",
                    "background_style": "default",
                    "horizontal_spacing": "default",
                    "columns": [
                        {
                            "tag": "column",
                            "width": "auto",
                            "elements": [
                                _v2_button("确认入库", "primary", "confirm_metric", open_id, card_id),
                            ],
                        },
                        {
                            "tag": "column",
                            "width": "auto",
                            "elements": [
                                _v2_button("取消", "default", "reject_metric", open_id, card_id),
                            ],
                        },
                    ],
                },
            ],
        },
    }


def _v2_done_card(text: str, template: str = "green") -> dict:
    """Card JSON v2 格式的完成状态卡片（替换原审核卡片）。"""
    return {
        "schema": "2.0",
        "header": {
            "title": {"tag": "plain_text", "content": "Forge"},
            "template": template,
        },
        "body": {
            "elements": [
                {"tag": "markdown", "content": text},
            ],
        },
    }


def _dispatch_query(open_id: str, text: str) -> None:
    """处理新查询：尝试用流式卡片展示进度，降级为普通卡片。"""
    import time

    # 尝试创建流式卡片
    card_id = _create_streaming_card()
    logger.info("streaming card_id=%s for user=%s", card_id, open_id)
    if card_id:
        _send_card_by_id(open_id, card_id)

        _stream_update_text(card_id, "正在理解你的问题...")
        time.sleep(0.3)

        _stream_update_text(card_id, "正在理解你的问题...\n\n正在生成 SQL...")

        resp = agent.process(open_id, text)
        logger.info("agent resp: action=%s sql=%s", resp.action, bool(resp.sql))

        if resp.action == "sql_review" and resp.sql:
            _stream_update_text(card_id, f"正在理解你的问题...\n\n正在生成 SQL...\n\n```\n{resp.sql}\n```")
            time.sleep(0.5)
            _close_streaming(card_id)
            time.sleep(0.3)
            _update_card_by_id(card_id, _v2_sql_review_card(resp.sql, open_id, card_id))

        elif resp.action == "metric_clarification":
            _stream_update_text(card_id, resp.text)
            time.sleep(0.3)
            _close_streaming(card_id)
            time.sleep(0.3)
            _update_card_by_id(card_id, _v2_metric_card(resp.text, open_id, card_id))

        else:
            final = resp.text or "（无回复）"
            _stream_update_text(card_id, final)
            time.sleep(0.3)
            _close_streaming(card_id)

    else:
        # 降级：流式卡片创建失败，用 v1 传统方式
        resp = agent.process(open_id, text)
        if resp.action == "sql_review":
            _send_sql_review_card(open_id, resp.sql or "")
        elif resp.action == "metric_clarification":
            _send_metric_card(open_id, resp.text)
        else:
            _send_chat_card(open_id, resp.text or "（无回复）")


# ── 事件处理器 ─────────────────────────────────────────────────────────────────

def _on_message(data: lark.im.v1.P2ImMessageReceiveV1) -> None:
    try:
        msg    = data.event.message
        sender = data.event.sender
        logger.info("msg_type=%s message_id=%s", msg.message_type, msg.message_id)

        if _is_duplicate(msg.message_id):
            logger.info("skip duplicate message_id=%s", msg.message_id)
            return

        if msg.message_type != "text":
            return

        content = json.loads(msg.content or "{}")
        text    = content.get("text", "").strip()
        if not text:
            return

        open_id = sender.sender_id.open_id
        if not open_id:
            return

        logger.info("message from %s: %s", open_id, text[:80])

        # 立刻发表情表示已收到，不等 LLM
        _react(msg.message_id, "OK")

        # 加入用户专属串行队列，保证消息顺序处理
        _enqueue(open_id, text)

    except Exception as exc:
        logger.exception("Error handling message: %s", exc)


# ── 卡片按钮回调 ──────────────────────────────────────────────────────────────

def _on_card_action(data: P2CardActionTrigger) -> P2CardActionTriggerResponse:
    """处理卡片按钮点击回调。"""
    response = P2CardActionTriggerResponse()
    try:
        action_value = data.event.action.value or {}
        action_type  = action_value.get("action", "")
        open_id      = (
            action_value.get("user_id")
            or data.event.operator.open_id
        )
        message_id   = data.event.context.open_message_id

        logger.info("card_action: action=%s open_id=%s message_id=%s",
                     action_type, open_id, message_id)

        card_id = action_value.get("card_id", "")

        def _update_card_state(text: str, template: str = "green") -> None:
            """根据卡片来源选择 v1 PATCH 或 v2 CardKit 更新。"""
            if card_id:
                _update_card_by_id(card_id, _v2_done_card(text, template))
            else:
                _update_card(message_id, _make_done_card(text, template))

        if action_type == "approve":
            response.toast = CallBackToast()
            response.toast.type = "info"
            response.toast.content = "正在执行查询..."
            threading.Thread(
                target=_handle_card_approve,
                args=(open_id, message_id, card_id),
                daemon=True,
            ).start()

        elif action_type == "cancel":
            resp = agent.cancel(open_id)
            response.toast = CallBackToast()
            response.toast.type = "info"
            response.toast.content = "已取消"
            _update_card_state("~~已取消~~", "grey")

        elif action_type == "confirm_metric":
            resp = agent.confirm_metric_definition(open_id)
            response.toast = CallBackToast()
            response.toast.type = "success"
            response.toast.content = resp.text or "指标已保存"
            _update_card_state("指标已保存", "green")

        elif action_type == "reject_metric":
            resp = agent.reject_metric_proposal(open_id)
            response.toast = CallBackToast()
            response.toast.type = "info"
            response.toast.content = "已取消"
            _update_card_state("指标提案已取消", "grey")

        elif action_type == "cache_verify":
            resp = agent.cache_verify(open_id)
            response.toast = CallBackToast()
            response.toast.type = "success"
            response.toast.content = "感谢反馈，查询已加入缓存"

        elif action_type == "cache_reject":
            resp = agent.cache_reject(open_id)
            response.toast = CallBackToast()
            response.toast.type = "info"
            response.toast.content = "已记录，该查询不会被缓存"

        else:
            logger.warning("unknown card action: %s", action_type)

    except Exception as exc:
        logger.exception("card_action error: %s", exc)
        response.toast = CallBackToast()
        response.toast.type = "error"
        response.toast.content = f"处理失败：{exc}"

    return response


def _handle_card_approve(open_id: str, message_id: str, card_id: str = "") -> None:
    """按钮回调后异步执行 approve + SQL 执行 + 发送结果卡片。"""
    try:
        _handle_approve(open_id)
        # 更新原审核卡片：按钮替换为"已执行"状态
        if card_id:
            _update_card_by_id(card_id, _v2_done_card("SQL 已确认并执行", "green"))
        else:
            _update_card(message_id, _make_done_card("SQL 已确认并执行", "green"))
    except Exception as exc:
        logger.exception("card approve error: %s", exc)
        _send_info_card(open_id, f"执行失败：{exc}", template="red")


def _make_cancelled_card() -> dict:
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": "Forge SQL 审核"},
            "template": "grey",
        },
        "elements": [
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": "~~已取消~~"},
            },
        ],
    }


def _make_done_card(text: str, template: str = "green") -> dict:
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": "Forge"},
            "template": template,
        },
        "elements": [
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": text},
            },
        ],
    }


# ── 启动 ───────────────────────────────────────────────────────────────────────

def start_bot() -> None:
    import time as _time

    if not cfg.FEISHU_APP_ID or not cfg.FEISHU_APP_SECRET:
        logger.error("FEISHU_APP_ID / FEISHU_APP_SECRET 未配置，飞书 Bot 不会启动。")
        return

    event_handler = (
        lark.EventDispatcherHandler.builder(
            cfg.FEISHU_VERIFICATION_TOKEN,
            cfg.FEISHU_ENCRYPT_KEY,
        )
        .register_p2_im_message_receive_v1(_on_message)
        .register_p2_card_action_trigger(_on_card_action)
        .build()
    )

    # 自动重连：ws_client.start() 在连接断开后会退出，循环重建连接
    while True:
        try:
            ws_client = lark.ws.Client(
                cfg.FEISHU_APP_ID,
                cfg.FEISHU_APP_SECRET,
                event_handler=event_handler,
                log_level=lark.LogLevel.INFO,
            )
            logger.info("Forge 飞书 Bot 已启动，等待消息…")
            ws_client.start()
        except Exception as exc:
            logger.error("WebSocket 异常退出: %s", exc)
        logger.warning("WebSocket 断开，3 秒后重连…")
        _time.sleep(3)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    start_bot()
