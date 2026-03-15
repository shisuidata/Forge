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
            except Exception:
                pass
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


def _sql_to_image(sql: str) -> bytes | None:
    """用 Pygments 把 SQL 渲染成带语法高亮的 PNG（Monokai 主题）。"""
    try:
        from pygments import highlight
        from pygments.lexers import SqlLexer
        from pygments.formatters import ImageFormatter
        fmt = ImageFormatter(style="monokai", font_size=15, line_pad=5, image_pad=16)
        return highlight(sql, SqlLexer(), fmt)
    except Exception as exc:
        logger.warning("sql_to_image failed: %s", exc)
        return None



def _send_sql_review_card(open_id: str, sql: str) -> None:
    """发送 SQL 审核卡片，SQL 以语法高亮图片展示。"""
    # 先尝试生成语法高亮图片
    sql_img_key: str | None = None
    png = _sql_to_image(sql)
    if png:
        sql_img_key = _upload_image(png)

    elements: list = []
    if sql_img_key:
        elements.append({
            "tag": "img",
            "img_key": sql_img_key,
            "alt": {"tag": "plain_text", "content": "SQL"},
            "mode": "fit_horizontal",
        })
    else:
        # 降级：纯文本
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": sql},
        })

    elements += [
        {"tag": "hr"},
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "回复 **确认** 执行 · 回复 **取消** 放弃",
            },
        },
    ]
    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": "🔍 Forge SQL 审核"},
            "template": "blue",
        },
        "elements": elements,
    }
    _send_card(open_id, card)


def _send_metric_card(open_id: str, proposal_text: str) -> None:
    """发送指标提案卡片（纯展示），文字「确认」/「取消」交互。"""
    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": "📋 Forge 指标提案"},
            "template": "orange",
        },
        "elements": [
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": proposal_text},
            },
            {"tag": "hr"},
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": "回复 **确认** 入库 · 回复 **取消** 放弃",
                },
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
    """发送 SQL 执行结果卡片：原生 table + 嵌入图表图片。"""
    # SQL 高亮图片
    sql_img_key: str | None = None
    if sql:
        png = _sql_to_image(sql)
        if png:
            sql_img_key = _upload_image(png)

    sql_elem: dict
    if sql_img_key:
        sql_elem = {
            "tag": "img",
            "img_key": sql_img_key,
            "alt": {"tag": "plain_text", "content": "SQL"},
            "mode": "fit_horizontal",
        }
    else:
        sql_elem = {
            "tag": "div",
            "text": {"tag": "lark_md", "content": sql},
        }

    elements: list = [
        {"tag": "div", "text": {"tag": "lark_md", "content": "**已执行 SQL**"}},
        sql_elem,
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

    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": "✅ 查询结果"},
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


def _send_cache_feedback_card(open_id: str) -> None:
    """Stage 2 反馈卡片：询问用户查询结果是否准确。"""
    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": "📊 结果反馈"},
            "template": "purple",
        },
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        "这次查询结果是否准确？\n\n"
                        "回复 **准确** 👍 将保存此查询，下次相似问题可快速复用\n"
                        "回复 **不准确** 👎 丢弃，不会影响后续查询"
                    ),
                },
            },
        ],
    }
    _send_card(open_id, card)


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

    # Stage 2：若已写入缓存（pending），发送结果反馈提示
    if store.get(open_id).pending_cache_id:
        _send_cache_feedback_card(open_id)


_ACCURATE_WORDS   = {"准确", "正确", "对的", "没错", "是的"}
_INACCURATE_WORDS = {"不准确", "不对", "不正确", "错了", "有问题", "错误"}


def _is_accurate(text: str) -> bool:
    t = text.strip()
    return t in _ACCURATE_WORDS or any(w in t for w in _ACCURATE_WORDS)


def _is_inaccurate(text: str) -> bool:
    t = text.strip()
    return t in _INACCURATE_WORDS or any(w in t for w in _INACCURATE_WORDS)


def _dispatch(open_id: str, text: str) -> None:
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


# ── 启动 ───────────────────────────────────────────────────────────────────────

def start_bot() -> None:
    if not cfg.FEISHU_APP_ID or not cfg.FEISHU_APP_SECRET:
        logger.error("FEISHU_APP_ID / FEISHU_APP_SECRET 未配置，飞书 Bot 不会启动。")
        return

    event_handler = (
        lark.EventDispatcherHandler.builder(
            cfg.FEISHU_VERIFICATION_TOKEN,
            cfg.FEISHU_ENCRYPT_KEY,
        )
        .register_p2_im_message_receive_v1(_on_message)
        .build()
    )

    ws_client = lark.ws.Client(
        cfg.FEISHU_APP_ID,
        cfg.FEISHU_APP_SECRET,
        event_handler=event_handler,
        log_level=lark.LogLevel.INFO,
    )

    logger.info("Forge 飞书 Bot 已启动，等待消息…")
    ws_client.start()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    start_bot()
