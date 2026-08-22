"""Thin Pi channel client and Feishu renderer.

This module never calls Forge Agent, Executor, Registry, or database code. It only
forwards authenticated ChannelEvents to Pi and renders ChannelPresentation.
"""
from __future__ import annotations

import hashlib
import json
import time
from typing import Any

import httpx

from config import cfg


class PiChannelError(RuntimeError):
    pass


def stable_channel_action_event_id(
    message_id: str,
    task_run_id: str,
    action: str,
    payload: dict[str, Any],
) -> str:
    """Return one idempotency key for retries of the same rendered-card action."""
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(
        f"{message_id}\n{task_run_id}\n{action}\n{canonical}".encode("utf-8")
    ).hexdigest()
    return f"feishu_action_{digest[:40]}"


class PiChannelClient:
    def __init__(
        self,
        base_url: str | None = None,
        service_key: str | None = None,
        timeout_seconds: float = 15,
        channel: str = "feishu",
    ) -> None:
        self.base_url = (base_url or cfg.PI_ORCHESTRATOR_URL).rstrip("/")
        self.service_key = service_key or cfg.PI_CHANNEL_SERVICE_KEY
        self.timeout_seconds = timeout_seconds
        if channel not in {"feishu", "dingtalk"}:
            raise PiChannelError(f"Unsupported Pi channel: {channel}")
        self.channel = channel
        if not self.service_key:
            raise PiChannelError("PI_CHANNEL_SERVICE_KEY is not configured")

    def submit_message(
        self,
        *,
        event_id: str,
        external_user_id: str,
        conversation_id: str,
        message_id: str,
        text: str,
        chat_type: str | None = None,
    ) -> dict[str, Any]:
        return self._submit({
            "event_id": event_id,
            "channel": self.channel,
            "event_type": "message",
            "external_user_id": external_user_id,
            "conversation_id": conversation_id,
            "message_id": message_id,
            "task_run_id": None,
            "payload": {
                "text": text,
                **({"chat_type": chat_type} if chat_type else {}),
            },
        })

    def submit_action(
        self,
        *,
        event_id: str,
        external_user_id: str,
        conversation_id: str,
        message_id: str,
        task_run_id: str,
        action: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return self._submit({
            "event_id": event_id,
            "channel": self.channel,
            "event_type": "action",
            "external_user_id": external_user_id,
            "conversation_id": conversation_id,
            "message_id": message_id,
            "task_run_id": task_run_id,
            "payload": {"action": action, **payload},
        })

    def get_presentation(self, task_run_id: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/tasks/{task_run_id}/presentation")["presentation"]

    def wait_for_presentation(
        self,
        task_run_id: str,
        *,
        timeout_seconds: float = 360,
        interval_seconds: float = 0.8,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            presentation = self.get_presentation(task_run_id)
            if presentation.get("kind") != "progress":
                return presentation
            time.sleep(interval_seconds)
        raise PiChannelError("Pi Task polling timed out")

    def _submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/v1/channel-events", payload)

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        try:
            response = httpx.request(
                method,
                f"{self.base_url}{path}",
                headers={"X-Channel-Service-Key": self.service_key},
                json=payload,
                timeout=self.timeout_seconds,
            )
            data = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise PiChannelError("Pi Orchestrator is unavailable") from exc
        if response.status_code >= 400:
            raise PiChannelError(data.get("error") or f"Pi returned HTTP {response.status_code}")
        return data


def task_run_id_from_response(response: dict[str, Any]) -> str:
    task = response.get("task")
    if not isinstance(task, dict) or not isinstance(task.get("task_run_id"), str):
        raise PiChannelError("Pi response does not contain task_run_id")
    return task["task_run_id"]


def action_progress_presentation(action: str) -> dict[str, Any]:
    title, message = {
        "approve_query": ("正在执行查询", "已收到审批，正在安全执行只读 SQL。"),
        "analyze": ("正在分析结果", "正在基于已审批的查询结果生成证据分析。"),
        "render_report": ("正在生成报告", "正在整理分析结论和证据，生成最终报告。"),
        "provide_input": ("正在处理补充信息", "正在根据你补充的信息继续任务。"),
        "request_supplement": ("正在准备补查", "正在生成补查 SQL，执行前仍会请你审批。"),
        "cancel_task": ("正在取消任务", "正在安全停止后续任务步骤。"),
    }.get(action, ("正在处理", "Forge 正在处理这项操作。"))
    return {
        "kind": "progress",
        "title": title,
        "markdown": f"⏳ {message}\n\n你可以离开当前页面，完成后这张卡片会自动更新。",
        "fields": [],
        "table": None,
        "actions": [],
    }


def presentation_to_feishu_card(
    presentation: dict[str, Any],
    *,
    external_user_id: str,
    conversation_id: str,
) -> dict[str, Any]:
    markdown = str(presentation.get("markdown") or "")
    if len(markdown) > 20_000:
        markdown = markdown[:20_000] + "\n\n（内容过长，已截断）"
    elements: list[dict[str, Any]] = [
        {"tag": "markdown", "content": markdown},
    ]
    fields = presentation.get("fields")
    if isinstance(fields, list) and fields:
        content = "\n".join(
            f"**{item.get('label', '')}**：{item.get('value', '')}"
            for item in fields
            if isinstance(item, dict)
        )
        if content:
            elements.append({"tag": "markdown", "content": content})

    table = presentation.get("table")
    if isinstance(table, dict):
        columns = table.get("columns") or []
        rows = table.get("rows") or []
        if columns:
            lines = [" | ".join(str(column) for column in columns)]
            lines.append(" | ".join("---" for _ in columns))
            lines.extend(" | ".join(str(value) for value in row) for row in rows[:20])
            elements.append({"tag": "markdown", "content": "\n".join(lines)})

    actions = presentation.get("actions")
    if isinstance(actions, list) and actions:
        columns: list[dict[str, Any]] = []
        for item in actions:
            if not isinstance(item, dict):
                continue
            style = item.get("style")
            value = {
                "pi_action": True,
                "action_type": item.get("type"),
                "task_run_id": item.get("task_run_id"),
                "payload": item.get("payload") or {},
                "user_id": external_user_id,
                "conversation_id": conversation_id,
            }
            if item.get("type") == "provide_input":
                elements.append({
                    "tag": "form",
                    "name": f"clarification_{item.get('task_run_id')}",
                    "elements": [
                        {
                            "tag": "input",
                            "name": "text",
                            "placeholder": {"tag": "plain_text", "content": "请输入补充信息"},
                        },
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": str(item.get("label") or "提交")},
                            "type": "primary",
                            "action_type": "form_submit",
                            "value": value,
                        },
                    ],
                })
                continue
            columns.append({
                "tag": "column",
                "width": "auto",
                "elements": [{
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": str(item.get("label") or "继续")},
                    "type": "primary" if style == "primary" else "danger" if style == "danger" else "default",
                    "value": value,
                }],
            })
        if columns:
            elements.append({
                "tag": "column_set",
                "flex_mode": "none",
                "columns": columns,
            })

    template = {
        "error": "red",
        "query_review": "blue",
        "needs_input": "orange",
        "report": "green",
    }.get(str(presentation.get("kind")), "blue")
    return {
        "schema": "2.0",
        "header": {
            "title": {"tag": "plain_text", "content": str(presentation.get("title") or "Forge")},
            "template": template,
        },
        "body": {"elements": elements},
    }
