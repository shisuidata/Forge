"""Thin Pi channel client and Feishu renderer.

This module never calls Forge Agent, Executor, Registry, or database code. It only
forwards authenticated ChannelEvents to Pi and renders ChannelPresentation.
"""
from __future__ import annotations

import time
from typing import Any

import httpx

from config import cfg


class PiChannelError(RuntimeError):
    pass


class PiChannelClient:
    def __init__(
        self,
        base_url: str | None = None,
        service_key: str | None = None,
        timeout_seconds: float = 15,
    ) -> None:
        self.base_url = (base_url or cfg.PI_ORCHESTRATOR_URL).rstrip("/")
        self.service_key = service_key or cfg.PI_CHANNEL_SERVICE_KEY
        self.timeout_seconds = timeout_seconds
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
    ) -> dict[str, Any]:
        return self._submit({
            "event_id": event_id,
            "channel": "feishu",
            "event_type": "message",
            "external_user_id": external_user_id,
            "conversation_id": conversation_id,
            "message_id": message_id,
            "task_run_id": None,
            "payload": {"text": text},
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
            "channel": "feishu",
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
