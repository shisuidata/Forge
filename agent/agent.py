"""
Forge Agent core loop.
Handles two modes:
  - Query mode:  user asks a data question → Forge JSON → SQL → pending approval
  - Define mode: user defines a metric → extract → save to registry
"""
from __future__ import annotations
import json
from pathlib import Path

from config import cfg
from forge.compiler import compile_query
from agent.session import store, Session
from agent import llm


# ── public entry point ────────────────────────────────────────────────────────

class AgentResponse:
    """Structured response from the agent."""

    def __init__(
        self,
        text:     str = "",
        sql:      str | None = None,      # set when SQL is ready for review
        forge_json: dict | None = None,
        action:   str = "message",        # message | sql_review | metric_saved | error
    ):
        self.text     = text
        self.sql      = sql
        self.forge_json = forge_json
        self.action   = action


def process(user_id: str, user_text: str) -> AgentResponse:
    """Process a user message and return an AgentResponse."""
    session = store.get(user_id)
    session.add("user", user_text)

    result = llm.call(session.recent())

    # ── plain text reply ──────────────────────────────────────────────────────
    if result["tool"] is None:
        text = result.get("text", "")
        session.add("assistant", text)
        return AgentResponse(text=text)

    # ── query mode ────────────────────────────────────────────────────────────
    if result["tool"] == "generate_forge_query":
        forge_json = result["input"]
        try:
            sql = compile_query(forge_json)
        except Exception as e:
            err = f"编译失败：{e}"
            session.add("assistant", err)
            return AgentResponse(text=err, action="error")

        session.pending_sql   = sql
        session.pending_forge = forge_json
        session.add("assistant", f"[SQL ready for review]\n{sql}")
        return AgentResponse(sql=sql, forge_json=forge_json, action="sql_review")

    # ── define mode ───────────────────────────────────────────────────────────
    if result["tool"] == "define_metric":
        metric = result["input"]
        _save_metric(metric)
        text = f"✅ 已保存指标「{metric['name']}」：{metric['description']}"
        session.add("assistant", text)
        return AgentResponse(text=text, action="metric_saved")

    return AgentResponse(text="未知操作", action="error")


def approve(user_id: str) -> AgentResponse:
    """User approved the pending SQL — mark as executed (execution handled by caller)."""
    session = store.get(user_id)
    sql = session.pending_sql
    if not sql:
        return AgentResponse(text="没有待确认的 SQL。", action="error")
    session.pending_sql   = None
    session.pending_forge = None
    return AgentResponse(text="✅ SQL 已确认，开始执行。", sql=sql, action="approved")


def cancel(user_id: str) -> AgentResponse:
    """User cancelled the pending SQL."""
    session = store.get(user_id)
    session.pending_sql   = None
    session.pending_forge = None
    return AgentResponse(text="已取消。", action="cancelled")


# ── registry helpers ──────────────────────────────────────────────────────────

def _save_metric(metric: dict) -> None:
    path = cfg.REGISTRY_PATH
    try:
        registry = json.loads(path.read_text())
    except Exception:
        registry = {}

    if "metrics" not in registry:
        registry["metrics"] = {}

    registry["metrics"][metric["name"]] = {
        "description": metric["description"],
    }
    path.write_text(json.dumps(registry, ensure_ascii=False, indent=2))
