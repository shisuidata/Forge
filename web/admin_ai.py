"""Admin AI assistant: LLM-proposed registry edits with explicit apply step."""
from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from registry.validator import validate_metric
from web.admin_registry_store import (
    _load_conventions,
    _load_disambiguations,
    _load_metrics,
    _load_schema,
    _save_conventions,
    _save_disambiguations,
    _save_metrics,
)
from web.auth import require_api_auth
from web.router_support import _run_sync

router = APIRouter()


class AdminChatRequest(BaseModel):
    message: str
    page: str = ""          # schema / metrics / semantic
    user_id: str = "admin"


def _admin_ai_process(message: str, page: str) -> dict:
    """
    管理助手：根据用户自然语言 + 当前页面上下文，生成结构化提议。

    返回 {"type": "...", "proposal": {...}, "summary": "..."} 或文字回复。
    """
    from agent import llm

    # 构建上下文：当前页面的数据摘要
    context_parts = []
    if page in ("schema", "metrics", "semantic", ""):
        schema = _load_schema()
        tables = schema.get("tables", {})
        if tables:
            table_names = ", ".join(tables.keys())
            context_parts.append(f"当前数据库有 {len(tables)} 张表：{table_names}")

    if page in ("metrics", ""):
        metrics = _load_metrics()
        if metrics:
            metric_names = ", ".join(f"{k}({v.get('label','')})" for k, v in metrics.items())
            context_parts.append(f"已有 {len(metrics)} 个指标：{metric_names}")

    if page in ("semantic", ""):
        disambiguations = _load_disambiguations()
        conventions = _load_conventions()
        if disambiguations:
            context_parts.append(f"已有 {len(disambiguations)} 条歧义消除规则")
        if conventions:
            context_parts.append(f"已有 {len(conventions)} 条字段约定")

    context = "\n".join(context_parts) if context_parts else "暂无 Registry 数据"

    system_prompt = f"""你是 Forge Registry 管理助手。用户在 Web 管理页面上通过自然语言管理语义库。

当前 Registry 状态：
{context}

你的任务：
1. 理解用户的管理意图
2. 生成一个结构化的操作提议（JSON 格式）
3. 用简洁的中文说明你打算做什么

请用以下 JSON 格式回复（不要加 markdown 代码块标记）：
{{
  "type": "add_metric" | "update_metric" | "delete_metric" | "add_disambiguation" | "update_disambiguation" | "add_convention" | "update_convention" | "message",
  "proposal": {{...操作的具体数据...}},
  "summary": "一句话说明"
}}

type=message 时 proposal 为空，summary 是对用户的文字回复。

指标 proposal 格式（add_metric / update_metric）：
{{"name": "xxx", "metric_class": "atomic|derivative", "label": "显示名", "description": "定义", "aggregation": "sum|count|...", "measure": "table.column", "qualifiers": ["条件"], "numerator": "xxx", "denominator": "xxx"}}

歧义规则 proposal 格式（add_disambiguation）：
{{"key": "xxx", "label": "显示名", "triggers": ["词1","词2"], "context": "注入说明", "requires_clarification": false}}

字段约定 proposal 格式（add_convention）：
{{"key": "xxx", "label": "显示名", "applies_to": ["table.column"], "convention": "约定内容"}}

delete 类型的 proposal 只需 {{"name": "要删除的标识符"}}。"""

    import json as _json
    msgs = [{"role": "user", "content": message}]
    try:
        result = llm.call(msgs, system_override=system_prompt)
        text = result.get("text", "")
        # 尝试解析为 JSON
        try:
            return _json.loads(text)
        except _json.JSONDecodeError:
            # LLM 可能加了 markdown 代码块
            import re
            m = re.search(r'\{[\s\S]+\}', text)
            if m:
                return _json.loads(m.group())
            return {"type": "message", "proposal": {}, "summary": text}
    except Exception as exc:
        return {"type": "message", "proposal": {}, "summary": f"处理失败：{exc}"}


@router.post("/api/admin-chat", response_class=JSONResponse)
async def api_admin_chat(req: AdminChatRequest, _auth=Depends(require_api_auth)):
    """管理助手 AI：返回结构化提议或文字回复。"""
    result = await _run_sync(_admin_ai_process, req.message, req.page)
    return result


@router.post("/api/admin-apply", response_class=JSONResponse)
async def api_admin_apply(request: Request, _auth=Depends(require_api_auth)):
    """应用管理助手的提议。"""
    body = await request.json()
    action_type = body.get("type", "")
    proposal = body.get("proposal", {})

    try:
        if action_type == "add_metric" or action_type == "update_metric":
            proposal = dict(proposal)
            name = proposal.pop("name", "")
            if not name:
                return {"ok": False, "error": "缺少指标名称"}
            metric_for_validation = dict(proposal)
            metric_for_validation["name"] = name
            structural = _load_schema()
            existing = _load_metrics()
            validation = validate_metric(
                metric_for_validation, structural, metric_name=name, all_metrics=existing
            )
            if not validation.valid:
                return {"ok": False, "error": "；".join(validation.errors)}
            proposal["updated_at"] = str(date.today())
            # 过滤空值
            entry = {k: v for k, v in proposal.items() if v not in (None, "", [], {})}
            metrics = existing
            metrics[name] = entry
            _save_metrics(metrics)
            return {"ok": True, "message": f"指标「{entry.get('label', name)}」已保存"}

        elif action_type == "delete_metric":
            name = proposal.get("name", "")
            metrics = _load_metrics()
            deleted = metrics.pop(name, None)
            if deleted:
                _save_metrics(metrics)
                return {"ok": True, "message": f"指标「{name}」已删除"}
            return {"ok": False, "error": f"指标「{name}」不存在"}

        elif action_type in ("add_disambiguation", "update_disambiguation"):
            proposal = dict(proposal)
            key = proposal.pop("key", "")
            if not key:
                return {"ok": False, "error": "缺少规则 key"}
            data = _load_disambiguations()
            data[key] = {k: v for k, v in proposal.items() if v not in (None, "", [], {})}
            _save_disambiguations(data)
            return {"ok": True, "message": f"歧义规则「{proposal.get('label', key)}」已保存"}

        elif action_type in ("add_convention", "update_convention"):
            proposal = dict(proposal)
            key = proposal.pop("key", "")
            if not key:
                return {"ok": False, "error": "缺少约定 key"}
            data = _load_conventions()
            data[key] = {k: v for k, v in proposal.items() if v not in (None, "", [], {})}
            _save_conventions(data)
            return {"ok": True, "message": f"字段约定「{proposal.get('label', key)}」已保存"}

        else:
            return {"ok": False, "error": f"不支持的操作类型：{action_type}"}

    except Exception as exc:
        return {"ok": False, "error": str(exc)}
