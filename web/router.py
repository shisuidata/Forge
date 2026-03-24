"""
Forge web UI — FastAPI router.

Routes
------
## Chat（查询对话）
GET  /chat                           → 对话界面
POST /api/chat                       → 发送消息，返回 AgentResponse JSON
POST /api/approve                    → 确认 SQL
POST /api/cancel                     → 取消 SQL

## Admin（管理后台，挂载在 /admin 前缀下）
GET  /admin                          → redirect to /admin/registry
GET  /admin/registry                 → registry overview (tables + metrics)
POST /admin/registry/metric          → add or update a metric definition
DELETE /admin/registry/metric/{name} → delete a metric
GET  /admin/semantic                 → 语义规则（歧义消除规则 + 字段使用约定）
GET  /admin/staging                  → staging 歧义确认队列
POST /admin/staging/promote/{name}   → 合并单条 staging 记录
POST /admin/staging/promote-all      → 合并全部 staging 记录
POST /admin/staging/discard/{name}   → 丢弃单条 staging 记录
GET  /admin/audit                    → recent audit log (last 100 entries)
GET  /admin/settings                 → current config (secrets masked)
"""
from __future__ import annotations

import json
import logging
import re
import shutil
from datetime import date
from pathlib import Path
from typing import Optional

import yaml
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from agent import audit
from agent.agent import process as agent_process
from agent.agent import approve as agent_approve
from agent.agent import cancel as agent_cancel
from forge.executor import execute_with_data
from config import cfg
from registry.validator import validate_metric
from registry.staging_sync import promote_staged

logger = logging.getLogger(__name__)

# Chat / API 路由 — 挂载在根级别
chat_router = APIRouter()
# Admin 路由 — 挂载在 /admin 前缀下
router = APIRouter()

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


# ── helpers ───────────────────────────────────────────────────────────────────

def _load_schema() -> dict:
    """Load structural layer (schema.registry.json)."""
    try:
        return json.loads(cfg.REGISTRY_PATH.read_text())
    except (FileNotFoundError, OSError, json.JSONDecodeError) as exc:
        logger.warning("Failed to load schema registry: %s", exc)
        return {}


def _load_metrics() -> dict:
    """Load semantic layer (metrics.registry.yaml)."""
    try:
        return yaml.safe_load(cfg.METRICS_PATH.read_text()) or {}
    except (FileNotFoundError, OSError, yaml.YAMLError) as exc:
        logger.warning("Failed to load metrics registry: %s", exc)
        return {}


def _save_metrics(metrics: dict) -> None:
    cfg.METRICS_PATH.write_text(
        yaml.dump(metrics, allow_unicode=True, sort_keys=False, default_flow_style=False)
    )


def _mask_secret(value: str, visible: int = 4) -> str:
    if not value:
        return "(not set)"
    if len(value) <= visible:
        return "*" * len(value)
    return "*" * (len(value) - visible) + value[-visible:]


def _mask_db_url(url: str) -> str:
    if not url:
        return "(not set)"
    return re.sub(r"(:)([^/@]+)(@)", lambda m: f"{m.group(1)}****{m.group(3)}", url)


def _parse_lines(text: str) -> list[str]:
    """Split textarea value into a list, stripping blank lines."""
    return [line.strip() for line in text.splitlines() if line.strip()]


# ── Chat API ──────────────────────────────────────────────────────────────────

import asyncio
from functools import partial

class ChatRequest(BaseModel):
    message: str
    user_id: str = "web_user"


def _run_sync(fn, *args):
    """在线程池中执行同步函数，避免阻塞事件循环。"""
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(None, partial(fn, *args))


@chat_router.get("/chat", response_class=HTMLResponse)
async def chat_page(request: Request):
    return templates.TemplateResponse("chat.html", {"request": request})


@chat_router.post("/api/chat", response_class=JSONResponse)
async def api_chat(req: ChatRequest):
    """调用 Agent process，返回 AgentResponse 的 JSON 表示。"""
    resp = await _run_sync(agent_process, req.user_id, req.message)
    # 写入审计日志
    status_map = {"sql_review": "pending", "error": "error", "metric_saved": "approved"}
    await audit.log(
        user_id=req.user_id,
        user_message=req.message,
        forge_json=resp.forge_json,
        sql=resp.sql,
        status=status_map.get(resp.action, "approved"),
        error_message=resp.text if resp.action == "error" else None,
    )
    return {
        "text": resp.text,
        "sql": resp.sql,
        "forge_json": resp.forge_json,
        "action": resp.action,
    }


@chat_router.post("/api/approve", response_class=JSONResponse)
async def api_approve(req: ChatRequest):
    """用户确认 SQL，随后执行并返回结果。"""
    resp = await _run_sync(agent_approve, req.user_id)
    result = {"text": resp.text, "sql": resp.sql, "action": resp.action,
              "columns": None, "rows": None, "row_count": 0, "exec_error": None}

    if resp.action == "approved" and resp.sql:
        try:
            text, cols, rows = await _run_sync(execute_with_data, resp.sql)
            # 将 Row 对象转为普通 list
            result["columns"] = cols
            result["rows"] = [list(r) for r in rows]
            result["row_count"] = len(rows)
            if text.startswith("⚠"):
                result["exec_error"] = text
        except Exception as exc:
            result["exec_error"] = str(exc)

    return result


@chat_router.post("/api/cancel", response_class=JSONResponse)
async def api_cancel(req: ChatRequest):
    """用户取消 SQL。"""
    resp = await _run_sync(agent_cancel, req.user_id)
    return {"text": resp.text, "action": resp.action}


# ── Admin AI 助手 API ─────────────────────────────────────────────────────────

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

    messages = [{"role": "user", "content": message}]
    # 调用 LLM
    from agent.session import Message
    msgs = [Message(role="user", content=message)]

    import json as _json
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


@chat_router.post("/api/admin-chat", response_class=JSONResponse)
async def api_admin_chat(req: AdminChatRequest):
    """管理助手 AI：返回结构化提议或文字回复。"""
    result = await _run_sync(_admin_ai_process, req.message, req.page)
    return result


@chat_router.post("/api/admin-apply", response_class=JSONResponse)
async def api_admin_apply(request: Request):
    """应用管理助手的提议。"""
    body = await request.json()
    action_type = body.get("type", "")
    proposal = body.get("proposal", {})

    try:
        if action_type == "add_metric" or action_type == "update_metric":
            name = proposal.pop("name", "")
            if not name:
                return {"ok": False, "error": "缺少指标名称"}
            proposal["updated_at"] = str(date.today())
            # 过滤空值
            entry = {k: v for k, v in proposal.items() if v not in (None, "", [], {})}
            metrics = _load_metrics()
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
            key = proposal.pop("key", "")
            if not key:
                return {"ok": False, "error": "缺少规则 key"}
            data = _load_disambiguations()
            data[key] = {k: v for k, v in proposal.items() if v not in (None, "", [], {})}
            _save_disambiguations(data)
            return {"ok": True, "message": f"歧义规则「{proposal.get('label', key)}」已保存"}

        elif action_type in ("add_convention", "update_convention"):
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


# ── Admin routes ──────────────────────────────────────────────────────────────

@router.get("/", response_class=RedirectResponse)
async def admin_root():
    return RedirectResponse(url="/admin/schema", status_code=302)


# ── 结构层（表 / 字段）─────────────────────────────────────────────────────────

@router.get("/schema", response_class=HTMLResponse)
async def schema_page(request: Request):
    schema = _load_schema()
    tables = schema.get("tables", {})
    return templates.TemplateResponse(
        "schema.html",
        {"request": request, "tables": tables},
    )


# 兼容旧路由
@router.get("/registry", response_class=RedirectResponse)
async def registry_redirect():
    return RedirectResponse(url="/admin/schema", status_code=302)


# ── 指标库 ─────────────────────────────────────────────────────────────────────

@router.get("/metrics", response_class=HTMLResponse)
async def metrics_page(request: Request):
    metrics = _load_metrics()
    atomics     = {k: v for k, v in metrics.items() if v.get("metric_class") == "atomic"}
    derivatives = {k: v for k, v in metrics.items() if v.get("metric_class") == "derivative"}
    return templates.TemplateResponse(
        "metrics.html",
        {"request": request, "atomics": atomics, "derivatives": derivatives, "all_metrics": metrics},
    )


@router.post("/metrics/metric", response_class=HTMLResponse)
async def upsert_metric(
    request:     Request,
    name:        str           = Form(...),
    label:       str           = Form(...),
    type:        str           = Form(...),
    description: str           = Form(...),
    numerator:   Optional[str] = Form(default=None),
    denominator: Optional[str] = Form(default=None),
    filters:     Optional[str] = Form(default=None),
    dimensions:  Optional[str] = Form(default=None),
    notes:       Optional[str] = Form(default=None),
):
    entry: dict = {
        "label":       label,
        "type":        type,
        "description": description,
    }
    if numerator:   entry["numerator"]   = numerator
    if denominator: entry["denominator"] = denominator
    if filters:     entry["filters"]     = _parse_lines(filters)
    if dimensions:  entry["dimensions"]  = _parse_lines(dimensions)
    if notes:       entry["notes"]       = notes

    structural  = _load_schema()
    all_metrics = _load_metrics()
    result = validate_metric(entry, structural, metric_name=name, all_metrics=all_metrics)
    if not result.valid:
        atomics     = {k: v for k, v in all_metrics.items() if v.get("metric_class") == "atomic"}
        derivatives = {k: v for k, v in all_metrics.items() if v.get("metric_class") == "derivative"}
        return templates.TemplateResponse(
            "metrics.html",
            {
                "request":       request,
                "atomics":       atomics,
                "derivatives":   derivatives,
                "all_metrics":   all_metrics,
                "form_errors":   result.errors,
                "form_warnings": result.warnings,
                "form_data":     {"name": name, **entry},
            },
            status_code=422,
        )

    entry["updated_at"] = str(date.today())
    metrics = _load_metrics()
    metrics[name] = entry
    _save_metrics(metrics)
    return RedirectResponse(url="/admin/metrics", status_code=303)


@router.delete("/metrics/metric/{name}")
async def delete_metric(name: str):
    metrics = _load_metrics()
    metrics.pop(name, None)
    _save_metrics(metrics)
    return {"deleted": name}


@router.get("/semantic", response_class=HTMLResponse)
async def semantic_page(request: Request, flash: str = ""):
    return templates.TemplateResponse(
        "semantic.html",
        {"request": request, "disambiguations": _load_disambiguations(),
         "conventions": _load_conventions(), "flash": flash},
    )


# ── 语义规则 CRUD helpers ─────────────────────────────────────────────────────

def _load_disambiguations() -> dict:
    try:
        return yaml.safe_load(cfg.DISAMBIGUATIONS_PATH.read_text()) or {}
    except (FileNotFoundError, OSError, yaml.YAMLError):
        return {}


def _save_disambiguations(data: dict) -> None:
    cfg.DISAMBIGUATIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    cfg.DISAMBIGUATIONS_PATH.write_text(
        yaml.dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False)
    )


def _load_conventions() -> dict:
    try:
        return yaml.safe_load(cfg.CONVENTIONS_PATH.read_text()) or {}
    except (FileNotFoundError, OSError, yaml.YAMLError):
        return {}


def _save_conventions(data: dict) -> None:
    cfg.CONVENTIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    cfg.CONVENTIONS_PATH.write_text(
        yaml.dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False)
    )


@router.post("/semantic/disambiguation", response_class=RedirectResponse)
async def upsert_disambiguation(
    key:                     str  = Form(...),
    label:                   str  = Form(...),
    triggers:                str  = Form(default=""),
    context:                 str  = Form(default=""),
    requires_clarification:  str  = Form(default="false"),
    clarification_question:  str  = Form(default=""),
    confirmed_by_users:      str  = Form(default="false"),
):
    data = _load_disambiguations()
    entry: dict = {
        "label": label,
        "triggers": _parse_lines(triggers),
        "context": context,
        "requires_clarification": requires_clarification == "true",
        "confirmed_by_users": confirmed_by_users == "true",
    }
    if entry["requires_clarification"] and clarification_question:
        entry["clarification_question"] = clarification_question
    data[key] = entry
    _save_disambiguations(data)
    return RedirectResponse(url="/admin/semantic?flash=歧义规则已保存", status_code=303)


@router.delete("/semantic/disambiguation/{key}")
async def delete_disambiguation(key: str):
    data = _load_disambiguations()
    data.pop(key, None)
    _save_disambiguations(data)
    return {"deleted": key}


@router.post("/semantic/convention", response_class=RedirectResponse)
async def upsert_convention(
    key:                str = Form(...),
    label:              str = Form(...),
    applies_to:         str = Form(default=""),
    convention:         str = Form(default=""),
    confirmed_by_users: str = Form(default="false"),
):
    data = _load_conventions()
    entry: dict = {
        "label": label,
        "applies_to": _parse_lines(applies_to),
        "convention": convention,
        "confirmed_by_users": confirmed_by_users == "true",
    }
    data[key] = entry
    _save_conventions(data)
    return RedirectResponse(url="/admin/semantic?flash=字段约定已保存", status_code=303)


@router.delete("/semantic/convention/{key}")
async def delete_convention(key: str):
    data = _load_conventions()
    data.pop(key, None)
    _save_conventions(data)
    return {"deleted": key}


@router.get("/staging", response_class=HTMLResponse)
async def staging_page(request: Request, flash: str = ""):
    staging_dir = cfg.STAGING_DIR
    records: list[dict] = []
    done_records: list[dict] = []

    if staging_dir.exists():
        for fp in sorted(staging_dir.glob("*.json")):
            try:
                r = json.loads(fp.read_text())
                r["_filename"] = fp.name
                records.append(r)
            except (json.JSONDecodeError, OSError) as exc:
                logger.debug("Skipping malformed staging file %s: %s", fp.name, exc)
        done_dir = staging_dir / "done"
        if done_dir.exists():
            for fp in sorted(done_dir.glob("*.json"), reverse=True)[:20]:
                try:
                    r = json.loads(fp.read_text())
                    done_records.append(r)
                except (json.JSONDecodeError, OSError) as exc:
                    logger.debug("Skipping malformed done file %s: %s", fp.name, exc)

    return templates.TemplateResponse(
        "staging.html",
        {"request": request, "records": records,
         "done_records": done_records, "flash": flash},
    )


@router.post("/staging/promote/{filename}", response_class=RedirectResponse)
async def staging_promote_one(filename: str):
    staging_dir = cfg.STAGING_DIR
    fp = staging_dir / filename
    if fp.exists():
        done_dir = staging_dir / "done"
        done_dir.mkdir(parents=True, exist_ok=True)
        # 只处理这一个文件：临时目录 → promote → done
        import tempfile, shutil as _shutil
        with tempfile.TemporaryDirectory() as tmp:
            tmp_fp = Path(tmp) / filename
            _shutil.copy(str(fp), str(tmp_fp))
            promote_staged(Path(tmp), cfg.DISAMBIGUATIONS_PATH)
        fp.unlink(missing_ok=True)
        done_dir.mkdir(parents=True, exist_ok=True)
    return RedirectResponse(url="/admin/staging?flash=已合并入语义库", status_code=303)


@router.post("/staging/promote-all", response_class=RedirectResponse)
async def staging_promote_all():
    stats = promote_staged(cfg.STAGING_DIR, cfg.DISAMBIGUATIONS_PATH)
    msg = f"合并完成：新增 {stats['added']}，更新 {stats['updated']}，跳过 {stats['skipped']}"
    return RedirectResponse(url=f"/admin/staging?flash={msg}", status_code=303)


@router.post("/staging/discard/{filename}", response_class=RedirectResponse)
async def staging_discard(filename: str):
    staging_dir = cfg.STAGING_DIR
    fp = staging_dir / filename
    if fp.exists():
        done_dir = staging_dir / "done"
        done_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(fp), done_dir / filename)
    return RedirectResponse(url="/admin/staging?flash=已丢弃", status_code=303)


@router.get("/audit", response_class=HTMLResponse)
async def audit_page(request: Request, status: str = "", q: str = ""):
    records = await audit.search(status=status, keyword=q, limit=200)
    counts = await audit.stats()
    return templates.TemplateResponse(
        "audit.html",
        {"request": request, "records": records, "counts": counts,
         "filter_status": status, "filter_q": q},
    )


def _load_forge_yaml() -> dict:
    """读取 forge.yaml 原始内容。"""
    yaml_path = Path(__file__).resolve().parent.parent / "forge.yaml"
    try:
        return yaml.safe_load(yaml_path.read_text()) or {}
    except (FileNotFoundError, OSError, yaml.YAMLError):
        return {}


def _save_forge_yaml(data: dict) -> None:
    """写回 forge.yaml。"""
    yaml_path = Path(__file__).resolve().parent.parent / "forge.yaml"
    yaml_path.write_text(
        yaml.dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False)
    )


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request, saved: str = ""):
    y = _load_forge_yaml()
    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "y": y,
            "mask_secret": _mask_secret,
            "mask_db_url": _mask_db_url,
            "saved": saved,
        },
    )


@router.post("/settings/llm", response_class=RedirectResponse)
async def settings_save_llm(
    provider: str  = Form(...),
    model:    str  = Form(default=""),
    api_key:  str  = Form(default=""),
    base_url: str  = Form(default=""),
):
    y = _load_forge_yaml()
    y.setdefault("llm", {})
    y["llm"]["provider"] = provider
    y["llm"]["model"]    = model
    if api_key and not api_key.startswith("*"):
        y["llm"]["api_key"] = api_key
    y["llm"]["base_url"] = base_url
    _save_forge_yaml(y)
    return RedirectResponse(url="/admin/settings?saved=llm", status_code=303)


@router.post("/settings/database", response_class=RedirectResponse)
async def settings_save_database(
    url: str = Form(default=""),
):
    y = _load_forge_yaml()
    y.setdefault("database", {})
    y["database"]["url"] = url
    _save_forge_yaml(y)
    return RedirectResponse(url="/admin/settings?saved=database", status_code=303)


@router.post("/settings/embedding", response_class=RedirectResponse)
async def settings_save_embedding(
    api_key:  str = Form(default=""),
    base_url: str = Form(default=""),
    model:    str = Form(default=""),
    top_k:    str = Form(default="5"),
):
    y = _load_forge_yaml()
    y.setdefault("embedding", {})
    if api_key and not api_key.startswith("*"):
        y["embedding"]["api_key"] = api_key
    y["embedding"]["base_url"] = base_url
    y["embedding"]["model"]    = model
    y["embedding"]["top_k"]    = int(top_k) if top_k.isdigit() else 5
    _save_forge_yaml(y)
    return RedirectResponse(url="/admin/settings?saved=embedding", status_code=303)


@router.post("/settings/registry", response_class=RedirectResponse)
async def settings_save_registry(
    schema_path:          str = Form(default=""),
    metrics_path:         str = Form(default=""),
    disambiguations_path: str = Form(default=""),
    conventions_path:     str = Form(default=""),
):
    y = _load_forge_yaml()
    y.setdefault("registry", {})
    y["registry"]["schema_path"]          = schema_path
    y["registry"]["metrics_path"]         = metrics_path
    y["registry"]["disambiguations_path"] = disambiguations_path
    y["registry"]["conventions_path"]     = conventions_path
    _save_forge_yaml(y)
    return RedirectResponse(url="/admin/settings?saved=registry", status_code=303)


@router.post("/settings/feishu", response_class=RedirectResponse)
async def settings_save_feishu(
    app_id:             str = Form(default=""),
    app_secret:         str = Form(default=""),
    verification_token: str = Form(default=""),
    encrypt_key:        str = Form(default=""),
):
    y = _load_forge_yaml()
    y.setdefault("feishu", {})
    y["feishu"]["app_id"] = app_id
    if app_secret and not app_secret.startswith("*"):
        y["feishu"]["app_secret"] = app_secret
    y["feishu"]["verification_token"] = verification_token
    y["feishu"]["encrypt_key"]        = encrypt_key
    _save_forge_yaml(y)
    return RedirectResponse(url="/admin/settings?saved=feishu", status_code=303)


@router.post("/settings/server", response_class=RedirectResponse)
async def settings_save_server(
    host: str = Form(default="0.0.0.0"),
    port: str = Form(default="8000"),
):
    y = _load_forge_yaml()
    y.setdefault("server", {})
    y["server"]["host"] = host
    y["server"]["port"] = int(port) if port.isdigit() else 8000
    _save_forge_yaml(y)
    return RedirectResponse(url="/admin/settings?saved=server", status_code=303)
