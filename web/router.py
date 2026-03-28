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
from fastapi import APIRouter, Depends, Form, Request
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
from web.auth import (
    require_web_auth,
    require_api_auth,
    set_session_cookie,
    clear_session_cookie,
    _LoginRedirect,
)

logger = logging.getLogger(__name__)

# Chat / API 路由 — 挂载在根级别
chat_router = APIRouter()
# Admin 路由 — 挂载在 /admin 前缀下（全部路由需要 Web 登录验证）
router = APIRouter(dependencies=[Depends(require_web_auth)])

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

# 注册自定义 filter：JSON 输出保留中文（不转义为 \uXXXX）
def _tojson_cn(value):
    return json.dumps(value, ensure_ascii=False)
templates.env.filters["tojson_cn"] = _tojson_cn


# ── 认证路由（login / logout）─────────────────────────────────────────────────

@chat_router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, next: str = "/chat"):
    return templates.TemplateResponse(
        request, "login.html", {"error": None, "next": next}
    )


@chat_router.post("/login", response_class=HTMLResponse)
async def login_submit(
    request: Request,
    password: str = Form(...),
    next: str = Form(default="/chat"),
):
    expected = cfg.AUTH_ADMIN_PASSWORD
    # auth disabled 或未设密码时任意密码均可通过
    if not cfg.AUTH_ENABLED or not expected or password == expected:
        response = RedirectResponse(url=next or "/chat", status_code=303)
        set_session_cookie(response, "admin")
        return response
    return templates.TemplateResponse(
        request,
        "login.html",
        {"error": "密码错误，请重试", "next": next},
        status_code=401,
    )


@chat_router.get("/logout")
async def logout():
    response = RedirectResponse(url="/login", status_code=302)
    clear_session_cookie(response)
    return response


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
async def chat_page(request: Request, _auth=Depends(require_web_auth)):
    return templates.TemplateResponse(request, "chat.html", {})


@chat_router.post("/api/chat", response_class=JSONResponse)
async def api_chat(req: ChatRequest, _auth=Depends(require_api_auth)):
    """调用 Agent process，分析/可视化意图自动走 Pipeline；其他走普通查询。"""
    from agent.pipeline import router as intent_router, runner as pipeline_runner

    pipeline_name = intent_router.route(req.message)

    if pipeline_name in ("analyze", "visualize", "report"):
        # Pipeline 模式：run() 返回 pending_approval 状态，等待用户确认 SQL
        run = await _run_sync(pipeline_runner.run, pipeline_name, req.user_id, req.message)
        # 取 generate stage 的结果（SQL + text）
        gen_stage = next((s for s in run.stages if s.stage == "generate"), None)
        art = gen_stage.artifact if gen_stage else None
        sql = getattr(art, "sql", None) if art else None
        forge_json = getattr(art, "forge_json", None) if art else None
        action = "sql_review" if sql else ("error" if run.status == "failed" else "message")
        text = (gen_stage.error or "Pipeline 启动失败") if run.status == "failed" else ""
        await audit.log(
            user_id=req.user_id, user_message=req.message,
            forge_json=forge_json, sql=sql,
            status="pending" if sql else "error",
            error_message=text or None,
        )
        return {"text": text, "sql": sql, "forge_json": forge_json, "action": action,
                "pipeline": pipeline_name}
    else:
        # 普通查询模式
        resp = await _run_sync(agent_process, req.user_id, req.message)
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
            "retry_count": getattr(resp, "retry_count", 0),
        }


@chat_router.post("/api/approve", response_class=JSONResponse)
async def api_approve(req: ChatRequest, _auth=Depends(require_api_auth)):
    """用户确认 SQL，随后执行并返回结果；若有活跃 Pipeline 则继续后续阶段。"""
    resp = await _run_sync(agent_approve, req.user_id)
    result = {"text": resp.text, "sql": resp.sql, "action": resp.action,
              "columns": None, "rows": None, "row_count": 0, "exec_error": None,
              "analysis": None, "chart_html": None}

    if resp.action == "approved" and resp.sql:
        # 1. 执行 SQL
        cols, rows_raw = [], []
        try:
            text, cols, rows_raw = await _run_sync(execute_with_data, resp.sql)
            result["columns"] = cols
            result["rows"] = [list(r) for r in rows_raw]
            result["row_count"] = len(rows_raw)
            if text.startswith("⚠"):
                result["exec_error"] = text
        except Exception as exc:
            result["exec_error"] = str(exc)

        # 2. 检查是否有活跃 Pipeline，有则注入数据并 resume
        try:
            from agent.memory import memory as _mem
            from agent.pipeline import runner as _runner, QueryResult, Artifact
            run_data = _mem.get_state(req.user_id, "pipeline_run")
            if run_data and run_data.get("status") == "pending_approval":
                # 找到 generate stage artifact，注入 rows / columns
                stages = run_data.get("stages", [])
                for s in stages:
                    if s.get("stage") == "generate" and s.get("artifact"):
                        art = s["artifact"]
                        art["rows"]    = result["rows"] or []
                        art["columns"] = cols
                        art["row_count"] = len(result["rows"] or [])
                        s["artifact"] = art
                run_data["stages"] = stages
                run_data["status"] = "running"
                _mem.set_state(req.user_id, "pipeline_run", run_data)

                # resume pipeline（analyze / chart / report 阶段）
                pipeline_run = await _run_sync(_runner.resume, req.user_id)
                if pipeline_run:
                    # 收集分析报告
                    for sr in pipeline_run.stages:
                        art = sr.artifact
                        if art is None:
                            continue
                        if isinstance(art, dict):
                            art = Artifact.from_dict(art)
                        if art._type == "analysis_report":
                            result["analysis"] = {
                                "summary":          getattr(art, "summary", ""),
                                "insights":         getattr(art, "insights", []),
                                "key_metrics":      getattr(art, "key_metrics", {}),
                                "trend_direction":  getattr(art, "trend_direction", ""),
                                "anomalies":        getattr(art, "anomalies", []),
                                "recommendations":  getattr(art, "recommendations", []),
                            }
                        elif art._type == "chart_spec":
                            result["chart_html"] = getattr(art, "html", None)
                    result["action"] = "pipeline_complete"
        except Exception as exc:
            import logging
            logging.getLogger(__name__).warning("Pipeline resume failed: %s", exc)

    return result


@chat_router.post("/api/cancel", response_class=JSONResponse)
async def api_cancel(req: ChatRequest, _auth=Depends(require_api_auth)):
    """用户取消 SQL。"""
    resp = await _run_sync(agent_cancel, req.user_id)
    return {"text": resp.text, "action": resp.action}


class ExecuteRawRequest(BaseModel):
    sql: str
    user_id: str = "web_user"


@chat_router.post("/api/execute-raw", response_class=JSONResponse)
async def api_execute_raw(req: ExecuteRawRequest, _auth=Depends(require_api_auth)):
    """直接执行用户编辑后的 SQL（跳过 Agent 编译）。"""
    result = {"text": "", "sql": req.sql, "action": "approved",
              "columns": None, "rows": None, "row_count": 0, "exec_error": None,
              "analysis": None, "chart_html": None}
    try:
        text, cols, rows_raw = await _run_sync(execute_with_data, req.sql)
        result["columns"] = cols
        result["rows"] = [list(r) for r in rows_raw]
        result["row_count"] = len(rows_raw)
        if text.startswith("⚠"):
            result["exec_error"] = text
    except Exception as exc:
        result["exec_error"] = str(exc)

    await audit.log(
        user_id=req.user_id,
        user_message="[手动编辑 SQL]",
        forge_json=None,
        sql=req.sql,
        status="approved" if not result["exec_error"] else "error",
        error_message=result["exec_error"],
    )
    return result


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
    return RedirectResponse(url="/admin/dashboard", status_code=302)


# ── Dashboard（概览）──────────────────────────────────────────────────────────

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    schema = _load_schema()
    tables = schema.get("tables", {})
    metrics = _load_metrics()
    disambiguations = _load_disambiguations()
    conventions = _load_conventions()

    # 系统健康检查
    health = {"db": False, "embedding": False}
    try:
        if cfg.DATABASE_URL:
            from sqlalchemy import create_engine, text as sa_text
            engine = create_engine(cfg.DATABASE_URL)
            with engine.connect() as conn:
                conn.execute(sa_text("SELECT 1"))
            health["db"] = True
    except Exception:
        pass
    health["embedding"] = bool(cfg.EMBED_API_KEY)

    # 今日查询数
    today_count = 0
    try:
        from datetime import date as _date
        today_str = _date.today().isoformat()
        import aiosqlite
        async with aiosqlite.connect(audit.DB_PATH) as db:
            await db.execute(audit._DDL)
            cursor = await db.execute(
                "SELECT COUNT(*) FROM audit_log WHERE timestamp >= ?",
                (today_str,),
            )
            row = await cursor.fetchone()
            today_count = row[0] if row else 0
    except Exception:
        pass

    # 最近查询
    recent_queries = await audit.recent(limit=5)

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "table_count": len(tables),
            "metric_count": len(metrics),
            "rule_count": len(disambiguations) + len(conventions),
            "today_query_count": today_count,
            "health": health,
            "llm_model": cfg.LLM_MODEL or "",
            "embed_model": cfg.EMBED_MODEL or "",
            "registry_path": str(cfg.REGISTRY_PATH),
            "recent_queries": recent_queries,
        },
    )


# ── 结构层（表 / 字段）─────────────────────────────────────────────────────────

@router.get("/schema", response_class=HTMLResponse)
async def schema_page(request: Request):
    schema = _load_schema()
    tables = schema.get("tables", {})
    return templates.TemplateResponse(
            request,
            "schema.html",
            {"tables": tables},
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
            request,
            "metrics.html",
            {"atomics": atomics, "derivatives": derivatives, "all_metrics": metrics},
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
                request,
                "metrics.html",
                {"atomics":       atomics,
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
        request,
        "semantic.html",
        {"disambiguations": _load_disambiguations(),
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
            request,
            "staging.html",
            {"records": records,
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
async def audit_page(request: Request, status: str = "", q: str = "", page: int = 1):
    per_page = 50
    offset = (max(1, page) - 1) * per_page
    records, total_filtered = await audit.search(
        status=status, keyword=q, limit=per_page, offset=offset,
    )
    counts = await audit.stats()
    total_pages = max(1, (total_filtered + per_page - 1) // per_page)
    return templates.TemplateResponse(
        request,
        "audit.html",
        {
            "records": records, "counts": counts,
            "filter_status": status, "filter_q": q,
            "page": page, "total_pages": total_pages,
            "total_filtered": total_filtered,
        },
    )


@router.get("/sessions", response_class=HTMLResponse)
async def sessions_page(request: Request, sid: str = ""):
    """对话日志页面：Session 列表 + 单 Session 详情。"""
    from agent.memory import memory
    if sid:
        # 单个 session 详情
        messages = memory.ems.get_full_session(sid)
        return templates.TemplateResponse(
                request,
                "sessions.html",
                {"session_id": sid, "messages": messages, "sessions": []},
            )
    else:
        # session 列表（聚合所有用户）
        try:
            conn = memory.ems._ensure_conn()
            rows = conn.execute(
                "SELECT session_id, user_id, MIN(created_at) as started, MAX(created_at) as ended, COUNT(*) as msg_count "
                "FROM memory_ems WHERE role != 'state' "
                "GROUP BY session_id ORDER BY ended DESC LIMIT 50"
            ).fetchall()
            sessions = [
                {"session_id": r[0], "user_id": r[1], "started": r[2], "ended": r[3], "msg_count": r[4]}
                for r in rows
            ]
        except Exception:
            sessions = []
        return templates.TemplateResponse(
                request,
                "sessions.html",
                {"session_id": "", "messages": [], "sessions": sessions},
            )


@router.get("/pipelines", response_class=HTMLResponse)
async def pipelines_page(request: Request):
    """Pipeline 执行视图。从 EMS 聚合所有查询活动。"""
    from agent.memory import memory
    runs = []
    try:
        conn = memory.ems._ensure_conn()

        # 1. 读取 PipelineRunner 产生的记录
        pr_rows = conn.execute(
            "SELECT tool_output FROM memory_ems "
            "WHERE tool_name = 'pipeline_complete' AND tool_output IS NOT NULL "
            "ORDER BY id DESC LIMIT 30"
        ).fetchall()
        for row in pr_rows:
            try:
                data = json.loads(row[0])
                data["total_ms"] = sum(s.get("duration_ms", 0) for s in data.get("stages", []))
                runs.append(data)
            except (json.JSONDecodeError, TypeError):
                continue

        # 2. 读取直接走 agent.process() 的查询（按 session 聚合）
        query_rows = conn.execute(
            """SELECT
                e.session_id, e.user_id,
                u.content as question,
                e.tool_output as sql,
                e.action,
                e.created_at,
                u.created_at as asked_at
            FROM memory_ems e
            INNER JOIN (
                SELECT session_id, MAX(id) as last_user_id, content, created_at
                FROM memory_ems
                WHERE role = 'user' AND content != '' AND action IS NULL
                GROUP BY session_id
            ) u ON e.session_id = u.session_id
            WHERE e.tool_name = 'generate_forge_query' AND e.action = 'sql_review'
            ORDER BY e.id DESC LIMIT 50"""
        ).fetchall()

        seen_sessions = {r.get("run_id", "") for r in runs}
        for row in query_rows:
            sid, uid, question, sql, action, created_at, asked_at = row
            if sid in seen_sessions:
                continue
            seen_sessions.add(sid)

            # 查找该 session 里是否有 approve/cancel
            status_row = conn.execute(
                "SELECT action FROM memory_ems "
                "WHERE session_id = ? AND action IN ('approved','cancelled') "
                "ORDER BY id DESC LIMIT 1",
                (sid,),
            ).fetchone()
            final_status = "completed" if status_row and status_row[0] == "approved" else (
                "cancelled" if status_row and status_row[0] == "cancelled" else "pending_approval"
            )

            runs.append({
                "run_id": sid,
                "pipeline": "query",
                "user_id": uid or "",
                "team_id": "",
                "question": question or "",
                "status": final_status,
                "started_at": asked_at or created_at or "",
                "ended_at": created_at or "",
                "total_ms": 0,
                "stages": [
                    {"stage": "generate", "agent": "forge_query",
                     "status": "completed", "duration_ms": 0, "error": None},
                ],
            })

        # 按时间倒序
        runs.sort(key=lambda r: r.get("started_at", ""), reverse=True)
        runs = runs[:50]

    except Exception as exc:
        logger.warning("Pipeline page error: %s", exc)
        runs = []

    return templates.TemplateResponse(
            request,
            "pipelines.html",
            {"runs": runs},
        )


# ── 知识源管理 ────────────────────────────────────────────────────────────────

@router.get("/knowledge", response_class=HTMLResponse)
async def knowledge_page(request: Request, flash: str = ""):
    from agent.knowledge import knowledge_store
    candidates = knowledge_store.list_candidates(status="pending", limit=50)
    confirmed = knowledge_store.list_candidates(status="confirmed", limit=20)
    sources = knowledge_store.list_sources(enabled_only=False)
    pending_count = knowledge_store.pending_count()
    return templates.TemplateResponse(
            request,
            "knowledge.html",
            {"candidates": candidates, "confirmed": confirmed,
         "sources": sources, "pending_count": pending_count, "flash": flash},
        )


@router.post("/knowledge/confirm/{cid}", response_class=RedirectResponse)
async def knowledge_confirm(cid: int):
    from agent.knowledge import knowledge_store
    knowledge_store.confirm(cid)
    return RedirectResponse(url="/admin/knowledge?flash=已确认", status_code=303)


@router.post("/knowledge/reject/{cid}", response_class=RedirectResponse)
async def knowledge_reject(cid: int):
    from agent.knowledge import knowledge_store
    knowledge_store.reject(cid)
    return RedirectResponse(url="/admin/knowledge?flash=已忽略", status_code=303)


@router.post("/knowledge/source", response_class=RedirectResponse)
async def knowledge_add_source(
    type: str = Form(...),
    name: str = Form(...),
    url:  str = Form(default=""),
    keywords: str = Form(default=""),
    schedule: str = Form(default="daily"),
):
    from agent.knowledge import knowledge_store
    config = {"schedule": schedule}
    if url:
        config["url"] = url
    if keywords:
        config["keywords"] = keywords
    knowledge_store.add_source(type, name, config)
    return RedirectResponse(url="/admin/knowledge?flash=知识源已添加", status_code=303)


@router.post("/knowledge/source/delete/{sid}", response_class=RedirectResponse)
async def knowledge_delete_source(sid: int):
    from agent.knowledge import knowledge_store
    knowledge_store.delete_source(sid)
    return RedirectResponse(url="/admin/knowledge?flash=已删除", status_code=303)


@router.post("/knowledge/collect", response_class=JSONResponse)
async def knowledge_collect_all():
    """手动触发所有知识源收集。"""
    try:
        from agent.knowledge import knowledge_collector
        stats = knowledge_collector.run_all()
        return JSONResponse({"ok": True, "added": stats["added"], "errors": stats["errors"],
                             "processed": stats["processed"]})
    except Exception as exc:
        logger.warning("Knowledge collect all failed: %s", exc)
        return JSONResponse({"ok": False, "added": 0, "errors": 1, "detail": str(exc)}, status_code=500)


@router.post("/knowledge/collect/{sid}", response_class=JSONResponse)
async def knowledge_collect_one(sid: int):
    """触发单个知识源收集。"""
    try:
        from agent.knowledge import knowledge_store, knowledge_collector
        sources = knowledge_store.list_sources(enabled_only=False)
        source = next((s for s in sources if s["id"] == sid), None)
        if source is None:
            return JSONResponse({"ok": False, "detail": "知识源不存在"}, status_code=404)
        added = knowledge_collector.run_source(source)
        return JSONResponse({"ok": True, "added": added, "errors": 0})
    except Exception as exc:
        logger.warning("Knowledge collect source %s failed: %s", sid, exc)
        return JSONResponse({"ok": False, "added": 0, "errors": 1, "detail": str(exc)}, status_code=500)


# ── 文档导入 ──────────────────────────────────────────────────────────────────

@router.get("/knowledge/import", response_class=HTMLResponse)
async def knowledge_import_page(request: Request):
    return templates.TemplateResponse(request, "import.html", {})


@router.post("/knowledge/import/upload", response_class=JSONResponse)
async def knowledge_import_upload(request: Request):
    """上传文件，LLM 提取知识点，返回预览列表。"""
    import re
    form = await request.form()
    file = form.get("file")
    if file is None:
        return JSONResponse({"ok": False, "detail": "未收到文件"}, status_code=400)

    filename = file.filename or ""
    raw_bytes = await file.read()

    # 解析文本
    text = ""
    if filename.lower().endswith(".pdf"):
        return JSONResponse({"ok": False, "detail": "请将 PDF 转换为 .txt 或 .md 后再导入"}, status_code=400)
    else:
        try:
            text = raw_bytes.decode("utf-8", errors="replace")
        except Exception:
            return JSONResponse({"ok": False, "detail": "文件编码无法识别，请使用 UTF-8 编码"}, status_code=400)

    if not text.strip():
        return JSONResponse({"ok": False, "detail": "文件内容为空"}, status_code=400)

    # 按 2000 字分段，每段用 LLM 提取
    chunk_size = 2000
    chunks = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

    all_items: list[dict] = []
    try:
        from agent import llm as llm_module
        system_prompt = (
            "你是知识提取助手。从以下文档内容中提取3-5条有价值的业务知识点，"
            "每条50字以内，JSON数组格式：[{\"key\":\"知识点标题\",\"value\":\"内容\"}]"
            "只输出 JSON 数组，不要有其他内容。"
        )
        for chunk in chunks[:5]:  # 最多处理前5段
            messages = [{"role": "user", "content": f"文档片段：\n\n{chunk}"}]
            result = llm_module.call(messages, system_override=system_prompt)
            raw = result.get("text", "") or ""
            json_match = re.search(r"\[.*\]", raw, re.DOTALL)
            if json_match:
                items = json.loads(json_match.group())
                for item in items:
                    k = str(item.get("key", "doc_fact"))[:80]
                    v = str(item.get("value", ""))[:500]
                    if v:
                        all_items.append({"key": k, "value": v, "selected": True})
    except Exception as exc:
        logger.info("LLM extraction failed during import: %s", exc)
        # 降级：直接把每段前200字作为一条
        for i, chunk in enumerate(chunks[:5]):
            all_items.append({
                "key": f"{filename}_段落{i + 1}",
                "value": chunk[:200],
                "selected": True,
            })

    if not all_items:
        return JSONResponse({"ok": False, "detail": "未能提取到知识点，请检查文件内容"}, status_code=400)

    # 临时存储到 .forge 目录
    forge_dir = Path(__file__).resolve().parent.parent / ".forge"
    forge_dir.mkdir(exist_ok=True)
    tmp_file = forge_dir / "import_tmp.json"
    tmp_file.write_text(
        json.dumps({"filename": filename, "items": all_items}, ensure_ascii=False),
        encoding="utf-8",
    )

    return JSONResponse({"ok": True, "items": all_items, "filename": filename})


@router.post("/knowledge/import/confirm", response_class=JSONResponse)
async def knowledge_import_confirm(request: Request):
    """确认导入选中的知识点到 KnowledgeStore。"""
    body = await request.json()
    items: list[dict] = body.get("items", [])
    if not items:
        return JSONResponse({"ok": False, "detail": "没有选中的知识点"}, status_code=400)

    from agent.knowledge import knowledge_store
    added = 0
    for item in items:
        k = str(item.get("key", "doc_fact"))[:80]
        v = str(item.get("value", ""))
        if not v:
            continue
        try:
            knowledge_store.add_candidate(
                source="document",
                category="fact",
                key=k,
                value=v,
                extracted_by="llm",
                confidence=0.8,
            )
            added += 1
        except Exception as exc:
            logger.debug("Failed to add import candidate: %s", exc)

    # 清理临时文件
    try:
        tmp_file = Path(__file__).resolve().parent.parent / ".forge" / "import_tmp.json"
        if tmp_file.exists():
            tmp_file.unlink()
    except Exception:
        pass

    return JSONResponse({"ok": True, "added": added})


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
            request,
            "settings.html",
            {"y": y,
            "mask_secret": _mask_secret,
            "mask_db_url": _mask_db_url,
            "saved": saved},
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


@router.post("/settings/auth", response_class=RedirectResponse)
async def settings_save_auth(
    request: Request,
    admin_password: str = Form(default=""),
    api_keys: str = Form(default=""),
):
    # enabled 是 checkbox，未勾选时 Form 不会提交该字段，用 request.form 获取
    form = await request.form()
    enabled = "enabled" in form
    y = _load_forge_yaml()
    y.setdefault("server", {}).setdefault("auth", {})
    y["server"]["auth"]["enabled"] = enabled
    if admin_password and not admin_password.startswith("*"):
        y["server"]["auth"]["admin_password"] = admin_password
    # 解析 api_keys：按行分割，去空白
    keys = [k.strip() for k in api_keys.splitlines() if k.strip()]
    y["server"]["auth"]["api_keys"] = keys
    _save_forge_yaml(y)
    return RedirectResponse(url="/admin/settings?saved=auth", status_code=303)


@router.post("/settings/memory", response_class=RedirectResponse)
async def settings_save_memory(
    db_url:  str = Form(default=""),
    db_path: str = Form(default=".forge/memory.db"),
):
    y = _load_forge_yaml()
    y.setdefault("memory", {})
    y["memory"]["db_url"]  = db_url
    y["memory"]["db_path"] = db_path
    _save_forge_yaml(y)
    return RedirectResponse(url="/admin/settings?saved=memory", status_code=303)


# ── Memory Management ─────────────────────────────────────────────────────────

def _get_smp_entries(limit: int = 200) -> list[dict]:
    """读取所有 SMP 条目（不限 user，管理员视图）。"""
    try:
        from agent.db import get_connection_raw
        conn = get_connection_raw()
        rows = conn.execute(
            "SELECT id, scope, user_id, category, key, value, confidence, updated_at "
            "FROM memory_smp ORDER BY scope, category, updated_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        result = []
        for r in rows:
            try:
                import json
                val = json.loads(r[5])
            except Exception:
                val = r[5]
            result.append({
                "id": r[0], "scope": r[1], "user_id": r[2], "category": r[3],
                "key": r[4], "value": val, "confidence": r[6], "updated_at": r[7],
            })
        return result
    except Exception:
        return []


def _get_ems_stats() -> dict:
    """读取 EMS 统计数据。"""
    try:
        from agent.db import get_connection_raw
        conn = get_connection_raw()
        total_sessions = conn.execute(
            "SELECT COUNT(DISTINCT session_id) FROM memory_ems"
        ).fetchone()[0] or 0
        total_events = conn.execute(
            "SELECT COUNT(*) FROM memory_ems"
        ).fetchone()[0] or 0
        # 按用户统计
        user_rows = conn.execute(
            "SELECT user_id, COUNT(DISTINCT session_id) as sessions, MAX(created_at) as last_active "
            "FROM memory_ems GROUP BY user_id ORDER BY last_active DESC LIMIT 50"
        ).fetchall()
        users = [{"user_id": r[0], "sessions": r[1], "last_active": r[2]} for r in user_rows]
        return {
            "total_sessions": total_sessions,
            "total_events": total_events,
            "active_users": len(users),
            "users": users,
        }
    except Exception:
        return {"total_sessions": 0, "total_events": 0, "active_users": 0, "users": []}


@router.get("/memory", response_class=HTMLResponse)
async def memory_page(request: Request, flash: str = ""):
    import json
    smp_entries = _get_smp_entries()
    ems_stats = _get_ems_stats()
    return templates.TemplateResponse(
        request, "memory.html",
        {"smp_entries": smp_entries, "ems_stats": ems_stats, "flash": flash, "json": json}
    )


@router.post("/memory/smp/delete/{entry_id}", response_class=RedirectResponse)
async def memory_smp_delete(entry_id: int):
    try:
        from agent.db import get_connection_raw
        conn = get_connection_raw()
        conn.execute("DELETE FROM memory_smp WHERE id = ?", (entry_id,))
        conn.commit()
    except Exception:
        pass
    return RedirectResponse(url="/admin/memory?flash=已删除", status_code=303)


@router.post("/memory/ems/clear/{user_id:path}", response_class=RedirectResponse)
async def memory_ems_clear_user(user_id: str):
    try:
        from agent.db import get_connection_raw
        conn = get_connection_raw()
        conn.execute("DELETE FROM memory_ems WHERE user_id = ?", (user_id,))
        conn.commit()
    except Exception:
        pass
    return RedirectResponse(url="/admin/memory?flash=已清空", status_code=303)


@router.post("/memory/ems/clear-all", response_class=RedirectResponse)
async def memory_ems_clear_all():
    try:
        from agent.db import get_connection_raw
        conn = get_connection_raw()
        conn.execute("DELETE FROM memory_ems")
        conn.commit()
    except Exception:
        pass
    return RedirectResponse(url="/admin/memory?flash=全部已清空", status_code=303)


# ── 团队管理 ──────────────────────────────────────────────────────────────────

def _get_all_tables() -> list[str]:
    """从 schema.registry.json 读取所有表名。"""
    try:
        schema = json.loads(cfg.REGISTRY_PATH.read_text())
        return sorted(schema.get("tables", {}).keys())
    except Exception:
        return []


@router.get("/teams", response_class=HTMLResponse)
async def teams_page(request: Request, flash: str = ""):
    from agent.tenant import tenants
    teams = tenants.list_teams()
    all_tables = _get_all_tables()
    # 为每个团队附加当前 ACL
    for t in teams:
        t["allowed_tables"] = tenants.get_allowed_tables(t["team_id"])  # None = 无限制
    return templates.TemplateResponse(
        request, "teams.html",
        {"teams": teams, "all_tables": all_tables, "flash": flash}
    )


@router.post("/teams/create", response_class=RedirectResponse)
async def teams_create(
    team_id:      str = Form(...),
    display_name: str = Form(default=""),
):
    from agent.tenant import tenants
    tenants.create_team(team_id.strip(), display_name.strip() or team_id.strip())
    return RedirectResponse(url="/admin/teams?flash=团队已创建", status_code=303)


@router.post("/teams/{team_id}/acl", response_class=RedirectResponse)
async def teams_save_acl(team_id: str, request: Request):
    form = await request.form()
    # checkbox 多选：getlist
    tables = form.getlist("tables")
    from agent.tenant import tenants
    tenants.set_allowed_tables(team_id, list(tables))
    msg = f"已限制 {len(tables)} 张表" if tables else "权限已清除（不限制）"
    return RedirectResponse(url=f"/admin/teams?flash={msg}", status_code=303)


@router.get("/teams/{team_id}/members", response_class=HTMLResponse)
async def team_members_page(request: Request, team_id: str, flash: str = ""):
    from agent.tenant import tenants
    members = tenants.get_team_members(team_id)
    return templates.TemplateResponse(
        request, "team_members.html",
        {"team_id": team_id, "members": members, "flash": flash}
    )


@router.post("/teams/{team_id}/members/add", response_class=RedirectResponse)
async def team_members_add(
    team_id:      str,
    user_id:      str = Form(...),
    display_name: str = Form(default=""),
    role:         str = Form(default="member"),
):
    from agent.tenant import tenants
    tenants.set_team(user_id.strip(), team_id, display_name.strip(), role)
    return RedirectResponse(url=f"/admin/teams/{team_id}/members?flash=已添加", status_code=303)


@router.post("/teams/{team_id}/members/remove", response_class=RedirectResponse)
async def team_members_remove(
    team_id: str,
    user_id: str = Form(...),
):
    # 把用户移回 default 团队
    from agent.tenant import tenants
    tenants.set_team(user_id, "default")
    return RedirectResponse(url=f"/admin/teams/{team_id}/members?flash=已移除", status_code=303)
