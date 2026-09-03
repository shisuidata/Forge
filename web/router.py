"""
Forge web UI — FastAPI router.

Routes
------
## Chat（查询对话）
GET  /chat                           → Web Chat（统一 Pi ChannelEvent / TaskRun）
POST /api/chat                       → 默认 410；仅显式回滚开关恢复旧 Agent API
POST /api/prepare-query              → 外部 Agent 生成可审核 SQL（不执行）
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

import hashlib
import json
import logging
import os
import re
import shutil
import hmac
import tempfile
from datetime import date
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

import httpx
import yaml
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from agent import audit
from agent import feedback
from agent.agent import process as agent_process
from agent.agent import prepare_query as agent_prepare_query
from agent.agent import approve as agent_approve
from agent.agent import cancel as agent_cancel
from forge.executor import execute_with_data
from config import cfg
from registry.validator import validate_metric
from registry.staging_sync import promote_staged
from web.routes.query_runs import router as query_runs_router
from web.routes.evaluate import router as evaluate_router
from web.routes.context import router as context_router
from web.routes.reports import router as reports_router
from web.routes.memory import router as memory_router
from web.routes.settings import router as settings_router
from web.routes.registry_studio import router as registry_studio_router
from web.routes.product import router as product_router
from web.pi_client import pi_request as _pi_request
from web.routes.accuracy_benchmark import router as accuracy_benchmark_router
from web.routes.benchmark_v2 import router as benchmark_v2_router
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
chat_router.include_router(query_runs_router)
chat_router.include_router(evaluate_router)
chat_router.include_router(context_router)
chat_router.include_router(reports_router)
chat_router.include_router(memory_router)
chat_router.include_router(product_router)
chat_router.include_router(benchmark_v2_router)
# Admin 路由 — 挂载在 /admin 前缀下（全部路由需要 Web 登录验证）
router = APIRouter(dependencies=[Depends(require_web_auth)])
router.include_router(settings_router)
router.include_router(registry_studio_router)

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

# 注册自定义 filter：JSON 输出保留中文（不转义为 \uXXXX）
router.include_router(accuracy_benchmark_router)
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
    # auth disabled 时任意密码均可通过；auth enabled 时必须配置并匹配密码
    if not cfg.AUTH_ENABLED or (expected and hmac.compare_digest(password, expected)):
        response = RedirectResponse(url=_safe_next_path(next), status_code=303)
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
    path = cfg.METRICS_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    content = yaml.safe_dump(
        metrics,
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
    )
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as tmp:
            tmp.write(content)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_path = Path(tmp.name)
        yaml.safe_load(tmp_path.read_text(encoding="utf-8"))
        os.replace(tmp_path, path)
    finally:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()


def _parse_lines(text: str) -> list[str]:
    """Split textarea value into a list, stripping blank lines."""
    return [line.strip() for line in text.splitlines() if line.strip()]


def _safe_next_path(next_path: str) -> str:
    """Allow redirects only to local absolute paths."""
    if not next_path or not next_path.startswith("/") or next_path.startswith("//"):
        return "/chat"
    return next_path


# ── Chat API ──────────────────────────────────────────────────────────────────

import asyncio
import time
from functools import partial

class ChatRequest(BaseModel):
    message: str
    user_id: str = "web_user"


class PrepareQueryRequest(BaseModel):
    question: str
    user_id: str = "external-agent"
    dialect: Optional[str] = None


class PiTaskCreateRequest(BaseModel):
    message: str
    intent: str = "query_prepare"
    channel_conversation_id: Optional[str] = None


class PiWebChatMessageRequest(BaseModel):
    message: str
    conversation_id: str
    message_id: str


class PiWebChatActionRequest(BaseModel):
    action: str
    conversation_id: str
    message_id: str
    payload: dict = Field(default_factory=dict)


class PiPrepareQueryRequest(BaseModel):
    question: str
    dialect: Optional[str] = None
    idempotency_key: Optional[str] = None
    run_async: bool = False


class PiSkillStageRequest(BaseModel):
    message: str
    idempotency_key: Optional[str] = None
    run_async: bool = False


class PiAdvisorySkillRequest(BaseModel):
    skill_name: str
    prompt: str
    idempotency_key: str


class PiAnalyzeRequest(BaseModel):
    question: Optional[str] = None
    idempotency_key: Optional[str] = None
    run_async: bool = False


class PiRenderReportRequest(BaseModel):
    audience: str
    idempotency_key: Optional[str] = None
    run_async: bool = False


class PiSupplementRequest(BaseModel):
    suggested_query_index: int
    idempotency_key: str


class PiResumeAnalysisRequest(BaseModel):
    child_task_run_id: str
    idempotency_key: str
    run_async: bool = False


class PiApproveQueryRequest(BaseModel):
    query_run_id: str
    sql_hash: str
    idempotency_key: str
    run_async: bool = False


def _pi_stage_payload(request: BaseModel) -> dict:
    payload = request.model_dump(exclude_none=True)
    if payload.pop("run_async", False):
        payload["async"] = True
    return payload


def _pi_disabled_response():
    return JSONResponse(
        {"status": "disabled", "error": "Pi Orchestrator is not enabled"},
        status_code=503,
    )


def _run_sync(fn, *args):
    """在线程池中执行同步函数，避免阻塞事件循环。"""
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(None, partial(fn, *args))



def _capability(
    domain: str,
    title: str,
    status: str,
    status_label: str,
    copy: str,
    dependency: str,
    action_label: str,
    href: str | None = None,
    features: tuple[str, ...] = (),
) -> dict:
    return {
        "domain": domain,
        "title": title,
        "status": status,
        "status_label": status_label,
        "copy": copy,
        "dependency": dependency,
        "action_label": action_label,
        "href": href,
        "features": features,
    }


_PRODUCT_SURFACES = {
    "governance": {
        "active": "governance",
        "page_title": "治理与审计",
        "page_eyebrow": "决策、证据与责任",
        "capability_kicker": "Shared Trust & Data Foundation",
        "capability_title": "治理与审计",
        "capability_status": "partial",
        "capability_status_label": "部分可用",
        "capability_copy": "查看精确审批、Evidence、Assurance 和 Action 记录；未进入 Runtime 的 Policy 与 Mandate 明确保持关闭。",
        "capability_boundary": "查询审批与现有 Audit 可用；通用 Decision、Policy PEP 与职责分离未实现。",
        "capability_dependency": "M1A Runtime Trust / M3 Coordination",
        "capability_notice": "Contract Coverage 为 100% 不等于 Runtime Governance Coverage。当前 Runtime Coverage 仍为 0%。",
        "capabilities": (
            _capability("Decision", "Decision Inbox", "partial", "部分可用", "集中查看等待确认的精确 Action。当前只接真实 SQL Approval。", "通用 DecisionRequest/Record：M3", "查看决策边界", "/governance/decisions", ("SQL hash 审批", "等待确认任务", "过期与失效")),
            _capability("Evidence", "Evidence & Assurance", "partial", "部分可用", "从结论回到 QueryResult、来源、范围、截断与 Assurance。", "平台级质量指标：Q1", "查看 Evidence 边界", "/governance/evidence", ("Query Evidence", "Assurance lineage", "限制与失败")),
            _capability("Audit", "Action Audit", "available", "可用", "查看查询、审批、执行和现有领域 Action 的审计记录。", "现有 Forge Audit Store", "打开审计", "/governance/audit", ("查询审计", "执行状态", "时间与责任主体")),
            _capability("Policy", "Policy & Mandate", "blocked", "未接入 Runtime", "定义 Principal、Mandate、Policy、资源范围和例外处理。", "M1A 生产 PEP", "查看未开放边界", "/governance/policies", ("PrincipalContext", "DelegatedMandate", "Default Deny")),
        ),
    },
    "governance/decisions": {
        "active": "governance", "page_title": "Decision Inbox", "page_eyebrow": "精确操作确认",
        "capability_kicker": "Human Accountable", "capability_title": "Decision Inbox",
        "capability_status": "partial", "capability_status_label": "查询审批可用",
        "capability_copy": "Decision 只代表有权主体批准精确 Action，不代表批准结论正确。",
        "capability_boundary": "当前只投影 SQL Approval；Registry、Report、Policy 等通用 Decision 尚未统一。",
        "capability_dependency": "M3 Participant / Decision Runtime", "capability_notice": "审批对象变化、身份变化、Registry revision 变化或过期都会使原批准失效。",
        "back_href": "/governance", "back_label": "治理与审计",
        "capabilities": (
            _capability("Query", "SQL Decision", "available", "可用", "从工作台或任务详情审核精确 SQL 与 Assurance hash。", "现有 Forge QueryRun", "查看待确认", "/workspace", ("精确 SQL", "只读执行", "过期失败关闭")),
            _capability("Coordination", "通用 Decision History", "planned", "规划中", "按 Action、Decision Owner、scope 和 revision 查询统一决策记录。", "M3 DecisionRecord", "等待 Decision Runtime", None, ("Decision Request", "Decision Record", "职责分离")),
        ),
    },
    "governance/evidence": {
        "active": "governance", "page_title": "Evidence & Assurance", "page_eyebrow": "来源、范围与限制",
        "capability_kicker": "Assurance", "capability_title": "Evidence & Assurance",
        "capability_status": "partial", "capability_status_label": "查询链可用",
        "capability_copy": "把 QueryResult、分析、报告和审批重新连接到来源、语义、执行与限制。",
        "capability_boundary": "Query Evidence 已进入任务详情；跨 Artifact Evidence Library 与质量指标尚未统一。",
        "capability_dependency": "Q1 Platform Assurance", "capability_notice": "Evidence 证明特定来源、时间和语义下的结果，不自动成为无作用域事实。",
        "back_href": "/governance", "back_label": "治理与审计",
        "capabilities": (
            _capability("Task", "Task Evidence", "available", "可用", "查看 QueryResult、Analysis、Report 的 Artifact 与 Evidence 引用。", "现有 Pi/Forge Product Projection", "打开任务", "/tasks", ("Artifact lineage", "QueryResult preview", "失败与限制")),
            _capability("Navigation", "Evidence Drawer", "planned", "规划中", "在 Conversation、Task 和 Deliverable 中原地查看来源、范围、行和限制，不离开当前上下文。", "Q1 Evidence Index", "等待 Evidence Drawer", None, ("Contextual source", "Claim/Evidence split", "Deep link")),
            _capability("Assurance", "Query Assurance", "partial", "部分可用", "审查 SQL、Registry、Policy、Model 与执行 revision。", "现有 QueryRun/Audit", "打开审计", "/admin/audit", ("SQL hash", "Registry revision", "执行状态")),
            _capability("Quality", "Assurance Metrics", "planned", "规划中", "统一展示 Coverage、Clarification、Safe Abstention、Silent Error 和 Evidence Coverage。", "Q1 Quality Contract", "等待 Quality Runtime", None),
        ),
    },
    "governance/audit": {
        "active": "governance", "page_title": "Action Audit", "page_eyebrow": "谁依据什么做了什么",
        "capability_kicker": "Audit", "capability_title": "Action Audit",
        "capability_status": "available", "capability_status_label": "可用",
        "capability_copy": "现有审计继续由领域 Store 持有；Product Shell 只组织入口，不复制记录。",
        "capability_boundary": "Query Audit 已可用；跨 Registry、Model、Report 的统一资源权限仍在演进。",
        "capability_dependency": "现有 Audit Store / 后续统一 Resource Policy", "capability_notice": None,
        "back_href": "/governance", "back_label": "治理与审计",
        "capabilities": (
            _capability("Query", "查询审计", "available", "可用", "查看待确认、已执行、取消和错误记录。", "Forge Audit Store", "打开查询审计", "/admin/audit", ("状态筛选", "SQL 与时间", "执行结果")),
            _capability("System", "跨领域 Action Audit", "planned", "规划中", "统一检索 Registry、Model、Skill Policy、Report 分享与 Agent Action。", "M1A Resource Policy / M3 Decision", "等待统一 Audit Index", None),
        ),
    },
    "governance/policies": {
        "active": "governance", "page_title": "Policy & Mandate", "page_eyebrow": "授权与资源范围",
        "capability_kicker": "Runtime Governance", "capability_title": "Policy & Mandate",
        "capability_status": "blocked", "capability_status_label": "Runtime 未接入",
        "capability_copy": "未来由 Principal、task-scoped Mandate、Policy、Binding 和 Default Deny 共同约束 Agent Action。",
        "capability_boundary": "Schema/Contract 已评审；生产 PEP 尚未执行，Runtime Governance Coverage 为 0%。",
        "capability_dependency": "M1A Runtime Trust Foundation", "capability_notice": "本页面不会创建 Mandate、修改 Policy 或生成看似有效的授权记录。",
        "back_href": "/governance", "back_label": "治理与审计",
        "capabilities": (
            _capability("Identity", "Principal & Membership", "blocked", "未开放", "区分 actor、accountable principal、organization 和 workspace。", "M1A Identity Boundary", "等待身份 Runtime", None),
            _capability("Delegation", "Delegated Mandate", "blocked", "未开放", "绑定 task、purpose、audience、capability、resource scope 和 expiry。", "M1A Mandate Store", "等待 Mandate Runtime", None),
            _capability("Policy", "Policy Decision", "blocked", "未开放", "生产 PEP 对每个受支持 Action 返回 allow、deny 或 conditional。", "M1A Policy PEP", "等待 Policy Runtime", None),
        ),
    },
    "runtime": {
        "active": "runtime", "page_title": "Agents & Apps", "page_eyebrow": "受控数据能力接入",
        "capability_kicker": "Agent-facing Trusted Data Runtime", "capability_title": "Agents & Apps",
        "capability_status": "blocked", "capability_status_label": "执行未开放",
        "capability_copy": "其他 Agent 将通过 Principal、Mandate、Task、Policy 和 Evidence 使用 Forge，而不是获得裸 SQL 或超级 Token。",
        "capability_boundary": "外部 prepare-query 安全边界保留；Agent Task execute、credential 与 Human takeover 尚未开放。",
        "capability_dependency": "M1A Runtime Trust → R1 Agent Data Runtime", "capability_notice": "当前页面只固定产品对象和边界，不创建 Agent Client，不回显 Credential，不开放执行。",
        "capabilities": (
            _capability("Client", "Agent Clients", "blocked", "未开放", "登记 Client、Owner、Purpose、Workspace 与 expiry。", "M1A Principal/Mandate", "查看 Client 边界", "/runtime/clients", ("Owner", "Purpose", "Workspace scope")),
            _capability("API", "Data Task API", "blocked", "未开放", "提交受治理的数据任务并等待 human clarification/Decision。", "R1 Agent Data Runtime", "查看 API 边界", "/runtime/tools", ("Data Task", "Approval wait", "Structured Artifact")),
            _capability("Operations", "Agent Activity", "blocked", "未开放", "查看调用、失败、Evidence、人工接管和重复抑制。", "R1 Agent Consumer", "查看活动边界", "/runtime/activity", ("Task lineage", "Failure reason", "Human takeover")),
        ),
    },
    "runtime/clients": {
        "active": "runtime", "page_title": "Agent Clients", "page_eyebrow": "Owner、Purpose 与 Mandate",
        "capability_kicker": "Agent Access", "capability_title": "Agent Clients",
        "capability_status": "blocked", "capability_status_label": "未开放",
        "capability_copy": "Agent Client 不是 API Key 列表，而是绑定 Principal、Owner、Purpose、Mandate 和资源范围的受托客户端。",
        "capability_boundary": "没有 Agent Client Store；任何创建、轮换和撤销 Action 均不可用。",
        "capability_dependency": "M1A Principal/Mandate Store", "capability_notice": "不会用示例 Client 或假 Token 填充页面。",
        "back_href": "/runtime", "back_label": "Agents & Apps",
        "capabilities": (
            _capability("Client", "Client Registry", "blocked", "未开放", "登记 Owner、Purpose、Workspace、expiry 和状态。", "M1A Client Store", "等待 Client Runtime", None),
            _capability("Credential", "Credential Lifecycle", "blocked", "未开放", "只允许创建、轮换和撤销；Secret 永不回显。", "M1A Secret Boundary", "等待 Credential Runtime", None),
        ),
    },
    "runtime/tools": {
        "active": "runtime", "page_title": "Data Runtime API", "page_eyebrow": "受治理的 Task 与 Artifact",
        "capability_kicker": "Runtime Contract", "capability_title": "Data Runtime API",
        "capability_status": "partial", "capability_status_label": "准备查询可用",
        "capability_copy": "当前外部边界只能安全准备待审核查询；完整 Agent Data Task 闭环尚未开放。",
        "capability_boundary": "`/api/prepare-query` 不执行 SQL、不批准 Action、不返回结果集。",
        "capability_dependency": "R1 Agent Data Runtime", "capability_notice": "现有接口不会被放宽成通用 execute API。",
        "back_href": "/runtime", "back_label": "Agents & Apps",
        "capabilities": (
            _capability("Query", "Prepare Query", "available", "可用", "生成待审核 SQL，不执行、不批准、不返回结果集。", "现有 Forge API", "查看 API Contract", "/docs", ("Bounded context", "SQL preview", "No execution")),
            _capability("Task", "Data Task", "blocked", "未开放", "Agent 提交 Purpose、Deliverable 并等待澄清与 Decision。", "R1 Agent Runtime", "等待 Task API", None),
            _capability("Artifact", "Structured Result", "blocked", "未开放", "消费 QueryResult、Analysis、Report reference 与 Evidence。", "R1 Artifact API", "等待 Artifact API", None),
        ),
    },
    "runtime/activity": {
        "active": "runtime", "page_title": "Agent Activity", "page_eyebrow": "调用、失败与人工接管",
        "capability_kicker": "Operations", "capability_title": "Agent Activity",
        "capability_status": "blocked", "capability_status_label": "无真实消费者",
        "capability_copy": "未来展示 Agent 发起的同一 Task、Decision、Artifact、Evidence 和 Outcome，不建立第二套活动日志。",
        "capability_boundary": "当前没有已注册 Agent Client 或 Agent Golden Journey。",
        "capability_dependency": "R1 真实 Agent Consumer", "capability_notice": "没有真实调用，因此保持空态，不生成示例活动。",
        "back_href": "/runtime", "back_label": "Agents & Apps",
        "capabilities": (
            _capability("Activity", "Task Activity", "blocked", "未开放", "按 Client、Task、Action 和状态查看真实调用。", "R1 Agent Runtime", "等待真实调用", None),
            _capability("Takeover", "Human Takeover", "blocked", "未开放", "人在 needs_input、waiting_decision 或 failed 时接管同一 Task。", "R1 Human Handoff", "等待 Handoff Contract", None),
        ),
    },
    "inbox": {
        "active": "", "page_title": "待办与通知", "page_eyebrow": "需要你处理的事项",
        "capability_kicker": "Inbox", "capability_title": "待办与通知",
        "capability_status": "partial", "capability_status_label": "任务待办可用",
        "capability_copy": "汇总需要补充、等待确认和失败恢复的真实 Task；通用通知订阅尚未实现。",
        "capability_boundary": "当前待办来自 Pi Task/Query Approval，不新增通知状态库。",
        "capability_dependency": "现有 Workspace Projection / 后续 Notification Contract", "capability_notice": "通知只提示真实对象，不复制或推进 Task 状态。",
        "capabilities": (
            _capability("Decision", "等待确认", "available", "可用", "查看等待 SQL Approval 的精确 Action。", "现有 QueryRun/Workspace", "打开待确认", "/workspace", ("SQL Approval", "expiry", "只读执行")),
            _capability("Task", "需要补充与失败恢复", "available", "可用", "查看 needs_input、failed 和 partial Task。", "现有 Pi Task Store", "打开任务", "/tasks", ("needs_input", "failed", "partial")),
            _capability("Notification", "通知订阅", "planned", "规划中", "按角色、资源和风险订阅 Decision、Conflict、Quality 与 Agent 事件。", "Notification Contract", "等待通知 Runtime", None),
        ),
    },
    "manage": {
        "active": "manage", "page_title": "管理", "page_eyebrow": "部署与运行设置",
        "capability_kicker": "System", "capability_title": "管理中心",
        "capability_status": "available", "capability_status_label": "现有入口可用",
        "capability_copy": "统一组织 Workspace、Model、Skill、Channel、Database 和系统状态；写入继续由现有 Admin/Domain Store 持有。",
        "capability_boundary": "当前仍是单用户私有部署；本页不把旧 Admin 状态复制到 Product Shell。",
        "capability_dependency": "现有 Forge Admin", "capability_notice": None,
        "capabilities": (
            _capability("Workspace", "Team & Workspace", "partial", "部分可用", "查看团队与成员入口；多 Workspace Policy 尚未进入 Runtime。", "现有 Team Admin / M1B", "管理团队", "/admin/teams"),
            _capability("Models", "Model & Skill", "available", "可用", "配置模型、Skill 与兼容性边界。", "现有 Settings/Model Control", "打开设置", "/admin/settings"),
            _capability("Connections", "Channel & Database", "available", "可用", "配置飞书、数据库和服务连接。", "现有 Settings", "打开连接设置", "/admin/settings"),
            _capability("Readiness", "System Readiness", "available", "可用", "查看依赖、最近查询与运行诊断。", "现有 Dashboard", "打开系统诊断", "/admin/dashboard"),
            _capability("Audit", "Audit", "available", "可用", "查看查询和执行审计记录。", "现有 Audit Store", "打开审计", "/admin/audit"),
        ),
    },
    "search": {
        "active": "", "page_title": "搜索", "page_eyebrow": "跨对象查找",
        "capability_kicker": "Global Navigation", "capability_title": "全局搜索",
        "capability_status": "planned", "capability_status_label": "规划中",
        "capability_copy": "未来按权限查找 Conversation、Task、Deliverable、Data Asset、Evidence 和 Agent Client。",
        "capability_boundary": "当前没有统一、scope-aware 的跨 Store Search Index。",
        "capability_dependency": "跨对象只读索引与 Resource Policy", "capability_notice": "搜索框不会在没有索引和权限门禁时伪造结果。",
        "capabilities": (
            _capability("Work", "Work Search", "planned", "规划中", "Conversation、Task、Decision 和 Deliverable。", "Product Search Index", "等待搜索索引", None),
            _capability("Trust", "Trust Search", "planned", "规划中", "Data Asset、Evidence、Policy、Audit 和 revision。", "Resource Policy / Search Index", "等待信任索引", None),
        ),
    },
    "data/quality": {
        "active": "data", "page_title": "Quality & Freshness", "page_eyebrow": "数据质量与时效",
        "capability_kicker": "Data Trust", "capability_title": "Quality & Freshness",
        "capability_status": "planned", "capability_status_label": "规划中",
        "capability_copy": "展示 Data Asset 的质量状态、freshness、影响任务和可采用边界。",
        "capability_boundary": "当前没有统一 Quality Contract 或 Runtime 指标。",
        "capability_dependency": "G1 Data Trust / Q1 Quality Contract", "capability_notice": "不会用静态绿灯冒充真实质量。",
        "back_href": "/data", "back_label": "数据资产",
        "capabilities": (
            _capability("Quality", "Quality Status", "planned", "规划中", "规则、结果、时间和影响范围。", "Quality Contract", "等待 Quality Runtime", None),
            _capability("Freshness", "Freshness", "planned", "规划中", "数据快照、迟到和过期状态。", "Datasource Snapshot Contract", "等待 Freshness Runtime", None),
        ),
    },
    "data/conflicts": {
        "active": "data", "page_title": "Conflict & Proposal", "page_eyebrow": "语义缺口与修订",
        "capability_kicker": "Data Stewardship", "capability_title": "Conflict & Proposal",
        "capability_status": "partial", "capability_status_label": "审核入口可用",
        "capability_copy": "保留多个有作用域的 Claim，通过 Proposal、Review 和 Registry revision 处理冲突。",
        "capability_boundary": "Staging/Knowledge 审核可用；通用 Claim/ConflictSet/Impact 尚未实现。",
        "capability_dependency": "G1 Claim/Conflict Runtime", "capability_notice": "新规则不会静默覆盖旧 revision，也不会自动继承旧审批。",
        "back_href": "/data", "back_label": "数据资产",
        "capabilities": (
            _capability("Staging", "Registry Staging", "available", "可用", "审核并提升结构与语义候选。", "现有 Staging Store", "打开 Staging", "/admin/staging"),
            _capability("Knowledge", "Knowledge Review", "available", "可用", "审核知识候选和来源。", "现有 Knowledge Store", "打开知识审核", "/admin/knowledge"),
            _capability("Revision", "Diff & Revision Viewer", "partial", "部分可用", "比较 Registry Draft/Revision 和确定性 diff；后续扩展到 Policy 与 Deliverable Definition。", "现有 Registry Studio / G1 Impact", "打开 Registry Studio", "/admin/registry-studio", ("Draft vs revision", "Dangerous diff", "Rollback lineage")),
            _capability("Conflict", "Conflict Set", "planned", "规划中", "比较 Claim、scope、来源、影响和修订 Decision。", "G1 Conflict Runtime", "等待 Conflict Runtime", None),
        ),
    },
    "deliverables/reusable": {
        "active": "deliverables", "page_title": "Reusable Deliverables", "page_eyebrow": "定义、运行与修订",
        "capability_kicker": "Deliverable", "capability_title": "Reusable Deliverables",
        "capability_status": "planned", "capability_status_label": "规划中",
        "capability_copy": "把已确认的语义查询、判断标准和交付方式保存为版本化定义，再创建不可变 Run。",
        "capability_boundary": "现有 Report revision 可用；Definition、SemanticQuerySpec 和 Run History 尚未实现。",
        "capability_dependency": "H6 Reusable Deliverables", "capability_notice": "保存模板不会隐式获得自动调度或免审批执行权。",
        "back_href": "/deliverables", "back_label": "交付",
        "capabilities": (
            _capability("Report", "Report Library", "available", "可用", "查看现有不可变报告与导出。", "现有 Report Store", "打开报告", "/reports"),
            _capability("Definition", "Reusable Definition", "planned", "规划中", "版本化 SemanticQuery、Criteria、Skill 和 Delivery policy。", "H6 Definition Contract", "等待 Definition Runtime", None),
            _capability("Run", "Run History", "planned", "规划中", "比较不可变 Run、数据快照和 revision lineage。", "H6 ReportRun", "等待 Run Runtime", None),
        ),
    },
    "deliverables/outcomes": {
        "active": "deliverables", "page_title": "Outcome & Feedback", "page_eyebrow": "采用、纠错与复用",
        "capability_kicker": "Learning Loop", "capability_title": "Outcome & Feedback",
        "capability_status": "planned", "capability_status_label": "规划中",
        "capability_copy": "记录交付是否被采用、修正和复用，并形成受审核的知识或规则 Proposal。",
        "capability_boundary": "当前没有统一 Outcome Ledger；反馈不能自动污染组织知识。",
        "capability_dependency": "Q1 Outcome Record / G1 Proposal", "capability_notice": "不会用点击量或消息数冒充可信 Outcome。",
        "back_href": "/deliverables", "back_label": "交付",
        "capabilities": (
            _capability("Outcome", "Outcome Acceptance", "planned", "规划中", "记录采用、拒绝、覆盖和责任主体。", "Q1 Outcome Record", "等待 Outcome Runtime", None),
            _capability("Feedback", "Correction Proposal", "planned", "规划中", "把失败或修正转为 Registry/Policy/Test Proposal。", "G1 Proposal Runtime", "等待 Proposal Runtime", None),
        ),
    },
}


_PRODUCT_STATE_PAGES = {
    "not_found": {
        "active": "", "page_title": "页面未找到", "page_eyebrow": "未知 Route",
        "state": "empty", "state_label": "未找到", "state_kicker": "404",
        "state_title": "这里没有可用的产品页面",
        "state_copy": "该 Route 不属于当前 Product Map，或对象已经被移除。系统不会猜测相似资源，也不会泄漏不可见对象是否存在。",
        "state_impact": "只影响当前页面；不会改变 Conversation、Task、Approval 或 Report。",
        "state_next_step": "返回工作台，或通过主导航进入已定义的产品面。",
        "primary_href": "/workspace", "primary_label": "返回工作台",
        "secondary_href": "/search", "secondary_label": "打开搜索",
    },
    "forbidden": {
        "active": "", "page_title": "没有权限", "page_eyebrow": "Policy 拒绝",
        "state": "forbidden", "state_label": "没有权限", "state_kicker": "403",
        "state_title": "当前身份不能访问这个范围",
        "state_copy": "Forge 会失败关闭且不披露资源是否存在。权限不能从 URL、Prompt 或请求体自行扩大。",
        "state_impact": "当前资源或 Action 不可见、不可执行；其他已授权页面不受影响。",
        "state_next_step": "返回有权访问的工作区；未来由合法 Membership/Policy 流程申请访问。",
        "primary_href": "/workspace", "primary_label": "返回工作台",
        "secondary_href": "/governance/policies", "secondary_label": "查看 Policy 边界",
    },
    "offline": {
        "active": "", "page_title": "依赖不可用", "page_eyebrow": "安全降级",
        "state": "offline", "state_label": "依赖不可用", "state_kicker": "Offline",
        "state_title": "部分产品能力暂时不可用",
        "state_copy": "页面会保留仍可读取的交付与数据资产，并明确标注受影响能力；高风险 Action 不会自动重放。",
        "state_impact": "依赖该 Runtime 的对话、任务或写入暂不可用；已发布的不可变交付可能仍可读取。",
        "state_next_step": "安全刷新当前页面；若依赖恢复，页面会重新从正式真相源读取。",
        "primary_href": "/workspace", "primary_label": "检查工作台",
        "secondary_href": "/manage", "secondary_label": "查看系统状态",
    },
}


def _product_state_page(request: Request, state_key: str, status_code: int = 200):
    state = _PRODUCT_STATE_PAGES[state_key]
    return templates.TemplateResponse(
        request,
        "product_state.html",
        dict(state),
        status_code=status_code,
    )


def _product_surface_page(request: Request, surface_key: str):
    surface = _PRODUCT_SURFACES.get(surface_key)
    if surface is None:
        return _product_state_page(request, "not_found", 404)
    return templates.TemplateResponse(
        request,
        "product_capability.html",
        dict(surface),
    )

@chat_router.get("/chat", response_class=HTMLResponse)
async def chat_page(request: Request, _auth=Depends(require_web_auth)):
    """First-class Web channel backed by Pi ChannelEvent and TaskRun contracts."""
    return templates.TemplateResponse(
        request,
        "product_chat.html",
        {
            "active": "chat",
            "page_title": "对话",
            "page_eyebrow": "连续交互",
            "pi_enabled": cfg.PI_ORCHESTRATOR_ENABLED,
        },
    )


@chat_router.get("/tasks", response_class=HTMLResponse)
async def task_workspace_page(request: Request, _auth=Depends(require_web_auth)):
    """Canonical Pi task UI with Artifact rendering and non-executable SQL review."""
    return templates.TemplateResponse(
        request,
        "product_tasks.html",
        {
            "active": "tasks",
            "page_title": "任务",
            "page_eyebrow": "执行与恢复",
            "pi_enabled": cfg.PI_ORCHESTRATOR_ENABLED,
        },
    )


@chat_router.get("/workspace", response_class=HTMLResponse)
async def product_workspace_page(request: Request, _auth=Depends(require_web_auth)):
    return templates.TemplateResponse(
        request,
        "product_workspace.html",
        {"active": "workspace", "page_title": "工作台", "page_eyebrow": "当前工作"},
    )


@chat_router.get("/tasks/{task_run_id}", response_class=HTMLResponse)
async def product_task_detail_page(
    task_run_id: str,
    request: Request,
    _auth=Depends(require_web_auth),
):
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return _product_state_page(request, "not_found", 404)
    return templates.TemplateResponse(
        request,
        "product_task_detail.html",
        {
            "active": "tasks",
            "page_title": "任务详情",
            "page_eyebrow": "状态、证据与操作",
            "task_run_id": task_run_id,
        },
    )


@chat_router.get("/deliverables", response_class=HTMLResponse)
@chat_router.get("/reports", response_class=HTMLResponse)
async def product_report_library_page(request: Request, _auth=Depends(require_web_auth)):
    return templates.TemplateResponse(
        request,
        "product_reports.html",
        {"active": "deliverables", "page_title": "交付", "page_eyebrow": "报告、导出与复用"},
    )


@chat_router.get("/deliverables/{surface_key}", response_class=HTMLResponse)
async def product_deliverable_surface_page(
    surface_key: str,
    request: Request,
    _auth=Depends(require_web_auth),
):
    return _product_surface_page(request, f"deliverables/{surface_key}")


@chat_router.get("/data", response_class=HTMLResponse)
async def product_data_page(request: Request, _auth=Depends(require_web_auth)):
    return templates.TemplateResponse(
        request,
        "product_data.html",
        {"active": "data", "page_title": "数据资产", "page_eyebrow": "结构、语义与质量"},
    )


@chat_router.get("/data/{surface_key}", response_class=HTMLResponse)
async def product_data_surface_page(
    surface_key: str,
    request: Request,
    _auth=Depends(require_web_auth),
):
    return _product_surface_page(request, f"data/{surface_key}")


@chat_router.get("/governance", response_class=HTMLResponse)
async def product_governance_page(request: Request, _auth=Depends(require_web_auth)):
    return _product_surface_page(request, "governance")


@chat_router.get("/governance/{surface_key}", response_class=HTMLResponse)
async def product_governance_surface_page(
    surface_key: str,
    request: Request,
    _auth=Depends(require_web_auth),
):
    return _product_surface_page(request, f"governance/{surface_key}")


@chat_router.get("/runtime", response_class=HTMLResponse)
async def product_runtime_page(request: Request, _auth=Depends(require_web_auth)):
    return _product_surface_page(request, "runtime")


@chat_router.get("/runtime/{surface_key}", response_class=HTMLResponse)
async def product_runtime_surface_page(
    surface_key: str,
    request: Request,
    _auth=Depends(require_web_auth),
):
    return _product_surface_page(request, f"runtime/{surface_key}")


@chat_router.get("/manage", response_class=HTMLResponse)
async def product_manage_page(request: Request, _auth=Depends(require_web_auth)):
    return _product_surface_page(request, "manage")


@chat_router.get("/search", response_class=HTMLResponse)
async def product_search_page(request: Request, _auth=Depends(require_web_auth)):
    return _product_surface_page(request, "search")


@chat_router.get("/inbox", response_class=HTMLResponse)
async def product_inbox_page(request: Request, _auth=Depends(require_web_auth)):
    return _product_surface_page(request, "inbox")


@chat_router.get("/forbidden", response_class=HTMLResponse)
async def product_forbidden_state_page(request: Request, _auth=Depends(require_web_auth)):
    return _product_state_page(request, "forbidden")


@chat_router.get("/offline", response_class=HTMLResponse)
async def product_offline_state_page(request: Request, _auth=Depends(require_web_auth)):
    return _product_state_page(request, "offline")

def _valid_web_event_id(value: str) -> bool:
    return re.fullmatch(r"web_[A-Za-z0-9_-]{8,128}", value) is not None


def _web_action_event_id(
    message_id: str,
    task_run_id: str,
    action: str,
    payload: dict,
) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(
        f"{message_id}\n{task_run_id}\n{action}\n{canonical}".encode()
    ).hexdigest()
    return f"web_action_{digest[:40]}"


def _web_admin_task_scopes() -> list[tuple[str, str]]:
    scopes: list[tuple[str, str]] = []
    for raw_scope in cfg.PI_WEB_ADMIN_TASK_SCOPES.split(","):
        org_id, separator, team_id = raw_scope.strip().partition(":")
        if (
            separator
            and re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", org_id)
            and re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", team_id)
        ):
            scopes.append((org_id, team_id))
    return scopes


def _web_admin_can_observe(task: dict) -> bool:
    return (task.get("org_id"), task.get("team_id")) in set(_web_admin_task_scopes())


async def _pi_scoped_task_get(task_run_id: str, suffix: str = "") -> tuple[int, dict]:
    task_status, task_data = await _pi_request("GET", f"/v1/tasks/{task_run_id}")
    task = task_data.get("task") if isinstance(task_data, dict) else None
    if task_status != 200 or not isinstance(task, dict):
        return task_status, task_data
    if not _web_admin_can_observe(task):
        return 404, {"status": "not_found"}
    if not suffix:
        return task_status, task_data
    return await _pi_request("GET", f"/v1/tasks/{task_run_id}/{suffix}")


def _bounded_web_execution_plan(artifacts: object) -> dict | None:
    """Project the latest ExecutionPlan without leaking arbitrary Artifact payloads."""
    if not isinstance(artifacts, list):
        return None
    candidates: list[tuple[int, dict]] = []
    for artifact in artifacts:
        if not isinstance(artifact, dict) or artifact.get("artifact_type") != "execution_plan":
            continue
        payload = artifact.get("payload")
        if not isinstance(payload, dict) or not isinstance(payload.get("steps"), list):
            continue
        revision = payload.get("plan_revision")
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
            continue
        candidates.append((revision, payload))
    if not candidates:
        return None
    _, payload = max(candidates, key=lambda item: item[0])
    steps = []
    for raw_step in payload["steps"][:12]:
        if not isinstance(raw_step, dict):
            continue
        step_id = raw_step.get("step_id")
        title = raw_step.get("title")
        capability = raw_step.get("capability")
        status = raw_step.get("status")
        dependencies = raw_step.get("depends_on")
        if not all(isinstance(value, str) for value in (step_id, title, capability, status)):
            continue
        if not isinstance(dependencies, list) or not all(isinstance(value, str) for value in dependencies):
            continue
        steps.append({
            "step_id": step_id[:64],
            "title": title[:200],
            "capability": capability[:64],
            "depends_on": dependencies[:12],
            "required": raw_step.get("required") is True,
            "status": status[:32],
        })
    return {
        "plan_revision": payload["plan_revision"],
        "status": str(payload.get("status") or "active")[:32],
        "route_kind": str(payload.get("route_kind") or "unknown")[:32],
        "goal": str(payload.get("goal") or "")[:500],
        "steps": steps,
    }


def _bounded_web_task_events(events: object) -> list[dict]:
    if not isinstance(events, list):
        return []
    bounded = []
    for event in events[:200]:
        if not isinstance(event, dict):
            continue
        sequence = event.get("sequence")
        event_type = event.get("event_type")
        created_at = event.get("created_at")
        if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
            continue
        if not isinstance(event_type, str) or not isinstance(created_at, str):
            continue
        bounded.append({
            "sequence": sequence,
            "event_type": event_type[:128],
            "created_at": created_at[:64],
        })
    return bounded


def _bounded_web_stage_attempts(attempts: object) -> list[dict]:
    if not isinstance(attempts, list):
        return []
    bounded = []
    for attempt in attempts[-50:]:
        if not isinstance(attempt, dict):
            continue
        required = ("attempt_id", "stage", "status", "started_at", "updated_at")
        if not all(isinstance(attempt.get(field), str) for field in required):
            continue
        bounded.append({
            "attempt_id": str(attempt["attempt_id"])[:128],
            "stage": str(attempt["stage"])[:128],
            "status": str(attempt["status"])[:32],
            "attempt_number": attempt.get("attempt_number") if isinstance(attempt.get("attempt_number"), int) else 0,
            "started_at": str(attempt["started_at"])[:64],
            "updated_at": str(attempt["updated_at"])[:64],
            "finished_at": str(attempt["finished_at"])[:64] if isinstance(attempt.get("finished_at"), str) else None,
            "deadline_at": str(attempt["deadline_at"])[:64] if isinstance(attempt.get("deadline_at"), str) else None,
            "progress_phase": (
                str(attempt["progress_phase"])[:32]
                if attempt.get("progress_phase") in {
                    "waiting_for_model", "model_responding", "artifact_submitted"
                }
                else None
            ),
            "first_model_activity_at": (
                str(attempt["first_model_activity_at"])[:64]
                if isinstance(attempt.get("first_model_activity_at"), str)
                else None
            ),
            "tool_submitted_at": (
                str(attempt["tool_submitted_at"])[:64]
                if isinstance(attempt.get("tool_submitted_at"), str)
                else None
            ),
        })
    return bounded


@chat_router.post("/api/pi/chat/messages", response_class=JSONResponse)
async def api_pi_web_chat_message(
    req: PiWebChatMessageRequest,
    _auth=Depends(require_api_auth),
):
    """Submit one authenticated Web message through the shared ChannelEvent ingress."""
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    message = req.message.strip()
    if not message or len(message) > 20_000:
        return JSONResponse(
            {"status": "invalid_request", "error": "消息不能为空且不能超过 20000 字符"},
            status_code=400,
        )
    if not _valid_web_event_id(req.conversation_id) or not _valid_web_event_id(req.message_id):
        return JSONResponse(
            {"status": "invalid_request", "error": "Invalid Web conversation or message ID"},
            status_code=400,
        )
    try:
        status, data = await _pi_request(
            "POST",
            "/v1/channel-events",
            {
                "event_id": req.message_id,
                "channel": "web",
                "event_type": "message",
                "external_user_id": "web_admin",
                "conversation_id": req.conversation_id,
                "message_id": req.message_id,
                "task_run_id": None,
                "payload": {"text": message, "chat_type": "web"},
            },
        )
        return JSONResponse(data, status_code=status)
    except httpx.HTTPError as exc:
        logger.warning("Pi Web chat message failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.get(
    "/api/pi/chat/tasks/{task_run_id}/presentation",
    response_class=JSONResponse,
)
async def api_pi_web_chat_presentation(
    task_run_id: str,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return JSONResponse(
            {"status": "invalid_request", "error": "Invalid task_run_id"},
            status_code=400,
        )
    try:
        task_status, task_data = await _pi_scoped_task_get(task_run_id)
        task = task_data.get("task") if isinstance(task_data, dict) else None
        if task_status != 200 or not isinstance(task, dict) or task.get("channel") != "web":
            return JSONResponse({"status": "not_found"}, status_code=404)
        status, data = await _pi_request("GET", f"/v1/tasks/{task_run_id}/presentation")
        return JSONResponse(data, status_code=status)
    except httpx.HTTPError as exc:
        logger.warning("Pi Web chat presentation failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.get(
    "/api/pi/chat/tasks/{task_run_id}/flow",
    response_class=JSONResponse,
)
async def api_pi_web_chat_task_flow(
    task_run_id: str,
    request: Request,
    _auth=Depends(require_api_auth),
):
    """Return a minimal read-only Plan/Event/Attempt projection for Web chat."""
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return JSONResponse(
            {"status": "invalid_request", "error": "Invalid task_run_id"},
            status_code=400,
        )
    raw_after = request.query_params.get("after", "0")
    if not raw_after.isdigit() or len(raw_after) > 12:
        return JSONResponse(
            {"status": "invalid_request", "error": "after must be a non-negative integer"},
            status_code=400,
        )
    after = int(raw_after)
    try:
        task_status, task_data = await _pi_scoped_task_get(task_run_id)
        task = task_data.get("task") if isinstance(task_data, dict) else None
        if (
            task_status != 200
            or not isinstance(task, dict)
            or task.get("channel") != "web"
            or task.get("user_id") != "web_admin"
        ):
            return JSONResponse({"status": "not_found"}, status_code=404)
        event_result, artifact_result, attempt_result = await asyncio.gather(
            _pi_request("GET", f"/v1/tasks/{task_run_id}/events?after={after}"),
            _pi_request("GET", f"/v1/tasks/{task_run_id}/artifacts"),
            _pi_request("GET", f"/v1/tasks/{task_run_id}/attempts"),
        )
        if any(status != 200 for status, _ in (event_result, artifact_result, attempt_result)):
            return JSONResponse(
                {"status": "upstream_unavailable", "error": "Task flow is temporarily unavailable"},
                status_code=502,
            )
        events = _bounded_web_task_events(event_result[1].get("events"))
        plan = _bounded_web_execution_plan(artifact_result[1].get("artifacts"))
        attempts = _bounded_web_stage_attempts(attempt_result[1].get("attempts"))
        return JSONResponse({
            "status": "ok",
            "task": {
                "task_run_id": task_run_id,
                "status": str(task.get("status") or "unknown")[:64],
                "current_stage": str(task.get("current_stage") or "")[:128],
                "updated_at": str(task.get("updated_at") or "")[:64],
            },
            "plan": plan,
            "events": events,
            "attempts": attempts,
            "last_event_sequence": max((event["sequence"] for event in events), default=after),
        })
    except httpx.HTTPError as exc:
        logger.warning("Pi Web chat task flow failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.post(
    "/api/pi/chat/tasks/{task_run_id}/actions",
    response_class=JSONResponse,
)
async def api_pi_web_chat_action(
    task_run_id: str,
    req: PiWebChatActionRequest,
    _auth=Depends(require_api_auth),
):
    """Forward only presentation-declared Web actions through shared ChannelEvent handling."""
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return JSONResponse(
            {"status": "invalid_request", "error": "Invalid task_run_id"},
            status_code=400,
        )
    allowed_actions = {
        "provide_input", "approve_query", "cancel_task", "request_supplement",
        "analyze", "render_report", "confirm_memory",
    }
    if req.action not in allowed_actions:
        return JSONResponse(
            {"status": "invalid_request", "error": "Unsupported Web chat action"},
            status_code=400,
        )
    if not _valid_web_event_id(req.conversation_id) or not _valid_web_event_id(req.message_id):
        return JSONResponse(
            {"status": "invalid_request", "error": "Invalid Web conversation or message ID"},
            status_code=400,
        )
    try:
        task_status, task_data = await _pi_scoped_task_get(task_run_id)
        task = task_data.get("task") if isinstance(task_data, dict) else None
        if (
            task_status != 200
            or not isinstance(task, dict)
            or task.get("channel") != "web"
            or task.get("user_id") != "web_admin"
        ):
            return JSONResponse({"status": "not_found"}, status_code=404)
        presentation_status, presentation_data = await _pi_request(
            "GET", f"/v1/tasks/{task_run_id}/presentation"
        )
        presentation = (
            presentation_data.get("presentation")
            if presentation_status == 200 and isinstance(presentation_data, dict)
            else None
        )
        declared_actions = (
            presentation.get("actions", []) if isinstance(presentation, dict) else []
        )
        allowed_extra = {"text"} if req.action == "provide_input" else set()
        declared = None
        for item in declared_actions:
            if (
                not isinstance(item, dict)
                or item.get("type") != req.action
                or item.get("task_run_id") != task_run_id
            ):
                continue
            candidate_payload = item.get("payload")
            if not isinstance(candidate_payload, dict):
                candidate_payload = {}
            if (
                all(req.payload.get(key) == value for key, value in candidate_payload.items())
                and not (set(req.payload) - set(candidate_payload) - allowed_extra)
            ):
                declared = item
                break
        if declared is None:
            return JSONResponse(
                {"status": "conflict", "error": "操作已失效，请刷新当前对话"},
                status_code=409,
            )
        if req.action == "provide_input" and not str(req.payload.get("text") or "").strip():
            return JSONResponse(
                {"status": "invalid_request", "error": "补充信息不能为空"},
                status_code=400,
            )
        task_conversation_id = task.get("channel_conversation_id")
        if not isinstance(task_conversation_id, str) or not _valid_web_event_id(task_conversation_id):
            return JSONResponse(
                {"status": "conflict", "error": "该任务不属于可交互的 Web 对话"},
                status_code=409,
            )
        action_message_id = f"web_card_{task_run_id}"
        event_id = _web_action_event_id(
            action_message_id,
            task_run_id,
            req.action,
            req.payload,
        )
        status, data = await _pi_request(
            "POST",
            "/v1/channel-events",
            {
                "event_id": event_id,
                "channel": "web",
                "event_type": "action",
                "external_user_id": "web_admin",
                "conversation_id": task_conversation_id,
                "message_id": action_message_id,
                "task_run_id": task_run_id,
                "payload": {"action": req.action, **req.payload},
            },
        )
        return JSONResponse(data, status_code=status)
    except httpx.HTTPError as exc:
        logger.warning("Pi Web chat action failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.get("/api/pi/tasks", response_class=JSONResponse)
async def api_pi_list_tasks(
    channel: str | None = None,
    status: str | None = None,
    limit: int = 50,
    _auth=Depends(require_api_auth),
):
    """List the authenticated admin team's cross-channel TaskRuns."""
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    allowed_channels = {"web", "feishu", "dingtalk", "api"}
    allowed_statuses = {
        "created", "clarifying", "ready_for_query", "waiting_for_query_approval",
        "waiting_for_action_approval", "querying", "ready_for_analysis", "analyzing",
        "ready_for_report", "rendering", "completed", "needs_input", "incomplete",
        "cancelled", "failed", "expired",
    }
    if channel is not None and channel not in allowed_channels:
        return JSONResponse({"status": "invalid_request", "error": "Invalid channel"}, status_code=400)
    if status is not None and status not in allowed_statuses:
        return JSONResponse({"status": "invalid_request", "error": "Invalid status"}, status_code=400)
    if limit < 1 or limit > 100:
        return JSONResponse({"status": "invalid_request", "error": "Invalid limit"}, status_code=400)
    scopes = _web_admin_task_scopes()
    if not scopes:
        return JSONResponse(
            {"status": "misconfigured", "error": "No valid Web admin task scope"},
            status_code=503,
        )
    try:
        tasks_by_id: dict[str, dict] = {}
        for org_id, team_id in scopes:
            query = {
                "org_id": org_id,
                "team_id": team_id,
                "limit": str(limit),
                **({} if channel is None else {"channel": channel}),
                **({} if status is None else {"status": status}),
            }
            upstream_status, data = await _pi_request("GET", f"/v1/tasks?{urlencode(query)}")
            if upstream_status != 200:
                return JSONResponse(data, status_code=upstream_status)
            for task in data.get("tasks", []):
                if isinstance(task, dict) and isinstance(task.get("task_run_id"), str):
                    tasks_by_id[task["task_run_id"]] = task
        tasks = sorted(
            tasks_by_id.values(),
            key=lambda task: (str(task.get("updated_at", "")), str(task.get("task_run_id", ""))),
            reverse=True,
        )[:limit]
        return JSONResponse({"tasks": tasks})
    except httpx.HTTPError as exc:
        logger.warning("Pi task list failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.post("/api/pi/tasks", response_class=JSONResponse)
async def api_pi_create_task(req: PiTaskCreateRequest, _auth=Depends(require_api_auth)):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    try:
        status, data = await _pi_request(
            "POST",
            "/v1/tasks",
            {
                "message": req.message,
                # Integration Spike 只有管理员 Web 渠道；不信任浏览器提交身份。
                # 正式 org/team/user 映射在 Phase 2 身份层完成。
                "user_id": "web_admin",
                "org_id": "org_default",
                "team_id": "team_default",
                "intent": req.intent,
                "channel": "web",
                "channel_conversation_id": req.channel_conversation_id,
            },
        )
        return JSONResponse(data, status_code=status)
    except httpx.HTTPError as exc:
        logger.warning("Pi task creation failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.post(
    "/api/pi/tasks/{task_run_id}/prepare-query",
    response_class=JSONResponse,
)
async def api_pi_prepare_query(
    task_run_id: str,
    req: PiPrepareQueryRequest,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return JSONResponse(
            {"status": "invalid_request", "error": "Invalid task_run_id"},
            status_code=400,
        )
    try:
        status, data = await _pi_request(
            "POST",
            f"/v1/tasks/{task_run_id}/prepare-query",
            _pi_stage_payload(req),
        )
        return JSONResponse(data, status_code=status)
    except httpx.HTTPError as exc:
        logger.warning("Pi query preparation failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.post(
    "/api/pi/tasks/{task_run_id}/clarify",
    response_class=JSONResponse,
)
async def api_pi_clarify_task(
    task_run_id: str,
    req: PiSkillStageRequest,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return JSONResponse(
            {"status": "invalid_request", "error": "Invalid task_run_id"},
            status_code=400,
        )
    try:
        status, data = await _pi_request(
            "POST", f"/v1/tasks/{task_run_id}/clarify", _pi_stage_payload(req)
        )
        return JSONResponse(data, status_code=status)
    except httpx.HTTPError as exc:
        logger.warning("Pi requirement clarification failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.post(
    "/api/pi/tasks/{task_run_id}/review-metric",
    response_class=JSONResponse,
)
async def api_pi_review_metric(
    task_run_id: str,
    req: PiSkillStageRequest,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return JSONResponse(
            {"status": "invalid_request", "error": "Invalid task_run_id"},
            status_code=400,
        )
    try:
        status, data = await _pi_request(
            "POST", f"/v1/tasks/{task_run_id}/review-metric", _pi_stage_payload(req)
        )
        return JSONResponse(data, status_code=status)
    except httpx.HTTPError as exc:
        logger.warning("Pi metric review failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.get("/api/pi/tasks/{task_run_id}", response_class=JSONResponse)
async def api_pi_task(
    task_run_id: str,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return JSONResponse(
            {"status": "invalid_request", "error": "Invalid task_run_id"},
            status_code=400,
        )
    try:
        upstream_status, data = await _pi_scoped_task_get(task_run_id)
        return JSONResponse(data, status_code=upstream_status)
    except httpx.HTTPError as exc:
        logger.warning("Pi task fetch failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.get(
    "/api/pi/tasks/{task_run_id}/attempts",
    response_class=JSONResponse,
)
async def api_pi_task_attempts(
    task_run_id: str,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return JSONResponse(
            {"status": "invalid_request", "error": "Invalid task_run_id"},
            status_code=400,
        )
    try:
        upstream_status, data = await _pi_scoped_task_get(task_run_id, "attempts")
        return JSONResponse(data, status_code=upstream_status)
    except httpx.HTTPError as exc:
        logger.warning("Pi task attempts fetch failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.get(
    "/api/pi/tasks/{task_run_id}/artifacts",
    response_class=JSONResponse,
)
async def api_pi_task_artifacts(
    task_run_id: str,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return JSONResponse(
            {"status": "invalid_request", "error": "Invalid task_run_id"},
            status_code=400,
        )
    try:
        upstream_status, data = await _pi_scoped_task_get(task_run_id, "artifacts")
        return JSONResponse(data, status_code=upstream_status)
    except httpx.HTTPError as exc:
        logger.warning("Pi artifact fetch failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.post(
    "/api/pi/tasks/{task_run_id}/supplements",
    response_class=JSONResponse,
)
async def api_pi_create_supplement(
    task_run_id: str,
    req: PiSupplementRequest,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return JSONResponse(
            {"status": "invalid_request", "error": "Invalid task_run_id"},
            status_code=400,
        )
    try:
        status, data = await _pi_request(
            "POST", f"/v1/tasks/{task_run_id}/supplements", req.model_dump()
        )
        return JSONResponse(data, status_code=status)
    except httpx.HTTPError as exc:
        logger.warning("Pi supplement creation failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.post(
    "/api/pi/tasks/{task_run_id}/resume-analysis",
    response_class=JSONResponse,
)
async def api_pi_resume_analysis(
    task_run_id: str,
    req: PiResumeAnalysisRequest,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return JSONResponse(
            {"status": "invalid_request", "error": "Invalid task_run_id"},
            status_code=400,
        )
    try:
        status, data = await _pi_request(
            "POST", f"/v1/tasks/{task_run_id}/resume-analysis", _pi_stage_payload(req)
        )
        return JSONResponse(data, status_code=status)
    except httpx.HTTPError as exc:
        logger.warning("Pi supplemental analysis failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.post(
    "/api/pi/tasks/{task_run_id}/run-skill",
    response_class=JSONResponse,
)
async def api_pi_run_skill(
    task_run_id: str,
    req: PiAdvisorySkillRequest,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return JSONResponse(
            {"status": "invalid_request", "error": "Invalid task_run_id"},
            status_code=400,
        )
    try:
        status, data = await _pi_request(
            "POST", f"/v1/tasks/{task_run_id}/run-skill", req.model_dump(exclude_none=True)
        )
        return JSONResponse(data, status_code=status)
    except httpx.HTTPError as exc:
        logger.warning("Pi advisory Skill failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.post(
    "/api/pi/tasks/{task_run_id}/analyze",
    response_class=JSONResponse,
)
async def api_pi_analyze_task(
    task_run_id: str,
    req: PiAnalyzeRequest,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return JSONResponse(
            {"status": "invalid_request", "error": "Invalid task_run_id"},
            status_code=400,
        )
    try:
        status, data = await _pi_request(
            "POST", f"/v1/tasks/{task_run_id}/analyze", _pi_stage_payload(req)
        )
        return JSONResponse(data, status_code=status)
    except httpx.HTTPError as exc:
        logger.warning("Pi analysis failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.post(
    "/api/pi/tasks/{task_run_id}/render-report",
    response_class=JSONResponse,
)
async def api_pi_render_report(
    task_run_id: str,
    req: PiRenderReportRequest,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return JSONResponse(
            {"status": "invalid_request", "error": "Invalid task_run_id"},
            status_code=400,
        )
    try:
        status, data = await _pi_request(
            "POST", f"/v1/tasks/{task_run_id}/render-report", _pi_stage_payload(req)
        )
        return JSONResponse(data, status_code=status)
    except httpx.HTTPError as exc:
        logger.warning("Pi report rendering failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.post(
    "/api/pi/tasks/{task_run_id}/approve-query",
    response_class=JSONResponse,
)
async def api_pi_approve_query(
    task_run_id: str,
    req: PiApproveQueryRequest,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return JSONResponse(
            {"status": "invalid_request", "error": "Invalid task_run_id"},
            status_code=400,
        )
    try:
        status, data = await _pi_request(
            "POST",
            f"/v1/tasks/{task_run_id}/approve-query",
            _pi_stage_payload(req),
        )
        return JSONResponse(data, status_code=status)
    except httpx.HTTPError as exc:
        logger.warning("Pi query approval failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.get("/api/pi/tasks/{task_run_id}/events", response_class=JSONResponse)
async def api_pi_task_events(
    task_run_id: str,
    after: int = 0,
    _auth=Depends(require_api_auth),
):
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return _pi_disabled_response()
    if re.fullmatch(r"tr_[A-Za-z0-9_-]+", task_run_id) is None:
        return JSONResponse(
            {"status": "invalid_request", "error": "Invalid task_run_id"},
            status_code=400,
        )
    try:
        task_status, task_data = await _pi_scoped_task_get(task_run_id)
        if task_status != 200:
            return JSONResponse(task_data, status_code=task_status)
        upstream_status, data = await _pi_request(
            "GET",
            f"/v1/tasks/{task_run_id}/events?after={max(after, 0)}",
        )
        return JSONResponse(data, status_code=upstream_status)
    except httpx.HTTPError as exc:
        logger.warning("Pi task events failed: %s", exc)
        return JSONResponse(
            {"status": "upstream_unavailable", "error": "Pi Orchestrator is unavailable"},
            status_code=502,
        )


@chat_router.post("/api/chat", response_class=JSONResponse)
async def api_chat(req: ChatRequest, _auth=Depends(require_api_auth)):
    """Deprecated legacy Agent path; disabled unless an explicit rollback flag is set."""
    if not cfg.LEGACY_AGENT_API_ENABLED:
        return JSONResponse(
            {
                "status": "deprecated",
                "error": "Legacy Agent API is disabled; create a Pi TaskRun via /tasks.",
            },
            status_code=410,
        )
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


@chat_router.post("/api/prepare-query", response_class=JSONResponse)
async def api_prepare_query(req: PrepareQueryRequest, _auth=Depends(require_api_auth)):
    """外部 Agent 嵌入入口：只生成可审核 SQL，不创建可执行 pending state。"""
    result = await _run_sync(agent_prepare_query, req.user_id, req.question, req.dialect)
    status = "needs_external_review" if result.get("status") == "needs_review" else "error"
    await audit.log(
        user_id=req.user_id,
        user_message=req.question,
        forge_json=result.get("forge_json"),
        sql=result.get("sql"),
        status=status,
        error_message=result.get("error") or result.get("text") or None,
    )
    return result


@chat_router.post("/api/approve", response_class=JSONResponse)
async def api_approve(req: ChatRequest, _auth=Depends(require_api_auth)):
    """Deprecated legacy approval path, available only for explicit rollback."""
    if not cfg.LEGACY_AGENT_API_ENABLED:
        return JSONResponse({"status": "deprecated", "error": "Legacy Agent API is disabled."}, status_code=410)
    resp = await _run_sync(agent_approve, req.user_id)
    result = {"text": resp.text, "sql": resp.sql, "action": resp.action,
              "columns": None, "rows": None, "row_count": 0, "exec_error": None,
              "analysis": None, "chart_html": None}

    if resp.action == "approved" and resp.sql:
        # 1. 执行 SQL
        cols, rows_raw = [], []
        execution_ms = None
        try:
            started = time.perf_counter()
            text, cols, rows_raw = await _run_sync(execute_with_data, resp.sql)
            execution_ms = int((time.perf_counter() - started) * 1000)
            result["columns"] = cols
            result["rows"] = [list(r) for r in rows_raw]
            result["row_count"] = len(rows_raw)
            if text.startswith("⚠"):
                result["exec_error"] = text
        except Exception:
            logger.exception("Approved SQL execution failed")
            result["exec_error"] = "⚠ 执行失败：数据库查询失败，请检查 SQL 或联系管理员。"

        if result["exec_error"]:
            result["action"] = "execution_failed"
            result["text"] = "SQL 执行失败，请重新生成或修改查询。"

        # 2. 仅在查询成功时恢复兼容 Pipeline，失败结果不能进入分析阶段。
        try:
            from agent.memory import memory as _mem
            from agent.pipeline import runner as _runner, QueryResult, Artifact
            run_data = _mem.get_state(req.user_id, "pipeline_run")
            if result["exec_error"] and run_data and run_data.get("status") == "pending_approval":
                run_data["status"] = "failed"
                run_data["error"] = "SQL 执行失败，Pipeline 已终止。"
                _mem.set_state(req.user_id, "pipeline_run", run_data)
            elif run_data and run_data.get("status") == "pending_approval":
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

        await audit.update_latest_pending(
            req.user_id,
            "error" if result["exec_error"] else "approved",
            error_message=result["exec_error"],
            row_count=result["row_count"],
            execution_ms=execution_ms,
        )

    return result


@chat_router.post("/api/cancel", response_class=JSONResponse)
async def api_cancel(req: ChatRequest, _auth=Depends(require_api_auth)):
    """Deprecated legacy cancellation path, available only for explicit rollback."""
    if not cfg.LEGACY_AGENT_API_ENABLED:
        return JSONResponse({"status": "deprecated", "error": "Legacy Agent API is disabled."}, status_code=410)
    resp = await _run_sync(agent_cancel, req.user_id)
    await audit.update_latest_pending(req.user_id, "cancelled")
    return {"text": resp.text, "action": resp.action}


class ExecuteRawRequest(BaseModel):
    sql: str
    user_id: str = "web_user"


class FeedbackRequest(BaseModel):
    user_id: str = "web_user"
    feedback_type: str = "wrong_result"
    message: str
    audit_id: int | None = None
    question: str | None = None
    sql: str | None = None
    expected: str | None = None


@chat_router.post("/api/execute-raw", response_class=JSONResponse)
async def api_execute_raw(req: ExecuteRawRequest, _auth=Depends(require_api_auth)):
    """直接执行用户编辑后的 SQL（跳过 Agent 编译）。"""
    result = {"text": "", "sql": req.sql, "action": "approved",
              "columns": None, "rows": None, "row_count": 0, "exec_error": None,
              "analysis": None, "chart_html": None}
    execution_ms = None
    if not cfg.RAW_SQL_ENABLED:
        result["exec_error"] = "⚠ 手动 SQL 执行已被配置禁用。"
        result["action"] = "execution_failed"
        result["text"] = "SQL 执行失败，请重新生成或修改查询。"
        await audit.log(
            user_id=req.user_id,
            user_message="[手动编辑 SQL]",
            forge_json=None,
            sql=req.sql,
            status="error",
            error_message=result["exec_error"],
        )
        return result
    try:
        started = time.perf_counter()
        text, cols, rows_raw = await _run_sync(execute_with_data, req.sql)
        execution_ms = int((time.perf_counter() - started) * 1000)
        result["columns"] = cols
        result["rows"] = [list(r) for r in rows_raw]
        result["row_count"] = len(rows_raw)
        if text.startswith("⚠"):
            result["exec_error"] = text
    except Exception:
        logger.exception("Raw SQL execution failed")
        result["exec_error"] = "⚠ 执行失败：数据库查询失败，请检查 SQL 或联系管理员。"

    if result["exec_error"]:
        result["action"] = "execution_failed"
        result["text"] = "SQL 执行失败，请重新生成或修改查询。"

    await audit.log(
        user_id=req.user_id,
        user_message="[手动编辑 SQL]",
        forge_json=None,
        sql=req.sql,
        status="approved" if not result["exec_error"] else "error",
        error_message=result["exec_error"],
        row_count=result["row_count"],
        execution_ms=execution_ms,
    )
    return result


@chat_router.post("/api/feedback", response_class=JSONResponse)
async def api_feedback(req: FeedbackRequest, _auth=Depends(require_api_auth)):
    """提交 SQL/结果反馈，进入待处理队列。"""
    try:
        feedback_id = await feedback.submit(
            user_id=req.user_id,
            audit_id=req.audit_id,
            question=req.question,
            sql=req.sql,
            feedback_type=req.feedback_type,
            message=req.message,
            expected=req.expected,
        )
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
    return {"ok": True, "feedback_id": feedback_id, "status": "pending"}


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
async def api_admin_chat(req: AdminChatRequest, _auth=Depends(require_api_auth)):
    """管理助手 AI：返回结构化提议或文字回复。"""
    result = await _run_sync(_admin_ai_process, req.message, req.page)
    return result


@chat_router.post("/api/admin-apply", response_class=JSONResponse)
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


# ── Admin routes ──────────────────────────────────────────────────────────────

@router.get("/", response_class=RedirectResponse)
async def admin_root():
    return RedirectResponse(url="/admin/dashboard", status_code=302)


@router.get("/architecture", response_class=HTMLResponse)
async def architecture_atlas():
    """Serve the standalone architecture atlas behind admin authentication."""
    path = (
        Path(__file__).resolve().parents[1]
        / "docs"
        / "architecture-diagrams"
        / "forge-platform-architecture.html"
    )
    if not path.exists():
        return HTMLResponse("架构图尚未生成。", status_code=404)
    return HTMLResponse(path.read_text(encoding="utf-8"))


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
        await audit._ensure_schema()
        async with aiosqlite.connect(audit._db_path()) as db:
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
async def metrics_page(request: Request, flash: str = ""):
    metrics = _load_metrics()
    atomics     = {k: v for k, v in metrics.items() if v.get("metric_class") == "atomic"}
    derivatives = {k: v for k, v in metrics.items() if v.get("metric_class") == "derivative"}
    return templates.TemplateResponse(
            request,
            "metrics.html",
            {"atomics": atomics, "derivatives": derivatives, "all_metrics": metrics,
             "flash": flash},
        )


@router.post("/metrics/metric", response_class=HTMLResponse)
async def upsert_metric(
    request:     Request,
    name:        str           = Form(...),
    label:       str           = Form(...),
    metric_class: str          = Form(...),
    description: str           = Form(...),
    measure:     Optional[str] = Form(default=None),
    aggregation: Optional[str] = Form(default=None),
    numerator:   Optional[str] = Form(default=None),
    denominator: Optional[str] = Form(default=None),
    qualifiers:  Optional[str] = Form(default=None),
    period_col:  Optional[str] = Form(default=None),
    dimensions:  Optional[str] = Form(default=None),
    notes:       Optional[str] = Form(default=None),
):
    entry: dict = {
        "label":       label,
        "description": description,
        "metric_class": metric_class,
    }
    if metric_class == "atomic":
        entry["measure"] = (measure or "").strip()
        entry["aggregation"] = (aggregation or "").strip()
        if qualifiers:
            entry["qualifiers"] = _parse_lines(qualifiers)
    elif metric_class == "derivative":
        entry["numerator"] = (numerator or "").strip()
        entry["denominator"] = (denominator or "").strip()
    if period_col and period_col.strip():
        entry["period_col"] = period_col.strip()
    if dimensions:
        entry["dimensions"] = _parse_lines(dimensions)
    if notes and notes.strip():
        entry["notes"] = notes.strip()

    structural  = _load_schema()
    all_metrics = _load_metrics()
    is_edit = name in all_metrics
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
                "form_data":     {"name": name, **entry, "_is_edit": is_edit},
            },
            status_code=422,
        )

    entry["updated_at"] = str(date.today())
    metrics = _load_metrics()
    metrics[name] = entry
    _save_metrics(metrics)
    if result.warnings:
        atomics = {k: v for k, v in metrics.items() if v.get("metric_class") == "atomic"}
        derivatives = {k: v for k, v in metrics.items() if v.get("metric_class") == "derivative"}
        return templates.TemplateResponse(
            request,
            "metrics.html",
            {
                "atomics": atomics,
                "derivatives": derivatives,
                "all_metrics": metrics,
                "form_warnings": result.warnings,
                "flash": "指标已保存，请检查以下口径警告。",
            },
        )
    return RedirectResponse(
        url="/admin/metrics?" + urlencode({"flash": "指标已保存"}),
        status_code=303,
    )


@router.delete("/metrics/metric/{name}")
async def delete_metric(name: str):
    metrics = _load_metrics()
    dependents = sorted(
        metric_name
        for metric_name, metric in metrics.items()
        if metric.get("metric_class") == "derivative"
        and name in {metric.get("numerator"), metric.get("denominator")}
    )
    if dependents:
        return JSONResponse(
            status_code=409,
            content={
                "deleted": None,
                "dependents": dependents,
                "error": f"指标 {name!r} 正被衍生指标引用，不能删除。",
            },
        )
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
