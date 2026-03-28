"""
Forge Agent — FastAPI entry point.

Endpoints:
  GET  /                 — redirect to /chat
  GET  /chat             — chat UI (natural language → SQL)
  POST /api/chat         — chat API
  POST /api/approve      — approve pending SQL
  POST /api/cancel       — cancel pending SQL
  POST /webhook/feishu   — Feishu event subscription + card callbacks
  GET  /health           — health check
  GET  /admin/*          — admin web UI (registry, audit log, settings)
"""
import logging
from pathlib import Path

import lark_oapi as lark
from fastapi import FastAPI, Request, Response
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from config import cfg
from agent.feishu import dispatcher
from web.router import chat_router, router as admin_router
from web.auth import _LoginRedirect

# ── 日志配置（可通过 forge.yaml 或环境变量调整）──────────────────────────────
_log_handlers: list[logging.Handler] = [logging.StreamHandler()]
if cfg.LOG_FILE:
    _log_handlers.append(logging.FileHandler(cfg.LOG_FILE, encoding="utf-8"))
logging.basicConfig(
    level=getattr(logging, cfg.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=_log_handlers,
)
app = FastAPI(title="Forge Agent")

logger = logging.getLogger("forge.startup")


@app.on_event("startup")
async def _startup_checks():
    """启动时的健康检查和安全提示。"""
    # ── #9 默认密码安全警告 ──
    if cfg.AUTH_ENABLED and cfg.AUTH_ADMIN_PASSWORD in ("123456", ""):
        logger.warning(
            "\n"
            "╔══════════════════════════════════════════════════════════╗\n"
            "║  ⚠  默认密码未修改！请设置 AUTH_ADMIN_PASSWORD 环境变量  ║\n"
            "╚══════════════════════════════════════════════════════════╝"
        )

    # ── #10 连接状态检测 ──
    checks = []
    # DB
    if cfg.DATABASE_URL:
        try:
            from sqlalchemy import create_engine, text as sa_text
            engine = create_engine(cfg.DATABASE_URL)
            with engine.connect() as conn:
                conn.execute(sa_text("SELECT 1"))
            checks.append(("数据库", "✓ 已连接"))
        except Exception as exc:
            checks.append(("数据库", f"✗ 连接失败: {exc}"))
    else:
        checks.append(("数据库", "✗ 未配置 DATABASE_URL"))

    # LLM
    if cfg.LLM_API_KEY:
        checks.append(("LLM", f"✓ {cfg.LLM_PROVIDER}/{cfg.LLM_MODEL}"))
    else:
        checks.append(("LLM", "✗ 未配置 LLM_API_KEY"))

    # Embedding
    if cfg.EMBED_API_KEY:
        checks.append(("Embedding", f"✓ {cfg.EMBED_MODEL}"))
    else:
        checks.append(("Embedding", "⚠ 未配置（将使用全表模式）"))

    # 打印状态表
    logger.info("系统状态检测：")
    for name, status in checks:
        logger.info("  %-12s %s", name, status)


# Chat + API 路由挂载到根级别（/chat, /api/*）
app.include_router(chat_router)
# Admin 管理后台路由保持 /admin 前缀
app.include_router(admin_router, prefix="/admin")


@app.exception_handler(_LoginRedirect)
async def login_redirect_handler(request: Request, exc: _LoginRedirect):
    """将 require_web_auth 抛出的 _LoginRedirect 转为 302 → /login。"""
    next_path = exc.next_path or request.url.path
    return RedirectResponse(url=f"/login?next={next_path}", status_code=302)


@app.get("/")
async def root():
    return RedirectResponse(url="/chat", status_code=302)

# 图表静态文件服务
_charts_dir = Path(__file__).parent / "web" / "static" / "charts"
_charts_dir.mkdir(parents=True, exist_ok=True)
app.mount("/charts", StaticFiles(directory=str(_charts_dir)), name="charts")


@app.post("/webhook/feishu")
async def feishu_webhook(request: Request) -> Response:
    body = await request.body()
    headers = dict(request.headers)
    resp = dispatcher.dispatch(
        lark.RawRequest.builder().headers(headers).body(body).build()
    )
    return Response(content=resp.body, media_type="application/json")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
