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
import tempfile
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


@app.get("/health/readiness")
async def readiness() -> dict:
    """Deployment readiness checks for private/commercial installations."""
    checks = _readiness_checks()
    failed = [c for c in checks if c["status"] == "fail"]
    warned = [c for c in checks if c["status"] == "warn"]
    overall = "fail" if failed else ("warn" if warned else "ok")
    return {"status": overall, "checks": checks}


def _readiness_checks() -> list[dict]:
    checks: list[dict] = []

    def add(name: str, status: str, message: str) -> None:
        checks.append({"name": name, "status": status, "message": message})

    if not cfg.AUTH_ENABLED:
        add("auth", "warn", "认证未开启；生产环境应开启 Web/API 认证。")
    elif not cfg.AUTH_ADMIN_PASSWORD or cfg.AUTH_ADMIN_PASSWORD == "123456":
        add("auth", "fail", "认证已开启，但管理员密码为空或仍为默认值。")
    else:
        add("auth", "ok", "认证已开启。")

    if cfg.EXECUTION_ENABLED and not cfg.DATABASE_URL:
        add("database", "fail", "SQL 执行已开启，但未配置 DATABASE_URL。")
    elif cfg.DATABASE_URL:
        add("database", "ok", "已配置 DATABASE_URL。")
    else:
        add("database", "warn", "未配置数据库连接，仅可生成 SQL。")

    if cfg.EXECUTION_ENABLED and cfg.DATABASE_URL and not cfg.DATABASE_READONLY_CONFIRMED:
        add("database_readonly", "fail", "SQL 执行已开启，但尚未确认 DATABASE_URL 使用数据库只读账号。")
    elif cfg.EXECUTION_ENABLED and cfg.DATABASE_URL:
        add("database_readonly", "ok", "已确认数据库连接使用只读账号。")
    else:
        add("database_readonly", "warn", "SQL 执行未启用，暂不检查数据库只读账号。")

    if not cfg.LLM_API_KEY:
        add("llm", "fail", "未配置 LLM_API_KEY。")
    else:
        add("llm", "ok", f"已配置 {cfg.LLM_PROVIDER}/{cfg.LLM_MODEL or '(default)'}。")

    if cfg.RAW_SQL_ENABLED:
        add("raw_sql", "warn", "手动 SQL 执行入口已开启；生产环境建议只给受信任管理员使用。")
    else:
        add("raw_sql", "ok", "手动 SQL 执行入口已关闭。")

    if cfg.EXECUTION_MAX_ROWS > 1000:
        add("row_cap", "warn", f"最大返回行数为 {cfg.EXECUTION_MAX_ROWS}，生产环境建议控制在 1000 行以内。")
    else:
        add("row_cap", "ok", f"最大返回行数为 {cfg.EXECUTION_MAX_ROWS}。")

    if cfg.EXECUTION_ENABLED and cfg.EXECUTION_TIMEOUT_SECONDS <= 0:
        add("query_timeout", "fail", "SQL 执行已开启，但未配置查询超时。")
    elif cfg.EXECUTION_TIMEOUT_SECONDS > 120:
        add("query_timeout", "warn", f"查询超时为 {cfg.EXECUTION_TIMEOUT_SECONDS}s，生产环境建议不超过 120s。")
    else:
        add("query_timeout", "ok", f"查询超时为 {cfg.EXECUTION_TIMEOUT_SECONDS}s。")

    if cfg.AUTH_ENABLED and not cfg.AUTH_COOKIE_SECURE:
        add("secure_cookie", "warn", "认证已开启，但 Secure Cookie 未开启；HTTPS 部署建议开启。")
    elif cfg.AUTH_ENABLED:
        add("secure_cookie", "ok", "Secure Cookie 已开启。")
    else:
        add("secure_cookie", "warn", "认证未开启，暂不检查 Secure Cookie。")

    registry_missing = [
        str(path)
        for path in (
            cfg.REGISTRY_PATH,
            cfg.METRICS_PATH,
            cfg.DISAMBIGUATIONS_PATH,
            cfg.CONVENTIONS_PATH,
        )
        if not Path(path).exists()
    ]
    if registry_missing:
        add("registry", "warn", "部分 Registry 文件不存在：" + ", ".join(registry_missing))
    else:
        add("registry", "ok", "Registry 文件齐备。")

    audit_path = Path(cfg.AUDIT_DB_PATH).expanduser()
    audit_dir = audit_path.parent if str(audit_path.parent) else Path(".")
    try:
        audit_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=audit_dir, prefix=".forge-audit-check-", delete=True):
            pass
        add("audit", "ok", f"审计目录可写：{audit_dir}")
    except OSError as exc:
        add("audit", "fail", f"审计目录不可写：{exc}")

    return checks
