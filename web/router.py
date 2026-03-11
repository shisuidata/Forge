"""
Forge admin web UI — FastAPI router mounted at /admin.

Routes
------
GET  /admin                       → redirect to /admin/registry
GET  /admin/registry              → registry overview (tables + metrics)
POST /admin/registry/metric       → add or update a metric definition
DELETE /admin/registry/metric/{name} → delete a metric
GET  /admin/audit                 → recent audit log (last 100 entries)
GET  /admin/settings              → current config (secrets masked)
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from agent import audit
from config import cfg

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


# ── helpers ───────────────────────────────────────────────────────────────────

def _load_registry() -> dict:
    try:
        return json.loads(cfg.REGISTRY_PATH.read_text())
    except Exception:
        return {}


def _save_registry(data: dict) -> None:
    cfg.REGISTRY_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def _mask_secret(value: str, visible: int = 4) -> str:
    """Return value with all but the last *visible* characters replaced by *."""
    if not value:
        return "(not set)"
    if len(value) <= visible:
        return "*" * len(value)
    return "*" * (len(value) - visible) + value[-visible:]


def _mask_db_url(url: str) -> str:
    """Mask the password in a database URL."""
    if not url:
        return "(not set)"
    # postgresql://user:password@host/db  →  postgresql://user:****@host/db
    return re.sub(r"(:)([^/@]+)(@)", lambda m: f"{m.group(1)}****{m.group(3)}", url)


# ── routes ────────────────────────────────────────────────────────────────────

@router.get("/", response_class=RedirectResponse)
async def admin_root():
    return RedirectResponse(url="/admin/registry", status_code=302)


@router.get("/registry", response_class=HTMLResponse)
async def registry_page(request: Request):
    reg = _load_registry()
    tables = reg.get("tables", {})
    metrics = reg.get("metrics", {})
    return templates.TemplateResponse(
        "registry.html",
        {"request": request, "tables": tables, "metrics": metrics},
    )


@router.post("/registry/metric", response_class=RedirectResponse)
async def upsert_metric(
    name: str = Form(...),
    description: str = Form(...),
):
    """Add or update a metric in the registry."""
    reg = _load_registry()
    reg.setdefault("metrics", {})[name] = {"description": description}
    _save_registry(reg)
    return RedirectResponse(url="/admin/registry", status_code=303)


@router.delete("/registry/metric/{name}")
async def delete_metric(name: str):
    """Delete a metric from the registry."""
    reg = _load_registry()
    metrics = reg.get("metrics", {})
    metrics.pop(name, None)
    reg["metrics"] = metrics
    _save_registry(reg)
    return {"deleted": name}


@router.get("/audit", response_class=HTMLResponse)
async def audit_page(request: Request):
    records = await audit.recent(limit=100)
    return templates.TemplateResponse(
        "audit.html",
        {"request": request, "records": records},
    )


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    settings = {
        "LLM Provider": cfg.LLM_PROVIDER,
        "LLM Model": cfg.LLM_MODEL,
        "LLM API Key": _mask_secret(cfg.LLM_API_KEY),
        "LLM Base URL": cfg.LLM_BASE_URL or "(default)",
        "Database URL": _mask_db_url(cfg.DATABASE_URL),
        "Registry Path": str(cfg.REGISTRY_PATH),
        "Server Host": cfg.HOST,
        "Server Port": str(cfg.PORT),
        "Feishu App ID": cfg.FEISHU_APP_ID or "(not set)",
        "Feishu App Secret": _mask_secret(cfg.FEISHU_APP_SECRET),
    }
    return templates.TemplateResponse(
        "settings.html",
        {"request": request, "settings": settings},
    )
