from __future__ import annotations

import os
import re
from pathlib import Path
from urllib.parse import quote

import yaml
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from config import cfg
from agent.model_config import (
    LLMConfigurationError,
    LLMNotConfiguredError,
    get_model_config,
    reset_model_config_cache,
)

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))


def _load_forge_yaml() -> dict:
    """读取 forge.yaml 原始内容。"""
    yaml_path = Path(__file__).resolve().parents[2] / "forge.yaml"
    try:
        return yaml.safe_load(yaml_path.read_text()) or {}
    except (FileNotFoundError, OSError, yaml.YAMLError):
        return {}


def _save_forge_yaml(data: dict) -> None:
    """Atomically write forge.yaml so hot readers never observe partial YAML."""
    yaml_path = Path(__file__).resolve().parents[2] / "forge.yaml"
    temp_path = yaml_path.with_suffix(".yaml.tmp")
    temp_path.write_text(
        yaml.dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
    os.chmod(temp_path, 0o600)
    os.replace(temp_path, yaml_path)


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


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request, saved: str = "", error: str = ""):
    y = _load_forge_yaml()
    try:
        active_model = get_model_config()
        model_status = {
            "configured": True,
            "provider": active_model.provider,
            "model": active_model.model,
            "source": active_model.source,
            "revision": active_model.revision[:15] + "…",
        }
    except (LLMNotConfiguredError, LLMConfigurationError) as exc:
        model_status = {"configured": False, "error": str(exc)}
    return templates.TemplateResponse(
        request,
        "settings.html",
        {
            "y": y,
            "mask_secret": _mask_secret,
            "mask_db_url": _mask_db_url,
            "saved": saved,
            "error": error,
            "model_status": model_status,
            "llm_env_override": any(
                os.getenv(name)
                for name in ("LLM_PROVIDER", "LLM_MODEL", "LLM_API_KEY", "LLM_BASE_URL")
            ),
            "database_status": {
                "configured": bool(cfg.DATABASE_URL),
                "masked_url": _mask_db_url(cfg.DATABASE_URL),
                "env_override": bool(os.getenv("DATABASE_URL")),
                "readonly_confirmed": bool(cfg.DATABASE_READONLY_CONFIRMED),
            },
        },
    )


@router.post("/settings/llm", response_class=RedirectResponse)
async def settings_save_llm(
    provider: str = Form(...),
    model: str = Form(default=""),
    api_key: str = Form(default=""),
    base_url: str = Form(default=""),
):
    y = _load_forge_yaml()
    y.setdefault("llm", {})
    y["llm"]["provider"] = provider
    y["llm"]["model"] = model
    if api_key and not api_key.startswith("*"):
        y["llm"]["api_key"] = api_key
    y["llm"]["base_url"] = base_url
    _save_forge_yaml(y)
    reset_model_config_cache()
    try:
        get_model_config()
    except (LLMNotConfiguredError, LLMConfigurationError) as exc:
        return RedirectResponse(
            url=f"/admin/settings?error={quote(str(exc))}",
            status_code=303,
        )
    return RedirectResponse(url="/admin/settings?saved=llm", status_code=303)


@router.post("/settings/database", response_class=RedirectResponse)
async def settings_save_database(url: str = Form(default="")):
    y = _load_forge_yaml()
    y.setdefault("database", {})
    y["database"]["url"] = url
    _save_forge_yaml(y)
    return RedirectResponse(url="/admin/settings?saved=database", status_code=303)


@router.post("/settings/execution", response_class=RedirectResponse)
async def settings_save_execution(
    request: Request,
    max_rows: str = Form(default="200"),
    display_rows: str = Form(default="50"),
    timeout_seconds: str = Form(default="30"),
):
    form = await request.form()
    y = _load_forge_yaml()
    y.setdefault("execution", {})
    y["execution"]["enabled"] = "enabled" in form
    y["execution"]["raw_sql_enabled"] = "raw_sql_enabled" in form
    y["execution"]["database_readonly_confirmed"] = "database_readonly_confirmed" in form
    y["execution"]["max_rows"] = int(max_rows) if max_rows.isdigit() else 200
    y["execution"]["display_rows"] = int(display_rows) if display_rows.isdigit() else 50
    y["execution"]["timeout_seconds"] = int(timeout_seconds) if timeout_seconds.isdigit() else 30
    _save_forge_yaml(y)
    return RedirectResponse(url="/admin/settings?saved=execution", status_code=303)


@router.post("/settings/embedding", response_class=RedirectResponse)
async def settings_save_embedding(
    api_key: str = Form(default=""),
    base_url: str = Form(default=""),
    model: str = Form(default=""),
    top_k: str = Form(default="5"),
):
    y = _load_forge_yaml()
    y.setdefault("embedding", {})
    if api_key and not api_key.startswith("*"):
        y["embedding"]["api_key"] = api_key
    y["embedding"]["base_url"] = base_url
    y["embedding"]["model"] = model
    y["embedding"]["top_k"] = int(top_k) if top_k.isdigit() else 5
    _save_forge_yaml(y)
    return RedirectResponse(url="/admin/settings?saved=embedding", status_code=303)


@router.post("/settings/registry", response_class=RedirectResponse)
async def settings_save_registry(
    schema_path: str = Form(default=""),
    metrics_path: str = Form(default=""),
    disambiguations_path: str = Form(default=""),
    conventions_path: str = Form(default=""),
):
    y = _load_forge_yaml()
    y.setdefault("registry", {})
    y["registry"]["schema_path"] = schema_path
    y["registry"]["metrics_path"] = metrics_path
    y["registry"]["disambiguations_path"] = disambiguations_path
    y["registry"]["conventions_path"] = conventions_path
    _save_forge_yaml(y)
    return RedirectResponse(url="/admin/settings?saved=registry", status_code=303)


@router.post("/settings/feishu", response_class=RedirectResponse)
async def settings_save_feishu(
    app_id: str = Form(default=""),
    app_secret: str = Form(default=""),
    verification_token: str = Form(default=""),
    encrypt_key: str = Form(default=""),
):
    y = _load_forge_yaml()
    y.setdefault("feishu", {})
    y["feishu"]["app_id"] = app_id
    if app_secret and not app_secret.startswith("*"):
        y["feishu"]["app_secret"] = app_secret
    y["feishu"]["verification_token"] = verification_token
    y["feishu"]["encrypt_key"] = encrypt_key
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
    form = await request.form()
    enabled = "enabled" in form
    cookie_secure = "cookie_secure" in form
    y = _load_forge_yaml()
    y.setdefault("server", {}).setdefault("auth", {})
    y["server"]["auth"]["enabled"] = enabled
    y["server"]["auth"]["cookie_secure"] = cookie_secure
    if admin_password and not admin_password.startswith("*"):
        y["server"]["auth"]["admin_password"] = admin_password
    keys = [k.strip() for k in api_keys.splitlines() if k.strip()]
    y["server"]["auth"]["api_keys"] = keys
    _save_forge_yaml(y)
    return RedirectResponse(url="/admin/settings?saved=auth", status_code=303)


@router.post("/settings/memory", response_class=RedirectResponse)
async def settings_save_memory(
    db_url: str = Form(default=""),
    db_path: str = Form(default=".forge/memory.db"),
):
    y = _load_forge_yaml()
    y.setdefault("memory", {})
    y["memory"]["db_url"] = db_url
    y["memory"]["db_path"] = db_path
    _save_forge_yaml(y)
    return RedirectResponse(url="/admin/settings?saved=memory", status_code=303)
