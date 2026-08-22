from __future__ import annotations

import asyncio
import logging
import math
import os
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import yaml
from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from config import cfg
from agent.model_config import (
    LLMConfigurationError,
    LLMNotConfiguredError,
    get_model_config,
    get_revision_model_config,
    model_control_db_path,
    reset_model_config_cache,
)
from agent.model_quality import (
    DEFAULT_THRESHOLDS,
    current_quality_lineage,
    run_quality_validation,
)
from agent.model_control import (    MODEL_SCOPE_QUERY_PLANNING,
    ModelBindingConflictError,
    ModelControlError,
    ModelControlStore,
)
from agent.llm import (
    LLMCompatibilityError,
    LLMRequestTimeoutError,
    validate_model_snapshot,
)

router = APIRouter()
logger = logging.getLogger(__name__)
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))


class ModelRevisionCreateRequest(BaseModel):
    profile_id: str
    name: str
    provider: str
    protocol: str
    model: str
    base_url: str = ""
    tool_choice: str = "required"
    timeout_seconds: float = 120
    max_output_tokens: int = 8192
    temperature: float = 0.0
    secret_ref: str
    capabilities: dict[str, Any] = Field(default_factory=dict)


class ModelActivationRequest(BaseModel):
    revision_id: str
    expected_version: int


class ModelRollbackRequest(BaseModel):
    expected_version: int


class ModelQualityValidationRequest(BaseModel):
    thresholds: dict[str, float] = Field(default_factory=lambda: dict(DEFAULT_THRESHOLDS))


_quality_validation_tasks: set[asyncio.Task] = set()


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
    control_path = model_control_db_path()
    try:
        control_active = (
            ModelControlStore(control_path).get_active(MODEL_SCOPE_QUERY_PLANNING)
            if control_path.exists() else None
        )
    except Exception as exc:
        logger.warning("Model Control Plane status unavailable: %s", type(exc).__name__)
        control_active = None
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
            "model_control_active": None if control_active is None else {
                "revision_id": control_active.revision_id,
                "profile_id": control_active.profile_id,
                "binding_version": control_active.binding_version,
                "validation_report": control_active.validation_report,
            },
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


@router.get("/settings/model-control")
async def model_control_status():
    store = ModelControlStore(model_control_db_path())
    try:
        active = store.get_active(MODEL_SCOPE_QUERY_PLANNING)
        audit = store.list_audit(MODEL_SCOPE_QUERY_PLANNING)
    except Exception as exc:
        logger.warning("Model Control Plane status unavailable: %s", type(exc).__name__)
        raise HTTPException(status_code=503, detail="Model Control Plane 状态不可用。") from exc
    return {
        "scope": MODEL_SCOPE_QUERY_PLANNING,
        "active": None if active is None else {
            "revision_id": active.revision_id,
            "profile_id": active.profile_id,
            "binding_version": active.binding_version,
            "validation_report": active.validation_report,
        },
        "audit": audit,
    }


@router.post("/settings/model-profiles", status_code=201)
async def create_model_profile_revision(payload: ModelRevisionCreateRequest):
    try:
        revision_id = ModelControlStore(model_control_db_path()).create_revision(
            profile_id=payload.profile_id,
            name=payload.name,
            config={
                "provider": payload.provider,
                "protocol": payload.protocol,
                "model": payload.model,
                "base_url": payload.base_url,
                "tool_choice": payload.tool_choice,
                "timeout_seconds": payload.timeout_seconds,
                "max_output_tokens": payload.max_output_tokens,
                "temperature": payload.temperature,
                "secret_ref": payload.secret_ref,
                "capabilities": payload.capabilities,
            },
        )
    except ModelControlError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"revision_id": revision_id, "validation_status": "pending"}


@router.post("/settings/model-profiles/{revision_id}/validate")
async def validate_model_profile_revision(revision_id: str):
    store = ModelControlStore(model_control_db_path())
    started = time.monotonic()
    try:
        snapshot = get_revision_model_config(revision_id, db_path=model_control_db_path())
        report = await asyncio.to_thread(validate_model_snapshot, snapshot)
        report["latency_ms"] = round((time.monotonic() - started) * 1000, 1)
        store.record_validation(revision_id, passed=True, report=report)
        return {"revision_id": revision_id, "status": "passed", "report": report}
    except ModelControlError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (LLMNotConfiguredError, LLMConfigurationError, LLMCompatibilityError, LLMRequestTimeoutError) as exc:
        report = {
            "tool_calling": False,
            "structured_output": False,
            "error": "模型协议、凭证或 Tool Calling 验证未通过。",
            "latency_ms": round((time.monotonic() - started) * 1000, 1),
        }
        try:
            store.record_validation(revision_id, passed=False, report=report)
        except ModelControlError as store_exc:
            status = 404 if store.get_revision(revision_id) is None else 409
            raise HTTPException(status_code=status, detail=str(store_exc)) from exc
        raise HTTPException(status_code=422, detail=report["error"]) from exc


async def _execute_quality_validation(store: ModelControlStore, run_id: str) -> None:
    try:
        await asyncio.to_thread(run_quality_validation, store, run_id)
    except Exception as exc:
        logger.warning(
            "Model quality validation failed: run=%s error=%s",
            run_id,
            type(exc).__name__,
        )


@router.post("/settings/model-profiles/{revision_id}/quality-validations", status_code=202)
async def start_model_quality_validation(
    revision_id: str,
    payload: ModelQualityValidationRequest,
):
    unknown = set(payload.thresholds) - set(DEFAULT_THRESHOLDS)
    thresholds = {**DEFAULT_THRESHOLDS, **payload.thresholds}
    if unknown or any(not math.isfinite(value) or value < 0 for value in thresholds.values()):
        raise HTTPException(status_code=400, detail="Quality Validation thresholds 不合法。")
    store = ModelControlStore(model_control_db_path())
    revision = store.get_revision(revision_id)
    if revision is None:
        raise HTTPException(status_code=404, detail="Model Profile Revision 不存在。")
    report = revision["validation_report"]
    if not (
        revision["validation_status"] == "passed"
        and report.get("tool_calling") is True
        and report.get("structured_output") is True
    ):
        raise HTTPException(status_code=409, detail="候选 Revision 尚未通过 Provider smoke。")
    try:
        run_id = store.create_quality_validation_run(revision_id, thresholds=thresholds)
    except ModelControlError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    task = asyncio.create_task(_execute_quality_validation(store, run_id))
    _quality_validation_tasks.add(task)
    task.add_done_callback(_quality_validation_tasks.discard)
    return {"run_id": run_id, "revision_id": revision_id, "status": "queued"}


@router.get("/settings/model-quality-validations/{run_id}")
async def get_model_quality_validation(run_id: str):
    run = ModelControlStore(model_control_db_path()).get_quality_validation_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Quality Validation Run 不存在。")
    return run


@router.post("/settings/model-bindings/activate")
async def activate_model_profile(payload: ModelActivationRequest):
    store = ModelControlStore(model_control_db_path())
    try:
        version = store.activate(
            payload.revision_id,
            expected_version=payload.expected_version,
            actor="admin:web",
            current_lineage=current_quality_lineage(),
        )
    except ModelBindingConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ModelControlError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    reset_model_config_cache()
    return {"scope": MODEL_SCOPE_QUERY_PLANNING, "revision_id": payload.revision_id, "binding_version": version}


@router.post("/settings/model-bindings/rollback")
async def rollback_model_profile(payload: ModelRollbackRequest):
    store = ModelControlStore(model_control_db_path())
    try:
        version = store.rollback(
            expected_version=payload.expected_version,
            actor="admin:web",
            current_lineage=current_quality_lineage(),
        )
        active = store.get_active(MODEL_SCOPE_QUERY_PLANNING)
    except ModelBindingConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ModelControlError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    reset_model_config_cache()
    return {
        "scope": MODEL_SCOPE_QUERY_PLANNING,
        "revision_id": active.revision_id if active else None,
        "binding_version": version,
    }


@router.post("/settings/llm", response_class=RedirectResponse)
async def settings_save_llm(
    provider: str = Form(...),
    model: str = Form(default=""),
    api_key: str = Form(default=""),
    base_url: str = Form(default=""),
):
    control_path = model_control_db_path()
    if control_path.exists() and ModelControlStore(control_path).get_active(MODEL_SCOPE_QUERY_PLANNING):
        return RedirectResponse(
            url="/admin/settings?error=" + quote(
                "forge.query_planning 已由 Model Control Plane 管理；请创建、验证并激活新 Revision。"
            ),
            status_code=303,
        )
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
