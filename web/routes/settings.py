from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
import yaml
from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from config import cfg
from web.feishu_runtime import feishu_runtime
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
from agent.model_control import (
    MODEL_SCOPE_QUERY_PLANNING,
    MODEL_STAGE_SCOPES,
    SQL_CRITICAL_MODEL_STAGES,
    ModelBindingConflictError,
    ModelControlError,
    ModelControlStore,
    model_scope_for_stage,
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
    stage: str = "query_generation"


class ModelRollbackRequest(BaseModel):
    expected_version: int
    stage: str = "query_generation"


class ModelQualityValidationRequest(BaseModel):
    thresholds: dict[str, float] = Field(default_factory=lambda: dict(DEFAULT_THRESHOLDS))


class ModelQualityGateRequest(BaseModel):
    enabled: bool


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


async def _get_pi_channel_status() -> dict[str, Any]:
    if not cfg.PI_ORCHESTRATOR_ENABLED:
        return {"available": False, "error": "Pi Runtime 未启用"}
    try:
        async with httpx.AsyncClient(timeout=2) as client:
            response = await client.get(f"{cfg.PI_ORCHESTRATOR_URL}/health/readiness")
        response.raise_for_status()
        capabilities = response.json().get("capabilities", {})
        return {
            "available": True,
            "ingress_configured": bool(capabilities.get("channelIngressConfigured")),
            "identity_count": int(capabilities.get("authorizedChannelIdentities") or 0),
            "auto_binding_pending": bool(capabilities.get("feishuAutoBindingPending")),
        }
    except (httpx.HTTPError, ValueError, TypeError):
        return {"available": False, "error": "Pi Channel 状态不可用"}


async def _validate_feishu_credentials(app_id: str, app_secret: str) -> tuple[bool, str]:
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                json={"app_id": app_id, "app_secret": app_secret},
            )
        payload = response.json()
    except (httpx.HTTPError, ValueError):
        return False, "无法连接飞书验证应用凭证"
    if response.status_code != 200 or payload.get("code") != 0:
        return False, str(payload.get("msg") or "飞书应用凭证无效")[:200]
    return True, ""


def _sync_pi_model_catalog(revision: dict[str, Any]) -> None:
    """Project one validated Pi-stage revision into the non-secret Pi model catalog."""
    if not cfg.PI_MODEL_CATALOG_PATH:
        raise ModelControlError("PI_MODEL_CATALOG_PATH 未配置，Pi Stage Binding 无法生效。")
    path = Path(cfg.PI_MODEL_CATALOG_PATH).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        body = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"providers": {}}
    except (OSError, json.JSONDecodeError) as exc:
        raise ModelControlError("Pi models catalog 不可读。") from exc
    config = revision["config"]
    capabilities = config.get("capabilities", {})
    provider_id = str(capabilities.get("pi_provider_id") or config["provider"])
    api = "openai-completions" if config["protocol"] == "openai_chat" else "anthropic-messages"
    providers = body.setdefault("providers", {})
    reusable_key_ref = next((
        candidate.get("apiKey")
        for candidate in providers.values()
        if candidate.get("baseUrl") == config.get("base_url", "")
        and isinstance(candidate.get("apiKey"), str)
    ), "$LLM_API_KEY")
    provider = providers.setdefault(provider_id, {
        "baseUrl": config.get("base_url", ""),
        "api": api,
        "apiKey": reusable_key_ref,
        "authHeader": True,
        "models": [],
    })
    if provider.get("baseUrl") != config.get("base_url", "") or provider.get("api") != api:
        raise ModelControlError("Pi provider ID 已绑定到不同协议或 Base URL。")
    if provider.get("apiKey") == "$LLM_API_KEY" and reusable_key_ref != "$LLM_API_KEY":
        provider["apiKey"] = reusable_key_ref
    models = provider.setdefault("models", [])
    model_entry = {
        "id": config["model"], "name": config["model"], "reasoning": False,
        "input": ["text"],
        "contextWindow": int(capabilities.get("context_window", 128000)),
        "maxTokens": int(config.get("max_output_tokens", 8192)),
        "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0},
        "compat": {"supportsDeveloperRole": False, "supportsReasoningEffort": False,
                   "supportsStrictMode": True},
    }
    models[:] = [item for item in models if item.get("id") != config["model"]]
    models.append(model_entry)
    encoded = json.dumps(body, ensure_ascii=False, indent=2) + "\n"
    fd, tmp_name = tempfile.mkstemp(prefix=".models.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp_name, path)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


def _mask_db_url(url: str) -> str:
    if not url:
        return "(not set)"
    return re.sub(r"(:)([^/@]+)(@)", lambda m: f"{m.group(1)}****{m.group(3)}", url)


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request, saved: str = "", error: str = ""):
    y = _load_forge_yaml()
    control_path = model_control_db_path()
    try:
        control_store = ModelControlStore(control_path)
        control_active = (
            control_store.get_active(MODEL_SCOPE_QUERY_PLANNING)
            if control_path.exists() else None
        )
        control_bindings = control_store.list_active() if control_path.exists() else {}
        sql_quality_gate_enabled = (
            control_store.sql_quality_gate_enabled() if control_path.exists() else True
        )
    except Exception as exc:
        logger.warning("Model Control Plane status unavailable: %s", type(exc).__name__)
        control_active = None
        control_bindings = {}
        sql_quality_gate_enabled = True
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
    pi_channel_status = await _get_pi_channel_status()
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
            "sql_quality_gate_enabled": sql_quality_gate_enabled,
            "model_stage_bindings": {
                stage: {
                    "scope": MODEL_STAGE_SCOPES[stage],
                    "gate_class": (
                        "SQL 核心强门禁" if sql_quality_gate_enabled else "兼容性验证（质量门禁关闭）"
                    ) if stage in SQL_CRITICAL_MODEL_STAGES else "Stage 能力门禁",
                    "revision_id": binding.revision_id if binding else None,
                    "profile_id": binding.profile_id if binding else None,
                    "binding_version": binding.binding_version if binding else 0,
                }
                for stage in MODEL_STAGE_SCOPES
                for binding in [control_bindings.get(stage)]
            },
            "llm_env_override": any(
                os.getenv(name)
                for name in ("LLM_PROVIDER", "LLM_MODEL", "LLM_API_KEY", "LLM_BASE_URL")
            ),
            "feishu_status": feishu_runtime.status(),
            "pi_channel_status": pi_channel_status,
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
        active_by_stage = store.list_active()
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
        "bindings": {
            stage: {
                "scope": binding.scope,
                "revision_id": binding.revision_id,
                "profile_id": binding.profile_id,
                "binding_version": binding.binding_version,
                "gate_class": "sql_critical" if stage in SQL_CRITICAL_MODEL_STAGES else "capability",
                "validation_report": binding.validation_report,
            }
            for stage, binding in active_by_stage.items()
        },
        "available_stages": MODEL_STAGE_SCOPES,
        "sql_quality_gate_enabled": store.sql_quality_gate_enabled(),
        "audit": audit,
    }


@router.post("/settings/model-quality-gate")
async def configure_model_quality_gate(payload: ModelQualityGateRequest):
    store = ModelControlStore(model_control_db_path())
    try:
        store.set_sql_quality_gate_enabled(payload.enabled, actor="admin:web")
    except ModelControlError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "enabled": store.sql_quality_gate_enabled(),
        "mode": "full_quality" if payload.enabled else "compatibility_only",
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
        scope = model_scope_for_stage(payload.stage)
        revision = store.get_revision(payload.revision_id)
        if revision is None:
            raise ModelControlError("Model Profile Revision 不存在。")
        if scope.startswith("pi."):
            _sync_pi_model_catalog(revision)
        version = store.activate(
            payload.revision_id,
            expected_version=payload.expected_version,
            actor="admin:web",
            current_lineage=current_quality_lineage(),
            scope=scope,
        )
    except ModelBindingConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ModelControlError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    reset_model_config_cache()
    return {
        "stage": payload.stage,
        "scope": scope,
        "gate_class": (
            "sql_critical" if store.sql_quality_gate_enabled() else "compatibility_only"
        ) if payload.stage in SQL_CRITICAL_MODEL_STAGES else "capability",
        "revision_id": payload.revision_id,
        "binding_version": version,
    }


@router.post("/settings/model-bindings/rollback")
async def rollback_model_profile(payload: ModelRollbackRequest):
    store = ModelControlStore(model_control_db_path())
    try:
        scope = model_scope_for_stage(payload.stage)
        version = store.rollback(
            expected_version=payload.expected_version,
            actor="admin:web",
            current_lineage=current_quality_lineage(),
            scope=scope,
        )
        active = store.get_active(scope)
    except ModelBindingConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ModelControlError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    reset_model_config_cache()
    return {
        "stage": payload.stage,
        "scope": scope,
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
    existing_secret = str(y["feishu"].get("app_secret") or "")
    existing_verification = str(y["feishu"].get("verification_token") or "")
    existing_encrypt_key = str(y["feishu"].get("encrypt_key") or "")
    candidate_secret = app_secret if app_secret and not app_secret.startswith("*") else existing_secret
    candidate_verification = (
        verification_token
        if verification_token and not verification_token.startswith("*")
        else existing_verification
    )
    candidate_encrypt_key = (
        encrypt_key if encrypt_key and not encrypt_key.startswith("*") else existing_encrypt_key
    )
    if not app_id or not candidate_secret:
        return RedirectResponse(
            url="/admin/settings?error=" + quote("请填写飞书 App ID 和 App Secret"),
            status_code=303,
        )
    valid, validation_error = await _validate_feishu_credentials(app_id, candidate_secret)
    if not valid:
        return RedirectResponse(
            url="/admin/settings?error=" + quote(validation_error),
            status_code=303,
        )
    y["feishu"]["app_id"] = app_id
    y["feishu"]["app_secret"] = candidate_secret
    y["feishu"]["verification_token"] = candidate_verification
    y["feishu"]["encrypt_key"] = candidate_encrypt_key
    y["feishu"]["pi_enabled"] = True
    _save_forge_yaml(y)
    runtime_status = feishu_runtime.reload()
    if not runtime_status.process_running:
        return RedirectResponse(
            url="/admin/settings?error=" + quote(runtime_status.last_error or "飞书 Runtime 启动失败"),
            status_code=303,
        )
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
