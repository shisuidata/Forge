"""
Forge admin web UI — FastAPI router mounted at /admin.

Routes
------
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
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from agent import audit
from config import cfg
from registry.validator import validate_metric
from registry.staging_sync import promote_staged

logger = logging.getLogger(__name__)

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


# ── routes ────────────────────────────────────────────────────────────────────

@router.get("/", response_class=RedirectResponse)
async def admin_root():
    return RedirectResponse(url="/admin/registry", status_code=302)


@router.get("/registry", response_class=HTMLResponse)
async def registry_page(request: Request):
    schema  = _load_schema()
    tables  = schema.get("tables", {})
    metrics = _load_metrics()
    atomics     = {k: v for k, v in metrics.items() if v.get("metric_class") == "atomic"}
    derivatives = {k: v for k, v in metrics.items() if v.get("metric_class") == "derivative"}
    return templates.TemplateResponse(
        "registry.html",
        {"request": request, "tables": tables,
         "atomics": atomics, "derivatives": derivatives, "all_metrics": metrics},
    )


@router.post("/registry/metric", response_class=HTMLResponse)
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

    # ── validate ──────────────────────────────────────────────────────────────
    structural  = _load_schema()
    all_metrics = _load_metrics()
    result = validate_metric(entry, structural, metric_name=name, all_metrics=all_metrics)
    if not result.valid:
        tables      = structural.get("tables", {})
        atomics     = {k: v for k, v in all_metrics.items() if v.get("metric_class") == "atomic"}
        derivatives = {k: v for k, v in all_metrics.items() if v.get("metric_class") == "derivative"}
        return templates.TemplateResponse(
            "registry.html",
            {
                "request":       request,
                "tables":        tables,
                "atomics":       atomics,
                "derivatives":   derivatives,
                "all_metrics":   all_metrics,
                "form_errors":   result.errors,
                "form_warnings": result.warnings,
                "form_data":     {"name": name, **entry},
            },
            status_code=422,
        )

    # ── save ──────────────────────────────────────────────────────────────────
    entry["updated_at"] = str(date.today())
    metrics = _load_metrics()
    metrics[name] = entry
    _save_metrics(metrics)
    return RedirectResponse(url="/admin/registry", status_code=303)


@router.delete("/registry/metric/{name}")
async def delete_metric(name: str):
    metrics = _load_metrics()
    metrics.pop(name, None)
    _save_metrics(metrics)
    return {"deleted": name}


@router.get("/semantic", response_class=HTMLResponse)
async def semantic_page(request: Request, flash: str = ""):
    try:
        disambiguations = yaml.safe_load(cfg.DISAMBIGUATIONS_PATH.read_text()) or {}
    except (FileNotFoundError, OSError, yaml.YAMLError) as exc:
        logger.warning("Failed to load disambiguations: %s", exc)
        disambiguations = {}
    try:
        conventions = yaml.safe_load(cfg.CONVENTIONS_PATH.read_text()) or {}
    except (FileNotFoundError, OSError, yaml.YAMLError) as exc:
        logger.warning("Failed to load conventions: %s", exc)
        conventions = {}
    return templates.TemplateResponse(
        "semantic.html",
        {"request": request, "disambiguations": disambiguations,
         "conventions": conventions, "flash": flash},
    )


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
async def audit_page(request: Request):
    records = await audit.recent(limit=100)
    return templates.TemplateResponse(
        "audit.html",
        {"request": request, "records": records},
    )


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    settings = {
        "LLM Provider":         cfg.LLM_PROVIDER,
        "LLM Model":            cfg.LLM_MODEL,
        "LLM API Key":          _mask_secret(cfg.LLM_API_KEY),
        "LLM Base URL":         cfg.LLM_BASE_URL or "(default)",
        "Database URL":         _mask_db_url(cfg.DATABASE_URL),
        "Registry Path":        str(cfg.REGISTRY_PATH),
        "Metrics Path":         str(cfg.METRICS_PATH),
        "Disambiguations Path": str(cfg.DISAMBIGUATIONS_PATH),
        "Conventions Path":     str(cfg.CONVENTIONS_PATH),
        "Staging Dir":          str(cfg.STAGING_DIR),
        "Server Host":          cfg.HOST,
        "Server Port":          str(cfg.PORT),
        "Feishu App ID":        cfg.FEISHU_APP_ID or "(not set)",
        "Feishu App Secret":    _mask_secret(cfg.FEISHU_APP_SECRET),
    }
    return templates.TemplateResponse(
        "settings.html",
        {"request": request, "settings": settings},
    )
