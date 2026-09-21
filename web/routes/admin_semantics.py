"""Admin metrics CRUD and semantic rules (disambiguations + conventions)."""
from __future__ import annotations

from datetime import date
from typing import Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from registry.validator import validate_metric
from web.admin_registry_store import (
    _load_conventions,
    _load_disambiguations,
    _load_metrics,
    _load_schema,
    _save_conventions,
    _save_disambiguations,
    _save_metrics,
)
from web.templates import templates

router = APIRouter()


def _parse_lines(text: str) -> list[str]:
    """Split textarea value into a list, stripping blank lines."""
    return [line.strip() for line in text.splitlines() if line.strip()]


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
