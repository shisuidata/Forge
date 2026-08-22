from __future__ import annotations

from functools import lru_cache
import json
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Request
from fastapi.responses import HTMLResponse

from config import cfg
from registry.studio import (
    RegistryStudioError,
    RegistryStudioStore,
    er_projection,
    parse_ddl_draft,
    render_ddl,
)

router = APIRouter()


@lru_cache(maxsize=1)
def _store() -> RegistryStudioStore:
    return RegistryStudioStore(cfg.REGISTRY_STUDIO_DB_PATH, cfg.REGISTRY_PATH)


def _active_projection(view: str) -> dict[str, Any]:
    active = _store().active()
    schema = active["schema"]
    if view == "json":
        projection: Any = schema
    elif view == "ddl":
        projection = render_ddl(schema)
    elif view == "er":
        projection = er_projection(schema)
    elif view == "table":
        projection = [
            {
                "table": table_name,
                "kind": table["kind"],
                "description": table.get("description", ""),
                "column_count": len(table["columns"]),
                "columns": [
                    {"name": name, **column}
                    for name, column in sorted(
                        table["columns"].items(), key=lambda item: item[1]["ordinal"]
                    )
                ],
            }
            for table_name, table in schema["tables"].items()
        ]
    else:
        raise RegistryStudioError("Registry Studio view 不受支持。")
    return {
        "version": active["version"],
        "revision_id": schema["registry_revision"],
        "view": view,
        "projection": projection,
    }


@router.get("/registry-studio", response_class=HTMLResponse)
async def registry_studio_page(request: Request):
    from web.router import templates

    active = _active_projection("table")
    return templates.TemplateResponse(
        request,
        "registry_studio.html",
        {
            "active": active,
            "ddl": _active_projection("ddl")["projection"],
            "er": _active_projection("er")["projection"],
        },
    )


@router.get("/api/registry-studio")
async def registry_studio_active(view: str = "table"):
    try:
        return _active_projection(view)
    except RegistryStudioError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/api/registry-studio/revisions")
async def registry_studio_revisions():
    return {"revisions": _store().history()}


@router.get("/api/registry-studio/drafts/{draft_id}")
async def registry_studio_draft(draft_id: str):
    try:
        return _store().get_draft(draft_id)
    except RegistryStudioError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/api/registry-studio/drafts", status_code=201)
async def registry_studio_create_draft(payload: dict[str, Any] = Body(...)):
    try:
        active = _store().active()
        reason = str(payload.get("reason") or "Registry Studio draft").strip()
        if not reason:
            raise RegistryStudioError("Draft change reason 不能为空。")
        if isinstance(payload.get("schema"), dict):
            candidate = payload["schema"]
        elif isinstance(payload.get("ddl"), str):
            candidate = parse_ddl_draft(
                payload["ddl"],
                datasource_id=active["schema"]["datasource"]["id"],
                dialect=str(payload.get("dialect") or active["schema"]["datasource"]["dialect"]),
            )
        else:
            raise RegistryStudioError("Draft 必须提供 schema 或 DDL。")
        return _store().create_draft(
            candidate,
            base_revision_id=str(payload.get("base_revision_id") or active["revision_id"]),
            actor="web-admin",
            reason=reason,
        )
    except RegistryStudioError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/api/registry-studio/drafts/{draft_id}/publish")
async def registry_studio_publish(draft_id: str, payload: dict[str, Any] = Body(...)):
    expected = payload.get("expected_version")
    if not isinstance(expected, int):
        raise HTTPException(status_code=400, detail="expected_version 必须是整数。")
    try:
        return _store().publish(draft_id, expected_version=expected, actor="web-admin")
    except RegistryStudioError as exc:
        status = 409 if "冲突" in str(exc) or "过期" in str(exc) else 400
        raise HTTPException(status_code=status, detail=str(exc)) from exc


@router.post("/api/registry-studio/relationships/{relationship_id}/confirm", status_code=201)
async def registry_studio_confirm_relationship(
    relationship_id: str,
    payload: dict[str, Any] = Body(...),
):
    try:
        active = _store().active()
        candidate = json.loads(json.dumps(active["schema"]))
        relationship = next(
            (item for item in candidate["relationships"] if item.get("id") == relationship_id),
            None,
        )
        if relationship is None:
            raise RegistryStudioError("Relationship proposal 不存在。")
        if relationship.get("status") in {"confirmed", "declared"}:
            raise RegistryStudioError("Relationship 已经可信，无需重复确认。")
        relationship["status"] = "confirmed"
        relationship["source"] = "manual_confirmation"
        return _store().create_draft(
            candidate,
            base_revision_id=active["schema"]["registry_revision"],
            actor="web-admin",
            reason=str(payload.get("reason") or f"confirm relationship {relationship_id}"),
        )
    except RegistryStudioError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/api/registry-studio/rollback")
async def registry_studio_rollback(payload: dict[str, Any] = Body(...)):
    expected = payload.get("expected_version")
    revision_id = payload.get("revision_id")
    reason = str(payload.get("reason") or "admin rollback").strip()
    if not isinstance(expected, int) or not isinstance(revision_id, str):
        raise HTTPException(status_code=400, detail="revision_id 和 expected_version 必填。")
    try:
        return _store().rollback(
            revision_id,
            expected_version=expected,
            actor="web-admin",
            reason=reason,
        )
    except RegistryStudioError as exc:
        status = 409 if "冲突" in str(exc) else 400
        raise HTTPException(status_code=status, detail=str(exc)) from exc
