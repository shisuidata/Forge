"""Authenticated report publication and download routes."""
from __future__ import annotations

import sqlite3
import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from config import cfg
from forge.reporting import ReportStore
from web.auth import require_pi_service_auth, require_human_principal
from web.pi_task_scope import task_scope
from forge.governance import connect_governance, ensure_governance_schema

router = APIRouter()


@lru_cache(maxsize=1)
def get_report_store() -> ReportStore:
    return ReportStore(cfg.REPORT_DB_PATH, cfg.REPORT_ARTIFACT_DIR)


def _public(record: dict[str, Any], request: Request) -> dict[str, Any]:
    base = (cfg.REPORT_PUBLIC_BASE_URL or str(request.base_url)).rstrip("/")
    visible = {key: value for key, value in record.items() if key not in {"org_id", "team_id", "user_id", "error"}}
    for key in ("internal_url", "technical_url", "pdf_url", "pptx_url"):
        if isinstance(visible.get(key), str) and visible[key].startswith("/"):
            visible[key] = base + visible[key]
    return visible


async def _owned_report(report_id: str, task_run_id: str) -> tuple[dict, dict]:
    scope = await task_scope(task_run_id)
    try:
        record = get_report_store().get(report_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="report_not_found") from exc
    except (OSError, sqlite3.Error) as exc:
        raise HTTPException(status_code=503, detail="report_store_unavailable") from exc
    if any(record[k] != scope[v] for k, v in (("task_run_id", "task_id"), ("org_id", "org_id"),
            ("team_id", "team_id"), ("user_id", "user_id"))):
        raise HTTPException(status_code=404, detail="report_not_found")
    return record, scope


def _verify_query_evidence(scope: dict, payload: dict) -> None:
    query = payload.get("query_result")
    if not isinstance(query, dict) or not isinstance(query.get("query_run_id"), str):
        raise HTTPException(status_code=422, detail="report_query_evidence_required")
    try:
        with connect_governance() as conn:
            ensure_governance_schema(conn)
            run = conn.execute("SELECT task_run_id,status,sql_hash,result_columns,result_rows,row_count,truncated FROM query_runs WHERE query_run_id=?",
                               (query["query_run_id"],)).fetchone()
    except (OSError, sqlite3.Error) as exc:
        raise HTTPException(status_code=503, detail="governance_unavailable") from exc
    if (run is None or run["task_run_id"] != scope["task_id"] or run["status"] != "completed"
            or query.get("sql_hash") != run["sql_hash"]
            or query.get("columns") != json.loads(run["result_columns"] or "[]")
            or query.get("rows") != json.loads(run["result_rows"] or "[]")
            or query.get("row_count") != run["row_count"]
            or query.get("truncated") != bool(run["truncated"])):
        raise HTTPException(status_code=422, detail="report_query_evidence_invalid")


@router.post("/api/internal/reports")
async def create_report(payload: dict[str, Any], background: BackgroundTasks, request: Request,
                        principal=Depends(require_pi_service_auth)):
    if set(payload) != {"task_run_id", "report_id", "revision", "bundle_hash", "title", "business_report",
                        "analysis", "query_result", "charts", "technical_report"}:
        raise HTTPException(status_code=422, detail="report input fields do not match the fixed contract")
    scope = await task_scope(payload["task_run_id"])
    _verify_query_evidence(scope, payload)
    source = {**payload, "org_id": scope["org_id"], "team_id": scope["team_id"], "user_id": scope["user_id"]}
    try:
        record = get_report_store().create(source)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except (OSError, sqlite3.Error) as exc:
        raise HTTPException(status_code=503, detail="report_store_unavailable") from exc
    if record["status"] == "publishing":
        background.add_task(get_report_store().build, record["report_id"])
    return JSONResponse(status_code=202, content={"status": "accepted", "report": _public(record, request)})


@router.get("/api/internal/reports/{report_id}")
async def get_internal_report(report_id: str, task_run_id: str, request: Request,
                              principal=Depends(require_pi_service_auth)):
    record, _ = await _owned_report(report_id, task_run_id)
    return {"report": _public(record, request)}


def _share_cookie(report_id: str) -> str:
    return f"forge_report_share_{report_id}"


async def _require_report_access(request: Request, report_id: str, *, technical: bool = False) -> str:
    try:
        record = get_report_store().get(report_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="report_not_found") from exc
    if not technical:
        token = request.cookies.get(_share_cookie(report_id), "")
        if token:
            try:
                share = get_report_store().resolve_share(token, report_id)
                return share["share_id"]
            except KeyError:
                pass
    principal = await require_human_principal(request)
    if ((principal.principal_id != record["user_id"] and principal.role != "admin") or
            principal.organization_id != record["org_id"] or principal.workspace_id != record["team_id"]):
        raise HTTPException(status_code=404, detail="report_not_found")
    return principal.principal_id


@router.post("/api/reports/{report_id}/shares", dependencies=[Depends(require_human_principal)])
async def create_report_share(report_id: str, payload: dict[str, Any], request: Request):
    await _require_report_access(request, report_id, technical=True)
    expires_at = payload.get("expires_at")
    if not isinstance(expires_at, str):
        raise HTTPException(status_code=422, detail="expires_at is required")
    try:
        share = get_report_store().create_share(report_id, expires_at)
    except (KeyError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    token = share.pop("token")
    return {
        **share,
        "scope": "business",
        "exchange_url": f"{(cfg.REPORT_PUBLIC_BASE_URL or str(request.base_url)).rstrip('/')}/reports/share/{share['share_id']}#token={token}",
    }


@router.delete("/api/reports/{report_id}/shares/{share_id}", dependencies=[Depends(require_human_principal)])
async def revoke_report_share(report_id: str, share_id: str, request: Request):
    await _require_report_access(request, report_id, technical=True)
    try:
        get_report_store().revoke_share(report_id, share_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Share not found") from exc
    return {"status": "revoked"}


@router.post("/api/reports/{report_id}/exports/{fmt}/retry", dependencies=[Depends(require_human_principal)])
async def retry_report_export(report_id: str, fmt: str, background: BackgroundTasks, request: Request):
    await _require_report_access(request, report_id, technical=True)
    if fmt not in {"pdf", "pptx"}:
        raise HTTPException(status_code=404, detail="Unsupported report format")
    try:
        get_report_store().get(report_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Report not found") from exc
    background.add_task(get_report_store().retry_export, report_id, fmt)
    return JSONResponse(status_code=202, content={"status": "accepted", "format": fmt})


@router.get("/reports/share/{share_id}")
async def report_share_exchange_page(share_id: str):
    safe_share_id = "".join(char for char in share_id if char.isalnum() or char in "_-")
    if safe_share_id != share_id or not share_id.startswith("rps_"):
        raise HTTPException(status_code=404, detail="Share not found")
    page = f"""<!doctype html><meta charset=\"utf-8\"><title>打开 Forge 报告</title>
<p id=\"status\">正在验证分享链接…</p><script>
const token=new URLSearchParams(location.hash.slice(1)).get('token');
if(!token){{document.getElementById('status').textContent='分享链接无效';}}
else{{fetch('/reports/share/{safe_share_id}/exchange',{{method:'POST',headers:{{'content-type':'application/json'}},body:JSON.stringify({{token}})}}).then(async r=>{{if(!r.ok)throw new Error();return r.json();}}).then(x=>location.replace(x.redirect)).catch(()=>document.getElementById('status').textContent='分享链接已失效或被撤销');}}
</script>"""
    return HTMLResponse(page, headers={"Cache-Control": "no-store"})


@router.post("/reports/share/{share_id}/exchange")
async def exchange_report_share(share_id: str, payload: dict[str, Any]):
    token = payload.get("token")
    if not isinstance(token, str):
        raise HTTPException(status_code=404, detail="Share is invalid")
    try:
        share = get_report_store().resolve_share(token, share_id=share_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Share is invalid, expired, or revoked") from exc
    response = JSONResponse({"redirect": f"/reports/{share['report_id']}"})
    response.set_cookie(
        _share_cookie(share["report_id"]), token, httponly=True, samesite="lax",
        secure=bool(getattr(cfg, "AUTH_COOKIE_SECURE", False)), path=f"/reports/{share['report_id']}",
    )
    return response


@router.get("/reports/{report_id}")
async def view_report(report_id: str, request: Request):
    await _require_report_access(request, report_id)
    try:
        path = get_report_store().file(report_id, "index.html")
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Report not ready") from exc
    return HTMLResponse(path.read_text(encoding="utf-8"), headers={"Cache-Control": "private, no-store"})


@router.get("/reports/{report_id}/technical")
async def view_technical_report(report_id: str, request: Request):
    await _require_report_access(request, report_id, technical=True)
    try:
        path = get_report_store().file(report_id, "technical.html")
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Technical report not ready") from exc
    return HTMLResponse(path.read_text(encoding="utf-8"), headers={"Cache-Control": "private, no-store"})


@router.get("/reports/{report_id}/download/{fmt}")
async def download_report(report_id: str, fmt: str, request: Request):
    actor = await _require_report_access(request, report_id)
    names = {"pdf": ("report.pdf", "application/pdf"),
             "pptx": ("report.pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation")}
    if fmt not in names:
        raise HTTPException(status_code=404, detail="Unsupported report format")
    filename, media_type = names[fmt]
    try:
        path: Path = get_report_store().file(report_id, filename)
        get_report_store().audit_download(report_id, fmt, actor)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Export not ready") from exc
    return FileResponse(
        path,
        media_type=media_type,
        filename=f"forge-report-{report_id}.{fmt}",
        headers={"Cache-Control": "private, no-store"},
    )
