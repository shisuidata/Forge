"""Authenticated report publication and download routes."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from config import cfg
from forge.reporting import ReportStore
from web.auth import require_pi_service_auth, require_web_auth, verify_web_request

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


@router.post("/api/internal/reports", dependencies=[Depends(require_pi_service_auth)])
async def create_report(payload: dict[str, Any], background: BackgroundTasks, request: Request):
    try:
        record = get_report_store().create(payload)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if record["status"] == "publishing":
        background.add_task(get_report_store().build, record["report_id"])
    return JSONResponse(status_code=202, content={"status": "accepted", "report": _public(record, request)})


@router.get("/api/internal/reports/{report_id}", dependencies=[Depends(require_pi_service_auth)])
async def get_internal_report(report_id: str, request: Request):
    try:
        record = get_report_store().get(report_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Report not found") from exc
    return {"report": _public(record, request)}


def _share_cookie(report_id: str) -> str:
    return f"forge_report_share_{report_id}"


def _require_report_access(request: Request, report_id: str, *, technical: bool = False) -> str:
    if verify_web_request(request) or not cfg.AUTH_ENABLED:
        return "web"
    if not technical:
        token = request.cookies.get(_share_cookie(report_id), "")
        if token:
            try:
                share = get_report_store().resolve_share(token, report_id)
                return share["share_id"]
            except KeyError:
                pass
    raise HTTPException(status_code=401, detail="Report access is unauthorized")


@router.post("/api/reports/{report_id}/shares", dependencies=[Depends(require_web_auth)])
async def create_report_share(report_id: str, payload: dict[str, Any], request: Request):
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


@router.delete("/api/reports/{report_id}/shares/{share_id}", dependencies=[Depends(require_web_auth)])
async def revoke_report_share(report_id: str, share_id: str):
    try:
        get_report_store().revoke_share(report_id, share_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Share not found") from exc
    return {"status": "revoked"}


@router.post("/api/reports/{report_id}/exports/{fmt}/retry", dependencies=[Depends(require_web_auth)])
async def retry_report_export(report_id: str, fmt: str, background: BackgroundTasks):
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
    _require_report_access(request, report_id)
    try:
        path = get_report_store().file(report_id, "index.html")
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Report not ready") from exc
    return HTMLResponse(path.read_text(encoding="utf-8"), headers={"Cache-Control": "private, no-store"})


@router.get("/reports/{report_id}/technical")
async def view_technical_report(report_id: str, request: Request):
    _require_report_access(request, report_id, technical=True)
    try:
        path = get_report_store().file(report_id, "technical.html")
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Technical report not ready") from exc
    return HTMLResponse(path.read_text(encoding="utf-8"), headers={"Cache-Control": "private, no-store"})


@router.get("/reports/{report_id}/download/{fmt}")
async def download_report(report_id: str, fmt: str, request: Request):
    actor = _require_report_access(request, report_id)
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
