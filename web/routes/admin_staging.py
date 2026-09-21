"""Admin staging queue: review, promote and discard staged semantic candidates."""
from __future__ import annotations

import json
import logging
import shutil
import tempfile
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from config import cfg
from registry.staging_sync import promote_staged
from web.templates import templates

logger = logging.getLogger(__name__)

router = APIRouter()


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
            request,
            "staging.html",
            {"records": records,
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
