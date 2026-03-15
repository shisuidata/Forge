"""
Forge Agent — FastAPI entry point.

Endpoints:
  POST /webhook/feishu   — Feishu event subscription + card callbacks
  GET  /health           — health check
  GET  /admin/*          — admin web UI (registry, audit log, settings)
"""
import logging
from pathlib import Path

import lark_oapi as lark
from fastapi import FastAPI, Request, Response
from fastapi.staticfiles import StaticFiles

from agent.feishu import dispatcher
from web.router import router as admin_router

logging.basicConfig(level=logging.INFO)
app = FastAPI(title="Forge Agent")

app.include_router(admin_router, prefix="/admin")

# 图表静态文件服务
_charts_dir = Path(__file__).parent / "web" / "static" / "charts"
_charts_dir.mkdir(parents=True, exist_ok=True)
app.mount("/charts", StaticFiles(directory=str(_charts_dir)), name="charts")


@app.post("/webhook/feishu")
async def feishu_webhook(request: Request) -> Response:
    body = await request.body()
    headers = dict(request.headers)
    resp = dispatcher.dispatch(
        lark.RawRequest.builder().headers(headers).body(body).build()
    )
    return Response(content=resp.body, media_type="application/json")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
