"""Admin knowledge sources: candidate review, collection and document import."""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from web.templates import templates

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/knowledge", response_class=HTMLResponse)
async def knowledge_page(request: Request, flash: str = ""):
    from agent.knowledge import knowledge_store
    candidates = knowledge_store.list_candidates(status="pending", limit=50)
    confirmed = knowledge_store.list_candidates(status="confirmed", limit=20)
    sources = knowledge_store.list_sources(enabled_only=False)
    pending_count = knowledge_store.pending_count()
    return templates.TemplateResponse(
            request,
            "knowledge.html",
            {"candidates": candidates, "confirmed": confirmed,
         "sources": sources, "pending_count": pending_count, "flash": flash},
        )


@router.post("/knowledge/confirm/{cid}", response_class=RedirectResponse)
async def knowledge_confirm(cid: int):
    from agent.knowledge import knowledge_store
    knowledge_store.confirm(cid)
    return RedirectResponse(url="/admin/knowledge?flash=已确认", status_code=303)


@router.post("/knowledge/reject/{cid}", response_class=RedirectResponse)
async def knowledge_reject(cid: int):
    from agent.knowledge import knowledge_store
    knowledge_store.reject(cid)
    return RedirectResponse(url="/admin/knowledge?flash=已忽略", status_code=303)


@router.post("/knowledge/source", response_class=RedirectResponse)
async def knowledge_add_source(
    type: str = Form(...),
    name: str = Form(...),
    url:  str = Form(default=""),
    keywords: str = Form(default=""),
    schedule: str = Form(default="daily"),
):
    from agent.knowledge import knowledge_store
    config = {"schedule": schedule}
    if url:
        config["url"] = url
    if keywords:
        config["keywords"] = keywords
    knowledge_store.add_source(type, name, config)
    return RedirectResponse(url="/admin/knowledge?flash=知识源已添加", status_code=303)


@router.post("/knowledge/source/delete/{sid}", response_class=RedirectResponse)
async def knowledge_delete_source(sid: int):
    from agent.knowledge import knowledge_store
    knowledge_store.delete_source(sid)
    return RedirectResponse(url="/admin/knowledge?flash=已删除", status_code=303)


@router.post("/knowledge/collect", response_class=JSONResponse)
async def knowledge_collect_all():
    """手动触发所有知识源收集。"""
    try:
        from agent.knowledge import knowledge_collector
        stats = knowledge_collector.run_all()
        return JSONResponse({"ok": True, "added": stats["added"], "errors": stats["errors"],
                             "processed": stats["processed"]})
    except Exception as exc:
        logger.warning("Knowledge collect all failed: %s", exc)
        return JSONResponse({"ok": False, "added": 0, "errors": 1, "detail": str(exc)}, status_code=500)


@router.post("/knowledge/collect/{sid}", response_class=JSONResponse)
async def knowledge_collect_one(sid: int):
    """触发单个知识源收集。"""
    try:
        from agent.knowledge import knowledge_store, knowledge_collector
        sources = knowledge_store.list_sources(enabled_only=False)
        source = next((s for s in sources if s["id"] == sid), None)
        if source is None:
            return JSONResponse({"ok": False, "detail": "知识源不存在"}, status_code=404)
        added = knowledge_collector.run_source(source)
        return JSONResponse({"ok": True, "added": added, "errors": 0})
    except Exception as exc:
        logger.warning("Knowledge collect source %s failed: %s", sid, exc)
        return JSONResponse({"ok": False, "added": 0, "errors": 1, "detail": str(exc)}, status_code=500)


# ── 文档导入 ──────────────────────────────────────────────────────────────────

@router.get("/knowledge/import", response_class=HTMLResponse)
async def knowledge_import_page(request: Request):
    return templates.TemplateResponse(request, "import.html", {})


@router.post("/knowledge/import/upload", response_class=JSONResponse)
async def knowledge_import_upload(request: Request):
    """上传文件，LLM 提取知识点，返回预览列表。"""
    import re
    form = await request.form()
    file = form.get("file")
    if file is None:
        return JSONResponse({"ok": False, "detail": "未收到文件"}, status_code=400)

    filename = file.filename or ""
    raw_bytes = await file.read()

    # 解析文本
    text = ""
    if filename.lower().endswith(".pdf"):
        return JSONResponse({"ok": False, "detail": "请将 PDF 转换为 .txt 或 .md 后再导入"}, status_code=400)
    else:
        try:
            text = raw_bytes.decode("utf-8", errors="replace")
        except Exception:
            return JSONResponse({"ok": False, "detail": "文件编码无法识别，请使用 UTF-8 编码"}, status_code=400)

    if not text.strip():
        return JSONResponse({"ok": False, "detail": "文件内容为空"}, status_code=400)

    # 按 2000 字分段，每段用 LLM 提取
    chunk_size = 2000
    chunks = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

    all_items: list[dict] = []
    try:
        from agent import llm as llm_module
        system_prompt = (
            "你是知识提取助手。从以下文档内容中提取3-5条有价值的业务知识点，"
            "每条50字以内，JSON数组格式：[{\"key\":\"知识点标题\",\"value\":\"内容\"}]"
            "只输出 JSON 数组，不要有其他内容。"
        )
        for chunk in chunks[:5]:  # 最多处理前5段
            messages = [{"role": "user", "content": f"文档片段：\n\n{chunk}"}]
            result = llm_module.call(messages, system_override=system_prompt)
            raw = result.get("text", "") or ""
            json_match = re.search(r"\[.*\]", raw, re.DOTALL)
            if json_match:
                items = json.loads(json_match.group())
                for item in items:
                    k = str(item.get("key", "doc_fact"))[:80]
                    v = str(item.get("value", ""))[:500]
                    if v:
                        all_items.append({"key": k, "value": v, "selected": True})
    except Exception as exc:
        logger.info("LLM extraction failed during import: %s", exc)
        # 降级：直接把每段前200字作为一条
        for i, chunk in enumerate(chunks[:5]):
            all_items.append({
                "key": f"{filename}_段落{i + 1}",
                "value": chunk[:200],
                "selected": True,
            })

    if not all_items:
        return JSONResponse({"ok": False, "detail": "未能提取到知识点，请检查文件内容"}, status_code=400)

    # 临时存储到 .forge 目录
    forge_dir = Path(__file__).resolve().parent.parent.parent / ".forge"
    forge_dir.mkdir(exist_ok=True)
    tmp_file = forge_dir / "import_tmp.json"
    tmp_file.write_text(
        json.dumps({"filename": filename, "items": all_items}, ensure_ascii=False),
        encoding="utf-8",
    )

    return JSONResponse({"ok": True, "items": all_items, "filename": filename})


@router.post("/knowledge/import/confirm", response_class=JSONResponse)
async def knowledge_import_confirm(request: Request):
    """确认导入选中的知识点到 KnowledgeStore。"""
    body = await request.json()
    items: list[dict] = body.get("items", [])
    if not items:
        return JSONResponse({"ok": False, "detail": "没有选中的知识点"}, status_code=400)

    from agent.knowledge import knowledge_store
    added = 0
    for item in items:
        k = str(item.get("key", "doc_fact"))[:80]
        v = str(item.get("value", ""))
        if not v:
            continue
        try:
            knowledge_store.add_candidate(
                source="document",
                category="fact",
                key=k,
                value=v,
                extracted_by="llm",
                confidence=0.8,
            )
            added += 1
        except Exception as exc:
            logger.debug("Failed to add import candidate: %s", exc)

    # 清理临时文件
    try:
        tmp_file = Path(__file__).resolve().parent.parent.parent / ".forge" / "import_tmp.json"
        if tmp_file.exists():
            tmp_file.unlink()
    except Exception:
        pass

    return JSONResponse({"ok": True, "added": added})
