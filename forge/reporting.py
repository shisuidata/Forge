"""Immutable report publication and deterministic HTML/PDF/PPTX projection.

This service consumes already-approved Pi artifacts.  It never queries a data
source and never invokes an LLM.  SQLite stores metadata; large files live in a
mode-700 artifact directory.
"""
from __future__ import annotations

import hashlib
import html
import json
import os
import secrets
import shutil
import sqlite3
import subprocess
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

_FORBIDDEN = ("<think", "system prompt", "tool call", "chain-of-thought", "api_key", "password", "secret")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode()).hexdigest()


def _safe_text(value: Any, limit: int = 20_000) -> str:
    text = str(value or "").strip()[:limit]
    lowered = text.lower()
    if any(marker in lowered for marker in _FORBIDDEN):
        raise ValueError("report input contains forbidden reasoning, prompt, or secret material")
    return text


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp_name, path)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


def _svg_chart(chart: dict[str, Any], query: dict[str, Any]) -> str:
    columns = query.get("columns") or []
    rows = (query.get("rows") or [])[:12]
    dimension = chart.get("dimension")
    measures = chart.get("measures") or []
    if dimension not in columns or not measures or measures[0] not in columns:
        return ""
    di, mi = columns.index(dimension), columns.index(measures[0])
    points: list[tuple[str, float]] = []
    for row in rows:
        if not isinstance(row, list) or max(di, mi) >= len(row):
            continue
        try:
            value = float(row[mi])
        except (TypeError, ValueError):
            continue
        points.append((str(row[di]), value))
    if not points:
        return ""
    width, height, pad = 820, 320, 46
    max_value = max(abs(value) for _, value in points) or 1
    if chart.get("chart_type") == "line":
        step = (width - pad * 2) / max(1, len(points) - 1)
        coords = []
        labels = []
        for index, (label, value) in enumerate(points):
            x = pad + index * step
            y = height - pad - (max(0, value) / max_value) * (height - pad * 2)
            coords.append(f"{x:.1f},{y:.1f}")
            labels.append(f'<text x="{x:.1f}" y="{height-18}" text-anchor="middle">{html.escape(label[:12])}</text>')
        body = f'<polyline fill="none" stroke="#3d6b5d" stroke-width="4" points="{" ".join(coords)}"/>' + "".join(labels)
    else:
        gap = 8
        bar_width = max(12, (width - pad * 2) / len(points) - gap)
        chunks = []
        for index, (label, value) in enumerate(points):
            bar_height = (max(0, value) / max_value) * (height - pad * 2)
            x = pad + index * (bar_width + gap)
            y = height - pad - bar_height
            chunks.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width:.1f}" height="{bar_height:.1f}" rx="4" fill="#3d6b5d"/>')
            chunks.append(f'<text x="{x+bar_width/2:.1f}" y="{height-18}" text-anchor="middle">{html.escape(label[:12])}</text>')
        body = "".join(chunks)
    return f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(str(chart.get("alt_text") or "图表"))}"><line x1="{pad}" y1="{height-pad}" x2="{width-pad}" y2="{height-pad}" stroke="#c8d0cc"/>{body}</svg>'


def _business_html(source: dict[str, Any]) -> str:
    report = source["business_report"]
    analysis = source["analysis"]
    query = source["query_result"]
    charts = source.get("charts") or []
    method = analysis.get("method_summary") or {}
    findings = "".join(f"<li>{html.escape(_safe_text(item.get('statement')))}</li>" for item in analysis.get("findings") or [])
    limitations = "".join(f"<li>{html.escape(_safe_text(item))}</li>" for item in analysis.get("limitations") or [])
    steps = "".join(f"<li>{html.escape(_safe_text(item))}</li>" for item in method.get("approach_steps") or [])
    recommendations = "".join(
        f"<li><strong>{html.escape(_safe_text(item.get('action')))}</strong><br>{html.escape(_safe_text(item.get('rationale')))}</li>"
        for item in report.get("recommendations") or []
    )
    chart_html = "".join(
        f'<section class="chart"><h3>{html.escape(_safe_text(chart.get("title")))}</h3>{_svg_chart(chart, query)}</section>'
        for chart in charts
    )
    table_columns = query.get("columns") or []
    table_rows = (query.get("rows") or [])[:20]
    head = "".join(f"<th>{html.escape(str(column))}</th>" for column in table_columns)
    body = "".join("<tr>" + "".join(f"<td>{html.escape(str(value))}</td>" for value in row) + "</tr>" for row in table_rows)
    title = html.escape(_safe_text(report.get("title") or source.get("title") or "分析报告"))
    summary = html.escape(_safe_text(report.get("executive_summary") or analysis.get("summary")))
    report_id = html.escape(_safe_text(source.get("report_id"), 128))
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{title}</title><style>
:root{{--ink:#17201d;--muted:#66736e;--paper:#f4f1e8;--card:#fffefa;--accent:#3d6b5d}}*{{box-sizing:border-box}}body{{margin:0;background:var(--paper);color:var(--ink);font:16px/1.7 system-ui,-apple-system,sans-serif}}main{{max-width:1080px;margin:auto;padding:56px 24px 96px}}header{{border-left:8px solid var(--accent);padding:12px 24px;margin-bottom:32px}}h1{{font-size:clamp(32px,6vw,64px);line-height:1.05;margin:0 0 12px}}h2{{margin-top:42px;font-size:28px}}.lead{{font-size:20px;color:var(--muted)}}.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:18px}}section,.card{{background:var(--card);border:1px solid #d8ded9;border-radius:18px;padding:24px;margin:18px 0;box-shadow:0 8px 28px #23352e0d}}svg{{width:100%;height:auto}}svg text{{font-size:12px;fill:#66736e}}table{{width:100%;border-collapse:collapse;font-size:14px}}th,td{{text-align:left;border-bottom:1px solid #dde2df;padding:10px;white-space:nowrap}}.table-wrap{{overflow:auto}}.toolbar{{position:sticky;top:0;z-index:3;display:flex;gap:10px;flex-wrap:wrap;padding:12px 24px;background:#f4f1e8ee;backdrop-filter:blur(12px);border-bottom:1px solid #d8ded9}}.toolbar a,.toolbar button{{border:1px solid #aab8b1;background:#fffefa;color:#17201d;border-radius:999px;padding:8px 14px;text-decoration:none;cursor:pointer}}#share-result{{width:100%;font-size:13px;color:var(--muted);overflow-wrap:anywhere}}footer{{margin-top:52px;color:var(--muted);font-size:13px}}@media print{{body{{background:#fff}}.toolbar{{display:none}}main{{max-width:none;padding:24px}}section,.card{{box-shadow:none;break-inside:avoid}}}}
</style></head><body><nav class="toolbar"><a href="/reports/{report_id}/download/pdf">下载 PDF</a><a href="/reports/{report_id}/download/pptx">下载 PPTX</a><a href="/reports/{report_id}/technical">技术报告</a><button id="share">创建 7 天分享链接</button><span id="share-result"></span></nav><main><header><p>FORGE ANALYSIS REPORT</p><h1>{title}</h1><p class="lead">{summary}</p></header><div class="grid"><section><h2>分析思路</h2><p><strong>目标：</strong>{html.escape(_safe_text(method.get('objective')))}</p><p><strong>维度：</strong>{html.escape('、'.join(method.get('dimensions') or []))}</p><p><strong>基线：</strong>{html.escape(_safe_text(method.get('comparison_baseline')))}</p><ol>{steps}</ol></section><section><h2>关键结论</h2><ul>{findings}</ul></section></div>{chart_html}<section><h2>数据明细</h2><div class="table-wrap"><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div></section><div class="grid"><section><h2>建议</h2><ul>{recommendations}</ul></section><section><h2>限制</h2><ul>{limitations}</ul></section></div><footer>本报告固定到不可变数据与分析版本；导出文件与网页共享同一内容摘要。</footer></main><script>document.getElementById('share').onclick=async()=>{{const expires_at=new Date(Date.now()+7*86400000).toISOString();const r=await fetch('/api/reports/{report_id}/shares',{{method:'POST',headers:{{'content-type':'application/json'}},body:JSON.stringify({{expires_at}})}});const out=document.getElementById('share-result');if(!r.ok){{out.textContent='创建失败，请确认已登录且有权限';return;}}const x=await r.json();out.textContent=x.exchange_url;try{{await navigator.clipboard.writeText(x.exchange_url);out.textContent='分享链接已复制：'+x.exchange_url;}}catch{{}}}};</script></body></html>"""


def _technical_html(source: dict[str, Any]) -> str:
    tech = source["technical_report"]
    decision_rows = "".join(
        f"<tr><td>{html.escape(_safe_text(item.get('stage')))}</td><td>{html.escape(_safe_text(item.get('decision')))}</td><td>{html.escape(_safe_text(item.get('rationale')))}</td></tr>"
        for item in tech.get("decision_log") or []
    )
    lineage_rows = "".join(f"<tr><th>{html.escape(str(key))}</th><td>{html.escape(str(value))}</td></tr>" for key, value in (tech.get("lineage") or {}).items())
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><title>{html.escape(_safe_text(tech.get('title')))}</title><style>body{{max-width:1000px;margin:40px auto;padding:0 20px;font:14px/1.6 ui-monospace,monospace;color:#18201d}}h1,h2{{font-family:system-ui}}pre{{white-space:pre-wrap;background:#f2f4f3;padding:18px;border-radius:10px}}table{{width:100%;border-collapse:collapse}}th,td{{border:1px solid #ccd4d0;padding:8px;text-align:left;vertical-align:top}}</style></head><body><h1>{html.escape(_safe_text(tech.get('title')))}</h1><p>该文档记录可复现的结构化决策、SQL、审批、执行和版本 lineage；不包含模型 hidden chain-of-thought、Prompt 或 Secret。</p><h2>SQL</h2><pre>{html.escape(_safe_text(tech.get('sql'), 100_000))}</pre><h2>审批与执行</h2><pre>{html.escape(_canonical({'approval': tech.get('approval'), 'execution': tech.get('execution')}))}</pre><h2>Decision Log</h2><table><tr><th>Stage</th><th>Decision</th><th>Rationale</th></tr>{decision_rows}</table><h2>Lineage</h2><table>{lineage_rows}</table></body></html>"""


class ReportStore:
    def __init__(self, db_path: str, artifact_dir: str):
        self.db_path = Path(db_path)
        self.artifact_dir = Path(artifact_dir)
        self.db_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.artifact_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.artifact_dir, 0o700)
        self._lock = threading.RLock()
        self._init()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init(self) -> None:
        with self._connect() as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS reports (
              report_id TEXT PRIMARY KEY, task_run_id TEXT NOT NULL UNIQUE,
              org_id TEXT NOT NULL, team_id TEXT NOT NULL, user_id TEXT NOT NULL,
              revision INTEGER NOT NULL, bundle_hash TEXT NOT NULL, title TEXT NOT NULL,
              status TEXT NOT NULL, pdf_status TEXT NOT NULL, pptx_status TEXT NOT NULL,
              error TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS report_attempts (
              attempt_id TEXT PRIMARY KEY, report_id TEXT NOT NULL, stage TEXT NOT NULL,
              status TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT, error TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_report_attempts_report ON report_attempts(report_id, started_at);
            CREATE TABLE IF NOT EXISTS report_shares (
              share_id TEXT PRIMARY KEY, report_id TEXT NOT NULL, token_hash TEXT NOT NULL UNIQUE,
              scope TEXT NOT NULL CHECK(scope IN ('business')),
              expires_at TEXT NOT NULL, revoked_at TEXT, created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_report_shares_report ON report_shares(report_id, expires_at);
            CREATE TABLE IF NOT EXISTS report_download_audit (
              audit_id TEXT PRIMARY KEY, report_id TEXT NOT NULL, format TEXT NOT NULL,
              actor TEXT NOT NULL, created_at TEXT NOT NULL
            );
            """)
            now = _now()
            conn.execute(
                "UPDATE report_attempts SET status='interrupted', finished_at=?, error='process restarted; not replayed' WHERE status='running'",
                (now,),
            )
            conn.execute(
                "UPDATE reports SET status='failed', error='publication interrupted; explicit retry required', updated_at=? WHERE status='publishing'",
                (now,),
            )
        os.chmod(self.db_path, 0o600)

    def create(self, source: dict[str, Any]) -> dict[str, Any]:
        allowed = {
            "task_run_id", "org_id", "team_id", "user_id", "report_id", "revision",
            "bundle_hash", "title", "business_report", "analysis", "query_result", "charts",
            "technical_report",
        }
        if set(source) != allowed:
            raise ValueError("report input fields do not match the fixed contract")
        for key in ("task_run_id", "org_id", "team_id", "user_id", "report_id", "bundle_hash", "title"):
            if not isinstance(source.get(key), str) or not source[key]:
                raise ValueError(f"{key} is required")
        if not str(source["bundle_hash"]).startswith("sha256:") or len(source["bundle_hash"]) != 71:
            raise ValueError("bundle_hash is invalid")
        if not all(isinstance(source.get(key), dict) for key in ("business_report", "analysis", "query_result", "technical_report")):
            raise ValueError("report artifacts must be objects")
        if not isinstance(source.get("charts"), list) or len(source["charts"]) > 12:
            raise ValueError("charts must be a bounded list")
        query = source["query_result"]
        if not isinstance(query.get("columns"), list) or len(query["columns"]) > 200 or not isinstance(query.get("rows"), list) or len(query["rows"]) > 200:
            raise ValueError("query result projection exceeds report bounds")
        serialized = _canonical(source)
        if len(serialized.encode()) > 2_000_000:
            raise ValueError("report input exceeds 2 MB")
        _safe_text(serialized, 2_000_000)
        now = _now()
        with self._lock, self._connect() as conn:
            existing = conn.execute("SELECT * FROM reports WHERE task_run_id = ?", (source["task_run_id"],)).fetchone()
            if existing:
                if existing["bundle_hash"] != source["bundle_hash"]:
                    raise ValueError("task already published with a different bundle")
                return self._row(existing)
            conn.execute(
                "INSERT INTO reports VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'publishing', 'pending', 'pending', NULL, ?, ?)",
                (source["report_id"], source["task_run_id"], source["org_id"], source["team_id"], source["user_id"],
                 int(source.get("revision") or 1), source["bundle_hash"], source["title"], now, now),
            )
            report_dir = self.artifact_dir / source["report_id"] / f"v{int(source.get('revision') or 1)}"
            _atomic_write(report_dir / "source.json", _canonical(source).encode())
        return self.get(source["report_id"])

    def _attempt(self, report_id: str, stage: str, operation):
        attempt_id, started = f"rpa_{uuid4().hex}", _now()
        with self._connect() as conn:
            conn.execute("INSERT INTO report_attempts VALUES (?, ?, ?, 'running', ?, NULL, NULL)",
                         (attempt_id, report_id, stage, started))
        try:
            result = operation()
            status, error = "succeeded", None
            return result
        except Exception as exc:
            status, error = "failed", str(exc)[:1000]
            raise
        finally:
            with self._connect() as conn:
                conn.execute("UPDATE report_attempts SET status=?, finished_at=?, error=? WHERE attempt_id=?",
                             (status, _now(), error, attempt_id))

    def build(self, report_id: str) -> dict[str, Any]:
        report = self.get(report_id)
        report_dir = self.artifact_dir / report_id / f"v{report['revision']}"
        source = json.loads((report_dir / "source.json").read_text())
        try:
            def html_job():
                _atomic_write(report_dir / "index.html", _business_html(source).encode())
                _atomic_write(report_dir / "technical.html", _technical_html(source).encode())
            self._attempt(report_id, "html", html_job)
            status, error = "published", None
        except Exception as exc:
            status, error = "failed", str(exc)[:1000]
        pdf_status = self._run_export(report_id, "pdf", report_dir, source) if status == "published" else "failed"
        pptx_status = self._run_export(report_id, "pptx", report_dir, source) if status == "published" else "failed"
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE reports SET status=?, pdf_status=?, pptx_status=?, error=?, updated_at=? WHERE report_id=?",
                (status, pdf_status, pptx_status, error, _now(), report_id),
            )
        return self.get(report_id)

    def _run_export(self, report_id: str, fmt: str, report_dir: Path, source: dict[str, Any]) -> str:
        try:
            def export_job():
                result = self._build_pdf(report_dir) if fmt == "pdf" else self._build_pptx(report_dir, source)
                if result != "ready":
                    raise RuntimeError(f"{fmt} exporter is unavailable or failed")
                return result
            self._attempt(report_id, fmt, export_job)
            return "ready"
        except Exception:
            return "failed"

    def retry_export(self, report_id: str, fmt: str) -> dict[str, Any]:
        if fmt not in {"pdf", "pptx"}:
            raise ValueError("unsupported export format")
        report = self.get(report_id)
        if report["status"] != "published":
            raise ValueError("report HTML is not published")
        report_dir = self.artifact_dir / report_id / f"v{report['revision']}"
        source = json.loads((report_dir / "source.json").read_text())
        result = self._run_export(report_id, fmt, report_dir, source)
        with self._connect() as conn:
            conn.execute(f"UPDATE reports SET {fmt}_status=?, updated_at=? WHERE report_id=?", (result, _now(), report_id))
        return self.get(report_id)

    def _build_pdf(self, report_dir: Path) -> str:
        chrome = next((path for path in (shutil.which("google-chrome"), shutil.which("chromium"), shutil.which("chromium-browser")) if path), None)
        if chrome is None:
            return "failed"
        target = (report_dir / "report.pdf").resolve()
        command = [chrome, "--headless", "--disable-gpu", "--no-sandbox", f"--print-to-pdf={target}", (report_dir / "index.html").resolve().as_uri()]
        result = subprocess.run(command, capture_output=True, timeout=90, check=False)
        if result.returncode != 0 or not target.exists():
            return "failed"
        os.chmod(target, 0o600)
        return "ready"

    def _build_pptx(self, report_dir: Path, source: dict[str, Any]) -> str:
        try:
            from pptx import Presentation
            from pptx.dml.color import RGBColor
            from pptx.enum.shapes import MSO_SHAPE
            from pptx.util import Inches, Pt
        except ImportError:
            return "failed"
        prs = Presentation()
        prs.slide_width, prs.slide_height = Inches(13.333), Inches(7.5)
        title_slide = prs.slides.add_slide(prs.slide_layouts[0])
        title_slide.shapes.title.text = _safe_text(source.get("title"))
        title_slide.placeholders[1].text = _safe_text(source["business_report"].get("executive_summary"))
        analysis = source["analysis"]
        slide = prs.slides.add_slide(prs.slide_layouts[1])
        slide.shapes.title.text = "分析思路"
        frame = slide.placeholders[1].text_frame
        frame.clear()
        method = analysis.get("method_summary") or {}
        for index, text in enumerate([method.get("objective"), *(method.get("approach_steps") or [])]):
            paragraph = frame.paragraphs[0] if index == 0 else frame.add_paragraph()
            paragraph.text = _safe_text(text)
            paragraph.font.size = Pt(22)
        slide = prs.slides.add_slide(prs.slide_layouts[1])
        slide.shapes.title.text = "关键结论"
        frame = slide.placeholders[1].text_frame
        frame.clear()
        for index, finding in enumerate(analysis.get("findings") or []):
            paragraph = frame.paragraphs[0] if index == 0 else frame.add_paragraph()
            paragraph.text = _safe_text(finding.get("statement"))
            paragraph.font.size = Pt(22)
        query = source.get("query_result") or {}
        columns, rows = query.get("columns") or [], (query.get("rows") or [])[:10]
        for chart in source.get("charts") or []:
            dimension, measures = chart.get("dimension"), chart.get("measures") or []
            if dimension not in columns or not measures or measures[0] not in columns:
                continue
            di, mi = columns.index(dimension), columns.index(measures[0])
            points = []
            for row in rows:
                try:
                    points.append((str(row[di]), max(0.0, float(row[mi]))))
                except (TypeError, ValueError, IndexError):
                    pass
            if not points:
                continue
            chart_slide = prs.slides.add_slide(prs.slide_layouts[5])
            chart_slide.shapes.title.text = _safe_text(chart.get("title"))
            maximum = max(value for _, value in points) or 1
            available = 11.4
            bar_width = min(0.8, available / max(1, len(points)) * 0.62)
            gap = available / max(1, len(points))
            for index, (label, value) in enumerate(points):
                height = 4.4 * value / maximum
                left = 0.9 + index * gap
                top = 6.4 - height
                shape = chart_slide.shapes.add_shape(
                    MSO_SHAPE.RECTANGLE, Inches(left), Inches(top), Inches(bar_width), Inches(height)
                )
                shape.fill.solid(); shape.fill.fore_color.rgb = RGBColor(61, 107, 93)
                shape.line.fill.background()
                box = chart_slide.shapes.add_textbox(Inches(left - 0.1), Inches(6.45), Inches(bar_width + 0.2), Inches(0.45))
                box.text_frame.text = label[:12]
                box.text_frame.paragraphs[0].font.size = Pt(10)
        target = report_dir / "report.pptx"
        prs.save(target)
        os.chmod(target, 0o600)
        return "ready"

    def get(self, report_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM reports WHERE report_id = ?", (report_id,)).fetchone()
        if row is None:
            raise KeyError(report_id)
        return self._row(row)

    def _row(self, row: sqlite3.Row) -> dict[str, Any]:
        report_id, revision = row["report_id"], row["revision"]
        return {
            **dict(row),
            "internal_url": f"/reports/{report_id}",
            "technical_url": f"/reports/{report_id}/technical",
            "pdf_url": f"/reports/{report_id}/download/pdf" if row["pdf_status"] == "ready" else None,
            "pptx_url": f"/reports/{report_id}/download/pptx" if row["pptx_status"] == "ready" else None,
            "revision": revision,
        }

    def create_share(self, report_id: str, expires_at: str) -> dict[str, str]:
        self.get(report_id)
        try:
            expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("expires_at must be an RFC 3339 date-time") from exc
        if expiry.tzinfo is None:
            raise ValueError("expires_at must include a timezone")
        now = datetime.now(timezone.utc)
        if expiry <= now or (expiry - now).total_seconds() > 30 * 24 * 3600:
            raise ValueError("share expiry must be within the next 30 days")
        token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        share_id = f"rps_{uuid4().hex}"
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO report_shares VALUES (?, ?, ?, 'business', ?, NULL, ?)",
                (share_id, report_id, token_hash, expiry.isoformat(), _now()),
            )
        return {"share_id": share_id, "report_id": report_id, "token": token, "expires_at": expiry.isoformat()}

    def resolve_share(
        self, token: str, report_id: str | None = None, share_id: str | None = None
    ) -> dict[str, str]:
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM report_shares WHERE token_hash = ? AND revoked_at IS NULL",
                (token_hash,),
            ).fetchone()
        if row is None or (report_id is not None and row["report_id"] != report_id) or (
            share_id is not None and row["share_id"] != share_id
        ):
            raise KeyError("share")
        expiry = datetime.fromisoformat(row["expires_at"])
        if expiry <= datetime.now(timezone.utc):
            raise KeyError("share")
        return dict(row)

    def revoke_share(self, report_id: str, share_id: str) -> None:
        with self._connect() as conn:
            result = conn.execute(
                "UPDATE report_shares SET revoked_at=? WHERE report_id=? AND share_id=? AND revoked_at IS NULL",
                (_now(), report_id, share_id),
            )
        if result.rowcount != 1:
            raise KeyError(share_id)

    def file(self, report_id: str, name: str) -> Path:
        report = self.get(report_id)
        path = self.artifact_dir / report_id / f"v{report['revision']}" / name
        if not path.is_file():
            raise KeyError(name)
        return path

    def audit_download(self, report_id: str, fmt: str, actor: str) -> None:
        with self._connect() as conn:
            conn.execute("INSERT INTO report_download_audit VALUES (?, ?, ?, ?, ?)",
                         (f"rda_{uuid4().hex}", report_id, fmt, actor[:128], _now()))
