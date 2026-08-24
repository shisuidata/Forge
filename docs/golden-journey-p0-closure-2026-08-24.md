# Golden Journey P0 Closure — 2026-08-24

> Requirement: [`REQ-2026-08-24-008`](requirements-pool.md#req-2026-08-24-008关闭-golden-journey-的-p0-可信交付缺陷)  
> Implementation: `b5e4884`  
> Verdict: **H4 PASS — three declared P0 findings closed**  
> Scope: desktop only; P1 and mobile are not part of this verdict.

## 1. Closure summary

| P0 | Deterministic change | Target verification | Verdict |
|---|---|---|---|
| PDF internal-path leak | Chrome exporter uses `--no-pdf-header-footer` | NAS Chrome 146 real PDF; `pdftotext` negative scan | PASS |
| Same-page Report/Publication blank | Viewport-bounded Chat shell, independent feed/flow scrolling, explicit task-card reveal | Long Analysis action focus/click → Report running → Publication, no reload | PASS |
| Misleading repeated-label Chart | Chart builder requires a unique visible string grain and exact 10-row evidence projection; Report Renderer independently suppresses unsafe legacy Chart input | 107 rows / repeated category labels creates 0 ChartArtifact and 0 report chart | PASS |

The chart result is intentionally fail-closed. It does not claim the current report has good visualization; it only proves the platform no longer publishes the known misleading chart. Rich, modern, annotated multi-chart storytelling is a separate product requirement, [`REQ-2026-08-24-009`](requirements-pool.md#req-2026-08-24-009专业报告的多图叙事现代图表与证据绑定交互).

## 2. Automated verification

Local:

- Python: `553 passed / 24 skipped`.
- Pi Orchestrator: `96 passed`.
- TypeScript typecheck: PASS.
- npm audit: `0 vulnerabilities`.
- Targeted reporting/Web tests: `10 passed`.
- Desktop Playwright regression with 80 flow rows: body remained 1000px, Chat shell 934.39px, feed and flow scrolled independently; Report running and Publication cards remained visible; 0 console/page errors.

Code review found no remaining blocker in the H4 diff. LSP diagnostics could not run because this workspace has no configured Python/TypeScript LSP route; compiler/typecheck/tests were used instead.

## 3. Real NAS exporter proof

An isolated ReportStore generated a new report with the actual NAS target exporter:

- Google Chrome `146.0.7680.164`.
- PDF status `ready`, 429,938 bytes, 3 pages.
- `pdftotext -layout` found no `file://`, `/home/`, `forge-m4.1` or `index.html`.
- Browser default date header was absent.

This test validates output content, not only command flags or file size. Existing published Report revisions were not rewritten.

## 4. Re-run of the same Golden Journey

Question:

```text
统计不同品类的销售额，分析主要差异，并生成完整报告。
```

Isolation:

- NAS loopback Forge `127.0.0.1:18101` and Pi `127.0.0.1:14410`.
- Existing real model credential references; no Secret read or echoed.
- Independent Task/Query/Report/Audit/Memory stores.
- Versioned datasource copy at mode `0400`.
- Production authentication, Store and database unchanged.

Result:

| Stage | Duration |
|---|---:|
| Query prepare | 3.548s |
| Query execution | 0.202s |
| Analysis | 180.976s |
| Report | 74.322s |
| Full Task | 262.399s |

Physical invariants:

- Task `completed / report_complete`.
- 4/4 StageAttempts succeeded.
- One `query.approval_submitted`, one `query.completed`.
- Exact duplicate Web message returned HTTP 200 and the same TaskRun; QueryRun count remained 1.
- QueryResult: 107 rows, not truncated, 31ms execution.
- Report published; PDF 1,510,087 bytes and PPTX 69,646 bytes ready.
- Datasource remained mode `0400` with no WAL/SHM.
- 0 ChartArtifact because the visible category labels did not identify the 107-row grain.
- Browser and Report page: 0 console/page errors.
- Same-page Report running and Publication links were visible without refresh.
- Golden Journey PDF content scan found no internal path.

The browser runner itself had one harness-only error after product completion: it concatenated an already-absolute Report URL with the base URL. The completed Task, same-page screenshots and persistence were unaffected. A bounded resume step corrected URL handling, opened the report, downloaded PDF/PPTX and recorded `chart_count=0`. This is not counted as a product failure, but the reusable runner must use URL joining before being promoted to CI.

## 5. Deployment and cleanup

- NAS source fast-forwarded from `9fca1ea` to `b5e4884` by Git bundle; no GitHub push.
- Backup: `~/services/forge-m4.1/backups/h4-p0-20260824T110913Z/`.
- Golden Journey evidence: `~/services/forge-m4.1/e2e/golden-h4-20260824T111426Z/`.
- Local visual evidence: `/tmp/forge-h4-golden-evidence/`.
- Isolated services and SSH tunnel stopped; ephemeral override files and unit files removed.
- Production Forge/Pi remained active with health/readiness `ok`; production worktree clean.

## 6. Remaining risk and next gate

H4 closes only the three declared P0 defects. It does **not** resolve:

- report template repetition and weak information hierarchy;
- lack of useful multi-chart storytelling, modern interactions and evidence-bound annotations;
- dense Executive Summary and PDF whitespace;
- PPTX cover summary truncation;
- decision-readiness for critical data quality;
- SQL review, result semantics and long-stage progress P1 findings.

These remain explicit requirements, not hidden acceptance debt. The next recommended gate is H5 `ChartArtifact v2 + two real fixtures + cross-media visual candidates`; no H5 code begins until user confirmation.
