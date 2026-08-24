# Forge Golden Journey Acceptance Review — 2026-08-24

> Requirement: [`REQ-2026-08-24-007`](requirements-pool.md#req-2026-08-24-007完整问数旅程的物理链路与逐阶段视觉验收)  
> Code under test: NAS `9fca1ea`  
> Verdict: **Physical chain PASS / Trusted product outcome FAIL**  
> Scope: desktop Web only. Mobile evidence is explicitly non-gating per the user's latest direction.

## 1. Executive verdict

The same `TaskRun` completed the full path:

```text
Question
→ ExecutionPlan
→ SQL review
→ Human test approval
→ Read-only query
→ QueryResult
→ Analysis
→ Report
→ HTML / PDF / PPTX
```

All physical invariants passed: one approval, one execution, all four StageAttempts succeeded, immutable lineage remained continuous, the isolated SQLite datasource stayed mode `0400` without WAL/SHM, and HTML/PDF/PPTX were published.

The product journey nevertheless **fails acceptance**. A successful backend state currently permits a user-visible outcome that is not yet safe or satisfactory:

1. The real PDF leaks an internal `file:///home/...` path through Chrome's default footer.
2. The deterministic chart presents repeated category labels from raw rows as a category comparison, although the query returned 107 rows but only 10 visible category labels; the chart can mislead.
3. After the user focuses and clicks the report action at the bottom of a long Analysis card, the actual same-page flow leaves the main pane visually blank during report generation and after publication. A fresh reload shows the publication card, proving the Artifact exists but the main journey loses the user's scroll anchor.
4. Critical data-quality defects are described in prose, but the task still becomes `complete` and publishes business conclusions before the risk is made decision-blocking.

`completed` therefore proves workflow completion, not a trusted user outcome.

## 2. Test topology and safety

- NAS loopback-only isolated Forge/Pi/Web services.
- Current real model runtime via existing credential references; no Secret was read or echoed.
- Independent Task, Query, Audit, Memory, Model Control copy, Report and Artifact stores.
- Versioned SQLite test datasource copied into the isolated root and set to mode `0400`.
- Ephemeral test Principal: `org_default / team_default / web_admin`.
- Production authentication, services, database and stores were unchanged.
- One test SQL approval was authorized; no write SQL or production data access occurred.
- Isolated services were stopped after evidence collection; ephemeral channel/service keys were deleted.

Evidence root on NAS:

```text
~/services/forge-m4.1/e2e/golden-20260824T101129Z/
```

Local visual evidence:

```text
/tmp/forge-golden-journey-evidence/
```

## 3. Physical trace

### 3.1 Outcome

| Invariant | Result |
|---|---|
| Task reached `completed / report_complete` | PASS |
| Latest ExecutionPlan v9 has Query → Analysis → Report completed | PASS |
| SQL review and approval were hash-bound | PASS |
| Query execution count | PASS — 1 |
| Exact duplicate ChannelEvent replay | PASS — HTTP 200, no second execution |
| All StageAttempts succeeded | PASS — 4/4 |
| QueryResult → Analysis → Report lineage | PASS |
| Report HTML/PDF/PPTX | PASS — all ready |
| Isolated datasource remained read-only | PASS — mode `0400`, no WAL/SHM |
| Console / page errors | PASS — 0 |

### 3.2 Timing

| Stage | Duration |
|---|---:|
| Query prepare | 4.144s |
| Query execution | 0.220s |
| Business root-cause analysis | 183.265s |
| Data analysis report | 49.051s |
| Report HTML projection | 0.007s |
| PDF export | 2.484s |
| PPTX export | 1.063s |
| Task lifecycle | 349.028s |

Analysis consumed 52.5% of the full Task lifecycle and remained below the 240s deadline, but 183s is still a long user wait and must be judged primarily through the waiting experience.

### 3.3 Artifacts

- 9 immutable ExecutionPlan revisions.
- 1 QueryResult: 107 rows, 4 columns, not truncated.
- 1 Chart.
- 1 Analysis: 6 findings, 4 hypotheses, 5 limitations, 5 suggested queries.
- 1 RenderedOutput: 6 findings, 5 recommendations, 5 limitations, 5 next steps.
- 1 TechnicalReport, 1 ReportBundle, 1 Publication.
- PDF: 1,439,225 bytes; PPTX: 69,365 bytes.

## 4. Desktop journey review

| Checkpoint | Physical | UX verdict | Main observation |
|---|---|---|---|
| Empty Chat | PASS | PASS | Clear starting point and trusted-execution proposition. |
| Planning | PASS | P1 FAIL | Main card says only “processing”; elapsed/deadline and responsibility are confined to a dense right rail. Current task is temporarily absent from Recent Tasks. |
| SQL review | PASS | P1 FAIL | Read-only consequence is visible, but there is no “revise requirement/regenerate” path, business explanation, data range or scan-risk summary. SQL is horizontally scrollable, but lacks an expansion/copy affordance. |
| Query executing | PASS | P1 | Short stage, but the main progress card still lacks explicit stage position and expected outcome. |
| Query result | PASS | P1 FAIL | 107-row result is shown as raw English columns without units or business formatting; scientific/astronomical quantity values receive no immediate data-quality warning. |
| Analysis running | PASS | P1 FAIL | Real elapsed/deadline exists only in the right rail. The main pane does not state 2/3, that the task can continue after leaving, or what will be delivered next. |
| Analysis complete | PASS | P1 FAIL | Long text is structurally improved, but facts, inference and limitations remain expensive to scan. The “generate report” action is below a very long card and not visible in the first viewport. |
| Report running | PASS | **P0 FAIL** | After focusing/clicking the bottom action, the same-page main pane becomes blank while only the right rail advances. |
| Publication complete | PASS | **P0 FAIL** | The same-page screenshot remains blank and does not expose delivery links. Reloading the Task renders the publication card, so this is a scroll/focus containment defect rather than missing Artifact data. |
| Business report | PASS | **P0 FAIL** | The category chart is not a valid category comparison; data-quality risk is not decision-blocking and appears after business conclusions. |
| Technical report | PASS | P1 | Reproducible and readable, but long decision tables dominate and are not optimized for quick verification. |
| PDF | PASS | **P0 FAIL** | Internal filesystem path leaks in the default Chrome footer. Default date/title header and excessive first-page whitespace reduce professional quality. |
| PPTX | PASS | P1 FAIL | Cover summary ends mid-sentence; complete text exists later, but the cover itself appears broken and overloaded. |

## 5. P0 findings

### GJ-P0-01 — PDF exports leak an internal filesystem path

Actual PDF footer exposes the NAS account, service directory, temporary E2E root, report ID and revision through a `file:///home/.../index.html` URL. This violates the existing “no internal path” channel/report boundary.

Likely owner: `forge/reporting.py::_build_pdf` Chrome invocation. The target exporter smoke checked status/size but not rendered headers and therefore missed this defect.

Required outcome:

- Disable Chrome default PDF header/footer.
- Add a regression that inspects the actual target-generated PDF first page or extracted text for `file://`, `/home/`, report filesystem paths and browser date/title headers.

### GJ-P0-02 — Same-page report completion loses the main delivery card

The real journey's `report-running` and `publication-complete` screenshots show an empty main pane. A fresh page rendering the same persisted Presentation immediately shows the links.

A focused diagnostic reproduced the precondition: Analysis is a long card, the browser scrolls the outer page to reach the report action, then the card is replaced by a much shorter progress/publication card without restoring the correct scroll container/anchor. The task-flow event list also expands page height instead of remaining fully contained.

Required outcome:

- Keep Chat and Task Flow inside a viewport-bounded flex layout.
- Make feed and flow independently scrollable.
- When a card is replaced by a shorter state, scroll the actual owning container so the card header and primary action remain visible.
- Add a same-card “long Analysis → focus action → progress → publication” Playwright regression; a fresh reload is not an acceptable substitute.

### GJ-P0-03 — Deterministic ChartArtifact can misrepresent the QueryResult grain

`buildChartPayload` selects the first string dimension and numeric measures from sample rows without verifying dimension uniqueness, grain or required aggregation. The report renderer then plots only the first rows. In this journey, 107 grouped rows map to only 10 repeated visible category labels, producing a chart that looks like a category ranking while repeating category names.

Owner: `services/pi-orchestrator/src/report-artifacts.ts::buildChartPayload` and deterministic report projections.

Required outcome:

- A chart must declare and validate its grain.
- Repeated visible labels require a stable secondary key, deterministic aggregation, or chart suppression.
- Chart evidence rows and rendered rows must match.
- A critical data-quality finding must prevent a misleading chart from becoming a business publication.

## 6. P1 findings

1. **Decision-readiness gate**: critical quantity corruption and category-grain ambiguity are documented but Analysis/Report still become `complete`. Introduce an explicit `decision_readiness`/critical-quality gate or equivalent deterministic policy; do not infer it from prose.
2. **SQL review recovery**: add “revise requirement/regenerate SQL” without allowing direct post-approval SQL editing. Show a business summary, filters/time range, tables and bounded scan/risk facts.
3. **Result semantics**: business labels, units, thousands separators and anomaly markers are missing. Scientific values that violate declared business types need immediate warning before Analysis.
4. **Long-stage main progress**: surface stage position, elapsed time, deadline meaning, safe-to-leave/recovery message and expected next deliverable in the main card; keep the right rail supplementary.
5. **Action discoverability**: keep the primary next action visible after long Analysis, for example through a sticky card footer or compact completion summary.
6. **Report hierarchy**: place critical data-quality warnings before business conclusions; split Executive Summary into decision status, reference findings and required actions; avoid a single dense paragraph.
7. **Report navigation/data density**: add desktop section anchors and collapse low-value full detail by default; show Top-N plus anomaly rows.
8. **PPTX cover**: do not place a clipped summary fragment on the cover. Keep the cover concise and move the complete Executive Summary to the next slide.
9. **Recent Task timing**: insert the newly created Task immediately rather than waiting for the first terminal Presentation refresh.
10. **Realtime Flow language**: reduce duplicate generic events and replace internal “safety window” wording with a user-facing deadline explanation. Do not fabricate percentage or ETA.

## 7. Corrected interpretations from visual review

The visual model was intentionally strict, but three conclusions required engineering correction:

- The SQL is not physically truncated; the code block supports horizontal scrolling. The real defect is poor discoverability and missing review aids, not loss of SQL bytes.
- The blank publication screenshot is not a missing Publication Artifact. Persistence and a fresh render are correct; the defect is same-page scroll/focus containment.
- Percentage/ETA should not be invented. The product should expose real elapsed/deadline and stage position, not a fabricated progress percentage.

## 8. Automation limitations

The first browser runner stopped after query execution because its duplicate-approval probe used an invalid synthetic conversation ID. No second SQL was executed. The same persisted TaskRun was resumed in a second browser process and completed, incidentally validating cross-session recovery. Exact idempotency was then tested at the ChannelEvent boundary with the original deterministic event ID: HTTP 200, one approval event, one query-completed event.

This is sufficient for the product verdict, but the reusable runner must remove the malformed probe before it becomes a CI/acceptance asset.

## 9. Final decision

- H3 test execution: **completed**.
- Physical workflow: **PASS**.
- Desktop user journey: **FAIL**.
- Trusted business outcome: **FAIL**.
- Production rollout status: no new product code was deployed by H3; production services remained healthy.
- Next gate: P0 remediation must be separately accepted through `REQ-2026-08-24-008`. Do not start edge journeys until P0 is closed and this same Golden Journey passes without reload or evidence ambiguity.
