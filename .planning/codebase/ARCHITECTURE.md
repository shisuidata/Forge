<!-- refreshed: 2026-08-25 -->
# Architecture

**Analysis Date:** 2026-08-25

## System Overview

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│ Channels and product presentation                                           │
│ Web/FastAPI `main.py`, `web/` · Feishu/DingTalk adapters `web/*_pi.py`      │
└───────────────────────────────────┬──────────────────────────────────────────┘
                                    │ authenticated ChannelEvent / Product API
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ Pi control plane `services/pi-orchestrator/src/`                            │
│ TaskRun · ExecutionPlan · StageAttempt · Artifact · Product Projection      │
└───────────────────────────────────┬──────────────────────────────────────────┘
                                    │ authenticated bounded HTTP ports
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ Forge trusted execution plane                                               │
│ `web/routes/query_runs.py` → `agent/agent.py` → `forge/assurance.py`        │
│ → `forge/compiler.py` / `forge/executor.py`                                 │
└──────────────────────┬───────────────────────────────┬───────────────────────┘
                       │                               │
                       ▼                               ▼
┌──────────────────────────────────┐   ┌───────────────────────────────────────┐
│ Registry and semantic truth      │   │ Durable evidence and execution state │
│ `registry/`, Registry JSON/YAML  │   │ SQLite stores + report artifacts     │
└──────────────────────────────────┘   └───────────────────────────────────────┘
```

Forge uses a split control-plane/execution-plane architecture. Pi is the sole task orchestrator and source of Task/Stage/Artifact workflow state in `services/pi-orchestrator/src/application.ts`; Forge retains independent validation, refusal, approval checking, read-only database execution, and QueryRun audit state in `forge/`. Channels in `web/` map identity and project state but do not own a second workflow.

The repository also contains a public documentation site in `website/`; it is a separate Astro content application and is not on the runtime request path described above.

## Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| FastAPI application | Process lifecycle, route mounting, static assets, readiness, and Feishu webhook selection | `main.py` |
| Web channel and legacy admin shell | Authenticated pages, Pi forwarding, bounded task-flow projection, and legacy rollback-only APIs | `web/router.py` |
| Product BFF | Stable authenticated read models for conversations, tasks, reports, workspace, and Registry summary | `web/routes/product.py` |
| Channel adapters | Convert Web, Feishu, and DingTalk events/actions to shared Pi `ChannelEvent` contracts and render channel presentations | `web/pi_channel.py`, `web/feishu_pi.py`, `web/dingtalk_pi.py` |
| Pi HTTP boundary | Authenticate channel/admin callers, validate bounded requests, and expose Task/Projection/Action endpoints | `services/pi-orchestrator/src/server.ts` |
| Pi application service | Own TaskRun transitions, execution plans, stage attempts, artifacts, idempotency, Skill dispatch, and calls to Forge | `services/pi-orchestrator/src/application.ts` |
| Pi state adapter | Persist tasks, events, channel claims, attempts, artifacts, and team Skill policy transactionally in SQLite | `services/pi-orchestrator/src/sqlite-store.ts` |
| Product projection service | Derive bounded, validated, redacted product contracts from Pi truth stores | `services/pi-orchestrator/src/product-projection-builder.ts` |
| Restricted Pi runtime | Load one allowlisted Skill per stage, disable built-in tools, and expose only typed Artifact/Forge tools | `services/pi-orchestrator/src/runtime.ts`, `services/pi-orchestrator/src/skills.ts`, `services/pi-orchestrator/src/skill-executor.ts` |
| Forge QueryRun API | Present the authenticated internal create/review/approve/cancel/result boundary used by Pi | `web/routes/query_runs.py` |
| Query planner adapter | Use the active model binding to produce candidate Forge JSON, retry bounded assurance failures, and stop at review | `agent/agent.py` |
| Assurance pipeline | Apply Contract, Registry/ACL, relationship, convention, compiler, and read-only SQL gates | `forge/assurance.py` |
| Deterministic compiler | Normalize and validate Forge JSON, then compile supported DSL operations to dialect-specific SQL | `forge/compiler.py`, `forge/schema.json` |
| Governed executor | Validate read-only SQL, apply timeout/row caps, execute through SQLAlchemy, and bound public errors | `forge/executor.py` |
| QueryRun store | Bind question, SQL hash, assurance hash/revisions, approval identity, lease, execution, and result in durable state | `forge/query_runs.py` |
| Registry core | Introspect sources, maintain canonical schema drafts/revisions, relationships, semantic rules, and deterministic projections | `registry/sync.py`, `registry/studio.py`, `registry/relationships.py`, `registry/validator.py` |
| Context retrieval | Build bounded Registry/semantic context and schema retrieval evidence for model planning | `forge/context.py`, `forge/retriever.py`, `agent/llm.py` |
| Report store | Publish immutable report revisions and render deterministic HTML/PDF/PPTX projections | `forge/reporting.py`, `web/routes/reports.py` |
| Cross-language contracts | Keep TypeScript product/governance schemas and Python semantic validators aligned through shared fixtures | `services/pi-orchestrator/src/product-projections.ts`, `agent/contracts/product_projection_semantics.py`, `agent/contracts/` |

## Pattern Overview

**Overall:** Modular monolith plus a sidecar-style Pi orchestrator, separated by explicit authenticated HTTP ports and versioned contracts.

**Key Characteristics:**
- Use orchestration/execution separation: workflow decisions live in `services/pi-orchestrator/src/application.ts`; database decisions and execution live in `forge/query_runs.py`, `forge/assurance.py`, and `forge/executor.py`.
- Use event-and-artifact state rather than free-text coupling: `services/pi-orchestrator/src/task-events.ts`, `services/pi-orchestrator/src/artifacts.ts`, and `services/pi-orchestrator/src/stage-attempts.ts` are the handoff model.
- Use ports/adapters around persistence and Forge calls: interfaces in `services/pi-orchestrator/src/task-store.ts`, `services/pi-orchestrator/src/application.ts`, and `services/pi-orchestrator/src/forge/query-run-client.ts` permit in-memory tests and SQLite/HTTP production adapters.
- Use fail-closed, hash-bound approval: `forge/query_runs.py` rechecks SQL hash, assurance hash, Registry revision, assurance/policy revisions, approver identity, expiration, execution enablement, and read-only-account confirmation.
- Use derived read models: `services/pi-orchestrator/src/product-projection-builder.ts` and `web/routes/product.py` project product UI contracts without creating new task truth stores.
- Use canonical Registry inputs with deterministic views: `registry/studio.py` creates draft/revision, diff, DDL, and ER projections; runtime assurance reads Registry state through `forge/assurance.py`.

## Layers

**Channel and Presentation Layer:**
- Purpose: Authenticate users, normalize channel-specific input, forward events/actions, and render bounded product/channel projections.
- Location: `main.py`, `web/router.py`, `web/routes/product.py`, `web/pi_channel.py`, `web/feishu_pi.py`, `web/dingtalk_pi.py`, `web/templates/`, `web/static/product/`.
- Contains: FastAPI routers, Jinja templates, browser assets, channel card rendering, and Pi HTTP clients.
- Depends on: `config.py`, Pi APIs through `web/pi_client.py`/`web/pi_channel.py`, report read models in `forge/reporting.py`, and Python contract validation in `agent/contracts/`.
- Used by: Browser users, external API callers, Feishu, and DingTalk.

**Pi Control Plane:**
- Purpose: Create and advance TaskRuns, route intent, generate execution plans, isolate Skills, wait for approval/input, and persist orchestration evidence.
- Location: `services/pi-orchestrator/src/`.
- Contains: HTTP server, application service, domain contracts, store interfaces/adapters, channel identity/event handling, Skill runtime, product projection, and Forge clients.
- Depends on: `@earendil-works/pi-coding-agent`, local SQLite via `node:sqlite`, external Skill files resolved by `services/pi-orchestrator/src/config.ts`, and Forge internal HTTP APIs.
- Used by: Web/Feishu/DingTalk channel adapters and Product BFF reads.

**Forge Trusted Execution Layer:**
- Purpose: Accept bounded planning requests, validate candidate Forge JSON, compile reviewable SQL, verify approval, and execute only read-only statements.
- Location: `forge/`, `agent/agent.py`, `web/routes/query_runs.py`.
- Contains: QueryRun lifecycle, model-backed query preparation, normalization, compiler, lint/assurance gates, execution, readiness, context, cache, chart, and report generation.
- Depends on: Registry inputs in `registry/`, global deployment configuration in `config.py`, SQLAlchemy data source connections, and model bindings in `agent/model_config.py`/`agent/llm.py`.
- Used by: Pi through `services/pi-orchestrator/src/forge/query-run-client.ts`; explicit prepare-only API callers through `web/router.py`.

**Registry and Semantic Layer:**
- Purpose: Own structural schema, relationships, metrics, disambiguation, conventions, business context, drafts, revisions, and validation.
- Location: `registry/` and configured Registry files referenced by `config.py`.
- Contains: Database introspection, canonical schema migration, deterministic diff/projections, metric validation, staging promotion, and relationship loading.
- Depends on: SQLAlchemy for introspection and JSON Schema/YAML for contracts and semantic assets.
- Used by: `forge/assurance.py`, `forge/retriever.py`, `agent/prompts.py`, Registry Studio routes in `web/routes/registry_studio.py`, and admin views in `web/router.py`.

**Persistence and Evidence Layer:**
- Purpose: Persist workflow, QueryRun, report, memory, audit, feedback, model-control, and Registry revision state under separate ownership.
- Location: `services/pi-orchestrator/src/sqlite-store.ts`, `forge/query_runs.py`, `forge/reporting.py`, `agent/audit.py`, `agent/memory/`, `agent/model_control.py`, `registry/studio.py`.
- Contains: SQLite schemas, filesystem artifacts, immutable records/revisions, leases, and reconciliation logic.
- Depends on: Deployment paths supplied by `services/pi-orchestrator/src/config.ts` and `config.py`.
- Used by: Their owning application services only; consumers use typed/domain APIs rather than sharing tables directly.

**Public Documentation Layer:**
- Purpose: Publish product, concept, guide, course, and reference documentation independently of the runtime.
- Location: `website/src/content/docs/`, `website/src/styles/`, `website/src/assets/`.
- Contains: Astro/Starlight content and static assets.
- Depends on: `website/astro.config.mjs`, `website/package.json`, and `website/src/content.config.ts`.
- Used by: Static site builds; it does not participate in `main.py` or Pi execution.

## Data Flow

### Primary Trusted Query Path

1. A Web/Feishu/DingTalk message becomes a shared authenticated `ChannelEvent` at `web/router.py:908`, `web/feishu_pi.py`, or `web/dingtalk_pi.py`, then reaches `POST /v1/channel-events` in `services/pi-orchestrator/src/server.ts:235`.
2. `OrchestratorApplication.ingestChannelMessage` claims the event idempotently, creates a TaskRun and `execution_plan` Artifact, and routes query intent in `services/pi-orchestrator/src/application.ts:414`.
3. `OrchestratorApplication.prepareQuery` starts a `query_prepare` StageAttempt and calls `ForgeQueryRunClient.createQueryRun` in `services/pi-orchestrator/src/application.ts:1030` and `services/pi-orchestrator/src/forge/query-run-client.ts`.
4. Forge receives the service-authenticated request at `web/routes/query_runs.py:120`; `create_query_run` records idempotency and invokes prepare-only planning in `forge/query_runs.py:178`.
5. `_prepare_query` gets the active model binding, supplies Registry-derived context, accepts only the `generate_forge_query` tool contract, and sends candidate DSL through `assure_query` in `agent/agent.py:110`.
6. `assure_query` validates the query contract/Registry/ACL/conventions, calls deterministic compilation, and runs SQL safety checks in `forge/assurance.py:56`, `forge/compiler.py:63`, and `forge/executor.py:95`.
7. Forge persists the candidate, SQL, SQL/assurance hashes, revisions, Registry version, and review expiry as a `needs_review` QueryRun in `forge/query_runs.py:241`; Pi records a `query.review_requested` event and transitions to `waiting_for_query_approval` in `services/pi-orchestrator/src/application.ts:1097`.
8. A channel action returns the exact `query_run_id`, `sql_hash`, and `assurance_report_hash`; Pi checks it against the Task event at `services/pi-orchestrator/src/application.ts:1149` and forwards approval to `web/routes/query_runs.py:151`.
9. Forge atomically claims execution, revalidates hashes, identity, expiry, Registry and policy revisions, and deployment gates in `forge/query_runs.py:299`; execution runs off the async loop through `forge/executor.py:193` and `forge/query_runs.py:436`.
10. Forge persists result metadata; Pi creates immutable `query_result` and optional `chart` Artifacts, advances the plan, and transitions to analysis or completion in `services/pi-orchestrator/src/application.ts:1254`.

### Product Projection Read Path

1. Browser pages under `web/templates/product_*.html` call authenticated `/api/product/*` endpoints defined in `web/routes/product.py:410`.
2. `web/routes/product.py` resolves one configured org/team/user scope and calls Pi conversation/task projection endpoints through `web/pi_client.py`; reports and Registry summaries are combined only as bounded read models.
3. Pi derives conversations and task details from Task/Event/Artifact/Attempt stores through `ProductProjectionService` in `services/pi-orchestrator/src/product-projection-builder.ts:397`.
4. TypeScript validation in `services/pi-orchestrator/src/product-projections.ts` and Python semantic validation in `agent/contracts/product_projection_semantics.py` guard the contract before the BFF returns it.
5. The Product BFF emits `Cache-Control: no-store` responses from `web/routes/product.py:43`; no Product BFF workflow state is persisted.

### Registry Draft and Publication Path

1. Database introspection or bounded DDL input produces a proposed canonical Registry shape through `registry/sync.py:265` or `registry/studio.py:200`.
2. `RegistryStudioStore` validates the canonical schema, computes deterministic diffs, and stores versioned drafts/revisions in `registry/studio.py:241`.
3. Authenticated Registry Studio endpoints in `web/routes/registry_studio.py` expose table, DDL, and ER projections from the same canonical schema.
4. Runtime assurance reloads the effective Registry and includes its revision in QueryRun review/approval checks through `forge/assurance.py:253` and `forge/query_runs.py:125`.

### Analysis and Report Path

1. A completed `query_result` Artifact unlocks analysis in `services/pi-orchestrator/src/application.ts:1301`; `runAdvisory`/`analyzeTask` execute one allowlisted Skill through `services/pi-orchestrator/src/skill-executor.ts`.
2. The restricted runtime loads exactly one Skill and only typed terminal Artifact tools in `services/pi-orchestrator/src/skills.ts:125` and `services/pi-orchestrator/src/runtime.ts:75`.
3. Analysis and chart Artifacts feed report rendering in `services/pi-orchestrator/src/application.ts:1674` and `services/pi-orchestrator/src/report-artifacts.ts`.
4. Immutable publication and deterministic HTML/PDF/PPTX projections are stored by `forge/reporting.py:216` and exposed by `web/routes/reports.py`.

**State Management:**
- Treat `TaskRun`, `TaskEvent`, `StageAttempt`, channel-event claim, Artifact, and Team Skill Policy as Pi-owned state behind interfaces in `services/pi-orchestrator/src/task-store.ts`, `services/pi-orchestrator/src/task-events.ts`, `services/pi-orchestrator/src/stage-attempts.ts`, and `services/pi-orchestrator/src/artifacts.ts`.
- Treat QueryRun, SQL, approval, execution lease, result, and their revision/hash lineage as Forge-owned state in `forge/query_runs.py`.
- Treat structural and semantic truth as Registry-owned state in `registry/`; UI views and DDL/ER projections do not become independent truth stores.
- Treat `web/routes/product.py` and `services/pi-orchestrator/src/product-projection-builder.ts` as disposable read-model builders, not writable domain stores.
- Keep long-lived memory separate from task/session state through `agent/memory/`; proposals and confirmation boundaries are represented by Pi events/actions in `services/pi-orchestrator/src/application.ts`.

## Key Abstractions

**TaskRun:**
- Purpose: Top-level, cross-channel unit of work with identity, intent, stage, status, and correlation metadata.
- Examples: `services/pi-orchestrator/src/task-store.ts`, `services/pi-orchestrator/src/sqlite-store.ts`.
- Pattern: State machine behind a `TaskStore` port; transitions are controlled by `OrchestratorApplication` and persisted transactionally.

**TaskEvent and StageAttempt:**
- Purpose: Append observable workflow history and durable stage execution/idempotency/lease evidence.
- Examples: `services/pi-orchestrator/src/task-events.ts`, `services/pi-orchestrator/src/stage-attempts.ts`, `services/pi-orchestrator/src/sqlite-store.ts`.
- Pattern: Append-only event stream plus explicit attempt lifecycle; startup/background reconciliation closes expired attempts rather than replaying unsafe work.

**Artifact:**
- Purpose: Versioned, typed, immutable handoff between planning, query, analysis, chart, and report stages.
- Examples: `services/pi-orchestrator/src/artifacts.ts`, `services/pi-orchestrator/src/structured-artifact-tools.ts`, `services/pi-orchestrator/src/report-artifacts.ts`.
- Pattern: Schema-on-read record with `artifact_id`, `artifact_type`, `schema_version`, `task_run_id`, producer, and payload; reruns create new Artifacts.

**QueryRun:**
- Purpose: Trusted lifecycle binding a task/question to candidate DSL, reviewable SQL, assurance evidence, approval, execution, and result.
- Examples: `forge/query_runs.py`, `web/routes/query_runs.py`, `services/pi-orchestrator/src/forge/query-run-client.ts`.
- Pattern: Durable state machine with idempotency keys, cryptographic hashes, revision pinning, expiration, and execution ownership lease.

**Forge JSON Contract:**
- Purpose: Model-independent, bounded query representation compiled deterministically to SQL.
- Examples: `forge/schema.json`, `forge/compiler.py`, `.agents/skills/forge-query/SKILL.md`.
- Pattern: JSON Schema plus normalization/reference-integrity validation; models submit DSL and never directly authorize executable SQL.

**Canonical Registry:**
- Purpose: Structural and semantic source used for planning context, access scoping, relationship validation, and query lineage.
- Examples: `registry/studio.py`, `registry/sync.py`, `registry/relationships.py`, `registry/validator.py`.
- Pattern: Versioned canonical data with deterministic projections and reviewable draft/diff/publish flow.

**Product Projection:**
- Purpose: Bounded, redacted, stable UI contract derived from internal task/report/Registry state.
- Examples: `services/pi-orchestrator/src/product-projection-builder.ts`, `services/pi-orchestrator/src/product-projections.ts`, `agent/contracts/product_projection_semantics.py`.
- Pattern: Cross-language schema and semantic fixtures; projections include source revisions and availability instead of masking missing sources.

**Restricted Skill Runtime:**
- Purpose: Execute professional analysis methods without database, shell, filesystem, or arbitrary-tool authority.
- Examples: `services/pi-orchestrator/src/skills.ts`, `services/pi-orchestrator/src/runtime.ts`, `services/pi-orchestrator/src/skill-executor.ts`.
- Pattern: Explicit allowlist, one Skill per stage, no built-ins, and one typed terminal Artifact tool.

## Entry Points

**FastAPI Runtime:**
- Location: `main.py` (`app`).
- Triggers: `uvicorn main:app`.
- Responsibilities: Run startup reconciliation/readiness checks, mount channel/admin/product/internal routes and local static assets, expose health endpoints, and manage Feishu runtime lifecycle.

**Pi Orchestrator Runtime:**
- Location: `services/pi-orchestrator/src/server.ts` (`isMain` block).
- Triggers: `npm --prefix services/pi-orchestrator run dev` or `npm --prefix services/pi-orchestrator start`.
- Responsibilities: Load Pi config, open `SqliteOrchestratorState`, reconcile attempts, authenticate callers, and serve `/v1/*` orchestration/projection endpoints.

**Python CLI:**
- Location: `forge/cli.py` (`main`), declared in `pyproject.toml` as the `forge` console script.
- Triggers: `forge compile`, Registry sync/validation, and other CLI subcommands.
- Responsibilities: Provide deterministic local/operator access to Forge capabilities without entering the Web/Pi channel path.

**Feishu Webhook / Managed Runtime:**
- Location: `main.py:157`, `web/feishu_pi.py`, `web/feishu_runtime.py`.
- Triggers: HTTP webhook when managed WebSocket runtime is disabled, or managed process runtime when enabled.
- Responsibilities: Verify/translate Feishu events, map identity, forward shared ChannelEvents to Pi, and render Pi presentations.

**DingTalk Adapter:**
- Location: `web/dingtalk_pi.py`.
- Triggers: Host integration calling `DingTalkPiAdapter`.
- Responsibilities: Translate DingTalk message/action identity and payloads into the same Pi channel contract.

**Public Documentation Site:**
- Location: `website/astro.config.mjs`, with content rooted at `website/src/content/docs/`.
- Triggers: Astro development/build commands from `website/package.json`.
- Responsibilities: Build static documentation only.

## Architectural Constraints

- **Orchestration authority:** Pi is the only task orchestrator; add Task/Plan/Stage transitions to `services/pi-orchestrator/src/application.ts`, not to `forge/`, channel adapters, or Product BFF routes.
- **Execution authority:** Only Forge may validate and execute database queries. Pi must call `services/pi-orchestrator/src/forge/query-run-client.ts`; Skills and channels must not import database execution code.
- **Approval boundary:** Execution must preserve the exact review binding enforced in `forge/query_runs.py`: query id, approver identity, SQL hash, assurance hash/revisions, Registry revision, expiry, idempotency, and read-only deployment gates.
- **Threading:** Pi runs on the Node single-threaded event loop and uses synchronous `node:sqlite` only behind `SqliteOrchestratorState`; FastAPI is async, while blocking planning/execution is explicitly offloaded with `asyncio.to_thread` in `forge/query_runs.py` and `_run_sync` in `web/router.py`.
- **Global state:** Python uses the configuration singleton `cfg` in `config.py`, a process-wide SQLAlchemy engine in `forge/executor.py`, module-level memory stores in `agent/memory/__init__.py`, and selected module-level service/store instances in Web routes. Keep new state behind an owning store before adding another singleton.
- **Persistence ownership:** Pi and Forge use separate SQLite schemas/stores. Do not read or mutate `services/pi-orchestrator/src/sqlite-store.ts` tables from Python or `forge/query_runs.py` tables from TypeScript.
- **Contract compatibility:** Modify TypeScript schema truth in `services/pi-orchestrator/src/product-projections.ts` together with generated/shared fixtures and Python validation under `agent/contracts/`; do not introduce divergent JSON shapes in `web/routes/product.py`.
- **Channel thinness:** Channel-specific modules in `web/*_pi.py` may map identity/input and render `ChannelPresentation`; they must route workflow behavior through `POST /v1/channel-events`.
- **Skill isolation:** Runtime Skills are external, explicit, and allowlisted by `services/pi-orchestrator/src/skills.ts`; production sessions disable built-in file/shell/edit tools in `services/pi-orchestrator/src/runtime.ts`.
- **Static assets:** Product Shell assets are local under `web/static/product/`; `main.py` mounts `/static` and `/charts`, so runtime pages must not require an external CDN.
- **Circular imports:** No cross-runtime circular chain is required by the inspected architecture. Python uses function-local imports at boundary points such as `forge/query_runs.py` → `agent.agent` and `web/router.py` → admin stores; preserve lazy boundary imports rather than creating import-time initialization cycles.
- **Current product gate:** Repository status in `docs/current-project-state.md` fixes the active phase at S0 design-partner/problem baseline and explicitly does not authorize a new runtime; architecture changes require corresponding accepted evidence/plan updates.

## Anti-Patterns

### Putting SQL or Database Access in Pi or Skills

**What happens:** A new Pi tool or Skill connects to the database, emits executable SQL outside Forge JSON, or treats model output as an approved plan.
**Why it's wrong:** It bypasses Registry/ACL, Assurance, hash-bound review, read-only execution controls, and the execution truth owned by `forge/query_runs.py`.
**Do this instead:** Submit bounded query intent/candidate DSL through `services/pi-orchestrator/src/forge/query-run-client.ts`; validate and execute through `web/routes/query_runs.py`, `forge/assurance.py`, and `forge/executor.py`.

### Duplicating Workflow in a Channel or BFF

**What happens:** `web/router.py`, `web/routes/product.py`, `web/feishu_pi.py`, or `web/dingtalk_pi.py` adds its own task status, retry loop, approval state, or persistence.
**Why it's wrong:** Web and messaging channels can disagree, cross-channel recovery breaks, and Pi stops being the Task truth source.
**Do this instead:** Convert input to `ChannelEventInput` from `services/pi-orchestrator/src/channels/contracts.ts`, advance state in `services/pi-orchestrator/src/application.ts`, and render `ChannelPresentation` from `services/pi-orchestrator/src/channels/renderer.ts`.

### Creating a Second Product Truth Store

**What happens:** UI-ready conversation/task/report objects are written as a new mutable database instead of derived from Pi/Forge state.
**Why it's wrong:** Projection state drifts from Task/Event/Artifact/QueryRun truth and availability/errors become hidden.
**Do this instead:** Extend bounded projections in `services/pi-orchestrator/src/product-projection-builder.ts` and combine read-only sources in `web/routes/product.py`, preserving source revisions and availability.

### Scattering Assurance Checks Across Callers

**What happens:** A route calls `forge/compiler.py`, `forge/lint.py`, or Registry validation selectively and treats its local subset as safe.
**Why it's wrong:** Entry points acquire inconsistent safety semantics and review evidence no longer describes actual execution.
**Do this instead:** Route every candidate through the unified `assure_query` pipeline in `forge/assurance.py`; persist the resulting report/hash/revisions in `forge/query_runs.py`.

### Treating Registry Views as Independent Schema Sources

**What happens:** DDL text, ER edits, table forms, or introspection output directly overwrite execution schema or migrate a database.
**Why it's wrong:** Structural truth becomes ambiguous and inferred relationships can appear authoritative.
**Do this instead:** Produce a draft, validate it, compute deterministic diff, obtain review, and publish a canonical revision through `registry/studio.py`; derive DDL/ER views from that revision.

### Extending the Deprecated Legacy Agent Path

**What happens:** New product behavior is added to `/api/chat`, `/api/approve`, `/api/cancel`, or raw execution handlers in `web/router.py`.
**Why it's wrong:** Those paths are marked rollback-only and do not represent the Pi Task/Artifact workflow boundary.
**Do this instead:** Add shared task behavior to `services/pi-orchestrator/src/application.ts`, expose it through `services/pi-orchestrator/src/server.ts`, and forward it through the authenticated `/api/pi/*` or `/api/product/*` surfaces in `web/router.py`/`web/routes/product.py`.

## Error Handling

**Strategy:** Validate and bound errors at every trust boundary, persist durable failure state where ownership requires it, and fail closed when identity, revision, contract, approval, or execution evidence is incomplete.

**Patterns:**
- Raise stable domain errors (`TaskStateError` in `services/pi-orchestrator/src/task-store.ts`, `QueryRunError` in `forge/query_runs.py`, `RegistryStudioError` in `registry/studio.py`) and translate them to bounded HTTP status/body shapes in `services/pi-orchestrator/src/server.ts` and `web/routes/query_runs.py`.
- Record Task failure/timed-out events and finish StageAttempts transactionally in `services/pi-orchestrator/src/application.ts`; startup/background reconciliation marks interrupted work rather than replaying SQL in `main.py` and `services/pi-orchestrator/src/sqlite-store.ts`.
- Convert model/assurance failures into retryable bounded diagnostics in `agent/agent.py`; retry only candidate generation a fixed number of times and never cross the approval boundary.
- Sanitize user-facing database errors while logging internal exception details in `forge/executor.py`; row and timeout caps remain deployment-enforced.
- Represent unavailable Product BFF sources explicitly through projection availability and non-200 bounded responses in `web/routes/product.py` rather than fabricating empty success.

## Cross-Cutting Concerns

**Logging:** Python uses standard `logging`, configured once in `main.py`; Pi writes minimal process/server output in `services/pi-orchestrator/src/server.ts` while workflow observability is primarily durable TaskEvents/StageAttempts in `services/pi-orchestrator/src/task-events.ts` and `services/pi-orchestrator/src/stage-attempts.ts`.

**Validation:** Pydantic request models protect FastAPI routes in `web/router.py`/`web/routes/`; manual bounded parsing protects the Node HTTP server in `services/pi-orchestrator/src/server.ts`; JSON Schema plus semantic gates protect Forge DSL and product/governance contracts in `forge/schema.json`, `forge/assurance.py`, `services/pi-orchestrator/src/product-projections.ts`, and `agent/contracts/`.

**Authentication:** Web session/API authentication lives in `web/auth.py`; Pi service authentication guards `/api/internal/*` routes; channel/admin service keys and external-to-internal identity mapping are enforced in `services/pi-orchestrator/src/server.ts` and `services/pi-orchestrator/src/channels/identity.ts`. Final data authorization is rechecked by Forge/Registry context rather than inferred from a channel-provided user id.

**Idempotency:** Channel events are claimed in `services/pi-orchestrator/src/channels/event-store.ts`; StageAttempts and QueryRuns carry idempotency keys in `services/pi-orchestrator/src/stage-attempts.ts` and `forge/query_runs.py`; approval replay is accepted only for the same completed binding.

**Privacy and redaction:** Product projections bound response size and reject secret/internal-path patterns in `services/pi-orchestrator/src/product-projection-builder.ts`; the Product BFF sanitizes text in `web/routes/product.py`; report rendering excludes hidden reasoning/prompts/secrets in `forge/reporting.py`.

---

*Architecture analysis: 2026-08-25*
