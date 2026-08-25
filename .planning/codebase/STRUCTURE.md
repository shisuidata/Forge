# Codebase Structure

**Analysis Date:** 2026-08-25

## Directory Layout

```text
Forge/
├── main.py                         # FastAPI process entry point and route mounting
├── config.py                       # Python runtime configuration singleton
├── pyproject.toml                  # Python package, dependencies, CLI, and pytest config
├── forge/                          # Trusted query execution, Registry context, reports
├── agent/                          # Model planning adapter, contracts, memory, model control
├── registry/                       # Canonical schema, semantic Registry, sync, Studio
├── web/                            # FastAPI channels, Product BFF, templates, static UI
├── services/
│   └── pi-orchestrator/            # Node/TypeScript task control plane
├── tests/                          # Python unit, integration, contract, benchmark, E2E suites
├── docs/                           # Current state, requirements, architecture, plans, evidence
├── website/                        # Astro/Starlight public documentation site
├── scripts/                        # Bootstrap, smoke, seed, and export operator scripts
├── tools/                          # Disposable visual/product/chart prototypes and bakeoffs
├── demo/                           # Local demonstration seeders and ignored demo databases
├── customer-poc-template/          # Sanitized customer PoC delivery skeleton
├── .agents/skills/                 # Repository-local agent skill instructions
├── .pi/skills/                     # Repository-local Pi skill mount point
├── .github/workflows/              # CI workflows
├── .planning/codebase/             # Generated GSD codebase maps
├── .forge/                         # Ignored local runtime databases/cache/logs
└── forge_audit.db                  # Ignored local audit database
```

The runtime source is not under one generic `src/` tree. Python package boundaries are top-level (`forge/`, `agent/`, `registry/`, `web/`), while the TypeScript control plane is isolated under `services/pi-orchestrator/src/`. Place code by ownership, not by language convenience.

## Directory Purposes

**`forge/`:**
- Purpose: Own trusted data execution and deterministic data contracts.
- Contains: Forge JSON schema/compiler, normalization, lint/assurance, query execution, QueryRun lifecycle, context/retrieval, readiness, reporting, charting, cache, CLI, and PoC utilities.
- Key files: `forge/compiler.py`, `forge/schema.json`, `forge/assurance.py`, `forge/executor.py`, `forge/query_runs.py`, `forge/context.py`, `forge/retriever.py`, `forge/reporting.py`, `forge/cli.py`.
- Add here when the behavior decides whether a data operation is valid, reviewable, executable, or reproducible; do not add Task orchestration here.

**`agent/`:**
- Purpose: Adapt active model bindings and professional reasoning/memory to Forge contracts without owning database execution.
- Contains: Query candidate generation, LLM provider compatibility, prompts, model profile/control/quality, audit/feedback, legacy pipeline, tenant ACL helpers, knowledge, and memory stores.
- Key files: `agent/agent.py`, `agent/llm.py`, `agent/model_config.py`, `agent/model_control.py`, `agent/model_quality.py`, `agent/prompts.py`, `agent/audit.py`, `agent/memory/`.
- Add model-provider or candidate-generation behavior beside `agent/llm.py`/`agent/agent.py`; keep deterministic validation in `forge/` and workflow transitions in `services/pi-orchestrator/src/`.

**`agent/contracts/`:**
- Purpose: Store generated/shared JSON contracts, cross-language fixture corpora, and Python semantic validators.
- Contains: Product Projection v1 schemas/fixtures, governance action/review contracts, Artifact schemas, and Python validation modules.
- Key files: `agent/contracts/product-projection-v1.schema.json`, `agent/contracts/product-projection-fixtures.v1.json`, `agent/contracts/product_projection_semantics.py`, `agent/contracts/governance_semantics.py`, `agent/contracts/chart-artifact-v2.schema.json`.
- Treat generated TypeScript-exported schemas as outputs; change their editing truth under `services/pi-orchestrator/src/` and refresh matching fixtures/validators together.

**`agent/memory/`:**
- Purpose: Separate working, episodic, and confirmed semantic memory concerns.
- Contains: WMB (`wmb.py`), EMS (`ems.py`), SMP (`smp.py`), extraction, and the composed memory facade.
- Key files: `agent/memory/__init__.py`, `agent/memory/wmb.py`, `agent/memory/ems.py`, `agent/memory/smp.py`, `agent/memory/extractor.py`.
- Add memory persistence/confirmation logic here; represent task-scoped proposals and approvals through Pi events in `services/pi-orchestrator/src/application.ts`.

**`registry/`:**
- Purpose: Own canonical physical schema and semantic Registry operations.
- Contains: Database introspection/sync, canonical draft/revision Studio, relationship loading, metric validation, business context, staging promotion, data, and JSON Schema contracts.
- Key files: `registry/sync.py`, `registry/studio.py`, `registry/contracts/canonical-schema.schema.json`, `registry/relationships.py`, `registry/validator.py`, `registry/business_context.py`, `registry/staging_sync.py`.
- Add new Registry domain rules here; expose them through `web/routes/registry_studio.py` or Forge assurance rather than directly editing Registry files from UI handlers.

**`registry/contracts/`:**
- Purpose: Define Registry-specific machine-readable contracts.
- Contains: Canonical schema JSON Schema.
- Key files: `registry/contracts/canonical-schema.schema.json`.
- Add versioned Registry schemas here and keep validation entry points in `registry/studio.py`.

**`registry/data/`:**
- Purpose: Hold configured Registry data assets for local/demo use.
- Contains: Registry-specific data/cache subtrees such as `registry/data/.forge/`.
- Key files: effective runtime paths are resolved by `config.py`; do not assume every deployment uses repository-default data.
- Add sanitized demo/example Registry data only; production Registry paths are deployment configuration.

**`web/`:**
- Purpose: Present Forge/Pi through FastAPI, browser pages, Feishu/DingTalk adapters, and internal service APIs.
- Contains: Root/admin routers, auth, Pi clients/channel rendering, messaging integrations, Product BFF routes, QueryRun/context/report/memory/Registry/settings routes, templates, and static files.
- Key files: `web/router.py`, `web/auth.py`, `web/pi_client.py`, `web/pi_channel.py`, `web/feishu_pi.py`, `web/dingtalk_pi.py`, `web/routes/product.py`, `web/routes/query_runs.py`.
- Add route modules under `web/routes/` when they form a coherent boundary; mount them from `web/router.py` and preserve the existing root `chat_router` versus `/admin` `router` split.

**`web/routes/`:**
- Purpose: Keep bounded subdomains out of the large compatibility/admin router.
- Contains: Product BFF, QueryRun internal service API, context, reports, memory, Registry Studio, and model/settings endpoints.
- Key files: `web/routes/product.py`, `web/routes/query_runs.py`, `web/routes/context.py`, `web/routes/reports.py`, `web/routes/memory.py`, `web/routes/registry_studio.py`, `web/routes/settings.py`.
- Add a new route module here when it has its own prefix/auth dependency/domain service; do not extend rollback-only legacy endpoints in `web/router.py` for new product behavior.

**`web/templates/`:**
- Purpose: Render authenticated server-side Product Shell, admin, chat, report, settings, Registry, and state pages.
- Contains: Shared bases and feature templates, including `product_base.html`, `product_chat.html`, `product_workspace.html`, `product_tasks.html`, `product_task_detail.html`, `product_reports.html`, and admin templates.
- Key files: `web/templates/product_base.html`, `web/templates/base.html`, `web/templates/product_state.html`.
- Add Product Shell pages as `product_<surface>.html` extending `web/templates/product_base.html`; add admin pages using the existing admin base in `web/templates/base.html`.

**`web/static/`:**
- Purpose: Serve local browser assets and generated chart HTML without external CDN dependency.
- Contains: Product Shell CSS/JavaScript under `web/static/product/` and ignored/generated chart outputs under `web/static/charts/`.
- Key files: `web/static/product/product.css`, `web/static/product/product-shell.js`, `web/static/product/product-pages.js`.
- Add persistent Product UI assets under `web/static/product/`; generated charts belong under `web/static/charts/` and are ignored by `.gitignore`.

**`services/pi-orchestrator/`:**
- Purpose: Package the Node/TypeScript Pi control-plane service as an independently runnable process.
- Contains: Source, Node tests, contract export script, TypeScript/package configuration, example channel identities/model bindings, and ignored runtime state.
- Key files: `services/pi-orchestrator/package.json`, `services/pi-orchestrator/tsconfig.json`, `services/pi-orchestrator/src/server.ts`, `services/pi-orchestrator/src/application.ts`, `services/pi-orchestrator/src/sqlite-store.ts`.
- Add all Task/Plan/Stage/Artifact workflow behavior here; Python should consume Pi through HTTP projections/actions rather than importing this implementation.

**`services/pi-orchestrator/src/`:**
- Purpose: Implement the Pi domain, application service, runtime, ports, and adapters.
- Contains: Server/config, Task/Event/Attempt/Artifact models, SQLite store, planning, model binding, Skill policy/execution, product/governance/report/chart contracts, channel adapters, Forge clients, and restricted tools.
- Key files: `services/pi-orchestrator/src/application.ts`, `services/pi-orchestrator/src/server.ts`, `services/pi-orchestrator/src/task-store.ts`, `services/pi-orchestrator/src/artifacts.ts`, `services/pi-orchestrator/src/product-projection-builder.ts`, `services/pi-orchestrator/src/runtime.ts`.
- Add domain contracts as focused root modules; place external-boundary adapters in `channels/`, `forge/`, or `tools/`.

**`services/pi-orchestrator/src/channels/`:**
- Purpose: Define shared channel event/action/presentation contracts and identity/idempotency/rendering adapters.
- Contains: Contracts, parser, identity resolver, intent routing, event store, and presentation renderer.
- Key files: `services/pi-orchestrator/src/channels/contracts.ts`, `services/pi-orchestrator/src/channels/identity.ts`, `services/pi-orchestrator/src/channels/intent.ts`, `services/pi-orchestrator/src/channels/event-store.ts`, `services/pi-orchestrator/src/channels/renderer.ts`.
- Add channel-neutral behavior here; channel SDK-specific code remains in Python `web/` adapters.

**`services/pi-orchestrator/src/forge/`:**
- Purpose: Isolate Pi's typed HTTP dependency on Forge.
- Contains: Base Forge client types/errors and QueryRun/context/report/memory calls.
- Key files: `services/pi-orchestrator/src/forge/client.ts`, `services/pi-orchestrator/src/forge/query-run-client.ts`.
- Add a Forge capability here as a port/client method before calling it from `services/pi-orchestrator/src/application.ts`; never add direct database drivers.

**`services/pi-orchestrator/src/tools/`:**
- Purpose: Define the narrow Forge tool surface available to restricted Pi sessions.
- Contains: The `forge_prepare_query` tool and trusted task context types.
- Key files: `services/pi-orchestrator/src/tools/forge-prepare-query.ts`.
- Add only bounded, authenticated Forge tools that preserve execution-plane authority; built-in shell/file tools remain disabled by `services/pi-orchestrator/src/runtime.ts`.

**`services/pi-orchestrator/tests/`:**
- Purpose: Test TypeScript domain state, server boundaries, channel flows, contracts, Skill isolation, and persistence.
- Contains: Node test-runner `.test.ts` files colocated in one test directory.
- Key files: `services/pi-orchestrator/tests/application.test.ts`, `services/pi-orchestrator/tests/server.test.ts`, `services/pi-orchestrator/tests/sqlite-store.test.ts`, `services/pi-orchestrator/tests/channels.test.ts`, `services/pi-orchestrator/tests/product-projection-builder.test.ts`.
- Add one focused `<module>.test.ts` file or extend the matching file when changing a Pi observable contract.

**`tests/`:**
- Purpose: Verify Python units, FastAPI integration, cross-language contracts, Registry/assurance/compiler/executor behavior, Product Shell/BFF, benchmarks, and browser E2E.
- Contains: Root `test_*.py` suites, fixtures, accuracy/benchmark/Spider2 harnesses, datasets, and failure corpora.
- Key files: `tests/conftest.py`, `tests/test_api.py`, `tests/test_query_runs.py`, `tests/test_assurance.py`, `tests/test_compiler.py`, `tests/test_product_bff.py`, `tests/test_product_projection_contracts.py`, `tests/test_e2e.py`.
- Add behavior tests to the narrow matching `test_<module>.py`; reusable data belongs under `tests/fixtures/`, while evaluation harnesses remain under `tests/accuracy/`, `tests/benchmark/`, or `tests/spider2/`.

**`docs/`:**
- Purpose: Store current project state, accepted requirements, active plan, stable architecture/product contracts, evidence, and operator/setup documentation.
- Contains: `current-project-state.md`, requirements pool, active enterprise plan, platform/product architecture, product projection contract, phase evidence, and historical/reference material.
- Key files: `docs/current-project-state.md`, `docs/requirements-pool.md`, `docs/forge-enterprise-evolution-plan.md`, `docs/platform-architecture.md`, `docs/product-projection-contracts.md`.
- Read `docs/current-project-state.md` first. Update stable responsibility boundaries in `docs/platform-architecture.md`; do not treat historical evidence documents as active authorization.

**`website/`:**
- Purpose: Build and publish the public Astro/Starlight documentation site.
- Contains: Astro config, package manifests, public assets, content collection config, Markdown/MDX docs, styles, and generated/ignored local build metadata.
- Key files: `website/astro.config.mjs`, `website/package.json`, `website/src/content.config.ts`, `website/src/content/docs/index.mdx`, `website/src/content/docs/reference/architecture.md`.
- Add public guides/concepts/reference/course content under `website/src/content/docs/`; runtime/operator truth remains in top-level `docs/`.

**`scripts/`:**
- Purpose: Provide operator-facing bootstrap, seed, smoke, demo, and export commands.
- Contains: Shell entry scripts and bounded Python utilities.
- Key files: `scripts/bootstrap-dev.sh`, `scripts/production-smoke.sh`, `scripts/provider_smoke.py`, `scripts/performance_smoke.py`, `scripts/seed_mock_data.py`.
- Add repeatable operational workflows here; reusable runtime logic belongs in a package and should be imported rather than duplicated.

**`tools/`:**
- Purpose: Hold non-runtime prototypes, chart candidates, bakeoffs, and visual experimentation assets.
- Contains: `tools/web-product-shell-prototype/`, `tools/chart-engine-bakeoff/`, `tools/chart-storytelling-echarts-candidate/`, and supporting scripts.
- Key files: `tools/chart_storytelling_candidate.py`, `tools/web-product-shell-prototype/`.
- Keep experimental code here until a candidate is explicitly promoted; production behavior must move into `web/`, `forge/`, or `services/pi-orchestrator/` with tests.

**`customer-poc-template/`:**
- Purpose: Provide a sanitized, reusable delivery skeleton for customer design-partner PoCs.
- Contains: Example Registry, case/provider-smoke examples, report/triage templates, and setup guidance.
- Key files: `customer-poc-template/README.md`, `customer-poc-template/cases.example.json`, `customer-poc-template/delivery_report.template.md`.
- Add examples/placeholders only; never place customer data or production credentials in this committed template.

**`.agents/skills/`:**
- Purpose: Define repository-local agent workflows and constraints.
- Contains: `forge-query/SKILL.md` with the review-before-execution Forge query workflow.
- Key files: `.agents/skills/forge-query/SKILL.md`.
- Add a new Skill only when it encodes a reusable repository workflow; keep data access bounded by the Forge review/approval contract.

**`.github/workflows/`:**
- Purpose: Define repository CI.
- Contains: The main CI workflow.
- Key files: `.github/workflows/ci.yml`.
- Add CI checks here only when they correspond to supported local commands in `pyproject.toml` or `services/pi-orchestrator/package.json`.

## Key File Locations

**Entry Points:**
- `main.py`: FastAPI application and process lifecycle for Web/internal Forge APIs.
- `forge/cli.py`: Python console-script entry point declared as `forge` in `pyproject.toml`.
- `services/pi-orchestrator/src/server.ts`: Node HTTP server entry point for Pi.
- `website/astro.config.mjs`: Public documentation site application configuration.
- `scripts/bootstrap-dev.sh`: Local Python development bootstrap.
- `scripts/production-smoke.sh`: Operator-facing production-profile smoke workflow.

**Configuration:**
- `config.py`: Python precedence and typed runtime settings (`environment` → `forge.yaml` → defaults); callers import `cfg`.
- `forge.yaml.example`: Sanitized Python runtime configuration example; local `forge.yaml` is ignored.
- `.env.example`, `.env.production.example`: Sanitized environment-variable name/examples; real `.env*` files are ignored and must not be read or committed.
- `pyproject.toml`: Python runtime/dev dependencies, package discovery, CLI entry point, and pytest defaults.
- `services/pi-orchestrator/src/config.ts`: Pi environment parsing, state/identity/Forge/Skill/model path configuration, and model revision derivation.
- `services/pi-orchestrator/package.json`: Node engine, scripts, and pinned Pi/TypeBox dependencies.
- `services/pi-orchestrator/tsconfig.json`: TypeScript compiler configuration.
- `website/package.json`, `website/astro.config.mjs`, `website/tsconfig.json`: Public site toolchain.
- `.github/workflows/ci.yml`: CI execution contract.

**Core Logic:**
- `services/pi-orchestrator/src/application.ts`: Task orchestration and stage transitions.
- `services/pi-orchestrator/src/task-store.ts`: TaskRun domain contract/state machine port.
- `services/pi-orchestrator/src/sqlite-store.ts`: Durable Pi store adapter and transactions.
- `services/pi-orchestrator/src/product-projection-builder.ts`: UI projection assembly/redaction/bounds.
- `forge/query_runs.py`: Trusted review/approval/execution lifecycle.
- `agent/agent.py`: Candidate query generation and bounded assurance retry.
- `forge/assurance.py`: Unified deterministic pre-review gate.
- `forge/compiler.py`: Forge JSON normalization, validation, and SQL compilation.
- `forge/executor.py`: Read-only bounded database execution.
- `registry/studio.py`: Canonical Registry draft/revision/diff/projection model.
- `forge/reporting.py`: Immutable report publication and projections.

**API Boundaries:**
- `web/routes/query_runs.py`: Pi-authenticated Forge QueryRun API.
- `web/routes/product.py`: Browser Product BFF.
- `web/router.py`: Root Pi forwarding, Product Shell pages, admin compatibility surface, and rollback-only legacy endpoints.
- `services/pi-orchestrator/src/server.ts`: Channel/Admin-to-Pi HTTP boundary.
- `services/pi-orchestrator/src/forge/query-run-client.ts`: Pi-to-Forge typed client boundary.
- `web/auth.py`: Web, API, and Pi service authentication.

**Contracts:**
- `forge/schema.json`: Forge JSON DSL schema.
- `registry/contracts/canonical-schema.schema.json`: Canonical Registry schema.
- `services/pi-orchestrator/src/artifacts.ts`: Pi Artifact type/validation contract.
- `services/pi-orchestrator/src/product-projections.ts`: Product Projection editing truth.
- `services/pi-orchestrator/src/governance-contracts.ts`: Governance contract definitions/validation.
- `agent/contracts/`: Python-consumed exported schemas, fixtures, and semantic validators.
- `docs/product-projection-contracts.md`: Human-readable Product Projection contract.

**Testing:**
- `tests/conftest.py`: Shared Python test configuration/fixtures.
- `tests/test_query_runs.py`: QueryRun state/approval/lease behavior.
- `tests/test_assurance.py`, `tests/test_compiler.py`, `tests/test_executor.py`: Trusted execution gates.
- `tests/test_product_bff.py`, `tests/test_product_projection_contracts.py`, `tests/test_product_shell.py`: Product read-model/UI contracts.
- `tests/test_pi_orchestrator_web.py`, `tests/test_pi_channel.py`: Python Web-to-Pi integration.
- `services/pi-orchestrator/tests/application.test.ts`: Pi application workflows.
- `services/pi-orchestrator/tests/server.test.ts`: Pi HTTP boundary.
- `services/pi-orchestrator/tests/sqlite-store.test.ts`: Durable Pi persistence.
- `services/pi-orchestrator/tests/product-projection-builder.test.ts`: Projection derivation.

**Authoritative Documentation:**
- `docs/current-project-state.md`: First-read current-state projection and active phase gate.
- `docs/requirements-pool.md`: Latest accepted requirements/decisions.
- `docs/forge-enterprise-evolution-plan.md`: Sole active plan and phase gates.
- `docs/platform-architecture.md`: Stable Pi/Forge/Skill/channel responsibility boundary.
- `docs/product-north-star.md`: Stable product direction.

## Naming Conventions

**Files:**
- Python modules use lowercase snake_case: `forge/query_runs.py`, `registry/business_context.py`, `agent/model_control.py`.
- Python tests use `test_<subject>.py`: `tests/test_registry_studio.py`, `tests/test_product_bff.py`.
- TypeScript source uses lowercase kebab-case: `services/pi-orchestrator/src/product-projection-builder.ts`, `services/pi-orchestrator/src/task-events.ts`.
- TypeScript tests use `<subject>.test.ts`: `services/pi-orchestrator/tests/sqlite-store.test.ts`.
- FastAPI route modules use domain nouns in snake_case: `web/routes/query_runs.py`, `web/routes/registry_studio.py`.
- Product templates use `product_<surface>.html`: `web/templates/product_task_detail.html`, `web/templates/product_reports.html`.
- Versioned machine contracts include subject/version/type: `agent/contracts/product-projection-v1.schema.json`, `agent/contracts/product-projection-fixtures.v1.json`.
- Example-only configuration uses `.example` before the extension where applicable: `forge.yaml.example`, `services/pi-orchestrator/channel-identities.example.json`.
- Current/evidence docs use explicit descriptive names and dates where they are snapshots: `docs/current-project-state.md`, `docs/product-spine-sp5-evidence-2026-08-25.md`.

**Directories:**
- Python package directories are lowercase nouns: `forge/`, `agent/`, `registry/`, `web/`.
- TypeScript service/module directories use kebab-case: `services/pi-orchestrator/`.
- Test/evaluation subtrees use purpose nouns: `tests/fixtures/`, `tests/accuracy/`, `tests/benchmark/`, `tests/spider2/`.
- Runtime/generated local state uses dot-directories and is ignored: `.forge/`, `services/pi-orchestrator/.runtime/`, `website/.astro/`.
- Public site content follows information architecture nouns: `website/src/content/docs/guides/`, `website/src/content/docs/concepts/`, `website/src/content/docs/reference/`, `website/src/content/docs/course/`.

## Where to Add New Code

**New Task/Workflow Feature:**
- Primary code: add transitions/application behavior in `services/pi-orchestrator/src/application.ts`; add focused domain types/stores beside `services/pi-orchestrator/src/task-store.ts`, `services/pi-orchestrator/src/artifacts.ts`, or `services/pi-orchestrator/src/stage-attempts.ts`.
- HTTP surface: expose validated actions/reads from `services/pi-orchestrator/src/server.ts`.
- Tests: `services/pi-orchestrator/tests/application.test.ts`, `services/pi-orchestrator/tests/server.test.ts`, plus a focused `<subject>.test.ts` when the domain is independent.
- Do not place orchestration loops in `forge/`, `web/routes/`, or channel adapters.

**New Trusted Query Capability:**
- Contract/compiler: extend `forge/schema.json` and deterministic handling in `forge/compiler.py`.
- Safety: add unified validation to `forge/assurance.py` and/or `forge/lint.py`, preserving `forge/executor.py` as the final read-only gate.
- Lifecycle/API: extend `forge/query_runs.py` and `web/routes/query_runs.py`; update the typed Pi port/client in `services/pi-orchestrator/src/application.ts` and `services/pi-orchestrator/src/forge/query-run-client.ts`.
- Tests: `tests/test_compiler.py` or a focused compiler test, `tests/test_assurance.py`, `tests/test_query_runs.py`, and Pi client/application tests when the contract crosses runtimes.

**New Registry Feature:**
- Domain logic: `registry/studio.py`, `registry/sync.py`, `registry/relationships.py`, or `registry/validator.py` according to ownership.
- Contract: `registry/contracts/`.
- API/UI: `web/routes/registry_studio.py`, then templates/assets under `web/templates/` and `web/static/product/` as needed.
- Tests: `tests/test_registry_studio.py`, `tests/test_registry_studio_api.py`, `tests/test_sync.py`, or `tests/test_registry_relationships.py`.
- Use draft/diff/review/publish; do not introduce a second schema truth in UI code.

**New Product Surface:**
- Page route: add a thin authenticated page handler in `web/router.py` or a dedicated route module in `web/routes/`.
- Template/assets: `web/templates/product_<surface>.html`, `web/static/product/product-pages.js`, and `web/static/product/product.css`.
- Data: extend a stable projection in `services/pi-orchestrator/src/product-projections.ts`/`product-projection-builder.ts` or the bounded aggregation in `web/routes/product.py`.
- Tests: `tests/test_product_shell.py`, `tests/test_web_product_content.py`, `tests/test_product_bff.py`, and `services/pi-orchestrator/tests/product-projection-builder.test.ts` for changed contracts.
- Keep page code projection-only; workflow actions route to Pi.

**New Channel:**
- Shared contract/intent/presentation changes: `services/pi-orchestrator/src/channels/`.
- Channel SDK adapter: create `web/<channel>_pi.py` following `web/feishu_pi.py` and `web/dingtalk_pi.py`.
- Pi forwarding/rendering helpers: reuse `web/pi_channel.py`.
- Tests: add `tests/test_<channel>_pi.py` and extend `services/pi-orchestrator/tests/channels.test.ts`.
- Map external identity to authorized `org_id`/`team_id`/`user_id`; do not trust channel user identifiers directly.

**New Professional Skill:**
- Skill package: add the real Skill under the configured external Skill root resolved by `services/pi-orchestrator/src/config.ts`; repository `.pi/skills/` is only a local mount point.
- Allowlist/policy: update `services/pi-orchestrator/src/skills.ts` and typed Artifact tool support in `services/pi-orchestrator/src/structured-artifact-tools.ts`/`skill-executor.ts`.
- Tests: `services/pi-orchestrator/tests/structured-skills.test.ts`, `services/pi-orchestrator/tests/runtime.test.ts`, and `services/pi-orchestrator/tests/application.test.ts`.
- Keep one Skill per isolated stage and no database/shell/filesystem authority.

**New Report/Artifact Type:**
- Pi contract: `services/pi-orchestrator/src/artifacts.ts`, with producer-specific validation in `services/pi-orchestrator/src/structured-artifact-tools.ts`, `report-artifacts.ts`, or a focused module.
- Forge publication: `forge/reporting.py` and `web/routes/reports.py` if the output is immutable/downloadable.
- Cross-language contract: export schemas/fixtures to `agent/contracts/` when Python consumes the shape.
- Tests: `services/pi-orchestrator/tests/report-artifacts.test.ts`, `tests/test_artifact_contracts.py`, and `tests/test_reporting.py`.

**New Model-Control Feature:**
- Python source: `agent/model_control.py`, `agent/model_config.py`, `agent/model_quality.py`, and `web/routes/settings.py`.
- Pi binding: `services/pi-orchestrator/src/model-bindings.ts` and `services/pi-orchestrator/src/config.ts`.
- Tests: `tests/test_model_control.py`, `tests/test_model_config.py`, `tests/test_model_quality.py`, and `services/pi-orchestrator/tests/model-bindings.test.ts`.
- Preserve revision pinning at StageAttempt creation; never copy credential values into contracts or artifacts.

**New Shared Utility:**
- Python trusted-execution helper: place beside the owning module under `forge/`; Registry helper under `registry/`; model/memory helper under `agent/`; Web-only helper under `web/`.
- TypeScript orchestration helper: place as a focused module in `services/pi-orchestrator/src/`; boundary-specific helpers go in `channels/`, `forge/`, or `tools/`.
- Avoid a generic top-level `utils.py`/`utils.ts`; ownership-specific modules are the existing pattern.

**New Public Documentation:**
- Runtime/product authority: `docs/`, following the precedence in `docs/current-project-state.md`.
- Public guide/reference/course content: `website/src/content/docs/`.
- Architecture changes: update `docs/platform-architecture.md` only when the stable boundary changes and the active plan authorizes it.

## Special Directories

**`.planning/codebase/`:**
- Purpose: Hold generated GSD architecture, structure, stack, integration, convention, testing, and concern maps used by later planning/execution agents.
- Generated: Yes.
- Committed: Determined by the orchestrating GSD workflow; source edits for this mapping are limited to `.planning/codebase/ARCHITECTURE.md` and `.planning/codebase/STRUCTURE.md`.

**`.forge/`:**
- Purpose: Store local runtime databases, caches, sessions, logs, and process metadata.
- Generated: Yes.
- Committed: No; ignored by `.gitignore`.

**`services/pi-orchestrator/.runtime/`:**
- Purpose: Store Pi-local model/auth runtime state resolved by `services/pi-orchestrator/src/config.ts`.
- Generated: Yes.
- Committed: No; ignored by `.gitignore`. Do not read or quote authentication state from this directory.

**`web/static/charts/`:**
- Purpose: Store locally generated chart HTML served by the `/charts` mount in `main.py`.
- Generated: Yes.
- Committed: No for generated `.html`; ignored by `.gitignore`.

**`website/.astro/`, `website/dist/`, `website/node_modules/`:**
- Purpose: Astro metadata, generated public-site output, and installed dependencies.
- Generated: Yes.
- Committed: No; ignored by `.gitignore`.

**`__pycache__/`, `.pytest_cache/`, `*.egg-info/`:**
- Purpose: Python bytecode, pytest cache, and local package metadata.
- Generated: Yes.
- Committed: No; ignored by `.gitignore`.

**`tests/fixtures/`:**
- Purpose: Store deterministic fixture generators and test-only chart/Registry/database inputs.
- Generated: Mixed; generators and canonical fixtures are source, while transient outputs may be generated.
- Committed: Source fixtures are committed; keep them sanitized and deterministic.

**`tests/accuracy/results/`, `tests/benchmark/results/`, `tests/spider2/results/`:**
- Purpose: Hold evaluation outputs.
- Generated: Yes.
- Committed: No; ignored by `.gitignore`.

**`demo/`:**
- Purpose: Provide demo seed scripts and local databases.
- Generated: Mixed; seed scripts are source and `.db` files are local outputs.
- Committed: Seed scripts are committed; `demo/*.db` is ignored by `.gitignore`.

**`tools/`:**
- Purpose: Preserve experimental prototypes and candidate bakeoffs outside production module boundaries.
- Generated: No as a directory; individual rendered/prototype artifacts may be generated.
- Committed: Selected source/prototype assets are committed; do not import them into runtime code.

**`customer-poc-template/`:**
- Purpose: Provide reusable, non-customer-specific PoC scaffolding.
- Generated: No.
- Committed: Yes; examples and templates only, with no customer data or production secrets.

**`.agents/skills/`:**
- Purpose: Provide repository-local AI agent operating instructions.
- Generated: No.
- Committed: Yes; `.agents/skills/forge-query/SKILL.md` is current project guidance.

**`.pi/skills/`:**
- Purpose: Serve as a repository-local Pi Skill mount point; production Skill discovery is configured by `services/pi-orchestrator/src/config.ts` and validated by `services/pi-orchestrator/src/skills.ts`.
- Generated: No.
- Committed: Directory presence may be committed; runtime Skill packages are external/configured and must satisfy the allowlist.

---

*Structure analysis: 2026-08-25*
