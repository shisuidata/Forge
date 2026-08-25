# Technology Stack

**Analysis Date:** 2026-08-25

## Languages

**Primary:**
- Python 3.11+ - FastAPI service, trusted query compiler/executor, Registry, agent adapters, report rendering, CLI, and test suite in `main.py`, `config.py`, `forge/`, `registry/`, `agent/`, `web/`, `scripts/`, and `tests/`; the supported floor is declared in `pyproject.toml`.
- TypeScript 5.9.3 - strict ES2022/NodeNext Pi orchestration control plane in `services/pi-orchestrator/src/`, with contract and integration tests in `services/pi-orchestrator/tests/`; compiler settings live in `services/pi-orchestrator/tsconfig.json`.

**Secondary:**
- JavaScript (ES modules) - Vite-based product-shell and chart evaluation tools in `tools/web-product-shell-prototype/`, `tools/chart-storytelling-echarts-candidate/`, and `tools/chart-engine-bakeoff/`.
- Astro/HTML/CSS - static documentation site in `website/src/` and server-rendered product/admin UI in `web/templates/` plus `web/static/`; `website/astro.config.mjs` configures the Astro surface.
- SQL - deterministic compiler output and SQLAlchemy-backed execution/introspection in `forge/compiler.py`, `forge/executor.py`, `registry/sync.py`, and `agent/db.py`; operational schemas are embedded in `agent/audit.py`, `forge/query_runs.py`, `forge/reporting.py`, and `services/pi-orchestrator/src/sqlite-store.ts`.
- JSON, JSON Schema, and YAML - Forge DSL/contracts and Registry/configuration data in `forge/schema.json`, `agent/contracts/`, `registry/data/`, `registry/*.yaml`, and `forge.yaml.example`.
- POSIX shell - developer bootstrap, production smoke, and bot helpers in `scripts/bootstrap-dev.sh`, `scripts/production-smoke.sh`, `scripts/demo-setup.sh`, and `scripts/bot.sh`.

## Runtime

**Environment:**
- CPython 3.11+ - `pyproject.toml` declares `requires-python = ">=3.11"`; `Dockerfile` and `.github/workflows/ci.yml` standardize container and CI execution on Python 3.11.
- Uvicorn ASGI - `Dockerfile` runs `uvicorn main:app` on port 8000; `main.py` owns the FastAPI lifespan, routes, and static mounts.
- Node.js 22.19+ - `services/pi-orchestrator/package.json` requires `>=22.19`; this floor supports the built-in `node:sqlite` store used by `services/pi-orchestrator/src/sqlite-store.ts`.
- Browser tooling - Chromium is required for Playwright E2E tests declared in `pyproject.toml` and for PDF export through the headless browser command in `forge/reporting.py`.

**Package Manager:**
- Python: pip with setuptools editable installation from `pyproject.toml`; no Python dependency lockfile is present at the repository root.
- Node.js: npm with lockfile version 3 in `services/pi-orchestrator/package-lock.json`, `website/package-lock.json`, and each `tools/*/package-lock.json`; npm itself is not version-pinned in the manifests.
- Lockfiles: present for every Node subproject listed above; missing for the Python project in `pyproject.toml`.

## Frameworks

**Core:**
- FastAPI >=0.110 and Uvicorn >=0.29 - HTTP API, Web/Product Shell, admin routes, readiness probes, internal service endpoints, and Feishu webhook in `main.py`, `web/router.py`, and `web/routes/`.
- Jinja2 >=3.1 - server-side HTML templates via `Jinja2Templates` in `web/router.py` and `web/routes/settings.py`, with templates in `web/templates/`.
- SQLAlchemy >=2.0 - database-neutral connections, execution, model-quality checks, memory persistence, and schema introspection in `forge/executor.py`, `registry/sync.py`, `agent/db.py`, and `agent/model_quality.py`.
- @earendil-works/pi-coding-agent 0.84.2 - restricted agent sessions, resource loading, tools, and model runtime in `services/pi-orchestrator/src/runtime.ts`, `services/pi-orchestrator/src/skills.ts`, and `services/pi-orchestrator/src/skill-executor.ts`.
- TypeBox 1.3.7 - runtime schemas and validation for governance, product projections, and structured artifact tools in `services/pi-orchestrator/src/governance-contracts.ts`, `services/pi-orchestrator/src/product-projections.ts`, and `services/pi-orchestrator/src/structured-artifact-tools.ts`.
- Astro 6.4.8 with Starlight 0.38.2 - static documentation portal configured by `website/astro.config.mjs`; resolved versions are recorded in `website/package-lock.json`.

**Testing:**
- pytest >=8.0 and pytest-asyncio >=0.23 - Python unit, contract, integration, compatibility, and API tests under `tests/`, configured by `[tool.pytest.ini_options]` in `pyproject.toml`.
- Playwright >=1.40 and pytest-playwright >=0.5 - browser E2E coverage in `tests/test_e2e.py`, installed through the `dev` extra in `pyproject.toml`.
- Node built-in test runner - Pi tests use `node --import tsx --test tests/*.test.ts` from `services/pi-orchestrator/package.json`; prototype tests use `node --test` from each `tools/*/package.json`.

**Build/Dev:**
- setuptools - Python package and `forge` CLI entry point build in `pyproject.toml`.
- TypeScript 5.9.3 and tsx 4.20.6 - no-emit typechecking, development, tests, and direct TypeScript startup in `services/pi-orchestrator/package.json` and `services/pi-orchestrator/tsconfig.json`.
- Vite 8.2.2 - isolated UI/chart prototype development and builds in `tools/web-product-shell-prototype/package.json`, `tools/chart-storytelling-echarts-candidate/package.json`, and `tools/chart-engine-bakeoff/package.json`.
- Astro build pipeline - static site development/build/preview scripts in `website/package.json`; Sharp 0.34.5 handles image processing according to `website/package-lock.json`.
- Docker - Python 3.11 slim image in `Dockerfile`, with development and production compose entry points at `docker-compose.yml` and `docker-compose.prod.yml`.

## Key Dependencies

**Critical:**
- jsonschema >=4.0 - validates Forge JSON and packaged task/artifact contracts in `forge/compiler.py` and `agent/contracts/__init__.py`.
- lark-oapi >=1.3 - Feishu event dispatch, WebSocket transport, messages, and cards in `main.py`, `agent/feishu.py`, and `web/feishu_pi.py`.
- anthropic >=0.25 - native Anthropic Messages API adapter with structured tool calls in `agent/llm.py`.
- httpx[socks] >=0.27 - OpenAI-compatible LLM calls, internal Forge/Pi calls, RSS, and URL retrieval in `agent/llm.py`, `web/pi_client.py`, `web/pi_channel.py`, and `agent/knowledge.py`.
- NumPy >=1.24 - vector retrieval and embedding-backed SQL cache similarity in `forge/retriever.py` and `forge/cache.py`.
- PyYAML >=6.0 - Registry, application, model, business-context, and runtime settings parsing in `config.py`, `agent/model_config.py`, `forge/context.py`, and `registry/business_context.py`.
- python-pptx >=1.0 - deterministic PPTX report export in `forge/reporting.py`.
- `requests` - synchronous embedding HTTP calls in `forge/retriever.py`; it is imported at runtime but is not declared directly in `pyproject.toml`, so new environments must not assume it is pinned.

**Infrastructure:**
- aiosqlite >=0.20 and Python/Node built-in SQLite clients - query-run, audit, feedback, Registry Studio, report, model-control, memory, cache, and Pi task state in `forge/query_runs.py`, `agent/audit.py`, `registry/studio.py`, `forge/reporting.py`, `agent/model_control.py`, `agent/db.py`, `forge/cache.py`, and `services/pi-orchestrator/src/sqlite-store.ts`.
- PyMySQL >=1.1 and psycopg2-binary >=2.9 - SQLAlchemy drivers for MySQL/MariaDB and PostgreSQL in `pyproject.toml`; compatibility services are exercised by `.github/workflows/ci.yml`.
- python-dotenv >=1.0 - root environment loading in `config.py`; the isolated Feishu subprocess explicitly disables dotenv loading in `web/feishu_runtime.py`.
- python-multipart >=0.0.9 - FastAPI form parsing for login and admin settings routes in `web/router.py` and `web/routes/settings.py`.
- ECharts 6.1.0, AntV G2 5.4.8, Vega/Vega-Lite 6.4.x - isolated chart candidate/bakeoff dependencies in `tools/chart-storytelling-echarts-candidate/package.json` and `tools/chart-engine-bakeoff/package.json`, not dependencies of the FastAPI production package in `pyproject.toml`.

## Configuration

**Environment:**
- Use the precedence implemented by `config.py`: process environment and root `.env`, then `forge.yaml`, then code defaults. A root `.env` exists but its contents are intentionally excluded from this map; templates are present at `.env.example`, `.env.production.example`, and `forge.yaml.example`.
- Configure core query behavior through `LLM_PROVIDER`, `LLM_API_KEY`, `LLM_MODEL`, optional `LLM_BASE_URL`, `DATABASE_URL`, `DATABASE_READONLY_CONFIRMED`, and optional embedding variables declared in `config.py`.
- Configure the Pi service only through process environment parsed by `services/pi-orchestrator/src/config.ts`; its state defaults under `services/pi-orchestrator/.runtime/`, while skill discovery defaults to an external sibling DATA-skill tree and can be overridden by `SHISUI_DATA_SKILLS_DIR`.
- Treat `.agents/skills/forge-query/SKILL.md` as the repository query-execution constraint: use registered semantics, compile through `forge.cli`, show exact SQL, and stop before database execution unless the normal review/execution flow is explicitly authorized.
- Keep the product shell self-contained: `main.py` mounts local assets from `web/static/` and states that the Product Shell must not depend on external CDNs; the separate documentation site in `website/astro.config.mjs` does load Google Fonts.

**Build:**
- Python build/package metadata and pytest defaults: `pyproject.toml`.
- ASGI image and startup command: `Dockerfile`.
- TypeScript strict compiler contract: `services/pi-orchestrator/tsconfig.json`.
- Pi runtime scripts and Node engine floor: `services/pi-orchestrator/package.json`.
- Documentation build: `website/package.json`, `website/astro.config.mjs`, and `website/tsconfig.json`.
- Continuous integration: `.github/workflows/ci.yml`; it runs Python tests and database compatibility jobs, with no Node typecheck/test or deployment job declared there.

## Platform Requirements

**Development:**
- Install Python 3.11+ and the editable `dev` extra from `pyproject.toml`; use Node 22.19+ and npm for `services/pi-orchestrator/` even though older website guidance in `website/src/content/docs/guides/installation.md` states a lower Node floor.
- Install Chromium for `tests/test_e2e.py` and report PDF export in `forge/reporting.py`; install Docker when using `docker-compose.yml` or the production template `docker-compose.prod.yml`.
- Supply a Registry rooted at paths configured in `config.py`; the query skill requires `registry/data/schema.registry.json` and relevant semantic Registry files according to `.agents/skills/forge-query/SKILL.md`.

**Production:**
- Deploy the FastAPI surface from `Dockerfile` behind HTTPS/reverse proxy, persist Registry and operational databases/artifacts, and connect through a read-only customer database account as specified in `website/src/content/docs/course/10-deployment.md` and enforced by `forge/readiness.py`.
- PostgreSQL is the preferred production datastore, MySQL/MariaDB is a PoC-compatible path, and SQLite is the demo/test default; BigQuery and Snowflake have compiler dialect paths but not equivalent sync/execution evidence, as documented in `website/src/content/docs/course/10-deployment.md` and represented by `services/pi-orchestrator/src/forge/client.ts`.
- Run the Pi control plane as a separate Node service on its configurable private bind address from `services/pi-orchestrator/src/config.ts`; persist its SQLite state and protect channel/admin/Forge service-key boundaries implemented by `services/pi-orchestrator/src/server.ts` and `web/auth.py`.

---

*Stack analysis: 2026-08-25*
