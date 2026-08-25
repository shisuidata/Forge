# External Integrations

**Analysis Date:** 2026-08-25

## APIs & External Services

**LLM Providers:**
- Anthropic Messages API - produces structured Forge-query tool calls through the native SDK adapter in `agent/llm.py`.
  - SDK/Client: `anthropic>=0.25` from `pyproject.toml`; `anthropic.Anthropic` is instantiated in `agent/llm.py`.
  - Auth: `LLM_API_KEY`, with provider/model/base URL selected by `LLM_PROVIDER`, `LLM_MODEL`, and `LLM_BASE_URL` in `config.py`.
- OpenAI-compatible Chat Completions - supports OpenAI itself and compatible providers such as Ark, DeepSeek, Tongyi, and MiniMax through `agent/llm.py`; the default endpoint is `/v1/chat/completions` and custom bases are supported.
  - SDK/Client: direct `httpx` requests from `agent/llm.py`, not the OpenAI Python SDK; `httpx[socks]>=0.27` is declared in `pyproject.toml`.
  - Auth: Bearer `LLM_API_KEY`; compatibility and model controls use `LLM_PROVIDER`, `LLM_MODEL`, `LLM_BASE_URL`, `LLM_TOOL_CHOICE`, and timeout/token settings from `config.py`.
- Pi model runtime - the orchestration service delegates model resolution/session execution to `@earendil-works/pi-coding-agent` in `services/pi-orchestrator/src/runtime.ts` and binds an explicit provider/model pair in `services/pi-orchestrator/src/config.ts`.
  - SDK/Client: `@earendil-works/pi-coding-agent` 0.84.2 from `services/pi-orchestrator/package.json`.
  - Auth: provider credentials are resolved by the Pi agent runtime under its configured agent directory; `PI_MODEL_SECRET_REF` can load `ARK_API_KEY` from a mode-0600 file according to `services/pi-orchestrator/src/config.ts`, while `PI_MODEL_PROVIDER` and `PI_MODEL_ID` select the model.

**Embedding Providers:**
- OpenAI-compatible and MiniMax-compatible embeddings - schema retrieval sends either `texts` plus `type` or standard `input` payloads and accepts both provider response shapes in `forge/retriever.py`.
  - SDK/Client: synchronous `requests.post` in `forge/retriever.py`; `requests` is not directly declared in `pyproject.toml`.
  - Auth: Bearer `EMBED_API_KEY`; endpoint/model come from `EMBED_BASE_URL` and `EMBED_MODEL` in `config.py`, whose default base points to SiliconFlow.
- Local retrieval fallback - when embedding credentials are absent, `SchemaRetriever` uses keyword/BM25-style retrieval rather than an external API in `forge/retriever.py`; startup reports this mode from `main.py`.

**Messaging Channels:**
- Feishu/Lark - receives messages and card callbacks, sends replies/cards, and supports managed WebSocket operation or HTTP webhook dispatch in `agent/feishu.py`, `web/feishu_pi.py`, `web/feishu_runtime.py`, and `main.py`.
  - SDK/Client: `lark-oapi>=1.3` from `pyproject.toml`.
  - Auth: `FEISHU_APP_ID` and `FEISHU_APP_SECRET`; HTTP callbacks additionally use `FEISHU_VERIFICATION_TOKEN` and `FEISHU_ENCRYPT_KEY`, all declared in `config.py`.
- DingTalk - channel-neutral messages/actions and ActionCard projection are implemented in `web/dingtalk_pi.py`, but the repository expects an already-verified external DingTalk SDK/Stream callback layer and does not declare a DingTalk SDK dependency in `pyproject.toml`.
  - SDK/Client: shared internal `PiChannelClient` from `web/pi_channel.py`; external DingTalk transport is not detected in the repository manifests.
  - Auth: the adapter forwards to Pi with `PI_CHANNEL_SERVICE_KEY` from `config.py`; DingTalk platform credential handling is outside `web/dingtalk_pi.py`.

**Knowledge Ingestion:**
- RSS/Atom feeds - enabled knowledge sources are fetched over arbitrary configured URLs, parsed with the Python XML library, and stored as reviewable candidates in `agent/knowledge.py`.
  - SDK/Client: `httpx.get` with redirects and a bounded timeout in `agent/knowledge.py`.
  - Auth: none implemented in `agent/knowledge.py`; source URLs are stored in the knowledge-source configuration.
- Web URL fetch - configured pages are fetched, stripped to text, and optionally summarized through the configured LLM in `agent/knowledge.py`.
  - SDK/Client: `httpx` plus the LLM adapter in `agent/llm.py`.
  - Auth: no per-URL authentication is implemented in `agent/knowledge.py`; LLM extraction uses the LLM variables in `config.py`.
- Search-engine API - a `web_search` source type exists but returns zero and has no external search SDK/API in `agent/knowledge.py`; do not plan against it as an active integration.

**Documentation Assets:**
- Google Fonts - the separate Astro documentation site preconnects to and loads Noto Sans SC and JetBrains Mono from Google Fonts in `website/astro.config.mjs`.
  - SDK/Client: browser stylesheet request configured by `website/astro.config.mjs`.
  - Auth: not applicable in `website/astro.config.mjs`.

**Internal Service APIs:**
- Web/Feishu/DingTalk to Pi Orchestrator - Python channel clients submit `ChannelEvent` payloads and poll presentations over private HTTP in `web/pi_client.py` and `web/pi_channel.py`.
  - SDK/Client: `httpx.AsyncClient` and `httpx.request` from `web/pi_client.py` and `web/pi_channel.py`.
  - Auth: `X-Channel-Service-Key` using `PI_CHANNEL_SERVICE_KEY`; Pi validates against `PI_CHANNEL_SERVICE_KEYS` in `services/pi-orchestrator/src/server.ts` and `services/pi-orchestrator/src/config.ts`.
- Pi Orchestrator to Forge - the Node service prepares/approves/cancels QueryRuns, searches context, writes memory, and creates/retrieves reports through `/api/internal/*` in `services/pi-orchestrator/src/forge/query-run-client.ts` and the FastAPI routes under `web/routes/`.
  - SDK/Client: Node built-in `fetch` in `services/pi-orchestrator/src/forge/client.ts` and `services/pi-orchestrator/src/forge/query-run-client.ts`.
  - Auth: `X-Pi-Service-Key` using `FORGE_PI_SERVICE_KEY`, validated against `PI_SERVICE_API_KEYS` by `web/auth.py`; the legacy prepare-query client can use `FORGE_API_KEY` as `X-API-Key` in `services/pi-orchestrator/src/forge/client.ts`.
- External DATA Skills filesystem - Pi loads an exact allowlist of skill `SKILL.md` resources from `SHISUI_DATA_SKILLS_DIR` and disables ambient skills, extensions, prompt templates, context files, and built-in tools in `services/pi-orchestrator/src/skills.ts` and `services/pi-orchestrator/src/runtime.ts`.
  - SDK/Client: `DefaultResourceLoader` from `@earendil-works/pi-coding-agent` in `services/pi-orchestrator/src/skills.ts`.
  - Auth: filesystem access plus the hardcoded allowlist in `services/pi-orchestrator/src/skills.ts`; skills never receive direct database access through the restricted session in `services/pi-orchestrator/src/runtime.ts`.

## Data Storage

**Databases:**
- Customer/source database - Forge introspects and executes through SQLAlchemy in `registry/sync.py` and `forge/executor.py`.
  - Connection: `DATABASE_URL`, `SQL_DIALECT`, `DATASOURCE_ID`, and the execution/read-only gates in `config.py`.
  - Client: SQLAlchemy >=2.0 with PyMySQL >=1.1 and psycopg2-binary >=2.9 from `pyproject.toml`; SQLite uses the standard driver, while BigQuery/Snowflake need separately installed SQLAlchemy dialect packages not declared in `pyproject.toml`.
- Memory database - defaults to local SQLite but accepts SQLAlchemy SQLite/PostgreSQL/MySQL URLs in `agent/db.py`.
  - Connection: `MEMORY_DB_URL` or `MEMORY_DB_PATH` from `config.py`.
  - Client: SQLAlchemy in `agent/db.py`, with SQLite WAL and busy-timeout configuration.
- Audit and feedback database - local SQLite stores review/audit and feedback records together in `agent/audit.py` and `agent/feedback.py`.
  - Connection: `AUDIT_DB_PATH` from `config.py`, defaulting to `forge_audit.db`.
  - Client: `aiosqlite` in `agent/audit.py` and `agent/feedback.py`.
- QueryRun state - local SQLite persists review hashes, approval state, execution status, and results in `forge/query_runs.py`.
  - Connection: `QUERY_RUN_DB_PATH` from `config.py`, defaulting under `.forge/`.
  - Client: `aiosqlite` in `forge/query_runs.py`.
- Pi orchestration state - local SQLite persists tasks, conversations, events, artifacts, attempts, skill policy, and product projections in `services/pi-orchestrator/src/sqlite-store.ts`.
  - Connection: `PI_ORCHESTRATOR_STATE_DB` parsed by `services/pi-orchestrator/src/config.ts`, defaulting under the Pi runtime agent directory.
  - Client: Node built-in `DatabaseSync` from `node:sqlite` in `services/pi-orchestrator/src/sqlite-store.ts`.
- Registry Studio, model control, and report metadata - separate local SQLite files are managed by `registry/studio.py`, `agent/model_control.py`, and `forge/reporting.py`.
  - Connection: `REGISTRY_STUDIO_DB_PATH`, `MODEL_CONTROL_DB_PATH`, and `REPORT_DB_PATH` from `config.py` and `agent/model_config.py`.
  - Client: Python `sqlite3` in `registry/studio.py`, `agent/model_control.py`, and `forge/reporting.py`.

**File Storage:**
- Registry and semantic configuration are local JSON/YAML files selected by `REGISTRY_PATH`, `METRICS_PATH`, `DISAMBIGUATIONS_PATH`, `CONVENTIONS_PATH`, and `BUSINESS_CONTEXT_PATH` in `config.py`; schema sync writes through `registry/sync.py`.
- Immutable report HTML/PDF/PPTX files are written atomically with restrictive permissions under `REPORT_ARTIFACT_DIR` by `forge/reporting.py`; no object-storage SDK is declared in `pyproject.toml`.
- Product assets and generated chart files are served from local `web/static/` and `web/static/charts/` mounts created by `main.py`; the product shell explicitly avoids an external CDN in `main.py`.
- A remote object/file-storage integration is not detected in `pyproject.toml`, `services/pi-orchestrator/package.json`, or `config.py`.

**Caching:**
- Verified SQL reuse uses a local SQLite cache with NumPy cosine similarity in `forge/cache.py`; the default file lives beside the active Registry under a `.forge/` directory.
- In-memory singletons cache SQLAlchemy engines and runtime resources in `forge/executor.py` and `agent/db.py`; no Redis or Memcached dependency is declared in `pyproject.toml` or `services/pi-orchestrator/package.json`.

## Authentication & Identity

**Auth Provider:**
- Custom Forge authentication - Web sessions use HMAC-SHA256 signed cookies, API routes accept configured API keys or a valid Web session, and Pi internal routes require a dedicated service key in `web/auth.py`.
  - Implementation: `forge_session` is HttpOnly, SameSite=Lax, optionally Secure, and expires after seven days in `web/auth.py`; credentials are sourced from `AUTH_PASSWORD`, `AUTH_API_KEYS`, `AUTH_COOKIE_SECURE`, and `PI_SERVICE_API_KEYS` in `config.py`.
- Pi service authentication - channel and admin HTTP surfaces require separate constant-time compared service keys in `services/pi-orchestrator/src/server.ts`.
  - Implementation: `X-Channel-Service-Key` and `X-Admin-Service-Key` are checked against `PI_CHANNEL_SERVICE_KEYS` and `PI_ADMIN_SERVICE_KEYS` parsed by `services/pi-orchestrator/src/config.ts`; an empty allowlist denies access.
- Channel identity mapping - external Feishu/DingTalk identifiers are resolved into Forge org/team/user identity through `services/pi-orchestrator/src/channels/identity.ts` and the path configured by `PI_CHANNEL_IDENTITY_MAP` in `services/pi-orchestrator/src/config.ts`.
- Hosted OAuth/OIDC/SAML integration is not detected in `pyproject.toml`, `services/pi-orchestrator/package.json`, or `web/auth.py`.

## Monitoring & Observability

**Error Tracking:**
- Not detected: no Sentry, OpenTelemetry, Prometheus, Datadog, or equivalent dependency is declared in `pyproject.toml` or `services/pi-orchestrator/package.json`.

**Logs:**
- Python uses the standard `logging` module with stderr and optional file output selected by `LOG_LEVEL` and `LOG_FILE` in `main.py` and `config.py`; service startup logs database, LLM, embedding, QueryRun reconciliation, and Feishu runtime status in `main.py`.
- Pi uses process output and structured HTTP error responses from the native Node server in `services/pi-orchestrator/src/server.ts`; no external log sink is configured in `services/pi-orchestrator/src/config.ts`.
- Health/readiness are exposed at `/health` and `/health/readiness` in `main.py`; production-profile checks cover database read-only confirmation and other gates in `forge/readiness.py`.
- Operational audit records live in SQLite through `agent/audit.py`, while durable task/stage events live in `services/pi-orchestrator/src/sqlite-store.ts`; these are application records, not an external telemetry backend.

## CI/CD & Deployment

**Hosting:**
- The FastAPI application has a container image in `Dockerfile` and development/production compose entry points in `docker-compose.yml` and `docker-compose.prod.yml`; the documented production profile expects reverse proxy/HTTPS, persistent volumes, and a read-only customer database in `website/src/content/docs/course/10-deployment.md`.
- The Pi Orchestrator is a separate Node HTTP service started from `services/pi-orchestrator/src/server.ts` using scripts in `services/pi-orchestrator/package.json`; no dedicated container manifest for that subservice is detected by the repository manifest scan.
- The Astro documentation site builds statically from `website/package.json`; a `website/.vercel/` directory exists, but `.github/workflows/ci.yml` contains no website deployment job.

**CI Pipeline:**
- GitHub Actions in `.github/workflows/ci.yml` runs pytest on pushes and pull requests to `main`, then runs a compatibility matrix against SQLite, PostgreSQL 16, and MySQL 8.0.
- `.github/workflows/ci.yml` does not run `services/pi-orchestrator/package.json` typecheck/tests, prototype tests/builds, the Astro build, image publication, or deployment.

## Environment Configuration

**Required env vars:**
- Core LLM: `LLM_PROVIDER`, `LLM_API_KEY`, `LLM_MODEL`, and optional `LLM_BASE_URL`; compatibility fallback variables for MiniMax are parsed in `config.py`.
- Embeddings when vector retrieval is desired: `EMBED_API_KEY`, `EMBED_BASE_URL`, and `EMBED_MODEL` in `config.py`; without a key the service falls back to non-vector retrieval in `main.py` and `forge/retriever.py`.
- Source database/execution: `DATABASE_URL`, `DATABASE_READONLY_CONFIRMED`, optional `SQL_DIALECT`, and execution limits/toggles in `config.py`; `forge/readiness.py` fails the production gate when execution is enabled without a configured read-only connection.
- Feishu when enabled: `FEISHU_APP_ID`, `FEISHU_APP_SECRET`, optional webhook verification/encryption values, `FEISHU_PI_ENABLED`, and `PI_CHANNEL_SERVICE_KEY` in `config.py` and `web/feishu_runtime.py`.
- Forge authentication when enabled: `AUTH_ENABLED`, `AUTH_PASSWORD`, `AUTH_API_KEYS`, and `AUTH_COOKIE_SECURE` in `config.py` and `web/auth.py`.
- Pi control plane: `PI_ORCHESTRATOR_HOST`, `PI_ORCHESTRATOR_PORT`, `PI_ORCHESTRATOR_STATE_DB`, `PI_CHANNEL_SERVICE_KEYS`, `PI_ADMIN_SERVICE_KEYS`, `FORGE_BASE_URL`, `FORGE_PI_SERVICE_KEY`, `PI_MODEL_PROVIDER`, and `PI_MODEL_ID` in `services/pi-orchestrator/src/config.ts`.
- Forge-to-Pi client: `PI_ORCHESTRATOR_ENABLED`, `PI_ORCHESTRATOR_URL`, and `PI_CHANNEL_SERVICE_KEY` in `config.py`, consumed by `web/pi_client.py` and `web/pi_channel.py`.
- Persistent storage locations are configurable through `MEMORY_DB_URL`, `MEMORY_DB_PATH`, `AUDIT_DB_PATH`, `QUERY_RUN_DB_PATH`, `REPORT_DB_PATH`, `REPORT_ARTIFACT_DIR`, `REGISTRY_STUDIO_DB_PATH`, and `MODEL_CONTROL_DB_PATH` in `config.py` and `agent/model_config.py`.

**Secrets location:**
- `config.py` supports process environment, root `.env`, and `forge.yaml`; a root `.env` exists but no value from it is read or reproduced in this map. Example-only templates exist at `.env.example`, `.env.production.example`, and `forge.yaml.example`.
- The isolated Feishu process receives only an explicit environment allowlist and sets `FORGE_DISABLE_DOTENV=true` in `web/feishu_runtime.py`, preventing the full root `.env` from entering that subprocess.
- Pi model credentials can be loaded from an external mode-0600 secret file referenced by `PI_MODEL_SECRET_REF` in `services/pi-orchestrator/src/config.ts`; the Pi runtime also references agent-local auth/model catalogs in `services/pi-orchestrator/src/runtime.ts`, whose contents must remain outside codebase maps.

## Webhooks & Callbacks

**Incoming:**
- `POST /webhook/feishu` in `main.py` dispatches Feishu event-subscription and card callback payloads through `lark-oapi`; it returns a conflict when the managed WebSocket runtime in `web/feishu_runtime.py` is enabled.
- Feishu WebSocket callbacks enter through the long-running Lark client configured in `agent/feishu.py` or the thin Pi adapter in `web/feishu_pi.py`, avoiding a public webhook URL.
- DingTalk callback transport is not implemented in-repository; `web/dingtalk_pi.py` accepts already-verified event/action identifiers from an external SDK/Stream layer.

**Outgoing:**
- Feishu messages, replies, and interactive cards are sent through `lark-oapi` clients in `agent/feishu.py` and `web/feishu_pi.py`.
- LLM and embedding requests leave through `agent/llm.py` and `forge/retriever.py`; RSS and arbitrary configured page fetches leave through `agent/knowledge.py`.
- Internal callbacks and polling flow between Forge and Pi over private HTTP through `web/pi_client.py`, `web/pi_channel.py`, `services/pi-orchestrator/src/forge/client.ts`, and `services/pi-orchestrator/src/forge/query-run-client.ts`.
- A generic outgoing webhook delivery subsystem is not detected in `pyproject.toml`, `config.py`, `agent/`, `forge/`, or `services/pi-orchestrator/src/`.

---

*Integration audit: 2026-08-25*
