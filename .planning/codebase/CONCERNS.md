# Codebase Concerns

**Analysis Date:** 2026-08-25

## Tech Debt

**Runtime governance exists as contract evidence, not enforcement:**
- Issue: The Governance Action Catalog reports Contract Coverage at 100% while Runtime Governance Coverage is 0%. Python contract code explicitly states that it is not wired into production authorization, and core Pi Task endpoints remain outside the authenticated channel/admin gates.
- Files: `agent/contracts/governance_semantics.py`, `services/pi-orchestrator/src/governance-contracts.ts`, `services/pi-orchestrator/src/server.ts`, `docs/forge-enterprise-evolution-plan.md`, `docs/current-project-state.md`
- Impact: A complete schema/fixture corpus can be mistaken for a production policy-enforcement point. Task creation, task inspection, artifact/event inspection, query preparation, query approval, analysis, and report rendering have no mandate/policy enforcement at the Pi HTTP boundary.
- Fix approach: Preserve the documented 0% runtime status until an explicitly approved Runtime phase adds fail-closed PEPs to every supported action, negative authorization tests, and machine-readable coverage evidence. Do not count contract tests as runtime coverage.

**Single-admin identity is embedded across the Product path:**
- Issue: Product BFF identity resolves to the literal `web_admin`; Web task creation uses `org_default`, `team_default`, and `web_admin`; configured scopes are deployment-wide rather than derived from an authenticated principal.
- Files: `web/routes/product.py`, `web/router.py`, `config.py`, `services/pi-orchestrator/src/channels/identity.ts`
- Impact: The implementation supports the documented single-user private-control-plane slice, but it cannot safely represent multiple requesters, stewards, approvers, or workspaces. Treating this projection as tenant-ready would create cross-user authorization risk.
- Fix approach: Keep the single-user boundary explicit. Before approving multi-user Runtime work, replace literal identities with authenticated PrincipalContext-to-scope resolution and prove cross-org, cross-team, cross-user, and cross-channel denial paths.

**Table ACLs fail open when no rows exist:**
- Issue: An absent `team_table_acl` configuration returns `None`, which means every registered table is visible to the LLM and compiler.
- Files: `agent/tenant.py`, `agent/agent.py`, `agent/llm.py`
- Impact: A newly created or unmapped team receives broad schema visibility rather than no access. This is acceptable only inside the explicitly trusted single-user deployment boundary.
- Fix approach: For any production multi-user profile, distinguish “ACL not configured” from “unrestricted by explicit policy” and fail closed unless an authorized policy grants a table set.

**Large modules combine unrelated responsibilities:**
- Issue: `web/router.py` is 2,955 lines, `services/pi-orchestrator/src/application.ts` is 2,103 lines, `forge/lint.py` is 1,711 lines, and `forge/compiler.py` is 1,532 lines. HTTP routing, Product Shell rendering, legacy endpoints, knowledge workflows, state transitions, artifact production, and validation logic are concentrated in a few files.
- Files: `web/router.py`, `services/pi-orchestrator/src/application.ts`, `forge/lint.py`, `forge/compiler.py`
- Impact: Small changes have wide review surfaces; task-state and query-language invariants are easy to update on one path but miss on another; merge conflicts and regression risk rise as the product evolves.
- Fix approach: Split only along proven responsibility seams: move remaining route families from `web/router.py` into `web/routes/`, isolate Pi stage handlers behind the existing stores/contracts, and keep compiler/linter rule ownership explicit. Preserve behavior with focused contract tests during each extraction.

**Legacy and Pi execution paths coexist:**
- Issue: The legacy Agent API and legacy Feishu dispatcher remain alongside the Pi Orchestrator path. Several defaults still select legacy behavior (`FEISHU_PI_ENABLED=false`) while legacy Web APIs are separately disabled (`LEGACY_AGENT_API_ENABLED=false`).
- Files: `agent/agent.py`, `agent/feishu.py`, `web/feishu.py`, `web/feishu_pi.py`, `web/router.py`, `main.py`, `config.py`
- Impact: Operators can select combinations with different approval, identity, memory, and execution semantics. Fixes on the Pi path do not automatically protect the legacy path.
- Fix approach: Do not expand either path during S0. When a cutover is approved, choose one supported channel runtime, migrate every caller, remove the other dispatcher/endpoints, and retain rollback through deployment revision rather than duplicate live code.

**Python dependency resolution is not reproducible:**
- Issue: Runtime dependencies use open lower bounds and the repository has no Python lockfile; the container runs `pip install -e .` against whatever compatible releases are available.
- Files: `pyproject.toml`, `Dockerfile`
- Impact: Builds made on different dates can resolve different FastAPI, SQLAlchemy, Anthropic, HTTPX, NumPy, and SDK behavior despite identical source. Dependency regressions cannot be reproduced reliably.
- Fix approach: Add a reviewed lock/constraints artifact for application and container builds while retaining `pyproject.toml` as the package declaration; refresh dependencies through an explicit, tested update workflow.

## Known Bugs

**Legacy Feishu approval does not execute the approved query:**
- Symptoms: A legacy Feishu card approval calls `forge_agent.approve()` and sends its text response, but the handler stops at a TODO and never executes SQL or returns query results.
- Files: `agent/feishu.py`, `main.py`, `config.py`
- Trigger: Run the HTTP webhook/legacy dispatcher with Pi Feishu disabled, generate a SQL review card, and press the approve action.
- Workaround: Use the authenticated Pi Feishu Runtime path in `web/feishu_pi.py`; do not claim execution support for the legacy dispatcher.

**Configured `web_search` knowledge sources always produce zero candidates:**
- Symptoms: Knowledge collection accepts a source of type `web_search`, marks its run timestamp, and returns zero collected items without performing a search.
- Files: `agent/knowledge.py`, `web/router.py`
- Trigger: Add a `web_search` source through `/admin/knowledge` and invoke collection.
- Workaround: Use reviewed RSS, URL, or document sources within their security boundary; do not expose `web_search` as operational until its connector is explicitly approved and implemented.

**PDF export fails in the repository’s standard container image:**
- Symptoms: Report publication can produce HTML and PPTX, but PDF status becomes `failed` when no Chrome/Chromium executable is present.
- Files: `forge/reporting.py`, `Dockerfile`
- Trigger: Build and run the supplied `Dockerfile`, then publish a report that requests PDF export; the image installs Python dependencies only and does not install Chrome/Chromium.
- Workaround: Use a deployment image with a compatible headless browser installed, or disable the PDF acceptance claim for that image.

## Security Considerations

**Insecure application defaults permit unauthenticated reads and raw query execution:**
- Risk: `AUTH_ENABLED` defaults to false while SQL execution and raw SQL are enabled by default; the FastAPI server binds to `0.0.0.0`. With authentication disabled, `/api/execute-raw`, admin routes, Product APIs, and report routes are reachable without credentials. The read-only SQL validator reduces mutation risk but does not prevent confidential-data reads.
- Files: `config.py`, `web/auth.py`, `web/router.py`, `web/routes/reports.py`, `forge/executor.py`, `main.py`
- Current mitigation: `forge/readiness.py` marks auth, raw SQL, read-only confirmation, timeout, and secure-cookie failures for the production profile; SQL is limited to one SELECT/WITH statement and bounded rows/time.
- Recommendations: Make production startup or deployment gating consume readiness and fail closed; use a database-enforced read-only role; disable raw SQL by default outside development; require HTTPS secure cookies and authentication before network exposure.

**Pi core Task HTTP APIs are unauthenticated:**
- Risk: Only channel ingress, Product conversation/detail projections, and Skill-policy routes call channel/admin authentication. Core Task list/create/get, event/artifact/attempt reads, and stage mutation routes do not authenticate or authorize a principal.
- Files: `services/pi-orchestrator/src/server.ts`, `services/pi-orchestrator/tests/server.test.ts`, `services/pi-orchestrator/src/config.ts`
- Current mitigation: Pi binds to `127.0.0.1` by default, and the Web BFF applies its own authentication and configured-scope checks before selected calls.
- Recommendations: Treat loopback as the only supported boundary while Runtime Governance Coverage is 0%. Before proxying or binding Pi externally, require service authentication plus object-scope authorization on every core route and add negative tests for every action.

**Knowledge URL collection permits SSRF and unbounded response downloads:**
- Risk: Admin-supplied RSS and URL sources are fetched with redirects enabled and no scheme, DNS, private-address, redirect-target, content-type, or response-size policy. The entire response is materialized before truncating extracted text.
- Files: `agent/knowledge.py`, `web/router.py`
- Current mitigation: Knowledge management sits under the admin router, HTTP requests have a 15-second timeout, and collected items require review before confirmation.
- Recommendations: Allowlist `http`/`https`, resolve and reject loopback/link-local/private/metadata addresses on every redirect, stream with a byte cap, restrict content types, and run collection outside the request event loop.

**Knowledge document upload has no request-size cap:**
- Risk: The upload route reads the complete file into memory, creates a chunk list for the complete decoded document, and only then limits LLM processing to five chunks. An authenticated or unauthenticated-default caller can exhaust memory.
- Files: `web/router.py`, `main.py`, `config.py`
- Current mitigation: PDF input is rejected and only the first five text chunks are sent to the LLM.
- Recommendations: Enforce Content-Length and streamed byte limits before buffering, reject unsupported extensions/MIME types, and cap decoded character count before chunk creation.

**Retriever cache deserialization trusts pickle content:**
- Risk: `pickle.load()` executes Python object reconstruction from the schema embedding cache. A user or process able to replace that cache can execute code when the retriever initializes.
- Files: `forge/retriever.py`, `agent/llm.py`
- Current mitigation: The cache path is local to the configured Registry directory and is expected to be deployment-controlled.
- Recommendations: Replace pickle with a non-executable format such as NumPy data plus validated JSON metadata; until then, enforce owner-only directory/file permissions and never accept a cache from uploaded or shared Registry content.

**Audit and operational stores retain sensitive material without lifecycle controls:**
- Risk: Audit rows store full user questions, Forge JSON, SQL, and errors in plaintext; local QueryRun, memory, report, and Pi SQLite/artifact stores also contain business data. Audit has no retention or deletion mechanism, and its default file creation relies on process umask.
- Files: `agent/audit.py`, `forge/query_runs.py`, `forge/reporting.py`, `agent/memory/ems.py`, `services/pi-orchestrator/src/sqlite-store.ts`, `config.py`
- Current mitigation: Report artifacts are written with restrictive modes, report share tokens are hashed, and `.dockerignore` excludes local runtime stores.
- Recommendations: Define data classification, retention, deletion, backup, at-rest encryption, and file-permission checks before real customer data is introduced; keep S0 work inside an explicitly authorized non-production data boundary.

**API keys may be accepted in URL query strings:**
- Risk: `api_key` query parameters can leak through browser history, reverse-proxy access logs, referrer handling, and copied URLs.
- Files: `web/auth.py`
- Current mitigation: Header-based `X-API-Key` and session cookies are also supported, and comparisons use constant-time HMAC comparison.
- Recommendations: Remove query-string credential support in a clean cutover and accept credentials only in headers or secure cookies.

**Login has no brute-force throttling:**
- Risk: The password login endpoint has no request-rate limit, account lockout, or incremental delay.
- Files: `web/router.py`, `web/auth.py`
- Current mitigation: Password comparison is constant-time and the session cookie is HTTP-only with SameSite=Lax; Secure is configurable.
- Recommendations: Add reverse-proxy or application-level rate limiting and security telemetry before exposing the login surface beyond a trusted private network.

## Performance Bottlenecks

**Container context and image include large repository artifacts:**
- Problem: `COPY . .` includes `local_sqlite.zip` (approximately 435 MB) and `demo/large_demo.db` (approximately 25 MB) because neither path is excluded.
- Files: `Dockerfile`, `.dockerignore`, `local_sqlite.zip`, `demo/large_demo.db`
- Cause: The container build copies the whole repository after installing the package.
- Improvement path: Exclude archives, demo databases, tests, evidence, and build-only material; copy only runtime packages/templates/static assets and required configuration examples.

**Blocking knowledge work runs inside async FastAPI handlers:**
- Problem: Knowledge collection performs synchronous HTTP and LLM calls, while document import performs synchronous LLM calls, directly inside `async def` routes.
- Files: `web/router.py`, `agent/knowledge.py`, `agent/llm.py`
- Cause: `/admin/knowledge/collect*` and `/admin/knowledge/import/upload` call blocking functions without `asyncio.to_thread`, a worker queue, or background-job isolation.
- Improvement path: Move bounded collection/import work to a controlled worker or thread boundary, return a job identifier, and expose progress/result state rather than occupying the event loop.

**Product Registry summary reparses complete files on every request:**
- Problem: Each Data Summary and Workspace request reads, parses, and hashes the complete schema and metrics Registry files.
- Files: `web/routes/product.py`, `config.py`
- Cause: `_registry_summary()` computes revision and counts from file bytes with no revision cache.
- Improvement path: Cache by stable file metadata/content revision and invalidate after Registry publication; keep the response’s source revision deterministic.

**Pi persistence is synchronous and single-process:**
- Problem: Node’s synchronous SQLite API performs transactions, JSON serialization/parsing, and list reads on the event-loop thread; per-task event, attempt, and artifact reads have no store-level result cap.
- Files: `services/pi-orchestrator/src/sqlite-store.ts`, `services/pi-orchestrator/src/server.ts`
- Cause: `DatabaseSync` is the task truth store and some list methods return the complete history for a task.
- Improvement path: Keep task histories bounded during the private single-instance slice; before higher concurrency, add pagination/limits and move persistence off the request event loop or adopt a server database with equivalent transaction invariants.

**Retriever initialization failure expands prompts to the full Registry:**
- Problem: When Registry/retriever initialization fails, LLM tool schema and system context fall back to all visible tables, increasing tokens, latency, and the chance of provider context limits.
- Files: `agent/llm.py`, `config.py`, `forge/retriever.py`
- Cause: Full Registry is the graceful-degradation path when no retriever object is available.
- Improvement path: Fail closed with a bounded diagnostic/clarification response when the Registry exceeds a safe prompt budget; preserve BM25 fallback when the Registry is valid but embedding is unavailable.

## Fragile Areas

**Pi task state machine and side effects are concentrated in one class:**
- Files: `services/pi-orchestrator/src/application.ts`, `services/pi-orchestrator/src/sqlite-store.ts`, `services/pi-orchestrator/tests/application.test.ts`
- Why fragile: State transitions, retries, leases, events, artifacts, approvals, report publication, and Skill execution are interleaved. A new transition can leave an attempt, event, execution-plan step, or artifact inconsistent even when the Task status looks correct.
- Safe modification: Update transitions transactionally; prove expected prior state, emitted events, StageAttempt terminal state, artifact lineage, idempotent replay, timeout recovery, and restart reconciliation for each changed path.
- Test coverage: Broad application tests exist, but the module’s size and number of side effects require branch-specific tests for every new transition and failure point.

**Compiler and linter jointly define the trusted DSL boundary:**
- Files: `forge/compiler.py`, `forge/lint.py`, `forge/assurance.py`, `tests/test_compiler.py`, `tests/test_compiler_extended.py`, `tests/test_lint.py`, `tests/test_assurance.py`
- Why fragile: Query validity spans schema shape, semantic rules, dialect rendering, join/grain safety, and assurance. Updating only one layer can allow a query that compiles but violates semantics, or reject a valid supported query inconsistently.
- Safe modification: Treat new DSL behavior as a cross-layer contract; update schema validation, linter, compiler, assurance, dialect fixtures, and negative tests together.
- Test coverage: Unit coverage is substantial, but real-database dialect acceptance and adversarial database-function behavior remain deployment-dependent.

**Multiple local stores form one logical workflow without one transaction:**
- Files: `services/pi-orchestrator/src/sqlite-store.ts`, `forge/query_runs.py`, `forge/reporting.py`, `agent/audit.py`, `agent/model_control.py`, `registry/studio.py`
- Why fragile: Task, QueryRun, report, audit, model-control, and Registry state live in separate SQLite databases/files. Cross-service operations use hashes, idempotency keys, and reconciliation rather than an atomic distributed transaction.
- Safe modification: Preserve immutable identifiers and hashes, make every cross-store write idempotent, record recoverable intermediate states, and add crash-point tests around each boundary.
- Test coverage: Restart and QueryRun tests cover important paths, but process termination between every cross-store pair is not exhaustively tested.

**Module-level singletons cache configuration and resources:**
- Files: `forge/executor.py`, `agent/llm.py`, `web/routes/reports.py`, `agent/knowledge.py`, `agent/tenant.py`, `config.py`
- Why fragile: SQL engines, retrievers, report stores, and repository connections outlive configuration changes and complicate tests, runtime reconfiguration, and multi-process behavior.
- Safe modification: Keep process-level settings restart-only unless a component has an explicit reset/revision mechanism; do not mutate `cfg` and assume every singleton follows it.
- Test coverage: Model configuration has explicit cache-reset tests, but equivalent lifecycle tests are not present for every singleton.

## Scaling Limits

**Single-instance private deployment:**
- Current capacity: Web and Pi are designed around one private admin identity, local files, and SQLite WAL stores; API list caps are generally 50–100 objects and query results are capped by `EXECUTION_MAX_ROWS` (default 200).
- Limit: Multiple Web/Pi replicas do not share in-memory locks, singleton caches, background export jobs, or a coordinated lease owner; local report files and several SQLite databases are node-local.
- Scaling path: Do not scale horizontally by adding replicas. First define approved identity/governance scope, then move shared truth stores/artifacts/jobs to coordinated services while preserving existing idempotency and lineage contracts.

**Task history growth:**
- Current capacity: Events can be read from a sequence cursor and Web projects at most 200 events, but the Pi store returns all matching events after the cursor and complete artifact/attempt histories.
- Limit: Long-running/retried tasks increase synchronous JSON parse/clone work and response size; no retention/archival policy bounds task history.
- Scaling path: Add store-level pagination, immutable archival, and retention rules tied to evidence/audit requirements; never truncate lineage silently.

**Registry and prompt size:**
- Current capacity: Retrieval selects a configurable top-k table set and expands related tables; full Registry fallback remains available.
- Limit: Large schemas increase startup/index cost, per-request Registry reads, prompt/tool size, and LLM latency when retrieval is unavailable.
- Scaling path: Version Registry indexes, enforce prompt budgets, make degraded retrieval explicit, and benchmark against the intended partner schema before S1 approval.

## Dependencies at Risk

**Headless Chrome/Chromium:**
- Risk: PDF publication depends on an external executable discovered at runtime, but it is absent from Python dependency metadata and the supplied container image.
- Impact: PDF acceptance is environment-dependent and silently degrades to `failed` export status.
- Migration plan: Package and version the browser in the deployment image, run a startup exporter readiness check, and keep a targeted real-export smoke test.

**Unpinned Python dependency graph:**
- Risk: Open lower bounds allow incompatible future dependency releases into fresh builds.
- Impact: API, SDK, numerical, serialization, and database behavior can drift without a source change.
- Migration plan: Generate a hashed lock/constraints set for supported Python/platform targets and automate reviewed upgrades with focused compatibility tests.

**Private Pi coding-agent package:**
- Risk: Pi execution depends on `@earendil-works/pi-coding-agent` and a local Skills directory contract.
- Impact: Package or Skill contract changes can alter model execution, artifact submission, timeout behavior, and availability even though Forge source is unchanged.
- Migration plan: Keep the package pinned, version the Skills contract/revision in artifacts, and require compatibility tests before upgrading either side.

## Missing Critical Features

**S0 Design Partner/problem baseline evidence:**
- Problem: The active phase requires one real small data team, one domain, one authorized data source, one semantic owner, a real historical/ongoing question set, privacy boundaries, and an existing manual-process baseline. This evidence is not represented as completed.
- Blocks: S1–S3 Runtime implementation, additional connectors, Agent Runtime, M1A, and enterprise expansion are not approved.
- Files: `docs/current-project-state.md`, `docs/forge-enterprise-evolution-plan.md`, `docs/requirements-pool.md`

**Runtime policy enforcement:**
- Problem: Supported governance contracts have no production PEP coverage.
- Blocks: Any claim that delegated Service/Agent actions, high-risk actions, or external Pi access are governed at runtime.
- Files: `agent/contracts/governance_semantics.py`, `services/pi-orchestrator/src/governance-contracts.ts`, `services/pi-orchestrator/src/server.ts`, `docs/forge-enterprise-evolution-plan.md`

**Human acceptance for shipped Product surfaces:**
- Problem: W2 body-content rules await user visual confirmation; Product Spine and full Product Shell Atlas candidates retain user re-verification gates.
- Blocks: Marking those surfaces verified or treating automated/browser checks as user acceptance.
- Files: `docs/current-project-state.md`, `docs/forge-enterprise-evolution-plan.md`, `docs/product-spine-sp5-evidence-2026-08-25.md`

**Production identity and authorization model:**
- Problem: Product Web uses a deployment-wide admin principal and configured scope list rather than real users, memberships, roles, and mandates.
- Blocks: Safe multi-user, multi-workspace, requester/steward/approver separation, and external Agent access.
- Files: `web/routes/product.py`, `web/router.py`, `agent/tenant.py`, `services/pi-orchestrator/src/server.ts`

## Test Coverage Gaps

**Pi core authorization:**
- What's not tested: Negative authentication/authorization requirements for core Task create/read/mutate, event, artifact, attempt, query-approval, analysis, and report routes; existing server tests intentionally call these routes without credentials.
- Files: `services/pi-orchestrator/src/server.ts`, `services/pi-orchestrator/tests/server.test.ts`
- Risk: Pi can be exposed beyond loopback without a failing test identifying the missing policy boundary.
- Priority: High

**Knowledge ingestion security and resource bounds:**
- What's not tested: SSRF denial, redirect revalidation, response byte caps, upload byte caps, MIME restrictions, event-loop isolation, and the `web_search` incomplete path.
- Files: `agent/knowledge.py`, `web/router.py`, `tests/`
- Risk: Internal network access and memory/event-loop exhaustion can ship unnoticed.
- Priority: High

**Secure deployment gating:**
- What's not tested: Application/container startup refusal when production readiness fails. Tests validate readiness payload classification, not enforcement by the process or deployment.
- Files: `forge/readiness.py`, `main.py`, `Dockerfile`, `tests/test_commercial_readiness.py`
- Risk: A service can report production readiness failure while continuing to serve insecure endpoints.
- Priority: High

**Legacy Feishu approval completion:**
- What's not tested: End-to-end legacy card approval through SQL execution and result delivery.
- Files: `agent/feishu.py`, `tests/`
- Risk: Users receive an approval acknowledgement without the promised result.
- Priority: Medium

**Container report export:**
- What's not tested: Building the supplied image and producing real HTML, PDF, and PPTX artifacts inside it.
- Files: `Dockerfile`, `forge/reporting.py`, `tests/test_reporting.py`
- Risk: Unit tests pass while PDF export is unavailable in the documented deployment artifact.
- Priority: Medium

**Concurrency and crash recovery across stores:**
- What's not tested: Multi-process writers, process termination at every Pi→QueryRun→Report/Audit boundary, long task histories, and horizontal replicas.
- Files: `services/pi-orchestrator/src/sqlite-store.ts`, `forge/query_runs.py`, `forge/reporting.py`, `agent/audit.py`, `tests/test_query_runs.py`, `services/pi-orchestrator/tests/sqlite-store.test.ts`
- Risk: Duplicate side effects, stuck state, or missing lineage appears only under failure timing or scale.
- Priority: Medium

**Human visual acceptance:**
- What's not tested: W2 content quality and Atlas `PASS / CHANGE / REMOVE` decisions are human gates, not automatable assertions.
- Files: `docs/current-project-state.md`, `docs/forge-enterprise-evolution-plan.md`, `docs/product-spine-sp5-evidence-2026-08-25.md`
- Risk: Automated regression results can be incorrectly promoted to user-verified product acceptance.
- Priority: High

---

*Concerns audit: 2026-08-25*
