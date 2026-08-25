# Coding Conventions

**Analysis Date:** 2026-08-25

## Naming Patterns

**Files:**
- Use lowercase `snake_case.py` for Python implementation modules, as in `forge/query_runs.py`, `agent/model_control.py`, `web/pi_channel.py`, and `registry/business_context.py`.
- Name Python tests `test_<subject>.py` under `tests/`, as in `tests/test_compiler.py`, `tests/test_query_runs.py`, and `tests/test_product_projection_contracts.py`.
- Use lowercase kebab-case for Pi TypeScript modules and suffix tests with `.test.ts`, as in `services/pi-orchestrator/src/task-store.ts` and `services/pi-orchestrator/tests/task-store.test.ts`.
- Use lowercase JavaScript module names and `.test.js` tests inside isolated prototypes, as in `tools/chart-storytelling-echarts-candidate/src/semantics.js` and `tools/chart-storytelling-echarts-candidate/tests/candidate.test.js`.
- Preserve framework-owned naming in the Astro site; `website/src/content.config.ts` follows Astro's expected filename rather than the Pi module convention.

**Functions:**
- Use `snake_case` for Python functions and coroutines; prefix module-private helpers with `_`, as in `forge/query_runs.py` (`create_query_run`, `_ensure_schema`, `_artifact_hash`) and `forge/executor.py` (`validate_readonly_sql`, `_public_execution_error`).
- Use `camelCase` for TypeScript/JavaScript functions and methods, as in `services/pi-orchestrator/src/config.ts` (`loadConfig`, `computePiModelRevision`) and `services/pi-orchestrator/src/application.ts` (`createTask`, `prepareQuery`).
- Name Python tests after the observable behavior or invariant, not the implementation call, as in `tests/test_compiler.py::test_rejects_unbound_table_reference_before_sql_execution` and `tests/test_executor.py::test_execute_with_data_respects_configured_row_cap`.
- Name Node tests with full behavior sentences passed to `test(...)`, as in `services/pi-orchestrator/tests/application.test.ts` and `tools/web-product-shell-prototype/tests/shell.test.js`.

**Variables:**
- Use `snake_case` for Python locals and parameters; use leading underscores for process-local/private state such as `_SCHEMA_LOCK` and `_PROCESS_OWNER` in `forge/query_runs.py`.
- Use `camelCase` for TypeScript locals and parameters; use ECMAScript `#privateField` syntax for class internals in `services/pi-orchestrator/src/application.ts`.
- Use uppercase `SCREAMING_SNAKE_CASE` for module constants in Python and TypeScript, as in `forge/assurance.py` (`ASSURANCE_REVISION`) and `services/pi-orchestrator/src/server.ts` (`MAX_BODY_BYTES`, `CHANNELS`).
- Preserve `snake_case` field names at serialized contracts and persistence boundaries, including `task_run_id`, `org_id`, and `assurance_report_hash` in `forge/query_runs.py`, `services/pi-orchestrator/src/product-projections.ts`, and `agent/contracts/product-projection-v1.schema.json`.
- Translate wire-level `snake_case` to internal TypeScript `camelCase` at adapters rather than changing the contract; `services/pi-orchestrator/src/server.ts` maps `idempotency_key` to `idempotencyKey`, and `services/pi-orchestrator/src/application.ts` exposes camelCase port inputs.

**Types:**
- Use `PascalCase` for Python classes, dataclasses, protocols, and exceptions, as in `forge/adapters.py` (`DatabaseCapabilities`, `DatabaseAdapter`) and `forge/query_runs.py` (`QueryRunError`).
- Use frozen dataclasses for immutable value/report objects, as in `forge/assurance.py` (`GateResult`, `QueryAssuranceReport`) and `forge/executor.py` (`QueryExecutionData`).
- Use `PascalCase` for TypeScript interfaces, type aliases, classes, and domain errors, as in `services/pi-orchestrator/src/application.ts` (`ForgeQueryRunPort`, `OrchestratorApplication`) and `services/pi-orchestrator/src/task-store.ts` (`TaskStateError`).
- Prefer modern built-in generic syntax and explicit unions in Python (`dict[str, Any]`, `str | None`, `tuple[...]`) as demonstrated by `forge/normalization.py` and `forge/query_runs.py`.
- Use `interface` for object/port contracts and `type` for unions or imported contract shapes in the Pi service, following `services/pi-orchestrator/src/application.ts` and `services/pi-orchestrator/src/product-projections.ts`.

## Code Style

**Formatting:**
- Python uses four-space indentation, double-quoted strings in most core modules, blank lines between top-level declarations, and multiline trailing commas where structures are expanded; follow the local shape in `forge/query_runs.py` and `forge/normalization.py`.
- Python formatting is manually maintained: no Black, Ruff formatter, or isort configuration is declared in `pyproject.toml`. Do not introduce repository-wide reformatting; match the touched module, including compact alignment found in older files such as `forge/compiler.py`.
- Pi TypeScript uses two-space indentation, double quotes, semicolons, trailing commas in multiline constructs, and braces on the same line, following `services/pi-orchestrator/src/config.ts` and `services/pi-orchestrator/src/server.ts`.
- Prototype JavaScript uses the same two-space/double-quote/semicolon style as Pi TypeScript, following `tools/web-product-shell-prototype/src/main.js` and `tools/web-product-shell-prototype/tests/shell.test.js`.
- Astro site code follows the Astro starter style of tabs and single quotes in `website/src/content.config.ts`; preserve that subproject's style instead of normalizing it to Pi conventions.
- No Prettier, Biome, or EditorConfig configuration is detected by the repository quality configuration; `services/pi-orchestrator/package.json` and `website/package.json` define no formatting scripts.

**Linting:**
- No Python linter or static type-checker command is configured in `pyproject.toml`; inline suppressions are rare and local, such as `# type: ignore[return-value]` in `agent/audit.py`.
- No ESLint, Prettier, or Biome configuration is detected for `services/pi-orchestrator/`, `website/`, or `tools/`; do not claim a lint gate from the package manifests.
- Treat TypeScript compilation as the enforced static-quality gate: `services/pi-orchestrator/tsconfig.json` enables `strict`, `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`, and `forceConsistentCasingInFileNames`, while `services/pi-orchestrator/package.json` runs `tsc --noEmit` via `typecheck`.
- Keep optional properties genuinely absent under `exactOptionalPropertyTypes`; use conditional object spreads as in `services/pi-orchestrator/src/application.ts` rather than assigning `undefined` to optional fields.
- Keep indexed values guarded or optional-chained under `noUncheckedIndexedAccess`, as in `services/pi-orchestrator/src/config.ts` and `services/pi-orchestrator/tests/application.test.ts`.

## Import Organization

**Order:**
1. Put `from __future__ import annotations` first in Python modules that use postponed annotations, as in `forge/query_runs.py`, `forge/normalization.py`, and `tests/test_product_projection_contracts.py`.
2. Group Python standard-library imports, then third-party imports, then repository-local imports with blank lines, following `forge/query_runs.py` and `tests/test_product_projection_contracts.py`; strict alphabetization is not enforced by `pyproject.toml`.
3. Put Node built-ins first, then relative project imports in TypeScript/JavaScript, following `services/pi-orchestrator/src/server.ts` and `services/pi-orchestrator/tests/product-projections.test.ts`.
4. Use `import type` or inline `type` specifiers for type-only TypeScript dependencies, as in `services/pi-orchestrator/src/application.ts` and `services/pi-orchestrator/src/server.ts`.
5. Keep runtime-conditional or cycle-avoiding Python imports inside the function/branch that needs them, as in `forge/query_runs.py`, `forge/executor.py`, and `main.py`.

**Path Aliases:**
- No Python path aliases are configured; import packages from repository roots such as `from forge.compiler import compile_query` and `from config import cfg`, as in `tests/test_compiler.py` and `forge/query_runs.py`.
- No TypeScript path aliases are configured in `services/pi-orchestrator/tsconfig.json`; use relative ESM imports and include the runtime `.js` extension even when the source file is `.ts`, as in `services/pi-orchestrator/src/application.ts`.
- No Astro aliases are configured in `website/tsconfig.json`; use package imports supported by Astro, as in `website/src/content.config.ts`.

## Error Handling

**Patterns:**
- Reject invalid deterministic inputs with `ValueError` in Python core logic and preserve the original exception with `raise ... from exc` where translation adds domain context, as in `forge/compiler.py` and `agent/contracts/__init__.py`.
- Use domain exceptions when callers need stable status or structured diagnostics: `QueryRunError` carries an HTTP-friendly status in `forge/query_runs.py`, and `QueryAssuranceError` carries a report in `forge/assurance.py`.
- Keep public error details bounded while retaining raw details in logs; `forge/executor.py` logs execution exceptions and returns `_public_execution_error(...)` instead of exposing SQLAlchemy internals.
- At optional/degradable boundaries, catch the narrow expected exception set and return an explicit fallback only when the fallback is part of the contract, as in the BM25 fallback in `forge/retriever.py` and optional chart generation in `forge/chart.py`.
- At FastAPI boundaries, return stable HTTP status/payload shapes or register an exception handler rather than leaking tracebacks; see `main.py`, `web/routes/query_runs.py`, and `web/router.py`.
- In TypeScript, validate inputs early and throw domain-specific `Error` subclasses (`RequestError`, `TaskStateError`, `SkillPolicyConflictError`) as used by `services/pi-orchestrator/src/server.ts`, `services/pi-orchestrator/src/task-store.ts`, and `services/pi-orchestrator/src/skill-policy.ts`.
- Normalize unknown caught values with `error instanceof Error ? error.message : <bounded fallback>` as in `services/pi-orchestrator/src/application.ts` and `services/pi-orchestrator/src/forge/query-run-client.ts`.
- Map TypeScript domain errors centrally to HTTP statuses and hide unknown internals behind `internal orchestrator error`, following the catch boundary in `services/pi-orchestrator/src/server.ts`.
- Preserve transaction semantics: roll back and rethrow the original error or wrap it with `cause`, following `services/pi-orchestrator/src/sqlite-store.ts` and `services/pi-orchestrator/src/channels/identity.ts`.
- Do not silently replay high-risk work after failures; reconciliation marks expired work failed or retryable without replay in `forge/query_runs.py`, `main.py`, and `services/pi-orchestrator/src/application.ts`.

## Logging

**Framework:** Python standard-library `logging` for the Python service; `console.log` only for the Pi process startup banner in `services/pi-orchestrator/src/server.ts`.

**Patterns:**
- Create module loggers with `logging.getLogger(__name__)`, as in `forge/executor.py`, `forge/cache.py`, `agent/llm.py`, and `web/router.py`.
- Configure handlers, level, and timestamped format once at the application entry point in `main.py`; library modules must not call `logging.basicConfig`.
- Use parameterized logging (`logger.warning("... %s", value)`) instead of eager string interpolation, following `main.py`, `forge/retriever.py`, and `agent/agent.py`.
- Use `logger.exception(...)` for unexpected failures whose traceback is required server-side, as in `agent/agent.py`; use `warning` for recoverable degraded modes and `debug` for expected parse/cache misses in `forge/chart.py` and `forge/cache.py`.
- Keep user-facing errors separate from operational logs and avoid returning provider/database exception internals, following `forge/executor.py` and `services/pi-orchestrator/src/server.ts`.
- Keep secrets out of logs; model and database configuration code exposes identifiers/revisions but not secret values in `agent/model_config.py`, `forge/query_runs.py`, and `services/pi-orchestrator/src/config.ts`.

## Comments

**When to Comment:**
- Use comments for invariants, security boundaries, non-obvious compatibility behavior, and reasons a fallback is safe, as in `forge/executor.py`, `forge/retriever.py`, `services/pi-orchestrator/src/model-bindings.ts`, and `services/pi-orchestrator/src/skill-executor.ts`.
- Use section dividers in long Python modules to make major subsystems navigable, following `forge/compiler.py`, `agent/agent.py`, and `web/router.py`; avoid adding divider noise to small modules such as `forge/normalization.py`.
- Keep comments focused on why a constraint exists, not a restatement of the next line; `services/pi-orchestrator/src/skill-executor.ts` explains why progress telemetry cannot affect a Stage result.
- Use Chinese or English consistently with the surrounding module; both are established in `forge/compiler.py`, `forge/query_runs.py`, and `services/pi-orchestrator/src/application.ts`.

**JSDoc/TSDoc:**
- Python public/domain functions and classes commonly use docstrings, with concise one-line docstrings for narrow helpers and expanded docstrings for public workflows, as in `forge/query_runs.py`, `forge/executor.py`, and `agent/agent.py`.
- TypeScript relies primarily on expressive interfaces/types and sparse inline comments rather than pervasive TSDoc, as shown by `services/pi-orchestrator/src/application.ts` and `services/pi-orchestrator/src/product-projections.ts`.
- Add TSDoc only where a public contract cannot express an invariant; do not document every field mechanically in `services/pi-orchestrator/src/`.

## Function Design

**Size:**
- Keep deterministic helpers narrow and composable, as in `forge/normalization.py`, `forge/query_runs.py`, and `services/pi-orchestrator/src/config.ts`.
- For state-machine workflows that remain large, isolate persistence, validation, and adapter work behind private helpers/ports rather than duplicating transitions; follow `services/pi-orchestrator/src/application.ts` and `forge/query_runs.py`.
- Prefer early validation and early returns to deeply nested branches, as in `forge/normalization.py`, `services/pi-orchestrator/src/config.ts`, and `services/pi-orchestrator/src/server.ts`.

**Parameters:**
- Use keyword-only Python parameters for domain operations with multiple identifiers or policy values, as in `forge/query_runs.py::create_query_run` and `agent/audit.py::log`.
- Use typed option objects for TypeScript constructors and multi-field operations, as in `services/pi-orchestrator/src/application.ts` and `services/pi-orchestrator/src/forge/query-run-client.ts`.
- Inject ports/callables for external work so behavior can be isolated in tests, as in `forge/query_runs.py` (`prepare_fn`) and `services/pi-orchestrator/src/application.ts` (`ForgeQueryRunPort`, `StructuredSkillExecutionPort`).
- Keep scope identifiers explicit (`org_id`, `team_id`, `user_id`, `task_run_id`) at trusted boundaries rather than hiding them in ambient state, as in `forge/query_runs.py` and `services/pi-orchestrator/src/application.ts`.

**Return Values:**
- Return typed immutable values for bounded internal results, such as `QueryExecutionData` in `forge/executor.py` and `QueryAssuranceReport` in `forge/assurance.py`.
- Return JSON-compatible dictionaries only at dynamic API/contract boundaries, as in `forge/query_runs.py`, `forge/readiness.py`, and `web/routes/product.py`.
- Return stable discriminated/status payloads rather than `None` for workflow outcomes; QueryRun and TaskRun state transitions in `forge/query_runs.py` and `services/pi-orchestrator/src/application.ts` use explicit status fields.
- Use `None`/`undefined` only for genuinely optional data and guard it before indexed access, following `forge/query_runs.py` and strict checks in `services/pi-orchestrator/src/config.ts`.

## Module Design

**Exports:**
- Keep Python package exports narrow; `forge/__init__.py` exposes only `compile_query`, while implementation helpers remain module-private.
- Export TypeScript interfaces and classes from their owning module and import them directly; `services/pi-orchestrator/src/application.ts`, `services/pi-orchestrator/src/task-store.ts`, and `services/pi-orchestrator/src/product-projections.ts` are the authoritative owners.
- Keep generated/shared contract artifacts under `agent/contracts/` and load them through `agent/contracts/__init__.py`; do not duplicate JSON Schema definitions in Python call sites.
- Keep TypeScript TypeBox contract truth in the owning Pi module and verify generated cross-language Schema artifacts, following `services/pi-orchestrator/src/product-projections.ts` and `services/pi-orchestrator/scripts/export-product-projection-schema.ts`.

**Barrel Files:**
- Python uses only small intentional package surfaces such as `forge/__init__.py` and `registry/__init__.py`; do not create broad re-export barrels that obscure ownership.
- TypeScript has no `index.ts` barrel in `services/pi-orchestrator/src/`; preserve direct relative imports to keep dependency ownership visible.
- Route packages may use empty/minimal `__init__.py` markers, as in `web/routes/__init__.py`; route registration remains explicit in `main.py` and `web/router.py`.

## Contract and Boundary Conventions

- Treat Pi as the Task/ExecutionPlan/Artifact orchestrator and Forge as the trusted validation/execution boundary; this rule is explicit in `docs/current-project-state.md` and encoded by ports in `services/pi-orchestrator/src/application.ts` plus QueryRun ownership in `forge/query_runs.py`.
- Compile from registered Forge JSON and stop at review unless execution is explicitly approved; `.agents/skills/forge-query/SKILL.md`, `forge/compiler.py`, and `forge/query_runs.py` define this fail-closed flow.
- Preserve versioned schemas, stable reason codes, hashes, and lineage fields across languages; `agent/contracts/`, `services/pi-orchestrator/src/governance-contracts.ts`, and `services/pi-orchestrator/src/product-projections.ts` are shared contract surfaces.
- Never add a second Task truth source in Web/channel adapters; `web/pi_channel.py`, `web/feishu_pi.py`, and `services/pi-orchestrator/src/channels/` project or transport Pi-owned state.

---

*Convention analysis: 2026-08-25*
