# Testing Patterns

**Analysis Date:** 2026-08-25

## Test Framework

**Runner:**
- Python uses pytest `>=8.0` from `pyproject.toml`, with discovery rooted at `tests/` through `[tool.pytest.ini_options].testpaths`.
- Async Python tests use pytest-asyncio `>=0.23`; `asyncio_mode = "auto"` in `pyproject.toml` allows async test methods without a marker, while some standalone tests retain explicit `@pytest.mark.asyncio`, as in `tests/test_audit.py`.
- Browser E2E uses Playwright `>=1.40` plus pytest-playwright `>=0.5`, declared in `pyproject.toml` and implemented in `tests/test_e2e.py`.
- Pi TypeScript uses the Node built-in `node:test` runner under Node `>=22.19`, executed through `tsx` so `.test.ts` files run directly; the exact script is in `services/pi-orchestrator/package.json`.
- Prototype JavaScript uses the Node built-in `node:test` runner through each tool's package script, including `tools/web-product-shell-prototype/package.json`, `tools/chart-storytelling-echarts-candidate/package.json`, and `tools/chart-engine-bakeoff/package.json`.
- Jest and Vitest are not used or configured in `services/pi-orchestrator/package.json`, `website/package.json`, or the prototype package manifests.

**Config:**
- Python test configuration is embedded in `pyproject.toml`; no standalone `pytest.ini`, `tox.ini`, or `conftest` configuration file exists outside `tests/conftest.py` fixtures.
- Pi TypeScript compilation/test inclusion is defined by `services/pi-orchestrator/tsconfig.json`; no standalone Node test config is required by `services/pi-orchestrator/package.json`.
- Playwright browser options and service availability gating live in `tests/test_e2e.py`; no `playwright.config.*` file is present.

**Assertion Library:**
- Python uses pytest's plain `assert`, `pytest.raises`, `pytest.approx`, and fixture/marker APIs, as demonstrated by `tests/test_compiler.py`, `tests/test_reporting.py`, and `tests/test_assurance.py`.
- TypeScript and prototype JavaScript use `node:assert/strict` (`assert.equal`, `assert.deepEqual`, `assert.match`, `assert.rejects`) with `node:test`, as demonstrated by `services/pi-orchestrator/tests/application.test.ts` and `tools/web-product-shell-prototype/tests/shell.test.js`.
- Browser assertions use pytest assertions over Playwright locators and page state rather than a separate expect library, as in `tests/test_e2e.py`.

**Run Commands:**
```bash
.venv/bin/python -m pytest tests -q
# Full Python suite; canonical repository command in `docs/current-project-state.md`.

.venv/bin/python -m pytest tests/test_compiler.py -v
# One Python file; use `path::test_name` for one behavior.

npm --prefix services/pi-orchestrator run typecheck
npm --prefix services/pi-orchestrator test
# Strict Pi type contract, then all Pi `tests/*.test.ts` files.

FORGE_BASE_URL=http://localhost:8000 .venv/bin/python -m pytest tests/test_e2e.py -v
# Browser E2E against a separately running Forge service; see `tests/test_e2e.py`.

npm --prefix tools/web-product-shell-prototype test
npm --prefix tools/chart-storytelling-echarts-candidate test
npm --prefix tools/chart-engine-bakeoff test
# Isolated prototype contract/static tests from each tool's `package.json`.
```

**Watch Mode:**
- No watch test script is configured in `pyproject.toml`, `services/pi-orchestrator/package.json`, `website/package.json`, or the prototype package manifests.

## Test File Organization

**Location:**
- Python tests are separated from implementation under root `tests/`; core modules in `forge/`, `agent/`, `registry/`, `web/`, and `main.py` are covered by corresponding `tests/test_*.py` files.
- Pi TypeScript tests are separated within the subproject at `services/pi-orchestrator/tests/`, mirroring owners in `services/pi-orchestrator/src/` (for example `src/task-store.ts` and `tests/task-store.test.ts`).
- Prototype tests remain inside their isolated tool directories under `tools/<prototype>/tests/`, next to each tool's `src/` and `package.json`.
- Shared contract schemas and fixture corpora live under `agent/contracts/` and are consumed by both Python tests such as `tests/test_product_projection_contracts.py` and Pi tests such as `services/pi-orchestrator/tests/product-projections.test.ts`.
- Accuracy and benchmark harness code lives under `tests/accuracy/`, `tests/benchmark/`, `tests/spider2/`, and `tests/text-to-sql-failures/`; these are executable evaluation tools rather than pytest-discovered `test_*.py` modules.

**Naming:**
- Name Python test modules `test_<subject>.py`, classes `Test<Area>`, fixtures/helper functions in `snake_case`, and tests `test_<observable_behavior>`, following `tests/test_api.py`, `tests/test_e2e.py`, and `tests/test_compiler.py`.
- Name Node test files `<subject>.test.ts` or `<subject>.test.js`; describe the invariant in the `test("...")` title, following `services/pi-orchestrator/tests/sqlite-store.test.ts` and `tools/chart-engine-bakeoff/tests/data.test.js`.
- Keep negative/fail-closed behavior explicit in names (`rejects`, `fails_closed`, `cannot`, `requires`) as in `tests/test_assurance.py` and `services/pi-orchestrator/tests/product-projections.test.ts`.

**Structure:**
```text
tests/
├── conftest.py                         # shared FastAPI/httpx fixtures
├── test_<python-area>.py               # unit and integration behavior
├── test_e2e.py                         # pytest-playwright browser flow
├── fixtures/                           # fixture-generation utilities
├── accuracy/                           # evaluation harnesses, not pytest tests
├── benchmark/                          # retrieval/accuracy benchmark tools
└── spider2/                            # external benchmark runners

services/pi-orchestrator/
├── src/<owner>.ts
└── tests/<owner>.test.ts               # node:test unit/integration tests

tools/<prototype>/
├── src/
└── tests/*.test.js                     # isolated node:test checks
```

## Test Structure

**Suite Organization:**

Python unit/integration style from `tests/test_compiler.py` and `tests/test_api.py`:
```python
class TestHealthCheck:
    async def test_health_returns_ok(self, client: AsyncClient):
        response = await client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"


def test_rejects_unbound_table_reference_before_sql_execution():
    with pytest.raises(ValueError, match="未加入 FROM/JOIN"):
        compile_query({...})
```

Node style from `services/pi-orchestrator/tests/application.test.ts`:
```typescript
test("Pi application owns TaskRun progression and emits a review event", async () => {
  const { application, calls } = createApplication(response());
  const created = application.createTask({...});

  const prepared = await application.prepareQuery(created.task.task_run_id, {...});

  assert.equal(prepared.task.status, "waiting_for_query_approval");
  assert.deepEqual(calls, [{...}]);
});
```

**Patterns:**
- Follow arrange/act/assert separated by blank lines for behavior-heavy tests, as in `tests/test_api.py`, `tests/test_query_runs.py`, and `services/pi-orchestrator/tests/application.test.ts`.
- Use direct top-level test functions for narrow deterministic modules, as in `tests/test_compiler.py`, `tests/test_lint.py`, and `tests/test_normalization.py`.
- Group endpoint/browser behaviors into `Test*` classes without class-level shared mutable state, as in `tests/test_api.py` and `tests/test_e2e.py`.
- Use `pytest.mark.parametrize` for the same contract over a table of inputs, as in `tests/test_api.py::TestAdminPages` and `tests/test_e2e.py::TestNavigation`.
- Small in-test loops are also used for exhaustive contract/data tables where each case shares setup, as in `tests/test_compiler.py` and `tests/test_product_projection_contracts.py`.
- Assert the full state transition/event order when orchestration semantics matter, as in `services/pi-orchestrator/tests/application.test.ts` and `services/pi-orchestrator/tests/sqlite-store.test.ts`.
- Assert bounded public failures and absence of internal implementation details at API boundaries, as in `tests/test_api.py::TestExecuteRaw` and `services/pi-orchestrator/tests/server.test.ts`.
- Use explicit performance budgets only for deterministic bounded operations, as in `tests/test_product_projection_contracts.py::test_bulk_product_projection_validation_stays_within_page_budget`.

**Setup:**
- Root FastAPI setup is centralized in `tests/conftest.py`: a session-scoped app fixture disables authentication and external model calls, and async `client`/`auth_client` fixtures use `httpx.ASGITransport` without opening a TCP port.
- Module-specific Python fixtures create isolated Registry/config/database state with `tmp_path` and `monkeypatch`, as in `tests/test_assurance.py`, `tests/test_context_api.py`, `tests/test_memory_api.py`, and `tests/test_model_config.py`.
- Pi tests construct in-memory ports/stores for domain tests and create temporary SQLite databases for persistence tests, as in `services/pi-orchestrator/tests/application.test.ts` and `services/pi-orchestrator/tests/sqlite-store.test.ts`.
- Pi HTTP integration tests bind servers to `127.0.0.1` port `0` and derive the assigned port, as in `services/pi-orchestrator/tests/server.test.ts` and `services/pi-orchestrator/tests/forge-prepare-query.test.ts`.

**Teardown:**
- Rely on pytest's `monkeypatch` and `tmp_path` lifecycles for automatic restoration/deletion; use yielding fixtures for explicit resource disposal, as in `tests/test_memory_api.py` and `tests/conftest.py`.
- Register Node cleanup with the test context's `context.after(...)` for live servers and stores, as in `services/pi-orchestrator/tests/server.test.ts`, `services/pi-orchestrator/tests/channels.test.ts`, and `services/pi-orchestrator/tests/forge-prepare-query.test.ts`.
- Close/reopen actual persistence stores within tests when recovery semantics are the behavior, as in `services/pi-orchestrator/tests/sqlite-store.test.ts`.

## Mocking

**Framework:**
- Python primarily uses pytest's built-in `monkeypatch`; `unittest.mock.Mock` appears for call inspection in tests such as `tests/test_dingtalk_pi.py`.
- Pi TypeScript has no external mocking library; use handwritten port objects, fake implementations, captured call arrays, and ephemeral local HTTP servers in `services/pi-orchestrator/tests/application.test.ts` and `services/pi-orchestrator/tests/forge-prepare-query.test.ts`.
- Browser network isolation uses Playwright `page.route(...).fulfill(...)` in `tests/test_e2e.py`.

**Patterns:**

Python dependency substitution from `tests/test_api.py`:
```python
monkeypatch.setattr(
    router_mod,
    "agent_prepare_query",
    lambda user_id, question, dialect=None: {
        "status": "needs_review",
        "question": question,
        "review_required": True,
        "can_execute": False,
    },
)
```

Python isolated filesystem/config from `tests/test_assurance.py`:
```python
path = tmp_path / "schema.registry.json"
path.write_text(json.dumps({...}), encoding="utf-8")
monkeypatch.setattr(cfg, "REGISTRY_PATH", path)
```

TypeScript port fake from `services/pi-orchestrator/tests/application.test.ts`:
```typescript
const calls: Array<Record<string, unknown>> = [];
const forgeClient: ForgeQueryRunPort = {
  async createQueryRun(input) {
    calls.push(input);
    return response({ task_run_id: input.taskRunId });
  },
  async approveQueryRun(input) {
    return {...};
  },
};
```

Browser route fake from `tests/test_e2e.py`:
```python
def product_conversations(route):
    route.fulfill(json={"conversations": [summary]})

page.route("**/api/product/conversations**", product_conversations)
```

**What to Mock:**
- Mock LLM/provider calls and capability validation so ordinary tests never require external credentials or network access; see `tests/test_agent_runtime.py`, `tests/test_llm_openai_compat.py`, and `tests/test_model_control.py`.
- Mock channel transports and callbacks while asserting the adapter contract, as in `tests/test_dingtalk_pi.py`, `tests/test_feishu_runtime.py`, and `services/pi-orchestrator/tests/channels.test.ts`.
- Patch configuration paths and module-level engine/store caches to isolated temporary files when global config is unavoidable, as in `tests/test_executor.py`, `tests/test_memory_api.py`, and `tests/test_query_runs.py`.
- Use local HTTP servers rather than mocking `fetch` when the transport/status/JSON boundary is under test, as in `services/pi-orchestrator/tests/forge-prepare-query.test.ts`.
- Fulfill browser API routes when verifying rendering/state behavior independently of the backend, as in the Product Conversation tests in `tests/test_e2e.py`.

**What NOT to Mock:**
- Do not mock deterministic compiler, normalization, lint, assurance, or contract validation logic in their unit suites; execute real logic in `tests/test_compiler.py`, `tests/test_normalization.py`, `tests/test_lint.py`, and `tests/test_assurance.py`.
- Do not mock SQLite when persistence, compare-and-set, restart recovery, idempotency, or transaction behavior is the contract; use real temporary databases in `tests/test_query_runs.py` and `services/pi-orchestrator/tests/sqlite-store.test.ts`.
- Do not mock the ASGI request/response boundary for Python API tests; use the real FastAPI app with `ASGITransport` from `tests/conftest.py`.
- Do not replace shared fixture corpora with hand-built equivalents in parity tests; both `tests/test_product_projection_contracts.py` and `services/pi-orchestrator/tests/product-projections.test.ts` must read `agent/contracts/product-projection-fixtures.v1.json`.

## Fixtures and Factories

**Test Data:**

Shared pytest fixtures from `tests/conftest.py`:
```python
@pytest_asyncio.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
```

Inline factory/override pattern from `services/pi-orchestrator/tests/application.test.ts`:
```typescript
function response(overrides: Partial<QueryRunReview> = {}): QueryRunReview {
  return {
    status: "needs_review",
    review_required: true,
    can_execute: false,
    ...overrides,
  };
}
```

Shared cross-language corpus pattern from `tests/test_product_projection_contracts.py` and `services/pi-orchestrator/tests/product-projections.test.ts`:
```text
agent/contracts/product-projection-fixtures.v1.json
  valid/<contract>[]
  invalid[] -> base_case_id + mutation + expected_code
```

**Location:**
- Put widely reused ASGI/auth fixtures in `tests/conftest.py`; keep area-specific fixtures in the owning `tests/test_<area>.py` file, as in `tests/test_agent_runtime.py` and `tests/test_model_config.py`.
- Put generated/versioned contract fixtures beside schemas in `agent/contracts/`, including `agent/contracts/product-projection-fixtures.v1.json` and `agent/contracts/governance-review-fixtures.v1.json`.
- Put test-data generators that are not fixtures in `tests/fixtures/`, such as `tests/fixtures/gen_large_registry.py`.
- Keep Pi builders/fakes local to the `.test.ts` file unless multiple suites share a stable production abstraction; `response(...)` and `createApplication(...)` remain local in `services/pi-orchestrator/tests/application.test.ts`.
- Use `tmp_path`/`mkdtemp(tmpdir())` for writable state; never write tests into committed Registry, runtime, or customer-data locations, following `tests/test_database_compatibility.py` and `services/pi-orchestrator/tests/sqlite-store.test.ts`.

## Coverage

**Requirements:**
- No numeric line/branch coverage target is enforced in `pyproject.toml` or `.github/workflows/ci.yml`.
- `pytest-cov` is not declared in the `dev` optional dependencies in `pyproject.toml`; coverage is not part of the default reproducible install.
- No JavaScript/TypeScript coverage tool or script is configured in `services/pi-orchestrator/package.json` or the prototype package manifests.
- Contract coverage and Runtime Governance Coverage are product/governance measures, not source-code line coverage; `docs/current-project-state.md` and `docs/governance-action-catalog.md` keep those meanings separate.

**View Coverage:**
```bash
# Not available from the declared dependency set in `pyproject.toml`.
# After explicitly installing pytest-cov, `docs/development.md` documents:
pytest --cov=forge --cov=agent --cov=registry --cov-report=term-missing
```

**Practical Gate:**
- Add or update the smallest behavior suite that owns the changed contract, then run the cross-language partner suite when a shared Pi/Forge contract changes; examples are documented in `docs/product-projection-contracts.md` and implemented by `tests/test_product_projection_contracts.py` plus `services/pi-orchestrator/tests/product-projections.test.ts`.
- Run `npm --prefix services/pi-orchestrator run typecheck` with Pi changes because strict compilation covers source, tests, and scripts through `services/pi-orchestrator/tsconfig.json`.

## Test Types

**Unit Tests:**
- Deterministic Forge JSON compilation, dialect behavior, normalization, lint, assurance, Registry semantics, and model configuration are tested directly in `tests/test_compiler.py`, `tests/test_compiler_extended.py`, `tests/test_compiler_window.py`, `tests/test_normalization.py`, `tests/test_lint.py`, and `tests/test_assurance.py`.
- Pi domain stores, state transitions, projections, planning, artifacts, and policy behavior are tested with in-memory collaborators in `services/pi-orchestrator/tests/task-store.test.ts`, `services/pi-orchestrator/tests/application.test.ts`, `services/pi-orchestrator/tests/planning.test.ts`, and `services/pi-orchestrator/tests/product-projection-builder.test.ts`.
- Prototype semantics and source/static constraints are tested without a browser in `tools/web-product-shell-prototype/tests/shell.test.js`, `tools/chart-storytelling-echarts-candidate/tests/candidate.test.js`, and `tools/chart-engine-bakeoff/tests/data.test.js`.

**Integration Tests:**
- FastAPI endpoint integration uses the real app in-process through httpx in `tests/test_api.py`, `tests/test_product_bff.py`, `tests/test_registry_studio_api.py`, and `tests/test_query_runs.py`.
- Persistence integration uses temporary SQLite files and exercises schema, transactions, idempotency, restart, and recovery in `tests/test_query_runs.py`, `tests/test_reporting.py`, `tests/test_model_control.py`, and `services/pi-orchestrator/tests/sqlite-store.test.ts`.
- Pi transport integration runs ephemeral HTTP servers and real clients in `services/pi-orchestrator/tests/server.test.ts`, `services/pi-orchestrator/tests/forge-prepare-query.test.ts`, and `services/pi-orchestrator/tests/channels.test.ts`.
- Database compatibility executes one common smoke against SQLite, PostgreSQL 16, and MySQL 8 through `tests/test_database_compatibility.py` and the `compatibility` job in `.github/workflows/ci.yml`.
- Cross-language schema/semantic parity uses shared JSON fixtures in `tests/test_artifact_contracts.py`, `tests/test_governance_contracts.py`, `tests/test_product_projection_contracts.py`, `services/pi-orchestrator/tests/governance-contracts.test.ts`, and `services/pi-orchestrator/tests/product-projections.test.ts`.

**E2E Tests:**
- `tests/test_e2e.py` uses pytest-playwright against a live Forge server selected by `FORGE_BASE_URL` and skips the entire file when `/health` is unavailable.
- Browser tests cover login/navigation, bounded query-result rendering, task status polling, desktop/mobile task panels, keyboard dismissal, and isolated Registry mutation gates in `tests/test_e2e.py`.
- Mutating metric CRUD browser coverage is opt-in through `FORGE_E2E_ALLOW_MUTATION=true` and must target an isolated Registry, as enforced by `tests/test_e2e.py`.
- The default GitHub Actions workflow does not start Forge or install browsers, so `tests/test_e2e.py` is skipped in `.github/workflows/ci.yml` unless an external service is provided.
- `website/` has no automated unit, integration, or browser test command in `website/package.json`.

**CI Tests:**
- `.github/workflows/ci.yml` runs on pushes and pull requests targeting `main`.
- The `test` job in `.github/workflows/ci.yml` uses Ubuntu, Python 3.11, `pip install -e ".[dev]"`, and `pytest -q`.
- The `compatibility` job in `.github/workflows/ci.yml` provisions PostgreSQL 16 and MySQL 8 services and runs `tests/test_database_compatibility.py` once for SQLite and once per external database.
- `.github/workflows/ci.yml` does not run Pi TypeScript typecheck/tests, prototype Node tests, Astro build/tests, Playwright browsers, linting, or coverage; those remain explicit local/release gates from `docs/current-project-state.md` and the relevant package manifests.

## Common Patterns

**Async Testing:**

Python from `tests/test_api.py` with `asyncio_mode = "auto"` from `pyproject.toml`:
```python
async def test_submit_feedback(self, client: AsyncClient, monkeypatch, tmp_path):
    monkeypatch.setattr(feedback.audit.cfg, "AUDIT_DB_PATH", str(tmp_path / "audit.db"))
    response = await client.post("/api/feedback", json={...})
    assert response.status_code == 200
```

Node from `services/pi-orchestrator/tests/server.test.ts`:
```typescript
test("server behavior", async (context) => {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  context.after(() => server.close());
  const response = await fetch(url);
  assert.equal(response.status, 200);
});
```

**Error Testing:**

Python exception contracts from `tests/test_assurance.py` and `tests/test_compiler.py`:
```python
with pytest.raises(QueryAssuranceError, match="Registry 不可用") as caught:
    assure_query(...)
assert caught.value.report.status == "failed"
```

Node rejected promises from `services/pi-orchestrator/tests/forge-prepare-query.test.ts`:
```typescript
await assert.rejects(
  () => client.prepareQuery({...}),
  /invalid JSON|request failed/,
);
```

API error-boundary checks from `tests/test_api.py`:
```python
assert response.status_code == 200
assert payload["action"] == "execution_failed"
assert "OperationalError" not in payload["exec_error"]
assert "sqlalche.me" not in payload["exec_error"]
```

**State and Security Testing:**
- Assert both positive progression and negative fail-closed behavior for approval, scope, identity, credentials, hashes, and lineage; representative suites are `tests/test_query_runs.py`, `tests/test_auth.py`, `services/pi-orchestrator/tests/channels.test.ts`, and `services/pi-orchestrator/tests/governance-contracts.test.ts`.
- Assert idempotency and compare-and-set conflicts using repeated operations or multiple store connections, as in `tests/test_query_runs.py`, `services/pi-orchestrator/tests/sqlite-store.test.ts`, and `tests/test_api.py::TestSettingsRoutes`.
- Assert secret-like data is absent from rendered/API output rather than merely checking success, as in `tests/test_feishu_runtime.py`, `tests/test_model_control.py`, and `services/pi-orchestrator/tests/product-projections.test.ts`.
- Assert recovery does not replay SQL or model stages after restart/timeouts, as in `tests/test_query_runs.py`, `services/pi-orchestrator/tests/application.test.ts`, and `services/pi-orchestrator/tests/sqlite-store.test.ts`.

---

*Testing analysis: 2026-08-25*
