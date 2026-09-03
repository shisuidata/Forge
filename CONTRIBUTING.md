# Contributing to Forge

Thank you for helping improve Forge.

Forge is actively evolving, early-stage data infrastructure. Its current focus is a trustworthy execution layer for AI-assisted data querying: bounded query candidates, deterministic SQL compilation, reviewable execution, evidence, and auditability. Contributions should preserve explicit failure modes and honest capability boundaries rather than optimize for impressive demos.

## Good contribution areas

The most useful contributions today are:

- minimal bug reproductions;
- regression tests for observable behavior;
- reproducible benchmark cases and evaluation tooling;
- SQLite, PostgreSQL, and MySQL dialect compatibility;
- documentation and deployment corrections;
- Registry and semantic-layer examples;
- LLM/provider compatibility with recorded configuration;
- failure triage that turns a real failure into a reusable test.

Large product-surface additions, new orchestration paths, or changes that bypass review and execution guardrails should start with an Issue. Forge deliberately avoids a second task truth source or an execution path that can skip authorization.

## Development setup

Forge requires Python 3.11 or newer.

```bash
git clone https://github.com/shisuidata/Forge.git
cd Forge
bash scripts/bootstrap-dev.sh
source .venv/bin/activate
forge quickstart --workdir .forge/independent-run
```

The Quickstart requires no `.env`, API key, LLM, embedding service, Pi, Forge JSON, or existing database. It must show one `assurance/readonly_violation` fail-closed result before the reviewed read-only query succeeds. The retained `summary.json` contains a privacy-bounded `run_receipt` and checksum.

Only the broader natural-language demo requires local configuration:

```bash
cp .env.example .env
```

Never commit `.env`, API keys, database credentials, customer data, production query results, or unsanitized receipts.

The optional Pi orchestrator requires Node.js 22.19 or newer:

```bash
npm --prefix services/pi-orchestrator ci
npm --prefix services/pi-orchestrator run typecheck
npm --prefix services/pi-orchestrator test
```

To run the configured local web application outside Quickstart:

```bash
uvicorn main:app --host 127.0.0.1 --port 8000
```

See [`README.md`](README.md) for the public path and [`docs/production-deployment.md`](docs/production-deployment.md) for deployment boundaries.

### Reporting an independent Quickstart run

Use the [Quickstart adoption report](https://github.com/shisuidata/Forge/issues/new?template=quickstart-adoption.yml). Submit the `run_receipt` object from `.forge/independent-run/summary.json`, the tested release or commit, fresh-clone setup time, first failure or confusing step, and your interpretation of the Policy verdict, Evidence integrity, and limitations.

Forge sends no telemetry. The receipt excludes hostnames, usernames, paths, SQL rows, credentials, and private schemas. Its checksum detects drift and supports deduplication; it does not attest identity. The GitHub-authored report supplies public provenance. Maintainer-authored runs, stars, and forks do not satisfy the external-adoption gate.

## Tests

Run the Python suite before opening a pull request:

```bash
.venv/bin/python -m pytest tests -q
```

The SQLite compatibility check used by CI can be run directly:

```bash
.venv/bin/python -m pytest tests/test_database_compatibility.py -v --tb=short
```

PostgreSQL and MySQL compatibility checks need disposable local databases and the `FORGE_SMOKE_DATABASE_URL` environment variable. The exact CI setup is documented in [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

### Adding tests

- Test observable behavior, boundaries, failure modes, and invariants.
- Add the smallest regression that fails before the fix and passes after it.
- Keep tests deterministic and independent of personal credentials or live customer systems.
- For cross-dialect changes, cover every affected dialect instead of special-casing one fixture.
- Do not weaken an existing assertion merely to make a change pass.

## Correctness bug reports

A useful minimal reproduction includes:

1. Forge commit or release version;
2. input kind (`direct_sql` or `forge_json`);
3. database dialect and version;
4. minimal sanitized schema and Registry entries;
5. natural-language intent or expected query semantics, when relevant;
6. generated candidate and compiled SQL;
7. expected result shape and actual result or bounded error code;
8. whether the SQL was reviewed and executed;
9. a small synthetic dataset when result values are required to reproduce the issue.

Remove credentials, internal hostnames, customer names, and sensitive row values. A synthetic schema is preferred.

## Benchmark contributions

Benchmark results are useful only when they are reproducible and comparable.

Include the dataset name and revision, case selection, model and provider revision, prompt/Registry revision, temperature, retry and timeout policy, number of runs, evaluator, and metric definition. Keep official metrics separate from diagnostic comparators.

Do not:

- invent, estimate, or manually improve results;
- compare runs that use different datasets, case filters, retry policies, or evaluators as if they were equivalent;
- hide compile, execution, timeout, or retrieval failures;
- tune behavior to named case IDs;
- include proprietary data without permission.

A benchmark contribution should include the cases or a public retrieval procedure, the runner configuration, raw bounded outcomes, and enough information to reproduce the aggregate.

## Pull requests

1. Open or reference an Issue for behavior changes that affect contracts, architecture, or the public workflow.
2. Keep the change focused; do not mix refactors with unrelated behavior changes.
3. Explain the problem, decision, compatibility impact, and verification performed.
4. Update documentation when a public command, contract, capability, or limitation changes.
5. Run the relevant Python tests and, when touched, the Pi typecheck and tests.
6. Preserve backward compatibility only when it is an explicit project requirement; otherwise prefer a complete, documented cutover.

Small documentation fixes and well-scoped regression tests do not need a design proposal.

## Security and sensitive information

Do not paste secrets, access tokens, connection strings, private schemas, customer data, or exploitable production details into a public Issue or pull request. Revoke any credential that was exposed. If a report cannot be safely described in public, open a non-sensitive Issue requesting a private maintainer contact path without including the sensitive details.

## Maintainer context

[`shisuidata/Forge`](https://github.com/shisuidata/Forge) is the canonical repository under the [`shisuidata`](https://github.com/shisuidata) organization. [`rockythink`](https://github.com/rockythink) is the organization administrator and primary maintainer. Historical commits attributed to the former `shisuidata` user now appear under [`shisuidata-legacy`](https://github.com/shisuidata-legacy); that account is retained only for history and is no longer used for project operations.
