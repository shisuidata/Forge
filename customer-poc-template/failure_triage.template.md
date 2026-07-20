# Failure Triage

## Run Metadata

- Customer:
- Schema version:
- Registry commit:
- Provider / model:
- Runs per case:
- Date:

## Summary

| Metric | Value |
|---|---:|
| Case EA(any) | |
| Case EA(all) | |
| Run ACC | |
| Compile failures | |

## Failures

| Case | Question | Root cause | Owner | Fix target | Status |
|---|---|---|---|---|---|
| C001 | | Registry / compiler / lint / dialect / model boundary | | | pending |

## Root Cause Rules

- Registry: missing metric, field convention, enum, or business definition.
- Compiler/lint/schema: the error can be deterministically prevented.
- Dialect: SQL differs across SQLite/PostgreSQL/MySQL/data warehouse engines.
- Model boundary: requires algorithm planning beyond Forge's current DSL.
- Question ambiguity: product should ask for clarification before generating SQL.
