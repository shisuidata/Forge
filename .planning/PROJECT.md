# Forge

## What This Is

Forge is a trusted business data question-answering assistant for small data teams that already have a database or warehouse but incomplete semantic governance. It reduces silent errors inside supported boundaries through registered semantics, provenance, permissions, evidence, deterministic compilation, and review; it does not promise open-world correctness.

The active project cycle is limited to S0 Design Partner / Problem Baseline. It establishes the real partner and problem evidence required before any new runtime capability can be considered.

## Core Value

Complete the S0 Design Partner / Problem Baseline for one small data team by fixing one business domain, datasource, semantic owner, real question set, privacy/authorization boundary, and current manual-process baseline.

## Business Context

- **Customer**: A small data team with an existing database or warehouse, recurring ad hoc business questions, and incomplete semantic governance
- **Revenue model**: Not established in S0; this cycle validates the problem and partner baseline rather than monetization
- **Success metric**: One eligible design partner has all seven S0 baseline elements explicitly recorded and reviewable
- **Strategy notes**: The long-term direction remains authoritative in [`docs/product-north-star.md`](../docs/product-north-star.md); current scope and precedence remain authoritative in [`docs/current-project-state.md`](../docs/current-project-state.md)

## Requirements

### Validated

Existing completed engineering foundations are reusable context, not active GSD requirements. Their current status remains authoritative in [`docs/current-project-state.md`](../docs/current-project-state.md); historical plans and completed milestones are not imported into this roadmap.

### Active

- [ ] Identify one eligible small data-team design partner with an existing queryable database or warehouse, recurring ad hoc questions, and incomplete semantic governance.
- [ ] Fix one bounded business domain for the baseline.
- [ ] Fix one existing queryable datasource and its authorized S0 testing boundary without adding a connector or ETL program.
- [ ] Name one semantic owner or steward authorized to confirm business definitions.
- [ ] Establish a corpus of real historical questions plus a way to collect ongoing questions from actual work.
- [ ] Agree the privacy, authorization, evidence-retention, prohibited-data, exit, and deletion boundary.
- [ ] Record the current manual process for question intake, SQL authoring, clarification, review, delivery, and silent-error discovery.

### Out of Scope

- S1 Direct Trusted Answer, S2 Semantic Learning Loop, and S3 validation implementation — each requires new evidence and explicit user approval after S0.
- M1A, Agent Runtime, a new Runtime, additional connectors, general ETL, and enterprise-platform expansion — not authorized by the current phase.
- New Product Shell pages, Decision Center, Economics/Outcome Ledger, Reusable Report, additional channels, and non-SQL actions — paused while the core problem baseline is established.
- Connecting real customer data, handling production credentials, or deploying to a real customer environment — requires explicit authorization and a separately approved data boundary.
- Treating W2 visual confirmation, Atlas candidate revalidation, or Runtime Governance Coverage as S0 implementation work — these remain unresolved acceptance facts, not active phases.

## Context

- [`docs/current-project-state.md`](../docs/current-project-state.md) is the authoritative current projection and fixes S0 as the only active work.
- The accepted requirement is `REQ-2026-08-25-023`; its active S0 gate is detailed in [`docs/requirements-pool.md`](../docs/requirements-pool.md) and the sole active plan [`docs/forge-enterprise-evolution-plan.md`](../docs/forge-enterprise-evolution-plan.md).
- The repository already contains a Python 3.11+ FastAPI/Forge execution plane and a Node 22.19+ TypeScript Pi control plane. This cycle preserves them and does not add a runtime.
- Existing Product Projection contracts are reusable completed foundation. They remain read-only projections of Pi/Forge truth and do not authorize new product-surface work.
- S0 must use questions from real partner work rather than questions reverse-designed for a demonstration. A demo, reference workspace, or low-frequency personal dataset cannot replace design-partner evidence.

## Constraints

- **Authority**: User decisions override the current-state projection; otherwise follow the authority order in `docs/current-project-state.md` — prevents historical material from becoming active scope.
- **Orchestration**: Pi remains the only primary Orchestrator and Task source of truth — no channel, Skill, projection, or Forge component may create a second task state machine.
- **Execution**: Forge remains the trusted data execution layer with independent validation, refusal, approval, and fail-closed authority — DATA Skills and channels receive no direct database execution authority.
- **Skills**: DATA Skills provide bounded professional methods only; they do not own task state, publish organizational truth, or bypass Forge review and approval.
- **Channels**: Web, Feishu, and DingTalk remain projections/adapters — they do not become business truth stores.
- **Runtime**: Preserve the existing Python API and TypeScript Pi Orchestrator; no new Runtime is in scope.
- **Scope gate**: Only S0 is active — S1-S3, M1A, Agent Runtime, connector expansion, and enterprise-platform work require new evidence and explicit user approval.
- **Safety**: Before real partner data is introduced, S0 must establish least privilege, data residency/private deployment, bounded evidence, Secret/PII exclusions, retention, exit, and deletion boundaries.
- **Correctness**: Claims apply only within supported semantic, provenance, permission, evidence, deterministic-compile, and approval boundaries — no open-world 100% correctness claim.
- **Existing gaps**: W2 visual confirmation, Atlas revalidation, and Runtime Governance Coverage at 0% must remain visible and must not be silently marked complete or converted into implementation scope.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Use one S0-only roadmap phase | The current projection explicitly authorizes only the design-partner/problem baseline | — Active |
| Preserve the Python API and TypeScript Pi Orchestrator | Existing runtime ownership is established; a new Runtime is not approved | — Active |
| Keep Pi as Task truth and Forge as execution truth | Prevents split orchestration, bypassed assurance, and conflicting channel state | — Active |
| Reference rather than restate the Product North Star | `docs/product-north-star.md` remains the authoritative long-term direction | — Active |
| Preserve unresolved W2, Atlas, and governance facts outside active phase scope | Existing acceptance gaps are not authorization for new implementation phases | — Active |

---
*Last updated: 2026-08-25 after new-project-from-ingest initialization*
