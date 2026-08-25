# Requirements: Forge

**Defined:** 2026-08-25
**Core Value:** Complete the S0 Design Partner / Problem Baseline for one small data team by fixing one business domain, datasource, semantic owner, real question set, privacy/authorization boundary, and current manual-process baseline.
**Source requirement:** [`REQ-2026-08-25-023`](../docs/requirements-pool.md#req-2026-08-25-023以任务驱动语义治理重排-forge-下一阶段产品路线)

## v1 Requirements

The active v1 is the S0 exit gate only. Each requirement is an observable part of the baseline and maps exactly once to the active roadmap.

### S0 Design Partner / Problem Baseline

- [ ] **S0-01**: The product owner can identify one eligible design partner: a small data team with an existing queryable database or warehouse, recurring ad hoc business questions, and incomplete semantic governance.
- [ ] **S0-02**: The design partner and product owner can agree one bounded business domain for the baseline.
- [ ] **S0-03**: The design partner can identify one existing queryable datasource and an authorized S0 testing boundary without requiring a new connector or ETL program.
- [ ] **S0-04**: The design partner can name one semantic owner or steward with authority to confirm business definitions for the selected domain.
- [ ] **S0-05**: The team can provide a reviewable corpus of real historical questions and a mechanism for collecting ongoing questions from actual work rather than demo-designed prompts.
- [ ] **S0-06**: The design partner and product owner can agree a documented privacy and authorization boundary covering private deployment or data residency, least privilege, permitted bounded evidence, prohibited Secret/PII access or display, retention, exit, and deletion.
- [ ] **S0-07**: The product owner can measure the current manual process, including how questions enter, who writes SQL, clarification count, review method, delivery method, and known silent-error discovery.

## Deferred / Unapproved

No S1-S3, M1A, Agent Runtime, connector expansion, or enterprise-platform requirement is active in this roadmap. Those areas require new evidence and explicit user approval before they can move into v1.

## Out of Scope

| Feature or claim | Reason |
|------------------|--------|
| S1 Direct Trusted Answer implementation | Candidate follow-on only; S0 evidence and explicit approval are required first |
| S2 Semantic Learning Loop implementation | Candidate follow-on only; not authorized by the active current-state projection |
| S3 multi-environment validation implementation | Candidate follow-on only; not authorized as current work |
| M1A, Agent Runtime, or any new Runtime | Explicitly deferred; preserve the existing Python API and TypeScript Pi Orchestrator |
| Additional connectors, general ETL, or enterprise-platform expansion | Would change the product cut and requires new partner evidence plus approval |
| Real customer data connection, production credentials, or deployment | Requires an explicitly approved privacy, authorization, and operational boundary |
| New Product Shell, channel, Decision Center, Economics/Outcome Ledger, Reusable Report, or non-SQL Action work | Paused to avoid expanding implementation before the core problem is validated |
| Closing W2 visual confirmation or Atlas candidate revalidation | Unresolved human acceptance facts; they do not authorize an S0 implementation phase |
| Claiming Runtime Governance Coverage above 0% | Contract Coverage is not production enforcement; no runtime governance implementation is approved |
| A second task or business truth source | Pi is the only primary Orchestrator and Task source of truth; channels remain projections |
| DATA Skills owning Task state or direct database execution | Skills are a bounded methods layer; Forge retains trusted execution authority |
| Open-world 100% correctness | Forge only reduces silent errors within supported semantic, provenance, permission, evidence, compile, and approval boundaries |

## Known Unresolved Acceptance Facts

These facts remain visible but are not active v1 requirements or roadmap phases:

- W2 body-content rules still require user visual confirmation.
- Product Spine and full Product Shell Atlas candidates still require user revalidation.
- Runtime Governance Coverage remains 0%; Contract Coverage does not substitute for production execution coverage.

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| S0-01 | Phase 1 | Pending |
| S0-02 | Phase 1 | Pending |
| S0-03 | Phase 1 | Pending |
| S0-04 | Phase 1 | Pending |
| S0-05 | Phase 1 | Pending |
| S0-06 | Phase 1 | Pending |
| S0-07 | Phase 1 | Pending |

**Coverage:**
- v1 requirements: 7 total
- Mapped to phases: 7
- Unmapped: 0
- Duplicate mappings: 0

---
*Requirements defined: 2026-08-25*
*Last updated: 2026-08-25 after new-project-from-ingest initialization*
