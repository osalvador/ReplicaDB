---
type: Learning
description: Repeated plan-to-implementation gaps should become explicit preflight checks and candidates for organization-level rules.
sources:
  - id: phase1a
    resource: .ai/archive/phase-1a-artifact-split.plan.md
  - id: phase1b
    resource: .ai/archive/phase-1b-state-layer.plan.md
  - id: phase1c1
    resource: .ai/archive/phase-1c-1-rest-api-core.plan.md
  - id: phase1c3
    resource: .ai/archive/phase-1c-3-security.plan.md
  - id: phase2a
    resource: .ai/archive/phase-2a-frontend-auth-monitoring.plan.md
  - id: phase2c
    resource: .ai/archive/phase-2c-frontend-administration.plan.md
  - id: phase33
    resource: .ai/archive/phase-3-3-api-high-availability-and-operational-hardening.plan.md
generated: { by: itx-code, at: "2026-08-25T13:42:47Z" }
status: draft
---

The archive contains repeated gaps in four families: framework/dependency assumptions, database or container readiness, wire-contract/nullability checks, and test-harness/client mismatch. These recurrences span more than three plans. Phase 3.1 added one-off but actionable gaps around resolved Flyway APIs, database-owned time in compatibility paths, staged repository-contract migration, and documentation-path discovery.

## Gap Recurrence

Candidate for promotion to organization-level instructions: require resolved dependency/API inspection, explicit Flyway targets for staged migrations, database-time consistency checks, migration-bridge callers, documentation path searches, exact-assertion impact searches, explicit MockMvc versus real-port classification, clean-runner package configuration, executable focused checks before broad validation, image/package freshness checks, database-observable failure barriers, baseline-tool-only CI scripts, and pipefail-safe output matching. Phase 3.3 confirms that framework probes, test-profile parity, fixture ordering, process readiness, and CI-shell portability should be treated as preflight requirements. This remains a proposal because no organization baseline was available for comparison.
