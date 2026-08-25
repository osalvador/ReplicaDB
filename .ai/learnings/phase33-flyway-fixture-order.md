---
type: Learning
description: Let metadata migrations own first schema initialization; load test data only after Flyway completes.
sources:
  - id: plan
    resource: .ai/archive/phase-3-3-api-high-availability-and-operational-hardening.plan.md
generated: { by: itx-code, at: "2026-08-25T13:42:47Z" }
status: stable
---

Mounting the Phase 3 source/sink fixture into PostgreSQL's initialization directory created tables before Flyway. The managed application then rejected the non-empty schema because no migration history existed.

The Compose harness now starts PostgreSQL and Flyway first, then loads the non-secret fixture through `psql`. Fixtures must not preempt the migration owner unless they are themselves versioned migrations.
