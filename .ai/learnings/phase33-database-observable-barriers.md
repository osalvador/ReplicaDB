---
type: Learning
description: Kill and restart tests must wait on database-visible operation state and terminate helper backends explicitly.
sources:
  - id: plan
    resource: .ai/archive/phase-3-3-api-high-availability-and-operational-hardening.plan.md
generated: { by: itx-code, at: "2026-08-25T13:42:47Z" }
status: stable
---

Fixed sleeps were insufficient to place a run deterministically in source copy, merge, or recovery. A PostgreSQL lock-holder process could also outlive its local shell and prevent the replacement attempt from becoming eligible.

The failure harness uses `pg_sleep`, `pg_stat_activity`, `pg_locks`, explicit backend termination, and health-aware worker restarts. Database-backed lifecycle tests should observe the operation state they intend to interrupt instead of inferring it from elapsed time.
