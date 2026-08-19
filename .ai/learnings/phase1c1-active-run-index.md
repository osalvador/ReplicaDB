---
type: Learning
description: Evaluate partial unique indexes against every multi-row state transition before finalizing the predicate.
sources:
  - id: plan
    resource: .ai/archive/phase-1c-1-rest-api-core.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Retry transitions the failed row to `RETRY_SCHEDULED` before inserting a replacement `PENDING` row in one transaction. The active-run predicate must exclude transitional rows that coexist with the replacement, while still covering executable states.
