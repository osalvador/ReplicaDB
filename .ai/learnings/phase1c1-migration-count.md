---
type: Learning
description: Search for exact migration-count and version assertions whenever Flyway migrations change.
sources:
  - id: plan
    resource: .ai/archive/phase-1c-1-rest-api-core.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Existing migration tests asserted an earlier exact count and failed after valid forward-only migrations were added. Include migration inventory assertions in the impact analysis and update them with the schema change.
