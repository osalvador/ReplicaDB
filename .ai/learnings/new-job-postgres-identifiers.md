---
type: Learning
description: Validate every new migration against PostgreSQL identifiers and reserved words before completion.
sources:
  - id: plan
    resource: .ai/archive/new-job-wizard-parity.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The `verbose` column required quoting for PostgreSQL compatibility. Run the migration against the target engine and inspect generated SQL for common-word identifiers instead of relying on a parser or another database's acceptance.
