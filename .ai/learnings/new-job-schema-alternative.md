---
type: Learning
description: Migrate the original database constraint when adding an alternative representation to a required field.
sources:
  - id: plan
    resource: .ai/archive/new-job-wizard-parity.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Adding `source_query` required changing the earlier `source_table NOT NULL` constraint and moving the table-or-query invariant into domain/API validation. Review old migrations and constraints whenever a new representation makes an existing field optional.
