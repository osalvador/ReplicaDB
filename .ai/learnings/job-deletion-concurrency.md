---
type: Learning
description: Safe physical deletion of job-owned state with PostgreSQL and active-run protection.
sources:
  - id: plan
    resource: .ai/archive/physical-job-deletion.plan.md
generated: { by: itx-code, at: "2026-09-03T21:10:00+02:00" }
status: draft
---

Physical job deletion must lock the parent definition, reject active run statuses, and use PostgreSQL foreign-key cascades for schedule, permissions, runs, logs, and job-scoped idempotency. A separate-connection test with a barrier proves that a run insert attempting after the parent lock cannot survive a committed definition delete. Preserve independent audit events because the deleted definition can no longer identify itself.
