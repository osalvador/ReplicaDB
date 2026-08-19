---
type: Learning
description: Check nullable alternatives at persistence, audit, summary, and serialization boundaries.
sources:
  - id: plan
    resource: .ai/archive/new-job-wizard-parity.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

A valid query-only definition reached audit code that called `Map.of` with a null table value. When a field becomes optional, inspect every summary, audit, mapper, and serializer path; use an explicit non-sensitive marker where the value is absent.
