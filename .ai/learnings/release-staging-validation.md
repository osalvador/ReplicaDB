---
type: Learning
sources:
  - id: plan
    resource: .ai/archive/release-1.0.0-controlada.plan.md
generated:
  by: itx-code
  at: 2026-09-06
---

Release staging exact-set checks must not create helper files inside the
staging directory being enumerated. Process substitutions keep `expected.list`
and `actual.list` out of the published set while preserving a strict diff.
