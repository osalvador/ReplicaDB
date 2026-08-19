---
type: Learning
description: Verify positional bind counts per manager before grouping SQL changes under a shared abstraction.
sources:
  - id: plan
    resource: .ai/archive/phase-0b2-watermark-injection.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Standard JDBC, MySQL, and SQLite had different pagination bind shapes. Inspect each current manager's generated SQL and bind order instead of relying on a shared `readTable` narrative; test every affected permutation.
