---
type: Learning
description: Prefer object null assertions when a driver's primitive getter does not maintain a reliable wasNull state.
sources:
  - id: plan
    resource: .ai/archive/fix-sqlite-null-handling-test.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

SQLite's `getBigDecimal()` on a null value did not update the driver's last-column state. For object-valued JDBC assertions, `getObject()` or the typed object getter can be safer than assuming `getter(); wasNull()` behaves identically across drivers.
