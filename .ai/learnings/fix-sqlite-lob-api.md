---
type: Learning
description: Use the JDBC methods actually implemented by SQLite rather than assuming optional LOB APIs are portable.
sources:
  - id: plan
    resource: .ai/archive/fix-sqlite-null-handling-test.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The SQLite driver did not implement `getBlob()` or `getClob()`. Use `getBytes()` and `getString()` for the supported SQLite binary/text paths, and keep the behavior covered by a real-driver test rather than a JDBC mock.
