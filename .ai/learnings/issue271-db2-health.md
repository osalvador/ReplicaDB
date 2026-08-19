---
type: Learning
description: Treat a running reused database container as unverified until JDBC initialization and logs confirm health.
sources:
  - id: plan
    resource: .ai/archive/issue-271-db2-rn-partition.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The reused DB2 container reported `Up` while internal vendor processes had stopped and JDBC initialization failed. Check the driver connection and container logs before attributing an integration failure to product code, especially under emulation or long-lived reuse.
