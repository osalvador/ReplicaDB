---
type: Learning
description: Keep local PostgreSQL data fixtures in a schema separate from Flyway metadata.
sources:
  - id: plan
    resource: .ai/archive/local-pg2pg-integration-fixture.plan.md
generated: { by: itx-init/2.1, at: "2026-09-04T05:47:18Z" }
status: stable
---

Flyway refuses to initialize a non-empty public schema without its history table. Local replication fixtures therefore belong in a dedicated schema, use fully qualified table names, and set the datasource `currentSchema` accordingly. Do not use control-plane tables as example source or sink tables.
