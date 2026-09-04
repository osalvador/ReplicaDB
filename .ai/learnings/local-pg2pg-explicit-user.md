---
type: Learning
description: Declare the container PostgreSQL role explicitly even when local authentication uses trust.
sources:
  - id: plan
    resource: .ai/archive/local-pg2pg-integration-fixture.plan.md
generated: { by: itx-init/2.1, at: "2026-09-04T05:47:18Z" }
status: stable
---

A trust-authenticated local container does not guarantee that the JDBC driver chooses an existing role. Fixtures must set the technical user explicitly, keep the password empty only for this local setup, and avoid relying on the host operating-system username.
