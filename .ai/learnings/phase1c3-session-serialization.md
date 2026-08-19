---
type: Learning
description: Test serialization of the complete authenticated principal graph for JDBC-backed sessions.
sources:
  - id: plan
    resource: .ai/archive/phase-1c-3-security.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

A successful password match still failed when Spring Session serialized the security context because the wrapped domain user was not serializable. Durable session plans need a login-to-session-reload test, not only authentication unit assertions.
