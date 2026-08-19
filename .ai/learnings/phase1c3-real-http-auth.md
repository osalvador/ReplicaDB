---
type: Learning
description: Classify test clients before planning authentication coverage because MockMvc and real HTTP sessions exercise different behavior.
sources:
  - id: plan
    resource: .ai/archive/phase-1c-3-security.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Real-port lifecycle tests using `TestRestTemplate` cannot be authenticated by MockMvc annotations. Reproduce login, JDBC session cookies, CSRF cookies, and headers in the real client, while keeping MockMvc for isolated controller behavior.
