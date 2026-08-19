---
type: Learning
description: Verify the full browser cookie handshake because matching CSRF names does not prove a deferred token was issued.
sources:
  - id: plan
    resource: .ai/archive/phase-2a-frontend-auth-monitoring.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The ignored login request did not emit the CSRF cookie required by logout. A public CSRF bootstrap endpoint, a real cookie jar, and MockMvc plus Playwright coverage are needed to prove login-to-logout behavior.
