---
type: Learning
description: Keep reproducible CI downloads separate from local certificate and browser limitations.
sources:
  - id: plan
    resource: .ai/archive/phase-2a-frontend-auth-monitoring.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Local trust policy blocked Node and Playwright downloads even though CI needed reproducible official downloads. Keep CI configuration strict and document a system-browser or installed-tool fallback for local validation without weakening the build.
