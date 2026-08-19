---
type: Learning
description: Use the production theme and a fresh query client in tests for components that consume custom theme extensions.
sources:
  - id: plan
    resource: .ai/archive/material-3-adapted-visual-system.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Token-backed `sx` callbacks failed in tests that rendered without `ThemeProvider`. Shared theme tokens require a common test wrapper whenever a component reads custom theme extensions, and query state should have an isolated client.
