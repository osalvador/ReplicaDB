---
type: Learning
description: Run the exact production TypeScript and Vite build early for polymorphic MUI and theme composition changes.
sources:
  - id: plan
    resource: .ai/archive/material-3-adapted-visual-system.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

`tsc -b` exposed stricter `SxProps` and polymorphic form-handler errors that no-emit checks and unit tests did not catch. Run the production build before a large frontend slice is considered complete.
