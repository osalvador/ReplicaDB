---
type: Learning
description: Disable native form validation when a custom inline validation path must observe empty submissions.
sources:
  - id: plan
    resource: .ai/archive/phase-2c-frontend-administration.plan.md
generated: { by: itx-init/2.1, at: "2026-08-20T11:00:36Z" }
status: stable
---

The password-reset dialog's required input prevented the React submit handler from running, so the planned inline error was never rendered. Use `noValidate` at that form boundary when controlled validation is the intended behavior, and test that blank input does not call the API.
