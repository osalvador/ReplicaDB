---
type: Learning
description: Keep authenticated browser tests environment-driven and distinguish missing credentials from application failures.
sources:
  - id: plan
    resource: .ai/archive/new-job-wizard-parity.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The Playwright flow was blocked because bootstrap credentials were absent from the shell. Do not request, generate, or hardcode secrets for validation; use environment-managed inputs and report a skipped credential-gated flow separately from failed product assertions.
