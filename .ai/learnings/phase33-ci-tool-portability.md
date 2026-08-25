---
type: Learning
description: CI validation scripts should depend only on explicitly provisioned tools or baseline POSIX utilities.
sources:
  - id: plan
    resource: .ai/archive/phase-3-3-api-high-availability-and-operational-hardening.plan.md
generated: { by: itx-code, at: "2026-08-25T13:42:47Z" }
status: stable
---

The GitHub-hosted Ubuntu runner did not provide `rg`, although the local development environment did. The first remote run therefore failed in the documentation gate and the next failed in the image gate before reaching the functional checks.

Replacing CI-invoked ripgrep checks with portable `grep` equivalents, including recursive source scanning and explicit error handling, restored the gates. Local tool availability is not evidence that a workflow runner provides the same binary.
