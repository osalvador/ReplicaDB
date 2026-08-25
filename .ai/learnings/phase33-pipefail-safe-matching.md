---
type: Learning
description: Under strict shell mode, avoid quiet consumers in pipelines whose producers must write complete captured output.
sources:
  - id: plan
    resource: .ai/archive/phase-3-3-api-high-availability-and-operational-hardening.plan.md
generated: { by: itx-code, at: "2026-08-25T13:42:47Z" }
status: stable
---

After CI portability was fixed, the load gate still failed even though the expected metric was present. `grep -q` exited after the match, `printf` received `SIGPIPE`, and `pipefail` converted that successful assertion into a failure.

The smoke and load gates now capture output first and match it with here-strings. Strict shell validation should exercise the success path with `pipefail` enabled and avoid early-closing consumers for generated output.
