---
type: Learning
description: Treat CLI registration and options-file loading as separate implementation paths.
sources:
  - id: plan
    resource: .ai/archive/phase-0b2-watermark-injection.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Adding an `Option.builder()` entry did not add the matching properties read in `loadOptionsFile()`. Whenever acceptance criteria mention options-file configuration, name and test the property-key path explicitly alongside command-line parsing.
