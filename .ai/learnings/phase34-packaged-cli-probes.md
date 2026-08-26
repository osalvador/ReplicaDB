---
type: Learning
description: Packaged CLI probes need explicit bundled-driver setup and machine-readable output.
sources:
  - id: plan
    resource: .ai/archive/phase-3-4-hybrid-worker-load-distribution-and-cli-compatibility-closeout.plan.md
generated: { by: itx-code, at: "2026-08-26" }
status: stable
---

A JShell readback of a fat CLI JAR did not discover the bundled SQLite driver automatically, and REPL prompts were not stable enough for parsing. Load the driver explicitly and print a stable marker before extracting a result. Keep packaged artifact inspection separate from managed database/container readiness.
