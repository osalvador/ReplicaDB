---
type: Learning
description: Classify heterogeneous database integration failures by container readiness and architecture before treating them as product regressions.
sources:
  - id: plan
    resource: .ai/archive/phase-3-3-api-high-availability-and-operational-hardening.plan.md
generated: { by: itx-code, at: "2026-08-25T13:42:47Z" }
status: stable
---

The full root CLI suite could not complete on the local Apple Silicon Docker host because the amd64 DB2 image failed during emulated startup. The resulting DB2, MongoDB, SQL Server, and dependent-pair errors were cascading infrastructure failures; the focused CLI Spring-free classpath check passed and GitHub Actions passed the complete matrix.

Record architecture-sensitive container limitations separately from product failures. Keep the CLI compatibility gate in CI or on a runner with native/sufficient database-container support rather than weakening the standalone artifact contract.
