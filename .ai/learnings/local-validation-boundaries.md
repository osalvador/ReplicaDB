---
type: Learning
description: Separate backend and fixture validation from frontend dependency installation in local acceptance runs.
sources:
  - id: plan
    resource: .ai/archive/local-pg2pg-integration-fixture.plan.md
generated: { by: itx-init/2.1, at: "2026-09-04T05:47:18Z" }
status: stable
---

A real API, PostgreSQL fixture, replication, and destination check can complete while `npm ci` remains blocked by network or certificate conditions. Report those outcomes separately and apply explicit limits to network installation steps so frontend setup does not obscure backend evidence.
