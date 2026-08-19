---
type: Learning
description: Wait for the externally reachable mapped port when an integration test connects through a Testcontainers JDBC URL.
sources:
  - id: plan
    resource: .ai/archive/phase-1b-state-layer.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

A container can report started while its host-mapped database port still refuses connections. Add an explicit listening-port readiness check when raw Flyway or JDBC clients connect through the mapped URL.
