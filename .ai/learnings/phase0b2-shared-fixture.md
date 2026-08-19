---
type: Learning
description: Prefer read-only assertions when a Testcontainers fixture is a JVM-wide singleton.
sources:
  - id: plan
    resource: .ai/archive/phase-0b2-watermark-injection.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The shared PostgreSQL fixture made inserted rows capable of changing unrelated test expectations. Before mutating a container, determine its lifecycle scope; for singleton fixtures, choose existing data and a different query boundary or create an isolated schema.
