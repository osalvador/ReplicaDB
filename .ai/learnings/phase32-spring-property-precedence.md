---
type: Learning
description: Verify effective Spring profile properties when bootstrapping API and worker runtimes.
sources: [{ id: plan, resource: .ai/archive/phase-3-2-worker-runtime-and-postgresql-dispatch.plan.md }]
generated: { by: itx-code, at: "2026-08-24" }
status: stable
---

# Spring Property Precedence in Shared Contexts

When a distributed integration harness starts multiple Spring contexts with profile YAML, `SpringApplicationBuilder.properties(...)` may not override profile defaults such as an empty datasource URL. Pass required test-only values as Spring Boot `--property=value` application arguments when command-line precedence is needed. Verify the resulting datasource and schema before adding lifecycle assertions.
