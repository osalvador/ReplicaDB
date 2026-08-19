---
type: Learning
description: Validate logging implementation compatibility before a managed server invokes the embedded CLI core.
sources:
  - id: plan
    resource: .ai/archive/phase-1b-state-layer.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Core Sentry initialization expected a Log4j2 context while Spring Boot's default bridge supplied an SLF4J context. When embedding the core, align exclusions and Log4j2 starters before exercising the execution path.
