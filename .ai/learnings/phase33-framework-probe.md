---
type: Learning
description: Resolve framework behavior and effective dependency APIs with a minimal executable probe before building dependent runtime tasks.
sources:
  - id: plan
    resource: .ai/archive/phase-3-3-api-high-availability-and-operational-hardening.plan.md
generated: { by: itx-code, at: "2026-08-25T13:42:47Z" }
status: stable
---

Phase 3.3 exposed two framework assumptions before the runtime was complete: the available Java selected by the shell was not usable for Maven, and Spring Boot 3.3.5 did not create the required management child context when `web-application-type=none`. The Prometheus exporter also required an explicit enabled property.

The implementation resolved Java 17 explicitly, verified Quartz 2.3.2 APIs/DDL from the effective dependency, and used a servlet-capable worker context with `server.port=-1` plus an internal management port. Framework probes should run before dependent health, metrics, or profile work.
