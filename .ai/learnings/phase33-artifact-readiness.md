---
type: Learning
description: Process harnesses must validate the packaged artifact and image health, not only compiled classes or published ports.
sources:
  - id: plan
    resource: .ai/archive/phase-3-3-api-high-availability-and-operational-hardening.plan.md
generated: { by: itx-code, at: "2026-08-25T13:42:47Z" }
status: stable
---

The first Compose runs used an older server jar because `mvn test` does not repackage the Spring Boot artifact. Published ports also existed before the JVM had completed startup, producing empty replies and misleading health failures.

The workflow now packages the server before image checks, the image includes healthcheck tooling, and Compose waits on health rather than port publication alone. Image and process tests should always establish artifact freshness and application-level readiness explicitly.
