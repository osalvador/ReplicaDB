---
type: Learning
description: ApplicationContextRunner system properties are not a complete substitute for OS environment-variable binding.
sources: [{ id: plan, resource: .ai/archive/keyring-lifecycle-status-re-encryption-and-flat-environment-configuration.plan.md }]
generated: { by: itx-code, at: "2026-09-10" }
status: stable
---

Test dotted configuration-property binding in ApplicationContextRunner and test uppercase environment names through the actual placeholder or Environment source. Do not assume `withPropertyValues("UPPER_SNAKE_CASE=...")` reproduces Spring Boot environment-variable relaxed binding.
