---
type: Learning
description: Deprecated configuration aliases must cross every pre-application deployment boundary.
sources: [{ id: plan, resource: .ai/archive/keyring-lifecycle-status-re-encryption-and-flat-environment-configuration.plan.md }]
generated: { by: itx-code, at: "2026-09-10" }
status: stable
---

An application-level fallback cannot help when Compose, a launcher, or another adapter resolves configuration before the process starts. Prefer the canonical name in new examples, but preserve the deprecated alias at each boundary during migration.
