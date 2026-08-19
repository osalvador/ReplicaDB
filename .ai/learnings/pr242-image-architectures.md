---
type: Learning
description: Validate image manifests and package-manager commands on every target architecture before choosing a runtime base.
sources:
  - id: plan
    resource: .ai/archive/PR-242.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The Java 17 Alpine image and its BusyBox user-management commands did not match the supported ARM64 target. Inspect manifests and execute image setup commands on each target architecture before standardizing a base image.
