---
type: Learning
description: Keep lockfiles and npm configuration registry-neutral and validate npm ci in a clean runner-like environment.
sources:
  - id: plan
    resource: .ai/archive/phase-2a-frontend-auth-monitoring.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

A development-machine lockfile referenced an inaccessible private registry. Rewrite committed resolution metadata to the intended public/project-approved registry policy and test `npm ci` without relying on local configuration.
