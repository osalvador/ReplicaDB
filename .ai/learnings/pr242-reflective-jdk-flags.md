---
type: Learning
description: Exercise packaged runtime paths when a Java upgrade affects reflective third-party libraries.
sources:
  - id: plan
    resource: .ai/archive/PR-242.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

ORC compression exposed reflective access to `java.nio` only in packaged Java 17 execution. A major-version migration needs compile, unit, packaged launcher, and relevant integration checks, with module-opening flags kept aligned where required.
