---
type: Learning
description: Verify archive pre-images before constructing runtime images from local packaging inputs.
sources:
  - id: plan
    resource: .ai/archive/azure-sql-authentication.plan.md
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Local archive and image checks needed an explicit pre-image integrity assertion. Before building a container or release archive, validate the source artifact and expected contents so a packaging failure is not mistaken for a runtime defect.
