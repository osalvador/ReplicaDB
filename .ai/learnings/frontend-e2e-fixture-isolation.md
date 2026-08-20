---
type: Learning
description: Parallel browser tests need environment-aware fixtures, SPA navigation, and isolated mutation semantics.
sources:
  - id: seed-script
    resource: replicadb-server/frontend/scripts/seed-local-jobs.mjs
  - id: e2e-layout
    resource: replicadb-server/frontend/e2e/responsive-layout.spec.ts
  - id: e2e-control-plane
    resource: replicadb-server/frontend/e2e/visual-control-plane.spec.ts
  - id: e2e-workflow
    resource: .github/workflows/CT_Push.yml
generated: { by: itx-init/2.1, at: "2026-08-20T12:55:57Z" }
status: stable
---

Three independent harness mismatches caused browser CI failures. Direct navigation to nested SPA URLs reached the Spring server and returned 404, while navigation through `/` exercised the real client router. Local PostgreSQL can use a dynamically selected port, so seeded JDBC URLs must use `REPLICADB_POSTGRES_PORT`. Finally, parallel workers sharing one seeded job raced on active-run protection.

Keep browser credentials environment-managed, navigate through user-visible links instead of direct deep links, and parameterize fixture endpoints from the test environment. For tests that only need a terminal run to inspect the UI, add the local-seed request contract in that test rather than serializing the whole suite or sharing an active mutation across workers.