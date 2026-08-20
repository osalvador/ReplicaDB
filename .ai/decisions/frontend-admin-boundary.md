---
type: Decision
description: Frontend administration consumes existing backend user and ACL contracts and adds only an ADMIN UX boundary.
sources:
  - id: routes
    resource: replicadb-server/frontend/src/router/routes.tsx
  - id: admin-page
    resource: replicadb-server/frontend/src/pages/JobPermissionsPage.tsx
  - id: access
    resource: replicadb-server/src/main/java/org/replicadb/server/security/JobAccessService.java
  - id: e2e
    resource: replicadb-server/frontend/e2e/admin-management.spec.ts
generated: { by: itx-init/2.1, at: "2026-08-20T11:00:36Z" }
status: stable
---

Driving forces: expose user and job-permission administration without changing the backend contract or creating a second authorization model.

Decision: ADMIN-only navigation, `RequireRole` routes, user dialogs, and the per-job permission editor live in the SPA. API modules alias generated schemas, mutations invalidate queries, and backend `@PreAuthorize`/`JobAccessService` checks remain authoritative. The Phase 2c admin slice is implemented without changing the backend ACL contract.

Trade-off: the current permission user picker requests a bounded page rather than server-side search, so larger user populations need a later search capability. Browser E2E coverage depends on environment-managed bootstrap credentials.
