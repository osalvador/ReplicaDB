---
type: Component System
description: The React SPA composes protected operational and ADMIN-only screens over generated API modules and TanStack Query.
sources:
  - id: routes
    resource: replicadb-server/frontend/src/router/routes.tsx
  - id: auth
    resource: replicadb-server/frontend/src/auth/AuthContext.tsx
  - id: client
    resource: replicadb-server/frontend/src/api/client.ts
  - id: admin-users
    resource: replicadb-server/frontend/src/pages/UsersPage.tsx
  - id: admin-permissions
    resource: replicadb-server/frontend/src/pages/JobPermissionsPage.tsx
  - id: e2e
    resource: replicadb-server/frontend/e2e/admin-management.spec.ts
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

The SPA uses React Router, MUI, Axios, and TanStack Query. `src/api` owns the credentialed `/api/v1` client and endpoint modules; `src/auth` owns session state; `src/router` owns `ProtectedRoute` and `RequireRole`; `src/pages` composes operational, job-editor, run, user, and permission screens.

Observed routes include login, dashboard, job creation/edit/detail, run detail, ADMIN-only users, and ADMIN-only job permissions. Admin pages consume existing users and job-permission APIs and do not add backend behavior. Mutations invalidate their owning queries and show RFC 7807 detail inline. The Playwright admin flow uses environment-managed credentials and skips when they are not configured.

`src/api/schema.ts` is generated from the server OpenAPI document. Frontend tests use Testing Library, fresh query clients, and matching router context for parameterized pages.

Reference implementations: `replicadb-server/frontend/src/router/routes.tsx`, `src/pages/UsersPage.tsx`, and `src/api/schema.ts`.
