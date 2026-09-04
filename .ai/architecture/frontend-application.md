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
  - id: job-form
    resource: replicadb-server/frontend/src/pages/JobFormPage.tsx
  - id: run-history
    resource: replicadb-server/frontend/src/components/RunHistoryTable.tsx
  - id: schema-test
    resource: replicadb-server/frontend/src/api/schema.test.ts
  - id: e2e
    resource: replicadb-server/frontend/e2e/admin-management.spec.ts
generated: { by: itx-init/2.1, at: "2026-09-04T05:47:18Z" }
status: stable
---

The SPA uses React Router, MUI, Axios, and TanStack Query. `src/api` owns the credentialed `/api/v1` client and endpoint modules; `src/auth` owns session state; `src/router` owns `ProtectedRoute` and `RequireRole`; `src/pages` composes dashboard, datasource, job-editor, run, user, and permission screens. The generated schema includes optional retry-policy inputs, resolved policy outputs, run `availableAt`, run diagnostics, and the physical job-delete response contract.

Observed routes include login, dashboard, profile, jobs, datasources, job creation/edit/detail, run detail, ADMIN-only datasource creation and permission pages, users, and job permissions. The desktop shell persists its collapsed-navigation preference locally and offers Dashboard, Jobs, Datasources, and ADMIN-only Users navigation. Admin pages consume existing user and ACL APIs and do not add backend authorization. Mutations invalidate their owning queries and show RFC 7807 detail inline. The Playwright admin flow uses environment-managed credentials and skips when they are not configured.

`src/api/schema.ts` is generated from the server OpenAPI document. Frontend tests use Testing Library, fresh query clients, and matching router context for parameterized pages. Lease tokens remain internal to managed execution and are asserted absent from the schema and UI. Retry-policy forms preserve explicit complete-mode choices, run views show retry eligibility and bounded diagnostic logs, and the dashboard queries access-controlled summaries for a selected fixed or custom time window.

Reference implementations: `replicadb-server/frontend/src/router/routes.tsx`, `replicadb-server/frontend/src/layout/AppLayout.tsx`, `replicadb-server/frontend/src/pages/DashboardPage.tsx`, `src/pages/JobFormPage.tsx`, and `src/api/schema.ts`.
