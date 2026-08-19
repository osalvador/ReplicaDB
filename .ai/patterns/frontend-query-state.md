---
type: Pattern
description: The SPA separates API modules, TanStack Query server state, route guards, and local form state.
sources:
  - id: dashboard
    resource: replicadb-server/frontend/src/pages/DashboardPage.tsx
  - id: schedule
    resource: replicadb-server/frontend/src/components/JobScheduleCard.tsx
  - id: users
    resource: replicadb-server/frontend/src/pages/UsersPage.tsx
  - id: routes
    resource: replicadb-server/frontend/src/router/routes.tsx
  - id: tests
    resource: replicadb-server/frontend/src/pages/JobPermissionsPage.test.tsx
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Pages and reusable cards use `useQuery` for server data and `useMutation` for writes. Successful mutations invalidate the owning query; failed mutations keep the relevant form/dialog open and show the API error detail. Pagination is represented in query keys and controls.

`ProtectedRoute` resolves authentication before protected content, while `RequireRole` provides the ADMIN UX boundary for users and job permissions. Parameterized page tests mount matching `Routes` so `useParams` receives an ID. Form tabs preserve hidden values and custom blank-field validation is explicit where needed.

Reference implementations: `DashboardPage.tsx`, `JobScheduleCard.tsx`, `UsersPage.tsx`, and `routes.tsx`.
