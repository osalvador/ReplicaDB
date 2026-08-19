# Implementation Plan: Phase 2c — Frontend Administration

## Task Source

No JIRA ticket. Source is [ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md), "Phase 2: Frontend" → "Phase 2c: Administration — PENDING" ("Job permission editor and user/role administration for `ADMIN` users") and Priority 3 ("Add the job permission editor and user/role administration screens to the frontend").

Acceptance criteria (derived from the architecture doc and confirmed with the user):
- **AC1**: An `ADMIN` user can list, create, and manage local users (role, enabled flag, password reset) through the frontend, backed by the existing `/api/v1/users` endpoints.
- **AC2**: An `ADMIN` user can view and manage per-job permission grants (`VIEW`/`EDIT`/`EXECUTE`/`CANCEL`) for any user, backed by the existing `/api/v1/jobs/{id}/permissions` endpoints.
- **AC3**: The job permission editor lives at its own route (`/jobs/:id/permissions`), reached from a "Manage permissions" action on the job detail page, visible only to `ADMIN` (frontend UX gate; backend `JobAccessService`/`@PreAuthorize` remain the authority).
- **AC4**: A persistent "Users" navigation link is visible only to `ADMIN` in the app header.
- **AC5**: A non-`ADMIN` user who navigates directly to an admin-gated route sees a friendly "Not authorized" page, not a raw 403 or a silent redirect.
- **AC6**: The backend contract is unchanged — Phase 2c is frontend-only, exactly like Phase 2b.

## Overview

Phases 1c-3a/b already implemented local-user authentication, global roles (`ADMIN`/`OPERATOR`/`VIEWER`), and per-job ACLs (`VIEW`/`EDIT`/`EXECUTE`/`CANCEL`) entirely in the backend, including `UserController` (ADMIN-only CRUD), `JobPermissionController` (per-job grant/revoke, gated by `EDIT` with ADMIN bypass), and their OpenAPI-generated frontend types. Phase 2c closes the last gap named in the architecture document: there is no frontend screen for either capability yet. This plan adds two admin screens (`UsersPage`, `JobPermissionsPage`), a reusable `ADMIN`-only route guard, a "Not authorized" fallback page, and the corresponding navigation/entry-point wiring — with zero backend changes, matching Phase 2b's precedent of consuming an already-validated API contract.

## Architecture & Design

**Approach**: Pragmatic Reuse — every new screen follows an existing, already-tested pattern instead of introducing new abstractions:
- Paginated list + dialog forms: mirrors `DashboardPage.tsx` (pagination) and `JobScheduleCard.tsx` (modal dialog, inline `Alert` error surface via `ApiError.detail`, `useMutation`/`useQuery` with query invalidation).
- User picker for granting job access: mirrors the `Autocomplete` + `getOptionLabel`/`isOptionEqualToValue` pattern already used for timezone selection in `JobScheduleCard.tsx`.
- Route guarding: mirrors `ProtectedRoute.tsx`'s hardcoded-behavior style (no configurable props), extended with a role check.
- Rejected alternative: a generic `AdminResourceTable<T>` abstraction shared by both screens. Rejected because the two screens have incompatible data shapes (`UserResponse` is paginated via `PageResponseUserResponse`; `JobPermissionResponse` is an unpaged, per-job array grouped by user) and there are only two call sites — this is the "abstraction for a one-time operation" anti-pattern the project's implementation discipline explicitly avoids.

**Integration points**:
- `src/api/client.ts`'s `apiClient`/`ApiError` (RFC 7807 detail surfaced inline, same as every existing mutation).
- `src/auth/useAuth()` (`user.role`) for both the UX-only nav/action gating and the new route guard.
- `src/api/schema.ts` generated types: `UserResponse`, `UserRequest`, `RoleUpdate`, `PasswordUpdate`, `PageResponseUserResponse`, `JobPermissionRequest`, `JobPermissionResponse` — no hand-maintained DTOs.
- Backend pagination default/max (`page`/`size`, max 200) from Decision 4 — the job-permissions user picker fetches one page of size 200 rather than adding server-side search, since the admin user base is expected to stay well under that bound.

> ⚠️ Critic note: `listUsers(0, 200)` is a hard, silent cap — a 201st user would be unselectable in the grant dialog with no error or indication. Accepted as a known MVP limitation for Phase 2c (consistent in spirit with Decision 4's own 200-row pagination maximum), but Task 3.2 makes this explicit rather than leaving it implicit, and any future plan adding server-side user search must treat this as the gap it closes.

- **Schema assumption**: every type used below (`UserResponse`, `UserRequest`, `RoleUpdate`, `PasswordUpdate`, `PageResponseUserResponse`, `JobPermissionRequest`, `JobPermissionResponse`) was confirmed present in the committed `src/api/schema.ts` during planning (Task Source research), so no `npm run generate:api-types` task is included — AC6 states the backend contract is unchanged. Task 1.1/1.2 must still run `npm run typecheck` immediately after adding the two API modules, before writing any page that consumes them, so a stale or drifted schema fails fast at the API-module layer instead of surfacing as a runtime `undefined` in a page component.

**Security**: The frontend gate (`ADMIN`-only nav link, `ADMIN`-only "Manage permissions" action, `RequireRole` route guard) is UX only. `UserController` is `@PreAuthorize("hasRole('ADMIN')")` and `JobPermissionController` calls `JobAccessService.require(..., EDIT)` (ADMIN bypasses) on every operation; a hidden button is not authorization, consistent with the project's existing anti-pattern rule. No credentials, resolved secrets, or plaintext passwords are logged, cached beyond submission, or stored in component state longer than the mutation call.

**Performance**: No new polling. `UsersPage` and `JobPermissionsPage` use one-shot `useQuery` fetches with TanStack Query cache invalidation on mutation success, identical to every existing admin-adjacent screen (`JobScheduleCard`).

## Implementation Tasks

### 1. Foundation: API clients and access guard

- [x] **1.1 Add `usersApi.ts`**
  Files: `replicadb-server/frontend/src/api/usersApi.ts` (new), `replicadb-server/frontend/src/api/usersApi.test.ts` (new)
  Changes: Export `UserResponse`, `UserRequest`, `RoleUpdate`, `PasswordUpdate` as aliases of the corresponding `components['schemas'][...]` types from `./schema`, plus `UserPage = Omit<components['schemas']['PageResponseUserResponse'], 'content'> & { content?: UserResponse[] }` (mirroring `JobDefinitionPage`/`JobRunPage`'s override pattern). Add `listUsers(page = 0, size = 50): Promise<UserPage>` (`GET /users` with `{ params: { page, size } }`), `createUser(request: UserRequest): Promise<UserResponse>` (`POST /users`), `updateUserRole(id: string, request: RoleUpdate): Promise<UserResponse>` (`PUT /users/{id}`), `updateUserPassword(id: string, request: PasswordUpdate): Promise<UserResponse>` (`PUT /users/{id}/password`). No try/catch — rely on `apiClient`'s existing `ApiError` interceptor.
  Tests: Using `axios-mock-adapter` against `apiClient` (mirroring `runsApi.test.ts`): `listUsers` sends `page`/`size` query params and returns the mocked page; `createUser` posts the request body and returns the created `UserResponse`; `updateUserRole` PUTs to `/users/{id}` with the role/enabled body; `updateUserPassword` PUTs to `/users/{id}/password`; a 409/400 RFC 7807 response from any of the four calls rejects with an `ApiError` carrying the response's `detail`.
  Dependencies: None

- [x] **1.2 Add `jobPermissionsApi.ts`**
  Files: `replicadb-server/frontend/src/api/jobPermissionsApi.ts` (new), `replicadb-server/frontend/src/api/jobPermissionsApi.test.ts` (new)
  Changes: Export `JobPermissionRequest`/`JobPermissionResponse` aliases from `./schema`. Add `listJobPermissions(jobId: string): Promise<JobPermissionResponse[]>` (`GET /jobs/{jobId}/permissions`), `replaceJobPermission(jobId: string, userId: string, request: JobPermissionRequest): Promise<JobPermissionResponse>` (`PUT /jobs/{jobId}/permissions/{userId}`), `deleteJobPermission(jobId: string, userId: string): Promise<void>` (`DELETE /jobs/{jobId}/permissions/{userId}`).
  Tests: `axios-mock-adapter` against `apiClient`: `listJobPermissions` GETs the job-scoped path and returns the array (including the empty-array/no-grants case); `replaceJobPermission` PUTs to the user-scoped path with `{ permissions }` and returns the updated grant; `deleteJobPermission` issues a `DELETE` and resolves with no body; a 403 response (non-EDIT caller) from any call rejects with an `ApiError` whose `detail` is surfaced (not swallowed).
  Dependencies: None

- [x] **1.3 Add `NotAuthorizedPage`**
  Files: `replicadb-server/frontend/src/pages/NotAuthorizedPage.tsx` (new), `replicadb-server/frontend/src/pages/NotAuthorizedPage.test.tsx` (new)
  Changes: A simple page using `PageHeader` (title `"Not authorized"`, description `"You do not have permission to view this page."`) and a `Button`/`Link` (`component={RouterLink}`, `to="/"`) reading `"Back to dashboard"`, matching the existing `PageHeader`/`EmptyState` visual style (no new layout primitives).
  Tests: Renders the "Not authorized" heading and description; the "Back to dashboard" link has `href="/"`.
  Dependencies: None

- [x] **1.4 Add the `RequireRole` route guard**
  Files: `replicadb-server/frontend/src/router/RequireRole.tsx` (new), `replicadb-server/frontend/src/router/RequireRole.test.tsx` (new)
  Changes: A component with a hardcoded `role: 'ADMIN' | 'OPERATOR' | 'VIEWER'` prop (mirroring `ProtectedRoute.tsx`'s no-configurable-fallback style). Reads `useAuth().user`; if `user?.role !== role`, renders `<NotAuthorizedPage />`; otherwise renders `<Outlet />`. This component is meant to be nested **inside** the existing `ProtectedRoute` subtree, so `status === 'loading'`/`'anonymous'` are already handled upstream and `user` is defined by the time this guard runs.
  Tests: Mirroring `ProtectedRoute.test.tsx`'s `AuthContext.Provider` + `MemoryRouter`/`Routes` harness: an `ADMIN` user sees the nested route's content; an `OPERATOR`/`VIEWER` user sees the "Not authorized" content instead of the protected route; the guard never calls any API (pure client-side check).
  Dependencies: Task 1.3

### 2. Users administration page

- [x] **2.1 `UsersPage` list and create dialog**
  Files: `replicadb-server/frontend/src/pages/UsersPage.tsx` (new), `replicadb-server/frontend/src/pages/UsersPage.test.tsx` (new)
  Changes: `PageHeader` (title `"Users"`, description `"Manage local accounts and roles."`, actions: `"Create user"` button opening a dialog). Paginated table (mirroring `DashboardPage.tsx`: `useQuery(['users', page], () => listUsers(page, size))`, `size = 50`, `TableContainer`/`Table`/`TablePagination`) with columns Username, Role, Enabled (Yes/No). Empty state via `<EmptyState title="No users configured." />` when `content` is empty. Create dialog: `TextField` username (required), `TextField` password (`type="password"`, required), `TextField` `select` role (`MenuItem` for `ADMIN`/`OPERATOR`/`VIEWER`); `useMutation(createUser)` on submit, inline `Alert` with `ApiError.detail` on failure (mirroring `JobScheduleCard`'s `errorMessage` helper), `invalidateQueries(['users'])` and dialog close on success.
  Tests: Renders the paginated user table with role/enabled columns; renders the empty state when the page has no users; opening the create dialog, filling username/password/role, and submitting calls `createUser` with the expected payload, closes the dialog, and refetches the list; a duplicate-username `ApiError` (409) is shown inline and keeps the dialog open.
  Dependencies: Task 1.1

- [x] **2.2 `UsersPage` edit role/enabled dialog**
  Files: `replicadb-server/frontend/src/pages/UsersPage.tsx`, `replicadb-server/frontend/src/pages/UsersPage.test.tsx`
  Changes: Add an "Edit" action per row opening a dialog pre-filled with the row's `role` (select) and `enabled` (`Switch`, mirroring `JobScheduleCard`'s `FormControlLabel`/`Switch` pattern); `useMutation(({id, request}) => updateUserRole(id, request))`, invalidates `['users']` on success, surfaces `ApiError.detail` inline on failure.
  Tests: Opening "Edit" pre-fills the current role/enabled state; submitting a changed role and toggled `enabled` calls `updateUserRole` with the expected `RoleUpdate` body and refetches the list; a failed update keeps the dialog open with the inline error.
  Dependencies: Task 2.1

- [x] **2.3 `UsersPage` change password dialog**
  Files: `replicadb-server/frontend/src/pages/UsersPage.tsx`, `replicadb-server/frontend/src/pages/UsersPage.test.tsx`
  Changes: Add a "Reset password" action per row opening a dialog with a single required `TextField` (`type="password"`, label "New password"); `useMutation(({id, request}) => updateUserPassword(id, request))`, invalidates `['users']` and closes on success, surfaces `ApiError.detail` inline on failure. The password value is never logged or retained in state after the mutation settles (dialog close resets local form state).
  Tests: Submitting a new password calls `updateUserPassword` with the expected `PasswordUpdate` body and closes the dialog; a blank password is blocked client-side before the API call (required-field validation, mirroring `JobScheduleCard`'s cron-expression required check); a failed reset shows the inline error and keeps the dialog open.
  Dependencies: Task 2.1

### 3. Job permissions page

- [x] **3.1 `JobPermissionsPage` list and revoke**
  Files: `replicadb-server/frontend/src/pages/JobPermissionsPage.tsx` (new), `replicadb-server/frontend/src/pages/JobPermissionsPage.test.tsx` (new)
  Changes: Route-level page reading `id` from `useParams`. `useQuery(['jobs', id], () => getJob(id))` for the page title (`PageHeader` title = job name, `backLink` to `/jobs/{id}`) and `useQuery(['jobPermissions', id], () => listJobPermissions(id))` for the grants table (columns: Username, VIEW/EDIT/EXECUTE/CANCEL as `Checkbox` cells, "Remove" action per row calling `useMutation(deleteJobPermission)` with `invalidateQueries(['jobPermissions', id])`). `<EmptyState title="No users have explicit access to this job." />` when the list is empty. A 403 from `listJobPermissions` (caller lacks `EDIT` on this job) renders an inline `Alert` instead of the table, since only `ADMIN`/`EDIT`-holders can reach this data per the backend contract.
  Tests: Renders the grants table with one row per user and checked cells matching their `permissions`; renders the empty state when no grants exist; clicking "Remove" on a row calls `deleteJobPermission` with the job/user ids and refetches the list; a 403 `ApiError` from the initial list call renders an inline error instead of an empty table.
  Dependencies: Task 1.2

- [x] **3.2 `JobPermissionsPage` grant-access dialog**
  Files: `replicadb-server/frontend/src/pages/JobPermissionsPage.tsx`, `replicadb-server/frontend/src/pages/JobPermissionsPage.test.tsx`
  Changes: Add a "Grant access" button opening a dialog with an `Autocomplete` (mirroring `JobScheduleCard`'s timezone `Autocomplete`: `options` from `useQuery(['users', 'all'], () => listUsers(0, 200))` filtered to exclude usernames already present in the current grants list, `getOptionLabel`, `isOptionEqualToValue` by `id`) plus a `VIEW`/`EDIT`/`EXECUTE`/`CANCEL` checkbox group (reusing the `FormControlLabel`+`Checkbox` composition already established in `DataFilteringTabs.tsx`'s `ToggleSetting`). Submitting calls `useMutation(({userId, request}) => replaceJobPermission(id, userId, request))`, invalidates `['jobPermissions', id]`, closes the dialog, and surfaces `ApiError.detail` inline on failure. Also allow editing an existing row's checkboxes directly with a per-row "Save" action calling the same `replaceJobPermission` mutation (no separate endpoint — `PUT` replaces the set idempotently).
  Tests: The Autocomplete excludes users who already have a grant; selecting a user, checking permissions, and submitting calls `replaceJobPermission` with the expected `{ permissions }` array and refetches the grants; toggling an existing row's checkboxes and clicking its "Save" action calls `replaceJobPermission` for that row's user; submitting with zero permissions checked is allowed (matches the backend's empty-array-is-valid contract) and results in a grant with no checked boxes; a failed grant/save shows the inline error without closing the dialog/losing the row's pending edit.
  Dependencies: Task 3.1

  > ⚠️ Critic note: the Autocomplete's `listUsers(0, 200)` fetch is a hard cap with no server-side search — a 201st user is silently unselectable. Add an explicit test asserting the Autocomplete requests `size: 200` (documenting the current limit rather than hiding it) and treat raising this limit as out of scope for this plan.

### 4. Routing and navigation integration

- [x] **4.1 Wire `/users` and `/jobs/:id/permissions` behind `RequireRole`**
  Files: `replicadb-server/frontend/src/router/routes.tsx`, `replicadb-server/frontend/src/router/routes.test.tsx`
  Changes: Import `RequireRole`, `UsersPage`, `JobPermissionsPage`. Inside the existing `ProtectedRoute` children array, add one new child `{ element: <RequireRole role="ADMIN" />, children: [{ path: 'users', element: <UsersPage /> }, { path: 'jobs/:id/permissions', element: <JobPermissionsPage /> }] }`, alongside (not replacing) the existing unguarded protected routes.
  Tests: Extend `routes.test.tsx` (mocking `usersApi`/`jobPermissionsApi`/`jobsApi` as needed, following the existing `vi.mock` + `createMemoryRouter(routeObjects)` pattern): an `ADMIN`-role `AuthContext` renders `UsersPage`'s heading at `/users` and `JobPermissionsPage`'s heading at `/jobs/job-1/permissions`; an `OPERATOR`-role `AuthContext` renders the "Not authorized" heading at both routes instead.
  Dependencies: Task 1.4, Task 2.1, Task 3.1

- [x] **4.2 Add the `ADMIN`-only "Users" navigation link**
  Files: `replicadb-server/frontend/src/layout/AppLayout.tsx`, `replicadb-server/frontend/src/layout/AppLayout.test.tsx`
  Changes: In the `Toolbar`, add a `Button`/`Link` (`component={RouterLink}`, `to="/users"`, label `"Users"`) rendered only when `user?.role === 'ADMIN'`, placed between the brand link and the signed-in identity group so it does not disrupt the existing `role="group"` "Signed-in identity" wrapper or its responsive wrapping behavior.
  Tests: With an `ADMIN` `AuthContext.Provider` value, the "Users" link is present with `href="/users"`; with an `OPERATOR`/`VIEWER` value, the "Users" link is absent; existing brand-link and logout assertions continue to pass unchanged.
  Dependencies: Task 4.1

- [x] **4.3 Add the `ADMIN`-only "Manage permissions" action on `JobDetailPage`**
  Files: `replicadb-server/frontend/src/pages/JobDetailPage.tsx`, `replicadb-server/frontend/src/pages/JobDetailPage.test.tsx`
  Changes: Import `useAuth`. In the `PageHeader`'s `actions` fragment (alongside the existing "Trigger run"/"Edit" buttons), add `{user?.role === 'ADMIN' && <Button component={RouterLink} to={`/jobs/${id}/permissions`} variant="outlined">Manage permissions</Button>}`.
  Tests: With an `ADMIN` `AuthContext`, the "Manage permissions" link is present with the expected `href`; with a non-`ADMIN` `AuthContext`, it is absent while "Trigger run"/"Edit" remain visible unchanged.
  Dependencies: Task 4.1

### 5. Integration coverage and documentation

- [x] **5.1 Playwright smoke coverage for the admin flow**
  Files: `replicadb-server/frontend/e2e/admin-management.spec.ts` (new)
  Changes: Following the existing `job-creation.spec.ts` pattern (environment-managed admin credentials, real login/session/CSRF flow against built static assets): log in as the bootstrapped `ADMIN`, navigate to `/users`, create a new `OPERATOR` user, assert it appears in the list; navigate to an existing job's detail page, follow "Manage permissions", grant the new user `VIEW`+`EXECUTE` on that job, assert the grant appears in the table. Report missing environment-managed admin credentials as a separate skip/failure reason, not a UI failure (matching the existing Playwright credential-handling convention).
  Tests: The spec itself is the test; it must pass against a locally built `replicadb-server` static bundle with a reachable PostgreSQL-backed instance and environment-managed admin credentials configured, exactly like the existing Playwright specs.
  Dependencies: Task 4.1, Task 4.2, Task 4.3

  > ⚠️ Critic note: this spec covers only the happy path (create user, grant permission). It intentionally does not add e2e coverage for the 403 "lacks EDIT" path on `JobPermissionsPage` — that path is already unit-tested in Task 3.1 with a mocked `ApiError`, and skipping it here matches the project's existing scope for Playwright (real cookie/session flow, not exhaustive error-path coverage). Also note this spec follows the existing `job-creation.spec.ts` convention of skipping (not failing) when environment-managed admin credentials are absent — if CI never configures those credentials, this remains the only gap in automated coverage for the admin flow, same as the existing e2e specs today; this is a pre-existing project-wide convention, not a gap introduced by this plan.

- [x] **5.2 Update architecture and context documentation**
  Files: `ARCHITECTURE_DECISIONS.md`, `.ai/context/frontend.md`, `.github/instructions/frontend.instructions.md`
  Changes: In `ARCHITECTURE_DECISIONS.md`, change "#### Phase 2c: Administration — PENDING" to "#### Phase 2c: Administration — IMPLEMENTED" with a short description mirroring Phase 2a/2b's style (users administration and per-job permission editor, no backend changes), update the executive-summary status line and the "Priority 3: Frontend Rollout" checkbox from `[ ]` to `[x]`, and update the Phase 2 "Success Metrics" bullet about the permission editor from future tense to completed. In `.ai/context/frontend.md`, update "Current Product Slice" to mention the admin screens are now available to `ADMIN` users. In `.github/instructions/frontend.instructions.md`, update the "Phase 2b ships..." bullet to also acknowledge Phase 2c's admin screens, preserving the underlying rule that mutating controls require a backend contract, permission check, CSRF coverage, and a planned product slice.
  Tests: None (documentation-only change); verify with a manual read-through that every edited status line is internally consistent (no remaining "PENDING"/"not started" wording for Phase 2c) — this task does not need automated tests per the project's "don't create tests for docs" convention implied by the rest of the codebase having no doc-linting.
  Dependencies: Task 5.1

## Technical Reference

<details>
<summary>Types & Data Structures</summary>

```ts
// src/api/usersApi.ts
export type UserResponse = components['schemas']['UserResponse']; // { id, username, role, enabled, createdAt, updatedAt }
export type UserRequest = components['schemas']['UserRequest'];   // { username, password, role } — create only
export type RoleUpdate = components['schemas']['RoleUpdate'];     // { role, enabled }
export type PasswordUpdate = components['schemas']['PasswordUpdate']; // { newPassword }
export type UserPage = Omit<components['schemas']['PageResponseUserResponse'], 'content'> & {
  content?: UserResponse[];
};

// src/api/jobPermissionsApi.ts
export type JobPermissionRequest = components['schemas']['JobPermissionRequest'];   // { permissions: (VIEW|EDIT|EXECUTE|CANCEL)[] }
export type JobPermissionResponse = components['schemas']['JobPermissionResponse']; // { userId, username, permissions }
```

No new backend types are introduced; every shape above already exists in the committed, OpenAPI-generated `src/api/schema.ts`.

</details>

<details>
<summary>Dependencies</summary>

No new npm packages. Reuses `@mui/material` (`Table`, `Dialog`, `Autocomplete`, `Checkbox`, `Switch`), `@tanstack/react-query`, `react-router-dom`, and the existing `axios-mock-adapter`/`vitest`/`@testing-library/react`/`@playwright/test` devDependencies.

</details>

<details>
<summary>Testing Strategy</summary>

- **API modules** (`usersApi.test.ts`, `jobPermissionsApi.test.ts`): `axios-mock-adapter` against the shared `apiClient`, matching `runsApi.test.ts`/`scheduleApi.test.ts`.
- **Guard/page components**: Vitest + Testing Library with a fresh `QueryClient` per test and an `AuthContext.Provider` stub supplying `{ status: 'authenticated', user: {...}, login, logout }`, matching `ProtectedRoute.test.tsx`/`AppLayout.test.tsx`/`JobScheduleCard.test.tsx`. Use role/label/heading queries only, never MUI class names, per `frontend.instructions.md`.
- **Routing**: extend `routes.test.tsx`'s `createMemoryRouter(routeObjects)` + mocked API modules pattern for both the `ADMIN`-success and non-`ADMIN`-blocked cases.
- **End-to-end**: one new Playwright spec exercising the real login/session/CSRF cookie flow, consistent with `job-creation.spec.ts`.
- Run `npm test` (Vitest) after every task in Sections 1–4, `npm run typecheck`, and `npm run build` after Section 4 completes; run `npm run test:e2e` (or the project's documented Playwright invocation) after Task 5.1.

</details>

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy
- Tasks completed as planned: 13/14 (92.9%).
- Tasks that required plan adjustment: 1/14 (7.1%).
- Test loop iterations: 26 total (8 first-pass task validations, 11 retry/fix iterations, 7 final/build/e2e checks).

### Gaps Encountered

#### Gap 1: Native form validation intercepted blank password submission (Plan-to-Implementation)
- **Task**: 2.3 — `UsersPage` password reset dialog.
- **Plan assumed**: The controlled `submitPassword` validation would receive an empty form submit and render the planned inline error.
- **Reality**: The browser's native `required` validation intercepted the submit before React's handler ran.
- **Resolution**: Added `noValidate` to the reset form and kept explicit controlled validation, with a regression test proving no API call occurs for a blank password.
- **Learning**: When a form needs custom inline validation and a submit must be testable with empty values, disable native validation at the form boundary and validate in the submit handler.

#### Gap 2: Route context was required in the page test harness (Plan-to-Implementation)
- **Task**: 3.1 — `JobPermissionsPage` list and revoke.
- **Plan assumed**: A `MemoryRouter` wrapper alone would be sufficient for a page using `useParams`.
- **Reality**: Without a matching `Routes`/`Route`, `id` was undefined and both queries remained disabled in the test.
- **Resolution**: Mounted the component at `/jobs/:id/permissions` in the test harness, following the existing route-test pattern.
- **Learning**: Page tests for parameterized React Router screens must mount the component through a matching route, not only provide a memory history.

### Patterns Discovered
- **Admin route gating**: Nest `RequireRole` inside `ProtectedRoute`; keep UI visibility checks separate from backend ACL enforcement.
- **Generated API contract**: New endpoint modules alias `schema.ts` types and keep all Axios calls in `src/api`.
- **Mutation forms**: Use query invalidation on success, inline `ApiError.detail` on failure, and reset sensitive password state on mutation settlement.
- **Permission editing**: Keep permission arrays in the canonical `VIEW`, `EDIT`, `EXECUTE`, `CANCEL` order so payloads do not depend on click order.
