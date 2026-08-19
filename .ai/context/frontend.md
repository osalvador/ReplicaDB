## Application Structure
The `replicadb-server/frontend` SPA uses React 18, TypeScript, Vite, React Router, MUI, Axios, and TanStack Query. `src/api` owns the configured client and endpoint modules; `src/auth` owns session bootstrap; `src/router` owns the protected shell; `src/pages` owns login/dashboard/job/run screens; `src/components` owns reusable tables; `src/theme` owns tokens.

## Current Product Slice
Phase 2a/2b/2c are implemented: users can log in, view ACL-filtered jobs, inspect job/run details, edit jobs, manage schedules and run actions, while ADMIN users can administer local users and per-job permissions. `ProtectedRoute` redirects anonymous users, `RequireRole` presents a friendly unauthorized page for non-ADMIN admin routes, and `AuthContext` treats 401/403 from `/auth/me` as an anonymous session.

## Data and Security Rules
- Use the generated `src/api/schema.ts` types; do not duplicate Java DTOs as hand-maintained interfaces.
- Keep `apiClient` at `/api/v1` with credentials enabled, and perform the CSRF bootstrap before login so logout has the browser token/header pair.
- Use TanStack Query for server state. Run polling follows the backend terminal contract: `SUCCEEDED`, `CANCELLED`, and `RETRY_SCHEDULED` stop polling; `FAILED` remains retryable.
- Treat backend ACLs as authoritative; hiding a button is not authorization. Never put credentials or resolved secrets in frontend state, fixtures, logs, or generated types.

## Build and Tests
`frontend-maven-plugin` runs `npm ci` and `npm run build` with pinned Node/npm and emits Vite assets into `src/main/resources/static`. `npm run generate:api-types` reads a live server OpenAPI document; the committed schema and CI drift check are the contract. Vitest/Testing Library cover pages, hooks, client behavior, and status mapping; Playwright covers the real login/dashboard/logout cookie flow.

## Reference Implementations
- `replicadb-server/frontend/src/api/client.ts`
- `replicadb-server/frontend/src/auth/AuthContext.tsx`
- `replicadb-server/frontend/src/router/ProtectedRoute.tsx`
- `replicadb-server/frontend/src/pages/RunDetailPage.tsx`
- `replicadb-server/frontend/scripts/generate-api-types.mjs`

## Recent Learnings
- [WARNING] Validate generated types against serialized null values, not only endpoint names. Source: `phase-2a-frontend-auth-monitoring`.
- [WARNING] Keep lockfiles and npm configuration free of machine-specific registries; validate `npm ci` in a clean runner-like environment. Source: `phase-2a-frontend-auth-monitoring`.
- [WARNING] Separate local certificate/browser limitations from reproducible CI build configuration. Source: `phase-2a-frontend-auth-monitoring`.
- [WARNING] Tabbed form state is persistent: `DataFilteringTabs` and `StagingOptionsTabs` must only switch the visible panel; their tab handlers must not call `onChange` with empty values for the hidden panel. Test switching away and back to confirm every value remains until the user edits or deletes it.
