# Implementation Plan: Phase 2a — Frontend Authentication and Read-Only Monitoring

## Task Source

`ARCHITECTURE_DECISIONS.md` — Phase 2: Frontend, sub-phase **Phase 2a: Authentication and read-only monitoring (next plan target)**.

**Acceptance criteria** (extracted from the Phase 2a scope statement and the Phase 2 Success Metrics):

- Login/logout works against the existing `POST /api/v1/auth/login` / `POST /api/v1/auth/logout` endpoints, and the SPA can tell "am I logged in?" on page load via `GET /api/v1/auth/me`.
- A dashboard lists the jobs visible to the current user (the backend already ACL-filters `GET /api/v1/jobs`).
- Job detail is read-only and surfaces `JobDefinitionResponse.modeWarning` when present.
- Run history and run detail show status, counters, timings, committed watermark, error message, cancellation warning, and the persisted log excerpt.
- No create, edit, trigger, cancel, or schedule affordance exists anywhere in this slice (that is Phase 2b).
- Generated TypeScript types stay in sync with `/api/v1` through an OpenAPI specification — no hand-maintained DTO duplicating the Java response records.
- Unauthenticated users are redirected to `/login`; authenticated users never see stale data because a non-terminal run keeps polling.

## Overview

This plan builds the first user-visible slice of the ReplicaDB control-plane frontend: a React/TypeScript/Vite single-page application, compiled to static assets and served by `replicadb-server` under the `api` profile, that lets an authenticated user log in and browse jobs, job definitions, and run history/detail without any mutating action. It also stands up the full technical foundation (build integration, OpenAPI-generated types, API client, state management, theming, testing) that Phase 2b and 2c will build on directly, so none of that plumbing needs to be redone later.

## Architecture & Design

**Approach**: single comprehensive plan for all of Phase 2a (scaffold + build integration + OpenAPI types + API client + auth + monitoring screens + tests), per explicit choice over the two-plan and three-plan splits discussed. This intentionally exceeds the usual ~20-task guideline (22 tasks) because the user chose to keep Phase 2a as one deliverable rather than split it further.

**Decisions locked in by `ARCHITECTURE_DECISIONS.md` and followed here without re-litigation**:

- **Stack**: React, TypeScript, Vite. Compiled output goes to `replicadb-server/src/main/resources/static`, served by the existing Spring Boot `api` profile — no new deployment topology.
- **Styling**: MUI v6 themed with Material 3 tokens (community-driven M3 support, not officially on-spec — accepted trade-off).
- **Data fetching/state**: TanStack Query, including polling `GET /api/v1/runs/{id}` while the run is non-terminal.
- **Routing**: React Router, with a `ProtectedRoute` wrapper gating everything except `/login`.
- **API types**: generated from an OpenAPI spec via `springdoc-openapi` (new backend dependency) + `openapi-typescript` (new frontend devDependency) — no hand-written DTO interfaces.
- **HTTP client**: `axios` with `withCredentials: true`; its default `XSRF-TOKEN`/`X-XSRF-TOKEN` cookie/header names already match `CookieCsrfTokenRepository.withHttpOnlyFalse()` in `SecurityConfig.java`, so no custom CSRF plumbing is needed.
- **Build integration**: `frontend-maven-plugin` wired into `replicadb-server/pom.xml`'s `mvn package` lifecycle.
- **Local dev**: Vite dev-server proxy for `/api/v1` and `/v3/api-docs` to the locally running Spring Boot process.
- **Testing**: Vitest + Testing Library for component/unit tests, Playwright for one end-to-end smoke spec.

**Integration points**:

- `GET /api/v1/auth/me`, `POST /api/v1/auth/login`, `POST /api/v1/auth/logout` (`AuthController.java`) — session bootstrap and login/logout.
- `GET /api/v1/auth/csrf` (`AuthController.java`) — public CSRF-token bootstrap requested before login so Spring can issue the `XSRF-TOKEN` cookie required by later state-changing requests.
- `GET /api/v1/jobs` (paginated), `GET /api/v1/jobs/{id}` (`JobDefinitionController.java`) — dashboard and job detail.
- `GET /api/v1/jobs/{id}/runs`, `GET /api/v1/runs/{id}`, `GET /api/v1/runs/{id}/log` (`JobRunController.java`) — run history and run detail.
- `SecurityConfig.java` — must permit unauthenticated access to the OpenAPI document, SPA static entrypoint/assets, and CSRF bootstrap while keeping `/api/v1/jobs` protected.

**Deliberate parity note (surfaced explicitly so it is not mistaken for an oversight)**: `JobRunStatus.isTerminal()` on the backend returns `true` only for `SUCCEEDED`, `CANCELLED`, and `RETRY_SCHEDULED` — **not** `FAILED`, because a `FAILED` run's row can still transition to `RETRY_SCHEDULED` via `scheduleRetry(...)`. The frontend's polling logic mirrors this exactly (see task 8.4) rather than treating `FAILED` as a dead end.

**Security/perf implications**: Phase 2a adds no persistence and only one security-infrastructure endpoint: `/api/v1/auth/csrf` returns a non-secret CSRF token and is publicly readable so the SPA can establish the cookie/header handshake. The OpenAPI spec and static SPA assets are also public; job/run data remains protected. Polling interval is bounded to non-terminal runs only, avoiding indefinite background requests.

## Implementation Tasks

### 1. Backend: expose an OpenAPI specification

- [x] **1.1 Add `springdoc-openapi` to `replicadb-server`**
  Files: `replicadb-server/pom.xml`
  Changes: Add the `org.springdoc:springdoc-openapi-starter-webmvc-ui` dependency (matching the existing Spring Boot 3.3.5 parent version compatibility). No custom `@OpenAPIDefinition` configuration needed beyond defaults for this slice.
  Tests: New `OpenApiSpecificationIT` (Spring Boot `@SpringBootTest` with `MockMvc`, reusing the existing Testcontainers PostgreSQL setup pattern from other `replicadb-server` tests) asserting `GET /v3/api-docs` returns `200` with content-type `application/json` and that the JSON contains `"/api/v1/jobs"` as a path key.
  Dependencies: None

- [x] **1.2 Permit unauthenticated access to the OpenAPI spec and frontend entrypoint**
  Files: `replicadb-server/src/main/java/org/replicadb/server/security/config/SecurityConfig.java`
  Changes: Add `"/v3/api-docs/**"` plus the SPA entrypoint/static asset patterns (`/`, `/index.html`, `/assets/**`, `/favicon.ico`) to the existing public matcher list. The OpenAPI document and compiled frontend must be downloadable before a session exists; `/api/v1/jobs` remains protected.
  Tests: Extend the existing Spring Security `MockMvc` test suite (or add one if none exists for `permitAll` routes) with cases asserting `GET /v3/api-docs` and `GET /` return `200` **without** an authenticated session, while `GET /api/v1/jobs` still returns `401`/`403` unauthenticated.
  Dependencies: 1.1

- [x] **1.3 Add the CSRF-token bootstrap used by SPA login/logout**
  Files: `replicadb-server/src/main/java/org/replicadb/server/security/api/AuthController.java`, `replicadb-server/src/main/java/org/replicadb/server/security/config/SecurityConfig.java`, `replicadb-server/src/test/java/org/replicadb/server/security/api/AuthControllerTest.java`
  Changes: Add public `GET /api/v1/auth/csrf` returning Spring Security's `CsrfToken`, and permit only this safe GET alongside the existing login/static/OpenAPI public routes. The endpoint forces `CookieCsrfTokenRepository` to issue the `XSRF-TOKEN` cookie before the frontend performs login; no credential or session data is returned.
  Tests: MockMvc test asserts unauthenticated `GET /api/v1/auth/csrf` returns a token; the existing unauthenticated job-request test remains protected. The frontend/E2E test verifies the real browser cookie jar and configured `X-XSRF-TOKEN` header are accepted on logout.
  Dependencies: 1.2

### 2. Frontend project scaffold

- [x] **2.1 Scaffold the Vite + React + TypeScript project**
  Files: `replicadb-server/frontend/package.json`, `replicadb-server/frontend/tsconfig.json`, `replicadb-server/frontend/tsconfig.node.json`, `replicadb-server/frontend/vite.config.ts`, `replicadb-server/frontend/index.html`, `replicadb-server/frontend/src/main.tsx`, `replicadb-server/frontend/src/App.tsx`, `replicadb-server/frontend/.gitignore`
  Changes: Standard Vite `react-ts` template layout. `package.json` scripts: `dev`, `build`, `preview`, `typecheck` (`tsc --noEmit`), `test` (Vitest), `test:e2e` (Playwright). `.gitignore` excludes `node_modules/`, `dist/`, and `test-results/`. `App.tsx` renders a placeholder `<div>ReplicaDB</div>` at this point.
  Tests: `npm run build` (from `replicadb-server/frontend/`) completes with exit code 0 and produces `dist/index.html`. `npm run typecheck` passes.
  Dependencies: None

- [x] **2.2 Add MUI v6 with Material 3 theme tokens**
  Files: `replicadb-server/frontend/package.json` (add `@mui/material`, `@mui/icons-material`, `@emotion/react`, `@emotion/styled`, plus `vitest`, `@testing-library/react`, `@testing-library/jest-dom`, `jsdom` as devDependencies), `replicadb-server/frontend/src/theme/theme.ts`, `replicadb-server/frontend/src/App.tsx`, `replicadb-server/frontend/vite.config.ts` (add the `test` block for Vitest with `environment: 'jsdom'`)
  Changes: `theme.ts` exports a `createTheme(...)` call using Material-3-inspired palette/typography tokens (documented in-code as community-driven M3 support, not officially on-spec, per `ARCHITECTURE_DECISIONS.md`). Wrap `<App>` in `<ThemeProvider theme={theme}><CssBaseline />...</ThemeProvider>` in `main.tsx`.
  Tests: New `src/App.test.tsx` (Vitest + Testing Library) rendering `<App />` wrapped in the theme provider and asserting it renders without throwing.
  Dependencies: 2.1

- [x] **2.3 Add React Router with a route shell and placeholder pages**
  Files: `replicadb-server/frontend/package.json` (add `react-router-dom`), `replicadb-server/frontend/src/router/routes.tsx`, `replicadb-server/frontend/src/router/ProtectedRoute.tsx` (placeholder — always renders children; real logic in task 7.3), `replicadb-server/frontend/src/pages/LoginPage.tsx` (placeholder), `replicadb-server/frontend/src/pages/DashboardPage.tsx` (placeholder), `replicadb-server/frontend/src/layout/AppLayout.tsx`
  Changes: `createBrowserRouter` with a public `/login` route and a protected root layout route (`AppLayout` wrapping `ProtectedRoute`) containing a placeholder `/` route rendering `DashboardPage`.
  Tests: Vitest test asserting navigating (via `MemoryRouter`/`createMemoryRouter`) to `/login` renders `LoginPage`, and navigating to `/` renders the placeholder `DashboardPage` (protected-route redirect logic is tested for real in task 7.3, once it exists).
  Dependencies: 2.2

### 3. Build integration

- [x] **3.1 Wire `frontend-maven-plugin` into `replicadb-server`'s Maven build**
  Files: `replicadb-server/pom.xml`, `replicadb-server/frontend/vite.config.ts`
  Changes: Set Vite's `build.outDir` to `../src/main/resources/static` with `emptyOutDir: true` (Vite creates this directory automatically if it does not already exist, so no manual pre-creation step is required). Add the `com.github.eirslett:frontend-maven-plugin` plugin to `replicadb-server/pom.xml` with executions bound to `generate-resources` (before `spring-boot-maven-plugin` packages the jar): `install-node-and-npm` (pinned Node/npm versions), `npm install` (using `npm ci`), and `npm run build`, all with `workingDirectory` set to `frontend`.
  Tests: Run `mvn -pl replicadb-server -am package` and verify `jar tf replicadb-server/target/replicadb-server-*.jar | grep -q static/index.html` succeeds.
  Dependencies: 2.1

- [x] **3.2 Update CI to build and verify the frontend**
  Files: `.github/workflows/CT_Push.yml`
  Changes: In the existing `server` job, after the Maven build step, add a step asserting `test -f replicadb-server/target/classes/static/index.html`. No separate Node setup step is needed since `frontend-maven-plugin` downloads its own pinned Node/npm.
  Tests: The CI job itself (fails the build if the assertion step fails); no separate test file.
  Dependencies: 3.1

- [x] **3.3 Add a Vite dev-server proxy for local development**
  Files: `replicadb-server/frontend/vite.config.ts`
  Changes: Add `server.proxy` entries forwarding `/api/v1` and `/v3/api-docs` to `http://localhost:8080` with `changeOrigin: true` (cookies pass through automatically since the proxy preserves the `Set-Cookie`/`Cookie` headers).
  Tests: New `vite.config.test.ts` (Vitest, Node environment) importing the exported Vite config object and asserting `server.proxy['/api/v1'].target === 'http://localhost:8080'` and the same for `/v3/api-docs`.
  Dependencies: 2.1

### 4. OpenAPI-generated TypeScript types

- [x] **4.1 Generate TypeScript types from the OpenAPI spec**
  Files: `replicadb-server/frontend/package.json`, `replicadb-server/frontend/scripts/generate-api-types.mjs`, `replicadb-server/frontend/src/api/schema.ts` (generated, committed to source control with a header comment noting it is generated and how to regenerate it)
  Changes: `generate:api-types` invokes the local `openapi-typescript` binary through the Node helper and writes to `OPENAPI_SCHEMA_OUTPUT` when set, otherwise `src/api/schema.ts`. This is a **manual developer command**, run against a locally started `replicadb-server` (e.g. `mvn -pl replicadb-server spring-boot:run -Dspring-boot.run.profiles=api`) whenever the backend API surface changes. Commit the generated `schema.ts` so `npm run build`/CI never require a live backend for a normal build. Drift between the committed file and the live API is caught in CI by task 9.3 (which already boots a real backend), not at build time.
  Tests: New `src/api/schema.test.ts` (Vitest, type-only assertion) that imports `paths` from `schema.ts` and references `paths['/api/v1/jobs']['get']` and `paths['/api/v1/auth/me']['get']` to confirm both endpoints are present in the generated types (a compile failure here means the schema is stale or the endpoint moved).
  Dependencies: 1.1, 1.2, 2.1

### 5. API client

- [x] **5.1 Add a configured axios client with typed error handling**
  Files: `replicadb-server/frontend/package.json` (add `axios`; add `axios-mock-adapter` as a devDependency for tests), `replicadb-server/frontend/src/api/client.ts`
  Changes: Export a configured `axios` instance with `baseURL: '/api/v1'` and `withCredentials: true`. Add a response interceptor that, on an RFC 7807 `application/problem+json` error response, throws a typed `ApiError { status, title, detail }` instead of the raw axios error.
  Tests: New `src/api/client.test.ts` (Vitest + `axios-mock-adapter`) asserting: (a) requests are sent with `withCredentials: true`; (b) a mocked `404` `application/problem+json` body is transformed into an `ApiError` with the expected `status`/`title`/`detail` fields; (c) a successful response passes through unchanged.
  Dependencies: 4.1

### 6. TanStack Query setup

- [x] **6.1 Add TanStack Query provider**
  Files: `replicadb-server/frontend/package.json` (add `@tanstack/react-query`), `replicadb-server/frontend/src/api/queryClient.ts`, `replicadb-server/frontend/src/main.tsx`
  Changes: Create a shared `QueryClient` with default `retry: 1`. Wrap the router in `<QueryClientProvider client={queryClient}>` in `main.tsx`.
  Tests: Extend `src/App.test.tsx` (from task 2.2) to render the full provider tree (`ThemeProvider` + `QueryClientProvider` + router) and assert it still renders without throwing.
  Dependencies: 5.1

### 7. Authentication

- [x] **7.1 Implement the auth context and `/auth/me` session bootstrap**
  Files: `replicadb-server/frontend/src/api/authApi.ts`, `replicadb-server/frontend/src/auth/AuthContext.tsx`, `replicadb-server/frontend/src/auth/useAuth.ts`
  Changes: `authApi.ts` exports `getMe()`, `getCsrf()`, `login(username, password)`, and `logout()` calling the axios client from task 5.1 against `/auth/me`, `/auth/csrf`, `/auth/login`, and `/auth/logout`; `login()` calls `getCsrf()` first so the subsequent session can perform CSRF-protected logout. `AuthContext` uses TanStack Query (`useQuery` on `getMe`) to expose `{ status: 'loading' | 'authenticated' | 'anonymous', user, login, logout }`, treating a `401`/`403` `ApiError` from `getMe()` as `'anonymous'` rather than a query error.
  Tests: New `src/auth/AuthContext.test.tsx` (Vitest + mocked `authApi`) asserting: (a) a resolved `getMe()` sets `status` to `'authenticated'` with the returned `id`/`username`/`role`; (b) both a `401 ApiError` and a `403 ApiError` from `getMe()` set `status` to `'anonymous'` (not an error state) — parameterized over both status codes.
  Dependencies: 5.1, 6.1

- [x] **7.2 Implement the login page**
  Files: `replicadb-server/frontend/src/pages/LoginPage.tsx`
  Changes: Replace the task 2.3 placeholder with a real MUI form (username, password `TextField`s, submit `Button`, disabled while empty or submitting). Calls `useAuth().login(...)`; on success, `navigate('/')`; on failure, renders the `ApiError.detail` message inline (covers both invalid-credential `401` and throttled `429` responses).
  Tests: New `src/pages/LoginPage.test.tsx` (Vitest + Testing Library, mocked `authApi.login`): (a) submitting valid credentials calls `login` and triggers navigation to `/`; (b) a rejected `login` call renders the error message and does not navigate; (c) the submit button is disabled while either field is empty.
  Dependencies: 7.1, 2.3

- [x] **7.3 Implement the real `ProtectedRoute` guard**
  Files: `replicadb-server/frontend/src/router/ProtectedRoute.tsx`
  Changes: Replace the task 2.3 placeholder: render a loading spinner while `useAuth().status === 'loading'`, redirect to `/login` (preserving the attempted path for a post-login redirect) when `'anonymous'`, and render `children`/`<Outlet />` when `'authenticated'`.
  Tests: New `src/router/ProtectedRoute.test.tsx` rendering the guard under each of the three `useAuth()` states (mocked via a test-only `AuthContext.Provider` value) and asserting the spinner, the redirect, and the rendered children respectively.
  Dependencies: 7.1

- [x] **7.4 Implement logout and the app bar**
  Files: `replicadb-server/frontend/src/layout/AppLayout.tsx`, `replicadb-server/frontend/src/auth/useAuth.ts`
  Changes: Add a `logout()` function to the auth hook that calls `authApi.logout()`, clears the TanStack Query cache (`queryClient.clear()`), and navigates to `/login`. Add an MUI `AppBar` in `AppLayout` showing the current `username`/`role` and a "Logout" button.
  Tests: New `src/layout/AppLayout.test.tsx` asserting clicking "Logout" calls the mocked `authApi.logout`, results in the query cache being cleared, and navigates to `/login`.
  Dependencies: 7.1, 7.3

### 8. Monitoring screens

- [x] **8.1 Implement the dashboard (job list)**
  Files: `replicadb-server/frontend/src/api/jobsApi.ts`, `replicadb-server/frontend/src/pages/DashboardPage.tsx`
  Changes: `jobsApi.ts` exports `listJobs(page, size)` calling `GET /api/v1/jobs`. `DashboardPage` replaces the task 2.3 placeholder with an MUI `Table` (name, source table, sink table, mode, a warning icon/tooltip when `modeWarning` is present) plus pagination controls, and a link per row to `/jobs/{id}`.
  Tests: New `src/pages/DashboardPage.test.tsx` (mocked `jobsApi.listJobs` returning a `PageResponse`-shaped fixture) asserting: (a) each job row renders its name/tables/mode; (b) a row whose `modeWarning` is non-null renders the warning indicator and a row with `null` does not; (c) clicking "next page" calls `listJobs` with the incremented page number; (d) a fixture where `content.length < size` (last page) or `totalElements === 0` (empty result) disables/hides the "next page" control.
  Dependencies: 4.1, 5.1, 6.1, 2.3

- [x] **8.2 Implement read-only job detail**
  Files: `replicadb-server/frontend/src/api/jobsApi.ts` (add `getJob(id)`), `replicadb-server/frontend/src/pages/JobDetailPage.tsx`, `replicadb-server/frontend/src/router/routes.tsx` (add the `/jobs/:id` route)
  Changes: Fetch `GET /api/v1/jobs/{id}` and render `name`, `sourceTable`/`sinkTable`, `mode`, `jobs` (parallelism), `incrementalWatermarkColumn`, `initialWatermarkValue`, `createdAt`/`updatedAt`, and a prominent MUI `Alert` banner for `modeWarning` when present. No edit affordance.
  Tests: New `src/pages/JobDetailPage.test.tsx` asserting: (a) all listed fields render from a mocked `JobDefinitionResponse` fixture; (b) a fixture with `mode: 'complete'` and a non-null `modeWarning` renders the alert banner; (c) a fixture with `mode: 'complete-atomic'` and `modeWarning: null` renders no banner.
  Dependencies: 8.1

- [x] **8.3 Implement the run history table**
  Files: `replicadb-server/frontend/src/api/runsApi.ts`, `replicadb-server/frontend/src/components/RunHistoryTable.tsx`, `replicadb-server/frontend/src/pages/JobDetailPage.tsx` (embed the table)
  Changes: `runsApi.ts` exports `listJobRuns(jobId, page, size)` calling `GET /api/v1/jobs/{id}/runs`. `RunHistoryTable` renders `status` as a color-coded MUI `Chip` (a fixed mapping for all 7 `JobRunStatus` values: `PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCEL_REQUESTED`, `CANCELLED`, `RETRY_SCHEDULED`), `attempt`, `startedAt`/`finishedAt`, and a link to `/runs/{id}`.
  Tests: New `src/components/RunHistoryTable.test.tsx` parameterized over all 7 `JobRunStatus` values, asserting each renders the expected chip label/color.
  Dependencies: 8.2

- [x] **8.4 Implement run detail with terminal-aware polling**
  Files: `replicadb-server/frontend/src/api/runsApi.ts` (add `getRun(id)`, `getRunLog(id)`), `replicadb-server/frontend/src/pages/RunDetailPage.tsx`, `replicadb-server/frontend/src/utils/runStatus.ts`, `replicadb-server/frontend/src/router/routes.tsx` (add the `/runs/:id` route)
  Changes: `runStatus.ts` exports `isTerminalRunStatus(status)` returning `true` only for `SUCCEEDED`, `CANCELLED`, `RETRY_SCHEDULED` — a deliberate TypeScript mirror of the backend's `JobRunStatus.isTerminal()` (see Architecture & Design note above; `FAILED` is intentionally excluded). `RunDetailPage` uses `useQuery` with `refetchInterval: (query) => isTerminalRunStatus(query.state.data?.status) ? false : 5000`, and renders `status`, `attempt`, `rowsProcessed`, `durationMillis`, `committedWatermark`, `errorMessage`, `cancellationWarning` (as an `Alert` when present), and the log excerpt from `getRunLog(id)`.
  Tests: New `src/pages/RunDetailPage.test.tsx` asserting: (a) a `RUNNING`-status fixture configures the query with polling enabled (not terminal → polling stays on); (b) a `SUCCEEDED` fixture stops polling; (c) a `FAILED` fixture **still polls** (asserting the deliberate parity with the backend); (d) a fixture with a non-null `cancellationWarning` renders the alert. Also a focused unit test for `runStatus.ts` asserting `isTerminalRunStatus` returns `true` only for `SUCCEEDED`/`CANCELLED`/`RETRY_SCHEDULED`.
  Dependencies: 8.3

### 9. Playwright end-to-end test

- [x] **9.1 Add Playwright configuration**
  Files: `replicadb-server/frontend/playwright.config.ts`, `replicadb-server/frontend/package.json` (add `@playwright/test` devDependency and `test:e2e` script), `replicadb-server/frontend/.gitignore` (add `test-results/`, `playwright-report/`)
  Changes: `playwright.config.ts` reads `baseURL` from `process.env.PLAYWRIGHT_BASE_URL ?? 'http://localhost:8080'` and uses the installed system Chrome by default (`PLAYWRIGHT_CHANNEL` can select bundled `chromium` in CI). The test targets a built app served by a running `replicadb-server`, not the Vite dev server, so it exercises the real static build and session cookies.
  Tests: `npx playwright test --list` (from `replicadb-server/frontend/`) runs without error and lists `0` tests before task 9.2 adds the first spec.
  Dependencies: 2.1

- [x] **9.2 Write the login/dashboard/logout smoke spec**
  Files: `replicadb-server/frontend/e2e/login.spec.ts`, `replicadb-server/frontend/e2e/README.md` (local-run prerequisites)
  Changes: Against a real running `replicadb-server` (documented prerequisite: PostgreSQL reachable, migrations applied, and an `ADMIN` user bootstrapped by starting the server with `REPLICADB_BOOTSTRAP_ADMIN_USERNAME` and `REPLICADB_BOOTSTRAP_ADMIN_PASSWORD` set — the exact two environment variables read by the existing `AdminBootstrapRunner.java`): navigate to `/`, assert redirect to `/login`; fill in the bootstrapped admin's credentials (read by the spec from `process.env.REPLICADB_BOOTSTRAP_ADMIN_USERNAME`/`_PASSWORD` so the same values configure both the server and the test) and submit; assert redirect to `/` and the dashboard heading is visible; click "Logout"; assert redirect back to `/login`.
  Tests: This task's deliverable is the Playwright spec itself. Scenarios covered: unauthenticated redirect, successful login, successful logout.
  Dependencies: 9.1, 7.2, 7.3, 7.4, 8.1

- [x] **9.3 Wire the Playwright spec into CI**
  Files: `.github/workflows/CT_Push.yml` (new `frontend-e2e` job)
  Changes: Add a job with a `postgres:16` service container, start the built `replicadb-server` jar in the background with `SPRING_PROFILES_ACTIVE=api`, `REPLICADB_BOOTSTRAP_ADMIN_USERNAME`, and `REPLICADB_BOOTSTRAP_ADMIN_PASSWORD` (a CI-only generated secret) pointed at the service container, wait for `/actuator/health` to return `200`, run `npx playwright test` from `replicadb-server/frontend/`, and upload the Playwright HTML report as a build artifact on failure. In the same job, after the Playwright run, also run `npm run generate:api-types` against the live backend into a temp file and `diff` it against the committed `src/api/schema.ts`, failing the job if they differ — this is the drift-detection gate referenced in task 4.1.
  Tests: The CI job itself is the verification (fails the workflow if the spec from 9.2 fails against a real backend, or if the committed OpenAPI types have drifted from the live spec).
  Dependencies: 9.2, 3.1, 3.2, 4.1

## Technical Reference

<details>
<summary>Types & Data Structures</summary>

Generated (task 4.1) from the OpenAPI spec — do not hand-write these; they mirror the existing Java records:

- `JobDefinitionResponse`: `id`, `name`, `sourceConnect`, `sourceUser`, `sourceTable`, `sourceWhere`, `sinkConnect`, `sinkUser`, `sinkTable`, `mode` (`'complete' | 'complete-atomic' | 'incremental'`), `jobs`, `incrementalWatermarkColumn`, `initialWatermarkValue`, `createdAt`, `updatedAt`, `sourcePasswordConfigured`, `sinkPasswordConfigured`, `modeWarning`.
- `JobRunResponse`: `id`, `jobDefinitionId`, `previousRunId`, `status` (`JobRunStatus`), `attempt`, `executorIdentity`, `leaseUntil`, `heartbeatAt`, `createdAt`, `startedAt`, `finishedAt`, `rowsProcessed`, `durationMillis`, `committedWatermark`, `errorMessage`, `cancellationWarning`.
- `JobRunStatus`: `'PENDING' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'CANCEL_REQUESTED' | 'CANCELLED' | 'RETRY_SCHEDULED'`.
- `PageResponse<T>`: `content: T[]`, `page`, `size`, `totalElements`.
- `UserIdentityResponse` (from `GET /api/v1/auth/me`): `id`, `username`, `role` (`'ADMIN' | 'OPERATOR' | 'VIEWER'`).

Hand-written app types:

- `ApiError { status: number; title: string; detail: string }` (`src/api/client.ts`).
- `AuthState = { status: 'loading' } | { status: 'anonymous' } | { status: 'authenticated'; user: UserIdentityResponse }` (`src/auth/AuthContext.tsx`).

</details>

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy

- Tasks completed as planned: 23/23 (100%).
- Tasks that required plan adjustment: 5/23 (22%).
- Test loop iterations: 20 total (14 first-pass, 6 second-pass, 0 third-pass).

### Gaps Encountered

#### Gap 1: Deferred CSRF cookie was not issued before login (Plan-to-Implementation)

- **Task**: 1.3, added during execution.
- **Plan assumed**: Matching axios and Spring Security XSRF cookie/header names was sufficient for login followed by logout.
- **Reality**: `CookieCsrfTokenRepository` did not emit `XSRF-TOKEN` while the login request was ignored by the CSRF filter, so logout returned 403.
- **Resolution**: Added public `GET /api/v1/auth/csrf`; the frontend requests it before login, and MockMvc plus Playwright cover the complete handshake.
- **Learning**: Test SPA session flows against a real cookie jar; naming conventions do not prove that a deferred CSRF token has been issued.

#### Gap 2: OpenAPI nullability did not match serialized Java nulls (Plan-to-Implementation)

- **Task**: 4.1 and 8.1/8.2.
- **Plan assumed**: Generated response types would represent nullable Java record fields as `string | null`.
- **Reality**: Springdoc emitted optional properties without `null`, while API responses serialize null watermark and warning fields.
- **Resolution**: Kept the generated schema as the source of truth and added a narrow typed response adapter for the observed nullable fields; live schema drift is checked in CI.
- **Learning**: Validate generated types against actual JSON fixtures, not only generated path existence.

#### Gap 3: Local tool downloads were blocked by certificate policy (Plan-to-Implementation)

- **Task**: 3.1 and 9.1/9.2.
- **Plan assumed**: Local Maven Node download and Playwright browser installation would be available with default trust settings.
- **Reality**: the local certificate chain rejected Node/Playwright downloads.
- **Resolution**: Kept reproducible downloads for CI, validated Maven packaging with the installed Node using official plugin skip flags, and defaulted local Playwright to installed Chrome while CI selects bundled Chromium.
- **Learning**: Separate local certificate/tooling constraints from build configuration and provide an explicit system-browser fallback for local E2E work.

#### Gap 4: CI resolved frontend packages through an inaccessible private registry (Plan-to-Implementation)

- **Task**: 3.1 and 9.3.
- **Plan assumed**: `npm ci` would resolve the committed lockfile in GitHub Actions using the repository's default npm configuration.
- **Reality**: The lockfile contained Artifactory URLs from the development machine, and the GitHub runner received `403 Forbidden` while downloading `yargs-parser`.
- **Resolution**: Rewrote lockfile URLs to `registry.npmjs.org`, added a project `.npmrc`, and aligned Maven/CI/local documentation on Node 22 and npm 10.
- **Learning**: Never commit environment-specific package registry URLs; validate `npm ci` in a clean runner-like environment before wiring it into Maven.

#### Gap 5: OpenAPI drift check was sensitive to framework-generated schema ordering (Plan-to-Implementation)

- **Task**: 1.3, 4.1, and 9.3.
- **Plan assumed**: The generated OpenAPI document would be byte-for-byte stable when the endpoint behavior was unchanged.
- **Reality**: Springdoc exposed the framework `CsrfToken` parameter type and emitted its properties in different orders across environments, causing the CI `diff` to fail even though the Playwright smoke test passed.
- **Resolution**: Added the explicit `CsrfTokenResponse` DTO with `@JsonPropertyOrder`, retrieved the token from the request attribute, and removed the framework token type from the generated schema.
- **Learning**: Keep framework implementation types out of public OpenAPI contracts and enforce deterministic DTO serialization when generated artifacts are compared byte-for-byte.

### Patterns Discovered

- **Static SPA packaging**: `frontend-maven-plugin` builds Vite output into `replicadb-server/src/main/resources/static`, which Spring Boot serves through the existing `api` profile.
- **Session bootstrap boundary**: the auth provider owns `/auth/me`, while the API client performs the CSRF bootstrap before login and retains session/XSRF cookies for later mutations.
- **Generated DTO adapters**: keep OpenAPI output committed and immutable, then isolate concrete backend nullability deviations in small API-layer type aliases.

<details>
<summary>Dependencies</summary>

**Backend (`replicadb-server/pom.xml`)**:
- `org.springdoc:springdoc-openapi-starter-webmvc-ui` (task 1.1)

**Frontend (`replicadb-server/frontend/package.json`)**:
- Runtime: `react`, `react-dom`, `react-router-dom`, `@mui/material`, `@mui/icons-material`, `@emotion/react`, `@emotion/styled`, `@tanstack/react-query`, `axios`
- Build: `vite`, `@vitejs/plugin-react`, `typescript`
- Codegen: `openapi-typescript`
- Test: `vitest`, `@testing-library/react`, `@testing-library/jest-dom`, `jsdom`, `axios-mock-adapter`, `@playwright/test`

**Build integration**: `com.github.eirslett:frontend-maven-plugin` in `replicadb-server/pom.xml` (task 3.1).

</details>

<details>
<summary>Testing Strategy</summary>

- **Vitest + Testing Library**: every component/hook/util introduced in tasks 2–8 gets a colocated unit test, run via `npm run test` and wired into the existing `mvn -pl replicadb-server` build only as a manual `npm test` step for this slice (no Maven-triggered `npm test` execution is added in this plan — `frontend-maven-plugin` only runs `npm run build`).
- **Backend integration tests**: `OpenApiSpecificationIT` and the extended Spring Security test (tasks 1.1/1.2) use the same `@SpringBootTest` + Testcontainers PostgreSQL pattern already used across `replicadb-server`'s test suite.
- **Playwright**: one smoke spec (task 9.2) covering the full login → dashboard → logout loop against a real running backend; deeper per-screen e2e coverage (job detail, run detail polling, pagination) is deferred to Phase 2b/2c plans once mutating flows exist to test alongside them.
- **Manual verification steps** (tasks 3.1, 9.1) are explicitly called out as build/tooling checks rather than automated tests, since they verify build-tool wiring rather than application behavior.

</details>
