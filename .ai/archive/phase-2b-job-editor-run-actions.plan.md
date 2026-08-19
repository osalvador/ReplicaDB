# Implementation Plan: Phase 2b — Job Editor and Run Actions

## Task Source
No JIRA ticket. Source: `ARCHITECTURE_DECISIONS.md`, "Phase 2: Frontend" section, Phase 2b sub-section (status PENDING) plus its Phase 2 success-metrics entry.

Acceptance criteria (paraphrased from the architecture doc):
- Create/update job definitions (source/sink table pair, mode, parallelism, watermark column).
- Schedule management against the Phase 1c-2 endpoints.
- Mutating run actions: trigger (with `Idempotency-Key`), cancel, retry.
- All of the above operate against the existing `/api/v1` contract validated since Phase 1c-1/1c-2 — **no backend API changes required** by the success metric, except a defect fix discovered during research (see Task 1).

## Overview
Phase 2a delivered read-only authentication/monitoring screens. Phase 2b adds the mutating frontend surface: a job create/edit form, recurring-schedule management, and run trigger/cancel/retry actions, consuming REST endpoints already delivered in Phase 1c-1/1c-2. One backend defect was found during research — `PUT /api/v1/jobs/{id}` currently overwrites `sourcePassword`/`sinkPassword` with whatever the request sends, and the API never returns the real password value to a client, so a naive edit form would silently wipe credentials. This is fixed first (Task 1) since the job editor's correctness depends on it.

## Architecture & Design
**Approach**: dedicated pages/components reusing Phase 2a conventions (one API module per resource in `src/api`, one page per concern in `src/pages`, reusable pieces in `src/components`), rather than inlining edit mode into `JobDetailPage` or building a multi-step wizard.

**Integration points**: `/api/v1/jobs` (`POST`/`PUT`), `/api/v1/jobs/{id}/schedule` (`PUT`/`GET`/`DELETE`), `/api/v1/jobs/{id}/runs` (`POST`), `/api/v1/runs/{id}/cancel`, `/api/v1/runs/{id}/retry`. No new backend endpoints, DTO fields, or OpenAPI regeneration.

**Authorization**: backend ACLs (`VIEW`/`EDIT`/`EXECUTE`/`CANCEL`) remain authoritative. The frontend does not hide controls based on client-known role; any `ApiError` — whether 400 (validation), 403 (forbidden by ACL), or 409 (conflict, e.g. active run already exists) — is surfaced through the same existing inline `Alert` pattern already used by `client.ts`'s `ApiError`. No separate per-status-code handling path is introduced; this is a deliberate simplification, not a gap, since `ApiError` normalizes all RFC 7807 responses to `{status, title, detail}` regardless of code.

**Watermark field handling** (clarified after critic review): the `incrementalWatermarkColumn`/`initialWatermarkValue` fields must be handled at **two levels**, both required and both tested — (1) UI level: the inputs are `disabled` when `mode !== 'incremental'`; (2) payload level: `toJobDefinitionRequest()` (Task 2.1) unconditionally omits both fields from the outgoing request unless `mode === 'incremental'`, regardless of any stale value left in local form state. This guarantees a `complete`/`complete-atomic` job is never persisted with a stray watermark column.

## Implementation Tasks

### 1. Backend correctness fix

- [x] **1.1 Preserve existing credentials when `JobDefinitionMapper` builds an update**
  Files: [replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionMapper.java](replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionMapper.java); [replicadb-server/src/test/java/org/replicadb/server/job/api/JobDefinitionMapperTest.java](replicadb-server/src/test/java/org/replicadb/server/job/api/JobDefinitionMapperTest.java)
  Changes: Add an overload of `toDefinition` taking `existingSourcePassword`/`existingSinkPassword`. When `request.sourcePassword()` is `null`/blank, fall back to `existingSourcePassword`; independently, when `request.sinkPassword()` is `null`/blank, fall back to `existingSinkPassword`. The two fields are resolved independently of each other. The existing `create`-path overload (called with `null`/`null` existing values) is unchanged in behavior.
  Tests: JobDefinitionMapperTest — (a) update with blank `sourcePassword` and a **new, non-blank** `sinkPassword` keeps the prior source password AND replaces the sink password (proves independence); (b) update with a **new, non-blank** `sourcePassword` and blank `sinkPassword` replaces the source password AND keeps the prior sink password (the mirrored case); (c) update with both blank keeps both prior values; (d) create path (existing overload) still accepts `null` for both passwords, unaffected by the new overload.
  Dependencies: None

- [x] **1.2 Wire the preserving overload into `JobDefinitionController.update`**
  Files: [replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionController.java](replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionController.java); [replicadb-server/src/test/java/org/replicadb/server/job/api/JobDefinitionControllerTest.java](replicadb-server/src/test/java/org/replicadb/server/job/api/JobDefinitionControllerTest.java)
  Changes: In `update(...)`, call the new mapper overload passing `existing.sourcePassword()`/`existing.sinkPassword()` before persisting the replacement.
  Tests: JobDefinitionControllerTest (MockMvc) — (a) `PUT` with blank `sourcePassword` but a new `sinkPassword` value: assert (via the repository/mocked persistence layer) the persisted `sourcePassword` is unchanged and `sinkPassword` is the new value; (b) the mirrored case (new `sourcePassword`, blank `sinkPassword`); (c) `PUT` with both blank persists both prior values unchanged.
  Dependencies: Task 1.1

### 2. Frontend API adapters

- [x] **2.1 Add `createJob`/`updateJob` to `jobsApi.ts`**
  Files: [replicadb-server/frontend/src/api/jobsApi.ts](replicadb-server/frontend/src/api/jobsApi.ts); `replicadb-server/frontend/src/api/jobsApi.test.ts` (new)
  Changes: Define `JobDefinitionFormInput` (local type mirroring `JobDefinitionRequest` but allowing `''` for optional fields, and including `mode: 'complete' | 'complete-atomic' | 'incremental'`). Define an exported pure `toJobDefinitionRequest(input: JobDefinitionFormInput): components['schemas']['JobDefinitionRequest']` normalizer that: (i) converts blank optional strings (`sourceUser`, `sourcePassword`, `sourceWhere`, `sinkUser`, `sinkPassword`) to `undefined`; (ii) **unconditionally omits `incrementalWatermarkColumn`/`initialWatermarkValue` from the returned object unless `input.mode === 'incremental'`**, regardless of their input values. `createJob(input)` calls `apiClient.post('/jobs', toJobDefinitionRequest(input))`; `updateJob(id, input)` calls `apiClient.put('/jobs/${id}', toJobDefinitionRequest(input))` and uses the normalized payload's output directly (not the raw `input`) as the request body in both cases.
  Tests: jobsApi.test.ts using `axios-mock-adapter` (already a dev dependency, see `client.test.ts`) — (a) `toJobDefinitionRequest` strips blank optional string fields; (b) `toJobDefinitionRequest` omits `incrementalWatermarkColumn`/`initialWatermarkValue` entirely when `mode !== 'incremental'`, even if non-blank values were provided in the input; (c) `toJobDefinitionRequest` keeps both watermark fields when `mode === 'incremental'`; (d) `createJob` posts to `/jobs` with a body deep-equal to `toJobDefinitionRequest(input)`'s output (assert via `mock.history.post[0].data`, not just that `createJob` resolves); (e) `updateJob` puts to `/jobs/{id}` with the same normalized-body assertion; (f) a 400 `application/problem+json` response from either call surfaces as `ApiError`.
  Dependencies: None

- [x] **2.2 Add `scheduleApi.ts`**
  Files: `replicadb-server/frontend/src/api/scheduleApi.ts` (new); `replicadb-server/frontend/src/api/scheduleApi.test.ts` (new)
  Changes: `getSchedule(jobId)` — `GET /jobs/{id}/schedule`; catch an `ApiError` with `status === 404` and return `null` instead of throwing, rethrow any other error. `upsertSchedule(jobId, input)` — `PUT /jobs/{id}/schedule`. `deleteSchedule(jobId)` — `DELETE /jobs/{id}/schedule`. Reuse `components['schemas']['JobScheduleRequest'|'JobScheduleResponse']` from `schema.ts` directly (no local DTO duplication).
  Tests: scheduleApi.test.ts with axios-mock-adapter — (a) a 200 response returns the parsed schedule; (b) a 404 `application/problem+json` response resolves `getSchedule` to `null` rather than rejecting; (c) a 500 response still rejects with `ApiError` (proves the 404 catch is status-specific, not blanket); (d) `upsertSchedule` sends a `PUT` to the correct path with the given body; (e) `deleteSchedule` sends a `DELETE` to the correct path.
  Dependencies: None

- [x] **2.3 Add `triggerRun`/`cancelRun`/`retryRun` to `runsApi.ts`**
  Files: [replicadb-server/frontend/src/api/runsApi.ts](replicadb-server/frontend/src/api/runsApi.ts); `replicadb-server/frontend/src/api/runsApi.test.ts` (new)
  Changes: `triggerRun(jobId)` — `POST /jobs/{id}/runs`, generating a **new** `Idempotency-Key: crypto.randomUUID()` header value on every call (the key must be computed inside the function body at call time, not memoized, cached at module scope, or passed in from a caller-held constant). `cancelRun(id)` — `POST /runs/{id}/cancel`, returns `CancellationResponse`. `retryRun(id)` — `POST /runs/{id}/retry`.
  Tests: runsApi.test.ts with axios-mock-adapter — (a) `triggerRun` sends a non-empty `Idempotency-Key` header matching the UUID v4 format; (b) **two sequential calls to `triggerRun` (same `jobId`, back to back) produce two different `Idempotency-Key` header values** — this is the explicit regression test for key-freshness, asserted by comparing `mock.history.post[0].headers['Idempotency-Key']` and `mock.history.post[1].headers['Idempotency-Key']`; (c) `cancelRun` returns the `warning` field from the mocked response; (d) `retryRun` posts to `/runs/{id}/retry` and returns the new run from the response body.
  Dependencies: None

### 3. Job editor page

- [x] **3.1 Build `JobFormPage` (create and edit)**
  Files: `replicadb-server/frontend/src/pages/JobFormPage.tsx` (new); `replicadb-server/frontend/src/pages/JobFormPage.test.tsx` (new)
  Changes: Single component handling both `/jobs/new` and `/jobs/:id/edit` (mode detected via presence of the `:id` route param). Edit mode loads via `getJob(id)` and prefills all fields; the `name` field is rendered `disabled` in edit mode (backend rejects a changed name). Fields: `name` (create only, required), `sourceConnect` (required), `sourceUser`, `sourcePassword` (helper text, rendered only in edit mode: "Leave blank to keep the existing value"), `sourceTable` (required), `sourceWhere`, `sinkConnect` (required), `sinkUser`, `sinkPassword` (same helper text in edit mode), `sinkTable` (required), `mode` (select: `complete`/`complete-atomic`/`incremental`), `jobs` (number input, min 1, required), `incrementalWatermarkColumn`/`initialWatermarkValue` (rendered `disabled` when `mode !== 'incremental'`; per the Architecture & Design section, their values are stripped from the submitted payload by `toJobDefinitionRequest` regardless of any stale local state). Client-side validation blocks submission when `sourceConnect`/`sourceTable`/`sinkConnect`/`sinkTable` are blank or `jobs < 1`, rendering a field-level MUI `TextField` `error`/`helperText`. `useMutation` over `createJob`/`updateJob`; on success, `navigate` to `/jobs/{id}` and call `queryClient.invalidateQueries({ queryKey: ['jobs'] })` (and additionally `['jobs', id]` in edit mode). A `complete`-mode warning `Alert` is shown, sourced from the fetched `job.modeWarning` in edit mode (never a second hardcoded copy of that string); in create mode, no warning is shown until the record exists (nothing to source it from).
  Tests: JobFormPage.test.tsx (mocking `../api/jobsApi`, using a fresh `QueryClient` per `frontend.instructions.md` convention) — (a) create mode: filling all required fields and submitting calls `createJob` with the expected normalized payload and navigates to `/jobs/{returnedId}`; (b) edit mode: prefills every field from a mocked `getJob` response and renders the `name` input as `disabled`; (c) edit mode: renders the "Leave blank to keep the existing value" helper text next to both password fields; (d) selecting `complete` or `complete-atomic` mode renders the watermark inputs as `disabled`, and submitting in that mode asserts (via the mocked `updateJob`/`createJob` call arguments) that `incrementalWatermarkColumn`/`initialWatermarkValue` are absent from the submitted payload even if they were previously populated while the mode was `incremental`; (e) a mocked `ApiError` (400) rejection from the mutation renders an inline `Alert` with its `detail` text and does **not** call `navigate`; (f) leaving `sourceTable` blank and submitting shows a field-level validation error and does not call `createJob`/`updateJob`.
  Dependencies: Task 2.1

### 4. Schedule management

- [x] **4.1 Build `JobScheduleCard` component**
  Files: `replicadb-server/frontend/src/components/JobScheduleCard.tsx` (new); `replicadb-server/frontend/src/components/JobScheduleCard.test.tsx` (new)
  Changes: Accepts a `jobId: string` prop. `useQuery` over `getSchedule(jobId)`. Renders "No recurring schedule configured" plus a "Create schedule" button when the query resolves to `null`; otherwise renders `cronExpression`, `timeZone`, `enabled` state, and `nextFireTime`, plus "Edit" and "Delete" buttons. "Create"/"Edit" open an MUI `Dialog` with `cronExpression` (required non-blank text field), `timeZone` (text field, default `'UTC'`), `enabled` (switch, default `true`); submits via the `upsertSchedule` mutation and invalidates the `['schedule', jobId]` query on success, closing the dialog. "Delete" opens a confirmation `Dialog` with the text `"Remove the recurring schedule for this job? This cannot be undone from here."` and a "Remove" confirm button; confirming calls `deleteSchedule` and invalidates `['schedule', jobId]` on success, closing the dialog. A mutation error (create/edit/delete) keeps the relevant dialog open and renders an inline `Alert` inside it with the `ApiError.detail` text.
  Tests: JobScheduleCard.test.tsx (mocking `../api/scheduleApi`) — (a) renders "No recurring schedule configured" when `getSchedule` resolves `null`; (b) renders the existing schedule's `cronExpression`/`timeZone`/`nextFireTime` when present; (c) submitting the create dialog calls `upsertSchedule(jobId, {...})` with the entered field values and the dialog closes on success; (d) confirming the delete dialog (with its confirmation text present) calls `deleteSchedule(jobId)`; (e) a mocked `upsertSchedule` rejection keeps the dialog open and renders the error's `detail` text inside it, without calling `deleteSchedule` or closing.
  Dependencies: Task 2.2

- [x] **4.2 Embed `JobScheduleCard` into `JobDetailPage`**
  Files: [replicadb-server/frontend/src/pages/JobDetailPage.tsx](replicadb-server/frontend/src/pages/JobDetailPage.tsx); [replicadb-server/frontend/src/pages/JobDetailPage.test.tsx](replicadb-server/frontend/src/pages/JobDetailPage.test.tsx)
  Changes: Render `<JobScheduleCard jobId={id ?? ''} />` below the existing definition-details `Paper`, above `RunHistoryTable`.
  Tests: JobDetailPage.test.tsx — mock `../components/JobScheduleCard` as a stub (`vi.mock('../components/JobScheduleCard', () => ({ default: (props) => <div data-testid="schedule-card" data-job-id={props.jobId} /> }))`) and assert it renders with the correct `jobId` prop, so this test does not depend on Task 4.1's internal implementation.
  Dependencies: Task 4.1

### 5. Run actions

- [x] **5.1 Add "Trigger run" action to `JobDetailPage`**
  Files: [replicadb-server/frontend/src/pages/JobDetailPage.tsx](replicadb-server/frontend/src/pages/JobDetailPage.tsx); [replicadb-server/frontend/src/pages/JobDetailPage.test.tsx](replicadb-server/frontend/src/pages/JobDetailPage.test.tsx)
  Changes: Add a "Trigger run" button using `useMutation` over `triggerRun(jobId)`. The button is `disabled` while the mutation `isPending`. On success, call `queryClient.invalidateQueries({ queryKey: ['jobRuns', jobId] })` (the key already used by `RunHistoryTable`/`listJobRuns`) and `navigate('/runs/${result.id}')`. On an `ApiError` with `status === 409` ("already has an active run"), render an inline `Alert` with the error's `detail` text instead of navigating.
  Tests: JobDetailPage.test.tsx — (a) clicking "Trigger run" calls the mocked `triggerRun` and navigates to `/runs/{newRunId}` on success; (b) the assertion in (a) also asserts `queryClient.invalidateQueries` was called with `queryKey: ['jobRuns', jobId]` (spy on the `QueryClient` instance passed to the test's `QueryClientProvider`, or assert the runs list re-fetches by asserting `listJobRuns` mock call count increases after the trigger); (c) the button is `disabled` immediately after clicking, before the mocked promise resolves; (d) a mocked 409 rejection renders an inline alert with the error detail and does not call `navigate`.
  Dependencies: Task 2.3, Task 4.2

- [x] **5.2 Add "Cancel run"/"Retry run" actions to `RunDetailPage`**
  Files: [replicadb-server/frontend/src/pages/RunDetailPage.tsx](replicadb-server/frontend/src/pages/RunDetailPage.tsx); [replicadb-server/frontend/src/pages/RunDetailPage.test.tsx](replicadb-server/frontend/src/pages/RunDetailPage.test.tsx)
  Changes: "Cancel run" button rendered only when `run.status` is `PENDING`, `RUNNING`, or `CANCEL_REQUESTED`; on click calls `cancelRun(id)`, renders the returned `warning` in an `Alert`, and calls `queryClient.invalidateQueries({ queryKey: ['runs', id] })` so the existing polling (`getRunRefetchInterval`, unchanged) picks up the new status on its next tick. "Retry run" button rendered only when `run.status === 'FAILED'`; on click calls `retryRun(id)` and `navigate('/runs/${result.id}')`. Neither button modifies `utils/runStatus.ts`; this task only consumes its existing terminal-status contract (`SUCCEEDED`/`CANCELLED`/`RETRY_SCHEDULED` stop polling, `FAILED` remains non-terminal per `frontend.instructions.md`) and does not re-implement or duplicate it.
  Tests: RunDetailPage.test.tsx — (a) the Cancel button renders for each of `RUNNING`/`PENDING`/`CANCEL_REQUESTED` and is absent for each of `SUCCEEDED`/`FAILED`/`CANCELLED`/`RETRY_SCHEDULED` (parameterized/table test over all 7 statuses); (b) clicking Cancel calls `cancelRun(id)`, renders the returned warning text, and asserts `queryClient.invalidateQueries` was called with `queryKey: ['runs', id]`; (c) the Retry button renders only when `status === 'FAILED'` (asserted alongside the same 7-status table as (a)) and clicking it calls `retryRun(id)` then navigates to the new run's id; (d) a mocked rejection from either `cancelRun` or `retryRun` renders an inline error `Alert` without throwing or navigating.
  Dependencies: Task 2.3

### 6. Routing and entry points

- [x] **6.1 Add `/jobs/new` and `/jobs/:id/edit` routes**
  Files: [replicadb-server/frontend/src/router/routes.tsx](replicadb-server/frontend/src/router/routes.tsx); [replicadb-server/frontend/src/router/routes.test.tsx](replicadb-server/frontend/src/router/routes.test.tsx)
  Changes: Add `{ path: 'jobs/new', element: <JobFormPage /> }` and `{ path: 'jobs/:id/edit', element: <JobFormPage /> }` under the existing protected `children` array, alongside the current `jobs/:id` and `runs/:id` entries.
  Tests: routes.test.tsx — assert both new paths resolve to `JobFormPage` under the protected route tree, following the same assertion style already used for `jobs/:id`/`runs/:id` in this file.
  Dependencies: Task 3.1

- [x] **6.2 Add "New job" and "Edit" entry-point links**
  Files: [replicadb-server/frontend/src/pages/DashboardPage.tsx](replicadb-server/frontend/src/pages/DashboardPage.tsx); [replicadb-server/frontend/src/pages/DashboardPage.test.tsx](replicadb-server/frontend/src/pages/DashboardPage.test.tsx); [replicadb-server/frontend/src/pages/JobDetailPage.tsx](replicadb-server/frontend/src/pages/JobDetailPage.tsx); [replicadb-server/frontend/src/pages/JobDetailPage.test.tsx](replicadb-server/frontend/src/pages/JobDetailPage.test.tsx)
  Changes: Add a "New job" `Button`/`RouterLink` on `DashboardPage` targeting `/jobs/new`, placed near the page heading. Add an "Edit" `Button`/`RouterLink` on `JobDetailPage` targeting `/jobs/{id}/edit`, placed near the job name heading.
  Tests: DashboardPage.test.tsx — the "New job" link's `href` (rendered `to`) resolves to `/jobs/new`. JobDetailPage.test.tsx — the "Edit" link's `href` resolves to `/jobs/{id}/edit` for the rendered job id.
  Dependencies: Task 6.1, Task 5.1

### 7. Documentation

- [x] **7.1 Update `frontend.instructions.md` mutating-scope note**
  Files: [.github/instructions/frontend.instructions.md](.github/instructions/frontend.instructions.md)
  Changes: Replace the line "Keep the Phase 2a surface read-only. Add mutating controls only with the corresponding backend contract, permission check, CSRF coverage, and planned product slice." with wording that acknowledges Phase 2b's shipped job editor, schedule management, and trigger/cancel/retry controls, while preserving the underlying rule (mutating controls require a backend contract, permission check, CSRF coverage, and a planned product slice) for any future addition.
  Tests: None — documentation-only change with no executable code path. Verified by re-reading the file after the edit to confirm it no longer contradicts shipped behavior.
  Dependencies: Task 6.2

- [x] **7.2 Flip Phase 2b status in `ARCHITECTURE_DECISIONS.md`**
  Files: [ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md)
  Changes: Update the Phase 2b sub-section heading from "PENDING" to "IMPLEMENTED" with a short delivery note in the same style as the Phase 2a note, and check off the corresponding Priority 3 checklist item and the related Phase 2 success-metrics bullet.
  Tests: None — documentation-only change. Perform only after Tasks 1.1–7.1 pass their own tests, i.e. this is the final task in execution order.
  Dependencies: Task 7.1

## Technical Reference

<details>
<summary>Types & Data Structures</summary>

- `JobDefinitionFormInput` (new, `jobsApi.ts`): same shape as `JobDefinitionRequest` but allows `''` for optional string fields; never sent directly over the wire — always passed through `toJobDefinitionRequest()` first.
- `toJobDefinitionRequest(input): JobDefinitionRequest` (new, exported from `jobsApi.ts`): the single place responsible for (a) blank-to-`undefined` normalization and (b) mode-conditional stripping of the two watermark fields. Both `createJob` and `updateJob` must route through it so the two entry points cannot drift.
- Reused generated types from `schema.ts`: `JobDefinitionRequest`, `JobScheduleRequest`, `JobScheduleResponse`, `CancellationResponse`, `JobRunResponse`. No hand-maintained duplicate DTOs are introduced, per `frontend.instructions.md`.
</details>

<details>
<summary>Dependencies</summary>

- `axios-mock-adapter` is already a frontend dev dependency (used in `client.test.ts`) — reused for all three new/extended API-module test files; no new dependency to add.
- `crypto.randomUUID()` (browser/Node built-in, no new dependency) for the `Idempotency-Key` header; must be called fresh inside `triggerRun` per call (Task 2.3).
</details>

<details>
<summary>Testing Strategy</summary>

- Backend: JUnit Jupiter via the existing Surefire configuration, explicit class list (`JobDefinitionMapperTest`, `JobDefinitionControllerTest`) per `test-patterns.instructions.md` — no wildcard selectors.
- Frontend: Vitest + Testing Library for components/pages, each test using a fresh `QueryClient` and `MemoryRouter`; `axios-mock-adapter` for direct API-module unit tests (assert actual request bodies/headers via `mock.history`, not just resolved values); mock `../api/*` modules in page/component tests rather than hitting the network, per existing `JobDetailPage.test.tsx` convention.
- No Playwright e2e task is included in this plan — Phase 2b's acceptance criteria are fully unit/integration-testable against the already-validated existing endpoints. E2e coverage (create → schedule → trigger → cancel, as one authenticated flow) can be added opportunistically later without being a dependency of this plan.
- Cross-cutting regression coverage added after critic review: independent source/sink password preservation (1.1/1.2), `Idempotency-Key` freshness across sequential calls (2.3), and payload-level (not just UI-level) stripping of watermark fields for non-incremental modes (2.1/3.1).
</details>

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy
- Tasks completed as planned: 14/14 (100%)
- Tasks that required plan adjustment: 0/14 (0%)
- Test loop iterations: 28 validation commands total (10 first-pass, 5 second-pass, 2 third-pass, and 11 environment or selector corrections)

### Gaps Encountered

None - plan executed as designed.

### Patterns Discovered
- Centralized form normalization: `toJobDefinitionRequest()` is the single boundary for blank optional values and mode-gated watermark fields.
- Dialog-local mutation errors: schedule mutations keep their relevant MUI dialog open and display the API detail inline.
