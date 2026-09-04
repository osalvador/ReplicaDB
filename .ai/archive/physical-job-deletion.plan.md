# Implementation Plan: Physical Job Deletion

## Task Source

User request: enable job deletion through the API and frontend. A job must be deletable from its detail page, and the jobs catalog must open a job detail when any area of a row is clicked.

Confirmed decisions:

- Deletion is physical: remove the job definition, run history, run logs, idempotency records, schedule, and job permissions.
- Only `ADMIN` can delete a job.
- A job with a `PENDING`, `RUNNING`, or `CANCEL_REQUESTED` run cannot be deleted; return `409 Conflict`.
- Keep audit events after deletion and record `JOB_DELETED` with the deleted job identity.
- Successful deletion returns `204` from the API; the frontend invalidates the catalog and navigates to `/jobs`.
- The existing clickable-row behavior is retained and covered by regression tests.

## Overview

Add a destructive `DELETE /api/v1/jobs/{id}` operation to the managed control plane and expose it from the job detail screen with an explicit confirmation dialog. PostgreSQL will own the dependent-record cleanup through a forward-only migration, while the API coordinates the active-run guard, Quartz unscheduling, authorization, and audit recording.

The implementation preserves the current ACL model by making deletion administrator-only rather than introducing a new `DELETE` permission. The frontend hides the action for non-admin users as a usability boundary, but backend authorization remains authoritative.

## Architecture & Design

Approach: Cascada en base de datos.

- Add migration `V21__cascade_job_dependent_state_on_definition_delete.sql`.
- In V21, explicitly drop the V2-generated `job_run_job_definition_id_fkey` constraint and recreate it with an explicit name and `ON DELETE CASCADE`; terminal history and its `run_log` rows are then removed with the definition. Verify the self-referencing `previous_run_id` chain is deleted safely in one statement.
- Add a named foreign key from `run_trigger_idempotency.job_definition_id` to `job_definition(id) ON DELETE CASCADE`; this currently has no foreign key and otherwise leaves stale idempotency entries after deletion. The migration must fail with an actionable error if legacy idempotency rows reference a missing job rather than silently deleting them.
- Preserve the existing cascade behavior for `job_schedule` and `job_permission`.
- Lock the definition with `findByIdForUpdate`, check `JobRunStore.hasActiveRun`, unschedule Quartz, then delete the definition in one API transaction. A run inserted before the parent lock is observed by the active check; a run insert after the lock is blocked by the parent foreign-key lock and fails after the definition is deleted, so no active run is silently orphaned or deleted.
- Return an explicit repository result containing only a deletion status and safe job name for the audit record, and map the active-run `IllegalStateException` through the existing RFC 7807 `409` handler.
- Add `JOB_DELETED` to the audit action vocabulary. The audit row is independent of the job definition and remains after physical deletion.
- Add `deleteJob` to the frontend API module, regenerate the OpenAPI-derived schema from the running server contract, and add a red `Delete` action with an MUI confirmation dialog on `JobDetailPage`.

Security and operations:

- Apply `@PreAuthorize("hasRole('ADMIN')")` to the endpoint.
- Do not include connection parameters, credentials, lease tokens, or run logs in the deletion response or audit detail.
- Unschedule Quartz before deleting the database row. If Quartz removal fails, fail the request and leave the database record available for retry/reconciliation.
- Reject active work rather than cancelling it implicitly. This avoids deleting a definition while an executor still holds a lease or is resolving datasource state.
- Keep RFC 7807 error responses and existing `ApiError` mapping.

## Implementation Tasks

### 1. Extend the database dependency contract

- [x] **1.1 Add cascading foreign keys for all job-owned runtime state**
  Files: `replicadb-server/src/main/resources/db/migration/V21__cascade_job_dependent_state_on_definition_delete.sql`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java`
  Changes: Create a forward-only migration that first checks for orphaned `run_trigger_idempotency.job_definition_id` values and raises an actionable exception if any exist; drop `job_run_job_definition_id_fkey` and add an explicitly named replacement with `ON DELETE CASCADE`; add an explicitly named `run_trigger_idempotency` foreign key with `ON DELETE CASCADE`. Preserve the existing schedule, permission, and run-log cascades. Update migration expectations from 20 to 21 and assert the exact delete rules and constraint names.
  Tests: Apply all migrations to an isolated PostgreSQL schema; assert migration count and validation; verify valid pre-existing idempotency rows survive V21; insert a job with a schedule, permissions, a three-level terminal retry chain linked by `previous_run_id`, run logs, and idempotency rows; delete the job and assert every dependent row is gone while an audit row remains. Run a migration fixture with an orphaned idempotency row and assert V21 fails with the actionable message. Leave active-run behavior to the controller and concurrency tests in Task 2.1.
  Dependencies: None

- [x] **1.2 Extend the job repository deletion contract**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/port/JobDefinitionStore.java`, `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobDefinitionRepository.java`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobDefinitionRepositoryIT.java`
  Changes: Define an explicit `DeleteResult` contract, for example `record DeleteResult(Status status, String jobName)` with `Status.DELETED` and `Status.NOT_FOUND`, and add `DeleteResult delete(UUID id)` to `JobDefinitionStore`. Implement a parameterized `DELETE FROM job_definition WHERE id = :id`, relying on V21 cascades, and return only the safe name needed for `JOB_DELETED`; do not return credentials or runtime payloads. Keep the existing `findByIdForUpdate` path available so the controller can serialize deletion against concurrent run creation.
  Tests: Verify deletion returns `DELETED` with the safe name for an existing definition and removes dependent schedule, permission, run, run-log, and idempotency rows; verify a second deletion returns `NOT_FOUND`; verify terminal three-level run chains are removable; verify the repository does not expose secret or lease fields.
  Dependencies: 1.1

### 2. Add the authenticated API operation

- [x] **2.1 Implement admin-only deletion with active-run protection and Quartz cleanup**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionController.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/QuartzScheduleService.java`, `replicadb-server/src/main/java/org/replicadb/server/audit/domain/AuditAction.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/JobDefinitionControllerTest.java`
  Changes: Switch the controller dependency to `JobDefinitionStore`, inject `JobRunStore` and `QuartzScheduleService`, and add `DELETE /api/v1/jobs/{id}` with `@PreAuthorize("hasRole('ADMIN')")` and `@Transactional`. Lock the definition, return `404` when absent, reject `PENDING`, `RUNNING`, and `CANCEL_REQUESTED` with a clear `409` detail, call `QuartzScheduleService.unschedule(id)` before the repository delete, delete through `JobDefinitionStore`, and record `JOB_DELETED` after a successful deletion. Add the audit enum value before using it. Keep unscheduling idempotent for jobs without a persisted Quartz schedule; let its existing `IllegalStateException` wrapper map scheduler failures to a clear conflict response while leaving the database row intact.
  Tests: Add MockMvc coverage for admin success (`204`), missing job (`404` problem response), non-admin (`403`), active pending/running/cancel-requested jobs (`409` with no audit success and no deletion), Quartz unschedule failure (`409`, database row remains), no schedule, and audit success containing only safe job identity. Add a real-PostgreSQL concurrency scenario with separate connections and barriers: a run committed before the definition lock produces `409`, while a run attempting insertion after the lock is rejected by the parent FK after deletion; assert no active run is deleted or orphaned.
  Dependencies: 1.2

- [x] **2.2 Update the jobs API documentation and generated contract source**
  Files: `.ai/interfaces/jobs-api.md`, `replicadb-server/frontend/src/api/schema.ts`, `replicadb-server/frontend/src/api/schema.test.ts`, `replicadb-server/frontend/scripts/generate-api-types.mjs`
  Changes: Document `DELETE /jobs/{id}`, its admin-only authorization, `204/403/404/409` responses, active-run rule, irreversibility, backup/recovery expectation, and physical dependent-state cleanup. After the backend endpoint is available on the API profile, run `npm run generate:api-types` from `replicadb-server/frontend/` with `OPENAPI_SCHEMA_URL` set to the reachable `/v3/api-docs`; commit the generated `src/api/schema.ts` output and do not hand-edit generated endpoint types. Confirm the generated contract includes the delete operation and no sensitive response schema.
  Tests: Run the OpenAPI schema drift assertions and verify the generated path exposes `delete` with the expected response statuses. Run the exact documented generation command after backend compilation/startup and before frontend API implementation, without committing credentials or internal secret values.
  Dependencies: 2.1

### 3. Consume deletion from the frontend

- [x] **3.1 Add the frontend delete API helper and request tests**
  Files: `replicadb-server/frontend/src/api/jobsApi.ts`, `replicadb-server/frontend/src/api/jobsApi.test.ts`
  Changes: Add `deleteJob(id: string): Promise<void>` using `apiClient.delete(`/jobs/${id}`)`, preserving the configured `/api/v1` client and RFC 7807 error mapping. Do not add a local DTO or expose deletion internals in client state.
  Tests: Mock a `204` delete and assert the exact request path; assert `400`, `403`, and `409` problem responses reject with the existing `ApiError` shape; assert no request body or credential fields are sent.
  Dependencies: 2.2

- [x] **3.2 Add the admin-only destructive action to job detail**
  Files: `replicadb-server/frontend/src/pages/JobDetailPage.tsx`, `replicadb-server/frontend/src/pages/JobDetailPage.test.tsx`
  Changes: Add a red `Delete` button with `DeleteOutlineIcon` after the existing management actions, separated from `Trigger run`, visible only for `ADMIN` users. Open an MUI `Dialog` with the job name, explicit cancel/confirm actions, disabled controls while pending, and an inline error on failure. On success invalidate `['jobs']` and `['jobs', id]`, then navigate to `/jobs`. Preserve the existing trigger/edit/permissions actions and their responsive grouping. Use native button keyboard activation (`Enter`/`Space`); do not add a global Delete-key shortcut.
  Tests: Verify admin visibility and non-admin absence; verify dialog copy includes the job name; verify cancel does not call the API; verify confirmation calls `deleteJob(id)`, disables the confirm action while pending, invalidates both queries, and navigates to `/jobs`; verify `ApiError` conflict/forbidden details remain visible, the dialog stays open, and a retry remains possible; verify keyboard focus and accessible names for Delete, Cancel, and confirmation.
  Dependencies: 3.1

### 4. Validate the catalog and end-to-end flow

- [x] **4.1 Preserve and regression-test clickable job rows**
  Files: `replicadb-server/frontend/src/pages/JobsPage.tsx`, `replicadb-server/frontend/src/pages/JobsPage.test.tsx`
  Changes: Keep the existing row-level click, `Enter`, and `Space` navigation behavior while adding no row-level destructive action. Ensure the row remains keyboard-focusable, keeps the visible focus indicator, and does not interfere with the name link. This task is regression-only unless the delete implementation accidentally changes the catalog surface.
  Tests: Verify clicking a non-name cell navigates to `/jobs/{id}`; verify `Enter` and `Space` activate the row; verify the name link still exposes the expected href; verify empty, loading, error, and paginated catalog states remain unchanged.
  Dependencies: 3.2

- [x] **4.2 Add authenticated browser coverage and final validation**
  Files: `replicadb-server/frontend/e2e/job-deletion.spec.ts`, `replicadb-server/src/test/java/org/replicadb/server/job/api/JobDefinitionControllerTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobDefinitionRepositoryIT.java`, `replicadb-server/frontend/src/api/jobsApi.test.ts`, `replicadb-server/frontend/src/pages/JobDetailPage.test.tsx`, `replicadb-server/frontend/src/pages/JobsPage.test.tsx`
  Changes: Add a Playwright spec using the existing environment-managed admin login and isolated test data. Cover opening a job from any row cell, opening/cancelling the delete dialog, deleting a job without active runs, redirecting to `/jobs`, and confirming the job no longer appears. Also exercise the `409` active-run path and confirm no destructive UI success is shown. Classify this as authenticated E2E coverage, not a MockMvc substitute; run it through the existing `npm run test:e2e` or isolated local admin stack.
  Tests: From `replicadb-server/`, `mvn -Dtest=JobDefinitionControllerTest,JobDefinitionRepositoryIT,FlywayMigrationTest test`; `npm --prefix replicadb-server/frontend test -- --run src/api/jobsApi.test.ts src/pages/JobDetailPage.test.tsx src/pages/JobsPage.test.tsx`; `npm --prefix replicadb-server/frontend run typecheck`; `npm --prefix replicadb-server/frontend run build`; `npm --prefix replicadb-server/frontend run test:e2e -- e2e/job-deletion.spec.ts`; `git diff --check`; report missing environment-managed E2E credentials separately from product failures.
  Dependencies: 1.1, 2.1, 2.2, 3.2, 4.1

## Technical Reference

### Types & Data Structures

- API path: `DELETE /api/v1/jobs/{id}`.
- Success: empty `204 No Content` response.
- Errors: existing RFC 7807 `application/problem+json` mapping for `403`, `404`, and `409`.
- Active statuses: `PENDING`, `RUNNING`, `CANCEL_REQUESTED`.
- Audit action: `JOB_DELETED`, resource type `JOB_DEFINITION`, resource ID equal to the deleted job UUID, safe detail limited to the job name and deletion category.
- Frontend helper: `deleteJob(id: string): Promise<void>`.

### Database Dependency Graph

```text
job_definition
  ├─ job_schedule              ON DELETE CASCADE (existing)
  ├─ job_permission             ON DELETE CASCADE (existing)
  ├─ job_run                    ON DELETE CASCADE (V21)
  │    ├─ run_log               ON DELETE CASCADE (existing)
  │    └─ previous_run_id       self-reference; verify terminal chains
  └─ run_trigger_idempotency    ON DELETE CASCADE (V21)
```

`audit_event` remains independent and is intentionally preserved.

### Dependencies

- No new Java or npm dependency.
- PostgreSQL migration V21 must run after the current V20 migration.
- Quartz unscheduling uses the existing `QuartzScheduleService` and stable job keys.
- OpenAPI types must be regenerated from the live server contract, never hand-copied.

### Testing Strategy

- Repository integration tests prove cascade behavior and repeatable `NOT_FOUND` semantics against PostgreSQL.
- Controller tests prove admin authorization, active-run conflict behavior, Quartz failure handling, audit behavior, and RFC 7807 responses.
- Frontend unit tests prove API mapping, dialog state, invalidation, redirect, and accessible controls.
- Jobs catalog tests preserve the already-delivered row click and keyboard behavior.
- Browser validation proves the complete authenticated delete flow and confirms the catalog no longer lists the deleted definition.

### Risks and Mitigations

- **Historical data loss:** physical deletion is intentional and confirmed; the confirmation dialog, admin-only authorization, and audit retention make the destructive boundary explicit. The operation is irreversible from the application API; recovery depends on PostgreSQL backup/restore or an operator-managed snapshot, which must be documented before rollout.
- **Concurrent dispatch:** lock the definition, reject active statuses, rely on FK locking for a racing run insert, and cover the race with PostgreSQL integration tests.
- **Quartz/database divergence:** unschedule before deletion and fail without deleting when Quartz cannot be updated; retain reconciliation as the operational recovery path.
- **Stale idempotency keys:** `run_trigger_idempotency` is job-scoped, not run-scoped; V21 adds a job foreign key with cascade, and the existing TTL cleanup remains a defense for any legacy rows that cannot pass the migration precondition.
- **Contract drift:** regenerate `schema.ts` from Spring OpenAPI and run the existing drift checks before frontend integration.

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy

- Tasks completed as planned: 5/8 (63%)
- Tasks that required plan adjustment: 3/8 (37%)
- Test loop iterations: 8 total (7 first-pass, 1 second-pass)

### Gaps Encountered

#### Gap 1: The server module is standalone rather than a root Maven reactor module (Plan-to-Implementation)

- **Task**: 4.2 — Add authenticated browser coverage and final validation.
- **Plan assumed**: `mvn -pl replicadb-server ...` could select the server module from the root POM.
- **Reality**: The root POM does not declare `replicadb-server` as a reactor module; Maven rejected the selector before running tests.
- **Resolution**: Ran the server tests from `replicadb-server/` with the installed Java 17 runtime and corrected the plan command.
- **Learning**: Validate Maven module topology before prescribing reactor selectors; standalone server modules need module-local commands.

#### Gap 2: OpenAPI defaulted the DELETE response to 200 and omitted documented error statuses (Plan-to-Implementation)

- **Task**: 2.2 — Update the jobs API documentation and generated contract source.
- **Plan assumed**: Springdoc would infer the declared `204/403/404/409` contract from the controller method and exception handlers.
- **Reality**: The first generated schema exposed only a `200` response for the new DELETE operation.
- **Resolution**: Added explicit Springdoc `@ApiResponses`, regenerated `schema.ts` from the live API, and added generated-type assertions for all four statuses.
- **Learning**: Treat generated OpenAPI output as the contract evidence; explicit response annotations are required when empty responses and global error handlers define the public behavior.

#### Gap 3: The first Playwright flow used a catalog action from the dashboard (Plan-to-Implementation)

- **Task**: 4.2 — Add authenticated browser coverage and final validation.
- **Plan assumed**: The dashboard exposed the `New job` action used to create isolated E2E data.
- **Reality**: `New job` is available on `/jobs`, while the dashboard only summarizes operations.
- **Resolution**: Navigated explicitly to `/jobs` before creating the test job and added a deterministic wait for the protected-route login redirect.
- **Learning**: Keep E2E helpers aligned with the owning page's action surface and wait for auth route transitions before querying protected controls.

### Patterns Discovered

- **Job-owned state deletion:** use PostgreSQL foreign-key cascades for schedule, permissions, runs, logs, and job-scoped idempotency while preserving independent audit events; see `V21__cascade_job_dependent_state_on_definition_delete.sql`.
- **Safe destructive API contract:** lock the parent definition, reject active runs, unschedule Quartz, delete through a port, then audit only the safe job identity; see `JobDefinitionController.java`.
