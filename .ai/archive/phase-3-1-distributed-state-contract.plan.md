# Implementation Plan: Phase 3.1 — Distributed State Contract

## Task Source — JIRA: none — Phase 3.1 distributed state contract

The source of truth is the approved Phase 3.1 section in [ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md). The user selected the **complete separation of domain and persistence** approach for this plan.

Acceptance criteria derived from the architecture decision:

- Add the persisted per-job retry policy: `maxAttempts`, `retryBackoffSeconds`, and `automaticRetryEnabled`.
- Default `maxAttempts` to `3` and `retryBackoffSeconds` to `60` seconds. Default `automaticRetryEnabled` to `true` for `complete-atomic` and `incremental`, and `false` for `complete`; a `complete` job may explicitly opt in.
- Add `JobRun.availableAt` and an opaque lease token generated for every claim. `availableAt` is evaluated with PostgreSQL `now()`.
- Define application ports and use-case services for claims, lease renewal, expiry recovery, and fenced finalization instead of making controllers depend directly on SQL-shaped behavior.
- Claim only eligible pending runs atomically with `FOR UPDATE SKIP LOCKED`, assigning a fresh lease token and PostgreSQL-owned lease timestamps.
- Renew a lease only when the run id and current lease token still own a non-expired run.
- Recover an expired run in one PostgreSQL transaction: retain the abandoned attempt, schedule a new attempt with `previousRunId` and backoff when policy allows, or mark the abandoned attempt `FAILED` when recovery is disabled or attempts are exhausted.
- Require the lease token on worker-owned heartbeat, counter, error, cancellation-completion, terminal-state, and watermark writes. A stale worker must update zero rows and must never advance a watermark.
- Persist cancellation intent independently of the API instance's in-memory execution registry while preserving the existing mode-specific sink warnings.
- Extend the Java records, repositories, REST DTOs, generated OpenAPI types, and frontend editor for retry policy fields without exposing lease tokens or credentials.
- Keep the current `api` profile executable while introducing the new contract. Do not add the `worker` profile, PostgreSQL notifications, listener/poller runtime, heartbeat loop, Quartz JDBC clustering, shared login throttling, or load/chaos deployment tests; those belong to Phase 3.2 and 3.3.
- Preserve the standalone CLI artifact, its no-PostgreSQL execution path, and all existing replication semantics.

## Overview

Phase 3.1 turns the existing single-instance run metadata into a state contract that remains correct when multiple API and worker processes share PostgreSQL. The change separates domain policy and lease/recovery use cases from the JDBC adapter, then updates the current API execution path to use the same fenced contract that Phase 3.2 will consume from a worker.

The phase does not distribute execution yet. Its release value is that a future worker can claim, renew, recover, and finalize runs without trusting process-local state or worker clocks, while retries still execute from the beginning and preserve the existing mode-dependent safety rules.

## Architecture & Design — Approach: Complete separation of domain and persistence

### Layering

The implementation uses four explicit boundaries:

1. **Domain**: `RetryPolicy`, `LeaseToken`, `JobDefinition`, `JobRun`, and the legal transition rules. The domain owns validation and mode-specific defaults; it does not know SQL, Spring, or HTTP.
2. **Application ports and services**: `JobDefinitionStore`, `JobRunStore`, lease, recovery, cancellation, and finalization services. These expose use-case operations and immutable results, including a directed claim for the current API coordinator and an undirected eligible claim for Phase 3.2 workers.
3. **PostgreSQL adapter**: the existing Spring JDBC repositories implement the ports. Claims, recovery, lease renewal, and fenced writes use short transactions and PostgreSQL time/locking primitives. No core replication work runs inside a database lock transaction.
4. **API and execution adapters**: controllers, the current `RunExecutionCoordinator`, and `JobExecutionService` depend on the ports/services. The coordinator's in-memory map remains only a best-effort local cancellation signal until Phase 3.2; persisted run state is authoritative.

The new claim contract accepts an optional requested run id. A worker calls it without a requested id and receives the oldest eligible run; the current API coordinator calls it with the id created by its trigger. Both paths execute the same `available_at`, row-locking, lease-token, and fencing rules, so retaining current API behavior does not preserve the old unsafe repository methods.

### Retry and recovery semantics

- `maxAttempts` includes the initial attempt.
- Automatic recovery uses the configured backoff directly: the replacement row's `availableAt` is PostgreSQL `now()` plus `retryBackoffSeconds`. No exponential or jitter formula is introduced.
- Automatic recovery applies to lease expiry only. A normal `FAILED` result remains eligible for the existing explicit manual retry endpoint; manual retry preserves history and is not silently converted into automatic recovery.
- If a lease expires after durable `CANCEL_REQUESTED`, cancellation wins: the abandoned row becomes `CANCELLED` with no replacement attempt. A cancellation request is never converted into an automatic retry.
- A failed or cancelled run never advances the committed watermark. A stale finalizer receives a fenced/no-op result and cannot write counters, errors, terminal status, or watermark.
- `complete` remains destructive. Enabling automatic retry for it is allowed only through an explicit policy value, and the existing complete-mode warning remains present in the API and frontend.

### Performance and security

- The partial eligible-run index covers `status = 'PENDING'`, `available_at`, creation order, and id so claims do not scan terminal history.
- `FOR UPDATE SKIP LOCKED` allows independent workers to make progress without waiting on a row another worker owns. Recovery locks one expired run per transaction and releases the lock before any replacement attempt executes.
- Lease timestamps and expiry comparisons come from PostgreSQL `now()`, avoiding clock skew between API and worker processes.
- Keep the current five-minute lease duration for the API compatibility path; the 30-second heartbeat cadence and worker-configurable renewal loop are Phase 3.2 responsibilities.
- Lease tokens are opaque UUID values stored only in PostgreSQL and internal Java state. They are absent from `JobRunResponse`, OpenAPI responses, frontend types, audit details, logs, and error messages.
- Retry policy fields contain no credentials. Existing environment-reference and redaction rules remain unchanged; resolved secrets are still created only immediately before core execution.

### Phase boundary

This plan defines the contract and adapts the current API executor, but it deliberately does not create a scheduler for expiry recovery or a worker process. Phase 3.2 will call the undirected claim/recovery ports from the isolated `worker` profile, add `LISTEN/NOTIFY` plus mandatory polling, move remote cancellation delivery, and run an independent heartbeat loop. Phase 3.3 will make Quartz and authentication state highly available.

## Implementation Tasks

### 1. Domain contract

- [x] **1.1 Add the validated `RetryPolicy` value object and attach it to `JobDefinition`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/domain/RetryPolicy.java`, `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobDefinition.java`, `replicadb-server/src/test/java/org/replicadb/server/job/domain/RetryPolicyTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/domain/JobDefinitionTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/domain/JobDefinitionTestFixtures.java`
  Changes: Introduce immutable `RetryPolicy` fields for `maxAttempts`, `retryBackoffSeconds`, and `automaticRetryEnabled`; validate attempts as at least 1 and backoff as nonnegative; provide mode-derived defaults of `3`, `60`, and mode-specific automatic retry. Add an explicit constructor/factory path for `complete` opt-in, preserve existing `JobDefinition` convenience constructors by applying mode defaults, and expose read-only `maxAttempts()`, `retryBackoffSeconds()`, and `automaticRetryEnabled()` accessors without duplicating policy state. Update the fixture builder so existing tests can opt into a policy without embedding policy construction in every test.
  Tests: Unit-test defaults for all three replication modes, explicit `complete` opt-in, lower/upper validation boundaries, rejection of null policy, and the rule that `maxAttempts` includes the initial attempt. Verify existing credential-reference, watermark-mode, and CLI compatibility validation still runs with the new constructor shape.
  Dependencies: None

- [x] **1.2 Add lease identity and recovery transitions to the run domain**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/domain/LeaseToken.java`, `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRun.java`, `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRunStateMachine.java`, `replicadb-server/src/test/java/org/replicadb/server/job/domain/LeaseTokenTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/domain/JobRunTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/domain/JobRunStateMachineTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/ScheduledRunTriggerJobTest.java`
  Changes: Add an opaque UUID-backed `LeaseToken` value object and add non-null `availableAt` plus nullable `leaseToken` to `JobRun`; compatibility constructors must derive `availableAt` from the persisted/created timestamp, while only the lease token may be absent before a claim. Extend legal transitions for lease expiry (`RUNNING -> RETRY_SCHEDULED` when a replacement is created and `RUNNING -> FAILED` when recovery is disabled or exhausted) while preserving the existing manual retry transition and terminal-state semantics. Update all direct run constructors and test fixtures to use the new fields without exposing the token through domain-to-API mapping.
  Tests: Assert token generation produces distinct nonblank values, `availableAt` survives construction, invalid attempts still fail, all recovery transitions are legal, terminal rows cannot be reopened, and old retry/cancellation transitions remain unchanged.
  Dependencies: Task 1.1

### 2. Application ports and use cases

- [x] **2.1 Define storage ports and lease/recovery/finalization services**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/port/JobDefinitionStore.java`, `replicadb-server/src/main/java/org/replicadb/server/job/port/JobRunStore.java`, `replicadb-server/src/main/java/org/replicadb/server/job/application/RunLeaseService.java`, `replicadb-server/src/main/java/org/replicadb/server/job/application/RunRecoveryService.java`, `replicadb-server/src/main/java/org/replicadb/server/job/application/RunCancellationService.java`, `replicadb-server/src/main/java/org/replicadb/server/job/application/RunFinalizationService.java`, `replicadb-server/src/main/java/org/replicadb/server/job/application/RunRecoveryResult.java`, `replicadb-server/src/test/java/org/replicadb/server/job/application/RunLeaseServiceTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/application/RunRecoveryServiceTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/application/RunCancellationServiceTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/application/RunFinalizationServiceTest.java`
  Changes: Define ports for definition lookup/update and run state operations instead of making controllers and execution code depend on repository-specific method sets. The run port must include: pending insertion with `availableAt`, eligible claim with optional requested id, token-checked renewal, expired-run recovery, persisted cancellation intent, pending cancellation, manual retry, token-checked progress/finalization, watermark lookup, and read/paging methods. Add immutable recovery results distinguishing the abandoned row from an optional replacement. Services must validate lease duration and transition intent, persist cancellation before attempting the local in-memory signal, and treat a zero-row token-checked finalization as a fenced stale-worker result rather than a successful state write.
  Tests: Mockito-based contract tests cover directed versus queue claims, positive lease-duration validation, cancellation persistence even when no local execution is registered, manual retry preserving the previous row, and finalization suppressing audit/state success when the port reports fencing. No test may assert that an in-memory map is the source of truth.
  Dependencies: Tasks 1.1 and 1.2

### 3. Forward-only database contract

- [x] **3.1 Persist retry policy fields with safe backfill defaults**
  Files: `replicadb-server/src/main/resources/db/migration/V13__add_job_retry_policy.sql`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobDefinitionRepositoryIT.java`
  Changes: Add `max_attempts`, `retry_backoff_seconds`, and `automatic_retry_enabled` to `job_definition` with constraints matching `RetryPolicy`. Backfill existing rows to `3` attempts and `60` seconds, set automatic retry true for existing `incremental` and `complete-atomic` rows and false for `complete`, then make the columns non-null. Keep the migration forward-only and make repository inserts bind explicit policy values so future database defaults cannot override a mode-specific application decision.
  Tests: Run Flyway against PostgreSQL and assert the new columns, constraints, and mode-specific backfill; update the migration-count assertion to include 13 migrations at this intermediate step; round-trip an existing-style definition and verify policy values are returned exactly.
  Dependencies: Task 1.1

- [x] **3.2 Add eligible-run and lease-token persistence**
  Files: `replicadb-server/src/main/resources/db/migration/V14__add_job_run_eligibility_and_lease.sql`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobRunRepositoryIT.java`
  Changes: Add non-null `available_at` with `now()` backfill/default and nullable UUID `lease_token` to `job_run`. Replace or supersede the pending index with a partial index ordered by `available_at`, `created_at`, and `id` for eligible pending claims. Preserve the active-run uniqueness behavior that excludes `RETRY_SCHEDULED`, and leave lease tokens available for fenced history without exposing them through API records.
  Tests: Apply and validate all 14 migrations, inspect the resulting PostgreSQL columns/indexes, verify old pending rows receive an eligible `availableAt`, and assert the active-run constraint still permits the recovery transition to insert a replacement after the abandoned row changes status.
  Dependencies: Task 3.1

### 4. JDBC adapters

- [x] **4.1 Make `JobDefinitionRepository` implement the definition storage port**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobDefinitionRepository.java`, `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobDefinitionRowMapper.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobDefinitionRepositoryIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/JobDefinitionMapperTest.java`
  Changes: Extract the existing row mapping into a named adapter mapper, bind/read the three retry-policy columns, construct `RetryPolicy` through the domain validator, and make insert/update paths preserve explicit mode-derived defaults and existing policy values. Keep credential references redacted at API boundaries and never include resolved credentials in persistence logs or exceptions. Update repository call sites to depend on `JobDefinitionStore` where they only need the port.
  Tests: Verify insert/find/update round trips for all policy fields, existing rows with backfilled values map correctly, invalid policy values are rejected before persistence, and connection/password redaction remains unchanged in mapper and repository-facing responses.
  Dependencies: Tasks 1.1, 2.1, and 3.1

- [x] **4.2 Implement the atomic eligible-claim adapter**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java`, `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRowMapper.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobRunRepositoryIT.java`
  Changes: Replace `claimNextPending` and `claimById` as public repository contracts with one transactional `claimNextEligible` operation that accepts an optional requested run id. Select only `PENDING` rows with `available_at <= now()`, order deterministically, use `FOR UPDATE SKIP LOCKED`, generate a fresh `LeaseToken` for every successful claim, and update status, executor identity, `started_at`, `heartbeat_at`, and `lease_until` using PostgreSQL `now()`. Keep the transaction limited to the claim/update and return the claimed row after commit; do not hold the row lock during ReplicaDB execution.
  Tests: With two independent PostgreSQL connections, prove concurrent queue claims select distinct eligible rows and skip a deliberately held lock; prove a future `availableAt` is ignored; prove a directed claim cannot claim a different run; prove repeated claims create distinct tokens and no second claim can reopen a running row.
  Dependencies: Tasks 1.2, 2.1, 3.2, and 4.1

- [x] **4.3 Implement token-fenced lease renewal**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobRunRepositoryIT.java`
  Changes: Add `renewLease(runId, leaseToken, leaseDuration)` that updates `lease_until` and `heartbeat_at` with PostgreSQL `now()` only when the id, token, and active status match and the current lease has not already expired. Return a boolean/typed outcome distinguishing renewed, missing, and fenced ownership; never accept a timestamp supplied by a worker.
  Tests: Assert the current token renews both timestamps, a random/stale token updates zero rows, an expired lease cannot be renewed by its former owner, a terminal run cannot be renewed, and a second worker's token never changes the first worker's lease.
  Dependencies: Task 4.2

- [x] **4.4 Implement transactional expired-run recovery**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java`, `replicadb-server/src/main/java/org/replicadb/server/job/application/RunRecoveryResult.java`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobRunRepositoryIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/application/RunRecoveryServiceTest.java`
  Changes: Add `recoverExpiredRun(UUID runId)` using one PostgreSQL transaction: lock the requested expired `RUNNING` or `CANCEL_REQUESTED` row with `FOR UPDATE SKIP LOCKED`, join and lock its `job_definition` policy row in the same SQL operation, and make cancellation win by transitioning an expired `CANCEL_REQUESTED` row to `CANCELLED` with no replacement. For an expired `RUNNING` row, change the abandoned row to `RETRY_SCHEDULED` and insert a new `PENDING` row with `previousRunId`, incremented attempt, and `available_at = now() + retryBackoffSeconds` when automatic retry is enabled and attempts remain. When automatic retry is disabled or `attempt == maxAttempts`, mark the abandoned row `FAILED` and create no replacement. Use PostgreSQL time in the predicate and backoff expression, preserve the abandoned lease token for history, and never reset the old row to `RUNNING`.
  Tests: Run two concurrent recovery calls for one expired row and prove exactly one replacement is created; verify backoff eligibility before and after the database time boundary; cover `previousRunId`, attempt numbering, automatic-retry defaults for all modes, explicit complete opt-in, max-attempt exhaustion, missing/non-expired rows, and an expired `CANCEL_REQUESTED` row becoming `CANCELLED` without a replacement.
  Dependencies: Tasks 1.1, 1.2, 2.1, 3.1, 3.2, and 4.1

- [x] **4.5 Enforce fencing on progress, cancellation completion, terminal state, and watermark writes**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobRunRepositoryIT.java`, `src/test/java/org/replicadb/ReplicaDBWatermarkCommitTest.java`
  Changes: Change worker-owned repository methods to require `runId + LeaseToken`: progress counters, `markSucceeded`, `markFailed`, `markCancelled`, error persistence, and successful watermark commit must all include the token and the expected status in their conditional updates. Keep `markCancelRequested` and pending cancellation as durable API intent operations, preserving the existing warning. Return a fenced outcome when an update affects zero rows so a late worker cannot be mistaken for a successful finalizer. Preserve the invariant that a failed/cancelled/stale run leaves the prior committed watermark unchanged.
  Tests: Prove a current owner can update counters, error, terminal status, and watermark; after recovery, the stale token cannot update any of them; a stale success cannot advance `findLastCommittedWatermark`; cancellation warnings survive the worker's token-fenced completion; an expired `CANCEL_REQUESTED` run cannot be turned into a retry; and existing watermark commit tests still cover successful merge versus failed/cancelled merge behavior.
  Dependencies: Tasks 1.2, 2.1, 4.2, 4.3, and 4.4

### 5. Current API execution compatibility

- [x] **5.1 Route execution through lease/finalization services**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/JobExecutionService.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/RunExecutionCoordinator.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/ScheduledRunTriggerJob.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/JobExecutionServiceIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/RunExecutionCoordinatorTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/ScheduledRunTriggerJobTest.java`
  Changes: Replace direct repository claim/finalization calls with `RunLeaseService` and `RunFinalizationService`. The coordinator must use the directed form of `claimNextEligible` for manually created/scheduled run ids, pass the claimed token through the full execution path, and retain its local `ToolOptions` map only as a best-effort cancellation signal. Do not add a heartbeat loop or recovery scheduler in this task. Suppress terminal audit/state success when finalization is fenced, preserve redacted error handling, temporary options-file cleanup, watermark lookup, and the unchanged `ReplicaDB.processReplica(ToolOptions)` contract.
  Tests: End-to-end SQLite source/sink plus PostgreSQL state tests must still cover successful incremental watermark commit, failed run preservation of the previous watermark, missing environment references, temporary-file cleanup, and asynchronous coordinator execution. Add a stale-finalizer scenario proving a recovered run cannot emit a second successful audit or overwrite the replacement attempt. Update mocks to target ports/services rather than old repository methods.
  Dependencies: Tasks 2.1, 4.2, 4.3, 4.4, and 4.5

- [x] **5.2 Extend REST definition and run representations without leaking lease identity**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionRequest.java`, `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionResponse.java`, `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionMapper.java`, `replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunResponse.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/JobDefinitionMapperTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/JobDefinitionControllerTest.java`
  Changes: Add nullable request fields for `maxAttempts`, `retryBackoffSeconds`, and `automaticRetryEnabled` so old clients can omit them and the mapper can apply the locked defaults; return resolved non-null policy values in `JobDefinitionResponse`. Add read-only `availableAt` to `JobRunResponse` for retry observability, but do not add `leaseToken` or any credential field. Apply bean validation at the request boundary and retain the complete-mode warning whenever the selected mode is destructive, including when automatic retry is explicitly enabled.
  Tests: Assert omitted fields receive mode-specific defaults, explicit values round-trip, invalid attempts/backoff are rejected as RFC 7807 validation failures, complete opt-in retains its warning, response JSON includes `availableAt`, response JSON/toString never contains `leaseToken`, and existing lower-case mode serialization and password redaction remain stable.
  Dependencies: Tasks 1.1, 1.2, 4.1, and 5.1

- [x] **5.3 Make trigger, cancellation, and manual retry flows use durable state**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunController.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/ScheduledRunTriggerJob.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunControllerTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunCancellationRaceTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/ScheduledJobLifecycleIT.java`
  Changes: Route trigger insertion, cancellation intent, pending cancellation, and explicit manual retry through `JobRunStore`/`RunCancellationService`. Persist `CANCEL_REQUESTED` before making the local cancellation signal, preserve the current race-safe response when the run reaches `CANCELLED`, and do not reject a durable cancellation solely because this API instance has no local `ToolOptions`. Manual retry must create a new immediately eligible pending row, retain `previousRunId` and attempt history, and continue to bypass only the automatic lease-expiry policy, not the no-resume rule. Keep ACL, idempotency, audit, and mode-warning behavior unchanged.
  Tests: Cover trigger idempotency and active-run uniqueness, durable cancellation when the local registry is empty, cancellation racing with token-fenced completion, manual retry after failed/terminal runs, scheduled trigger compatibility, and absence of lease tokens in all controller responses.
  Dependencies: Tasks 2.1, 4.4, 4.5, 5.1, and 5.2

### 6. OpenAPI and frontend contract

- [x] **6.1 Regenerate and verify the OpenAPI TypeScript contract**
  Files: `replicadb-server/src/test/java/org/replicadb/server/OpenApiSpecificationIT.java`, `replicadb-server/frontend/scripts/generate-api-types.mjs`, `replicadb-server/frontend/src/api/schema.ts`, `replicadb-server/frontend/src/api/schema.test.ts`
  Changes: Generate `schema.ts` from the live Springdoc `/v3/api-docs` endpoint using the existing script; do not hand-edit generated declarations. Add contract assertions for retry-policy request/response fields, their nullable/optional input shape and resolved response shape, `availableAt`, lower-case replication modes, and the deliberate absence of `leaseToken` and secrets. Keep the generation command and output path compatible with the existing frontend Maven build.
  Tests: Run `OpenApiSpecificationIT` against the API profile, run the frontend generated-schema Vitest tests, and fail the contract test if a policy field disappears, becomes incorrectly required/null, or exposes the lease token.
  Dependencies: Tasks 5.2 and 5.3

- [x] **6.2 Add retry policy editing and retry-eligibility visibility in the SPA**
  Files: `replicadb-server/frontend/src/api/jobsApi.ts`, `replicadb-server/frontend/src/api/jobsApi.test.ts`, `replicadb-server/frontend/src/api/runsApi.ts`, `replicadb-server/frontend/src/components/RunHistoryTable.tsx`, `replicadb-server/frontend/src/components/RunHistoryTable.test.tsx`, `replicadb-server/frontend/src/pages/JobFormPage.tsx`, `replicadb-server/frontend/src/pages/JobFormPage.test.tsx`, `replicadb-server/frontend/src/pages/RunDetailPage.tsx`, `replicadb-server/frontend/src/pages/RunDetailPage.test.tsx`
  Changes: Extend the form model and normalized job payload with maximum automatic attempts, retry backoff seconds, and automatic-retry enablement. Initialize new jobs with `3`, `60`, and the current mode's automatic-retry default; when the mode changes, update the checkbox only if the user has not edited the policy, so an explicit complete-mode choice is preserved. Allow explicit complete-mode opt-in while retaining the destructive warning, preserve values on edit, and strip no policy fields accidentally during mode changes. Map and display `availableAt` for retry-scheduled runs where the existing run views show attempt/status information; never add a lease-token field to API helpers or UI state.
  Tests: Assert normalized create/update payloads contain policy fields and no token/credential values; cover defaults for all modes, validation for attempts/backoff, complete warning/opt-in, edit prefilling and save behavior, `availableAt` rendering, and existing retry/cancel action matrices. Run typecheck and the focused Vitest suites with a fresh Query client.
  Dependencies: Task 6.1

### 7. Documentation and release verification

- [x] **7.1 Document the distributed state contract and run the phase exit checks**
  Files: `replicadb-server/frontend/README.develop.md`, `ARCHITECTURE_DECISIONS.md`, `replicadb-server/pom.xml`, `pom.xml`
  Changes: Document the retry defaults, mode-specific automatic-retry behavior, direct backoff semantics, no-resume rule, PostgreSQL time authority, lease-token fencing, durable cancellation intent, and the explicit boundary between Phase 3.1 and the future worker/HA phases. Update the architecture status only after the implementation and all checks pass; do not describe `worker`, `LISTEN/NOTIFY`, or Quartz clustering as implemented in this phase. Keep both Maven artifacts and their dependency direction unchanged.
  Tests: Review documentation references and phase-boundary wording with a targeted search; run the focused server suite covering domain, Flyway, repository, execution, API, and OpenAPI tests; run `mvn -B test` for the server module and the root CLI; run `npm run typecheck`, `npm test`, and `npm run build` in `replicadb-server/frontend`; finish with `git diff --check`. Verify that the root CLI still starts without PostgreSQL and that no Spring Boot dependency is introduced into its artifact.
  Dependencies: Tasks 1.1 through 6.2

## Technical Reference

<details>
<summary>Types & Data Structures</summary>

- `RetryPolicy`: validated `maxAttempts >= 1`, `retryBackoffSeconds >= 0`, and `automaticRetryEnabled`; mode defaults are `3`, `60`, and `false` for `complete`, with automatic retry enabled by default for `complete-atomic` and `incremental`.
- `JobDefinition`: owns one `RetryPolicy` value and exposes policy accessors for persistence/API mapping. Existing convenience constructors remain valid and derive the correct mode default.
- `LeaseToken`: opaque UUID value used only by internal state operations. It is generated for every successful claim and is never serialized in API responses.
- `JobRun`: adds non-null `availableAt` for every persisted run and nullable `leaseToken`; `availableAt` controls eligibility, while the token fences ownership. A new retry is always a new row with `previousRunId` and incremented `attempt`.
- `RunRecoveryResult`: identifies the immutable abandoned run transition and, when policy permits, the newly inserted pending run and its `availableAt`.
- `JobDefinitionRequest` accepts optional policy fields for backward-compatible clients. `JobDefinitionResponse` returns resolved policy values. `JobRunResponse` returns `availableAt` but not `leaseToken`.

</details>

<details>
<summary>Dependencies</summary>

- No new third-party dependency is required. The implementation reuses Spring JDBC, Flyway, PostgreSQL, Spring validation, Testcontainers, Springdoc, React, TypeScript, TanStack Query, Vitest, and the existing Maven/frontend build.
- New production packages are limited to `job.domain`, `job.port`, and `job.application`; JDBC remains in `job.persistence` and HTTP remains in `job.api`.
- The root `pom.xml`, CLI launchers, core manager packages, and standalone artifact dependency graph are compatibility surfaces, not extension points for this phase.
- PostgreSQL is mandatory for managed state tests and deployment. SQLite remains only a source/sink fixture for execution tests, never the production state store.

</details>

<details>
<summary>Testing Strategy</summary>

| Layer | Tooling | Required evidence |
| --- | --- | --- |
| Domain | JUnit Jupiter 6, no container | Policy defaults/validation, lease-token opacity, legal recovery transitions, no-resume invariants |
| Application ports/services | JUnit Jupiter + Mockito | Fenced outcomes, durable cancellation before local signaling, service-level lease/recovery decisions |
| Migrations | Flyway + PostgreSQL Testcontainers | Fourteen forward-only migrations, constraints, indexes, backfill, migration validation |
| JDBC claims/recovery | Spring Boot integration + PostgreSQL Testcontainers | `SKIP LOCKED`, distinct concurrent claims, PostgreSQL time, backoff eligibility, duplicate recovery, exhaustion |
| Fencing | PostgreSQL integration | Stale token cannot renew, finalize, update counters/errors, or commit a watermark |
| Managed execution/API | Spring integration, MockMvc, SQLite fixtures | Existing run outcomes, cancellation races, ACL/idempotency behavior, redaction, no token serialization |
| Frontend contract | Springdoc, openapi-typescript, Vitest/Testing Library | Wire shape/nullability, policy editor defaults/validation, retry scheduling visibility, no token exposure |
| Release regression | Maven, npm, CLI smoke path | Server and root suites pass; frontend builds; CLI runs without PostgreSQL/Spring Boot |

</details>

## Risks, Assumptions, and Deferred Work

- The current API coordinator remains in-process in this plan. Its local cancellation registry is intentionally a delivery optimization only; persisted `CANCEL_REQUESTED` state is the contract that Phase 3.2 workers will consume.
- Recovery is implemented and integration-tested but is not scheduled by a new background component in Phase 3.1. Without Phase 3.2, no worker automatically scans expired rows in production.
- A long-running core operation can outlive its lease until Phase 3.2's heartbeat loop is present. The fencing contract prevents stale state writes, but this phase does not yet guarantee lease renewal during merge/swap.
- Manual retry remains an explicit operator action and preserves the existing endpoint semantics. Automatic `maxAttempts` limits lease-expiry recovery; it must not be presented as a resume mechanism.
- Existing migration-count, constructor, generated-schema, and Testcontainers readiness assertions are known historical failure points. The task sequence updates those exact assertions before broad validation and keeps PostgreSQL timestamps explicitly bound at JDBC boundaries.
- No JIRA acceptance criteria were supplied. The acceptance criteria above are extracted from the approved architecture decision and the selected retry defaults confirmed during planning.

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy

- Tasks completed as planned: 12/16 (75%).
- Tasks that required plan adjustment: 4/16 (25%).
- Test loop iterations: approximately 30 total (focused checks were rerun after each implementation failure; most repairs passed on the second iteration).

### Gaps Encountered

#### Gap 1: Flyway staging required the actual configuration API (Plan-to-Implementation)

- **Task**: 3.1/3.2 — migration verification.
- **Plan assumed**: the migration test could chain `target()` and `load()` directly on `Flyway` and isolate each migration step without additional configuration typing.
- **Reality**: this Flyway version exposes those methods on `FluentConfiguration`, and applying the second configuration without a target reran both pending migrations.
- **Resolution**: typed the helper as `FluentConfiguration` and targeted V12, V13, and V14 explicitly so each staged assertion proves the intended migration count.
- **Learning**: inspect the resolved library API and stage forward-only migration tests against explicit schema targets when testing backfills and indexes.

#### Gap 2: JVM-created compatibility timestamps could be briefly ahead of PostgreSQL (Plan-to-Implementation)

- **Task**: 4.2 — eligible claim adapter.
- **Plan assumed**: the legacy `insertPending(...)` wrapper could use `Instant.now()` while the claim predicate used PostgreSQL `now()`.
- **Reality**: separate JDBC/JVM timing made a newly inserted legacy run briefly ineligible, breaking existing single-instance tests even though both clocks were correct.
- **Resolution**: kept the new contract's explicit `availableAt` path authoritative and made only the deprecated compatibility wrapper use a small past margin; claims, retries, recovery, and leases still use PostgreSQL-owned timestamps.
- **Learning**: database eligibility predicates must not compare server time with an application timestamp in compatibility paths; use a database default or an explicitly conservative past value.

#### Gap 3: Replacing repository methods required a compatibility bridge during the same phase (Plan-to-Implementation)

- **Task**: 4.2/5.1/5.3 — migrate API execution and scheduled triggers to ports.
- **Plan assumed**: old claim/finalization methods could be removed as soon as `JobRunStore` was introduced.
- **Reality**: existing integration fixtures and API tests still exercised those methods while the new services were being wired, and Spring also needed an unambiguous constructor after test-only compatibility overloads were retained.
- **Resolution**: implemented the new port methods first, kept deprecated wrappers until all production callers moved, marked the Spring constructor with `@Autowired`, and updated tests incrementally.
- **Learning**: when a state contract replaces a repository API across ordered tasks, use a time-bounded deprecated bridge and explicitly test the production bean constructor during the migration.

#### Gap 4: The planned development README path did not exist (Intent-to-Plan)

- **Task**: 7.1 — documentation and release verification.
- **Plan assumed**: development instructions lived at `replicadb-server/README.develop.md`.
- **Reality**: the maintained development guide is `replicadb-server/frontend/README.develop.md` in this checkout.
- **Resolution**: documented the Phase 3.1 retry, lease, fencing, cancellation, and Phase 3.2/3.3 boundary in the existing frontend development guide and corrected the task path in this plan.
- **Learning**: resolve documentation paths during plan creation with a repository file search; do not infer module-level README locations from adjacent modules.

### Patterns Discovered

- **Fenced state adapter**: typed `UPDATED`/`FENCED`/`NOT_FOUND` outcomes keep stale-worker handling explicit at the repository and application boundaries; see `replicadb-server/src/main/java/org/replicadb/server/job/port/JobRunStore.java`.
- **Database-owned eligibility**: `available_at`, lease timestamps, and recovery backoff are evaluated with PostgreSQL `now()`; see `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java`.
- **Contract exposure without ownership leakage**: OpenAPI and frontend expose retry policy and `availableAt`, while `LeaseToken` remains internal; see `replicadb-server/src/test/java/org/replicadb/server/OpenApiSpecificationIT.java` and `replicadb-server/frontend/src/api/schema.test.ts`.

### Validation Summary

- `replicadb-server`: full Maven suite passed with 246 tests and zero failures using PostgreSQL Testcontainers.
- Frontend: 36 Vitest files passed with 180 tests; `npm run typecheck` and `npm run build` passed. The build emitted only the existing large-chunk warning.
- OpenAPI: Springdoc IT passed; `schema.ts` was regenerated from the live API and verified to contain retry policy/`availableAt` without `leaseToken`.
- Root CLI: the full Maven suite was launched and reached DB2/MariaDB/MongoDB/Oracle integration coverage, but the ARM64-emulated DB2/Oracle run was still active when this plan was archived. Its final result is therefore not claimed here.

### Remaining Concern

The deprecated repository wrappers remain for compatibility with existing tests and callers outside the migrated API path. Phase 3.2 should remove them after the worker profile and shared dispatch path use `JobRunStore` exclusively. The root CLI integration suite should also be rerun to completion on a runner with the required database containers.
