# Implementation Plan: Per-Run Replication Diagnostics with Bounded Logs

## Task Source

This plan implements the agreed follow-up to Phase 4. There is no JIRA ticket. The managed server currently persists only a generic message when `ReplicaDB.processReplica()` returns exit code `1`, even though the core logs the underlying failure. The goal is to expose a safe, detailed, per-run diagnostic log without changing the standalone CLI contract.

## Overview

Managed runs will capture INFO, DEBUG, WARN, and ERROR events for their own execution, including exception stacktraces, up to 256 KiB. A hybrid design combines an explicit core collector for reliable failure context with a Log4j appender for the existing emitted log stream. Logs are redacted before persistence and are stored in a dedicated `run_log` table, while `JobRun.errorMessage` remains a short summary for lists and status responses.

The standalone CLI keeps exit codes `0`, `1`, and `2`, existing logging behavior, options-file parsing, and Spring-free packaging. Detailed per-run persistence is a managed-server capability only.

## Architecture and Design

### Agreed approach: hybrid collector and Log4j appender

```text
managed run
    |
    v
RunExecutionContext + runId + diagnostic collector
    |                         |
    | explicit stage/error    | Log4j events with run context
    v                         v
safe fallback             bounded per-run appender
    \                         /
     v                       v
       redaction + 256 KiB truncation
                    |
                    v
                 run_log
                    |
                    v
          GET /api/v1/runs/{id}/log
                    |
                    v
             frontend log viewer
```

The appender must filter by a run identifier and must never capture the global process log as one run. The managed execution boundary sets and clears the logging context. Context propagation must cover the core's parallel task threads. The explicit collector remains the fallback when a framework or executor path does not propagate logging context.

### Security and retention rules

- Redact passwords, usernames, tokens, secrets, URI user-info, certificates, private keys, and sensitive connection parameters before storing or returning logs.
- Capture the log content and stacktraces, but never persist `ToolOptions`, decrypted datasource maps, master-key data, lease tokens, or arbitrary request bodies.
- Limit persisted content to 256 KiB per run. Use a deterministic UTF-8 byte policy: retain the first 75% and last 25% of the budget, inserting `[TRUNCATED: middle omitted]` between them when content exceeds the budget. The marker and valid UTF-8 boundary are included in the budget.
- Persist whether truncation occurred and the captured byte/character count.
- Keep `JobRun.errorMessage` bounded and summary-oriented; the detailed log is read through the dedicated endpoint.
- Do not change standalone CLI output, parser behavior, environment expansion, or exit codes.

## Implementation Tasks

### 1. Core diagnostic contract

- [x] **1.1 Add the bounded diagnostic event and collector contract**
  Files: `src/main/java/org/replicadb/execution/ReplicationDiagnosticEvent.java` (new), `src/main/java/org/replicadb/execution/ReplicationDiagnosticCollector.java` (new), `src/main/java/org/replicadb/execution/ReplicationExecutionContext.java`
  Changes: Define stable stage/category/severity values, event fields for timestamp, task id, component, safe message, throwable summary, and stacktrace text, plus a collector interface that is independent of Spring, PostgreSQL, and HTTP. Add a bounded collector with deterministic truncation and immutable snapshot access. Keep the existing context getters and cancellation behavior compatible.
  Tests: Verify event validation, stage/category values, ordering, 256 KiB bound, truncation metadata, concurrent writes from multiple tasks, stacktrace capture, null/blank handling, and collector snapshot immutability.
  Dependencies: None

### 2. Core failure capture

- [x] **2.1 Capture explicit diagnostics at every replication stage**
  Files: `src/main/java/org/replicadb/ReplicaTask.java`, `src/main/java/org/replicadb/ReplicaDB.java`, `src/main/java/org/replicadb/manager/ConnManager.java`, `src/main/java/org/replicadb/manager/SqlManager.java`
  Changes: Record redaction-ready diagnostic events for source connection, sink connection, source read, sink write, watermark resolution, pre-task, post-task, cancellation, interruption, validation, task aggregation, and cleanup failures. Preserve exception causes for the collector, retain existing log messages, and keep return codes unchanged. Record multi-task failures without exposing credentials or full options.
  Tests: Use existing manager stubs to assert diagnostics for source/sink connection failures, missing source table, sink write failure, post-task/merge failure, watermark failure, cancellation, interrupted execution, validation failure, cleanup failure, and multiple parallel task failures. Assert `processReplica()` still returns `0`, `1`, or `2` as before.
  Dependencies: Task 1.1

### 3. Core logging context boundary

- [x] **3.1 Add run-scoped logging context hooks without changing CLI behavior**
  Files: `src/main/java/org/replicadb/execution/ReplicationLogContext.java` (new), `src/main/java/org/replicadb/ReplicaDB.java`, `src/main/java/org/replicadb/ReplicaTask.java`, `src/test/java/org/replicadb/execution/ReplicationLogContextTest.java` (new)
  Changes: Use the core's existing Log4j dependency to define a context hook that associates the execution context run id with `ThreadContext`, restores or clears it in `finally`, and provides a no-op path for standalone CLI use. Capture the parent context map before task submission and explicitly install/clear that snapshot at the start/end of every `ReplicaTask`; do not rely on implicit inheritance from executor threads. This adds no server dependency to the root artifact and does not alter CLI parsing or output.
  Tests: Verify set/restore/clear behavior, nested executions, explicit propagation into core-created executor tasks, absence of context leakage between sequential runs, standalone CLI no-op behavior, and no change to existing CLI log output or classpath dependencies.
  Dependencies: Task 1.1

### 4. Managed Log4j capture

- [x] **4.1 Implement an isolated bounded Log4j appender for managed runs**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/RunLogCaptureAppender.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/execution/RunLogCaptureRegistry.java` (new), `replicadb-server/src/main/resources/log4j2-spring.xml` or managed logging configuration, `replicadb-server/src/test/java/org/replicadb/server/job/execution/RunLogCaptureAppenderTest.java` (new)
  Changes: Capture Log4j events only when a registered run id is present, include formatted message and throwable stacktrace, preserve event order under concurrency, redact each event, enforce the 256 KiB UTF-8 policy, and avoid capturing unrelated server logs. Create a server-only `log4j2-spring.xml` and select it through `logging.config` in the API and worker profiles; the root artifact must continue using its existing logging configuration and must not load the server appender. Make registry registration/removal atomic: once capture closes, late events are dropped rather than reassigned or sent to another run, and no registry entry is retained.
  Tests: Prove run isolation with concurrent runs, DEBUG/INFO/WARN/ERROR capture, throwable stacktraces, event ordering, first-75%/last-25% truncation and marker behavior, registry cleanup, unregistered-event exclusion, late-event dropping, appender failure isolation, and no plaintext credential retention after redaction.
  Dependencies: Tasks 1.1 and 3.1

### 5. Run log persistence model

- [x] **5.1 Add the managed run-log domain and persistence port**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/domain/RunLog.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/port/RunLogStore.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunController.java`, `replicadb-server/src/main/java/org/replicadb/server/job/api/RunLogResponse.java` (new)
  Changes: Model run id, bounded content, truncation flag, captured size, capture timestamps, and format version. Define repository operations for replace-once terminal persistence and safe read. Replace the existing nested `JobRunController.RunLogResponse(UUID runId, String excerpt)` with the new response type and fields in one coordinated change. Keep detailed logs separate from `JobRun` and exclude log contents from list responses.
  Tests: Validate model bounds, required identifiers, empty successful logs, truncated logs and marker metadata, immutable content, response serialization, migration-compatible byte limits, and absence of lease tokens, datasource credentials, key references, and arbitrary sensitive fields.
  Dependencies: Task 1.1

### 6. Database schema

- [x] **6.1 Add the `run_log` Flyway migration and repository adapter**
  Files: `replicadb-server/src/main/resources/db/migration/V20__create_run_log.sql` (new), `replicadb-server/src/main/java/org/replicadb/server/job/persistence/RunLogRepository.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/persistence/RunLogRepositoryIT.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java`
  Changes: Create `run_log` with run UUID primary key, bounded text content, truncation flag, captured size, format version, captured and updated timestamps, and `ON DELETE CASCADE` foreign key to `job_run`; run-log retention follows run retention. Add parameterized SQL for safe replace/read and enforce the size bound in application and database constraints. Keep Flyway ordering and fixture setup deterministic.
  Tests: Run V1-V20 against PostgreSQL, verify columns, constraints, indexes, size checks, insert/update/read behavior, duplicate replacement semantics, missing-run rejection, rollback behavior, and no plaintext credentials in stored content. Confirm old migrations remain unchanged.
  Dependencies: Task 5.1

### 7. Managed execution capture lifecycle

- [x] **7.1 Open, collect, and close capture around every managed execution**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/JobExecutionService.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/RunExecutionCoordinator.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerDispatchCoordinator.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/RunLogCaptureRegistry.java`
  Changes: Register the run before datasource materialization/core execution, bind the run logging context, merge explicit collector events with captured Log4j events, persist the final bounded log after terminal outcome, and always unregister/clear in `finally`. Preserve the existing temporary-file cleanup path on all success, exception, cancellation, and fencing outcomes. Persist decryption/materialization failures and fenced outcomes without logging secrets. Keep active cancellation, heartbeat, fencing, retry, and cleanup semantics intact.
  Tests: Cover successful, failed, cancelled, retry, fenced, decryption-failure, builder-failure, and worker-loss executions. Assert logs persist on every terminal path, run isolation holds, no capture remains registered, temporary files are cleaned after failure, active cancellation still works, and no database lock is held during core execution.
  Dependencies: Tasks 2.1, 3.1, 4.1, 5.1, and 6.1

### 8. Better summary error selection

- [x] **8.1 Replace the generic non-zero exit message with a safe summary**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/JobExecutionService.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/JobExecutionServiceIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/JobExecutionServiceTest.java` (new if needed)
  Changes: When `processReplica()` returns `1`, derive `JobRun.errorMessage` from the first meaningful explicit diagnostic or a bounded safe fallback, including stage/component and a short redacted detail. Keep the detailed log in `run_log`. Preserve the current generic message only when no diagnostic exists. Do not expose raw datasource credentials or unbounded exception text.
  Tests: Assert useful summaries for source table missing, sink write failure, connection failure, multi-task failure, empty diagnostic fallback, redaction, truncation, and exit code `2` cancellation behavior.
  Dependencies: Tasks 2.1, 5.1, 6.1, and 7.1

### 9. Run log API

- [x] **9.1 Expose the bounded detailed log through the runs API**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunController.java`, `replicadb-server/src/main/java/org/replicadb/server/job/api/RunLogResponse.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunControllerTest.java`, `replicadb-server/src/test/java/org/replicadb/server/OpenApiSpecificationIT.java`
  Changes: Return persisted run-log content, truncation metadata, captured size, and format information from `GET /api/v1/runs/{id}/log`. Keep job-level VIEW authorization, return an empty safe response when no log exists, and return RFC 7807 errors for missing or unauthorized runs. Never dynamically include credentials, internal key references, or lease tokens.
  Tests: MockMvc tests for success/failure/cancelled runs, absent logs, truncated logs, authorization, 404, response size, redaction, content type, and generated OpenAPI schema.
  Dependencies: Tasks 5.1, 6.1, 7.1, and 8.1

### 10. Frontend API contract

- [x] **10.1 Add typed run-log metadata and client handling**
  Files: `replicadb-server/frontend/src/api/schema.ts` (generated), `replicadb-server/frontend/src/api/runsApi.ts`, `replicadb-server/frontend/src/api/runsApi.test.ts`, `replicadb-server/frontend/src/api/schema.test.ts`, `replicadb-server/frontend/scripts/generate-api-types.mjs`
  Changes: Regenerate types from the live OpenAPI contract and model log content, truncation, captured size, and format metadata. Keep lease tokens, credentials, encrypted envelopes, and server-only fields out of frontend state. Preserve query keys, error mapping, and cache invalidation behavior.
  Tests: Validate schema drift, response parsing, empty logs, truncation metadata, RFC 7807 failures, cache keys, and absence of secret-shaped fields.
  Dependencies: Task 9.1

### 11. Frontend detailed log viewer

- [x] **11.1 Render full bounded logs and stacktraces clearly in run details**
  Files: `replicadb-server/frontend/src/pages/RunDetailPage.tsx`, `replicadb-server/frontend/src/pages/RunDetailPage.test.tsx`, `replicadb-server/frontend/src/components/RunLogViewer.tsx` (new), `replicadb-server/frontend/src/components/RunLogViewer.test.tsx` (new)
  Changes: Replace the current single-line log display with a readable bounded viewer that preserves whitespace, wraps long lines, shows captured size and truncation state, separates summary from detailed log, and handles loading, empty, API error, and large-content states. Keep responsive layout and do not add secret readback or client-side persistence.
  Tests: Render multiline logs, nested stacktraces, DEBUG lines, truncation warning, empty success, loading/error states, long unbroken lines, mobile layout constraints, and assert no credential values are rendered when API data is redacted.
  Dependencies: Task 10.1

### 12. Documentation and operations

- [x] **12.1 Document bounded diagnostic logs and redaction boundaries**
  Files: `DEPLOYMENT.md`, `README.md`, `replicadb-server/README.md` if present, `replicadb-server/src/main/resources/application.yml`, `replicadb-server/src/main/resources/application-api.yml`, `replicadb-server/src/main/resources/application-worker.yml`
  Changes: Document the 256 KiB per-run limit, DEBUG capture behavior, `run_log` retention/cleanup, API authorization, truncation semantics, stacktrace handling, redaction guarantees, database backup impact, and the distinction between managed run logs and standalone CLI logs. Document that detailed logs may contain SQL/object names and must be treated as operational data.
  Tests: Run documentation/security scans, verify no credential examples, check configuration consistency between API and worker profiles, and run `git diff --check`.
  Dependencies: Tasks 6.1, 9.1, and 11.1

### 13. Run-log retention

- [x] **13.1 Add bounded retention and cleanup for persisted run logs**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/RunLogRetentionTask.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/config/RunLogRetentionConfiguration.java` (new), `replicadb-server/src/main/resources/application.yml`, `replicadb-server/src/main/resources/application-api.yml`, `replicadb-server/src/main/resources/application-worker.yml`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/RunLogRetentionTaskTest.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/persistence/RunLogRepositoryIT.java`
  Changes: Define an explicit configurable retention period for `run_log`, defaulting to the existing managed run retention policy, and delete only logs whose associated runs are already outside retention. Ensure cleanup is bounded per invocation, safe to retry, disabled in worker-only profiles when appropriate, and cannot delete logs for active or recent runs. Document the operational setting and backup/restore implications.
  Tests: Verify default and configured retention, active/recent run protection, batch limits, idempotent cleanup, database failure handling, API/worker profile behavior, and no deletion of a run log before its run is eligible for retention.
  Dependencies: Task 6.1

### 14. Security and redaction hardening

- [x] **14.1 Prove logs cannot persist or return sensitive material**
  Files: `src/test/java/org/replicadb/config/CredentialRedactorTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/RunLogCaptureAppenderTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/RunLogRepositoryIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunControllerTest.java`
  Changes: Extend redaction only where concrete log formats require it, without weakening existing rules. Add a reusable sensitive-data corpus using generated values and verify redaction before collector storage, appender storage, PostgreSQL parameters, API responses, audit events, and exception output. Add bounded cleanup and memory-retention assertions.
  Tests: Cover JDBC parameters, URL user-info, query parameters, Kafka/S3/Azure keys, Mongo URIs, certificates/private keys, `${...}` literals, nested causes, concurrent runs, and truncation boundaries. Assert generated test secrets never appear in persisted or returned content.
  Dependencies: Tasks 2.1, 4.1, 6.1, 7.1, and 9.1

### 15. Compatibility and acceptance harness

- [x] **15.1 Extend acceptance gates for detailed managed diagnostics**
  Files: `scripts/phase4-acceptance.sh`, `scripts/phase3-compose-smoke.sh`, `scripts/phase3-load-test.sh`, `replicadb-server/frontend/e2e/admin-management.spec.ts`, `replicadb-server/frontend/e2e/datasource-management.spec.ts`, `replicadb-server/frontend/e2e/run-log-diagnostics.spec.ts` (new), `src/test/java/org/replicadb/CliOfflineExecutionTest.java`, `src/test/java/org/replicadb/ReplicaDBMultipleTablesTest.java`
  Changes: Add deterministic failure scenarios for missing source/destination tables, invalid connection profiles, multi-task failures, stacktrace persistence, truncation, and redaction. Verify detailed logs are isolated per run, visible through the UI, and absent from notifications/metrics/audit data. Keep CLI compatibility gates unchanged in meaning and prove the root artifact remains Spring-free.
  Tests: Run focused core/server/frontend tests, full server suite, frontend unit/build, authenticated Playwright log viewer flow, Compose smoke/load/resilience/worker-loss/fairness gates, packaged CLI offline execution, OpenAPI drift, security scans, and `git diff --check`. Use database-visible barriers and no fixed sleeps for state inference.
  Dependencies: Tasks 8.1, 9.1, 11.1, 12.1, 13.1, and 14.1

### 16. Final review and evidence

- [x] **16.1 Validate the complete detailed-error lifecycle and record evidence**
  Files: `implementation_plan.md`, `ARCHITECTURE_DECISIONS.md`, `DEPLOYMENT.md`, `README.md`, all task test reports and acceptance outputs
  Changes: Review the implementation against every acceptance criterion: full per-run log capture, stacktraces, DEBUG inclusion, 256 KiB bound, truncation metadata, run isolation, redaction, PostgreSQL persistence, API authorization, frontend rendering, CLI compatibility, cancellation, retry, fencing, worker execution, and cleanup. Record any residual limitations without weakening the security boundary.
  Tests: Run the complete acceptance harness with Java 17, Docker, PostgreSQL, frontend build tooling, authenticated browser flow, and packaged CLI checks. Confirm no acceptance process or temporary secret material remains after cleanup.
  Dependencies: Tasks 1.1 through 15.1

## Technical Reference

### Proposed run-log persistence contract

```text
run_log
  run_id UUID PRIMARY KEY REFERENCES job_run(id)
  content TEXT NOT NULL
  truncated BOOLEAN NOT NULL DEFAULT FALSE
  captured_size INTEGER NOT NULL
  format_version INTEGER NOT NULL
  captured_at TIMESTAMPTZ NOT NULL
  updated_at TIMESTAMPTZ NOT NULL
```

The API response should contain only safe log content and metadata:

```text
RunLogResponse
  runId
  content
  truncated
  capturedSize
  formatVersion
  capturedAt
  updatedAt
```

### Capture semantics

- Capture events for the managed run only, identified by the run-scoped logging context.
- Include the formatted Log4j message and throwable stacktrace.
- Merge explicit collector events with appender output deterministically.
- Apply `CredentialRedactor` before any bounded buffer is persisted.
- Enforce 256 KiB after redaction and use a deterministic truncation marker.
- Clear registry entries and thread context in all normal, exceptional, cancelled, and fenced paths.

### Acceptance criteria

- A failed managed run exposes a useful summary instead of only `ReplicaDB execution failed for run ...`.
- The detailed endpoint and frontend show the complete captured bounded log, including stacktraces and DEBUG events when enabled.
- Logs from concurrent runs never mix.
- Logs are persisted in `run_log` and include truncation metadata when the 256 KiB limit is reached.
- No password, token, username, URI credential, certificate, private key, encrypted bundle, master key, lease token, or datasource security map appears in storage, API output, audit data, notifications, metrics, or frontend state.
- Success, failure, cancellation, retry, decryption failure, worker loss, fencing, and cleanup paths all finalize capture correctly.
- Standalone CLI behavior and exit codes remain unchanged and the root artifact remains Spring-free.

### Known risks and mitigations

- **Context propagation failure:** explicit collector events remain available as a fallback; tests cover core-created task pools and concurrent runs.
- **Log leakage:** redaction occurs before storage and API response, with generated corpus tests and bounded exception handling.
- **Database growth:** one bounded row per run, explicit truncation metadata, retention/cleanup documentation, and repository cleanup tests.
- **Performance:** synchronized/atomic bounded append operations, no database writes per event, and one terminal persistence operation.
- **Sensitive SQL text:** SQL/object names may remain operationally useful but are redacted where credential-shaped; documentation labels logs as sensitive operational data.

## Test and Validation Strategy

| Layer | Required evidence |
| --- | --- |
| Core collector | Events, stage classification, stacktraces, concurrency, 256 KiB truncation |
| Core compatibility | Exit codes `0`/`1`/`2`, CLI output, multi-table behavior, Spring-free artifact |
| Log4j capture | Run isolation, DEBUG inclusion, context propagation, cleanup, appender failure isolation |
| Persistence | V20 migration, constraints, replace/read behavior, no plaintext secrets |
| Managed execution | All terminal paths, retry, cancellation, fencing, worker loss, no locks during execution |
| API | Authorized detailed reads, empty/truncated responses, OpenAPI, RFC 7807 errors |
| Frontend | Multiline/stacktrace viewer, truncation state, loading/error/empty/responsive behavior |
| Security | Generated corpus scans across collector, DB, API, audit, notifications, metrics, and UI |
| Acceptance | Compose, load, resilience, fairness, worker loss, Playwright, packaged CLI, docs, cleanup |

## Quality Gate Notes

The central design decision is to capture the managed run's own logging stream rather than the process-wide log file. The appender must never become a cross-run log sink. The explicit collector and a safe summary are required fallbacks so a logging-context failure cannot erase the diagnostic reason. No task may weaken credential redaction or add a managed dependency to the standalone CLI artifact.

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy
- Tasks completed as planned: 16/16 (100%)
- Tasks that required plan adjustment: 2/16 (12.5%)
- Test loop iterations: 4 total (first-pass 2, second-pass 2, third-pass 0)

### Gaps Encountered

#### Gap 1: Durable and core run identifiers differ (Plan-to-Implementation)
- **Task**: 7.1 — Open, collect, and close capture around every managed execution
- **Plan assumed**: The managed run identifier could directly identify the core logging context.
- **Reality**: `ToolOptions` creates a separate core execution identifier.
- **Resolution**: Registered the durable run id for pre-core events and aliased the core context id to the same capture before core execution.
- **Learning**: Trace identifier ownership across module boundaries before designing context-keyed capture.

#### Gap 2: Redaction corpus required PEM and generic token handling (Intent-to-Plan)
- **Task**: 14.1 — Prove logs cannot persist or return sensitive material
- **Plan assumed**: Existing message redaction covered all required log formats.
- **Reality**: Generic `token=` values, environment placeholders, and PEM markers were not covered.
- **Resolution**: Extended the shared redactor and added corpus assertions for those concrete formats.
- **Learning**: Security acceptance corpora should include marker-only and incomplete-secret representations.

### Patterns Discovered
- Run-scoped Log4j capture uses an atomic registry entry plus explicit MDC propagation.
- Managed detailed logs are persisted through a dedicated port and table rather than embedded in `JobRun`.

### Validation Notes
- Passed focused core, server, PostgreSQL Flyway, managed execution, MockMvc, security, frontend Vitest, typecheck, package, build, documentation, and diff checks.
- Full Compose/load/resilience/worker-loss and authenticated Playwright acceptance flows were not run in this session and remain residual validation work.
