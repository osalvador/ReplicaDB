# Architecture Decisions and Evolution Strategy

## Executive Summary

ReplicaDB is currently a Java 17 command-line tool for heterogeneous batch data replication. The existing core owns database-specific connections, partitioning, type handling, staging, and sink lifecycle behavior.

The approved direction is to evolve the product into an operable job platform while preserving the CLI. The first product step is a control plane with API-managed jobs, scheduling, monitoring, durable execution state, and incremental watermarks.

ReplicaDB is not being extended into a CDC, ETL, schema-migration, or universal data-reconciliation platform. The control plane manages and observes the batch engine; it does not move database-specific behavior into generic orchestration code.

The control plane does not resume interrupted work. A run either completes or is executed again from the beginning. Safety comes from the staging-based replication modes, not from progress checkpoints.

**Date**: August 13, 2026
**Last decision review**: August 25, 2026
**Status**: Approved direction; Phase 0-a, Phase 0-b1, Phase 0-b2, Phase 1a (artifact split), Phase 1b (state layer), Phase 1c-1 (REST API core), Phase 1c-2 (scheduler), Phase 1c-3a+b+c (authentication, global roles, per-job ACLs, audit events, retention, and persisted cancellation warnings), Phase 2a/2b/2c (frontend authentication, monitoring, job actions, scheduling, user administration, and job permissions), Phase 3.1 (distributed state contract, leases, retries, and fencing), Phase 3.2 (worker runtime and PostgreSQL dispatch), Phase 3.3 (API high availability, shared throttling, observability, packaging, and process validation), and Phase 3.4 (hybrid worker load distribution and standalone CLI compatibility validation) implemented and validated; Phase 4 (reusable managed datasources) approved for planning
**Owner**: Development Team

---

## Core Decisions

### Decision 1: Single Codebase, Two Artifacts, CLI Compatibility

**Status**: APPROVED

ReplicaDB will remain a single codebase. The current CLI is the compatibility baseline. No separate `replicadb-core` project is extracted.

The build produces **two artifacts from the same sources**:

- `replicadb` — the existing CLI assembly. Spring Boot is never on its classpath.
- `replicadb-server` — the managed runtime, started with the `api` or `worker` profile.

A single fat artifact carrying Spring Boot into every CLI installation is rejected. It would change footprint, startup time, and dependency surface for users who never adopt the control plane, which contradicts the compatibility contract below.

This preserves:

- One implementation of manager dispatch, type mapping, staging, and lifecycle behavior.
- Existing CLI arguments, options files, launcher scripts, and exit codes.
- A gradual migration path from standalone execution to managed jobs.

The standalone `replicadb` CLI remains free of Spring Boot and control-plane dependencies, while the sibling `replicadb-server` module now provides the REST API, Quartz scheduler, and frontend under the `api` profile. Distributed workers and the `worker` profile are the Phase 3 deployment capabilities.

```text
Current:
  CLI -> ToolOptions -> ReplicaDB core -> source/sink managers

Target:
  replicadb        (CLI)    ---------------------------> ReplicaDB core
  replicadb-server (api)    -> JobDefinition -> Executor -> ReplicaDB core
  replicadb-server (worker) -> JobRun -------> Executor -> ReplicaDB core
```

`ToolOptions` remains the configuration boundary for the existing core. The server translates a stored job definition into `ToolOptions` rather than duplicating manager behavior.

PostgreSQL is required only by the managed `api` and `worker` profiles for control-plane metadata, scheduling, leases, watermarks, and worker coordination. The standalone CLI does not require it and remains executable with its existing source and sink configuration.

#### CLI compatibility contract

The CLI is a permanent execution mode across all implementation phases and deployment models. "Compatibility" is a verifiable contract, not an aspiration. A change breaks it if it alters any of:

- Accepted arguments, options-file keys, and their semantics.
- Process exit codes.
- The ability to run with no metadata database reachable.
- The `replicadb` artifact's classpath gaining a Spring Boot application context.

#### Multi-table replication remains CLI-only

`ReplicaDB.executeMultipleReplications` iterates a list of `ReplicationTable` entries sequentially and stops at the first failure, leaving earlier tables already applied. This partial-application outcome is acceptable for an interactive CLI invocation, but it is not a state model the control plane will manage: a single run identifier cannot honestly report "three of seven tables replicated", and retrying it would re-truncate tables that already succeeded.

Therefore a managed `JobDefinition` targets **exactly one source/sink table pair**. Scheduling N tables means N job definitions, each with its own run history, watermark, and permissions. Options files containing a multi-table replication list remain fully supported through the CLI and are rejected by the API with an explicit validation error.

### Decision 2: Monolithic Control Plane First

**Status**: APPROVED

The first evolution step is a monolithic control plane around the existing batch engine. It should provide value in a single-instance deployment before introducing distributed workers.

The implementation target for Phase 1 is a Spring Boot application using the `api` profile. This profile starts the REST API and the scheduler in the same JVM. Spring Boot is not required by the existing CLI path.

#### Phase 1 scope

- REST API for job definitions and run management.
- Scheduler for recurring executions.
- Asynchronous execution so API requests do not block on replication.
- Durable job history, run state, and incremental watermarks.
- Monitoring of status, progress counters, timings, throughput, and errors.
- Existing CLI remains usable without starting the API runtime.

The API initially manages configuration and execution state. A future frontend can edit job definitions through the API without introducing a data transformation layer.

```text
                    +------------------+
                    | REST API         |
                    | Scheduler        |
                    +--------+---------+
                             |
                       JobDefinition
                             |
                             v
                    +------------------+          +---------------+
                                                  | Job Executor     +--------->+ State store   |
                                                  +--------+---------+          | Monitoring    |
                                                                       |                    +---------------+
                                                                       v
                                                  +------------------+
                                                  | ReplicaDB Core   |
                                                  +--------+---------+
                                                                       |
                                                        Source and sink
```

The monitoring transport is an implementation detail. REST polling is sufficient for the first version; a push channel can be added later if operational requirements justify it.

#### Run concurrency inside one JVM

Before Phase 0-a, the core kept process-global mutable state. `ConnManager.randomSinkStagingTableName` and `FileManager.tempFilesPath` were `static` and were reset at the start of every replication in `ReplicaDB.executeSingleReplication`. Two replications running concurrently in the same JVM could overwrite each other's staging table name and temporary file map, silently corrupting data or dropping another run's staging table.

```text
     Before Phase 0-a:
     JobRun A --+                     +-- reset global state --+
                         +---- same JVM ------+                       +--> collision
     JobRun B --+                     +-- reset global state --+
```

Phase 0-a, delivered in commit `c228ddc`, removes this static state and replaces it with a `ReplicationExecutionContext` owned by each `ToolOptions` instance. All managers and tasks created from one options instance share that context, while `ToolOptions.forReplicationTable(...)` creates a fresh context for each table. Generated staging names and temporary-file paths are therefore isolated across runs and remain shared within the parallel tasks of one run.

The context also provides a run identifier and a concurrent temporary-file map. Generated staging-name creation is synchronized per context, so parallel managers cannot initialize different names for the same run. The static-state reason for forcing managed execution concurrency to one is resolved by Phase 0-a, and the cancellation and watermark work required by the managed execution contract is implemented in Phase 0-b1 and Phase 0-b2.

#### Replication mode guidance for managed jobs

Because runs are never resumed (Decision 3), the safety of a retry depends entirely on the replication mode:

| Mode | Sink during the run | Full re-execution | Commit point |
|---|---|---|---|
| `complete` | mutated from the start (`TRUNCATE` then direct writes) | destructive | none |
| `complete-atomic` | untouched until the final swap | safe | `atomicInsertStagingTable()` |
| `incremental` | untouched until the merge | safe when the merge is an idempotent upsert by primary key | `mergeStagingTable()` |

`complete-atomic` is the **recommended mode for managed jobs** and the API surfaces that recommendation when a definition is created or updated. `complete` remains available, but the API must return an explicit warning stating that an interrupted or retried run leaves the sink truncated or partially loaded, and that warning must be persisted on the job definition so it is visible in the UI. Implemented in Phase 1c-1 at the API-response level: `JobDefinitionResponse.modeWarning` is computed dynamically from the definition's `mode` at read time rather than stored as a separate `job_definition` column — a deliberate deviation from the literal "persisted on the job definition" wording, since the warning is entirely derivable from the already-persisted `mode` and storing it redundantly would have required a breaking change to the Phase 1b `JobDefinition` record's positional constructor across roughly ten existing call sites.

#### State storage

PostgreSQL is the mandatory state store for the managed `api` and `worker` profiles. It owns job definitions, runs, leases, watermarks, users, permissions, audit events, and scheduler coordination. In direct CLI mode the state store is not used at all. Sentry and application logs are telemetry, not the source of truth for job state. SQLite remains suitable for isolated CLI fixtures or unit tests, but it is not a supported control-plane deployment store.

Phase 1b implements the job-definition and job-run portion of this store: Flyway-versioned `job_definition`/`job_run` tables accessed through Spring JDBC repositories. Leases and heartbeats carry simple single-instance values until Phase 3's distributed-worker rules apply; users and permissions are implemented in Phase 1c-3a+b, and audit events are implemented in Phase 1c-3c. Phase 1c-1 adds the partial-unique-indexed `job_run` constraint enforcing one active run per job definition and the `run_trigger_idempotency` table for the `Idempotency-Key` replay rule. Phase 1c-2 adds the `job_schedule` table as the product-level durable source of truth for recurring schedules, plus an index on `(job_definition_id, created_at DESC)` for job run history queries. Phase 3 adds forward-only migrations for retry policy fields on `job_definition` and eligibility/fencing fields on `job_run`, and changes the API and scheduler dispatches to publish durable run identifiers transactionally.

### Decision 3: Durable State, No Resume, and Incremental Watermarks

**Status**: APPROVED

The control plane distinguishes a reusable job definition from an individual execution. There is no third level.

```text
JobDefinition
     configuration reference, schedule, mode,
     one source/sink table pair, optional watermark column
                    |
                    v
JobRun
     status, attempt, timestamps, row counters, error,
     executor identity, lease, heartbeat, committed watermark
```

A `Checkpoint` entity was considered and removed from the model. See "No resume" below.

#### Job run states

The initial state model supports:

- `PENDING`
- `RUNNING`
- `SUCCEEDED`
- `FAILED`
- `CANCEL_REQUESTED`
- `CANCELLED`
- `RETRY_SCHEDULED`

Retries and cancellation requests are state transitions. They must not be inferred from log messages.

Implemented in Phase 1b as the `JobRunStatus` enum, with `JobRunStateMachine` enforcing the transition table from Decision 3 (for example `FAILED -> RETRY_SCHEDULED`, never a transition back to `RUNNING` on the same row).

#### No resume

**Status**: APPROVED as an explicit non-feature.

An interrupted run is never continued. A retry executes the job from the beginning.

This follows from how the engine partitions work, not from a wish to simplify scheduling. Partition assignment is manager-specific and is not reproducible across executions:

```text
PostgreSQL   SELECT * FROM (source-query) as T1 OFFSET ? LIMIT ?
                               no ORDER BY, so the row-to-task assignment may differ
                               between executions even over identical data

Oracle       ... WHERE ora_hash(rowid, jobs-1) = ?
                               reproducible only while rowids remain stable
```

Skipping "already completed" partitions on a second execution would therefore drop rows silently on at least one supported source, with no error and no log entry. Correctness comes from the replication mode instead (see the mode table in Decision 2): `complete-atomic` and `incremental` leave the sink untouched until `postSinkTasks()`, so a full re-execution is safe.

The control plane persists row counters and timings for observability. It must never present them as resumable progress, and no API field may imply that a retry continues where a previous attempt stopped.

Phase 1b's `JobRunRepository.scheduleRetry(...)` implements this precisely: it transitions the failed row to `RETRY_SCHEDULED` and inserts a brand-new `PENDING` row referencing the failed run as `previousRunId` with `attempt` incremented — it never resets the original row back to `RUNNING`.

#### Lease recovery and automatic retries

Phase 3 applies the same no-resume rule to worker loss. A lease expiration never reopens the abandoned `JobRun` for execution. Recovery runs in one PostgreSQL transaction:

1. Lock an expired `RUNNING` row with `FOR UPDATE SKIP LOCKED` and verify that its lease is still expired using PostgreSQL `now()`.
2. Transition the abandoned row to `RETRY_SCHEDULED` and retain it as immutable history.
3. If the job's retry policy permits another attempt, insert a new `PENDING` row with `previousRunId`, an incremented `attempt`, and an `available_at` computed from the job's backoff policy.
4. If the attempt limit is exhausted, transition the abandoned row to `FAILED` and create no replacement run.

`JobDefinition` owns the automatic recovery policy: `maxAttempts`, `retryBackoffSeconds`, and `automaticRetryEnabled`. `maxAttempts` includes the initial attempt. Automatic recovery is enabled by default for `complete-atomic` and `incremental`, and disabled by default for `complete` because a full re-execution can truncate or partially rebuild the sink. A `complete` job may opt in explicitly, but its existing destructive-mode warning must remain visible in the API and frontend. These settings govern lease-expiry recovery; the existing manual retry endpoint remains available for ordinary `FAILED` runs.

Every claim creates a fresh opaque `leaseToken` and stores it on the `JobRun`. Heartbeat, cancellation, terminal-state, and watermark updates must include both the run identifier and the lease token in their conditional `UPDATE`. A worker that lost its lease may finish locally, but its late result must not change state, counters, errors, or watermarks. This fencing rule is required because PostgreSQL state and sink operations are at-least-once rather than exactly-once.

#### Incremental watermarks

Watermarks apply only to `incremental` mode and are deliberately minimal.

- The job definition declares **one source column** as the watermark column. Arbitrary expressions and multi-column watermarks are out of scope.
- The committed value is persisted as a `String` holding the highest-precision textual representation the source driver can produce. This keeps one storage format across every supported engine.
- The comparison type is **inferred from the source column metadata at run time** and used to bind the stored value back as a typed parameter. The value is never concatenated into SQL.
- The predicate is injected by the engine and composes with the existing `$CONDITIONS` partition substitution. A `source-query` may be used only if it exposes the declared watermark column.
- The initial value is explicit configuration. Absent it, the first run replicates everything the query returns.
- The boundary is `>` against the last committed value, so a run never re-reads the previous boundary row.
- Each parallel `ReplicaTask` reports its highest observed value; the executor reduces them to one candidate for the run.
- The candidate is committed **only after `mergeStagingTable()` succeeds**.
- A failed or cancelled run leaves the last committed watermark unchanged, so retrying cannot advance it twice.

Accepted limitations, which must be stated in user-facing documentation:

- **Deletes are never propagated.** The merge is an upsert by primary key.
- **Sources without primary keys cannot merge**, and the engine already warns about this.
- **Late-committing source transactions are lost.** A transaction whose timestamp precedes the committed watermark but which commits after the read will never be replicated. A configurable read lag subtracted from the committed value mitigates this; the default lag is `0` and must be tuned per source.
- No watermark is inferred from an arbitrary `source-query`. Without a declared column the job still runs as a batch job, but the control plane does not automate its watermark.

### Decision 4: API, Frontend, Scheduler, and Access Control

**Status**: APPROVED

The control plane exposes these operations:

- Create, update, disable, and delete a job definition.
- Trigger a run manually.
- Schedule recurring runs.
- Query the current run and historical runs.
- Request cancellation.
- Retry a failed run by re-executing its persisted definition.
- View watermarks, counters, timings, and failure details.
- Authenticate local users and manage their roles.
- Enforce permissions for each job through backend-controlled ACLs.
- Serve a small planning and monitoring frontend from the `api` profile.

The API must execute runs asynchronously. The current `ReplicaDB.processReplica(ToolOptions)` method returns a run-level success/error code and does not expose progress events, so the first adapter reports coarse-grained state until Phase 0 widens the task result.

#### API conventions

These are decided so that they are not re-litigated per endpoint:

- Base path is `/api/v1`. Breaking changes require a new major path segment.
- Errors use RFC 7807 `application/problem+json`, and never echo connection strings or credentials.
- Collection endpoints are paginated with `page` and `size`; default `size` is 50 and the maximum is 200.
- Triggering a run requires an `Idempotency-Key` header. A replay of the same key within 24 hours returns the originally created run instead of starting a second one.
- Session cookies are `HttpOnly`, `Secure`, `SameSite=Lax`, and every state-changing request carries a CSRF token.
- Authentication endpoints are rate limited to 5 failed attempts per 15 minutes, counted per account and per source address.

#### Credentials and secret references

The standalone CLI continues to use its existing environment expansion and options-file behavior. The managed server follows a different credential boundary after Phase 4: datasource credentials are submitted over the authenticated TLS API, encrypted by the application before insertion into PostgreSQL, and decrypted only in memory while an attempt is materialized through an additive `ToolOptionsBuilder` API in the root artifact. Managed datasource credentials do not require source or sink passwords to be present in process environment variables, and the managed server does not create an options file for them.

All credential-bearing values use the encrypted datasource security bundle, including JDBC passwords and users where configured, MongoDB connection strings containing URI credentials, S3 access/secret keys, Kafka SASL and keystore secrets, Azure service-principal secrets and certificate/key values, and sensitive arbitrary `*.connect.parameter.*` entries. `connectionParams` is reserved for non-secret technical values. Provider-prefixed references such as `${secret:<provider>/<path>#<key>}` remain reserved for the future Vault/CyberArk provider integration and are not accepted by the initial database-encrypted provider.

The initial managed provider uses application-level envelope encryption with AES-256-GCM. Each datasource security bundle has a fresh data-encryption key, a wrapped key, nonce, ciphertext, encryption algorithm version, and key version. PostgreSQL stores only the envelope and never receives a plaintext credential. The key-encryption key is supplied outside PostgreSQL through a Kubernetes/Docker Secret mounted as a file; it is not a source or sink credential environment variable. The server fails during startup when that key is absent, unreadable, incorrectly sized, or invalid. A provider interface is required so a future KMS, Vault, or CyberArk implementation can replace the initial file-backed key provider without changing job or CLI contracts. The managed execution path keeps decrypted values in memory and passes them directly to `ToolOptionsBuilder`; the existing options-file and redaction rules remain authoritative only for standalone CLI execution.

The current Phase 1b `${env:VARIABLE}` validation is historical managed-job behavior and is removed by the Phase 4 pre-production metadata reset. After the reset, managed API responses, audit events, notifications, metrics, exceptions, and worker payloads never contain password values, key references, resolved credentials, or encrypted bundle contents. Key rotation is performed by re-encrypting bundles under a new key version without exporting plaintext.

#### Identity and permissions

The first identity model uses local users stored in PostgreSQL. `ADMIN`, `OPERATOR`, and `VIEWER` are global roles, while job-level ACLs grant `VIEW`, `EDIT`, `EXECUTE`, and `CANCEL` permissions. The backend is the authority for these permissions; hiding a button in the frontend is not a security control.

Phase 3 makes login throttling shared across API instances by persisting the account/source-address failure window in PostgreSQL. No authentication decision may depend on an API instance's local heap when the API is deployed as a cluster.

### Decision 5: Immediate Cancellation

**Status**: APPROVED

`POST /api/v1/runs/{id}/cancel` stops the replication as soon as it is invoked. Cancellation is immediate and unconditional: it does not wait for a partition boundary, a batch commit, or any other safe point.

The core had no cancellation support before Phase 0-b1: there were no interrupt checks in the copy path, no access to the active statements, and `ReplicaDB.executeReplicationTasks` blocked on `invokeAll` until every task finished. Phase 0 added:

- A per-run cancellation token carried by the execution context.
- Access to the active `Statement` of each `ReplicaTask` so a control thread can call `Statement.cancel()`. `Future.cancel(true)` alone is insufficient, because a thread blocked inside a JDBC call does not observe interruption.
- Interrupt checks in the row-copy loop so a task exits promptly and still runs its cleanup.
- Replacement of the blocking `invokeAll` pattern with individually cancellable futures.

Cancellation also interrupts `mergeStagingTable()` and `atomicInsertStagingTable()` when they are already running. The endpoint's contract is obedience, not consistency.

Consequently the API response to a cancellation request must **explicitly warn that the sink may be left in an indeterminate state**. The warning is returned by the API and recorded on the run; it is not merely a frontend message. Its severity depends on the mode and on when cancellation arrives:

| Mode | Cancelled before `postSinkTasks()` | Cancelled during merge or swap |
|---|---|---|
| `incremental` | sink intact, staging discarded | partially merged rows, watermark not advanced |
| `complete-atomic` | sink intact, staging discarded | swap interrupted, sink indeterminate |
| `complete` | sink already truncated or partially loaded | not applicable |

After cancellation the engine runs its normal cleanup path and drops the staging table when it was auto-generated. A user-provided `sink-staging-table` is left untouched, consistent with existing behavior.

A cancelled run never advances the watermark and terminates in `CANCELLED`, never in `FAILED`.

This decision's core-side plumbing is implemented in Phase 0-b1 (commit `4dd4cb5`). The `/api/v1/runs/{id}/cancel` endpoint itself is implemented in Phase 1c-1: it delivers the cancellation signal to the running `ReplicationExecutionContext` synchronously before persisting any state change, and its response always carries the per-mode warning text described above. Phase 1c-3c also persists the same warning on the `job_run` row atomically with the cancellation request, preserving it through the executor's terminal transition. SQL Server `BulkCopy` and PostgreSQL `COPY` cancellation remain best-effort rather than immediate.

In Phase 3, cancellation is no longer dependent on the API instance that accepted the request. The API persists `CANCEL_REQUESTED` and publishes a run-control notification; the owning worker listens for it and requests cancellation on its local execution context. Polling of persisted state remains the recovery path if that notification is missed. The same fencing rule applies to the worker's terminal `CANCELLED` update.

### Decision 6: PostgreSQL Worker Dispatch

**Status**: APPROVED FOR PHASE 3

Distributed workers are introduced as a third phase, after the monolithic control plane (Phase 1) and the frontend (Phase 2), when a single control-plane instance cannot provide the required concurrency or isolation. PostgreSQL is both the durable source of truth and the dispatch coordination point. `LISTEN/NOTIFY` wakes workers quickly, while periodic polling is the mandatory recovery path for missed notifications.

```text
                         +----------------------+
                         | PostgreSQL state     |
                         | definitions          |
                         | runs / leases        |
                         | watermarks           |
                         | users / permissions  |
                         +----------+-----------+
                                    ^
                                    |
                      API + Quartz  |  Worker updates
                                    |
                         +----------+-----------+
                         | JobRun(PENDING)      |
                         +----------+-----------+
                                    |
                       LISTEN/NOTIFY + polling
                                    |
                         +----------v-----------+
                         | Worker execution     |
                         | claim -> ToolOptions |
                         | -> ReplicaDB core    |
                         +----------------------+
```

The distributed contract is:

- Multiple API instances share PostgreSQL as the only source of truth; no API-local run registry is authoritative.
- The API or Quartz inserts the `JobRun` and issues `pg_notify('replicadb_runs', run_id)` in the same PostgreSQL transaction.
- The notification payload contains only the durable `JobRun` identifier; never credentials or a complete configuration.
- Every worker receives the signal, but workers compete to claim work using PostgreSQL row locking.
- Workers load the job definition and last committed watermark from PostgreSQL.
- A worker claims one run at a time by default.
- Worker loss is recovered through leases, heartbeats, and polling of expired or retryable runs.
- A worker profile exposes no public API, frontend, Spring Security session, or Quartz scheduler. It starts only the shared repositories, dispatch coordinator, execution service, and core engine.
- The `api` profile uses the PostgreSQL-backed clustered Quartz store; preventing duplicate schedule firings across multiple API instances is covered by Phase 3.3 Quartz JDBC clustering.
- Duplicate notifications and duplicate polling must be safe under the claim, sink idempotency, and watermark commit rules.
- The API reads status only from PostgreSQL.

`LISTEN/NOTIFY` provides no acknowledgements, replay, consumer groups, or durable per-worker delivery: it is a wake-up signal, not a work queue. The durable work item is always the `JobRun` row, so periodic polling is the mandatory recovery path and notification delivery is only an optimization. An external broker is deliberately excluded from this phase.

#### Hybrid worker load distribution

**Status**: IMPLEMENTED and validated on August 26, 2026. Phase 3.4 is complete.

The worker fleet should distribute load as uniformly as practical without introducing a central dispatcher, a worker registry, or an external broker. The target is approximate fairness with bounded coordination overhead. PostgreSQL remains the only arbiter of ownership, and the existing lease, `FOR UPDATE SKIP LOCKED`, and fencing contracts do not change.

The dispatch policy has two lanes:

1. **Directed notification lane.** A `replicadb_runs` notification gives each worker at most one directed claim opportunity for the advertised `runId`. Duplicate notifications for the same run are coalesced locally. The worker calls `claimRequested(runId)` only when it has local capacity. If that claim is empty because another worker won the race or the run is no longer eligible, the worker may make exactly one immediate `claimNextEligible()` fallback. A fallback never chains into another fallback. The notification itself does not fan out into one directed attempt per free slot.
2. **Generic refill lane.** Startup, listener reconnect, periodic polling, and run completion create refill opportunities for currently free slots. A refill may create at most one generic claim opportunity per free slot, using `claimNextEligible()`. These opportunities are coalesced and bounded; they do not prefetch or hold `JobRun` rows outside a PostgreSQL claim.

The local admission rules are:

- The capacity permit is acquired immediately before a claim opportunity is executed and remains held for the claimed run's coordination and execution lifetime. Jitter or cooldown must never occupy a run slot while waiting.
- A small per-worker random jitter makes simultaneous workers less likely to reach PostgreSQL in the same order on every event.
- After every successful claim, a bounded cooldown applies before that worker's next generic refill opportunity. This is the primary anti-monopoly control for a fast worker.
- Repeated empty or duplicate claim outcomes may increase an adaptive contention backoff for later generic opportunities. The backoff has a maximum and decays after successful or uncontended work; it is a contention-control signal, not a permanent penalty for losing a race.
- There is no age-based priority escape from the cooldown. The maximum delay and mandatory polling are the starvation guard, while `available_at` and the existing queue ordering continue to determine eligible work.
- A worker configured with more slots is expected to process proportionally more work. With `max-concurrent-runs > 1`, only the generic refill lane uses one opportunity per currently free slot; a single notification still creates one directed opportunity per worker.

The primary fairness measure is normalized busy-slot time: `busy-slot-seconds / max-concurrent-runs` for each worker over a measurement window. Normalized completed-run count is secondary because run durations and sizes differ. Queue age, especially the oldest eligible `JobRun`, remains an operational latency signal but does not override the admission policy.

This policy is probabilistic rather than a guarantee. Network latency, database connection latency, worker capacity, and different run durations can still produce unequal results. The policy must therefore be evaluated with aggregate per-worker metrics and not inferred from one notification or a short test window. It must preserve duplicate-notification safety, missed-notification polling, lease recovery, cancellation delivery, and token-fenced finalization.

#### Lease and heartbeat rules

- Every lease and heartbeat timestamp is computed with PostgreSQL `now()`, never with a worker's local clock. Workers do not need synchronized clocks and clock skew cannot expire a healthy lease.
- The default heartbeat interval is 30 seconds and the default lease duration is 5 minutes, so a lease survives several missed heartbeats.
- The heartbeat keeps running during `mergeStagingTable()` and `atomicInsertStagingTable()`. Those are server-side operations that can take minutes, and a lapsed lease during a merge would let a second worker start a duplicate run.
- A run whose `lease_until` has elapsed is recovered as a new attempt according to the lease-recovery policy in Decision 3; the expired row itself is never claimed again.

#### Lease fencing and recovery invariants

- `job_run.available_at` determines when a `PENDING` retry is eligible. It is evaluated with PostgreSQL `now()` and is not based on a worker clock.
- `job_run.lease_token` is generated on every claim. All lease renewal and terminal updates include `WHERE id = :id AND lease_token = :leaseToken`.
- Lease recovery and replacement-run creation are atomic. A duplicate recovery scan can affect at most one row because of row locking and the expired-lease predicate.
- A stale worker cannot advance a watermark, mark a replacement run terminal, or overwrite counters after a new worker has claimed the replacement.
- The listener is a wake-up mechanism only. Startup polling, reconnect polling, and periodic polling must discover work and control requests even when notifications are lost.

### Decision 7: Explicitly Out of Scope

The following are deliberately excluded from the current roadmap:

- **Resuming an interrupted run**: partition assignment is not reproducible across executions, so partition-level resume would lose rows silently. See Decision 3.
- **Managed multi-table jobs**: multi-table replication stays in the CLI. See Decision 1.
- **Delete propagation**: incremental merge is an upsert by primary key and never removes rows from the sink.
- **Exactly-once execution**: the model is at-least-once, made safe by claim state, idempotent merges, and watermark commit rules.
- **CDC and real-time replication**: this requires log readers, offsets, ordering, delete capture, and backpressure that are different from batch execution.
- **Universal validation or reconciliation**: source data may change during a run, queries may be non-repeatable, and heterogeneous databases do not provide one equivalent consistency or comparison model. Operational row counters are not proof of source/sink equality.
- **Schema evolution, mappings, and ETL transformations**: ReplicaDB remains a transport tool. Query-specific conversions stay in `source-query` for now. A future frontend may edit job configuration, but it will not add a transformation engine through this architecture decision.

- **Sharing live JDBC connections or a central connection pool across runs**: A managed datasource reuses connection configuration, users, password references, authentication, and technical parameters. It does not reuse open source or sink connections. The core continues to let each task own its connections and lifecycle.

### Decision 8: Reusable Managed Datasources

**Status**: APPROVED FOR PHASE 4

The managed server will expose a first-class datasource catalog. A datasource is a reusable connection profile, not a new replication engine and not a pool of already-open database connections. Jobs select a source datasource and a sink datasource instead of repeating connection details in every job.

This decision applies only to the managed `api` and `worker` profiles. The standalone `replicadb` artifact, its CLI arguments, its options-file keys, its launchers, and its execution semantics remain unchanged. The existing `source.*` and `sink.*` options continue to be the permanent CLI contract.

#### Datasource scope and domain model

The internal Java name is `ManagedDataSource` to avoid confusion with `javax.sql.DataSource`. The public API and frontend use the term `datasource`. A managed datasource owns:

- A stable identifier and unique display name.
- A validated connector type or custom scheme classification used for UI and capability checks.
- A redacted display connection string and non-secret technical `connectionParams`.
- An encrypted `connect.security` bundle containing the actual connection string when it may contain credentials, user/password values, authentication values, and sensitive `*.connect.parameter.*` entries.
- File-format and non-secret Kafka producer settings currently carried in the endpoint `technicalParams` map.
- Creation and update timestamps for temporal audit correlation.

 All credential-bearing values are accepted only through the authenticated API request, encrypted before persistence, and never returned to the client. The server-side `connect.security` namespace uses relative CLI property keys such as `password`, `user`, `connect`, `connect.parameter.secretKey`, `connect.parameter.sasl.jaas.config`, and `auth.client.key`. It is not emitted as a CLI options-file section. During execution the datasource materializer decrypts the namespace and maps it to the existing `source.*` or `sink.*` properties. Raw passwords, key material, encrypted bundle contents, and resolved credentials never enter API responses, audit detail, notifications, metrics, exceptions, or worker payloads. On update, an omitted or blank security value preserves the existing encrypted entry; removal requires an explicit `clearSecurityKeys` list, so editing a datasource never clears a credential accidentally.

The initial provider uses application-level AES-256-GCM envelope encryption. A fresh per-datasource data key is wrapped by a file-backed key-encryption key outside PostgreSQL; every encrypted bundle has a random nonce and explicit algorithm/key versions. A mounted Kubernetes/Docker Secret supplies the key-encryption key, and the server fails during startup when it cannot load or validate it. The provider boundary is extensible to KMS, Vault, or CyberArk without requiring source or sink credentials in process environment variables. The managed execution path keeps decrypted values in memory and passes them directly to `ToolOptionsBuilder`; the existing options-file and redaction rules remain authoritative only for standalone CLI execution.

The managed `JobDefinition` owns replication-specific configuration only:

- `sourceDatasourceId` and `sinkDatasourceId`, both required.
- `sourceDatasourceUseEnabled` and `sinkDatasourceUseEnabled`, both defaulting to `true` when a binding is created.
- Source table/query, columns, filters, sink table/columns, staging settings, mode, watermark, retry policy, and execution tuning.

`SourceEndpoint` and `SinkEndpoint` must reference datasource identifiers rather than embedding `ConnectionCredentials`. The execution layer may create an in-memory resolved definition containing the current datasource credentials, but that resolved value is never persisted as job state.

#### Live references and run boundaries

Datasource references are live. An update to a datasource is used by the next run that resolves the job. A `PENDING` run resolves each datasource when it is claimed, not when the run row is inserted. The PostgreSQL claim transaction locks the eligible run, the job binding, and both datasource rows, records the selected datasource IDs and `resolved_at` on the run, and returns the encrypted bundle snapshot for in-memory decryption after commit. If a datasource update commits before the claim, the run uses the new bundle; if the claim commits first, the active attempt uses the old bundle. A retry is a new pending run and resolves the current profiles again. Updating a datasource never mutates an already-resolved attempt.

The first implementation does not version datasource configuration. `JobRun` retains the source/sink datasource identifiers and `datasources_resolved_at` for temporal correlation, but not a complete configuration snapshot or secret material. This is an explicit trade-off: exact historical reconstruction after a datasource update is deferred until a future revision or immutable-version model is justified.

#### Job-datasource use binding

The user ACL and the job binding are separate concerns:

- A datasource ACL grants users `VIEW`, `USE`, and `EDIT`. `VIEW` returns safe metadata, `USE` permits selecting the datasource in a job, and `EDIT` permits changing its configuration. ADMIN retains the global bypass and manages datasource permissions in the first version.
- The job definition stores `sourceDatasourceUseEnabled` and `sinkDatasourceUseEnabled`. These flags represent whether the job is currently allowed to use each selected datasource.
- Creating or changing a binding requires job edit permission and datasource `USE` permission for the caller.
- Manual triggers, scheduled triggers, and worker claims must all require both binding flags to be enabled. Disabling either flag blocks all future executions of that job, regardless of whether the trigger is manual or scheduled.
- Disabling a binding does not cancel a `RUNNING` attempt. The existing cancellation endpoint remains the explicit mechanism for stopping active work. Pending work must not be claimed after the disabling transaction takes effect, and the implementation must make the flag check and claim boundary race-safe.

The datasource is deleted with `RESTRICT`. A delete request returns a conflict identifying that the datasource is referenced by one or more jobs. No automatic detachment, credential copy, or conversion to inline configuration is performed.

#### Connector capabilities

The server will derive source/sink capability from the connector type and the existing manager contract. Capabilities are not editable JSON stored on a datasource. The catalog must remain aligned with `ManagerFactory`, `SupportedManagers`, and the manager-specific mode restrictions. At minimum, it must represent the existing UI/core distinctions such as Kafka being sink-only and Denodo being source-only; mode-specific restrictions such as a sink that cannot perform `complete-atomic` remain job validation rules.

The connector type is metadata for validation and frontend selection. The connection string remains the value passed to the core, so the server must reject a type/scheme mismatch before persistence or normalize the type from the scheme. A custom connection is allowed only when the catalog can treat its scheme as a supported generic manager; unknown runtime failures must not be disguised as a valid capability.

#### API version and surface

The datasource-only managed contract reuses `/api/v1` because the managed server is not in production and there are no managed jobs or clients that require preservation. This is a documented pre-production reset, not a general exception to Decision 4: after Phase 4, incompatible managed API changes require a new major path segment.

The API adds:

```text
POST   /api/v1/datasources                         Create a datasource
GET    /api/v1/datasources                         List safe, visible datasource metadata
GET    /api/v1/datasources/{id}                    Read safe datasource metadata
PUT    /api/v1/datasources/{id}                    Update connection profile and settings
DELETE /api/v1/datasources/{id}                    Delete when no job references it
GET    /api/v1/datasources/{id}/permissions        Read ADMIN-managed datasource ACLs
PUT    /api/v1/datasources/{id}/permissions/{userId}
                                                     Replace a user's datasource permissions
DELETE /api/v1/datasources/{id}/permissions/{userId}
                                                     Remove a user's datasource permissions
```

 `DatasourceRequest` accepts the transient plaintext values needed to create or rotate a datasource over TLS and an explicit `clearSecurityKeys` list for removal. Omitted or blank secret fields preserve their existing encrypted entries on update. `DatasourceResponse` includes the redacted display connection string, connector type, safe technical settings, capability summary, configured-state flags, and caller capability flags, but never returns any security-bundle value, encryption envelope, or key reference. `JobDefinitionRequest` accepts only `sourceDatasourceId` and `sinkDatasourceId` for connection selection; inline source/sink connection fields are removed from the managed API. `JobDefinitionResponse` returns the selected identifiers, resolution metadata when available, and safe datasource summaries without duplicating credentials.

The first datasource slice does not add a public `test-connection` endpoint. Connection validation remains part of job validation and execution; a bounded test operation can be designed separately if operational demand justifies the security and network implications.

#### Persistence and reset policy

PostgreSQL remains the source of truth. The next forward-only migration creates `managed_datasource` and `datasource_permission`, adds the two datasource identifiers and two use flags to `job_definition`, and removes the inline connection columns from the managed schema. It must first assert that `job_definition` contains no rows; if old development state exists, startup fails with an explicit reset instruction rather than silently deleting or backfilling jobs.

There is no dual-read path, no automatic deduplication of old inline connections, and no conversion of old jobs into datasources. The server metadata database is intentionally recreated/reset for this pre-production contract. Foreign keys from jobs to datasources use restrictive deletion semantics. Existing job runs, schedules, permissions, and idempotency rows are also not migrated because the old jobs do not survive the reset.

#### Execution and distributed runtime

`JobExecutionService` and the worker execution path claim and resolve the job and both referenced datasources at one PostgreSQL boundary before calling the additive root-artifact `ToolOptionsBuilder`. The builder sets the same `ToolOptions` values that the CLI/options-file parser produces, but keeps decrypted credentials in memory and never creates a managed options file. No change is required in `ReplicaDB.processReplica(ToolOptions)` or in any core manager.

Manual and Quartz dispatches validate the binding flags before creating a run. Worker claim SQL locks the job binding and datasource rows, excludes disabled bindings, records the resolved datasource IDs on the run, and returns the encrypted profile snapshot, so a pending run cannot bypass a disabled datasource relationship or change configuration halfway through resolution. A worker notification still carries only the run UUID. Datasource updates, permission changes, and binding changes are re-evaluated from PostgreSQL; no API-local or worker-local datasource cache is authoritative.

#### Audit and security boundary

Add datasource resource/action vocabulary for create, update, delete, and permission changes. Audit details contain only datasource id, safe name, connector type, affected permission, binding side, and bounded timestamps. They never contain password references, resolved values, connection strings, connection parameters, or certificate/key contents.

Datasource list visibility and permission checks occur in backend repositories/services before pagination and before a job binding is accepted. Frontend hiding is only an ergonomic aid. A user with job `VIEW` but without datasource `VIEW` must not receive datasource connection metadata; a user without datasource `USE` cannot create or change the binding. Scheduled execution is authorized by the active job binding, not by the identity of the human who originally created the schedule.

---

## Implementation Phases

### Phase 0: Core and State Foundation

This phase prepares the existing core for managed execution without adding an HTTP server. Most of its cost is **inside the engine**, not in the state layer: the control plane cannot honestly offer watermarks or cancellation until the core supports them. Direct CLI execution remains independent of the managed state store.

#### Phase 0-a: Per-run execution context and rich task results — IMPLEMENTED

Delivered in commit `c228ddc` and covered by focused JUnit tests, concurrency tests, orchestration regressions, and the successful `Only CI/CT` workflow:

- Replaced the static generated staging-table name and temporary-file map with `ReplicationExecutionContext`, owned by `ToolOptions`.
- Preserved sharing of generated staging state across parallel tasks in one run while isolating independent runs and multi-table `ToolOptions` copies.
- Added a run identifier and a `ConcurrentHashMap`-backed temporary-file registry to the context.
- Made generated staging-name initialization safe when multiple managers first access the same run context concurrently.
- Widened `ReplicaTask` from `Callable<Integer>` to `Callable<ReplicaTaskResult>`.
- Added row counts, start/finish timestamps, duration calculation, and a reserved nullable watermark candidate to the task result.
- Added run-level aggregation for total rows, task count, and longest task duration without changing the `processReplica(ToolOptions)` exit-code contract.
- Removed obsolete static reset calls and updated file-manager and multi-table tests.

The Phase 0-a exit criterion is met: two concurrent runs use independent staging names and temporary-file maps, verified across 100 repetitions. Phase 0-a did not add cancellation or watermark extraction/injection; cancellation plumbing was implemented in Phase 0-b1 and watermark extraction/injection was implemented in Phase 0-b2 below.

#### Phase 0-b1: Cancellation plumbing — IMPLEMENTED

Delivered in commit `4dd4cb5`, covered by focused JUnit tests (Mockito-backed context/manager unit tests, `CountDownLatch`-synchronized mid-loop cancellation tests, and a Testcontainers-backed PostgreSQL `COPY` cancellation test), orchestration regressions, and the successful `Only CI/CT` workflow:

- Extended `ReplicationExecutionContext` with an `AtomicBoolean` cancellation flag and a concurrent active-`Statement` registry. `requestCancellation()` sets the flag and calls `.cancel()` on every registered statement, tolerating a failing `cancel()` on one statement without skipping the rest.
- Added `ReplicationCancelledException` (checked, extends `SQLException`) so cancellation propagates through every existing manager method's declared `throws` clause without any signature changes.
- Added `checkCancellation()`/`registerActiveStatement()`/`unregisterActiveStatement()` helpers to `ConnManager` and `FileManager`, and centralized source-side statement tracking in `SqlManager.execute()`/`release()` so `readTable()` cancellation is covered for every SQL manager in one place.
- Wired per-row interrupt checks and active-statement tracking into the batch insert loops of `StandardJDBCManager`, `MySQLManager`, `OracleManager`, `Db2Manager`, `SqliteManager`, PostgreSQL's binary and text `COPY`, MongoDB's bulk write, Kafka's producer loop, and the CSV/ORC file writers; added a best-effort pre-flight guard before SQL Server's `BulkCopy` (no mid-transfer cancel hook exists in that API).
- Guarded `SqlManager.atomicInsertStagingTable()` and every real `mergeStagingTable()` override, including a pre-merge guard for MongoDB's native aggregation merge, which has no `Statement` to register.
- Replaced the blocking `invokeAll(...)` in `ReplicaDB.executeReplicationTasks(...)` with individually submitted, cancellable futures. A cancellation observed on one task cancels its sibling futures and shuts the executor down before rethrowing, so no thread pool is leaked on the cancelled or failed path.
- Added a package-visible `CANCELLED` exit code distinct from `SUCCESS`/`ERROR`. `executeSingleReplication(...)` maps both `ReplicationCancelledException` and any other exception observed while the execution context's cancellation flag is set to `CANCELLED`, so a JDBC driver that reports a cancelled `Statement` as an ordinary `SQLException` is still classified as `CANCELLED`, not `FAILED`.
- Cleanup already ran unconditionally before this change (`finally { cleanupResources(...); }`), so a cancelled run continues to drop an auto-generated staging table and leave a user-provided one untouched without further modification.

Known limitations, accepted for this plan and unchanged by later work: SQL Server `BulkCopy` and PostgreSQL's `COPY` protocol are best-effort — cancellation is observed at the next loop iteration or call boundary, not as an immediate `Statement.cancel()`; MongoDB, Kafka, and ORC have cancellation checks wired in but no dedicated mid-stream cancellation test (only non-cancelled regression coverage); Decision 5's full per-mode/per-timing severity table is exercised only through general cleanup-path tests, not one assertion per table cell.

#### Phase 0-b2: Watermark injection — IMPLEMENTED

Covered by focused JUnit tests (mocked-connection manager unit tests asserting generated SQL and bind order, orchestration unit tests, and a Testcontainers-backed PostgreSQL end-to-end test):

- Added `--incremental-watermark-column`/`--incremental-watermark-value` CLI and options-file settings to `ToolOptions`, validated to require `incremental` mode and to reject combination with `replication.table.*` multi-table entries.
- Added `manager.util.WatermarkBinder`, which resolves the watermark column's JDBC type from the existing `probeSourceMetadata()`/`ColumnDescriptor` machinery (now also triggered by a declared watermark column, not just `sink.auto-create`), converts the configured value into a typed bind parameter, and reduces per-task candidate strings type-aware (not lexicographically) for run-level aggregation.
- Injected a bound `AND <column> > ?` predicate into the 8 JDBC-based managers' existing `readTable()` query-building code (`Db2Manager`, `DenodoManager` — source-only, since Denodo cannot be a sink — `MySQLManager`, `OracleManager`, `PostgresqlManager`, `SqliteManager`, `SQLServerManager`, `StandardJDBCManager`), positioned correctly relative to each manager's existing partition/pagination binds. Non-SQL managers (`MongoDBManager`, `KafkaManager`, `S3Manager`, file managers) are explicitly out of scope, consistent with their existing merge/incremental limitations.
- Centralized the last-executed source SQL/bind-args in `SqlManager.execute()` and added `resolveWatermarkCandidate(int taskId)`, which issues a follow-up `SELECT MAX(...)` probe reusing the same query shape and bind values to report each task's highest observed value.
- Populated `ReplicaTaskResult.watermarkCandidate` from `ReplicaTask`, and widened `ReplicaDB.summarize(...)` to reduce all tasks' candidates to one run-level value.
- Exposed the reduced candidate on `ReplicationExecutionContext.setWatermarkCandidate(...)`/`getWatermarkCandidate()` **only after `executePostTasks()` (staging load + merge) completes without throwing** — a failed run, a cancelled run (either the explicit `ReplicationCancelledException` path or the flag-checked generic-exception path), and every other exception path leave it unset.

Scope boundary at the time of Phase 0-b2: this phase did not persist the watermark anywhere, since no `JobDefinition`/`JobRun`/PostgreSQL state store existed yet. Phase 1b (below) added `JobExecutionService`, which reads the reduced candidate from `ReplicationExecutionContext` and persists it as `JobRun.committedWatermark` after a successful run. The CLI itself still has no mechanism to feed a previous run's committed value back in automatically — a script, an orchestrator, or the Phase 1b execution service must pass it via `--incremental-watermark-value` on the next invocation.

#### Core changes

1. **Watermark injection.** ~~Populate `ReplicaTaskResult.watermarkCandidate`, inject a typed `> :lastWatermark` predicate composed with the existing `$CONDITIONS` partition substitution, and infer the bind type from the source column metadata.~~ Implemented in Phase 0-b2 above.

#### State layer — IMPLEMENTED IN PHASE 1B

- ~~Introduce `JobDefinition` and `JobRun` domain models. There is no `Checkpoint` entity.~~ Implemented in Phase 1b below.
- ~~Add a persistence layer for job definitions, run states, and watermarks.~~ Implemented in Phase 1b below.
- ~~Define legal state transitions and the claim mechanism, using PostgreSQL row locking rather than application-level optimistic locking.~~ Implemented in Phase 1b below.
- ~~Add an execution service that converts a job definition into `ToolOptions`.~~ Implemented in Phase 1b below.
- `ReplicaDB.processReplica(ToolOptions)` remains the compatibility entry point; Phase 1b's execution service calls it unchanged.

#### Resources and tools

- Java 17 and the existing Maven build, producing the two artifacts of Decision 1.
- Spring JDBC for the PostgreSQL metadata store; the replication managers remain unchanged.
- **Flyway** for versioning the metadata schema. Migrations are forward-only, and a rolling upgrade must tolerate one release of schema skew between `api` and `worker` instances.
- JUnit Jupiter 6, Mockito, and Testcontainers for state, retry, cancellation, and integration tests.

#### Exit criteria

- **Met in Phase 0-a:** Two replications run concurrently in one JVM with independent staging tables and temporary files.
- **Met in Phase 0-a:** Task results expose row counters and timings, and the executor aggregates them into a run-level summary.
- **Met in Phase 0-b1:** A cancellation request stops an in-flight replication, including SQL merge and atomic-insert statements, and the run ends in `CANCELLED`, never in `FAILED`, even when a JDBC driver reports the cancelled statement as a plain `SQLException`. SQL Server `BulkCopy` and PostgreSQL `COPY` remain best-effort, and MongoDB's native merge has a pre-merge guard but no active statement to cancel mid-operation.
- **Met in Phase 0-b2, at the core level; durably persisted in Phase 1b:** A failed or cancelled incremental run leaves its previous watermark unchanged (verified at the core level: the reduced candidate is never written to `ReplicationExecutionContext` unless `executePostTasks()` succeeds; verified at the state layer: `JobRunRepository.findLastCommittedWatermark(...)` returns the prior `SUCCEEDED` run's value after a `FAILED` or `CANCELLED` run).
- **Met in Phase 0-b2, at the core level; durably persisted in Phase 1b:** A successful staging load and merge commit exactly one new watermark, reduced from all parallel tasks, exposed on `ReplicationExecutionContext` and persisted by `JobExecutionService` as `JobRun.committedWatermark`.
- A retry can identify the previous run and attempt number, and never claims to resume it.
- CLI behavior and existing `ToolOptions` configuration remain compatible, including multi-table options files.

### Phase 1: Spring Boot API and Scheduler

This is the first user-facing platform phase. API and scheduler run together in one Spring Boot process, while the existing replication core continues to execute the actual database transfer.

#### Phase 1a: Artifact split — IMPLEMENTED

Delivered as the standalone `replicadb-server` sibling Maven project:

- The `replicadb` CLI remains the root Maven artifact, with no Spring Boot dependency or application context on its classpath.
- `replicadb-server` builds against the installed CLI artifact, starts under the `api` profile, and exposes only `/actuator/health` through Actuator.
- The server skeleton excludes inherited MongoDB auto-configuration until the metadata state layer exists, so startup does not require an external database.
- CI builds and tests the server module after installing the CLI artifact; the release workflow uploads its unreleased `0.1.0-SNAPSHOT` jar as a separate build artifact rather than publishing it with the CLI release assets.

The next slice, **Phase 1b: State layer**, is implemented below, followed by **Phase 1c-1: REST API core**, **Phase 1c-2: Quartz scheduler**, and **Phase 1c-3a+b+c: authentication, global roles, per-job ACLs, audit events, retention, and persisted cancellation warnings**, also implemented below. The frontend is elevated to its own top-level **Phase 2** (split into Phase 2a/2b/2c); all three frontend slices are implemented. Distributed workers are renumbered to **Phase 3**.

#### Phase 1b: State layer — IMPLEMENTED

Delivered as additions to `replicadb-server` plus one small, additive core change, covered by focused JUnit unit tests and Testcontainers-backed PostgreSQL integration tests:

- **Domain**: `JobDefinition` and `JobRun` Java records under `org.replicadb.server.job.domain`. `JobDefinition` reuses the existing public `org.replicadb.cli.ReplicationMode` enum instead of duplicating it, and its compact constructor enforces Decision 1's one-source/sink-table-pair rule, a positive `jobs` value, and Decision 4's credential-reference rule (`sourcePassword`/`sinkPassword` must be `null` or `${env:VARIABLE}`, and connection strings may not embed credentials). `JobRunStatus` is the 7-value enum from Decision 3, with `isTerminal()` and `fromReplicaExitCode(int)` mapping `ReplicaDB.processReplica`'s `0`/`1`/`2` to `SUCCEEDED`/`FAILED`/`CANCELLED`. `JobRunStateMachine.assertLegalTransition(from, to)` enforces the transition table from Decision 3.
- **Persistence**: Forward-only Flyway migrations `V1__create_job_definition.sql` and `V2__create_job_run.sql` create the metadata schema, including `job_run`'s `executor_identity`/`lease_until`/`heartbeat_at` columns (populated with simple single-instance values now, ready for Phase 3's lease rules without a later `ALTER TABLE`). `JobDefinitionRepository` and `JobRunRepository` use `NamedParameterJdbcTemplate` (Spring JDBC, not JPA). `JobRunRepository.claimNextPending(executorIdentity, leaseDuration)` runs a `SELECT ... FOR UPDATE SKIP LOCKED` followed by a conditional `UPDATE` in one transaction — real PostgreSQL row locking, not application-level optimistic locking. `scheduleRetry(failedRunId)` implements Decision 3's "no resume" rule as described above. `findLastCommittedWatermark(jobDefinitionId)` returns the last `SUCCEEDED` run's watermark, falling back to the job definition's `initialWatermarkValue`.
- **Execution service**: `JobDefinitionEnvResolver` resolves `${env:VARIABLE}` references (rejecting `${secret:...}` explicitly, per Decision 4) immediately before `JobDefinitionOptionsFileWriter` converts the resolved `JobDefinition` into the CLI-compatible temporary options file consumed by `ToolOptions` — this is the concrete mechanism behind "converts a job definition into `ToolOptions`". `JobExecutionService.executeNextPending(executorIdentity)` claims a run, builds `ToolOptions`, calls the unchanged `ReplicaDB.processReplica(options)`, maps the exit code via `JobRunStatus.fromReplicaExitCode(...)`, and persists `rowsProcessed`/`durationMillis` for every outcome and `committedWatermark` only on success. It never logs the resolved connect strings, passwords, or the generated options file contents.
- **Core widening**: `ReplicationExecutionContext` gained `rowsProcessed`/`durationMillis` accessors, populated unconditionally by `ReplicaDB.executeSingleReplication` right after the parallel tasks finish — unlike the watermark candidate, which stays conditional on `executePostTasks()` succeeding. This is what lets `JobExecutionService` persist row counters and timings for a `FAILED`/`CANCELLED` run, not only a `SUCCEEDED` one.
- **Runtime alignment**: `replicadb-server` now depends on `spring-boot-starter-log4j2` instead of Spring Boot's default logging bridge, because ReplicaDB core's Sentry initialization requires a Log4j2 `LoggerContext`; without this, `ReplicaDB.processReplica(...)` fails immediately when invoked from the managed runtime.
- **Testing**: Testcontainers PostgreSQL via Spring Boot's `@ServiceConnection` backs the full-context and repository tests (including the two Phase 1a context tests, updated to boot with the now-mandatory `DataSource`); a dependency-light `FlywayMigrationTest` validates the migrations with a raw `PostgreSQLContainer` before any Spring wiring exists; `JobExecutionServiceIT` exercises a real end-to-end `incremental` run against SQLite source/sink fixture files, asserting the persisted `committedWatermark` and that a failed run leaves the prior committed value unchanged.
- **CI**: The `server` job in `CT_Push.yml` now sets the same `TESTCONTAINERS_CONFIG_FILE`/`DOCKER_HOST`/`TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` env and `docker info` check as the `integration`/`non_integration` jobs, since its tests now require Docker.

Known limitations, accepted for this phase and not yet addressed: `JobRun.errorMessage` on `FAILED` is a generic message, since `processReplica(ToolOptions)` does not expose the underlying exception (except when `ToolOptions` construction itself throws); audit events and their retention purge were delivered in Phase 1c-3c, while shared multi-instance login throttling and distributed lease fencing are Phase 3 work; `executor_identity`/`lease_until`/`heartbeat_at` use simple single-instance values until Phase 3's distributed-worker lease rules apply; no `mode_warning` column exists yet for Decision 2's `complete`-mode API warning, since no API exists to surface it **(Phase 1c-1 computes it dynamically in the API response instead of adding that column — see below)**. See `.ai/archive/phase-1b-state-layer.plan.md` for the full implementation plan and execution retrospective.

#### Phase 1c-1: REST API core (job definitions and runs, no auth) — IMPLEMENTED

Delivered as additions to `replicadb-server`, covered by focused JUnit unit tests, Testcontainers-backed PostgreSQL/MockMvc integration tests, and one full end-to-end HTTP test with an embedded server:

- **Controllers**: `JobDefinitionController` (`POST`/`GET`/`GET {id}`/`PUT` under `/api/v1/jobs`) and `JobRunController` (`GET /api/v1/jobs/{id}/runs`, `GET /api/v1/runs`, `GET /api/v1/runs/{id}`, `GET /api/v1/runs/{id}/log`, `POST /api/v1/jobs/{id}/runs`, `POST /api/v1/runs/{id}/cancel`, `POST /api/v1/runs/{id}/retry`) implement the exact endpoint table in the "API surface" section below. REST request/response `mode` values are the lower-case `ReplicationMode.getModeText()` strings (`complete`, `complete-atomic`, `incremental`), not the Java enum names — `JobDefinitionMapper` parses and serializes them case-insensitively at the DTO boundary, since Jackson's default enum binding only accepts the upper-case Java names.
- **Asynchronous execution**: `RunExecutionCoordinator` is a new `@Service` with a bounded `ThreadPoolExecutor` (`replicadb.server.execution.pool-size`, default 4) and an in-memory `ConcurrentHashMap<UUID, ToolOptions>` registry of in-flight runs. `JobExecutionService.executeClaimedRun(JobRun, Consumer<ToolOptions>)` was widened with an `onStarted` callback (its existing no-callback `executeNextPending(...)` entry point is unchanged and still used by its own tests) so the coordinator can register the live `ToolOptions`/`ReplicationExecutionContext` the instant it is constructed, before the blocking `ReplicaDB.processReplica(...)` call returns — this is what lets `POST /api/v1/runs/{id}/cancel` reach a running replication's `ReplicationExecutionContext.requestCancellation()` synchronously and immediately, satisfying Decision 5's "immediate and unconditional" requirement within this single-instance, monolithic control-plane phase.
- **Non-overlap**: a partial unique PostgreSQL index (`ux_job_run_one_active_per_definition` on `job_run(job_definition_id)`, restricted to `PENDING`/`RUNNING`/`CANCEL_REQUESTED`, added by `V3__add_job_run_active_constraint.sql`) is the actual atomic guarantee that a job definition never has two active runs; `JobRunRepository.hasActiveRun(...)` is only a fast, non-authoritative pre-check that produces a cleaner error message before hitting the constraint. `RETRY_SCHEDULED` is deliberately excluded from the index because `scheduleRetry(...)` must transition the failed row to that status and insert its replacement `PENDING` row in the same transaction — including it would make every retry fail the uniqueness check against itself.
- **Idempotency**: `run_trigger_idempotency(idempotency_key, job_definition_id, run_id, created_at)` (`V4__create_run_trigger_idempotency.sql`) backs the 24-hour `Idempotency-Key` replay rule from Decision 4; `IdempotencyCleanupTask` (Spring's built-in `@Scheduled`, deliberately not Quartz, so it does not encroach on the Phase 1c-2 scheduler) purges rows older than 48 hours once daily.
- **Errors and pagination**: `GlobalExceptionHandler` builds RFC 7807 `ProblemDetail` responses (400/404/409/500), passing every detail through the existing `CredentialRedactor` before it reaches a client. `PageRequestParams` enforces Decision 4's `page`/`size` defaults and 200-row maximum on every collection endpoint.
- **Cancellation warning**: the cancel response body (`{"runId", "status", "warning"}`) always includes Decision 5's per-mode warning text, computed from the job definition's `mode` at request time — the API cannot observe whether cancellation landed before or during a merge, so it returns the worst-case warning for that mode.

Known limitations, accepted for this slice and not yet addressed: `GET /api/v1/runs/{id}/log` returns only the persisted `error_message` as a stub excerpt, not the full 256 KB captured run log described in the Operational Defaults table; the cancellation warning is returned in the HTTP response and persisted on the `job_run` row; two simultaneous `PUT /api/v1/jobs/{id}` requests are last-write-wins with no optimistic-locking `version` column; a JVM crash while a run is `RUNNING` (cancellation-related or not) leaves that `job_run` row stuck in a non-terminal status, since there is no lease-expiry reconciliation until Phase 3; Spring Security/users/roles/ACLs were later Phase 1c slices, and the frontend is now its own Phase 2. See `.ai/archive/phase-1c-1-rest-api-core.plan.md` for the full implementation plan and execution retrospective.

#### Phase 1c-2: Scheduler — IMPLEMENTED

Delivered in commit `8d12cdc` and covered by focused unit tests, Testcontainers-backed PostgreSQL integration tests, a real RAMJobStore lifecycle test, and the full `replicadb-server` suite:

- **Quartz runtime**: `spring-boot-starter-quartz` is configured with `RAMJobStore`, scheduler name `ReplicaDbScheduler`, and a two-thread Quartz firing pool. The product-level durable schedule state is stored in PostgreSQL; Quartz trigger bookkeeping is intentionally in memory for this single-instance phase.
- **Schedule persistence**: Flyway migration `V5__create_job_schedule.sql` adds the one-to-one `job_schedule` table. `JobScheduleRepository` supports upsert, lookup, enabled-schedule reconciliation, and idempotent deletion. Migration `V6__add_job_run_definition_created_index.sql` adds the `(job_definition_id, created_at DESC)` index used by job run history queries; the metadata schema now has six validated migrations.
- **Execution path**: `ScheduledRunTriggerJob` reads the job definition identifier from Quartz job data, performs the same active-run pre-check and pending-row insertion as the manual trigger, and submits through `RunExecutionCoordinator` with executor identity `scheduler`. `@DisallowConcurrentExecution` is defense-in-depth; the PostgreSQL partial unique index remains the authoritative non-overlap guarantee.
- **Startup durability**: `ScheduleReconciler` loads every enabled `job_schedule` row on application startup and registers it in Quartz. Cron triggers use the job's validated IANA timezone, skip misfires rather than catching up, and use stable per-job Quartz keys so reconciliation and API upserts converge safely.
- **API surface**: `PUT`, `GET`, and idempotent `DELETE /api/v1/jobs/{id}/schedule` manage recurring schedules. The API defaults a missing or blank timezone to `UTC`, returns the computed `nextFireTime`, and removes disabled schedules from Quartz.
- **Known limitations**: RAMJobStore does not persist Quartz-native trigger bookkeeping, missed fires are deliberately not replayed, and there is no metrics/alert signal for silent misfires yet. Phase 3 replaces RAMJobStore with Quartz JDBC clustering for the multi-API topology. Audit events, persistence of the cancellation warning, and frontend administration are implemented.

#### Phase 1c-3: Security — 1c-3a+b+c IMPLEMENTED

Phase 1c-3a+b+c is implemented in `replicadb-server`. Delivered security scope:

- Local-user authentication via `spring-boot-starter-security` with `spring-session-jdbc` sessions persisted in PostgreSQL (no external identity provider in this phase), Argon2id password hashes, secure session cookies, CSRF protection, and login throttling.
- Global roles `ADMIN`, `OPERATOR`, and `VIEWER`, with admin-only user management and environment-seeded, fail-closed bootstrap of the first administrator.
- Per-job ACLs for `VIEW`, `EDIT`, `EXECUTE`, and `CANCEL`, enforced on every `/api/v1` endpoint added in Phase 1c-1/1c-2, including SQL-side visibility filtering before pagination.
- Durable `audit_event` records for login attempts, logout, user/bootstrap changes, job definition and ACL changes, schedule changes, API run actions, scheduled triggers, and terminal run outcomes. Detail values are redacted and bounded before persistence; passwords, connection secrets, and credential-bearing strings are excluded.
- An explicit `AuditService` provides the single fail-open write boundary, with system actors for background execution and terminal outcomes. `AuditEventRepository` persists JSONB details and supports indexed filtering, paging, counting, user-deletion preservation, and retention deletion.
- ADMIN-only `GET /api/v1/audit` exposes newest-first history with RFC 7807 errors, case-insensitive action/resource filters, time windows, and the standard `page`/`size` contract.
- `AuditRetentionTask` purges audit rows older than the configurable 365-day default each day at 03:30.
- Cancellation warnings are persisted on `job_run` in V11 and returned through `JobRunResponse`, with the warning written atomically during pending or running cancellation requests.

Known limitations accepted for this phase:
- Audit insertion is fail-open: `AuditService` logs an `ERROR` and swallows repository/serialization failures, so a metadata outage can produce a missing audit row without failing the audited operation.
- Terminal run outcomes use a system actor derived from `executor_identity` rather than propagating the human request actor; correlate with the run-trigger event by run id.
- Audit history is ADMIN-only and is not filtered by per-job ACL. Read operations, including run logs and job definitions, are not audited.
- Audit rows retain `actor_username` after user deletion while the foreign-key `actor_user_id` is set to `NULL`.

The frontend, originally scoped as a Phase 1c-4 sub-slice, has since been elevated to its own top-level **Phase 2** (see below), given the amount of distinct screen, stack, and build-integration work it carries; distributed workers are renumbered to **Phase 3**.

#### Spring Boot modules

The planned runtime would use the following components:

- `spring-boot-starter-web` for REST controllers and JSON requests.
- `spring-boot-starter-validation` for validating job definitions before persistence.
- `spring-boot-starter-jdbc` for the metadata repository while preserving the project's JDBC-oriented design.
- `spring-boot-starter-security` for authentication and endpoint authorization.
- `spring-session-jdbc` for server-side sessions persisted in PostgreSQL.
- `spring-boot-starter-quartz` for durable schedules, misfire handling, and non-overlapping triggers.
- `spring-boot-starter-actuator` and Micrometer for health, counters, and operational metrics.
- The existing Log4j2 and Sentry integration for logs and error reporting, with credential redaction preserved.
- `springdoc-openapi`, added in Phase 2a to generate the OpenAPI specification the frontend's TypeScript types are generated from.

These dependencies belong exclusively to the `replicadb-server` artifact. The `replicadb` CLI assembly is built from the same sources without them, as decided in Decision 1.

#### API and scheduler execution

The API and scheduler are not two separate processes in this phase:

```text
Spring Boot process (profile=api)
    |
    +-- REST controllers
    +-- Quartz scheduler
    +-- Job repository / state store
    +-- Bounded execution executor
    +-- ReplicaDB execution service
              |
              +-- ReplicaDB.processReplica(ToolOptions)
```

The execution sequence is:

1. A client creates a `JobDefinition` through the API.
2. The API validates and stores the definition without storing raw secrets.
3. A manual request or Quartz trigger creates a `JobRun` in `PENDING` state.
4. Quartz submits the run to a bounded application executor. Quartz must not hold a scheduler thread while a database replication is running.
5. The executor claims the run, changes it to `RUNNING`, resolves configuration references, and creates `ToolOptions`.
6. The executor calls the existing ReplicaDB core.
7. Row counters, timings, errors, and the final state are persisted.
8. For incremental mode, the watermark candidate reduced from all parallel tasks is committed only after the sink staging and merge operations succeed.

By default, a job should not overlap with another run of itself. Quartz's non-concurrent execution controls or an equivalent database claim must enforce this rule. Different jobs may run concurrently subject to configured resource limits, and only after the Phase 0 execution-context refactor has removed the static state described in Decision 2.

#### Operational defaults

These values are decided so that the first deployment is operable without further discussion. They are configurable; the defaults are what ships.

| Concern | Default | Rationale |
|---|---|---|
| Run history retention | 90 days | Bounded growth of `JobRun`; a scheduled purge deletes older rows |
| Audit event retention | 365 days | Longer than run history because it supports access review |
| Maximum run duration | 24 hours | Exceeding it cancels the run and marks it `FAILED` with a timeout reason |
| Schedule timezone | explicit IANA zone per job, `UTC` when unset | Avoids ambiguous and skipped local times across DST transitions |
| Persisted run log | last 256 KB of the run's own log, truncated from the head | Enough for a failure diagnosis in the UI without turning PostgreSQL into a log store |
| Full run logs | process logs and Sentry | The state store is the source of truth for state, not for output |
| Metadata backup | daily managed backup with point-in-time recovery | The metadata store becomes critical once jobs are managed there |

#### API surface

The first API remains small and explicit. **Implemented in Phase 1c-1 and extended through Phase 1c-3c**, exactly as specified below, except `GET /api/v1/runs/{id}/log` returns only a stubbed excerpt (see the Phase 1c-1 section above):

```text
POST   /api/v1/jobs                 Create a job definition
GET    /api/v1/jobs                 List job definitions
GET    /api/v1/jobs/{id}            Read a definition and current state
PUT    /api/v1/jobs/{id}            Update a definition
POST   /api/v1/jobs/{id}/runs       Trigger a manual run (Idempotency-Key)
GET    /api/v1/jobs/{id}/runs       Read historical runs for a job
GET    /api/v1/runs                 List runs, filterable by state
GET    /api/v1/runs/{id}            Read run status and counters
GET    /api/v1/runs/{id}/log        Read the persisted log excerpt
GET    /api/v1/audit                Read ADMIN-only audit history
POST   /api/v1/runs/{id}/cancel     Cancel immediately, returns the sink warning
POST   /api/v1/runs/{id}/retry      Re-execute the job from the beginning
PUT    /api/v1/jobs/{id}/schedule  Create or replace a recurring schedule
GET    /api/v1/jobs/{id}/schedule  Read the recurring schedule
DELETE /api/v1/jobs/{id}/schedule  Remove the recurring schedule
```

The API returns a run identifier quickly. Clients poll `GET /api/v1/runs/{id}` initially; server-sent events or WebSocket monitoring can be considered later without changing the execution contract.

#### Identity model already backing the frontend

The `app_user`/`app_role`/`app_user_role`/`job_permission`/`user_session`/`audit_event` persistence model, Argon2id password hashing, fail-closed `ADMIN` bootstrap, and per-job ACL enforcement described in Decision 4 are already implemented as of Phase 1c-3a+b+c. Phase 2 (below) only adds screens on top of this existing backend; it introduces no new identity or permission tables.

#### How the API deployment starts

The two artifacts of Decision 1 are started as follows:

```bash
# Existing CLI artifact: no Spring Boot context on the classpath
./bin/replicadb --options-file ./conf/replicadb.conf

# Server artifact: Spring Boot starts REST API and Quartz together
java -Dspring.profiles.active=api -jar replicadb-server.jar

# Equivalent container path
docker run -p 8080:8080 \
  -e SPRING_PROFILES_ACTIVE=api \
  -e REPLICADB_METADATA_URL=${REPLICADB_METADATA_URL} \
  replicadb-server:api
```

At startup, Spring Boot loads the `api` profile configuration, connects to the metadata store, registers Quartz jobs, exposes the HTTP port, and creates the bounded executor. The scheduler then creates run records; it does not invoke database managers directly.

#### Phase 1 resources

- One API container or JVM process.
- PostgreSQL for local, managed, and multi-instance control-plane deployments.
- Network access from the API process to the configured source and sink databases.
- CPU and memory reserved for the executor's concurrent ReplicaDB runs.
- Environment variables or a secret manager for credentials and connection references.
- Docker or Podman for packaging; Testcontainers for API, metadata, security, and database integration tests.

### Phase 2: Frontend

Phase 2a, Phase 2b, and Phase 2c are implemented. Elevated from a Phase 1c-4 sub-slice to its own top-level phase: it has a distinct stack, a distinct build pipeline (`frontend-maven-plugin`), and a three-slice scope large enough to warrant the same weight as Phase 1 and Phase 3. It consumes the `/api/v1` surface and the identity/permission model already implemented through Phase 1c-3a+b+c. Phase 2a adds the OpenAPI specification dependency and CSRF bootstrap endpoint required by the SPA, but no new persistence.

```text
Browser
  |
  +-- Login session cookie (HttpOnly, Secure, SameSite)
  |
  +-- Frontend static assets served by api
  |
  +-- REST API
        |
        +-- Spring Security authentication
        +-- role checks
        +-- job ACL checks
        +-- JobDefinition / JobRun services
```

Split into three slices, mirroring how Phase 1c-3 was split into 3a/3b/3c, so no single plan tries to cover login, monitoring, editing, scheduling, and administration at once:

#### Phase 2a: Authentication and read-only monitoring — IMPLEMENTED

Implemented with login/logout, CSRF bootstrap via `GET /api/v1/auth/csrf`, session bootstrap via the existing `GET /api/v1/auth/me`, a dashboard listing jobs visible to the current user, read-only job detail (including `JobDefinitionResponse.modeWarning`), run history, and run detail with status, counters, timings, and the log excerpt. No create, edit, trigger, cancel, or schedule actions were added.

#### Phase 2b: Job editor and run actions — IMPLEMENTED

Implemented with create/update job definitions (source/sink table pair, mode, parallelism, and watermark column), schedule management against the Phase 1c-2 endpoints, and mutating run actions: trigger (with `Idempotency-Key`), cancel, and retry. The frontend uses the existing `/api/v1` contract without backend API changes.

#### Phase 2c: Administration — IMPLEMENTED

Implemented with ADMIN-only user administration (list, create, role/enabled updates, and password reset), per-job permission management for `VIEW`/`EDIT`/`EXECUTE`/`CANCEL`, protected routes, and the frontend navigation/action gates. It consumes the existing `/api/v1/users` and `/api/v1/jobs/{id}/permissions` contracts without backend changes.

#### Frontend technical shape

Decided technical shape, applying to all three sub-phases:

- **Stack**: React, TypeScript, and Vite, compiled to static assets served by the `replicadb-server` process under the `api` profile.
- **Styling**: MUI v6 themed with Material 3 design tokens. MUI's M3 support is community-driven rather than an officially on-spec implementation; this is an accepted trade-off in exchange for React-native component ergonomics over Google's `@material/web` custom elements, which would require manual event wiring and value binding in React/TypeScript.
- **Data fetching and state**: TanStack Query, driving both response caching and polling of `GET /api/v1/runs/{id}` while a run is non-terminal.
- **Routing**: React Router.
- **API types**: generated from an OpenAPI specification rather than hand-written, so DTO types stay in sync as the API evolves. `springdoc-openapi` was added to `replicadb-server` in Phase 2a, together with the committed schema generation and CI drift check.
- **HTTP client**: `axios`, chosen because its default XSRF cookie/header names (`XSRF-TOKEN` cookie, `X-XSRF-TOKEN` header) match the `CookieCsrfTokenRepository` defaults already configured in `SecurityConfig`, removing the need for hand-written CSRF cookie-to-header plumbing.
- **Build integration**: `frontend-maven-plugin`, wired into `replicadb-server`'s `mvn package` lifecycle so one Maven build installs Node/npm, runs the Vite build, and copies the output into `src/main/resources/static`. No separate CI build step and no committed build artifacts.
- **Local development**: a Vite dev-server proxy for `/api/v1`, since the SPA dev server and the Spring Boot process run on different ports locally; the production build has no such concern since it is served from the same origin.
- **Testing**: Playwright end-to-end tests.

#### Phase 2 resources

- Frontend Node/Vite build tooling, with compiled assets packaged into the `replicadb-server` image.
- Playwright browser binaries in CI.
- No new database, credentials, or deployment topology beyond what Phase 1 already provides.

### Phase 3: Distributed Workers and Highly Available Control Plane

Phase 3 expands the single-instance `api` runtime into a shared PostgreSQL control plane with multiple API instances and one or more replication workers. The implementation is split into four ordered slices. The slices are ordered because dispatch and execution cannot be made reliable until the persisted lease and retry contract is safe under stale workers, and load distribution cannot be measured safely until the worker runtime and operational telemetry exist.

#### Locked topology and invariants

```text
API instances (1..N)
     |
     v
PostgreSQL state and Quartz JDBC cluster
     |
LISTEN/NOTIFY plus mandatory polling
     |
Workers (1..N) -> claim, heartbeat, ReplicaDB core
```

- PostgreSQL is the only source of truth for run state. API-local maps, Quartz notifications, and worker logs are not state stores.
- Phase 3.2 introduced the worker runtime while the API still used its compatible single-instance RAMJobStore. Phase 3.3 now provides the PostgreSQL-backed clustered scheduler; all API instances in a deployment must use the clustered configuration before they share schedules.
- The worker profile has no public API, frontend, Spring Security session, or Quartz scheduler. It starts only the repositories, dispatch coordinator, execution services, listener/poller, and ReplicaDB core.
- A worker executes one `JobRun` at a time by default. A bounded `worker.max-concurrent-runs` setting may increase this later; ReplicaDB's existing `jobs` option still controls the internal tasks of one run.
- Worker admission follows the approved hybrid distribution policy: notifications use one directed opportunity per worker with at most one general fallback, while startup/reconnect/polling/completion refill currently free slots one opportunity per slot. Duplicate wake-ups are coalesced, and bounded jitter/cooldown reduces repeated wins by a fast worker.
- The hybrid policy is an approximate fairness mechanism only. It does not add a global worker registry or central assignment service; PostgreSQL claims remain authoritative, and fairness is evaluated with normalized busy-slot time rather than raw run counts.
- Runs are never resumed. Worker loss creates a new attempt from the beginning, subject to the retry policy and replication-mode safety rules.
- `LISTEN/NOTIFY` is a wake-up optimization. Startup polling, reconnect polling, and periodic polling are mandatory recovery paths.
- The phase does not introduce an external broker, CDC, managed multi-table jobs, partition resume, or exactly-once execution.

#### Plan 3.1: Distributed state contract, leases, retries, and fencing

This plan makes the PostgreSQL state model safe for multiple APIs and workers before introducing the worker runtime.

Scope:

- Add forward-only migrations for `JobDefinition.maxAttempts`, `retryBackoffSeconds`, and `automaticRetryEnabled`.
- Add `JobRun.availableAt` and an opaque `JobRun.leaseToken` generated on every claim. `availableAt` controls retry eligibility; it is evaluated with PostgreSQL `now()`.
- Extend the Java records, repositories, mappers, OpenAPI schema, and frontend editor to expose the retry policy without exposing secrets.
- Replace the single-instance claim methods with an atomic `claimNextEligible(...)` contract using `FOR UPDATE SKIP LOCKED`, `available_at <= now()`, and a fresh lease token.
- Add `renewLease(runId, leaseToken, leaseDuration)` and make it update `lease_until` and `heartbeat_at` only when the token still owns the run.
- Add an atomic `recoverExpiredRun(...)` operation. It locks an expired `RUNNING` row, moves it to `RETRY_SCHEDULED`, and inserts a new `PENDING` row with `previousRunId`, incremented `attempt`, and the computed `availableAt` when another attempt is allowed.
- When `maxAttempts` is exhausted, recovery marks the abandoned run `FAILED` and creates no replacement row.
- Automatic recovery applies to lease expiration, not to every ordinary `FAILED` result. The existing manual retry endpoint remains explicit and preserves run history.
- Default `automaticRetryEnabled` to true for `complete-atomic` and `incremental`, and false for `complete`. A `complete` job may explicitly opt in, but the destructive-mode warning is mandatory in the API and frontend.
- Require `id` plus `leaseToken` on heartbeat, cancellation completion, terminal-state, counter, error, and watermark updates. A stale worker's update must affect zero rows and must never advance a watermark.
- Persist cancellation requests so they can be observed by a worker that did not receive the original HTTP request. Preserve the existing per-mode sink warning.

The retry backoff is the persisted per-job delay in seconds before the replacement `PENDING` row becomes eligible. The first implementation uses this configured delay directly; changing to a different backoff formula is outside this phase unless the state contract is revised.

Tests and exit criteria:

- Repository integration tests prove that concurrent claims select distinct rows, duplicate recovery scans recover a row once, and `availableAt` is honored.
- Fencing tests prove that a stale worker cannot renew a lease, finish a run, overwrite counters, or commit a watermark after recovery.
- Retry tests prove attempt numbering, `previousRunId`, backoff eligibility, maximum-attempt exhaustion, and mode-specific automatic-retry defaults.
- The plan exits when two independent processes can safely claim, renew, recover, and finalize runs using only PostgreSQL state, with no worker runtime required.

**Implemented in Phase 3.1**: Flyway V13/V14 persist retry policy, eligibility, and lease identity; Spring JDBC claims use `FOR UPDATE SKIP LOCKED`; recovery creates fenced replacement attempts with PostgreSQL-owned backoff timestamps; API and execution paths use the shared ports and token-checked finalization; the REST/OpenAPI/frontend contract exposes policy and `availableAt` without exposing lease tokens. The `api` coordinator remains a compatibility path alongside the isolated Phase 3.2 worker runtime.

#### Plan 3.2: Worker runtime and PostgreSQL dispatch

This plan introduces the `worker` profile and connects API/scheduler run creation to long-lived worker execution.

Scope:

- Add `application-worker.yml` and profile conditions that prevent REST controllers, frontend resources, Spring Security/session bootstrap, Quartz, API schedule reconciliation, and API-only maintenance tasks from starting in a worker process.
- Extract a shared execution path from `RunExecutionCoordinator` and `JobExecutionService` so both the current `api` profile and the new worker profile use the same definition resolution, `ToolOptions` construction, ReplicaDB invocation, watermark commit, audit, and cleanup behavior.
- Implement a bounded `WorkerDispatchCoordinator` with one active run by default. It must claim from PostgreSQL rather than receive a complete job configuration in a message.
- Implement a dedicated PostgreSQL listener connection subscribed to `replicadb_runs` for run identifiers and a run-control channel for cancellation identifiers. The listener must reconnect and re-subscribe after connection failure.
- Implement `PollingFallback` at worker startup, after listener reconnect, and at a configurable interval. Polling discovers pending/retryable runs and active cancellation requests even when notifications are lost.
- Change manual triggers, Quartz triggers, and worker recovery dispatches to use a transactional `RunDispatchService`: insert or create the durable run and issue `pg_notify('replicadb_runs', run_id)` before the PostgreSQL transaction commits. The payload contains only the run identifier.
- Move cancellation delivery from the API's in-memory `ToolOptions` registry to persisted `CANCEL_REQUESTED` state plus a run-control notification. The worker maps that state to `ReplicationExecutionContext.requestCancellation()` and completes the run with the lease token it still owns.
- Implement an independent heartbeat loop that renews the lease during source reads, staging, `mergeStagingTable()`, `atomicInsertStagingTable()`, cleanup, and any other long-running core operation. A failed heartbeat must not falsely extend a lease; a lost lease must trigger local cancellation and fencing.
- Keep source and sink connections owned by ReplicaDB tasks. The listener uses its own PostgreSQL connection and never transports credentials or resolved options.

Tests and exit criteria:

- Profile tests prove that `api` still exposes the REST/frontend runtime and `worker` exposes no public login or UI surface.
- PostgreSQL integration tests prove transaction-coupled notification, rollback-without-notification, duplicate notifications, missed notifications, listener reconnect, startup polling, and periodic polling.
- Multi-worker tests prove that one run is claimed once, duplicate polling does not duplicate a watermark, and a remote cancellation reaches the owning worker.
- Long-running-operation tests prove that heartbeats remain active during merge and atomic swap and that a lease does not expire while the worker is healthy.
- The plan exits when one API instance and multiple worker instances can execute, cancel, recover, and monitor runs through PostgreSQL without API-local execution state.

**Implemented in Phase 3.2**: the API and Quartz dispatch durable UUID notifications through a transaction-bound PostgreSQL publisher; the worker profile starts a bounded coordinator, mandatory startup/reconnect/periodic polling, a dedicated reconnecting listener, independent lease heartbeats, durable cancellation delivery, and the shared execution service. Shared-schema Testcontainers scenarios prove two workers claim a run once, retries and lease recovery create new attempts, incremental watermarks remain fenced, and the worker exposes no HTTP, security-session, frontend, or Quartz surface. **Phase 3.3 is complete**: Quartz JDBC clustering, shared login throttling, health/metrics, server packaging, local Compose topology, process-level failure/load validation, and final release gates passed in local validation and GitHub Actions.

Phase 3.4 preserves the current durable claim contract while providing the directed-notification/fallback split, refill opportunities for free slots, wake-up coalescing, bounded jitter, success cooldown, adaptive contention backoff, and normalized busy-slot observability described above.

#### Plan 3.3: API high availability and operational hardening

This plan makes the API cluster and the distributed runtime operable as a deployment rather than only as a set of components. Quartz JDBC clustering, shared login throttling, health/metrics, server packaging, local Compose topology, process-level failure/load validation, and final release hardening are implemented and validated.

Scope:

- Replace `RAMJobStore` with Quartz JDBC clustering backed by PostgreSQL. Add the Quartz schema through forward-only migrations, stable scheduler instance identities, cluster check-in, misfire behavior, and connection-pool sizing.
- Make schedule reconciliation and schedule updates safe when multiple API instances start or receive concurrent changes. PostgreSQL/Quartz state remains authoritative; no API-local trigger is treated as durable.
- Replace the in-memory `LoginAttemptService` window with a PostgreSQL-backed shared throttle so account and source-address limits apply consistently across API instances.
- Add API and worker health/readiness signals for PostgreSQL connectivity, listener status, claim availability, and executor capacity. Add Micrometer counters/timers for claims, notification latency, polling lag, lease renewals, expired leases, retries, stale updates, cancellations, and terminal outcomes.
- Document and test rolling startup/shutdown, metadata migrations, worker identity, listener reconnect, database outage behavior, and the separation between API and worker network exposure.
- Add Docker Compose topology tests for multiple API instances and workers. Kubernetes deployment templates and autoscaling remain optional packaging work, but the concurrency formula must be documented: `worker instances * concurrent runs per worker * jobs per run`.
- Add reproducible load and failure tests for duplicate schedule firing, worker loss during source copy and merge, API loss, PostgreSQL restart, notification loss, duplicate polling, and stale-worker fencing.

Tests and exit criteria:

- Two or more API instances share schedules without duplicate durable runs and continue serving the same PostgreSQL-backed state.
- Shared login throttling and session behavior remain correct across API instances.
- The deployment exposes actionable health and metrics for every Phase 3 recovery path.
- The plan exits when the documented API-cluster plus worker topology passes integration, failure, and load checks with measured concurrency limits.

#### Phase 3.4: Hybrid Worker Load Distribution

**Status**: IMPLEMENTED and validated on August 26, 2026.

Phase 3.4 improves worker-fleet fairness without introducing a central dispatcher, a worker registry, an external broker, or a second source of truth. It depends on the durable worker runtime from Phase 3.2 and the health/metrics and process-validation foundations from Phase 3.3. Its objective is approximate uniformity of real worker utilization, not strict round-robin assignment or perfect equality.

Implemented with `WorkerAdmissionPolicy`, bounded `WorkerAdmissionQueue` coalescing, a separate `WorkerAdmissionScheduler`, per-free-slot generic refill, one non-recursive directed fallback, bounded jitter, successful-claim cooldown, capped decaying contention backoff, and monotonic raw/normalized busy-slot metrics. Focused JUnit tests, PostgreSQL/Testcontainers integration, Compose fairness validation, admission-aware worker-loss/resilience validation, and the standalone CLI compatibility gate passed. The result is approximate fairness, not a strict scheduling guarantee.

The phase preserves the Phase 3.2 claim and fencing contract and adds two local admission lanes:

- **Directed notification lane**: each worker gets at most one directed claim opportunity per `replicadb_runs` notification. Duplicate notifications for the same `runId` are coalesced. When the worker has capacity it calls `claimRequested(runId)`; if that claim is empty, it performs at most one immediate `claimNextEligible()` fallback. The fallback never chains, and a notification does not create one directed attempt per free slot.
- **Generic refill lane**: startup, listener reconnect, periodic polling, and run completion create refill opportunities. The worker may create at most one `claimNextEligible()` opportunity per currently free slot. Refill opportunities are coalesced and bounded and never prefetch a `JobRun` or hold a database row before the claim.

The local admission behavior is part of the phase contract:

- The capacity permit is acquired immediately before a claim opportunity and remains held through the claimed run's coordination and execution lifetime. Jitter and cooldown must not consume a run slot while waiting.
- Each worker uses a small independent random jitter. After every successful claim, a bounded cooldown delays that worker's next generic refill opportunity. This cooldown is the primary control against a fast worker monopolizing a sustained backlog.
- Repeated empty or duplicate claims may increase a decaying contention backoff for later generic opportunities. The backoff is capped and must not permanently penalize a worker that loses a race.
- There is no age-based priority escape from the cooldown. A maximum delay and mandatory polling provide the starvation guard; `available_at` and the existing PostgreSQL queue ordering remain authoritative.
- `max-concurrent-runs` remains the worker capacity boundary. With different capacities, the expected share is proportional to the configured slots; the fairness comparison uses normalized busy-slot time rather than raw run count.

Phase 3.4 does not change leases, `available_at`, retry/recovery semantics, cancellation delivery, watermarks, or token-fenced finalization. It does not use SQL random ordering to distribute work and does not treat notification arrival as proof of ownership. The only durable work item remains `JobRun`.

The primary fairness measure is `busy-slot-seconds / max-concurrent-runs` per worker over a sustained-backlog window. Normalized completed-run count is secondary because run sizes and durations differ. Queue age remains an operational signal and must be measured, but it does not override the admission policy. Metrics must identify workers through bounded operational dimensions such as `executor_identity` at the aggregation boundary and must never include run ids, job ids, credentials, lease tokens, or resolved configuration values.

Phase 3.4 exit criteria, all met:

- No worker exceeds its configured concurrent-run capacity, including during duplicate notifications, polling overlap, reconnects, and run completion races.
- Under a sustained backlog, normalized busy-slot time is approximately balanced for workers with equal capacity and proportional for workers with different capacity.
- A directed notification produces no more than one directed claim and one general fallback per worker, while generic refill never exceeds the number of free slots.
- Jitter, cooldown, and contention backoff remain bounded, do not hold execution slots while waiting, and do not prevent eligible work from being discovered by startup, reconnect, or periodic polling.
- Duplicate notifications, missed notifications, worker loss, cancellation, retry recovery, stale-worker fencing, and incremental watermark commit retain the Phase 3.2 behavior.
- A reproducible multi-worker test records aggregate busy-slot time, claim outcomes, queue age, and throughput for enough work and time to distinguish distribution from a single scheduling race.

#### Phase 3 dependency and overall exit criteria

```text
Plan 3.1: state, retry, lease token, fencing
        |
        v
Plan 3.2: worker profile, listener, polling, heartbeat
        |
        v
Plan 3.3: Quartz JDBC HA, shared security, metrics, chaos/load
     |
     v
Phase 3.4: hybrid worker load distribution
```

Plan 3.1 is a prerequisite for Plan 3.2. Plan 3.3 depends on both and provides the operational measurement foundations for Phase 3.4. Phase 3.4 depends on the worker claim/runtime contract and the observability required to measure normalized busy-slot time. The overall phase is complete; its exit criteria are:

- Worker loss produces a new, fully independent attempt within the configured lease/recovery window or a terminal `FAILED` row after the attempt limit.
- A stale worker cannot mutate PostgreSQL state or advance a watermark after fencing.
- A missed or duplicated notification has the same result as normal polling.
- A merge longer than the lease duration does not trigger a duplicate claim while the heartbeat is healthy.
- Multiple API instances do not duplicate Quartz schedule firings or bypass shared login throttling.
- The standalone CLI artifact and its no-PostgreSQL execution path remain unchanged.

### Phase 4: Reusable Managed Datasources

**Status**: APPROVED FOR PLANNING; NOT IMPLEMENTED

#### Objective

The managed server currently stores a complete source and sink connection inside every `JobDefinition`. This repeats connection strings, users, passwords, authentication settings, and technical parameters across jobs and makes credential rotation unnecessarily repetitive. Phase 4 introduces reusable managed datasources so a job selects an existing source and sink profile while retaining only replication-specific configuration.

This phase is server-only. The standalone CLI is not converted into a datasource client and does not read the server datasource catalog. Its existing command-line arguments, options-file keys, launchers, exit codes, multi-table behavior, and no-PostgreSQL execution path remain unchanged.

The managed server is not in production and no managed jobs need to survive this change. The new datasource-only contract therefore reuses `/api/v1`, resets the managed metadata state, and removes the old inline connection model. There is no legacy job backfill, no dual-read compatibility path, and no period in which one managed job can contain both inline connections and datasource references.

#### Locked design

- `ManagedDataSource` is the internal Java domain name. The public API and frontend call the resource `datasource`; this avoids ambiguity with `javax.sql.DataSource` without making the product vocabulary less clear.
- A managed datasource contains safe connection metadata and technical settings: connector type, a redacted display connection string, and non-secret `technicalParams`. Its encrypted `connect.security` bundle contains the actual connection string when it may contain credentials, user/password values, Azure security values, and any sensitive `*.connect.parameter.*` values. File-format and non-secret Kafka producer settings remain in `technicalParams` because the existing CLI parser already consumes them from `source.connect.parameter.*` and `sink.connect.parameter.*`.
- A managed job contains `sourceDatasourceId` and `sinkDatasourceId`, source/sink tables or queries, filters, columns, staging settings, replication mode, watermark, retry policy, and execution tuning. It contains no source or sink credentials.
- Datasource references are live. A datasource update applies to the next run. A `PENDING` run resolves both profiles when it is claimed, not when it is inserted. The PostgreSQL claim transaction locks the eligible run, the job binding, and both datasource rows, records the selected IDs and `datasources_resolved_at`, and returns the encrypted bundle snapshot for in-memory decryption after commit. An active attempt keeps that resolution and is not changed halfway through execution.
- Datasource configuration is not versioned in this phase. Runs and audit events retain datasource identifiers and `datasources_resolved_at` for temporal correlation, but not a complete configuration snapshot or secret material. Exact historical reconstruction after a concurrent update is deferred.
- A datasource can be selected as source or sink only when the authoritative connector capability catalog permits that role. Capabilities are derived from the connector type and core manager contract, not stored as user-editable JSON.
- A datasource delete is restricted while any job references it. No automatic detachment, credential copy, or conversion to inline configuration is allowed.
- The datasource ACL grants users `VIEW`, `USE`, and `EDIT`. ADMIN keeps the global bypass and manages datasource permissions in the first version. `USE` permits a user to select or change a datasource binding; it is not a substitute for the job's own execution permission.
- `job_definition` stores `source_datasource_use_enabled` and `sink_datasource_use_enabled`. These flags are the job-level use authorization. Disabling either flag blocks new manual runs, scheduled runs, and worker claims for that job. Pending rows remain durable but are not claimable while the binding is disabled; a currently `RUNNING` attempt is not cancelled automatically.
- A job binding change requires job edit permission and datasource `USE` permission. Manual execution still requires job `EXECUTE`; scheduling and worker execution use the active binding flags as the durable authorization boundary.
- The server reuses connection configuration, not live JDBC connections. Every core task continues to own its source and sink connection lifecycle.
- The logical `connect.security` map uses relative CLI property keys: `connect`, `user`, `password`, `auth.*`, and sensitive `connect.parameter.*` entries. It is encrypted as one authenticated bundle in PostgreSQL. `technicalParams` contains only non-secret values such as file format, Kafka topic/partition/acks, S3 mode, or driver tuning; the validator rejects secret-looking keys and values in that map.
- The initial keyring is loaded from `replicadb.security.master-key-file` (default `/run/secrets/replicadb-master-key`). It contains a current key version and Base64-encoded 256-bit key material, with previous versions available during rotation. No source or sink credential is loaded from process environment variables. If the keyring is missing or invalid, the `api` or `worker` process fails startup.
- The managed execution path never creates an options file. A server-side resolver decrypts the bundle in memory and calls the additive root-artifact `ToolOptionsBuilder`; only standalone CLI invocations use `OptionsFile` and environment expansion.

#### Target flow

```text
Datasource catalog                         Job definition
-------------------                        ------------------------
ManagedDataSource A -- sourceDatasourceId  tables, query, mode,
ManagedDataSource B -- sinkDatasourceId    watermark, staging, retry
                         |                                         |
                         +-------------------+---------------------+
                                                                           v
                                                  resolve at claim time
                                                                           |
                                                                           v
                                         ToolOptionsBuilder in memory
                                                                           |
                                                                           v
                                                  ToolOptions -> ReplicaDB core
```

#### Persistence model

The canonical managed schema after the pre-production reset is conceptually:

```text
managed_datasource
     id, name, connector_type, safe_connect_display
     technical_params JSONB              -- non-secret only
     encrypted_security BYTEA            -- connect.security bundle
     encryption_algorithm, key_version
     created_at, updated_at

datasource_permission
     datasource_id, user_id, permission, created_at

job_definition
     source_datasource_id, sink_datasource_id
     source_datasource_use_enabled, sink_datasource_use_enabled
     source/sink endpoint-specific fields
     mode, watermark, retry policy, execution settings

job_run
     ... existing run state ...
     resolved_source_datasource_id, resolved_sink_datasource_id
     datasources_resolved_at
```

The next forward-only Flyway migration must create the datasource and ACL tables, add restrictive foreign keys and indexes, add the two datasource references and two use flags to `job_definition`, add the resolved datasource fields to `job_run`, and remove the inline connection/authentication/parameter columns. `managed_datasource` stores only safe display metadata, non-secret technical parameters, and the encrypted `connect.security` envelope; no credential column may remain plaintext. It must first assert that `job_definition` contains no rows. If an existing development database contains managed jobs, migration fails with a reset instruction instead of silently deleting or deduplicating them. A clean database applies the new contract; no old job, run, schedule, permission, or idempotency state is migrated.

#### Capability and validation boundary

The existing core identifies managers from the connection scheme and receives `DataSourceType.SOURCE` or `DataSourceType.SINK`. Phase 4 must expose or reuse an authoritative capability adapter rather than copy a second, editable capability matrix into the server. The capability contract must cover source/sink role support and job-mode restrictions, including the existing Kafka sink-only and Denodo source-only behavior and sink limitations for `complete-atomic`.

The connector type is safe metadata for the API and frontend. The connection string remains the value passed to the core. Creation and update must reject or normalize a connector type that does not match the connection scheme. A custom connection is valid only when the server can classify its scheme as a supported generic manager; a later connection failure must not be represented as a successful capability check.

#### API and frontend boundary

The API adds CRUD and ADMIN-managed datasource permission endpoints under `/api/v1/datasources`. `DatasourceRequest` accepts transient plaintext security values only over authenticated TLS and an explicit `clearSecurityKeys` list; omitted or blank security values preserve existing encrypted entries on update. Datasource responses contain only safe metadata: redacted display connection string, connector type, redacted technical parameters, capability summary, configured-state flags, and caller permissions. They never contain security-bundle values, encryption envelopes, key references, or resolved credentials.

`JobDefinitionRequest` and `JobDefinitionResponse` replace the current inline source/sink connection fields with datasource identifiers and safe datasource summaries. The generated OpenAPI schema remains the only frontend contract. `ConnectionSettingsCard` moves from the job editor to the datasource editor; `JobFormPage` becomes a selector for source and sink datasources and keeps only endpoint-specific replication settings.

Datasource list queries must apply ACL and capability filtering in the backend before pagination. The frontend may disable an unavailable option for usability, but it is never the authorization boundary. A user with job `VIEW` but without datasource `VIEW` must not receive datasource connection metadata.

#### Execution and race semantics

Manual triggers and Quartz triggers reject a job whose source or sink use flag is disabled and do not create a new run. Worker claim SQL must join `job_definition` and exclude disabled bindings, so a pending row cannot bypass a disabled relationship. The disable operation and the claim boundary must be serialized or expressed through conditional database updates so the state transition is race-safe. If a claim has already linearized before the disable, its `RUNNING` attempt may finish; disabling the binding is not implicit cancellation.

`JobExecutionService`, `ScheduledRunTriggerJob`, and the worker execution path load the current datasource records at claim/resolution time before `ToolOptionsBuilder` creates the in-memory options object. The notification payload remains only the run UUID. A datasource update or permission change is read from PostgreSQL on the next relevant operation; no API-local or worker-local datasource cache is authoritative. The managed path does not create an options file; the CLI options-file path remains standalone and unchanged.

#### Phase 4 implementation tasks

##### 1. Define the datasource domain and capability contract

- [ ] **1.1 Add `ManagedDataSource`, connector types, datasource references, and capability classification**
     Files: `replicadb-server/src/main/java/org/replicadb/server/job/domain/ManagedDataSource.java` (new), `ConnectorType.java` (new), `DataSourceCapabilities.java` (new or equivalent), `DataSourceCapabilityCatalog.java` (new or shared core adapter), `ConnectionCredentials.java`, `SourceEndpoint.java`, `SinkEndpoint.java`, `JobDefinition.java`
     Changes: Reuse the existing immutable connection value objects where appropriate, but move all managed credential-bearing values into a `connect.security` bundle protected by the datasource encryption service. Replace embedded endpoint credentials in the managed domain with datasource UUID references and source/sink use flags. Keep endpoint-specific fields on `SourceEndpoint`/`SinkEndpoint`. Define lower-case wire values matching the frontend connector builder and classify source/sink support from the existing manager contract. Do not add capability JSON or any CLI datasource concept.
     Tests: Validate datasource names, connector types, encrypted security-bundle shape, immutable technical parameters, source/sink capability decisions, custom-scheme handling, Kafka/Denodo role restrictions, and mode-specific sink restrictions. Assert that a managed `JobDefinition` cannot be built without both datasource identifiers or with a disabled binding that is treated as executable.
     Dependencies: None

##### 2. Protect datasource secrets at rest

- [ ] **2.1 Add the encrypted `connect.security` bundle and key-provider boundary**
     Files: `replicadb-server/src/main/java/org/replicadb/server/security/secret/EncryptedSecurityBundle.java` (new), `SecretProtectionService.java` (new), `KeyEncryptionKeyProvider.java` (new), `FileBackedKeyEncryptionKeyProvider.java` (new), `SecretProtectionProperties.java` (new), `replicadb-server/src/main/resources/application-api.yml`, `application-worker.yml`, server `pom.xml` if a vetted crypto dependency is required
     Changes: Implement application-level AES-256-GCM envelope encryption for a canonical security map. Generate a fresh 256-bit data-encryption key and random 96-bit nonce per bundle, bind ciphertext with authenticated data containing the datasource UUID and bundle format version, and wrap the data key with the external key-encryption key. Validate a Base64 256-bit key loaded from the configured Kubernetes/Docker Secret file, fail server startup when it is absent/unreadable/invalid, and expose algorithm/key versions for rotation. Keep the provider interface replaceable by KMS, Vault, or CyberArk later. Never log plaintext, ciphertext, keys, or request bodies.
     Tests: Cover encryption/decryption round trips, unique nonces/data keys, authenticated tamper and wrong-datasource rejection, malformed bundle rejection, key-version selection, missing/invalid key startup failure, file-backed configuration, provider replacement through the interface, and absence of secret material in logs/exceptions.
     Dependencies: Task 1.1

- [ ] **2.2 Define security-key classification and update semantics**
     Files: `ManagedDataSource.java`, `DatasourceRequest.java`, `DatasourceMapper.java`, `SecretProtectionService.java`, security/domain tests
     Changes: Define the logical `connect.security` keys for `connect`, `user`, `password`, Azure security fields, MongoDB credential-bearing URIs, S3 `accessKey`/`secretKey`, Kafka SASL/SSL secrets, and sensitive arbitrary `connect.parameter.*` values. Keep non-secret driver/file/Kafka tuning values in `technical_params`. Reject secret-looking entries in technical parameters and reject embedded credentials in any unencrypted display value. On update, omitted or blank security fields preserve the existing encrypted entries; an explicit `clearSecurityKeys` list removes selected entries. Responses expose only configured booleans and redacted display metadata.
     Tests: Verify every supported manager's sensitive keys are classified into the encrypted bundle, MongoDB full-URI handling, S3/Kafka/Azure security mapping, technical-parameter rejection, blank-preservation, explicit clearing, redaction, and no secret values in DTOs or audit details.
     Dependencies: Task 2.1

##### 3. Define live-resolution and safe JobRun semantics

- [ ] **3.1 Define claim-time datasource resolution and run correlation**
     Files: `replicadb-server/src/main/java/org/replicadb/server/job/domain/ResolvedJobDefinition.java` (new or equivalent), `JobRun.java`, `JobRunRepository.java`, `JobRunStore.java` or a new preparation port, resolution tests
     Changes: Define the in-memory value created by resolving a job's current datasources at claim time. Lock the eligible run, binding, and both datasource rows in one PostgreSQL transaction; record `resolved_source_datasource_id`, `resolved_sink_datasource_id`, and `datasources_resolved_at`; return the encrypted bundle snapshot; then decrypt after commit. If an update commits first, the pending run uses the new bundle; if the claim commits first, the active attempt uses the snapshot read by that claim. A retry resolves again. Do not persist a complete configuration or secret.
     Tests: With concurrent PostgreSQL transactions, prove update-vs-claim ordering, pending-run claim-time resolution, active-attempt immutability, retry re-resolution, persisted ID/timestamp correlation, and no secret material in `JobRun` or notifications.
     Dependencies: Tasks 1.1, 2.1, and 2.2

##### 4. Replace managed inline connection persistence

- [ ] **4.1 Add the datasource schema and pre-production reset migration**
     Files: `replicadb-server/src/main/resources/db/migration/V17__create_managed_datasource.sql` (new or next migration), `V18__replace_inline_job_connections.sql` (new or equivalent), migration tests
     Changes: Create `managed_datasource` with unique names, safe display metadata, JSONB non-secret technical parameters, encrypted security-bundle storage, algorithm/key-version metadata, and `datasource_permission`. Add `source_datasource_id`, `sink_datasource_id`, `source_datasource_use_enabled`, and `sink_datasource_use_enabled` to `job_definition`, plus the resolved datasource IDs and resolution timestamp to `job_run`, with restrictive foreign keys and lookup indexes. Assert no existing `job_definition` rows before dropping the old inline connection/auth/parameter columns. Do not backfill, deduplicate, or silently delete jobs. Keep Flyway forward-only and make a non-empty development database fail with an actionable reset message.
     Tests: Run the complete migration chain in PostgreSQL Testcontainers, verify the clean-schema shape, restrictive delete behavior, name uniqueness, permission constraints, indexes, no inline connection columns, and an explicit failure when old managed jobs are present. Verify no root CLI table or dependency is involved.
     Dependencies: Task 1.1

- [ ] **4.2 Update repositories and application ports**
     Files: `ManagedDataSourceRepository.java` (new), `DataSourcePermissionRepository.java` (new), `JobDefinitionRepository.java`, `JobDefinitionRowMapper.java`, `JobDefinitionStore.java` or new datasource port, related integration tests
     Changes: Persist and load encrypted security bundles through a dedicated `SecretProtectionService`, serialize only non-secret technical parameters through the existing ObjectMapper JSONB pattern, map only datasource IDs and use flags in `JobDefinition`, and implement page/count queries with backend visibility and capability restrictions before pagination. Return explicit outcomes for not found, forbidden, invalid ciphertext, and referenced-on-delete cases.
     Tests: Round-trip datasource metadata, encrypted credentials, and parameters; preserve blank-secret update semantics; prove decrypt/re-encrypt behavior and key-version handling; prove restricted deletes; verify concurrent datasource reads/updates; and prove non-admin list results cannot include unauthorized profiles or bypass capability filters.
     Dependencies: Tasks 1.1, 2.1, 2.2, and 4.1

##### 5. Replace the managed REST contract

- [ ] **5.1 Add datasource CRUD and safe mapping**
     Files: `DatasourceController.java` (new), `DatasourceRequest.java` (new), `DatasourceResponse.java` (new), `DatasourceMapper.java` (new), `GlobalExceptionHandler.java`, OpenAPI/controller tests
     Changes: Add `POST`, paginated `GET`, `GET {id}`, `PUT`, and restrictive `DELETE` under `/api/v1/datasources`. Use RFC 7807 errors, redacted connection strings/parameters, password-configured booleans, blank-password preservation on update, capability summaries, and caller permission flags. Return `409` when a datasource is referenced by jobs. Do not add a public connection-test endpoint in this phase.
     Tests: Cover validation, redaction, create/update/delete, pagination, duplicate names, referenced delete conflicts, forbidden access, admin bypass, and absence of password references/resolved values from every response and problem detail.
     Dependencies: Tasks 1.1, 2.1, 2.2, and 4.2

- [ ] **5.2 Convert job endpoints to datasource-only references**
     Files: `JobDefinitionRequest.java`, `JobDefinitionResponse.java`, `JobDefinitionMapper.java`, `JobDefinitionController.java`, `JobDefinitionRepository.java`, OpenAPI tests
     Changes: Remove managed inline source/sink connection fields from request/response/domain mapping and require `sourceDatasourceId` and `sinkDatasourceId`. Validate datasource existence, role capability, caller `USE`, and active binding flags. Return selected IDs and safe summaries without duplicating connection credentials. Reuse `/api/v1` under the explicit pre-production reset rule.
     Tests: Assert datasource-only create/update behavior, missing or incompatible datasource errors, source/sink capability checks, binding permission checks, safe responses, and no acceptance of the old inline payload shape.
     Dependencies: Tasks 4.2 and 5.1

##### 6. Add datasource authorization and audit coverage

- [ ] **6.1 Add datasource ACLs and binding authorization**
     Files: `DataSourcePermissionType.java` (new), `DataSourceAccessService.java` (new), `DataSourcePermissionController.java` (new), `DataSourcePermissionRepository.java`, `JobAccessService.java`, security tests
     Changes: Implement `VIEW`, `USE`, and `EDIT` user permissions, ADMIN bypass, backend filtering before pagination, and ADMIN-managed permission replacement/removal endpoints. Require datasource `USE` plus job `EDIT` for binding changes. Store the job-level source/sink use flags and expose an explicit operation to disable or re-enable each binding. Manual triggers require job `EXECUTE`; scheduled and worker execution require both active binding flags.
     Tests: Exercise the full user/job/datasource permission matrix, including a user who can view a job but cannot see datasource metadata, a user who can view but not use a datasource, binding disablement blocking manual and scheduled runs, and ADMIN bypass behavior.
     Dependencies: Tasks 4.2 and 5.2

- [ ] **6.2 Extend audit actions and safe operational detail**
     Files: `AuditAction.java`, `AuditResourceType.java`, datasource controllers/services, audit tests
     Changes: Add datasource create/update/delete and permission-change vocabulary. Audit only datasource id, safe name, connector type, binding side, permission, outcome, and bounded timestamps. Never audit password references, connection strings, connection parameters, resolved options, or certificate/key contents. Preserve audit retention and fail-open behavior.
     Tests: Verify every state-changing datasource and binding operation emits the expected event, deletion conflicts do not claim success, user deletion preserves actor text, and secret-like values are absent from persisted JSONB details and API audit responses.
     Dependencies: Tasks 5.1 and 6.1

##### 7. Resolve datasources in every managed execution path

- [ ] **7.1 Add the programmatic `ToolOptionsBuilder` without changing the CLI parser**
     Files: `src/main/java/org/replicadb/cli/ToolOptionsBuilder.java` (new), `src/main/java/org/replicadb/cli/ToolOptions.java`, `src/test/java/org/replicadb/cli/ToolOptionsBuilderTest.java` (new), root CLI compatibility tests
     Changes: Add an additive builder/factory in the root artifact for programmatic construction of a valid `ToolOptions` object. Support every field required by the managed job and datasource contract, including source/sink connect strings, users/passwords, Azure authentication, connection parameter maps, tables/query/filters, staging, modes, watermark, fetch size, throttling, flags, and verbosity. Build directly in memory with defensive copies and the same validation/default semantics as the CLI. Keep the CLI constructor, Commons CLI parsing, options-file expansion, accepted keys, multi-table behavior, exit codes, and root dependency graph unchanged; do not log builder values.
     Tests: Round-trip all programmatic fields through getters, verify required/mode/default validation, immutable parameter copies, Azure and manager-specific security properties, no options-file creation, unchanged CLI/options-file tests, and packaged root artifact absence of Spring Boot/server dependencies.
     Dependencies: None

- [ ] **7.2 Resolve profiles before in-memory ToolOptions construction**
     Files: `JobExecutionService.java`, `JobDefinitionOptionsFileWriter.java` (remove from managed path), `ToolOptionsBuilder.java`, `ScheduledRunTriggerJob.java`, `RunExecutionCoordinator.java`, worker execution services, new resolution service and tests
     Changes: Load both datasource records at the claim/resolution boundary, verify binding flags and connector capabilities, record resolved datasource IDs and timestamp on the run, decrypt the security bundle in memory, construct an in-memory resolved definition, and pass it to `ToolOptionsBuilder`. Do not create or delete a managed options file. Do not change `ReplicaDB.processReplica(ToolOptions)` or core manager connection ownership. Keep the old CLI options-file path unchanged for standalone invocations.
     Tests: Run successful, failed, cancelled, retry, and worker executions through encrypted datasource references; verify current profiles are used by later claims, active attempts keep their initial resolution, missing/disabled profiles and decryption failures stop before core execution, no temporary managed file is created, and no resolved configuration is logged or persisted.
     Dependencies: Tasks 3.1, 4.2, 5.2, and 7.1

- [ ] **7.3 Make dispatch and claims honor binding disablement atomically**
     Files: `JobRunRepository.java`, `JobRunStore.java`, `RunLeaseService.java`, `JobRunController.java`, `ScheduledRunTriggerJob.java`, worker claim/recovery paths, state/integration tests
     Changes: Prevent manual and scheduled trigger insertion when either job-datasource use flag is disabled. Make eligible worker claims join the job definition and exclude disabled bindings. Serialize flag changes and claim decisions at the database boundary. Keep pending rows durable but unclaimable while disabled, allow explicit cancellation, and do not automatically cancel a running attempt. Ensure retries and lease-recovery replacements re-check the binding flags.
     Tests: Use concurrent PostgreSQL transactions to prove a disabled binding cannot be bypassed by manual trigger, Quartz trigger, worker polling, notification, retry, or lease recovery. Prove a run already claimed before disablement can finish, and re-enabling makes still-pending work eligible again without creating a duplicate run.
     Dependencies: Tasks 6.1 and 7.2

##### 8. Replace connection editing with datasource selection in the frontend

- [ ] **8.1 Add datasource API modules and generated contract coverage**
     Files: `replicadb-server/frontend/src/api/datasourcesApi.ts` (new), `datasourcesApi.test.ts` (new), generated `schema.ts`, `schema.test.ts`
     Changes: Add typed CRUD, pagination, safe permission/capability fields, and datasource-permission API methods using the generated OpenAPI types. Use TanStack Query keys scoped to datasource lists/details/permissions and invalidate them after mutations. Do not duplicate DTO definitions or include password/reference fields in frontend state.
     Tests: Assert request normalization, blank-password omission, redacted response typing, pagination, capability filtering, permission mutations, RFC 7807 errors, and generated schema drift checks.
     Dependencies: Tasks 5.1 and 6.1

- [ ] **8.2 Build datasource list, editor, and permission screens**
     Files: `replicadb-server/frontend/src/pages/DatasourcesPage.tsx` (new), `DatasourceFormPage.tsx` (new), `DatasourcePermissionsPage.tsx` (new), routes, `AppLayout.tsx`, reused `ConnectionSettingsCard.tsx`, frontend tests and Playwright flows
     Changes: Move connector selection, connection builder, authentication, file settings, Kafka settings, and technical parameters into the datasource editor. Show only safe redacted values in detail/list screens. Add datasource ACL management under the ADMIN route and clear disabled/in-use states. Reuse existing MUI, TanStack Query, protected-route, and error patterns.
     Tests: Cover create/edit/password preservation, type-specific fields, source/sink capability display, forbidden screens, ACL changes, delete conflicts, responsive layout, and an ADMIN browser flow without credentials in fixtures.
     Dependencies: Task 8.1

- [ ] **8.3 Convert job editor and detail views to datasource pickers**
     Files: `JobFormPage.tsx`, `JobDetailPage.tsx`, `jobsApi.ts`, job page tests, route/e2e tests
     Changes: Replace source/sink connection fields with datasource selectors filtered by capability and user `USE`. Keep tables, queries, filters, staging, modes, watermarks, retry policy, and execution settings in the job form. Show datasource names and connector types in job details without exposing connection values. Expose binding enable/disable actions according to backend permissions and explain that disabling blocks future manual and scheduled runs.
     Tests: Assert no inline password/connection controls remain in the job editor, selected IDs are sent, unauthorized or wrong-role profiles cannot be submitted, binding disablement is reflected in the UI, and existing job/run workflows remain usable.
     Dependencies: Tasks 5.2, 6.1, and 8.1

##### 9. Preserve the standalone CLI boundary

- [ ] **9.1 Prove datasource work does not alter the CLI artifact or options contract**
     Files: root `pom.xml`, root CLI tests, `ToolOptions`/`OptionsFile` compatibility tests, `NoSpringBootOnClasspathTest.java`, `CliOfflineExecutionTest.java`, packaging/CLI scripts
     Changes: Keep all datasource domain, API, PostgreSQL, and frontend dependencies inside `replicadb-server`. Do not add datasource IDs or server lookups to CLI options. Re-run the packaged Spring-free artifact inspection, exit-code tests, options-file precedence, legacy single-table properties, incremental watermark properties, multi-table options, and SQLite execution with metadata unavailable.
     Tests: Assert the root artifact has no Spring Boot or datasource-catalog dependency, accepts every existing connection property unchanged, runs without PostgreSQL, and preserves exit codes `0`, `1`, and `2`.
     Dependencies: Tasks 7.2 and 8.3

##### 10. Document and validate the end-to-end contract

- [ ] **10.1 Add integration, worker, frontend, and operational acceptance gates**
     Files: `ARCHITECTURE_DECISIONS.md`, `DEPLOYMENT.md`, `README.md`, server/frontend test suites, Testcontainers fixtures, Compose or validation scripts
     Changes: Document the datasource lifecycle, ACL matrix, live-reference boundary, reset requirement, restrictive deletion, binding flags, capability catalog, no-pooling rule, secret handling, and the separation from CLI configuration. Validate API and worker profiles, Quartz dispatch, remote cancellation, retries, watermarks, audit retention, and packaging with datasource references.
     Tests: Run migration, repository, MockMvc, security, worker, distributed PostgreSQL, frontend unit, Playwright, packaged CLI, Compose, documentation, and `git diff --check` gates. Include a sustained scenario proving a datasource update affects the next run, binding disablement blocks every future trigger path, and a referenced datasource cannot be deleted.
     Dependencies: Tasks 4.1 through 9.1

#### Phase 4 exit criteria

- Every managed job uses required source and sink datasource identifiers; no managed API or database path accepts inline connection credentials.
- A datasource owns connection/authentication/technical settings once, and jobs retain only endpoint-specific replication settings.
- All credential-bearing values, including MongoDB URI credentials, S3 keys, Kafka security parameters, and Azure security values, are encrypted before PostgreSQL persistence in the `connect.security` bundle. The AES-256-GCM key-encryption key is external to PostgreSQL and loaded from the configured mounted Secret; missing or invalid key material prevents startup.
- Live updates affect the next claim, while an active attempt uses one atomically resolved in-memory configuration. `JobRun` records only the datasource IDs and resolution timestamp, and no plaintext or encrypted secret bundle is persisted or emitted outside the datasource store.
- Datasource ACLs are enforced in the backend, datasource `USE` is required for binding changes, and job-level use flags block manual, scheduled, retry, recovery, and worker execution when disabled.
- Source/sink and mode capabilities are derived from the authoritative core manager contract, with no editable capability matrix in PostgreSQL.
- Deletion is rejected while referenced, with no automatic detachment or credential duplication.
- Multiple API instances and workers use the same PostgreSQL datasource state without local authoritative caches, notification payloads, or connection sharing.
- The frontend edits datasources in a dedicated section and jobs select safe datasource summaries; generated OpenAPI types remain synchronized.
- The standalone CLI artifact, options-file contract, exit codes, multi-table behavior, and no-metadata-database execution remain unchanged.

## Implementation Priorities

### Priority 1: Core and State Contract

- [x] Replace the static staging-name and temp-file state with a per-run execution context. **Completed in Phase 0-a (`c228ddc`).**
- [x] Widen the `ReplicaTask` result to carry counters, timings, and a watermark candidate. **Counters and timings completed in Phase 0-a (`c228ddc`); watermark candidate population completed in Phase 0-b2.**
- [x] Add cancellation: statement handles, interrupt checks, cancellable futures. **Implemented in Phase 0-b1 (commit `4dd4cb5`).**
- [x] Implement watermark injection with type inference from source column metadata. **Implemented in Phase 0-b2.**
- [x] Define `JobDefinition` and `JobRun` persistence models and Flyway migrations. **Completed in Phase 1b** (`JobDefinition`/`JobRun` records, `V1__create_job_definition.sql`, `V2__create_job_run.sql`).
- [x] Define legal state transitions, retry behavior, and idempotency rules. **Completed in Phase 1b** (`JobRunStateMachine`, `JobRunRepository.claimNextPending(...)`/`scheduleRetry(...)`).
- [x] Split the build into the `replicadb` and `replicadb-server` artifacts. **Completed in Phase 1a.**
- [x] Preserve CLI behavior, including multi-table options files. **Continuously verified**; the Phase 1b state-layer additions do not touch `ToolOptions`'s CLI/options-file contract.
- [x] Add focused tests for concurrent runs, cancellation, and watermark advancement. **Completed across Phase 0-a, Phase 0-b1, and Phase 0-b2.**

### Priority 2: Monolithic Control Plane

- [x] Add the `/api/v1` REST API for job and run management. **Completed in Phase 1c-1** (`JobDefinitionController`, `JobRunController`).
- [x] Reject multi-table definitions at the API boundary with an explicit error. **Satisfied by construction since Phase 1b**: `JobDefinition`/`JobDefinitionRequest` model exactly one source/sink table pair, so the Phase 1c-1 API has no field through which a multi-table definition could be submitted.
- [x] **Phase 1c-2.** Add Quartz scheduling with an explicit timezone per job. **Completed in Phase 1c-2** (`JobSchedule`, `JobScheduleRepository`, `ScheduledRunTriggerJob`, `QuartzScheduleService`, `ScheduleReconciler`, and schedule endpoints).
- [x] Add asynchronous execution and monitoring. **Completed in Phase 1c-1**: `RunExecutionCoordinator` executes runs on a bounded pool without blocking the triggering request, and `GET /api/v1/runs`/`GET /api/v1/runs/{id}` expose status, counters, and timings for polling.
- [x] Add run history, operational counters, persisted log excerpts, and error details. **Run history, row counters, durations, and error details are persisted since Phase 1b and exposed over HTTP since Phase 1c-1**; the log excerpt endpoint is a stub returning only `error_message`, not the full 256 KB captured log.
- [x] **Phase 1c-1 gap closed in Phase 1c-3c.** Persist the indeterminate-sink warning on cancellation onto the `job_run` row. The cancel response and persisted column carry the same mode-specific warning.
- [x] **Phase 1c-3a+b.** Add local-user authentication with Spring Security and PostgreSQL sessions.
- [x] **Phase 1c-3a+b.** Add global roles `ADMIN`, `OPERATOR`, and `VIEWER`.
- [x] **Phase 1c-3a+b.** Add per-job ACLs for `VIEW`, `EDIT`, `EXECUTE`, and `CANCEL`.
- [x] **Phase 1c-3c.** Add audit events and the 365-day audit retention purge. **Implemented** with `audit_event`, `AuditService`, the ADMIN-only audit history endpoint, and `AuditRetentionTask`.
- [x] Keep credentials outside persisted job payloads. **Enforced in Phase 1b**: `JobDefinition`'s compact constructor rejects a `sourcePassword`/`sinkPassword` that is not `null` or an `${env:VARIABLE}` reference, and rejects connection strings with embedded credentials, so `job_definition` never holds a literal secret.

### Priority 3: Frontend Rollout

- [x] **Phase 2a.** Add authentication and read-only monitoring screens (login, dashboard, job detail, run history/detail) to the `replicadb-server` package.
- [x] **Phase 2b.** Add the job editor, schedule management, and mutating run actions (trigger/cancel/retry) to the frontend.
- [x] **Phase 2c.** Add the job permission editor and user/role administration screens to the frontend.

### Priority 4: Phase 3 Distributed Runtime

- [x] **Phase 3.1.** Define the distributed state contract: per-job retry policy, `available_at`, lease tokens, atomic claims, lease renewal, expiry recovery, fencing, and mode-specific automatic-retry defaults.
- [x] **Phase 3.2.** Add the isolated `worker` profile, shared execution service, transactional `pg_notify` dispatch, dedicated listener, reconnect logic, mandatory polling, remote cancellation, and heartbeat during merge/swap. **Implemented** with the Phase 3.2 runtime and PostgreSQL integration tests described above.
- [x] **Phase 3.3.** Add Quartz JDBC clustering for multiple API instances, shared login throttling, health/metrics, deployment documentation, multi-node integration tests, and reproducible load/chaos checks. **Completed and validated on August 25, 2026; GitHub Actions passed the server, integration, frontend, multinode, and release gates.**
- [x] **Phase 3.4.** Add hybrid worker load distribution: directed notifications with one bounded fallback, free-slot refill, wake-up coalescing, bounded jitter, success cooldown, decaying contention backoff, and normalized busy-slot observability. Preserve PostgreSQL claim/fencing semantics and validate approximate fairness under sustained backlog. **Completed and validated on August 26, 2026.**
- [x] Preserve the CLI artifact, CLI exit codes, options-file contract, and no-metadata-database execution path throughout all Phase 3 slices. **Completed and validated with packaged Spring-free JAR inspection, SQLite execution, and exit-code tests.**

### Priority 5: Reusable Managed Datasources

- [ ] **Phase 4.1.** Replace managed inline source/sink connection data with reusable `ManagedDataSource` profiles and required `sourceDatasourceId`/`sinkDatasourceId` job references. Derive connector capabilities from the core manager contract and preserve per-task connection ownership.
- [ ] **Phase 4.2.** Add AES-256-GCM envelope encryption with an external mounted key, the pre-production PostgreSQL reset migration, datasource repositories, restrictive foreign keys, datasource ACLs (`VIEW`/`USE`/`EDIT`), job-level binding-use flags, and race-safe claim/trigger enforcement.
- [ ] **Phase 4.3.** Replace the managed REST contract and frontend connection fields with datasource CRUD, safe datasource summaries, datasource permission administration, and source/sink datasource selectors. Reuse `/api/v1` under the explicit pre-production reset rule and keep generated OpenAPI types authoritative.
- [ ] **Phase 4.4.** Resolve live datasource references at claim time, retain only temporal datasource correlation in run/audit state, construct `ToolOptions` through the additive root-artifact builder without a managed options file, and preserve worker UUID-only dispatch and secret redaction.
- [ ] **Phase 4.5.** Validate datasource update visibility on the next run, binding disablement across manual/scheduled/retry/recovery/worker paths, restrictive deletion, capability enforcement, distributed execution, frontend behavior, and standalone CLI compatibility.

---

## Success Metrics

### Phase 0

- **Met:** Two concurrent runs in one JVM produce two distinct staging tables and zero cross-run interference across 100 repetitions.
- **Met:** Task results report row counts and timings, and run-level aggregation reports the total rows and longest task duration.
- A cancellation request halts source reads within 5 seconds at the 95th percentile.
- Control-plane overhead adds no more than 5% to the wall-clock duration of an equivalent CLI run.

### Phase 1

- CLI invocation, exit codes, and existing configuration remain compatible, verified by the existing CLI test suite with zero modifications.
- The `replicadb` artifact contains no Spring Boot classes. **Met** — verified by `NoSpringBootOnClasspathTest`.
- A failed or cancelled run never advances its incremental watermark. **Met at the state layer since Phase 1b** — `JobRunRepository.findLastCommittedWatermark(...)` returns the prior `SUCCEEDED` run's value after a `FAILED`/`CANCELLED` run.
- Restarting the control plane does not lose persisted run state. **Met since Phase 1b** for the persisted `JobDefinition`/`JobRun` rows themselves; a process restart mid-execution still leaves that one run `RUNNING` until Phase 3's lease-expiry recovery reclaims it.
- Monitoring exposes status, counters, timestamps, and failure details. **Met since Phase 1c-1** via `GET /api/v1/runs`/`GET /api/v1/runs/{id}` (log excerpt is a stub, see above).
- Unauthorized users cannot view, edit, execute, or cancel jobs. **Met for Phase 1c-3a+b** — Spring Security requires authentication for `/api/v1`, ADMIN bypasses ACLs, and OPERATOR/VIEWER operations are checked against the per-job permission table in backend services and controllers.
- Administrators can manage users, roles, job permissions, and audit history. **Met in Phase 1c-3c** — state-changing authentication, user, job, permission, schedule, and run actions are recorded, retained for 365 days by default, and readable through ADMIN-only `GET /api/v1/audit`.
- The backend API can create, schedule, trigger, and monitor jobs through `/api/v1`. **Met since Phase 1c-2**; the authenticated frontend UI that exposes this to users is implemented across Phase 2a, Phase 2b, and Phase 2c.
- Credentials are absent from job payloads, state records, API responses, and logs. **Met for state records since Phase 1b, and for API responses since Phase 1c-1** — `JobDefinitionResponse` never includes a literal password (only a boolean "configured" flag) and redacts connection strings via `CredentialRedactor`; log absence for the managed runtime is unchanged from the CLI's existing redaction behavior.
- A replayed `Idempotency-Key` never produces a second run. **Met since Phase 1c-1** — `RunTriggerIdempotencyRepository` returns the original run for a key replayed within 24 hours.

### Phase 2

- **Met:** Authenticated users can log in, view the dashboard, and inspect job/run detail with no create/edit/trigger/cancel/schedule affordance present in Phase 2a.
- **Met:** The job editor, schedule management, and trigger/cancel/retry actions in Phase 2b operate against the same backend contract validated since Phase 1c-1/1c-2, with no API changes required.
- **Met:** The permission editor and user/role administration in Phase 2c use the existing backend ACLs from Phase 1c-3a+b; the frontend never becomes a second source of authorization truth.
- Generated TypeScript types stay in sync with `/api/v1` through the OpenAPI specification, with no hand-maintained DTO duplicating the Java response records.

### Phase 3

- **Met:** Two or more API instances use Quartz JDBC clustering without duplicate schedule firings.
- **Met:** Worker loss preserves the original run as history and creates a new attempt only when the job retry policy permits it.
- **Met:** Expired leases are recovered using PostgreSQL `now()` and never resume the abandoned attempt.
- **Met:** A stale worker cannot renew, finalize, or commit a watermark after its lease token is fenced.
- **Met:** Duplicate notifications and duplicate database polling do not advance a watermark twice.
- **Met:** Under a sustained backlog, normalized busy-slot time is approximately balanced across workers with equal capacity, while workers with different capacities receive proportionally different shares.
- **Met:** A directed notification causes at most one directed claim and one general fallback per worker; generic refill never exceeds the number of free local slots.
- **Met:** Jitter, success cooldown, and adaptive contention backoff remain bounded, do not hold execution slots while waiting, and do not prevent an eligible run from being discovered by mandatory polling.
- **Met:** A worker reconnects and rescans PostgreSQL after a listener failure.
- **Met:** Missed notifications are recovered by startup, reconnect, and periodic polling.
- **Met:** A merge lasting longer than the lease duration never triggers a duplicate claim while heartbeat renewal is healthy.
- **Met:** Automatic recovery is enabled by default only for `complete-atomic` and `incremental`; `complete` remains manual unless explicitly opted in.
- **Met:** Remote cancellation reaches a worker through persisted state and control notification, without an API-local execution registry.
- **Met:** Shared login throttling behaves consistently across API instances.
- **Met:** Notification latency, polling lag, lease recovery, executor capacity, and concurrency limits are measured from reproducible tests.

### Phase 4

- Every managed job uses source and sink datasource identifiers, and no managed request, domain record, database row, worker payload, audit event, or API response contains inline connection credentials.
- A datasource update is visible to the next run while an already-running attempt retains one resolved in-memory configuration; no datasource update mutates active core tasks.
- Datasource ACLs and job binding flags are enforced server-side. Revoking or disabling either binding blocks manual, scheduled, retry, recovery, and worker execution without implicitly cancelling an already-running attempt.
- Connector capabilities are derived from the authoritative core manager contract, and incompatible source/sink or mode combinations are rejected before persistence or claim.
- A referenced datasource cannot be deleted, and no automatic detachment, credential duplication, or hidden fallback to inline configuration occurs.
- Datasource and binding changes have safe audit records with no password references, resolved values, connection strings, technical secret-like parameters, or certificate/key contents.
- API and worker instances resolve the same PostgreSQL-backed datasource state without local authoritative caches, shared live JDBC connections, or credential-bearing notifications.
- The frontend provides a dedicated datasource section and job selectors, while generated OpenAPI types remain synchronized and legacy connection controls are absent from the managed job editor.
- The standalone CLI artifact, options-file contract, multi-table behavior, exit codes, and no-metadata-database execution remain unchanged.

---

## Constraints and Limitations

### Current core

- Phase 0-a removed the static staging-name and temporary-file state. Each `ToolOptions` owns one `ReplicationExecutionContext`; a multi-table copy receives a fresh context.
- `ReplicaTask` now returns `ReplicaTaskResult` with row counts, timings, and a populated watermark candidate; `executeReplicationTasks` reduces those candidates to one run-level value.
- Phase 0-b1 added the per-run cancellation token, active-statement registry, and cancellable futures described above; SQL Server `BulkCopy` and PostgreSQL `COPY` cancellation remain best-effort, and MongoDB/Kafka/ORC lack dedicated mid-stream cancellation tests.
- Phase 0-b2 added watermark predicate injection to the 8 JDBC-based managers (Denodo source-only); MongoDB/Kafka/S3/file managers do not support it. The reduced watermark candidate is exposed on `ReplicationExecutionContext` only after a successful merge.
- Phase 1b added `ReplicationExecutionContext.getRowsProcessed()`/`getDurationMillis()`, populated unconditionally right after the parallel tasks finish (unlike the watermark candidate, which stays conditional on merge success), so `JobExecutionService` can persist row counters and timings for every outcome, not only success.
- `ReplicaDB.processReplica(ToolOptions)` still returns only an exit code (`0`/`1`/`2`); it does not expose the underlying exception. `JobRun.errorMessage` on `FAILED` is therefore a generic message except when `ToolOptions` construction itself throws — widening the core's error contract remains open for a later phase.
- Each parallel task owns its source and sink managers; the state layer must not assume shared JDBC connections.
- Manager capabilities differ by source, sink, and replication mode.
- Multi-table replication stops at the first failure and leaves earlier tables applied; it is a CLI-only capability.

### Execution semantics

- Runs are never resumed. A retry is a full re-execution.
- Partition assignment is not reproducible across executions, so no feature may depend on partition identity.
- Retrying a `complete` run is destructive because the sink is truncated before any write.
- Automatic lease-expiry recovery is enabled by default only for `complete-atomic` and `incremental`; `complete` requires explicit opt-in and retains its destructive warning.
- Lease recovery creates a new `JobRun` with a new lease token; it never resumes or reopens the expired attempt.
- Worker load distribution is approximate. The hybrid policy uses notification-directed claims, one bounded fallback, free-slot refill, coalesced wake-ups, bounded jitter, success cooldown, and decaying contention backoff; it does not provide strict round-robin assignment or perfect equality.
- Cancelling during a merge or atomic swap leaves the sink in an indeterminate state, and the API must say so.
- Watermarks exist only for `incremental` mode, use a single declared column, and never propagate deletes.
- An incremental merge requires primary keys on the source.
- Monitoring counters describe execution; they do not prove source/sink equality.

### Deployment

- PostgreSQL is mandatory for the `api` and `worker` profiles; the CLI does not use it. **Implemented through Phase 3.4**: `application-api.yml` and `application-worker.yml` wire `spring.datasource`/`spring.flyway`, and the current `job_definition`, `job_run`, `job_schedule`, `audit_event`, cancellation-warning, retry-policy, eligibility, lease-fencing, Quartz, and login-throttle schema plus supporting indexes are versioned by Flyway migrations V1 through V16. **Planned for Phase 4**: the managed datasource catalog, datasource ACLs, job datasource references, and binding-use flags are added by forward-only migrations V17 and later under the explicit pre-production reset rule. The worker dispatch, hybrid admission, Quartz JDBC clustering, shared throttle runtime, and future datasource catalog remain isolated from the CLI artifact.
- SQLite is limited to isolated CLI fixtures or unit tests.
- The CLI remains available in every implementation phase and deployment model.
- The `api` profile may run as multiple stateless instances; Quartz uses PostgreSQL JDBC clustering in Phase 3.
- The `worker` profile has no public login, REST API, frontend, session bootstrap, or Quartz scheduler.
- PostgreSQL `LISTEN/NOTIFY` is a wake-up signal, not a durable queue; polling recovery is mandatory.
- Workers require a dedicated listener connection and must reconnect and re-subscribe after failure.
- Lease and heartbeat timestamps come from PostgreSQL `now()`, never from worker clocks.
- PostgreSQL row locking and leases must prevent two workers from claiming the same run.
- All lease-owned updates require the current opaque lease token; stale workers must be fenced from state and watermark writes.
- `available_at` controls retry eligibility and is evaluated with PostgreSQL `now()`.
- Login throttling must be PostgreSQL-backed when more than one API instance is deployed.
- User passwords are stored only as Argon2id hashes; sessions require secure cookie and CSRF configuration.
- Every job operation must enforce ACLs in the backend, independently of frontend visibility.
- The first administrator requires a controlled bootstrap flow with no default password.
- The frontend is served by the `api` profile; workers expose no public login or UI.
- Managed datasource secrets are encrypted before PostgreSQL persistence, decrypted only in memory at claim-time execution, and never transported as job data. The mounted key-encryption key is external to PostgreSQL; Vault, CyberArk, and KMS providers remain future implementations behind the provider interface.
- More workers increase source and sink contention; scaling limits must be measured.
- Worker load distribution is approximate; normalized busy-slot time is the fairness measure and raw run count is only a secondary signal.
- Managed datasources reuse unresolved connection configuration, not live JDBC connections. Each core task continues to open, use, and close its own source and sink connections.
- Managed datasource references are live and intentionally unversioned in Phase 4. Runs retain datasource identifiers and timestamps for temporal audit correlation, but not a complete historical configuration snapshot.
- A datasource ACL (`VIEW`/`USE`/`EDIT`) protects users, while source/sink binding-use flags on the job control whether any future execution is allowed. Frontend visibility is never sufficient authorization.
- Phase 4's migration is pre-production destructive with respect to managed metadata: a non-empty existing `job_definition` table causes an actionable failure, and no old jobs, runs, schedules, permissions, or idempotency rows are backfilled.
- Datasource deletion is restrictive while referenced by a job. The server does not detach jobs, copy credentials, or convert them back to inline connection configuration.
- Datasource capability metadata is derived from connector type and the core manager contract rather than stored as editable JSON. The server must keep this catalog synchronized with manager support and mode restrictions.

---

## References

### Internal Documentation

- `README.md` - CLI, deployment, and compatibility baseline.
- `docs/docs/docs.md` - Replication modes and connector behavior.
- `src/main/java/org/replicadb/ReplicaDB.java` - Orchestration, multi-table loop, cancellable task execution, and the `CANCELLED` exit code.
- `src/main/java/org/replicadb/ReplicaTask.java` - Task execution and rich result production.
- `src/main/java/org/replicadb/ReplicaTaskResult.java` - Row counters, timings, and reserved watermark candidate.
- `src/main/java/org/replicadb/execution/ReplicationExecutionContext.java` - Per-run identifier, staging name, temporary-file state, cancellation flag, and active-statement registry.
- `src/main/java/org/replicadb/execution/ReplicationCancelledException.java` - Checked cancellation signal propagated through existing manager method signatures.
- `src/main/java/org/replicadb/cli/ReplicationMode.java` - `complete`, `complete-atomic`, `incremental`.
- `src/main/java/org/replicadb/cli/ToolOptions.java` - Current configuration boundary.
- `src/main/java/org/replicadb/manager/SqlManager.java` - Staging, merge, atomic swap, and cancellation-aware statement lifecycle.
- `src/main/java/org/replicadb/manager/ConnManager.java` - Per-run staging-table name access and cancellation helpers.
- `src/main/java/org/replicadb/manager/file/FileManager.java` - Per-run temporary-file access and cancellation helper.
- `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobDefinition.java` - Validated job definition record (one source/sink table pair, `${env:VARIABLE}`-only credential references).
- `replicadb-server/src/main/java/org/replicadb/server/job/domain/ConnectionCredentials.java` - Current managed credential validation and technical-parameter boundary; Phase 4 moves this value into `ManagedDataSource`.
- `replicadb-server/src/main/java/org/replicadb/server/job/domain/SourceEndpoint.java`, `SinkEndpoint.java` - Current endpoint-specific replication fields; Phase 4 retains these fields while replacing embedded connection values with datasource references.
- `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRun.java` - Job run record: status, attempt, lease/heartbeat, counters, committed watermark, error message.
- `replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunResponse.java` - HTTP representation of a run, including the persisted cancellation warning.
- `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRunStatus.java` - The 7 job-run states, `isTerminal()`, and `fromReplicaExitCode(...)`.
- `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRunStateMachine.java` - Legal `JobRun` state transitions.
- `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobDefinition.java` - Job configuration boundary to extend with the Phase 3 retry policy.
- `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionRequest.java` - API input boundary to extend with retry policy fields.
- `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobDefinitionRepository.java` - Spring JDBC persistence for job definitions.
- `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java` - Row-locking claim (`FOR UPDATE SKIP LOCKED`), state transitions, `scheduleRetry(...)`, and watermark lookup.
- `replicadb-server/src/main/java/org/replicadb/server/job/execution/JobDefinitionEnvResolver.java` - `${env:VARIABLE}` resolution; rejects `${secret:...}`.
- `replicadb-server/src/main/java/org/replicadb/server/job/execution/JobDefinitionOptionsFileWriter.java` - Historical managed adapter that Phase 4 removes from the server execution path; standalone CLI options-file parsing remains unchanged.
- `src/main/java/org/replicadb/cli/ToolOptionsBuilder.java` - Planned additive programmatic factory for constructing managed `ToolOptions` in memory without changing CLI parsing or options-file behavior.
- `replicadb-server/src/main/java/org/replicadb/server/job/execution/JobExecutionService.java` - Claims a run, invokes `ReplicaDB.processReplica(ToolOptions)`, and persists the outcome.
- `replicadb-server/src/main/resources/db/migration/V1__create_job_definition.sql`, `V2__create_job_run.sql` - Forward-only Flyway migrations for the initial metadata schema.
- `.ai/archive/phase-1b-state-layer.plan.md` - Phase 1b implementation plan and execution retrospective.
- `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionController.java`, `JobRunController.java` - REST controllers implementing the `/api/v1/jobs`/`/api/v1/runs` endpoint table.
- `replicadb-server/src/main/java/org/replicadb/server/job/api/GlobalExceptionHandler.java` - RFC 7807 `ProblemDetail` error mapping with credential redaction.
- `replicadb-server/src/main/java/org/replicadb/server/job/execution/RunExecutionCoordinator.java` - Bounded async executor and in-memory cancellation registry for managed runs.
- `replicadb-server/src/main/java/org/replicadb/server/job/execution/AdmissionLane.java`, `WorkerAdmissionPolicy.java`, `ContentionBackoff.java` - Local directed/fallback/generic admission lanes and bounded timing controls.
- `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerAdmissionQueue.java`, `WorkerAdmissionScheduler.java` - Coalesced local wake-ups and delayed opportunities without consuming execution permits.
- `replicadb-server/src/main/java/org/replicadb/server/observability/WorkerBusySlotTracker.java`, `WorkerMetricsIdentity.java` - Monotonic raw/normalized utilization accounting and bounded worker metric identity.
- `scripts/phase3-fairness-test.sh`, `scripts/phase3-cli-compatibility.sh` - Reproducible Phase 3.4 fairness and standalone CLI artifact gates.
- `replicadb-server/src/main/java/org/replicadb/server/job/persistence/RunTriggerIdempotencyRepository.java` - `Idempotency-Key` replay lookup and upsert.
- `replicadb-server/src/main/java/org/replicadb/server/job/execution/IdempotencyCleanupTask.java` - Scheduled purge of expired idempotency keys.
- `replicadb-server/src/main/resources/db/migration/V3__add_job_run_active_constraint.sql`, `V4__create_run_trigger_idempotency.sql` - Forward-only Flyway migrations adding the non-overlap constraint and idempotency table.
- `.ai/archive/phase-1c-1-rest-api-core.plan.md` - Phase 1c-1 implementation plan and execution retrospective.
- `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobSchedule.java` - Validated cron and IANA timezone schedule value.
- `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobScheduleRepository.java` - PostgreSQL persistence for recurring schedules.
- `replicadb-server/src/main/java/org/replicadb/server/job/execution/ScheduledRunTriggerJob.java`, `QuartzScheduleService.java`, `ScheduleReconciler.java` - Quartz trigger execution, lifecycle, and startup reconciliation.
- `replicadb-server/src/main/java/org/replicadb/server/job/execution/RunExecutionCoordinator.java` - Current single-instance execution coordinator to replace or narrow behind the Phase 3 shared dispatch path.
- `replicadb-server/frontend/src/components/ConnectionSettingsCard.tsx`, `replicadb-server/frontend/src/utils/connectionBuilder.ts` - Current connection editor and connector-string builder to move into the Phase 4 datasource editor.
- `replicadb-server/frontend/src/pages/JobFormPage.tsx`, `replicadb-server/frontend/src/api/jobsApi.ts` - Current managed job connection fields and API mapping to replace with datasource selectors in Phase 4.
- `replicadb-server/src/main/java/org/replicadb/server/job/domain/ManagedDataSource.java`, `DataSourceCapabilities.java`, `DataSourceCapabilityCatalog.java` - Planned Phase 4 datasource domain and authoritative capability boundary.
- `replicadb-server/src/main/java/org/replicadb/server/job/api/DatasourceController.java`, `DatasourceMapper.java`, `DatasourceResponse.java` - Planned Phase 4 datasource REST contract and safe response mapping.
- `replicadb-server/src/main/java/org/replicadb/server/job/persistence/ManagedDataSourceRepository.java`, `DataSourcePermissionRepository.java` - Planned Phase 4 PostgreSQL adapters for datasource state and ACLs.
- `replicadb-server/src/main/resources/db/migration/V17__create_managed_datasource.sql` - Planned Phase 4 datasource schema migration and pre-production reset boundary.
- `replicadb-server/src/main/resources/application.yml`, `application-api.yml` - Current single-instance Quartz/API configuration and the Phase 3 profile boundaries.
- `replicadb-server/src/main/java/org/replicadb/server/job/api/JobScheduleController.java` - Schedule management endpoints under `/api/v1/jobs/{id}/schedule`.
- `replicadb-server/src/main/resources/db/migration/V5__create_job_schedule.sql`, `V6__add_job_run_definition_created_index.sql` - Schedule persistence and job-history query index migrations.
- `.ai/archive/phase-1c-2-quartz-scheduler.plan.md` - Phase 1c-2 implementation plan and execution retrospective.
- `replicadb-server/src/main/java/org/replicadb/server/audit/domain/AuditAction.java`, `AuditOutcome.java`, `AuditResourceType.java`, `AuditActor.java`, `AuditEvent.java` - Audit event vocabulary and immutable domain records.
- `replicadb-server/src/main/java/org/replicadb/server/audit/AuditService.java`, `AuditActorResolver.java` - Explicit audit write boundary and actor resolution.
- `replicadb-server/src/main/java/org/replicadb/server/audit/persistence/AuditEventRepository.java`, `AuditEventFilter.java` - JSONB audit persistence, indexed filters, paging, and retention deletion.
- `replicadb-server/src/main/java/org/replicadb/server/audit/api/AuditEventController.java`, `AuditEventResponse.java` - ADMIN-only audit history endpoint and response mapping.
- `replicadb-server/src/main/java/org/replicadb/server/audit/execution/AuditRetentionTask.java` - Scheduled 365-day audit retention purge.
- `replicadb-server/src/main/resources/db/migration/V10__create_audit_event.sql`, `V11__add_job_run_cancellation_warning.sql` - Audit-event schema and persisted cancellation-warning migration.
- `openspec/` - Change proposals and specs for engine-level behavior; this document governs product direction, not individual engine changes.
- `.ai/context/execution.md` - Current execution and lifecycle constraints.
- `.ai/context/operations.md` - Current runtime, telemetry, and deployment constraints.

### External Resources

- ReplicaDB GitHub: https://github.com/osalvador/ReplicaDB
- PostgreSQL `NOTIFY`: https://www.postgresql.org/docs/current/sql-notify.html
- PostgreSQL `LISTEN`: https://www.postgresql.org/docs/current/sql-listen.html
- PostgreSQL explicit locking: https://www.postgresql.org/docs/current/explicit-locking.html

---

**Document Version**: 4.4
**Last Updated**: August 26, 2026
**Next Review**: After the Phase 4 datasource plan or implementation
