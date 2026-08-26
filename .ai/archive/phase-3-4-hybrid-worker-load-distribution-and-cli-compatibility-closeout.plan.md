# Implementation Plan: Phase 3.4 - Hybrid Worker Load Distribution and CLI Compatibility Closeout

## Task Source - JIRA: none - approved Phase 3.4 architecture decision

The source of truth is the approved Phase 3.4 section and the final compatibility checkbox in [ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md). No JIRA acceptance criteria were supplied.

The current checklist audit found exactly two unchecked implementation entries:

- Phase 3.4 hybrid worker load distribution.
- The cross-phase requirement to preserve the CLI artifact, exit codes, options-file contract, and no-metadata-database execution path.

The second entry is an acceptance gate for the final phase, not a separate product phase. Phase 3.1, Phase 3.2, and Phase 3.3 are marked implemented and validated in the architecture decision document. After Phase 3.4 and this compatibility gate pass, no other unchecked Phase 3 implementation scope remains in that document.

## Overview

Phase 3.4 adds approximate fairness to the existing PostgreSQL-coordinated worker fleet. It introduces two local admission lanes: one coalesced directed opportunity for each run notification, with at most one generic fallback, and bounded generic refill opportunities for currently free worker slots. Jitter, successful-claim cooldown, and decaying contention backoff are local scheduling controls only; they never become durable work state and never replace PostgreSQL claim, lease, retry, cancellation, or fencing semantics.

The final compatibility gate proves that these server-only changes do not alter the standalone `replicadb` artifact. It validates the built Spring-free CLI artifact, exit codes `0`/`1`/`2`, command-line and options-file behavior including multi-table execution, and a real SQLite replication with no metadata database or Spring Boot runtime available.

## Architecture & Design - Approach: Explicit deterministic admission policy

### Existing boundary and hypothesis

`WorkerDispatchCoordinator` currently acquires a capacity permit for each wake-up and performs one directed or generic claim. `PollingFallback` currently coalesces scans but requests only one generic claim per scan, regardless of free capacity. The smallest falsifiable hypothesis is that the fairness gap is local admission behavior rather than PostgreSQL ownership: changing the coordinator's opportunity scheduling while leaving `JobRunStore.claimNextEligible(...)` and all token-fenced writes untouched should improve slot distribution without changing durable run correctness.

The first implementation check is therefore a deterministic coordinator test with an injected clock, jitter source, and scheduler. It must be able to prove the opportunity counts and permit lifetime without waiting on wall-clock sleeps or requiring a database. A PostgreSQL integration/load check follows only after that local behavior is established.

### Locked invariants

- PostgreSQL remains the only arbiter of ownership. No worker registry, central dispatcher, external broker, SQL random ordering, or local durable queue is added.
- `JobRunStore`, `available_at`, `FOR UPDATE SKIP LOCKED`, lease tokens, heartbeats, recovery, cancellation delivery, watermarks, and token-fenced finalization retain their Phase 3.1-3.3 semantics.
- A notification contains only a UUID. A notification is a wake-up, never proof of ownership and never a transport for job configuration or credentials.
- The capacity permit is acquired immediately before a claim attempt and remains held through the claimed run's coordination and execution lifetime. A delayed jitter, cooldown, or backoff opportunity never holds a run permit.
- A directed notification is coalesced by run UUID while queued or active. One worker gets at most one directed claim opportunity for that coalesced signal. If `claimRequested(runId, ...)` is empty, that same opportunity may perform exactly one generic fallback claim; the fallback cannot call another fallback.
- A generic refill request is coalesced and schedules no more than one generic claim opportunity per currently free slot after accounting for already scheduled generic opportunities. It does not prefetch rows or hold a database row while waiting.
- Startup, listener reconnect, periodic polling, and completion of a claimed run all create generic refill opportunities. Polling remains mandatory recovery when local signals are dropped or delayed.
- A successful claim arms a bounded generic cooldown. Empty or duplicate/contention outcomes may increase a capped local backoff that decays after successful or uncontended work. Queue age is measured but does not bypass the cooldown.
- Fairness is evaluated with normalized busy-slot time, `busy-slot-seconds / max-concurrent-runs`, over a sustained backlog. Raw completed-run count is secondary and is not treated as a fairness guarantee.
- Operational metrics use bounded tags such as a sanitized worker identity, lane, outcome, and trigger. They never contain run IDs, job IDs, usernames, DSNs, passwords, lease tokens, resolved options, or connection strings.
- The root `replicadb` build, root Maven dependency graph, CLI launcher, core managers, and `ToolOptions` contract are not moved behind the server module and do not gain Spring Boot or PostgreSQL metadata dependencies.

### Proposed local policy and defaults

Add an `admission` section below `replicadb.worker` with configuration that is safe but overrideable:

| Property | Default | Purpose |
| --- | --- | --- |
| `replicadb.worker.admission.jitter-max` | `100ms` | Per-worker random delay before a claim opportunity; `0` disables it. |
| `replicadb.worker.admission.generic-cooldown` | `250ms` | Maximum delay before the next generic refill after a successful claim; `0` disables it. |
| `replicadb.worker.admission.adaptive-backoff.enabled` | `true` | Enables local contention backoff for later generic opportunities. |
| `replicadb.worker.admission.adaptive-backoff.initial-delay` | `25ms` | First contention delay. |
| `replicadb.worker.admission.adaptive-backoff.max-delay` | `2s` | Hard cap for contention delay. |
| `replicadb.worker.admission.adaptive-backoff.decay-half-life` | `30s` | Monotonic-time decay after successful or uncontended work. |
| `replicadb.worker.admission.directed-queue-capacity` | `1024` | Maximum distinct pending directed UUID signals retained by one worker; overflow is observable and polling recovers durable work. |

Durations are local scheduling values and must not be used for leases or eligibility. The policy receives an injectable monotonic time source and random source in tests. Production wiring uses `System.nanoTime()` and a per-worker random source. The scheduler used for delayed opportunities is separate from the bounded execution pool, so waiting never consumes an execution slot.

### Metrics design

Keep the existing managed metrics and add bounded lane/admission outcomes. Add a per-worker utilization tracker that observes permit transitions using monotonic time and exposes cumulative counters suitable for scrape-time deltas:

- `replicadb.worker.busy.slot.seconds` with a sanitized `worker_identity` tag.
- `replicadb.worker.normalized.busy.slot.seconds` with the same tag, incremented by `activeSlotsDelta / maxConcurrentRuns` over elapsed time.
- `replicadb.worker.completed.runs` with bounded outcome tags.
- Admission/claim counters distinguishing `directed`, `fallback`, and `generic` lanes, plus `claimed`, `empty`, `coalesced`, `dropped`, and `error` outcomes.
- Existing polling lag, active/free slot, lease, cancellation, retry, and terminal metrics remain available.

The tracker records the interval since the previous permit transition, including time held by an empty claim and all time held by a claimed run. Fairness harnesses capture a baseline and final cumulative value for every worker, then compare normalized deltas over the same sustained-backlog window. A worker restart resets its local counters; the harness excludes restart intervals and reports that boundary explicitly.

## Implementation Tasks

### 1. Add and validate the worker admission configuration

- [x] **1.1 Add the explicit admission properties and production defaults**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/config/WorkerRuntimeProperties.java`, `replicadb-server/src/main/java/org/replicadb/server/job/config/WorkerRuntimeConfiguration.java`, `replicadb-server/src/main/resources/application-worker.yml`, `replicadb-server/src/test/java/org/replicadb/server/job/config/WorkerRuntimeConfigurationTest.java`
  Changes: Add a nested `Admission`/`AdaptiveBackoff` configuration model under `replicadb.worker.admission`. Bind the defaults in `application-worker.yml`, preserve all existing worker properties, and perform eager validation in `WorkerRuntimeConfiguration` before the worker lifecycle is returned or started. Validate non-negative jitter/cooldown, positive backoff values when enabled, `max-delay >= initial-delay`, a positive bounded directed queue capacity, and the existing datasource headroom rule. Permit `0` only for optional jitter/cooldown; invalid or unbounded values must fail worker startup rather than the first admission attempt. Keep the property names environment-overridable for Compose.
  Tests: Verify default binding, zero-value disabling, invalid negative/zero backoff rejection, maximum/initial ordering, queue-capacity bounds, and unchanged `max-concurrent-runs + 4` datasource validation. Assert the production worker profile receives the same values used by the standalone configuration probe.
  Dependencies: None

### 2. Implement deterministic policy primitives

- [x] **2.1 Add lane, jitter, cooldown, and contention-backoff policy objects**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerAdmissionPolicy.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/execution/ContentionBackoff.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/execution/AdmissionLane.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/execution/WorkerAdmissionPolicyTest.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/execution/ContentionBackoffTest.java` (new)
  Changes: Model `DIRECTED`, `FALLBACK`, and `GENERIC` lanes without embedding database calls. Compute a bounded random jitter before admission, apply cooldown only to later generic refill opportunities after a successful claim, and track contention with a capped backoff that decays by monotonic elapsed time. Inject a time supplier and random supplier so policy tests never sleep. Treat coalesced duplicate signals as contention evidence without creating another claim opportunity, and keep queue age out of delay calculation.
  Tests: Assert jitter is always within its configured range, directed signals are not multiplied by free capacity, cooldown delays generic work but not the directed lane, one empty/fallback outcome increases backoff, the cap is respected, half-life decay reduces it, success/uncontended work resets or reduces it, and disabled backoff is inert. Assert no policy method can produce a second fallback lane.
  Dependencies: Task 1.1

### 3. Add utilization tracking and bounded metrics

- [x] **3.1 Extend managed metrics with lane outcomes and normalized busy-slot time**
  Files: `replicadb-server/src/main/java/org/replicadb/server/observability/ManagedRuntimeMetrics.java`, `replicadb-server/src/main/java/org/replicadb/server/observability/WorkerBusySlotTracker.java` (new), `replicadb-server/src/main/java/org/replicadb/server/observability/WorkerMetricsIdentity.java` (new), `replicadb-server/src/test/java/org/replicadb/server/observability/ManagedRuntimeMetricsTest.java`, `replicadb-server/src/test/java/org/replicadb/server/observability/WorkerBusySlotTrackerTest.java` (new)
  Changes: Centralize allowlisted lane, admission outcome, trigger, and terminal tag values. Add a `WorkerMetricsIdentity` normalizer that trims the configured `WorkerRunIdentity`, replaces characters outside `[A-Za-z0-9._-]` with `_`, caps the result at 64 characters, and falls back to `other` when blank; never use run/job/user/secret values as tags. Add a monotonic `WorkerBusySlotTracker` that records active-slot transitions, busy slot seconds, normalized busy slot seconds, and completed-run counts. Preserve existing meter names and tags unless a new bounded tag is required to distinguish `fallback` from generic claims. Metrics failures remain swallowed and cannot alter dispatch state.
  Tests: With a fake monotonic ticker, verify interval accounting for zero, partial, and full capacity, normalized values for capacities 1/2/4, final flush behavior, worker restart/reset semantics, bounded identity normalization, lane/outcome allowlists, no high-cardinality identifiers, no secrets in names/tags, and non-blocking behavior when a registry rejects a meter operation.
  Dependencies: Task 1.1

### 4. Refactor local admission while preserving PostgreSQL claims

- [x] **4.1 Make `WorkerDispatchCoordinator` own bounded coalescing and permit lifetime**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerDispatchCoordinator.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerAdmissionQueue.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerAdmissionScheduler.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/application/RunLeaseService.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/WorkerDispatchCoordinatorTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/WorkerAdmissionQueueTest.java` (new)
  Changes: Introduce a bounded local queue/set for distinct directed UUIDs, a coalesced generic-refill request, and an admission scheduler that applies policy delays before acquiring the semaphore. Store directed suppression in a thread-safe `ConcurrentHashMap<UUID, DirectedSignal>` using `putIfAbsent`; retain the first receive timestamp, keep the UUID suppressed while queued, during its claim/fallback, and through claimed execution, and remove it in a `finally` path after an empty/fallback attempt or run cleanup. Limit the queued distinct entries to `directed-queue-capacity`; active entries are not duplicated and all pending state is cleared on shutdown. Keep the existing execution executor bounded and acquire the semaphore immediately before each database claim. Use `try/finally` around every claim/execution admission so the permit and busy-slot tracker are released on empty claims, claim exceptions, execution exceptions, cancellation, fencing, rejection, and shutdown. Add a `claimFallback(...)` method in `RunLeaseService` that calls the existing null-request `JobRunStore` claim while recording the fallback lane, and preserve the existing generic claim entry point for compatibility. Mark the old/simple constructors `@Deprecated` and make them delegate to the full constructor with the default policy and an owned test-safe scheduler; update every existing production/test caller to exercise the full policy-aware constructor rather than bypassing Phase 3.4 behavior.
  Tests: Prove duplicate directed signals coalesce while queued and while a run is active, queue overflow is bounded and observable, delayed opportunities do not reduce available execution capacity, permits are held for the full claimed run, empty claims release promptly, every exception path releases the permit, and shutdown cancels delayed admissions without leaking scheduler threads. Verify two workers still claim distinct rows through the same `JobRunStore` contract, and verify existing coordinator/integration tests use an injected policy/scheduler rather than the deprecated constructor path.
  Dependencies: Tasks 2.1 and 3.1

- [x] **4.2 Implement one directed claim plus one non-recursive fallback**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerDispatchCoordinator.java`, `replicadb-server/src/main/java/org/replicadb/server/job/application/RunLeaseService.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/WorkerDispatchCoordinatorTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/application/RunLeaseServiceTest.java` (new or existing test file)
  Changes: Route a directed opportunity to `claimRequested(runId, ...)`; if it returns empty, call `claimFallback(...)` once in the same admission attempt. Do not call `signalEligibleWork`, enqueue another directed signal, or recursively invoke fallback from the fallback path. Mark the directed UUID suppressed until the claim attempt/fallback and any claimed execution have completed, so duplicate notifications cannot produce extra fallbacks. Record notification latency only for the first coalesced signal and record directed/fallback outcomes separately.
  Tests: Verify a successful directed claim performs no fallback, an empty directed claim performs exactly one generic fallback, an empty fallback performs no second generic claim, duplicate notifications produce one directed call and at most one fallback, notification timestamps are not double-counted, and a run won by another worker does not trigger a fallback chain on later duplicate notifications.
  Dependencies: Task 4.1

- [x] **4.3 Refill one generic opportunity per current free slot**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerDispatchCoordinator.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerAdmissionQueue.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/WorkerDispatchCoordinatorTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/WorkerAdmissionQueueTest.java`
  Changes: Add a coalesced `requestGenericRefill(trigger)` path. At processing time, compute current free permits minus already scheduled generic opportunities and schedule no more than that many generic claim opportunities. Recompute at the next startup/reconnect/poll/completion event rather than prefetching. Apply jitter, cooldown, and contention backoff before the claim without occupying a run permit. A successful generic claim arms cooldown; completion creates the next refill opportunity. Keep `signalEligibleWork()` as a compatibility delegate for current callers.
  Tests: With capacities 1, 2, and 4, assert a refill schedules no more opportunities than free slots, repeated refill events coalesce, a claim filling a slot reduces later refill demand, completion refills a newly free slot, jitter/cooldown/backoff do not occupy semaphore permits, and a permanently empty queue does not create an unbounded self-triggering loop.
  Dependencies: Tasks 2.1, 4.1, and 4.2

### 5. Wire notifications, polling, recovery, and lifecycle events

- [x] **5.1 Connect all wake-up sources to the two admission lanes**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/dispatch/PostgreSQLNotificationListener.java`, `replicadb-server/src/main/java/org/replicadb/server/job/dispatch/PollingFallback.java`, `replicadb-server/src/main/java/org/replicadb/server/job/config/WorkerRuntimeConfiguration.java`, `replicadb-server/src/main/java/org/replicadb/server/job/config/WorkerRuntimeLifecycle.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerDispatchCoordinator.java`, `replicadb-server/src/test/java/org/replicadb/server/job/dispatch/PostgreSQLNotificationListenerTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/dispatch/PollingFallbackTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/config/WorkerRuntimeConfigurationTest.java`
  Changes: Keep the listener's UUID-only run/control channel behavior and let the coordinator perform run-notification coalescing. Change startup, reconnect, periodic, and manual polling scans to request a generic refill rather than directly issuing one claim. Preserve pending directed UUIDs across listener disconnect/reconnect without re-publishing them; reconnect polling creates a generic refill that discovers durable rows whose notification was lost. Keep cancellation scans and expired-run recovery unchanged except that replacement notifications enter the directed lane. Add completion refill wiring and explicit ownership/shutdown of the admission scheduler. Enforce shutdown order as: stop accepting new signals, stop polling, stop the listener, cancel delayed admission tasks and active run handles, keep resources needed for cancellation until active execution exits or the shutdown timeout expires, then stop heartbeats and close the admission/execution executors. Preserve listener-down/polling-up readiness semantics and the worker's lack of REST, Security/session, and Quartz beans.
  Tests: Verify a burst of duplicate notifications reaches one directed admission, a notification arriving during a poll is coalesced, pending directed UUIDs survive listener reconnect without duplicate claims, startup/reconnect/periodic/completion each create a bounded refill, missed notifications are recovered by polling, cancellation/control notifications still reach the local active handle, expired recovery publishes and admits one replacement, listener reconnect does not stop heartbeats, and lifecycle shutdown follows the stated order while stopping listener, polling, admission scheduler, heartbeat, and execution.
  Dependencies: Tasks 4.1 through 4.3

### 6. Regression-test durable state, fencing, and run safety

- [x] **6.1 Prove hybrid admission does not change claim, recovery, cancellation, or watermark invariants**
  Files: `replicadb-server/src/test/java/org/replicadb/server/job/execution/WorkerDispatchCoordinatorIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/dispatch/DistributedWorkerLifecycleIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/dispatch/PostgreSQLNotificationListenerIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/dispatch/PollingFallbackIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobRunRepositoryIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/HybridWorkerDistributionIT.java` (new)
  Changes: Extend shared-PostgreSQL scenarios to use two or more workers with different slot capacities and the new admission settings. Keep all assertions against PostgreSQL rows and token-fenced repository results, never local coordinator state. Add a deterministic sustained-backlog fixture with fixed run duration, bounded jitter seeds, and enough runs to measure slot time; record claim lane/outcome, active capacity, queue age, terminal outcomes, and normalized busy-slot counters. Do not add migrations or change `JobRunStore` ownership semantics.
  Tests: Assert duplicate directed notifications and simultaneous polling claim each run at most once; a worker never exceeds capacity; a stale lease token cannot finalize or commit a watermark; remote cancellation, retry recovery, and healthy heartbeats remain correct; no run is resumed; equal-capacity workers have approximately balanced normalized busy-slot time; different-capacity workers receive approximately proportional normalized utilization; and queue age remains observable without bypassing cooldown.
  Dependencies: Tasks 4.1 through 5.1

### 7. Validate utilization instrumentation from real coordinator transitions

- [x] **7.1 Integrate the busy-slot tracker with worker permits and terminal outcomes**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerDispatchCoordinator.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerAdmissionScheduler.java`, `replicadb-server/src/main/java/org/replicadb/server/job/execution/JobExecutionService.java`, `replicadb-server/src/main/java/org/replicadb/server/observability/WorkerBusySlotTracker.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/WorkerDispatchCoordinatorTest.java`, `replicadb-server/src/test/java/org/replicadb/server/observability/ManagedRuntimeMetricsTest.java`
  Changes: Start utilization accounting at the exact permit acquisition boundary and stop it only after claimed-run coordination releases the permit. Record successful, failed, cancelled, and fenced terminal outcomes without double-counting stale finalization. Expose a snapshot method for health/test harnesses that contains only numeric counts, configured capacity, and bounded worker identity. Ensure admission scheduler failures and metrics exceptions release permits and cannot leave a run unobservable or stuck.
  Tests: Use latches and a fake ticker to assert busy time spans claim plus execution, empty claims contribute only their short held interval, simultaneous slots sum correctly, completion/failure/cancellation each count once, fenced finalization does not create a second completion, and a scheduler/metrics exception leaves capacity and lifecycle state consistent.
  Dependencies: Tasks 3.1 and 4.1 through 6.1

### 8. Expose configurable capacities and admission settings in the local topology

- [x] **8.1 Update Compose and deployment configuration for fairness scenarios**
  Files: `docker-compose.server.yml`, `replicadb-server/src/main/resources/application-worker.yml`, `DEPLOYMENT.md`, `replicadb-server/src/test/java/org/replicadb/server/job/config/WorkerRuntimeConfigurationTest.java`, `scripts/check-phase3-docs.sh`
  Changes: Add environment overrides for each worker's `max-concurrent-runs`, jitter, cooldown, backoff, and directed queue capacity while retaining current defaults and internal management exposure. Allow the fairness harness to run equal capacities and a mixed-capacity topology without editing the Compose file. Document that effective replication capacity remains `worker instances * concurrent runs per worker * jobs per run`, that worker identity is unique, and that fairness is probabilistic and measured with normalized busy-slot time. Keep credentials and metadata URLs environment-managed.
  Tests: Run `docker compose config` with placeholders and both equal/mixed capacity overrides; assert resolved YAML contains no credentials; start the existing one-worker and two-worker smoke topologies with defaults; verify the documented property names match both profile YAML and Compose; and run the documentation gate with stale/secret fixtures.
  Dependencies: Tasks 1.1, 5.1, and 7.1

### 9. Add a reproducible process-level fairness harness

- [x] **9.1 Measure equal and proportional worker utilization under sustained backlog**
  Files: `scripts/phase3-fairness-test.sh` (new), `scripts/phase3-load-test.sh`, `scripts/phase3-multinode-test.sh`, `replicadb-server/src/test/resources/phase3/fixture.sql`, `.gitignore`
  Changes: Add a bounded harness with a fixed seed and parameters for run count, operation duration, worker capacities, tolerance, and project name. Build the current artifacts explicitly with `mvn -B install -DskipTests -f pom.xml` followed by `mvn -B package -DskipTests -f replicadb-server/pom.xml`, then use the existing `worker-one` and `worker-two` Compose services with per-service capacity environment overrides rather than scaling anonymous workers. Start two APIs and two workers with an isolated project and volume, create enough jobs to keep `job_run` pending while other rows are `RUNNING`, and use database-visible barriers: poll `job_run` counts/statuses, `pg_stat_activity` for the `pg_sleep` source query, and `pg_locks` where a lock is part of a scenario. Scrape each worker's baseline/final busy-slot and normalized counters plus claim, polling, queue-age, and terminal metrics. Calculate normalized busy-slot deltas, compare equal capacities and a mixed-capacity proportional case with documented tolerances, and assert no capacity breach, duplicate terminal run, duplicate watermark, chained fallback, or secret-bearing metric. Use captured output and here-string matching where strict `pipefail` could turn a successful assertion into SIGPIPE. Keep cleanup traps, dynamic projects, and the existing short load smoke separate from the longer fairness scenario.
  Tests: Run a short deterministic smoke variant and a sustained local/CI variant. Assert all accepted runs finish or form only an expected retry chain, equal-capacity normalized utilization is within tolerance, mixed-capacity utilization is approximately proportional, oldest eligible queue age is reported, directed claims/fallbacks/refills stay within their bounds, and missing notifications remain recoverable by polling. Verify the script uses only provisioned/baseline tools and leaves no containers, networks, volumes, credentials, or state directories behind.
  Dependencies: Tasks 6.1 and 8.1

### 10. Re-run failure and recovery scenarios with delayed admission

- [x] **10.1 Preserve resilience behavior during jitter, cooldown, contention, and worker loss**
  Files: `scripts/phase3-worker-loss-test.sh`, `scripts/phase3-resilience-test.sh`, `scripts/phase3-chaos-test.sh`, `scripts/phase3-load-test.sh`, `replicadb-server/src/test/java/org/replicadb/server/job/dispatch/DistributedWorkerLifecycleIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/dispatch/RemoteCancellationIT.java`
  Changes: Add explicit admission-aware cases to the existing harnesses: duplicate notifications assert one directed attempt and at most one fallback; listener outage/reconnect and notification loss assert a pending row is found by startup/reconnect/periodic polling; completion asserts refill is bounded by newly free slots; source-copy loss uses the existing `pg_sleep` query and checks the old attempt before killing its worker; merge loss uses `pg_stat_activity`/`pg_locks` to prove the statement is waiting before killing the worker; PostgreSQL restart asserts no duplicate claim; cancellation asserts delivery through the owner or polling; retry recovery asserts `available_at`, attempt, and fencing; and mixed capacity asserts no slot overrun. Keep database-observable barriers (`job_run` state, `pg_stat_activity`, and `pg_locks`) and bounded cleanup from Phase 3.3. Ensure a delayed local signal never becomes a durable loss: polling must discover the run, and a dropped local queue entry must be recoverable without a second watermark or resumed attempt.
  Tests: Assert a worker never exceeds its configured slots during every failure transition; a directed notification has no more than one directed claim and one fallback per worker; old attempts remain immutable; replacement attempts obey `available_at` and retry policy; healthy heartbeats prevent false recovery during merge; remote cancellation reaches the owner or polling fallback; and all existing Phase 3.2/3.3 resilience assertions still pass with non-zero admission delays.
  Dependencies: Tasks 5.1, 6.1, and 9.1

### 11. Add an explicit standalone CLI artifact compatibility gate

- [x] **11.1 Prove Spring-free packaging, unchanged exit codes, options-file behavior, and offline execution**
  Files: `src/test/java/org/replicadb/NoSpringBootOnClasspathTest.java`, `src/test/java/org/replicadb/CliOfflineExecutionTest.java` (new), `src/test/java/org/replicadb/ReplicaDBRunCountersTest.java`, `src/test/java/org/replicadb/ReplicaDBCancellationTest.java`, `src/test/java/org/replicadb/cli/ToolOptionsMultipleTablesTest.java`, `src/test/java/org/replicadb/cli/ToolOptionsIncrementalWatermarkTest.java`, `scripts/phase3-cli-compatibility.sh` (new)
  Changes: Keep the root production source and root `pom.xml` free of Spring Boot/server dependencies. Extend the existing classpath guard to inspect the actual root dependency graph where practical, add `CliOfflineExecutionTest` in the root JUnit 5 suite using `@TempDir`, `jdbc:sqlite:<temp>/source.db`, `jdbc:sqlite:<temp>/sink.db`, JDBC-created tables, and a generated options file. The test must run with no `DB_URL`, `DB_USERNAME`, `DB_PASSWORD`, or Spring profile and must not create or load a server application context. Preserve assertions for success `0`, error `1`, and cancellation `2`. Add a shell gate that clean-builds the root `test`/release-compatible artifact, inspects the packaged JAR for the `ReplicaDB` main class and absence of `org/springframework/boot` classes, verifies the root runtime dependency list contains no server-only artifacts, and runs the packaged CLI against the same temporary SQLite options shape with all metadata/server variables unset. Exercise help/version, CLI-over-options-file precedence, legacy single-table properties, incremental watermark properties, and sequential multi-table options without changing their accepted keys or semantics. Use only generated temporary paths and environment-managed values; never print credentials.
  Tests: Run the new script on macOS and the CI JDK 17 image. Assert the packaged CLI performs a real SQLite replication while PostgreSQL is absent/unreachable, returns OS exit code `0` for success and `1` for malformed/failing invocation, and that existing JUnit cancellation coverage still maps an in-flight cancellation to `2`. Assert the options-file and multi-table tests pass unchanged in meaning, the root JAR/classpath contains no Spring Boot application context, and no metadata connection is attempted or required.
  Dependencies: Tasks 6.1 and 10.1

### 12. Add CI and release enforcement for Phase 3.4 and the CLI boundary

- [x] **12.1 Run fairness, resilience, packaging, and CLI compatibility gates in supported pipelines**
  Files: `.github/workflows/CT_Push.yml`, `.github/workflows/CI_Release.yml`, `replicadb-server/pom.xml`, `scripts/phase3-fairness-test.sh`, `scripts/phase3-cli-compatibility.sh`, `pom.xml`
  Changes: Add a named, bounded Phase 3.4 validation step/job with JDK 17, Docker/Testcontainers configuration, explicit package-before-image behavior, and failure diagnostics. Add a separate `cli_compatibility` CI job that checks out the repository, installs JDK 17, runs `scripts/phase3-cli-compatibility.sh` without PostgreSQL or Testcontainers, and does not depend on the managed `server` job. Run the fairness/resilience jobs only after the server artifact/image is freshly packaged, and run the CLI compatibility gate before root release packaging in `CI_Release.yml`. Keep server installation/build order, release archive contents, server JAR/image publication, root Maven profiles, and the no-Spring root dependency graph unchanged. Do not add Spring Boot, Micrometer, Quartz, PostgreSQL metadata, or worker configuration to the root artifact; do not edit `pom.xml` unless a test-only verification hook is required, and in all cases audit `mvn dependency:tree` plus the packaged JAR contents.
  Tests: Execute the exact focused server suite, named multinode profile, fairness smoke, resilience/chaos checks, CLI compatibility script, root CLI unit/options/cancellation tests, root JAR inspection, `docker compose config`, shell syntax checks, workflow lint where available, and `git diff --check`. Verify CI scripts use baseline tools available on the runner and avoid quiet producer pipelines under `pipefail`. Preserve existing integration, non-integration, server, frontend, multi-node, and release jobs.
  Dependencies: Tasks 9.1, 10.1, and 11.1

### 13. Document and close the final Phase 3 checklist

- [x] **13.1 Update architecture and operational status only after every gate passes**
  Files: `ARCHITECTURE_DECISIONS.md`, `DEPLOYMENT.md`, `README.md`, `scripts/check-phase3-docs.sh`
  Changes: Document the two admission lanes, duplicate coalescing, one-fallback rule, free-slot refill, bounded jitter, success cooldown, adaptive backoff, normalized busy-slot measurement, probabilistic fairness limits, configuration defaults, and the CLI compatibility boundary. After all implementation and validation tasks pass, mark Phase 3.4 and the final CLI preservation checkbox as complete, update the Phase 3 status/next-review wording, and retain the statement that PostgreSQL claim/fencing semantics and the standalone CLI remain unchanged. Do not claim strict round-robin fairness or move server prerequisites into the CLI deployment section.
  Tests: Run `scripts/check-phase3-docs.sh` against the repository and an intentionally stale temporary copy; verify the script rejects stale "Phase 3.4 not started"/missing compatibility wording and secret patterns. Re-run the final CLI artifact inspection, no-metadata SQLite smoke, server package/static-asset check, Compose configuration validation, and all Phase 3.4 fairness/resilience gates before changing either checkbox. Confirm there are no unchecked Phase 3 implementation entries remaining in `ARCHITECTURE_DECISIONS.md`.
  Dependencies: Tasks 8.1 through 12.1

## Technical Reference

<details>
<summary>Types & Data Structures</summary>

- `AdmissionLane`: bounded local lane vocabulary for `DIRECTED`, `FALLBACK`, and `GENERIC` opportunities.
- `WorkerAdmissionPolicy`: pure timing/state policy with injected monotonic time and random source; it does not claim, load, or persist a run.
- `ContentionBackoff`: capped, decaying contention delay state; it is reset/reduced by successful or uncontended work and never controls `available_at` or leases.
- `WorkerAdmissionQueue`: bounded coalescing state for directed UUID signals and generic refill demand. It stores no job definition, credentials, row, lease, or durable run state.
- `WorkerAdmissionScheduler`: delayed local opportunity scheduler separate from the execution pool; waiting opportunities do not consume capacity permits.
- `WorkerBusySlotTracker`: monotonic interval accumulator for active slots, raw busy-slot seconds, normalized busy-slot seconds, and completed-run observations.
- `ManagedRuntimeMetrics`: existing managed metrics facade extended with bounded lane/admission/utilization observations and worker identity normalization.
- `JobRunStore`: unchanged durable port. All ownership and terminal state remains decided by PostgreSQL and all worker writes remain lease-token fenced.

</details>

<details>
<summary>Dependencies</summary>

- Existing Java 17, Maven, Spring Boot 3.3.5, PostgreSQL JDBC, Quartz, Micrometer, Testcontainers, Docker Compose, and the current `replicadb-server` module remain the foundation.
- No new external broker, scheduler, database, schema migration, or dependency is required for Phase 3.4.
- The root CLI continues to use its existing Commons CLI, JDBC drivers, Log4j2/Sentry, launchers, and Maven assembly/release profiles. Server-only dependencies remain in `replicadb-server/pom.xml`.
- Any new scheduled executor is local and bounded. It must be closed by `WorkerRuntimeLifecycle`; it is not a durable queue or a Quartz scheduler.
- Process validation requires Docker and the existing Compose/Testcontainers setup. CLI compatibility validation must also have a no-Docker path for the Spring-free artifact and SQLite fixture.

</details>

<details>
<summary>Testing Strategy</summary>

| Layer | Tooling | Required evidence |
| --- | --- | --- |
| Policy | JUnit Jupiter with injected ticker/random source | Jitter bounds, cooldown lane scope, backoff cap/decay, no age escape, no fallback recursion |
| Coordinator | JUnit/Mockito with latches and injected scheduler | Coalescing, bounded queue, per-slot refill, permit lifetime, shutdown, capacity limits |
| Metrics | Micrometer `SimpleMeterRegistry` and fake ticker | Busy-slot accounting, normalized counters, bounded tags, no secrets/high-cardinality IDs, metrics failure isolation |
| Durable dispatch | PostgreSQL Testcontainers | One claim per run, duplicate notification/poll safety, token fencing, retries, cancellation, heartbeats, unchanged watermarks |
| Fairness | Shared PostgreSQL plus deterministic worker coordinators | Equal-capacity normalized busy-slot balance, mixed-capacity proportional utilization, queue age and claim outcomes |
| Process topology | Docker Compose scripts with dynamic projects and health barriers | Sustained backlog, directed/fallback bounds, refill bounds, no leaked resources, no secret-bearing output |
| Resilience | Existing worker-loss/restart/notification/chaos harnesses | Missed notification polling, reconnect, merge/copy loss, cancellation, recovery, stale fencing under delay |
| CLI compatibility | Root Maven tests, JAR inspection, packaged SQLite invocation | No Spring Boot in CLI artifact, exit codes `0`/`1`/`2`, options-file/multi-table behavior, no metadata database required |
| Documentation/release | POSIX shell checks, Maven, Compose, workflow lint | No stale Phase 3 status, no secret patterns, exact current artifact/profile commands, clean diff |

</details>

## Risks, Assumptions, and Deferred Work

- Fairness is probabilistic. Network latency, PostgreSQL lock timing, worker capacity, source/sink duration, and process scheduling can still produce unequal raw run counts. Acceptance uses sustained windows and normalized busy-slot time, not a single notification or short burst.
- Local admission signals can be lost on worker shutdown or bounded-queue overflow. This is acceptable only because durable `PENDING`/retryable rows are recovered by startup, reconnect, and periodic PostgreSQL polling. The harness must prove this rather than treating local signal delivery as durable.
- A directed notification that loses its race may perform one generic fallback and accidentally claim another eligible run. That is explicitly allowed once per directed opportunity; recursive fallback and notification fan-out are prohibited and tested.
- Delayed opportunities must never hold a semaphore permit. A scheduler implementation that sleeps inside the execution pool would distort utilization and violate the contract; the injected-scheduler tests are an early blocker for that mistake.
- Busy-slot metrics are cumulative process-local counters. The fairness harness must capture deltas within one process lifetime and report restarts separately; it must not compare absolute counters across restarts.
- No Phase 3.4 migration is expected. If implementation discovers that durable state is needed for admission policy, stop and revisit the architecture decision rather than silently adding a second source of truth.
- The full heterogeneous CLI integration matrix remains architecture-sensitive on Apple Silicon, as documented in the Phase 3.3 learning. The Spring-free classpath, packaged SQLite, and CI/native-database gates must remain distinct from vendor-container readiness failures.
- Existing Phase 3.3 shell portability lessons remain binding: use only provisioned or baseline tools, build the current JAR before image tests, wait on container/database-visible barriers, and avoid `grep -q`/SIGPIPE failures under `pipefail`.

## Phase Exit Criteria

Phase 3.4 and the final compatibility gate are complete only when:

- Directed notifications are locally coalesced, create no more than one directed claim and one generic fallback per worker, and never chain fallbacks.
- Generic refill opportunities are coalesced and bounded by currently free slots, including during duplicate notifications, polling overlap, listener reconnect, and run completion.
- Jitter, successful-claim cooldown, and adaptive contention backoff are bounded, observable, decoupled from permits, and do not override PostgreSQL queue ordering or starvation recovery through mandatory polling.
- No worker exceeds `max-concurrent-runs`; permits cover the full claimed-run coordination/execution lifetime and are released on every empty, failure, cancellation, fencing, and shutdown path.
- Normalized busy-slot time is recorded per worker with bounded dimensions; equal-capacity workers are approximately balanced and mixed capacities are approximately proportional under a reproducible sustained backlog.
- Existing lease, heartbeat, retry, recovery, cancellation, fencing, watermark, notification-loss, and PostgreSQL restart behavior remains unchanged and passes the Phase 3.2/3.3 regression gates.
- The standalone root artifact remains Spring-free and PostgreSQL metadata-independent; its packaged CLI returns the existing exit codes, accepts the existing CLI/options-file contract, supports multi-table CLI execution, and performs a real SQLite replication without the server runtime.
- CI/release packaging keeps `replicadb` and `replicadb-server` separate, and all fairness, resilience, documentation, and compatibility gates are reproducible with no credentials in artifacts, logs, metrics, or fixtures.
- Only after all of the above pass, `ARCHITECTURE_DECISIONS.md` marks both remaining unchecked entries as complete. No other Phase 3 implementation checklist item remains open.

## Quality Gate Notes

The selected approach is **Explicit deterministic admission policy**. The main critic risks to check are: delayed work accidentally consuming execution permits; duplicate notifications causing fallback recursion; fairness being inferred from raw run counts; local queue state being treated as durable; metrics leaking worker/run identifiers or secrets; and the CLI gate validating only classes rather than the packaged artifact and offline process path. These risks have explicit tasks and tests above.

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy

- Tasks completed as planned: 15/15 (100%).
- Tasks that required implementation adjustment: 4/15 (26.7%).
- Test loop iterations: approximately 35 total (first-pass checks: 20, repair reruns: 11, final gate reruns: 4).

### Gaps Encountered

#### Gap 1: Completion refill had to follow permit release (Plan-to-Implementation)

- **Task**: 4.3 - Refill one generic opportunity per current free slot.
- **Plan assumed**: Completion could request the next generic refill from the execution cleanup path without an explicit ordering requirement.
- **Reality**: Requesting refill before releasing the permit caused the coordinator to observe no free slot and could lose the next opportunity when concurrent runs completed.
- **Resolution**: Completion refill now runs after tracker update, permit release, and capacity refresh, and only when the claim action actually produced a run. Focused tests use one-shot durable-claim fixtures and wait for cleanup completion.
- **Learning**: Capacity-driven refill signals must be emitted after the resource transition they describe, and claim fixtures must model rows becoming ineligible after a successful claim.

#### Gap 2: Raw capacity share and normalized utilization were initially conflated (Plan-to-Implementation)

- **Task**: 6.1 - Prove hybrid admission does not change durable run safety and fairness behavior.
- **Plan assumed**: The test implementation would directly preserve the distinction between proportional raw work and balanced normalized busy-slot time.
- **Reality**: The first integration assertion expected normalized busy-slot time itself to be higher for the two-slot worker, while the correct result was higher raw completion count with approximately balanced normalized utilization.
- **Resolution**: The integration test now asserts both dimensions separately: raw runs increase with configured capacity and normalized busy-slot time remains within a bounded balance tolerance.
- **Learning**: Fairness tests must name the unit under comparison in the assertion; capacity-proportional throughput and normalized utilization are complementary, not interchangeable.

#### Gap 3: Packaged CLI readback required explicit driver registration and output markers (Plan-to-Implementation)

- **Task**: 11.1 - Prove Spring-free packaging and offline execution.
- **Plan assumed**: A JShell readback against the assembled JAR would discover the bundled SQLite driver and expose a parseable numeric result using default interactive output.
- **Reality**: The readback JShell process did not register the driver and its prompt output did not match the initial parser, even though the packaged CLI had completed the replication successfully.
- **Resolution**: The probe explicitly loads `org.sqlite.JDBC` and prints a `ROW_COUNT=` marker before the shell extracts the value. The packaged gate then passes with two replicated rows and exit code `1` for the malformed invocation.
- **Learning**: Artifact smoke probes must explicitly register bundled service providers when using ad hoc runtimes and should communicate through machine-readable markers rather than REPL formatting.

#### Gap 4: Listener tests lagged the timestamped signal overload (Intent-to-Plan)

- **Task**: 5.1/6.1 - Wire wake-up sources and preserve distributed listener behavior.
- **Plan assumed**: Existing listener integration tests already exercised the current timestamp-aware `signalRun(UUID, long)` contract after coordinator changes.
- **Reality**: One integration test still stubbed and verified the older one-argument overload, so the full distributed suite failed even though production routing was correct.
- **Resolution**: The test now matches the timestamped overload and verifies the receive-time argument without changing the listener contract.
- **Learning**: When event APIs carry observability metadata, exact overload and argument assertions belong in the same compatibility sweep as the implementation change.

### Patterns Discovered

- **Post-transition refill**: emit capacity refill wake-ups only after releasing the permit and updating utilization state; see `WorkerDispatchCoordinator`.
- **Two-dimensional fairness**: compare raw completed work against configured capacity and normalized busy-slot time independently; see `HybridWorkerDistributionIT`.
- **Machine-readable artifact probes**: explicitly initialize bundled drivers and emit stable markers from packaged-process checks; see `phase3-cli-compatibility.sh`.
- **Timestamped event compatibility**: preserve receive timestamps for latency metrics and update tests against the exact listener overload; see `PostgreSQLNotificationListenerIT`.
