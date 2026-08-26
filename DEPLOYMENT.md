# ReplicaDB Managed Deployment

This guide covers the managed `replicadb-server` artifact. The root
`replicadb` artifact remains the standalone CLI and does not require
PostgreSQL, Spring Boot, or this topology.

## Topology

The supported distributed topology contains:

- PostgreSQL for job state, schedules, Quartz JDBC tables, sessions, audit
  events, retry state, leases, and login throttling.
- Two or more `api` instances using the same PostgreSQL database and the
  clustered Quartz store.
- One or more `worker` instances using the same PostgreSQL database,
  `LISTEN/NOTIFY`, mandatory polling, and lease heartbeats.

The API product port is normally `8080`. Worker processes set
`server.port=-1` and expose Actuator only on the separately configured
management port, `9091` by default. Do not publish the worker management port
to the public network.

The effective execution capacity is:

```text
worker instances * concurrent runs per worker * jobs per run
```

`jobs per run` is ReplicaDB's existing internal task parallelism. It is not a
second scheduler concurrency setting.

## Build

Build the CLI artifact first because the server POM consumes that artifact:

```bash
mvn -B install -DskipTests
mvn -B -f replicadb-server/pom.xml package -DskipTests
scripts/phase3-image-smoke.sh
```

The server image runs as the non-root `replicadb` user. Select the runtime
with `SPRING_PROFILES_ACTIVE=api` or `SPRING_PROFILES_ACTIVE=worker`; both
profiles use the same server jar.

## Configuration

Provide `DB_URL`, `DB_USERNAME`, and `DB_PASSWORD` through the deployment
secret manager. Do not commit them, put them in Compose files, or print them
in diagnostics. Managed job definitions store `${env:VARIABLE}` references;
the worker resolves them immediately before execution.

API settings include:

- `SPRING_PROFILES_ACTIVE=api`
- `REPLICADB_SERVER_LOCAL_EXECUTION_ENABLED=false` when workers own execution
- `REPLICADB_SECURITY_BOOTSTRAP_ENABLED=true` only for the controlled first
  administrator bootstrap
- `REPLICADB_BOOTSTRAP_ADMIN_USERNAME` and
  `REPLICADB_BOOTSTRAP_ADMIN_PASSWORD` supplied by the secret manager during
  bootstrap
- `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE=health,metrics,prometheus`

Worker settings include:

- `SPRING_PROFILES_ACTIVE=worker`
- `REPLICADB_WORKER_IDENTITY`, unique per process
- `REPLICADB_WORKER_MANAGEMENT_ADDRESS`, normally a private interface
- `REPLICADB_WORKER_MANAGEMENT_PORT`, `9091` by default
- `REPLICADB_WORKER_MAX_CONCURRENT_RUNS`, `1` by default
- `REPLICADB_WORKER_ADMISSION_JITTER_MAX`, `100ms` by default
- `REPLICADB_WORKER_ADMISSION_GENERIC_COOLDOWN`, `250ms` by default
- `REPLICADB_WORKER_ADMISSION_DIRECTED_QUEUE_CAPACITY`, `1024` by default
- `REPLICADB_WORKER_ADMISSION_ADAPTIVE_BACKOFF_ENABLED`, `true` by default
- `REPLICADB_WORKER_ADMISSION_ADAPTIVE_BACKOFF_INITIAL_DELAY`, `25ms` by default
- `REPLICADB_WORKER_ADMISSION_ADAPTIVE_BACKOFF_MAX_DELAY`, `2s` by default
- `REPLICADB_WORKER_ADMISSION_ADAPTIVE_BACKOFF_DECAY_HALF_LIFE`, `30s` by default
- `REPLICADB_WORKER_LEASE_DURATION`, `5m` by default
- `REPLICADB_WORKER_HEARTBEAT_INTERVAL`, `30s` by default
- `REPLICADB_WORKER_POLL_INTERVAL`, `30s` by default

The worker datasource pool must leave headroom for the listener, claim and
recovery scans, heartbeats, finalization, and the active ReplicaDB runs:

```text
spring.datasource.hikari.maximum-pool-size >= max-concurrent-runs + 4
```

The worker has no product REST controllers, frontend, Spring Security session,
or Quartz scheduler. Its Actuator management port is an operational surface,
not a product API; restrict it with the private network, firewall, or an
authenticated internal proxy.

Worker admission is local to each process. A run notification creates at most
one directed claim opportunity per worker; an empty directed claim can make
one generic fallback, never another fallback. Startup, listener reconnect,
periodic polling, and completion refill at most one generic opportunity per
currently free slot. Jitter, successful-claim cooldown, and decaying
contention backoff delay only the opportunity scheduler and never occupy a run
permit. PostgreSQL remains the only ownership arbiter.

The worker fleet is expected to distribute work approximately, not by strict
round robin. Evaluate sustained backlog with normalized busy-slot time:
`busy-slot-seconds / max-concurrent-runs`. Equal-capacity workers should be
approximately balanced; workers with different capacities should receive
proportionally different raw work while their normalized utilization remains
comparable. Queue age and polling lag are operational signals and do not bypass
cooldown.

## PostgreSQL migrations

Flyway is enabled for both managed profiles and schema initialization is
disabled in Quartz. Apply the forward-only migrations before starting the
cluster:

- V1 through V14: existing managed state, sessions, audit, retries, leases,
  and dispatch state.
- V15: Quartz JDBC PostgreSQL tables and scheduler lock rows.
- V16: shared login-attempt reservations and cleanup indexes.

Do not run Quartz's automatic schema initializer. Do not edit or remove an
applied migration. PostgreSQL is the only durable source of truth for product
schedules and run state.

## Quartz rollout

The API uses the same scheduler name and stable per-job Quartz keys on every
instance. `instanceId=AUTO`, PostgreSQL locking, clustered mode, and
`MISFIRE_INSTRUCTION_DO_NOTHING` prevent duplicate ownership and avoid replaying
missed fires.

The RAMJobStore-to-JDBC handoff is a controlled deployment step. Drain and
stop every API instance using RAMJobStore before starting the first JDBC
cluster member. Apply V15, verify the JDBC settings, then start all API
instances with the same clustered configuration. Mixed RAM/JDBC scheduler
ownership for the same schedules is prohibited; the API clustered-required
guard must reject an accidental RAMJobStore configuration.

Schedule rows in `job_schedule` remain the product-level intent. Startup
reconciliation and schedule updates converge them into one stable Quartz job
and trigger. The PostgreSQL active-run constraint and transactional run
dispatch remain the final protection against overlapping executions.

## Login throttling

Failed authentication is limited to five attempts in a rolling 15-minute
window per account and per source address. Reservations for both keys are
serialized with PostgreSQL transaction advisory locks acquired in sorted order.
The decision fails closed when PostgreSQL is unavailable. Successful
authentication clears the reservation and prior failures; failed
authentication finalizes the reservation as a failure.

The API-only cleanup task deletes expired pending and failed reservations.
The cleanup task is not a Quartz job. A crashed request can temporarily retain
a pending reservation until cleanup; this is conservative and does not open a
cluster-wide bypass.

## Health and metrics

API probes are available at `/actuator/health`,
`/actuator/health/liveness`, and `/actuator/health/readiness`. API metrics and
Prometheus scraping require the authenticated/internal management boundary.
The worker exposes the same Actuator paths only on its internal management
port and never on its primary product port.

Health separates process liveness from readiness. PostgreSQL reachability and
the worker polling/executor lifecycle are readiness signals. A disconnected
worker listener is degraded while polling remains active, because
`LISTEN/NOTIFY` is only a wake-up optimization.

Metrics use bounded tags and aggregate values. They cover claims,
notification-to-claim latency, polling lag, lease renewal and expiry,
retries, stale/fenced updates, cancellations, terminal outcomes, listener
state, polling state, and worker capacity. Job ids, run ids, usernames,
connection strings, lease tokens, and resolved credentials are never metric
tags or values.

## Operations and recovery

Workers claim durable run identifiers; they do not receive credentials or
complete job definitions in notifications. A worker reconnects its listener
and polls at startup, after reconnect, and periodically. Missing notifications
therefore affect latency, not correctness.

Runs are never resumed. Worker loss preserves the abandoned attempt and may
create a new attempt from the beginning according to the job retry policy.
`complete-atomic` and `incremental` are the recommended managed modes.
`complete` remains destructive and keeps its cancellation/retry warning even
when an operator explicitly enables automatic retry.

Use the following operational sequence for a planned worker shutdown:

1. Stop accepting new work on the worker.
2. Let active runs finish or request cancellation according to the sink-risk
   warning.
3. Stop polling and listener delivery.
4. Confirm the process exits within the configured shutdown timeout.

For an unplanned worker loss, wait for the PostgreSQL lease to expire, let
polling recover the row, and inspect the old attempt plus its replacement.
Never reopen the old row or infer progress from its row counter.

Back up PostgreSQL with point-in-time recovery. Restore the metadata database
before restoring API/worker processes; it contains schedules, permissions,
leases, retry chains, watermarks, sessions, audit history, and throttling
state.

## Local Compose validation

The repository includes `docker-compose.server.yml` with two APIs, one worker,
and PostgreSQL. Supply the required database/bootstrap variables through the
environment, then run:

```bash
mvn -B -f replicadb-server/pom.xml package -DskipTests
scripts/phase3-compose-smoke.sh
```

The smoke harness creates an isolated Compose project, loads non-secret source
and sink fixtures after Flyway completes, exercises login/CSRF, creates a job,
checks schedule visibility from the second API, triggers a run, and verifies
that the worker writes the expected rows. The worker management port is
internal-only in this topology.

Process-level worker-loss, PostgreSQL-restart, fairness, load, and chaos
validation is a separate Phase 3.4 release gate. The standalone CLI
compatibility gate runs independently of PostgreSQL and Docker. Both gates use
dynamic project names and isolated state, and report missing infrastructure
separately from product failures.
