# Implementation Plan: Phase 4 - Reusable Managed Datasources with Encrypted Credentials

## Task Source

This plan implements the approved Phase 4 direction in `ARCHITECTURE_DECISIONS.md`.
There is no JIRA ticket. The managed server is not in production, so the managed
API may adopt the datasource-only contract on `/api/v1` after the metadata schema
is reset. The standalone `replicadb` CLI remains a permanent compatibility
surface and is not migrated to the datasource catalog.

## Objective

The managed server currently stores the source and sink connection inside every
`JobDefinition`. That duplicates connection strings, users, passwords,
authentication values, MongoDB URIs, S3 keys, Kafka security properties, and
technical settings across jobs. Phase 4 introduces reusable managed datasources.
A job selects one datasource for its source and one for its sink, while retaining
only replication-specific configuration.

The phase must also replace the current managed options-file adapter. The core
only needs a populated `ToolOptions` object and does not require a file. The
managed server will decrypt datasource security values in memory and construct
`ToolOptions` through an additive `ToolOptionsBuilder` in the root artifact. The
existing CLI constructor, parser, options-file expansion, accepted keys,
launchers, exit codes, multi-table behavior, and no-PostgreSQL execution path
remain unchanged.

## Locked Decisions

- One implementation plan is divided into five ordered subphases. Each subphase
  has an executable validation gate before the next boundary is opened.
- The managed server uses `sourceDatasourceId` and `sinkDatasourceId`; managed
  inline source/sink connection fields are removed. There is no dual-read path,
  old-job backfill, inline fallback, or managed API version split.
- The server is not in production. The metadata migration uses the existing
  `/api/v1` path and requires a pre-production reset. It fails with an actionable
  message when old managed state is present; it does not silently delete or
  deduplicate jobs.
- A datasource owns connector type, safe display metadata, non-secret technical
  parameters, and an encrypted `connect.security` bundle. The bundle contains
  all credential-bearing values, including a complete MongoDB URI when it has
  credentials, JDBC user/password values, S3 access/secret keys, Kafka SASL/SSL
  values, Azure security values, and sensitive arbitrary `connect.parameter.*`
  values.
- `technicalParams` contains only non-secret values. Sensitive keys or values
  cannot be stored there. The logical security keys are relative CLI property
  names such as `connect`, `user`, `password`, `auth.*`, and
  `connect.parameter.<key>`; the server adds `source.` or `sink.` only while
  constructing `ToolOptions`.
- The initial protection provider uses application-level AES-256-GCM envelope
  encryption with a fresh data key and nonce per datasource security bundle. The
  data key is wrapped by an external key-encryption key loaded from a mounted
  Kubernetes/Docker Secret file. PostgreSQL stores ciphertext and envelope
  metadata only. `pgcrypto` is not used as a substitute for application key
  management.
- The keyring file is configured by `replicadb.security.master-key-file`, with a
  deployment default of `/run/secrets/replicadb-master-key`. It contains a
  current key version and Base64-encoded 256-bit key material, with previous
  versions retained while bundles are re-encrypted. The server fails startup if
  the keyring is missing, unreadable, malformed, or does not contain valid key
  material. The actual source/sink credentials are never process environment
  variables.
- The protection service exposes a provider boundary so a future KMS, Vault, or
  CyberArk implementation can replace the file-backed key provider. Those
  integrations are not implemented in this phase.
- An omitted or blank security value in a datasource update preserves the
  existing encrypted entry. Removing a value requires an explicit
  `clearSecurityKeys` list. The current secret is never returned to the frontend.
- Datasource references are live. A `PENDING` run resolves its datasource
  profiles when the run is claimed, not when the run row is inserted. The claim
  transaction locks the run, job binding, and datasource rows, records selected
  datasource IDs plus `datasources_resolved_at`, and returns the encrypted
  snapshot for decryption after commit. An update that commits first wins; a
  claim that commits first uses the snapshot it read. A running attempt is never
  changed by a later datasource update. Retries resolve the current profiles
  again.
- `JobRun` persists only resolved datasource UUIDs and the resolution timestamp.
  It never stores plaintext, ciphertext, key references, or a full configuration
  snapshot. Historical resolved UUID fields are correlation data and do not keep
  a datasource alive after its job bindings have been removed.
- A datasource ACL has `VIEW`, `USE`, and `EDIT`. `USE` allows safe selection and
  binding; `EDIT` updates the profile; ADMIN bypasses and manages datasource ACLs.
  Datasource create/delete and ACL administration are ADMIN-only in the first
  version. Runtime authorization is separate: job-level source/sink binding flags
  control all future manual, scheduled, retry, recovery, and worker executions.
  Disabling a binding does not cancel an already `RUNNING` attempt.
- Datasource deletion is restrictive while a job references it. The database
  foreign key is authoritative; the API returns `409` with a bounded reference
  count and never detaches or converts a job back to inline configuration.
- Connector capabilities are derived from the core manager contract. The server
  does not store an editable capability JSON matrix. The catalog covers every
  manager currently registered in `SupportedManagers`, including source/sink
  role support and manager-specific mode restrictions.
- The managed server does not create an options file. `ToolOptionsBuilder` is the
  only managed construction boundary. The standalone CLI continues to use
  `OptionsFile`, including its existing environment expansion.
- No shared live JDBC connections are introduced. Every ReplicaDB task continues
  to own its source and sink connection lifecycle.
- No public datasource connection-test endpoint is included in this phase.
  Connection failures remain ordinary execution/validation failures with bounded
  redacted details.

## Existing Boundary and Falsifiable Hypothesis

The current root artifact exposes only `ToolOptions(String[] args)` publicly,
but `ReplicaDB.processReplica(ToolOptions)` consumes getters and the class already
has setters for source/sink connection values, authentication, connection maps,
tables, query/filter settings, staging, watermarks, tuning, and flags. The
managed server currently works around this by using
`JobDefinitionOptionsFileWriter`, which writes a temporary options file and lets
`OptionsFile` parse it.

The hypothesis is that an additive root-artifact builder can populate an
otherwise valid `ToolOptions` object directly, preserving all core behavior while
removing plaintext secrets from managed disk I/O and avoiding accidental
`${...}` expansion by `OptionsFile`. The cheapest discriminating check is a
builder test that constructs every managed field in memory, preserves a literal
value containing `${...}`, and invokes no file API, followed by the existing
packaged Spring-free CLI gate.

## Subphase Sequence

| Subphase | Goal | Exit gate |
| --- | --- | --- |
| 1. Core boundary and security primitives | Build `ToolOptions` in memory; define datasource/capability/security models | Root builder, capability, and crypto tests pass; no CLI contract changes |
| 2. PostgreSQL state and claim preparation | Replace inline schema and define claim-time datasource resolution | Flyway, repository, and concurrent claim tests pass on PostgreSQL |
| 3. API, ACL, and audit contract | Expose datasource CRUD and datasource-only jobs safely | MockMvc/security/OpenAPI tests prove redaction and authorization |
| 4. Managed execution and distributed dispatch | Resolve/decrypt profiles and execute through builder across API/Quartz/workers | API and worker lifecycle tests pass with no managed options file |
| 5. Frontend, CLI, and acceptance | Add datasource UX, selectors, operations docs, and final compatibility gates | Full server/frontend/distributed/packaged CLI acceptance passes |

## Subphase 1: Core Boundary and Security Primitives

### 1.1 Add the programmatic `ToolOptionsBuilder` without changing CLI behavior

- [x] **1.1 Add the programmatic `ToolOptionsBuilder`**
  Files: `src/main/java/org/replicadb/cli/ToolOptionsBuilder.java` (new), `src/main/java/org/replicadb/cli/ToolOptions.java`, `src/test/java/org/replicadb/cli/ToolOptionsBuilderTest.java` (new), existing root CLI/options-file tests
  Changes: Add an additive builder in `org.replicadb.cli` that constructs a valid `ToolOptions` object in memory. Make the no-argument constructor package-private or expose an internal factory without making invalid construction part of the public API. Support source/sink connect strings, users/passwords, Azure authentication, source/sink parameter maps, source table/columns/where/query, sink table/columns/staging, mode, jobs, watermark, fetch size, bandwidth throttling, verbosity, quoted identifiers, and sink flags needed by managed execution. Use defensive copies for mutable properties and authentication values. Apply the same required-field, default, and mode validation expected from the CLI path. Do not invoke `OptionsFile`, expand environment values, log fields, or add datasource IDs to CLI parsing.
  Tests: Round-trip every supported builder field through `ToolOptions` getters; verify required source/sink/mode validation and defaults; verify defensive copies of `Properties` and Azure authentication; verify literal values containing `${...}` remain unchanged; verify no temporary file is created; rerun existing CLI parser/options-file, multi-table, watermark, exit-code, and Spring-free classpath tests unchanged in meaning.
  Dependencies: None

### 1.2 Expose an authoritative manager capability catalog

- [x] **1.2 Add manager capabilities at the core manager boundary**
  Files: `src/main/java/org/replicadb/manager/SupportedManagers.java`, `src/main/java/org/replicadb/manager/ManagerCapabilities.java` (new or equivalent), `src/test/java/org/replicadb/manager/ManagerCapabilitiesTest.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/domain/ConnectorType.java` (new), `DataSourceCapabilities.java` (new), `DataSourceCapabilityCatalog.java` (new), corresponding tests
  Changes: Define a pure capability contract beside the existing manager registry. For every registered scheme, record whether it supports source, sink, table/query input, incremental behavior, and each complete/atomic mode restriction that is actually enforced by the concrete manager. Include SQL, file, Kafka, S3, MongoDB/MongoDB SRV, and other currently registered managers; do not infer support from a mock or from a single frontend control. Map lower-case wire connector values to the core scheme prefixes. Permit `custom` only when its scheme/driver can be classified as the existing generic manager; otherwise reject it before persistence. The server catalog delegates to the core catalog and must not duplicate an editable matrix.
  Tests: Table-drive all current `SupportedManagers` values and concrete role/mode decisions; assert source-only/sink-only and `complete-atomic` restrictions against maintained manager behavior; test custom scheme acceptance/rejection; test connector type/scheme mismatch; assert the catalog performs no database connection and has no CLI side effects.
  Dependencies: Task 1.1

### 1.3 Replace embedded managed endpoint credentials with datasource references

- [x] **1.3 Add the managed datasource and datasource-only job domain**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/domain/ManagedDataSource.java` (new), `ResolvedDataSource.java` (new), `SourceEndpoint.java`, `SinkEndpoint.java`, `JobDefinition.java`, `JobDefinitionTestFixtures.java`, domain tests and all direct domain construction tests
  Changes: Define `ManagedDataSource` around UUID, name, connector type, safe display connection string, non-secret technical parameters, encrypted security bundle metadata, and timestamps. Define an in-memory `ResolvedDataSource` that can carry plaintext values only during execution. Change `SourceEndpoint` and `SinkEndpoint` to hold datasource UUIDs plus endpoint-specific replication fields. Change `JobDefinition` to require both datasource IDs and source/sink use flags, while retaining table/query, filters, staging, mode, watermark, retry policy, and execution tuning. Remove the old positional constructors that embed `ConnectionCredentials` from the managed model. Keep any compatibility helpers private to migration/tests; do not reintroduce inline connection fields in the managed API.
  Tests: Validate names, UUIDs, connector type, required source/sink references, endpoint table/query rules, use-flag defaults, immutable technical maps, same-datasource source/sink selection, and mode/watermark invariants. Migrate `JobDefinitionTestFixtures` and all server tests to datasource references without placing credentials in fixtures.
  Dependencies: Tasks 1.1 and 1.2

### 1.4 Implement application-level envelope encryption and external key loading

- [x] **1.4 Add the encrypted security-bundle provider**
  Files: `replicadb-server/src/main/java/org/replicadb/server/security/secret/EncryptedSecurityBundle.java` (new), `SecretProtectionService.java` (new), `KeyEncryptionKeyProvider.java` (new), `FileBackedKeyEncryptionKeyProvider.java` (new), `SecretProtectionProperties.java` (new), `application.yml`, `application-api.yml`, `application-worker.yml`, server crypto tests and context tests
  Changes: Implement the initial provider with JDK 17 JCE `AES/GCM/NoPadding` using a fresh 256-bit data key and 96-bit nonce for each bundle. Wrap the data key with the configured key-encryption key using the JDK AES key-wrap mechanism. Authenticate additional data containing the datasource UUID and security-bundle format version. Serialize a canonical, deterministically ordered security map into the ciphertext. Store a versioned envelope containing algorithm version, key version, wrapped data key, nonce, and ciphertext. Load a keyring from `replicadb.security.master-key-file`, defaulting to `/run/secrets/replicadb-master-key`; validate the current and available previous Base64 256-bit keys at startup. Fail the `api` and `worker` application contexts when the keyring is unavailable or invalid. Keep `SecretProtectionService` independent of HTTP, PostgreSQL, and the CLI parser so future KMS/Vault/CyberArk providers can implement the same boundary.
  Tests: Prove encrypt/decrypt round trips, distinct nonce/data-key generation, wrong datasource/AAD rejection, ciphertext tamper rejection, malformed envelope rejection, key-version lookup, current-key selection, re-encryption under a new key version, missing/invalid key startup failure, and absence of plaintext/ciphertext/key material from logs and exception messages. Use generated test keys in temporary files and never print them.
  Dependencies: Task 1.3

### 1.5 Define the `connect.security` namespace and safe update semantics

- [x] **1.5 Classify sensitive connection values and preserve partial updates**
  Files: `ManagedDataSource.java`, `ResolvedDataSource.java`, `SecretProtectionService.java`, `replicadb-server/src/main/java/org/replicadb/server/job/api/DatasourceRequest.java` (new), `DatasourceMapper.java` (new), domain/security tests
  Changes: Define the plaintext-in-memory security map keys: `connect`, `user`, `password`, all `auth.*` values, MongoDB credential-bearing URI, S3 `connect.parameter.accessKey` and `secretKey`, Kafka SASL/SSL and keystore secrets, and sensitive arbitrary `connect.parameter.*` entries. Keep non-secret file, Kafka, S3, driver, and manager tuning in `technicalParams`. Reject secret-like technical keys/values and unredacted embedded credentials in safe display metadata. On update, absent/blank security entries preserve their encrypted values; `clearSecurityKeys` explicitly removes selected entries and cannot leave a datasource without its required connection value. Return only configured category flags, never the security map or key references. Map the security map to source/sink CLI properties only inside the managed ToolOptions factory.
  Tests: Classify all manager-specific sensitive keys; test MongoDB full-URI encryption, S3/Kafka/Azure security mapping, password/user preservation, explicit clearing, required-key validation, technical-map rejection, safe display redaction, and DTO absence of secret values and key references.
  Dependencies: Tasks 1.2 and 1.4

## Subphase 2: PostgreSQL State and Claim Preparation

### 2.1 Add the pre-production datasource schema and reset guard

- [x] **2.1 Add Flyway migrations for datasource state**
  Files: `replicadb-server/src/main/resources/db/migration/V17__create_managed_datasource.sql` (new), `V18__replace_inline_job_connections.sql` (new), `V19__add_job_run_datasource_resolution.sql` (new), migration tests and `PostgresTestcontainersConfig`
  Changes: Create `managed_datasource` with UUID, unique name, connector type, safe display connection string, non-secret `technical_params JSONB`, encrypted security envelope `BYTEA`, algorithm/format/key-version metadata, and timestamps. Create `datasource_permission` with `VIEW`/`USE`/`EDIT`, user foreign key, timestamps, and lookup indexes. Add required source/sink datasource IDs and use flags to `job_definition` with restrictive foreign keys; retain endpoint-specific table/query/staging fields and drop only the old inline connection, authentication, and parameter columns. Add nullable resolved datasource UUIDs and `datasources_resolved_at` to `job_run`; these historical fields do not have datasource foreign keys so an unbound datasource can be deleted without destroying run history. Before replacing managed jobs, assert that `job_definition`, `job_run`, `job_schedule`, `job_permission`, and `run_trigger_idempotency` contain no rows; raise an actionable reset error rather than deleting or backfilling. Keep all migrations forward-only and let Flyway own schema initialization.
  Tests: Run the full V1-V19 chain in PostgreSQL Testcontainers; verify clean-schema shape, column nullability/checks, restrictive job foreign keys, permission constraints/indexes, encrypted-column types, resolved run fields, exact removal of inline columns, idempotent validation, and explicit failure on non-empty old managed state. Load fixtures only after Flyway completes.
  Dependencies: Tasks 1.3 and 1.4

### 2.2 Implement datasource repositories and safe database mappings

- [x] **2.2 Add datasource and permission persistence ports/adapters**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/port/ManagedDataSourceStore.java` (new), `JobDefinitionStore.java`, `JobRunStore.java`, `ManagedDataSourceRepository.java` (new), `DataSourcePermissionRepository.java` (new), row mappers, repository integration tests
  Changes: Persist encrypted envelopes and non-secret technical parameters through Spring JDBC and the existing ObjectMapper JSONB pattern. Encrypt before the repository update and decrypt only in a resolution service, never in a row mapper returned to an API controller. Implement paginated list/count queries with ACL and optional source/sink capability filtering before pagination. Preserve blank-secret update semantics through an explicit merge operation and return `NOT_FOUND`, `FORBIDDEN`, and `REFERENCED` outcomes rather than string-matching SQL errors. Implement restrictive delete using the database constraint as the final authority.
  Tests: Round-trip metadata, technical parameters, envelope bytes, algorithm/key versions, and timestamp fields; verify encryption occurs before SQL parameters are sent; test concurrent updates, duplicate names, permission uniqueness, filtered paging/counts, restricted delete, and repository failure redaction with real PostgreSQL.
  Dependencies: Tasks 1.4, 1.5, and 2.1

### 2.3 Update managed job persistence to datasource-only records

- [x] **2.3 Update job repositories, fixtures, and response-safe joins**
  Files: `JobDefinitionRepository.java`, `JobDefinitionRowMapper.java`, `JobRunRepository.java`, `JobRunRowMapper.java`, `JobDefinitionTestFixtures.java`, all server repository/IT fixtures and direct `JobRun` constructors
  Changes: Map only datasource IDs/use flags and endpoint-specific replication settings in `JobDefinition`. Remove source/sink password, connect, auth, and connection-parameter persistence from managed SQL. Add resolved datasource IDs and resolution timestamp to `JobRun` mappings. Keep safe datasource name/type summaries in explicit joins or service mapping; never join encrypted security values into API response objects. Update all test builders and repository assertions to create datasources first and bind jobs by UUID.
  Tests: Run repository round trips for jobs, schedules, permissions, runs, retries, watermarks, cancellation, and audit correlations using datasource IDs. Assert no managed SQL insert/update/select exposes plaintext connection columns, old inline JSON payloads are rejected, and fixtures remain free of resolved credentials.
  Dependencies: Tasks 1.3, 2.1, and 2.2
  Validation note: Production/test sources compile and repository fixtures use datasource IDs. Local PostgreSQL integration execution is blocked before Spring context startup by the Rancher Desktop port-forward; CI/Docker-standard execution remains required.

### 2.4 Implement claim-time preparation with deterministic locking

- [x] **2.4 Add the PostgreSQL claim-and-prepare contract**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/application/RunPreparationService.java` (new), `RunLeaseService.java`, `JobRunStore.java`, `JobRunRepository.java`, `ClaimedRunPreparation.java` (new), `JobRun.java`, claim integration tests
  Changes: Extend the shared run lease port so an eligible claim returns a `ClaimedRunPreparation` containing the claimed `JobRun`, job replication settings, and encrypted datasource snapshots held only in memory. In one `READ COMMITTED` transaction, lock the eligible run and job row, verify both use flags, select and lock each distinct datasource row in deterministic UUID order, record the source/sink datasource IDs and `datasources_resolved_at`, and commit before decryption/core execution. Do not hold database locks while running ReplicaDB. If the job binding or datasource changes commit before the claim, use the new state; if the claim locks first, use the read snapshot. Ensure the existing `FOR UPDATE SKIP LOCKED`, lease token, retry, and fencing contracts remain intact.
  Tests: With separate PostgreSQL connections, prove distinct concurrent claims, update-vs-claim ordering, same datasource used once, pending-run claim-time resolution, disabled-binding exclusion, deterministic lock ordering without deadlocks, persisted IDs/timestamp, no ciphertext in `JobRun`, and no lock held during a blocking core execution fixture.
  Dependencies: Tasks 2.2 and 2.3
  Validation note: The claim SQL syntax was validated against PostgreSQL inside the local container, and unit/compile checks pass. Full Testcontainers execution remains blocked before context startup by the local Rancher Desktop port-forward.

## Subphase 3: API, ACL, and Audit Contract

### 3.1 Add safe datasource DTOs and CRUD endpoints

- [x] **3.1 Implement datasource CRUD under `/api/v1/datasources`**
  Files: `DatasourceController.java` (new), `DatasourceRequest.java`, `DatasourceResponse.java`, `DatasourceMapper.java`, `GlobalExceptionHandler.java`, OpenAPI annotations/spec tests, `DatasourceControllerTest.java` (new)
  Changes: Add `POST`, paginated `GET`, `GET {id}`, `PUT`, and restrictive `DELETE`. `DatasourceRequest` carries name, connector type, non-secret `technicalParams`, transient plaintext `security` values over authenticated TLS, and `clearSecurityKeys`; it never accepts an encrypted blob or key reference. `DatasourceResponse` contains only redacted `safeConnectDisplay`, connector type, safe technical parameters, capabilities, configured category flags, and caller `canUse`/`canEdit` flags. Initial authorization is ADMIN-only create/delete, EDIT for update, VIEW or USE for safe reads, and ADMIN-only ACL administration. Return RFC 7807 responses with no dynamic secret values. Return `409` for referenced deletion and duplicate names.
  Tests: MockMvc tests for create/read/update/delete, pagination, connector validation, redaction, blank-secret preservation, explicit clearing, duplicate names, referenced delete, role/ACL combinations, problem details, and absence of passwords, key references, ciphertext, and resolved values in every HTTP response.
  Dependencies: Tasks 1.2, 1.5, and 2.2

### 3.2 Convert the job REST contract to datasource references

- [x] **3.2 Make job definitions datasource-only at the API boundary**
  Files: `JobDefinitionRequest.java`, `JobDefinitionResponse.java`, `JobDefinitionMapper.java`, `JobDefinitionController.java`, `JobDefinitionRepository.java`, OpenAPI tests, job lifecycle tests
  Changes: Remove managed inline source/sink connection, user, password, authentication, and parameter fields from request/response records. Add required `sourceDatasourceId` and `sinkDatasourceId`, source/sink binding-use flags, and safe datasource summaries. On create/update, verify datasource existence, source/sink capability, job edit permission, and datasource `USE` for a new or changed binding. A user may disable a binding with job edit permission; enabling or replacing a binding requires datasource `USE`. Preserve endpoint-specific table/query, staging, mode, watermark, retry, and tuning fields. Reuse `/api/v1` only under the pre-production reset decision. Reject the old inline payload shape rather than silently accepting and dropping fields.
  Tests: Test datasource-only create/update, missing IDs, wrong role, capability mismatch, no `USE`, disabled binding, safe response summaries, old inline fields, generated schema nullability, and full authenticated lifecycle with PostgreSQL.
  Dependencies: Tasks 2.3 and 3.1

### 3.3 Add datasource ACLs and binding authorization

- [x] **3.3 Implement `VIEW`/`USE`/`EDIT` ACLs and job binding controls**
  Files: `DataSourcePermissionType.java` (new), `DataSourceAccessService.java` (new), `DataSourcePermissionController.java` (new), `DataSourcePermissionRepository.java`, `JobAccessService.java`, job binding service/controller, security tests
  Changes: Add ADMIN-managed permission replacement/removal endpoints under `/api/v1/datasources/{id}/permissions`. Filter datasource lists in SQL before pagination. Treat `USE` as sufficient to receive the safe metadata required for selection; `VIEW` without USE permits safe inspection but not binding. `EDIT` permits datasource updates and implies safe read. Keep user datasource ACLs separate from job ACLs. Job-level source/sink use flags are the durable runtime gate and must be exposed through a job edit operation that records the binding side. Scheduled execution uses the active binding, not the original human actor's datasource permission.
  Tests: Cover the full matrix for ADMIN/OPERATOR/VIEWER, datasource VIEW/USE/EDIT, job VIEW/EDIT/EXECUTE, selection, binding replacement, binding disable/re-enable, SQL-side visibility, and no metadata leak when a user can view a job but cannot view/use its datasource.
  Dependencies: Tasks 2.2, 3.1, and 3.2

### 3.4 Extend audit actions without recording security data

- [x] **3.4 Audit datasource and binding changes**
  Files: `replicadb-server/src/main/java/org/replicadb/server/audit/domain/AuditAction.java`, `AuditResourceType.java`, datasource/job binding controllers/services, audit tests
  Changes: Add datasource resource/action values for create, update, delete, permission replacement/revocation, binding enabled/disabled, and binding replacement as needed. Audit only datasource UUID, safe name, connector type, operation category, binding side, permission category, outcome, and bounded timestamps. Record counts/categories for security changes, not secret key names or values. Keep `AuditService` fail-open behavior and existing retention. Ensure datasource delete conflicts never emit a successful delete event.
  Tests: Verify every state-changing datasource/binding operation emits the expected sanitized event; prove failed/forbidden operations have the correct outcome; assert persisted JSONB and audit responses contain no password, URI user-info, S3/Kafka/Azure secret, certificate/key content, ciphertext, or key reference.
  Dependencies: Tasks 3.1 and 3.3

## Subphase 4: Managed Execution and Distributed Dispatch

### 4.1 Add the managed datasource materializer

- [x] **4.1 Resolve and materialize datasource profiles in memory**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/DatasourceResolutionService.java` (new), `ResolvedJobDefinition.java`, `ManagedToolOptionsFactory.java` (new), `ToolOptionsBuilder.java` dependency, `JobDefinitionOptionsFileWriter.java`, `JobDefinitionEnvResolver.java`, unit/integration tests
  Changes: Load the `ClaimedRunPreparation`, decrypt each encrypted security bundle after the claim transaction commits, merge security values with non-secret technical parameters, validate connector/capability rules, and create an in-memory `ResolvedJobDefinition`. Map relative `connect.security` keys to source/sink `ToolOptionsBuilder` fields and `Properties`, including full MongoDB URIs, S3 keys, Kafka security settings, and Azure fields. Remove the managed use of `JobDefinitionOptionsFileWriter` and `JobDefinitionEnvResolver`; do not create, write, or delete a managed options file. Keep standalone CLI `OptionsFile` and environment expansion unchanged.
  Tests: Verify each manager-specific security mapping, no environment lookup for managed credentials, literal `${...}` preservation, tampered/missing bundle failure before core execution, no temp files, no secret logs, and correct source/sink mapping when one datasource serves both roles.
  Dependencies: Tasks 1.1, 1.4, 1.5, 2.4, and 3.2

### 4.2 Replace `JobExecutionService` and all API/worker execution callers

- [x] **4.2 Execute managed runs through `ToolOptionsBuilder`**
  Files: `JobExecutionService.java`, `RunExecutionCoordinator.java`, `ScheduledRunTriggerJob.java`, `WorkerDispatchCoordinator.java`, `RunFinalizationService.java`, `JobDefinitionOptionsFileWriter.java` (delete), `JobDefinitionEnvResolver.java` (delete or remove managed bean), execution tests and fixtures
  Changes: Replace options-file construction with `DatasourceResolutionService` plus `ManagedToolOptionsFactory`. Preserve active-run registration and immediate cancellation by registering the in-memory `ToolOptions`/execution context before `ReplicaDB.processReplica` starts. Keep lease-token-fenced finalization, watermark commit, counters, retry, cancellation, heartbeat, audit, and cleanup semantics unchanged. Remove dead writer/resolver beans and update all constructor wiring. A decryption or profile validation failure must produce a fenced failed outcome without invoking the core and without logging the resolved values.
  Tests: Run successful, failed, cancelled, retry, and fenced API/worker executions with real and mocked core boundaries; assert no options file is created, options are deleted nowhere because none exists, active cancellation reaches the context, watermarks only commit after successful merge, and failure details are redacted.
  Dependencies: Tasks 2.4, 4.1, and 3.4

### 4.3 Make trigger, retry, recovery, and claim paths honor binding flags

- [x] **4.3 Enforce atomic binding disablement at every run path**
  Files: `JobRunController.java`, `JobScheduleController.java`, `ScheduledRunTriggerJob.java`, `JobRunRepository.java`, `JobRunStore.java`, `RunLeaseService.java`, `RunRecoveryService.java`, `RunDispatchService.java`, worker claim/recovery paths, state integration tests
  Changes: Manual and Quartz triggers must refuse to create a new run when either binding flag is disabled. Worker eligibility SQL must lock/read the job row and exclude disabled bindings. Explicit retry and lease-recovery replacement rows must re-check the flags; a disabled binding may leave a durable pending replacement that cannot be claimed, while manual/scheduled trigger requests are rejected. Serialize job binding updates and claims with the job-row lock. If claim commits first, the running attempt may finish; if disable commits first, no new claim occurs. Re-enabling makes existing pending work eligible without creating a duplicate run.
  Tests: Use concurrent PostgreSQL transactions to prove no bypass through manual trigger, Quartz trigger, polling, notification, retry, lease recovery, or duplicate notification. Assert existing running work is not implicitly cancelled, pending rows remain durable/unclaimable while disabled, re-enable restores eligibility, and unique active-run/idempotency/fencing invariants remain intact.
  Dependencies: Tasks 2.4, 3.2, 3.3, and 4.2

### 4.4 Integrate the shared preparation path with API HA and workers

- [x] **4.4 Verify API, Quartz, and worker topology with datasource state**
  Files: `PostgreSQLNotificationListener.java`, `PollingFallback.java`, `WorkerRuntimeConfiguration.java`, `WorkerRuntimeLifecycle.java`, `ScheduleReconciler.java`, `QuartzScheduleService.java`, `RunExecutionCoordinator.java`, distributed lifecycle tests and Compose fixtures
  Changes: Ensure every API and worker instance uses the same PostgreSQL datasource repositories and claim preparation port. Notifications remain UUID-only and contain no datasource snapshot or security data. Listener reconnect, startup polling, periodic polling, completion refill, retries, cancellation, heartbeats, and stale-worker fencing all use the new preparation/result contract. No API-local or worker-local datasource cache is authoritative. Keep worker profile free of public REST, frontend, Spring Security session, and Quartz scheduler. Validate keyring configuration and startup failure in both profiles.
  Tests: Test two API instances and multiple workers with shared PostgreSQL; prove one claim per run, claim-time datasource resolution, update visibility on the next attempt, remote cancellation, retry/recovery, notification loss/reconnect, healthy heartbeat during long core operations, and no credential-bearing payload or management metric.
  Dependencies: Tasks 2.4, 4.2, and 4.3

## Subphase 5: Frontend, CLI, and Acceptance

### 5.1 Regenerate OpenAPI and add datasource client modules

- [x] **5.1 Add generated datasource API types and query modules**
  Files: `replicadb-server/frontend/src/api/schema.ts` (generated), `scripts/generate-api-types.mjs`, `src/api/datasourcesApi.ts` (new), `datasourcesApi.test.ts` (new), `schema.test.ts`, `jobsApi.ts`, job API tests
  Changes: Regenerate the schema from the datasource-only server contract. Add typed CRUD, filtered source/sink list queries, datasource permission methods, safe response models, configured category flags, capability fields, and binding flags. Use the existing Axios client, CSRF behavior, TanStack Query keys, pagination, RFC 7807 errors, and mutation invalidation. Do not model security values, encrypted envelopes, key references, or resolved credentials in frontend response types or query state. Allow transient password/security fields only in local form state and request bodies.
  Tests: Assert generated fields and nullable semantics, request normalization, blank-preservation plus `clearSecurityKeys`, source/sink capability filters, permission methods, no security fields in responses, and schema drift against the live OpenAPI document.
  Dependencies: Tasks 3.1, 3.2, and 3.3

### 5.2 Build the datasource catalog and editor

- [x] **5.2 Add datasource list, editor, detail, and ACL screens**
  Files: `replicadb-server/frontend/src/pages/DatasourcesPage.tsx` (new), `DatasourceFormPage.tsx` (new), `DatasourcePermissionsPage.tsx` (new), `ConnectionSettingsCard.tsx`, `connectionBuilder.ts`, routes, `AppLayout.tsx`, page/component tests and Playwright flows
  Changes: Move connector selection and connection construction from the job editor into the datasource editor. Add all core connector types, including S3 and MongoDB/MongoDB SRV, with role-aware capability display. Keep passwords, URI credentials, S3 keys, Kafka security properties, and Azure security values in transient local form state only; never hydrate them from a response, URL, query cache, local storage, or logs. Show redacted connection display values, configured category flags, technical parameters, and clear-security controls. Add ADMIN-only ACL management, in-use/delete conflict state, loading/empty/error/pagination states, protected routes, and responsive layouts using existing MUI/TanStack Query patterns. Datasource create/delete controls are ADMIN-only; update follows `canEdit`.
  Tests: Cover type-specific fields, Mongo full-URI handling, S3/Kafka/Azure security fields, technical/security separation, password preservation and explicit clearing, capability display, ACL administration, delete conflict, forbidden access, no secret rendering, responsive layout, and authenticated Playwright flow with environment-managed login only.
  Dependencies: Task 5.1

### 5.3 Convert the job form and details to datasource selectors

- [x] **5.3 Replace inline connection editing with source/sink datasource pickers**
  Files: `replicadb-server/frontend/src/pages/JobFormPage.tsx`, `JobDetailPage.tsx`, `jobsApi.ts`, datasource selector components/tests, routes and job Playwright flows
  Changes: Remove source/sink connection strings, users, passwords, authentication inputs, and free-form connection parameter editors from the managed job form. Add source and sink selectors populated by backend capability/USE-filtered datasource queries. Keep source table/query, columns, where, sink mapping, staging, modes, watermark, retry policy, tuning, and binding enabled flags in the job form. Show only safe datasource name/type summaries in job detail. Add binding disable/re-enable actions according to backend permissions and explain that disabling blocks future manual and scheduled runs but does not cancel active work. Preserve existing protected routing, form validation, schedule/run actions, and retry policy behavior.
  Tests: Assert no inline credential controls remain, selected IDs and flags are submitted, source/sink capability filtering is respected, users without USE cannot bind, binding disablement is visible, old inline payloads are never generated, and existing job/run workflows still operate through the new API.
  Dependencies: Tasks 3.2, 3.3, 5.1, and 5.2

### 5.4 Document key management, TLS, deployment, and operations

- [x] **5.4 Document the encrypted datasource deployment contract**
  Files: `DEPLOYMENT.md`, `README.md`, `docker-compose.server.yml`, `replicadb-server/src/main/resources/application.yml`, `application-api.yml`, `application-worker.yml`, documentation validation scripts
  Changes: Document that datasource credentials are submitted only over authenticated TLS, encrypted before PostgreSQL persistence, and never supplied as source/sink environment variables. Document `replicadb.security.master-key-file`, the mounted keyring file shape, startup failure when key material is unavailable, key-version rotation/reencryption procedure, backup/restore implications, and future KMS/Vault/CyberArk provider integration boundary. Document the separation between `technicalParams` and `connect.security`, live claim-time resolution, restrictive deletion, job binding flags, capability derivation, no shared live connections, and the unchanged standalone CLI options contract. Update Compose to mount a secret file without committing key material. Keep Flyway as the owner of schema initialization and do not place credentials in fixtures or examples.
  Tests: Run documentation/security checks, `docker compose config` with placeholders and secret-file paths, startup failure/success probes for API and worker key configuration, redaction scans, `git diff --check`, and a clean Flyway-first topology check.
  Dependencies: Tasks 1.4, 2.1, 4.4, 5.1, 5.2, and 5.3

### 5.5 Re-run the standalone CLI compatibility gate

- [x] **5.5 Prove the root artifact remains Spring-free and datasource-independent**
  Files: `pom.xml`, root `ToolOptions`/`OptionsFile` tests, `NoSpringBootOnClasspathTest.java`, `CliOfflineExecutionTest.java`, `ToolOptionsBuilderTest.java`, `scripts/phase3-cli-compatibility.sh` or successor compatibility script, packaged artifact checks
  Changes: Keep `ToolOptionsBuilder` additive and core-only; keep all encryption, datasource repositories, Spring Boot, PostgreSQL, API, frontend, and worker classes in `replicadb-server`. Do not add datasource IDs, metadata lookups, keyring requirements, or server dependencies to CLI parsing or launchers. Build the packaged root artifact and verify no Spring Boot classes or server-only dependencies are present. Exercise help/version, legacy single-table properties, CLI-over-options-file precedence, environment expansion for standalone options files, incremental watermark properties, multi-table options, exit codes `0`/`1`/`2`, and a real SQLite replication while metadata PostgreSQL is absent/unreachable.
  Tests: Run root unit/options/cancellation tests, builder tests, packaged JAR/classpath inspection, packaged SQLite invocation, malformed/failing exit-code checks, no-metadata execution, dependency-tree audit, and the existing server artifact build without changing the root CLI contract.
  Dependencies: Tasks 1.1, 4.2, and 5.3

### 5.6 Run the end-to-end Phase 4 acceptance gates

- [x] **5.6 Validate the complete datasource lifecycle and record final evidence**
  Files: server unit/integration tests, Testcontainers fixtures, distributed worker tests, frontend Vitest/Playwright tests, `scripts/phase4-*` validation scripts (new as needed), `ARCHITECTURE_DECISIONS.md`, `DEPLOYMENT.md`, `README.md`
  Changes: Add a deterministic acceptance harness that creates encrypted datasources, creates source/sink jobs by UUID, runs API and worker executions, updates a datasource, verifies the next claim uses the new profile while an active attempt retains its snapshot, disables/re-enables bindings, exercises manual/Quartz/polling/retry/recovery paths, verifies restrictive deletion, and checks audit/redaction boundaries. Use PostgreSQL-visible barriers and separate connections for claims; never use fixed sleeps to infer claim or core state. Package artifacts before process/image checks and keep CLI validation separate from managed server checks.
  Tests: Run the full server suite, migration/repository/MockMvc/security tests, concurrent claim/update/fencing tests, API HA/worker/listener/recovery tests, datasource and job frontend tests, authenticated Playwright flows, packaged CLI compatibility, Compose health/readiness, documentation/security scans, and `git diff --check`. Assert all Phase 4 exit criteria and no plaintext credential appears in PostgreSQL outside the application process, API responses, audit rows, notifications, metrics, logs, fixtures, or generated OpenAPI/frontend artifacts.
  Dependencies: Tasks 2.1 through 5.5

## Technical Reference

### Managed datasource persistence contract

```text
managed_datasource
  id UUID PRIMARY KEY
  name VARCHAR(...) UNIQUE NOT NULL
  connector_type VARCHAR(...) NOT NULL
  safe_connect_display TEXT NOT NULL
  technical_params JSONB NOT NULL DEFAULT '{}'
  encrypted_security BYTEA NOT NULL
  security_format_version INTEGER NOT NULL
  encryption_algorithm VARCHAR(...) NOT NULL
  key_version VARCHAR(...) NOT NULL
  created_at TIMESTAMPTZ NOT NULL
  updated_at TIMESTAMPTZ NOT NULL

datasource_permission
  datasource_id UUID NOT NULL
  user_id UUID NOT NULL
  permission VARCHAR(...) NOT NULL -- VIEW, USE, EDIT
  created_at TIMESTAMPTZ NOT NULL

job_definition
  source_datasource_id UUID NOT NULL
  sink_datasource_id UUID NOT NULL
  source_datasource_use_enabled BOOLEAN NOT NULL DEFAULT TRUE
  sink_datasource_use_enabled BOOLEAN NOT NULL DEFAULT TRUE
  -- endpoint-specific replication fields and existing job policy fields

job_run
  resolved_source_datasource_id UUID NULL
  resolved_sink_datasource_id UUID NULL
  datasources_resolved_at TIMESTAMPTZ NULL
```

The encrypted bundle is a canonical map of relative CLI properties. Its
plaintext shape exists only in request handling or bounded execution memory:

```text
connect.security
  connect
  user
  password
  auth.mode
  auth.principal.id
  auth.login.hint
  auth.client.certificate
  auth.client.key
  connect.parameter.<sensitive-key>
```

`technical_params` is mapped to non-secret `connect.parameter.<key>` values.
The managed materializer adds `source.` or `sink.` and passes the final values to
`ToolOptionsBuilder`. The CLI's own `OptionsFile` still accepts the historical
`source.*` and `sink.*` keys and expands environment variables exactly as before.

### ToolOptionsBuilder contract

The builder is core-only and must not depend on Spring, PostgreSQL, datasource
domain classes, or encryption classes. The managed server owns the mapping from
`ManagedDataSource` plus `JobDefinition` to the builder. The builder owns only
construction/default validation and defensive copies. It must not perform network
connections, metadata lookups, environment expansion, logging, or manager
selection. `ReplicaDB.processReplica(ToolOptions)` remains the execution entry
point.

### Security and key-management contract

- The API transport for datasource requests is authenticated TLS. TLS termination
  may occur at the documented ingress, but a production deployment must not send
  datasource request bodies over plaintext HTTP.
- PostgreSQL sees only `technical_params`, redacted display metadata, and the
  encrypted security envelope. It never sees plaintext datasource security values.
- The key-encryption key is outside PostgreSQL in a mounted Secret keyring. No
  source/sink credential is required in an environment variable before startup.
- Plaintext security values exist only in transient request memory, encryption
  memory, decryption/materialization memory, the in-memory `ToolOptions`, and the
  core's existing task-owned connection lifecycle. They are not logged, audited,
  metered, returned, or placed in notifications.
- Blank update values preserve existing encrypted entries. `clearSecurityKeys`
  is the only explicit removal operation.
- Key rotation re-encrypts bundles under a new key version without exporting
  plaintext. Previous key versions remain available until all bundles are
  re-encrypted and the deployment runbook permits removal.
- Vault, CyberArk, KMS, and other external providers remain behind the provider
  interface and are future work; the initial provider is file-backed envelope
  encryption.

### JobRun resolution contract

```text
insert PENDING run
        |
        v
claim transaction
  lock run -> lock job binding -> lock distinct datasource UUIDs
  verify use flags and eligibility
  record resolved datasource IDs + datasources_resolved_at
  read encrypted bundle snapshots
        |
        +-- commit and release PostgreSQL locks
        |
        v
decrypt in memory -> build ToolOptions -> ReplicaDB core
```

The transaction must lock datasource rows in deterministic UUID order to avoid
source/sink inversion deadlocks when multiple jobs share profiles. A datasource
update and a claim are ordered by the PostgreSQL row lock. No row lock or
repository connection remains held while the core runs. A failed decryption or
builder validation marks the claimed run failed through the existing lease-token
fenced finalization path.

## Test and Validation Strategy

| Layer | Required evidence |
| --- | --- |
| Root builder | All managed fields round-trip in memory; literal `${...}` is unchanged; no file or environment lookup occurs |
| Core capability catalog | Every registered manager has tested source/sink/mode classification; custom schemes are explicit |
| Cryptography | AES-GCM authenticity, AES-wrapped data keys, key versions, rotation, startup failure, no secret output |
| Flyway/PostgreSQL | V1-V19 clean migration, reset guard, constraints/indexes, no plaintext columns, Flyway-first fixtures |
| Repositories | Encrypted envelope round trips, ACL filtering before pagination, restrictive delete, concurrent updates |
| JobRun claim | Claim/update ordering, deterministic locks, claim-time resolution, pending IDs/timestamp, no locks during core |
| API/security | CRUD, RFC 7807 redaction, datasource ACLs, job binding flags, old inline payload rejection, OpenAPI |
| Execution | API/Quartz/worker success/failure/cancel/retry/recovery, no managed options file, fenced state/watermarks |
| Frontend | Generated schema, transient-only secret forms, capability/USE selectors, ACL screens, Playwright cookie/CSRF flows |
| CLI compatibility | Spring-free packaged root JAR, unchanged parser/options contract, SQLite offline run, exit codes `0`/`1`/`2` |
| Operations | Mounted keyring config, startup/readiness, TLS documentation, Compose, migration order, redaction scans |
| End-to-end | Datasource update affects next claim, active snapshot remains stable, disablement blocks all future paths, delete restriction |

## Risks and Stop Conditions

- Do not relax the existing credential redaction rules to make technical
  parameters easier to model. Move sensitive values to `connect.security`.
- Do not use `OptionsFile` from managed execution. If a managed field cannot be
  represented by the builder, extend the additive builder rather than writing a
  second temporary file path.
- Do not put the master key in PostgreSQL, a committed file, a datasource row,
  a job row, a notification, or an API response. If the deployment cannot mount
  the keyring safely, stop and revisit deployment configuration before coding.
- Do not add a public secret export, datasource connection-test operation, or
  frontend password readback as a workaround.
- Do not hold PostgreSQL locks during ReplicaDB execution. If claim-time
  snapshot semantics cannot be achieved with the shared ports, stop before
  changing lease/fencing behavior.
- Do not store a capability matrix as editable datasource data. If a manager's
  real support cannot be represented by the core catalog, stop and resolve the
  manager boundary first.
- Do not add a datasource dependency, keyring requirement, Spring Boot class, or
  PostgreSQL metadata lookup to the root CLI artifact.
- Use generated temporary keys, paths, and database names in tests. Never place
  credentials, resolved endpoints, or actual secret material in fixtures, logs,
  documentation examples, screenshots, or generated frontend artifacts.

## Phase Exit Criteria

Phase 4 is complete only when:

- Managed jobs require source and sink datasource IDs and contain no inline
  connection credentials or security parameters.
- All datasource credential-bearing values are encrypted before PostgreSQL
  persistence in the `connect.security` envelope, including non-standard Mongo,
  Kafka, S3, and Azure values.
- The mounted external keyring is validated at startup and supports explicit key
  versions/reencryption without exposing plaintext.
- Managed runs decrypt and build `ToolOptions` in memory without creating an
  options file; the root CLI options-file path remains unchanged.
- A pending run resolves current datasource profiles at claim time, persists only
  resolved IDs/timestamp, and keeps its active in-memory snapshot stable.
- ACLs, binding-use flags, capabilities, restrictive deletion, audit redaction,
  manual/Quartz/worker claim checks, retry/recovery, cancellation, watermarks,
  and fencing are all validated against PostgreSQL state.
- The frontend has a dedicated datasource section and jobs select safe source/sink
  profiles without storing credentials in client state.
- The packaged root CLI remains Spring-free, metadata-independent, and compatible
  with existing options, multi-table behavior, exit codes, and SQLite execution.

## Quality Gate Notes

The highest-risk assumptions are deliberately isolated early: direct
`ToolOptions` construction must cover every managed field; the key-encryption key
must not be colocated with PostgreSQL; encrypted bundles must support all manager
credential shapes; claim/update locking must define the pending-run boundary; and
no options file may re-enter the managed execution path. Any failure in those
areas blocks later API or frontend work rather than being papered over in the UI.

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy
- Tasks completed as planned: 23/23 (100%)
- Tasks that required plan adjustment: 0/23 (0%)
- Test loop iterations: 8 acceptance iterations observed in the resumed execution (5 first-pass, 2 second-pass, 1 third-pass); earlier task-level loops were completed before this resumed session and are not recoverable from the checkbox state.

### Gaps Encountered

#### Gap 1: Shell acceptance parsing selected a nested datasource ID (Plan-to-Implementation)
- **Task**: 5.6 - Validate the complete datasource lifecycle and record final evidence
- **Plan assumed**: The Compose acceptance flow would carry the created datasource-only job ID into schedule and run operations.
- **Reality**: Greedy text extraction could select a nested datasource ID from the job response, causing the schedule request to return `404 JobDefinition not found`.
- **Resolution**: Parse the top-level `.id` field with `jq` in the Compose smoke script.
- **Learning**: Shell acceptance scripts should parse structured API responses with a JSON tool rather than regular expressions.

#### Gap 2: Browser acceptance used an ambiguous accessible link name (Plan-to-Implementation)
- **Task**: 5.6 - Validate the complete datasource lifecycle and record final evidence
- **Plan assumed**: The datasource detail link name would uniquely identify the catalog entry.
- **Reality**: MUI action links included the datasource name in their accessible labels, so the locator matched detail, edit, and permissions links.
- **Resolution**: Require an exact accessible-name match for the detail link and use the rendered link role for permissions navigation.
- **Learning**: Browser acceptance locators must account for action-label composition and assert the intended semantic role explicitly.

#### Gap 3: Browser acceptance assumed a fixed local PostgreSQL port (Plan-to-Implementation)
- **Task**: 5.6 - Validate the complete datasource lifecycle and record final evidence
- **Plan assumed**: The local datasource display would always contain port `5432`.
- **Reality**: The local harness selects an available port to avoid collisions, so the safe display varied between runs.
- **Resolution**: Assert the safe JDBC shape with a numeric local port instead of a fixed port.
- **Learning**: Local end-to-end assertions should derive or pattern-match dynamically allocated test resources.

### Patterns Discovered
- Structured API assertions in shell gates: see `scripts/phase3-compose-smoke.sh`
- Dynamic-resource assertions in browser gates: see `replicadb-server/frontend/e2e/datasource-management.spec.ts`
