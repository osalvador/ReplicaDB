# Implementation Plan: New Job screen parity with the ReplicaDB configuration wizard

## Task Source
No JIRA ticket. Source: direct user request. `ARCHITECTURE_DECISIONS.md` records Phase 2b ("Job editor and run actions") as already **IMPLEMENTED** — this plan is a follow-on to that shipped surface, not a continuation of a pending item.

Acceptance criteria (agreed during planning, since there is no ticket):
- Single-page layout (not a step wizard): Basics on top, Source and Sink side-by-side cards, Execution/Review below — mirroring the jQuery wizard's structure.
- A database-type selector (Oracle, MySQL, MariaDB, PostgreSQL, DB2 LUW, DB2 for i, SQLite, SQL Server, Denodo, File, Kafka [sink only]) drives a dynamic connection builder (host/port/database, or file path, or SQLite path) that composes the JDBC/file/Kafka connect string, exactly like the wizard's Handlebars `createConnect` helper.
- All non-CLI-only sections from the wizard are reachable: Connection Settings, Azure AD Authentication, Data Filtering (Table+Columns+Where vs. Query tabs), File Settings/Parsing, Sink Data Mapping, Staging Options, Escape/Truncate toggles, Kafka Settings, Extra JDBC/Kafka parameters, Fetch Size, Bandwidth Throttling, Verbose.
- Excluded: the "Generate/Download config file" / "Run with this command" footer — that is CLI-only and not applicable to the managed server, which persists and executes jobs itself.
- The backend must actually persist and **execute** these fields (not just accept and drop them), since the user explicitly asked to extend the backend rather than fake the fields client-side only.

## Overview
The managed-server job form only exposes a fraction of what ReplicaDB's CLI/options-file supports: a raw JDBC connect string, user/password, table, where, mode, jobs and watermark. This plan extends the domain model, persistence, REST contract, and CLI execution wiring to cover the remaining wizard sections, then rebuilds the New/Edit Job page as a single-page, card-based form with a type-driven connection builder, matching the wizard's information architecture and adopting Airbyte-style visual polish (dynamic per-type sections, grouped cards, inline help text).

**Scope note**: this is intentionally one large plan per explicit user instruction ("only 1 plan for everything, ignore the 20-task limitation"). Tasks are still ordered foundation → adapters → API → execution → frontend → tests so each can be implemented and verified independently.

## Architecture & Design

**Approach chosen — structured domain extension + options-file execution (rather than 30+ flat parameters or CLI-arg patching):**

1. **Domain model**: `JobDefinition` gains two nested value objects, `SourceEndpoint` and `SinkEndpoint`, each wrapping a shared `ConnectionCredentials` (connect/user/password/`AzureAuthentication`/`connectionParams` map). This avoids a 30+ parameter flat record and mirrors the wizard's own card grouping (Connection Settings, Azure AD Authentication, Extra JDBC params). `StagingOptions` is a small nested value object on `SinkEndpoint`.

2. **The "extra JDBC parameters" bucket does double duty.** Research into `OptionsFile`/`ToolOptions` (core CLI) shows that the wizard's File-format details (`delimiter`, `quote`, `escape`, `nullString`, `firstRecordAsHeader`, `ignoreEmptyLines`, `ignoreSurroundingSpaces`, `trim`, `recordSeparator`) and Kafka sink settings (`topic`, `partition`, `acks`) are **not separate CLI flags** — they are all read from `source.connect.parameter.*` / `sink.connect.parameter.*` properties in an options file. So the domain only needs **one** generic `Map<String,String> connectionParams` per endpoint; the frontend's File Settings and Kafka Settings forms just write into that same map using reserved keys (`format`, `format.delimiter`, `topic`, `partition`, `acks`, ...). This keeps the backend schema an order of magnitude smaller than modeling every File/Kafka field as its own column.

3. **Execution wiring changes from CLI-args to a generated options file.** Today `JobExecutionService` calls `ToolOptionsArgsBuilder.build(...)` to produce a `String[]` of `--flag value` pairs, then `new ToolOptions(args)`. CLI args have no way to express `source.connect.parameter.*` (arbitrary key=value pairs), so `ToolOptionsArgsBuilder` is replaced with `JobDefinitionOptionsFileWriter`, which serializes the full definition (secrets resolved) into a temporary properties file using the exact key names `OptionsFile` already parses, then invokes `new ToolOptions(new String[]{"--options-file", tempPath})`. This is the same mechanism the standalone CLI uses, so managed-server runs and CLI runs share one parser — lower drift risk than maintaining two translation layers.
   - **Security**: the temp file transiently contains resolved secrets (passwords, certs). It is created with `Files.createTempFile` + POSIX owner-only permissions (`rw-------`) where supported, and deleted in a `finally` block immediately after `ReplicaDB.processReplica(options)` returns (success or failure). This mirrors the existing rule that secrets must never linger in logs/audit/context.

4. **REST DTOs stay flat** (`JobDefinitionRequest`/`JobDefinitionResponse` remain plain records with prefixed field names, ~34 fields) — that matches the project's existing DTO convention and keeps the generated OpenAPI/TS types simple to consume from the frontend. `JobDefinitionMapper` translates flat ⇄ nested.

5. **Frontend stays single-page** (not a step wizard) with MUI `Card`s for Basics / Source / Sink / Execution, each internally using `Select`, `Tabs`, and collapsible sections — reusing the wizard's card names so the mental model transfers directly for existing users. A new `connectionBuilder.ts` module composes a JDBC/file/Kafka connect string from `{type, host, port, database, sqliteFilePath, filePath, oracleFormat}` (mirrors the wizard's Handlebars `createConnect` helper) and best-effort **parses** an existing connect string back into those fields for edit-mode prefill. Unrecognized/custom connect strings fall back to a "Custom" type with a single free-text field, so editing a hand-crafted or future connect string never loses data.

**Integration points**: `job_definition` table (Flyway), `JobDefinitionRepository`, `JobDefinitionMapper`, `JobDefinitionRequest`/`Response`, `JobDefinitionController` (no route changes), `JobExecutionService`, `JobDefinitionOptionsFileWriter` (new, replaces `ToolOptionsArgsBuilder`), frontend `jobsApi.ts`, `JobFormPage.tsx`, `JobDetailPage.tsx`, generated `schema.ts`.

**Performance/security implications**: `connectionParams` is stored as `jsonb` (pattern already used by `AuditEventRepository` for `Map<String,String>` via Jackson `ObjectMapper` + `CAST(:detail AS jsonb)`). Each entry is validated to reject embedded-credential-looking values (same defense-in-depth spirit as the existing `EMBEDDED_CREDENTIAL` check on connect strings), so a user can't smuggle a raw password into what's meant to be non-secret JDBC/Kafka tuning properties. `JobDefinitionResponse` redacts `connectionParams` with the existing `CredentialRedactor.redactProperties` before returning them, same as `ToolOptions.toString()` already does.

**What could break**: every existing test that constructs `new JobDefinition(...)` positionally (14 files total) breaks once the constructor becomes nested — `JobDefinitionTest.java` is migrated by hand in Task 1.3 (it's rewriting that file's cases anyway), `ToolOptionsArgsBuilderTest.java` is deleted in Task 4.1 (its subject class is deleted), and Task 5.1 adds a shared `JobDefinitionTestFixtures` builder-style helper for the remaining 12 files so future field additions don't require touching all of them again.

## Implementation Tasks

### 1. Domain Model Foundation
- [x] **1.1 Add `AzureAuthentication`, `ConnectionCredentials`, `StagingOptions` value objects**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/domain/AzureAuthentication.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/domain/ConnectionCredentials.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/domain/StagingOptions.java` (new)
  Changes: `AzureAuthentication(String mode, String principalId, String loginHint, String clientCertificate, String clientKey)` — all nullable; a `null` `AzureAuthentication` constructor argument passed into `ConnectionCredentials` is replaced with `new AzureAuthentication(null, null, null, null, null)` ("no auth configured"), so `ConnectionCredentials.authentication()` is never null and callers never null-check the container itself. `ConnectionCredentials(String connect, String user, String password, AzureAuthentication authentication, Map<String,String> connectionParams)` with a compact constructor that (a) requires `connect` non-blank, (b) rejects embedded credentials in `connect` (reuse the existing `EMBEDDED_CREDENTIAL` regex from `JobDefinition`), (c) **carries over `JobDefinition`'s existing `validateSecretReference` rule verbatim: `password`, when non-null, must match `${env:VARIABLE}`** (this rule is being moved out of `JobDefinition`, not dropped — Task 1.3 must not leave a gap here), (d) rejects any `connectionParams` entry whose key or value matches a password/secret-looking pattern, (e) defaults `connectionParams` to `Map.of()` when null and wraps it `Map.copyOf(...)` for immutability.  `StagingOptions(String schema, String table)`.
  Tests: New `AzureAuthenticationTest`, `ConnectionCredentialsTest`, `StagingOptionsTest` — valid/blank/embedded-credential/malicious-connectionParams-entry cases; **`rejectsLiteralPassword` and `acceptsEnvironmentPasswordReference` cases mirroring the two equivalent existing `JobDefinitionTest` cases being removed in Task 1.3**; `nullAuthenticationDefaultsToEmptyInstance` (never returns `null` from `authentication()`); immutability of `connectionParams` (mutating the input map after construction does not change the object).
  Dependencies: None

- [x] **1.2 Add `SourceEndpoint` / `SinkEndpoint` value objects**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/domain/SourceEndpoint.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/domain/SinkEndpoint.java` (new)
  Changes: `SourceEndpoint(ConnectionCredentials connection, String table, String columns, String where, String query)` — compact constructor requires at least one of `table`/`query` to be non-blank (mirrors the wizard's Table-vs-Query tabs being alternatives). `SinkEndpoint(ConnectionCredentials connection, String table, String columns, StagingOptions staging, boolean disableEscape, boolean disableTruncate)` — `table` remains always-required (no query alternative on the sink side, matching the wizard).
  Tests: New `SourceEndpointTest` (table-only, query-only, neither present throws, both present allowed), `SinkEndpointTest` (blank table throws, staging optional).
  Dependencies: Task 1.1

- [x] **1.3 Restructure `JobDefinition` to use the new nested endpoints and add execution-tuning fields**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobDefinition.java`
  Changes: Replace the 20 flat source/sink fields with `SourceEndpoint source, SinkEndpoint sink`; add `int fetchSize, int bandwidthThrottling, boolean verbose`. Keep `id, name, mode, jobs, incrementalWatermarkColumn, initialWatermarkValue, createdAt, updatedAt` flat. Compact constructor: keep existing `name`/`jobs`/`mode`/watermark-mode validation; add `fetchSize > 0` and `bandwidthThrottling >= 0` checks; **delete** the now-duplicated `EMBEDDED_CREDENTIAL`/`validateSecretReference` checks from `JobDefinition` itself, since `ConnectionCredentials` (Task 1.1) now owns them — do not delete them until Task 1.1 is merged.
  Tests: Update `JobDefinitionTest` directly (this task migrates this one file by hand, using the new nested constructor inline — it does **not** wait for the shared fixture helper, avoiding a dependency cycle with Task 5.1, which migrates the *other* 12 test files only after this task lands): replace positional flat-field cases with the new nested shape; remove the two password-format cases now covered by `ConnectionCredentialsTest` (Task 1.1); add `rejectsNonPositiveFetchSize`, `rejectsNegativeBandwidthThrottling`.
  Dependencies: Task 1.2

- [x] **1.4 Reserved `connectionParams` key convention (documentation, no runtime code)**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/domain/ConnectionCredentials.java` (Javadoc only)
  Changes: Document, on the `connectionParams` field, the reserved key namespace the frontend relies on: `format`, `format.delimiter`, `format.quote`, `format.escape`, `format.nullString`, `format.firstRecordAsHeader`, `format.ignoreEmptyLines`, `format.ignoreSurroundingSpaces`, `format.trim`, `format.recordSeparator` (File settings), and `topic`, `partition`, `acks` (Kafka sink settings) — these become `source.connect.parameter.<key>` / `sink.connect.parameter.<key>` in the generated options file (Task 4.1). This is a one-line-per-key Javadoc list, not a validation change; it exists so a future contributor doesn't duplicate these as first-class columns.
  Tests: None (documentation-only; covered indirectly by Task 4.1's writer tests).
  Dependencies: Task 1.1

### 2. Persistence Layer
- [x] **2.1 Flyway migration for the new columns**
  Files: `replicadb-server/src/main/resources/db/migration/V12__extend_job_definition_advanced_options.sql` (new)
  Changes: `ALTER TABLE job_definition` drops `NOT NULL` from `source_table` so query-only source definitions can be persisted, then adds: `source_auth_mode`, `source_auth_principal_id`, `source_auth_login_hint`, `source_auth_client_certificate`, `source_auth_client_key`, `source_connection_params jsonb NOT NULL DEFAULT '{}'::jsonb`, `source_columns`, `source_query`, `sink_auth_mode`, `sink_auth_principal_id`, `sink_auth_login_hint`, `sink_auth_client_certificate`, `sink_auth_client_key`, `sink_connection_params jsonb NOT NULL DEFAULT '{}'::jsonb`, `sink_columns`, `sink_staging_schema`, `sink_staging_table`, `sink_disable_escape boolean NOT NULL DEFAULT false`, `sink_disable_truncate boolean NOT NULL DEFAULT false`, `fetch_size integer NOT NULL DEFAULT 100`, `bandwidth_throttling integer NOT NULL DEFAULT 0`, and quoted PostgreSQL column `"verbose" boolean NOT NULL DEFAULT false`. All other new columns are nullable `text` and numeric columns have database checks matching the domain invariants.
  Tests: `JobDefinitionRepositoryIT` (Task 2.2) exercises the migration via the Testcontainers Flyway bootstrap already used by existing IT tests; no dedicated migration test beyond that.
  Dependencies: None

- [x] **2.2 Update `JobDefinitionRepository` for the new columns and nested mapping**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobDefinitionRepository.java`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobDefinitionRepositoryIT.java`
  Changes: Extend `INSERT_SQL`/`SELECT_COLUMNS`/`UPDATE` SQL and `ROW_MAPPER` to read/write all new columns, flattening/unflattening `SourceEndpoint`/`SinkEndpoint` at the repository boundary (repository still talks in flat SQL columns; only the Java object passed in/out is nested). Inject `ObjectMapper` (constructor parameter, same pattern as `AuditEventRepository`) to serialize `connectionParams` to/from `jsonb` via `CAST(:sourceConnectionParams AS jsonb)` / `objectMapper.readValue(..., new TypeReference<Map<String,String>>(){})`.
  Tests: Extend `JobDefinitionRepositoryIT` — round-trip insert/update/findById covering: non-empty `connectionParams` on both source and sink, Azure auth fields populated, `sourceQuery`-only definition (no `sourceTable`), staging schema vs. staging table, `fetchSize`/`bandwidthThrottling`/`verbose` non-default values.
  Dependencies: Task 1.3, Task 2.1

### 3. API Contract
- [x] **3.1 Extend `JobDefinitionRequest`/`JobDefinitionResponse` DTOs**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionRequest.java`, `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionResponse.java`
  Changes: Add the flat, prefixed fields listed in Technical Reference → "New DTO fields" to both records (Request additionally carries `sourcePassword`/`sinkPassword`; Response never does). Remove `@NotBlank` from `sourceTable` (now conditionally required — enforced by `SourceEndpoint`'s compact constructor); keep `@NotBlank` on `sinkTable`.
  Tests: Covered by Task 3.2 (mapper) and Task 3.3 (controller); no dedicated DTO test (records have no behavior beyond field access).
  Dependencies: Task 1.3

- [x] **3.2 Redact `connectionParams` in `JobDefinitionMapper.toResponse`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionMapper.java`
  Changes: Map flat request fields into nested `SourceEndpoint`/`SinkEndpoint`/`ConnectionCredentials` when building a `JobDefinition` (`toDefinition`), and flatten back out in `toResponse`. `CredentialRedactor.redactProperties(Properties)` takes a `Properties`, not the `Map<String,String>` used by `connectionParams`; add a small private helper in `JobDefinitionMapper` — `redactConnectionParams(Map<String,String> params)` — that copies the map into a `Properties` instance, calls `CredentialRedactor.redactProperties(...)`, and converts the result back to `Map<String,String>` via `stringPropertyNames()`, applied to both source and sink `connectionParams` before they reach the response (defense-in-depth against a user pasting a secret into an "extra JDBC parameter" field). Preserve the existing password-fallback-on-update behavior, now operating on `request.sourcePassword()`/`existingSourcePassword` feeding into the nested `ConnectionCredentials`.
  Tests: Extend `JobDefinitionMapperTest` — round-trip every new field through `toDefinition`→`toResponse`; assert a `connectionParams` entry containing `password=secret` is redacted in the response; assert query-only source (no table) maps correctly; assert staging/escape/truncate/fetchSize/bandwidthThrottling/verbose round-trip.
  Dependencies: Task 3.1

- [x] **3.3 Update `JobDefinitionController` audit detail and validation groups**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionController.java`
  Changes: No route/method changes required. Verify `@Validated({Default.class, JobDefinitionRequest.Create.class})` still fires for the new required-on-create semantics; keep `auditDetail(...)` limited to name/mode/jobs/tables and make its source-table summary null-safe (`<query>` marker when the source uses `sourceQuery`) without recording the SQL text or connection details.
  Tests: Extend `JobDefinitionControllerTest`/`JobLifecycleIT` — create/update a job using the new fields end-to-end through the REST layer; assert 400 with a clear message when neither `sourceTable` nor `sourceQuery` is provided; assert `connectionParams` redaction survives a full HTTP round trip.
  Dependencies: Task 3.2

### 4. Execution Engine Wiring
- [x] **4.1 Replace `ToolOptionsArgsBuilder` with `JobDefinitionOptionsFileWriter`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/JobDefinitionOptionsFileWriter.java` (new), delete `replicadb-server/src/main/java/org/replicadb/server/job/execution/ToolOptionsArgsBuilder.java`, delete `replicadb-server/src/test/java/org/replicadb/server/job/execution/ToolOptionsArgsBuilderTest.java` (fully superseded by the new `JobDefinitionOptionsFileWriterTest` below — do not leave it behind referencing the deleted class or the old positional `JobDefinition` constructor)
  Changes: `Path write(JobDefinition definition, String previousWatermarkValue, Function<String,String> valueResolver) throws IOException` — creates a `Properties` object with keys matching `OptionsFile`'s expected format (`mode`, `jobs`, `fetch.size`, `bandwidth.throttling`, `verbose`, `source.connect`, `source.user`, `source.password`, `source.auth.mode`/`.principal.id`/`.login.hint`/`.client.certificate`/`.client.key`, `source.table`, `source.columns`, `source.where`, `source.query`, `source.connect.parameter.<key>` per `connectionParams` entry, mirrored `sink.*` keys plus `sink.staging.schema`/`.staging.table`/`.disable.escape`/`.disable.truncate`, `incremental.watermark.column`/`.value`), writes it to a `Files.createTempFile("replicadb-job-", ".conf")` with owner-only POSIX permissions when the filesystem supports them, returns the path. Caller is responsible for deletion.
  Tests: New `JobDefinitionOptionsFileWriterTest` — parses the written file back with a real `Properties.load(...)` and asserts every field/section (including `connectionParams` prefixing, omission of blank optional fields, watermark precedence between `previousWatermarkValue` and `initialWatermarkValue`) round-trips exactly like the removed `ToolOptionsArgsBuilderTest` did for the simple fields, plus new cases for auth/staging/columns/query/connectionParams; assert file permissions are owner-only when `PosixFileAttributeView` is supported.
  Dependencies: Task 1.3

- [x] **4.2 Wire `JobExecutionService` to the new writer with guaranteed cleanup**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/JobExecutionService.java`
  Changes: Replace `argumentsBuilder.build(...)` + `new ToolOptions(arguments)` with `Path optionsFile = writer.write(definition, previousWatermark, environmentResolver::resolve); try { options = new ToolOptions(new String[]{"--options-file", optionsFile.toString()}); ...existing execution... } finally { Files.deleteIfExists(optionsFile); }`. Inject `JobDefinitionOptionsFileWriter` in place of `ToolOptionsArgsBuilder`.
  Tests: Extend `JobExecutionServiceIT` — assert the temp file no longer exists on disk after both a successful and a failing run (simulate a bad connect string); assert a run using `sourceQuery` (no `sourceTable`) executes successfully; assert `connectionParams` reach the manager by capturing the `ToolOptions` instance passed to the existing `onStarted` callback (already a parameter of `executeClaimedRun`) and asserting `options.getSourceConnectionParams()`/`getSinkConnectionParams()` contain the exact key/value pairs configured on the test's `JobDefinition` — this is a concrete, decidable assertion, not a best-effort observation.
  Dependencies: Task 4.1

- [x] **4.3 Update remaining execution-path call sites and delete dead references**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/execution/RunExecutionCoordinator.java` (verify no direct `ToolOptionsArgsBuilder` reference — currently none, confirm still true after refactor), `replicadb-server/src/test/java/org/replicadb/server/job/execution/RunExecutionCoordinatorTest.java`
  Changes: No functional change expected; this task is a verification pass to ensure removing `ToolOptionsArgsBuilder` doesn't leave a dangling Spring bean reference or unused import anywhere in the execution package.
  Tests: `mvn -pl replicadb-server test -Dtest=RunExecutionCoordinatorTest` passes unmodified; compile check (`get_errors`) on the `execution` package.
  Dependencies: Task 4.2

### 5. Backend Test Fixture Migration
- [x] **5.1 Add `JobDefinitionTestFixtures` builder helper and migrate remaining call sites**
  Files: `replicadb-server/src/test/java/org/replicadb/server/job/domain/JobDefinitionTestFixtures.java` (new), and the remaining files calling `new JobDefinition(...)` positionally (excluding `JobDefinitionTest.java`, already migrated by hand in Task 1.3, and excluding `ToolOptionsArgsBuilderTest.java`, deleted by Task 4.1): `replicadb-server/src/test/java/org/replicadb/server/job/api/JobDefinitionControllerTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/JobPermissionControllerTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunCancellationRaceTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunControllerTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/api/JobScheduleControllerTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/JobExecutionServiceIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/RunExecutionCoordinatorTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/execution/ScheduleReconcilerTest.java`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobDefinitionRepositoryIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobRunRepositoryIT.java`, `replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobScheduleRepositoryIT.java`, `replicadb-server/src/test/java/org/replicadb/server/security/persistence/JobPermissionRepositoryIT.java` (note: this last file lives under the `security` package, not `job` — different package root than the others in this list)
  Changes: `JobDefinitionTestFixtures` exposes a fluent builder (`aJobDefinition().withName(...).withSourceConnect(...).withSourceQuery(...).withSinkStaging(...).build()`) with sensible defaults for every field (matching today's minimal test fixtures), so adding a future field never again requires touching 12 files. Replace each positional `new JobDefinition(...)` call in the 12 listed files with the builder, preserving each test's specific overridden values.
  Tests: Run the full existing suite for all 12 files unmodified in behavior (this task is a mechanical migration — no new assertions, just confirming nothing regresses via `mvn -pl replicadb-server test`).
  Dependencies: Task 1.3

- [x] **5.2 Cross-field validation tests for the new source/sink rules**
  Files: `replicadb-server/src/test/java/org/replicadb/server/job/api/JobLifecycleIT.java`
  Changes: Add end-to-end HTTP scenarios: create with `sourceQuery` only (no `sourceTable`) succeeds; create with neither `sourceTable` nor `sourceQuery` returns 400 with an RFC 7807 body naming the missing field; update that changes `sourceTable`→`sourceQuery` succeeds; `connectionParams` containing a `password=` entry is rejected with 400.
  Tests: The scenarios above ARE the tests for this task.
  Dependencies: Task 3.3

### 6. Frontend API Client & Connection Builder
- [x] **6.1 Regenerate the OpenAPI-derived schema**
  Files: `replicadb-server/frontend/src/api/schema.ts` (generated), `replicadb-server/frontend/scripts/generate-api-types.mjs` (no code change, just re-run)
  Changes: Run `npm run generate:api-types` against a locally running backend built with Tasks 1–4 so `schema.ts` includes every new `JobDefinitionRequest`/`Response` field.
  Tests: `schema.test.ts` (existing) continues to pass; add an assertion that the new field names exist on the generated type (compile-time check via a type-only test, following the existing pattern in `schema.test.ts`).
  Dependencies: Task 4.3 (the backend must actually build and run end-to-end — migration, repository, mapper, controller, and execution wiring all complete — before its OpenAPI document reflects the new fields)

- [x] **6.2 `connectionBuilder.ts` — compose and parse JDBC/file/Kafka connect strings**
  Files: `replicadb-server/frontend/src/utils/connectionBuilder.ts` (new), `replicadb-server/frontend/src/utils/connectionBuilder.test.ts` (new)
  Changes: `buildConnectString(type: DbType, fields: ConnectionFields): string` mirroring the wizard's `createConnect` Handlebars helper switch (oracle service-name vs. SID format, mysql/mariadb/postgres/db2/db2i/sqlserver/denodo host:port/database, sqlite file path, file `file://` path, kafka `kafka://` bootstrap servers). `parseConnectString(connect: string): ParsedConnection` best-effort reverse-parses a known scheme back into `{type, host, port, database, sqliteFilePath, filePath, oracleFormat}`; returns `{type: 'custom', raw: connect}` for anything unrecognized so edit mode never loses data.
  Tests: Table-driven unit tests per `DbType` covering build→parse round trip, Oracle service-name vs SID variants, an unrecognized custom string falling back to `'custom'`, and edge cases (missing port, IPv6 host).
  Dependencies: None (pure utility, can be built in parallel with backend tasks)

- [x] **6.3 Extend `jobsApi.ts` types and request/response mapping**
  Files: `replicadb-server/frontend/src/api/jobsApi.ts`, `replicadb-server/frontend/src/api/jobsApi.test.ts`
  Changes: Extend `JobDefinitionFormInput` with every new field from Technical Reference (grouped by prefix, matching the DTO); extend `toJobDefinitionRequest` to include them, keeping the existing "omit watermark fields unless incremental" pattern and adding "omit `sourceTable` when `sourceQuery` is set" / vice versa; `connectionParams` sent as a plain `Record<string,string>` (built by the form from structured File/Kafka sub-forms per Task 1.4's reserved-key convention).
  Tests: Extend `jobsApi.test.ts` — request normalization for every new optional field (blank → omitted), `connectionParams` pass-through, mutual exclusivity of table/query in the outgoing payload.
  Dependencies: Task 6.1

### 7. Frontend UI Redesign
- [x] **7.1 `JobFormPage.tsx` — Basics card**
  Files: `replicadb-server/frontend/src/pages/JobFormPage.tsx`
  Changes: Replace the current flat `Stack` with an MUI `Card`-based layout. Basics card: `name` (disabled in edit mode, unchanged), `mode` select, `jobs` (parallel tasks), `fetchSize`, `bandwidthThrottling`, `verbose` checkbox — laid out in a responsive `Grid`/`Stack` row like the wizard's `FormBasics`.
  Tests: Extend `JobFormPage.test.tsx` — Basics fields render with correct defaults/labels and submit the expected payload values.
  Dependencies: Task 6.3

- [x] **7.2 Source card — type selector + connection builder + Azure auth**
  Files: `replicadb-server/frontend/src/pages/JobFormPage.tsx`, `replicadb-server/frontend/src/components/ConnectionSettingsCard.tsx` (new, shared by Source and Sink)
  Changes: `ConnectionSettingsCard` is a reusable component parameterized by `side: 'source' | 'sink'` and `availableTypes: DbType[]` (Source excludes Kafka; Sink includes it). Renders: DB-type `Select`; conditional Host/Port/Database fields (hidden for SQLite/File/Kafka), SQLite file path field, File path field, Kafka bootstrap-servers field, Oracle Service-Name-vs-SID radio group (shown only when type is Oracle, matching the wizard's PDB warning behavior), User/Password fields, and a collapsible "Microsoft Entra Authentication" section (auth mode select + principal id/login hint/client certificate/client key fields) — **shown only when `type === 'sqlserver'`, exactly matching the wizard's `CardSourceAzureAuthentication`/`CardSinkAzureAuthentication` JS toggle (`docs/wizard/index.html`), which reveals that card only for the SQL Server type on both source and sink, never for any other type**. Also renders an "Extra JDBC parameters" multiline text area (`key=value` per line, parsed into the `connectionParams` record). Uses `connectionBuilder.buildConnectString` to compose the final `sourceConnect` on submit.
  Tests: New `ConnectionSettingsCard.test.tsx` — switching type reveals/hides the right fields; Oracle format radio changes the composed connect string; Azure auth section appears only when `type === 'sqlserver'` and is absent for every other type in `availableTypes` (parameterized over all non-SQL-Server types); extra JDBC textarea parses into a params object and back.
  Dependencies: Task 6.2, Task 7.1

- [x] **7.3 Source card — Data Filtering (Table/Columns/Where vs. Query tabs) + File Settings**
  Files: `replicadb-server/frontend/src/pages/JobFormPage.tsx`, `replicadb-server/frontend/src/components/DataFilteringTabs.tsx` (new)
  Changes: `DataFilteringTabs` renders MUI `Tabs` ("Options" / "Query") matching the wizard: Options tab has Table/Columns/Where fields; Query tab has a multiline SQL `TextField`. Selecting one tab clears the other's values before submit (enforces the backend's table-XOR-query-ish rule from Task 1.2, surfaced as a form-level rule, not just a backend 400). When Source type is `file`, render the File Settings sub-form (format select + delimiter/quote/escape/nullString/firstRecordAsHeader/ignoreEmptyLines/ignoreSurroundingSpaces/trim) that writes into `sourceConnectionParams` using the reserved keys from Task 1.4.
  Tests: Extend `JobFormPage.test.tsx` / new `DataFilteringTabs.test.tsx` — switching tabs clears the other's fields; File Settings only appears for `sourceType === 'file'`; submitted payload has the right reserved `connectionParams` keys for File settings.
  Dependencies: Task 7.2

- [x] **7.4 Sink card — type selector, connection builder, Azure auth (reuse Task 7.2's component)**
  Files: `replicadb-server/frontend/src/pages/JobFormPage.tsx`
  Changes: Instantiate `ConnectionSettingsCard` with `side="sink"` and `availableTypes` including Kafka. When type is Kafka, render Kafka-specific fields (Topic, Partition, ACKs, "Extra Kafka producer properties" textarea) instead of Host/Port/Database, writing into `sinkConnectionParams` per the reserved-key convention.
  Tests: Extend `ConnectionSettingsCard.test.tsx` (parameterized for `side="sink"`) — Kafka type reveals Topic/Partition/ACKs and hides Host/Port; submitted `sinkConnectionParams` includes `topic`/`partition`/`acks` keys.
  Dependencies: Task 7.2

- [x] **7.5 Sink card — Data Mapping, Staging Options, Escape/Truncate, File Settings**
  Files: `replicadb-server/frontend/src/pages/JobFormPage.tsx`, `replicadb-server/frontend/src/components/StagingOptionsTabs.tsx` (new)
  Changes: Data Mapping section: Table + Columns fields. `StagingOptionsTabs` mirrors `DataFilteringTabs`'s structure ("Schema" / "Table" tabs) for `sinkStagingSchema`/`sinkStagingTable`. Escape/Truncate checkboxes (`sinkDisableEscape`/`sinkDisableTruncate`, inverted-label to match the wizard's "Escape"/"Truncate" checked-by-default checkboxes). File Settings sub-form shown when Sink type is `file` (same reserved-key convention as Task 7.3, minus first-record-as-header-only fields the wizard omits on the sink side).
  Tests: Extend `JobFormPage.test.tsx` — staging tab switch clears the other field; escape/truncate checkboxes map to the inverted boolean correctly; File Settings only appears for `sinkType === 'file'`.
  Dependencies: Task 7.4

- [x] **7.6 Edit-mode prefill via connection string reverse-parsing**
  Files: `replicadb-server/frontend/src/pages/JobFormPage.tsx`
  Changes: On load in edit mode, run `connectionBuilder.parseConnectString` against the loaded `sourceConnect`/`sinkConnect` to prefill type/host/port/database/oracleFormat; fall back to the `'custom'` type (single free-text field) when parsing fails, so no data is lost. `connectionParams` returned by the API are split back into structured File/Kafka sub-form fields using the reserved-key convention, with any unrecognized keys shown in the raw "Extra parameters" textarea so nothing is silently dropped.
  Tests: Extend `JobFormPage.test.tsx` — loading a job with a recognized Postgres connect string prefills host/port/database; loading one with an unrecognized scheme falls back to the custom raw field; loading `connectionParams` with a mix of reserved File-format keys and one unknown key splits them correctly between the structured fields and the raw textarea.
  Dependencies: Task 7.5, Task 6.2

### 8. Frontend Detail Page, Regression Tests & E2E
- [x] **8.1 `JobDetailPage.tsx` — display the new fields**
  Files: `replicadb-server/frontend/src/pages/JobDetailPage.tsx`, `replicadb-server/frontend/src/pages/JobDetailPage.test.tsx`
  Changes: Extend the `details` array with grouped rows for the new fields (Source: Columns, Query, Auth mode; Sink: Columns, Staging schema/table, Escape/Truncate; Execution: Fetch size, Bandwidth throttling, Verbose), each falling back to "Not configured" like existing rows. Do not render raw `connectionParams` values verbatim without the existing redaction already applied server-side (the response is already redacted by Task 3.2, so this is a direct display, not a new redaction point).
  Tests: Extend `JobDetailPage.test.tsx` — new rows render with populated and empty values.
  Dependencies: Task 6.1

- [x] **8.2 Full `JobFormPage.test.tsx` rewrite for the new structure**
  Files: `replicadb-server/frontend/src/pages/JobFormPage.test.tsx`
  Changes: Replace the current flat-field queries (`getByLabelText(/^Source connection/)`, etc.) with queries against the new card/section structure (type selects, tabs, collapsible Azure section). Keep the existing create/update/validation-error scenarios, adapted to the new interaction pattern (select a type, fill host/port/database, assert the composed `sourceConnect` in the submitted payload) rather than typing a raw connect string.
  Tests: This task's changes ARE the tests — full pass required before moving on.
  Dependencies: Task 7.6

- [x] **8.3 Playwright job-creation smoke test**
  Files: `replicadb-server/frontend/e2e/job-creation.spec.ts` (new)
  Changes: Real-browser flow: log in, navigate to "New job", select PostgreSQL as both source and sink type, fill host/port/database/table for each, submit, assert redirect to the created job's detail page and that the detail page shows the composed connect string's table/columns as configured. Does not attempt to cover every DB type/section — this is a smoke test for the critical path, per the project's existing Playwright scope (`login.spec.ts` is the only other e2e spec today).
  Tests: This task's spec IS the test.
  Dependencies: Task 8.2

- [x] **8.4 Full regression pass**
  Files: none (verification task)
  Changes: Run `mvn -pl replicadb-server test` (backend unit/IT), `npm run typecheck && npm test` and `npm run test:e2e` in `replicadb-server/frontend`, and `get_errors` across both trees to confirm no dangling references to the removed `ToolOptionsArgsBuilder` or the old flat `JobDefinition` fields remain anywhere (including `RunExecutionCoordinator`, schedule reconciliation, and any Javadoc/comments).
  Tests: All of the above test suites passing is the acceptance bar for this task.
  Dependencies: All previous tasks

## Technical Reference

<details>
<summary>New DTO fields (JobDefinitionRequest / JobDefinitionResponse)</summary>

Grouped by prefix; all new fields are optional/nullable except where noted.

**Source**: `sourceColumns`, `sourceQuery` (at least one of `sourceTable`/`sourceQuery` required), `sourceAuthMode`, `sourceAuthPrincipalId`, `sourceAuthLoginHint`, `sourceAuthClientCertificate`, `sourceAuthClientKey`, `sourceConnectionParams: Map<String,String>`.

**Sink**: `sinkColumns`, `sinkAuthMode`, `sinkAuthPrincipalId`, `sinkAuthLoginHint`, `sinkAuthClientCertificate`, `sinkAuthClientKey`, `sinkConnectionParams: Map<String,String>`, `sinkStagingSchema`, `sinkStagingTable`, `sinkDisableEscape: Boolean`, `sinkDisableTruncate: Boolean`.

**Execution**: `fetchSize: Integer` (default 100), `bandwidthThrottling: Integer` (default 0), `verbose: Boolean` (default false).

`JobDefinitionResponse` mirrors the above minus `sourcePassword`/`sinkPassword` (unchanged: only `sourcePasswordConfigured`/`sinkPasswordConfigured` booleans), and redacts `connectionParams` via `CredentialRedactor.redactProperties`.
</details>

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy
- Tasks completed as planned: 27/27 (100%)
- Tasks that required plan adjustment: 5/27 (19%)
- Test loop iterations: 35 validation commands total (24 first-pass, 10 second-pass, 1 third-pass)

### Gaps Encountered

#### Gap 1: Query-only persistence required a schema constraint change (Intent-to-Plan)
- **Task**: 2.1 — Flyway migration for the new columns
- **Plan assumed**: Adding `source_query` would be sufficient for query-only source definitions.
- **Reality**: V1 defined `source_table` as `NOT NULL`, so query-only requests failed at the database boundary.
- **Resolution**: V12 drops `NOT NULL` from `source_table` and the domain/API enforce the table-or-query invariant.
- **Learning**: When adding an alternative representation to a persisted required field, inspect and migrate the original constraint in the same task.

#### Gap 2: PostgreSQL reserved-word compatibility was missing (Plan-to-Implementation)
- **Task**: 2.1 — Flyway migration for the new columns
- **Plan assumed**: The `verbose` column could be added and referenced unquoted.
- **Reality**: PostgreSQL rejected the migration at `verbose`.
- **Resolution**: V12 and repository SQL quote `"verbose"`; the Flyway integration test caught the issue before proceeding.
- **Learning**: Validate every new migration against the target database engine before marking the schema task complete, including common-word identifiers.

#### Gap 3: Query-only audit summaries could not use `Map.of` with null (Intent-to-Plan)
- **Task**: 3.3 — controller validation and audit behavior
- **Plan assumed**: Existing audit detail code was safe when a source table became optional.
- **Reality**: `Map.of("sourceTable", null)` caused a 500 after a valid query-only create.
- **Resolution**: Audit uses the non-sensitive `<query>` marker when no table is configured and never records the SQL text.
- **Learning**: Nullable API/domain alternatives must be checked at every summary, audit, and serialization boundary, not only persistence and validation.

#### Gap 4: Nested domain migration needed an intermediate compatibility surface (Plan-to-Implementation)
- **Task**: 1.3 — nested `JobDefinition` endpoints
- **Plan assumed**: All consumers could migrate atomically with the record change.
- **Reality**: Repository, mapper, execution, and tests were intentionally staged across later tasks, so an immediate removal broke the intermediate build.
- **Resolution**: Nested `source`/`sink` are canonical while legacy scalar accessors and a constructor remain temporarily available to adapter migration callers.
- **Learning**: For cross-layer record migrations, preserve a narrow compatibility bridge until all adapters are converted, then remove it in a dedicated cleanup pass.

#### Gap 5: Authenticated e2e validation requires environment-managed credentials (Environment)
- **Task**: 8.4 — full regression pass
- **Plan assumed**: The local shell would provide the existing Playwright bootstrap credentials.
- **Reality**: Both `REPLICADB_BOOTSTRAP_ADMIN_USERNAME` and `REPLICADB_BOOTSTRAP_ADMIN_PASSWORD` were unset; both Playwright specs stopped at their explicit credential guards.
- **Resolution**: The e2e suite was executed and recorded as environment-blocked; no secret was requested, generated, or hardcoded. Unit, typecheck, build, and backend integration coverage passed.
- **Learning**: Keep authenticated browser tests environment-driven and report missing credential configuration separately from application failures.

### Patterns Discovered
- Options-file parity: `JobDefinitionOptionsFileWriter` can reuse the core `OptionsFile` parser to carry arbitrary JDBC/File/Kafka properties without duplicating manager behavior.
- Endpoint composition: `ConnectionSettingsCard` plus `connectionBuilder.ts` keeps connector-specific UI and URL parsing out of API modules.
- Query-safe summaries: audit and detail views should use stable placeholders/fallbacks for optional source representations rather than rendering nullable values directly.

<details>
<summary>Reserved connectionParams keys (frontend ⇄ backend contract)</summary>

File settings (source and sink, sink omits per-record-header-only nuances the wizard doesn't expose): `format`, `format.delimiter`, `format.quote`, `format.escape`, `format.nullString`, `format.firstRecordAsHeader`, `format.ignoreEmptyLines`, `format.ignoreSurroundingSpaces`, `format.trim`, `format.recordSeparator` (sink only).

Kafka settings (sink only): `topic`, `partition`, `acks`.

These become `source.connect.parameter.<key>` / `sink.connect.parameter.<key>` properties in the generated options file (Task 4.1), exactly matching what `OptionsFile`/`ToolOptions` (core CLI) already parses.
</details>

<details>
<summary>Dependencies</summary>

No new third-party dependencies. `com.fasterxml.jackson.databind.ObjectMapper` is already a Spring Boot Starter transitive dependency and already used for `jsonb` mapping in `AuditEventRepository` — the same pattern is reused for `connectionParams`. `java.nio.file.attribute.PosixFilePermissions` (JDK standard library) is used for temp-file permission hardening in Task 4.1.
</details>

<details>
<summary>Testing Strategy</summary>

- **Domain**: JUnit Jupiter unit tests for every new value object's compact-constructor validation (Tasks 1.1–1.3), following the existing `assertThrows(IllegalArgumentException.class, ...)` style in `JobDefinitionTest`.
- **Persistence**: Testcontainers-backed `JobDefinitionRepositoryIT` round-trips (Task 2.2), same harness already used by existing `*RepositoryIT` classes.
- **API**: `JobDefinitionMapperTest` (pure unit), `JobDefinitionControllerTest`/`JobLifecycleIT` (Spring MVC / full-stack) for HTTP-level validation and redaction (Tasks 3.2–3.3, 5.2).
- **Execution**: `JobDefinitionOptionsFileWriterTest` parses its own output with real `Properties.load` (Task 4.1); `JobExecutionServiceIT` verifies temp-file cleanup and query-only source execution (Task 4.2).
- **Frontend**: Vitest/Testing Library for `connectionBuilder.ts`, `jobsApi.ts`, `ConnectionSettingsCard`, `DataFilteringTabs`, `StagingOptionsTabs`, and the rewritten `JobFormPage`/`JobDetailPage` (Tasks 6.2–8.2); one Playwright smoke spec for the end-to-end creation flow (Task 8.3).
</details>
