# Implementation Plan: Keyring Lifecycle — Status, Re-encryption, and Flat Environment Configuration

## Task Source

No JIRA ticket. Derived from an `/itx-explore` session investigating the ReplicaDB key-management subsystem. The exploration converged on a locked-in scope (see `.ai/archive` note in Overview) after verifying the actual crypto, persistence, and deployment code — several assumptions from an external design review were confirmed, corrected, or discarded against evidence.

Acceptance criteria extracted from the exploration's final consolidation:
1. `GET /api/v1/keyring/status` reports envelope counts per key version, cross-referenced against the keyring this process actually knows, without ever exposing key material.
2. `POST /api/v1/keyring/reencrypt` performs a real, previously-missing operation: batched, idempotent, resumable re-encryption of `managed_datasource` rows to the current key version, using short per-row transactions that do not block job claims.
3. The keyring can be supplied as flat environment variables (`current` + `secondary` slots, max 2 versions) as an alternative to the existing JSON file, without JSON-escaping in shell/CI configuration.
4. `REPLICADB_SECURITY_MASTER_KEY_FILE` keeps working as a deprecated alias; the canonical name becomes `REPLICADB_SECURITY_KEYRING_FILE`.
5. Documentation stops promising a re-encryption operation that doesn't exist, and describes the real two-generation rotation procedure with snapshot-at-startup semantics.

## Overview

`docs/src/content/docs/operations/key-management.md` currently documents a rotation procedure whose step 3 — "Run the datasource re-encryption operation" — has no implementation outside a unit test. This plan closes that gap and, while touching the same configuration surface, replaces the single `REPLICADB_SECURITY_MASTER_KEY_FILE` file-path variable with a small, explicit configuration model (`file` source, or `current`/`secondary` inline slots) that removes JSON from environment variables entirely and matches how AWS Secrets Manager / ECS inject values. The re-encryption operation is deliberately synchronous-per-call and stateless: the existing `key_version` column on `managed_datasource` already acts as a resumability cursor, so no job table, progress tracker, or background scheduler is introduced.

## Architecture & Design

**Approach: Pragmatic Balance** (selected after presenting three scoped alternatives — Minimal reencrypt-only, this balanced scope, and an Extended scope adding a `DataKeyWrappingProvider` KMS seam that was explicitly deferred as premature).

### Key decisions and why

- **No hot-reload, no shared mutable state.** The keyring stays a snapshot loaded once at process construction, exactly as today. Rotation across a fleet is a two-generation rollout (distribute the new version, then activate it), which the exploration determined has **zero unsafe window** when followed in order, because a completed Kubernetes/ECS/Cloud Run rollout is already a synchronization barrier — no new mechanism is needed to detect "does everyone have the key yet". This is a documentation fix, not a code change.
- **`key_version` is the resumability cursor.** `managed_datasource.key_version` is already a first-class column (not buried in the encrypted blob). `POST /keyring/reencrypt` needs no job table, no progress state, and is naturally idempotent: calling it repeatedly converges and eventually reports `remaining: 0`.
- **Short per-row transactions, not one big batch transaction.** `findByIdForUpdate` on `managed_datasource` is already taken inside the job-claim transaction (`JobRunRepository.claim`). A long-lived re-encryption transaction over many rows would contend with job claiming. Each row is re-encrypted in its own transaction using `SELECT ... FOR UPDATE SKIP LOCKED`, so a row currently being claimed is simply skipped and picked up on the next call.
- **Spring self-invocation pitfall, designed around explicitly.** The per-row transactional method (`KeyringRowReencryptor.reencryptOne`) is a separate Spring bean from the batch loop (`KeyringAdministrationService.reencrypt`), because `@Transactional` on a method called via `this.` from within the same class is silently not proxied. This is called out here so a future maintainer does not "simplify" it into one class.
- **The batch loop lives in the client, not the server.** No async job, no `202 Accepted`, no progress-polling endpoint. Each `POST /keyring/reencrypt` call processes one bounded batch and returns `{reencrypted, remaining, converged}`; a caller (operator script, curl loop) repeats the call until `converged: true`. Two admins calling concurrently is safe (`SKIP LOCKED` makes it pure parallelism, not a race).
- **Two configuration sources, not a URI scheme.** A `file:`/`env:` prefixed single variable was considered and rejected: it collides with Windows drive letters (`C:\...`) and implies KMS could plug into the same slot, which is misleading (KMS wraps keys, it does not supply a keyring). Instead: `replicadb.security.keyring.file` (existing JSON file mechanism, renamed) and `replicadb.security.keyring.current.{version,key}` / `.secondary.{version,key}` (new, flat, max 2 versions — matching what a 2-generation rotation ever needs). Setting both is a startup error.
- **Malformed key material fails at startup, not silently later.** Both `FileBackedKeyEncryptionKeyProvider` and the new `EnvBackedKeyEncryptionKeyProvider` validate their key material in their constructors, and `SecretProtectionService`'s constructor already calls `keyProvider.validate()`. A wrong-length or malformed key therefore fails Spring bean creation (process startup), before any request is ever served — this is existing behavior the new provider must preserve, not new behavior to add.
- **`current`/`secondary`, not `current`/`previous`.** During generation 1 of a rotation, the second slot holds the *future* key, not a past one — naming it "previous" would be actively misleading while it is being distributed.
- **`REPLICADB_SECURITY_MASTER_KEY_FILE` becomes a deprecated alias**, not a hard rename. The YAML placeholder chain resolves the new name first, falls back to the old one, and a startup log warning fires when the old name is present. `LocalMasterKeyBootstrap` (pre-Spring local-mode bootstrap) gets the equivalent fallback chain independently, since it does not go through Spring property binding.
- **No hot file-watching, no active-version-in-database.** Both were explored and rejected: file-watching only works reliably on a subset of platforms (breaks silently under Kubernetes `subPath`), and an active-write-version row in PostgreSQL would let a database-write compromise (without keyring access) deny writes fleet-wide — a new integrity attack surface not present today.

### Integration points

```
KeyEncryptionKeyProvider (interface, + new knownVersions())
        │
        ├── FileBackedKeyEncryptionKeyProvider   (existing, renamed property)
        └── EnvBackedKeyEncryptionKeyProvider     (new)
                        ▲
                        │ selects one
                KeyringSourceResolver (new, pure function, unit-testable)
                        ▲
                SecretProtectionConfiguration (existing, updated wiring)

ManagedDataSourceStore (port)
        + countByKeyVersion()
        + findIdPendingReencryption(currentVersion, knownVersions)
                        │
        KeyringRowReencryptor (new, @Transactional per row)
                        │
        KeyringAdministrationService (new, batch loop, no transaction)
                        │
        KeyringController (new, /api/v1/keyring/status, /api/v1/keyring/reencrypt)
                        │
        AuditService.record(KEYRING_REENCRYPTED, KEYRING, "keyring", ...)
```

### Security notes

- `EncryptedSecurityBundle`'s AAD binds each envelope to its `datasourceId`; re-encryption always decrypts and re-encrypts against the *same* `datasourceId`, never moving ciphertext between rows.
- `/keyring/status` never returns key material, only version identifiers and counts — version names are already visible to anyone with database access via the existing `key_version` column, so this introduces no new disclosure.
- `POST /keyring/reencrypt` is `ADMIN`-only and CSRF-protected, consistent with every other mutating endpoint in the codebase (`DatasourceController`, `UserController`).
- Inline environment key material is decoded with a Base64 decoder that strips surrounding whitespace before decoding and reports the variable name and byte count on failure — copy/paste of secrets is the primary real-world source of key-material errors, more so than the encoding mechanism itself.

## Implementation Tasks

### 1. Foundation — provider interface and configuration model

- [x] **1.1 Add `knownVersions()` to `KeyEncryptionKeyProvider` and implement it in `FileBackedKeyEncryptionKeyProvider`**
  Files:
  - `replicadb-server/src/main/java/org/replicadb/server/security/secret/KeyEncryptionKeyProvider.java`
  - `replicadb-server/src/main/java/org/replicadb/server/security/secret/FileBackedKeyEncryptionKeyProvider.java`
  - `replicadb-server/src/test/java/org/replicadb/server/config/PostgresTestcontainersConfig.java`
  - `replicadb-server/src/test/java/org/replicadb/server/job/execution/DatasourceResolutionServiceTest.java`
  - `replicadb-server/src/test/java/org/replicadb/server/security/secret/SecretProtectionServiceTest.java`
  Changes:
  - Add `Set<String> knownVersions();` to the `KeyEncryptionKeyProvider` interface (import `java.util.Set`).
  - `FileBackedKeyEncryptionKeyProvider`: add `@Override public Set<String> knownVersions() { return keys.keySet(); }`.
  - Update the three test-double implementations of `KeyEncryptionKeyProvider` to implement the new method: `PostgresTestcontainersConfig`'s anonymous class returns `Set.of("test")`; `DatasourceResolutionServiceTest`'s anonymous class returns `Set.of(<its single version literal>)`; `SecretProtectionServiceTest.TestKeyProvider` returns `Set.of(first.version(), second.version())`.
  Tests:
  - `FileBackedKeyEncryptionKeyProviderTest`: add a test asserting `knownVersions()` returns all versions present in the keyring JSON (both current and any previous versions), not just the current one.
  - Compile-level verification only for the three test-double updates (they exist purely to satisfy the interface for other tests that must keep passing).
  Dependencies: None

- [x] **1.2 Restructure `SecretProtectionProperties` into a nested `keyring` model and update `application.yml`**
  Files:
  - `replicadb-server/src/main/java/org/replicadb/server/security/secret/SecretProtectionProperties.java`
  - `replicadb-server/src/main/resources/application.yml`
  Changes:
  - Replace the flat `private String masterKeyFile` field with a nested `Keyring` object: `Keyring { String file = "/run/secrets/replicadb-master-key"; Slot current = new Slot(); Slot secondary = new Slot(); }` and `Slot { String version = ""; String key = ""; }`, each with public getters/setters. Follow the exact nested-static-class pattern already used by `WorkerRuntimeProperties` (its `Listener`/`Admission` fields) — plain static classes with getter/setter pairs, no `@NestedConfigurationProperty` annotation needed; that annotation only affects IDE metadata generation, not actual binding, and no class in this codebase uses it.
  - Add constants: `KEYRING_FILE_PROPERTY = "replicadb.security.keyring.file"` and `DEFAULT_KEYRING_FILE = "/run/secrets/replicadb-master-key"`.
  - Keep `MASTER_KEY_FILE_PROPERTY = "replicadb.security.master-key-file"` with a one-line `// deprecated: retained for LocalMasterKeyBootstrap's pre-Spring fallback lookup` comment — it is no longer bound by `@ConfigurationProperties` but is still read as a raw property/env key by `LocalMasterKeyBootstrap` (task 2.4).
  - In `application.yml`, replace:
    ```yaml
    replicadb:
      security:
        master-key-file: ${REPLICADB_SECURITY_MASTER_KEY_FILE:/run/secrets/replicadb-master-key}
    ```
    with:
    ```yaml
    replicadb:
      security:
        keyring:
          file: ${REPLICADB_SECURITY_KEYRING_FILE:${REPLICADB_SECURITY_MASTER_KEY_FILE:/run/secrets/replicadb-master-key}}
          current:
            version: ${REPLICADB_SECURITY_KEYRING_CURRENT_VERSION:}
            key: ${REPLICADB_SECURITY_KEYRING_CURRENT_KEY:}
          secondary:
            version: ${REPLICADB_SECURITY_KEYRING_SECONDARY_VERSION:}
            key: ${REPLICADB_SECURITY_KEYRING_SECONDARY_KEY:}
    ```
  Tests:
  - No standalone unit test for the properties class itself (it is a plain data holder); its bindings are exercised by task 2.3's `SecretProtectionConfigurationTest` updates.
  Dependencies: None

### 2. Adapters — env-backed provider, selection logic, local-mode fallback

- [x] **2.1 Create `EnvBackedKeyEncryptionKeyProvider`**
  Files:
  - `replicadb-server/src/main/java/org/replicadb/server/security/secret/EnvBackedKeyEncryptionKeyProvider.java` (new)
  - `replicadb-server/src/test/java/org/replicadb/server/security/secret/EnvBackedKeyEncryptionKeyProviderTest.java` (new)
  Changes:
  - New final class implementing `KeyEncryptionKeyProvider`, constructor `(String currentVersion, String currentKeyBase64, String secondaryVersion, String secondaryKeyBase64)`.
  - Validates `currentVersion` non-blank; decodes `currentKeyBase64` via a private `decode(String variableName, String base64Value)` helper that: (1) strips leading/trailing whitespace from the input, (2) calls `Base64.getDecoder().decode(...)`, catching `IllegalArgumentException` and rethrowing as `IllegalStateException(variableName + " is not valid Base64", exception)` for malformed input, (3) separately checks the decoded length and throws `IllegalStateException(variableName + " decodes to " + length + " bytes; 32 are required")` when the length is not exactly 32 — these are two distinct, separately-tested error paths (malformed encoding vs. wrong length).
  - Secondary slot is optional as a pair: if exactly one of `secondaryVersion`/`secondaryKeyBase64` is blank while the other is not, throw `IllegalStateException`. If `secondaryVersion` equals `currentVersion`, throw `IllegalStateException`.
  - `current()` returns the current-version `KeyEncryptionKey`; `find(version)` looks up either slot; `knownVersions()` returns the key set (1 or 2 entries).
  Tests (`EnvBackedKeyEncryptionKeyProviderTest`):
  - Constructs successfully with only a current slot; `knownVersions()` has size 1.
  - Constructs successfully with both slots; `find()` resolves both versions; `current()` resolves only the current one.
  - Blank `currentVersion` throws `IllegalStateException`.
  - `currentKeyBase64` containing characters outside the Base64 alphabet (e.g. `"not-valid-base64!!"`) throws `IllegalStateException` whose message contains `REPLICADB_SECURITY_KEYRING_CURRENT_KEY` and "not valid Base64".
  - `currentKeyBase64` decoding to 31 bytes (well-formed Base64, wrong length) throws `IllegalStateException` whose message contains `REPLICADB_SECURITY_KEYRING_CURRENT_KEY` and `31`.
  - `currentKeyBase64` with a trailing newline (simulating `echo ... | base64`), and separately with leading/trailing spaces and a leading tab, all still decode successfully (whitespace-tolerant on every side, not just trailing newline).
  - Only `secondaryVersion` set (no key) throws `IllegalStateException`.
  - `secondaryVersion` equal to `currentVersion` throws `IllegalStateException`.
  Dependencies: Task 1.1 (interface has `knownVersions()`)

- [x] **2.2 Create `KeyringSourceResolver`**
  Files:
  - `replicadb-server/src/main/java/org/replicadb/server/security/secret/KeyringSourceResolver.java` (new)
  - `replicadb-server/src/test/java/org/replicadb/server/security/secret/KeyringSourceResolverTest.java` (new)
  Changes:
  - Final utility class, single static method `resolve(SecretProtectionProperties properties, org.springframework.core.env.Environment environment, com.fasterxml.jackson.databind.ObjectMapper objectMapper)` returning `KeyEncryptionKeyProvider`.
  - Logs a `LogManager.getLogger` message **at WARN level** if `environment.getProperty("REPLICADB_SECURITY_MASTER_KEY_FILE")` is non-null: `"REPLICADB_SECURITY_MASTER_KEY_FILE is deprecated; use REPLICADB_SECURITY_KEYRING_FILE instead."` — this check never throws, it only logs.
  - Computes `inlineConfigured = !current.version.isBlank() || !current.key.isBlank()` and `fileExplicit = !SecretProtectionProperties.DEFAULT_KEYRING_FILE.equals(keyring.file)`.
  - If both are true, throws `IllegalStateException("Configure either replicadb.security.keyring.file or inline keyring values, not both")`.
  - If `inlineConfigured`, returns a new `EnvBackedKeyEncryptionKeyProvider` built from the four slot fields.
  - Otherwise (the zero-configuration case included, since an untouched `keyring.file` equals `DEFAULT_KEYRING_FILE` and `inlineConfigured` is false) returns a new `FileBackedKeyEncryptionKeyProvider(Path.of(keyring.file), objectMapper)` — this is exactly today's default behavior, unchanged.
  Tests (`KeyringSourceResolverTest`, using `org.springframework.mock.env.MockEnvironment` to control `Environment.getProperty`):
  - Default properties (no inline, default file) → returns a `FileBackedKeyEncryptionKeyProvider` instance (assert via `instanceof`); requires a valid temp keyring file on disk for this case since the constructor reads it.
  - Inline `current` set, file left at default → returns an `EnvBackedKeyEncryptionKeyProvider` instance.
  - Inline `current` set AND file explicitly set to a non-default path → throws `IllegalStateException` with a message mentioning both `keyring.file` and "inline".
  - `MockEnvironment` with `REPLICADB_SECURITY_MASTER_KEY_FILE` property present does not throw (just resolves normally) — verifies the deprecation path is a warning, not a failure.
  Dependencies: Task 2.1, Task 1.2

- [x] **2.3 Wire `SecretProtectionConfiguration` through the resolver and update its test**
  Files:
  - `replicadb-server/src/main/java/org/replicadb/server/security/config/SecretProtectionConfiguration.java`
  - `replicadb-server/src/test/java/org/replicadb/server/security/secret/SecretProtectionConfigurationTest.java`
  Changes:
  - Replace the direct `new FileBackedKeyEncryptionKeyProvider(...)` construction in the `keyEncryptionKeyProvider` bean method with `KeyringSourceResolver.resolve(properties, environment, objectMapper)`, adding an `org.springframework.core.env.Environment environment` parameter to the bean method (Spring injects it automatically).
  - In `SecretProtectionConfigurationTest`, replace the two existing `"replicadb.security.master-key-file=" + ...` context-runner property overrides with the dotted canonical property `"replicadb.security.keyring.file=" + ...`; `ApplicationContextRunner.withPropertyValues` supplies system properties rather than OS environment variables, so dotted properties are the correct binding-level test. The real `REPLICADB_SECURITY_KEYRING_FILE` environment name is exercised by the `application.yml` placeholder and `KeyringSourceResolverTest`'s environment-backed path.
  Tests (`SecretProtectionConfigurationTest`):
  - Existing "creates protection beans with a valid keyring" test updated to use `REPLICADB_SECURITY_KEYRING_FILE` and still passes.
  - Existing "missing keyring file fails startup" test updated to use `REPLICADB_SECURITY_KEYRING_FILE` and still fails startup the same way.
  - New test: context starts successfully using only `REPLICADB_SECURITY_MASTER_KEY_FILE` (the deprecated name, no new variable set at all) — regression test proving the resolver fallback works in the context runner; the old uppercase key is read directly from `Environment` because the runner does not emulate OS-environment relaxed binding.
  - New test: context fails to start when both `replicadb.security.keyring.file` and `replicadb.security.keyring.current.version`/`replicadb.security.keyring.current.key` are set, with the failure message mentioning the conflict.
  - New test: context starts successfully using only `replicadb.security.keyring.current.version` + `replicadb.security.keyring.current.key` (no file property at all) and the resulting `KeyEncryptionKeyProvider` bean is an `EnvBackedKeyEncryptionKeyProvider`.
  Dependencies: Task 2.2

- [x] **2.4 Update local-mode bootstrap to accept the renamed property/variable with fallback**
  Files:
  - `replicadb-server/src/main/java/org/replicadb/server/local/LocalMasterKeyBootstrap.java`
  - `replicadb-server/src/main/java/org/replicadb/server/local/EmbeddedPostgresLaunchOptions.java`
  - `replicadb-server/src/test/java/org/replicadb/server/local/LocalMasterKeyBootstrapTest.java`
  - `replicadb-server/src/test/java/org/replicadb/server/job/dispatch/DistributedWorkerLifecycleIT.java`
  - `replicadb-server/src/test/java/org/replicadb/server/job/execution/WorkerExecutionIT.java`
  Changes:
  - `LocalMasterKeyBootstrap.configuredKeyring(...)`: check, in order, `KEYRING_FILE_PROPERTY` system property → `REPLICADB_SECURITY_KEYRING_FILE` env var → `MASTER_KEY_FILE_PROPERTY` system property (deprecated) → `REPLICADB_SECURITY_MASTER_KEY_FILE` env var (deprecated). Update the blank-value error message to reference `KEYRING_FILE_PROPERTY`.
  - `EmbeddedPostgresLaunchOptions.getSpringDefaults(...)`: replace the two `setProperty("REPLICADB_SECURITY_MASTER_KEY_FILE", ...)` / `setProperty("replicadb.security.master-key-file", ...)` lines with `setProperty("REPLICADB_SECURITY_KEYRING_FILE", ...)` / `setProperty("replicadb.security.keyring.file", ...)`.
  - `DistributedWorkerLifecycleIT` and `WorkerExecutionIT`: change the spawned-process argument `"--replicadb.security.master-key-file=" + keyringPath` to `"--replicadb.security.keyring.file=" + keyringPath`.
  Tests:
  - `LocalMasterKeyBootstrapTest`: existing test using `MASTER_KEY_FILE_PROPERTY` system property still passes unchanged (regression: deprecated system property still honored).
  - `LocalMasterKeyBootstrapTest`: new test using `KEYRING_FILE_PROPERTY` system property, asserting it takes precedence when both the new and the deprecated system property are set to different paths.
  - `DistributedWorkerLifecycleIT` and `WorkerExecutionIT`: existing test bodies unchanged aside from the argument string; passing is the verification that the renamed property is actually honored by a real spawned server process.
  Dependencies: Task 1.2 (must be complete first — `KEYRING_FILE_PROPERTY` is defined there)

### 3. Persistence — status and re-encryption candidate queries

- [x] **3.1 Add `countByKeyVersion()` and `findIdPendingReencryption(...)` to the managed datasource store**
  Files:
  - `replicadb-server/src/main/java/org/replicadb/server/job/port/ManagedDataSourceStore.java`
  - `replicadb-server/src/main/java/org/replicadb/server/job/persistence/ManagedDataSourceRepository.java`
  - `replicadb-server/src/test/java/org/replicadb/server/job/persistence/ManagedDataSourceRepositoryTest.java` (new)
  Changes:
  - `ManagedDataSourceStore`: add `Map<String, Long> countByKeyVersion();` and `Optional<UUID> findIdPendingReencryption(String currentVersion, Set<String> knownVersions);`.
  - `ManagedDataSourceRepository.countByKeyVersion()`: `SELECT key_version, COUNT(*) FROM managed_datasource GROUP BY key_version`, collected into a `Map<String, Long>` via `jdbcTemplate.query(...)` with a row callback/mapper.
  - `ManagedDataSourceRepository.findIdPendingReencryption(...)`: `SELECT id FROM managed_datasource WHERE key_version <> :currentVersion AND key_version = ANY(:knownVersions) ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`, passing `knownVersions.toArray(String[]::new)` with `Types.ARRAY` (same pattern already used for `restrictToIds`/`ANY(:restrictToIds)` in `findPage`), returning `Optional<UUID>` via `queryOne`.
  Tests (`ManagedDataSourceRepositoryTest`, new file, `@SpringBootTest` + `@Import(PostgresTestcontainersConfig.class)` following the same pattern as `DatasourceControllerTest`):
  - `countByKeyVersion()` returns correct counts after inserting three datasources with key versions `"test"`, `"test"`, `"other"` (using the repository's own `insert` to seed rows, forging `key_version` via direct SQL update afterward since `insert` always uses the configured test provider's version).
  - `findIdPendingReencryption("test", Set.of("test", "other"))` returns the id of the row whose `key_version = "other"` when one such row exists, and `Optional.empty()` when none exist.
  - `findIdPendingReencryption` excludes a row whose `key_version` is not in `knownVersions` (simulating an orphaned/unknown version) — asserts it is never returned even though it differs from `currentVersion`.
  - `SKIP LOCKED` concurrency test: inject `PlatformTransactionManager` and build two `TransactionTemplate` instances. Seed two pending rows. On a background thread (via `ExecutorService`), start transaction A with `transactionTemplate.execute(status -> { UUID id = repository.findIdPendingReencryption(...).orElseThrow(); latch.countDown(); awaitReleaseSignal(); return id; })` and deliberately keep it uncommitted until a `CountDownLatch`/signal releases it. On the main thread, wait for the first latch, then run transaction B calling `findIdPendingReencryption` and assert it returns the *other* row's id (not the one locked by A), then assert a third call (with no more pending rows) returns `Optional.empty()`. Release the background thread and let both transactions commit before asserting final row states.
  Dependencies: None (independent of Section 2; can be built in parallel)

- [x] **3.2 Add an index on `managed_datasource.key_version`**
  Files:
  - `replicadb-server/src/main/resources/db/migration/V22__index_managed_datasource_key_version.sql` (new)
  Changes:
  - New Flyway migration (next available version after `V21__cascade_job_dependent_state_on_definition_delete.sql`): `CREATE INDEX idx_managed_datasource_key_version ON managed_datasource (key_version);` — supports both the `GROUP BY key_version` in `countByKeyVersion()` and the `WHERE key_version <> ... AND key_version = ANY(...)` filter in `findIdPendingReencryption()`, task 3.1's two new query shapes.
  Tests:
  - Flyway migration tests already run as part of the standard Spring context startup in every `@SpringBootTest` (e.g. `DatasourceControllerTest`, the new `ManagedDataSourceRepositoryTest`); a successful application context load after this migration is added is sufficient verification that the migration applies cleanly against the existing schema. No dedicated test class needed for a single `CREATE INDEX` statement.
  Dependencies: None

### 4. Domain service — batched, resumable re-encryption

- [x] **4.1 Create `KeyringRowReencryptor` (transactional single-row re-encryption)**
  Files:
  - `replicadb-server/src/main/java/org/replicadb/server/security/secret/KeyringRowReencryptor.java` (new)
  - `replicadb-server/src/test/java/org/replicadb/server/security/secret/KeyringRowReencryptorTest.java` (new)
  Changes:
  - `@Component` class with constructor-injected `ManagedDataSourceStore` and `SecretProtectionService`.
  - `@Transactional public Optional<UUID> reencryptOne(String currentVersion, Set<String> knownVersions)`: calls `repository.findIdPendingReencryption(...)`; if empty, returns `Optional.empty()`. Otherwise loads the full row via `repository.findByIdForUpdate(id)` (re-locking is a no-op cost since the row is already locked in the same transaction), deserializes `existing.encryptedSecurity()` via `protectionService.deserialize(...)`, calls `protectionService.reencrypt(id, bundle)`, serializes the result, and builds a replacement `ManagedDataSource` record copying every field from `existing` except `encryptedSecurity`, `securityFormatVersion`, `encryptionAlgorithm`, and `keyVersion` (taken from the rotated bundle), then calls `repository.update(replacement)`. Returns `Optional.of(id)`.
  - Explicit one-line comment on the class noting it must remain a separate bean from its caller so `@Transactional` is not bypassed by self-invocation.

  > ⚠️ Critic note: a code comment alone does not prevent a future refactor from re-introducing the Spring self-invocation bug (merging this class into `KeyringAdministrationService` would silently drop transactionality with no compile error). This plan does not add architecture-enforcement tooling (e.g. ArchUnit) since none exists elsewhere in the codebase; the mitigation is the comment plus this task's explicit test coverage of transactional behavior, and any reviewer of a future refactor should re-run `KeyringRowReencryptorTest`.

  Tests (`KeyringRowReencryptorTest`, `@SpringBootTest` + Testcontainers, using a `KeyEncryptionKeyProvider` test double with two known versions so a real rotation can be exercised):
  - A row with an old key version is re-encrypted: after calling `reencryptOne`, the row's `key_version` matches the provider's current version and decrypting it with `SecretProtectionService` yields the original plaintext values.
  - Calling `reencryptOne` when no row needs re-encryption returns `Optional.empty()` and touches no data (verified by an unchanged `updated_at`).
  - Calling `reencryptOne` when a matching row exists but its `key_version` is not in `knownVersions` (simulating an orphaned version) returns `Optional.empty()` and leaves that row untouched.
  Dependencies: Task 3.1, Task 1.1 (needs `knownVersions()` for the "orphaned version" scenario)

- [x] **4.2 Create `KeyringAdministrationService` (status + batch loop)**
  Files:
  - `replicadb-server/src/main/java/org/replicadb/server/security/secret/KeyringAdministrationService.java` (new)
  - `replicadb-server/src/test/java/org/replicadb/server/security/secret/KeyringAdministrationServiceTest.java` (new)
  Changes:
  - `@Service` class, constructor-injected `ManagedDataSourceStore`, `KeyEncryptionKeyProvider`, `KeyringRowReencryptor`.
  - Public record `KeyringEnvelopeCount(String keyVersion, long count, boolean known)`; public record `KeyringStatus(Set<String> knownVersions, String currentVersion, List<KeyringEnvelopeCount> envelopes, long reencryptionRequired, long unknownVersionCount, boolean converged)`; public record `ReencryptResult(int reencrypted, long remaining)`.
  - `status()`: reads `repository.countByKeyVersion()` and `keyProvider.knownVersions()`/`current().version()`; builds the envelope list (sorted by `keyVersion`); computes `reencryptionRequired` as the sum of counts where the version is known but not current; `unknownVersionCount` as the sum of counts where the version is not known; `converged` as true only when every envelope's version equals the current version.
  - `reencrypt(int batchSize)`: loops up to `batchSize` times calling `rowReencryptor.reencryptOne(current, known)`, stopping early on the first `Optional.empty()`; after the loop, recomputes `remaining` from a fresh `countByKeyVersion()` call (sum of every non-current version, including unknown/orphaned versions that this process cannot re-encrypt) and returns `ReencryptResult(processed, remaining)`. This prevents the HTTP response from claiming convergence while `status()` still reports an orphaned envelope.
  Tests (`KeyringAdministrationServiceTest`, `@SpringBootTest` + Testcontainers):
  - `status()` with a mix of current/old/unknown-version rows reports correct `reencryptionRequired` and `unknownVersionCount`, and `converged == false`.
  - `status()` with only current-version rows reports `converged == true` and `reencryptionRequired == 0`.
  - `reencrypt(2)` with 5 pending rows processes exactly 2 and reports `remaining == 3`.
  - `reencrypt(10)` with 3 pending rows processes exactly 3, stops early (does not call the reencryptor a 4th time — verifiable via a `Mockito.spy`/call-count assertion or by asserting total elapsed calls equal 3 through a wrapping test double), and reports `remaining == 0`.
  - Calling `reencrypt(...)` twice in a row when already converged is a no-op both times (idempotency), returning `reencrypted == 0, remaining == 0` on the second call.
  Dependencies: Task 4.1

### 5. API — audit vocabulary, DTOs, controller

- [x] **5.1 Extend audit vocabulary for keyring operations**
  Files:
  - `replicadb-server/src/main/java/org/replicadb/server/audit/domain/AuditAction.java`
  - `replicadb-server/src/main/java/org/replicadb/server/audit/domain/AuditResourceType.java`
  Changes:
  - Add `KEYRING_REENCRYPTED` to `AuditAction`.
  - Add `KEYRING` to `AuditResourceType`.
  Tests:
  - No dedicated test; covered by task 5.3's controller test asserting an audit row is recorded with these exact values.
  Dependencies: None

- [x] **5.2 Create keyring API response DTOs**
  Files:
  - `replicadb-server/src/main/java/org/replicadb/server/security/api/KeyringEnvelopeCountResponse.java` (new)
  - `replicadb-server/src/main/java/org/replicadb/server/security/api/KeyringStatusResponse.java` (new)
  - `replicadb-server/src/main/java/org/replicadb/server/security/api/KeyringReencryptResponse.java` (new)
  Changes:
  - `KeyringEnvelopeCountResponse(String keyVersion, long count, boolean known)` — plain record, mirrors `KeyringAdministrationService.KeyringEnvelopeCount`.
  - `KeyringStatusResponse(Set<String> knownVersions, String currentVersion, List<KeyringEnvelopeCountResponse> envelopes, long reencryptionRequired, long unknownVersionCount, boolean converged)`.
  - `KeyringReencryptResponse(int reencrypted, long remaining, boolean converged)`.
  Tests:
  - No dedicated unit test (plain data-carrying records with no logic); covered by task 5.3's controller test via JSON path assertions on the actual HTTP response body.
  Dependencies: Task 4.2

- [x] **5.3 Create `KeyringController` with `GET /status` and `POST /reencrypt`**
  Files:
  - `replicadb-server/src/main/java/org/replicadb/server/security/api/KeyringController.java` (new)
  - `replicadb-server/src/main/java/org/replicadb/server/security/config/OpenApiConfiguration.java`
  - `replicadb-server/src/test/java/org/replicadb/server/OpenApiSpecificationIT.java`
  - `replicadb-server/src/test/java/org/replicadb/server/security/api/KeyringControllerTest.java` (new)
  Changes:
  - `@RestController @Profile("api") @RequestMapping("/api/v1/keyring") @PreAuthorize("hasRole('ADMIN')") @Tag(name = "Keyring", ...) @SecurityRequirement(name = "sessionCookie")`, constructor-injected `KeyringAdministrationService`, `AuditService`, `AuditActorResolver`, following the exact `@Operation`/`@ApiResponses` annotation style used in `AuditEventController`.
  - `GET /status` (`operationId = "getKeyringStatus"`): calls `service.status()`, maps to `KeyringStatusResponse`. Not audited (read-only, matching the convention that list/get endpoints in `DatasourceController` are not audited).
  - `POST /reencrypt` (`operationId = "reencryptKeyring"`): accepts optional `@RequestParam Integer batchSize`, **silently clamps** (never throws or returns 4xx for an out-of-range value) to `[1, 1000]` with a default of `200` via a private `clampBatchSize` helper (`null` → 200; values below 1 → 1; values above 1000 → 1000); calls `service.reencrypt(size)`; records an audit event via `auditService.record(auditActorResolver.resolve(authentication), AuditAction.KEYRING_REENCRYPTED, AuditResourceType.KEYRING, "keyring", AuditOutcome.SUCCESS, Map.of("reencrypted", String.valueOf(result.reencrypted()), "remaining", String.valueOf(result.remaining())))`; returns `KeyringReencryptResponse(result.reencrypted(), result.remaining(), result.remaining() == 0)`.
  - `OpenApiConfiguration.java`: add `tag("Keyring", "Keyring status and datasource re-encryption administration.")` to the existing `tags(List.of(...))` call, alongside the existing `Audit`/`Users` entries.
  Tests (`KeyringControllerTest`, following `DatasourceControllerTest`'s `@SpringBootTest` + `@AutoConfigureMockMvc` + `@Import(PostgresTestcontainersConfig.class)` + `@WithMockUser(roles = "ADMIN")` pattern):
  - `GET /api/v1/keyring/status` with no datasources returns `200` with an empty `envelopes` list and `converged: true`.
  - `GET /api/v1/keyring/status` with datasources at the test provider's single known version returns correct counts and `converged: true`.
  - `POST /api/v1/keyring/reencrypt` without CSRF token returns `403` (mirrors existing CSRF assertions on other mutating endpoints).
  - `POST /api/v1/keyring/reencrypt` with CSRF, using a `@WithMockUser(roles = "USER")` (non-admin) override, returns `403`.
  - `POST /api/v1/keyring/reencrypt?batchSize=1` with CSRF as ADMIN, against a fixture with datasources at a stale key version, returns `200` with `reencrypted: 1` and asserts a corresponding row now exists in `audit_event` with `action = 'KEYRING_REENCRYPTED'`. Use a dedicated nested `@TestConfiguration` inside `KeyringControllerTest` that defines a `@Bean @Primary KeyEncryptionKeyProvider` two-version test double (mirroring `SecretProtectionServiceTest.TestKeyProvider`'s `rotate()` pattern) to override the single-version bean from `PostgresTestcontainersConfig`, rather than seeding a stale `key_version` via direct SQL — this is the same mechanism `KeyringRowReencryptorTest` (task 4.1) and `KeyringAdministrationServiceTest` (task 4.2) use, for consistency across all three test classes.
  - `POST /api/v1/keyring/reencrypt?batchSize=5000` clamps to 1000 internally and never returns a 4xx response for the out-of-range value (assert `200`, and that the response's `reencrypted` field never exceeds the number of pending rows in the fixture).
  Dependencies: Task 5.1, Task 5.2

### 6. Documentation and generated artifacts

- [x] **6.1 Update environment variable reference, example file, and compose deployment example**
  Files:
  - `docs/src/content/docs/reference/environment-variables.md`
  - `docs/src/content/docs/operations/distributed-deployment.mdx`
  - `README.md`
  - `DEPLOYMENT.md`
  - `replicadb-server/conf/replicadb-server.env.example`
  - `docker-compose.server.yml`
  - `docs/tests/operations-contract.test.mjs`
  Changes:
  - In `environment-variables.md`'s table, replace the single `REPLICADB_SECURITY_MASTER_KEY_FILE` row with rows for `REPLICADB_SECURITY_KEYRING_FILE` (marking it as the canonical name), `REPLICADB_SECURITY_KEYRING_CURRENT_VERSION`, `REPLICADB_SECURITY_KEYRING_CURRENT_KEY`, `REPLICADB_SECURITY_KEYRING_SECONDARY_VERSION`, `REPLICADB_SECURITY_KEYRING_SECONDARY_KEY`, and keep a final row for `REPLICADB_SECURITY_MASTER_KEY_FILE` explicitly marked "Deprecated alias for `REPLICADB_SECURITY_KEYRING_FILE`."
  - Correct the existing `REPLICADB_WORKER_IDENTITY` row, which currently reads "Required for workers." — update it to state that it is optional and a random `worker-<uuid>` identity is generated at startup when unset, referencing that `docker-compose.server.yml` sets it explicitly only as an operational choice, not a requirement.
  - In `replicadb-server.env.example`, add commented-out examples for the new inline variables alongside the existing `REPLICADB_SECURITY_MASTER_KEY_FILE=...` line, with a comment noting the new canonical name and that inline/file are mutually exclusive.
  - In `docker-compose.server.yml`, change the `secrets: replicadb-master-key: file: ${REPLICADB_SECURITY_MASTER_KEY_FILE:?REPLICADB_SECURITY_MASTER_KEY_FILE must point to a keyring file}` line to use `REPLICADB_SECURITY_KEYRING_FILE` instead, so the shipped deployment example uses the canonical name rather than the deprecated alias.
  - In `docs/tests/operations-contract.test.mjs`, add `REPLICADB_SECURITY_KEYRING_FILE`, `REPLICADB_SECURITY_KEYRING_CURRENT_VERSION`, and `REPLICADB_SECURITY_KEYRING_SECONDARY_VERSION` to the existing `for (const required of [...])` substring list, so the new canonical variable names are explicitly contract-tested, not just incidentally present.
  Tests:
  - Run `docs/tests/operations-contract.test.mjs` — the existing assertions (including `REPLICADB_SECURITY_MASTER_KEY_FILE` and `REPLICADB_WORKER_IDENTITY`) must still pass since both strings remain present (as the deprecated-alias row and the corrected description respectively), and the three newly-added required substrings must also match.
  Dependencies: None (documentation-only, can proceed independently of code tasks, but should be written after task 2 lands so the described behavior is accurate)

- [x] **6.2 Rewrite `key-management.md` to describe the real rotation procedure and the now-implemented re-encryption operation**
  Files:
  - `docs/src/content/docs/operations/key-management.md`
  - `docs/src/content/docs/operations/backups-and-restore.md`
  Changes:
  - Replace the "Rotation" section's step 3 ("Run the datasource re-encryption operation" with no further detail) with concrete instructions: `curl -X POST .../api/v1/keyring/status` to check pending counts, then repeated `curl -X POST .../api/v1/keyring/reencrypt?batchSize=200` calls until the response reports `converged: true`.
  - Add an explicit two-generation rotation walkthrough (add secondary version and roll out → activate it as current and roll out → re-encrypt → remove the retired version and roll out), stating plainly that a rollout that has *fully completed* is what makes each step safe, and that skipping a generation (activating a version before every instance has it) is the only way to create an unsafe window. State explicitly how an operator confirms a rollout is fully complete before moving to the next generation: the platform's own rollout-completion signal (e.g. `kubectl rollout status deployment/<name>`, the equivalent ECS/Cloud Run/Container Apps "deployment complete" state) — not a ReplicaDB-specific check, since none exists.
  - Add a short "Snapshot at startup" callout: the keyring is read once when the process starts; changing the underlying file or environment variables requires a restart/redeploy on every platform (no exceptions), and this is intentional, not a bug.
  - Add a short "Known limitation" callout stating plainly that ReplicaDB cannot detect a partially-rolled-out fleet (some instances still on the old generation) from inside the application; the only mitigation is following the documented rollout order and using the platform's own rollout-completion signal before each subsequent step, and recovery from a mistake is always to redeploy the previous, all-versions-known keyring — this is a deliberate scope boundary from the design, not an oversight, and is covered further by `GET /api/v1/keyring/status`'s `unknownVersionCount` field for detecting an already-occurred mismatch after the fact.
  - Add the new environment variable names (`REPLICADB_SECURITY_KEYRING_FILE`, `REPLICADB_SECURITY_KEYRING_CURRENT_*`, `REPLICADB_SECURITY_KEYRING_SECONDARY_*`) with a short note that the inline form tops out at two simultaneous versions by design (matching what a two-generation rotation ever needs) and that more complex rotations should use the file form.
  - Add one sentence clarifying that "shared keyring" means the same key material across processes, not a shared filesystem — each instance may receive its own independent projection of the same values.
  Tests:
  - Run `docs/tests/operations-contract.test.mjs` (asserts `'keyring'`, `'AES'`, and other required substrings remain present) and confirm it still passes after the rewrite.
  Dependencies: Task 5.3 (documents the actual, now-real endpoints)

- [x] **6.3 Regenerate and verify the OpenAPI contract**
  Files:
  - `docs/openapi/replicadb-server.json` (regenerated, not hand-edited)
  - `docs/tests/openapi-contract.test.mjs` (optional strengthening)
  - `docs/src/content/docs/api/index.md`
  Changes:
  - Run `docs/scripts/update-openapi.sh` from the repository root to regenerate the canonical `docs/openapi/replicadb-server.json` from the running `OpenApiSpecificationIT`, now including the two new `/api/v1/keyring/*` paths and the `Keyring` tag.
  - Optionally add `'Keyring'` to the tag list already asserted in `docs/tests/openapi-contract.test.mjs`'s `for (const tag of [...])` block (task 5.3 already makes the underlying assertion pass without this, but it strengthens the regression check for the new tag specifically).
  Tests:
  - Run the full `docs` test suite (`npm test` in `docs/`, or the specific `node --test docs/tests/openapi-contract.test.mjs`) and confirm: the committed JSON is canonical (byte-for-byte matches the script's sorted-key output), the new paths' `operationId`s (`getKeyringStatus`, `reencryptKeyring`) have corresponding generated operation pages, and no existing assertion regresses.
  Dependencies: Task 5.3

## Technical Reference

<details>
<summary>Types & Data Structures</summary>

```java
// KeyEncryptionKeyProvider.java (interface addition)
Set<String> knownVersions();

// SecretProtectionProperties.java (new shape)
public class Keyring {
    private String file = "/run/secrets/replicadb-master-key";
    private Slot current = new Slot();
    private Slot secondary = new Slot();
}
public static class Slot {
    private String version = "";
    private String key = "";
}

// KeyringAdministrationService.java (new records)
public record KeyringEnvelopeCount(String keyVersion, long count, boolean known) {}
public record KeyringStatus(Set<String> knownVersions, String currentVersion,
        List<KeyringEnvelopeCount> envelopes, long reencryptionRequired,
        long unknownVersionCount, boolean converged) {}
public record ReencryptResult(int reencrypted, long remaining) {}
```

```sql
-- ManagedDataSourceRepository.java (new queries)
SELECT key_version, COUNT(*) FROM managed_datasource GROUP BY key_version;

SELECT id FROM managed_datasource
WHERE key_version <> :currentVersion AND key_version = ANY(:knownVersions)
ORDER BY id
LIMIT 1
FOR UPDATE SKIP LOCKED;
```

</details>

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy
- Tasks completed as planned: 16/16 (100%).
- Tasks that required plan adjustment: 6/16 (37.5%).
- Test loop iterations: 29 total (10 first-pass task validations, 10 repair/retry validations, 9 final generation and suite checks).

### Gaps Encountered

#### Gap 1: Existing property wiring had to move with the nested model (Plan-to-Implementation)
- **Task**: 1.2
- **Plan assumed**: The nested `SecretProtectionProperties` change could wait until task 2.3 updated the provider wiring.
- **Reality**: The existing configuration directly called the removed `getMasterKeyFile()` accessor, so the module stopped compiling immediately after task 1.2.
- **Resolution**: Updated the existing wiring to read `properties.getKeyring().getFile()` before introducing the source resolver.
- **Learning**: When replacing a bound configuration shape, compile all current consumers in the same task even if a later task will replace their behavior.

#### Gap 2: `ApplicationContextRunner` does not emulate OS-environment relaxed binding (Plan-to-Implementation)
- **Task**: 2.3
- **Plan assumed**: `withPropertyValues("REPLICADB_SECURITY_KEYRING_FILE=...")` would exercise uppercase environment-variable binding.
- **Reality**: The runner supplies system properties; Spring's environment-variable underscore mapping is not equivalent in that harness.
- **Resolution**: Used dotted properties for binding-level context tests, while keeping uppercase resolution covered by `application.yml` placeholders and `MockEnvironment` resolver tests.
- **Learning**: Test environment-variable placeholders separately from `@ConfigurationProperties` binding when the harness does not provide a real OS environment source.

#### Gap 3: PostgreSQL array typing required bypassing the repository `Map` helper (Plan-to-Implementation)
- **Task**: 3.1
- **Plan assumed**: The existing `queryOne(Map, ...)` helper could carry the typed `String[]` parameter needed by `ANY(:knownVersions)`.
- **Reality**: `MapSqlParameterSource` preserves `Types.ARRAY`, but the helper accepted only `Map<String, ?>`.
- **Resolution**: Executed the typed query directly and used a row mapper; the initial callback overload was also made explicit through a mapper-based count query.
- **Learning**: Keep typed PostgreSQL parameter sources intact through the final JDBC call; generic repository helpers can erase the metadata needed for arrays.

#### Gap 4: Unknown key versions must prevent a false convergence result (Intent-to-Plan)
- **Task**: 4.2
- **Plan assumed**: `remaining` could count only known, non-current versions while the API derived `converged` from `remaining == 0`.
- **Reality**: A database row using an unknown version would make the response claim convergence even though `status()` correctly reported an orphaned envelope.
- **Resolution**: `remaining` now counts every non-current version, including unknown versions; status still separates `reencryptionRequired` from `unknownVersionCount`.
- **Learning**: Progress counters used by operators must include blocked work when their zero value is used as a convergence signal.

#### Gap 5: Documentation contracts are closed-world and generated pages are part of the API change (Intent-to-Plan)
- **Task**: 5.3/6.3
- **Plan assumed**: Adding the controller tag and regenerating JSON was sufficient.
- **Reality**: `OpenApiSpecificationIT` asserted a fixed tag set and operation count, while the docs contract required a human guide link and built pages for every new operation.
- **Resolution**: Updated server and docs contract assertions, added the Keyring guide link, regenerated `docs/openapi/replicadb-server.json`, and rebuilt `docs/dist`.
- **Learning**: Treat server OpenAPI assertions, committed schema, human domain navigation, and generated operation pages as one atomic API change.

#### Gap 6: Deprecated configuration must remain usable through deployment adapters (Intent-to-Plan)
- **Task**: 6.1
- **Plan assumed**: Switching the Compose interpolation to the canonical variable was compatible because the application itself accepts the legacy alias.
- **Reality**: Compose resolves the secret path before the application starts, so an old deployment using only the legacy variable would fail before ReplicaDB could apply its fallback.
- **Resolution**: Compose now prefers the canonical variable and falls back to `REPLICADB_SECURITY_MASTER_KEY_FILE`; root deployment references use the canonical name while internal legacy producers remain compatible.
- **Learning**: Deprecation aliases must be implemented at every pre-application configuration boundary, not only inside application binding.

### Patterns Discovered
- Per-row `FOR UPDATE SKIP LOCKED` work can be safely exposed as a bounded, repeatable HTTP batch when the persisted key-version column is the convergence cursor.
- A dedicated Spring bean is required for transactional row work when the caller loops over batches; self-invocation would bypass the transaction proxy.

<details>
<summary>Dependencies</summary>

No new third-party dependencies. Uses existing Spring Boot (`@ConfigurationProperties`, `Environment`, `@Transactional`), existing Jackson `ObjectMapper`, existing `javax.crypto` usage, and the existing `NamedParameterJdbcTemplate` pattern already used throughout `ManagedDataSourceRepository`.

</details>

<details>
<summary>Testing Strategy</summary>

- Unit tests (no Spring context) for pure logic: `EnvBackedKeyEncryptionKeyProvider`, `KeyringSourceResolver` (using `MockEnvironment`).
- `@SpringBootTest` + Testcontainers PostgreSQL (existing `PostgresTestcontainersConfig` pattern) for anything touching `managed_datasource` rows: repository queries, `KeyringRowReencryptor`, `KeyringAdministrationService`, `KeyringController`.
- `KeyringControllerTest` and `KeyringAdministrationServiceTest` need at least one scenario with *two* known key versions to exercise real rotation; the shared `PostgresTestcontainersConfig.testKeyEncryptionKeyProvider()` bean only offers one version, so these tests must either seed a stale `key_version` directly via SQL (bypassing the provider) for "already-stale" scenarios, or provide a locally `@Import`ed two-version `KeyEncryptionKeyProvider` test double where an actual `reencrypt()` transition needs to be observed end-to-end.
- CSRF and role checks in `KeyringControllerTest` follow the exact `.with(csrf())` / `@WithMockUser(roles = ...)` conventions already used in `DatasourceControllerTest`.
- Documentation changes are verified by the existing Node-based contract tests under `docs/tests/`, run via the docs package's own test script — no new test framework introduced.

</details>
