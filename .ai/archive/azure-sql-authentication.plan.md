# Implementation Plan: First-class Azure SQL and Microsoft Entra Authentication

## Task Source
GitHub issue #206, "Support Active Directory Interactive for MSSQL databases in Azure", plus the expanded requirement to support interactive and non-interactive authentication from local machines, outside Azure, and Azure-hosted runtimes.

## Acceptance Criteria

- Support Microsoft Entra authentication for both SQL Server sources and SQL Server sinks.
- Support user-present `ActiveDirectoryInteractive` authentication with browser and MFA from a local JVM.
- Support non-interactive `ActiveDirectoryDefault` authentication for Azure CLI, environment credentials, workload identity, and Azure-hosted default credential chains.
- Support `ActiveDirectoryServicePrincipal` for portable unattended execution outside Azure.
- Support `ActiveDirectoryServicePrincipalCertificate` for portable unattended execution without a client secret.
- Support `ActiveDirectoryManagedIdentity` and the `ActiveDirectoryMSI` alias for Azure-hosted execution, including optional user-assigned identity selection.
- Preserve pass-through support for existing JDBC connection parameters and existing SQL username/password connections.
- Expose the supported authentication modes through explicit source and sink CLI/options-file configuration without putting secrets in command-line examples.
- Preserve options-file to command-line precedence and all existing CLI defaults.
- Include the required Microsoft identity libraries in test jars, release distributions, and both runtime image families.
- Prevent interactive authentication from launching concurrent browser flows through the default parallel task model.
- Redact authentication secrets and credential-bearing JDBC values from debug output and Sentry telemetry.
- Add deterministic unit coverage and opt-in integration coverage for local, external, and Azure-hosted environments without committing credentials or requiring Azure access in normal CI.
- Document database permissions, Azure SQL firewall/network prerequisites, environment variables, headless-runtime limitations, and failure diagnosis.

## Overview

ReplicaDB already selects `SQLServerManager` for `jdbc:sqlserver:` URLs and delegates authentication to the Microsoft JDBC driver. The current generic JDBC connection path forwards arbitrary `source.connect.parameter.*` and `sink.connect.parameter.*` values, but it does not provide a typed Azure authentication contract and the SQL Server driver declares the identity libraries as optional.

The selected approach adds a small first-class configuration layer at the existing `ToolOptions` boundary. `SQLServerManager` translates that configuration into the driver properties; it does not implement OAuth, browser handling, token acquisition, or a competing credential provider. This keeps Microsoft Entra behavior aligned with the vendor driver while making the supported deployment modes discoverable, validated, secure, and testable.

## Approach Selection

### Selected: First-class Azure configuration over the native driver

Add explicit source and sink authentication fields while retaining the existing generic JDBC parameter escape hatch. The driver remains responsible for MSAL4J, Azure Identity, browser redirects, token caching, certificate handling, managed identity, and token renewal.

Advantages for this repository:

- Reuses the existing `ToolOptions` and `OptionsFile` configuration boundary.
- Keeps vendor-specific authentication mapping in `SQLServerManager` rather than in orchestration.
- Supports all required deployment environments without introducing a cloud SDK abstraction into ReplicaDB.
- Makes invalid combinations fail before a replication task starts.
- Keeps existing `source.user`, `source.password`, and raw JDBC URLs backward compatible.

Trade-offs:

- Adds a new CLI and properties-file contract that must be maintained.
- The exact credential behavior remains dependent on the selected `mssql-jdbc` version and its optional Azure Identity/MSAL dependency chain.
- Browser authentication remains unavailable in headless containers and must be explicitly documented.

### Rejected: Driver-only pass-through

Only add `azure-identity` and documentation for `source.connect.parameter.*`. This is the smallest change, but users must know vendor-specific property names, invalid mode combinations are discovered late, and interactive parallelism remains easy to misconfigure.

### Rejected: ReplicaDB-owned credential provider

Add an internal Azure SDK/token callback abstraction and share tokens across task connections. This duplicates capabilities already present in `mssql-jdbc`, expands secret-handling responsibility, complicates classpath and lifecycle management, and risks coupling the orchestrator to Azure. A user-supplied `accessTokenCallbackClass` remains available through the advanced JDBC property path but is outside the first-class contract.

## Architecture and Design

### Configuration boundary

`ToolOptions` remains the single source of configuration. Add independent source and sink objects with the following non-secret fields:

- `auth.mode`
- `auth.principal.id`
- `auth.login.hint`
- `auth.client.certificate`
- `auth.client.key`

The corresponding CLI options are:

- `--source-auth-mode`, `--sink-auth-mode`
- `--source-auth-principal-id`, `--sink-auth-principal-id`
- `--source-auth-login-hint`, `--sink-auth-login-hint`
- `--source-auth-client-certificate`, `--sink-auth-client-certificate`
- `--source-auth-client-key`, `--sink-auth-client-key`

The options-file spellings are `source.auth.mode`, `sink.auth.mode`, and so on. Existing `source.user`, `source.password`, `sink.user`, and `sink.password` remain available. Secrets such as service-principal secrets, certificate passwords, and private-key passwords must come from environment-expanded values or advanced connection parameters, not new command-line examples.

### Supported mode mapping

| First-class mode | Driver property | Local use | Outside Azure | Azure-hosted use | Credential inputs |
| --- | --- | --- | --- | --- | --- |
| `ActiveDirectoryInteractive` | `authentication=ActiveDirectoryInteractive` | Browser and MFA | Manual operator runs | Only with an attached desktop/browser | Optional login hint; no password |
| `ActiveDirectoryDefault` | `authentication=ActiveDirectoryDefault` | Azure CLI or environment credential | CI/workload identity where configured | Default Azure Identity chain | Environment-managed credential settings |
| `ActiveDirectoryManagedIdentity` | `authentication=ActiveDirectoryManagedIdentity` | Not normally available | Not applicable | System or user-assigned managed identity | Optional `msiClientId` from principal ID |
| `ActiveDirectoryServicePrincipal` | `authentication=ActiveDirectoryServicePrincipal` | Manual or automation | Portable unattended execution | Possible but usually inferior to managed identity | Principal ID plus existing `source.password`/`sink.password` secret |
| `ActiveDirectoryServicePrincipalCertificate` | `authentication=ActiveDirectoryServicePrincipalCertificate` | Manual or automation | Preferred portable unattended execution | Possible | Principal ID, certificate path, optional key path |
| `ActiveDirectoryIntegrated` | `authentication=ActiveDirectoryIntegrated` | Domain/Kerberos setup | Kerberos or native auth setup | Only where the host is domain-integrated | Platform credential or Kerberos ticket |

`ActiveDirectoryMSI` is accepted as an input alias and canonicalized to `ActiveDirectoryManagedIdentity`. `ActiveDirectoryPassword` is not part of the first-class mode enum because Microsoft deprecates it and it is incompatible with MFA; legacy raw JDBC parameters remain backward compatible but are documented as deprecated.

### Property mapping and precedence

- If no first-class authentication mode is configured, preserve the current URL and connection-parameter behavior exactly.
- If a first-class mode is configured, map it into the JDBC `authentication` property through `Properties`, avoiding credentials in the URL.
- For service principal and certificate modes, `auth.principal.id` maps to the JDBC `user` property unless an explicit existing `source.user`/`sink.user` value is present and equal.
- For managed identity, `auth.principal.id` maps to `msiClientId`; an empty value means system-assigned identity.
- For interactive mode, `auth.login.hint` maps to `user`; an existing source/sink user may be used as a backward-compatible login hint.
- `auth.client.certificate` maps to `clientCertificate`; `auth.client.key` maps to `clientKey`.
- Existing `source.connect.parameter.*` and `sink.connect.parameter.*` values remain the advanced escape hatch. First-class values take precedence for their mapped keys, and conflicting duplicate values fail validation rather than silently changing identity.
- Command-line values continue to override options-file values through the existing setter behavior.
- Azure tenant and client-secret environment settings are supplied through Azure Identity environment variables such as `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET`; they are not copied into JDBC URLs or emitted in telemetry.

### Validation ownership

- `AzureAuthenticationOptions.validate()` owns mode-specific field rules and source/sink symmetry.
- `ToolOptions.validateAzureAuthentication()` calls those option validators and enforces the `jobs=1` rule for Interactive mode.
- `ManagerFactory.validateAzureAuthenticationConfiguration(options)` is called at the start of `ReplicaDB.executeReplication()`, immediately after the temporary-file reset and before `SentryInit(options)`. It uses the existing source/sink scheme extraction and `SupportedManagers.SQLSERVER` dispatch check to reject first-class Azure settings on non-SQL Server connections before managers or tasks are created.
- `ManagerFactory.accept()` repeats the scheme guard defensively for direct callers and tests, but connection opening remains the responsibility of the manager.

### Connection lifecycle

Refactor only the common connection-property assembly in `SqlManager`. Introduce `protected Properties buildConnectionProperties(DataSourceType dataSourceType)` for collecting credentials and advanced parameters, and `protected void customizeConnectionProperties(DataSourceType dataSourceType, Properties properties) throws SQLException` as a no-op hook. `makeSourceConnection()` and `makeSinkConnection()` call the builder, then the hook, then `DriverManager.getConnection(url, properties)` whenever first-class or advanced properties exist; ordinary connections retain the current username/password overload. `SQLServerManager` overrides only the hook and never owns token acquisition.

Do not add token acquisition, browser code, Azure SDK calls, connection pooling, or shared JDBC connections to `ReplicaDB`, `ReplicaTask`, or `SQLServerManager`. Each task continues to own its connections. Interactive mode is validated as `jobs=1` for first-class configuration; non-interactive modes may use parallel jobs and rely on the driver token cache.

### Security boundary

Create one redaction utility used by `ToolOptions.toString()`, Sentry scope contexts, Sentry tags, log messages, and authentication errors. Redact `password`, `accessToken`, `clientSecret`, `aadSecurePrincipalSecret`, `AADSecurePrincipalSecret`, `clientKeyPassword`, `privateKeyPassword`, `secretKey`, `sasToken`, and `sentry.dsn`, plus URL user-info and sensitive query parameters. Also mask identity/PII keys such as `user`, `username`, `userName`, `loginHint`, `principalId`, `clientId`, `tenantId`, and `msiClientId` in telemetry; retain only the authentication mode and non-sensitive host/database context. Install the same scrubber for Sentry contexts, tags, exception values, and chained causes so driver/MSAL exception messages cannot bypass map redaction.

## Implementation Tasks

### 1. Define the first-class Azure authentication model

- [x] **1.1 Add canonical Azure authentication modes and source/sink option objects**
  Files: `src/main/java/org/replicadb/cli/AzureAuthenticationMode.java`, `src/main/java/org/replicadb/cli/AzureAuthenticationOptions.java`, `src/main/java/org/replicadb/cli/ToolOptions.java`
  Changes: Add canonical mode values, case-insensitive parsing, the `ActiveDirectoryMSI` alias, mode-specific fields, and validation rules. Define the required fields explicitly: Interactive accepts no principal or password and may have only an optional login hint; Default accepts no required fields and may optionally select a managed identity; Managed Identity accepts an optional principal ID for a user-assigned identity; Service Principal requires a principal ID and the existing source/sink password as its secret; Service Principal Certificate requires a principal ID and certificate path, with optional key path and key-password advanced parameter; Integrated accepts no user/password and requires external Kerberos/native setup. Reject `ActiveDirectoryPassword` from the first-class enum by throwing `IllegalArgumentException` with a message directing users to Interactive, Default, Service Principal, or Managed Identity. Keep the new fields optional so existing jobs behave unchanged. Define source/sink independence in one place rather than duplicating validation in managers.
  Tests: Add `src/test/java/org/replicadb/cli/AzureAuthenticationOptionsTest.java` covering canonical values, case-insensitive input, the MSI alias, the exact required/forbidden fields for every mode, unsupported/deprecated values and error text, optional login hints, system-assigned Managed Identity with an empty principal ID, and independent source/sink objects. Assert that `toString()` never contains secret values.
  Dependencies: None

### 2. Extend CLI and options-file parsing without changing precedence

- [x] **2.1 Parse explicit source and sink Azure authentication settings**
  Files: `src/main/java/org/replicadb/cli/ToolOptions.java`, `src/main/java/org/replicadb/cli/EnvironmentVariableEvaluator.java`, `conf/_replicadb.conf`
  Changes: Add the CLI options and `source.auth.*`/`sink.auth.*` properties defined in the architecture section. `ToolOptions.loadOptionsFile()` reads these scalar keys directly from `OptionsFile.getProperties()`; no second prefix extractor is added because `OptionsFile` already loads and environment-expands the complete Java `Properties` object, while its existing `source.connect.parameter.*` extraction remains unchanged. Load options-file values before command-line overrides, preserve empty/default behavior, and expand `${ENV_NAME}` values for certificate paths, principal IDs, and advanced secret properties. Do not add secret-bearing values to help examples.
  Tests: Add `src/test/java/org/replicadb/cli/ToolOptionsAzureAuthenticationTest.java` cases for options-file loading, command-line override, environment expansion, missing source/sink connect strings, and validation of source-only versus sink-only authentication. Verify help output contains the non-secret option names.
  Dependencies: Task 1.1

### 3. Add SQL Server driver-property customization

- [x] **3.1 Map first-class options into JDBC `Properties` at the SQL Server boundary**
  Files: `src/main/java/org/replicadb/manager/SqlManager.java`, `src/main/java/org/replicadb/manager/SQLServerManager.java`, `src/main/java/org/replicadb/manager/ManagerFactory.java`
  Changes: In `SqlManager`, add `buildConnectionProperties(DataSourceType)` and the no-op hook `customizeConnectionProperties(DataSourceType, Properties) throws SQLException`; route both `makeSourceConnection()` and `makeSinkConnection()` through them, keep `driver` filtering and `setAutoCommit(false)`, and preserve the existing username/password overload when no properties exist. In `SQLServerManager.customizeConnectionProperties(...)`, map the validated source or sink option object to `authentication`, `user`, `msiClientId`, `clientCertificate`, and `clientKey`; remove `password` for Interactive, Default, Managed Identity, and Integrated; retain it only as the service-principal secret or certificate password where the driver requires it. Reject conflicting first-class versus advanced values for the same key. The scheme validation itself is owned by `ManagerFactory.validateAzureAuthenticationConfiguration()` in Task 4.1. Preserve existing raw `authentication` URL and connection-parameter support when no first-class mode is present.
  Tests: Add `src/test/java/org/replicadb/manager/SQLServerManagerAuthenticationTest.java` using a capturing in-process JDBC test driver or the existing Mockito seam to assert source and sink properties for every mode, MSI alias normalization, principal/login-hint mapping, certificate/key mapping, omission of passwords for interactive/managed identity/default/integrated modes, service-principal secret and certificate-password mapping, conflict rejection, and preservation of ordinary SQL Server username/password connections.
  Dependencies: Tasks 1.1 and 2.1

### 4. Validate authentication modes before work and protect task cleanup

- [x] **4.1 Enforce lifecycle and parallelism rules for interactive authentication**
  Files: `src/main/java/org/replicadb/ReplicaDB.java`, `src/main/java/org/replicadb/ReplicaTask.java`, `src/main/java/org/replicadb/manager/ManagerFactory.java`, `src/main/java/org/replicadb/cli/ToolOptions.java`
  Changes: Add `ManagerFactory.validateAzureAuthenticationConfiguration(options)` and call it as the first validation step in `ReplicaDB.executeReplication()`, immediately after the existing temporary-file reset and before `SentryInit(options)` or transaction creation. It calls `ToolOptions.validateAzureAuthentication()`, checks both source and sink schemes with the existing `SupportedManagers.SQLSERVER` dispatch logic, and rejects first-class Azure settings on any non-SQL Server URL. It also detects raw `authentication=ActiveDirectoryInteractive` in SQL Server URLs or advanced JDBC parameters so the original connection-string usage cannot launch concurrent browser flows. Reject Interactive mode with `jobs > 1` with a clear remediation to use `jobs=1`; allow parallel jobs for non-interactive modes. Refactor `ReplicaTask.call()` to create managers inside a `try/finally`, close sink and source independently even when source authentication succeeds and sink authentication fails, and preserve both close exceptions without masking the original failure. Add a package-private injectable `ManagerFactory` constructor while retaining the current public constructor so the test can supply fake managers; do not share connections or change executor ownership. Preserve task names, exception causes, exit codes, and existing executor cleanup.
  Tests: Add `src/test/java/org/replicadb/ReplicaTaskAuthenticationFailureTest.java` with injected fake managers that fail source or sink authentication and assert both managers are closed, including a close failure that must not replace the authentication exception. Add validation cases for first-class and raw-URL Interactive `jobs=1`, Interactive `jobs>1`, non-interactive parallel jobs, first-class auth on non-SQLServer source or sink URLs, invalid source/sink mode fields, and failure before Sentry initialization.
  Dependencies: Tasks 1.1, 2.1, and 3.1

### 5. Package Microsoft identity dependencies for every supported runtime

- [x] **5.1 Add the matching Azure Identity dependency and verify release classpaths**
  Files: `pom.xml`, `Dockerfile`, `Containerfile`, `bin/configure-replicadb`, `bin/replicadb`, `bin/replicadb.cmd`
  Changes: In `pom.xml`, add the property `<version.azure-identity>1.15.3</version>` next to the existing dependency version properties and add `com.azure:azure-identity` with that property and Maven's default compile scope (therefore included in runtime classpaths); do not use `provided`. The existing `test` profile jar-with-dependencies and `release`/`release-no-oracle` `maven-dependency-plugin` executions already include compile/runtime dependencies, so verify them rather than adding duplicate profile entries. Remove `-Djava.awt.headless=true` from the default local launcher JVM options, or make it conditional on an explicit headless deployment setting, because local Interactive mode must be able to launch the system browser. Preserve headless operation for container/non-interactive deployments through an explicit image/runtime setting and document that Interactive is unsupported there. Preserve the Java 17 module-opening flag and launcher classpath layout.
  Tests: First run `mvn -B dependency:tree -Dincludes=com.microsoft.sqlserver:mssql-jdbc,com.azure:azure-identity,com.microsoft.azure:msal4j,com.microsoft.azure:msal4j-persistence-extension` and record the resolved baseline before implementation changes. Then run test compilation; build the test jar-with-dependencies; build both release profiles; inspect `target/lib`; invoke the packaged launcher with a non-networking classpath smoke check that loads the SQL Server driver and Azure identity classes; verify local Interactive startup does not set the JVM headless flag; build both runtime images and verify the same classpath and explicit headless setting on their supported architectures. If dependency mediation changes the MSAL family, resolve it in `pom.xml` and rerun the dependency/security scan before proceeding.
  Dependencies: Task 3.1

### 6. Redact Azure credentials from logs, debug output, and telemetry

- [x] **6.1 Centralize redaction before Sentry and diagnostics consume options**
  Files: `src/main/java/org/replicadb/config/CredentialRedactor.java`, `src/main/java/org/replicadb/cli/ToolOptions.java`, `src/main/java/org/replicadb/cli/OptionsFile.java`, `src/main/java/org/replicadb/config/Sentry.java`, `src/main/java/org/replicadb/ReplicaDB.java`, `src/main/java/org/replicadb/ReplicaTask.java`, `src/main/resources/log4j2.xml`
  Changes: Add key-aware redaction for `password`, `accessToken`, `clientSecret`, `aadSecurePrincipalSecret`, `AADSecurePrincipalSecret`, `clientKeyPassword`, `privateKeyPassword`, `secretKey`, `sasToken`, `sentry.dsn`, URL user-info, and sensitive URL query parameters. Mask `user`, `username`, `userName`, `loginHint`, `principalId`, `clientId`, `tenantId`, and `msiClientId` in telemetry while retaining only non-sensitive host/database context. In `Sentry.java`, pass redacted copies rather than the original `Properties` objects, redact `source.connect`/`sink.connect` tags, and register a Sentry `beforeSend`/event processor that scrubs exception values, chained causes, contexts, tags, and breadcrumbs. In `ReplicaDB` and `ReplicaTask`, sanitize authentication-related messages before `LOG.error` and `Sentry.captureException` while preserving the original cause for local control flow. Remove the raw connection-parameter context path; `log4j2.xml` changes only if needed to enforce the same policy for exception layouts.
  Tests: Add `src/test/java/org/replicadb/config/CredentialRedactorTest.java` covering Java `Properties`, JDBC URLs, nested or case-variant keys, every listed secret and identity key, nulls, non-secret properties, and synthetic exception/cause chains containing sentinel secrets. Assert through `ToolOptions.toString()`, the Sentry event-preparation seam, `ReplicaDB` error handling, and `ReplicaTask` error handling that configured sentinel values never appear.
  Dependencies: Tasks 1.1, 2.1, and 4.1

### 7. Document the deployment matrix and configuration contract

- [x] **7.1 Add local, external, and Azure-hosted runbooks and wizard support**
  Files: `README.md`, `docs/docs/docs.md`, `docs/index.md`, `conf/_replicadb.conf`, `docs/wizard/index.html`, `RELEASE_GUIDE.md`
  Changes: Document both source and sink configuration, the new CLI/options-file names, driver-native advanced parameters, and environment-variable conventions without real endpoints or credentials. Add separate examples for local Interactive/MFA, local `ActiveDirectoryDefault` after `az login`, outside-Azure service principal secret and certificate, Azure managed identity, Azure workload/default credential, and `ActiveDirectoryIntegrated` limitations. Explain Azure SQL contained users, firewall/network access, certificate file permissions, headless container limitations, `jobs=1` for Interactive, and the deprecation of `ActiveDirectoryPassword`. Update release guidance to mention the bundled Azure Identity runtime libraries. Extend the existing wizard source and sink connection-settings panels around `CardSourceConnectionSettings` and `CardSinkConnectionSettings` with SQL Server-only authentication-mode selectors and non-secret fields for login hint, principal ID, certificate path, and client key path. Update the `configTemplate` Handlebars output to emit `source.auth.*` and `sink.auth.*` properties, omit password placeholders for Interactive/Default/Managed Identity/Integrated modes, retain the existing secret placeholder convention for Service Principal, and preserve the existing `extraJdbcParams` helper for advanced properties. Hide or disable Azure controls for non-SQL Server connection types, make the generated configuration explicit about `jobs=1` for Interactive, and never add client secrets, access tokens, certificate contents, tenant secrets, or a hard-coded Sentry DSN to generated examples.
  Tests: Run documentation build/lint checks, `git diff --check`, placeholder scans for credential-bearing values, and searches confirming every documented CLI option matches `ToolOptions`. Exercise the wizard in its browser test/build path with SQL Server source and sink selections, each supported auth mode, source-only and sink-only configurations, non-SQL Server control hiding, Interactive `jobs=1` generation, environment-variable placeholders, and absence of secret values from generated output. Verify the README Java 17/runtime statements remain consistent with the packaged images.
  Dependencies: Tasks 1.1, 2.1, 5.1, and 6.1

### 8. Add deterministic unit coverage for configuration and mapping

- [x] **8.1 Execute the focused no-cloud test slice**
  Files: `src/test/java/org/replicadb/cli/AzureAuthenticationOptionsTest.java`, `src/test/java/org/replicadb/manager/SQLServerManagerAuthenticationTest.java`, `src/test/java/org/replicadb/config/CredentialRedactorTest.java`, `src/test/java/org/replicadb/ReplicaTaskAuthenticationFailureTest.java`
  Changes: Complete the unit tests from Tasks 1 through 6 and ensure they do not require Azure credentials, a browser, Docker, or network access. Add cases for source/sink symmetry, command-line precedence, invalid mode combinations, driver property capture, task cleanup, secret redaction, and backward-compatible ordinary JDBC authentication.
  Tests: Run the focused JUnit Jupiter 6 classes, then run the full Docker-free unit suite. Review reports for discovery, linkage, and secret leakage; do not treat skipped cloud tests as unit-test success.
  Dependencies: Tasks 1.1 through 6.1

### 9. Verify local Interactive and Azure CLI authentication manually

- [x] **9.1 Add an opt-in real Azure SQL integration test for local execution**
  Files: `src/test/java/org/replicadb/sqlserver/SqlserverAzureAuthenticationTest.java`, `src/test/resources/replicadb-azure-auth.properties`, `pom.xml`
  Changes: Add a JUnit Jupiter test class gated with `@EnabledIfSystemProperty(named = "replicadb.azure.auth.enabled", matches = "true")`; normal Maven runs must discover it as skipped without contacting Azure. Read only environment-expanded placeholders from `REPLICADB_AZURE_SOURCE_CONNECT`, `REPLICADB_AZURE_SINK_CONNECT`, `REPLICADB_AZURE_SOURCE_TABLE`, `REPLICADB_AZURE_SINK_TABLE`, `REPLICADB_AZURE_INTERACTIVE_USER`, and the mode-specific identity variables defined in the fixture template. Exercise the product path, not a standalone driver sample, for Interactive/MFA with `jobs=1` and `ActiveDirectoryDefault` after `az login` or with Azure Identity environment credentials. Use the same source/sink lifecycle and assert a non-sensitive identity query plus a minimal replication operation. Keep the fixture template credential-free and document the exact command `mvn -Dreplicadb.azure.auth.enabled=true ...`.
  Tests: Run the Interactive case on a local desktop with a browser; run the Default case after `az login`; test missing dependency, browser-unavailable/headless JVM, invalid permission, firewall failure, and expired/invalid credential diagnostics. Confirm no token, password, URL secret, or tenant-specific value appears in output. Normal unit/CI execution must prove that the gate prevents network access.
  Dependencies: Tasks 3.1, 4.1, 5.1, 6.1, and 7.1

### 10. Verify outside-Azure unattended authentication

- [x] **10.1 Cover service principal secret and certificate modes through the product path**
  Files: `src/test/java/org/replicadb/sqlserver/SqlserverAzureAuthenticationTest.java`, `src/test/resources/replicadb-azure-auth.properties`, `docs/docs/docs.md`
  Changes: Extend the opt-in integration test and runbook for `ActiveDirectoryServicePrincipal` and `ActiveDirectoryServicePrincipalCertificate`. Read principal IDs, secrets, certificate paths, and key passwords only from environment-expanded properties. Exercise both source and sink roles independently, validate minimal replication, and document certificate rotation and file-permission requirements. Do not add credentials to GitHub repository variables, test resources, logs, or reports.
  Tests: Run secret-based and certificate-based cases outside Azure; verify invalid secret, expired certificate, missing key, wrong principal, insufficient database permission, and network failure. Run the same tests with parallel non-interactive jobs to confirm the driver/token behavior does not trigger browser flows or leak secrets.
  Dependencies: Tasks 5.1, 6.1, 7.1, and 9.1

### 11. Verify Azure-hosted execution and release delivery

- [x] **11.1 Add protected, opt-in cloud smoke coverage and final packaging acceptance**
  Files: `.github/workflows/azure-auth.yml`, `.github/actionlint.yaml`, `.github/workflows/CT_Push.yml`, `.github/workflows/CI_Release.yml`, `Dockerfile`, `Containerfile`, `RELEASE_GUIDE.md`, `scripts/README.md`
  Changes: Add a `workflow_dispatch`-only workflow using the GitHub Environment `azure-auth` with required reviewers, restricted branches, and environment-scoped non-secret variables for subscription, tenant, and client IDs. The OIDC job must grant `id-token: write`, use `azure/login@v2`, and test `ActiveDirectoryDefault` after Azure CLI login without a client secret. Add a separate job selector for an Azure self-hosted runner/VM with system-assigned or user-assigned managed identity to test `ActiveDirectoryManagedIdentity`; the workflow must not provision or delete infrastructure or contain certificates/secrets. Document the required Azure SQL contained users/roles, firewall/private-network access, runner identity role assignments, environment protection settings, and cleanup/rotation ownership. Keep normal CI cloud-independent. Add package checks that run the same opt-in smoke command from the executable release, Temurin image, and UBI image.
  Tests: Run ordinary CI without Azure credentials and prove the workflow is not part of required checks. Run the protected workflow with OIDC/Default credential, a user-assigned identity, and a system-assigned identity where infrastructure exists; run an external Azure runner smoke test; verify source and sink combinations, parallel non-interactive jobs, token refresh during a long enough operation, and cleanup after authentication failure. Validate workflow YAML, permissions, environment references, and the absence of secret literals. Ensure skipped cloud jobs are reported as opt-in rather than passed product coverage.
  Dependencies: Tasks 5.1 through 10.1

## Technical Reference

### Types and data structures

`AzureAuthenticationMode` is the canonical enum for accepted first-class values and driver aliases. `AzureAuthenticationOptions` holds only non-secret source/sink configuration. `ToolOptions` owns the two option objects and preserves existing flat username/password fields for compatibility. `SqlManager` owns common JDBC property assembly; `SQLServerManager` owns the vendor-specific mapping.

The first-class fields must never store or print secret values beyond the existing password fields, which are already masked by `ToolOptions.toString()`. Advanced properties such as `clientKeyPassword`, `accessToken`, and custom `accessTokenCallbackClass` remain available through the existing connection-parameter mechanism but receive the same redaction treatment.

### Dependency and classpath contract

- `com.microsoft.sqlserver:mssql-jdbc:13.2.1.jre11` remains the SQL Server driver baseline unless an opt-in integration test demonstrates a driver defect.
- Add `com.azure:azure-identity:1.15.3` as a normal runtime dependency so its transitive MSAL4J dependencies are present in test assemblies, release `lib`, and container images.
- Confirm the resolved MSAL4J and persistence-extension versions against the driver dependency family and review dependency/security scanners for transitive changes.
- Keep the Java 17 baseline and `--add-opens=java.base/java.nio=ALL-UNNAMED` launcher/Surefire behavior unchanged.

### Testing strategy

The normal test suite must remain cloud-independent. Unit tests capture driver properties without opening a real network connection. Real Azure SQL tests are opt-in and environment-gated. Test configuration uses placeholders and `${ENV_NAME}` expansion only.

Minimum environment matrix:

| Environment | Authentication | Required verification |
| --- | --- | --- |
| Local desktop | Interactive | Browser/MFA, `jobs=1`, source and sink lifecycle |
| Local desktop/CI | Default | Azure CLI or environment credential, no browser loop |
| Outside Azure | Service principal | Secret mode and permission failure |
| Outside Azure | Certificate | Certificate/key loading, rotation and expiry failure |
| Azure host | Managed identity | System and user-assigned identity |
| Azure host or CI | Default/workload identity | OIDC/workload credential chain |
| Headless container | Interactive | Explicitly rejected or documented as unsupported |

### Performance and operational constraints

Non-interactive connections may keep the existing parallel task model, but each task continues to own its own connection. The Microsoft driver is responsible for token caching and refresh. Interactive authentication is serialized at the job level by requiring `jobs=1`; no JDBC connection sharing or incidental pooling is introduced.

Authentication validation must happen before Sentry initialization and before task submission. Authentication errors must retain their root causes while exposing only sanitized mode and endpoint context. Cleanup must close task-owned resources even when one side authenticates and the other side fails.

### Rollback and compatibility

All new fields are optional. A job without `source.auth.*` or `sink.auth.*` follows the current connection path. Removing the first-class fields from an options file returns the job to raw JDBC parameter behavior. The dependency addition is additive; if the Azure integration test uncovers a transitive conflict, isolate or align the dependency version rather than changing unrelated database drivers.

## Quality Gate

Before `/itx-code` execution, confirm:

- Every task has a concrete file list, change description, focused tests, and dependency ordering.
- No task requires committed credentials, a default Azure subscription, or a browser in ordinary CI.
- Source and sink behavior are both covered.
- Interactive and non-interactive modes are distinguished at validation and documentation boundaries.
- Local, outside-Azure, and Azure-hosted deployment paths are all represented in tests or protected runbooks.
- Runtime dependencies are verified in the jar assembly, release archive, launcher classpath, Docker image, and UBI image.
- Sentry, debug output, error messages, and test reports cannot expose credentials or tokens.

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy

- Tasks completed as planned: 11/11 (100%).
- Tasks that required plan adjustment: 4/11 (36%).
- Test loop iterations: 11 total (first-pass 7, second-pass 4, third-pass 0).

### Gaps Encountered

#### Gap 1: Raw JDBC Interactive authentication needed parallelism protection (Intent-to-Plan)

- **Task**: 4.1 — Enforce lifecycle and parallelism rules for interactive authentication.
- **Plan assumed**: The first-class `source.auth.mode`/`sink.auth.mode` validation was sufficient to prevent concurrent browser flows.
- **Reality**: Issue #206 also describes `Authentication=ActiveDirectoryInteractive` directly in the JDBC URL, and the existing advanced parameter pass-through bypasses first-class mode validation.
- **Resolution**: `ManagerFactory` now detects raw SQL Server `authentication=ActiveDirectoryInteractive` in URLs and advanced properties and enforces `jobs=1`; a regression test covers the URL form.
- **Learning**: When adding typed configuration over an existing pass-through API, apply safety invariants to both configuration paths.

#### Gap 2: Windows launcher retained unconditional headless mode (Intent-to-Plan)

- **Task**: 5.1 — Package Microsoft identity dependencies for every supported runtime.
- **Plan assumed**: Updating the Unix launcher and container runtime settings covered local Interactive execution.
- **Reality**: `bin/replicadb.cmd` independently passed `-Djava.awt.headless=true`, which would prevent browser-based authentication on Windows.
- **Resolution**: The Windows launcher now applies headless mode only when `REPLICADB_HEADLESS=true`, matching the Unix launcher behavior.
- **Learning**: Cross-platform launcher behavior must be checked independently even when the Java invocation is otherwise shared.

#### Gap 3: Prefixed configuration keys bypassed initial redaction (Intent-to-Plan)

- **Task**: 6.1 — Centralize redaction before Sentry and diagnostics consume options.
- **Plan assumed**: Matching exact keys such as `password`, `accessToken`, and `clientId` covered the options-file and telemetry surfaces.
- **Reality**: Keys such as `source.password`, `sink.auth.principal.id`, and `sentry.dsn` normalize to prefixed names and were not initially recognized; `OptionsFile.printProperties()` also printed raw values.
- **Resolution**: Redaction now matches sensitive tokens inside prefixed keys, masks Sentry DSNs in `ToolOptions.toString()`, and prints only redacted option-file properties.
- **Learning**: Security key matchers must test the complete names emitted by configuration boundaries, not only driver-level leaf names.

#### Gap 4: Local archive integrity needed an explicit pre-image check (Plan-to-Implementation)

- **Task**: 11.1 — Verify Azure-hosted execution and release delivery.
- **Plan assumed**: A locally created release archive could be passed directly to both image builds.
- **Reality**: The first macOS-generated temporary archive was truncated and included extended attributes; Docker correctly failed while extracting it.
- **Resolution**: Rebuilt the smoke-test archive with `COPYFILE_DISABLE=1`, verified it with `gzip -t`, then built both Temurin and UBI images successfully.
- **Learning**: Validate archive integrity before image construction, especially when packaging from macOS and checking Linux containers.

### Validation Limits

The complete Maven matrix reached 117 tests but was stopped by local Testcontainers infrastructure after 40 DB2/Oracle errors and an exit 130/143. DB2 failed with communication error `-4499`; Oracle containers could not provide mapped ports under local ARM64 emulation. No assertion failures occurred in the changed Azure, security, SQL Server, or focused non-Azure regression tests. The opt-in Azure tests compiled and were safely skipped because no Azure credentials/endpoints were available; real Azure OIDC, managed identity, browser MFA, service-principal, and certificate runs remain protected environment checks.

### Post-implementation scope adjustment

The external Azure workflow and credential-gated Azure integration fixture were removed after review because they cannot run in the repository's normal Docker/Testcontainers CI. The reproducible `AzureAuthenticationSimulationTest` remains as the local validation of the ReplicaDB-to-JDBC authentication contract. Real Azure authentication is documented as an environment-specific manual concern rather than an unexecuted repository action.
