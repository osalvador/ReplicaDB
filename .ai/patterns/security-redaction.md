---
type: Pattern
description: Credential references are stored unresolved and dynamic output is redacted at configuration, execution, and API boundaries.
sources:
  - id: redactor
    resource: src/main/java/org/replicadb/config/CredentialRedactor.java
  - id: domain
    resource: replicadb-server/src/main/java/org/replicadb/server/job/domain/ConnectionCredentials.java
  - id: resolver
    resource: replicadb-server/src/main/java/org/replicadb/server/job/execution/DatasourceResolutionService.java
  - id: api
    resource: replicadb-server/src/main/java/org/replicadb/server/job/api/GlobalExceptionHandler.java
  - id: options
    resource: src/main/java/org/replicadb/cli/OptionsFile.java
  - id: metrics
    resource: replicadb-server/src/main/java/org/replicadb/server/observability/ManagedRuntimeMetrics.java
generated: { by: itx-code, at: "2026-08-25T13:42:47Z" }
status: stable
---

Managed datasource profiles retain encrypted security bundles rather than plaintext passwords. The executor decrypts the selected profile only in memory immediately before constructing `ToolOptions`; managed execution never creates an options file. Standalone CLI options files and their environment expansion remain a separate compatibility boundary.

`CredentialRedactor` is applied before API problem details, persisted failure text, audit details, Log4j2/Sentry output, options-file diagnostic output, and operational telemetry. Metrics use bounded tags and never include usernames, job/run ids, DSNs, lease tokens, or resolved credentials. Security tests cover prefixed configuration names, DSN-like values, and connection-string forms. Generated context follows the same rule and records only paths or finding types.

Reference implementations: `CredentialRedactor.java`, `DatasourceResolutionService.java`, and `GlobalExceptionHandler.java`.
